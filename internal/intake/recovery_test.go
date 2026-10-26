package intake

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/FaintGhost/loom/internal/model"
)

func TestDuplicateReplayKeepsDestinationAfterFailureAndClosure(t *testing.T) {
	home, db, cfg, _ := intakeFixture(t)
	target, err := db.AddReport(t.Context(), model.Report{Body: "same broken behavior", Status: model.New, Kind: "bug", IssueNumber: 7, AuthorID: "author", ChannelID: "123", MessageID: "400"})
	if err != nil {
		t.Fatal(err)
	}
	var modelCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		modelCalls.Add(1)
		_, _ = w.Write([]byte(`{"answers":{"kind":{"choice":"bug"},"language":{"choice":"en"},"duplicate_of":{"choice":"r1","confidence":1}}}`))
	}))
	defer server.Close()
	cfg.MagpieURL = server.URL
	cfg.DedupeEnabled = true
	countPath := filepath.Join(home, "views")
	logPath := filepath.Join(home, "writes")
	t.Setenv("TEST_VIEW_COUNT", countPath)
	t.Setenv("TEST_GH_WRITES", logPath)
	script := `#!/bin/sh
case "$*" in
 *'issue view'*)
  count=0
  if [ -f "$TEST_VIEW_COUNT" ]; then read count < "$TEST_VIEW_COUNT"; fi
  count=$((count+1)); printf '%s\n' "$count" > "$TEST_VIEW_COUNT"
  if [ "$count" -eq 2 ]; then exit 1; fi
  printf '{"state":"OPEN","labels":[]}' ;;
 *'--method POST'*)
  case "$2" in
   */comments) printf 'comment\n' >> "$TEST_GH_WRITES"; printf '{"id":9}' ;;
   *) printf 'issue\n' >> "$TEST_GH_WRITES"; printf '{"number":8,"html_url":"https://github.com/owner/repo/issues/8"}' ;;
  esac ;;
 *'/comments'*) printf '[[]]' ;;
 *'issues?state=all'*) printf '[[]]' ;;
 *) printf '{}' ;;
esac
`
	if err = os.WriteFile(filepath.Join(home, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", home+":"+os.Getenv("PATH"))
	message := Message{ID: "401", ChannelID: "123", AuthorID: "author", Content: "same broken behavior again"}
	if _, err = Accept(t.Context(), home, cfg, db, message); err == nil {
		t.Fatal("expected interrupted follow-up")
	}
	linked, err := db.ByMessage(t.Context(), message.ID)
	if err != nil || linked.ID != target.ID {
		t.Fatalf("follow-up destination was not linked: %v", err)
	}
	destination, found, err := duplicateRoute(t.Context(), db, message)
	if err != nil || !found || destination != target.ID {
		t.Fatal("duplicate destination was not durable before follow-up")
	}
	if _, err = db.Transition(t.Context(), target.ID, model.New, model.Closed, nil); err != nil {
		t.Fatal(err)
	}
	cfg.MagpieURL = "http://127.0.0.1:1"
	cfg.DedupeEnabled = false
	replay, err := Replay(t.Context(), home, cfg, db, key(message))
	if err != nil {
		t.Fatal(err)
	}
	if replay.ReportID != target.ID || replay.Issue != 7 || !replay.Duplicate || modelCalls.Load() != 2 {
		t.Fatalf("replay changed destination or reranked: %+v calls=%d", replay, modelCalls.Load())
	}
	writes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(writes) != "comment\n" {
		t.Fatalf("duplicate replay created a second issue: %s", writes)
	}
	rows, err := db.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.ID != target.ID && row.IssueNumber != 0 {
			t.Fatal("duplicate evidence row acquired an issue")
		}
	}
}

func TestFreshReadyCatchesGapWithoutAdvancingFromLiveMessage(t *testing.T) {
	home, db, _, _ := intakeFixture(t)
	if _, err := db.AddReport(t.Context(), model.Report{Body: "old", Status: model.Held, ChannelID: "123", MessageID: "100"}); err != nil {
		t.Fatal(err)
	}
	cursors, err := loadCursors(t.Context(), home, db, []string{"123"})
	if err != nil {
		t.Fatal(err)
	}
	// A live message arrives after an unresumable outage, before its READY is serviced.
	if _, err = db.AddReport(t.Context(), model.Report{Body: "new live", Status: model.Held, ChannelID: "123", MessageID: "300"}); err != nil {
		t.Fatal(err)
	}
	reloaded, err := loadCursors(t.Context(), home, db, []string{"123"})
	if err != nil {
		t.Fatal(err)
	}
	if reloaded["123"] != "100" {
		t.Fatal("live message skipped unreconciled gap")
	}
	ready := make(chan struct{}, 1)
	seen := []string{}
	fetches := 0
	for _, id := range []string{"200", "400"} {
		signalReady(ready)
		select {
		case <-ready:
		default:
			t.Fatal("fresh READY did not request catch-up")
		}
		fetch := func(after string) ([]*discordgo.Message, error) {
			fetches++
			expected := "100"
			if id == "400" {
				expected = "200"
			}
			if after != expected {
				t.Fatalf("gap skipped: after %s, want %s", after, expected)
			}
			return []*discordgo.Message{{ID: id, ChannelID: "123", Author: &discordgo.User{ID: "author"}, Content: "missed message"}}, nil
		}
		if err = reconcileChannel(home, "123", cursors, fetch, func(m Message) error { seen = append(seen, m.ID); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if fetches != 2 || !slices.Equal(seen, []string{"200", "400"}) {
		t.Fatalf("READY gaps not recovered: %v", seen)
	}
	persisted, err := loadCursors(t.Context(), home, db, []string{"123"})
	if err != nil {
		t.Fatal(err)
	}
	if persisted["123"] != "400" {
		t.Fatal("REST cursor was not durable")
	}
}

func TestCatchupKeepsCursorWhenMessageCannotBePreserved(t *testing.T) {
	home, db, _, _ := intakeFixture(t)
	cursors, err := loadCursors(t.Context(), home, db, []string{"123"})
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("disk unavailable")
	fetch := func(string) ([]*discordgo.Message, error) {
		return []*discordgo.Message{{ID: "200", ChannelID: "123", Author: &discordgo.User{ID: "author"}, Content: "must not skip"}}, nil
	}
	if err = reconcileChannel(home, "123", cursors, fetch, func(Message) error { return failure }); !errors.Is(err, failure) {
		t.Fatal("missing persistence error")
	}
	data, err := os.ReadFile(filepath.Join(home, "discord-cursors.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "200") || cursors["123"] != "" {
		t.Fatal("cursor advanced past unpreserved message")
	}
	seen := false
	if err = reconcileChannel(home, "123", cursors, fetch, func(Message) error { seen = true; return nil }); err != nil {
		t.Fatal(err)
	}
	if !seen || cursors["123"] != "200" {
		t.Fatal("failed page did not retry")
	}
}

func TestReopenReceiptSurvivesCrashAndLaterWorkerFailure(t *testing.T) {
	home, db, cfg, calls := intakeFixture(t)
	fakeGH(t, home)
	cfg.MaxReopens = 1
	row, err := db.AddReport(t.Context(), model.Report{Body: "need reproduction", Status: model.NeedsReporter, Attempt: 3, IssueNumber: 7, AuthorID: "author", ChannelID: "123", MessageID: "500"})
	if err != nil {
		t.Fatal(err)
	}
	message := Message{ID: "501", ChannelID: "123", AuthorID: "author", ReplyTo: "500", Attachments: []Attachment{{URL: "https://cdn.discordapp.com/reproduction.png"}}}
	if _, err = db.Reserve(t.Context(), "intake:"+key(message), "intake", 0, jsonText(message)); err != nil {
		t.Fatal(err)
	}
	if _, err = db.AppendFollowup(t.Context(), row.ID, map[string]any{"author_id": "author", "body": "", "trusted": true, "message_id": message.ID}); err != nil {
		t.Fatal(err)
	}
	if err = db.LinkMessage(t.Context(), message.ID, row.ID, "report"); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash immediately after the store commit: no caller-side event
	// recording or intake completion occurs before the worker starts again.
	if _, err = db.RequeueMessage(t.Context(), row.ID, model.NeedsReporter, model.New, map[string]any{"error": ""}, message.ID, cfg.MaxReopens); err != nil {
		t.Fatal(err)
	}
	claimed, err := db.Claim(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != row.ID || claimed.Attempt != 4 {
		t.Fatal("worker did not claim a fresh evidence attempt")
	}
	if err = db.Finish(t.Context(), row.ID, model.VerifyFailed, "new evidence still fails"); err != nil {
		t.Fatal(err)
	}
	replay, err := Replay(t.Context(), home, cfg, db, key(message))
	if err != nil {
		t.Fatal(err)
	}
	saved, err := db.Get(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != "verify_failed" || saved.Status != model.VerifyFailed || saved.Attempt != 4 || saved.RetryBase != 3 || calls.Load() != 0 {
		t.Fatal("same message reopened or reset the later failed attempt")
	}
	next := message
	next.ID = "502"
	denied, err := Accept(t.Context(), home, cfg, db, next)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(denied.Reply, "retry limit") || denied.Status != "verify_failed" {
		t.Fatal("crash bypassed MaxReopens")
	}
	events, err := db.Events(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Kind == "reopened" {
			count++
			if event.Detail != message.ID {
				t.Fatal("unexpected reopen receipt")
			}
		}
	}
	if count != 1 {
		t.Fatalf("want one atomic reopen receipt, got %d", count)
	}
}
