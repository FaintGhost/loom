package intake

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
)

func intakeFixture(t *testing.T) (string, *store.Store, worker.Config, *atomic.Int32) {
	t.Helper()
	home := t.TempDir()
	db, err := store.Open(filepath.Join(home, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	calls := new(atomic.Int32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/systemone" || r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("wrong Jev protocol: %s", r.URL.Path)
		}
		var payload struct {
			Model     string            `json:"model"`
			State     map[string]string `json:"state"`
			Questions map[string]any    `json:"questions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if payload.Model != "jev" || payload.Questions["kind"] == nil {
			t.Error("missing System One model/questions")
		}
		kind := "question"
		if strings.HasPrefix(payload.State["message"], "Please change") {
			kind = "feature_request"
			if !strings.Contains(payload.State["conversation_context"], "Previous user message") {
				t.Error("explicit change lost conversation context")
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"answers": map[string]any{"kind": map[string]any{"choice": kind}, "language": map[string]any{"choice": "en"}, "work": map[string]any{"score": .2}}})
	}))
	t.Cleanup(server.Close)
	t.Setenv("TEST_MAGPIE_KEY", "test-key")
	cfg := worker.Config{MagpieURL: server.URL, MagpieKeyEnv: "TEST_MAGPIE_KEY", DeciderModel: "jev", GitHubRepo: "owner/repo", DiscordChannel: "123", AllowedUsers: []string{"author"}, AutoKinds: []string{"bug"}, MaxAutoWork: .5, MaxQueue: 20}
	return home, db, cfg, calls
}
func fakeGH(t *testing.T, home string) string {
	t.Helper()
	log := filepath.Join(home, "gh-args")
	t.Setenv("TEST_GH_ARGS", log)
	script := `#!/bin/sh
printf '%s\n' "$@" >> "$TEST_GH_ARGS"
case "$*" in
 *'--method POST'*) case "$2" in */comments) printf '{"id":9}' ;; *) printf '{"number":7,"html_url":"https://github.com/owner/repo/issues/7"}' ;; esac ;;
 *'issues?state=all'*) printf '[[]]' ;;
 *'/comments'*) printf '[[]]' ;;
 *'issue view'*) printf '{"state":"OPEN","labels":[]}' ;;
 *) printf '{}' ;;
esac
`
	if err := os.WriteFile(filepath.Join(home, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", home+":"+os.Getenv("PATH"))
	return log
}
func TestConversationFollowupExplicitWorkAndReplay(t *testing.T) {
	home, db, cfg, calls := intakeFixture(t)
	log := fakeGH(t, home)
	first := Message{ID: "100", ChannelID: "123", AuthorID: "author", AuthorName: "Reporter", Content: "How is tax calculated?"}
	result, err := Accept(t.Context(), home, cfg, db, first)
	if err != nil {
		t.Fatal(err)
	}
	row, err := db.Get(t.Context(), result.ReportID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != model.Held || row.IssueNumber != 0 || row.Kind != "question" {
		t.Fatalf("question escaped held conversation: %+v", row)
	}
	if _, err = os.Stat(log); !os.IsNotExist(err) {
		t.Fatal("question contacted GitHub")
	}
	if err = db.LinkMessage(t.Context(), "101", row.ID, "bot"); err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID = "102"
	second.ReplyTo = "101"
	second.Content = "What about rounding?"
	follow, err := Accept(t.Context(), home, cfg, db, second)
	if err != nil {
		t.Fatal(err)
	}
	if follow.ReportID == result.ReportID || follow.Issue != 0 {
		t.Fatal("follow-up was not a fresh local turn")
	}
	third := first
	third.ID = "103"
	third.ReplyTo = "102"
	third.Content = "Please change it to round half up."
	// Simulate interruption immediately after raw intake was durably reserved.
	if _, err = db.Reserve(t.Context(), "intake:"+key(third), "intake", 0, jsonText(third)); err != nil {
		t.Fatal(err)
	}
	work, err := Replay(t.Context(), home, cfg, db, key(third))
	if err != nil {
		t.Fatal(err)
	}
	if work.Issue != 7 || work.Status != "awaiting_approval" {
		t.Fatalf("wrong work policy: %+v", work)
	}
	row, err = db.Get(t.Context(), work.ReportID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(row.Body, "What about rounding?") || !strings.Contains(row.Body, "only the latest message above authorizes") {
		t.Fatal("work lost bounded prior context")
	}
	before, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Accept(t.Context(), home, cfg, db, third)
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate || again.ReportID != work.ReportID || string(before) != string(after) || calls.Load() != 3 {
		t.Fatal("replay repeated model/GitHub side effects")
	}
}
func TestGatedReplyNeverCallsModelOrGitHub(t *testing.T) {
	home, db, cfg, calls := intakeFixture(t)
	log := fakeGH(t, home)
	row, err := db.AddReport(t.Context(), model.Report{Body: "broken", Status: model.NeedsReporter, Attempt: 3, AuthorID: "author", Kind: "bug", IssueNumber: 7, MessageID: "200", Brief: "stale generated brief", TriageDecision: `{"needs_info":true}`})
	if err != nil {
		t.Fatal(err)
	}
	result, err := Accept(t.Context(), home, cfg, db, Message{ID: "201", ChannelID: "123", AuthorID: "outsider", Content: "Please fix this", ReplyTo: "200"})
	if err != nil {
		t.Fatal(err)
	}
	saved, err := db.Get(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Status != model.NeedsReporter || result.ReportID != row.ID || calls.Load() != 0 {
		t.Fatal("gated reply performed work")
	}
	if _, err = os.Stat(log); !os.IsNotExist(err) {
		t.Fatal("gated reply contacted GitHub")
	}
	if !strings.Contains(saved.Followups, `"trusted":false`) {
		t.Fatal("untrusted follow-up can steer worker")
	}
	reopened, err := Accept(t.Context(), home, cfg, db, Message{ID: "202", ChannelID: "123", AuthorID: "author", ReplyTo: "200", Attachments: []Attachment{{URL: "https://cdn.discordapp.com/evidence.png"}}})
	if err != nil {
		t.Fatal(err)
	}
	saved, err = db.Get(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Status != "new" || saved.Brief != "" || saved.TriageDecision != "" || saved.Attempt != 3 || saved.RetryBase != 3 || calls.Load() != 0 {
		t.Fatal("trusted screenshot did not reopen with fresh triage and retry budget")
	}
}
func TestDecisionFailureStaysLocal(t *testing.T) {
	home, db, cfg, _ := intakeFixture(t)
	log := fakeGH(t, home)
	cfg.MagpieURL = "http://127.0.0.1:1"
	result, err := Accept(t.Context(), home, cfg, db, Message{ID: "300", AuthorID: "author", Content: "please fix something"})
	if err != nil {
		t.Fatal(err)
	}
	row, err := db.Get(t.Context(), result.ReportID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Kind != "unclassified" || row.Status != model.Held || row.IssueNumber != 0 {
		t.Fatal("classification failure authorized work")
	}
	if _, err = os.Stat(log); !os.IsNotExist(err) {
		t.Fatal("unknown classification contacted GitHub")
	}
}
func TestAttachmentTrustBoundary(t *testing.T) {
	for _, raw := range []string{"http://cdn.discordapp.com/a", "https://cdn.discordapp.com:443/a", "https://user@cdn.discordapp.com/a", "https://cdn.discordapp.com.evil/a", "https://127.0.0.1/a"} {
		if attachmentURLAllowed(raw) {
			t.Fatalf("unsafe attachment accepted: %s", raw)
		}
	}
	if !attachmentURLAllowed("https://cdn.discordapp.com/attachments/1/file.png?ex=123") {
		t.Fatal("signed Discord attachment rejected")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("123456"))
	}))
	defer server.Close()
	dir := t.TempDir()
	if _, err := saveAttachment(t.Context(), server.Client(), dir, server.URL+"/a.png", 5, []string{"image/png"}); err == nil {
		t.Fatal("oversized attachment accepted")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("partial attachment retained")
	}
}
