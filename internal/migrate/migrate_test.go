package migrate

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
)

func legacyFixture(t *testing.T, status string) (string, string) {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "python")
	destination := filepath.Join(root, "go")
	repo := filepath.Join(root, "clone")
	for _, dir := range []string{source, filepath.Join(repo, ".git"), filepath.Join(source, "jobs", "17", "attempt-2")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	config := fmt.Sprintf("[repo]\nslug='example/project'\nclone=%q\nverify='true'\n[harness]\ndefault='codex'\ngui='codex'\nmode='headless'\n[harness.codex]\nkind='codex'\ncmd=['codex','exec','--sandbox','workspace-write','{prompt}']\n", repo)
	if err := os.WriteFile(filepath.Join(source, "config.toml"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "jobs", "17", "attempt-2", "verify.txt"), []byte("original evidence\n"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(source, "store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE reports(id INTEGER PRIMARY KEY,source TEXT,source_ref TEXT,body TEXT,status TEXT,attempts INTEGER,worktree TEXT,issue_number INTEGER,pr_number INTEGER,branch TEXT,created_at TEXT,updated_at TEXT,followups TEXT,unknown_column TEXT);
 CREATE TABLE messages(discord_message_id TEXT PRIMARY KEY,report_id INTEGER,role TEXT,at TEXT);
 CREATE TABLE events(id INTEGER PRIMARY KEY,report_id INTEGER,at TEXT,kind TEXT,detail TEXT);
 CREATE TABLE extra_evidence(payload TEXT);
 INSERT INTO extra_evidence VALUES ('unknown table retained');
 INSERT INTO messages VALUES ('message-17',17,'report','2026-01-01T00:00:00Z');
 INSERT INTO messages VALUES ('bot-17',17,'bot','2026-01-01T00:00:00Z');
 INSERT INTO events VALUES (81,17,'2026-01-02T00:00:00Z','verified','original audit');
 INSERT INTO events VALUES (82,NULL,'2026-01-02T00:00:00Z','retro','global audit');`)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO reports VALUES (17,'discord','{"message":"message-17","channel":"channel-1"}','retained report',?,2,'/legacy/worktree',31,45,'feedback/17','2026-01-01T00:00:00Z','2026-01-02T00:00:00Z','[{"body":"prior follow-up"}]','future evidence')`, status)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	return source, destination
}

func TestImportRetainsIdentityEvidenceAndIsIdempotent(t *testing.T) {
	source, destination := legacyFixture(t, "pr_open")
	before, err := os.ReadFile(filepath.Join(source, "store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := Import(t.Context(), source, destination)
	if err != nil {
		t.Fatal(err)
	}
	if result.Reports != 1 || result.Messages != 2 || result.Events != 2 || result.AlreadyImported {
		t.Fatalf("import result: %+v", result)
	}
	after, err := os.ReadFile(filepath.Join(source, "store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("source database changed")
	}
	cfg, err := worker.Load(destination)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Publish || cfg.Retro.Enabled || cfg.GitHubRepo != "example/project" {
		t.Fatalf("unsafe imported config: %+v", cfg)
	}
	db, err := store.Open(filepath.Join(destination, "go-store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	report, err := db.ByMessage(t.Context(), "bot-17")
	if err != nil {
		t.Fatal(err)
	}
	if report.ID != 17 || report.Status != model.PROpen || report.IssueNumber != 31 || report.PRNumber != 45 || report.Attempt != 2 || report.Worktree != "" || report.LegacyWorktree != "/legacy/worktree" || report.Followups != `[{"body":"prior follow-up"}]` {
		t.Fatalf("lost report metadata: %+v", report)
	}
	if _, err = db.AppendFollowup(t.Context(), 17, map[string]any{"message_id": "next", "body": "more context"}); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Transition(t.Context(), 17, model.PROpen, model.ReviseRequested, map[string]any{"pending_review": "read existing PR"}); err != nil {
		t.Fatal(err)
	}
	archive, err := os.ReadFile(filepath.Join(destination, "legacy", "database.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(archive, []byte("future evidence")) || !bytes.Contains(archive, []byte("unknown table retained")) {
		t.Fatal("unknown legacy data discarded")
	}
	evidence, err := os.ReadFile(filepath.Join(destination, "jobs", "17", "attempt-2", "verify.txt"))
	if err != nil || string(evidence) != "original evidence\n" {
		t.Fatalf("evidence: %q %v", evidence, err)
	}
	again, err := Import(t.Context(), source, destination)
	if err != nil || !again.AlreadyImported {
		t.Fatalf("repeat import: %+v %v", again, err)
	}
	current, err := db.Get(t.Context(), 17)
	if err != nil || current.Status != model.ReviseRequested {
		t.Fatalf("repeat import overwrote live state: %+v %v", current, err)
	}
}

func TestImportFailsClosedAndPreservesOtherStates(t *testing.T) {
	for _, status := range []string{"working", "claimed", "revising", "unknown-future-state", "closed", "conversation", "queued"} {
		t.Run(status, func(t *testing.T) {
			source, destination := legacyFixture(t, status)
			_, err := Import(t.Context(), source, destination)
			rejected := status == "working" || status == "claimed" || status == "revising" || status == "unknown-future-state"
			if rejected {
				if err == nil {
					t.Fatal("unsafe state imported")
				}
				if _, e := os.Stat(destination); !os.IsNotExist(e) {
					t.Fatal("failed import published destination")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			db, err := store.Open(filepath.Join(destination, "go-store.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			}()
			r, err := db.Get(t.Context(), 17)
			if err != nil || string(r.Status) != status {
				t.Fatalf("state lost: %+v %v", r, err)
			}
			if status == "queued" {
				r, err = db.Claim(t.Context())
				if err != nil || r.ID != 17 || r.Attempt != 3 {
					t.Fatalf("queue claim: %+v %v", r, err)
				}
			}
		})
	}
}

func TestLegacyConfigRejectsUnknownAndUnsafeKeys(t *testing.T) {
	source, _ := legacyFixture(t, "closed")
	data, err := os.ReadFile(filepath.Join(source, "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"\n[unknown]\nvalue=true\n", "\n[sandbox]\nnetwork=true\n"} {
		if _, err := ParseConfig(append(append([]byte{}, data...), []byte(suffix)...)); err == nil {
			t.Fatal("unsafe config accepted")
		}
	}
	_, err = ParseConfig(append(data, []byte("\n[unknown]\nvalue=true\n")...))
	if err == nil || !strings.Contains(err.Error(), "unknown.value") {
		t.Fatalf("missing exact unknown key: %v", err)
	}
}

func TestImportPreservesFailedIntakeAndOutcomeReceipts(t *testing.T) {
	source, destination := legacyFixture(t, "pr_open")
	record := `{"at":"2026-01-03T00:00:00Z","source_ref":"{\"message\":\"pending-99\",\"channel\":\"channel-1\",\"guild\":\"guild-1\"}","author":"Reporter","author_id":"user-1","content":"What does this calculate?","attachments":["https://cdn.discordapp.com/attachments/example.png"],"conversation_context":"Previous user turn"}`
	if err := os.WriteFile(filepath.Join(source, "intake-failed.jsonl"), []byte(record+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := Import(t.Context(), source, destination)
	if err != nil {
		t.Fatal(err)
	}
	if result.PendingIntakes != 1 {
		t.Fatalf("pending intakes: %+v", result)
	}
	db, err := store.Open(filepath.Join(destination, "go-store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	reservation, err := db.Reservation(t.Context(), "intake:discord:pending-99")
	if err != nil || reservation.State != "reserved" || !strings.Contains(reservation.Payload, "Previous user turn") {
		t.Fatalf("lost failed intake: %+v %v", reservation, err)
	}
	receipt, err := db.Reservation(t.Context(), "result:17:2:0:pr_open:complete")
	if err != nil || receipt.State != "complete" {
		t.Fatalf("legacy result would redeliver: %+v %v", receipt, err)
	}
}
