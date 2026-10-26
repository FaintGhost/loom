package pipeline

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
)

func publicationFixture(t *testing.T, gate string) (string, worker.Config, *store.Store, model.Report, string) {
	t.Helper()
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("bwrap unavailable")
	}
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	bare := filepath.Join(root, "remote.git")
	if err := os.Mkdir(repo, 0700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"-c", "user.name=test", "-c", "user.email=test@localhost", "commit", "--allow-empty", "-qm", "base"}, {"init", "--bare", bare}, {"remote", "add", "origin", bare}} {
		if _, err := worker.Git(t.Context(), repo, args...); err != nil {
			t.Fatal(err)
		}
	}
	cfg := worker.Config{Repo: repo, Base: "main", Agent: []string{"bash", "-c", "printf verified > change.txt"}, Verify: gate, TimeoutSeconds: 10, GitHubRepo: "owner/repo"}
	home := filepath.Join(root, "home")
	if err := worker.Init(home, cfg); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(filepath.Join(home, "go-store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	row, err := db.AddReport(t.Context(), model.Report{Body: "repair", Title: "Repair report", IssueNumber: 3, Status: model.New})
	if err != nil {
		t.Fatal(err)
	}
	row, runErr := worker.Work(t.Context(), home, cfg, db)
	if gate == "true" && runErr != nil {
		t.Fatal(runErr)
	}
	row, err = db.Patch(t.Context(), row.ID, map[string]any{"branch": "feedback/test"})
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(root, "gh-state")
	log := filepath.Join(root, "gh-log")
	t.Setenv("PIPELINE_GH_STATE", state)
	t.Setenv("PIPELINE_GH_LOG", log)
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$PIPELINE_GH_LOG"
case "$1 $2" in
 "pr create") touch "$PIPELINE_GH_STATE"; printf 'https://github.com/owner/repo/pull/7\n';;
 "api --paginate")
   if test -f "$PIPELINE_GH_STATE"; then
     printf '%s\n' '[[{"number":7,"html_url":"https://github.com/owner/repo/pull/7","state":"open","head":{"ref":"feedback/test","repo":{"full_name":"owner/repo"}}}]]'
   else printf '%s\n' '[[]]'; fi;;
 *) exit 92;;
esac
`
	if err := os.WriteFile(filepath.Join(root, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root+":"+os.Getenv("PATH"))
	cfg.Publish = true
	return home, cfg, db, row, log
}

func TestPublicationCreatesOnlyVerifiedCommit(t *testing.T) {
	home, cfg, db, row, log := publicationFixture(t, "true")
	if err := publish(t.Context(), home, cfg, db, row); err != nil {
		t.Fatal(err)
	}
	saved, err := db.Get(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Status != model.PROpen || saved.PRNumber != 7 || saved.CommitSHA != row.VerifiedSHA {
		t.Fatalf("published %+v", saved)
	}
	out, err := worker.Git(t.Context(), cfg.Repo, "ls-remote", "origin", "refs/heads/feedback/test")
	if err != nil || !strings.HasPrefix(string(out), row.VerifiedSHA) {
		t.Fatalf("remote commit %s %v", out, err)
	}
	calls, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(calls), "pr create") != 1 {
		t.Fatalf("create calls %s", calls)
	}
}

func TestPublicationReconcilesCrashWithoutDuplicatePR(t *testing.T) {
	home, cfg, db, row, log := publicationFixture(t, "true")
	if _, err := db.Reserve(t.Context(), "pr-create:1", "pr-create", row.ID, row.Branch); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("PIPELINE_GH_STATE"), []byte("created before crash"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := publish(t.Context(), home, cfg, db, row); err != nil {
		t.Fatal(err)
	}
	saved, err := db.Get(t.Context(), row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if saved.PRNumber != 7 {
		t.Fatalf("replaced PR %+v", saved)
	}
	calls, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(calls), "pr create") {
		t.Fatalf("duplicate create: %s", calls)
	}
}

func TestFailedGateCannotReachPublication(t *testing.T) {
	home, cfg, db, row, log := publicationFixture(t, "exit 7")
	if row.Status != model.VerifyFailed {
		t.Fatalf("failed gate %+v", row)
	}
	if err := publish(t.Context(), home, cfg, db, row); err == nil {
		t.Fatal("unverified tree published")
	}
	if _, err := os.Stat(log); !os.IsNotExist(err) {
		t.Fatalf("unverified tree contacted GitHub: %v", err)
	}
}

func TestMaintainerBriefIgnoresReporterContract(t *testing.T) {
	comments := []comment{{ID: 1, Body: "## Agent Brief\nmaintainer", Association: "COLLABORATOR", Created: "2026-01-01"}, {ID: 2, Body: "## Agent Brief\nstranger", Association: "NONE", Created: "2026-01-02"}}
	found := latestBrief(comments)
	if found == nil || found.ID != 1 {
		t.Fatalf("untrusted contract accepted: %+v", found)
	}
}

func TestInterruptedPRSetupRetainsRoundAndStopsAtBudget(t *testing.T) {
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	bare := filepath.Join(root, "remote.git")
	if err := os.Mkdir(repo, 0700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"-c", "user.name=test", "-c", "user.email=test@localhost", "commit", "--allow-empty", "-qm", "base"}, {"init", "--bare", bare}, {"remote", "add", "origin", bare}, {"push", "origin", "HEAD:refs/heads/feedback/test"}} {
		if _, err := worker.Git(t.Context(), repo, args...); err != nil {
			t.Fatal(err)
		}
	}
	head, err := worker.Git(t.Context(), repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	sha := strings.TrimSpace(string(head))
	t.Setenv("RECOVERY_HEAD", sha)
	script := `#!/bin/sh
printf '{"number":7,"state":"open","head":{"sha":"%s","ref":"feedback/test","repo":{"full_name":"owner/repo"}}}\n' "$RECOVERY_HEAD"
`
	if err := os.WriteFile(filepath.Join(root, "gh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root+":"+os.Getenv("PATH"))
	db, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cfg := worker.Config{Repo: repo, GitHubRepo: "owner/repo"}
	row, err := db.AddReport(t.Context(), model.Report{Body: "repair", IssueNumber: 3, PRNumber: 7, Branch: "feedback/test", CommitSHA: sha, Status: model.PROpen})
	if err != nil {
		t.Fatal(err)
	}
	row, err = db.Transition(t.Context(), row.ID, model.PROpen, model.ReviseRequested, map[string]any{"pending_review": "preserve this maintainer request"})
	if err != nil {
		t.Fatal(err)
	}
	row, err = db.StartReview(t.Context(), row.ID, row.Status, nil)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		row, err = db.Claim(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Finish(t.Context(), row.ID, model.Interrupted, "setup interrupted"); err != nil {
			t.Fatal(err)
		}
		row, err = db.Get(t.Context(), row.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := retryInterrupted(t.Context(), cfg, db, row, 2); err != nil {
			t.Fatal(err)
		}
		row, err = db.Get(t.Context(), row.ID)
		if err != nil {
			t.Fatal(err)
		}
		want := model.New
		if attempt == 2 {
			want = model.NeedsHuman
		}
		if row.Status != want || row.Attempt != attempt || row.ReviewRounds != 1 || row.PRNumber != 7 || row.Branch != "feedback/test" || row.PendingReview != "preserve this maintainer request" {
			t.Fatalf("recovery lost protocol state: %+v", row)
		}
	}
}
