package worker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/store"
)

func localInstance(t *testing.T, agent, verify string) (string, Config, *store.Store) {
	t.Helper()
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"-c", "user.name=test", "-c", "user.email=test@localhost", "commit", "--allow-empty", "-qm", "base"}} {
		if _, err := git(t.Context(), repo, args...); err != nil {
			t.Fatal(err)
		}
	}
	cfg := Config{Repo: repo, Base: "HEAD", Agent: []string{"bash", "-c", agent}, Verify: verify, TimeoutSeconds: 10}
	home := filepath.Join(root, "home")
	if err := Init(home, cfg); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(filepath.Join(home, "go-store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return home, cfg, db
}

func TestLocalPipelineOutcomesAndEvidence(t *testing.T) {
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("bwrap unavailable")
	}
	for _, tc := range []struct {
		name, agent, gate string
		status            model.Status
		wantErr           bool
	}{
		{"verified", "printf fixed > change.txt", "test \"$(cat change.txt)\" = fixed", model.Verified, false},
		{"no-change", "true", "exit 9", model.NoChange, false},
		{"agent-error", "printf partial > change.txt; exit 3", "true", model.AgentFailed, true},
		{"gate-error", "printf fixed > change.txt", "exit 7", model.VerifyFailed, true},
		{"gate-mutation", "printf fixed > change.txt", "printf changed > change.txt", model.VerifyFailed, true},
		{"git-link-tamper", "printf 'gitdir: /tmp/not-owned\\n' > .git", "true", model.Failed, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home, cfg, db := localInstance(t, tc.agent, tc.gate)
			queued, err := db.Add(t.Context(), "repair the reported bug")
			if err != nil {
				t.Fatal(err)
			}
			got, err := Work(t.Context(), home, cfg, db)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v, wanted error=%v", err, tc.wantErr)
			}
			if got.Status != tc.status {
				t.Fatalf("status=%s; want %s (%v)", got.Status, tc.status, err)
			}
			saved, err := db.Get(t.Context(), queued.ID)
			if err != nil {
				t.Fatal(err)
			}
			if saved.Status != tc.status || saved.Attempt != 1 || saved.Worktree != got.Worktree {
				t.Fatalf("saved %+v", saved)
			}
			data, err := os.ReadFile(filepath.Join(attemptDir(home, saved), "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			var manifest Manifest
			if err := json.Unmarshal(data, &manifest); err != nil {
				t.Fatal(err)
			}
			if manifest.Status != tc.status || manifest.Base == "" || manifest.FinishedAt == "" {
				t.Fatalf("manifest %+v", manifest)
			}
			if tc.status == model.Verified {
				patch, err := os.ReadFile(filepath.Join(attemptDir(home, saved), "patch.diff"))
				if err != nil {
					t.Fatal(err)
				}
				if string(patch) == "" {
					t.Fatal("verified change lost its patch")
				}
				if err := db.Retry(t.Context(), saved.ID); err != nil {
					t.Fatal(err)
				}
				again, err := Work(t.Context(), home, cfg, db)
				if err != nil {
					t.Fatal(err)
				}
				if again.Attempt != 2 || again.Worktree == saved.Worktree {
					t.Fatalf("retry reused prior evidence: %+v", again)
				}
				old, err := os.ReadFile(filepath.Join(attemptDir(home, saved), "manifest.json"))
				if err != nil {
					t.Fatal(err)
				}
				if string(old) != string(data) {
					t.Fatal("retry overwrote completed attempt")
				}
			}
		})
	}
}

func TestRecoveryPreservesAttemptAndRequiresExplicitRetry(t *testing.T) {
	home, cfg, db := localInstance(t, "exit 99", "true")
	if _, err := db.Add(t.Context(), "interrupted job"); err != nil {
		t.Fatal(err)
	}
	claimed, err := db.Claim(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_, err = Work(t.Context(), home, cfg, db)
	if !errors.Is(err, model.ErrEmptyQueue) {
		t.Fatalf("expected no automatic rerun, got %v", err)
	}
	report, err := db.Get(t.Context(), claimed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != model.Interrupted || report.Attempt != 1 {
		t.Fatalf("recovery %+v", report)
	}
	data, err := os.ReadFile(filepath.Join(attemptDir(home, report), "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Status != model.Interrupted {
		t.Fatalf("manifest %+v", manifest)
	}
}

func TestCanceledWorkDoesNotClaimReport(t *testing.T) {
	home, cfg, db := localInstance(t, "exit 99", "true")
	queued, err := db.Add(t.Context(), "still queued")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Work(ctx, home, cfg, db); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	report, err := db.Get(t.Context(), queued.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != model.New || report.Attempt != 0 {
		t.Fatalf("canceled worker claimed %+v", report)
	}
}

func TestManifestHasCheckoutBeforeSetupFinishes(t *testing.T) {
	home, cfg, db := localInstance(t, "true", "true")
	ready := filepath.Join(home, "setup-ready")
	t.Setenv("FBGO_SETUP_READY", ready)
	cfg.Setup = "touch \"$FBGO_SETUP_READY\"; sleep 60"
	queued, err := db.Add(t.Context(), "check crash evidence")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := Work(ctx, home, cfg, db); done <- err }()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("setup ended before evidence check: %v", err)
		case <-deadline:
			cancel()
			<-done
			t.Fatal("setup did not start")
		case <-tick.C:
		}
	}
	queued.Attempt = 1
	data, readErr := os.ReadFile(filepath.Join(attemptDir(home, queued), "manifest.json"))
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Status != model.Working || manifest.Base == "" || manifest.Worktree == "" {
		t.Fatalf("in-flight attempt lost checkout identity: %+v", manifest)
	}
}

func TestInitRejectsSymlinkedHomeBeforeWriting(t *testing.T) {
	home, cfg, _ := localInstance(t, "true", "true")
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(filepath.Dir(home), alias); err != nil {
		t.Fatal(err)
	}
	if err := Init(filepath.Join(alias, "other"), cfg); err == nil {
		t.Fatal("accepted a noncanonical home")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(home), "other")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected initialization wrote instance data: %v", err)
	}
}
