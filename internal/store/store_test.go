// Tests intentionally discard returned reports when testing error identity or
// reading the persisted state separately; all database and close errors are checked.
package store

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/FaintGhost/loom/internal/model"
)

func openStore(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestConcurrentOpenAndOldestClaims(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.sqlite")
	var stores [2]*Store
	var openErrors [2]error
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range stores {
		wg.Go(func() {
			<-start
			stores[i], openErrors[i] = Open(path)
		})
	}
	close(start)
	wg.Wait()
	for i, s := range stores {
		if openErrors[i] != nil {
			t.Errorf("open %d: %v", i, openErrors[i])
		}
		if s != nil {
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
		}
	}
	if t.Failed() {
		t.FailNow()
	}
	const count = 32
	for i := range count {
		if _, err := stores[0].Add(t.Context(), fmt.Sprintf("report %d", i)); err != nil {
			t.Fatal(err)
		}
	}
	first, err := stores[1].Claim(t.Context())
	if err != nil || first.ID != 1 || first.Attempt != 1 || first.Status != model.Working {
		t.Fatalf("oldest claim = %+v, %v", first, err)
	}
	claims := make(chan model.Report, count)
	claimErrors := make(chan error, len(stores))
	for _, s := range stores {
		wg.Go(func() {
			for {
				r, err := s.Claim(t.Context())
				if errors.Is(err, model.ErrEmptyQueue) {
					return
				}
				if err != nil {
					claimErrors <- err
					return
				}
				claims <- r
			}
		})
	}
	wg.Wait()
	close(claims)
	close(claimErrors)
	for err := range claimErrors {
		t.Error(err)
	}
	seen := map[int64]bool{first.ID: true}
	for r := range claims {
		if seen[r.ID] || r.Attempt != 1 || r.Status != model.Working {
			t.Errorf("duplicate or invalid claim: %+v", r)
		}
		seen[r.ID] = true
	}
	if len(seen) != count {
		t.Fatalf("claimed %d unique reports, want %d", len(seen), count)
	}
	persisted, err := stores[0].List(t.Context())
	if err != nil || len(persisted) != count {
		t.Fatalf("persisted reports = %d, %v", len(persisted), err)
	}
	for _, r := range persisted {
		if r.Status != model.Working || r.Attempt != 1 {
			t.Errorf("unpersisted claim: %+v", r)
		}
	}
}

func TestRetryEligibilityRecoveryAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.sqlite")
	s := openStore(t, path)
	ctx := t.Context()
	if _, err := s.Add(ctx, " \t\n\u2003"); !errors.Is(err, model.ErrInvalidReport) {
		t.Fatalf("blank body: %v", err)
	}
	if _, err := s.Get(ctx, 999); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("missing report: %v", err)
	}
	if err := s.Retry(ctx, 999); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("retry missing: %v", err)
	}
	r, err := s.Add(ctx, "preserve the evidence")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Retry(ctx, r.ID); !errors.Is(err, model.ErrTransition) {
		t.Fatalf("retry queued report: %v", err)
	}
	if err := s.SetWorktree(ctx, r.ID, "too soon"); !errors.Is(err, model.ErrTransition) {
		t.Fatalf("queued worktree: %v", err)
	}
	if err := s.Finish(ctx, r.ID, model.Verified, "too soon"); !errors.Is(err, model.ErrTransition) {
		t.Fatalf("finish queued report: %v", err)
	}
	r, err = s.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Retry(ctx, r.ID); !errors.Is(err, model.ErrTransition) {
		t.Fatalf("retry running report: %v", err)
	}
	if err := s.Finish(ctx, r.ID, model.New, "invalid"); !errors.Is(err, model.ErrTransition) {
		t.Fatalf("finish nonterminal: %v", err)
	}
	worktree := filepath.Join(t.TempDir(), "retained-worktree")
	if err := s.SetWorktree(ctx, r.ID, worktree); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, path)
	recovered, err := s.Recover(ctx)
	if err != nil || len(recovered) != 1 {
		t.Fatalf("recover = %+v, %v", recovered, err)
	}
	interrupted := recovered[0]
	if interrupted.Status != model.Interrupted || interrupted.Attempt != 1 || interrupted.Worktree != worktree || interrupted.Error == "" || interrupted.Body != r.Body || interrupted.CreatedAt != r.CreatedAt {
		t.Fatalf("recovery lost evidence: %+v", interrupted)
	}
	if again, err := s.Recover(ctx); err != nil || len(again) != 0 {
		t.Fatalf("second recovery = %+v, %v", again, err)
	}
	if _, err := s.Claim(ctx); !errors.Is(err, model.ErrEmptyQueue) {
		t.Fatalf("recovery requeued work: %v", err)
	}
	if err := s.Retry(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	retried, err := s.Get(ctx, r.ID)
	if err != nil || retried.Status != model.New || retried.Attempt != 1 || retried.Worktree != worktree {
		t.Fatalf("retry lost evidence: %+v, %v", retried, err)
	}
	terminals := []model.Status{model.Verified, model.NoChange, model.SetupFailed, model.AgentFailed, model.VerifyFailed, model.Interrupted, model.Failed}
	for i, status := range terminals {
		claimed, err := s.Claim(ctx)
		if err != nil || claimed.Attempt != i+2 || claimed.ID != r.ID || claimed.Worktree != "" {
			t.Fatalf("attempt claim = %+v, %v", claimed, err)
		}
		if err := s.Finish(ctx, r.ID, status, "terminal detail"); err != nil {
			t.Fatalf("finish %s: %v", status, err)
		}
		if err := s.Finish(ctx, r.ID, status, "duplicate finish"); !errors.Is(err, model.ErrTransition) {
			t.Fatalf("finish terminal %s: %v", status, err)
		}
		if err := s.Retry(ctx, r.ID); err != nil {
			t.Fatalf("retry %s: %v", status, err)
		}
	}
	var from, to, detail string
	if err := s.db.QueryRowContext(ctx, "SELECT from_status, to_status, detail FROM events WHERE report_id = ? AND to_status = ? ORDER BY id LIMIT 1", r.ID, model.Interrupted).Scan(&from, &to, &detail); err != nil {
		t.Fatal(err)
	}
	if from != string(model.Working) || to != string(model.Interrupted) || detail != interrupted.Error {
		t.Fatalf("recovery event lost detail: %q -> %q: %q", from, to, detail)
	}
}

func TestForeignDatabaseIsUnchanged(t *testing.T) {
	for _, schema := range []string{
		"CREATE TABLE reports (id INTEGER PRIMARY KEY, body TEXT); INSERT INTO reports VALUES (1, 'Python evidence')",
		"PRAGMA application_id = 12345; PRAGMA user_version = 1",
		"PRAGMA user_version = 99",
		"VACUUM",
		"PRAGMA application_id = 1178748751; PRAGMA user_version = 99",
		"PRAGMA application_id = 1178748751; PRAGMA user_version = 1; CREATE TABLE reports (id INTEGER PRIMARY KEY)",
	} {
		t.Run(schema, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "foreign.sqlite")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			// Fixture statement result carries no useful data; both execution and close errors matter.
			_, execErr := db.ExecContext(t.Context(), schema)
			if err := errors.Join(execErr, db.Close()); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			s, err := Open(path)
			if s != nil {
				if closeErr := s.Close(); closeErr != nil {
					t.Error(closeErr)
				}
			}
			if err == nil {
				t.Fatal("foreign database accepted")
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("foreign database changed")
			}
		})
	}
}

func TestEventFailureRollsBackState(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "queue.sqlite"))
	ctx := t.Context()
	r, err := s.Add(ctx, "atomic history")
	if err != nil {
		t.Fatal(err)
	}
	// Trigger DDL has no useful result; it deliberately injects an actual SQLite failure.
	if _, err := s.db.ExecContext(ctx, "CREATE TRIGGER fail_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT, 'event unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx); err == nil {
		t.Fatal("claim ignored event failure")
	}
	current, err := s.Get(ctx, r.ID)
	if err != nil || current.Status != model.New || current.Attempt != 0 {
		t.Fatalf("failed event committed claim: %+v, %v", current, err)
	}
	// Dropping the failure trigger returns no data.
	if _, err := s.db.ExecContext(ctx, "DROP TRIGGER fail_event"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx); err != nil {
		t.Fatal(err)
	}
	// Reinstall failure injection to cover both single-row and recovery transitions.
	if _, err := s.db.ExecContext(ctx, "CREATE TRIGGER fail_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT, 'event unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx, r.ID, model.Failed, "failed attempt"); err == nil {
		t.Fatal("finish ignored event failure")
	}
	if _, err := s.Recover(ctx); err == nil {
		t.Fatal("recovery ignored event failure")
	}
	current, err = s.Get(ctx, r.ID)
	if err != nil || current.Status != model.Working || current.Error != "" || current.Attempt != 1 {
		t.Fatalf("failed event committed terminal state: %+v, %v", current, err)
	}
	// DDL results carry no data; restore a terminal report to test retry atomicity.
	if _, err := s.db.ExecContext(ctx, "DROP TRIGGER fail_event"); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx, r.ID, model.Failed, "retained failure"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, "CREATE TRIGGER fail_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT, 'event unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	if err := s.Retry(ctx, r.ID); err == nil {
		t.Fatal("retry ignored event failure")
	}
	current, err = s.Get(ctx, r.ID)
	if err != nil || current.Status != model.Failed || current.Error != "retained failure" || current.Attempt != 1 {
		t.Fatalf("failed event committed retry: %+v, %v", current, err)
	}
}

func TestForeignWALDatabaseIsUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "foreign.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	// Keep the writer connection alive so committed foreign evidence remains in WAL.
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(t.Context(), "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE reports (id INTEGER PRIMARY KEY, body TEXT); INSERT INTO reports VALUES (1, 'foreign WAL evidence')"); err != nil {
		t.Fatal(err)
	}
	beforeDB, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	beforeWAL, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if s != nil {
		if closeErr := s.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}
	if err == nil {
		t.Fatal("foreign WAL database accepted")
	}
	afterDB, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	afterWAL, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeDB, afterDB) || !bytes.Equal(beforeWAL, afterWAL) {
		t.Fatal("foreign database or WAL changed")
	}
}
