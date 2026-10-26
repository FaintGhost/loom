package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
)

func listInstances(args []string, output io.Writer) error {
	f := flag.NewFlagSet("instances", flag.ContinueOnError)
	f.SetOutput(output)
	root := f.String("root", "/root/.hermes/fb-go", "directory containing Go instance homes")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("instances accepts only --root")
	}
	paths, err := filepath.Glob(filepath.Join(*root, "*", "config.json"))
	if err != nil {
		return err
	}
	type instance struct {
		Home    string `json:"home"`
		Repo    string `json:"repo"`
		Channel string `json:"channel"`
		Publish bool   `json:"publish"`
	}
	instances := make([]instance, 0, len(paths))
	for _, path := range paths {
		home := filepath.Dir(path)
		cfg, err := worker.Load(home)
		if err != nil {
			return fmt.Errorf("instance %s: %w", home, err)
		}
		instances = append(instances, instance{home, cfg.GitHubRepo, cfg.DiscordChannel, cfg.Publish})
	}
	return json.NewEncoder(output).Encode(instances)
}

func lockFile(path string, fn func() error) (retErr error) {
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, lock.Close()) }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("instance operation is busy: %w", err)
	}
	return fn()
}

func collectWorktrees(ctx context.Context, home string, cfg worker.Config, db *store.Store, args []string, output io.Writer) error {
	f := flag.NewFlagSet("gc", flag.ContinueOnError)
	f.SetOutput(output)
	dryRun := f.Bool("dry-run", false, "list owned expired checkouts without removing them")
	keepDays := f.Int("keep-days", 14, "retain unsuccessful attempt checkouts for this many days")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *keepDays < 0 {
		return errors.New("usage: gc [--dry-run] [--keep-days NONNEGATIVE_DAYS]")
	}
	return lockFile(filepath.Join(home, "pipeline.lock"), func() error {
		return lockFile(filepath.Join(home, "worker.lock"), func() error {
			return lockFile(filepath.Join(home, "herdr.lock"), func() error {
				reports, err := db.List(ctx)
				if err != nil {
					return err
				}
				cutoff := time.Now().AddDate(0, 0, -*keepDays)
				paths := make([]string, 0)
				for _, report := range reports {
					if report.Worktree == "" {
						continue
					}
					eligible := report.Status == model.Status("done") || report.Status == model.Status("closed")
					if !eligible {
						switch report.Status {
						case model.NoChange, model.SetupFailed, model.AgentFailed, model.VerifyFailed, model.Interrupted, model.Failed, model.Status("guard_blocked"), model.Status("needs_reporter"), model.Status("needs_human"):
							updated, err := time.Parse(time.RFC3339Nano, report.UpdatedAt)
							eligible = err == nil && updated.Before(cutoff)
						case model.AwaitingApproval, model.Claimed, model.Closed, model.Conversation, model.Done, model.Held, model.Merged, model.NeedsInfo, model.New, model.PROpen, model.Queued, model.ReviewPending, model.ReviseRequested, model.Revising, model.Verified, model.Working:
							// These states do not expire through failed-attempt retention.
						}
					}
					if !eligible {
						continue
					}
					if err := worker.Intact(cfg, home, report.Worktree); err != nil {
						return fmt.Errorf("refusing cleanup of report %d: %w", report.ID, err)
					}
					paths = append(paths, report.Worktree)
					if *dryRun {
						continue
					}
					if err := worker.RemoveOwned(ctx, cfg, home, report.Worktree); err != nil {
						return err
					}
					if _, err := db.Patch(ctx, report.ID, map[string]any{"worktree": ""}); err != nil {
						return err
					}
					if err := db.RecordEvent(ctx, report.ID, "gc", "removed owned checkout; attempt evidence retained"); err != nil {
						return err
					}
				}
				return json.NewEncoder(output).Encode(paths)
			})
		})
	})
}
