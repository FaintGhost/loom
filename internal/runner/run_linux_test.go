package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRunTerminatesGrandchildren(t *testing.T) {
	for _, mode := range []string{"normal", "timeout", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			pidsFile := filepath.Join(dir, "pids")
			t.Cleanup(func() {
				data, _ := os.ReadFile(pidsFile)
				for _, text := range strings.Fields(string(data)) {
					if pid, err := strconv.Atoi(text); err == nil && pid > 1 {
						_ = syscall.Kill(pid, syscall.SIGKILL)
					}
				}
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "cancel" {
				watchDone := make(chan struct{})
				go func() {
					defer close(watchDone)
					tick := time.NewTicker(5 * time.Millisecond)
					defer tick.Stop()
					for {
						select {
						case <-ctx.Done():
							return
						case <-tick.C:
							if data, _ := os.ReadFile(pidsFile); len(strings.Fields(string(data))) == 2 {
								cancel()
								return
							}
						}
					}
				}()
				defer func() { cancel(); <-watchDone }()
			}
			script := `bash -c 'sleep 60 & echo "$$ $!" > "$1"; wait' bash "$1" &
while [ ! -s "$1" ]; do sleep 0.01; done
if [ "$2" = normal ]; then exit 7; fi
wait`
			timeout := 5 * time.Second
			if mode == "timeout" {
				timeout = 500 * time.Millisecond
			}
			result, err := Run(ctx, []string{"bash", "-c", script, "bash", pidsFile, mode}, dir, filepath.Join(dir, "run.log"), timeout)
			switch mode {
			case "normal":
				if err != nil || result.ExitCode != 7 || result.Canceled || result.TimedOut {
					t.Fatalf("normal exit: %+v, %v", result, err)
				}
			case "timeout":
				if !errors.Is(err, context.DeadlineExceeded) || !result.TimedOut || result.Canceled {
					t.Fatalf("timeout: %+v, %v", result, err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) || !result.Canceled || result.TimedOut {
					t.Fatalf("cancel: %+v, %v", result, err)
				}
			}
			data, err := os.ReadFile(pidsFile)
			if err != nil {
				t.Fatal(err)
			}
			pids := strings.Fields(string(data))
			if len(pids) != 2 {
				t.Fatalf("expected child and grandchild PIDs, got %q", data)
			}
			for _, pid := range pids {
				if _, err := strconv.Atoi(pid); err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(3 * time.Second)
				for {
					stat, err := os.ReadFile("/proc/" + pid + "/stat")
					if errors.Is(err, os.ErrNotExist) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					_, rest, _ := strings.Cut(string(stat), ") ")
					if strings.HasPrefix(rest, "Z ") || strings.HasPrefix(rest, "X ") {
						break // Orphan reaping belongs to PID 1, not this process.
					}
					if time.Now().After(deadline) {
						t.Fatalf("descendant %s still running", pid)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		})
	}
}

func TestRunCanceledBeforeLaunch(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := Run(ctx, []string{"sh", "-c", "touch launched"}, dir, filepath.Join(dir, "run.log"), time.Second)
	if !errors.Is(err, context.Canceled) || !result.Canceled || result.ExitCode != -1 {
		t.Fatalf("canceled start: %+v, %v", result, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "launched")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled command ran: %v", err)
	}
}

func TestRunEvidenceAndInheritedFile(t *testing.T) {
	dir := t.TempDir()
	inherited, err := os.Create(filepath.Join(dir, "inherited"))
	if err != nil {
		t.Fatal(err)
	}
	defer inherited.Close()
	logPath := filepath.Join(dir, "run.log")
	result, err := Run(t.Context(), []string{"sh", "-c", "printf output; printf error >&2; printf retained >&3"}, dir, logPath, time.Second, inherited)
	if err != nil || result.ExitCode != 0 {
		t.Fatalf("run: %+v, %v", result, err)
	}
	data, err := os.ReadFile(logPath)
	if err != nil || string(data) != "outputerror" {
		t.Fatalf("direct log: %q, %v", data, err)
	}
	data, err = os.ReadFile(inherited.Name())
	if err != nil || string(data) != "retained" {
		t.Fatalf("inherited descriptor: %q, %v", data, err)
	}
	if _, err := Run(t.Context(), []string{"true"}, dir, logPath, time.Second); !errors.Is(err, os.ErrExist) {
		t.Fatalf("existing evidence overwritten: %v", err)
	}
	if _, err := Run(t.Context(), []string{filepath.Join(dir, "missing")}, dir, filepath.Join(dir, "failed.log"), time.Second); err == nil {
		t.Fatal("launch failure was not returned")
	}
}
