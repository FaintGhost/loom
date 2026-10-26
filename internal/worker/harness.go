package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/runner"
)

func harnessCommand(cfg Config, report model.Report, cwd, runDir, prompt string) ([]string, error) {
	name := cfg.Harness
	if report.Harness != "" {
		name = report.Harness
	} else if report.Surface == "gui_or_frontend" && cfg.GUIHarness != "" {
		name = cfg.GUIHarness
	}
	args := cfg.Agent
	modelName := ""
	var imageArgs []string
	if spec, ok := cfg.Commands[name]; ok {
		args = spec.Args
		modelName = spec.Model
		imageArgs = spec.ImageArgs
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("no configured harness %q", name)
	}
	replacer := strings.NewReplacer("{task}", filepath.Join(runDir, "TASK.md"), "{prompt}", prompt, "{last}", filepath.Join(runDir, "last.txt"), "{model}", modelName)
	var argv []string
	images, _ := filepath.Glob(filepath.Join(cwd, ".fb-attachments", "*"))
	// Attachments are also described in TASK.md. Only known image suffixes become model arguments.
	for _, arg := range args {
		if strings.Contains(arg, "{prompt}") && len(imageArgs) > 0 {
			count := 0
			for _, image := range images {
				if slices.Contains([]string{".png", ".jpg", ".jpeg", ".webp", ".gif"}, strings.ToLower(filepath.Ext(image))) {
					for _, a := range imageArgs {
						argv = append(argv, strings.ReplaceAll(a, "{path}", image))
					}
					count++
				}
			}
			if count > 0 {
				argv = append(argv, "--")
			}
		}
		argv = append(argv, replacer.Replace(arg))
	}
	if len(cfg.HarnessEnv) > 0 || cfg.CodexHome != "" {
		env := []string{"env"}
		if cfg.CodexHome != "" {
			env = append(env, "CODEX_HOME="+cfg.CodexHome)
		}
		for k, v := range cfg.HarnessEnv {
			if k == "" || strings.ContainsAny(k, "=\x00") || strings.ContainsRune(v, 0) {
				return nil, errors.New("invalid harness environment")
			}
			env = append(env, k+"="+v)
		}
		argv = append(env, argv...)
	}
	return argv, nil
}

func herdr(ctx context.Context, args ...string) (map[string]json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "herdr", args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("herdr %s: %w", args[0], err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(out, &envelope); err != nil {
		return nil, err
	}
	if e, ok := envelope["error"]; ok && string(e) != "null" {
		return nil, fmt.Errorf("herdr %s failed", args[0])
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(envelope["result"], &result); err != nil {
		return nil, err
	}
	return result, nil
}

func closeHarness(ctx context.Context, label string) error {
	result, err := herdr(ctx, "workspace", "list")
	if err != nil {
		return err
	}
	var workspaces []struct {
		ID    string `json:"workspace_id"`
		Label string `json:"label"`
	}
	if err := json.Unmarshal(result["workspaces"], &workspaces); err != nil {
		return err
	}
	var errs []error
	for _, ws := range workspaces {
		if ws.Label == label {
			_, err := herdr(ctx, "workspace", "close", ws.ID)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Harness preserves named GUI/default selection and visible herdr execution. Once a
// pane is launched, failure never starts a second agent beside it.
func Harness(ctx context.Context, cfg Config, report model.Report, cwd, runDir, prompt string, timeout time.Duration, lock *os.File) (runner.Result, error) {
	argv, err := harnessCommand(cfg, report, cwd, runDir, prompt)
	if err != nil {
		return runner.Result{ExitCode: -1}, err
	}
	if err := os.MkdirAll(runDir, 0700); err != nil {
		return runner.Result{ExitCode: -1}, err
	}
	if cfg.HarnessMode != "herdr" {
		if lock == nil {
			return runner.Run(ctx, argv, cwd, filepath.Join(runDir, "agent.log"), timeout)
		}
		return runner.Run(ctx, argv, cwd, filepath.Join(runDir, "agent.log"), timeout, lock)
	}
	home := filepath.Dir(filepath.Dir(cwd))
	stage := filepath.Base(runDir)
	if stage != "review" && stage != "triage" {
		stage = "run"
	}
	label := harnessLabel(home, report.ID, stage)
	// Close only this run's own workspace. If Herdr is unavailable before launch,
	// its headless command is still the configured harness.
	if err := closeHarness(ctx, label); err != nil {
		if lock == nil {
			return runner.Run(ctx, argv, cwd, filepath.Join(runDir, "agent.log"), timeout)
		}
		return runner.Run(ctx, argv, cwd, filepath.Join(runDir, "agent.log"), timeout, lock)
	}
	token := make([]byte, 24)
	if _, err := rand.Read(token); err != nil {
		return runner.Result{}, err
	}
	lease := filepath.Join(home, "harness-lease")
	if err := os.WriteFile(lease, []byte(hex.EncodeToString(token)), 0600); err != nil {
		return runner.Result{}, err
	}
	result, err := herdr(ctx, "workspace", "create", "--label", label, "--cwd", cwd, "--no-focus")
	if err != nil {
		return runner.Result{}, err
	}
	var pane struct {
		ID string `json:"pane_id"`
	}
	if err := json.Unmarshal(result["root_pane"], &pane); err != nil || pane.ID == "" {
		return runner.Result{}, errors.New("herdr returned no pane")
	}
	keepPane := false
	defer func() {
		if !keepPane {
			_ = closeHarness(context.WithoutCancel(ctx), label)
		}
	}()
	if err := writeJSON(filepath.Join(runDir, "pane.json"), map[string]string{"pane": pane.ID, "label": label}, false); err != nil {
		return runner.Result{}, err
	}
	words := make([]string, len(argv))
	for i, a := range argv {
		words[i] = quote(a)
	}
	// The independent lock survives worker death. The lease closes the launch race:
	// recovery invalidates it before probing the lock. GNU timeout bounds orphan work.
	script := "exec 9>" + quote(filepath.Join(home, "herdr.lock")) + "\nflock -n 9 || exit 125\n" +
		"test \"$(cat " + quote(lease) + ")\" = " + quote(hex.EncodeToString(token)) + " || exit 125\n" +
		"cd " + quote(cwd) + " || exit 125\ntimeout --signal=TERM --kill-after=5 " + strconv.FormatInt(int64(timeout.Seconds()), 10) + " " + strings.Join(words, " ") + "\nfb_rc=$?\nprintf '\\nFB-DONE exit=%s\\n' \"$fb_rc\"\n"
	scriptPath := filepath.Join(runDir, "pane.sh")
	if err := os.WriteFile(scriptPath, []byte(script), 0600); err != nil {
		return runner.Result{}, err
	}
	if _, err := herdr(ctx, "pane", "run", pane.ID, "bash "+quote(scriptPath)); err != nil {
		return runner.Result{}, err
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout+10*time.Second)
	defer cancel()
	var text string
	defer func() { _ = os.WriteFile(filepath.Join(runDir, "agent.log"), []byte(text), 0600) }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			return runner.Result{ExitCode: -1, TimedOut: errors.Is(runCtx.Err(), context.DeadlineExceeded), Canceled: errors.Is(runCtx.Err(), context.Canceled)}, runCtx.Err()
		case <-ticker.C:
			read, err := herdr(runCtx, "pane", "read", pane.ID, "--source", "recent-unwrapped", "--lines", "400")
			if err != nil {
				continue
			}
			for _, key := range []string{"text", "raw", "output", "content"} {
				var s string
				if json.Unmarshal(read[key], &s) == nil && s != "" {
					text = s
					break
				}
			}
			if at := strings.LastIndex(text, "FB-DONE exit="); at >= 0 {
				fields := strings.Fields(text[at+len("FB-DONE exit="):])
				if len(fields) == 0 {
					continue
				}
				code, err := strconv.Atoi(fields[0])
				if err != nil {
					continue
				}
				keepPane = stage == "run" && code != 124 && code != 137
				return runner.Result{ExitCode: code, TimedOut: code == 124 || code == 137}, nil
			}
		}
	}
}

func invalidateHarness(home string) error {
	if err := os.Remove(filepath.Join(home, "harness-lease")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(home, "herdr.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return ErrBusy
		}
		return err
	}
	return nil
}

func harnessLabel(home string, id int64, stage string) string {
	return "loom-" + filepath.Base(home) + "-" + strconv.FormatInt(id, 10) + "-" + stage
}

// Cleanup removes only checkouts belonging to this home and configured clone.
// Attempt files remain durable evidence; imported legacy worktrees are never used.
func Cleanup(ctx context.Context, cfg Config, home string, report model.Report) error {
	var errs []error
	if cfg.HarnessMode == "herdr" {
		for _, stage := range []string{"run", "review", "triage"} {
			if err := closeHarness(ctx, harnessLabel(home, report.ID, stage)); err != nil {
				errs = append(errs, err)
			}
		}
	}
	root := filepath.Join(home, "workspaces")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return errors.Join(errs...)
	}
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	prefix := strconv.FormatInt(report.ID, 10) + "-"
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			if err := RemoveOwned(ctx, cfg, home, filepath.Join(root, entry.Name())); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}
