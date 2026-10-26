// Package runner executes Linux subprocesses with bounded lifetimes and isolated gates.
package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

type Result struct {
	ExitCode int  `json:"exit_code"`
	TimedOut bool `json:"timed_out"`
	Canceled bool `json:"canceled"`
}

// Run records only child output; argv and inherited environment are never logged.
// Cleanup covers the process group. A deliberately detached descendant can escape
// it; sandboxed gates additionally have a PID namespace and die-with-parent.
func Run(ctx context.Context, argv []string, cwd, logPath string, timeout time.Duration, inherited ...*os.File) (result Result, err error) {
	result.ExitCode = -1
	if ctx.Err() != nil {
		result.TimedOut = errors.Is(ctx.Err(), context.DeadlineExceeded)
		result.Canceled = !result.TimedOut
		return result, ctx.Err()
	}
	if len(argv) == 0 || argv[0] == "" || timeout <= 0 {
		return result, errors.New("process requires a command and positive timeout")
	}
	log, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return result, errors.Join(ctx.Err(), fmt.Errorf("create process log: %w", err))
	}
	defer func() { err = errors.Join(err, log.Sync(), log.Close()) }()

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	args := argv[1:]
	if filepath.Base(argv[0]) == "bwrap" && len(inherited) != 0 {
		// bwrap closes other inherited descriptors; its monitor retains sync-fds.
		args = make([]string, 0, len(argv)-1+2*len(inherited))
		for i := range inherited {
			args = append(args, "--sync-fd", strconv.Itoa(3+i))
		}
		args = append(args, argv[1:]...)
	}
	cmd := exec.CommandContext(runCtx, argv[0], args...)
	cmd.Dir = cwd
	cmd.Stdout, cmd.Stderr = log, log
	cmd.ExtraFiles = inherited
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	killGroup := func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	cmd.Cancel = killGroup
	if err = cmd.Start(); err != nil {
		if runCtx.Err() != nil {
			result.TimedOut = errors.Is(runCtx.Err(), context.DeadlineExceeded)
			result.Canceled = !result.TimedOut
			return result, errors.Join(runCtx.Err(), err)
		}
		return result, fmt.Errorf("start process: %w", err)
	}
	waitErr := cmd.Wait()
	cleanupErr := killGroup()
	if cmd.ProcessState != nil {
		result.ExitCode = cmd.ProcessState.ExitCode()
	}
	if runCtx.Err() != nil {
		result.TimedOut = errors.Is(runCtx.Err(), context.DeadlineExceeded)
		result.Canceled = !result.TimedOut
		return result, errors.Join(runCtx.Err(), cleanupErr)
	}
	if _, exited := errors.AsType[*exec.ExitError](waitErr); exited {
		waitErr = nil
	}
	return result, errors.Join(waitErr, cleanupErr)
}
