package improvement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/FaintGhost/loom/internal/runner"
	"github.com/FaintGhost/loom/internal/worker"
)

const EffectgoBaseline = "3e5543d266b19747ab64f1e03ccfb5fd6f4c7160"

type compatibilityCommand struct {
	Argv        []string      `json:"argv"`
	SandboxArgv []string      `json:"sandbox_argv,omitempty"`
	Directory   string        `json:"directory"`
	Log         string        `json:"log"`
	Result      runner.Result `json:"result"`
	Error       string        `json:"error,omitempty"`
}

type compatibilityRecord struct {
	StartedAt  string                 `json:"started_at"`
	FinishedAt string                 `json:"finished_at"`
	Baseline   string                 `json:"baseline"`
	Revision   string                 `json:"configured_revision"`
	Binary     string                 `json:"binary"`
	SHA256     string                 `json:"sha256"`
	Module     string                 `json:"module"`
	Commands   []compatibilityCommand `json:"commands"`
	Error      string                 `json:"error,omitempty"`
}

// CheckCompatibility retains every command, tool identity, log and failed result.
// SHA256 pins the actual executable; Revision records the operator's provenance.
// Source, deployed binaries and shared caches are read-only inside the sandbox.
func CheckCompatibility(ctx context.Context, home string, cfg worker.Config) (folder string, retErr error) {
	root := filepath.Join(home, "compatibility")
	if err := os.MkdirAll(root, 0700); err != nil {
		return "", err
	}
	folder, err := os.MkdirTemp(root, time.Now().UTC().Format("20060102T150405Z")+"-")
	if err != nil {
		return "", err
	}
	redact := compatibilityRedactor(cfg)
	record := compatibilityRecord{StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Baseline: EffectgoBaseline,
		Revision: cfg.Retro.EffectgoRevision, Binary: cfg.Retro.EffectgoBinary, Module: cfg.Retro.CompatibilityModule}
	defer func() {
		record.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if retErr != nil {
			record.Error = redact(retErr.Error())
			if record.Error != retErr.Error() {
				retErr = errors.New(record.Error)
			}
		}
		retErr = errors.Join(retErr, saveJSON(filepath.Join(folder, "result.json"), record))
	}()
	if !filepath.IsAbs(record.Binary) || !filepath.IsAbs(record.Module) || len(cfg.Retro.EffectgoSHA256) != 64 || record.Revision == "" {
		return folder, errors.New("compatibility requires absolute module/binary paths, pinned effectgo_sha256 and effectgo_revision")
	}
	binary, err := os.Open(record.Binary)
	if err != nil {
		return folder, err
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, binary)
	if err := errors.Join(copyErr, binary.Close()); err != nil {
		return folder, err
	}
	record.SHA256 = hex.EncodeToString(digest.Sum(nil))
	if record.SHA256 != strings.ToLower(cfg.Retro.EffectgoSHA256) {
		return folder, errors.New("effectgo executable differs from pinned sha256")
	}
	if _, err := os.Stat(filepath.Join(record.Module, "go.mod")); err != nil {
		return folder, err
	}
	bwrap, err := exec.LookPath("bwrap")
	if err != nil {
		return folder, fmt.Errorf("read-only compatibility requires bubblewrap: %w", err)
	}
	// The one writable host directory is disposable scratch; evidence logs are opened by runner outside it.
	scratch, err := os.MkdirTemp("", "loom-compat-")
	if err != nil {
		return folder, err
	}
	defer func() { retErr = errors.Join(retErr, os.RemoveAll(scratch)) }()
	sandbox, err := compatibilitySandbox(bwrap, home, scratch, record.Module, record.Binary, cfg)
	if err != nil {
		return folder, err
	}
	commands := [][]string{
		{"go", "version"},
		{"go", "version", "-m", record.Binary},
		{"git", "-C", record.Module, "rev-parse", "HEAD"},
		{"env", "-u", "EFFECTGO_STUBS", "go", "vet", "-vettool=" + record.Binary, "./..."},
		{"env", "EFFECTGO_BIN=" + record.Binary, "EFFECTGO_SHA256=" + record.SHA256, "bash", filepath.Join(record.Module, "check-contracts.sh")},
	}
	commands = append(commands, cfg.Retro.RegressionCommands...)
	// This operator-owned file holds regressions retained from earlier compatibility failures.
	persisted := filepath.Join(home, "compatibility-regressions.json")
	if data, err := os.ReadFile(persisted); err == nil {
		var regressions [][]string
		if err := json.Unmarshal(data, &regressions); err != nil {
			return folder, fmt.Errorf("persisted compatibility regressions: %w", err)
		}
		commands = append(commands, regressions...)
	} else if !errors.Is(err, os.ErrNotExist) {
		return folder, err
	}
	timeout := cfg.Retro.TimeoutSeconds
	if timeout <= 0 {
		timeout = 1200
	}
	for i, command := range commands {
		entry := compatibilityCommand{Argv: redactedArgs(command, redact), Directory: record.Module, Log: filepath.Join(folder, fmt.Sprintf("%02d.log", i+1)), Result: runner.Result{ExitCode: -1}}
		if len(command) == 0 || command[0] == "" {
			entry.Error = "empty compatibility command"
		} else if err := credentialCommand(command, redact); err != nil {
			entry.Error = err.Error()
		} else if ctx.Err() != nil {
			entry.Error = redact(ctx.Err().Error())
		} else {
			argv := append(append([]string(nil), sandbox...), command...)
			entry.SandboxArgv = redactedArgs(argv, redact)
			result, err := runner.Run(ctx, argv, record.Module, entry.Log, time.Duration(timeout)*time.Second)
			entry.Result = result
			if err != nil {
				entry.Error = redact(err.Error())
			} else if result.ExitCode != 0 {
				entry.Error = fmt.Sprintf("command exited %d", result.ExitCode)
			}
		}
		record.Commands = append(record.Commands, entry)
		if entry.Error != "" {
			retErr = errors.Join(retErr, fmt.Errorf("compatibility command %d: %s", i+1, entry.Error))
		}
		if err := saveJSON(filepath.Join(folder, "result.json"), record); err != nil {
			return folder, errors.Join(retErr, err)
		}
	}
	return folder, retErr
}
