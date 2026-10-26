package improvement

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/FaintGhost/loom/internal/runner"
	"github.com/FaintGhost/loom/internal/worker"
)

func TestCompatibilitySandboxHidesCredentialsAndProtectsSources(t *testing.T) {
	bwrap, err := exec.LookPath("bwrap")
	if err != nil {
		t.Skip("bubblewrap unavailable")
	}
	root := t.TempDir()
	home, scratch, module, cache := filepath.Join(root, "home"), filepath.Join(root, "scratch"), filepath.Join(root, "module"), filepath.Join(root, "cache")
	for _, path := range []string{home, scratch, module, cache} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	secret, private, source := filepath.Join(module, ".credential"), filepath.Join(home, "config.json"), filepath.Join(module, "source.go")
	for _, path := range []string{secret, private, source} {
		if err := os.WriteFile(path, []byte("original-private-content"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("FBGO_TEST_PRIVATE_ENV", "private-environment-value")
	t.Setenv("GOROOT", runtime.GOROOT())
	t.Setenv("GOMODCACHE", cache)
	shim := filepath.Join(root, "shims")
	if err := os.Mkdir(shim, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(shim, "go"), []byte("#!/bin/sh\necho 'mise refuses config trust' >&2\nexit 73\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))
	cfg := worker.Config{MagpieKeyFile: secret}
	argv, err := compatibilitySandbox(bwrap, home, scratch, module, "/bin/sh", cfg)
	if err != nil {
		t.Fatal(err)
	}
	script := `
[ -z "${FBGO_TEST_PRIVATE_ENV+x}" ] || exit 10
[ -z "${GOROOT+x}" ] || exit 11
[ ! -s "$1" ] || exit 12
[ ! -e "$2" ] || exit 13
if (printf changed > "$3") 2>/dev/null; then exit 14; fi
if (printf changed > "$GOMODCACHE/changed") 2>/dev/null; then exit 15; fi
[ "$GOPROXY" = off ] || exit 16
[ "$GOTOOLCHAIN" = local ] || exit 17
go version > "$TMPDIR/go-version" || exit 18
printf writable > "$TMPDIR/allowed"
`
	argv = append(argv, "/bin/sh", "-c", script, "compatibility-check", secret, private, source)
	result, err := runner.Run(t.Context(), argv, module, filepath.Join(root, "sandbox.log"), 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if result.ExitCode != 0 {
		data, _ := os.ReadFile(filepath.Join(root, "sandbox.log"))
		t.Fatalf("sandbox failed (%d): %s", result.ExitCode, data)
	}
	data, err := os.ReadFile(source)
	if err != nil || string(data) != "original-private-content" {
		t.Fatal("sandbox changed module source")
	}
	if _, err := os.Stat(filepath.Join(scratch, "allowed")); err != nil {
		t.Fatal("scratch was not writable:", err)
	}
	version, err := os.ReadFile(filepath.Join(scratch, "go-version"))
	if err != nil || !strings.HasPrefix(string(version), "go version ") {
		t.Fatalf("concrete Go toolchain was not used: %s %v", version, err)
	}
	cfg.Hidden = []string{module}
	if _, err := compatibilitySandbox(bwrap, home, scratch, module, "/bin/sh", cfg); err == nil {
		t.Fatal("hidden mount covering the module was accepted")
	}
	cfg.Hidden = []string{runtime.GOROOT()}
	if _, err := compatibilitySandbox(bwrap, home, scratch, module, "/bin/sh", cfg); err == nil {
		t.Fatal("hidden mount covering the Go toolchain was accepted")
	}
}

func TestCompatibilityEvidenceRedactsConfiguredCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	fileSecret, envSecret := "configured-file-token", "configured-env-token"
	if err := os.WriteFile(path, []byte(fileSecret+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FBGO_TEST_COMPAT_TOKEN", envSecret)
	redact := compatibilityRedactor(worker.Config{MagpieKeyFile: path, DiscordTokenEnv: "FBGO_TEST_COMPAT_TOKEN"})
	argv := []string{"tool", "--key=" + fileSecret, "--token=" + envSecret}
	if credentialCommand(argv, redact) == nil {
		t.Fatal("credential-bearing command was executable")
	}
	for _, value := range redactedArgs(argv, redact) {
		if strings.Contains(value, fileSecret) || strings.Contains(value, envSecret) {
			t.Fatal("persisted argv leaked a credential")
		}
	}
	if got := redact("command failed with " + envSecret); strings.Contains(got, envSecret) {
		t.Fatal("persisted error leaked a credential")
	}
	if got := redact("command exited 3"); got != "command exited 3" {
		t.Fatal("ordinary failure evidence was lost")
	}
}
