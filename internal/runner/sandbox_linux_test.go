package runner

import (
	"archive/zip"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSandboxRealIsolation(t *testing.T) {
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("bubblewrap binary is absent")
	}
	// Use /var/tmp so the root-readonly and explicit hiding checks cannot pass
	// merely because the sandbox replaces /tmp with an empty filesystem.
	root, err := os.MkdirTemp("/var/tmp", "loom-sandbox-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	home := filepath.Join(root, "home")
	worktree := filepath.Join(home, "workspaces", "repo")
	private := filepath.Join(root, "private")
	for _, path := range []string{worktree, private} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	gitLink := makeSandboxWorktree(t, worktree, filepath.Join(root, "clone"))
	files := map[string]string{
		filepath.Join(worktree, ".git"):    gitLink,
		filepath.Join(worktree, "gate.go"): "package main\nfunc main() {}\n",
		filepath.Join(private, "token"):    "private-directory-value",
		filepath.Join(home, "token"):       "private-home-value",
		filepath.Join(root, "token"):       "private-file-value",
		filepath.Join(root, "public"):      "public-readable-value",
	}
	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	alias := filepath.Join(root, "secret-alias")
	if err := os.Symlink(filepath.Join(root, "token"), alias); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FBGO_SECRET_TOKEN", "must-not-reach-gate")
	netNS, err := os.Readlink("/proc/self/ns/net")
	if err != nil {
		t.Fatal(err)
	}
	pidNS, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	script := `set -eu
[ ! -e "$1/token" ]
[ ! -s "$2" ]
[ ! -e "$4/token" ]
[ ! -e "/proc/1/root$4/token" ]
[ -z "${FBGO_SECRET_TOKEN+x}" ]
[ "$HOME" = /tmp ]
[ "$GOCACHE" = /tmp/go-build ]
[ "$GOPROXY" = off ]
[ "$(readlink /proc/self/ns/net)" != "$5" ]
[ "$(readlink /proc/self/ns/pid)" != "$6" ]
[ "$(cat "$3/public")" = public-readable-value ]
printf writable > inside
if (printf forbidden > .git) 2>/dev/null; then exit 11; fi
if (rm .git) 2>/dev/null; then exit 12; fi
if (printf forbidden > "$3/outside") 2>/dev/null; then exit 13; fi
if (printf forbidden > "$4/outside") 2>/dev/null; then exit 14; fi
if (printf forbidden > "$1/outside") 2>/dev/null; then exit 15; fi
if (printf forbidden > /dev/tcp/127.0.0.1/"$7") 2>/dev/null; then exit 16; fi
go run gate.go
printf sandbox-ok`
	args, err := Sandbox([]string{"bash", "-c", script, "bash", private, alias, root, home, netNS, pidNS, port}, worktree, home, []string{private, alias, filepath.Join(root, "missing")})
	if err != nil {
		t.Fatal(err)
	}
	lock, err := os.Create(filepath.Join(root, "lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	logPath := filepath.Join(root, "gate.log")
	result, runErr := Run(t.Context(), args, worktree, logPath, 45*time.Second, lock)
	output, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if runErr != nil || result.ExitCode != 0 || string(output) != "sandbox-ok" {
		t.Fatalf("sandbox gate: %+v, %v; output=%s", result, runErr, output)
	}
	for path, content := range files {
		actual, err := os.ReadFile(path)
		if err != nil || string(actual) != content {
			t.Fatalf("host file changed: %s: %v", path, err)
		}
	}
	if actual, err := os.ReadFile(filepath.Join(worktree, "inside")); err != nil || string(actual) != "writable" {
		t.Fatalf("worktree write did not persist: %q, %v", actual, err)
	}
	if strings.Contains(string(output), "private-") || strings.Contains(string(output), "must-not-reach-gate") {
		t.Fatal("sandbox leaked a secret")
	}
}

func TestSandboxRejectsUnsafeMounts(t *testing.T) {
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("bubblewrap binary is absent")
	}
	home := t.TempDir()
	worktree := filepath.Join(home, "worktree")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	gitFile := filepath.Join(worktree, ".git")
	if _, err := Sandbox([]string{"true"}, worktree, home, nil); err == nil {
		t.Fatal("accepted missing .git file")
	}
	makeSandboxWorktree(t, worktree, filepath.Join(t.TempDir(), "clone"))
	for _, hidden := range []string{"/", "/proc", worktree, gitFile, "relative"} {
		if _, err := Sandbox([]string{"true"}, worktree, home, []string{hidden}); err == nil {
			t.Fatalf("accepted unsafe hidden mount %q", hidden)
		}
	}
	if err := os.Remove(gitFile); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(home, "token"), gitFile); err != nil {
		t.Fatal(err)
	}
	if _, err := Sandbox([]string{"true"}, worktree, home, nil); err == nil {
		t.Fatal("accepted symlink .git")
	}
}

func TestSandboxUsesSetupModuleCache(t *testing.T) {
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("bubblewrap binary is absent")
	}
	for _, mode := range []string{"default", "gopath-list", "explicit-cache"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			home := filepath.Join(root, "app-home")
			worktree := filepath.Join(home, "workspaces", "repo")
			proxy := filepath.Join(root, "proxy")
			versionDir := filepath.Join(proxy, "example.com", "cacheprobe", "@v")
			for _, path := range []string{worktree, versionDir} {
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			makeSandboxWorktree(t, worktree, filepath.Join(root, "clone"))
			module := "module example.com/cacheprobe\n\ngo 1.26.0\n"
			for path, content := range map[string]string{
				filepath.Join(versionDir, "v1.0.0.mod"):  module,
				filepath.Join(versionDir, "v1.0.0.info"): `{"Version":"v1.0.0","Time":"2026-01-01T00:00:00Z"}`,
				filepath.Join(worktree, "go.mod"):        "module example.com/gate\n\ngo 1.26.0\n\nrequire example.com/cacheprobe v1.0.0\n",
				filepath.Join(worktree, "main.go"):       "package main\nimport (\"fmt\"; \"example.com/cacheprobe\")\nfunc main() { fmt.Print(cacheprobe.Value) }\n",
			} {
				if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			archive, err := os.Create(filepath.Join(versionDir, "v1.0.0.zip"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := archive.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
					t.Error(err)
				}
			})
			zw := zip.NewWriter(archive)
			for name, content := range map[string]string{"go.mod": module, "value.go": "package cacheprobe\nconst Value = \"cached-ok\"\n"} {
				entry, err := zw.Create("example.com/cacheprobe@v1.0.0/" + name)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := entry.Write([]byte(content)); err != nil {
					t.Fatal(err)
				}
			}
			if err := zw.Close(); err != nil {
				t.Fatal(err)
			}
			if err := archive.Close(); err != nil {
				t.Fatal(err)
			}
			t.Setenv("HOME", filepath.Join(root, "user-home"))
			t.Setenv("GOPATH", "")
			t.Setenv("GOMODCACHE", "")
			switch mode {
			case "gopath-list":
				t.Setenv("GOPATH", filepath.Join(root, "first-go")+":"+filepath.Join(root, "second-go"))
			case "explicit-cache":
				t.Setenv("GOMODCACHE", filepath.Join(root, "module-cache"))
			}
			t.Setenv("GOPROXY", "file://"+proxy)
			t.Setenv("GOSUMDB", "off")
			t.Setenv("GOTOOLCHAIN", "local")
			t.Setenv("GOFLAGS", "-modcacherw")
			setupLog := filepath.Join(root, "setup.log")
			result, err := Run(t.Context(), []string{filepath.Join(runtime.GOROOT(), "bin", "go"), "mod", "download", "all"}, worktree, setupLog, 15*time.Second)
			if err != nil || result.ExitCode != 0 {
				output, _ := os.ReadFile(setupLog)
				t.Fatalf("setup download: %+v, %v; %s", result, err, output)
			}
			// The file proxy is hidden by /tmp's scratch mount. The gate must
			// use setup's readonly cache with networking and GOPROXY disabled.
			args, err := Sandbox([]string{"bash", "-c", `set -eu
if (printf forbidden > "$GOMODCACHE/gate-write") 2>/dev/null; then exit 11; fi
go run .`}, worktree, home, nil)
			if err != nil {
				t.Fatal(err)
			}
			gateLog := filepath.Join(root, "gate.log")
			result, err = Run(t.Context(), args, worktree, gateLog, 45*time.Second)
			output, readErr := os.ReadFile(gateLog)
			if err != nil || readErr != nil || result.ExitCode != 0 || string(output) != "cached-ok" {
				t.Fatalf("offline cached gate: %+v, %v, %v; %s", result, err, readErr, output)
			}
		})
	}
}

func TestSandboxGitMetadataUnderTmp(t *testing.T) {
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("bubblewrap binary is absent")
	}
	root, err := os.MkdirTemp("/tmp", "loom-git-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	home := filepath.Join(root, "home")
	worktree := filepath.Join(home, "workspaces", "repo")
	clone := filepath.Join(root, "clone")
	makeSandboxWorktree(t, worktree, clone)
	args, err := Sandbox([]string{"bash", "-c", `set -eu
git status --porcelain
git rev-parse --verify HEAD > /dev/null
if (printf forbidden > "$1/config") 2>/dev/null; then exit 11; fi
printf git-ok`, "bash", filepath.Join(clone, ".git")}, worktree, home, nil)
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(root, "git-gate.log")
	result, err := Run(t.Context(), args, worktree, logPath, 10*time.Second)
	output, readErr := os.ReadFile(logPath)
	if err != nil || readErr != nil || result.ExitCode != 0 || string(output) != "git-ok" {
		t.Fatalf("Git metadata gate: %+v, %v, %v; %s", result, err, readErr, output)
	}
	if _, err := Sandbox([]string{"true"}, worktree, home, []string{clone}); err == nil {
		t.Fatal("accepted Git metadata under hidden private state")
	}
	commonLink := filepath.Join(clone, ".git", "worktrees", "repo", "commondir")
	if err := os.Rename(commonLink, commonLink+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(commonLink+".original", commonLink); err != nil {
		t.Fatal(err)
	}
	if _, err := Sandbox([]string{"true"}, worktree, home, nil); err == nil {
		t.Fatal("accepted symlink commondir")
	}
}

func makeSandboxWorktree(t *testing.T, worktree, clone string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(worktree), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"init", "-q", clone},
		{"-C", clone, "-c", "user.name=Runner Test", "-c", "user.email=runner@example.invalid", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", "commit", "-q", "--allow-empty", "-m", "initial"},
		{"-C", clone, "worktree", "add", "-q", "--detach", worktree},
	} {
		if output, err := exec.CommandContext(t.Context(), "git", args...).CombinedOutput(); err != nil {
			t.Fatalf("prepare linked worktree: %v; %s", err, output)
		}
	}
	link, err := os.ReadFile(filepath.Join(worktree, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	return string(link)
}
