package improvement

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/FaintGhost/loom/internal/worker"
)

// Compatibility has no writable worktree: runner.Sandbox requires linked Git
// metadata, while this check runs from a module directory. Match its private
// mounts and clean environment, granting writes only to disposable scratch.
func compatibilitySandbox(bwrap, home, scratch, module, binary string, cfg worker.Config) ([]string, error) {
	userHome, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		gopath = filepath.Join(userHome, "go")
	}
	goPaths := filepath.SplitList(gopath)
	for _, path := range goPaths {
		if !filepath.IsAbs(path) {
			return nil, errors.New("compatibility GOPATH must be absolute")
		}
	}
	modCache := os.Getenv("GOMODCACHE")
	if modCache == "" {
		modCache = filepath.Join(goPaths[0], "pkg", "mod")
	}
	if !filepath.IsAbs(modCache) {
		return nil, errors.New("compatibility GOMODCACHE must be absolute")
	}
	goRoot, err := filepath.EvalSymlinks(runtime.GOROOT())
	if err != nil || !filepath.IsAbs(goRoot) {
		return nil, errors.New("compatibility requires an installed Go toolchain at runtime.GOROOT; unset an invalid inherited GOROOT")
	}
	goBinary := filepath.Join(goRoot, "bin", "go")
	resolvedGo, err := filepath.EvalSymlinks(goBinary)
	if err != nil || resolvedGo != goBinary {
		return nil, errors.New("compatibility Go executable must resolve inside the installed toolchain")
	}
	goInfo, err := os.Stat(goBinary)
	if err != nil || !goInfo.Mode().IsRegular() || goInfo.Mode().Perm()&0111 == 0 {
		return nil, errors.New("compatibility Go toolchain executable is unavailable")
	}
	required := []string{scratch, module, binary, goRoot}
	for i, path := range required {
		if !filepath.IsAbs(path) {
			return nil, errors.New("compatibility paths must be absolute")
		}
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			return nil, err
		}
		required[i] = resolved
	}
	if resolved, err := filepath.EvalSymlinks(modCache); err == nil {
		modCache = resolved
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	required = append(required, modCache)
	paths := []string{home, "/root/.hermes/secrets", "/root/.config/gh", "/root/.codex", "/root/.ssh", "/root/.hermes/fb"}
	paths = append(paths, cfg.Hidden...)
	paths = append(paths, cfg.SecretFiles...)
	paths = append(paths, cfg.MagpieKeyFile, cfg.DiscordTokenFile)
	type hiddenMount struct {
		path      string
		directory bool
	}
	var hidden []hiddenMount
	for _, path := range paths {
		if path == "" {
			continue
		}
		if !filepath.IsAbs(path) {
			return nil, errors.New("compatibility hidden paths must be absolute")
		}
		resolved, err := filepath.EvalSymlinks(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if resolved == "/" || slices.Contains([]string{"/tmp", "/usr", "/bin", "/sbin", "/lib", "/lib64", "/etc"}, resolved) || inside("/dev", resolved) || inside("/proc", resolved) || inside("/sys", resolved) {
			return nil, errors.New("compatibility hidden path overlaps a required system mount")
		}
		for _, requiredPath := range required {
			if inside(resolved, requiredPath) || inside(required[0], resolved) || inside(goRoot, resolved) {
				return nil, errors.New("compatibility hidden path overlaps module, tool, Go toolchain, cache or scratch")
			}
		}
		info, err := os.Stat(resolved)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return nil, errors.New("compatibility hidden path must be a regular file or directory")
		}
		hidden = append(hidden, hiddenMount{path: resolved, directory: info.IsDir()})
	}
	slices.SortFunc(hidden, func(a, b hiddenMount) int { return strings.Compare(a.path, b.path) })
	args := []string{bwrap, "--unshare-all", "--die-with-parent", "--new-session", "--cap-drop", "ALL", "--ro-bind", "/", "/", "--proc", "/proc", "--dev", "/dev"}
	var covered []string
	for _, mount := range hidden {
		if slices.ContainsFunc(covered, func(parent string) bool { return inside(parent, mount.path) }) {
			continue
		}
		if mount.directory {
			args = append(args, "--tmpfs", mount.path, "--remount-ro", mount.path)
		} else {
			args = append(args, "--ro-bind", "/dev/null", mount.path)
		}
		covered = append(covered, mount.path)
	}
	pathDirs := []string{filepath.Join(goRoot, "bin")}
	for _, path := range filepath.SplitList(os.Getenv("PATH")) {
		if filepath.IsAbs(path) {
			pathDirs = append(pathDirs, path)
		}
	}
	pathDirs = append(pathDirs, "/usr/local/bin", "/usr/bin", "/bin")
	args = append(args, "--bind", scratch, scratch, "--clearenv", "--setenv", "PATH", strings.Join(pathDirs, string(os.PathListSeparator)),
		"--setenv", "HOME", scratch, "--setenv", "TMPDIR", scratch, "--setenv", "GOCACHE", filepath.Join(scratch, "go-build"),
		"--setenv", "GOPATH", gopath, "--setenv", "GOMODCACHE", modCache, "--setenv", "GOPROXY", "off", "--setenv", "GOTOOLCHAIN", "local",
		"--chdir", module, "--")
	return args, nil
}

func inside(parent, path string) bool {
	return path == parent || strings.HasPrefix(path, parent+string(os.PathSeparator))
}

func compatibilityRedactor(cfg worker.Config) func(string) string {
	cfg.SecretFiles = append(slices.Clone(cfg.SecretFiles), cfg.MagpieKeyFile, cfg.DiscordTokenFile)
	var credentials []string
	for _, variable := range []string{cfg.MagpieKeyEnv, cfg.DiscordTokenEnv} {
		if value := strings.TrimSpace(os.Getenv(variable)); value != "" {
			credentials = append(credentials, value)
		}
	}
	for _, path := range []string{cfg.MagpieKeyFile, cfg.DiscordTokenFile} {
		if data, err := os.ReadFile(path); err == nil {
			if value := strings.TrimSpace(string(data)); value != "" {
				credentials = append(credentials, value)
			}
		}
	}
	// Report metadata is already JSON when collected; recognize its escaped
	// string values before another serialization can hide the original token.
	for _, credential := range credentials {
		encoded, _ := json.Marshal(credential)
		escaped := string(encoded[1 : len(encoded)-1])
		if escaped != credential {
			credentials = append(credentials, escaped)
		}
	}
	return func(text string) string {
		for _, credential := range credentials {
			if strings.Contains(text, credential) {
				return "[credential-bearing value withheld]"
			}
		}
		return worker.Publishable(cfg, text)
	}
}

func redactedArgs(argv []string, redact func(string) string) []string {
	out := make([]string, len(argv))
	for i, value := range argv {
		out[i] = redact(value)
	}
	return out
}

func credentialCommand(argv []string, redact func(string) string) error {
	for _, value := range argv {
		if redact(value) != value {
			return errors.New("compatibility command contains a credential; execution withheld")
		}
	}
	return nil
}
