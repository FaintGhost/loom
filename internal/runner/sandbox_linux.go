package runner

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

// Sandbox requires bubblewrap and returns no unsandboxed fallback. Only the
// worktree and ephemeral /tmp and /dev are writable; private state is covered.
func Sandbox(argv []string, worktree, home string, extraHidden []string) ([]string, error) {
	if len(argv) == 0 || argv[0] == "" {
		return nil, errors.New("sandbox requires a command")
	}
	bwrap, err := exec.LookPath("bwrap")
	if err != nil {
		return nil, fmt.Errorf("bubblewrap is required: %w", err)
	}
	worktree, err = directory(worktree)
	if err != nil {
		return nil, fmt.Errorf("sandbox worktree: %w", err)
	}
	home, err = directory(home)
	if err != nil {
		return nil, fmt.Errorf("sandbox home: %w", err)
	}
	if protectedMount(worktree) || protectedMount(home) || within(worktree, home) {
		return nil, errors.New("sandbox home and worktree must be distinct safe directories")
	}
	gitFile := filepath.Join(worktree, ".git")
	gitCommon, err := worktreeGitCommon(worktree)
	if err != nil {
		return nil, fmt.Errorf("sandbox Git metadata: %w", err)
	}

	type hiddenPath struct {
		path string
		dir  bool
	}
	hidden := []hiddenPath{{home, true}}
	paths := []string{"/root/.hermes/secrets", "/root/.config/gh", "/root/.codex", "/root/.ssh", "/root/.hermes/fb"}
	paths = append(paths, extraHidden...)
	for _, path := range paths {
		if !filepath.IsAbs(path) {
			return nil, errors.New("hidden paths must be absolute")
		}
		resolved, err := filepath.EvalSymlinks(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("resolve hidden path: %w", err)
		}
		if protectedMount(resolved) || within(worktree, resolved) || within(resolved, worktree) {
			return nil, errors.New("hidden path overlaps the worktree or a required system mount")
		}
		info, err := os.Stat(resolved)
		if err != nil {
			return nil, fmt.Errorf("inspect hidden path: %w", err)
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return nil, errors.New("hidden paths must be regular files or directories")
		}
		hidden = append(hidden, hiddenPath{resolved, info.IsDir()})
	}
	// Cover ancestors first and omit their children: a later mount must never
	// uncover state hidden by an earlier one.
	slices.SortFunc(hidden, func(a, b hiddenPath) int { return strings.Compare(a.path, b.path) })
	covered := hidden[:0]
	for _, h := range hidden {
		if !slices.ContainsFunc(covered, func(parent hiddenPath) bool { return within(parent.path, h.path) }) {
			covered = append(covered, h)
		}
	}
	goroot := runtime.GOROOT()
	if goroot != "" {
		goroot, err = directory(goroot)
		if err != nil {
			return nil, fmt.Errorf("sandbox Go toolchain: %w", err)
		}
		for _, h := range covered {
			if within(h.path, goroot) {
				return nil, errors.New("sandbox hides the Go toolchain")
			}
		}
	}
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("resolve Go workspace: %w", err)
		}
		gopath = filepath.Join(userHome, "go")
	}
	goPaths := filepath.SplitList(gopath)
	for _, path := range goPaths {
		if !filepath.IsAbs(path) {
			return nil, errors.New("GOPATH entries must be absolute")
		}
	}
	modCache := os.Getenv("GOMODCACHE")
	if modCache == "" {
		modCache = filepath.Join(goPaths[0], "pkg", "mod")
	}
	if !filepath.IsAbs(modCache) {
		return nil, errors.New("GOMODCACHE must be absolute")
	}
	cacheMount, err := directory(modCache)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("resolve Go module cache: %w", err)
	}
	if cacheMount != "" {
		if protectedMount(cacheMount) || within(cacheMount, worktree) {
			return nil, errors.New("Go module cache overlaps a required mount")
		}
		for _, h := range covered {
			if within(h.path, cacheMount) || within(cacheMount, h.path) {
				return nil, errors.New("Go module cache overlaps hidden private state")
			}
		}
		modCache = cacheMount
	}
	if protectedMount(gitCommon) || within(gitCommon, worktree) || within(worktree, gitCommon) {
		return nil, errors.New("Git metadata overlaps the worktree or a required mount")
	}
	for _, h := range covered {
		if within(h.path, gitCommon) || within(gitCommon, h.path) {
			return nil, errors.New("Git metadata overlaps hidden private state")
		}
	}

	args := []string{bwrap, "--unshare-all", "--die-with-parent", "--new-session", "--cap-drop", "ALL",
		"--ro-bind", "/", "/", "--tmpfs", "/tmp", "--proc", "/proc", "--dev", "/dev"}
	for _, h := range covered {
		if h.dir {
			args = append(args, "--tmpfs", h.path)
		} else {
			args = append(args, "--ro-bind", "/dev/null", h.path)
		}
	}
	// Home is hidden before the writable worktree is restored. Remounting the
	// covering tmpfs afterward is nonrecursive, preserving the worktree mount.
	args = append(args, "--bind", worktree, worktree, "--ro-bind", gitFile, gitFile,
		"--ro-bind", gitCommon, gitCommon)
	if cacheMount != "" {
		// Preserve setup's downloaded modules, including caches under /tmp,
		// without giving the gate permission to modify the host cache.
		args = append(args, "--ro-bind", cacheMount, cacheMount)
	}
	for _, h := range covered {
		if h.dir {
			args = append(args, "--remount-ro", h.path)
		}
	}
	pathDirs := []string{}
	if goroot != "" {
		pathDirs = append(pathDirs, filepath.Join(goroot, "bin"))
	}
	for _, path := range filepath.SplitList(os.Getenv("PATH")) {
		if filepath.IsAbs(path) {
			pathDirs = append(pathDirs, path)
		}
	}
	pathDirs = append(pathDirs, "/usr/local/bin", "/usr/bin", "/bin")
	args = append(args, "--clearenv", "--setenv", "PATH", strings.Join(pathDirs, string(os.PathListSeparator)),
		"--setenv", "HOME", "/tmp", "--setenv", "GOCACHE", "/tmp/go-build",
		"--setenv", "GOPROXY", "off", "--setenv", "GOTOOLCHAIN", "local",
		"--setenv", "GOPATH", gopath, "--setenv", "GOMODCACHE", modCache)
	if goroot != "" {
		args = append(args, "--setenv", "GOROOT", goroot)
	}
	args = append(args, "--chdir", worktree, "--")
	return append(args, argv...), nil
}

func directory(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", errors.New("directory must be absolute")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("path is not a directory")
	}
	return resolved, nil
}

// within includes equality, using a separator boundary rather than a prefix.
func within(parent, path string) bool {
	return path == parent || strings.HasPrefix(path, parent+string(os.PathSeparator))
}

func protectedMount(path string) bool {
	return path == "/" || path == "/tmp" || within("/dev", path) || within("/proc", path) || within("/sys", path) ||
		path == "/usr" || path == "/bin" || path == "/sbin" || path == "/lib" || path == "/lib64" || path == "/etc"
}

// Linked worktrees use common/worktrees/name plus a regular commondir link.
// Reject symlinked metadata paths: masked ancestors must not change where Git's
// lexical paths point after the common directory is restored inside the gate.
func worktreeGitCommon(worktree string) (string, error) {
	link, err := regularLink(filepath.Join(worktree, ".git"))
	if err != nil {
		return "", err
	}
	gitPath, ok := strings.CutPrefix(link, "gitdir: ")
	if !ok || gitPath == "" {
		return "", errors.New("invalid worktree .git link")
	}
	if !filepath.IsAbs(gitPath) {
		gitPath = filepath.Join(worktree, gitPath)
	}
	gitDir, err := directory(gitPath)
	if err != nil {
		return "", err
	}
	if gitDir != filepath.Clean(gitPath) {
		return "", errors.New("symlinked Git metadata is unsupported")
	}
	commonPath, err := regularLink(filepath.Join(gitDir, "commondir"))
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(commonPath) {
		commonPath = filepath.Join(gitDir, commonPath)
	}
	common, err := directory(commonPath)
	if err != nil {
		return "", err
	}
	if common != filepath.Clean(commonPath) || filepath.Dir(gitDir) != filepath.Join(common, "worktrees") {
		return "", errors.New("invalid linked-worktree common directory")
	}
	return common, nil
}

func regularLink(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("Git metadata link must be a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	link := strings.TrimSpace(string(data))
	if link == "" || strings.ContainsAny(link, "\r\n\x00") {
		return "", errors.New("invalid Git metadata link contents")
	}
	return link, nil
}
