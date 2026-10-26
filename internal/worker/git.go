package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func git(ctx context.Context, dir string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	argv := append([]string{"-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null"}, args...)
	cmd := exec.CommandContext(ctx, "git", argv...)
	cmd.Dir = dir
	// Do not let inherited GIT_DIR, alternate object stores or global hooks redirect a local job.
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0"}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", args[0], err, stderr.String())
	}
	return out, nil
}

func prepare(ctx context.Context, cfg Config, home string, id int64, attempt int) (string, string, error) {
	base, err := git(ctx, cfg.Repo, "rev-parse", "--verify", "--end-of-options", cfg.Base+"^{commit}")
	if err != nil {
		return "", "", err
	}
	sha := strings.TrimSpace(string(base))
	root := filepath.Join(home, "workspaces")
	if err := os.MkdirAll(root, 0700); err != nil {
		return "", "", err
	}
	path := filepath.Join(root, fmt.Sprintf("%d-%d", id, attempt))
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return "", "", fmt.Errorf("worktree path already exists or is unreadable: %s", path)
	}
	if _, err := git(ctx, cfg.Repo, "worktree", "add", "--detach", path, sha); err != nil {
		return path, sha, err
	}
	return path, sha, nil
}

// intact must precede every git command on agent-controlled files.
func intact(cfg Config, home, path, expectedLink string) error {
	if filepath.Dir(path) != filepath.Join(home, "workspaces") {
		return errors.New("worktree is outside this instance")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return errors.New("worktree path changed or became a symlink")
	}
	link := filepath.Join(path, ".git")
	info, err := os.Lstat(link)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("worktree git link is missing or not a regular file")
	}
	data, err := os.ReadFile(link)
	if err != nil {
		return err
	}
	text := string(data)
	if expectedLink != "" && text != expectedLink {
		return errors.New("agent changed the worktree git link")
	}
	line := strings.TrimSuffix(text, "\n")
	if strings.ContainsAny(line, "\r\n") || !strings.HasPrefix(line, "gitdir: ") {
		return errors.New("invalid worktree git link")
	}
	target := strings.TrimPrefix(line, "gitdir: ")
	if !filepath.IsAbs(target) {
		target = filepath.Join(path, target)
	}
	target, err = filepath.EvalSymlinks(target)
	if err != nil {
		return err
	}
	admin, err := filepath.EvalSymlinks(filepath.Join(cfg.Repo, ".git", "worktrees"))
	if err != nil || filepath.Dir(target) != admin {
		return errors.New("worktree git link does not belong to the configured clone")
	}
	backlink, err := os.ReadFile(filepath.Join(target, "gitdir"))
	if err != nil || strings.TrimSpace(string(backlink)) != link {
		return errors.New("worktree git backlink mismatch")
	}
	return nil
}

// fingerprint includes tracked and non-ignored untracked files, including deletions and symlink targets.
func fingerprint(ctx context.Context, cfg Config, home, path, link string) (string, error) {
	if err := intact(cfg, home, path, link); err != nil {
		return "", err
	}
	files, err := git(ctx, path, "ls-files", "--cached", "--others", "--exclude-standard", "-z")
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	for name := range strings.SplitSeq(string(files), "\x00") {
		if name == "" {
			continue
		}
		if !filepath.IsLocal(name) {
			return "", errors.New("git returned a nonlocal path")
		}
		// NUL separates paths; each content hash has fixed length, avoiding ambiguous concatenation.
		parent := filepath.Dir(filepath.Join(path, name))
		for {
			resolved, err := filepath.EvalSymlinks(parent)
			if errors.Is(err, os.ErrNotExist) && parent != path {
				parent = filepath.Dir(parent)
				continue
			}
			if err != nil || resolved != parent {
				return "", fmt.Errorf("source path has a symlinked parent: %s", name)
			}
			break
		}
		_, _ = io.WriteString(hash, name+"\x00")
		fileHash := sha256.New()
		file := filepath.Join(path, name)
		info, err := os.Lstat(file)
		switch {
		case errors.Is(err, os.ErrNotExist):
			_, _ = io.WriteString(fileHash, "missing")
		case err != nil:
			return "", err
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(file)
			if err != nil {
				return "", err
			}
			_, _ = io.WriteString(fileHash, "link:"+target)
		case info.Mode().IsRegular():
			_, _ = fmt.Fprintf(fileHash, "mode:%d:", info.Mode().Perm())
			f, err := os.Open(file)
			if err != nil {
				return "", err
			}
			_, copyErr := io.Copy(fileHash, f)
			if err := errors.Join(copyErr, f.Close()); err != nil {
				return "", err
			}
		default:
			return "", fmt.Errorf("unsupported worktree entry: %s", name)
		}
		_, _ = hash.Write(fileHash.Sum(nil)) // hash.Hash writes never fail.
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// Git runs Git without inherited configuration or executable hooks.
func Git(ctx context.Context, dir string, args ...string) ([]byte, error) {
	return git(ctx, dir, args...)
}

func Intact(cfg Config, home, path string) error { return intact(cfg, home, path, "") }

// RemoveOwned never follows an untrusted checkout link or removes a target clone.
func RemoveOwned(ctx context.Context, cfg Config, home, path string) error {
	if err := intact(cfg, home, path, ""); err != nil {
		return err
	}
	_, err := git(ctx, cfg.Repo, "worktree", "remove", "--force", path)
	return err
}
