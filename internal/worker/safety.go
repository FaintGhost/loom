package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/FaintGhost/loom/internal/runner"
)

var builtinSecrets = []string{`ghp_[A-Za-z0-9]{30,}`, `github_pat_[A-Za-z0-9_]{30,}`, `gh[ousr]_[A-Za-z0-9]{30,}`, `sk-[A-Za-z0-9_-]{20,}`, `xox[baprs]-[A-Za-z0-9-]{10,}`, `-----BEGIN [A-Z ]*PRIVATE KEY-----`, `[MNO][A-Za-z\d_-]{23,25}\.[A-Za-z\d_-]{6}\.[A-Za-z\d_-]{27,}`}
var secretValue = regexp.MustCompile(`["']?[A-Za-z0-9_.-]+["']?\s*:\s*["']?([^"'\s,#]+)`)

func matches(path string, patterns []string) bool {
	for _, glob := range patterns {
		var b strings.Builder
		b.WriteByte('^')
		for len(glob) > 0 {
			switch {
			case strings.HasPrefix(glob, "**/"):
				b.WriteString(`(?:.*/)?`)
				glob = glob[3:]
			case strings.HasPrefix(glob, "**"):
				b.WriteString(`.*`)
				glob = glob[2:]
			case glob[0] == '*':
				b.WriteString(`[^/]*`)
				glob = glob[1:]
			case glob[0] == '?':
				b.WriteString(`[^/]`)
				glob = glob[1:]
			default:
				b.WriteString(regexp.QuoteMeta(glob[:1]))
				glob = glob[1:]
			}
		}
		b.WriteByte('$')
		if regexp.MustCompile(b.String()).MatchString(path) {
			return true
		}
	}
	return false
}

func secretLiterals(cfg Config) []string {
	var literals []string
	files := slices.Clone(cfg.SecretFiles)
	for _, path := range cfg.SecretFiles {
		if entries, err := os.ReadDir(path); err == nil {
			for _, entry := range entries {
				if !entry.IsDir() {
					files = append(files, filepath.Join(path, entry.Name()))
				}
			}
		}
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for line := range strings.SplitSeq(string(data), "\n") {
			line = strings.TrimSpace(line)
			if len(line) >= 12 {
				literals = append(literals, line)
			}
			for _, m := range secretValue.FindAllStringSubmatch(line, -1) {
				if len(m[1]) >= 12 {
					literals = append(literals, m[1])
				}
			}
		}
	}
	return literals
}

func holdsSecret(cfg Config, text string) bool {
	for _, literal := range secretLiterals(cfg) {
		if strings.Contains(text, literal) {
			return true
		}
	}
	for _, pattern := range append(slices.Clone(builtinSecrets), cfg.SecretPatterns...) {
		re, err := regexp.Compile(pattern)
		if err != nil || re.MatchString(text) {
			return true
		}
	}
	return false
}

// Publishable never returns a matched credential in an error or notification.
func Publishable(cfg Config, text string) string {
	if holdsSecret(cfg, text) {
		return "Output withheld: it appeared to contain a credential. See the local job evidence."
	}
	return text
}

func changed(ctx context.Context, path, base string) ([]string, error) {
	data, err := git(ctx, path, "diff", "--cached", "--name-only", "--no-renames", "-z", base, "--")
	if err != nil {
		return nil, err
	}
	var paths []string
	for name := range strings.SplitSeq(string(data), "\x00") {
		if name != "" {
			if !filepath.IsLocal(name) {
				return nil, errors.New("nonlocal changed path")
			}
			paths = append(paths, name)
		}
	}
	return paths, nil
}

// Guard judges staged names, complete changed files, removed content, and outgoing prose.
func Guard(ctx context.Context, cfg Config, home, path, base string, texts ...string) error {
	if err := intact(cfg, home, path, ""); err != nil {
		return err
	}
	paths, err := changed(ctx, path, base)
	if err != nil {
		return err
	}
	for _, name := range paths {
		if matches(name, cfg.GuardPaths) || matches(name, []string{".fb-attachments/**", ".fb-pr.md", ".fb-triage.json", ".fb-review.json", ".fb-agent-report.md"}) {
			return fmt.Errorf("protected path: %s", name)
		}
		file := filepath.Join(path, name)
		info, err := os.Lstat(file)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		parent, err := filepath.EvalSymlinks(filepath.Dir(file))
		if err != nil || parent != filepath.Dir(file) {
			return fmt.Errorf("changed file has a symlinked parent: %s", name)
		}
		var data []byte
		if info.Mode()&os.ModeSymlink != 0 {
			target, e := os.Readlink(file)
			err = e
			data = []byte(target)
		} else if info.Mode().IsRegular() {
			if info.Size() > 2_000_000 {
				return fmt.Errorf("changed file exceeds credential scan limit: %s", name)
			}
			data, err = os.ReadFile(file)
		} else {
			return fmt.Errorf("unsupported changed file: %s", name)
		}
		if err != nil {
			return err
		}
		if holdsSecret(cfg, string(data)) {
			return fmt.Errorf("credential detected in %s", name)
		}
	}
	diff, err := git(ctx, path, "diff", "--cached", "--no-ext-diff", "--no-textconv", base, "--")
	if err != nil {
		return err
	}
	if holdsSecret(cfg, string(diff)) {
		return errors.New("credential detected in change")
	}
	for _, text := range texts {
		if holdsSecret(cfg, text) {
			return errors.New("credential detected in agent write-up")
		}
	}
	return nil
}

func readRegular(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, errors.New("agent output is not a bounded regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, limit+1))
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
func budget(value, fallback int) time.Duration {
	if value <= 0 {
		value = fallback
	}
	return time.Duration(value) * time.Second
}

func sandbox(cfg Config, argv []string, path, home string) ([]string, error) {
	args, err := runner.Sandbox(argv, path, home, cfg.Hidden)
	if err != nil {
		return nil, err
	}
	// Add operator-owned environment/mounts before the command delimiter.
	at := slices.Index(args, "--")
	extra := []string{}
	if cfg.SandboxNetwork {
		extra = append(extra, "--share-net")
	}
	for _, mount := range cfg.SandboxTmpfs {
		if !filepath.IsAbs(mount) || filepath.Clean(mount) != mount || mount == "/" || mount == home || mount == path || strings.HasPrefix(path, mount+"/") || strings.HasPrefix(mount, path+"/") {
			return nil, fmt.Errorf("unsafe sandbox tmpfs: %s", mount)
		}
		extra = append(extra, "--tmpfs", mount)
	}
	for key, value := range cfg.SandboxEnv {
		if strings.ContainsAny(key, "=\x00") || key == "" || strings.ContainsRune(value, 0) {
			return nil, errors.New("invalid sandbox environment")
		}
		extra = append(extra, "--setenv", key, value)
	}
	return slices.Insert(args, at, extra...), nil
}

func testChanges(ctx context.Context, cfg Config, path, base string) ([]string, []string, error) {
	paths, err := changed(ctx, path, base)
	if err != nil {
		return nil, nil, err
	}
	var tests, repro []string
	for _, name := range paths {
		if !matches(name, cfg.TestGlobs) {
			continue
		}
		info, err := os.Lstat(filepath.Join(path, name))
		if err != nil || !info.Mode().IsRegular() {
			return nil, nil, fmt.Errorf("test file deleted or replaced: %s", name)
		}
		tests = append(tests, name)
		if matches(name, cfg.RedFileGlobs) {
			repro = append(repro, name)
		}
		patch, err := git(ctx, path, "diff", "--cached", "--no-ext-diff", "--no-textconv", base, "--", name)
		if err != nil {
			return nil, nil, err
		}
		for line := range strings.SplitSeq(string(patch), "\n") {
			if !strings.HasPrefix(line, "+") || strings.HasPrefix(line, "+++") {
				continue
			}
			for _, pattern := range cfg.WeakeningPatterns {
				re, err := regexp.Compile(pattern)
				if err != nil {
					return nil, nil, fmt.Errorf("invalid weakening pattern: %w", err)
				}
				if re.MatchString(line[1:]) {
					return nil, nil, fmt.Errorf("test skipped or narrowed: %s", name)
				}
			}
		}
	}
	return tests, repro, nil
}

// redCheck uses an isolated base checkout, so a crash cannot leave the fix stashed away.
func redCheck(ctx context.Context, cfg Config, home, path, base, job string, round bool, lock *os.File) (runner.Result, error) {
	var result runner.Result
	if cfg.RedVerify == "" && !cfg.GuardWeakening {
		return result, nil
	}
	tests, repro, err := testChanges(ctx, cfg, path, base)
	if err != nil {
		return result, err
	}
	if cfg.RedVerify == "" || round {
		return result, nil
	}
	if len(repro) == 0 {
		return result, errors.New("no reproduction test: add a test failing without the fix")
	}
	paths, err := changed(ctx, path, base)
	if err != nil {
		return result, err
	}
	if len(paths) == len(tests) {
		return result, errors.New("change only touches tests; the bug is not fixed")
	}
	red := path + "-red"
	if _, err := os.Lstat(red); err == nil {
		if err := RemoveOwned(ctx, cfg, home, red); err != nil {
			return result, err
		}
	}
	if _, err := git(ctx, cfg.Repo, "worktree", "add", "--detach", red, base); err != nil {
		return result, err
	}
	defer func() { _ = RemoveOwned(context.WithoutCancel(ctx), cfg, home, red) }()
	args := append([]string{"diff", "--cached", "--binary", "--no-ext-diff", "--no-textconv", base, "--"}, tests...)
	patch, err := git(ctx, path, args...)
	if err != nil {
		return result, err
	}
	patchPath := filepath.Join(job, "red-tests.patch")
	if err := os.WriteFile(patchPath, patch, 0600); err != nil {
		return result, err
	}
	if _, err := git(ctx, red, "apply", "--index", patchPath); err != nil {
		return result, err
	}
	expected, err := fingerprint(ctx, cfg, home, red, "")
	if err != nil {
		return result, err
	}
	if cfg.Setup != "" {
		r, e := runner.Run(ctx, []string{"bash", "-c", cfg.Setup}, red, filepath.Join(job, "red-setup.log"), budget(cfg.SetupTimeoutSeconds, cfg.TimeoutSeconds), lock)
		if e != nil || r.ExitCode != 0 {
			return r, errors.Join(e, errors.New("red checkout setup failed"))
		}
	}
	prepared, err := fingerprint(ctx, cfg, home, red, "")
	if err != nil {
		return result, err
	}
	if prepared != expected {
		return result, errors.New("red setup changed the unfixed source or reproduction tests")
	}
	files := make([]string, len(repro))
	var packages []string
	for i, name := range repro {
		files[i] = quote(name)
		pkg := "./" + filepath.ToSlash(filepath.Dir(name))
		if !slices.Contains(packages, pkg) {
			packages = append(packages, pkg)
		}
	}
	for i := range packages {
		packages[i] = quote(packages[i])
	}
	command := strings.NewReplacer("{files}", strings.Join(files, " "), "{packages}", strings.Join(packages, " ")).Replace(cfg.RedVerify)
	argv, err := sandbox(cfg, []string{"bash", "-c", command}, red, home)
	if err != nil {
		return result, err
	}
	before, err := fingerprint(ctx, cfg, home, red, "")
	if err != nil {
		return result, err
	}
	result, err = runner.Run(ctx, argv, red, filepath.Join(job, "red.log"), budget(cfg.VerifyTimeoutSeconds, cfg.TimeoutSeconds), lock)
	after, inspect := fingerprint(ctx, cfg, home, red, "")
	if inspect != nil || before != after {
		return result, errors.Join(inspect, errors.New("red command changed source"))
	}
	if err != nil {
		return result, err
	}
	if result.ExitCode == 0 {
		return result, errors.New("reproduction test passed on unfixed code")
	}
	if result.ExitCode < 0 || result.ExitCode == 126 || result.ExitCode == 127 || result.TimedOut || result.Canceled {
		return result, errors.New("reproduction command did not fail cleanly")
	}
	return result, nil
}

func harvest(path, job string) (string, error) {
	var report strings.Builder
	for _, name := range []string{".fb-agent-report.md", ".fb-pr.md", "REPORT.md"} {
		data, err := readRegular(filepath.Join(path, name), 200_000)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		if err := os.WriteFile(filepath.Join(job, name), data, 0600); err != nil {
			return "", err
		}
		if err := os.Remove(filepath.Join(path, name)); err != nil {
			return "", err
		}
		report.Write(data)
		report.WriteByte('\n')
	}
	if report.Len() == 0 {
		if data, err := readRegular(filepath.Join(job, "last.txt"), 200_000); err == nil {
			report.Write(data)
		}
	}
	attachments := filepath.Join(path, ".fb-attachments")
	if _, err := os.Lstat(attachments); err == nil {
		if err := os.RemoveAll(attachments); err != nil {
			return "", err
		}
	}
	return report.String(), nil
}

func sourcePatch(ctx context.Context, path, base, job string) ([]byte, error) {
	if _, err := git(ctx, path, "add", "--all"); err != nil {
		return nil, err
	}
	patch, err := git(ctx, path, "diff", "--cached", "--no-ext-diff", "--no-textconv", "--binary", base, "--")
	if err != nil {
		return nil, err
	}
	return patch, os.WriteFile(filepath.Join(job, "patch.diff"), patch, 0600)
}

func TestPath(cfg Config, path string) bool { return matches(path, cfg.TestGlobs) }

func trustedFollowups(raw string) string {
	var entries []map[string]any
	if json.Unmarshal([]byte(raw), &entries) != nil {
		return ""
	}
	var accepted []map[string]any
	for _, entry := range entries {
		if trust, ok := entry["trusted"].(bool); !ok || trust {
			accepted = append(accepted, entry)
		}
	}
	if len(accepted) == 0 {
		return ""
	}
	data, _ := json.Marshal(accepted)
	return string(data)
}

func copyAttachments(home, path string, id int64) error {
	source := filepath.Join(home, "jobs", fmt.Sprint(id), "attachments")
	entries, err := os.ReadDir(source)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	destination := filepath.Join(path, ".fb-attachments")
	if _, err := os.Lstat(destination); err == nil {
		return errors.New("attachment destination already exists")
	}
	if len(entries) == 0 {
		return nil
	}
	if err := os.Mkdir(destination, 0700); err != nil {
		return err
	}
	for _, entry := range entries {
		data, err := readRegular(filepath.Join(source, entry.Name()), 8_000_000)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(destination, entry.Name()), data, 0600); err != nil {
			return err
		}
	}
	return nil
}
