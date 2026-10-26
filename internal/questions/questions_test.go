package questions

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/worker"
)

func gitFixture(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.invalid", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.invalid")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git fixture %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}
func TestQuestionFreshSnapshotEvidenceCleanupAndGuards(t *testing.T) {
	root := t.TempDir()
	origin := filepath.Join(root, "origin")
	if err := os.Mkdir(origin, 0700); err != nil {
		t.Fatal(err)
	}
	gitFixture(t, origin, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(origin, "source.txt"), []byte("first revision"), 0600); err != nil {
		t.Fatal(err)
	}
	gitFixture(t, origin, "add", "source.txt")
	gitFixture(t, origin, "commit", "-m", "initial")
	clone := filepath.Join(root, "clone")
	gitFixture(t, root, "clone", origin, clone)
	home := filepath.Join(root, "home")
	if err := os.Mkdir(home, 0700); err != nil {
		t.Fatal(err)
	}
	agent := filepath.Join(root, "reader")
	script := `#!/bin/sh
out="$1"
case "$2" in
 mutate) printf 'changed' > source.txt; printf 'unsafe answer' > "$out" ;;
 secret) printf 'secret-value' > "$out" ;;
 *) cat source.txt > "$out" ;;
esac
`
	if err := os.WriteFile(agent, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	cfg := worker.Config{Repo: clone, GitHubRepo: "owner/repo", Harness: "test", Agent: []string{agent, "{last}", "normal", "--sandbox", "workspace-write"}, QuestionTimeoutSeconds: 10, SecretPatterns: []string{"secret-value"}}
	report := model.Report{ID: 7, Body: "How does this work?", Language: "en"}
	first, err := Answer(t.Context(), home, cfg, report, "")
	if err != nil {
		t.Fatal(err)
	}
	if first != "first revision" {
		t.Fatalf("unexpected answer %q", first)
	}
	if err = os.WriteFile(filepath.Join(origin, "source.txt"), []byte("second revision"), 0600); err != nil {
		t.Fatal(err)
	}
	gitFixture(t, origin, "commit", "-am", "update")
	second, err := Answer(t.Context(), home, cfg, report, "first question and answer")
	if err != nil {
		t.Fatal(err)
	}
	if second != "second revision" {
		t.Fatal("question read stale local default")
	}
	evidence, err := os.ReadDir(filepath.Join(home, "jobs", "7", "question"))
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence) != 2 {
		t.Fatal("answers did not retain distinct evidence")
	}
	commits := map[string]bool{}
	for _, entry := range evidence {
		path := filepath.Join(home, "jobs", "7", "question", entry.Name())
		data, err := os.ReadFile(filepath.Join(path, "snapshot.json"))
		if err != nil {
			t.Fatal(err)
		}
		var snapshot map[string]string
		if err = json.Unmarshal(data, &snapshot); err != nil {
			t.Fatal(err)
		}
		commits[snapshot["commit"]] = true
		if _, err = os.Stat(filepath.Join(path, "prompt.txt")); err != nil {
			t.Fatal(err)
		}
	}
	if len(commits) != 2 {
		t.Fatal("snapshot evidence did not pin each current remote commit")
	}
	cfg.Agent[2] = "mutate"
	if _, err = Answer(t.Context(), home, cfg, report, ""); err == nil {
		t.Fatal("mutated checkout answer published")
	}
	cfg.Agent[2] = "secret"
	if _, err = Answer(t.Context(), home, cfg, report, ""); err == nil {
		t.Fatal("credential answer published")
	}
	entries, err := os.ReadDir(filepath.Join(home, "workspaces"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("temporary question checkout leaked")
	}
}
func TestReadOnlyRequiredBeforeRepositoryAccess(t *testing.T) {
	cfg := worker.Config{Repo: "/does/not/exist", Agent: []string{"unsupported-reader"}}
	if _, err := Answer(t.Context(), t.TempDir(), cfg, model.Report{ID: 1, Body: "question"}, ""); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("missing strict read-only refusal: %v", err)
	}
}
