package worker

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/FaintGhost/loom/internal/model"
)

func TestGuardStopsProtectedPathsBeforeGate(t *testing.T) {
	home, cfg, db := localInstance(t, "printf changed > AGENTS.md", "touch should-never-run")
	cfg.GuardPaths = []string{"AGENTS.md"}
	if _, err := db.Add(t.Context(), "change source"); err != nil {
		t.Fatal(err)
	}
	report, err := Work(t.Context(), home, cfg, db)
	if err == nil || report.Status != model.NeedsHuman {
		t.Fatalf("protected file accepted: %+v %v", report, err)
	}
	if _, err := os.Stat(filepath.Join(AttemptDir(home, report), "verify.log")); !os.IsNotExist(err) {
		t.Fatalf("guard ran gate: %v", err)
	}
	if _, err := VerifiedSnapshot(t.Context(), home, cfg, report); err == nil {
		t.Fatal("guarded change obtained publication snapshot")
	}
}

func TestTrustedFollowupsAndCredentialGuard(t *testing.T) {
	text := trustedFollowups(`[{"body":"trusted legacy"},{"body":"allowed","trusted":true},{"body":"stranger instruction","trusted":false}]`)
	if strings.Contains(text, "stranger") || !strings.Contains(text, "trusted legacy") || !strings.Contains(text, "allowed") {
		t.Fatalf("trust filtering: %s", text)
	}
	cfg := Config{}
	secret := "ghp_" + strings.Repeat("a", 36)
	if got := Publishable(cfg, "tool failed: "+secret); strings.Contains(got, secret) {
		t.Fatal("credential disclosed")
	}
	if !matches("one.test.ts", []string{"**/*.test.*"}) || matches("src/nested/file", []string{"src/*"}) {
		t.Fatal("recursive glob boundaries changed")
	}
}

func TestIsolatedReviewFixIsReverifiedAndBoundToSnapshot(t *testing.T) {
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("bwrap unavailable")
	}
	home, cfg, db := localInstance(t, `if test -f change.txt; then printf fixed > change.txt; else printf initial > change.txt; fi`, `test -f change.txt`)
	cfg.ReviewEnabled = true
	cfg.ReviewFixRounds = 1
	cfg.ReviewAgent = []string{"bash", "-c", `printf poison > change.txt; printf '%s' '{"standards":[{"severity":"blocking","where":"change.txt:1","finding":"Use the final value"}],"spec":[],"summary":"one defect","report_md":"Standards: one defect. Spec: otherwise met."}' > .fb-review.json`}
	if _, err := db.Add(t.Context(), "repair the value"); err != nil {
		t.Fatal(err)
	}
	report, err := Work(t.Context(), home, cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != model.Verified {
		t.Fatalf("status %s", report.Status)
	}
	data, err := os.ReadFile(filepath.Join(report.Worktree, "change.txt"))
	if err != nil || string(data) != "fixed" {
		t.Fatalf("isolated/fix result %s %v", data, err)
	}
	manifest, err := VerifiedSnapshot(t.Context(), home, cfg, report)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Review.Verdict != "fixed" {
		t.Fatalf("review %+v", manifest.Review)
	}
	if result, ok := manifest.Results["verify-fix"]; !ok || result.ExitCode != 0 {
		t.Fatal("final fix was not independently gated")
	}
	old, err := git(t.Context(), cfg.Repo, "show", manifest.Review.Snapshot+":change.txt")
	if err != nil || string(old) != "initial" {
		t.Fatalf("review snapshot %s %v", old, err)
	}
	if err := os.WriteFile(filepath.Join(report.Worktree, "change.txt"), []byte("after gate"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifiedSnapshot(t.Context(), home, cfg, report); err == nil {
		t.Fatal("post-verification mutation could publish")
	}
}

func TestRedCheckLeavesImplementerTreeAndRejectsWeakening(t *testing.T) {
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("bwrap unavailable")
	}
	home, cfg, db := localInstance(t, `printf fixed > value; mkdir -p tests; printf 'test "$(cat value)" = fixed\n' > tests/repro.sh`, `bash tests/repro.sh`)
	cfg.RedVerify = "bash {files}"
	cfg.TestGlobs = []string{"tests/**"}
	cfg.RedFileGlobs = []string{"tests/*.sh"}
	cfg.WeakeningPatterns = []string{`skip`}
	if _, err := db.Add(t.Context(), "fix value"); err != nil {
		t.Fatal(err)
	}
	report, err := Work(t.Context(), home, cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	m, err := VerifiedSnapshot(t.Context(), home, cfg, report)
	if err != nil {
		t.Fatal(err)
	}
	if result := m.Results["verify-red"]; result.ExitCode == 0 || result.TimedOut {
		t.Fatalf("red evidence %+v", result)
	}
	if _, err := os.Stat(report.Worktree + "-red"); !os.IsNotExist(err) {
		t.Fatalf("red checkout not removed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(report.Worktree, "tests", "repro.sh"), []byte("skip this test\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := git(t.Context(), report.Worktree, "add", "--all"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := testChanges(t.Context(), cfg, report.Worktree, m.Base); err == nil {
		t.Fatal("test weakening accepted")
	}
}

func TestHarnessGUISelectionAndReadOnlyOverride(t *testing.T) {
	cfg := Config{Harness: "default", GUIHarness: "gui", Commands: map[string]Command{"default": {Args: []string{"default", "{prompt}"}}, "gui": {Model: "gui-model", Args: []string{"gui", "{model}", "{prompt}"}}}}
	args, err := harnessCommand(cfg, model.Report{Surface: "gui_or_frontend"}, t.TempDir(), t.TempDir(), "inspect")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(args, "|") != "gui|gui-model|inspect" {
		t.Fatalf("GUI harness %v", args)
	}
	cfg.Commands = nil
	cfg.Agent = []string{"codex", "--sandbox", "read-only", "{prompt}"}
	args, err = harnessCommand(cfg, model.Report{}, t.TempDir(), t.TempDir(), "inspect")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(args)
	if !strings.Contains(string(b), "read-only") {
		t.Fatal("read-only override lost")
	}
}

func TestUnconfirmedTriageCannotAuthorizeImplementation(t *testing.T) {
	for _, verified := range []string{"failed", "insufficient", "", "unknown"} {
		got := confirmedTriage(triageDecision{Decision: "ready-for-agent", Verified: verified, Brief: "## Agent Brief\nUnproven fix"}, "en")
		if got.Decision != "needs-info" || got.Brief != "" || got.Notes == "" || got.DowngradedFrom != "ready-for-agent" {
			t.Fatalf("accepted %q: %+v", verified, got)
		}
	}
	home, cfg, db := localInstance(t, "touch implementer-ran", "true")
	cfg.TriageEnabled = true
	cfg.AutoKinds = []string{"bug"}
	cfg.TriageAgent = []string{"bash", "-c", `printf '%s' '{"decision":"ready-for-agent","verified":"failed","category":"bug","brief":"Unconfirmed fix"}' > .fb-triage.json`}
	if _, err := db.AddReport(t.Context(), model.Report{Body: "unconfirmed report", Source: "github", Kind: "bug", Status: model.New}); err != nil {
		t.Fatal(err)
	}
	row, err := Work(t.Context(), home, cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != model.NeedsReporter || row.Brief != "" {
		t.Fatalf("unconfirmed triage proceeded: %+v", row)
	}
	if _, err := os.Stat(filepath.Join(row.Worktree, "implementer-ran")); !os.IsNotExist(err) {
		t.Fatal("implementer ran before confirmation")
	}
}

func TestMaintainerApprovalSurvivesFeatureRetriage(t *testing.T) {
	for _, approved := range []bool{false, true} {
		t.Run(map[bool]string{false: "unapproved", true: "approved"}[approved], func(t *testing.T) {
			home, cfg, db := localInstance(t, "true", "true")
			cfg.TriageEnabled = true
			cfg.AutoKinds = []string{"bug"}
			cfg.TriageAgent = []string{"bash", "-c", `printf '%s' '{"decision":"ready-for-agent","verified":"confirmed","category":"enhancement","brief":"## Agent Brief\nConfirmed enhancement scope"}' > .fb-triage.json`}
			row := model.Report{Body: "feature request", Source: "github", Kind: "feature_request", Status: model.New}
			if approved {
				row.DecidedBy = "maintainer"
			}
			if _, err := db.AddReport(t.Context(), row); err != nil {
				t.Fatal(err)
			}
			result, err := Work(t.Context(), home, cfg, db)
			if err != nil {
				t.Fatal(err)
			}
			want := model.AwaitingApproval
			if approved {
				want = model.NoChange
			}
			if result.Status != want {
				t.Fatalf("approved=%t result=%+v", approved, result)
			}
		})
	}
}

func TestRetryTaskRetainsFailureAndEvidenceWithoutCredentials(t *testing.T) {
	for _, secret := range []bool{false, true} {
		t.Run(map[bool]string{false: "reason", true: "secret"}[secret], func(t *testing.T) {
			home, cfg, db := localInstance(t, "exit 7", "true")
			if _, err := db.Add(t.Context(), "repair"); err != nil {
				t.Fatal(err)
			}
			failed, err := Work(t.Context(), home, cfg, db)
			if err == nil || failed.Status != model.AgentFailed {
				t.Fatalf("initial failure %+v %v", failed, err)
			}
			credential := "ghp_" + strings.Repeat("z", 36)
			if secret {
				if _, err := db.Patch(t.Context(), failed.ID, map[string]any{"error": "tool failed with " + credential}); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Retry(t.Context(), failed.ID); err != nil {
				t.Fatal(err)
			}
			cfg.Agent = []string{"bash", "-c", "true"}
			next, err := Work(t.Context(), home, cfg, db)
			if err != nil {
				t.Fatal(err)
			}
			task, err := os.ReadFile(filepath.Join(AttemptDir(home, next), "TASK.md"))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(task), "Previous attempt failed") || !strings.Contains(string(task), filepath.Join(AttemptDir(home, failed), "manifest.json")) {
				t.Fatalf("retry lost context: %s", task)
			}
			if secret {
				if strings.Contains(string(task), credential) || !strings.Contains(string(task), "withheld") {
					t.Fatalf("unsafe retry context: %s", task)
				}
			} else if !strings.Contains(string(task), "agent exited 7") {
				t.Fatalf("failure reason missing: %s", task)
			}
		})
	}
}
