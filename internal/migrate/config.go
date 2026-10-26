package migrate

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/FaintGhost/loom/internal/worker"
	"github.com/pelletier/go-toml/v2"
)

// LoadConfig converts a Python instance configuration without invoking its runtime.
// Unknown keys and unsafe sandbox modes fail closed, with the precise key named.
// Publication and retrospective execution remain opt-in after migration validation.
func LoadConfig(path string) (worker.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return worker.Config{}, err
	}
	return ParseConfig(data)
}

func ParseConfig(data []byte) (worker.Config, error) {
	var raw map[string]any
	if err := toml.Unmarshal(data, &raw); err != nil {
		return worker.Config{}, err
	}
	c := worker.Config{
		Base: "origin/main", Toolchain: "vp", TimeoutSeconds: 1500, SetupTimeoutSeconds: 900, VerifyTimeoutSeconds: 900, PollSeconds: 120, MaxAttempts: 2,
		ReadyLabel: "ready-for-agent", ApprovedLabel: "ready-for-agent", NeedsHumanLabel: "ready-for-human",
		Hidden:      []string{"/root/.hermes/secrets", "/root/.config/gh", "/root/.codex", "/root/.ssh", "/root/.hermes/fb"},
		SecretFiles: []string{"/root/.hermes/secrets", "/root/.config/gh/hosts.yml", "/root/.codex/auth.json"},
		TestGlobs:   []string{"**/*.test.*", "**/*.spec.*", "**/__tests__/**", "tests/**"}, RedFileGlobs: []string{"**/*.test.*", "**/*.spec.*"},
		WeakeningPatterns: []string{`\.(skip|only|todo)\(`, `\bx(it|describe|test)\(`, `\b(it|test|describe)\.skip\b`},
		GuardWeakening:    true, TriageEnabled: true, TriageTimeoutSeconds: 480, CategoryLabels: map[string]string{"bug": "bug", "feature_request": "enhancement"},
		ReviewEnabled: true, ReviewTimeoutSeconds: 600, ReviewFixRounds: 1, MaxReviewRounds: 3, SafeMaxLines: 400,
		Harness: "codex", GUIHarness: "codex", HarnessMode: "herdr", Commands: map[string]worker.Command{},
		MaxPerUserPerDay: 5, MaxQueue: 20, MaxReopens: 3, DedupeEnabled: true, DedupeMinConfidence: 0.8, DedupeCandidates: 15,
		AutoKinds: []string{"bug"}, MaxAutoWork: 1.6, AttachmentMaxFiles: 6, AttachmentMaxBytes: 8000000,
		AttachmentContentTypes: []string{"image/png", "image/jpeg", "image/webp", "image/gif", "text/plain", "application/json", "application/zip"},
		QuestionTimeoutSeconds: 480, Releases: true, Retro: worker.RetroConfig{TimeoutSeconds: 1200, Hours: 24},
		SandboxEnv: map[string]string{}, HarnessEnv: map[string]string{},
	}
	var conversion error
	take := func(path string, dst any) {
		if conversion != nil {
			return
		}
		parts := strings.Split(path, ".")
		table := raw
		for _, part := range parts[:len(parts)-1] {
			value, exists := table[part]
			if !exists {
				return
			}
			next, ok := value.(map[string]any)
			if !ok {
				conversion = fmt.Errorf("config %s must be a table", part)
				return
			}
			table = next
		}
		key := parts[len(parts)-1]
		value, exists := table[key]
		if !exists {
			return
		}
		encoded, err := json.Marshal(value)
		if err == nil {
			err = json.Unmarshal(encoded, dst)
		}
		if err != nil {
			conversion = fmt.Errorf("config %s: %w", path, err)
			return
		}
		delete(table, key)
	}
	take("repo.toolchain", &c.Toolchain)
	common := []string{".github/**", "AGENTS.md", "CLAUDE.md", "LESSONS.md", ".gitignore", ".fb-attachments/**", ".fb-pr.md", ".fb-triage.json", ".fb-review.json"}
	switch c.Toolchain {
	case "vp":
		c.Setup = "vp install"
		c.Verify = "vp check && vp run typecheck && vp test run && vp run build"
		c.GuardPaths = append(common, "package.json", "pnpm-lock.yaml", "package-lock.json", "yarn.lock", ".npmrc")
	case "pnpm":
		c.Setup = "pnpm install --frozen-lockfile"
		c.Verify = "pnpm run --if-present typecheck && pnpm test && pnpm run --if-present build"
		c.RedVerify = "pnpm exec vitest run {files}"
		c.GuardPaths = append(common, "package.json", "pnpm-lock.yaml", ".npmrc")
	case "npm":
		c.Setup = "npm ci"
		c.Verify = "npm run --if-present typecheck && npm test && npm run --if-present build"
		c.RedVerify = "npx vitest run {files}"
		c.GuardPaths = append(common, "package.json", "package-lock.json", ".npmrc")
	case "go":
		c.Setup = "go mod download"
		c.Verify = "go vet ./... && go build ./... && go test ./..."
		c.RedVerify = "go test {packages}"
		c.TestGlobs = []string{"**/*_test.go", "**/testdata/**"}
		c.RedFileGlobs = []string{"**/*_test.go"}
		c.WeakeningPatterns = []string{`\bt\.Skip(Now|f)?\(`, `\bt\.SkipNow\(\)`}
		c.GuardPaths = append(common, "go.mod", "go.sum", "vendor/**")
		c.SandboxTmpfs = []string{"$GOCACHE"}
		c.SandboxEnv = map[string]string{"GOFLAGS": "-mod=readonly", "GOPROXY": "off"}
		c.HarnessEnv = map[string]string{"GOFLAGS": "-mod=mod", "GOCACHE": "/tmp/fb-gocache"}
	default:
		return c, fmt.Errorf("unknown repo.toolchain %q", c.Toolchain)
	}
	setupMinutes, verifyMinutes, harnessMinutes, triageMinutes, reviewMinutes, retroMinutes := 15, 15, 25, 8, 10, 20
	sandboxMode := "bwrap"
	sandboxEnv, harnessEnv := map[string]string{}, map[string]string{}
	for _, binding := range []struct {
		key    string
		target any
	}{
		{"instance.name", &c.InstanceName}, {"repo.slug", &c.GitHubRepo}, {"repo.clone", &c.Repo}, {"repo.base", &c.Base}, {"repo.setup", &c.Setup}, {"repo.verify", &c.Verify},
		{"repo.setup_minutes", &setupMinutes}, {"repo.verify_minutes", &verifyMinutes}, {"repo.merge_poll_seconds", &c.PollSeconds}, {"repo.releases", &c.Releases},
		{"magpie.base", &c.MagpieURL}, {"magpie.key_file", &c.MagpieKeyFile}, {"magpie.title_model", &c.TitleModel}, {"models.decider", &c.DeciderModel},
		{"gate.red_cmd", &c.RedVerify}, {"gate.test_globs", &c.TestGlobs}, {"gate.red_file_globs", &c.RedFileGlobs}, {"gate.weakening_patterns", &c.WeakeningPatterns},
		{"sandbox.verify", &sandboxMode}, {"sandbox.network", &c.SandboxNetwork}, {"sandbox.hide", &c.Hidden}, {"sandbox.tmpfs", &c.SandboxTmpfs}, {"sandbox.env", &sandboxEnv},
		{"guard.deny_paths", &c.GuardPaths}, {"guard.secret_files", &c.SecretFiles}, {"guard.secret_patterns", &c.SecretPatterns},
		{"attachments.max_files", &c.AttachmentMaxFiles}, {"attachments.max_bytes", &c.AttachmentMaxBytes}, {"attachments.content_types", &c.AttachmentContentTypes},
		{"intake.allowed_users", &c.AllowedUsers}, {"intake.max_per_user_per_day", &c.MaxPerUserPerDay}, {"intake.max_queue", &c.MaxQueue}, {"intake.max_reopens", &c.MaxReopens},
		{"dedupe.enabled", &c.DedupeEnabled}, {"dedupe.min_confidence", &c.DedupeMinConfidence}, {"dedupe.candidates", &c.DedupeCandidates},
		{"policy.auto_kinds", &c.AutoKinds}, {"policy.max_auto_work", &c.MaxAutoWork}, {"policy.max_diff_lines", &c.SafeMaxLines},
		{"triage.category_labels", &c.CategoryLabels}, {"triage.enabled", &c.TriageEnabled}, {"triage.timeout_minutes", &triageMinutes},
		{"review.max_rounds", &c.MaxReviewRounds}, {"reviewer.enabled", &c.ReviewEnabled}, {"reviewer.harness", &c.ReviewerHarness}, {"reviewer.timeout_minutes", &reviewMinutes}, {"reviewer.fix_rounds", &c.ReviewFixRounds},
		{"retro.harness", &c.Retro.Harness}, {"retro.timeout_minutes", &retroMinutes},
		{"discord.token_file", &c.DiscordTokenFile}, {"discord.guild", &c.DiscordGuild}, {"discord.channel", &c.DiscordChannel},
		{"harness.mode", &c.HarnessMode}, {"harness.default", &c.Harness}, {"harness.gui", &c.GUIHarness}, {"harness.timeout_minutes", &harnessMinutes}, {"harness.env", &harnessEnv},
	} {
		take(binding.key, binding.target)
	}
	for key, value := range sandboxEnv {
		c.SandboxEnv[key] = value
	}
	for key, value := range harnessEnv {
		c.HarnessEnv[key] = value
	}
	if harnesses, ok := raw["harness"].(map[string]any); ok {
		names := make([]string, 0, len(harnesses))
		for name := range harnesses {
			names = append(names, name)
		}
		for _, name := range names {
			if strings.Contains(name, ".") {
				return c, fmt.Errorf("invalid harness name %q", name)
			}
			var cmd worker.Command
			for _, binding := range []struct {
				key    string
				target any
			}{{"kind", &cmd.Kind}, {"model", &cmd.Model}, {"cmd", &cmd.Args}, {"image_args", &cmd.ImageArgs}, {"readonly_args", &cmd.ReadOnlyArgs}, {"review_args", &cmd.ReviewArgs}} {
				take("harness."+name+"."+binding.key, binding.target)
			}
			if len(cmd.Args) == 0 {
				return c, fmt.Errorf("harness.%s.cmd is required", name)
			}
			c.Commands[name] = cmd
		}
	}
	if conversion != nil {
		return c, conversion
	}
	if unknown := unknownKeys(raw, ""); len(unknown) > 0 {
		slices.Sort(unknown)
		return c, fmt.Errorf("unknown legacy config keys: %s", strings.Join(unknown, ", "))
	}
	if sandboxMode != "bwrap" || c.SandboxNetwork {
		return c, errors.New("migration requires sandbox.verify=bwrap and sandbox.network=false")
	}
	if c.HarnessMode != "headless" && c.HarnessMode != "herdr" {
		return c, fmt.Errorf("unknown harness.mode %q", c.HarnessMode)
	}
	command, ok := c.Commands[c.Harness]
	if !ok {
		return c, fmt.Errorf("harness.default refers to unknown slot %q", c.Harness)
	}
	c.Agent = command.Args
	for _, name := range []string{c.GUIHarness, c.ReviewerHarness, c.Retro.Harness} {
		if name != "" {
			if _, ok := c.Commands[name]; !ok {
				return c, fmt.Errorf("unknown harness slot %q", name)
			}
		}
	}
	for _, minutes := range []int{setupMinutes, verifyMinutes, harnessMinutes, triageMinutes, reviewMinutes, retroMinutes} {
		if minutes <= 0 || minutes > 1440 {
			return c, errors.New("config timeout_minutes must be between 1 and 1440")
		}
	}
	c.SetupTimeoutSeconds, c.VerifyTimeoutSeconds, c.TimeoutSeconds = setupMinutes*60, verifyMinutes*60, harnessMinutes*60
	c.TriageTimeoutSeconds, c.ReviewTimeoutSeconds, c.Retro.TimeoutSeconds = triageMinutes*60, reviewMinutes*60, retroMinutes*60
	c.ReviewFixRounds = max(0, min(c.ReviewFixRounds, 1))
	return c, nil
}

func unknownKeys(table map[string]any, prefix string) []string {
	var keys []string
	for key, value := range table {
		path := prefix + key
		if nested, ok := value.(map[string]any); ok {
			keys = append(keys, unknownKeys(nested, path+".")...)
		} else {
			keys = append(keys, path)
		}
	}
	return keys
}
