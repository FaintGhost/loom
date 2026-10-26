package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Config describes one local instance. Commands are operator-controlled; report text is never a shell command.
type Config struct {
	Repo                   string             `json:"repo"`
	Base                   string             `json:"base"`
	Agent                  []string           `json:"agent"`
	Setup                  string             `json:"setup,omitempty"`
	Verify                 string             `json:"verify"`
	TimeoutSeconds         int                `json:"timeout_seconds"`
	Hidden                 []string           `json:"hidden,omitempty"`
	GitHubRepo             string             `json:"github_repo,omitempty"`
	InstanceName           string             `json:"instance_name,omitempty"`
	Toolchain              string             `json:"toolchain,omitempty"`
	Publish                bool               `json:"publish"`
	ReadyLabel             string             `json:"ready_label,omitempty"`
	ApprovedLabel          string             `json:"approved_label,omitempty"`
	NeedsHumanLabel        string             `json:"needs_human_label,omitempty"`
	SetupTimeoutSeconds    int                `json:"setup_timeout_seconds,omitzero"`
	VerifyTimeoutSeconds   int                `json:"verify_timeout_seconds,omitzero"`
	PollSeconds            int                `json:"poll_seconds,omitzero"`
	MaxAttempts            int                `json:"max_attempts,omitzero"`
	RedVerify              string             `json:"red_verify,omitempty"`
	TestGlobs              []string           `json:"test_globs,omitempty"`
	RedFileGlobs           []string           `json:"red_file_globs,omitempty"`
	WeakeningPatterns      []string           `json:"weakening_patterns,omitempty"`
	GuardPaths             []string           `json:"guard_paths,omitempty"`
	SecretFiles            []string           `json:"secret_files,omitempty"`
	SecretPatterns         []string           `json:"secret_patterns,omitempty"`
	GuardWeakening         bool               `json:"guard_weakening"`
	SandboxNetwork         bool               `json:"sandbox_network"`
	SandboxEnv             map[string]string  `json:"sandbox_env,omitempty"`
	SandboxTmpfs           []string           `json:"sandbox_tmpfs,omitempty"`
	HarnessEnv             map[string]string  `json:"harness_env,omitempty"`
	Harness                string             `json:"harness,omitempty"`
	GUIHarness             string             `json:"gui_harness,omitempty"`
	ReviewerHarness        string             `json:"reviewer_harness,omitempty"`
	HarnessMode            string             `json:"harness_mode,omitempty"`
	CodexHome              string             `json:"codex_home,omitempty"`
	Commands               map[string]Command `json:"commands,omitempty"`
	TriageEnabled          bool               `json:"triage_enabled"`
	TriageAgent            []string           `json:"triage_agent,omitempty"`
	TriageTimeoutSeconds   int                `json:"triage_timeout_seconds,omitzero"`
	CategoryLabels         map[string]string  `json:"category_labels,omitempty"`
	ReviewEnabled          bool               `json:"review_enabled"`
	ReviewAgent            []string           `json:"review_agent,omitempty"`
	ReviewTimeoutSeconds   int                `json:"review_timeout_seconds,omitzero"`
	ReviewFixRounds        int                `json:"review_fix_rounds,omitzero"`
	MaxReviewRounds        int                `json:"max_review_rounds,omitzero"`
	SafeMaxFiles           int                `json:"safe_max_files,omitzero"`
	SafeMaxLines           int                `json:"safe_max_lines,omitzero"`
	DiscordTokenFile       string             `json:"discord_token_file,omitempty"`
	DiscordTokenEnv        string             `json:"discord_token_env,omitempty"`
	DiscordGuild           string             `json:"discord_guild,omitempty"`
	DiscordChannel         string             `json:"discord_channel,omitempty"`
	DiscordChannels        []string           `json:"discord_channels,omitempty"`
	AllowedUsers           []string           `json:"allowed_users,omitempty"`
	MaxPerUserPerDay       int                `json:"max_per_user_per_day,omitzero"`
	MaxPerUserPerHour      int                `json:"max_per_user_per_hour,omitzero"`
	MaxPerHour             int                `json:"max_per_hour,omitzero"`
	MaxQueue               int                `json:"max_queue,omitzero"`
	MaxReopens             int                `json:"max_reopens,omitzero"`
	MagpieURL              string             `json:"magpie_url,omitempty"`
	MagpieKeyFile          string             `json:"magpie_key_file,omitempty"`
	MagpieKeyEnv           string             `json:"magpie_key_env,omitempty"`
	DeciderModel           string             `json:"decider_model,omitempty"`
	TitleModel             string             `json:"title_model,omitempty"`
	DedupeEnabled          bool               `json:"dedupe_enabled"`
	DedupeMinConfidence    float64            `json:"dedupe_min_confidence,omitzero"`
	DedupeCandidates       int                `json:"dedupe_candidates,omitzero"`
	AutoKinds              []string           `json:"auto_kinds,omitempty"`
	MaxAutoWork            float64            `json:"max_auto_work,omitzero"`
	AttachmentMaxFiles     int                `json:"attachment_max_files,omitzero"`
	AttachmentMaxBytes     int64              `json:"attachment_max_bytes,omitzero"`
	AttachmentContentTypes []string           `json:"attachment_content_types,omitempty"`
	QuestionAgent          []string           `json:"question_agent,omitempty"`
	QuestionTimeoutSeconds int                `json:"question_timeout_seconds,omitzero"`
	Releases               bool               `json:"releases"`
	Retro                  RetroConfig        `json:"retro"`
}

type Command struct {
	Kind         string   `json:"kind,omitempty"`
	Model        string   `json:"model,omitempty"`
	Args         []string `json:"args"`
	ImageArgs    []string `json:"image_args,omitempty"`
	ReadOnlyArgs []string `json:"read_only_args,omitempty"`
	ReviewArgs   []string `json:"review_args,omitempty"`
}

type RetroConfig struct {
	Enabled             bool       `json:"enabled"`
	Command             []string   `json:"command,omitempty"`
	Harness             string     `json:"harness,omitempty"`
	TimeoutSeconds      int        `json:"timeout_seconds,omitzero"`
	Hours               int        `json:"hours,omitzero"`
	AutomationHome      string     `json:"automation_home,omitempty"`
	EffectgoHome        string     `json:"effectgo_home,omitempty"`
	CompatibilityModule string     `json:"compatibility_module,omitempty"`
	EffectgoBinary      string     `json:"effectgo_binary,omitempty"`
	EffectgoSHA256      string     `json:"effectgo_sha256,omitempty"`
	EffectgoRevision    string     `json:"effectgo_revision,omitempty"`
	RegressionCommands  [][]string `json:"regression_commands,omitempty"`
}

func (c Config) Validate() error {
	if !filepath.IsAbs(c.Repo) || filepath.Clean(c.Repo) != c.Repo {
		return errors.New("repo must be an absolute, clean path to a local clone")
	}
	info, err := os.Stat(filepath.Join(c.Repo, ".git"))
	if err != nil || !info.IsDir() {
		return errors.New("repo must have a .git directory; use a dedicated clone, not a linked worktree")
	}
	if c.Base == "" || strings.HasPrefix(c.Base, "-") || len(c.Agent) == 0 || c.Agent[0] == "" || strings.TrimSpace(c.Verify) == "" {
		return errors.New("base, agent argv and verify command are required")
	}
	if c.TimeoutSeconds <= 0 || c.TimeoutSeconds > 86400 {
		return errors.New("timeout_seconds must be between 1 and 86400")
	}
	for _, path := range c.Hidden {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("hidden path must be absolute and clean: %q", path)
		}
	}
	for name, seconds := range map[string]int{"setup_timeout_seconds": c.SetupTimeoutSeconds, "verify_timeout_seconds": c.VerifyTimeoutSeconds, "triage_timeout_seconds": c.TriageTimeoutSeconds, "review_timeout_seconds": c.ReviewTimeoutSeconds, "question_timeout_seconds": c.QuestionTimeoutSeconds, "retro.timeout_seconds": c.Retro.TimeoutSeconds} {
		if seconds < 0 || seconds > 86400 {
			return fmt.Errorf("%s must be between 0 and 86400", name)
		}
	}
	if c.Publish && (strings.Count(c.GitHubRepo, "/") != 1 || strings.ContainsAny(c.GitHubRepo, " \t\r\n") || strings.HasPrefix(c.GitHubRepo, "-") || strings.HasPrefix(c.GitHubRepo, "/") || strings.HasSuffix(c.GitHubRepo, "/")) {
		return errors.New("publish requires an owner/repository github_repo")
	}
	if c.SandboxNetwork {
		return errors.New("sandbox network access is not permitted")
	}
	if c.HarnessMode != "" && c.HarnessMode != "headless" && c.HarnessMode != "herdr" {
		return fmt.Errorf("unknown harness_mode %q", c.HarnessMode)
	}
	if c.ReviewFixRounds < 0 || c.ReviewFixRounds > 1 {
		return errors.New("review_fix_rounds must be 0 or 1")
	}
	if c.DedupeMinConfidence < 0 || c.DedupeMinConfidence > 1 {
		return errors.New("dedupe_min_confidence must be between 0 and 1")
	}
	for _, n := range []int{c.PollSeconds, c.MaxAttempts, c.MaxReviewRounds, c.SafeMaxFiles, c.SafeMaxLines, c.MaxPerUserPerDay, c.MaxPerUserPerHour, c.MaxPerHour, c.MaxQueue, c.MaxReopens, c.DedupeCandidates, c.AttachmentMaxFiles} {
		if n < 0 {
			return errors.New("configuration limits must not be negative")
		}
	}
	if c.AttachmentMaxBytes < 0 || c.MaxAutoWork < 0 {
		return errors.New("configuration limits must not be negative")
	}
	for _, patterns := range [][]string{c.WeakeningPatterns, c.SecretPatterns} {
		for _, pattern := range patterns {
			if _, err := regexp.Compile(pattern); err != nil {
				return fmt.Errorf("invalid configured pattern %q: %w", pattern, err)
			}
		}
	}
	for name, command := range c.Commands {
		if name == "" || len(command.Args) == 0 || command.Args[0] == "" {
			return fmt.Errorf("command %q requires executable argv", name)
		}
	}
	for _, environment := range []map[string]string{c.SandboxEnv, c.HarnessEnv} {
		for key, value := range environment {
			if key == "" || strings.ContainsAny(key, "=\x00\r\n") || strings.ContainsRune(value, '\x00') {
				return fmt.Errorf("invalid environment entry %q", key)
			}
		}
	}
	return nil
}

func CheckHome(home string) error {
	if !filepath.IsAbs(home) || filepath.Clean(home) != home || home == "/" {
		return errors.New("--home must be an absolute, clean instance directory")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(home))
	if err != nil {
		return fmt.Errorf("home parent: %w", err)
	}
	if parent != filepath.Dir(home) {
		return errors.New("--home must use its canonical path, without symlinked ancestors")
	}
	if info, err := os.Lstat(home); err == nil {
		if !info.IsDir() {
			return errors.New("--home must be a real directory, not a symlink")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, name := range []string{"config.toml", "store.sqlite"} {
		if _, err := os.Lstat(filepath.Join(home, name)); err == nil {
			return errors.New("refusing a Python instance home; choose a new Go home")
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func Init(home string, cfg Config) error {
	if err := CheckHome(home); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	// An existing directory may hold another instance's evidence. Never adopt or overwrite it.
	if err := os.Mkdir(home, 0700); err != nil {
		return fmt.Errorf("create fresh home: %w", err)
	}
	return writeJSON(filepath.Join(home, "config.json"), cfg, true)
}

func Load(home string) (Config, error) {
	var cfg Config
	if err := CheckHome(home); err != nil {
		return cfg, err
	}
	f, err := os.Open(filepath.Join(home, "config.json"))
	if err != nil {
		return cfg, err
	}
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&cfg)
	if err == nil {
		var extra any
		if next := decoder.Decode(&extra); !errors.Is(next, io.EOF) {
			err = errors.New("config.json must contain exactly one JSON object")
		}
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}

func writeJSON(path string, value any, exclusive bool) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if exclusive {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, err = f.Write(data)
		return errors.Join(err, f.Sync(), f.Close())
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".manifest-*")
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	err = errors.Join(err, f.Sync(), f.Close())
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		return errors.Join(err, os.Remove(f.Name()))
	}
	return nil
}
