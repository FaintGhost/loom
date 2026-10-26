package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/FaintGhost/loom/internal/improvement"
	"github.com/FaintGhost/loom/internal/intake"
	"github.com/FaintGhost/loom/internal/migrate"
	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/pipeline"
	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdin, os.Stdout)
	stop()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, input io.Reader, output io.Writer) (retErr error) {
	flags := flag.NewFlagSet("loom", flag.ContinueOnError)
	flags.SetOutput(output)
	home := flags.String("home", os.Getenv("FB_HOME"), "absolute instance home (or FB_HOME)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	args = flags.Args()
	if len(args) == 0 {
		return errors.New("usage: loom --home PATH init|import|inject|list|show|approve|promote|retry|resume|notify|replay|work|worker|bot|retro|compat|stats|gc; loom instances")
	}
	encode := func(value any) error { return json.NewEncoder(output).Encode(value) }
	if args[0] == "instances" {
		return listInstances(args[1:], output)
	}
	if err := worker.CheckHome(*home); err != nil {
		return err
	}
	if args[0] == "import" {
		f := flag.NewFlagSet("import", flag.ContinueOnError)
		f.SetOutput(output)
		source := f.String("source", "", "stopped Python instance home; read-only source")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if *source == "" || f.NArg() != 0 {
			return errors.New("usage: loom --home NEW_HOME import --source PYTHON_HOME")
		}
		result, err := migrate.Import(ctx, *source, *home)
		if err != nil {
			return err
		}
		return encode(result)
	}
	if args[0] == "init" {
		return initInstance(*home, args[1:], output)
	}
	cfg, err := worker.Load(*home)
	if err != nil {
		return err
	}
	db, err := store.Open(filepath.Join(*home, "go-store.sqlite"))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()
	switch args[0] {
	case "inject":
		return inject(ctx, *home, cfg, db, args[1:], input, output)
	case "list", "stats":
		if len(args) != 1 {
			return errors.New("list and stats accept no positional arguments")
		}
		reports, err := db.List(ctx)
		if err != nil {
			return err
		}
		if args[0] == "list" {
			return encode(reports)
		}
		counts := make(map[model.Status]int)
		for _, report := range reports {
			counts[report.Status]++
		}
		return encode(struct {
			Total    int                  `json:"total"`
			Statuses map[model.Status]int `json:"statuses"`
		}{len(reports), counts})
	case "show", "retry", "approve", "promote", "resume", "notify":
		if len(args) != 2 {
			return fmt.Errorf("%s requires one report ID", args[0])
		}
		id, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || id <= 0 {
			return errors.New("report ID must be a positive integer")
		}
		if args[0] == "notify" {
			return pipeline.Notify(ctx, *home, cfg, db, id)
		}
		if args[0] == "resume" {
			var report model.Report
			err := lockFile(filepath.Join(*home, "pipeline.lock"), func() error {
				var err error
				report, err = worker.Resume(ctx, *home, cfg, db, id)
				return err
			})
			if report.ID != 0 {
				err = errors.Join(err, encode(report))
			}
			return err
		}
		if args[0] == "promote" {
			result, err := intake.Promote(ctx, *home, cfg, db, id)
			if err != nil {
				return err
			}
			return encode(result)
		}
		report, err := db.Get(ctx, id)
		if err != nil {
			return err
		}
		switch args[0] {
		case "retry":
			if err := db.Retry(ctx, id); err != nil {
				return err
			}
			report, err = db.Get(ctx, id)
		case "approve":
			if report.Status != model.Status("awaiting_approval") {
				return errors.New("only an awaiting_approval report can be approved; use promote for held intake")
			}
			report, err = db.Transition(ctx, id, report.Status, model.New, map[string]any{"decided_by": "maintainer"})
		}
		if err != nil {
			return err
		}
		return encode(report)
	case "replay":
		if len(args) != 2 {
			return errors.New("usage: replay INTAKE_KEY")
		}
		result, err := intake.Replay(ctx, *home, cfg, db, args[1])
		if err != nil {
			return err
		}
		return encode(result)
	case "work":
		if len(args) > 2 || (len(args) == 2 && args[1] != "--once") {
			return errors.New("usage: work [--once]")
		}
		if cfg.Publish {
			return pipeline.Tick(ctx, *home, cfg, db)
		}
		report, err := worker.Work(ctx, *home, cfg, db)
		if errors.Is(err, model.ErrEmptyQueue) {
			return encode(map[string]string{"status": "empty"})
		}
		if report.ID != 0 {
			err = errors.Join(err, encode(report))
		}
		return err
	case "worker", "bot", "retro", "compat", "gc":
		if args[0] == "gc" {
			return collectWorktrees(ctx, *home, cfg, db, args[1:], output)
		}
		if len(args) != 1 {
			return fmt.Errorf("%s accepts no positional arguments", args[0])
		}
		return serviceLock(*home, args[0], func() error {
			switch args[0] {
			case "bot":
				return intake.Run(ctx, *home, cfg, db)
			case "retro":
				return improvement.Run(ctx, *home, cfg, db)
			case "compat":
				return runCompatibility(ctx, *home, cfg, output)
			default:
				return runWorker(ctx, *home, cfg, db)
			}
		})
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func initInstance(home string, args []string, output io.Writer) error {
	f := flag.NewFlagSet("init", flag.ContinueOnError)
	f.SetOutput(output)
	repo := f.String("repo", "", "absolute dedicated Git clone")
	base := f.String("base", "HEAD", "base revision")
	agent := f.String("agent", "", "JSON argv array; {task}, {prompt}, {last} supported")
	verify := f.String("verify", "", "sandbox verification command")
	setup := f.String("setup", "", "trusted dependency setup command")
	timeout := f.Int("timeout", 720, "subprocess timeout in seconds")
	config := f.String("config", "", "complete JSON configuration to initialize")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected init arguments")
	}
	cfg := worker.Config{Repo: *repo, Base: *base, Verify: *verify, Setup: *setup, TimeoutSeconds: *timeout}
	if *config != "" {
		data, err := os.ReadFile(*config)
		if err != nil {
			return err
		}
		decoder := json.NewDecoder(strings.NewReader(string(data)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cfg); err != nil {
			return err
		}
	} else if err := json.Unmarshal([]byte(*agent), &cfg.Agent); err != nil {
		return fmt.Errorf("--agent must be a JSON argv array: %w", err)
	}
	if err := worker.Init(home, cfg); err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]string{"home": home, "repo": cfg.Repo})
}

func inject(ctx context.Context, home string, cfg worker.Config, db *store.Store, args []string, input io.Reader, output io.Writer) error {
	f := flag.NewFlagSet("inject", flag.ContinueOnError)
	f.SetOutput(output)
	key := f.String("key", "", "stable intake idempotency key")
	author := f.String("author", "operator", "report author")
	noDedupe := f.Bool("no-dedupe", false, "file a distinct report")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 1 {
		return errors.New("usage: inject [--key KEY] [--author AUTHOR] [--no-dedupe] TEXT (or - for stdin)")
	}
	body := f.Arg(0)
	if body == "-" {
		data, err := io.ReadAll(io.LimitReader(input, (1<<20)+1))
		if err != nil {
			return err
		}
		body = string(data)
	}
	if len(body) > 1<<20 {
		return errors.New("report exceeds 1 MiB")
	}
	if cfg.MagpieURL == "" && cfg.GitHubRepo == "" {
		report, err := db.Add(ctx, body)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(report)
	}
	if *key == "" {
		*key = "operator-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	result, err := intake.Accept(ctx, home, cfg, db, intake.Message{
		ID: *key, AuthorID: *author, AuthorName: *author, Content: body, NoDedupe: *noDedupe,
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
}

func serviceLock(home, name string, fn func() error) error {
	return lockFile(filepath.Join(home, name+".service.lock"), fn)
}

func runWorker(ctx context.Context, home string, cfg worker.Config, db *store.Store) error {
	interval := cfg.PollSeconds
	if interval <= 0 {
		interval = 15
	}
	fmt.Fprintf(os.Stderr, "%s worker up: repo %s home %s\n", time.Now().UTC().Format(time.RFC3339), cfg.GitHubRepo, home)
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()
	for {
		if err := pipeline.Tick(ctx, home, cfg, db); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "%s worker: %v\n", time.Now().UTC().Format(time.RFC3339), err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
