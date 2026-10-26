// Package pipeline connects durable intake, guarded execution, publication and delivery.
package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/FaintGhost/loom/internal/intake"
	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/remote"
	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
)

func lockFile(home, name string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(home, name), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, worker.ErrBusy
		}
		return nil, err
	}
	return f, nil
}

// Tick holds one instance lease throughout watch, work, publication and delivery.
// Every externally visible write is either naturally idempotent or reconciled by
// its durable marker; an uncertain create is never blindly repeated.
func Tick(ctx context.Context, home string, cfg worker.Config, db *store.Store) error {
	lock, err := lockFile(home, "pipeline.lock")
	if err != nil {
		return err
	}
	defer lock.Close()
	var errs []error
	attemptLimit := cfg.MaxAttempts
	if attemptLimit <= 0 {
		attemptLimit = 2
	}
	if cfg.Publish && cfg.GitHubRepo != "" {
		if err := watchReady(ctx, cfg, db); err != nil {
			errs = append(errs, err)
		}
		if err := watchPRs(ctx, home, cfg, db); err != nil {
			errs = append(errs, err)
		}
	}
	rows, err := db.List(ctx)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	queued := 0
	for _, r := range rows {
		if r.Status == model.New {
			queued++
		}
	}
	for _, r := range rows {
		if r.Status == model.Held && r.Error == "queue full" && r.IssueNumber > 0 && (cfg.MaxQueue == 0 || queued < cfg.MaxQueue) {
			if _, err := db.Transition(ctx, r.ID, r.Status, model.New, map[string]any{"error": ""}); err != nil {
				errs = append(errs, err)
			} else {
				queued++
			}
		}
		if r.Status == model.Interrupted {
			// Recover useful work in place; setup/triage interruptions get a fresh attempt.
			var manifest worker.Manifest
			data, e := os.ReadFile(filepath.Join(worker.AttemptDir(home, r), "manifest.json"))
			if e == nil && json.Unmarshal(data, &manifest) == nil && slices.Contains([]string{"agent", "verify", "review", "review-fix", "verify-fix", "verified"}, manifest.Phase) {
				_, e = worker.Resume(ctx, home, cfg, db, r.ID)
				if e != nil {
					errs = append(errs, e)
				}
			} else if e := retryInterrupted(ctx, cfg, db, r, attemptLimit); e != nil {
				errs = append(errs, e)
			}
		}
	}
	rows, err = db.List(ctx)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	canWork := true
	if cfg.Publish {
		for _, r := range rows {
			if r.Status != model.New && r.Status != model.Queued {
				continue
			}
			if r.IssueNumber == 0 {
				r, err = intake.EnsureIssue(ctx, cfg, db, r)
				if err != nil {
					errs = append(errs, err)
					canWork = false
					continue
				}
			}
			if r.PRNumber == 0 {
				if err := refreshBrief(ctx, cfg, db, r); err != nil {
					errs = append(errs, err)
					canWork = false
				}
			}
		}
	}
	// Refresh the operator-selected base before allocating a new checkout.
	if cfg.Publish && cfg.GitHubRepo != "" {
		if _, err := worker.Git(ctx, cfg.Repo, "-c", "credential.helper=!gh auth git-credential", "fetch", "origin", "--prune"); err != nil {
			return errors.Join(append(errs, err)...)
		}
	}
	var report model.Report
	var workErr error
	if canWork {
		report, workErr = worker.Work(ctx, home, cfg, db)
	}
	if workErr != nil && !errors.Is(workErr, model.ErrEmptyQueue) {
		errs = append(errs, workErr)
	}
	if report.ID != 0 {
		if err := db.RecordEvent(ctx, report.ID, "worker-result", string(report.Status)); err != nil {
			errs = append(errs, err)
		}
		if report.PRNumber == 0 && slices.Contains([]model.Status{model.SetupFailed, model.AgentFailed, model.VerifyFailed}, report.Status) && report.Attempt-report.RetryBase < attemptLimit {
			if err := db.Retry(ctx, report.ID); err != nil {
				errs = append(errs, err)
			}
		} else if cfg.Publish && report.PRNumber > 0 && report.Status != model.Verified && report.Status != model.Interrupted {
			// A failed round never replaces or closes its existing PR.
			if err := roundStopped(ctx, cfg, db, report); err != nil {
				errs = append(errs, err)
			}
		}
	}
	rows, err = db.List(ctx)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	for _, r := range rows {
		exhaustedPR := r.PRNumber > 0 && r.Status == model.NeedsHuman && r.Attempt-r.RetryBase >= attemptLimit
		if !exhaustedPR && r.PRNumber > 0 && r.PendingReview != "" && slices.Contains([]model.Status{model.NoChange, model.SetupFailed, model.AgentFailed, model.VerifyFailed, model.Failed, model.NeedsHuman, model.NeedsReporter}, r.Status) && cfg.Publish {
			if err := roundStopped(ctx, cfg, db, r); err != nil {
				errs = append(errs, err)
			}
		}
		if slices.Contains([]model.Status{model.Done, model.Merged, model.Closed}, r.Status) && r.Worktree != "" {
			if err := cleanup(ctx, home, cfg, db, r); err != nil {
				errs = append(errs, err)
			}
		}
		if r.Status == model.Verified && cfg.Publish {
			if err := publish(ctx, home, cfg, db, r); err != nil {
				errs = append(errs, err)
				continue
			}
		}
		if cfg.Publish {
			if err := Notify(ctx, home, cfg, db, r.ID); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

type issue struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	URL    string `json:"html_url"`
	State  string `json:"state"`
	Labels []struct {
		Name string `json:"name"`
	} `json:"labels"`
	PR   json.RawMessage `json:"pull_request"`
	User struct {
		Login string `json:"login"`
	} `json:"user"`
}
type comment struct {
	ID          int64  `json:"id"`
	Body        string `json:"body"`
	Association string `json:"author_association"`
	Created     string `json:"created_at"`
	Submitted   string `json:"submitted_at"`
	Path        string `json:"path"`
	Line        int    `json:"line"`
	User        struct {
		Login string `json:"login"`
	} `json:"user"`
}
type pull struct {
	Number   int    `json:"number"`
	URL      string `json:"html_url"`
	State    string `json:"state"`
	Merged   bool   `json:"merged"`
	MergeSHA string `json:"merge_commit_sha"`
	Head     struct {
		Ref  string `json:"ref"`
		SHA  string `json:"sha"`
		Repo struct {
			FullName string `json:"full_name"`
		} `json:"repo"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
	} `json:"base"`
	Labels []struct {
		Name string `json:"name"`
	} `json:"labels"`
}

func list[T any](ctx context.Context, endpoint string) ([]T, error) {
	sep := "?"
	if strings.Contains(endpoint, "?") {
		sep = "&"
	}
	data, err := remote.GH(ctx, "api", "--paginate", "--slurp", endpoint+sep+"per_page=100")
	if err != nil {
		return nil, err
	}
	var pages [][]T
	if err := json.Unmarshal(data, &pages); err != nil {
		return nil, err
	}
	var out []T
	for _, page := range pages {
		out = append(out, page...)
	}
	return out, nil
}
func endpoint(cfg worker.Config, suffix string) string {
	return "repos/" + cfg.GitHubRepo + "/" + suffix
}
func trusted(c comment) bool {
	return slices.Contains([]string{"OWNER", "MEMBER", "COLLABORATOR"}, c.Association)
}
func loopText(s string) bool {
	return strings.Contains(s, "<!-- fb-loop -->") || strings.Contains(s, "<!-- feedback-loop")
}
func brief(s string) bool {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "> *This was generated by AI during triage.*") {
		s = strings.TrimSpace(strings.TrimPrefix(s, "> *This was generated by AI during triage.*"))
	}
	return strings.HasPrefix(s, "## Agent Brief\n") || s == "## Agent Brief"
}
func latestBrief(comments []comment) *comment {
	var latest *comment
	for _, c := range comments {
		if trusted(c) && brief(c.Body) && (latest == nil || c.Created > latest.Created) {
			copy := c
			latest = &copy
		}
	}
	return latest
}
func refreshBrief(ctx context.Context, cfg worker.Config, db *store.Store, r model.Report) error {
	comments, err := list[comment](ctx, endpoint(cfg, fmt.Sprintf("issues/%d/comments", r.IssueNumber)))
	if err != nil {
		return err
	}
	if b := latestBrief(comments); b != nil {
		_, err = db.Patch(ctx, r.ID, map[string]any{"brief": b.Body, "brief_comment_id": strconv.FormatInt(b.ID, 10)})
		return err
	}
	if r.BriefCommentID != "" {
		_, err = db.Patch(ctx, r.ID, map[string]any{"brief": "", "brief_comment_id": ""})
		return err
	}
	return nil
}

func roles(ctx context.Context, cfg worker.Config) map[string]string {
	names := map[string]string{"needs-triage": "needs-triage", "needs-info": "needs-info", "ready-for-agent": cfg.ReadyLabel, "ready-for-human": cfg.NeedsHumanLabel, "wontfix": "wontfix"}
	if names["ready-for-agent"] == "" {
		names["ready-for-agent"] = "ready-for-agent"
	}
	if names["ready-for-human"] == "" {
		names["ready-for-human"] = "ready-for-human"
	}
	// Repository-owned label documentation remains authoritative after migration.
	data, err := worker.Git(ctx, cfg.Repo, "show", cfg.Base+":docs/agents/triage-labels.md")
	if err != nil {
		return names
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		cells := strings.Split(line, "|")
		if len(cells) < 4 {
			continue
		}
		role := strings.Trim(strings.TrimSpace(cells[1]), "`")
		label := strings.Trim(strings.TrimSpace(cells[2]), "`")
		if _, ok := names[role]; ok && label != "" {
			names[role] = label
		}
	}
	return names
}
func labelsHave(labels []struct {
	Name string `json:"name"`
}, name string) bool {
	for _, label := range labels {
		if label.Name == name {
			return true
		}
	}
	return false
}
func stateLabel(ctx context.Context, cfg worker.Config, number int, role string) error {
	if number == 0 {
		return nil
	}
	names := roles(ctx, cfg)
	data, err := remote.GH(ctx, "api", endpoint(cfg, fmt.Sprintf("issues/%d", number)))
	if err != nil {
		return err
	}
	var issue issue
	if err := json.Unmarshal(data, &issue); err != nil {
		return err
	}
	args := []string{"issue", "edit", strconv.Itoa(number), "--repo", cfg.GitHubRepo}
	for key, label := range names {
		if key != role && key != "wontfix" && labelsHave(issue.Labels, label) {
			args = append(args, "--remove-label", label)
		}
	}
	if role != "" && !labelsHave(issue.Labels, names[role]) {
		args = append(args, "--add-label", names[role])
	}
	if len(args) == 5 {
		return nil
	}
	_, err = remote.GH(ctx, args...)
	return err
}

func watchReady(ctx context.Context, cfg worker.Config, db *store.Store) error {
	names := roles(ctx, cfg)
	labels := []string{names["ready-for-agent"]}
	if cfg.ApprovedLabel != "" && cfg.ApprovedLabel != labels[0] {
		labels = append(labels, cfg.ApprovedLabel)
	}
	var errs []error
	for _, label := range labels {
		issues, err := list[issue](ctx, endpoint(cfg, "issues?state=open&labels="+url.QueryEscape(label)))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, item := range issues {
			if len(item.PR) > 0 || labelsHave(item.Labels, names["wontfix"]) {
				continue
			}
			row, err := db.ByIssue(ctx, item.Number)
			if errors.Is(err, model.ErrNotFound) {
				body := item.Body
				if strings.TrimSpace(body) == "" {
					body = item.Title
				}
				row, err = db.AddReport(ctx, model.Report{Body: body, Title: item.Title, Source: "github", SourceKey: fmt.Sprintf("github:%s:issue:%d", cfg.GitHubRepo, item.Number), Repository: cfg.GitHubRepo, Author: item.User.Login, IssueNumber: item.Number, IssueURL: item.URL, Status: model.New, Kind: "bug", DecidedBy: "maintainer"})
			} else if err == nil && slices.Contains([]model.Status{model.AwaitingApproval, model.Held, model.NeedsReporter, model.NeedsInfo, model.NeedsHuman, model.VerifyFailed, model.AgentFailed, model.NoChange, model.GuardBlocked, model.SetupFailed, model.Failed, model.Closed}, row.Status) {
				if row.PRNumber > 0 {
					if cfg.MaxReviewRounds > 0 && row.ReviewRounds >= cfg.MaxReviewRounds {
						errs = append(errs, stateLabel(ctx, cfg, item.Number, "ready-for-human"))
						continue
					}
					pr, e := getPR(ctx, cfg, row.PRNumber)
					if e != nil {
						errs = append(errs, e)
						continue
					}
					if pr.Merged {
						errs = append(errs, errors.New("cannot restart a merged PR"))
						continue
					}
					if pr.State == "closed" {
						if _, e := remote.GH(ctx, "pr", "reopen", strconv.Itoa(row.PRNumber), "--repo", cfg.GitHubRepo); e != nil {
							errs = append(errs, e)
							continue
						}
					}
					row, err = db.Patch(ctx, row.ID, map[string]any{"pending_review": "A maintainer marked the original issue ready again. Address its current authoritative brief and trusted reproduction context; preserve the existing PR's fix and tests.", "decided_by": "maintainer"})
					if err == nil {
						err = queueReview(ctx, cfg, db, row, pr)
					}
					if err == nil {
						row, err = db.Get(ctx, row.ID)
					}
				} else {
					row, err = db.Requeue(ctx, row.ID, row.Status, model.New, map[string]any{"error": "", "decided_by": "maintainer"})
				}
			} else if err == nil {
				continue
			}
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if err := refreshBrief(ctx, cfg, db, row); err != nil {
				errs = append(errs, err)
				continue
			}
			if err := stateLabel(ctx, cfg, item.Number, ""); err != nil {
				errs = append(errs, err)
			}
			if label == cfg.ApprovedLabel && label != names["ready-for-agent"] {
				if _, err := remote.GH(ctx, "issue", "edit", strconv.Itoa(item.Number), "--repo", cfg.GitHubRepo, "--remove-label", label); err != nil {
					errs = append(errs, err)
				}
			}
			if err := db.RecordEvent(ctx, row.ID, "approved", "maintainer readiness label"); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func getPR(ctx context.Context, cfg worker.Config, number int) (pull, error) {
	var pr pull
	data, err := remote.GH(ctx, "api", endpoint(cfg, fmt.Sprintf("pulls/%d", number)))
	if err != nil {
		return pr, err
	}
	err = json.Unmarshal(data, &pr)
	return pr, err
}
func watchPRs(ctx context.Context, home string, cfg worker.Config, db *store.Store) error {
	rows, err := db.List(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, r := range rows {
		if r.PRNumber == 0 || !slices.Contains([]model.Status{model.PROpen, model.ReviseRequested, model.ReviewPending, model.NeedsHuman}, r.Status) {
			continue
		}
		pr, err := getPR(ctx, cfg, r.PRNumber)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if pr.Merged || pr.State == "closed" {
			next := model.Closed
			fields := map[string]any{"pending_review": ""}
			if pr.Merged {
				next = model.Done
				fields["commit_sha"] = pr.MergeSHA
			}
			updated, err := db.Transition(ctx, r.ID, r.Status, next, fields)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if err := Notify(ctx, home, cfg, db, updated.ID); err != nil {
				errs = append(errs, err)
			}
			if r.Worktree != "" {
				if err := cleanup(ctx, home, cfg, db, r); err != nil {
					errs = append(errs, err)
				}
			}
			continue
		}
		if r.Status != model.PROpen {
			if r.Status == model.ReviseRequested || r.Status == model.ReviewPending {
				if err := queueReview(ctx, cfg, db, r, pr); err != nil {
					errs = append(errs, err)
				}
			}
			continue
		}
		names := roles(ctx, cfg)
		if !labelsHave(pr.Labels, names["ready-for-agent"]) || labelsHave(pr.Labels, names["wontfix"]) {
			if r.ReviewSeenAt == "" {
				_, err = db.Patch(ctx, r.ID, map[string]any{"review_seen_at": time.Now().UTC().Format(time.RFC3339)})
				if err != nil {
					errs = append(errs, err)
				}
			}
			continue
		}
		if cfg.MaxReviewRounds > 0 && r.ReviewRounds >= cfg.MaxReviewRounds {
			err := remote.IssueCommentOnce(ctx, cfg, db, r.ID, r.PRNumber, "This PR reached its configured review-round limit; a maintainer must decide the next change.", fmt.Sprintf("review-limit:%d:%d", r.ID, r.ReviewRounds))
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if err := stateLabel(ctx, cfg, r.PRNumber, "ready-for-human"); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		comments, err := list[comment](ctx, endpoint(cfg, fmt.Sprintf("issues/%d/comments", r.PRNumber)))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		all := slices.Clone(comments)
		for _, suffix := range []string{"comments", "reviews"} {
			items, e := list[comment](ctx, endpoint(cfg, fmt.Sprintf("pulls/%d/%s", r.PRNumber, suffix)))
			if e != nil {
				err = e
				break
			}
			all = append(all, items...)
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var points []comment
		watermark := r.ReviewSeenAt
		contract := ""
		if b := latestBrief(comments); b != nil && b.Created > r.ReviewSeenAt {
			contract = b.Body
			watermark = max(watermark, b.Created)
		}
		for _, c := range all {
			created := c.Created
			if created == "" {
				created = c.Submitted
			}
			if trusted(c) && !loopText(c.Body) && strings.TrimSpace(c.Body) != "" && created > r.ReviewSeenAt {
				points = append(points, c)
				watermark = max(watermark, created)
			}
		}
		if contract == "" && len(points) == 0 {
			if err := remote.IssueCommentOnce(ctx, cfg, db, r.ID, r.PRNumber, "Add a new Agent Brief or specific review feedback, then label this PR ready for the agent again.", fmt.Sprintf("review-empty:%d:%s", r.ID, r.ReviewSeenAt)); err != nil {
				errs = append(errs, err)
				continue
			}
			if err := stateLabel(ctx, cfg, r.PRNumber, "ready-for-human"); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		slices.SortFunc(points, func(a, b comment) int {
			return strings.Compare(max(a.Created, a.Submitted), max(b.Created, b.Submitted))
		})
		payload, _ := json.Marshal(map[string]any{"brief": contract, "feedback": points})
		r, err = db.Transition(ctx, r.ID, r.Status, model.ReviseRequested, map[string]any{"pending_review": string(payload), "review_seen_at": watermark})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := stateLabel(ctx, cfg, r.PRNumber, ""); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := queueReview(ctx, cfg, db, r, pr); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func fetchPRHead(ctx context.Context, cfg worker.Config, pr pull) error {
	if pr.Head.Repo.FullName != cfg.GitHubRepo || pr.Head.Ref == "" || pr.Head.SHA == "" {
		return errors.New("existing PR head does not belong to the configured repository")
	}
	if _, err := worker.Git(ctx, cfg.Repo, "-c", "credential.helper=!gh auth git-credential", "fetch", "origin", "refs/heads/"+pr.Head.Ref); err != nil {
		return err
	}
	sha, err := worker.Git(ctx, cfg.Repo, "rev-parse", "FETCH_HEAD")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(sha)) != pr.Head.SHA {
		return errors.New("PR head moved while fetching; retry next watch")
	}
	return nil
}

func queueReview(ctx context.Context, cfg worker.Config, db *store.Store, r model.Report, pr pull) error {
	if err := fetchPRHead(ctx, cfg, pr); err != nil {
		return err
	}
	_, err := db.StartReview(ctx, r.ID, r.Status, map[string]any{"branch": pr.Head.Ref, "commit_sha": pr.Head.SHA, "decided_by": "maintainer", "error": ""})
	return err
}

func retryInterrupted(ctx context.Context, cfg worker.Config, db *store.Store, r model.Report, limit int) error {
	if r.Attempt-r.RetryBase >= limit {
		_, err := db.Transition(ctx, r.ID, model.Interrupted, model.NeedsHuman, map[string]any{"error": "Interrupted work exhausted its automatic retry budget. A maintainer must decide the next step.\n" + r.Error})
		return err
	}
	if r.PRNumber == 0 {
		return db.Retry(ctx, r.ID)
	}
	pr, err := getPR(ctx, cfg, r.PRNumber)
	if err != nil {
		return err
	}
	if pr.Merged || pr.State == "closed" {
		_, err = db.Transition(ctx, r.ID, model.Interrupted, model.NeedsHuman, map[string]any{"error": "The PR was merged or closed while its round was interrupted; awaiting the PR state watcher."})
		return err
	}
	if err := fetchPRHead(ctx, cfg, pr); err != nil {
		return err
	}
	_, err = db.Transition(ctx, r.ID, model.Interrupted, model.New, map[string]any{"branch": pr.Head.Ref, "commit_sha": pr.Head.SHA})
	return err
}

func roundStopped(ctx context.Context, cfg worker.Config, db *store.Store, r model.Report) error {
	text := fmt.Sprintf("Review round %d stopped without publishing a change (%s).\n\n%s", r.ReviewRounds, r.Status, worker.Publishable(cfg, r.Error))
	if err := remote.IssueCommentOnce(ctx, cfg, db, r.ID, r.PRNumber, text, fmt.Sprintf("round:%d:%d", r.ID, r.ReviewRounds)); err != nil {
		return err
	}
	if err := stateLabel(ctx, cfg, r.PRNumber, "ready-for-human"); err != nil {
		return err
	}
	_, err := db.Transition(ctx, r.ID, r.Status, model.PROpen, map[string]any{"pending_review": ""})
	return err
}

func publish(ctx context.Context, home string, cfg worker.Config, db *store.Store, r model.Report) error {
	if cfg.GitHubRepo == "" {
		return errors.New("publication requires github_repo")
	}
	lock, err := lockFile(home, "worker.lock")
	if err != nil {
		return err
	}
	defer lock.Close()
	child, err := lockFile(home, "herdr.lock")
	if err != nil {
		return err
	}
	defer child.Close()
	m, err := worker.VerifiedSnapshot(ctx, home, cfg, r)
	if err != nil {
		return err
	}
	branch := r.Branch
	if branch == "" {
		sum := sha256.Sum256([]byte(home))
		branch = fmt.Sprintf("feedback/%s/%d", hex.EncodeToString(sum[:4]), r.ID)
		r, err = db.Patch(ctx, r.ID, map[string]any{"branch": branch})
		if err != nil {
			return err
		}
	}
	if _, err := worker.Git(ctx, cfg.Repo, "check-ref-format", "refs/heads/"+branch); err != nil {
		return err
	}
	if r.PRNumber > 0 {
		pr, err := getPR(ctx, cfg, r.PRNumber)
		if err != nil {
			return err
		}
		if pr.Merged || pr.State == "closed" {
			next := model.Closed
			if pr.Merged {
				next = model.Done
			}
			_, err = db.Transition(ctx, r.ID, r.Status, next, map[string]any{"commit_sha": pr.MergeSHA})
			return err
		}
		if pr.Head.Repo.FullName != cfg.GitHubRepo || pr.Head.Ref != branch {
			return errors.New("PR branch ownership changed")
		}
		if pr.Head.SHA != m.Base && pr.Head.SHA != m.Commit {
			return errors.New("PR head changed since verification; refusing overwrite")
		}
	}
	refs, err := worker.Git(ctx, cfg.Repo, "-c", "credential.helper=!gh auth git-credential", "ls-remote", "--heads", "origin", "refs/heads/"+branch)
	if err != nil {
		return err
	}
	fields := strings.Fields(string(refs))
	remoteHead := ""
	if len(fields) > 0 {
		remoteHead = fields[0]
	}
	if remoteHead != "" && remoteHead != m.Commit && (r.PRNumber == 0 || remoteHead != m.Base) {
		return errors.New("publication branch already contains another change")
	}
	if remoteHead != m.Commit {
		// Force-with-lease only enforces the observed head, including branch absence.
		// The candidate is a direct child of that head for all existing-PR rounds.
		if _, err := worker.Git(ctx, cfg.Repo, "-c", "credential.helper=!gh auth git-credential", "push", "--force-with-lease=refs/heads/"+branch+":"+remoteHead, "origin", m.Commit+":refs/heads/"+branch); err != nil {
			return err
		}
	}
	if _, err := db.Patch(ctx, r.ID, map[string]any{"commit_sha": m.Commit}); err != nil {
		return err
	}
	if r.PRNumber > 0 {
		if err := remote.IssueCommentOnce(ctx, cfg, db, r.ID, r.PRNumber, evidenceBody(cfg, r, m), fmt.Sprintf("round-published:%d:%d", r.ID, r.ReviewRounds)); err != nil {
			return err
		}
		if err := stateLabel(ctx, cfg, r.PRNumber, "ready-for-human"); err != nil {
			return err
		}
		_, err = db.Transition(ctx, r.ID, model.Verified, model.PROpen, map[string]any{"pending_review": "", "error": ""})
		return err
	}
	// Reconcile by exact branch before reserving creation, including closed PRs.
	owner, _, ok := strings.Cut(cfg.GitHubRepo, "/")
	if !ok {
		return errors.New("invalid github repository")
	}
	prs, err := list[pull](ctx, endpoint(cfg, "pulls?state=all&head="+url.QueryEscape(owner+":"+branch)))
	if err != nil {
		return err
	}
	number := 0
	prURL := ""
	for _, pr := range prs {
		if pr.Head.Ref == branch && pr.Head.Repo.FullName == cfg.GitHubRepo {
			if number != 0 {
				return errors.New("more than one PR matches publication branch")
			}
			number, prURL = pr.Number, pr.URL
		}
	}
	key := fmt.Sprintf("pr-create:%d", r.ID)
	size, files, err := diffSize(ctx, cfg, m)
	if err != nil {
		return err
	}
	draft := (cfg.SafeMaxLines > 0 && size > cfg.SafeMaxLines) || (cfg.SafeMaxFiles > 0 && files > cfg.SafeMaxFiles)
	if number == 0 {
		fresh, err := db.Reserve(ctx, key, "pr-create", r.ID, branch)
		if err != nil {
			return err
		}
		if !fresh {
			return errors.New("PR creation is uncertain; no matching PR found, operator reconciliation required")
		}
		body := evidenceBody(cfg, r, m) + fmt.Sprintf("\n\nNon-test lines changed: %d; files changed: %d. Draft threshold exceeded: %t.\n\nCloses #%d\n\n<!-- feedback-loop:report:%d -->", size, files, draft, r.IssueNumber, r.ID)
		bodyPath := filepath.Join(worker.AttemptDir(home, r), "pr-body.md")
		if err := os.WriteFile(bodyPath, []byte(body), 0600); err != nil {
			return err
		}
		title := r.Title
		if title == "" {
			title = "Address feedback #" + strconv.FormatInt(r.ID, 10)
		}
		title = worker.Publishable(cfg, title)
		args := []string{"pr", "create", "--repo", cfg.GitHubRepo, "--head", branch, "--base", strings.TrimPrefix(cfg.Base, "origin/"), "--title", title, "--body-file", bodyPath}
		if draft {
			args = append(args, "--draft")
		}
		if _, err := remote.GH(ctx, args...); err != nil {
			return err
		}
		prs, err = list[pull](ctx, endpoint(cfg, "pulls?state=all&head="+url.QueryEscape(owner+":"+branch)))
		if err != nil {
			return err
		}
		for _, pr := range prs {
			if pr.Head.Ref == branch && pr.Head.Repo.FullName == cfg.GitHubRepo {
				number, prURL = pr.Number, pr.URL
				break
			}
		}
		if number == 0 {
			return errors.New("created PR could not be reconciled")
		}
	}
	if _, err := db.Reserve(ctx, key, "pr-create", r.ID, branch); err != nil {
		return err
	}
	if err := db.Complete(ctx, key, strconv.Itoa(number)); err != nil {
		return err
	}
	_, err = db.Transition(ctx, r.ID, model.Verified, model.PROpen, map[string]any{"pr_number": number, "pr_url": prURL, "pr_draft": draft, "review_seen_at": time.Now().UTC().Format(time.RFC3339), "error": ""})
	return err
}

func diffSize(ctx context.Context, cfg worker.Config, m worker.Manifest) (int, int, error) {
	data, err := worker.Git(ctx, cfg.Repo, "diff", "--numstat", "--no-renames", "-z", m.Base, m.Commit, "--")
	if err != nil {
		return 0, 0, err
	}
	lines, files := 0, 0
	for record := range strings.SplitSeq(string(data), "\x00") {
		if record == "" {
			continue
		}
		parts := strings.SplitN(record, "\t", 3)
		if len(parts) != 3 {
			return 0, 0, errors.New("invalid diff size")
		}
		files++
		if worker.TestPath(cfg, parts[2]) {
			continue
		}
		a, _ := strconv.Atoi(parts[0])
		d, _ := strconv.Atoi(parts[1])
		lines += a + d
	}
	return lines, files, nil
}

func evidenceBody(cfg worker.Config, r model.Report, m worker.Manifest) string {
	var text strings.Builder
	fmt.Fprintf(&text, "## Summary\n\n%s\n\n## Evidence\n\nVerified commit: `%s`\nBase: `%s`\nAttempt: %d\n\n", worker.Publishable(cfg, m.AgentReport), m.Commit, m.Base, m.Attempt)
	for _, stage := range []string{"setup", "agent", "verify", "verify-red", "review-fix", "verify-fix", "verify-fix-red"} {
		if result, ok := m.Results[stage]; ok {
			fmt.Fprintf(&text, "- %s: exit %d; timed out: %t; canceled: %t\n", stage, result.ExitCode, result.TimedOut, result.Canceled)
		}
	}
	if cfg.RedVerify == "" {
		text.WriteString("- Reproduction on unfixed code: disabled by configuration.\n")
	}
	fmt.Fprintf(&text, "\n## Review\n\nVerdict: %s. %s\n\n%s\n", m.Review.Verdict, worker.Publishable(cfg, m.Review.Reason), worker.Publishable(cfg, m.Review.Report))
	if m.Review.Verdict == "fixed" {
		text.WriteString("\nOne implementer fix round addressed the original blocking findings. The resulting tree passed verification; it was not independently reviewed again.\n")
	}
	text.WriteString("\n## Merge danger\n\nAutomation did not exercise production deployment or user sessions. A maintainer must assess the recorded review and remaining risk. Local attempt logs retain full evidence.\n\n<!-- fb-loop -->")
	return text.String()
}

// Notify replays a report's current durable outcome without duplicate comments.
func Notify(ctx context.Context, home string, cfg worker.Config, db *store.Store, id int64) error {
	r, err := db.Get(ctx, id)
	if err != nil {
		return err
	}
	var text, role string
	switch r.Status {
	case model.PROpen:
		text = fmt.Sprintf("A verified change is ready for maintainer review: %s\nCommit: `%s`. It has not been merged or deployed.", r.PRURL, r.CommitSHA)
	case model.Done, model.Merged:
		text = fmt.Sprintf("The change was merged: %s\nCommit: `%s`.", r.PRURL, r.CommitSHA)
		if cfg.Releases {
			text += " It is awaiting the repository's release process."
		}
	case model.Closed:
		text = "The PR was closed without merging. A maintainer will decide the next step."
		role = "ready-for-human"
	case model.NeedsReporter, model.NeedsInfo:
		text = "More information is needed before changing code. Reply with reproduction steps and the missing evidence.\n\n" + worker.Publishable(cfg, r.Error)
		role = "needs-info"
	case model.NeedsHuman, model.GuardBlocked:
		text = "The report needs a maintainer's decision; no unverified change was published.\n\n" + worker.Publishable(cfg, r.Error)
		role = "ready-for-human"
	case model.AwaitingApproval:
		text = "This report is waiting for maintainer approval.\n\n" + worker.Publishable(cfg, r.Brief)
		role = "ready-for-human"
	case model.SetupFailed, model.AgentFailed, model.VerifyFailed, model.Failed, model.NoChange:
		text = fmt.Sprintf("The attempt stopped (%s); no change was published.\n\n%s", r.Status, worker.Publishable(cfg, r.Error))
		role = "ready-for-human"
	default:
		return nil
	}
	if r.Language == "zh" {
		switch r.Status {
		case model.PROpen:
			text = fmt.Sprintf("改动已通过检查，等待维护者评审：%s\n提交：`%s`。尚未合并或部署。", r.PRURL, r.CommitSHA)
		case model.Done, model.Merged:
			text = fmt.Sprintf("改动已合并：%s\n提交：`%s`。", r.PRURL, r.CommitSHA)
			if cfg.Releases {
				text += "等待仓库后续发布。"
			}
		case model.Closed:
			text = "PR 未合并就关闭了，下一步留给维护者决定。"
		case model.NeedsReporter, model.NeedsInfo:
			text = "目前还需要复现步骤和更多线索，请回复补充；这次没有猜测着修改代码。\n\n" + worker.Publishable(cfg, r.Error)
		case model.AwaitingApproval:
			text = "报告已记录，等待维护者批准。\n\n" + worker.Publishable(cfg, r.Brief)
		default:
			text = fmt.Sprintf("本次处理已停止（%s），没有发布未通过检查的改动，留给维护者查看。\n\n%s", r.Status, worker.Publishable(cfg, r.Error))
		}
	}
	if r.Source == "github" && (r.Status == model.NeedsReporter || r.Status == model.NeedsInfo) {
		if r.Language == "zh" {
			text += "\n\n请在 issue 中补充线索，再由维护者标记为 ready-for-agent。"
		} else {
			text += "\n\nAdd the clues to the issue and ask a maintainer to mark it ready for the agent again."
		}
	}
	key := fmt.Sprintf("result:%d:%d:%d:%s", r.ID, r.Attempt, r.ReviewRounds, r.Status)
	receipt := key + ":complete"
	if sent, e := db.Reservation(ctx, receipt); e == nil && sent.State == "complete" {
		return nil
	} else if e != nil && !errors.Is(e, model.ErrNotFound) {
		return e
	}
	if _, err := db.Reserve(ctx, receipt, "result-delivery", r.ID, text); err != nil {
		return err
	}
	if r.IssueNumber > 0 {
		if r.TriageDecision != "" && r.Brief != "" {
			if err := remote.IssueCommentOnce(ctx, cfg, db, r.ID, r.IssueNumber, "> *This was generated by AI during triage.*\n\n"+worker.Publishable(cfg, r.Brief), fmt.Sprintf("triage:%d:%d", r.ID, r.Attempt)); err != nil {
				return err
			}
		}
		if r.TriageDecision != "" {
			if category := cfg.CategoryLabels[r.Kind]; category != "" {
				if _, err := remote.GH(ctx, "issue", "edit", strconv.Itoa(r.IssueNumber), "--repo", cfg.GitHubRepo, "--add-label", category); err != nil {
					return err
				}
			}
		}
		if err := remote.IssueCommentOnce(ctx, cfg, db, r.ID, r.IssueNumber, text, key); err != nil {
			return err
		}
		if err := stateLabel(ctx, cfg, r.IssueNumber, role); err != nil {
			return err
		}
	}
	if r.Status == model.Closed {
		return db.Complete(ctx, receipt, "delivered")
	}
	channel, message := r.ChannelID, r.MessageID
	if channel == "" && r.SourceRef != "" {
		var ref struct {
			Channel string `json:"channel"`
			Message string `json:"message"`
		}
		if json.Unmarshal([]byte(r.SourceRef), &ref) == nil {
			channel, message = ref.Channel, ref.Message
		}
	}
	if channel == "" {
		return db.Complete(ctx, receipt, "delivered")
	}
	deliveryKey := key + ":discord"
	existing, err := db.Reservation(ctx, deliveryKey)
	if err == nil && existing.State == "complete" {
		var ids []string
		if json.Unmarshal([]byte(existing.Result), &ids) == nil {
			for _, id := range ids {
				if err := db.LinkMessage(ctx, id, r.ID, "result"); err != nil {
					return err
				}
			}
		}
		return db.Complete(ctx, receipt, "delivered")
	}
	if err != nil && !errors.Is(err, model.ErrNotFound) {
		return err
	}
	if _, err := db.Reserve(ctx, deliveryKey, "discord-result", r.ID, text); err != nil {
		return err
	}
	ids, err := remote.SendDiscordDelivery(ctx, cfg, db, r.ID, channel, message, text, deliveryKey)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := db.LinkMessage(ctx, id, r.ID, "result"); err != nil {
			return err
		}
	}
	data, _ := json.Marshal(ids)
	if err := db.Complete(ctx, deliveryKey, string(data)); err != nil {
		return err
	}
	return db.Complete(ctx, receipt, "delivered")
}

func cleanup(ctx context.Context, home string, cfg worker.Config, db *store.Store, r model.Report) error {
	lock, err := lockFile(home, "worker.lock")
	if err != nil {
		return err
	}
	defer lock.Close()
	child, err := lockFile(home, "herdr.lock")
	if err != nil {
		return err
	}
	defer child.Close()
	if err := worker.Cleanup(ctx, cfg, home, r); err != nil {
		return err
	}
	_, err = db.Patch(ctx, r.ID, map[string]any{"worktree": ""})
	return err
}
