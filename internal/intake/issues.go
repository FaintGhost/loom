package intake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/remote"
	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
)

// EnsureIssue materializes one durable report before it may enter execution.
func EnsureIssue(ctx context.Context, cfg worker.Config, db *store.Store, row model.Report) (model.Report, error) {
	return ensureIssue(ctx, cfg, db, row)
}
func ensureIssue(ctx context.Context, cfg worker.Config, db *store.Store, row model.Report) (model.Report, error) {
	if row.IssueNumber != 0 {
		return row, nil
	}
	if cfg.GitHubRepo == "" {
		return row, errors.New("GitHub repository is required to file actionable feedback")
	}
	identity := row.SourceKey
	if identity == "" {
		identity = fmt.Sprintf("report:%s:%d", row.Repository, row.ID)
	}
	marker := "<!-- feedback-loop:" + identity + " -->"
	reservationKey := "issue-create:" + identity
	title := row.Title
	if title == "" {
		title = prefix(strings.SplitN(row.Body, "\n", 2)[0], 80)
	}
	kind := "Bug"
	if row.Kind == "feature_request" {
		kind = "Feature"
	}
	title = "[" + kind + "] " + title
	body := fmt.Sprintf("Reported by %s:\n\n%s", row.Author, row.Body)
	var urls []string
	_ = json.Unmarshal([]byte(row.Attachments), &urls)
	for _, url := range urls {
		body += "\n" + url
	}
	body += "\n\n" + marker
	if receipt, err := db.Reservation(ctx, reservationKey); err == nil && receipt.State == "complete" {
		var issue struct {
			Number int    `json:"number"`
			URL    string `json:"html_url"`
		}
		if err = json.Unmarshal([]byte(receipt.Result), &issue); err != nil {
			return row, err
		}
		if issue.Number <= 0 || issue.URL == "" {
			return row, errors.New("invalid persisted issue receipt")
		}
		return db.Patch(ctx, row.ID, map[string]any{"issue_number": issue.Number, "issue_url": issue.URL, "title": title})
	} else if err != nil && !errors.Is(err, model.ErrNotFound) {
		return row, err
	}
	// Read the authoritative issue list, including closed issues; search indexing
	// can lag after a successful write whose local commit was interrupted.
	data, err := remote.GH(ctx, "api", "repos/"+cfg.GitHubRepo+"/issues?state=all&per_page=100", "--paginate", "--slurp")
	if err != nil {
		return row, err
	}
	var pages [][]struct {
		Number      int             `json:"number"`
		URL         string          `json:"html_url"`
		Body        string          `json:"body"`
		PullRequest json.RawMessage `json:"pull_request"`
	}
	if err = json.Unmarshal(data, &pages); err != nil {
		return row, err
	}
	for _, page := range pages {
		for _, issue := range page {
			if len(issue.PullRequest) == 0 && strings.Contains(issue.Body, marker) {
				if _, err = db.Reserve(ctx, reservationKey, "issue-create", row.ID, body); err != nil {
					return row, err
				}
				if err = db.Complete(ctx, reservationKey, jsonText(map[string]any{"number": issue.Number, "html_url": issue.URL})); err != nil {
					return row, err
				}
				return db.Patch(ctx, row.ID, map[string]any{"issue_number": issue.Number, "issue_url": issue.URL, "title": title})
			}
		}
	}
	claimed, err := db.Reserve(ctx, reservationKey, "issue-create", row.ID, body)
	if err != nil {
		return row, err
	}
	if !claimed {
		return row, errors.New("issue creation uncertain: no remote marker found; operator must reconcile before retry")
	}
	data, err = remote.GH(ctx, "api", "repos/"+cfg.GitHubRepo+"/issues", "--method", "POST", "-f", "title="+title, "-f", "body="+body)
	if err != nil {
		return row, err
	}
	var issue struct {
		Number int    `json:"number"`
		URL    string `json:"html_url"`
	}
	if err = json.Unmarshal(data, &issue); err != nil {
		return row, err
	}
	if issue.Number <= 0 || issue.URL == "" {
		return row, errors.New("GitHub omitted created issue identity")
	}
	if err = db.Complete(ctx, reservationKey, jsonText(issue)); err != nil {
		return row, err
	}
	return db.Patch(ctx, row.ID, map[string]any{"issue_number": issue.Number, "issue_url": issue.URL, "title": title})
}
func issueState(ctx context.Context, cfg worker.Config, number int) ([]string, string, error) {
	data, err := remote.GH(ctx, "issue", "view", strconv.Itoa(number), "--repo", cfg.GitHubRepo, "--json", "labels,state")
	if err != nil {
		return nil, "", err
	}
	var issue struct {
		State  string `json:"state"`
		Labels []struct {
			Name string `json:"name"`
		} `json:"labels"`
	}
	if err = json.Unmarshal(data, &issue); err != nil {
		return nil, "", err
	}
	labels := make([]string, 0, len(issue.Labels))
	for _, l := range issue.Labels {
		labels = append(labels, l.Name)
	}
	return labels, issue.State, nil
}
func labelIssue(ctx context.Context, cfg worker.Config, row model.Report) error {
	if row.IssueNumber == 0 {
		return nil
	}
	labels := []string{}
	if category := cfg.CategoryLabels[row.Kind]; category != "" {
		labels = append(labels, category)
	}
	if row.Status == model.AwaitingApproval || row.Status == model.Held {
		if cfg.NeedsHumanLabel != "" {
			labels = append(labels, cfg.NeedsHumanLabel)
		}
	}
	for _, label := range labels {
		if _, err := remote.GH(ctx, "label", "create", label, "--repo", cfg.GitHubRepo, "--force"); err != nil {
			return err
		}
		if _, err := remote.GH(ctx, "issue", "edit", strconv.Itoa(row.IssueNumber), "--repo", cfg.GitHubRepo, "--add-label", label); err != nil {
			return err
		}
	}
	return nil
}
func findDuplicate(ctx context.Context, cfg worker.Config, reports []model.Report, current model.Report) (model.Report, error) {
	criteria := map[string]any{"none": "a different problem or uncertain"}
	candidates := map[string]model.Report{}
	cap := cfg.DedupeCandidates
	if cap <= 0 {
		cap = 15
	}
	for i := len(reports) - 1; i >= 0 && len(candidates) < cap; i-- {
		r := reports[i]
		if r.ID == current.ID || r.IssueNumber == 0 || model.Terminal(r.Status) {
			continue
		}
		labels, state, err := issueState(ctx, cfg, r.IssueNumber)
		if err != nil {
			continue
		}
		wontfix := false
		for _, l := range labels {
			if l == "wontfix" || l == "won't-fix" {
				wontfix = true
			}
		}
		if state != "OPEN" || wontfix {
			continue
		}
		key := fmt.Sprintf("r%d", r.ID)
		criteria[key] = r.Title + " — " + prefix(r.Body, 300)
		candidates[key] = r
	}
	if len(candidates) == 0 {
		return model.Report{}, nil
	}
	answers, err := remote.SystemOne(ctx, cfg, map[string]any{"duplicate_of": map[string]any{"type": "choice", "instructions": "Is the latest message the SAME problem as an open report (same broken behavior, not merely same area)? Choose none if unsure or different.", "criteria": criteria}}, map[string]any{"message": prefix(current.Body, 6000)})
	if err != nil {
		return model.Report{}, nil
	}
	a := answers["duplicate_of"]
	threshold := cfg.DedupeMinConfidence
	if threshold <= 0 {
		threshold = .8
	}
	if !(a.Confidence >= threshold) {
		return model.Report{}, nil
	}
	return candidates[a.Choice], nil
}
func issueComment(ctx context.Context, cfg worker.Config, db *store.Store, id int64, number int, body, key string) error {
	return remote.IssueCommentOnce(ctx, cfg, db, id, number, body, key)
}

func notePriorDecision(ctx context.Context, cfg worker.Config, db *store.Store, row model.Report) {
	ref, err := worker.Git(ctx, cfg.Repo, "symbolic-ref", "--quiet", "refs/remotes/origin/HEAD")
	if err != nil {
		return
	}
	revision := strings.TrimSpace(string(ref))
	files, err := worker.Git(ctx, cfg.Repo, "ls-tree", "-r", "--name-only", revision, "--", ".out-of-scope/")
	if err != nil {
		return
	}
	criteria := map[string]any{"none": "not the same concept, or uncertain"}
	records := map[string]string{}
	for path := range strings.SplitSeq(strings.TrimSpace(string(files)), "\n") {
		if !strings.HasSuffix(path, ".md") {
			continue
		}
		text, err := worker.Git(ctx, cfg.Repo, "show", revision+":"+path)
		if err != nil {
			continue
		}
		k := fmt.Sprintf("o%d", len(records))
		records[k] = path
		criteria[k] = path + " — " + prefix(string(text), 600)
	}
	if len(records) == 0 {
		return
	}
	answers, err := remote.SystemOne(ctx, cfg, map[string]any{"out_of_scope": map[string]any{"type": "choice", "instructions": "Does the latest feature request concern the SAME concept previously rejected in one of these decisions? Match concept, not keywords; choose none if uncertain.", "criteria": criteria}}, map[string]any{"message": prefix(row.Body, 6000)})
	if err != nil {
		return
	}
	a := answers["out_of_scope"]
	threshold := cfg.DedupeMinConfidence
	if threshold <= 0 {
		threshold = .8
	}
	path := records[a.Choice]
	if path == "" || !(a.Confidence >= threshold) {
		return
	}
	note := say(row.Language, "像之前定过的一件事：`"+path+"`；维护者会再确认一下。", "Resembles a prior decision: `"+path+"`; a maintainer will confirm.")
	_ = remote.IssueCommentOnce(ctx, cfg, db, row.ID, row.IssueNumber, note, fmt.Sprintf("prior-decision:%d", row.ID))
}
