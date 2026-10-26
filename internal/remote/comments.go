package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
)

// IssueCommentOnce reconciles a persisted intent with a marker on the issue/PR.
// An uncertain prior POST with no matching comment requires operator resolution.
func IssueCommentOnce(ctx context.Context, cfg worker.Config, db *store.Store, reportID int64, issue int, body, key string) error {
	marker := "<!-- feedback-loop:" + key + " -->"
	reservationKey := "gh-comment:" + key
	old, err := db.Reservation(ctx, reservationKey)
	if err == nil && old.State == "complete" {
		return nil
	}
	if err != nil && !errors.Is(err, model.ErrNotFound) {
		return err
	}
	data, err := GH(ctx, "api", fmt.Sprintf("repos/%s/issues/%d/comments", cfg.GitHubRepo, issue), "--paginate", "--slurp")
	if err != nil {
		return err
	}
	var pages [][]struct {
		ID   int64  `json:"id"`
		Body string `json:"body"`
	}
	if err = json.Unmarshal(data, &pages); err != nil {
		return err
	}
	claimed, err := db.Reserve(ctx, reservationKey, "github-comment", reportID, body)
	if err != nil {
		return err
	}
	for _, page := range pages {
		for _, comment := range page {
			if strings.Contains(comment.Body, marker) {
				return db.Complete(ctx, reservationKey, strconv.FormatInt(comment.ID, 10))
			}
		}
	}
	if !claimed {
		return errors.New("comment delivery uncertain: persisted intent has no remote marker; review before retry")
	}
	data, err = GH(ctx, "api", fmt.Sprintf("repos/%s/issues/%d/comments", cfg.GitHubRepo, issue), "--method", "POST", "-f", "body="+body+"\n\n"+marker)
	if err != nil {
		return err
	}
	var comment struct {
		ID int64 `json:"id"`
	}
	if err = json.Unmarshal(data, &comment); err != nil {
		return err
	}
	if comment.ID == 0 {
		return errors.New("GitHub omitted comment ID")
	}
	return db.Complete(ctx, reservationKey, strconv.FormatInt(comment.ID, 10))
}
