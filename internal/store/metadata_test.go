package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/FaintGhost/loom/internal/model"
)

func TestDurableIdentityAndConcurrentFollowups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reports.sqlite")
	first := openStore(t, path)
	second := openStore(t, path)
	ctx := t.Context()
	report, err := first.AddReport(ctx, model.Report{Body: "the original", Status: model.PROpen, Source: "discord", SourceKey: "discord:111", MessageID: "111", IssueNumber: 12, PRNumber: 34, Followups: "[]"})
	if err != nil {
		t.Fatal(err)
	}
	stores := []*Store{first, second}
	failures := make(chan error, 32)
	reserved := make(chan bool, 16)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			db := stores[i%2]
			duplicate, err := db.AddReport(ctx, model.Report{Body: "redelivered", SourceKey: "discord:111"})
			if err != nil {
				failures <- err
				return
			}
			if duplicate.ID != report.ID {
				failures <- fmt.Errorf("new identity on replay: %d", duplicate.ID)
				return
			}
			entry := map[string]any{"message_id": fmt.Sprintf("followup-%d", i), "body": "additional evidence"}
			if _, err = db.AppendFollowup(ctx, report.ID, entry); err != nil {
				failures <- err
				return
			}
			if _, err = db.AppendFollowup(ctx, report.ID, entry); err != nil {
				failures <- err
				return
			}
			claimed, err := db.Reserve(ctx, "discord-delivery:1", "discord", report.ID, "notice")
			if err != nil {
				failures <- err
				return
			}
			reserved <- claimed
		})
	}
	wg.Wait()
	close(failures)
	close(reserved)
	for err := range failures {
		t.Error(err)
	}
	winners := 0
	for winner := range reserved {
		if winner {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("external delivery claimed %d times", winners)
	}
	current, err := first.Get(ctx, report.ID)
	if err != nil {
		t.Fatal(err)
	}
	var followups []map[string]any
	if err = json.Unmarshal([]byte(current.Followups), &followups); err != nil {
		t.Fatal(err)
	}
	if len(followups) != 16 || current.IssueNumber != 12 || current.PRNumber != 34 {
		t.Fatalf("lost followup or remote identity: %+v", current)
	}
	if _, err = first.Patch(ctx, report.ID, map[string]any{"pr_number": 99}); !errors.Is(err, model.ErrConflict) {
		t.Fatalf("PR replacement: %v", err)
	}
	if _, err = first.Patch(ctx, report.ID, map[string]any{"status": "new"}); err == nil {
		t.Fatal("patch bypassed transition guard")
	}
	if _, err = first.Patch(ctx, report.ID, map[string]any{"followups": "[]"}); err == nil {
		t.Fatal("patch overwrote concurrent followups")
	}
	other, err := first.Add(ctx, "other report")
	if err != nil {
		t.Fatal(err)
	}
	if err = first.LinkMessage(ctx, "111", other.ID, "report"); !errors.Is(err, model.ErrConflict) {
		t.Fatalf("message ownership replacement: %v", err)
	}
	if err = first.Complete(ctx, "discord-delivery:1", "sent-id"); err != nil {
		t.Fatal(err)
	}
	if err = second.Complete(ctx, "discord-delivery:1", "sent-id"); err != nil {
		t.Fatal(err)
	}
	if err = second.Complete(ctx, "discord-delivery:1", "different-id"); !errors.Is(err, model.ErrConflict) {
		t.Fatalf("delivery result replacement: %v", err)
	}
}

func TestTransitionCompareAndResumePreservesAttempt(t *testing.T) {
	db := openStore(t, filepath.Join(t.TempDir(), "reports.sqlite"))
	ctx := t.Context()
	report, err := db.Add(ctx, "resume original attempt")
	if err != nil {
		t.Fatal(err)
	}
	report, err = db.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Transition(ctx, report.ID, model.New, model.Working, nil); !errors.Is(err, model.ErrTransition) {
		t.Fatalf("stale state consumed twice: %v", err)
	}
	if err = db.Finish(ctx, report.ID, model.Interrupted, "retained evidence"); err != nil {
		t.Fatal(err)
	}
	resumed, err := db.Transition(ctx, report.ID, model.Interrupted, model.Working, nil)
	if err != nil || resumed.Attempt != report.Attempt {
		t.Fatalf("resume incremented attempt: %+v %v", resumed, err)
	}
	if err = db.Finish(ctx, report.ID, model.NeedsReporter, "missing steps"); err != nil {
		t.Fatal(err)
	}
}

func TestRetryCannotReauthorizeConversationOrExistingPR(t *testing.T) {
	db := openStore(t, filepath.Join(t.TempDir(), "reports.sqlite"))
	for _, report := range []model.Report{
		{Body: "failed review", Status: model.VerifyFailed, IssueNumber: 12, PRNumber: 34},
		{Body: "unanswered question", Status: model.Failed, Kind: "question"},
		{Body: "waiting for approval", Status: model.AwaitingApproval, IssueNumber: 12, Kind: "bug"},
	} {
		r, err := db.AddReport(t.Context(), report)
		if err != nil {
			t.Fatal(err)
		}
		if err = db.Retry(t.Context(), r.ID); !errors.Is(err, model.ErrTransition) {
			t.Fatalf("retry bypassed authorization for %+v: %v", r, err)
		}
	}
}

func TestAuthorizedRequeueResetsBudgetWithoutReusingAttempt(t *testing.T) {
	db := openStore(t, filepath.Join(t.TempDir(), "reports.sqlite"))
	ctx := t.Context()
	report, err := db.AddReport(ctx, model.Report{Body: "needs a better reproduction", Status: model.VerifyFailed, Attempt: 3, IssueNumber: 42, Worktree: "/retained/attempt-3"})
	if err != nil {
		t.Fatal(err)
	}
	requeued, err := db.Requeue(ctx, report.ID, model.VerifyFailed, model.New, map[string]any{"error": ""})
	if err != nil || requeued.Attempt != 3 || requeued.RetryBase != 3 || requeued.Worktree != report.Worktree {
		t.Fatalf("requeue lost evidence identity: %+v %v", requeued, err)
	}
	if _, err = db.Requeue(ctx, report.ID, model.VerifyFailed, model.New, nil); !errors.Is(err, model.ErrTransition) {
		t.Fatalf("stale reopen reset budget twice: %v", err)
	}
	claimed, err := db.Claim(ctx)
	if err != nil || claimed.Attempt != 4 || claimed.Attempt-claimed.RetryBase != 1 {
		t.Fatalf("new claim reused evidence or exhausted fresh budget: %+v %v", claimed, err)
	}
	if err = db.Finish(ctx, report.ID, model.VerifyFailed, "new attempt failed"); err != nil {
		t.Fatal(err)
	}
	if err = db.Retry(ctx, report.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err = db.Claim(ctx)
	if err != nil || claimed.Attempt != 5 || claimed.Attempt-claimed.RetryBase != 2 {
		t.Fatalf("ordinary retry reset budget: %+v %v", claimed, err)
	}
	if _, err = db.Patch(ctx, report.ID, map[string]any{"retry_base": 5}); err == nil {
		t.Fatal("ordinary patch reset retry authorization")
	}
	for _, status := range []model.Status{model.Queued, model.Held} {
		r, err := db.AddReport(ctx, model.Report{Body: "explicit requeue", Status: model.Failed, Attempt: 2, IssueNumber: 45})
		if err != nil {
			t.Fatal(err)
		}
		out, err := db.Requeue(ctx, r.ID, model.Failed, status, nil)
		if err != nil || out.RetryBase != 2 || out.Attempt != 2 {
			t.Fatalf("requeue %s: %+v %v", status, out, err)
		}
	}
	withPR, err := db.AddReport(ctx, model.Report{Body: "failed PR review", Status: model.VerifyFailed, Attempt: 2, IssueNumber: 42, PRNumber: 51})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Requeue(ctx, withPR.ID, model.VerifyFailed, model.New, nil); !errors.Is(err, model.ErrTransition) {
		t.Fatalf("requeue replaced existing PR lifecycle: %v", err)
	}
}

func TestExistingPRReviewRestartAndInterruptedRetry(t *testing.T) {
	db := openStore(t, filepath.Join(t.TempDir(), "reports.sqlite"))
	ctx := t.Context()
	report, err := db.AddReport(ctx, model.Report{Body: "existing change", Status: model.Closed, Attempt: 4, RetryBase: 2, ReviewRounds: 2, IssueNumber: 42, PRNumber: 51, Branch: "feedback/original", PendingReview: "maintainer feedback"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.StartReview(ctx, report.ID, model.Closed, nil); !errors.Is(err, model.ErrTransition) {
		t.Fatalf("closed PR restarted without authorization: %v", err)
	}
	if _, err = db.Transition(ctx, report.ID, model.Closed, model.New, map[string]any{"decided_by": "maintainer"}); !errors.Is(err, model.ErrTransition) {
		t.Fatalf("generic transition bypassed review accounting: %v", err)
	}
	restarted, err := db.StartReview(ctx, report.ID, model.Closed, map[string]any{"decided_by": "maintainer"})
	if err != nil || restarted.PRNumber != 51 || restarted.Branch != "feedback/original" || restarted.Attempt != 4 || restarted.RetryBase != 4 || restarted.ReviewRounds != 3 {
		t.Fatalf("review restart lost identity or budget: %+v %v", restarted, err)
	}
	if _, err = db.StartReview(ctx, report.ID, model.Closed, map[string]any{"decided_by": "maintainer"}); !errors.Is(err, model.ErrTransition) {
		t.Fatalf("stale restart consumed another review round: %v", err)
	}
	claimed, err := db.Claim(ctx)
	if err != nil || claimed.Attempt != 5 {
		t.Fatalf("claim: %+v %v", claimed, err)
	}
	if err = db.Finish(ctx, report.ID, model.Interrupted, "setup interrupted before model launch"); err != nil {
		t.Fatal(err)
	}
	queued, err := db.Transition(ctx, report.ID, model.Interrupted, model.New, map[string]any{"error": ""})
	if err != nil || queued.Attempt != 5 || queued.RetryBase != 4 || queued.ReviewRounds != 3 || queued.PendingReview != "maintainer feedback" {
		t.Fatalf("PR retry consumed review authorization: %+v %v", queued, err)
	}
	claimed, err = db.Claim(ctx)
	if err != nil || claimed.Attempt != 6 || claimed.Error != "" || claimed.RetryReason != "setup interrupted before model launch" {
		t.Fatalf("claim lost previous failure context: %+v %v", claimed, err)
	}
	if err = db.Finish(ctx, report.ID, model.Interrupted, "second interruption"); err != nil {
		t.Fatal(err)
	}
	exhausted, err := db.Transition(ctx, report.ID, model.Interrupted, model.NeedsHuman, nil)
	if err != nil || exhausted.PRNumber != 51 || exhausted.PendingReview != "maintainer feedback" {
		t.Fatalf("exhaustion lost PR context: %+v %v", exhausted, err)
	}
}

func TestLocalRetryRetainsPriorFailureReason(t *testing.T) {
	db := openStore(t, filepath.Join(t.TempDir(), "reports.sqlite"))
	ctx := t.Context()
	report, err := db.AddReport(ctx, model.Report{Body: "local failure", Status: model.VerifyFailed, Attempt: 2, Error: "regression failed on the unfixed code"})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Retry(ctx, report.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err := db.Claim(ctx)
	if err != nil || claimed.Attempt != 3 || claimed.Error != "" || claimed.RetryReason != "regression failed on the unfixed code" {
		t.Fatalf("retry failure context lost: %+v %v", claimed, err)
	}
}

func TestMessageRequeueReceiptCommitsWithBudgetAndSurvivesLaterWork(t *testing.T) {
	db := openStore(t, filepath.Join(t.TempDir(), "reports.sqlite"))
	ctx := t.Context()
	report, err := db.AddReport(ctx, model.Report{Body: "trusted follow-up", Status: model.VerifyFailed, Attempt: 3, IssueNumber: 42})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.db.ExecContext(ctx, "CREATE TRIGGER fail_reopen BEFORE INSERT ON events WHEN NEW.kind='reopened' BEGIN SELECT RAISE(ABORT,'reopen receipt unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err = db.RequeueMessage(ctx, report.ID, model.VerifyFailed, model.New, nil, "message-1", 1); err == nil {
		t.Fatal("requeue ignored receipt failure")
	}
	unchanged, err := db.Get(ctx, report.ID)
	if err != nil || unchanged.Status != model.VerifyFailed || unchanged.RetryBase != 0 || unchanged.Attempt != 3 {
		t.Fatalf("receipt failure committed partial state: %+v %v", unchanged, err)
	}
	if _, err = db.db.ExecContext(ctx, "DROP TRIGGER fail_reopen"); err != nil {
		t.Fatal(err)
	}
	queued, err := db.RequeueMessage(ctx, report.ID, model.VerifyFailed, model.New, nil, "message-1", 1)
	if err != nil || queued.RetryBase != 3 {
		t.Fatalf("message requeue: %+v %v", queued, err)
	}
	claimed, err := db.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Finish(ctx, report.ID, model.VerifyFailed, "later independent failure"); err != nil {
		t.Fatal(err)
	}
	// The caller crashed before finishing intake; even an invalid/stale expected
	// state must return the current report when this message already reopened it.
	replayed, err := db.RequeueMessage(ctx, report.ID, model.New, model.New, nil, "message-1", 1)
	if err != nil || replayed.Status != model.VerifyFailed || replayed.Attempt != claimed.Attempt || replayed.RetryBase != 3 {
		t.Fatalf("message replay reset later work: %+v %v", replayed, err)
	}
	if _, err = db.RequeueMessage(ctx, report.ID, model.VerifyFailed, model.New, nil, "message-2", 1); !errors.Is(err, model.ErrReopenLimit) {
		t.Fatalf("new message bypassed reopen limit: %v", err)
	}
	events, err := db.Events(ctx, report.ID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Kind == "reopened" {
			count++
			if event.Detail != "message-1" {
				t.Fatalf("wrong reopen receipt: %+v", event)
			}
		}
	}
	if count != 1 {
		t.Fatalf("reopen receipt count=%d", count)
	}
}
