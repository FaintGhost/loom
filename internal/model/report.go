// Package model contains the durable report protocol without I/O.
//
//effectgo:package pure
package model

import "errors"

//effectgo:sealed
type Status string

const (
	New              Status = "new"
	Working          Status = "working"
	Verified         Status = "verified"
	NoChange         Status = "no_change"
	SetupFailed      Status = "setup_failed"
	AgentFailed      Status = "agent_failed"
	VerifyFailed     Status = "verify_failed"
	Interrupted      Status = "interrupted"
	Failed           Status = "failed"
	Claimed          Status = "claimed"
	Queued           Status = "queued"
	AwaitingApproval Status = "awaiting_approval"
	Held             Status = "held"
	NeedsReporter    Status = "needs_reporter"
	NeedsInfo        Status = "needs_info"
	NeedsHuman       Status = "needs_human"
	GuardBlocked     Status = "guard_blocked"
	PROpen           Status = "pr_open"
	ReviseRequested  Status = "revise_requested"
	Revising         Status = "revising"
	ReviewPending    Status = "review_pending"
	Done             Status = "done"
	Merged           Status = "merged"
	Closed           Status = "closed"
	Conversation     Status = "conversation"
)

var (
	ErrEmptyQueue    = errors.New("no queued reports")
	ErrNotFound      = errors.New("report not found")
	ErrTransition    = errors.New("invalid report transition")
	ErrInvalidReport = errors.New("report body must not be blank")
	ErrConflict      = errors.New("durable identity conflict")
	ErrReopenLimit   = errors.New("report reopen limit reached")
)

type Report struct {
	ID               int64   `json:"id"`
	Body             string  `json:"body"`
	Status           Status  `json:"status"`
	Attempt          int     `json:"attempt"`
	RetryBase        int     `json:"retry_base,omitzero"`
	Worktree         string  `json:"worktree"`
	LegacyWorktree   string  `json:"legacy_worktree,omitempty"`
	Error            string  `json:"error"`
	RetryReason      string  `json:"retry_reason,omitempty"`
	CreatedAt        string  `json:"created_at"`
	UpdatedAt        string  `json:"updated_at"`
	Source           string  `json:"source,omitempty"`
	SourceKey        string  `json:"source_key,omitempty"`
	SourceRef        string  `json:"source_ref,omitempty"`
	Repository       string  `json:"repository,omitempty"`
	Author           string  `json:"author,omitempty"`
	AuthorID         string  `json:"author_id,omitempty"`
	Title            string  `json:"title,omitempty"`
	Attachments      string  `json:"attachments,omitempty"`
	Kind             string  `json:"kind,omitempty"`
	Language         string  `json:"language,omitempty"`
	Surface          string  `json:"surface,omitempty"`
	Work             float64 `json:"work,omitzero"`
	Decision         string  `json:"decision,omitempty"`
	DecidedBy        string  `json:"decided_by,omitempty"`
	IssueNumber      int     `json:"issue_number,omitzero"`
	IssueURL         string  `json:"issue_url,omitempty"`
	PRNumber         int     `json:"pr_number,omitzero"`
	PRURL            string  `json:"pr_url,omitempty"`
	Branch           string  `json:"branch,omitempty"`
	CommitSHA        string  `json:"commit_sha,omitempty"`
	BaseSHA          string  `json:"base_sha,omitempty"`
	VerifiedSHA      string  `json:"verified_sha,omitempty"`
	Harness          string  `json:"harness,omitempty"`
	PaneID           string  `json:"pane_id,omitempty"`
	HarnessStartedAt float64 `json:"harness_started_at,omitzero"`
	PRDraft          bool    `json:"pr_draft,omitzero"`
	Followups        string  `json:"followups,omitempty"`
	ReviewRounds     int     `json:"review_rounds,omitzero"`
	ReviewSeenAt     string  `json:"review_seen_at,omitempty"`
	PendingReview    string  `json:"pending_review,omitempty"`
	BriefCommentID   string  `json:"brief_comment_id,omitempty"`
	TriageDecision   string  `json:"triage_decision,omitempty"`
	ReviewVerdict    string  `json:"review_verdict,omitempty"`
	Brief            string  `json:"brief,omitempty"`
	ChannelID        string  `json:"channel_id,omitempty"`
	MessageID        string  `json:"message_id,omitempty"`
}

type Event struct {
	ID         int64  `json:"id"`
	ReportID   int64  `json:"report_id"`
	FromStatus Status `json:"from_status"`
	ToStatus   Status `json:"to_status"`
	Kind       string `json:"kind"`
	Detail     string `json:"detail"`
	At         string `json:"at"`
}

type Message struct {
	ID       string `json:"id"`
	ReportID int64  `json:"report_id"`
	Role     string `json:"role"`
	At       string `json:"at"`
}

// A reserved external operation is intentionally never retried automatically:
// a crash after the remote write requires reconciliation before another send.
type Reservation struct {
	Key       string `json:"key"`
	Kind      string `json:"kind"`
	ReportID  int64  `json:"report_id"`
	Payload   string `json:"payload"`
	Result    string `json:"result"`
	State     string `json:"state"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func Known(status Status) bool {
	switch status {
	case New, Working, Verified, NoChange, SetupFailed, AgentFailed, VerifyFailed, Interrupted, Failed,
		Claimed, Queued, AwaitingApproval, Held, NeedsReporter, NeedsInfo, NeedsHuman,
		GuardBlocked, PROpen, ReviseRequested, Revising, ReviewPending, Done, Merged, Closed, Conversation:
		return true
	}
	return false
}

func Terminal(status Status) bool {
	switch status {
	case Verified, NoChange, SetupFailed, AgentFailed, VerifyFailed, Interrupted, Failed, GuardBlocked, Done, Merged, Closed:
		return true
	case New, Working, Claimed, Queued, AwaitingApproval, Held, NeedsReporter, NeedsInfo, NeedsHuman, PROpen, ReviseRequested, Revising, ReviewPending, Conversation:
		return false
	}
	return false
}

func CanTransition(from, to Status) bool {
	if !Known(from) || !Known(to) || from == to {
		return false
	}
	switch from {
	case New, Queued:
		return to == Working || to == Claimed || to == Held || to == AwaitingApproval || to == NeedsReporter || to == NeedsInfo || to == NeedsHuman || to == Conversation || to == Closed
	case Claimed:
		return to == Working || to == SetupFailed || to == Interrupted || to == NeedsHuman
	case Working:
		return Terminal(to) || to == PROpen || to == NeedsReporter || to == NeedsInfo || to == NeedsHuman || to == Held || to == AwaitingApproval
	case PROpen:
		return to == Done || to == Merged || to == Closed || to == ReviseRequested || to == ReviewPending || to == NeedsHuman
	case ReviseRequested, ReviewPending:
		return to == New || to == Revising || to == Working || to == PROpen || to == NeedsHuman || to == Closed || to == Done || to == Merged
	case Revising:
		return Terminal(to) || to == PROpen || to == NeedsHuman
	case Held, AwaitingApproval, NeedsReporter, NeedsInfo, NeedsHuman, Conversation:
		return to == New || to == Queued || to == AwaitingApproval || to == Held || to == Conversation || to == NeedsReporter || to == NeedsInfo || to == NeedsHuman || to == Closed || to == ReviseRequested || to == ReviewPending || to == PROpen || to == Done || to == Merged
	case Verified, NoChange, SetupFailed, AgentFailed, VerifyFailed, Interrupted, Failed, GuardBlocked:
		return to == New || (from != Verified && (to == Queued || to == Held)) || (from == Interrupted && (to == Working || to == NeedsHuman)) || to == PROpen || (from == Verified && (to == NeedsHuman || to == GuardBlocked || to == Done || to == Merged || to == Closed))
	case Done, Merged:
		return to == AwaitingApproval || to == Held
	case Closed:
		return to == New || to == AwaitingApproval || to == Held
	}
	return false
}

//effectgo:errors ErrTransition
func CheckTransition(from, to Status) error {
	if !CanTransition(from, to) {
		return ErrTransition
	}
	return nil
}
