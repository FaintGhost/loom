package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/FaintGhost/loom/internal/model"
)

func insertReport(ctx context.Context, tx *sql.Tx, r model.Report) (model.Report, error) {
	if r.Status == "" {
		r.Status = model.New
	}
	if !model.Known(r.Status) {
		return r, fmt.Errorf("unknown report status %q", r.Status)
	}
	if r.RetryBase < 0 || r.RetryBase > r.Attempt {
		return r, errors.New("retry baseline must be within lifetime attempts")
	}
	if r.CreatedAt == "" {
		r.CreatedAt = timestamp()
	}
	if r.UpdatedAt == "" {
		r.UpdatedAt = r.CreatedAt
	}
	data, err := json.Marshal(r)
	if err != nil {
		return r, err
	}
	var id any
	if r.ID != 0 {
		id = r.ID
	}
	return scanReport(tx.QueryRowContext(ctx, "INSERT INTO reports(id,body,status,attempt,worktree,error,created_at,updated_at,metadata) VALUES (?,?,?,?,?,?,?,?,?) RETURNING "+reportColumns,
		id, r.Body, r.Status, r.Attempt, r.Worktree, r.Error, r.CreatedAt, r.UpdatedAt, string(data)))
}

func hasContent(r model.Report) bool {
	if strings.TrimSpace(r.Body) != "" {
		return true
	}
	var attachments []json.RawMessage
	return json.Unmarshal([]byte(r.Attachments), &attachments) == nil && len(attachments) > 0
}

// AddReport reserves the source identity and inserts the report in one transaction.
// Concurrent deliveries of the same source return the original report.
func (s *Store) AddReport(ctx context.Context, r model.Report) (out model.Report, err error) {
	if !hasContent(r) {
		return out, model.ErrInvalidReport
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer rollback(tx, &err)
	if r.SourceKey != "" {
		out, err = scanReport(tx.QueryRowContext(ctx, "SELECT "+reportColumns+" FROM reports WHERE json_extract(metadata,'$.source_key') = ?", r.SourceKey))
		if err == nil {
			return out, tx.Commit()
		}
		if !errors.Is(err, model.ErrNotFound) {
			return out, err
		}
	}
	out, err = insertReport(ctx, tx, r)
	if err != nil {
		return out, err
	}
	if out.MessageID != "" {
		if _, err = tx.ExecContext(ctx, "INSERT INTO messages(message_id,report_id,role,at) VALUES (?,?,'report',?)", out.MessageID, out.ID, out.CreatedAt); err != nil {
			return out, err
		}
	}
	if err = event(ctx, tx, out, "", "report added"); err != nil {
		return out, err
	}
	return out, tx.Commit()
}

func patchReport(r model.Report, fields map[string]any) (model.Report, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return r, err
	}
	var values map[string]json.RawMessage
	if err = json.Unmarshal(data, &values); err != nil {
		return r, err
	}
	for key, value := range fields {
		switch key {
		case "id", "status", "created_at", "updated_at", "attempt", "retry_base", "review_rounds", "source_key", "followups":
			return r, fmt.Errorf("field %q requires a dedicated atomic operation", key)
		}
		encoded, encodeErr := json.Marshal(value)
		if encodeErr != nil {
			return r, encodeErr
		}
		values[key] = encoded
	}
	data, err = json.Marshal(values)
	if err != nil {
		return r, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var out model.Report
	if err = decoder.Decode(&out); err != nil {
		return r, err
	}
	if out.Body != r.Body && !hasContent(out) {
		return r, model.ErrInvalidReport
	}
	// An issue or PR identity may only be assigned once, never replaced by a follow-up.
	if r.IssueNumber != 0 && out.IssueNumber != r.IssueNumber || r.PRNumber != 0 && out.PRNumber != r.PRNumber {
		return r, model.ErrConflict
	}
	out.UpdatedAt = timestamp()
	return out, nil
}

func saveReport(ctx context.Context, tx *sql.Tx, r model.Report) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE reports SET body=?,status=?,attempt=?,worktree=?,error=?,updated_at=?,metadata=? WHERE id=?", r.Body, r.Status, r.Attempt, r.Worktree, r.Error, r.UpdatedAt, string(data), r.ID)
	return err
}

func (s *Store) Patch(ctx context.Context, id int64, fields map[string]any) (out model.Report, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer rollback(tx, &err)
	out, err = scanReport(tx.QueryRowContext(ctx, "SELECT "+reportColumns+" FROM reports WHERE id=?", id))
	if err != nil {
		return out, err
	}
	out, err = patchReport(out, fields)
	if err != nil {
		return out, err
	}
	if err = saveReport(ctx, tx, out); err != nil {
		return out, err
	}
	return out, tx.Commit()
}

func (s *Store) Transition(ctx context.Context, id int64, from, to model.Status, fields map[string]any) (out model.Report, err error) {
	return s.changeReport(ctx, id, from, to, fields, "", "", 0)
}

// Requeue starts a new retry budget after an authorized follow-up or maintainer
// approval. Attempt remains monotonic because it identifies retained evidence.
func (s *Store) Requeue(ctx context.Context, id int64, from, to model.Status, fields map[string]any) (model.Report, error) {
	return s.changeReport(ctx, id, from, to, fields, "requeue", "", 0)
}

// RequeueMessage commits a message-specific reopen receipt with the state change.
// Replaying the same message returns the current row even after later work.
func (s *Store) RequeueMessage(ctx context.Context, id int64, from, to model.Status, fields map[string]any, messageID string, maxReopens int) (model.Report, error) {
	if strings.TrimSpace(messageID) == "" || maxReopens < 0 {
		return model.Report{}, errors.New("reopen requires a message id and nonnegative limit")
	}
	return s.changeReport(ctx, id, from, to, fields, "requeue", messageID, maxReopens)
}

func canRequeue(from, to model.Status) bool {
	if to != model.New && to != model.Queued && to != model.Held {
		return false
	}
	switch from {
	case model.AwaitingApproval, model.Held, model.NeedsInfo, model.NeedsReporter, model.NeedsHuman, model.NoChange, model.VerifyFailed, model.SetupFailed, model.AgentFailed, model.Failed, model.Interrupted, model.GuardBlocked:
		return true
	case model.Claimed, model.Closed, model.Conversation, model.Done, model.Merged, model.New, model.PROpen, model.Queued, model.ReviewPending, model.ReviseRequested, model.Revising, model.Verified, model.Working:
		return false
	}
	return false
}

// StartReview starts one authorized round on the existing PR. Its atomic counter
// update prevents stale callbacks from consuming two rounds or reusing evidence.
func (s *Store) StartReview(ctx context.Context, id int64, from model.Status, fields map[string]any) (model.Report, error) {
	switch from {
	case model.ReviseRequested, model.ReviewPending:
	case model.Closed, model.AwaitingApproval, model.Held, model.NeedsInfo, model.NeedsReporter, model.NeedsHuman, model.NoChange, model.VerifyFailed, model.SetupFailed, model.AgentFailed, model.Failed, model.Interrupted, model.GuardBlocked:
		if fields["decided_by"] != "maintainer" {
			return model.Report{}, model.ErrTransition
		}
	default:
		return model.Report{}, model.ErrTransition
	}
	return s.changeReport(ctx, id, from, model.New, fields, "review", "", 0)
}

func rememberFailure(r *model.Report) {
	if r.Error == "" {
		return
	}
	switch r.Status {
	case model.NoChange, model.SetupFailed, model.AgentFailed, model.VerifyFailed, model.Interrupted, model.Failed, model.GuardBlocked, model.NeedsHuman, model.NeedsReporter, model.NeedsInfo:
		r.RetryReason = r.Error
	case model.AwaitingApproval, model.Claimed, model.Closed, model.Conversation, model.Done, model.Held, model.Merged, model.New, model.PROpen, model.Queued, model.ReviewPending, model.ReviseRequested, model.Revising, model.Verified, model.Working:
		return
	}
}

func (s *Store) changeReport(ctx context.Context, id int64, from, to model.Status, fields map[string]any, operation, messageID string, maxReopens int) (out model.Report, err error) {
	legal := model.CanTransition(from, to)
	if messageID == "" && !legal {
		return out, model.ErrTransition
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer rollback(tx, &err)
	out, err = scanReport(tx.QueryRowContext(ctx, "SELECT "+reportColumns+" FROM reports WHERE id=?", id))
	if err != nil {
		return out, err
	}
	if messageID != "" {
		var count, seen int
		if err = tx.QueryRowContext(ctx, "SELECT count(*),coalesce(max(detail = ?),0) FROM events WHERE report_id=? AND kind='reopened'", messageID, id).Scan(&count, &seen); err != nil {
			return out, err
		}
		if seen != 0 {
			return out, tx.Commit()
		}
		if maxReopens > 0 && count >= maxReopens {
			return out, model.ErrReopenLimit
		}
	}
	if !legal || out.Status != from {
		return out, model.ErrTransition
	}
	if operation == "requeue" && (!canRequeue(from, to) || out.PRNumber != 0 || out.IssueNumber == 0) {
		return out, model.ErrTransition
	}
	if from == model.Closed && to == model.New && operation != "review" {
		return out, model.ErrTransition
	}
	if operation == "review" && out.PRNumber == 0 {
		return out, model.ErrTransition
	}
	if to == model.New || to == model.Queued || to == model.Held || to == model.Working || to == model.Revising {
		rememberFailure(&out)
	}
	out, err = patchReport(out, fields)
	if err != nil {
		return out, err
	}
	if operation != "" {
		out.RetryBase = out.Attempt
	}
	if operation == "review" {
		if out.PendingReview == "" {
			return out, errors.New("review restart requires retained feedback")
		}
		out.ReviewRounds++
	}
	if to == model.PROpen && out.PRNumber <= 0 {
		return out, model.ErrTransition
	}
	out.Status = to
	if (to == model.Working && from != model.Interrupted) || to == model.Revising {
		out.Attempt++
	}
	if err = saveReport(ctx, tx, out); err != nil {
		return out, err
	}
	detail := out.Error
	if operation != "" {
		detail = fmt.Sprintf("%s retry budget restarted after lifetime attempt %d: %s", operation, out.Attempt, detail)
	}
	if err = event(ctx, tx, out, from, detail); err != nil {
		return out, err
	}
	if messageID != "" {
		if _, err = tx.ExecContext(ctx, "INSERT INTO events(report_id,from_status,to_status,kind,detail,time) VALUES (?,?,?,'reopened',?,?)", out.ID, from, out.Status, messageID, out.UpdatedAt); err != nil {
			return out, err
		}
	}
	return out, tx.Commit()
}

func (s *Store) LinkMessage(ctx context.Context, messageID string, reportID int64, role string) (err error) {
	if messageID == "" {
		return errors.New("message id is empty")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx, &err)
	if _, err = tx.ExecContext(ctx, "INSERT INTO messages(message_id,report_id,role,at) VALUES (?,?,?,?) ON CONFLICT(message_id) DO NOTHING", messageID, reportID, role, timestamp()); err != nil {
		return err
	}
	var existing int64
	if err = tx.QueryRowContext(ctx, "SELECT report_id FROM messages WHERE message_id=?", messageID).Scan(&existing); err != nil {
		return err
	}
	if existing != reportID {
		return model.ErrConflict
	}
	return tx.Commit()
}

func (s *Store) ByMessage(ctx context.Context, messageID string) (model.Report, error) {
	return scanReport(s.db.QueryRowContext(ctx, "SELECT "+reportColumns+" FROM reports WHERE id=(SELECT report_id FROM messages WHERE message_id=?)", messageID))
}

func (s *Store) ByIssue(ctx context.Context, number int) (model.Report, error) {
	return scanReport(s.db.QueryRowContext(ctx, "SELECT "+reportColumns+" FROM reports WHERE json_extract(metadata,'$.issue_number')=? ORDER BY id DESC LIMIT 1", number))
}

func (s *Store) AppendFollowup(ctx context.Context, id int64, entry map[string]any) (out model.Report, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer rollback(tx, &err)
	out, err = scanReport(tx.QueryRowContext(ctx, "SELECT "+reportColumns+" FROM reports WHERE id=?", id))
	if err != nil {
		return out, err
	}
	var entries []map[string]any
	if out.Followups != "" {
		if err = json.Unmarshal([]byte(out.Followups), &entries); err != nil {
			return out, fmt.Errorf("report %d followups: %w", id, err)
		}
	}
	// Discord messages provide durable identity; repeated delivery cannot append twice.
	for _, key := range []string{"message_id", "message"} {
		if value, ok := entry[key].(string); ok && value != "" {
			for _, old := range entries {
				if old[key] == value {
					return out, tx.Commit()
				}
			}
		}
	}
	entries = append(entries, entry)
	data, err := json.Marshal(entries)
	if err != nil {
		return out, err
	}
	out.Followups, out.UpdatedAt = string(data), timestamp()
	if err = saveReport(ctx, tx, out); err != nil {
		return out, err
	}
	if err = event(ctx, tx, out, out.Status, "follow-up appended"); err != nil {
		return out, err
	}
	return out, tx.Commit()
}

func (s *Store) RecordEvent(ctx context.Context, id int64, kind, detail string) error {
	var reportID any
	if id != 0 {
		reportID = id
	}
	_, err := s.db.ExecContext(ctx, "INSERT INTO events(report_id,from_status,to_status,kind,detail,time) VALUES (?,'','',?,?,?)", reportID, kind, detail, timestamp())
	return err
}

func (s *Store) Events(ctx context.Context, id int64) (events []model.Event, err error) {
	rows, err := s.db.QueryContext(ctx, "SELECT id,coalesce(report_id,0),from_status,to_status,kind,detail,time FROM events WHERE (?=0 OR report_id=?) ORDER BY id", id, id)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		var e model.Event
		if err = rows.Scan(&e.ID, &e.ReportID, &e.FromStatus, &e.ToStatus, &e.Kind, &e.Detail, &e.At); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (s *Store) Reserve(ctx context.Context, key, kind string, reportID int64, payload string) (bool, error) {
	if key == "" || kind == "" {
		return false, errors.New("reservation key and kind are required")
	}
	var id any
	if reportID != 0 {
		id = reportID
	}
	now := timestamp()
	result, err := s.db.ExecContext(ctx, "INSERT INTO reservations(key,kind,report_id,payload,state,created_at,updated_at) VALUES (?,?,?,?,'reserved',?,?) ON CONFLICT(key) DO NOTHING", key, kind, id, payload, now, now)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}

func (s *Store) Complete(ctx context.Context, key, result string) error {
	res, err := s.db.ExecContext(ctx, "UPDATE reservations SET state='complete',result=?,updated_at=? WHERE key=? AND (state='reserved' OR result=?)", result, timestamp(), key, result)
	if err != nil {
		return err
	}
	count, err := res.RowsAffected()
	if err == nil && count == 0 {
		return model.ErrConflict
	}
	return err
}

const reservationColumns = "key,kind,coalesce(report_id,0),payload,result,state,created_at,updated_at"

func scanReservation(row scanner) (r model.Reservation, err error) {
	err = row.Scan(&r.Key, &r.Kind, &r.ReportID, &r.Payload, &r.Result, &r.State, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		err = model.ErrNotFound
	}
	return r, err
}

func (s *Store) Reservation(ctx context.Context, key string) (model.Reservation, error) {
	return scanReservation(s.db.QueryRowContext(ctx, "SELECT "+reservationColumns+" FROM reservations WHERE key=?", key))
}

func (s *Store) Pending(ctx context.Context, kind string) (items []model.Reservation, err error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+reservationColumns+" FROM reservations WHERE state='reserved' AND kind=? ORDER BY created_at,key", kind)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		r, scanErr := scanReservation(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, r)
	}
	return items, rows.Err()
}

// Import accepts a validated legacy snapshot only into an empty store. All rows,
// original IDs, message ownership, and event history commit together.
func (s *Store) Import(ctx context.Context, reports []model.Report, messages []model.Message, events []model.Event, reservations ...model.Reservation) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx, &err)
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM reports)+(SELECT count(*) FROM events)+(SELECT count(*) FROM messages)+(SELECT count(*) FROM reservations)").Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return errors.New("import destination store is not empty")
	}
	for _, r := range reports {
		if r.ID <= 0 {
			return errors.New("import report id must be positive")
		}
		if _, err = insertReport(ctx, tx, r); err != nil {
			return err
		}
	}
	for _, m := range messages {
		if _, err = tx.ExecContext(ctx, "INSERT INTO messages(message_id,report_id,role,at) VALUES (?,?,?,?)", m.ID, m.ReportID, m.Role, m.At); err != nil {
			return err
		}
	}
	for _, e := range events {
		var rid any
		if e.ReportID != 0 {
			rid = e.ReportID
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO events(id,report_id,from_status,to_status,kind,detail,time) VALUES (?,?,?,?,?,?,?)", e.ID, rid, e.FromStatus, e.ToStatus, e.Kind, e.Detail, e.At); err != nil {
			return err
		}
	}
	for _, r := range reservations {
		var rid any
		if r.ReportID != 0 {
			rid = r.ReportID
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO reservations(key,kind,report_id,payload,result,state,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)", r.Key, r.Kind, rid, r.Payload, r.Result, r.State, r.CreatedAt, r.UpdatedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}
