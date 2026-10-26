package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/FaintGhost/loom/internal/model"
	_ "modernc.org/sqlite"
)

const applicationID = 0x4642474f // FBGO; deliberately incompatible with the Python database.
const schemaVersion = 2
const reportColumns = "id, body, status, attempt, worktree, error, created_at, updated_at, metadata"
const recoveryReason = "worker stopped before completing this attempt; explicit retry required"

const schema = `
CREATE TABLE reports (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 body TEXT NOT NULL,
 status TEXT NOT NULL CHECK (status IN ('new','working','verified','no_change','setup_failed','agent_failed','verify_failed','interrupted','failed','claimed','queued','awaiting_approval','held','needs_reporter','needs_info','needs_human','guard_blocked','pr_open','revise_requested','revising','review_pending','done','merged','closed','conversation')),
 attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
 worktree TEXT NOT NULL DEFAULT '',
 error TEXT NOT NULL DEFAULT '',
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 metadata TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata))
);
CREATE INDEX reports_queue ON reports(status, id);
CREATE UNIQUE INDEX reports_source_key ON reports(json_extract(metadata, '$.source_key')) WHERE json_extract(metadata, '$.source_key') IS NOT NULL AND json_extract(metadata, '$.source_key') != '';
CREATE TABLE events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 report_id INTEGER,
 from_status TEXT NOT NULL,
 to_status TEXT NOT NULL,
 detail TEXT NOT NULL,
 kind TEXT NOT NULL DEFAULT '',
 time TEXT NOT NULL
);
CREATE INDEX events_report ON events(report_id, id);
CREATE TABLE messages (
 message_id TEXT PRIMARY KEY,
 report_id INTEGER NOT NULL REFERENCES reports(id),
 role TEXT NOT NULL CHECK(role IN ('report','bot')),
 at TEXT NOT NULL
);
CREATE TABLE reservations (
 key TEXT PRIMARY KEY,
 kind TEXT NOT NULL,
 report_id INTEGER REFERENCES reports(id),
 payload TEXT NOT NULL,
 result TEXT NOT NULL DEFAULT '',
 state TEXT NOT NULL CHECK(state IN ('reserved','complete')),
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL
);
PRAGMA application_id = 1178748751;
PRAGMA user_version = 2;
`

// Store owns one connection. SQLite's immediate transactions serialize writers
// across Store instances and processes, including first-time schema creation.
type Store struct {
	db *sql.DB
}

func Open(path string) (store *Store, err error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("database path is empty")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	// The file lock makes the read-only ownership probe race-free with another
	// process's first initialization. Flock does not alter database contents.
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	var db *sql.DB
	defer func() {
		err = errors.Join(err, lock.Close()) // Closing the descriptor releases flock.
		if err != nil && db != nil {
			err = errors.Join(err, db.Close())
			store = nil
		}
	}()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return nil, err
	}
	info, err := lock.Stat()
	if err != nil {
		return nil, err
	}
	existingData := info.Size() > 0
	if existingData {
		if err = checkOwnership(path); err != nil {
			return nil, err
		}
	}
	dsn := url.URL{Scheme: "file", Path: path}
	query := url.Values{
		"_pragma": {"busy_timeout(10000)", "foreign_keys(1)"},
		"_txlock": {"immediate"},
	}
	dsn.RawQuery = query.Encode()
	db, err = sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store = &Store{db: db}
	if err = store.initialize(existingData); err != nil {
		return nil, err
	}
	return store, nil
}

// A read-only connection cannot checkpoint or rewrite a foreign WAL database
// when closed. Never open an existing unowned database through the writer DSN.
func checkOwnership(path string) (err error) {
	dsn := url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro&_pragma=busy_timeout(10000)"}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	db.SetMaxOpenConns(1)
	var appID, version int
	if err = db.QueryRow("PRAGMA application_id").Scan(&appID); err != nil {
		return err
	}
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if appID != applicationID || (version != 1 && version != schemaVersion) {
		return fmt.Errorf("refusing incompatible database (application_id=%d, user_version=%d)", appID, version)
	}
	return nil
}

func (s *Store) initialize(existingData bool) (err error) {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx, &err)
	var appID, version, objects int
	if err = tx.QueryRowContext(ctx, "PRAGMA application_id").Scan(&appID); err != nil {
		return err
	}
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if appID == applicationID && version == 1 {
		// Validate the original schema before writing, then rebuild its state constraint.
		rows, queryErr := tx.QueryContext(ctx, "SELECT id, body, status, attempt, worktree, error, created_at, updated_at FROM reports LIMIT 0")
		if queryErr != nil {
			return fmt.Errorf("incompatible store schema: %w", queryErr)
		}
		if err = rows.Close(); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `CREATE TEMP TABLE old_reports AS SELECT * FROM reports;
		CREATE TEMP TABLE old_events AS SELECT * FROM events;
		DROP TABLE events; DROP TABLE reports;`)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, schema); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO reports(id,body,status,attempt,worktree,error,created_at,updated_at) SELECT id,body,status,attempt,worktree,error,created_at,updated_at FROM old_reports;
		INSERT INTO events(id,report_id,from_status,to_status,detail,time) SELECT id,report_id,from_status,to_status,detail,time FROM old_events;
		DROP TABLE old_reports; DROP TABLE old_events;`)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	if appID == applicationID && version == schemaVersion {
		for _, query := range []string{
			"SELECT " + reportColumns + " FROM reports LIMIT 0",
			"SELECT id, report_id, from_status, to_status, detail, time FROM events LIMIT 0",
		} {
			rows, queryErr := tx.QueryContext(ctx, query)
			if queryErr != nil {
				return fmt.Errorf("incompatible store schema: %w", queryErr)
			}
			if closeErr := rows.Close(); closeErr != nil {
				return closeErr
			}
		}
		return tx.Commit()
	}
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema").Scan(&objects); err != nil {
		return err
	}
	if existingData || appID != 0 || version != 0 || objects != 0 {
		return fmt.Errorf("refusing incompatible database (application_id=%d, user_version=%d)", appID, version)
	}
	// SQL pragmas cannot bind parameters; these schema identifiers are constants.
	// The result of schema creation has no useful row count or insert ID.
	if _, err = tx.ExecContext(ctx, schema); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Close() error {
	return s.db.Close()
}

// rollback keeps rollback failures without treating an already committed or
// context-rolled-back transaction as a second failure.
func rollback(tx *sql.Tx, err *error) {
	if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
		*err = errors.Join(*err, rollbackErr)
	}
}

func timestamp() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

type scanner interface{ Scan(...any) error }

func scanReport(row scanner) (r model.Report, err error) {
	var metadata string
	err = row.Scan(&r.ID, &r.Body, &r.Status, &r.Attempt, &r.Worktree, &r.Error, &r.CreatedAt, &r.UpdatedAt, &metadata)
	if errors.Is(err, sql.ErrNoRows) {
		return r, model.ErrNotFound
	}
	if err != nil {
		return r, err
	}
	var extra model.Report
	if err = json.Unmarshal([]byte(metadata), &extra); err != nil {
		return r, err
	}
	extra.ID, extra.Body, extra.Status, extra.Attempt = r.ID, r.Body, r.Status, r.Attempt
	extra.Worktree, extra.Error, extra.CreatedAt, extra.UpdatedAt = r.Worktree, r.Error, r.CreatedAt, r.UpdatedAt
	return extra, nil
}

func readReports(rows *sql.Rows) (reports []model.Report, err error) {
	defer func() { err = errors.Join(err, rows.Close()) }()
	reports = []model.Report{}
	for rows.Next() {
		r, scanErr := scanReport(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		reports = append(reports, r)
	}
	return reports, rows.Err()
}

func event(ctx context.Context, tx *sql.Tx, r model.Report, from model.Status, detail string) error {
	// Event IDs and affected-row counts are not needed; insertion errors are mandatory.
	_, err := tx.ExecContext(ctx, "INSERT INTO events(report_id, from_status, to_status, detail, time) VALUES (?, ?, ?, ?, ?)", r.ID, from, r.Status, detail, r.UpdatedAt)
	return err
}

func (s *Store) Add(ctx context.Context, body string) (r model.Report, err error) {
	if strings.TrimSpace(body) == "" {
		return r, model.ErrInvalidReport
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return r, err
	}
	defer rollback(tx, &err)
	now := timestamp()
	r, err = scanReport(tx.QueryRowContext(ctx, "INSERT INTO reports(body, status, created_at, updated_at) VALUES (?, ?, ?, ?) RETURNING "+reportColumns, body, model.New, now, now))
	if err != nil {
		return r, err
	}
	if err = event(ctx, tx, r, "", "report added"); err != nil {
		return r, err
	}
	return r, tx.Commit()
}

func (s *Store) List(ctx context.Context) ([]model.Report, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+reportColumns+" FROM reports ORDER BY id")
	if err != nil {
		return nil, err
	}
	return readReports(rows)
}

func (s *Store) Get(ctx context.Context, id int64) (model.Report, error) {
	return scanReport(s.db.QueryRowContext(ctx, "SELECT "+reportColumns+" FROM reports WHERE id = ?", id))
}

func (s *Store) Claim(ctx context.Context) (r model.Report, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return r, err
	}
	defer rollback(tx, &err)
	r, err = scanReport(tx.QueryRowContext(ctx, "SELECT "+reportColumns+" FROM reports WHERE status IN (?,?) ORDER BY id LIMIT 1", model.New, model.Queued))
	if errors.Is(err, model.ErrNotFound) {
		return r, model.ErrEmptyQueue
	}
	if err != nil {
		return r, err
	}
	from := r.Status
	if r.RetryReason == "" && r.Error != "" {
		r.RetryReason = r.Error
	}
	r.Status, r.Attempt, r.Worktree, r.Error, r.UpdatedAt = model.Working, r.Attempt+1, "", "", timestamp()
	if err = saveReport(ctx, tx, r); err != nil {
		return r, err
	}
	if err = event(ctx, tx, r, from, "attempt claimed"); err != nil {
		return r, err
	}
	return r, tx.Commit()
}

func (s *Store) SetWorktree(ctx context.Context, id int64, path string) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx, &err)
	r, err := scanReport(tx.QueryRowContext(ctx, "SELECT "+reportColumns+" FROM reports WHERE id = ?", id))
	if err != nil {
		return err
	}
	if r.Status != model.Working {
		return model.ErrTransition
	}
	// The locked read proves this row exists; the affected-row count adds no information.
	if _, err = tx.ExecContext(ctx, "UPDATE reports SET worktree = ?, updated_at = ? WHERE id = ?", path, timestamp(), id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Finish(ctx context.Context, id int64, status model.Status, detail string) error {
	_, err := s.Transition(ctx, id, model.Working, status, map[string]any{"error": detail})
	return err
}

func (s *Store) Retry(ctx context.Context, id int64) error {
	return s.transition(ctx, id, model.New, "")
}

func (s *Store) transition(ctx context.Context, id int64, status model.Status, detail string) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx, &err)
	r, err := scanReport(tx.QueryRowContext(ctx, "SELECT "+reportColumns+" FROM reports WHERE id = ?", id))
	if err != nil {
		return err
	}
	if r.PRNumber != 0 || !model.Terminal(r.Status) || (r.IssueNumber == 0 && (r.Kind == "question" || r.Kind == "noise" || r.Kind == "unclassified" || r.Status == model.Conversation)) {
		return model.ErrTransition
	}
	if !model.CanTransition(r.Status, status) {
		return model.ErrTransition
	}
	from := r.Status
	rememberFailure(&r)
	r.Status, r.Error, r.UpdatedAt = status, detail, timestamp()
	if err = saveReport(ctx, tx, r); err != nil {
		return err
	}
	if err = event(ctx, tx, r, from, detail); err != nil {
		return err
	}
	return tx.Commit()
}

// Recover requires the caller to hold the exclusive worker lock. It preserves
// attempts and worktree evidence and never places interrupted work back in queue.
func (s *Store) Recover(ctx context.Context) (reports []model.Report, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer rollback(tx, &err)
	if !model.CanTransition(model.Working, model.Interrupted) {
		return nil, model.ErrTransition
	}
	rows, err := tx.QueryContext(ctx, "SELECT "+reportColumns+" FROM reports WHERE status = ? ORDER BY id", model.Working)
	if err != nil {
		return nil, err
	}
	reports, err = readReports(rows)
	if err != nil {
		return nil, err
	}
	for i := range reports {
		r := &reports[i]
		r.Status, r.Error, r.UpdatedAt = model.Interrupted, recoveryReason, timestamp()
		// Every selected row is held by this write transaction; only execution errors matter.
		if _, err = tx.ExecContext(ctx, "UPDATE reports SET status = ?, error = ?, updated_at = ? WHERE id = ?", r.Status, r.Error, r.UpdatedAt, r.ID); err != nil {
			return nil, err
		}
		if err = event(ctx, tx, *r, model.Working, recoveryReason); err != nil {
			return nil, err
		}
	}
	return reports, tx.Commit()
}
