package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
	"golang.org/x/sys/unix"
)

type Result struct {
	Source          string `json:"source"`
	Destination     string `json:"destination"`
	Reports         int    `json:"reports"`
	Messages        int    `json:"messages"`
	Events          int    `json:"events"`
	PendingIntakes  int    `json:"pending_intakes"`
	AlreadyImported bool   `json:"already_imported"`
}

type manifest struct {
	Result
	Digest string `json:"digest"`
}

type tableSnapshot struct {
	Name       string           `json:"name"`
	Columns    []string         `json:"columns"`
	Rows       []map[string]any `json:"rows"`
	ValueTypes [][]string       `json:"value_types"`
}

// Import reads an offline Python home into a fresh Go home. A complete SQLite
// read transaction, original configuration and evidence are retained. The only
// publication is an atomic rename after the database and files have been synced.
func Import(ctx context.Context, sourceHome, destinationHome string) (result Result, err error) {
	result = Result{Source: sourceHome, Destination: destinationHome}
	if err = canonicalDirectory(sourceHome); err != nil {
		return result, fmt.Errorf("source home: %w", err)
	}
	if err = worker.CheckHome(destinationHome); err != nil {
		return result, err
	}
	if overlaps(sourceHome, destinationHome) {
		return result, errors.New("source and destination homes must not overlap")
	}
	if err = requireOffline(sourceHome); err != nil {
		return result, err
	}
	for _, name := range []string{"config.toml", "store.sqlite"} {
		info, statErr := os.Lstat(filepath.Join(sourceHome, name))
		if statErr != nil {
			return result, statErr
		}
		if !info.Mode().IsRegular() {
			return result, fmt.Errorf("source %s must be a regular file", name)
		}
	}
	configData, err := os.ReadFile(filepath.Join(sourceHome, "config.toml"))
	if err != nil {
		return result, err
	}
	cfg, err := ParseConfig(configData)
	if err != nil {
		return result, err
	}
	if overlaps(destinationHome, cfg.Repo) {
		return result, errors.New("destination home must not overlap repository clone")
	}
	if err = cfg.Validate(); err != nil {
		return result, fmt.Errorf("import config: %w", err)
	}
	// A sibling lock serializes concurrent importers without creating the destination.
	lock, err := os.OpenFile(destinationHome+".import.lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return result, fmt.Errorf("another import is active: %w", err)
	}
	dsn := url.URL{Scheme: "file", Path: filepath.Join(sourceHome, "store.sqlite"), RawQuery: "mode=ro&_pragma=busy_timeout(10000)&_pragma=query_only(1)"}
	source, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	source.SetMaxOpenConns(1)
	var versionBefore int
	if err = source.QueryRowContext(ctx, "PRAGMA data_version").Scan(&versionBefore); err != nil {
		return result, err
	}
	tx, err := source.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return result, err
	}
	defer func() {
		if e := tx.Rollback(); e != nil && !errors.Is(e, sql.ErrTxDone) {
			err = errors.Join(err, e)
		}
	}()
	snapshots, err := snapshotTables(ctx, tx)
	if err != nil {
		return result, err
	}
	reports, messages, events, err := convertRows(snapshots, cfg)
	if err != nil {
		return result, err
	}
	result.Reports, result.Messages, result.Events = len(reports), len(messages), len(events)
	stage, err := os.MkdirTemp(filepath.Dir(destinationHome), ".feedback-import-")
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(stage)) }()
	archive := filepath.Join(stage, "legacy")
	if err = os.Mkdir(archive, 0700); err != nil {
		return result, err
	}
	raw, err := json.Marshal(snapshots)
	if err != nil {
		return result, err
	}
	if err = writeSynced(filepath.Join(archive, "database.json"), raw); err != nil {
		return result, err
	}
	hashes := map[string]string{"database.json": digest(raw)}
	if err = copyEvidence(ctx, sourceHome, stage, hashes); err != nil {
		return result, err
	}
	if hashes["files/config.toml"] != digest(configData) {
		return result, errors.New("source configuration changed during import")
	}
	pending, err := failedIntakes(filepath.Join(archive, "files", "intake-failed.jsonl"), messages)
	if err != nil {
		return result, err
	}
	result.PendingIntakes = len(pending)
	for _, r := range reports {
		switch r.Status {
		case model.PROpen, model.Done, model.Merged, model.Closed, model.NoChange, model.SetupFailed, model.AgentFailed, model.VerifyFailed, model.Failed, model.GuardBlocked, model.NeedsHuman, model.NeedsReporter, model.NeedsInfo, model.AwaitingApproval:
			pending = append(pending, model.Reservation{Key: fmt.Sprintf("result:%d:%d:%d:%s:complete", r.ID, r.Attempt, r.ReviewRounds, r.Status), Kind: "result", ReportID: r.ID, State: "complete", Result: "imported legacy outcome; do not automatically redeliver", CreatedAt: r.UpdatedAt, UpdatedAt: r.UpdatedAt})
		case model.Claimed, model.Conversation, model.Held, model.Interrupted, model.New, model.Queued, model.ReviewPending, model.ReviseRequested, model.Revising, model.Verified, model.Working:
			break
		}
	}
	if err = tx.Commit(); err != nil {
		return result, err
	}
	var versionAfter int
	if err = source.QueryRowContext(ctx, "PRAGMA data_version").Scan(&versionAfter); err != nil {
		return result, err
	}
	if versionBefore != versionAfter {
		return result, errors.New("source database changed during import; stop Python services before importing")
	}
	if err = requireOffline(sourceHome); err != nil {
		return result, err
	}
	hashData, err := json.Marshal(hashes)
	if err != nil {
		return result, err
	}
	fingerprint := digest(hashData)
	if _, statErr := os.Lstat(destinationHome); statErr == nil {
		existingData, readErr := os.ReadFile(filepath.Join(destinationHome, "migration.json"))
		if readErr != nil {
			return result, errors.New("destination exists without a migration manifest; refusing to overwrite")
		}
		var existing manifest
		if err = json.Unmarshal(existingData, &existing); err != nil {
			return result, err
		}
		if existing.Source != sourceHome || existing.Destination != destinationHome || existing.Digest != fingerprint {
			return result, errors.New("destination contains another or changed source snapshot")
		}
		if _, err = worker.Load(destinationHome); err != nil {
			return result, fmt.Errorf("existing imported configuration: %w", err)
		}
		result.AlreadyImported = true
		return result, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return result, statErr
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return result, err
	}
	if err = writeSynced(filepath.Join(stage, "config.json"), data); err != nil {
		return result, err
	}
	db, err := store.Open(filepath.Join(stage, "go-store.sqlite"))
	if err != nil {
		return result, err
	}
	importErr := db.Import(ctx, reports, messages, events, pending...)
	if err = errors.Join(importErr, db.Close()); err != nil {
		return result, err
	}
	data, err = json.MarshalIndent(manifest{Result: result, Digest: fingerprint}, "", "  ")
	if err != nil {
		return result, err
	}
	if err = writeSynced(filepath.Join(stage, "migration.json"), data); err != nil {
		return result, err
	}
	if err = syncTree(stage); err != nil {
		return result, err
	}
	if err = unix.Renameat2(unix.AT_FDCWD, stage, unix.AT_FDCWD, destinationHome, unix.RENAME_NOREPLACE); err != nil {
		return result, err
	}
	return result, syncDirectory(filepath.Dir(destinationHome))
}

func canonicalDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return errors.New("must be an absolute clean directory")
	}
	actual, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if actual != path {
		return errors.New("symlinked homes are unsafe")
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("home is not a directory")
	}
	return nil
}

func overlaps(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+string(filepath.Separator)) || strings.HasPrefix(b, a+string(filepath.Separator))
}

func snapshotTables(ctx context.Context, tx *sql.Tx) ([]tableSnapshot, error) {
	schema, err := readTable(ctx, tx, "sqlite_schema")
	if err != nil {
		return nil, err
	}
	tables := []tableSnapshot{schema}
	var names []string
	for _, row := range schema.Rows {
		if row["type"] == "table" {
			names = append(names, fmt.Sprint(row["name"]))
		}
	}
	slices.Sort(names)
	for _, name := range names {
		table, err := readTable(ctx, tx, name)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func readTable(ctx context.Context, tx *sql.Tx, name string) (table tableSnapshot, err error) {
	table.Name = name
	rows, err := tx.QueryContext(ctx, `SELECT * FROM "`+strings.ReplaceAll(name, `"`, `""`)+`"`)
	if err != nil {
		return table, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	table.Columns, err = rows.Columns()
	if err != nil {
		return table, err
	}
	values := make([]any, len(table.Columns))
	pointers := make([]any, len(values))
	for i := range values {
		pointers[i] = &values[i]
	}
	for rows.Next() {
		if err = rows.Scan(pointers...); err != nil {
			return table, err
		}
		row := make(map[string]any, len(values))
		for i, value := range values {
			row[table.Columns[i]] = value
		}
		types := make([]string, len(values))
		for i, value := range values {
			switch value.(type) {
			case nil:
				types[i] = "null"
			case int64:
				types[i] = "integer"
			case float64:
				types[i] = "real"
			case string:
				types[i] = "text"
			case []byte:
				types[i] = "blob"
			default:
				return table, fmt.Errorf("unsupported SQLite value type %T", value)
			}
		}
		table.ValueTypes = append(table.ValueTypes, types)
		table.Rows = append(table.Rows, row)
	}
	return table, rows.Err()
}

func convertRows(tables []tableSnapshot, cfg worker.Config) ([]model.Report, []model.Message, []model.Event, error) {
	var reports []model.Report
	var messages []model.Message
	var events []model.Event
	haveReports, haveEvents := false, false
	messageOwners := map[string]int64{}
	for _, table := range tables {
		switch table.Name {
		case "reports":
			haveReports = true
			for _, row := range table.Rows {
				copyRow := make(map[string]any, len(row))
				for key, value := range row {
					copyRow[key] = value
				}
				copyRow["attempt"] = row["attempts"]
				if draft, ok := copyRow["pr_draft"].(int64); ok {
					copyRow["pr_draft"] = draft != 0
				}
				data, err := json.Marshal(copyRow)
				if err != nil {
					return nil, nil, nil, err
				}
				var r model.Report
				if err = json.Unmarshal(data, &r); err != nil {
					return nil, nil, nil, fmt.Errorf("legacy report %v: %w", row["id"], err)
				}
				if !model.Known(r.Status) {
					return nil, nil, nil, fmt.Errorf("legacy report %d has unknown state %q", r.ID, r.Status)
				}
				switch r.Status {
				case model.Working, model.Claimed, model.Revising:
					return nil, nil, nil, fmt.Errorf("legacy report %d is active (%s); stop and reconcile active jobs before import", r.ID, r.Status)
				case model.AgentFailed, model.AwaitingApproval, model.Closed, model.Conversation, model.Done, model.Failed, model.GuardBlocked, model.Held, model.Interrupted, model.Merged, model.NeedsHuman, model.NeedsInfo, model.NeedsReporter, model.New, model.NoChange, model.PROpen, model.Queued, model.ReviewPending, model.ReviseRequested, model.SetupFailed, model.Verified, model.VerifyFailed:
					break
				}
				if r.ID <= 0 || r.CreatedAt == "" || r.UpdatedAt == "" {
					return nil, nil, nil, fmt.Errorf("legacy report %d lacks identity or timestamps", r.ID)
				}
				r.Repository = cfg.GitHubRepo
				r.RetryBase = 0
				if r.Source == "discord" {
					var ref map[string]any
					decoder := json.NewDecoder(strings.NewReader(r.SourceRef))
					decoder.UseNumber()
					if err = decoder.Decode(&ref); err != nil {
						return nil, nil, nil, fmt.Errorf("legacy report %d source_ref: %w", r.ID, err)
					}
					if ref["message"] != nil {
						r.MessageID = fmt.Sprint(ref["message"])
					}
					if ref["channel"] != nil {
						r.ChannelID = fmt.Sprint(ref["channel"])
					}
					if r.MessageID != "" {
						r.SourceKey = "discord:" + r.MessageID
					}
				}
				r.LegacyWorktree, r.Worktree = r.Worktree, ""
				reports = append(reports, r)
			}
		case "messages":
			for _, row := range table.Rows {
				m := model.Message{ID: text(row["discord_message_id"]), ReportID: integer(row["report_id"]), Role: text(row["role"]), At: text(row["at"])}
				if m.ID == "" || m.ReportID <= 0 || (m.Role != "report" && m.Role != "bot") || m.At == "" {
					return nil, nil, nil, errors.New("invalid legacy message mapping")
				}
				messageOwners[m.ID] = m.ReportID
				messages = append(messages, m)
			}
		case "events":
			haveEvents = true
			for _, row := range table.Rows {
				events = append(events, model.Event{ID: integer(row["id"]), ReportID: integer(row["report_id"]), Kind: text(row["kind"]), Detail: text(row["detail"]), At: text(row["at"])})
			}
		}
	}
	if !haveReports || !haveEvents {
		return nil, nil, nil, errors.New("source is not a Python feedback-loop database (reports/events required)")
	}
	for _, r := range reports {
		if r.MessageID != "" {
			if _, exists := messageOwners[r.MessageID]; !exists {
				messages = append(messages, model.Message{ID: r.MessageID, ReportID: r.ID, Role: "report", At: r.CreatedAt})
				messageOwners[r.MessageID] = r.ID
			}
		}
	}
	return reports, messages, events, nil
}

func text(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}
func integer(value any) int64   { n, _ := value.(int64); return n }
func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

// Symlinks are recorded, never followed or recreated into the running home.
// All regular source files are preserved, including unknown evidence files.
func copyEvidence(ctx context.Context, source, stage string, hashes map[string]string) error {
	links := map[string]string{}
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil || relative == "." {
			return err
		}
		switch relative {
		case ".venv", "venv", "__pycache__", ".pytest_cache", "tests", "go", ".git":
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Dir(relative) == "." && strings.HasSuffix(relative, ".py") {
			return nil
		}
		if relative == "store.sqlite" || relative == "store.sqlite-wal" || relative == "store.sqlite-shm" {
			return nil
		}
		destination := filepath.Join(stage, "legacy", "files", relative)
		if relative == "jobs" || strings.HasPrefix(relative, "jobs"+string(filepath.Separator)) {
			destination = filepath.Join(stage, relative)
		}
		if entry.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			links[relative] = target
			return nil
		}
		if entry.IsDir() {
			return os.MkdirAll(destination, 0700)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unsupported evidence file type: %s", relative)
		}
		sourceFile, err := os.Open(path)
		if err != nil {
			return err
		}
		before, err := sourceFile.Stat()
		if err != nil {
			return errors.Join(err, sourceFile.Close())
		}
		if err = os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
			return errors.Join(err, sourceFile.Close())
		}
		output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return errors.Join(err, sourceFile.Close())
		}
		h := sha256.New()
		_, copyErr := io.Copy(io.MultiWriter(output, h), sourceFile)
		after, statErr := sourceFile.Stat()
		err = errors.Join(copyErr, statErr, output.Sync(), output.Close(), sourceFile.Close())
		if err != nil {
			return err
		}
		if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
			return fmt.Errorf("evidence changed during import: %s", relative)
		}
		hashes["files/"+relative] = hex.EncodeToString(h.Sum(nil))
		return nil
	})
	if err != nil {
		return err
	}
	data, err := json.Marshal(links)
	if err != nil {
		return err
	}
	hashes["symlinks.json"] = digest(data)
	return writeSynced(filepath.Join(stage, "legacy", "symlinks.json"), data)
}

func writeSynced(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	return errors.Join(err, file.Sync(), file.Close())
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func syncTree(root string) error {
	var directories []string
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			directories = append(directories, path)
		}
		return nil
	}); err != nil {
		return err
	}
	for i := len(directories) - 1; i >= 0; i-- {
		if err := syncDirectory(directories[i]); err != nil {
			return err
		}
	}
	return nil
}
