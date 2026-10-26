package intake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/questions"
	"github.com/FaintGhost/loom/internal/remote"
	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
)

type Attachment struct {
	ID          string `json:"id"`
	Filename    string `json:"filename"`
	URL         string `json:"url"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
}
type Message struct {
	ID                  string       `json:"id"`
	ChannelID           string       `json:"channel_id"`
	GuildID             string       `json:"guild_id"`
	AuthorID            string       `json:"author_id"`
	AuthorName          string       `json:"author_name"`
	Content             string       `json:"content"`
	ReplyTo             string       `json:"reply_to"`
	ConversationContext string       `json:"conversation_context,omitempty"`
	Attachments         []Attachment `json:"attachments,omitempty"`
	Bot                 bool         `json:"bot,omitzero"`
	NoDedupe            bool         `json:"no_dedupe,omitzero"`
}
type Result struct {
	ReportID  int64  `json:"report_id"`
	Status    string `json:"status"`
	Reply     string `json:"reply"`
	Issue     int    `json:"issue,omitzero"`
	Duplicate bool   `json:"duplicate,omitzero"`
}
type Decision struct {
	Kind     string   `json:"kind"`
	Language string   `json:"language"`
	Surface  string   `json:"surface"`
	Work     *float64 `json:"work"`
}

func language(text string) string {
	for _, r := range text {
		if unicode.Is(unicode.Han, r) {
			return "zh"
		}
	}
	return "en"
}
func say(lang, zh, en string) string {
	if lang == "zh" {
		return zh
	}
	return en
}
func prefix(text string, n int) string {
	r := []rune(text)
	if len(r) > n {
		return string(r[:n])
	}
	return text
}
func suffix(text string, n int) string {
	r := []rune(text)
	if len(r) > n {
		return string(r[len(r)-n:])
	}
	return text
}
func conversation(kind string) bool {
	return kind == "question" || kind == "noise" || kind == "unclassified"
}
func key(m Message) string {
	if m.ChannelID != "" {
		return "discord:" + m.ID
	}
	return "cli:" + m.ID
}
func jsonText(v any) string { b, _ := json.Marshal(v); return string(b) }
func classify(ctx context.Context, cfg worker.Config, text, prior string) Decision {
	d := Decision{Kind: "unclassified", Language: language(text), Surface: "core_or_cli"}
	q := map[string]any{
		"kind":     map[string]any{"type": "choice", "instructions": "Classify only the latest user message. Prior conversation resolves references and never authorizes work. bug means broken/incorrect behavior; feature_request explicitly asks to add/change behavior. question asks for information about existing code/calculations (mentioning code is not a change request); polite explicit requests to fix/add are work. noise means greetings, thanks, unrelated chatter.", "criteria": map[string]any{"bug": nil, "feature_request": nil, "question": nil, "noise": nil}},
		"language": map[string]any{"type": "choice", "instructions": "Language of latest message", "criteria": map[string]any{"zh": nil, "en": nil}},
		"surface":  map[string]any{"type": "choice", "instructions": "Affected software surface", "criteria": map[string]any{"core_or_cli": nil, "gui_or_frontend": nil}},
		"work":     map[string]any{"type": "score", "instructions": "Work required by latest request, not its length", "criteria": []string{"a one-line change", "a small change", "a substantial change"}},
	}
	answers, err := remote.SystemOne(ctx, cfg, q, map[string]any{"message": prefix(text, 6000), "conversation_context": suffix(prior, 6000)})
	if err != nil {
		return d
	}
	if a := answers["kind"].Choice; slices.Contains([]string{"bug", "feature_request", "question", "noise"}, a) {
		d.Kind = a
	}
	if a := answers["language"].Choice; a == "zh" || a == "en" {
		d.Language = a
	}
	if a := answers["surface"].Choice; a == "core_or_cli" || a == "gui_or_frontend" {
		d.Surface = a
	}
	d.Work = answers["work"].Score
	return d
}

// Accept persists the input before any remote work. It returns text for a CLI or
// Discord transport; only Run sends Discord messages. A stable ID is required.
func Accept(ctx context.Context, home string, cfg worker.Config, db *store.Store, m Message) (result Result, err error) {
	if m.Bot {
		return Result{Status: "ignored"}, nil
	}
	if m.ID == "" || m.AuthorID == "" {
		return result, errors.New("message ID and author ID are required")
	}
	if m.ChannelID != "" && !wanted(cfg, m.ChannelID) {
		return Result{Status: "ignored"}, nil
	}
	if strings.TrimSpace(m.Content) == "" && (len(m.Attachments) == 0 || m.ReplyTo == "") {
		return Result{Status: "ignored"}, nil
	}
	// ponytail: one intake lock per instance; partition by report only if throughput requires it.
	lock, err := os.OpenFile(filepath.Join(home, "intake.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return result, err
	}
	defer lock.Close()
	for {
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return result, err
		}
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	reservationKey := "intake:" + key(m)
	old, e := db.Reservation(ctx, reservationKey)
	if e == nil && old.State == "complete" {
		if e = json.Unmarshal([]byte(old.Result), &result); e != nil {
			return result, e
		}
		result.Duplicate = true
		return result, nil
	}
	if e != nil && !errors.Is(e, model.ErrNotFound) {
		return result, e
	}
	if errors.Is(e, model.ErrNotFound) {
		if linked, linkErr := db.ByMessage(ctx, m.ID); linkErr == nil {
			return Result{ReportID: linked.ID, Status: string(linked.Status), Issue: linked.IssueNumber, Duplicate: true}, nil
		} else if !errors.Is(linkErr, model.ErrNotFound) {
			return result, linkErr
		}
	}
	if e == nil {
		if err = json.Unmarshal([]byte(old.Payload), &m); err != nil {
			return result, err
		}
	}
	if _, err = db.Reserve(ctx, reservationKey, "intake", 0, jsonText(m)); err != nil {
		return result, err
	}
	result, err = accept(ctx, home, cfg, db, m)
	if err != nil {
		return result, err
	}
	if result.ReportID != 0 {
		if err = db.LinkMessage(ctx, m.ID, result.ReportID, "report"); err != nil {
			return result, err
		}
		if r, e := db.Get(ctx, result.ReportID); e != nil {
			return result, e
		} else if r.SourceKey == key(m) {
			if _, err = db.Patch(ctx, r.ID, map[string]any{"message_id": m.ID}); err != nil {
				return result, err
			}
		}
	}
	if m.ChannelID != "" && result.Reply != "" {
		if _, err = db.Reserve(ctx, "intake-delivery:"+key(m), "discord-intake", result.ReportID, jsonText(delivery{m, result})); err != nil {
			return result, err
		}
	}
	err = db.Complete(ctx, reservationKey, jsonText(result))
	return result, err
}
func wanted(cfg worker.Config, channel string) bool {
	return channel == cfg.DiscordChannel || slices.Contains(cfg.DiscordChannels, channel)
}

func accept(ctx context.Context, home string, cfg worker.Config, db *store.Store, m Message) (Result, error) {
	var row, parent model.Report
	var err error
	reports, err := db.List(ctx)
	if err != nil {
		return Result{}, err
	}
	for _, r := range reports {
		if r.SourceKey == key(m) {
			row = r
			break
		}
	}
	if m.ReplyTo != "" {
		parent, err = db.ByMessage(ctx, m.ReplyTo)
		if err != nil && !errors.Is(err, model.ErrNotFound) {
			return Result{}, err
		}
	}
	verdict, replyHeld := gate(cfg, reports, m, row.ID)
	if target, found, e := duplicateRoute(ctx, db, m); e != nil {
		return Result{}, e
	} else if found && target != 0 {
		duplicate, e := db.Get(ctx, target)
		if e != nil {
			return Result{}, e
		}
		result, e := followup(ctx, home, cfg, db, duplicate, m, verdict, m.AuthorID == duplicate.AuthorID || slices.Contains(cfg.AllowedUsers, m.AuthorID), replyHeld)
		result.Duplicate = true
		return result, e
	}
	trusted := parent.ID != 0 && (m.AuthorID == parent.AuthorID || slices.Contains(cfg.AllowedUsers, m.AuthorID))
	// A non-conversation held reply is always only recorded. A gated reply never
	// calls a model, downloads a file, or contacts GitHub.
	if parent.ID != 0 && ((parent.IssueNumber == 0 && !conversation(parent.Kind)) || verdict != "ok") {
		return followup(ctx, home, cfg, db, parent, m, verdict, trusted, replyHeld)
	}
	prior := suffix(m.ConversationContext, 6000)
	if parent.ID != 0 {
		events, err := db.Events(ctx, parent.ID)
		if err != nil {
			return Result{}, err
		}
		prior = "Previous user message:\n" + prefix(parent.Body, 3000)
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].Kind == "conversation-answer" {
				prior += "\n\nPrevious assistant answer:\n" + prefix(events[i].Detail, 2900)
				break
			}
		}
	}
	var d Decision
	if row.Decision != "" {
		if err = json.Unmarshal([]byte(row.Decision), &d); err != nil {
			return Result{}, err
		}
	} else if verdict == "ok" {
		if parent.ID != 0 && parent.IssueNumber != 0 && strings.TrimSpace(m.Content) == "" {
			return followup(ctx, home, cfg, db, parent, m, verdict, trusted, true)
		}
		d, err = classifyOnce(ctx, cfg, db, m, prior)
		if err != nil {
			return Result{}, err
		}
		if parent.ID != 0 && parent.IssueNumber != 0 && !conversation(d.Kind) {
			return followup(ctx, home, cfg, db, parent, m, verdict, trusted, true)
		}
	}
	if row.ID == 0 {
		source := "cli"
		if m.ChannelID != "" {
			source = "discord"
		}
		body := m.Content
		if strings.TrimSpace(body) == "" {
			body = "[attachments]"
		}
		row, err = db.AddReport(ctx, model.Report{Body: body, Status: model.Held, Source: source, SourceKey: key(m), SourceRef: jsonText(map[string]string{"guild": m.GuildID, "channel": m.ChannelID, "message": m.ID}), Author: m.AuthorName, AuthorID: m.AuthorID, Title: prefix(strings.SplitN(body, "\n", 2)[0], 80), Language: language(body), ChannelID: m.ChannelID, Repository: cfg.GitHubRepo, Attachments: attachmentURLs(m.Attachments)})
		if err != nil {
			return Result{}, err
		}
	}
	if verdict != "ok" {
		row, err = db.Patch(ctx, row.ID, map[string]any{"kind": verdict, "error": verdict})
		if err != nil {
			return Result{}, err
		}
		reply := ""
		if replyHeld {
			reply = say(row.Language, "这条已保留，等维护者查看。", "I've kept this for a maintainer to review.")
		}
		return Result{ReportID: row.ID, Status: string(row.Status), Reply: reply}, nil
	}
	patch := map[string]any{"kind": d.Kind, "language": d.Language, "surface": d.Surface, "decision": jsonText(d), "decided_by": cfg.DeciderModel}
	if d.Work != nil {
		patch["work"] = *d.Work
	}
	row, err = db.Patch(ctx, row.ID, patch)
	if err != nil {
		return Result{}, err
	}
	if conversation(d.Kind) {
		events, err := db.Events(ctx, row.ID)
		if err != nil {
			return Result{}, err
		}
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].Kind == "conversation-answer" {
				return Result{ReportID: row.ID, Status: "held", Reply: events[i].Detail}, nil
			}
		}
		answer := say(d.Language, "你想了解哪一处行为，还是希望修改它？请具体说一下。", "Which behavior would you like explained, or changed? Please be specific.")
		if d.Kind == "noise" {
			answer = say(d.Language, "收到。想了解现有功能，或者有具体问题，继续说就行。", "Got it. Feel free to ask about existing behavior or share a specific problem.")
		}
		if d.Kind == "question" {
			answer, err = questions.Answer(ctx, home, cfg, row, prior)
			if err != nil {
				cause := prefix(worker.Publishable(cfg, err.Error()), 4000)
				answer = say(d.Language, "暂时无法核实实现细节。请补充具体页面、操作或代码位置，方便继续查看。", "I couldn't verify the implementation details right now. Please share the specific screen, action, or code location.")
				if _, e := db.Patch(ctx, row.ID, map[string]any{"error": "conversation answer unavailable: " + cause}); e != nil {
					return Result{}, e
				}
				if e := db.RecordEvent(ctx, row.ID, "conversation-error", cause); e != nil {
					return Result{}, e
				}
			}
		}
		if err = db.RecordEvent(ctx, row.ID, "conversation-answer", answer); err != nil {
			return Result{}, err
		}
		return Result{ReportID: row.ID, Status: "held", Reply: answer}, nil
	}
	if prior != "" && !strings.Contains(row.Body, "Earlier conversation (reference only;") {
		row, err = db.Patch(ctx, row.ID, map[string]any{"body": row.Body + "\n\nEarlier conversation (reference only; only the latest message above authorizes the current change):\n" + suffix(prior, 6000)})
		if err != nil {
			return Result{}, err
		}
	}
	if row.IssueNumber == 0 && !m.NoDedupe && cfg.DedupeEnabled {
		duplicate, err := duplicateOnce(ctx, cfg, db, reports, row, m)
		if err != nil {
			return Result{}, err
		}
		if duplicate.ID != 0 {
			// The reservation's result points at the existing report; the held intake row
			// remains durable evidence and does not enter the queue.
			if _, err = db.Patch(ctx, row.ID, map[string]any{"kind": "duplicate"}); err != nil {
				return Result{}, err
			}
			result, err := followup(ctx, home, cfg, db, duplicate, m, "ok", m.AuthorID == duplicate.AuthorID || slices.Contains(cfg.AllowedUsers, m.AuthorID), true)
			if err != nil {
				return result, err
			}
			result.Duplicate = true
			return result, nil
		}
	}
	row, err = ensureIssue(ctx, cfg, db, row)
	if err != nil {
		return Result{ReportID: row.ID, Status: string(row.Status)}, err
	}
	status := policy(cfg, d)
	reason := ""
	if row.Status != model.Held && row.Status != model.AwaitingApproval {
		status = row.Status
		reason = row.Error
	} else if status == model.New && queueFull(cfg, reports) {
		status = model.Held
		reason = "queue full"
	}
	fetchAttachments(ctx, home, cfg, row.ID, m.Attachments)
	labelled := row
	labelled.Status = status
	if err = labelIssue(ctx, cfg, labelled); err != nil {
		return Result{}, err
	}
	note := say(d.Language, "已记录；需要维护者批准后才会动手。", "Recorded; a maintainer must approve before work starts.")
	if status == model.New {
		note = say(d.Language, "已排入代理队列，完成后会附验证结果。", "Queued for the agent; verification evidence will be reported when complete.")
	}
	if reason != "" {
		note = say(d.Language, "队列已满，这条先保留。", "The queue is full; this report is held.")
	}
	if err = issueComment(ctx, cfg, db, row.ID, row.IssueNumber, note, "intake-status:"+key(m)); err != nil {
		return Result{}, err
	}
	if row.Kind == "feature_request" {
		notePriorDecision(ctx, cfg, db, row)
	}
	if err = db.RecordEvent(ctx, row.ID, "accepted", fmt.Sprintf("%s → #%d", row.Kind, row.IssueNumber)); err != nil {
		return Result{}, err
	}
	if row.Status != status {
		row, err = db.Transition(ctx, row.ID, row.Status, status, map[string]any{"error": reason})
	} else {
		row, err = db.Patch(ctx, row.ID, map[string]any{"error": reason})
	}
	if err != nil {
		return Result{}, err
	}
	reply := fmt.Sprintf("Filed as [issue #%d](%s).", row.IssueNumber, row.IssueURL)
	if d.Language == "zh" {
		reply = fmt.Sprintf("已记成 [issue #%d](%s)。", row.IssueNumber, row.IssueURL)
	}
	if status == model.AwaitingApproval {
		reply += say(d.Language, "需要维护者先批准才会动手。", " A maintainer must approve it before work starts.")
	} else if reason != "" {
		reply += say(d.Language, "队列已满，先保留等候。", " The queue is full, so it is held for now.")
	} else {
		reply += say(d.Language, "已排队，完成后会在这里报告验证结果。", " It is queued; I'll report the verification results here.")
	}
	return Result{ReportID: row.ID, Status: string(status), Reply: reply, Issue: row.IssueNumber}, nil
}

func gate(cfg worker.Config, reports []model.Report, m Message, exclude int64) (string, bool) {
	verdict := "ok"
	if len(cfg.AllowedUsers) > 0 && !slices.Contains(cfg.AllowedUsers, m.AuthorID) {
		verdict = "unvetted"
	}
	now := time.Now()
	daily, hourly, global := 0, 0, 0
	answered := false
	for _, r := range reports {
		if r.ID == exclude || r.Kind == "duplicate" {
			continue
		}
		created, _ := time.Parse(time.RFC3339Nano, r.CreatedAt)
		age := now.Sub(created)
		if age < time.Hour {
			global++
			if r.AuthorID == m.AuthorID {
				hourly++
			}
		}
		if age < 24*time.Hour && r.AuthorID == m.AuthorID {
			daily++
			if r.Kind == "unvetted" || r.Kind == "rate_limited" {
				answered = true
			}
		}
		var entries []struct {
			AuthorID  string `json:"author_id"`
			At        string `json:"at"`
			Reply     string `json:"reply"`
			MessageID string `json:"message_id"`
		}
		_ = json.Unmarshal([]byte(r.Followups), &entries)
		for _, f := range entries {
			if f.MessageID == m.ID {
				continue
			}
			at, _ := time.Parse(time.RFC3339Nano, f.At)
			age := now.Sub(at)
			if age < time.Hour {
				global++
				if f.AuthorID == m.AuthorID {
					hourly++
				}
			}
			if age < 24*time.Hour && f.AuthorID == m.AuthorID {
				daily++
				if f.Reply == "followup_rate_limited" {
					answered = true
				}
			}
		}
	}
	if verdict == "ok" && ((cfg.MaxPerUserPerDay > 0 && daily >= cfg.MaxPerUserPerDay) || (cfg.MaxPerUserPerHour > 0 && hourly >= cfg.MaxPerUserPerHour) || (cfg.MaxPerHour > 0 && global >= cfg.MaxPerHour)) {
		verdict = "rate_limited"
	}
	return verdict, !answered
}
func queueFull(cfg worker.Config, reports []model.Report) bool {
	n := 0
	for _, r := range reports {
		if r.Status == model.New || r.Status == model.Queued {
			n++
		}
	}
	return cfg.MaxQueue > 0 && n >= cfg.MaxQueue
}
func policy(cfg worker.Config, d Decision) model.Status {
	if slices.Contains(cfg.AutoKinds, d.Kind) && (d.Work == nil || *d.Work <= cfg.MaxAutoWork) {
		return model.New
	}
	return model.AwaitingApproval
}
func attachmentURLs(a []Attachment) string {
	urls := make([]string, 0, len(a))
	for _, item := range a {
		urls = append(urls, item.URL)
	}
	return jsonText(urls)
}

func Replay(ctx context.Context, home string, cfg worker.Config, db *store.Store, k string) (Result, error) {
	if !strings.HasPrefix(k, "intake:") {
		k = "intake:" + k
	}
	reservation, err := db.Reservation(ctx, k)
	if err != nil {
		return Result{}, err
	}
	var m Message
	if err = json.Unmarshal([]byte(reservation.Payload), &m); err != nil {
		return Result{}, err
	}
	return Accept(ctx, home, cfg, db, m)
}
func Promote(ctx context.Context, home string, cfg worker.Config, db *store.Store, id int64) (Result, error) {
	row, err := db.Get(ctx, id)
	if err != nil {
		return Result{}, err
	}
	if row.Status != model.Held && row.Status != model.AwaitingApproval {
		return Result{}, errors.New("only held or awaiting-approval reports may be promoted")
	}
	d := Decision{Kind: row.Kind, Language: row.Language, Surface: row.Surface, Work: new(row.Work)}
	if row.Kind != "bug" && row.Kind != "feature_request" {
		d = classify(ctx, cfg, row.Body, "")
	}
	if conversation(d.Kind) {
		return Result{ReportID: id, Status: string(row.Status)}, errors.New("a question or unclassified report cannot be promoted to work")
	}
	row, err = db.Patch(ctx, id, map[string]any{"kind": d.Kind, "language": d.Language, "surface": d.Surface, "decision": jsonText(d)})
	if err != nil {
		return Result{}, err
	}
	row, err = ensureIssue(ctx, cfg, db, row)
	if err != nil {
		return Result{}, err
	}
	row, err = db.Transition(ctx, id, row.Status, model.New, map[string]any{"error": ""})
	if err != nil {
		return Result{}, err
	}
	return Result{ReportID: id, Status: string(row.Status), Issue: row.IssueNumber}, labelIssue(ctx, cfg, row)
}

func followup(ctx context.Context, home string, cfg worker.Config, db *store.Store, row model.Report, m Message, verdict string, trusted, replyHeld bool) (Result, error) {
	var entries []map[string]any
	_ = json.Unmarshal([]byte(row.Followups), &entries)
	present := false
	for _, entry := range entries {
		if entry["message_id"] == m.ID {
			present = true
			break
		}
	}
	if !present {
		entry := map[string]any{"author": m.AuthorName, "author_id": m.AuthorID, "body": m.Content, "attachments": json.RawMessage(attachmentURLs(m.Attachments)), "at": time.Now().UTC().Format(time.RFC3339Nano), "trusted": trusted && verdict == "ok", "message_id": m.ID}
		if verdict != "ok" && replyHeld {
			entry["reply"] = "followup_rate_limited"
		}
		var err error
		row, err = db.AppendFollowup(ctx, row.ID, entry)
		if err != nil {
			return Result{}, err
		}
	}
	// A duplicate initially owns an intake evidence row; the message must ultimately
	// link to the report being continued. Store enforces identity, so caller only
	// adds this link when no other report owns it.
	if linked, err := db.ByMessage(ctx, m.ID); errors.Is(err, model.ErrNotFound) {
		if err = db.LinkMessage(ctx, m.ID, row.ID, "report"); err != nil {
			return Result{}, err
		}
	} else if err != nil {
		return Result{}, err
	} else if linked.ID != row.ID {
		return Result{}, model.ErrConflict
	}
	result := Result{ReportID: row.ID, Status: string(row.Status), Issue: row.IssueNumber}
	if row.IssueNumber == 0 {
		return result, nil
	}
	if verdict != "ok" {
		if replyHeld {
			result.Reply = say(language(m.Content), "补充已保留，等维护者查看。", "I've kept your follow-up for a maintainer.")
		}
		return result, nil
	}
	if trusted {
		fetchAttachments(ctx, home, cfg, row.ID, m.Attachments)
	}
	labels, state, err := issueState(ctx, cfg, row.IssueNumber)
	if err != nil {
		return result, err
	}
	wontfix := slices.Contains(labels, "wontfix") || slices.Contains(labels, "won't-fix")
	result.Reply = say(row.Language, "收到，补充已记到 issue 上。", "Got it, added to the issue.")
	reopen := trusted && !wontfix && slices.Contains([]model.Status{model.NeedsInfo, model.NeedsReporter, model.NoChange, model.VerifyFailed, model.SetupFailed, model.AgentFailed, model.Failed, model.Interrupted}, row.Status)
	if reopen {
		events, err := db.Events(ctx, row.ID)
		if err != nil {
			return result, err
		}
		count := 0
		already := false
		for _, e := range events {
			if e.Kind == "reopened" {
				count++
				if e.Detail == m.ID {
					already = true
				}
			}
		}
		if cfg.MaxReopens > 0 && count >= cfg.MaxReopens && !already {
			result.Reply = say(row.Language, "已重试多次，接下来交给维护者。", "This report has reached its retry limit; a maintainer will review it.")
		} else if !already {
			reports, err := db.List(ctx)
			if err != nil {
				return result, err
			}
			status := model.New
			reason := ""
			if queueFull(cfg, reports) {
				status = model.Held
				reason = "queue full"
			}
			if state == "CLOSED" {
				if _, err = remote.GH(ctx, "issue", "reopen", strconv.Itoa(row.IssueNumber), "--repo", cfg.GitHubRepo); err != nil {
					return result, err
				}
			}
			patch := map[string]any{"error": reason}
			if row.TriageDecision != "" {
				patch["brief"] = ""
				patch["triage_decision"] = ""
			}
			requeued, e := db.RequeueMessage(ctx, row.ID, row.Status, status, patch, m.ID, cfg.MaxReopens)
			if errors.Is(e, model.ErrReopenLimit) {
				result.Reply = say(row.Language, "已重试多次，接下来交给维护者。", "This report has reached its retry limit; a maintainer will review it.")
			} else {
				if e != nil {
					return result, e
				}
				row = requeued
				if err = labelIssue(ctx, cfg, row); err != nil {
					return result, err
				}
				result.Status = string(row.Status)
				result.Reply = say(row.Language, "收到补充，已重新排队。", "Thanks — queued to try again with this.")
				if reason != "" {
					result.Reply = say(row.Language, "补充已保留，队列满了，先等候。", "Your follow-up is kept; the queue is full, so this is held.")
				}
			}
		}
	}
	if wontfix {
		result.Reply = say(row.Language, "维护者已标记不修，补充会保留给他们查看。", "A maintainer marked this won't-fix; your follow-up is recorded for them.")
	}
	body := fmt.Sprintf("Follow-up from %s:\n\n%s", m.AuthorName, m.Content)
	for _, a := range m.Attachments {
		body += "\n" + a.URL
	}
	if err = issueComment(ctx, cfg, db, row.ID, row.IssueNumber, body, "followup:"+key(m)); err != nil {
		return result, err
	}
	return result, nil
}

func classifyOnce(ctx context.Context, cfg worker.Config, db *store.Store, m Message, prior string) (Decision, error) {
	k := "intake-decision:" + key(m)
	if saved, err := db.Reservation(ctx, k); err == nil {
		var d Decision
		if err = json.Unmarshal([]byte(saved.Payload), &d); err != nil {
			return d, err
		}
		return d, db.Complete(ctx, k, saved.Payload)
	} else if !errors.Is(err, model.ErrNotFound) {
		return Decision{}, err
	}
	d := classify(ctx, cfg, m.Content, prior)
	encoded := jsonText(d)
	if _, err := db.Reserve(ctx, k, "intake-decision", 0, encoded); err != nil {
		return d, err
	}
	return d, db.Complete(ctx, k, encoded)
}

func duplicateRoute(ctx context.Context, db *store.Store, m Message) (int64, bool, error) {
	saved, err := db.Reservation(ctx, "intake-route:"+key(m))
	if errors.Is(err, model.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	id, err := strconv.ParseInt(saved.Payload, 10, 64)
	if err != nil || id < 0 {
		return 0, false, errors.New("invalid persisted duplicate destination")
	}
	return id, true, nil
}
func duplicateOnce(ctx context.Context, cfg worker.Config, db *store.Store, reports []model.Report, row model.Report, m Message) (model.Report, error) {
	if id, found, err := duplicateRoute(ctx, db, m); err != nil {
		return model.Report{}, err
	} else if found {
		if id == 0 {
			return model.Report{}, nil
		}
		return db.Get(ctx, id)
	}
	// An already linked destination also recovers interrupted intakes written by
	// the earlier implementation, which did not persist a separate route.
	duplicate, err := db.ByMessage(ctx, m.ID)
	if errors.Is(err, model.ErrNotFound) || err == nil && duplicate.ID == row.ID {
		duplicate, err = findDuplicate(ctx, cfg, reports, row)
	}
	if err != nil {
		return model.Report{}, err
	}
	k := "intake-route:" + key(m)
	target := strconv.FormatInt(duplicate.ID, 10)
	if _, err = db.Reserve(ctx, k, "intake-route", row.ID, target); err != nil {
		return model.Report{}, err
	}
	if err = db.Complete(ctx, k, target); err != nil {
		return model.Report{}, err
	}
	return duplicate, nil
}
