package migrate

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/FaintGhost/loom/internal/model"
)

func failedIntakes(path string, messages []model.Message) (pending []model.Reservation, err error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	handled := map[string]bool{}
	for _, message := range messages {
		handled[message.ID] = true
	}
	seen := map[string]string{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 16<<20)
	line := 0
	for scanner.Scan() {
		line++
		var entry struct {
			At                  string   `json:"at"`
			SourceRef           string   `json:"source_ref"`
			Author              string   `json:"author"`
			AuthorID            string   `json:"author_id"`
			Channel             string   `json:"channel"`
			Content             string   `json:"content"`
			Attachments         []string `json:"attachments"`
			ConversationContext string   `json:"conversation_context"`
		}
		if err = json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, fmt.Errorf("intake-failed.jsonl line %d: %w", line, err)
		}
		var ref map[string]string
		if err = json.Unmarshal([]byte(entry.SourceRef), &ref); err != nil {
			return nil, fmt.Errorf("intake-failed.jsonl line %d source_ref: %w", line, err)
		}
		id := ref["message"]
		channel := ref["channel"]
		if channel == "" {
			channel = entry.Channel
		}
		if id == "" || channel == "" || entry.At == "" {
			return nil, fmt.Errorf("intake-failed.jsonl line %d lacks identity, channel or timestamp", line)
		}
		if handled[id] {
			continue
		}
		attachments := make([]map[string]string, 0, len(entry.Attachments))
		for _, url := range entry.Attachments {
			attachments = append(attachments, map[string]string{"url": url})
		}
		payload, err := json.Marshal(map[string]any{"id": id, "channel_id": channel, "guild_id": ref["guild"], "author_id": entry.AuthorID, "author_name": entry.Author, "content": entry.Content, "reply_to": ref["reply_to"], "attachments": attachments, "conversation_context": entry.ConversationContext})
		if err != nil {
			return nil, err
		}
		if previous, exists := seen[id]; exists {
			if previous != string(payload) {
				return nil, fmt.Errorf("conflicting failed intake records for message %s", id)
			}
			continue
		}
		seen[id] = string(payload)
		pending = append(pending, model.Reservation{Key: "intake:discord:" + id, Kind: "intake", Payload: string(payload), State: "reserved", CreatedAt: entry.At, UpdatedAt: entry.At})
	}
	return pending, scanner.Err()
}
