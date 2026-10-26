package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
)

// SendDiscordDelivery retains each chunk receipt before advancing. A restart
// reconciles only an uncertain chunk, starting at its persisted send window.
func SendDiscordDelivery(ctx context.Context, cfg worker.Config, db *store.Store, reportID int64, channel, replyTo, text, key string) ([]string, error) {
	var ids []string
	for i, chunk := range DiscordChunks(text) {
		chunkKey := key + ":chunk:" + strconv.Itoa(i)
		payload, _ := json.Marshal(map[string]string{"channel": channel, "reply_to": replyTo, "text": chunk})
		if _, err := db.Reserve(ctx, chunkKey, "discord-chunk", reportID, string(payload)); err != nil {
			return ids, err
		}
		receipt, err := db.Reservation(ctx, chunkKey)
		if err != nil {
			return ids, err
		}
		if receipt.Payload != string(payload) {
			return ids, errors.New("Discord delivery key reused with different content")
		}
		if receipt.State == "complete" {
			ids = append(ids, receipt.Result)
			continue
		}
		created, err := time.Parse(time.RFC3339Nano, receipt.CreatedAt)
		if err != nil {
			return ids, err
		}
		// Discord snowflakes encode milliseconds since 2015. A minute of overlap
		// covers local clock skew while keeping recovery independent of channel age.
		milliseconds := created.Add(-time.Minute).UnixMilli() - 1420070400000
		if milliseconds < 0 {
			return ids, errors.New("invalid Discord delivery timestamp")
		}
		after := strconv.FormatInt(milliseconds<<22, 10)
		if replyTo != "" && snowflakeLess(after, replyTo) {
			after = replyTo
		}
		bounded, cancel := context.WithTimeout(ctx, 2*time.Minute)
		sent, err := sendDiscordOnce(bounded, cfg, channel, replyTo, chunk, chunkKey, after)
		cancel()
		if err != nil {
			return ids, err
		}
		if len(sent) != 1 {
			return ids, fmt.Errorf("Discord chunk returned %d message IDs", len(sent))
		}
		if err = db.LinkMessage(ctx, sent[0], reportID, "bot"); err != nil {
			return ids, err
		}
		if err = db.Complete(ctx, chunkKey, sent[0]); err != nil {
			return ids, err
		}
		ids = append(ids, sent[0])
	}
	return ids, nil
}
