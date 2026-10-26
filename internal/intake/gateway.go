package intake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/FaintGhost/loom/internal/remote"
	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
)

type delivery struct {
	Message Message `json:"message"`
	Result  Result  `json:"result"`
}

// Run uses discordgo for authenticated gateway sessions, heartbeat ACK handling,
// session resume, reconnect backoff and invalid-session recovery.
func Run(ctx context.Context, home string, cfg worker.Config, db *store.Store) error {
	if cfg.DiscordChannel == "" && len(cfg.DiscordChannels) == 0 {
		return errors.New("Discord channel must be configured")
	}
	token, err := remote.Secret(cfg.DiscordTokenFile, cfg.DiscordTokenEnv)
	if err != nil {
		return err
	}
	session, err := discordgo.New("Bot " + token)
	if err != nil {
		return err
	}
	session.Identify.Intents = discordgo.IntentsGuilds | discordgo.IntentsGuildMessages | discordgo.IntentsMessageContent
	session.ShouldReconnectOnError = true
	channels := slices.Clone(cfg.DiscordChannels)
	if cfg.DiscordChannel != "" && !slices.Contains(channels, cfg.DiscordChannel) {
		channels = append(channels, cfg.DiscordChannel)
	}
	cursors, err := loadCursors(ctx, home, db, channels)
	if err != nil {
		return err
	}
	ready := make(chan struct{}, 1)
	incoming := make(chan Message, 256)
	session.AddHandler(func(s *discordgo.Session, event *discordgo.MessageCreate) {
		if event.Message == nil || event.Author == nil || event.Author.Bot || !wanted(cfg, event.ChannelID) {
			return
		}
		m := discordMessage(event.Message)
		select {
		case incoming <- m:
		case <-ctx.Done():
		}
	})
	session.AddHandler(func(s *discordgo.Session, event *discordgo.Ready) {
		log.Printf("Discord ready as %s; reconciling configured channels", event.User.ID)
		signalReady(ready)
	})
	if err = session.Open(); err != nil {
		return err
	}
	defer session.Close()
	// Recover persisted messages and delivery intents before accepting a new turn.
	if err = resume(ctx, home, cfg, db); err != nil {
		log.Printf("intake recovery: %v", err)
	}
	reconcile := func() {
		for _, channel := range channels {
			if e := catchup(ctx, session, home, cfg, db, channel, cursors); e != nil {
				log.Printf("Discord catch-up for %s remains pending: %v", channel, e)
			}
		}
	}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ready:
			reconcile()
		case <-ticker.C:
			if err = resume(ctx, home, cfg, db); err != nil {
				log.Printf("intake recovery: %v", err)
			}
			reconcile()
		case m := <-incoming:
			result, e := Accept(ctx, home, cfg, db, m)
			if e != nil {
				log.Printf("intake message %s preserved: %v", m.ID, e)
				continue
			}
			if e = sendResult(ctx, cfg, db, m, result); e != nil {
				log.Printf("Discord delivery pending for %s: %v", m.ID, e)
			}
		}
	}
}
func discordMessage(m *discordgo.Message) Message {
	result := Message{ID: m.ID, ChannelID: m.ChannelID, GuildID: m.GuildID, Content: m.Content}
	if m.Author != nil {
		result.AuthorID = m.Author.ID
		result.AuthorName = m.Author.GlobalName
		if result.AuthorName == "" {
			result.AuthorName = m.Author.Username
		}
		result.Bot = m.Author.Bot
	}
	if m.MessageReference != nil {
		result.ReplyTo = m.MessageReference.MessageID
	}
	for _, a := range m.Attachments {
		result.Attachments = append(result.Attachments, Attachment{ID: a.ID, Filename: a.Filename, URL: a.URL, ContentType: a.ContentType, Size: int64(a.Size)})
	}
	return result
}
func sendResult(ctx context.Context, cfg worker.Config, db *store.Store, m Message, result Result) error {
	if result.Reply == "" || result.ReportID == 0 || m.ChannelID == "" {
		return nil
	}
	k := "intake-delivery:" + key(m)
	if _, err := db.Reserve(ctx, k, "discord-intake", result.ReportID, jsonText(delivery{m, result})); err != nil {
		return err
	}
	reservation, err := db.Reservation(ctx, k)
	if err != nil {
		return err
	}
	if reservation.State == "complete" {
		return nil
	}
	ids, err := remote.SendDiscordDelivery(ctx, cfg, db, result.ReportID, m.ChannelID, m.ID, result.Reply, k)
	for _, id := range ids {
		if e := db.LinkMessage(ctx, id, result.ReportID, "bot"); e != nil {
			return errors.Join(err, e)
		}
	}
	if err != nil {
		return err
	}
	return db.Complete(ctx, k, jsonText(ids))
}
func resume(ctx context.Context, home string, cfg worker.Config, db *store.Store) error {
	pending, err := db.Pending(ctx, "intake")
	if err != nil {
		return err
	}
	var errs []error
	for _, p := range pending {
		var m Message
		if e := json.Unmarshal([]byte(p.Payload), &m); e != nil {
			errs = append(errs, e)
			continue
		}
		result, e := Accept(ctx, home, cfg, db, m)
		if e != nil {
			errs = append(errs, e)
			continue
		}
		if e = sendResult(ctx, cfg, db, m, result); e != nil {
			errs = append(errs, e)
		}
	}
	pending, err = db.Pending(ctx, "discord-intake")
	if err != nil {
		return errors.Join(errors.Join(errs...), err)
	}
	for _, p := range pending {
		var d delivery
		if e := json.Unmarshal([]byte(p.Payload), &d); e != nil {
			errs = append(errs, e)
			continue
		}
		if e := sendResult(ctx, cfg, db, d.Message, d.Result); e != nil {
			errs = append(errs, e)
		}
	}
	return errors.Join(errs...)
}
func signalReady(ready chan<- struct{}) {
	select {
	case ready <- struct{}{}:
	default:
	}
}

// Cursors describe the last REST-reconciled page, never the newest live message.
// Seed once before opening the gateway; a later READY cannot jump across a gap.
func loadCursors(ctx context.Context, home string, db *store.Store, channels []string) (map[string]string, error) {
	cursors := map[string]string{}
	data, err := os.ReadFile(filepath.Join(home, "discord-cursors.json"))
	if err == nil {
		if err = json.Unmarshal(data, &cursors); err != nil {
			return nil, err
		}
		if cursors == nil {
			return nil, errors.New("invalid Discord cursor file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	reports, err := db.List(ctx)
	if err != nil {
		return nil, err
	}
	changed := false
	for _, channel := range channels {
		if _, exists := cursors[channel]; exists {
			continue
		}
		after := ""
		for _, r := range reports {
			if r.ChannelID != channel {
				continue
			}
			if newer(r.MessageID, after) {
				after = r.MessageID
			}
			var entries []struct {
				MessageID string `json:"message_id"`
			}
			if r.Followups != "" {
				if err = json.Unmarshal([]byte(r.Followups), &entries); err != nil {
					return nil, err
				}
			}
			for _, f := range entries {
				if newer(f.MessageID, after) {
					after = f.MessageID
				}
			}
		}
		cursors[channel] = after
		changed = true
	}
	if changed {
		if err = saveCursors(home, cursors); err != nil {
			return nil, err
		}
	}
	return cursors, nil
}
func saveCursors(home string, cursors map[string]string) (err error) {
	data, err := json.Marshal(cursors)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(home, ".discord-cursors-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, err = file.Write(data)
	err = errors.Join(err, file.Sync(), file.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(file.Name(), filepath.Join(home, "discord-cursors.json")); err != nil {
		return err
	}
	directory, err := os.Open(home)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
func catchup(ctx context.Context, session *discordgo.Session, home string, cfg worker.Config, db *store.Store, channel string, cursors map[string]string) error {
	fetch := func(after string) ([]*discordgo.Message, error) {
		return session.ChannelMessages(channel, 100, "", after, "", discordgo.WithContext(ctx))
	}
	handle := func(m Message) error {
		result, err := Accept(ctx, home, cfg, db, m)
		if err != nil {
			// A preserved raw message is safe to retry independently of the REST cursor.
			if _, saved := db.Reservation(ctx, "intake:"+key(m)); saved != nil {
				return errors.Join(err, saved)
			}
			log.Printf("catch-up message %s preserved: %v", m.ID, err)
			return nil
		}
		if err = sendResult(ctx, cfg, db, m, result); err != nil {
			log.Printf("catch-up reply %s pending: %v", m.ID, err)
		}
		return nil
	}
	return reconcileChannel(home, channel, cursors, fetch, handle)
}
func reconcileChannel(home, channel string, cursors map[string]string, fetch func(string) ([]*discordgo.Message, error), handle func(Message) error) error {
	after, exists := cursors[channel]
	if !exists {
		return fmt.Errorf("Discord channel %s has no reconciliation cursor", channel)
	}
	for {
		messages, err := fetch(after)
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			return nil
		}
		slices.SortFunc(messages, func(a, b *discordgo.Message) int {
			if a.ID == b.ID {
				return 0
			}
			if newer(a.ID, b.ID) {
				return 1
			}
			return -1
		})
		for _, raw := range messages {
			if raw.Author == nil || raw.Author.Bot {
				continue
			}
			if err = handle(discordMessage(raw)); err != nil {
				return err
			}
		}
		last := messages[len(messages)-1].ID
		if !newer(last, after) {
			return nil
		}
		updated := maps.Clone(cursors)
		updated[channel] = last
		if err = saveCursors(home, updated); err != nil {
			return err
		}
		cursors[channel] = last
		if len(messages) < 100 || after == "" {
			return nil
		}
		after = last
	}
}
func newer(a, b string) bool {
	if len(a) != len(b) {
		return len(a) > len(b)
	}
	return a > b
}
