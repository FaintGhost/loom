package remote

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf16"

	"github.com/FaintGhost/loom/internal/worker"
)

// GH runs exact argv, with bounded output and lifetime. Callers specify --repo.
func GH(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "gh", args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	cmd.WaitDelay = time.Second
	var out, stderr limitedBuffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("gh command failed: %w", err)
	}
	if out.overflow {
		return nil, errors.New("gh response exceeds 8 MiB")
	}
	return out.Bytes(), nil
}

type limitedBuffer struct {
	bytes.Buffer
	overflow bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remaining := (8 << 20) - b.Len()
	if n > remaining {
		b.overflow = true
		p = p[:remaining]
	}
	_, err := b.Buffer.Write(p)
	return n, err
}

func Secret(file, variable string) (string, error) {
	if variable != "" {
		if value := strings.TrimSpace(os.Getenv(variable)); value != "" {
			return value, nil
		}
	}
	if file == "" {
		return "", errors.New("credential file or environment variable is required")
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return "", errors.New("credential file unavailable")
	}
	v := strings.TrimSpace(string(b))
	if v == "" {
		return "", errors.New("credential is empty")
	}
	return v, nil
}

func request(ctx context.Context, method, url, token string, payload any, out any) error {
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", token)
	if req.URL.Host == "discord.com" && strings.HasPrefix(req.URL.Path, "/api/") {
		req.Header.Set("User-Agent", "DiscordBot (https://github.com/FaintGhost/loom, 1.0)")
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if _, bounded := ctx.Deadline(); !bounded {
		client.Timeout = 60 * time.Second
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, (8<<20)+1))
	if err != nil {
		return err
	}
	if len(b) > 8<<20 {
		return errors.New("remote response too large")
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("remote HTTP status %d", res.StatusCode)
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

type Answer struct {
	Choice     string   `json:"choice"`
	Confidence float64  `json:"confidence"`
	Score      *float64 `json:"score"`
}

func SystemOne(ctx context.Context, cfg worker.Config, questions, state map[string]any) (map[string]Answer, error) {
	key, err := Secret(cfg.MagpieKeyFile, cfg.MagpieKeyEnv)
	if err != nil {
		return nil, err
	}
	var result struct {
		Answers map[string]Answer `json:"answers"`
		Error   json.RawMessage   `json:"error"`
	}
	err = request(ctx, "POST", strings.TrimRight(cfg.MagpieURL, "/")+"/v1/systemone", "Bearer "+key, map[string]any{"model": cfg.DeciderModel, "state": state, "questions": questions}, &result)
	if err != nil {
		return nil, err
	}
	if len(result.Error) > 0 && string(result.Error) != "null" {
		return nil, errors.New("decision model refused request")
	}
	if result.Answers == nil {
		return nil, errors.New("decision model omitted answers")
	}
	return result.Answers, nil
}
func ChatJSON(ctx context.Context, cfg worker.Config, system, prompt string, out any) error {
	key, err := Secret(cfg.MagpieKeyFile, cfg.MagpieKeyEnv)
	if err != nil {
		return err
	}
	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	err = request(ctx, "POST", strings.TrimRight(cfg.MagpieURL, "/")+"/v1/chat/completions", "Bearer "+key, map[string]any{"model": cfg.TitleModel, "stream": false, "response_format": map[string]string{"type": "json_object"}, "messages": []map[string]string{{"role": "system", "content": system}, {"role": "user", "content": prompt}}}, &result)
	if err != nil {
		return err
	}
	if len(result.Choices) != 1 {
		return errors.New("chat response omitted unique answer")
	}
	return json.Unmarshal([]byte(result.Choices[0].Message.Content), out)
}

// DiscordChunks bounds UTF-16 code units without splitting a Unicode scalar.
func DiscordChunks(text string) []string {
	var chunks []string
	start, units := 0, 0
	for offset, r := range text {
		n := utf16.RuneLen(r)
		if units+n > 2000 {
			chunks = append(chunks, text[start:offset])
			start = offset
			units = 0
		}
		units += n
	}
	if start < len(text) {
		chunks = append(chunks, text[start:])
	}
	return chunks
}
func SendDiscord(ctx context.Context, cfg worker.Config, channel, replyTo, text string) ([]string, error) {
	return SendDiscordOnce(ctx, cfg, channel, replyTo, text, "")
}

// SendDiscordOnce uses Discord's enforced nonce and also reconciles old retries
// against message history, since Discord only retains nonce deduplication briefly.
func SendDiscordOnce(ctx context.Context, cfg worker.Config, channel, replyTo, text, key string) ([]string, error) {
	return sendDiscordOnce(ctx, cfg, channel, replyTo, text, key, replyTo)
}
func sendDiscordOnce(ctx context.Context, cfg worker.Config, channel, replyTo, text, key, after string) ([]string, error) {
	token, err := Secret(cfg.DiscordTokenFile, cfg.DiscordTokenEnv)
	if err != nil {
		return nil, err
	}
	if _, err := strconv.ParseUint(channel, 10, 64); err != nil {
		return nil, errors.New("invalid Discord channel")
	}
	var ids []string
	for i, chunk := range DiscordChunks(text) {
		payload := map[string]any{"content": chunk, "allowed_mentions": map[string]any{"parse": []string{}, "replied_user": false}}
		if replyTo != "" {
			payload["message_reference"] = map[string]any{"message_id": replyTo, "fail_if_not_exists": false}
		}
		if key != "" {
			sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", key, i)))
			nonce := hex.EncodeToString(sum[:12])
			payload["nonce"] = nonce
			payload["enforce_nonce"] = true
			existing, err := discordNonce(ctx, token, channel, after, nonce)
			if err != nil {
				return ids, err
			}
			if existing != "" {
				ids = append(ids, existing)
				continue
			}
		}
		var sent struct {
			ID string `json:"id"`
		}
		if err := request(ctx, "POST", "https://discord.com/api/v10/channels/"+channel+"/messages", "Bot "+token, payload, &sent); err != nil {
			return ids, err
		}
		if sent.ID == "" {
			return ids, errors.New("Discord omitted message ID")
		}
		ids = append(ids, sent.ID)
	}
	return ids, nil
}
func discordNonce(ctx context.Context, token, channel, after, nonce string) (string, error) {
	backwards := after == ""
	cursor := after
	for {
		var messages []struct {
			ID    string          `json:"id"`
			Nonce json.RawMessage `json:"nonce"`
		}
		endpoint := "https://discord.com/api/v10/channels/" + channel + "/messages?limit=100"
		if cursor != "" {
			if backwards {
				endpoint += "&before=" + cursor
			} else {
				endpoint += "&after=" + cursor
			}
		}
		err := request(ctx, "GET", endpoint, "Bot "+token, nil, &messages)
		if err != nil {
			return "", err
		}
		next := cursor
		for _, m := range messages {
			if strings.Trim(string(m.Nonce), "\"") == nonce {
				return m.ID, nil
			}
			if backwards {
				if next == "" || snowflakeLess(m.ID, next) {
					next = m.ID
				}
			} else if snowflakeLess(next, m.ID) {
				next = m.ID
			}
		}
		if len(messages) < 100 || next == cursor {
			return "", nil
		}
		cursor = next
	}
}
func snowflakeLess(a, b string) bool {
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return a < b
}
