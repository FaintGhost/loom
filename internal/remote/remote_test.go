package remote

import (
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestDiscordChunkUnicodeAndDurableNonce(t *testing.T) {
	text := strings.Repeat("😀", 1001) + strings.Repeat("文", 2001)
	chunks := DiscordChunks(text)
	if strings.Join(chunks, "") != text {
		t.Fatal("Discord chunking changed text")
	}
	for _, chunk := range chunks {
		if len(utf16.Encode([]rune(chunk))) > 2000 {
			t.Fatal("Discord UTF16 limit exceeded")
		}
	}
	prior := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = prior })
	t.Setenv("TEST_DISCORD_TOKEN", "test-token")
	sent := []map[string]any{}
	posts := 0
	http.DefaultTransport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Header.Get("Authorization") != "Bot test-token" {
			t.Error("wrong Discord authentication")
		}
		var payload any
		if req.Method == "GET" {
			payload = sent
		} else {
			posts++
			var message map[string]any
			if err := json.NewDecoder(req.Body).Decode(&message); err != nil {
				t.Fatal(err)
			}
			allowed := message["allowed_mentions"].(map[string]any)
			if len(allowed["parse"].([]any)) != 0 || allowed["replied_user"] != false || message["enforce_nonce"] != true {
				t.Error("unsafe Discord payload")
			}
			message["id"] = "500"
			sent = append(sent, message)
			payload = message
		}
		if req.Header.Get("User-Agent") != "DiscordBot (https://github.com/FaintGhost/loom, 1.0)" {
			t.Error("missing required DiscordBot User-Agent")
		}
		data, _ := json.Marshal(payload)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(data))), Header: make(http.Header)}, nil
	})
	cfg := worker.Config{DiscordTokenEnv: "TEST_DISCORD_TOKEN"}
	first, err := SendDiscordOnce(t.Context(), cfg, "123", "100", "reply", "durable-key")
	if err != nil {
		t.Fatal(err)
	}
	retry, err := SendDiscordOnce(t.Context(), cfg, "123", "100", "reply", "durable-key")
	if err != nil {
		t.Fatal(err)
	}
	if posts != 1 || len(first) != 1 || len(retry) != 1 || first[0] != retry[0] {
		t.Fatal("reconciled Discord delivery sent duplicate")
	}
}

func TestCompletedDiscordChunkNeedsNoNetwork(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row, err := db.Add(t.Context(), "report")
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]string{"channel": "123", "reply_to": "100", "text": "reply"})
	if _, err = db.Reserve(t.Context(), "delivery:chunk:0", "discord-chunk", row.ID, string(payload)); err != nil {
		t.Fatal(err)
	}
	if err = db.Complete(t.Context(), "delivery:chunk:0", "500"); err != nil {
		t.Fatal(err)
	}
	prior := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = prior })
	http.DefaultTransport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("completed chunk made a network request")
		return nil, nil
	})
	ids, err := SendDiscordDelivery(t.Context(), worker.Config{}, db, row.ID, "123", "100", "reply", "delivery")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != "500" {
		t.Fatal("persisted Discord receipt lost")
	}
}
