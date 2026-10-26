package remote

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/FaintGhost/loom/internal/worker"
)

func TestChatHonorsLongerCallerBudget(t *testing.T) {
	previous := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = previous })
	t.Setenv("MODEL_BUDGET_TOKEN", "test-token")
	deadline := time.Now().Add(3 * time.Minute)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	http.DefaultTransport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		actual, ok := req.Context().Deadline()
		if !ok || actual.Before(deadline) {
			return nil, context.DeadlineExceeded
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"content":"{\"finding\":\"retained\"}"}}]}`))}, nil
	})
	var answer struct {
		Finding string `json:"finding"`
	}
	err := ChatJSON(ctx, worker.Config{MagpieURL: "https://model.invalid", MagpieKeyEnv: "MODEL_BUDGET_TOKEN", TitleModel: "test"}, "Return JSON", "Review evidence", &answer)
	if err != nil {
		t.Fatal(err)
	}
	if answer.Finding != "retained" {
		t.Fatalf("lost completed model result: %+v", answer)
	}
}
