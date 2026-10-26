package improvement

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FaintGhost/loom/internal/model"
	"github.com/FaintGhost/loom/internal/store"
	"github.com/FaintGhost/loom/internal/worker"
)

func TestFindingRoutingAndEvidenceGate(t *testing.T) {
	evidence := []Evidence{{ID: "jobs/1/verify.log", ReportID: 1, Text: "the sandbox leaked writable git metadata"}}
	repositories := map[string]string{"target_project": "owner/shop", "automation": "owner/loop", "effectgo": "owner/effectgo"}
	valid := Finding{Category: "automation", Repository: "owner/loop", Kind: "bug", Work: new(1.0), Fingerprint: "sandbox-git-metadata", Title: "Protect metadata", Change: "Mount shared Git metadata read-only", Acceptance: "Attempted metadata writes fail", Evidence: []Citation{{EvidenceID: evidence[0].ID, Quote: evidence[0].Text}}}
	if err := validateFinding(valid, evidence, repositories); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Finding)
	}{
		{"business routing", func(f *Finding) { f.Repository = "owner/shop" }},
		{"wrong tooling route", func(f *Finding) { f.Category = "effectgo" }},
		{"missing evidence", func(f *Finding) { f.Evidence = nil }},
		{"invented evidence", func(f *Finding) {
			f.Evidence = []Citation{{EvidenceID: evidence[0].ID, Quote: "an invented unsupported finding"}}
		}},
		{"missing acceptance", func(f *Finding) { f.Acceptance = "" }},
		{"missing kind", func(f *Finding) { f.Kind = "" }},
		{"unknown kind", func(f *Finding) { f.Kind = "question" }},
		{"missing work", func(f *Finding) { f.Work = nil }},
		{"negative work", func(f *Finding) { f.Work = new(-0.1) }},
		{"out-of-range work", func(f *Finding) { f.Work = new(2.1) }},
		{"non-finite work", func(f *Finding) { f.Work = new(math.NaN()) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := valid
			tc.mutate(&f)
			if validateFinding(f, evidence, repositories) == nil {
				t.Fatal("unsafe finding accepted")
			}
		})
	}
}

func TestFindingDeduplicationIsDurableAndScoped(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "go-store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	finding := Finding{Category: "automation", Repository: "owner/loop", Fingerprint: "Sandbox Git Metadata"}
	first, err := db.AddReport(t.Context(), model.Report{Body: "evidence-backed task", Source: "retrospective", SourceKey: findingKey(finding), Repository: finding.Repository, Status: model.AwaitingApproval})
	if err != nil {
		t.Fatal(err)
	}
	finding.Title = "A renamed title on another day"
	finding.Fingerprint = "  sandbox   git metadata "
	again, err := db.AddReport(t.Context(), model.Report{Body: "new wording", Source: "retrospective", SourceKey: findingKey(finding), Repository: finding.Repository, Status: model.AwaitingApproval})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != first.ID || again.Body != first.Body {
		t.Fatal("repeat created or replaced an existing task")
	}
	finding.Repository = "owner/effectgo"
	finding.Category = "effectgo"
	if findingKey(finding) == first.SourceKey {
		t.Fatal("distinct tooling project shares dedup key")
	}
}

func TestRetrospectiveTargetOnlyAndModelFailureNeverCreateTasks(t *testing.T) {
	for _, testCase := range []string{"target-only", "unavailable", "missing-findings"} {
		t.Run(testCase, func(t *testing.T) {
			home := t.TempDir()
			db, err := store.Open(filepath.Join(home, "go-store.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			report, err := db.Add(t.Context(), "Shipping business rule failed")
			if err != nil {
				t.Fatal(err)
			}
			job := filepath.Join(home, "jobs", fmt.Sprint(report.ID), "attempts", "1")
			if err := os.MkdirAll(job, 0700); err != nil {
				t.Fatal(err)
			}
			log := filepath.Join(job, "verify.log")
			quote := "FAIL: requires pallet count for air shipments"
			if err := os.WriteFile(log, []byte(quote), 0600); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if testCase == "unavailable" {
					http.Error(w, "model unavailable", 503)
					return
				}
				answer := classification{Findings: []Finding{{Category: "target_project", Repository: "owner/shop", Kind: "bug", Work: new(1.0), Fingerprint: "shipping-validation", Title: "Shipping rule", Change: "Correct target shipping validation", Acceptance: "Business test passes", Evidence: []Citation{{EvidenceID: log, Quote: quote}}}}}
				data, err := json.Marshal(answer)
				if err != nil {
					t.Error(err)
					return
				}
				if testCase == "missing-findings" {
					data = []byte("{}")
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]string{"content": string(data)}}}}); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			t.Setenv("FBGO_RETRO_TEST_KEY", "test-key")
			cfg := worker.Config{GitHubRepo: "owner/shop", MagpieURL: server.URL, MagpieKeyEnv: "FBGO_RETRO_TEST_KEY", TitleModel: "configured-model", Retro: worker.RetroConfig{Enabled: true, Hours: 24}}
			err = Run(t.Context(), home, cfg, db)
			if (testCase == "target-only") != (err == nil) {
				t.Fatalf("unexpected result: %v", err)
			}
			rows, err := db.List(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 {
				t.Fatalf("created %d tasks from target-only/failed classification", len(rows)-1)
			}
			results, err := filepath.Glob(filepath.Join(home, "jobs", "retro-*", "result.json"))
			if err != nil || len(results) != 1 {
				t.Fatalf("missing retrospective evidence: %v %v", results, err)
			}
			data, err := os.ReadFile(results[0])
			if err != nil {
				t.Fatal(err)
			}
			if testCase != "target-only" && !strings.Contains(string(data), `"error"`) {
				t.Fatal("failed model result was not retained")
			}
			data, err = os.ReadFile(log)
			if err != nil || string(data) != quote {
				t.Fatal("failed business check evidence was changed")
			}
		})
	}
}

func TestCompatibilityPinFailureRetainsEvidence(t *testing.T) {
	home := t.TempDir()
	folder, err := CheckCompatibility(t.Context(), home, worker.Config{})
	if err == nil {
		t.Fatal("accepted an unpinned tool")
	}
	data, readErr := os.ReadFile(filepath.Join(folder, "result.json"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(data), "pinned effectgo_sha256") {
		t.Fatalf("lost failed compatibility check: %s", data)
	}
}

func TestFindingUsesDestinationAutomationPolicy(t *testing.T) {
	cfg := worker.Config{AutoKinds: []string{"bug"}, MaxAutoWork: 1.6}
	for _, tc := range []struct {
		name, kind string
		work       float64
		want       model.Status
	}{
		{"one-line bug", "bug", 0, model.New},
		{"small bug", "bug", 1, model.New},
		{"exact work boundary", "bug", 1.6, model.New},
		{"above work boundary", "bug", 1.6001, model.AwaitingApproval},
		{"substantial bug", "bug", 2, model.AwaitingApproval},
		{"small feature", "feature_request", 0, model.AwaitingApproval},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := findingStatus(cfg, Finding{Kind: tc.kind, Work: new(tc.work)}); got != tc.want {
				t.Fatalf("status = %s, want %s", got, tc.want)
			}
		})
	}
	if got := findingStatus(worker.Config{}, Finding{Kind: "bug", Work: new(0.0)}); got != model.AwaitingApproval {
		t.Fatal("missing auto policy queued work")
	}
	cfg.AutoKinds = []string{"bug", "feature_request"}
	if got := findingStatus(cfg, Finding{Kind: "feature_request", Work: new(1.0)}); got != model.New {
		t.Fatal("explicit destination feature policy was ignored")
	}
}

func TestCollectKeepsFailuresBeforeArtifactsExist(t *testing.T) {
	for _, emptyDirectory := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty-directory-%t", emptyDirectory), func(t *testing.T) {
			home := t.TempDir()
			db, err := store.Open(filepath.Join(home, "go-store.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			report, err := db.AddReport(t.Context(), model.Report{Body: "Tool startup failed", Status: model.Failed, Error: "failed before attempt directory creation"})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.RecordEvent(t.Context(), report.ID, "guard-blocked", "refused unsafe worktree ownership before creating artifacts"); err != nil {
				t.Fatal(err)
			}
			if emptyDirectory {
				if err := os.MkdirAll(filepath.Join(home, "jobs", fmt.Sprint(report.ID)), 0700); err != nil {
					t.Fatal(err)
				}
			}
			evidence, err := collect(t.Context(), home, db, time.Now().Add(-time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			foundReport, foundEvent := false, false
			for _, item := range evidence {
				if item.ID == fmt.Sprintf("report:%d", report.ID) && strings.Contains(item.Text, report.Error) {
					foundReport = true
				}
				if item.ID == fmt.Sprintf("events:%d", report.ID) && strings.Contains(item.Text, "guard-blocked") && strings.Contains(item.Text, "unsafe worktree ownership") {
					foundEvent = true
				}
			}
			if !foundReport || !foundEvent {
				t.Fatalf("artifactless failure lost: report=%t event=%t", foundReport, foundEvent)
			}
		})
	}
}

func TestRetrospectiveWithholdsOpaqueConfiguredCredentials(t *testing.T) {
	for _, stage := range []string{"input", "finding"} {
		t.Run(stage, func(t *testing.T) {
			home := t.TempDir()
			db, err := store.Open(filepath.Join(home, "go-store.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			envToken, fileToken := "opaque-env-value-\"a8f31f11\"", "opaque-file-value-d9e41c22"
			t.Setenv("FBGO_RETRO_OPAQUE_TOKEN", envToken)
			t.Setenv("FBGO_RETRO_MODEL_KEY", "test-model-key")
			credentialFile := filepath.Join(home, "model.key")
			if err := os.WriteFile(credentialFile, []byte(fileToken), 0600); err != nil {
				t.Fatal(err)
			}
			body := "Post-review validation failed"
			if stage == "input" {
				body += " " + envToken
			}
			report, err := db.Add(t.Context(), body)
			if err != nil {
				t.Fatal(err)
			}
			job := filepath.Join(home, "jobs", fmt.Sprint(report.ID), "attempts", "1")
			if err := os.MkdirAll(job, 0700); err != nil {
				t.Fatal(err)
			}
			log := filepath.Join(job, "verify-fix.log")
			quote := "post-review validation failed with exit 7"
			if err := os.WriteFile(log, []byte(quote), 0600); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var request struct {
					Messages []struct {
						Content string `json:"content"`
					} `json:"messages"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				if len(request.Messages) != 2 || !strings.Contains(request.Messages[1].Content, quote) {
					t.Error("model input omitted verify-fix.log failure")
				}
				answer := classification{Findings: []Finding{{Category: "target_project", Repository: "owner/shop", Kind: "bug", Work: new(1.0), Fingerprint: "post-review-gate", Title: "Gate failure", Change: "Leaked model credential " + fileToken, Acceptance: "Validation passes", Evidence: []Citation{{EvidenceID: log, Quote: quote}}}}}
				data, err := json.Marshal(answer)
				if err != nil {
					t.Error(err)
					return
				}
				if err := json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]string{"content": string(data)}}}}); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			cfg := worker.Config{GitHubRepo: "owner/shop", MagpieURL: server.URL, TitleModel: "configured-model", MagpieKeyEnv: "FBGO_RETRO_MODEL_KEY", MagpieKeyFile: credentialFile, DiscordTokenEnv: "FBGO_RETRO_OPAQUE_TOKEN", Retro: worker.RetroConfig{Enabled: true}}
			if err := Run(t.Context(), home, cfg, db); err == nil || !strings.Contains(err.Error(), "credential") {
				t.Fatalf("credential was not withheld: %v", err)
			}
			wantCalls := int32(0)
			if stage == "finding" {
				wantCalls = 1
			}
			if calls.Load() != wantCalls {
				t.Fatalf("model calls = %d, want %d", calls.Load(), wantCalls)
			}
			files, err := filepath.Glob(filepath.Join(home, "jobs", "retro-*", "*.json"))
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range files {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(data), envToken) || strings.Contains(string(data), fileToken) {
					t.Fatalf("credential persisted in %s", path)
				}
			}
			rows, err := db.List(t.Context())
			if err != nil || len(rows) != 1 {
				t.Fatalf("credential created a task: %v %v", rows, err)
			}
		})
	}
}
