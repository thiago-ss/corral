package ocx

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPromptAsyncEncodesModelReference(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	client := New(srv.URL, t.TempDir())
	if err := client.PromptAsync(context.Background(), "ses_1", "work", "openrouter/anthropic/claude-sonnet-4"); err != nil {
		t.Fatal(err)
	}
	model, ok := body["model"].(map[string]any)
	if !ok {
		t.Fatalf("model = %#v, want object", body["model"])
	}
	if model["providerID"] != "openrouter" || model["modelID"] != "anthropic/claude-sonnet-4" {
		t.Fatalf("model = %#v", model)
	}
}

func TestPromptAsyncRejectsAmbiguousModel(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer srv.Close()

	client := New(srv.URL, t.TempDir())
	err := client.PromptAsync(context.Background(), "ses_1", "work", "claude-sonnet-4")
	if err == nil || !strings.Contains(err.Error(), "provider/model") {
		t.Fatalf("error = %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("HTTP requests = %d, want 0", got)
	}
}

func TestMessagesDecodeRoleDependentSummary(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[
			{"info":{"role":"user","summary":{"title":"change","body":"body","diffs":[{"file":"a.txt","patch":"+a","additions":1,"deletions":0,"status":"added"}]}},"parts":[]},
			{"info":{"role":"assistant","summary":true,"finish":"stop"},"parts":[]}
		]`))
	}))
	defer srv.Close()

	messages, err := New(srv.URL, t.TempDir()).Messages(context.Background(), "ses_1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0].Info.Summary == nil || len(messages[0].Info.Summary.Diffs) != 1 {
		t.Fatalf("user summary = %#v", messages)
	}
	if messages[1].Info.Summary == nil || !messages[1].Info.Summary.Compacted {
		t.Fatalf("assistant summary = %#v", messages[1].Info.Summary)
	}
}
