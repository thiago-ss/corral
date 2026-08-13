package ocxadapter_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"corral/internal/adapter"
	"corral/internal/clock"
	"corral/internal/graph"
	"corral/internal/livetest"
	"corral/internal/ocx"
	"corral/internal/ocxadapter"
	"corral/internal/sched"
	"corral/internal/spike"
	"corral/internal/store"
	"corral/internal/verify"
)

func TestStartRejectsDuplicateAndClosedDriver(t *testing.T) {
	var mu sync.Mutex
	created := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/session":
			mu.Lock()
			created++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "ses_unit"})
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_unit/prompt_async":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_unit/abort":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/global/event":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	drv := ocxadapter.New(ocx.New(srv.URL, t.TempDir()), ocxadapter.Options{})
	t.Cleanup(drv.Close)
	a := adapter.Attempt{ID: "run/n/1", NodeID: "n", Objective: "work"}
	if _, err := drv.Start(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if _, err := drv.Start(context.Background(), a); err == nil || !strings.Contains(err.Error(), "already started") {
		t.Fatalf("duplicate Start error = %v", err)
	}
	mu.Lock()
	gotCreated := created
	mu.Unlock()
	if gotCreated != 1 {
		t.Fatalf("created sessions = %d, want 1", gotCreated)
	}
	drv.Close()
	if _, err := drv.Start(context.Background(), adapter.Attempt{ID: "run/n/2", NodeID: "n", Objective: "work"}); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Start after Close error = %v", err)
	}
}

func TestStartUsesAttemptModelOverrideOnPromptWire(t *testing.T) {
	tests := []struct {
		name         string
		driverModel  string
		attemptModel string
		wantProvider string
		wantModel    string
	}{
		{
			name:         "attempt override",
			driverModel:  "anthropic/claude-sonnet-4",
			attemptModel: "openai/gpt-5",
			wantProvider: "openai",
			wantModel:    "gpt-5",
		},
		{
			name:         "driver fallback",
			driverModel:  "anthropic/claude-sonnet-4",
			wantProvider: "anthropic",
			wantModel:    "claude-sonnet-4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got struct {
				Model struct {
					ProviderID string `json:"providerID"`
					ModelID    string `json:"modelID"`
				} `json:"model"`
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/session":
					_ = json.NewEncoder(w).Encode(map[string]string{"id": "ses_model"})
				case r.Method == http.MethodGet && r.URL.Path == "/global/event":
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(http.StatusOK)
					if f, ok := w.(http.Flusher); ok {
						f.Flush()
					}
					<-r.Context().Done()
				case r.Method == http.MethodPost && r.URL.Path == "/session/ses_model/prompt_async":
					if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
						t.Errorf("decode prompt: %v", err)
					}
					w.WriteHeader(http.StatusNoContent)
				case r.Method == http.MethodGet && r.URL.Path == "/permission":
					_ = json.NewEncoder(w).Encode([]any{})
				case r.Method == http.MethodPost && r.URL.Path == "/session/ses_model/abort":
					w.WriteHeader(http.StatusNoContent)
				default:
					http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
				}
			}))
			t.Cleanup(srv.Close)

			drv := ocxadapter.New(ocx.New(srv.URL, t.TempDir()), ocxadapter.Options{Model: tt.driverModel})
			t.Cleanup(drv.Close)
			_, err := drv.Start(context.Background(), adapter.Attempt{
				ID: "run/n/1", NodeID: "n", Objective: "work", Model: tt.attemptModel,
			})
			if err != nil {
				t.Fatal(err)
			}
			if got.Model.ProviderID != tt.wantProvider || got.Model.ModelID != tt.wantModel {
				t.Fatalf("prompt model = %#v, want providerID=%q modelID=%q", got.Model, tt.wantProvider, tt.wantModel)
			}
		})
	}
}

func TestStartSubscribesBeforePromptCanEmitPermission(t *testing.T) {
	streamReady := make(chan struct{})
	permissionSent := make(chan struct{})
	var sendOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/session":
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "ses_fast"})
		case r.Method == http.MethodGet && r.URL.Path == "/global/event":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			f, _ := w.(http.Flusher)
			f.Flush()
			close(streamReady)
			<-permissionSent
			payload, _ := json.Marshal(map[string]any{
				"type":       "permission.asked",
				"properties": map[string]any{"sessionID": "ses_fast", "id": "perm-fast"},
			})
			frame, _ := json.Marshal(map[string]any{"payload": json.RawMessage(payload)})
			_, _ = fmt.Fprintf(w, "data: %s\n\n", frame)
			f.Flush()
			<-r.Context().Done()
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_fast/prompt_async":
			select {
			case <-streamReady:
			default:
				t.Error("prompt arrived before event subscription was ready")
			}
			sendOnce.Do(func() { close(permissionSent) })
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/permission":
			select {
			case <-permissionSent:
				_ = json.NewEncoder(w).Encode([]map[string]string{{"id": "perm-fast", "sessionID": "ses_fast"}})
			default:
				_ = json.NewEncoder(w).Encode([]any{})
			}
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_fast/abort":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	drv := ocxadapter.New(ocx.New(srv.URL, t.TempDir()), ocxadapter.Options{PollInterval: time.Hour})
	t.Cleanup(drv.Close)
	sess, err := drv.Start(context.Background(), adapter.Attempt{ID: "run/n/1", NodeID: "n", Objective: "work"})
	if err != nil {
		t.Fatal(err)
	}
	ps := sess.(adapter.PermissionSession)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if id, ok, _ := ps.PendingPermission(context.Background()); ok && id == "perm-fast" {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("permission emitted during prompt was lost")
}

func TestStartFallsBackToRESTWhenEventStreamIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/session":
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "ses_rest"})
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_rest/prompt_async":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/session/ses_rest/message":
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"info": map[string]any{
					"id": "msg-rest", "role": "assistant",
					"sessionID": "ses_rest", "finish": "stop",
				},
				"parts": []any{},
			}})
		case r.Method == http.MethodGet && r.URL.Path == "/permission":
			_ = json.NewEncoder(w).Encode([]any{})
		case r.Method == http.MethodGet && r.URL.Path == "/global/event":
			http.Error(w, "events unavailable", http.StatusServiceUnavailable)
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_rest/abort":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	drv := ocxadapter.New(ocx.New(srv.URL, t.TempDir()), ocxadapter.Options{
		PollInterval:       5 * time.Millisecond,
		StreamReadyTimeout: 20 * time.Millisecond,
	})
	t.Cleanup(drv.Close)
	if _, err := drv.Start(context.Background(), adapter.Attempt{
		ID: "run/n/1", NodeID: "n", Objective: "work",
	}); err != nil {
		t.Fatalf("Start with unavailable SSE: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := drv.Step(context.Background(), time.Now()); len(got) > 0 {
			if got[0].Status != adapter.StatusIdle {
				t.Fatalf("REST completion status = %q, want idle", got[0].Status)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("REST polling did not complete attempt while SSE was unavailable")
}

func TestPermissionReconcilesAfterEventGapWithoutBlockingCallers(t *testing.T) {
	streamReady := make(chan struct{})
	permissionPolled := make(chan struct{})
	var pollOnce sync.Once
	var mu sync.Mutex
	pending := false
	blockPermission := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/session":
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "ses_gap"})
		case r.Method == http.MethodGet && r.URL.Path == "/global/event":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			select {
			case <-streamReady:
			default:
				close(streamReady)
			}
			// End this subscription without a permission event. The durable
			// permission poll must recover the missed request.
			return
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_gap/prompt_async":
			<-streamReady
			mu.Lock()
			pending = true
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/permission":
			mu.Lock()
			isPending := pending
			block := blockPermission
			mu.Unlock()
			if block {
				<-r.Context().Done()
				return
			}
			if isPending {
				pollOnce.Do(func() { close(permissionPolled) })
				_ = json.NewEncoder(w).Encode([]map[string]string{{
					"id": "perm-gap", "sessionID": "ses_gap",
				}})
				return
			}
			_ = json.NewEncoder(w).Encode([]any{})
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_gap/abort":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	drv := ocxadapter.New(ocx.New(srv.URL, t.TempDir()), ocxadapter.Options{
		PollInterval:       5 * time.Millisecond,
		StreamReadyTimeout: 100 * time.Millisecond,
	})
	t.Cleanup(drv.Close)
	sess, err := drv.Start(context.Background(), adapter.Attempt{
		ID: "run/n/1", NodeID: "n", Objective: "work",
	})
	if err != nil {
		t.Fatal(err)
	}
	ps := sess.(adapter.PermissionSession)

	select {
	case <-permissionPolled:
	case <-time.After(time.Second):
		t.Fatal("background reconciliation never polled durable permissions")
	}
	deadline := time.Now().Add(time.Second)
	for {
		if id, ok, err := ps.PendingPermission(context.Background()); err != nil {
			t.Fatal(err)
		} else if ok && id == "perm-gap" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("missed permission was not reconciled into local state")
		}
		time.Sleep(time.Millisecond)
	}

	mu.Lock()
	blockPermission = true
	mu.Unlock()
	returned := make(chan struct{})
	go func() {
		_, _, _ = ps.PendingPermission(context.Background())
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("PendingPermission performed blocking network I/O")
	}
}

func TestAbortFailureDoesNotReportLocalAbort(t *testing.T) {
	var abortCalls int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/session":
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "ses_abort"})
		case r.Method == http.MethodGet && r.URL.Path == "/global/event":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_abort/prompt_async":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_abort/abort":
			mu.Lock()
			abortCalls++
			call := abortCalls
			mu.Unlock()
			if call == 1 {
				http.Error(w, "abort failed", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	drv := ocxadapter.New(ocx.New(srv.URL, t.TempDir()), ocxadapter.Options{})
	t.Cleanup(drv.Close)
	sess, err := drv.Start(context.Background(), adapter.Attempt{ID: "run/n/1", NodeID: "n", Objective: "work"})
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.Abort(context.Background()); err == nil {
		t.Fatal("provider abort failure was hidden")
	}
	if status, err := sess.Status(context.Background()); err == nil && status == adapter.StatusAborted {
		t.Fatal("failed provider abort was reported as locally aborted")
	}
}

func TestPermissionResponseValidatesIDAndRetainsFailedDecision(t *testing.T) {
	var mu sync.Mutex
	permissionCalls := 0
	failDecision := true
	pending := true
	gotReply := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/session":
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "ses_perm"})
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_perm/prompt_async":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_perm/abort":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/permission":
			mu.Lock()
			isPending := pending
			mu.Unlock()
			if isPending {
				_ = json.NewEncoder(w).Encode([]map[string]string{{"id": "perm-1", "sessionID": "ses_perm"}})
			} else {
				_ = json.NewEncoder(w).Encode([]any{})
			}
		case r.Method == http.MethodPost && r.URL.Path == "/permission/perm-1/reply":
			var body struct {
				Reply string `json:"reply"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode permission reply: %v", err)
			}
			mu.Lock()
			permissionCalls++
			fail := failDecision
			gotReply = body.Reply
			if !fail {
				pending = false
			}
			mu.Unlock()
			if fail {
				http.Error(w, "try again", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/global/event":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			f, _ := w.(http.Flusher)
			f.Flush()
			// Deliberately emit no permission event. The durable /permission
			// endpoint must reconcile a request missed during an SSE gap.
			<-r.Context().Done()
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	drv := ocxadapter.New(ocx.New(srv.URL, t.TempDir()), ocxadapter.Options{PollInterval: time.Hour})
	t.Cleanup(drv.Close)
	sess, err := drv.Start(context.Background(), adapter.Attempt{ID: "run/n/1", NodeID: "n", Objective: "work"})
	if err != nil {
		t.Fatal(err)
	}
	ps := sess.(adapter.PermissionSession)
	deadline := time.Now().Add(time.Second)
	for {
		if id, ok, _ := ps.PendingPermission(context.Background()); ok && id == "perm-1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("permission event was not tracked")
		}
		time.Sleep(time.Millisecond)
	}
	if err := ps.RespondPermission(context.Background(), "wrong", true); err == nil {
		t.Fatal("wrong permission ID was accepted")
	}
	mu.Lock()
	gotCalls := permissionCalls
	mu.Unlock()
	if gotCalls != 0 {
		t.Fatalf("wrong permission reached provider %d times", gotCalls)
	}
	if err := ps.RespondPermission(context.Background(), "perm-1", true); err == nil {
		t.Fatal("provider failure was hidden")
	}
	if id, ok, _ := ps.PendingPermission(context.Background()); !ok || id != "perm-1" {
		t.Fatalf("failed response cleared pending permission: %q, %v", id, ok)
	}
	mu.Lock()
	failDecision = false
	mu.Unlock()
	if err := ps.RespondPermission(context.Background(), "perm-1", true); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	reply := gotReply
	mu.Unlock()
	if reply != "once" {
		t.Fatalf("allow reply = %q, want once", reply)
	}
	if id, ok, _ := ps.PendingPermission(context.Background()); ok {
		t.Fatalf("successful response left permission pending: %q", id)
	}
}

func TestIntermediateFinishDoesNotComplete(t *testing.T) {
	var mu sync.Mutex
	finish := "tool-calls"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/session":
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "ses_finish"})
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_finish/prompt_async":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/session/ses_finish/message":
			mu.Lock()
			current := finish
			mu.Unlock()
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"info":  map[string]any{"id": "msg-1", "role": "assistant", "sessionID": "ses_finish", "finish": current},
				"parts": []any{},
			}})
		case r.Method == http.MethodPost && r.URL.Path == "/session/ses_finish/abort":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/global/event":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
		default:
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	drv := ocxadapter.New(ocx.New(srv.URL, t.TempDir()), ocxadapter.Options{PollInterval: 5 * time.Millisecond})
	t.Cleanup(drv.Close)
	if _, err := drv.Start(context.Background(), adapter.Attempt{ID: "run/n/1", NodeID: "n", Objective: "work"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if got := drv.Step(context.Background(), time.Now()); len(got) != 0 {
		t.Fatalf("intermediate finish emitted completion: %+v", got)
	}
	mu.Lock()
	finish = "stop"
	mu.Unlock()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := drv.Step(context.Background(), time.Now()); len(got) > 0 {
			if got[0].Status != adapter.StatusIdle {
				t.Fatalf("terminal status = %q, want idle", got[0].Status)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("finish=stop did not emit completion")
}

const (
	w1Prompt = "Create a file named alpha.txt containing exactly one line: CORRAL-OC1. Do not run any other commands."
	w2Prompt = "Append one line to beta.txt every second, 30 lines total, numbered 1 to 30, using bash. Keep going until the loop finishes. Do not stop early."
)

func ocNode(id graph.NodeID, prompt string) *graph.Node {
	return &graph.Node{
		ID:                 id,
		Type:               graph.NodeAgent,
		Objective:          prompt,
		AcceptanceCriteria: []string{"file produced"},
		Priority:           graph.PriorityNormal,
		RetryPolicy:        graph.RetryPolicy{MaxRetries: 1, Backoff: 5 * time.Second},
		Budget:             graph.Budget{MaxDuration: 12 * time.Minute},
	}
}

func TestOpenCodeAdapterParallelAndCancel(t *testing.T) {
	livetest.SkipIfDisabled(t)
	if _, err := exec.LookPath("opencode"); err != nil {
		t.Skip("opencode binary not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	proj, err := os.MkdirTemp("", "corral-oc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(proj) })
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"commit", "-q", "--allow-empty", "-m", "init"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = proj
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}

	srv, err := spike.StartServer(ctx, proj, 0, os.Stderr)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(srv.Stop)

	st, err := store.Open(filepath.Join(t.TempDir(), "oc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	oc := ocx.New(srv.Base, proj)
	drv := ocxadapter.New(oc, ocxadapter.Options{PollInterval: 400 * time.Millisecond})
	t.Cleanup(func() { drv.Close() })
	ver := sched.NewFakeVerifier(nil, sched.Verdict{Pass: true, Evidence: "ok"})
	s := sched.New(st, drv, ver, clock.Real{}, sched.Options{Concurrency: 2})

	g := &graph.Graph{Nodes: []*graph.Node{ocNode("w1", w1Prompt), ocNode("w2", w2Prompt)}}
	h, err := s.Create(ctx, "run-oc", g)
	if err != nil {
		t.Fatal(err)
	}

	// Track peak concurrent busy sessions from the server's perspective.
	var peakMu sync.Mutex
	peak := 0
	stopPoll := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPoll:
				return
			case <-ticker.C:
				sts, err := oc.SessionStatus(ctx)
				if err != nil {
					continue
				}
				busy := 0
				for _, s2 := range sts {
					if s2.Type == "busy" {
						busy++
					}
				}
				peakMu.Lock()
				if busy > peak {
					peak = busy
				}
				peakMu.Unlock()
			}
		}
	}()

	// Drive the run; cancel w2 once it is running.
	runDone := make(chan error, 1)
	go func() { runDone <- h.Run(ctx, 200*time.Millisecond) }()

	w2Canceled := false
	deadline := time.Now().Add(360 * time.Second)
	for !h.Done() && time.Now().Before(deadline) {
		if !w2Canceled {
			if st2, _ := h.State("w2"); st2 == graph.StateRunning {
				// Let the attempt make progress, then cancel mid-run.
				time.Sleep(5 * time.Second)
				if err := h.CancelNode(ctx, "w2"); err != nil {
					t.Logf("cancel: %v", err)
				} else {
					w2Canceled = true
					t.Log("w2 canceled mid-run")
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err := <-runDone; err != nil {
		for _, id := range []string{"w1", "w2"} {
			if st2, _ := h.State(graph.NodeID(id)); st2 != "" {
				t.Logf("at timeout: %s -> %s", id, st2)
			}
		}
		evs, evErr := st.Events(context.Background(), "run-oc")
		if evErr == nil {
			for _, ev := range evs {
				if ev.NodeID != "" {
					t.Logf("event %d %s %s->%s att=%s", ev.Seq, ev.Type, ev.From, ev.To, ev.AttemptID)
				}
			}
		}
		t.Fatalf("run: %v", err)
	}
	close(stopPoll)
	wg.Wait()
	peakMu.Lock()
	pk := peak
	peakMu.Unlock()

	if !w2Canceled {
		t.Fatal("w2 was never observed running; cancellation not exercised")
	}
	if !h.Done() {
		t.Fatal("run did not complete")
	}
	if pk < 2 {
		t.Errorf("peak concurrent busy sessions = %d, want >= 2 (parallel execution)", pk)
	}

	// Terminal states.
	if st1, _ := h.State("w1"); st1 != graph.StateDone {
		t.Errorf("w1 state = %s, want done", st1)
	}
	if st2, _ := h.State("w2"); st2 != graph.StateCanceled {
		t.Errorf("w2 state = %s, want canceled", st2)
	}

	// Exactly one attempt each: no duplicate completion from duplicate
	// or missing events.
	for _, id := range []string{"w1", "w2"} {
		atts, err := st.Attempts(ctx, "run-oc", id)
		if err != nil {
			t.Fatal(err)
		}
		if len(atts) != 1 {
			t.Fatalf("%s attempts = %d, want exactly 1 (no duplicates)", id, len(atts))
		}
		at := atts[0]
		if !strings.HasPrefix(at.SessionID, "ses_") {
			t.Errorf("%s session id = %q, want ses_ prefix (OpenCode session id recorded)", id, at.SessionID)
		}
		if at.ServerID != srv.Base {
			t.Errorf("%s server id = %q, want %q", id, at.ServerID, srv.Base)
		}
	}
	atts, _ := st.Attempts(ctx, "run-oc", "w1")
	if atts[0].Status != "done" {
		t.Errorf("w1 attempt status = %s, want done", atts[0].Status)
	}
	atts, _ = st.Attempts(ctx, "run-oc", "w2")
	if atts[0].Status != "aborted" {
		t.Errorf("w2 attempt status = %s, want aborted", atts[0].Status)
	}

	// w1's work is real.
	data, err := os.ReadFile(filepath.Join(proj, "alpha.txt"))
	if err != nil {
		t.Fatalf("alpha.txt missing: %v", err)
	}
	if got := strings.TrimSpace(string(data)); got != "CORRAL-OC1" {
		t.Errorf("alpha.txt = %q, want CORRAL-OC1", got)
	}
}

func TestOpenCodeEvidenceGates(t *testing.T) {
	livetest.SkipIfDisabled(t)
	if _, err := exec.LookPath("opencode"); err != nil {
		t.Skip("opencode binary not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	proj, err := os.MkdirTemp("", "corral-gates-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(proj) })
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"commit", "-q", "--allow-empty", "-m", "init"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = proj
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	srv, err := spike.StartServer(ctx, proj, 0, os.Stderr)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(srv.Stop)

	st, err := store.Open(filepath.Join(t.TempDir(), "gates.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	oc := ocx.New(srv.Base, proj)
	drv := ocxadapter.New(oc, ocxadapter.Options{PollInterval: 400 * time.Millisecond})
	t.Cleanup(func() { drv.Close() })

	// w1 writes a fixed marker and passes a grep gate deterministically.
	// w2 writes a marker that the gate deliberately rejects, so the node
	// fails verification permanently and its dependent becomes blocked.
	eng := verify.New(proj)
	w1 := ocNode("w1", "Create a file named gate.txt with some content. Do not run any other commands.")
	w1.Verification = &graph.Verification{Kind: "command", Command: []string{"test", "-s", "gate.txt"}}
	w1.RetryPolicy = graph.RetryPolicy{MaxRetries: 1, Backoff: 5 * time.Second}

	w2 := ocNode("w2", "Create a file named bad.txt containing exactly one line: WRONG-CONTENT. Do not run any other commands.")
	w2.Verification = &graph.Verification{Kind: "command", Command: []string{"grep", "-q", "CORRAL-GATE-OK", "bad.txt"}}
	w2.RetryPolicy = graph.RetryPolicy{MaxRetries: 1, Backoff: 5 * time.Second}

	w3 := ocNode("w3", "Create a file named never.txt containing one line: NEVER.")
	w3.DependsOn = []graph.NodeID{"w2"}
	w3.Verification = &graph.Verification{Kind: "command", Command: []string{"test", "-f", "never.txt"}}

	s := sched.New(st, drv, &sched.EngineVerifier{Eng: eng}, clock.Real{}, sched.Options{Concurrency: 3})
	h, err := s.Create(ctx, "run-gates", &graph.Graph{Nodes: []*graph.Node{w1, w2, w3}})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx, 250*time.Millisecond); err != nil {
		for _, id := range []string{"w1", "w2", "w3"} {
			if st2, _ := h.State(graph.NodeID(id)); st2 != "" {
				t.Logf("at timeout: %s -> %s", id, st2)
			}
		}
		evs, evErr := st.Events(context.Background(), "run-gates")
		if evErr == nil {
			for _, ev := range evs {
				if ev.NodeID != "" {
					t.Logf("event %d %s %s->%s", ev.Seq, ev.Type, ev.From, ev.To)
				}
			}
		}
		t.Fatalf("run: %v", err)
	}

	if st1, _ := h.State("w1"); st1 != graph.StateDone {
		t.Errorf("w1 state = %s, want done (gate passed)", st1)
	}
	if st2, _ := h.State("w2"); st2 != graph.StateFailed {
		t.Errorf("w2 state = %s, want failed (gate permanently rejected)", st2)
	}
	if st3, _ := h.State("w3"); st3 != graph.StateBlocked {
		t.Errorf("w3 state = %s, want blocked (dep failed)", st3)
	}
	// w2 attempted at most maxRetries+1 times; w3 never ran.
	atts, _ := st.Attempts(ctx, "run-gates", "w2")
	if len(atts) > 2 {
		t.Errorf("w2 attempts = %d, want <= 2 (bounded retries)", len(atts))
	}
	n, _ := st.CountAttempts(ctx, "run-gates", "w3")
	if n != 0 {
		t.Errorf("w3 attempts = %d, want 0", n)
	}
}

var _ = adapter.StatusRunning
