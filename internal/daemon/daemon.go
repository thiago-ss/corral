// Package daemon exposes the corral scheduler as an HTTP control API.
// The OpenCode plugin calls these endpoints; roles are enforced
// server-side so agents can only perform the actions their role allows.
package daemon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"corral/internal/graph"
	"corral/internal/sched"
	"corral/internal/store"
)

// Role identifies the OpenCode agent acting on the API.
type Role string

const (
	RoleOperator     Role = "operator"     // the human user
	RoleOrchestrator Role = "orchestrator" // graph controls only; no edits/bash
	RolePlanner      Role = "planner"      // read-only; produces graphs
	RoleWorker       Role = "worker"       // scoped edits/bash
	RoleReviewer     Role = "reviewer"     // read-only plus tests/diff
	RoleMerger       Role = "merger"       // restricted git operations
)

func parseRole(s string) (Role, bool) {
	switch Role(s) {
	case RoleOperator, RoleOrchestrator, RolePlanner, RoleWorker, RoleReviewer, RoleMerger:
		return Role(s), true
	}
	return "", false
}

// Planner produces a validated graph from a goal statement.
type Planner interface {
	Plan(ctx context.Context, goal string) (*graph.Graph, error)
}

type Daemon struct {
	st     *store.Store
	sched  *sched.Scheduler
	plan   Planner
	dir    string
	apiKey string
	ctx    context.Context

	mu   sync.Mutex
	runs map[string]*sched.RunHandle
}

// Dir returns the project directory the daemon manages.
func (d *Daemon) Dir() string { return d.dir }

// SetPlanner installs the planner (used by tests and lazy wiring).
func (d *Daemon) SetPlanner(p Planner) { d.plan = p }

func New(st *store.Store, s *sched.Scheduler, plan Planner, dir, apiKey string) *Daemon {
	return &Daemon{
		st: st, sched: s, plan: plan, dir: dir, apiKey: apiKey,
		ctx:  context.Background(),
		runs: map[string]*sched.RunHandle{},
	}
}

// SetContext replaces the daemon's background context (its lifetime).
func (d *Daemon) SetContext(ctx context.Context) { d.ctx = ctx }

// Resume loads persisted active/waiting runs so a daemon restart continues
// them without duplicate execution.
func (d *Daemon) Resume(ctx context.Context) error {
	// Rehydrate every run handle from the store; runs with a blocked node
	// or a running human gate simply wait for operator action.
	runs, err := d.st.ListRuns(ctx)
	if err != nil {
		return err
	}
	for _, r := range runs {
		if r.Status != "active" && r.Status != "waiting" {
			continue
		}
		h, err := d.sched.Load(ctx, r.ID)
		if err != nil {
			return fmt.Errorf("resume %s: %w", r.ID, err)
		}
		d.mu.Lock()
		d.runs[r.ID] = h
		d.mu.Unlock()
		go h.Run(ctx, 250*time.Millisecond)
	}
	return nil
}

func (d *Daemon) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", d.handleHealth)
	mux.HandleFunc("POST /api/plan", d.role(RolePlanner, RoleOrchestrator, RoleOperator)(d.handlePlan))
	mux.HandleFunc("POST /api/runs", d.role(RoleOrchestrator, RoleOperator)(d.handleCreateRun))
	mux.HandleFunc("GET /api/runs", d.handleListRuns)
	mux.HandleFunc("GET /api/runs/{id}", d.handleGetRun)
	mux.HandleFunc("GET /api/runs/{id}/watch", d.handleWatchRun)
	mux.HandleFunc("POST /api/runs/{id}/approve", d.role(RoleOperator, RoleOrchestrator)(d.handleApprove))
	mux.HandleFunc("POST /api/runs/{id}/reject", d.role(RoleOperator, RoleOrchestrator)(d.handleReject))
	mux.HandleFunc("POST /api/runs/{id}/cancel", d.role(RoleOperator, RoleOrchestrator)(d.handleCancel))
	mux.HandleFunc("POST /api/runs/{id}/retry", d.role(RoleOperator, RoleOrchestrator)(d.handleRetry))
	mux.HandleFunc("POST /api/runs/{id}/steer", d.role(RoleOperator, RoleOrchestrator)(d.handleSteer))
	mux.HandleFunc("POST /api/runs/{id}/permission", d.role(RoleOperator, RoleOrchestrator)(d.handlePermission))
	mux.HandleFunc("GET /api/runs/{id}/export", d.handleExport)
	mux.HandleFunc("GET /doc", d.handleOpenAPI)
	return d.auth(mux)
}

func (d *Daemon) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if d.apiKey != "" && r.Header.Get("Authorization") != "Bearer "+d.apiKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// role wraps a handler, enforcing the allowed roles from X-Corral-Role.
func (d *Daemon) role(allowed ...Role) func(http.HandlerFunc) http.HandlerFunc {
	set := map[Role]bool{}
	for _, r := range allowed {
		set[r] = true
	}
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			role, ok := parseRole(r.Header.Get("X-Corral-Role"))
			if !ok || !set[role] {
				http.Error(w, fmt.Sprintf("role %q is not allowed to %s", role, r.URL.Path), http.StatusForbidden)
				return
			}
			next(w, r)
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	return dec.Decode(v)
}

func (d *Daemon) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"healthy": true, "dir": d.dir})
}

func (d *Daemon) handlePlan(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Goal string `json:"goal"`
	}
	if err := readJSON(r, &req); err != nil || strings.TrimSpace(req.Goal) == "" {
		http.Error(w, "goal required", http.StatusBadRequest)
		return
	}
	if d.plan == nil {
		http.Error(w, "no planner configured", http.StatusServiceUnavailable)
		return
	}
	ctx := r.Context()
	g, err := d.plan.Plan(ctx, req.Goal)
	if err != nil {
		http.Error(w, "plan failed: "+err.Error(), http.StatusUnprocessableEntity)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"graph": g})
}

func (d *Daemon) handleCreateRun(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Graph            *graph.Graph `json:"graph"`
		AutoApproveGates bool         `json:"autoApproveGates"`
	}
	if err := readJSON(r, &req); err != nil || req.Graph == nil {
		http.Error(w, "graph required", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	runID := "run_" + randID(6)
	h, err := d.sched.CreateWithOptions(ctx, runID, req.Graph, sched.RunOptions{AutoApproveGates: req.AutoApproveGates})
	if err != nil {
		http.Error(w, "invalid graph: "+err.Error(), http.StatusUnprocessableEntity)
		return
	}
	d.mu.Lock()
	d.runs[runID] = h
	d.mu.Unlock()
	go h.Run(d.ctx, 250*time.Millisecond)
	writeJSON(w, http.StatusCreated, map[string]any{"runID": runID})
}

func (d *Daemon) runHandle(id string) (*sched.RunHandle, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	h, ok := d.runs[id]
	if !ok {
		return nil, fmt.Errorf("unknown run %s", id)
	}
	return h, nil
}

type runSummary struct {
	ID     string            `json:"id"`
	Status string            `json:"status"`
	States map[string]string `json:"states"`
	Done   bool              `json:"done"`
}

func (d *Daemon) handleListRuns(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	runs, err := d.st.ListRuns(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var out []runSummary
	for _, ru := range runs {
		sum := runSummary{ID: ru.ID, Status: ru.Status, States: map[string]string{}}
		if h, ok := d.runs[ru.ID]; ok {
			sum.Done = h.Done()
			for _, n := range ru.Graph.Nodes {
				if st, ok := h.State(n.ID); ok {
					sum.States[string(n.ID)] = string(st)
				}
			}
		} else {
			states, err := d.st.NodeStates(ctx, ru.ID)
			if err == nil {
				for id, st := range states {
					sum.States[string(id)] = string(st)
				}
			}
		}
		out = append(out, sum)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	writeJSON(w, http.StatusOK, out)
}

func (d *Daemon) handleGetRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	ru, err := d.st.Run(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	events, err := d.st.Events(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	attempts := map[string][]store.Attempt{}
	for _, n := range ru.Graph.Nodes {
		atts, err := d.st.Attempts(ctx, id, string(n.ID))
		if err != nil {
			continue
		}
		attempts[string(n.ID)] = atts
	}
	resp := map[string]any{
		"runID": id, "status": ru.Status, "graph": ru.Graph,
		"autoApproveGates": ru.AutoApproveGates,
		"events":           events, "attempts": attempts,
	}
	if h, ok := d.runs[id]; ok {
		states := map[string]string{}
		for _, n := range ru.Graph.Nodes {
			if st, ok := h.State(n.ID); ok {
				states[string(n.ID)] = string(st)
			}
		}
		resp["states"] = states
		resp["done"] = h.Done()
	}
	writeJSON(w, http.StatusOK, resp)
}

// watchFrame is one SSE frame of the run watch stream. Every frame is a
// delta: a store event (enriched with gate/awaitingApproval when the node
// is a human gate) or the terminal run-done notification.
type watchFrame struct {
	Seq              int64  `json:"seq"`
	Type             string `json:"type"` // "event" | "done"
	RunID            string `json:"runID"`
	Event            string `json:"event,omitempty"`
	NodeID           string `json:"nodeID,omitempty"`
	From             string `json:"from,omitempty"`
	To               string `json:"to,omitempty"`
	AttemptID        string `json:"attemptID,omitempty"`
	Gate             bool   `json:"gate,omitempty"`
	AwaitingApproval bool   `json:"awaitingApproval,omitempty"`
	Status           string `json:"status,omitempty"`
	Payload          string `json:"payload,omitempty"`
}

// handleWatchRun streams the run's event log as Server-Sent Events, one
// frame per delta after the `after` sequence number (default 0). Frames
// carry node transitions, flag human gates awaiting approval, and emit a
// final "done" frame once the run settles. The stream closes on client
// disconnect or when the run is done and every pending event was sent.
func (d *Daemon) handleWatchRun(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	if _, err := d.st.Run(ctx, id); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	after := int64(0)
	if v := r.URL.Query().Get("after"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			after = n
		}
	}

	// Which nodes are human gates, so transitions to running can be
	// flagged as awaiting operator approval.
	isGate := map[string]bool{}
	if ru, err := d.st.Run(ctx, id); err == nil {
		for _, n := range ru.Graph.Nodes {
			isGate[string(n.ID)] = n.Type == graph.NodeHuman
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	fl.Flush()

	writeSSE := func(v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return err
		}
		fl.Flush()
		return nil
	}

	doneSent := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.ctx.Done():
			return
		default:
		}
		events, err := d.st.Events(ctx, id)
		if err != nil {
			return
		}
		last := after
		for _, ev := range events {
			if ev.Seq <= after {
				continue
			}
			last = ev.Seq
			frame := watchFrame{
				Seq:       ev.Seq,
				Type:      "event",
				RunID:     id,
				Event:     string(ev.Type),
				NodeID:    ev.NodeID,
				From:      string(ev.From),
				To:        string(ev.To),
				AttemptID: ev.AttemptID,
				Payload:   string(ev.Payload),
			}
			if ev.NodeID != "" && isGate[ev.NodeID] {
				frame.Gate = true
				if ev.Type == store.EventTransition && ev.To == graph.StateRunning {
					frame.AwaitingApproval = true
				}
			}
			if err := writeSSE(frame); err != nil {
				return
			}
		}
		after = last

		if d.runDone(ctx, id) && !doneSent {
			doneSent = true
			if err := writeSSE(watchFrame{
				Seq:    after,
				Type:   "done",
				RunID:  id,
				Status: d.runStatus(ctx, id),
			}); err != nil {
				return
			}
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// runDone reports whether the run has settled (completed or waiting for a
// human decision). In-memory handles expose Done(); persisted runs without
// a live handle are done when their stored status is terminal.
func (d *Daemon) runDone(ctx context.Context, id string) bool {
	d.mu.Lock()
	h, live := d.runs[id]
	d.mu.Unlock()
	if live {
		return h.Done()
	}
	ru, err := d.st.Run(ctx, id)
	return err == nil && (ru.Status == "completed" || ru.Status == "waiting")
}

func (d *Daemon) runStatus(ctx context.Context, id string) string {
	ru, err := d.st.Run(ctx, id)
	if err != nil {
		return ""
	}
	return ru.Status
}

func (d *Daemon) nodeAction(w http.ResponseWriter, r *http.Request, fn func(ctx context.Context, id graph.NodeID) error) {
	var req struct {
		NodeID string `json:"nodeID"`
	}
	if err := readJSON(r, &req); err != nil || req.NodeID == "" {
		http.Error(w, "nodeID required", http.StatusBadRequest)
		return
	}
	_, err := d.runHandle(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err := fn(r.Context(), graph.NodeID(req.NodeID)); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (d *Daemon) handleApprove(w http.ResponseWriter, r *http.Request) {
	h, err := d.runHandle(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	d.nodeAction(w, r, func(ctx context.Context, id graph.NodeID) error { return h.ApproveNode(ctx, id) })
}

func (d *Daemon) handleReject(w http.ResponseWriter, r *http.Request) {
	h, err := d.runHandle(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	d.nodeAction(w, r, func(ctx context.Context, id graph.NodeID) error { return h.RejectNode(ctx, id) })
}

func (d *Daemon) handleCancel(w http.ResponseWriter, r *http.Request) {
	h, err := d.runHandle(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	d.nodeAction(w, r, func(ctx context.Context, id graph.NodeID) error { return h.CancelNode(ctx, id) })
}

func (d *Daemon) handleRetry(w http.ResponseWriter, r *http.Request) {
	h, err := d.runHandle(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	wasDone := h.Done()
	d.nodeAction(w, r, func(ctx context.Context, id graph.NodeID) error { return h.RetryNode(ctx, id) })
	if wasDone {
		// The run had settled; re-activate its driver loop.
		go h.Run(d.ctx, 250*time.Millisecond)
	}
}

func (d *Daemon) handleSteer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID  string `json:"nodeID"`
		Message string `json:"message"`
	}
	if err := readJSON(r, &req); err != nil || req.NodeID == "" || req.Message == "" {
		http.Error(w, "nodeID and message required", http.StatusBadRequest)
		return
	}
	h, err := d.runHandle(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err := h.Steer(r.Context(), graph.NodeID(req.NodeID), req.Message); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handlePermission answers a pending permission request of a running
// attempt (allow or deny), resuming the session.
func (d *Daemon) handlePermission(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID       string `json:"nodeID"`
		PermissionID string `json:"permissionID"`
		Allow        bool   `json:"allow"`
	}
	if err := readJSON(r, &req); err != nil || req.NodeID == "" || req.PermissionID == "" {
		http.Error(w, "nodeID and permissionID required", http.StatusBadRequest)
		return
	}
	h, err := d.runHandle(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	rec, err := h.PermissionSession(r.Context(), graph.NodeID(req.NodeID))
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err := rec.RespondPermission(r.Context(), req.PermissionID, req.Allow); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	// A settled run (blocked on the permission) re-activates.
	if h.Done() {
		if err := h.Resume(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		go h.Run(d.ctx, 250*time.Millisecond)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleExport dumps the full audit trail of a run: graph, states, event
// log, attempts (with sessions/worktrees/branches) and content-addressed
// artifacts. Secrets are redacted at the store boundary.
func (d *Daemon) handleExport(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	ru, err := d.st.Run(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	events, err := d.st.Events(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	attempts := map[string][]store.Attempt{}
	artifacts := map[string][]store.Artifact{}
	for _, n := range ru.Graph.Nodes {
		if atts, err := d.st.Attempts(ctx, id, string(n.ID)); err == nil {
			attempts[string(n.ID)] = atts
			for _, at := range atts {
				if arts, err := d.st.Artifacts(ctx, id, at.ID); err == nil {
					artifacts[at.ID] = arts
				}
			}
		}
	}
	states, err := d.st.NodeStates(ctx, id)
	if err != nil {
		states = map[graph.NodeID]graph.State{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"runID": id, "status": ru.Status, "graph": ru.Graph,
		"states": states, "events": events,
		"attempts": attempts, "artifacts": artifacts,
		"exportedAt": time.Now().UTC().Format(time.RFC3339),
	})
}

func randID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
