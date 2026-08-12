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

	mu               sync.Mutex
	runs             map[string]*sched.RunHandle
	broker           *broker
	eventHeartbeat   time.Duration
	eventHeartbeatMu sync.RWMutex
	eventUnsubscribe func()
	eventPumpDone    chan struct{}
	closeOnce        sync.Once
}

// Dir returns the project directory the daemon manages.
func (d *Daemon) Dir() string { return d.dir }

// SetPlanner installs the planner (used by tests and lazy wiring).
func (d *Daemon) SetPlanner(p Planner) { d.plan = p }

func New(st *store.Store, s *sched.Scheduler, plan Planner, dir, apiKey string) *Daemon {
	events, unsubscribe := st.SubscribeEvents()
	d := &Daemon{
		st: st, sched: s, plan: plan, dir: dir, apiKey: apiKey,
		ctx:              context.Background(),
		runs:             map[string]*sched.RunHandle{},
		broker:           newBroker(),
		eventHeartbeat:   defaultEventHeartbeat,
		eventUnsubscribe: unsubscribe,
		eventPumpDone:    make(chan struct{}),
	}
	go d.forwardEvents(events)
	return d
}

func (d *Daemon) forwardEvents(events <-chan store.Event) {
	defer close(d.eventPumpDone)
	for ev := range events {
		d.broker.Publish(ev)
	}
	d.broker.Close()
}

// Close detaches the daemon from the store and closes live event streams.
// Other daemons subscribed to the same store are unaffected.
func (d *Daemon) Close() {
	d.closeOnce.Do(func() {
		d.eventUnsubscribe()
		d.broker.Close()
		<-d.eventPumpDone
	})
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
	mux.HandleFunc("GET /api/runs/{id}/tail", d.handleTail)
	mux.HandleFunc("POST /api/runs/{id}/approve", d.role(RoleOperator, RoleOrchestrator)(d.handleApprove))
	mux.HandleFunc("POST /api/runs/{id}/reject", d.role(RoleOperator, RoleOrchestrator)(d.handleReject))
	mux.HandleFunc("POST /api/runs/{id}/cancel", d.role(RoleOperator, RoleOrchestrator)(d.handleCancel))
	mux.HandleFunc("POST /api/runs/{id}/retry", d.role(RoleOperator, RoleOrchestrator)(d.handleRetry))
	mux.HandleFunc("POST /api/runs/{id}/steer", d.role(RoleOperator, RoleOrchestrator)(d.handleSteer))
	mux.HandleFunc("POST /api/runs/{id}/permission", d.role(RoleOperator, RoleOrchestrator)(d.handlePermission))
	mux.HandleFunc("GET /api/runs/{id}/export", d.handleExport)
	mux.HandleFunc("GET /api/runs/{id}/events", d.handleEvents)
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
	h, err := d.sched.Create(ctx, runID, req.Graph, sched.CreateOptions{AutoApproveGates: req.AutoApproveGates})
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
		"runID":            id,
		"status":           ru.Status,
		"graph":            ru.Graph,
		"autoApproveGates": ru.AutoApproveGates,
		"events":           events,
		"attempts":         attempts,
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

// handleWatchRun long-polls a run for the orchestrator run loop. It
// returns as soon as the run produces new events (milestones, a gate
// awaiting approval, resolution, completion) or after the timeout, and
// always carries the current snapshot: node states, gates awaiting
// approval, whether the run is pre-authorized to auto-approve them, and
// the event cursor to pass back as `since`.
func (d *Daemon) handleWatchRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	q := r.URL.Query()
	since, _ := strconv.ParseInt(q.Get("since"), 10, 64)
	timeout := 60
	if v := q.Get("timeout"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			timeout = n
		}
	}
	if timeout < 1 {
		timeout = 1
	}
	if timeout > 120 {
		timeout = 120
	}
	deadline := time.Now().Add(time.Duration(timeout) * time.Second)

	ctx := r.Context()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		snap, changed, done, err := d.watchSnapshot(ctx, id, since)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		if changed || done || time.Now().After(deadline) {
			writeJSON(w, http.StatusOK, snap)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// watchSnapshot builds the watch response for a run. changed reports
// whether any event happened after since; done whether the run settled.
func (d *Daemon) watchSnapshot(ctx context.Context, id string, since int64) (map[string]any, bool, bool, error) {
	ru, err := d.st.Run(ctx, id)
	if err != nil {
		return nil, false, false, err
	}
	events, err := d.st.Events(ctx, id)
	if err != nil {
		return nil, false, false, err
	}
	maxSeq := since
	var newEvents []store.Event
	for _, e := range events {
		if e.Seq > maxSeq {
			maxSeq = e.Seq
		}
		if e.Seq > since {
			newEvents = append(newEvents, e)
		}
	}

	states := map[string]string{}
	done := false
	if h, ok := d.runs[id]; ok {
		for _, n := range ru.Graph.Nodes {
			if st, ok := h.State(n.ID); ok {
				states[string(n.ID)] = string(st)
			}
		}
		done = h.Done()
	} else {
		if ns, err := d.st.NodeStates(ctx, id); err == nil {
			for nid, st := range ns {
				states[string(nid)] = string(st)
			}
		}
		done = ru.Status != "active"
	}
	// The run status above was read before done was resolved; the store is
	// finalized before the handle reports done, so re-read it so the
	// snapshot never carries done=true with a stale status.
	if done && ru.Status == "active" {
		if ru2, err := d.st.Run(ctx, id); err == nil {
			ru.Status = ru2.Status
		}
	}

	var gates []string
	for _, n := range ru.Graph.Nodes {
		if n.Type == graph.NodeHuman && states[string(n.ID)] == string(graph.StateRunning) {
			gates = append(gates, string(n.ID))
		}
	}

	return map[string]any{
		"runID":                 id,
		"status":                ru.Status,
		"done":                  done,
		"autoApproveGates":      ru.AutoApproveGates,
		"states":                states,
		"gatesAwaitingApproval": gates,
		"since":                 maxSeq,
		"events":                newEvents,
	}, len(newEvents) > 0, done, nil
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

// handleTail returns the live transcript tail of a node's in-flight
// attempt (query params: node, lines). Used by the TUI's inspect view.
func (d *Daemon) handleTail(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	node := q.Get("node")
	if node == "" || len(node) > 256 {
		http.Error(w, "node required", http.StatusBadRequest)
		return
	}
	lines := 40
	if v := q.Get("lines"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			http.Error(w, "lines must be between 1 and 500", http.StatusBadRequest)
			return
		}
		lines = n
	}
	h, err := d.runHandle(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	tail, err := h.Tail(r.Context(), graph.NodeID(node), lines)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": node, "lines": tail})
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
