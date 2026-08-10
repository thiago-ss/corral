// Package sched implements the durable run scheduler: a single control
// loop that leases ready nodes, runs attempts through a Driver, verifies
// completions, applies retry policies and budgets, and records everything
// in the SQLite event log. The loop is cooperative: every state change
// happens inside Step, which makes simulations deterministic.
package sched

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"corral/internal/adapter"
	"corral/internal/clock"
	"corral/internal/graph"
	"corral/internal/store"
	"corral/internal/verify"
	"corral/internal/worktree"
)

const (
	resultsBuffer    = 32
	runStatusDone    = "completed"
	runStatusWaiting = "waiting"
)

type Options struct {
	Concurrency        int
	LeaseTTL           time.Duration
	AgingBoostPerTick  int                  // effective priority gain per saturated step
	AgingCap           int                  // max boost applied to a waiting node
	CheckRunner        verify.CommandRunner // for check/merge nodes; defaults to ExecRunner
	Worktrees          *worktree.Manager    // enables writable-work isolation
	RunMaxTokens       int                  // run-level budget; exceeded -> block new work
	RunMaxCost         float64              // run-level budget; exceeded -> block new work
	BreakerMaxFailures int                  // circuit breaker: block new work after N node failures
	BreakerWindow      time.Duration        // failures counted within this window
	BudgetAbortGrace   time.Duration
}

type Result struct {
	AttemptID string
	SessionID string
	Status    adapter.Status
	Messages  []adapter.Message
	Err       error
	Budget    bool // aborted because the node budget expired
}

type Verdict struct {
	Pass     bool
	Feedback string
	Evidence string
}

type Verifier interface {
	// Verdict runs the evidence gate for an attempt. cwd is the directory
	// the attempt ran in (its worktree), where command gates execute.
	Verdict(ctx context.Context, n *graph.Node, attemptNo int, cwd string, msgs []adapter.Message) (Verdict, error)
}

// EngineVerifier adapts a verify.Engine (command / json_schema / reviewer
// gates) to the scheduler Verifier contract.
type EngineVerifier struct {
	Eng *verify.Engine
}

func (v *EngineVerifier) Verdict(ctx context.Context, n *graph.Node, attemptNo int, cwd string, msgs []adapter.Message) (Verdict, error) {
	if cwd == "" {
		cwd = v.Eng.Root
	}
	res, err := v.Eng.Verify(ctx, n, cwd, attemptNo, msgs)
	if err != nil {
		return Verdict{}, err
	}
	return Verdict{Pass: res.Pass, Feedback: res.Feedback, Evidence: res.Evidence}, nil
}

type Scheduler struct {
	store *store.Store
	drv   adapter.Driver
	ver   Verifier
	clk   clock.Clock
	opts  Options
}

func New(st *store.Store, drv adapter.Driver, ver Verifier, clk clock.Clock, opts Options) *Scheduler {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = 30 * time.Second
	}
	return &Scheduler{store: st, drv: drv, ver: ver, clk: clk, opts: opts}
}

type sessionRec struct {
	nodeID        graph.NodeID
	attemptID     string
	no            int
	sess          adapter.Session
	deadline      time.Time
	budgeted      bool // time budget active
	abortIsBudget bool // abort was initiated by the scheduler (budget), not operator
	worktree      string
	branch        string
	merged        []string // branches merged by this attempt (merge nodes)
}

type RunHandle struct {
	s     *Scheduler
	runID string
	g     *graph.Graph
	tr    *graph.Tracker
	mu    sync.Mutex

	sessions  map[graph.NodeID]*sessionRec
	suspended map[graph.NodeID]*sessionRec // permission-blocked sessions
	retryAt   map[graph.NodeID]time.Time
	age       map[graph.NodeID]int
	feedback  map[graph.NodeID]string
	failures  []time.Time
	runCost   float64
	runTokens int
	breaker   bool
	results   chan Result
	holder    string
	done      bool
	stepCount int64
	started   time.Time
}

// Create starts a new run for g.
func (s *Scheduler) Create(ctx context.Context, runID string, g *graph.Graph) (*RunHandle, error) {
	if err := graph.Validate(g); err != nil {
		return nil, err
	}
	now := s.clk.Now()
	if err := s.store.CreateRun(ctx, runID, g, now); err != nil {
		return nil, err
	}
	return s.newHandle(ctx, runID, g, now)
}

// Load resumes a persisted run: events are replayed into a fresh tracker
// (validating the log), interrupted attempts are marked, and non-terminal
// active nodes are restored to ready so they re-execute exactly once more.
func (s *Scheduler) Load(ctx context.Context, runID string) (*RunHandle, error) {
	r, err := s.store.Run(ctx, runID)
	if err != nil {
		return nil, err
	}
	tr, err := graph.NewTracker(r.Graph)
	if err != nil {
		return nil, err
	}
	events, err := s.store.Events(ctx, runID)
	if err != nil {
		return nil, err
	}
	now := s.clk.Now()
	for _, ev := range events {
		if ev.NodeID == "" {
			continue
		}
		switch ev.Type {
		case store.EventTransition:
			if err := tr.Transit(graph.NodeID(ev.NodeID), ev.From, ev.To); err != nil {
				return nil, fmt.Errorf("replay seq %d: %w", ev.Seq, err)
			}
		case store.EventRecovery:
			if err := tr.Set(graph.NodeID(ev.NodeID), ev.To); err != nil {
				return nil, fmt.Errorf("recovery seq %d: %w", ev.Seq, err)
			}
		}
	}
	// Restore interrupted nodes to ready (exactly one re-execution each).
	var recoveries []graph.NodeID
	for _, n := range r.Graph.Nodes {
		st, _ := tr.State(n.ID)
		switch st {
		case graph.StateLeased, graph.StateRunning, graph.StateVerifying:
			recoveries = append(recoveries, n.ID)
		case graph.StateRetryWait:
			if rw, ok := retryReadyAt(events, n.ID); ok && !rw.After(now) {
				recoveries = append(recoveries, n.ID)
			}
		}
	}
	if len(recoveries) > 0 {
		for _, id := range recoveries {
			from, _ := tr.State(id)
			if err := tr.Set(id, graph.StateReady); err != nil {
				return nil, fmt.Errorf("recover %s: %w", id, err)
			}
			if _, err := s.store.AppendEvent(ctx, runID, string(id), store.EventRecovery, from, graph.StateReady, "", "", now); err != nil {
				return nil, err
			}
			if err := s.store.SetNodeState(ctx, runID, string(id), graph.StateReady, now); err != nil {
				return nil, err
			}
			if err := s.store.ClearLease(ctx, runID, string(id), now); err != nil {
				return nil, err
			}
			if err := s.store.MarkInterrupted(ctx, runID, string(id)); err != nil {
				return nil, err
			}
		}
	}
	h := &RunHandle{
		s:         s,
		runID:     runID,
		g:         r.Graph,
		tr:        tr,
		sessions:  map[graph.NodeID]*sessionRec{},
		suspended: map[graph.NodeID]*sessionRec{},
		retryAt:   map[graph.NodeID]time.Time{},
		age:       map[graph.NodeID]int{},
		feedback:  map[graph.NodeID]string{},
		results:   make(chan Result, resultsBuffer),
		holder:    fmt.Sprintf("corral-%d", now.UnixNano()),
		started:   now,
	}
	for _, ev := range events {
		if ev.Type == store.EventRetry {
			var p struct {
				ReadyAt int64 `json:"readyAt"`
			}
			if json.Unmarshal(ev.Payload, &p) == nil && p.ReadyAt > 0 {
				h.retryAt[graph.NodeID(ev.NodeID)] = time.UnixMilli(p.ReadyAt)
			}
		}
	}
	return h, nil
}

// retryReadyAt finds the scheduled retry time from retry events.
func retryReadyAt(events []store.Event, nodeID graph.NodeID) (time.Time, bool) {
	for _, ev := range events {
		if graph.NodeID(ev.NodeID) != nodeID || ev.Type != store.EventRetry {
			continue
		}
		var p struct {
			ReadyAt int64 `json:"readyAt"`
		}
		if json.Unmarshal(ev.Payload, &p) == nil && p.ReadyAt > 0 {
			return time.UnixMilli(p.ReadyAt), true
		}
	}
	return time.Time{}, false
}

func (s *Scheduler) newHandle(ctx context.Context, runID string, g *graph.Graph, now time.Time) (*RunHandle, error) {
	tr, err := graph.NewTracker(g)
	if err != nil {
		return nil, err
	}
	return &RunHandle{
		s:         s,
		runID:     runID,
		g:         g,
		tr:        tr,
		sessions:  map[graph.NodeID]*sessionRec{},
		suspended: map[graph.NodeID]*sessionRec{},
		retryAt:   map[graph.NodeID]time.Time{},
		age:       map[graph.NodeID]int{},
		feedback:  map[graph.NodeID]string{},
		results:   make(chan Result, resultsBuffer),
		holder:    fmt.Sprintf("corral-%d", now.UnixNano()),
		started:   now,
	}, nil
}

// Step advances the run by one deterministic step.
func (h *RunHandle) Step(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.done {
		return nil
	}
	now := h.s.clk.Now()

	// 1. Retry timers.
	for id, at := range h.retryAt {
		if !at.After(now) {
			if err := h.transit(ctx, id, graph.StateRetryWait, graph.StateReady, ""); err != nil {
				return err
			}
			delete(h.retryAt, id)
		}
	}

	// 2. Budget deadlines: abort attempts that exceeded their time budget.
	for _, rec := range h.sessions {
		if rec.budgeted && !rec.deadline.IsZero() && now.After(rec.deadline) {
			_ = rec.sess.Abort(ctx)
			rec.budgeted = false // abort already requested; completion arrives via results
			rec.abortIsBudget = true
		}
	}

	// 2b. Permission requests become an explicit blocked state: the
	// session is suspended (its completion still resolves the attempt)
	// and the node blocks until the permission is answered.
	for id, rec := range h.sessions {
		ps, ok := rec.sess.(adapter.PermissionSession)
		if !ok {
			continue
		}
		pid, pending, err := ps.PendingPermission(ctx)
		if err != nil || !pending {
			continue
		}
		delete(h.sessions, id)
		h.suspended[id] = rec
		rec.budgeted = false
		payload, _ := json.Marshal(map[string]any{"reason": "permission", "permissionID": pid})
		if err := h.transit(ctx, id, graph.StateRunning, graph.StateBlocked, string(payload)); err != nil {
			return err
		}
	}

	// 2c. Resume suspended sessions whose permission was answered. A run
	// that settled (waiting) while blocked re-activates.
	for id, rec := range h.suspended {
		ps, ok := rec.sess.(adapter.PermissionSession)
		if !ok {
			continue
		}
		if _, pending, err := ps.PendingPermission(ctx); err != nil || pending {
			continue
		}
		delete(h.suspended, id)
		h.sessions[id] = rec
		if n := h.nodeByID(id); n != nil && n.Budget.MaxDuration > 0 {
			// Re-arm the time budget with the remaining time.
			rec.deadline = now.Add(time.Until(rec.deadline).Round(0))
			rec.budgeted = true
		}
		if h.done {
			h.done = false
			_ = h.s.store.CompleteRun(ctx, h.runID, "active", now)
		}
		if err := h.chainToRunning(ctx, id); err != nil {
			return err
		}
	}

	// 3. Cooperative driver completions.
	if stepper, ok := h.s.drv.(adapter.Stepper); ok {
		for _, c := range stepper.Step(ctx, now) {
			h.results <- Result{
				AttemptID: c.AttemptID,
				SessionID: c.SessionID,
				Status:    c.Status,
				Messages:  c.Messages,
				Err:       c.Err,
				Budget:    c.Budget,
			}
		}
	}

	// 4. Drain and handle results (completions from any source).
	handled := 0
	for {
		select {
		case res := <-h.results:
			handled++
			if err := h.finishAttempt(ctx, res); err != nil {
				return err
			}
		default:
			goto drained
		}
	}
drained:

	// 5. Block permanently-unrunnable nodes.

	// 2d. Circuit breaker: after too many node failures in the window,
	// stop starting new work; pending nodes become blocked.
	if h.s.opts.BreakerMaxFailures > 0 {
		cutoff := now.Add(-h.s.opts.BreakerWindow)
		kept := h.failures[:0]
		for _, t := range h.failures {
			if !t.Before(cutoff) {
				kept = append(kept, t)
			}
		}
		h.failures = kept
		if len(h.failures) >= h.s.opts.BreakerMaxFailures {
			h.breaker = true
		}
	}
	if h.breaker {
		for _, n := range h.g.Nodes {
			st, _ := h.tr.State(n.ID)
			if st == graph.StatePending {
				if err := h.transit(ctx, n.ID, graph.StatePending, graph.StateBlocked, `{"reason":"circuit breaker"}`); err != nil {
					return err
				}
			}
		}
	}

	// 2e. Run-level budget: once cost/tokens are exhausted, no new work.
	if h.s.opts.RunMaxTokens > 0 && h.runTokens >= h.s.opts.RunMaxTokens ||
		h.s.opts.RunMaxCost > 0 && h.runCost >= h.s.opts.RunMaxCost {
		for _, n := range h.g.Nodes {
			st, _ := h.tr.State(n.ID)
			if st == graph.StatePending {
				if err := h.transit(ctx, n.ID, graph.StatePending, graph.StateBlocked, `{"reason":"run budget exceeded"}`); err != nil {
					return err
				}
			}
		}
	}

	ready, blocked := graph.ComputeReady(h.g, h.tr)
	for _, n := range blocked {
		if err := h.transit(ctx, n.ID, graph.StatePending, graph.StateBlocked, ""); err != nil {
			return err
		}
	}

	// 6. Start attempts up to the concurrency limit, priority + aging order.
	active := len(h.sessions)
	candidates := ready
	sort.SliceStable(candidates, func(i, j int) bool {
		ei := int(candidates[i].Priority) + min(h.age[candidates[i].ID], h.s.opts.AgingCap)*h.s.opts.AgingBoostPerTick
		ej := int(candidates[j].Priority) + min(h.age[candidates[j].ID], h.s.opts.AgingCap)*h.s.opts.AgingBoostPerTick
		if ei != ej {
			return ei > ej
		}
		return candidates[i].ID < candidates[j].ID
	})
	started := 0
	for _, n := range candidates {
		if active+started >= h.s.opts.Concurrency {
			break
		}
		if _, suspended := h.suspended[n.ID]; suspended {
			continue // session alive; awaiting permission resolution
		}
		if h.scopeCollides(n) {
			continue // keep ready; re-selected once the conflicting writer finishes
		}
		if err := h.startAttempt(ctx, n); err != nil {
			return err
		}
		started++
	}

	// 7. Aging: candidates that stayed pending while saturated gain boost.
	if len(h.sessions) >= h.s.opts.Concurrency {
		for _, n := range candidates {
			if _, active := h.sessions[n.ID]; !active {
				h.age[n.ID]++
			}
		}
	}

	h.stepCount++

	// 8. Settle check: the run is finished when every node is terminal
	// (completed) or blocked (waiting for human intervention).
	if settled, waiting := h.allSettled(); settled {
		status := runStatusDone
		if waiting {
			status = runStatusWaiting
		}
		if err := h.s.store.CompleteRun(ctx, h.runID, status, h.s.clk.Now()); err != nil {
			return err
		}
		h.done = true
	}
	return nil
}

// transit applies a legal transition and records it durably.
func (h *RunHandle) transit(ctx context.Context, id graph.NodeID, from, to graph.State, payload string) error {
	if err := h.tr.Transit(id, from, to); err != nil {
		return err
	}
	if _, err := h.s.store.AppendTransition(ctx, h.runID, string(id), from, to, payload, h.s.clk.Now()); err != nil {
		return err
	}
	return nil
}

func (h *RunHandle) startAttempt(ctx context.Context, n *graph.Node) error {
	now := h.s.clk.Now()
	cur, _ := h.tr.State(n.ID)
	if cur == graph.StatePending {
		if err := h.transit(ctx, n.ID, graph.StatePending, graph.StateReady, ""); err != nil {
			return err
		}
	}
	ok, err := h.s.store.AcquireLease(ctx, h.runID, string(n.ID), h.holder, h.s.opts.LeaseTTL, now)
	if err != nil {
		return err
	}
	if !ok {
		return nil // leased elsewhere; re-selected next step
	}
	if err := h.transit(ctx, n.ID, graph.StateReady, graph.StateLeased, ""); err != nil {
		return err
	}
	no, err := h.s.store.CountAttempts(ctx, h.runID, string(n.ID))
	if err != nil {
		return err
	}
	no++
	attemptID := fmt.Sprintf("%s/%d", n.ID, no)
	switch n.Type {
	case graph.NodeCheck:
		return h.startCheck(ctx, n, attemptID, no, now)
	case graph.NodeMerge:
		return h.startMerge(ctx, n, attemptID, no, now)
	case graph.NodeHuman:
		return h.startGate(ctx, n, attemptID, no, now)
	}
	a := adapter.Attempt{
		ID:                 attemptID,
		NodeID:             string(n.ID),
		Objective:          n.Objective,
		Role:               n.Role,
		Model:              n.Model,
		Cwd:                h.cwd(n),
		WriteScope:         n.WriteScope,
		Feedback:           h.feedback[n.ID],
		MaxDurationSeconds: int(n.Budget.MaxDuration.Seconds()),
	}
	// Writable-work isolation: writing agents get their own worktree and
	// branch; the attempt runs there, leaving the main checkout untouched.
	var branch, wt string
	if wtm := h.s.opts.Worktrees; wtm != nil && worktree.NodeIsWriting(string(n.Type), n.Role) {
		branch = fmt.Sprintf("corral/%s/%s/%d", h.runID, n.ID, no)
		wt, err = wtm.Add(ctx, branch)
		if err != nil {
			_ = h.s.store.ReleaseLease(ctx, h.runID, string(n.ID), h.holder, now)
			if err2 := h.transit(ctx, n.ID, graph.StateLeased, graph.StateFailed, `{"reason":"worktree create failed"}`); err2 != nil {
				return err2
			}
			return nil
		}
		a.Cwd = wt
	}
	delete(h.feedback, n.ID)
	sess, err := h.s.drv.Start(ctx, a)
	if err != nil {
		// Could not start: clean up, release and fail the node.
		if wt != "" {
			_ = h.s.opts.Worktrees.Remove(ctx, branch)
		}
		_ = h.s.store.ReleaseLease(ctx, h.runID, string(n.ID), h.holder, now)
		if err2 := h.transit(ctx, n.ID, graph.StateLeased, graph.StateFailed, `{"reason":"start failed"}`); err2 != nil {
			return err2
		}
		return nil
	}
	started := now.UnixMilli()
	h.sessions[n.ID] = &sessionRec{
		nodeID:    n.ID,
		attemptID: attemptID,
		no:        no,
		sess:      sess,
		deadline:  now.Add(n.Budget.MaxDuration),
		budgeted:  n.Budget.MaxDuration > 0,
		worktree:  wt,
		branch:    branch,
	}
	if err := h.transit(ctx, n.ID, graph.StateLeased, graph.StateRunning, ""); err != nil {
		return err
	}
	if err := h.s.store.RecordAttempt(ctx, store.Attempt{
		ID: attemptID, RunID: h.runID, NodeID: string(n.ID), No: no,
		Status: "running", ServerID: sess.ServerID(), SessionID: sess.ID(),
		Worktree: wt, Branch: branch, StartedAt: &started,
	}); err != nil {
		return err
	}
	return h.emitEvent(ctx, store.EventAttempt, n.ID, graph.State(""), graph.State(""), attemptID,
		`{"phase":"start","sessionID":"`+sess.ID()+`"}`)
}

// scopeCollides reports whether starting n would overlap the write scope
// of an active writing session (declared collisions block scheduling).
// Only enforced when writable-work isolation is enabled.
func (h *RunHandle) scopeCollides(n *graph.Node) bool {
	if h.s.opts.Worktrees == nil || !worktree.NodeIsWriting(string(n.Type), n.Role) {
		return false
	}
	for _, rec := range h.sessions {
		other := h.nodeByID(rec.nodeID)
		if other == nil || !worktree.NodeIsWriting(string(other.Type), other.Role) {
			continue
		}
		if worktree.ScopesOverlap(n.WriteScope, other.WriteScope) {
			return true
		}
	}
	return false
}

// startCheck runs a check node's command inline (no agent session), then
// completes the attempt through the normal evidence gate. The command
// result is carried in message Meta and judged by verify.verifyCheck.
func (h *RunHandle) startCheck(ctx context.Context, n *graph.Node, attemptID string, no int, now time.Time) error {
	if n.Verification == nil || len(n.Verification.Command) == 0 {
		return fmt.Errorf("check node %s requires a command verification", n.ID)
	}
	timeout := verify.DefaultCommandTimeout
	if n.Budget.MaxDuration > 0 {
		timeout = n.Budget.MaxDuration
	}
	runner := h.s.opts.CheckRunner
	if runner == nil {
		runner = verify.ExecRunner{}
	}
	exit, stdout, stderr, err := runner.Run(ctx, h.checkCwd(ctx, n), n.Verification.Command, timeout)
	if err != nil && exit == 0 {
		// The command never ran (e.g. a missing binary). Fail the check
		// gate with the real error as feedback instead of erroring the
		// whole run.
		stderr = "command failed to run: " + err.Error()
		exit = 1
	}
	sess := &checkSession{id: "check:" + string(n.ID)}
	started := now.UnixMilli()
	h.sessions[n.ID] = &sessionRec{
		nodeID:    n.ID,
		attemptID: attemptID,
		no:        no,
		sess:      sess,
		deadline:  now.Add(n.Budget.MaxDuration),
		budgeted:  false, // the command already ran within its timeout
	}
	if err := h.transit(ctx, n.ID, graph.StateLeased, graph.StateRunning, ""); err != nil {
		return err
	}
	if err := h.s.store.RecordAttempt(ctx, store.Attempt{
		ID: attemptID, RunID: h.runID, NodeID: string(n.ID), No: no,
		Status: "running", ServerID: "local", SessionID: sess.ID(), StartedAt: &started,
	}); err != nil {
		return err
	}
	msg := adapter.Message{
		Role:   "assistant",
		Finish: "stop",
		Meta: map[string]string{
			"exit":   fmt.Sprint(exit),
			"stdout": stdout,
			"stderr": stderr,
		},
	}
	h.results <- Result{AttemptID: attemptID, SessionID: sess.ID(), Status: adapter.StatusIdle, Messages: []adapter.Message{msg}}
	return h.emitEvent(ctx, store.EventAttempt, n.ID, graph.State(""), graph.State(""), attemptID,
		`{"phase":"start","sessionID":"`+sess.ID()+`"}`)
}

// checkSession is a placeholder session for inline check execution.
type checkSession struct{ id string }

func (c *checkSession) ID() string                         { return c.id }
func (c *checkSession) ServerID() string                   { return "local" }
func (c *checkSession) Send(context.Context, string) error { return nil }
func (c *checkSession) Abort(context.Context) error        { return nil }
func (c *checkSession) Status(context.Context) (adapter.Status, error) {
	return adapter.StatusIdle, nil
}
func (c *checkSession) Messages(context.Context) ([]adapter.Message, error) { return nil, nil }
func (c *checkSession) Close(context.Context) error                         { return nil }

func (h *RunHandle) cwd(n *graph.Node) string { return n.Meta["cwd"] }

// checkCwd picks where a check node runs: the worktree of its writing
// dependency when there is exactly one completed writing dep, otherwise
// the main checkout.
func (h *RunHandle) checkCwd(ctx context.Context, n *graph.Node) string {
	var found string
	if h.s.opts.Worktrees == nil {
		return h.cwd(n)
	}
	for _, dep := range n.DependsOn {
		dn := h.nodeByID(dep)
		if dn == nil || !worktree.NodeIsWriting(string(dn.Type), dn.Role) {
			continue
		}
		atts, err := h.s.store.Attempts(ctx, h.runID, string(dep))
		if err != nil {
			continue
		}
		for i := len(atts) - 1; i >= 0; i-- {
			if atts[i].Status == "done" && atts[i].Worktree != "" {
				found = atts[i].Worktree
				break
			}
		}
	}
	if found != "" {
		return found
	}
	return h.cwd(n)
}

// startMerge folds the branches of completed writing dependencies into
// the main checkout, then completes through the merge node's command
// verification (which runs in the main checkout).
func (h *RunHandle) startMerge(ctx context.Context, n *graph.Node, attemptID string, no int, now time.Time) error {
	if n.Verification == nil || len(n.Verification.Command) == 0 {
		return fmt.Errorf("merge node %s requires a command verification", n.ID)
	}
	wtm := h.s.opts.Worktrees
	if wtm == nil {
		return fmt.Errorf("merge node %s requires a worktree manager", n.ID)
	}
	type branchRef struct {
		branch   string
		worktree string
	}
	var branches []branchRef
	seen := map[string]bool{}
	var collect func(id graph.NodeID)
	collect = func(id graph.NodeID) {
		dn := h.nodeByID(id)
		if dn == nil || seen[string(id)] {
			return
		}
		seen[string(id)] = true
		for _, dep := range dn.DependsOn {
			collect(dep)
		}
		if !worktree.NodeIsWriting(string(dn.Type), dn.Role) {
			return
		}
		atts, err := h.s.store.Attempts(ctx, h.runID, string(id))
		if err != nil {
			return
		}
		for i := len(atts) - 1; i >= 0; i-- {
			if atts[i].Status == "done" && atts[i].Branch != "" {
				branches = append(branches, branchRef{branch: atts[i].Branch, worktree: atts[i].Worktree})
				return
			}
		}
	}
	for _, dep := range n.DependsOn {
		collect(dep)
	}

	// Commit each worktree and merge into main; stop at the first failure.
	var merged []string
	mergeErr := error(nil)
	for _, ref := range branches {
		if ref.worktree == "" {
			mergeErr = fmt.Errorf("no worktree recorded for branch %s", ref.branch)
			break
		}
		if err := wtm.CommitWorktree(ctx, ref.worktree); err != nil {
			mergeErr = err
			break
		}
		if err := wtm.MergeBranch(ctx, ref.branch); err != nil {
			mergeErr = err
			break
		}
		merged = append(merged, ref.branch)
	}
	if mergeErr != nil {
		// A failed merge is still surfaced through the gate as exit 1.
		stdout, stderr := "", mergeErr.Error()
		return h.completeInline(ctx, n, attemptID, no, now, 1, stdout, stderr, merged)
	}
	// The verification command runs in the main checkout after merging.
	timeout := verify.DefaultCommandTimeout
	if n.Budget.MaxDuration > 0 {
		timeout = n.Budget.MaxDuration
	}
	runner := h.s.opts.CheckRunner
	if runner == nil {
		runner = verify.ExecRunner{}
	}
	exit, stdout, stderr, err := runner.Run(ctx, wtm.Repo(), n.Verification.Command, timeout)
	if err != nil && exit == 0 {
		// The verification command never ran (e.g. a missing binary).
		// Surface it through the merge gate instead of erroring the run.
		stderr = "verification failed to run: " + err.Error()
		exit = 1
	}
	return h.completeInline(ctx, n, attemptID, no, now, exit, stdout, stderr, merged)
}

// startGate parks a human gate node in running until an operator approves
// or rejects it. No driver session is involved.
func (h *RunHandle) startGate(ctx context.Context, n *graph.Node, attemptID string, no int, now time.Time) error {
	sess := &gateSession{id: "gate:" + string(n.ID)}
	started := now.UnixMilli()
	h.sessions[n.ID] = &sessionRec{
		nodeID:    n.ID,
		attemptID: attemptID,
		no:        no,
		sess:      sess,
	}
	if err := h.transit(ctx, n.ID, graph.StateLeased, graph.StateRunning, ""); err != nil {
		return err
	}
	if err := h.s.store.RecordAttempt(ctx, store.Attempt{
		ID: attemptID, RunID: h.runID, NodeID: string(n.ID), No: no,
		Status: "running", ServerID: "local", SessionID: sess.ID(), StartedAt: &started,
	}); err != nil {
		return err
	}
	return h.emitEvent(ctx, store.EventAttempt, n.ID, graph.State(""), graph.State(""), attemptID,
		`{"phase":"start","sessionID":"`+sess.ID()+`"}`)
}

// completeInline registers the session, transitions to running, records
// the attempt and pushes a completion whose Meta carries the command
// result; the evidence gate judges it via verify.verifyCheck.
func (h *RunHandle) completeInline(ctx context.Context, n *graph.Node, attemptID string, no int, now time.Time, exit int, stdout, stderr string, merged []string) error {
	sess := &checkSession{id: "check:" + string(n.ID)}
	started := now.UnixMilli()
	h.sessions[n.ID] = &sessionRec{
		nodeID:    n.ID,
		attemptID: attemptID,
		no:        no,
		sess:      sess,
		merged:    merged,
	}
	if err := h.transit(ctx, n.ID, graph.StateLeased, graph.StateRunning, ""); err != nil {
		return err
	}
	if err := h.s.store.RecordAttempt(ctx, store.Attempt{
		ID: attemptID, RunID: h.runID, NodeID: string(n.ID), No: no,
		Status: "running", ServerID: "local", SessionID: sess.ID(), StartedAt: &started,
	}); err != nil {
		return err
	}
	msg := adapter.Message{
		Role:   "assistant",
		Finish: "stop",
		Meta: map[string]string{
			"exit":   fmt.Sprint(exit),
			"stdout": stdout,
			"stderr": stderr,
		},
	}
	h.results <- Result{AttemptID: attemptID, SessionID: sess.ID(), Status: adapter.StatusIdle, Messages: []adapter.Message{msg}}
	return h.emitEvent(ctx, store.EventAttempt, n.ID, graph.State(""), graph.State(""), attemptID,
		`{"phase":"start","sessionID":"`+sess.ID()+`"}`)
}

// gateSession parks a human gate until approval.
type gateSession struct{ id string }

func (g *gateSession) ID() string                         { return g.id }
func (g *gateSession) ServerID() string                   { return "local" }
func (g *gateSession) Send(context.Context, string) error { return nil }
func (g *gateSession) Abort(context.Context) error        { return nil }
func (g *gateSession) Status(context.Context) (adapter.Status, error) {
	return adapter.StatusRunning, nil
}
func (g *gateSession) Messages(context.Context) ([]adapter.Message, error) { return nil, nil }
func (g *gateSession) Close(context.Context) error                         { return nil }

// ApproveNode approves a running human gate: it completes and its
// dependents (e.g. the merge node) may proceed.
func (h *RunHandle) ApproveNode(ctx context.Context, id graph.NodeID) error {
	return h.decideGate(ctx, id, true)
}

// RejectNode rejects a running human gate: the node becomes blocked.
func (h *RunHandle) RejectNode(ctx context.Context, id graph.NodeID) error {
	return h.decideGate(ctx, id, false)
}

// RetryNode reschedules a node by operator request:
//   - blocked → ready (human resolved)
//   - retry_wait → ready (skip the backoff)
//   - failed → ready (operator override; retry budget reset)
func (h *RunHandle) RetryNode(ctx context.Context, id graph.NodeID) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	st, ok := h.tr.State(id)
	if !ok {
		return fmt.Errorf("unknown node %s", id)
	}
	now := h.s.clk.Now()
	h.breaker = false
	switch st {
	case graph.StateBlocked:
		if err := h.transit(ctx, id, graph.StateBlocked, graph.StateReady, `{"reason":"operator retry"}`); err != nil {
			return err
		}
	case graph.StateRetryWait:
		delete(h.retryAt, id)
		if err := h.transit(ctx, id, graph.StateRetryWait, graph.StateReady, `{"reason":"operator retry"}`); err != nil {
			return err
		}
	case graph.StateFailed:
		n := h.nodeByID(id)
		if n == nil {
			return fmt.Errorf("unknown node %s", id)
		}
		if err := h.tr.Set(id, graph.StateReady); err != nil {
			return err
		}
		if _, err := h.s.store.AppendEvent(ctx, h.runID, string(id), store.EventRecovery, graph.StateFailed, graph.StateReady, "", `{"reason":"operator retry"}`, now); err != nil {
			return err
		}
		if err := h.s.store.SetNodeState(ctx, h.runID, string(id), graph.StateReady, now); err != nil {
			return err
		}
		if err := h.s.store.ClearLease(ctx, h.runID, string(id), now); err != nil {
			return err
		}
		if err := h.s.store.SetRetriesLeft(ctx, h.runID, string(id), n.RetryPolicy.MaxRetries, now); err != nil {
			return err
		}
	default:
		return fmt.Errorf("node %s is in state %s and cannot be retried", id, st)
	}
	// A settled run must be re-activated.
	if h.done {
		h.done = false
		return h.s.store.CompleteRun(ctx, h.runID, "active", now)
	}
	return nil
}

// PermissionSession returns the permission-capable session of a node's
// in-flight attempt (running or permission-suspended).
func (h *RunHandle) PermissionSession(_ context.Context, id graph.NodeID) (adapter.PermissionSession, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rec := h.sessions[id]
	if rec == nil {
		rec = h.suspended[id]
	}
	if rec == nil {
		return nil, fmt.Errorf("node %s has no in-flight attempt", id)
	}
	ps, ok := rec.sess.(adapter.PermissionSession)
	if !ok {
		return nil, fmt.Errorf("node %s session does not support permission requests", id)
	}
	return ps, nil
}

// Steer sends a message to the in-flight attempt of a node (agent steer).
func (h *RunHandle) Steer(ctx context.Context, id graph.NodeID, message string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	rec, ok := h.sessions[id]
	if !ok {
		return fmt.Errorf("node %s has no in-flight attempt", id)
	}
	return rec.sess.Send(ctx, message)
}

func (h *RunHandle) decideGate(ctx context.Context, id graph.NodeID, approve bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	rec, ok := h.sessions[id]
	if !ok {
		return fmt.Errorf("gate %s is not running", id)
	}
	n := h.nodeByID(id)
	if n == nil || n.Type != graph.NodeHuman {
		return fmt.Errorf("node %s is not a human gate", id)
	}
	now := h.s.clk.Now()
	delete(h.sessions, id)
	finished := now.UnixMilli()
	if err := h.transit(ctx, id, graph.StateRunning, graph.StateVerifying, ""); err != nil {
		return err
	}
	if approve {
		if err := h.transit(ctx, id, graph.StateVerifying, graph.StateDone, ""); err != nil {
			return err
		}
		return h.s.store.RecordAttempt(ctx, store.Attempt{
			ID: rec.attemptID, RunID: h.runID, NodeID: string(id), No: rec.no,
			Status: "done", SessionID: rec.sess.ID(), FinishedAt: &finished,
		})
	}
	if err := h.transit(ctx, id, graph.StateVerifying, graph.StateBlocked, ""); err != nil {
		return err
	}
	return h.s.store.RecordAttempt(ctx, store.Attempt{
		ID: rec.attemptID, RunID: h.runID, NodeID: string(id), No: rec.no,
		Status: "aborted", SessionID: rec.sess.ID(), FinishedAt: &finished,
	})
}

func (h *RunHandle) finishAttempt(ctx context.Context, res Result) error {
	rec := h.findSession(res.AttemptID)
	if rec == nil {
		return fmt.Errorf("result for unknown attempt %s", res.AttemptID)
	}
	res.Budget = res.Budget || rec.abortIsBudget
	now := h.s.clk.Now()
	delete(h.sessions, rec.nodeID)
	delete(h.suspended, rec.nodeID)
	finished := now.UnixMilli()
	_ = h.s.store.ReleaseLease(ctx, h.runID, string(rec.nodeID), h.holder, now)

	// Capture the worktree diff as a content-addressed artifact (kept for
	// inspection and audit, including for failed attempts).
	if rec.worktree != "" && h.s.opts.Worktrees != nil && res.Status != adapter.StatusAborted {
		if patch, err := h.s.opts.Worktrees.Diff(ctx, rec.worktree); err == nil && patch != "" {
			_ = h.s.store.RecordArtifact(ctx, store.Artifact{
				RunID: h.runID, AttemptID: rec.attemptID, NodeID: string(rec.nodeID),
				Name: "diff", Hash: worktree.HashContent(patch), Path: rec.worktree, Content: patch,
			})
		}
	}

	// Aggregate message cost/tokens for the attempt row and the run budget.
	var cost float64
	var tokens int
	for _, m := range res.Messages {
		cost += m.Cost
		tokens += m.Tokens
	}
	h.runCost += cost
	h.runTokens += tokens

	switch res.Status {
	case adapter.StatusAborted, adapter.StatusError:
		to := graph.StateFailed
		reason := `"aborted"`
		if res.Err != nil {
			reason = `"` + jsonEscape(res.Err.Error()) + `"`
		}
		if res.Status == adapter.StatusAborted && !res.Budget {
			to = graph.StateCanceled
			reason = `"canceled"`
		}
		if err := h.chainToRunning(ctx, rec.nodeID); err != nil {
			return err
		}
		if err := h.transit(ctx, rec.nodeID, graph.StateRunning, to, `{"reason":`+reason+`}`); err != nil {
			return err
		}
		st := "failed"
		if to == graph.StateCanceled {
			st = "aborted"
		}
		return h.s.store.RecordAttempt(ctx, store.Attempt{
			ID: rec.attemptID, RunID: h.runID, NodeID: string(rec.nodeID), No: rec.no,
			Status: st, SessionID: rec.sess.ID(), Worktree: rec.worktree, Branch: rec.branch,
			FinishedAt: &finished, Cost: cost, Tokens: tokens,
		})
	}

	// Completion: run the evidence gate (resuming through the machine when
	// the node was blocked, e.g. permission-suspended).
	if err := h.chainToVerifying(ctx, rec.nodeID); err != nil {
		return err
	}
	node := h.nodeByID(rec.nodeID)
	verdict, err := h.s.ver.Verdict(ctx, node, rec.no, rec.worktree, res.Messages)
	if err != nil {
		verdict = Verdict{Pass: false, Feedback: "verifier error: " + err.Error()}
	}
	ev, _ := json.Marshal(map[string]any{"pass": verdict.Pass, "feedback": verdict.Feedback, "evidence": verdict.Evidence})
	if err := h.emitEvent(ctx, store.EventVerdict, rec.nodeID, graph.State(""), graph.State(""), rec.attemptID, string(ev)); err != nil {
		return err
	}

	if verdict.Pass {
		if err := h.transit(ctx, rec.nodeID, graph.StateVerifying, graph.StateDone, ""); err != nil {
			return err
		}
		// A successful merge consumes the worktrees it folded in.
		if node.Type == graph.NodeMerge && h.s.opts.Worktrees != nil {
			for _, b := range rec.merged {
				_ = h.s.opts.Worktrees.Remove(ctx, b)
			}
		}
		return h.s.store.RecordAttempt(ctx, store.Attempt{
			ID: rec.attemptID, RunID: h.runID, NodeID: string(rec.nodeID), No: rec.no,
			Status: "done", SessionID: rec.sess.ID(), Worktree: rec.worktree, Branch: rec.branch,
			FinishedAt: &finished, Evidence: verdict.Evidence, Cost: cost, Tokens: tokens,
		})
	}

	// Failed verification: focused feedback is carried into the next
	// attempt. Retry only while retries and cost/token budgets remain.
	row, err := h.s.store.NodeRow(ctx, h.runID, string(rec.nodeID))
	if err != nil {
		return err
	}
	h.feedback[rec.nodeID] = verdict.Feedback
	budgetOK := true
	if node.Budget.MaxTokens > 0 || node.Budget.MaxCost > 0 {
		prevCost, prevTokens, err := h.s.store.NodeCost(ctx, h.runID, string(rec.nodeID))
		if err != nil {
			return err
		}
		if node.Budget.MaxTokens > 0 && prevTokens+tokens >= node.Budget.MaxTokens {
			budgetOK = false
		}
		if node.Budget.MaxCost > 0 && prevCost+cost >= node.Budget.MaxCost {
			budgetOK = false
		}
	}
	if row.RetriesLeft > 0 && budgetOK {
		if err := h.s.store.SetRetriesLeft(ctx, h.runID, string(rec.nodeID), row.RetriesLeft-1, now); err != nil {
			return err
		}
		backoff := h.backoffFor(rec.nodeID)
		readyAt := now.Add(backoff)
		h.retryAt[rec.nodeID] = readyAt
		if err := h.transit(ctx, rec.nodeID, graph.StateVerifying, graph.StateRetryWait, ""); err != nil {
			return err
		}
		p, _ := json.Marshal(map[string]any{"readyAt": readyAt.UnixMilli(), "attempt": rec.no})
		if err := h.emitEvent(ctx, store.EventRetry, rec.nodeID, graph.State(""), graph.State(""), rec.attemptID, string(p)); err != nil {
			return err
		}
		return h.s.store.RecordAttempt(ctx, store.Attempt{
			ID: rec.attemptID, RunID: h.runID, NodeID: string(rec.nodeID), No: rec.no,
			Status: "failed", SessionID: rec.sess.ID(), Worktree: rec.worktree, Branch: rec.branch,
			FinishedAt: &finished, Evidence: verdict.Feedback, Cost: cost, Tokens: tokens,
		})
	}
	h.failures = append(h.failures, now)
	if err := h.transit(ctx, rec.nodeID, graph.StateVerifying, graph.StateFailed, ""); err != nil {
		return err
	}
	return h.s.store.RecordAttempt(ctx, store.Attempt{
		ID: rec.attemptID, RunID: h.runID, NodeID: string(rec.nodeID), No: rec.no,
		Status: "failed", SessionID: rec.sess.ID(), Worktree: rec.worktree, Branch: rec.branch,
		FinishedAt: &finished, Evidence: verdict.Feedback, Cost: cost, Tokens: tokens,
	})
}

// chainToRunning walks a blocked node back to running (permission resume
// or completion arriving while blocked).
func (h *RunHandle) chainToRunning(ctx context.Context, id graph.NodeID) error {
	st, _ := h.tr.State(id)
	switch st {
	case graph.StateRunning:
		return nil
	case graph.StateBlocked:
		if err := h.transit(ctx, id, graph.StateBlocked, graph.StateReady, ""); err != nil {
			return err
		}
		if err := h.transit(ctx, id, graph.StateReady, graph.StateLeased, ""); err != nil {
			return err
		}
		return h.transit(ctx, id, graph.StateLeased, graph.StateRunning, "")
	}
	return fmt.Errorf("cannot resume node %s from state %s", id, st)
}

// chainToVerifying moves a node to the evidence gate, resuming it through
// the machine when it is blocked (e.g. permission-suspended sessions that
// completed).
func (h *RunHandle) chainToVerifying(ctx context.Context, id graph.NodeID) error {
	if err := h.chainToRunning(ctx, id); err != nil {
		return err
	}
	return h.transit(ctx, id, graph.StateRunning, graph.StateVerifying, "")
}

func (h *RunHandle) nodeByID(id graph.NodeID) *graph.Node {
	for _, n := range h.g.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

func (h *RunHandle) backoffFor(id graph.NodeID) time.Duration {
	for _, n := range h.g.Nodes {
		if n.ID == id {
			return n.RetryPolicy.Backoff
		}
	}
	return 0
}

func (h *RunHandle) findSession(attemptID string) *sessionRec {
	for _, rec := range h.sessions {
		if rec.attemptID == attemptID {
			return rec
		}
	}
	for _, rec := range h.suspended {
		if rec.attemptID == attemptID {
			return rec
		}
	}
	return nil
}

func (h *RunHandle) emitEvent(ctx context.Context, typ store.EventType, id graph.NodeID, from, to graph.State, attemptID, payload string) error {
	_, err := h.s.store.AppendEvent(ctx, h.runID, string(id), typ, from, to, attemptID, payload, h.s.clk.Now())
	return err
}

// allSettled reports whether every node is terminal or blocked, and
// whether any node is blocked (run waiting for human intervention).
func (h *RunHandle) allSettled() (settled, waiting bool) {
	for _, n := range h.g.Nodes {
		st, _ := h.tr.State(n.ID)
		if st.Terminal() {
			continue
		}
		if st == graph.StateBlocked {
			waiting = true
			continue
		}
		return false, false
	}
	return true, waiting
}

// Resume restarts a settled run (e.g. after a human unblocks a node).
func (h *RunHandle) Resume(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.done {
		return nil
	}
	h.done = false
	return h.s.store.CompleteRun(ctx, h.runID, "active", h.s.clk.Now())
}

func (h *RunHandle) Done() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.done
}

func (h *RunHandle) StepCount() int64 { return h.stepCount }

func (h *RunHandle) State(id graph.NodeID) (graph.State, bool) { return h.tr.State(id) }

func (h *RunHandle) RunID() string { return h.runID }

func (h *RunHandle) ActiveSessions() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.sessions)
}

// CancelNode aborts the in-flight attempt of a node (operator cancel).
// The node transitions to canceled once the driver reports the abort.
func (h *RunHandle) CancelNode(ctx context.Context, id graph.NodeID) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	rec, ok := h.sessions[id]
	if !ok {
		return fmt.Errorf("node %s has no in-flight attempt", id)
	}
	return rec.sess.Abort(ctx)
}

// Run drives the run to completion in real time.
func (h *RunHandle) Run(ctx context.Context, tick time.Duration) error {
	for !h.Done() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.s.clk.After(tick):
			if err := h.Step(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func jsonEscape(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
