package sched_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"corral/internal/adapter"
	"corral/internal/graph"
	"corral/internal/sched"
	"corral/internal/store"
	"corral/internal/verify"
)

type flakyAbortDriver struct {
	mu      sync.Mutex
	session *flakyAbortSession
	emitted bool
	attempt adapter.Attempt
}

type flakyAbortSession struct {
	driver     *flakyAbortDriver
	abortCalls int
	aborted    bool
}

func (d *flakyAbortDriver) Start(_ context.Context, attempt adapter.Attempt) (adapter.Session, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.attempt = attempt
	d.session = &flakyAbortSession{driver: d}
	return d.session, nil
}

func (d *flakyAbortDriver) Step(context.Context, time.Time) []adapter.Completion {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.session == nil || !d.session.aborted || d.emitted {
		return nil
	}
	d.emitted = true
	return []adapter.Completion{{AttemptID: d.attempt.ID, SessionID: d.session.ID(), Status: adapter.StatusAborted}}
}

func (s *flakyAbortSession) ID() string                         { return "ses-flaky-abort" }
func (s *flakyAbortSession) ServerID() string                   { return "fake" }
func (s *flakyAbortSession) Send(context.Context, string) error { return nil }
func (s *flakyAbortSession) Abort(context.Context) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	s.abortCalls++
	if s.abortCalls == 1 {
		return fmt.Errorf("transient abort failure")
	}
	s.aborted = true
	return nil
}
func (s *flakyAbortSession) Status(context.Context) (adapter.Status, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	if s.aborted {
		return adapter.StatusAborted, nil
	}
	return adapter.StatusRunning, nil
}
func (s *flakyAbortSession) Messages(context.Context) ([]adapter.Message, error) { return nil, nil }
func (s *flakyAbortSession) Close(context.Context) error                         { return nil }

func TestBudgetAbortFailureRetriesUntilProviderStops(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	drv := &flakyAbortDriver{}
	n := agent("w1")
	n.Budget.MaxDuration = tick
	s := sched.New(st, drv, sched.NewFakeVerifier(nil, sched.Verdict{Pass: true}), clk, sched.Options{Concurrency: 1})
	h, err := s.Create(context.Background(), "run-abort-retry", &graph.Graph{Nodes: []*graph.Node{n}})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * tick)
	if err := h.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state, _ := h.State("w1"); state != graph.StateRunning {
		t.Fatalf("state after failed abort = %s, want running for retry", state)
	}
	if err := h.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state, _ := h.State("w1"); state != graph.StateFailed {
		t.Fatalf("state after retried abort = %s, want failed", state)
	}
	drv.mu.Lock()
	calls := drv.session.abortCalls
	drv.mu.Unlock()
	if calls != 2 {
		t.Fatalf("abort calls = %d, want 2", calls)
	}
}

func TestPermissionWaitPausesAttemptTimeBudget(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	n := agent("w1")
	n.Budget.MaxDuration = 10 * tick

	drv := sched.NewFakeDriver(clk, map[string][]sched.Script{
		"w1": {{Delay: time.Hour, Permission: "perm-1"}},
	})
	s := newSched(t, st, drv, &sched.EngineVerifier{Eng: verify.New(t.TempDir())}, clk, sched.Options{Concurrency: 1})
	h, err := s.Create(context.Background(), "run-budget-pause", &graph.Graph{Nodes: []*graph.Node{n}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		step(h, clk, ctx)
		if state, _ := h.State("w1"); state == graph.StateBlocked {
			break
		}
	}
	if state, _ := h.State("w1"); state != graph.StateBlocked {
		t.Fatalf("w1 = %s, want blocked on permission", state)
	}

	// Time spent waiting for an operator must not consume attempt runtime.
	clk.Advance(100 * tick)
	ps, err := h.PermissionSession(ctx, "w1")
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.RespondPermission(ctx, "perm-1", true); err != nil {
		t.Fatal(err)
	}
	if err := h.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	step(h, clk, ctx) // resumes and re-arms the saved runtime budget

	clk.Advance(8 * tick)
	if err := h.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if state, _ := h.State("w1"); state != graph.StateRunning {
		t.Fatalf("w1 = %s before saved budget elapsed, want running", state)
	}

	clk.Advance(2 * tick)
	if err := h.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if state, _ := h.State("w1"); state != graph.StateFailed {
		t.Fatalf("w1 = %s after saved budget elapsed, want failed", state)
	}
}

func TestSharedDriverRoutesCompletionsToOwningRun(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	drv := sched.NewFakeDriver(clk, map[string][]sched.Script{
		"a": {{Delay: tick, Messages: []adapter.Message{{Role: "assistant", Finish: "stop", Text: "a"}}}},
		"b": {{Delay: tick, Messages: []adapter.Message{{Role: "assistant", Finish: "stop", Text: "b"}}}},
	})
	engine := &sched.EngineVerifier{Eng: verify.New(t.TempDir())}
	s := newSched(t, st, drv, engine, clk, sched.Options{Concurrency: 1})
	n1, n2 := agent("a"), agent("b")
	n1.Verification = &graph.Verification{Kind: "command", Command: []string{"true"}}
	n2.Verification = &graph.Verification{Kind: "command", Command: []string{"true"}}
	h1, err := s.Create(context.Background(), "run-one", &graph.Graph{Nodes: []*graph.Node{n1}})
	if err != nil {
		t.Fatal(err)
	}
	h2, err := s.Create(context.Background(), "run-two", &graph.Graph{Nodes: []*graph.Node{n2}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := h1.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Step(ctx); err != nil {
		t.Fatal(err)
	}
	clk.Advance(tick)

	// One Stepper call drains both completions. Each must be retained for its
	// owning handle instead of making whichever run stepped first fail.
	if err := h1.Step(ctx); err != nil {
		t.Fatalf("run one consumed another run's completion: %v", err)
	}
	if state, _ := h1.State("a"); state != graph.StateDone {
		t.Fatalf("run one node = %s, want done", state)
	}
	if state, _ := h2.State("b"); state != graph.StateRunning {
		t.Fatalf("run two node changed before its handle stepped: %s", state)
	}
	if err := h2.Step(ctx); err != nil {
		t.Fatalf("run two lost its routed completion: %v", err)
	}
	if state, _ := h2.State("b"); state != graph.StateDone {
		t.Fatalf("run two node = %s, want done", state)
	}
}

func TestCompletionBurstBeyondLegacyBuffer(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	const count = 40
	nodes := make([]*graph.Node, 0, count)
	scripts := make(map[string][]sched.Script, count)
	for i := 0; i < count; i++ {
		id := graph.NodeID(fmt.Sprintf("n%02d", i))
		n := agent(id)
		n.Verification = &graph.Verification{Kind: "command", Command: []string{"true"}}
		nodes = append(nodes, n)
		scripts[string(id)] = []sched.Script{{Delay: tick}}
	}
	drv := sched.NewFakeDriver(clk, scripts)
	s := newSched(t, st, drv, &sched.EngineVerifier{Eng: verify.New(t.TempDir())}, clk, sched.Options{Concurrency: count})
	h, err := s.Create(context.Background(), "run-burst", &graph.Graph{Nodes: nodes})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	clk.Advance(tick)
	done := make(chan error, 1)
	go func() { done <- h.Step(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("completion burst blocked while routing more than 32 results")
	}
	for _, n := range nodes {
		if state, _ := h.State(n.ID); state != graph.StateDone {
			t.Fatalf("%s = %s, want done", n.ID, state)
		}
	}
}

// TestPermissionRequestBlocksExplicitly drives the permission flow: the
// node moves to an explicit blocked state while the session waits, then
// resumes automatically after the operator answers, and completes.
func TestPermissionRequestBlocksExplicitly(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	workdir := t.TempDir()
	eng := verify.New(workdir)
	eng.Runner = verify.ExecRunner{}

	n := agent("w1")
	n.Role = "worker"
	n.WriteScope = []string{"a.txt"}
	n.Verification = &graph.Verification{Kind: "command", Command: []string{"test", "-f", "a.txt"}}
	n.Meta = map[string]string{"cwd": workdir}

	drv := sched.NewFakeDriver(clk, map[string][]sched.Script{
		"w1": {{Delay: 5 * tick, Permission: "perm-1", Write: map[string]string{"a.txt": "A1"}}},
	})
	s := newSched(t, st, drv, &sched.EngineVerifier{Eng: eng}, clk, sched.Options{Concurrency: 1})
	h, err := s.Create(context.Background(), "run-perm", &graph.Graph{Nodes: []*graph.Node{n}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Drive until the permission blocks the node.
	blocked := false
	for i := 0; i < 50; i++ {
		step(h, clk, ctx)
		if st2, _ := h.State("w1"); st2 == graph.StateBlocked {
			blocked = true
			break
		}
	}
	if !blocked {
		t.Fatal("node never reached blocked on permission")
	}
	// The blocked transition carries the permission reason.
	evs, _ := st.Events(ctx, "run-perm")
	found := false
	for _, ev := range evs {
		if string(ev.NodeID) == "w1" && ev.To == graph.StateBlocked {
			if string(ev.Payload) == "" || string(ev.Payload) == "{}" {
				t.Fatalf("blocked transition has no payload: %s", ev.Payload)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("blocked transition not recorded")
	}
	if h.ActiveSessions() != 0 {
		t.Fatal("permission-blocked session must be suspended, not active")
	}

	// Operator answers the permission; the run resumes by itself. The run
	// had settled (waiting) while blocked, so drive steps manually.
	ps, err := h.PermissionSession(ctx, "w1")
	if err != nil {
		t.Fatalf("permission session: %v", err)
	}
	if err := ps.RespondPermission(ctx, "perm-1", true); err != nil {
		t.Fatal(err)
	}
	// The run settled (waiting) while blocked; re-activate it like the
	// daemon's permission handler does.
	if err := h.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	drive(t, h, clk, 100)
	if st2, _ := h.State("w1"); st2 != graph.StateDone {
		t.Fatalf("w1 = %s, want done after permission allowed", st2)
	}
	atts, _ := st.Attempts(ctx, "run-perm", "w1")
	if len(atts) != 1 || atts[0].Status != "done" {
		t.Fatalf("attempts = %+v, want exactly one done attempt", atts)
	}
}

// TestCircuitBreakerStopsNewWork: after MaxFailures node failures within
// the window, remaining pending nodes block and the run settles.
func TestCircuitBreakerStopsNewWork(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	eng := verify.New(t.TempDir())
	eng.Runner = &scriptedRunner{results: []runResult{{exit: 1, stderr: "boom"}}}

	var nodes []*graph.Node
	for i := 0; i < 3; i++ {
		id := graph.NodeID(string(rune('a' + i)))
		n := agent(id)
		n.Verification = &graph.Verification{Kind: "command", Command: []string{"false"}}
		n.RetryPolicy.MaxRetries = 0
		nodes = append(nodes, n)
	}
	waiting := agent("z", nodes[0].ID, nodes[1].ID, nodes[2].ID)
	waiting.Verification = &graph.Verification{Kind: "command", Command: []string{"true"}}
	nodes = append(nodes, waiting)

	drv := sched.NewFakeDriver(clk, nil)
	s := newSched(t, st, drv, &sched.EngineVerifier{Eng: eng}, clk, sched.Options{
		Concurrency: 3, BreakerMaxFailures: 2, BreakerWindow: tick * 100,
	})
	h, err := s.Create(context.Background(), "run-breaker", &graph.Graph{Nodes: nodes})
	if err != nil {
		t.Fatal(err)
	}
	drive(t, h, clk, 200)
	if !h.Done() {
		t.Fatal("run should settle after breaker trips")
	}
	if st2, _ := h.State("z"); st2 != graph.StateBlocked {
		t.Fatalf("z = %s, want blocked (circuit breaker)", st2)
	}
	if n, _ := st.CountAttempts(context.Background(), "run-breaker", "z"); n != 0 {
		t.Fatalf("z ran despite breaker: %d attempts", n)
	}
}

// TestRunBudgetBlocksNewWork: once the run token budget is exhausted, no
// further nodes start.
func TestRunBudgetBlocksNewWork(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	eng := verify.New(t.TempDir())
	eng.Runner = verify.ExecRunner{}

	workdir := t.TempDir()
	eng = verify.New(workdir)
	n1 := agent("n1")
	n1.Role = "worker"
	n1.WriteScope = []string{"a.txt"}
	n1.Verification = &graph.Verification{Kind: "command", Command: []string{"test", "-f", "a.txt"}}
	n1.Meta = map[string]string{"cwd": workdir}
	n2 := agent("n2", "n1") // only ready after n1 finishes (budget already tripped)
	n2.Role = "worker"
	n2.WriteScope = []string{"b.txt"}
	n2.Verification = &graph.Verification{Kind: "command", Command: []string{"test", "-f", "b.txt"}}
	n2.Meta = map[string]string{"cwd": workdir}

	drv := sched.NewFakeDriver(clk, map[string][]sched.Script{
		"n1": {{Delay: tick, Write: map[string]string{"a.txt": "A"}, Messages: []adapter.Message{{Role: "assistant", Finish: "stop", Tokens: 100}}}},
		"n2": {{Delay: tick, Write: map[string]string{"b.txt": "B"}, Messages: []adapter.Message{{Role: "assistant", Finish: "stop", Tokens: 100}}}},
	})
	s := newSched(t, st, drv, &sched.EngineVerifier{Eng: eng}, clk, sched.Options{
		Concurrency: 2, RunMaxTokens: 50, // first attempt (100 tok) already exceeds
	})
	h, err := s.Create(context.Background(), "run-budget", &graph.Graph{Nodes: []*graph.Node{n1, n2}})
	if err != nil {
		t.Fatal(err)
	}
	drive(t, h, clk, 200)
	if st1, _ := h.State("n1"); st1 != graph.StateDone {
		t.Fatalf("n1 = %s, want done", st1)
	}
	if st2, _ := h.State("n2"); st2 != graph.StateBlocked {
		t.Fatalf("n2 = %s, want blocked (run budget exceeded)", st2)
	}
	if n, _ := st.CountAttempts(context.Background(), "run-budget", "n2"); n != 0 {
		t.Fatalf("n2 ran despite budget: %d attempts", n)
	}
}

func TestLoadRestoresRunBudgetAndRetryCannotBypassIt(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	ctx := context.Background()
	a, z := agent("a"), agent("z")
	g := &graph.Graph{Nodes: []*graph.Node{a, z}}
	seed := newSched(t, st, sched.NewFakeDriver(clk, nil), sched.NewFakeVerifier(nil, sched.Verdict{Pass: true}), clk, sched.Options{})
	if _, err := seed.Create(ctx, "run-budget-reload", g); err != nil {
		t.Fatal(err)
	}
	persistTerminalAttempt(t, st, clk.Now(), "run-budget-reload", "a", graph.StateDone, 1.25, 100)

	drv := sched.NewFakeDriver(clk, scriptsFor("z"))
	s := newSched(t, st, drv, sched.NewFakeVerifier(nil, sched.Verdict{Pass: true}), clk, sched.Options{
		Concurrency: 1, RunMaxTokens: 50,
	})
	h, err := s.Load(ctx, "run-budget-reload")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := h.State("z"); got != graph.StateBlocked {
		t.Fatalf("z = %s after reload, want blocked by restored run budget", got)
	}
	if err := h.RetryNode(ctx, "z"); err != nil {
		t.Fatal(err)
	}
	if err := h.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := h.State("z"); got != graph.StateBlocked {
		t.Fatalf("z = %s after retry, want run budget to block it again", got)
	}
	if attempts, _ := st.CountAttempts(ctx, "run-budget-reload", "z"); attempts != 0 {
		t.Fatalf("z started despite restored run budget: %d attempts", attempts)
	}
}

func TestLoadRestoresBreakerAndOperatorRetryResetsHistory(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	ctx := context.Background()
	a, z := agent("a"), agent("z")
	g := &graph.Graph{Nodes: []*graph.Node{a, z}}
	seed := newSched(t, st, sched.NewFakeDriver(clk, nil), sched.NewFakeVerifier(nil, sched.Verdict{Pass: true}), clk, sched.Options{})
	if _, err := seed.Create(ctx, "run-breaker-reload", g); err != nil {
		t.Fatal(err)
	}
	persistTerminalAttempt(t, st, clk.Now(), "run-breaker-reload", "a", graph.StateFailed, 0, 0)

	opts := sched.Options{Concurrency: 1, BreakerMaxFailures: 1, BreakerWindow: time.Hour}
	drv := sched.NewFakeDriver(clk, scriptsFor("z"))
	s := newSched(t, st, drv, sched.NewFakeVerifier(nil, sched.Verdict{Pass: true}), clk, opts)
	h, err := s.Load(ctx, "run-breaker-reload")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := h.State("z"); got != graph.StateBlocked {
		t.Fatalf("z = %s after reload, want blocked by restored breaker", got)
	}
	if err := h.RetryNode(ctx, "z"); err != nil {
		t.Fatal(err)
	}

	// Retry reset is durable: a fresh handle must not reconstruct failures
	// from before the operator override.
	drv2 := sched.NewFakeDriver(clk, scriptsFor("z"))
	s2 := newSched(t, st, drv2, sched.NewFakeVerifier(nil, sched.Verdict{Pass: true}), clk, opts)
	h2, err := s2.Load(ctx, "run-breaker-reload")
	if err != nil {
		t.Fatal(err)
	}
	if err := h2.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := h2.State("z"); got != graph.StateRunning {
		t.Fatalf("z = %s after durable breaker reset, want running", got)
	}
}

func TestLoadIgnoresHistoricalRetryForTerminalNode(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	ctx := context.Background()
	n := agent("w1")
	seed := newSched(t, st, sched.NewFakeDriver(clk, nil), sched.NewFakeVerifier(nil, sched.Verdict{Pass: true}), clk, sched.Options{})
	if _, err := seed.Create(ctx, "run-terminal-retry", &graph.Graph{Nodes: []*graph.Node{n}}); err != nil {
		t.Fatal(err)
	}
	now := clk.Now()
	for _, edge := range [][2]graph.State{
		{graph.StatePending, graph.StateReady},
		{graph.StateReady, graph.StateLeased},
		{graph.StateLeased, graph.StateRunning},
		{graph.StateRunning, graph.StateVerifying},
		{graph.StateVerifying, graph.StateRetryWait},
	} {
		if _, err := st.AppendTransition(ctx, "run-terminal-retry", "w1", edge[0], edge[1], "", now); err != nil {
			t.Fatal(err)
		}
	}
	oldReady := now.Add(-time.Hour).UnixMilli()
	if _, err := st.AppendEvent(ctx, "run-terminal-retry", "w1", store.EventRetry, "", "", "", fmt.Sprintf(`{"readyAt":%d}`, oldReady), now); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]graph.State{
		{graph.StateRetryWait, graph.StateReady},
		{graph.StateReady, graph.StateLeased},
		{graph.StateLeased, graph.StateRunning},
		{graph.StateRunning, graph.StateVerifying},
		{graph.StateVerifying, graph.StateDone},
	} {
		if _, err := st.AppendTransition(ctx, "run-terminal-retry", "w1", edge[0], edge[1], "", now); err != nil {
			t.Fatal(err)
		}
	}

	s := newSched(t, st, sched.NewFakeDriver(clk, nil), sched.NewFakeVerifier(nil, sched.Verdict{Pass: true}), clk, sched.Options{})
	h, err := s.Load(ctx, "run-terminal-retry")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Step(ctx); err != nil {
		t.Fatalf("historical retry corrupted terminal replay: %v", err)
	}
	if state, _ := h.State("w1"); state != graph.StateDone {
		t.Fatalf("w1 = %s, want done", state)
	}
}

func TestLoadUsesLatestRetryDeadline(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	ctx := context.Background()
	n := agent("w1")
	seed := newSched(t, st, sched.NewFakeDriver(clk, nil), sched.NewFakeVerifier(nil, sched.Verdict{Pass: true}), clk, sched.Options{})
	if _, err := seed.Create(ctx, "run-latest-retry", &graph.Graph{Nodes: []*graph.Node{n}}); err != nil {
		t.Fatal(err)
	}
	now := clk.Now()
	for _, edge := range [][2]graph.State{
		{graph.StatePending, graph.StateReady},
		{graph.StateReady, graph.StateLeased},
		{graph.StateLeased, graph.StateRunning},
		{graph.StateRunning, graph.StateVerifying},
		{graph.StateVerifying, graph.StateRetryWait},
	} {
		if _, err := st.AppendTransition(ctx, "run-latest-retry", "w1", edge[0], edge[1], "", now); err != nil {
			t.Fatal(err)
		}
	}
	for _, readyAt := range []int64{now.Add(-time.Hour).UnixMilli(), now.Add(time.Hour).UnixMilli()} {
		if _, err := st.AppendEvent(ctx, "run-latest-retry", "w1", store.EventRetry, "", "", "", fmt.Sprintf(`{"readyAt":%d}`, readyAt), now); err != nil {
			t.Fatal(err)
		}
	}
	s := newSched(t, st, sched.NewFakeDriver(clk, nil), sched.NewFakeVerifier(nil, sched.Verdict{Pass: true}), clk, sched.Options{})
	h, err := s.Load(ctx, "run-latest-retry")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if state, _ := h.State("w1"); state != graph.StateRetryWait {
		t.Fatalf("w1 = %s, want retry_wait until latest deadline", state)
	}
}

func TestRetryDependentWithFailedDependencyReblocksCleanly(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	a := agent("a")
	a.RetryPolicy.MaxRetries = 0
	b := agent("b", "a")
	drv := sched.NewFakeDriver(clk, scriptsFor("a"))
	ver := sched.NewFakeVerifier(map[string][]sched.Verdict{
		"a": {{Pass: false, Feedback: "fail"}},
	}, sched.Verdict{Pass: true})
	s := newSched(t, st, drv, ver, clk, sched.Options{Concurrency: 1})
	h, err := s.Create(context.Background(), "run-dependent-retry", &graph.Graph{Nodes: []*graph.Node{a, b}})
	if err != nil {
		t.Fatal(err)
	}
	drive(t, h, clk, 20)
	if got, _ := h.State("b"); got != graph.StateBlocked {
		t.Fatalf("b = %s, want blocked", got)
	}
	if err := h.RetryNode(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	if err := h.Step(context.Background()); err != nil {
		t.Fatalf("reblock retried dependent: %v", err)
	}
	if got, _ := h.State("b"); got != graph.StateBlocked {
		t.Fatalf("b = %s after retry, want blocked while dependency failed", got)
	}
}

func persistTerminalAttempt(t *testing.T, st *store.Store, now time.Time, runID string, nodeID graph.NodeID, terminal graph.State, cost float64, tokens int) {
	t.Helper()
	ctx := context.Background()
	states := []graph.State{graph.StateReady, graph.StateLeased, graph.StateRunning, graph.StateVerifying, terminal}
	from := graph.StatePending
	for _, to := range states {
		if _, err := st.AppendEvent(ctx, runID, string(nodeID), store.EventTransition, from, to, "", "", now); err != nil {
			t.Fatal(err)
		}
		from = to
	}
	if err := st.SetNodeState(ctx, runID, string(nodeID), terminal, now); err != nil {
		t.Fatal(err)
	}
	started, finished := now.Add(-tick).UnixMilli(), now.UnixMilli()
	status := "done"
	if terminal == graph.StateFailed {
		status = "failed"
	}
	if err := st.RecordAttempt(ctx, store.Attempt{
		ID: runID + "/" + string(nodeID) + "/1", RunID: runID, NodeID: string(nodeID), No: 1,
		Status: status, StartedAt: &started, FinishedAt: &finished, Cost: cost, Tokens: tokens,
	}); err != nil {
		t.Fatal(err)
	}
}

var _ = sched.Verdict{}
