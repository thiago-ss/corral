package sched_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"corral/internal/adapter"
	"corral/internal/clock"
	"corral/internal/graph"
	"corral/internal/sched"
	"corral/internal/store"
)

const tick = time.Millisecond

func newStore(t *testing.T) *store.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "corral.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func fakeClock() *clock.Fake {
	return clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
}

func newSched(t *testing.T, st *store.Store, drv *sched.FakeDriver, ver sched.Verifier, clk *clock.Fake, opts sched.Options) *sched.Scheduler {
	t.Helper()
	return sched.New(st, drv, ver, clk, opts)
}

// drive runs the run to completion (or maxSteps) deterministically.
func drive(t *testing.T, h *sched.RunHandle, clk *clock.Fake, maxSteps int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < maxSteps && !h.Done(); i++ {
		clk.Advance(tick)
		if err := h.Step(ctx); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
}

func agent(id graph.NodeID, deps ...graph.NodeID) *graph.Node {
	return &graph.Node{
		ID:                 id,
		Type:               graph.NodeAgent,
		Objective:          "do " + string(id),
		AcceptanceCriteria: []string{"criterion"},
		Priority:           graph.PriorityNormal,
		DependsOn:          deps,
		RetryPolicy:        graph.RetryPolicy{MaxRetries: 2, Backoff: 3 * tick},
		Budget:             graph.Budget{},
	}
}

func scriptsFor(ids ...graph.NodeID) map[string][]sched.Script {
	m := map[string][]sched.Script{}
	for _, id := range ids {
		m[string(id)] = []sched.Script{{Delay: 1 * tick}}
	}
	return m
}

func seqOf(t *testing.T, st *store.Store, runID string, nodeID graph.NodeID, typ store.EventType, to graph.State) int64 {
	t.Helper()
	evs, err := st.Events(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range evs {
		if string(ev.NodeID) == string(nodeID) && ev.Type == typ {
			if to != "" && ev.To != to {
				continue
			}
			return ev.Seq
		}
	}
	return 0
}

func TestSixNodeForkJoinDeterministic(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	g := &graph.Graph{Nodes: []*graph.Node{
		agent("a"),
		agent("b", "a"),
		agent("c", "a"),
		agent("d", "b"),
		agent("e", "c"),
		agent("f", "d", "e"),
	}}
	drv := sched.NewFakeDriver(clk, scriptsFor("a", "b", "c", "d", "e", "f"))
	ver := sched.NewFakeVerifier(nil, sched.Verdict{Pass: true})
	s := newSched(t, st, drv, ver, clk, sched.Options{Concurrency: 4})
	h, err := s.Create(context.Background(), "run-forkjoin", g)
	if err != nil {
		t.Fatal(err)
	}
	drive(t, h, clk, 100)
	if !h.Done() {
		t.Fatalf("run did not complete; states: %v", statesOf(t, st, "run-forkjoin"))
	}

	// Fan-out: b and c are both leased before either completes.
	lb, lc := seqOf(t, st, "run-forkjoin", "b", store.EventTransition, graph.StateLeased),
		seqOf(t, st, "run-forkjoin", "c", store.EventTransition, graph.StateLeased)
	db, dc := seqOf(t, st, "run-forkjoin", "b", store.EventTransition, graph.StateDone),
		seqOf(t, st, "run-forkjoin", "c", store.EventTransition, graph.StateDone)
	if lb == 0 || lc == 0 || db == 0 || dc == 0 {
		t.Fatal("missing events")
	}
	if lb > db || lc > dc {
		t.Fatalf("node completed before its own lease: b(%d,%d) c(%d,%d)", lb, db, lc, dc)
	}
	if lb > dc || lc > db {
		t.Fatalf("fan-out not concurrent: b leased=%d c leased=%d, first done=%d", lb, lc, min2(db, dc))
	}

	// Fan-in: f starts only after both d and e are done.
	lf := seqOf(t, st, "run-forkjoin", "f", store.EventTransition, graph.StateLeased)
	dd := seqOf(t, st, "run-forkjoin", "d", store.EventTransition, graph.StateDone)
	de := seqOf(t, st, "run-forkjoin", "e", store.EventTransition, graph.StateDone)
	if lf == 0 || lf < dd || lf < de {
		t.Fatalf("fan-in violated: f leased=%d, d done=%d, e done=%d", lf, dd, de)
	}

	// Deterministic completion order a, b, c, d, e, f.
	order := []string{"a", "b", "c", "d", "e", "f"}
	prev := int64(0)
	for _, id := range order {
		sq := seqOf(t, st, "run-forkjoin", graph.NodeID(id), store.EventTransition, graph.StateDone)
		if sq == 0 {
			t.Fatalf("%s never done", id)
		}
		if sq < prev {
			t.Fatalf("completion order not deterministic: %s at %d after %d", id, sq, prev)
		}
		prev = sq
	}

	// Exactly one attempt per node.
	for _, id := range order {
		n, err := st.CountAttempts(context.Background(), "run-forkjoin", id)
		if err != nil || n != 1 {
			t.Fatalf("node %s attempts = %d, want 1 (%v)", id, n, err)
		}
	}
}

func TestConcurrencyLimit(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	var nodes []*graph.Node
	var ids []graph.NodeID
	for i := 0; i < 8; i++ {
		id := graph.NodeID(fmt.Sprintf("n%d", i))
		ids = append(ids, id)
		nodes = append(nodes, agent(id))
	}
	drv := sched.NewFakeDriver(clk, scriptsFor(ids...))
	ver := sched.NewFakeVerifier(nil, sched.Verdict{Pass: true})
	s := newSched(t, st, drv, ver, clk, sched.Options{Concurrency: 3})
	h, err := s.Create(context.Background(), "run-conc", &graph.Graph{Nodes: nodes})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	peak := 0
	for i := 0; i < 200 && !h.Done(); i++ {
		clk.Advance(tick)
		if err := h.Step(ctx); err != nil {
			t.Fatal(err)
		}
		if a := h.ActiveSessions(); a > peak {
			peak = a
		}
	}
	if !h.Done() {
		t.Fatal("run did not complete")
	}
	if peak > 3 {
		t.Fatalf("concurrency limit violated: peak %d > 3", peak)
	}
}

func TestPriorityAndAging(t *testing.T) {
	run := func(t *testing.T, agingBoost, agingCap int) (int64, int64) {
		st := newStore(t)
		clk := fakeClock()
		// gate -> h1 -> h2 -> ... -> h6: high-priority nodes arrive one at a
		// time, so only lo accumulates age while waiting.
		gate := agent("gate")
		gate.Priority = graph.PriorityNormal
		nodes := []*graph.Node{gate, agent("lo")}
		nodes[1].Priority = graph.PriorityLow
		prev := graph.NodeID("gate")
		for i := 1; i <= 6; i++ {
			h := agent(graph.NodeID(fmt.Sprintf("h%d", i)), prev)
			h.Priority = graph.PriorityHigh
			nodes = append(nodes, h)
			prev = h.ID
		}
		var all []graph.NodeID
		for _, n := range nodes {
			all = append(all, n.ID)
		}
		drv := sched.NewFakeDriver(clk, scriptsFor(all...))
		ver := sched.NewFakeVerifier(nil, sched.Verdict{Pass: true})
		s := newSched(t, st, drv, ver, clk, sched.Options{
			Concurrency: 1, AgingBoostPerTick: agingBoost, AgingCap: agingCap,
		})
		h, err := s.Create(context.Background(), "run-aging", &graph.Graph{Nodes: nodes})
		if err != nil {
			t.Fatal(err)
		}
		drive(t, h, clk, 200)
		if !h.Done() {
			t.Fatal("run did not complete")
		}
		return seqOf(t, st, "run-aging", "lo", store.EventTransition, graph.StateLeased),
			seqOf(t, st, "run-aging", "h6", store.EventTransition, graph.StateLeased)
	}

	// Without aging, lo is strictly last.
	loSeq, h6Seq := run(t, 0, 0)
	if loSeq <= h6Seq {
		t.Fatalf("without aging lo (%d) should start after h6 (%d)", loSeq, h6Seq)
	}
	// With aging (boost 20/tick, cap 200) lo overtakes h6 before it starts.
	loSeq, h6Seq = run(t, 20, 200)
	if loSeq >= h6Seq || loSeq == 0 {
		t.Fatalf("aging failed to promote lo: lo=%d h6=%d", loSeq, h6Seq)
	}
}

func TestRetryPolicy(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	r := agent("r")
	r.RetryPolicy = graph.RetryPolicy{MaxRetries: 2, Backoff: 3 * tick}
	drv := sched.NewFakeDriver(clk, scriptsFor("r"))
	ver := sched.NewFakeVerifier(map[string][]sched.Verdict{
		"r": {{Pass: false, Feedback: "nope"}, {Pass: true}},
	}, sched.Verdict{Pass: true})
	s := newSched(t, st, drv, ver, clk, sched.Options{Concurrency: 1})
	h, err := s.Create(context.Background(), "run-retry", &graph.Graph{Nodes: []*graph.Node{r}})
	if err != nil {
		t.Fatal(err)
	}
	drive(t, h, clk, 100)
	if !h.Done() {
		t.Fatal("run did not complete")
	}
	if st2, _ := h.State("r"); st2 != graph.StateDone {
		t.Fatalf("r state = %s, want done", st2)
	}
	atts, err := st.Attempts(context.Background(), "run-retry", "r")
	if err != nil {
		t.Fatal(err)
	}
	if len(atts) != 2 {
		t.Fatalf("attempts = %d, want 2 (fail then pass)", len(atts))
	}
	if atts[0].Status != "failed" || atts[1].Status != "done" {
		t.Fatalf("attempt statuses = %s, %s; want failed, done", atts[0].Status, atts[1].Status)
	}
	if sq := seqOf(t, st, "run-retry", "r", store.EventTransition, graph.StateRetryWait); sq == 0 {
		t.Fatal("retry_wait transition missing")
	}
}

func TestRetryExhaustedFails(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	r := agent("r")
	r.RetryPolicy = graph.RetryPolicy{MaxRetries: 1, Backoff: tick}
	drv := sched.NewFakeDriver(clk, scriptsFor("r"))
	ver := sched.NewFakeVerifier(map[string][]sched.Verdict{
		"r": {{Pass: false}, {Pass: false}},
	}, sched.Verdict{Pass: false})
	s := newSched(t, st, drv, ver, clk, sched.Options{Concurrency: 1})
	h, err := s.Create(context.Background(), "run-exhaust", &graph.Graph{Nodes: []*graph.Node{r}})
	if err != nil {
		t.Fatal(err)
	}
	drive(t, h, clk, 100)
	if st2, _ := h.State("r"); st2 != graph.StateFailed {
		t.Fatalf("r state = %s, want failed", st2)
	}
	n, _ := st.CountAttempts(context.Background(), "run-exhaust", "r")
	if n != 2 {
		t.Fatalf("attempts = %d, want 2", n)
	}
}

func TestAttemptIDsAreUniqueAcrossRuns(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	ctx := context.Background()

	for _, runID := range []string{"run-first", "run-second"} {
		drv := sched.NewFakeDriver(clk, scriptsFor("worker"))
		ver := sched.NewFakeVerifier(nil, sched.Verdict{Pass: true})
		s := newSched(t, st, drv, ver, clk, sched.Options{Concurrency: 1})
		h, err := s.Create(ctx, runID, &graph.Graph{Nodes: []*graph.Node{agent("worker")}})
		if err != nil {
			t.Fatal(err)
		}
		drive(t, h, clk, 10)
		if !h.Done() {
			t.Fatalf("%s did not complete", runID)
		}
	}

	for _, runID := range []string{"run-first", "run-second"} {
		attempts, err := st.Attempts(ctx, runID, "worker")
		if err != nil {
			t.Fatal(err)
		}
		if len(attempts) != 1 {
			t.Fatalf("%s attempts = %d, want 1", runID, len(attempts))
		}
		wantID := runID + "/worker/1"
		if attempts[0].ID != wantID {
			t.Fatalf("%s attempt ID = %q, want %q", runID, attempts[0].ID, wantID)
		}
	}
}

func TestBudgetAbortFails(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	x := agent("x")
	x.Budget = graph.Budget{MaxDuration: 3 * tick}
	drv := sched.NewFakeDriver(clk, map[string][]sched.Script{
		"x": {{Delay: 100 * tick}},
	})
	ver := sched.NewFakeVerifier(nil, sched.Verdict{Pass: true})
	s := newSched(t, st, drv, ver, clk, sched.Options{Concurrency: 1})
	h, err := s.Create(context.Background(), "run-budget", &graph.Graph{Nodes: []*graph.Node{x}})
	if err != nil {
		t.Fatal(err)
	}
	drive(t, h, clk, 100)
	if st2, _ := h.State("x"); st2 != graph.StateFailed {
		t.Fatalf("x state = %s, want failed (budget exceeded)", st2)
	}
	if sq := seqOf(t, st, "run-budget", "x", store.EventTransition, graph.StateFailed); sq == 0 {
		t.Fatal("failed transition missing after budget abort")
	}
}

func TestCrashRestartNoDuplicateAttempts(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	g := &graph.Graph{Nodes: []*graph.Node{
		agent("a"),
		agent("b", "a"),
		agent("c", "a"),
		agent("d", "b", "c"),
	}}
	ctx := context.Background()

	// Phase 1: run until a and b are done, c is mid-flight.
	drv1 := sched.NewFakeDriver(clk, scriptsFor("a", "b", "c", "d"))
	ver1 := sched.NewFakeVerifier(nil, sched.Verdict{Pass: true})
	s1 := newSched(t, st, drv1, ver1, clk, sched.Options{Concurrency: 4})
	h1, err := s1.Create(ctx, "run-crash", g)
	if err != nil {
		t.Fatal(err)
	}
	// Step 1: a starts. Step 2: a completes; b, c start (running).
	for i := 0; i < 2; i++ {
		clk.Advance(tick)
		if err := h1.Step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if stA, _ := h1.State("a"); stA != graph.StateDone {
		t.Fatalf("milestone wrong: a = %s", stA)
	}
	if stC, _ := h1.State("c"); stC != graph.StateRunning && stC != graph.StateVerifying {
		t.Fatalf("milestone wrong: c = %s", stC)
	}

	// "Crash": a fresh scheduler loads the same store.
	s2 := newSched(t, st, nil, nil, clk, sched.Options{Concurrency: 4})
	h2, err := s2.Load(ctx, "run-crash")
	if err != nil {
		t.Fatal(err)
	}
	// b and c were interrupted and must be restored to ready; a stays done.
	for _, id := range []string{"b", "c"} {
		if stC, _ := h2.State(graph.NodeID(id)); stC != graph.StateReady {
			t.Fatalf("after restart %s = %s, want ready", id, stC)
		}
	}
	if stA, _ := h2.State("a"); stA != graph.StateDone {
		t.Fatalf("after restart a = %s, want done", stA)
	}

	// Phase 2: continue to completion with a fresh driver.
	drv2 := sched.NewFakeDriver(clk, scriptsFor("b", "c", "d"))
	ver2 := sched.NewFakeVerifier(nil, sched.Verdict{Pass: true})
	s3 := newSched(t, st, drv2, ver2, clk, sched.Options{Concurrency: 4})
	h3, err := s3.Load(ctx, "run-crash")
	if err != nil {
		t.Fatal(err)
	}
	drive(t, h3, clk, 100)
	if !h3.Done() {
		t.Fatal("run did not complete after restart")
	}

	// No duplicate execution: a and d ran exactly once.
	for _, id := range []string{"a", "d"} {
		n, err := st.CountAttempts(ctx, "run-crash", id)
		if err != nil || n != 1 {
			t.Fatalf("node %s attempts = %d, want 1 (no duplicate after restart)", id, n)
		}
	}
	// b and c ran twice: one interrupted attempt + one resume.
	for _, id := range []string{"b", "c"} {
		n, _ := st.CountAttempts(ctx, "run-crash", id)
		if n != 2 {
			t.Fatalf("node %s attempts = %d, want 2 (interrupted + resume)", id, n)
		}
		atts, _ := st.Attempts(ctx, "run-crash", id)
		if atts[0].Status != "interrupted" {
			t.Fatalf("%s attempt 1 status = %s, want interrupted", id, atts[0].Status)
		}
		if atts[1].Status != "done" {
			t.Fatalf("%s attempt 2 status = %s, want done", id, atts[1].Status)
		}
	}

	// Event replay reproduces the materialized state.
	replayStates := replayStates(t, st, "run-crash")
	live, err := st.NodeStates(ctx, "run-crash")
	if err != nil {
		t.Fatal(err)
	}
	if len(replayStates) != len(live) {
		t.Fatalf("replay states %d != live %d", len(replayStates), len(live))
	}
	for id, st2 := range replayStates {
		if live[id] != st2 {
			t.Fatalf("replay mismatch %s: %s vs %s", id, st2, live[id])
		}
	}
}

func TestLoadRecoversVerifyingNode(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	ctx := context.Background()
	g := &graph.Graph{Nodes: []*graph.Node{agent("v")}}
	drv := sched.NewFakeDriver(clk, scriptsFor("v"))
	ver := sched.NewFakeVerifier(nil, sched.Verdict{Pass: true})
	s := newSched(t, st, drv, ver, clk, sched.Options{Concurrency: 1})
	h, err := s.Create(ctx, "run-verify-crash", g)
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(tick)
	if err := h.Step(ctx); err != nil { // v starts
		t.Fatal(err)
	}
	// Simulate a crash mid-verification by writing the transition manually.
	clk.Advance(tick)
	now := clk.Now()
	if _, err := st.AppendTransition(ctx, "run-verify-crash", "v", graph.StateRunning, graph.StateVerifying, "", now); err != nil {
		t.Fatal(err)
	}
	s2 := newSched(t, st, nil, nil, clk, sched.Options{Concurrency: 1})
	h2, err := s2.Load(ctx, "run-verify-crash")
	if err != nil {
		t.Fatal(err)
	}
	if stv, _ := h2.State("v"); stv != graph.StateReady {
		t.Fatalf("v after recovery = %s, want ready", stv)
	}
}

func statesOf(t *testing.T, st *store.Store, runID string) map[string]string {
	t.Helper()
	m, err := st.NodeStates(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for k, v := range m {
		out[string(k)] = string(v)
	}
	return out
}

func replayStates(t *testing.T, st *store.Store, runID string) map[graph.NodeID]graph.State {
	t.Helper()
	r, err := st.Run(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := graph.NewTracker(r.Graph)
	if err != nil {
		t.Fatal(err)
	}
	events, err := st.Events(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.NodeID == "" {
			continue
		}
		switch ev.Type {
		case store.EventTransition:
			if err := tr.Transit(graph.NodeID(ev.NodeID), ev.From, ev.To); err != nil {
				t.Fatalf("replay seq %d: %v", ev.Seq, err)
			}
		case store.EventRecovery:
			if err := tr.Set(graph.NodeID(ev.NodeID), ev.To); err != nil {
				t.Fatalf("recovery seq %d: %v", ev.Seq, err)
			}
		}
	}
	out := map[graph.NodeID]graph.State{}
	for _, n := range r.Graph.Nodes {
		st, _ := tr.State(n.ID)
		out[n.ID] = st
	}
	return out
}

func min2(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

var _ = adapter.StatusRunning
var _ = os.Getenv
