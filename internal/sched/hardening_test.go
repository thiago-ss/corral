package sched_test

import (
	"context"
	"testing"
	"time"

	"corral/internal/adapter"
	"corral/internal/graph"
	"corral/internal/sched"
	"corral/internal/verify"
)

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

var _ = sched.Verdict{}
