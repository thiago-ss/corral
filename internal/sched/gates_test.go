package sched_test

import (
	"context"
	"testing"
	"time"

	"corral/internal/adapter"
	"corral/internal/clock"
	"corral/internal/graph"
	"corral/internal/sched"
	"corral/internal/verify"
)

// verifierFor builds the production evidence verifier (verify.Engine).
func verifierFor(t *testing.T, eng *verify.Engine) *sched.EngineVerifier {
	t.Helper()
	return &sched.EngineVerifier{Eng: eng}
}

// gateNode is an agent node with an explicit command gate. The runner is
// scripted so attempt 1 fails the gate and attempt 2 passes it.
func TestFailThenPassAfterRetryWithFeedback(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	worktree := t.TempDir()
	runner := &scriptedRunner{results: []runResult{
		{exit: 1, stderr: "assertion failed on line 7"},
		{exit: 0},
	}}
	eng := verify.New(worktree)
	eng.Runner = runner

	n := agent("w")
	n.Verification = &graph.Verification{Kind: "command", Command: []string{"test", "-f", "out.txt"}}
	n.RetryPolicy = graph.RetryPolicy{MaxRetries: 2, Backoff: 2 * tick}

	drv := sched.NewFakeDriver(clk, scriptsFor("w"))
	s := newSched(t, st, drv, verifierFor(t, eng), clk, sched.Options{Concurrency: 1})
	h, err := s.Create(context.Background(), "run-gate", &graph.Graph{Nodes: []*graph.Node{n}})
	if err != nil {
		t.Fatal(err)
	}
	drive(t, h, clk, 100)
	if st2, _ := h.State("w"); st2 != graph.StateDone {
		t.Fatalf("w state = %s, want done after one retry", st2)
	}
	atts, _ := st.Attempts(context.Background(), "run-gate", "w")
	if len(atts) != 2 {
		t.Fatalf("attempts = %d, want 2 (fail then pass)", len(atts))
	}
	if atts[0].Status != "failed" || atts[1].Status != "done" {
		t.Fatalf("attempt statuses = %s/%s, want failed/done", atts[0].Status, atts[1].Status)
	}
	// Focused feedback from gate 1 must reach attempt 2.
	fb := drv.Feedback["w"]
	if len(fb) != 1 || fb[0] != "assertion failed on line 7" {
		t.Fatalf("attempt 2 feedback = %q, want focused gate feedback", fb)
	}
	// Evidence recorded on the attempt row.
	if atts[1].Evidence == "" {
		t.Fatal("passing attempt has no recorded evidence")
	}
}

func TestPermanentFailureBlocksDownstream(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	runner := &scriptedRunner{results: []runResult{{exit: 1, stderr: "always broken"}}}
	eng := verify.New(t.TempDir())
	eng.Runner = runner

	a := agent("a")
	a.Verification = &graph.Verification{Kind: "command", Command: []string{"false"}}
	a.RetryPolicy = graph.RetryPolicy{MaxRetries: 1, Backoff: tick}
	b := agent("b", "a")

	drv := sched.NewFakeDriver(clk, scriptsFor("a", "b"))
	s := newSched(t, st, drv, verifierFor(t, eng), clk, sched.Options{Concurrency: 2})
	h, err := s.Create(context.Background(), "run-block", &graph.Graph{Nodes: []*graph.Node{a, b}})
	if err != nil {
		t.Fatal(err)
	}
	drive(t, h, clk, 100)
	if !h.Done() {
		t.Fatal("run with a blocked node must settle (waiting, not hung)")
	}
	if st2, _ := h.State("a"); st2 != graph.StateFailed {
		t.Fatalf("a state = %s, want failed", st2)
	}
	if st2, _ := h.State("b"); st2 != graph.StateBlocked {
		t.Fatalf("b state = %s, want blocked (downstream of permanent failure)", st2)
	}
	// b must never have run.
	n, _ := st.CountAttempts(context.Background(), "run-block", "b")
	if n != 0 {
		t.Fatalf("b attempts = %d, want 0", n)
	}
	r, err := st.Run(context.Background(), "run-block")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "waiting" {
		t.Fatalf("run status = %s, want waiting (blocked node)", r.Status)
	}
}

func TestTokenBudgetBoundsRetries(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	eng := verify.New(t.TempDir())
	eng.Runner = &scriptedRunner{results: []runResult{{exit: 1, stderr: "nope"}}}
	n := agent("w")
	n.Verification = &graph.Verification{Kind: "command", Command: []string{"false"}}
	n.RetryPolicy = graph.RetryPolicy{MaxRetries: 5, Backoff: tick}
	n.Budget = graph.Budget{MaxTokens: 1} // every attempt burns >= 1 token

	drv := sched.NewFakeDriver(clk, scriptsFor("w"))
	// Scripts carry token usage per attempt.
	drv.SetScript("w", sched.Script{Delay: tick, Messages: []adapter.Message{{Role: "assistant", Finish: "stop", Text: "done", Tokens: 1}}})
	s := newSched(t, st, drv, verifierFor(t, eng), clk, sched.Options{Concurrency: 1})
	h, err := s.Create(context.Background(), "run-tokenbudget", &graph.Graph{Nodes: []*graph.Node{n}})
	if err != nil {
		t.Fatal(err)
	}
	drive(t, h, clk, 100)
	if st2, _ := h.State("w"); st2 != graph.StateFailed {
		t.Fatalf("w state = %s, want failed (token budget exhausted)", st2)
	}
	// First attempt consumed the entire token budget: no retry allowed.
	atts, _ := st.Attempts(context.Background(), "run-tokenbudget", "w")
	if len(atts) != 1 {
		t.Fatalf("attempts = %d, want 1 (budget must bound retries)", len(atts))
	}
}

func TestCheckNodeRunsCommandAndRetries(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	runner := &scriptedRunner{results: []runResult{
		{exit: 1, stderr: "lint error in file.go"},
		{exit: 0},
	}}
	eng := verify.New(t.TempDir())
	eng.Runner = runner

	n := &graph.Node{
		ID:           "lint",
		Type:         graph.NodeCheck,
		Objective:    "run linter",
		Priority:     graph.PriorityNormal,
		Verification: &graph.Verification{Kind: "command", Command: []string{"golint"}},
		RetryPolicy:  graph.RetryPolicy{MaxRetries: 2, Backoff: tick},
	}

	// Check nodes must not spawn agent sessions: an empty driver is fine.
	drv := sched.NewFakeDriver(clk, nil)
	s := newSched(t, st, drv, verifierFor(t, eng), clk, sched.Options{Concurrency: 1, CheckRunner: runner})
	h, err := s.Create(context.Background(), "run-check", &graph.Graph{Nodes: []*graph.Node{n}})
	if err != nil {
		t.Fatal(err)
	}
	drive(t, h, clk, 100)
	if st2, _ := h.State("lint"); st2 != graph.StateDone {
		t.Fatalf("lint state = %s, want done after one retry", st2)
	}
	if runner.calls != 2 {
		t.Fatalf("runner calls = %d, want 2 (fail then pass)", runner.calls)
	}
	atts, _ := st.Attempts(context.Background(), "run-check", "lint")
	if len(atts) != 2 || atts[1].Status != "done" {
		t.Fatalf("check attempts wrong: %d, last=%s", len(atts), atts[len(atts)-1].Status)
	}
}

// A check node whose command cannot even be started (missing binary) must
// fail its evidence gate and let the run settle — never pass with a
// phantom exit 0.
func TestCheckNodeFailsGateOnMissingBinary(t *testing.T) {
	st := newStore(t)
	clk := fakeClock()
	n := &graph.Node{
		ID:           "lint",
		Type:         graph.NodeCheck,
		Objective:    "run linter",
		Priority:     graph.PriorityNormal,
		Verification: &graph.Verification{Kind: "command", Command: []string{"definitely-not-a-real-binary-xyz"}},
		RetryPolicy:  graph.RetryPolicy{MaxRetries: 0, Backoff: tick},
	}
	// CheckRunner left nil so startCheck falls back to verify.ExecRunner,
	// which reports the real start error.
	drv := sched.NewFakeDriver(clk, nil)
	s := newSched(t, st, drv, verifierFor(t, verify.New(t.TempDir())), clk, sched.Options{Concurrency: 1})
	h, err := s.Create(context.Background(), "run-check-missing", &graph.Graph{Nodes: []*graph.Node{n}})
	if err != nil {
		t.Fatal(err)
	}
	drive(t, h, clk, 100)
	if !h.Done() {
		t.Fatal("run must settle, not error, when a check command cannot start")
	}
	if st2, _ := h.State("lint"); st2 != graph.StateFailed {
		t.Fatalf("lint state = %s, want failed (missing binary must fail the gate)", st2)
	}
	atts, _ := st.Attempts(context.Background(), "run-check-missing", "lint")
	if len(atts) != 1 || atts[0].Status != "failed" {
		t.Fatalf("check attempts wrong: %+v", atts)
	}
}

// --- helpers ---------------------------------------------------------

type runResult struct {
	exit   int
	stdout string
	stderr string
}

type scriptedRunner struct {
	results []runResult
	calls   int
}

func (r *scriptedRunner) Run(_ context.Context, _ string, _ []string, _ time.Duration) (int, string, string, error) {
	idx := r.calls
	if idx >= len(r.results) {
		idx = len(r.results) - 1
	}
	r.calls++
	return r.results[idx].exit, r.results[idx].stdout, r.results[idx].stderr, nil
}

var _ = clock.NewFake
