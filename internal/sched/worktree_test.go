package sched_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"corral/internal/clock"
	"corral/internal/graph"
	"corral/internal/sched"
	"corral/internal/store"
	"corral/internal/verify"
	"corral/internal/worktree"
)

func gitRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, args := range [][]string{
		{"init", "-q", "-b", "main"},
		{"-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	return dir
}

func gitStatus(t *testing.T, dir string) string {
	t.Helper()
	out, err := exec.Command("git", "-C", dir, "status", "--porcelain").CombinedOutput()
	if err != nil {
		t.Fatalf("git status: %v", err)
	}
	return string(out)
}

// workerNode is a writing agent that produces one file, verified by a
// command gate that runs inside its own worktree.
func workerNode(id graph.NodeID, file, content string, scope ...string) *graph.Node {
	ws := scope
	if len(ws) == 0 {
		ws = []string{file}
	}
	return &graph.Node{
		ID:                 id,
		Type:               graph.NodeAgent,
		Role:               "worker",
		Objective:          "produce " + file,
		AcceptanceCriteria: []string{file + " produced"},
		Priority:           graph.PriorityNormal,
		WriteScope:         ws,
		Verification:       &graph.Verification{Kind: "command", Command: []string{"test", "-f", file}},
		RetryPolicy:        graph.RetryPolicy{MaxRetries: 2, Backoff: tick},
	}
}

func checkNode(id graph.NodeID, file string, deps ...graph.NodeID) *graph.Node {
	return &graph.Node{
		ID:           id,
		Type:         graph.NodeCheck,
		Objective:    "verify " + file,
		Priority:     graph.PriorityNormal,
		DependsOn:    deps,
		Verification: &graph.Verification{Kind: "command", Command: []string{"test", "-f", file}},
		RetryPolicy:  graph.RetryPolicy{MaxRetries: 2, Backoff: tick},
	}
}

func gateNode(id graph.NodeID, deps ...graph.NodeID) *graph.Node {
	return &graph.Node{
		ID:        id,
		Type:      graph.NodeHuman,
		Objective: "approve the merged work",
		Priority:  graph.PriorityNormal,
		DependsOn: deps,
	}
}

func mergeNode(id graph.NodeID, verifyCmd []string, deps ...graph.NodeID) *graph.Node {
	return &graph.Node{
		ID:           id,
		Type:         graph.NodeMerge,
		Objective:    "merge accepted work into main",
		Priority:     graph.PriorityNormal,
		DependsOn:    deps,
		Verification: &graph.Verification{Kind: "command", Command: verifyCmd},
		RetryPolicy:  graph.RetryPolicy{MaxRetries: 1, Backoff: tick},
	}
}

func setupIsolated(t *testing.T, g *graph.Graph, scripts map[string][]sched.Script) (*store.Store, *sched.RunHandle, *worktree.Manager, *clock.Fake) {
	st := newStore(t)
	clk := fakeClock()
	repo := gitRepo(t)
	wtm := worktree.NewManager(repo)
	drv := sched.NewFakeDriver(clk, scripts)
	eng := verify.New(repo)
	eng.Runner = verify.ExecRunner{}
	s := newSched(t, st, drv, &sched.EngineVerifier{Eng: eng}, clk, sched.Options{
		Concurrency: 4, Worktrees: wtm,
	})
	h, err := s.Create(context.Background(), "run-wt", g)
	if err != nil {
		t.Fatal(err)
	}
	return st, h, wtm, clk
}

func step(h *sched.RunHandle, clk *clock.Fake, ctx context.Context) error {
	clk.Advance(tick)
	return h.Step(ctx)
}

func attemptsOf(t *testing.T, st *store.Store, runID, node string) []store.Attempt {
	t.Helper()
	atts, err := st.Attempts(context.Background(), runID, node)
	if err != nil {
		t.Fatal(err)
	}
	return atts
}

func transitionType() store.EventType { return store.EventTransition }

func TestWorkersIsolatedAndMainUntouched(t *testing.T) {
	g := &graph.Graph{Nodes: []*graph.Node{
		workerNode("w1", "a.txt", "A1"),
		workerNode("w2", "b.txt", "B2"),
	}}
	scripts := map[string][]sched.Script{
		"w1": {{Delay: 2 * tick, Write: map[string]string{"a.txt": "A1"}}},
		"w2": {{Delay: 2 * tick, Write: map[string]string{"b.txt": "B2"}}},
	}
	st, h, wtm, clk := setupIsolated(t, g, scripts)
	ctx := context.Background()
	for i := 0; i < 50 && !h.Done(); i++ {
		if err := step(h, clk, ctx); err != nil {
			t.Fatal(err)
		}
	}
	_ = st
	if !h.Done() {
		t.Fatal("run did not complete")
	}
	// Main checkout untouched by parallel workers.
	if s2 := gitStatus(t, wtm.Repo()); s2 != "" {
		t.Fatalf("main checkout dirty: %q", s2)
	}
	// Each worker's file lives in its own worktree.
	repo := wtm.Repo()
	_ = repo
	for _, id := range []string{"w1", "w2"} {
		atts := attemptsOf(t, st, "run-wt", id)
		if len(atts) != 1 || atts[0].Worktree == "" {
			t.Fatalf("%s attempts wrong: %+v", id, atts)
		}
		file := "a.txt"
		if id == "w2" {
			file = "b.txt"
		}
		data, err := os.ReadFile(filepath.Join(atts[0].Worktree, file))
		if err != nil {
			t.Fatalf("%s file missing in worktree: %v", id, err)
		}
		if string(data) == "" {
			t.Fatalf("%s worktree file empty", id)
		}
	}
}

func TestScopeCollisionDefersConcurrentWriter(t *testing.T) {
	g := &graph.Graph{Nodes: []*graph.Node{
		workerNode("w1", "a.txt", "A1", "a.txt"),
		workerNode("w2", "a.txt", "A2", "a.txt"),
	}}
	scripts := map[string][]sched.Script{
		"w1": {{Delay: 3 * tick, Write: map[string]string{"a.txt": "A1"}}},
		"w2": {{Delay: 1 * tick, Write: map[string]string{"a.txt": "A2"}}},
	}
	st, h, _, clk := setupIsolated(t, g, scripts)
	ctx := context.Background()
	for i := 0; i < 50 && !h.Done(); i++ {
		if err := step(h, clk, ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !h.Done() {
		t.Fatal("run did not complete")
	}
	// w2 must start only after w1 finished (declared scope collision).
	w1done := seqOf(t, st, "run-wt", "w1", store.EventTransition, graph.StateDone)
	w2leased := seqOf(t, st, "run-wt", "w2", store.EventTransition, graph.StateLeased)
	if w1done == 0 || w2leased == 0 || w2leased < w1done {
		t.Fatalf("collision not enforced: w1 done=%d, w2 leased=%d", w1done, w2leased)
	}
}

func TestFailedWorktreeRetainedForInspection(t *testing.T) {
	bad := workerNode("bad", "x.txt", "X", "x.txt")
	bad.Verification = &graph.Verification{Kind: "command", Command: []string{"test", "-f", "missing.txt"}}
	bad.RetryPolicy = graph.RetryPolicy{MaxRetries: 1, Backoff: tick}
	g := &graph.Graph{Nodes: []*graph.Node{bad}}
	scripts := map[string][]sched.Script{
		"bad": {
			{Delay: tick, Write: map[string]string{"x.txt": "X"}},
			{Delay: tick, Write: map[string]string{"x.txt": "X"}},
		},
	}
	st, h, _, clk := setupIsolated(t, g, scripts)
	ctx := context.Background()
	for i := 0; i < 50 && !h.Done(); i++ {
		if err := step(h, clk, ctx); err != nil {
			t.Fatal(err)
		}
	}
	if st2, _ := h.State("bad"); st2 != graph.StateFailed {
		t.Fatalf("bad state = %s, want failed", st2)
	}
	atts := attemptsOf(t, st, "run-wt", "bad")
	if len(atts) == 0 || atts[len(atts)-1].Worktree == "" {
		t.Fatalf("no worktree recorded for failed node")
	}
	if _, err := os.Stat(atts[len(atts)-1].Worktree); err != nil {
		t.Fatalf("failed worktree not retained: %v", err)
	}
	// Its content must still be inspectable.
	if _, err := os.Stat(filepath.Join(atts[len(atts)-1].Worktree, "x.txt")); err != nil {
		t.Fatalf("failed worktree content gone: %v", err)
	}
}

func TestOutOfScopeWorktreeChangesCannotPass(t *testing.T) {
	n := workerNode("bad-scope", "allowed.txt", "ok", "allowed.txt")
	n.RetryPolicy.MaxRetries = 0
	st, h, _, clk := setupIsolated(t, &graph.Graph{Nodes: []*graph.Node{n}}, map[string][]sched.Script{
		"bad-scope": {{Delay: tick, Write: map[string]string{"outside.txt": "escape"}}},
	})
	ctx := context.Background()
	for i := 0; i < 50 && !h.Done(); i++ {
		if err := step(h, clk, ctx); err != nil {
			t.Fatal(err)
		}
	}
	if state, _ := h.State("bad-scope"); state != graph.StateFailed {
		t.Fatalf("out-of-scope state = %s, want failed", state)
	}
	atts := attemptsOf(t, st, "run-wt", "bad-scope")
	if len(atts) == 0 || !strings.Contains(atts[len(atts)-1].Evidence, "outside write scope") {
		t.Fatalf("scope failure evidence = %+v", atts)
	}
}

func TestMergeRunsOnlyAfterApproval(t *testing.T) {
	g := &graph.Graph{Nodes: []*graph.Node{
		workerNode("w1", "a.txt", "A1", "a.txt"),
		checkNode("c1", "a.txt", "w1"),
		gateNode("gate", "c1"),
		mergeNode("m", []string{"test", "-f", "a.txt"}, "gate"),
	}}
	scripts := map[string][]sched.Script{
		"w1": {{Delay: tick, Write: map[string]string{"a.txt": "A1"}}},
	}
	st, h, wtm, clk := setupIsolated(t, g, scripts)
	ctx := context.Background()

	// Drive until the gate is running; the merge must not have started.
	for i := 0; i < 50; i++ {
		if err := step(h, clk, ctx); err != nil {
			t.Fatal(err)
		}
		if st, _ := h.State("gate"); st == graph.StateRunning {
			break
		}
	}
	if st, _ := h.State("gate"); st != graph.StateRunning {
		t.Fatalf("gate never reached running: %v", st)
	}
	// Merge must not run before approval.
	if n, _ := st.CountAttempts(ctx, "run-wt", "m"); n != 0 {
		t.Fatalf("merge ran before approval: %d attempts", n)
	}
	// A few more steps must not change that.
	for i := 0; i < 5; i++ {
		if err := step(h, clk, ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n2, _ := st.CountAttempts(ctx, "run-wt", "m"); n2 != 0 {
		t.Fatalf("merge ran without approval: %d attempts", n2)
	}

	// Approve: the merge runs and folds the branch into main.
	if err := h.ApproveNode(ctx, "gate"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50 && !h.Done(); i++ {
		if err := step(h, clk, ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !h.Done() {
		t.Fatalf("run did not complete after approval; states=%v", statesOf(t, st, "run-wt"))
	}
	if st, _ := h.State("m"); st != graph.StateDone {
		t.Fatalf("merge state = %s, want done", st)
	}
	if st, _ := h.State("gate"); st != graph.StateDone {
		t.Fatalf("gate state = %s, want done", st)
	}
	// Merged content present in the main checkout.
	atts := attemptsOf(t, st, "run-wt", "w1")
	data, err := os.ReadFile(filepath.Join(wtm.Repo(), "a.txt"))
	if err != nil {
		t.Fatalf("merged file missing: %v", err)
	}
	if string(data) != "A1" {
		t.Fatalf("merged content = %q, want A1", data)
	}
	// Consumed worktree pruned.
	if len(atts) == 0 {
		t.Fatal("no w1 attempt")
	}
	if _, err := os.Stat(atts[0].Worktree); !os.IsNotExist(err) {
		t.Fatal("merged worktree not pruned")
	}
}

// A merge whose post-merge verification command cannot be started (missing
// binary) must fail the merge gate and let the run settle.
func TestMergeFailsOnMissingVerificationBinary(t *testing.T) {
	g := &graph.Graph{Nodes: []*graph.Node{
		workerNode("w1", "a.txt", "A1", "a.txt"),
		mergeNode("m", []string{"definitely-not-a-real-binary-xyz"}, "w1"),
	}}
	scripts := map[string][]sched.Script{
		"w1": {{Delay: tick, Write: map[string]string{"a.txt": "A1"}}},
	}
	st, h, _, clk := setupIsolated(t, g, scripts)
	ctx := context.Background()
	for i := 0; i < 50 && !h.Done(); i++ {
		if err := step(h, clk, ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !h.Done() {
		t.Fatal("run must settle, not error, when the merge verification cannot start")
	}
	if st2, _ := h.State("m"); st2 != graph.StateFailed {
		t.Fatalf("merge state = %s, want failed (missing verification binary)", st2)
	}
	atts := attemptsOf(t, st, "run-wt", "m")
	if len(atts) == 0 || atts[len(atts)-1].Status != "failed" {
		t.Fatalf("merge attempts wrong: %+v", atts)
	}
}

func TestRejectedGateBlocksMerge(t *testing.T) {
	g := &graph.Graph{Nodes: []*graph.Node{
		workerNode("w1", "a.txt", "A1", "a.txt"),
		gateNode("gate", "w1"),
		mergeNode("m", []string{"true"}, "gate"),
	}}
	scripts := map[string][]sched.Script{
		"w1": {{Delay: tick, Write: map[string]string{"a.txt": "A1"}}},
	}
	st, h, _, clk := setupIsolated(t, g, scripts)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		if err := step(h, clk, ctx); err != nil {
			t.Fatal(err)
		}
		if st, _ := h.State("gate"); st == graph.StateRunning {
			break
		}
	}
	if err := h.RejectNode(ctx, "gate"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50 && !h.Done(); i++ {
		if err := step(h, clk, ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !h.Done() {
		t.Fatal("run should settle waiting after rejection")
	}
	if st, _ := h.State("gate"); st != graph.StateBlocked {
		t.Fatalf("gate = %s, want blocked", st)
	}
	if st, _ := h.State("m"); st != graph.StateBlocked {
		t.Fatalf("merge = %s, want blocked", st)
	}
	if n, _ := st.CountAttempts(ctx, "run-wt", "m"); n != 0 {
		t.Fatalf("merge ran after rejection: %d", n)
	}
}

func TestDiffArtifactContentAddressed(t *testing.T) {
	g := &graph.Graph{Nodes: []*graph.Node{workerNode("w1", "a.txt", "A1", "a.txt")}}
	scripts := map[string][]sched.Script{
		"w1": {{Delay: tick, Write: map[string]string{"a.txt": "A1"}}},
	}
	st, h, _, clk := setupIsolated(t, g, scripts)
	ctx := context.Background()
	for i := 0; i < 50 && !h.Done(); i++ {
		if err := step(h, clk, ctx); err != nil {
			t.Fatal(err)
		}
	}
	atts := attemptsOf(t, st, "run-wt", "w1")
	arts, err := st.Artifacts(ctx, "run-wt", atts[0].ID)
	if err != nil || len(arts) != 1 {
		t.Fatalf("diff artifact missing: %v %v", err, arts)
	}
	if arts[0].Name != "diff" || arts[0].Hash == "" || arts[0].Content == "" {
		t.Fatalf("artifact wrong: %+v", arts[0])
	}
	if !strings.Contains(arts[0].Content, "+A1") {
		t.Fatalf("artifact patch missing content: %q", arts[0].Content)
	}
	if worktree.HashContent(arts[0].Content) != arts[0].Hash {
		t.Fatal("artifact hash not content-addressed")
	}
}
