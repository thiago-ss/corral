package daemon_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"corral/internal/clock"
	"corral/internal/daemon"
	"corral/internal/graph"
	"corral/internal/livetest"
	"corral/internal/ocx"
	"corral/internal/ocxadapter"
	"corral/internal/sched"
	"corral/internal/spike"
	"corral/internal/store"
	"corral/internal/verify"
	"corral/internal/worktree"
)

// TestDaemonEndToEndRealOpenCode drives the acceptance flow entirely
// through the daemon API against a real OpenCode server: submit a graph
// (as if planned), start the run, follow execution, approve the gate, and
// observe the merge — no direct database or daemon internals.
func TestDaemonEndToEndRealOpenCode(t *testing.T) {
	livetest.SkipIfDisabled(t)
	if _, err := exec.LookPath("opencode"); err != nil {
		t.Skip("opencode binary not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	proj, err := os.MkdirTemp("", "corral-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(proj) })
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"commit", "-q", "--allow-empty", "-m", "init"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = proj
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git: %v: %s", err, out)
		}
	}
	srv, err := spike.StartServer(ctx, proj, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)

	st, err := store.Open(filepath.Join(t.TempDir(), "e2e.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	oc := ocx.New(srv.Base, proj)
	drv := ocxadapter.New(oc, ocxadapter.Options{PollInterval: 400 * time.Millisecond})
	t.Cleanup(func() { drv.Close() })
	wtm := worktree.NewManager(proj)
	s := sched.New(st, drv, &sched.EngineVerifier{Eng: verify.New(proj)}, clock.Real{}, sched.Options{
		Concurrency: 2, Worktrees: wtm,
	})
	d := daemon.New(st, s, nil, proj, "")
	srvHTTP := httptest.NewServer(d.Handler())
	t.Cleanup(srvHTTP.Close)
	api := &api{t: t, cli: srvHTTP.Client(), base: srvHTTP.URL}

	w1 := ocNodeE2E("w1", "Create a file named alpha.txt with some content. Do not run any other commands.", "alpha.txt", "")
	gate := &graph.Node{ID: "gate", Type: graph.NodeHuman, Objective: "approve", Priority: graph.PriorityNormal, DependsOn: []graph.NodeID{"w1"}}
	merge := &graph.Node{
		ID: "m", Type: graph.NodeMerge, Objective: "merge", Priority: graph.PriorityNormal,
		DependsOn:    []graph.NodeID{"gate"},
		Verification: &graph.Verification{Kind: "command", Command: []string{"test", "-s", "alpha.txt"}},
		RetryPolicy:  graph.RetryPolicy{MaxRetries: 1, Backoff: 5 * time.Second},
	}
	g := &graph.Graph{Nodes: []*graph.Node{w1, gate, merge}}

	// Submit the graph through the API (as the orchestrator would).
	code, body := api.do("orchestrator", "POST", "/api/runs", map[string]any{"graph": g})
	if code != 201 {
		t.Fatalf("create: %d %s", code, body)
	}
	var created struct{ RunID string }
	json.Unmarshal([]byte(body), &created)

	// Follow execution through the API.
	api.waitState(t, "", created.RunID, "gate", graph.StateRunning, 5*time.Minute)

	// The main checkout must be clean while the worker is isolated.
	out, _ := exec.Command("git", "-C", proj, "status", "--porcelain").CombinedOutput()
	if strings.TrimSpace(string(out)) != "" {
		t.Fatalf("main checkout dirty while isolated: %q", out)
	}

	code, body = api.do("operator", "POST", "/api/runs/"+created.RunID+"/approve", map[string]any{"nodeID": "gate"})
	if code != 200 {
		t.Fatalf("approve: %d %s", code, body)
	}
	api.waitState(t, "", created.RunID, "m", graph.StateDone, 5*time.Minute)

	data, err := os.ReadFile(filepath.Join(proj, "alpha.txt"))
	if err != nil || len(strings.TrimSpace(string(data))) == 0 {
		t.Fatalf("merged content wrong: %v %q", err, data)
	}
	code, body = api.do("operator", "GET", "/api/runs/"+created.RunID, nil)
	if code != 200 || !strings.Contains(body, `"done":true`) {
		t.Fatalf("run not done: %d %s", code, body)
	}
}

// TestPlannerSmoke exercises the live planner agent against a trivial
// goal. Model output is nondeterministic, so a failure to produce a valid
// graph is reported but does not fail the suite; a success proves the
// planner wiring end-to-end.
func TestPlannerSmoke(t *testing.T) {
	livetest.SkipIfDisabled(t)
	if _, err := exec.LookPath("opencode"); err != nil {
		t.Skip("opencode binary not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	proj, err := os.MkdirTemp("", "corral-plan-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(proj) })
	srv, err := spike.StartServer(ctx, proj, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)

	planner := daemon.NewOpenCodePlanner(ocx.New(srv.Base, proj), "", 4*time.Minute)
	g, err := planner.Plan(ctx, "Create a file report.txt containing 'hello'.")
	if err != nil {
		t.Logf("planner smoke: %v (model did not produce a valid graph)", err)
		return
	}
	t.Logf("planner produced graph with %d nodes", len(g.Nodes))
	for _, n := range g.Nodes {
		t.Logf("  %s (%s) -> %v", n.ID, n.Type, n.DependsOn)
	}
	if err := graph.Validate(g); err != nil {
		t.Errorf("planner graph invalid: %v", err)
	}
}

func ocNodeE2E(id graph.NodeID, prompt, file, marker string) *graph.Node {
	cmd := []string{"test", "-s", file}
	if marker != "" {
		cmd = []string{"grep", "-q", marker, file}
	}
	return &graph.Node{
		ID: id, Type: graph.NodeAgent, Role: "worker",
		Objective:          prompt,
		AcceptanceCriteria: []string{file + " produced"},
		Priority:           graph.PriorityNormal,
		WriteScope:         []string{file},
		Verification:       &graph.Verification{Kind: "command", Command: cmd},
		RetryPolicy:        graph.RetryPolicy{MaxRetries: 1, Backoff: 5 * time.Second},
		Budget:             graph.Budget{MaxDuration: 12 * time.Minute},
	}
}
