package daemon_test

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
	"testing"
	"time"

	"corral/internal/assets"
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
	srv, err := spike.StartServer(ctx, proj, 0, os.Stderr)
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
	srv, err := spike.StartServer(ctx, proj, 0, os.Stderr)
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

// orchGraph builds the small approved graph the orchestrator drives in the
// run-loop E2E: a writing worker, a human gate, and an approval-gated merge.
func orchGraph() *graph.Graph {
	return &graph.Graph{Version: 1, Nodes: []*graph.Node{
		ocNodeE2E("w1", "Create a file named alpha.txt containing one line: CORRAL. Do not run any other commands.", "alpha.txt", ""),
		{ID: "gate", Type: graph.NodeHuman, Objective: "approve", Priority: graph.PriorityNormal, DependsOn: []graph.NodeID{"w1"}},
		{ID: "m", Type: graph.NodeMerge, Objective: "merge accepted work", Priority: graph.PriorityNormal,
			DependsOn:    []graph.NodeID{"gate"},
			Verification: &graph.Verification{Kind: "command", Command: []string{"test", "-s", "alpha.txt"}}},
	}}
}

// orchEnv is the full real-OpenCode stack for the orchestrator run-loop
// test: a git project with the corral plugin + agents installed, an
// embedded opencode serve, the daemon, and its HTTP API.
type orchEnv struct {
	api  *api
	oc   *ocx.Client
	proj string
	srv  *spike.Server
}

func setupOrchestratorEnv(t *testing.T, ctx context.Context) *orchEnv {
	t.Helper()
	proj, err := os.MkdirTemp("", "corral-orch-e2e-")
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
	// Install the corral plugin + agent config so the real OpenCode server
	// exposes the corral_* tools and the corral-orchestrator agent.
	if err := os.MkdirAll(filepath.Join(proj, ".opencode", "tools"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proj, ".opencode", "tools", "corral.ts"), []byte(assets.CorralPluginTS), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proj, "opencode.json"), []byte(assets.OpenCodeConfigJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	srv, err := spike.StartServer(ctx, proj, 0, os.Stderr)
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
	t.Setenv("CORRAL_DAEMON_URL", srvHTTP.URL)

	return &orchEnv{api: &api{t: t, cli: srvHTTP.Client(), base: srvHTTP.URL}, oc: oc, proj: proj, srv: srv}
}

// orchTools is the tool allowlist for the orchestrator session: only the
// corral_* control tools, so the agent can never wander into edits/bash.
var orchTools = map[string]bool{
	"corral_plan": true, "corral_start": true, "corral_status": true,
	"corral_watch": true, "corral_approve": true, "corral_reject": true,
	"corral_cancel": true, "corral_retry": true, "corral_steer": true,
}

func orchestratorPrompt(autoApprove bool) string {
	gj, _ := json.Marshal(orchGraph())
	return fmt.Sprintf(`You are driving a corral run as the orchestrator.

First call corral_start with the EXACT graph JSON below and autoApproveGates set to %t to create the run:

%s

Then drive it to completion:
1. Call corral_watch with the runID from corral_start, passing a timeout around 60 and the previous response's "since" value on later calls.
2. Each time corral_watch returns, report the current progress to the user (nodes done or running, and any milestones).
3. When gatesAwaitingApproval is non-empty and autoApproveGates is true, you are pre-authorized: call corral_approve with the runID and each waiting gate nodeID, then keep watching.
4. When gatesAwaitingApproval is non-empty and autoApproveGates is false, you are NOT pre-authorized: never call corral_approve. Report to the user that the gate awaits their approval, and keep calling corral_watch until it resolves, then continue.
5. Keep watching until the response shows done: true, then report the final outcome.

You are the corral-orchestrator agent: never edit files, never run bash, and use only the corral_* tools.`, autoApprove, gj)
}

// waitForRun polls the daemon API until at least one run exists and
// returns its id (the orchestrator creates it via corral_start).
func waitForRun(t *testing.T, env *orchEnv, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		code, body := env.api.do("operator", http.MethodGet, "/api/runs", nil)
		if code == http.StatusOK {
			var runs []struct {
				ID string `json:"id"`
			}
			if json.Unmarshal([]byte(body), &runs) == nil && len(runs) > 0 {
				return runs[0].ID
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("no run appeared; did the orchestrator call corral_start?")
	return ""
}

// runDetail returns the daemon's run detail as raw JSON.
func (env *orchEnv) runDetail(runID string) (int, string) {
	return env.api.do("operator", http.MethodGet, "/api/runs/"+runID, nil)
}

func (env *orchEnv) nodeState(runID, nodeID string) string {
	code, body := env.runDetail(runID)
	if code != http.StatusOK {
		return ""
	}
	var r struct {
		States map[string]string `json:"states"`
		Done   bool              `json:"done"`
	}
	if json.Unmarshal([]byte(body), &r) != nil {
		return ""
	}
	return r.States[nodeID]
}

func (env *orchEnv) waitNodeState(runID, nodeID, want string, timeout time.Duration) {
	t := env.api.t
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if env.nodeState(runID, nodeID) == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("node %s never reached %s", nodeID, want)
}

func (env *orchEnv) waitRunDone(runID string, timeout time.Duration) {
	t := env.api.t
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		code, body := env.runDetail(runID)
		if code == http.StatusOK {
			var r struct {
				Done bool `json:"done"`
			}
			if json.Unmarshal([]byte(body), &r) == nil && r.Done {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	code, body := env.runDetail(runID)
	t.Fatalf("run %s never completed (last: %d %s)", runID, code, body)
}

// sessionTools returns the ordered completed tool calls of a session and
// the assistant's text output, used to prove the orchestrator drove the
// loop (start/watch/approve) the way the policy dictates.
func sessionTools(t *testing.T, oc *ocx.Client, sid string) (tools []string, text string) {
	t.Helper()
	msgs, err := oc.Messages(context.Background(), sid, 0)
	if err != nil {
		return nil, ""
	}
	var out strings.Builder
	for _, m := range msgs {
		if m.Info.Role != "assistant" {
			continue
		}
		for _, p := range m.Parts {
			var part struct {
				Type  string `json:"type"`
				Text  string `json:"text"`
				Tool  string `json:"tool"`
				State string `json:"state"`
			}
			if json.Unmarshal(p, &part) != nil {
				continue
			}
			switch part.Type {
			case "text":
				out.WriteString(part.Text)
				out.WriteString("\n")
			case "tool":
				if part.State == "completed" {
					tools = append(tools, part.Tool)
				}
			}
		}
	}
	return tools, out.String()
}

// TestOrchestratorRunLoopRealOpenCode exercises the corral-orchestrator
// agent prompt end to end against a real OpenCode install. The
// orchestrator session starts a run with corral_start and drives the watch
// loop, and the two subtests cover the approval policy:
//   - pre-authorized (autoApproveGates=true): the orchestrator approves the
//     gate itself and the run completes with no human action;
//   - not pre-authorized: the run parks at the gate, the orchestrator
//     reports it and never approves, and the run resumes only after an
//     external (operator) API approve.
func TestOrchestratorRunLoopRealOpenCode(t *testing.T) {
	livetest.SkipIfDisabled(t)
	if _, err := exec.LookPath("opencode"); err != nil {
		t.Skip("opencode binary not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	for _, tc := range []struct {
		name        string
		autoApprove bool
	}{
		{"pre-authorized: run completes without any human", true},
		{"not pre-authorized: parks at the gate, resumes after external approve", false},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			env := setupOrchestratorEnv(t, ctx)
			sess, err := env.oc.CreateSession(ctx, "corral/orchestrator")
			if err != nil {
				t.Fatalf("create orchestrator session: %v", err)
			}
			if err := env.oc.PromptAsyncAgentWithTools(ctx, sess.ID, orchestratorPrompt(tc.autoApprove), "", "corral-orchestrator", orchTools); err != nil {
				t.Fatalf("prompt orchestrator: %v", err)
			}

			runID := waitForRun(t, env, 5*time.Minute)

			if tc.autoApprove {
				// The orchestrator must approve the gate itself: the run
				// reaches the gate and completes with no operator action.
				env.waitRunDone(runID, 12*time.Minute)
				// Gate auto-approved and the merge folded the file into main.
				if got := env.nodeState(runID, "gate"); got != string(graph.StateDone) {
					t.Fatalf("gate state = %s, want done (orchestrator should have approved it)", got)
				}
			} else {
				// The run parks at the gate: the orchestrator must NOT
				// approve on its own.
				env.waitNodeState(runID, "gate", string(graph.StateRunning), 8*time.Minute)
				// Give the orchestrator a generous window to (incorrectly)
				// approve; a policy violation would resolve the gate here.
				time.Sleep(8 * time.Second)
				if got := env.nodeState(runID, "gate"); got != string(graph.StateRunning) {
					t.Fatalf("gate state = %s while not pre-authorized, want running (orchestrator must not approve)", got)
				}
				// The human approves externally, as an operator would.
				code, body := env.api.do("operator", http.MethodPost, "/api/runs/"+runID+"/approve", map[string]any{"nodeID": "gate"})
				if code != 200 {
					t.Fatalf("external approve: %d %s", code, body)
				}
				env.waitRunDone(runID, 12*time.Minute)
			}

			// The merged artifact must be in the main checkout.
			data, err := os.ReadFile(filepath.Join(env.proj, "alpha.txt"))
			if err != nil || len(strings.TrimSpace(string(data))) == 0 {
				t.Fatalf("merged content wrong: %v %q", err, data)
			}

			// Wait for the orchestrator session to settle, then inspect its
			// tool usage: it must have started the run, watched repeatedly,
			// and honored the approval policy.
			deadline := time.Now().Add(2 * time.Minute)
			for time.Now().Before(deadline) {
				if msgs, err := env.oc.Messages(ctx, sess.ID, 4); err == nil && len(msgs) > 0 {
					last := msgs[len(msgs)-1]
					if last.Info.Finish != nil || last.Info.Error != nil {
						break
					}
				}
				time.Sleep(1 * time.Second)
			}
			tools, transcript := sessionTools(t, env.oc, sess.ID)

			started, watched := false, 0
			approved := false
			for _, name := range tools {
				switch name {
				case "corral_start":
					started = true
				case "corral_watch":
					watched++
				case "corral_approve":
					approved = true
				}
			}
			if !started || watched < 2 {
				t.Errorf("orchestrator loop wrong: start=%v watch=%d; transcript:\n%s", started, watched, transcript)
			}
			if tc.autoApprove && !approved {
				t.Errorf("pre-authorized orchestrator never called corral_approve; transcript:\n%s", transcript)
			}
			if !tc.autoApprove && approved {
				t.Errorf("not-pre-authorized orchestrator called corral_approve; transcript:\n%s", transcript)
			}
			if !tc.autoApprove && !strings.Contains(strings.ToLower(transcript), "approval") && !strings.Contains(strings.ToLower(transcript), "gate") {
				t.Errorf("orchestrator did not report the gate to the user; transcript:\n%s", transcript)
			}
		})
	}
}
