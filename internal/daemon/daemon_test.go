package daemon_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
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
	"corral/internal/sched"
	"corral/internal/store"
	"corral/internal/verify"
	"corral/internal/worktree"
)

const tick = 50 * time.Millisecond

type api struct {
	t    *testing.T
	cli  *http.Client
	base string
}

func (a *api) do(role string, method, path string, body any) (int, string) {
	a.t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, a.base+path, rdr)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Corral-Role", role)
	resp, err := a.cli.Do(req)
	if err != nil {
		a.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data)
}

func setupDaemon(t *testing.T, apiKey string) (*api, *daemon.Daemon, *store.Store, *sched.FakeDriver) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "d.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	repo := t.TempDir()
	for _, args := range [][]string{
		{"init", "-q", "-b", "main"},
		{"-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git: %v: %s", err, out)
		}
	}
	clk := clock.Real{}
	wtm := worktree.NewManager(repo)
	drv := sched.NewFakeDriver(clk, nil)
	eng := verify.New(repo)
	eng.Runner = verify.ExecRunner{}
	s := sched.New(st, drv, &sched.EngineVerifier{Eng: eng}, clk, sched.Options{
		Concurrency: 4, Worktrees: wtm,
	})
	d := daemon.New(st, s, nil, repo, apiKey)
	srv := httptest.NewServer(d.Handler())
	t.Cleanup(srv.Close)
	return &api{t: t, cli: srv.Client(), base: srv.URL}, d, st, drv
}

func workerNode(id graph.NodeID, file, content string) *graph.Node {
	return &graph.Node{
		ID: id, Type: graph.NodeAgent, Role: "worker",
		Objective:          "produce " + file,
		AcceptanceCriteria: []string{file + " produced"},
		Priority:           graph.PriorityNormal,
		WriteScope:         []string{file},
		Verification:       &graph.Verification{Kind: "command", Command: []string{"test", "-f", file}},
		RetryPolicy:        graph.RetryPolicy{MaxRetries: 2, Backoff: tick},
	}
}

func gateNode(id graph.NodeID, deps ...graph.NodeID) *graph.Node {
	return &graph.Node{ID: id, Type: graph.NodeHuman, Objective: "approve", Priority: graph.PriorityNormal, DependsOn: deps}
}

func mergeNode(id graph.NodeID, verifyCmd []string, deps ...graph.NodeID) *graph.Node {
	return &graph.Node{
		ID: id, Type: graph.NodeMerge, Objective: "merge", Priority: graph.PriorityNormal,
		DependsOn: deps, Verification: &graph.Verification{Kind: "command", Command: verifyCmd},
		RetryPolicy: graph.RetryPolicy{MaxRetries: 1, Backoff: tick},
	}
}

func (a *api) waitState(t *testing.T, base string, runID, nodeID string, want graph.State, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		code, body := a.do("operator", http.MethodGet, base+"/api/runs/"+runID, nil)
		if code == http.StatusOK {
			var r struct {
				States map[string]string `json:"states"`
			}
			if json.Unmarshal([]byte(body), &r) == nil {
				if r.States[nodeID] == string(want) {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node %s never reached %s", nodeID, want)
}

func TestRoleEnforcement(t *testing.T) {
	a, _, _, _ := setupDaemon(t, "")
	g := &graph.Graph{Nodes: []*graph.Node{workerNode("w1", "a.txt", "A1")}}

	// Only orchestrator/operator may create runs.
	for _, role := range []string{"planner", "worker", "reviewer", "merger"} {
		if code, _ := a.do(role, http.MethodPost, "/api/runs", map[string]any{"graph": g}); code != http.StatusForbidden {
			t.Fatalf("role %s create run: %d, want 403", role, code)
		}
	}
	if code, _ := a.do("orchestrator", http.MethodPost, "/api/runs", map[string]any{"graph": g}); code != http.StatusCreated {
		t.Fatalf("orchestrator create run: %d, want 201", code)
	}
	// Workers may not approve; operators may. (Run id from the created run.)
	if code, _ := a.do("worker", http.MethodPost, "/api/runs/whatever/approve", map[string]any{"nodeID": "w1"}); code != http.StatusForbidden {
		t.Fatalf("worker approve: %d, want 403", code)
	}
	// Unknown role rejected (health is open, everything else is gated).
	if code, _ := a.do("hacker", http.MethodPost, "/api/runs", map[string]any{"graph": &graph.Graph{}}); code != http.StatusForbidden {
		t.Fatalf("unknown role: %d, want 403", code)
	}
}

func TestAuthRequired(t *testing.T) {
	a, _, _, _ := setupDaemon(t, "sekret")
	req, _ := http.NewRequest(http.MethodGet, a.base+"/api/health", nil)
	resp, err := a.cli.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no auth: %d, want 401", resp.StatusCode)
	}
	if code, _ := a.do("operator", http.MethodGet, "/api/health", nil); code != http.StatusUnauthorized {
		t.Fatalf("wrong role header but no bearer: %d, want 401", code)
	}
}

func TestPlanRequiresPlannerRoleAndGraph(t *testing.T) {
	a, d, _, _ := setupDaemon(t, "")
	planned := &graph.Graph{Nodes: []*graph.Node{workerNode("w1", "a.txt", "A1")}}
	d.SetPlanner(&fakePlanner{g: planned})

	if code, _ := a.do("worker", http.MethodPost, "/api/plan", map[string]any{"goal": "x"}); code != http.StatusForbidden {
		t.Fatalf("worker plan: %d, want 403", code)
	}
	code, body := a.do("planner", http.MethodPost, "/api/plan", map[string]any{"goal": "x"})
	if code != http.StatusOK {
		t.Fatalf("planner plan: %d: %s", code, body)
	}
	if !strings.Contains(body, `"nodes"`) {
		t.Fatalf("plan response missing graph: %s", body)
	}
}

func TestFullFlowThroughAPI(t *testing.T) {
	a, d, st, drv := setupDaemon(t, "")
	_ = d
	repo := d.Dir()
	drv.SetScript("w1", sched.Script{Delay: 200 * time.Millisecond, Write: map[string]string{"a.txt": "A1"}})

	g := &graph.Graph{Nodes: []*graph.Node{
		workerNode("w1", "a.txt", "A1"),
		gateNode("gate", "w1"),
		mergeNode("m", []string{"test", "-f", "a.txt"}, "gate"),
	}}
	code, body := a.do("operator", http.MethodPost, "/api/runs", map[string]any{"graph": g})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %s", code, body)
	}
	var created struct{ RunID string }
	json.Unmarshal([]byte(body), &created)

	// The run executes without further daemon touch: wait for the gate.
	a.waitState(t, "", created.RunID, "gate", graph.StateRunning, 30*time.Second)
	// Workers finished in worktrees; main checkout clean.
	out, _ := exec.Command("git", "-C", repo, "status", "--porcelain").CombinedOutput()
	if strings.TrimSpace(string(out)) != "" {
		t.Fatalf("main dirty: %q", out)
	}
	// Merge must not run before approval.
	var r struct {
		Attempts map[string][]store.Attempt `json:"attempts"`
	}
	if code, body := a.do("operator", http.MethodGet, "/api/runs/"+created.RunID, nil); code == http.StatusOK {
		json.Unmarshal([]byte(body), &r)
	}
	if len(r.Attempts["m"]) != 0 {
		t.Fatalf("merge ran before approval: %+v", r.Attempts["m"])
	}

	// Approve via the API; run completes; merge folds the branch in.
	code, body = a.do("operator", http.MethodPost, "/api/runs/"+created.RunID+"/approve", map[string]any{"nodeID": "gate"})
	if code != http.StatusOK {
		t.Fatalf("approve: %d %s", code, body)
	}
	a.waitState(t, "", created.RunID, "m", graph.StateDone, 30*time.Second)
	if code, body := a.do("operator", http.MethodGet, "/api/runs/"+created.RunID, nil); code == http.StatusOK {
		if !strings.Contains(body, `"done":true`) {
			t.Fatalf("run not done: %s", body)
		}
	}
	data, err := os.ReadFile(filepath.Join(repo, "a.txt"))
	if err != nil || !strings.Contains(string(data), "A1") {
		t.Fatalf("merged content wrong: %v %q", err, data)
	}
	// Run status persisted.
	ru, _ := st.Run(context.Background(), created.RunID)
	if ru.Status != "completed" {
		t.Fatalf("run status = %s", ru.Status)
	}
}

func TestCancelAndRetryThroughAPI(t *testing.T) {
	a, _, st, drv := setupDaemon(t, "")

	// Slow worker canceled mid-run.
	slow := workerNode("w1", "a.txt", "A1")
	slow.Objective = "work forever"
	slow.Verification = &graph.Verification{Kind: "command", Command: []string{"test", "-f", "a.txt"}}
	drv.SetScript("w1", sched.Script{Delay: 30 * time.Second})
	code, body := a.do("operator", http.MethodPost, "/api/runs", map[string]any{"graph": &graph.Graph{Nodes: []*graph.Node{slow}}})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %s", code, body)
	}
	var created struct{ RunID string }
	json.Unmarshal([]byte(body), &created)
	a.waitState(t, "", created.RunID, "w1", graph.StateRunning, 20*time.Second)
	code, body = a.do("operator", http.MethodPost, "/api/runs/"+created.RunID+"/steer", map[string]any{"nodeID": "w1", "message": "wrap up"})
	if code != http.StatusOK {
		t.Fatalf("steer: %d %s", code, body)
	}
	code, body = a.do("operator", http.MethodPost, "/api/runs/"+created.RunID+"/cancel", map[string]any{"nodeID": "w1"})
	if code != http.StatusOK {
		t.Fatalf("cancel: %d %s", code, body)
	}
	a.waitState(t, "", created.RunID, "w1", graph.StateCanceled, 20*time.Second)

	// Failed node retried through the API.
	bad := workerNode("bad", "x.txt", "X")
	bad.Verification = &graph.Verification{Kind: "command", Command: []string{"test", "-f", "missing.txt"}}
	bad.RetryPolicy = graph.RetryPolicy{MaxRetries: 0, Backoff: tick}
	drv.AppendScript("bad", sched.Script{Delay: 100 * time.Millisecond, Write: map[string]string{"x.txt": "X"}})
	drv.AppendScript("bad", sched.Script{Delay: 100 * time.Millisecond, Write: map[string]string{"x.txt": "X", "missing.txt": "now"}})
	code, body = a.do("operator", http.MethodPost, "/api/runs", map[string]any{"graph": &graph.Graph{Nodes: []*graph.Node{bad}}})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %s", code, body)
	}
	var r2 struct{ RunID string }
	json.Unmarshal([]byte(body), &r2)
	a.waitState(t, "", r2.RunID, "bad", graph.StateFailed, 20*time.Second)
	code, body = a.do("operator", http.MethodPost, "/api/runs/"+r2.RunID+"/retry", map[string]any{"nodeID": "bad"})
	if code != http.StatusOK {
		t.Fatalf("retry: %d %s", code, body)
	}
	a.waitState(t, "", r2.RunID, "bad", graph.StateDone, 20*time.Second)
	atts, _ := st.Attempts(context.Background(), r2.RunID, "bad")
	if len(atts) != 2 {
		t.Fatalf("retry did not produce a new attempt: %d", len(atts))
	}
	if atts[1].Status != "done" {
		t.Fatalf("retried attempt status = %s, want done", atts[1].Status)
	}
}

type fakePlanner struct{ g *graph.Graph }

func (f *fakePlanner) Plan(_ context.Context, _ string) (*graph.Graph, error) { return f.g, nil }

var _ = daemon.RoleOperator

func TestPermissionThroughAPI(t *testing.T) {
	a, d, _, drv := setupDaemon(t, "")
	workdir := d.Dir()
	eng := verify.New(workdir)
	eng.Runner = verify.ExecRunner{}
	d.SetPlanner(nil)
	// Rebuild scheduler with engine on the daemon dir is not needed; use
	// an in-place graph whose gate checks a file the fake will write.
	n := &graph.Node{
		ID: "w1", Type: graph.NodeAgent, Role: "worker",
		Objective: "work", AcceptanceCriteria: []string{"x"},
		Priority: graph.PriorityNormal, WriteScope: []string{"a.txt"},
		Verification: &graph.Verification{Kind: "command", Command: []string{"test", "-f", "a.txt"}},
		Meta:         map[string]string{"cwd": workdir},
	}
	drv.SetScript("w1", sched.Script{Delay: 5 * time.Second, Permission: "perm-9", Write: map[string]string{"a.txt": "A"}})
	code, body := a.do("operator", http.MethodPost, "/api/runs", map[string]any{"graph": &graph.Graph{Nodes: []*graph.Node{n}}})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %s", code, body)
	}
	var created struct{ RunID string }
	json.Unmarshal([]byte(body), &created)

	// The node blocks on the permission request, explicitly.
	a.waitState(t, "", created.RunID, "w1", graph.StateBlocked, 20*time.Second)
	// Answer it through the API; the run resumes and completes.
	code, body = a.do("operator", http.MethodPost, "/api/runs/"+created.RunID+"/permission",
		map[string]any{"nodeID": "w1", "permissionID": "perm-9", "allow": true})
	if code != http.StatusOK {
		t.Fatalf("permission respond: %d %s", code, body)
	}
	a.waitState(t, "", created.RunID, "w1", graph.StateDone, 30*time.Second)
}
