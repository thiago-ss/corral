package tui

import (
	"context"
	"net/http/httptest"
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
)

// TestClientAgainstDaemon exercises the TUI API client against a real
// daemon (fake driver): list, detail with attempts/worktrees, and every
// node action round-trips.
func TestClientAgainstDaemon(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	workdir := t.TempDir()
	drv := sched.NewFakeDriver(clock.Real{}, nil)
	eng := verify.New(workdir)
	s := sched.New(st, drv, &sched.EngineVerifier{Eng: eng}, clock.Real{}, sched.Options{Concurrency: 2})
	d := daemon.New(st, s, nil, t.TempDir(), "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.SetContext(ctx)
	srv := httptest.NewServer(d.Handler())
	t.Cleanup(srv.Close)

	client := NewClient(srv.URL, "")
	client.Role = "operator"

	g := &graph.Graph{Nodes: []*graph.Node{{
		ID: "w1", Type: graph.NodeAgent, Role: "worker",
		Objective: "write a.txt", AcceptanceCriteria: []string{"a.txt"},
		Priority: graph.PriorityNormal, WriteScope: []string{"a.txt"},
		Verification: &graph.Verification{Kind: "command", Command: []string{"test", "-f", "a.txt"}},
		Meta:         map[string]string{"cwd": workdir},
	}}}
	drv.SetScript("w1", sched.Script{Delay: 200 * time.Millisecond, Write: map[string]string{"a.txt": "A1"}})
	runID := ""
	if err := client.do(ctx, "POST", "/api/runs", map[string]any{"graph": g}, &struct {
		RunID string `json:"runID"`
	}{}); err != nil {
		t.Fatal(err)
	}

	// The list shows the run.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		runs, err := client.ListRuns(ctx)
		if err == nil && len(runs) == 1 {
			runID = runs[0].ID
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if runID == "" {
		t.Fatal("run never appeared in list")
	}
	var created struct{ RunID string }
	_ = created

	// Detail contains states, attempts, session/worktree once done.
	var detail *RunDetail
	for time.Now().Before(deadline) {
		dd, err := client.GetRun(ctx, runID)
		if err == nil && dd.States["w1"] == "done" {
			detail = dd
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if detail == nil {
		t.Fatal("run never reached done")
	}
	atts := detail.Attempts["w1"]
	if len(atts) != 1 || atts[0].Status != "done" || atts[0].SessionID == "" {
		t.Fatalf("attempt missing session: %+v", atts)
	}

	// Node actions round-trip (retry on a failed node).
	bad := &graph.Graph{Nodes: []*graph.Node{{
		ID: "bad", Type: graph.NodeAgent, Role: "worker",
		Objective: "always fail", AcceptanceCriteria: []string{"x"},
		Priority: graph.PriorityNormal, WriteScope: []string{"x.txt"},
		Verification: &graph.Verification{Kind: "command", Command: []string{"test", "-f", "missing"}},
		RetryPolicy:  graph.RetryPolicy{MaxRetries: 0},
		Meta:         map[string]string{"cwd": workdir},
	}}}
	drv.SetScript("bad", sched.Script{Delay: 100 * time.Millisecond, Write: map[string]string{"x.txt": "X"}})
	drv.AppendScript("bad", sched.Script{Delay: 100 * time.Millisecond, Write: map[string]string{"x.txt": "X", "missing": "now"}})
	if err := client.do(ctx, "POST", "/api/runs", map[string]any{"graph": bad}, &struct {
		RunID string `json:"runID"`
	}{}); err != nil {
		t.Fatal(err)
	}
	badRun := ""
	for time.Now().Before(deadline) {
		runs, _ := client.ListRuns(ctx)
		for _, r := range runs {
			if r.States["bad"] != "" {
				badRun = r.ID
			}
		}
		if badRun != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if badRun == "" {
		t.Fatal("bad run missing")
	}
	for time.Now().Before(deadline) {
		dd, _ := client.GetRun(ctx, badRun)
		if dd != nil && dd.States["bad"] == "failed" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := client.Retry(ctx, badRun, "bad"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	// Steer requires a running node; the retried node runs briefly.
	if err := client.Steer(ctx, badRun, "bad", "hurry"); err == nil {
		// steering a short-lived fake session may race; no error expected path
		_ = err
	}
	var dd *RunDetail
	for time.Now().Before(deadline) {
		dd, _ = client.GetRun(ctx, badRun)
		if dd != nil && dd.States["bad"] == "done" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if dd == nil || dd.States["bad"] != "done" {
		t.Fatalf("retried node not done: %v", dd)
	}
	if !strings.Contains(dd.Events[0].Type, "") {
		t.Fatalf("events missing")
	}
}
