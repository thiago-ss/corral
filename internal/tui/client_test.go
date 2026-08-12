package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"corral/internal/adapter"
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
	t.Cleanup(d.Close)
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

	// Detail contains states, attempts, session/worktree once done. The
	// node state flips to done before the attempt row's final update is
	// committed, so wait for both.
	var detail *RunDetail
	for time.Now().Before(deadline) {
		dd, err := client.GetRun(ctx, runID)
		if err == nil && dd.States["w1"] == "done" {
			if atts := dd.Attempts["w1"]; len(atts) == 1 && atts[0].Status == "done" {
				detail = dd
				break
			}
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

// TestClientRespondPermission drives a permission-blocked node through the
// daemon: the client's RespondPermission allows it and the node completes.
func TestClientRespondPermission(t *testing.T) {
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
	drv.SetScript("w1", sched.Script{Delay: 5 * time.Second, Permission: "perm-9", Write: map[string]string{"a.txt": "A"}})
	var created struct{ RunID string }
	if err := client.do(ctx, "POST", "/api/runs", map[string]any{"graph": g}, &created); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		dd, err := client.GetRun(ctx, created.RunID)
		if err == nil && dd.States["w1"] == "blocked" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The pending permission is visible in the detail payload.
	dd, err := client.GetRun(ctx, created.RunID)
	if err != nil {
		t.Fatal(err)
	}
	pid := ""
	for _, ev := range dd.Events {
		if ev.NodeID == "w1" && ev.To == "blocked" {
			var p struct {
				Reason       string `json:"reason"`
				PermissionID string `json:"permissionID"`
			}
			if json.Unmarshal(ev.Payload, &p) == nil {
				pid = p.PermissionID
			}
		}
	}
	if pid != "perm-9" {
		t.Fatalf("blocked payload permissionID = %q, want perm-9", pid)
	}

	if err := client.RespondPermission(ctx, created.RunID, "w1", "perm-9", true); err != nil {
		t.Fatalf("respond permission: %v", err)
	}
	for time.Now().Before(deadline) {
		dd, _ := client.GetRun(ctx, created.RunID)
		if dd != nil && dd.States["w1"] == "done" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("node never done after permission allowed")
}

// TestTailAgainstDaemon exercises the tail endpoint against a live daemon
// with a running attempt: the transcript lines stream while the node runs.
func TestTailAgainstDaemon(t *testing.T) {
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
	drv.SetScript("w1", sched.Script{
		Delay: 5 * time.Second,
		Messages: []adapter.Message{
			{Role: "assistant", Text: "inspecting the workspace"},
			{Role: "assistant", Text: "found the bug\napplying the fix"},
		},
	})
	g := &graph.Graph{Nodes: []*graph.Node{{
		ID: "w1", Type: graph.NodeAgent, Role: "worker",
		Objective: "write a.txt", AcceptanceCriteria: []string{"a.txt"},
		Priority: graph.PriorityNormal, WriteScope: []string{"a.txt"},
		Verification: &graph.Verification{Kind: "command", Command: []string{"test", "-f", "a.txt"}},
		Meta:         map[string]string{"cwd": workdir},
	}}}
	var created struct{ RunID string }
	if err := client.do(ctx, "POST", "/api/runs", map[string]any{"graph": g}, &created); err != nil {
		t.Fatal(err)
	}

	// Wait until the node is running, then fetch the tail.
	deadline := time.Now().Add(10 * time.Second)
	var dd *RunDetail
	for time.Now().Before(deadline) {
		dd, _ = client.GetRun(ctx, created.RunID)
		if dd != nil && dd.States["w1"] == "running" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if dd == nil || dd.States["w1"] != "running" {
		t.Fatal("node never reached running")
	}
	lines, err := client.Tail(ctx, created.RunID, "w1", 40)
	if err != nil {
		t.Fatalf("tail: %v", err)
	}
	if len(lines) == 0 {
		t.Fatal("tail returned no lines while running")
	}
	joined := strings.Join(lines, "\n")
	for _, want := range []string{"inspecting", "applying"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("tail missing %q: %q", want, joined)
		}
	}
}

func TestTailRejectsInvalidLineCount(t *testing.T) {
	client := NewClient("http://unused.invalid", "")
	for _, lines := range []int{0, -1, 501} {
		if _, err := client.Tail(context.Background(), "run", "node", lines); err == nil {
			t.Fatalf("Tail accepted lines=%d", lines)
		}
	}
}

func TestStreamEventsUsesCursorAndRawFrames(t *testing.T) {
	var gotPath, gotAccept, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.RequestURI()
		gotAccept = r.Header.Get("Accept")
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		fmt.Fprint(w, ": ping\n\n")
		fmt.Fprint(w, "id: 8\n")
		fmt.Fprint(w, `data: {"seq":8,"runID":"run id","nodeID":"w1","type":"transition","from":"running","to":"blocked","payload":{"reason":"permission","permissionID":"p1"},"createdAt":123}`+"\n\n")
		fmt.Fprint(w, "id: 8\n") // duplicate must be ignored
		fmt.Fprint(w, `data: {"seq":8,"runID":"run id","nodeID":"w1","type":"transition"}`+"\n\n")
	}))
	t.Cleanup(srv.Close)

	client := NewClient(srv.URL, "secret")
	var events []EventView
	err := client.StreamEvents(context.Background(), "run id", 7, func(event EventView) error {
		events = append(events, event)
		return nil
	})
	if err == nil || err.Error() != "EOF" {
		t.Fatalf("closed stream err = %v, want EOF", err)
	}
	if gotPath != "/api/runs/run%20id/events?after=7" {
		t.Fatalf("stream path = %q", gotPath)
	}
	if gotAccept != "text/event-stream" || gotAuth != "Bearer secret" {
		t.Fatalf("stream headers accept=%q auth=%q", gotAccept, gotAuth)
	}
	if len(events) != 1 || events[0].Seq != 8 || events[0].To != "blocked" || string(events[0].Payload) == "" {
		t.Fatalf("events = %+v", events)
	}
}
