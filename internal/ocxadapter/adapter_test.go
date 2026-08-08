package ocxadapter_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"corral/internal/adapter"
	"corral/internal/clock"
	"corral/internal/graph"
	"corral/internal/livetest"
	"corral/internal/ocx"
	"corral/internal/ocxadapter"
	"corral/internal/sched"
	"corral/internal/spike"
	"corral/internal/store"
	"corral/internal/verify"
)

const (
	w1Prompt = "Create a file named alpha.txt containing exactly one line: CORRAL-OC1. Do not run any other commands."
	w2Prompt = "Append one line to beta.txt every second, 30 lines total, numbered 1 to 30, using bash. Keep going until the loop finishes. Do not stop early."
)

func ocNode(id graph.NodeID, prompt string) *graph.Node {
	return &graph.Node{
		ID:                 id,
		Type:               graph.NodeAgent,
		Objective:          prompt,
		AcceptanceCriteria: []string{"file produced"},
		Priority:           graph.PriorityNormal,
		RetryPolicy:        graph.RetryPolicy{MaxRetries: 1, Backoff: 5 * time.Second},
		Budget:             graph.Budget{MaxDuration: 12 * time.Minute},
	}
}

func TestOpenCodeAdapterParallelAndCancel(t *testing.T) {
	livetest.SkipIfDisabled(t)
	if _, err := exec.LookPath("opencode"); err != nil {
		t.Skip("opencode binary not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	proj, err := os.MkdirTemp("", "corral-oc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(proj) })
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"commit", "-q", "--allow-empty", "-m", "init"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = proj
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}

	srv, err := spike.StartServer(ctx, proj, 0, os.Stderr)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(srv.Stop)

	st, err := store.Open(filepath.Join(t.TempDir(), "oc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	oc := ocx.New(srv.Base, proj)
	drv := ocxadapter.New(oc, ocxadapter.Options{PollInterval: 400 * time.Millisecond})
	t.Cleanup(func() { drv.Close() })
	ver := sched.NewFakeVerifier(nil, sched.Verdict{Pass: true, Evidence: "ok"})
	s := sched.New(st, drv, ver, clock.Real{}, sched.Options{Concurrency: 2})

	g := &graph.Graph{Nodes: []*graph.Node{ocNode("w1", w1Prompt), ocNode("w2", w2Prompt)}}
	h, err := s.Create(ctx, "run-oc", g)
	if err != nil {
		t.Fatal(err)
	}

	// Track peak concurrent busy sessions from the server's perspective.
	var peakMu sync.Mutex
	peak := 0
	stopPoll := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopPoll:
				return
			case <-ticker.C:
				sts, err := oc.SessionStatus(ctx)
				if err != nil {
					continue
				}
				busy := 0
				for _, s2 := range sts {
					if s2.Type == "busy" {
						busy++
					}
				}
				peakMu.Lock()
				if busy > peak {
					peak = busy
				}
				peakMu.Unlock()
			}
		}
	}()

	// Drive the run; cancel w2 once it is running.
	runDone := make(chan error, 1)
	go func() { runDone <- h.Run(ctx, 200*time.Millisecond) }()

	w2Canceled := false
	deadline := time.Now().Add(360 * time.Second)
	for !h.Done() && time.Now().Before(deadline) {
		if !w2Canceled {
			if st2, _ := h.State("w2"); st2 == graph.StateRunning {
				// Let the attempt make progress, then cancel mid-run.
				time.Sleep(5 * time.Second)
				if err := h.CancelNode(ctx, "w2"); err != nil {
					t.Logf("cancel: %v", err)
				} else {
					w2Canceled = true
					t.Log("w2 canceled mid-run")
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err := <-runDone; err != nil {
		for _, id := range []string{"w1", "w2"} {
			if st2, _ := h.State(graph.NodeID(id)); st2 != "" {
				t.Logf("at timeout: %s -> %s", id, st2)
			}
		}
		evs, evErr := st.Events(context.Background(), "run-oc")
		if evErr == nil {
			for _, ev := range evs {
				if ev.NodeID != "" {
					t.Logf("event %d %s %s->%s att=%s", ev.Seq, ev.Type, ev.From, ev.To, ev.AttemptID)
				}
			}
		}
		t.Fatalf("run: %v", err)
	}
	close(stopPoll)
	wg.Wait()
	peakMu.Lock()
	pk := peak
	peakMu.Unlock()

	if !w2Canceled {
		t.Fatal("w2 was never observed running; cancellation not exercised")
	}
	if !h.Done() {
		t.Fatal("run did not complete")
	}
	if pk < 2 {
		t.Errorf("peak concurrent busy sessions = %d, want >= 2 (parallel execution)", pk)
	}

	// Terminal states.
	if st1, _ := h.State("w1"); st1 != graph.StateDone {
		t.Errorf("w1 state = %s, want done", st1)
	}
	if st2, _ := h.State("w2"); st2 != graph.StateCanceled {
		t.Errorf("w2 state = %s, want canceled", st2)
	}

	// Exactly one attempt each: no duplicate completion from duplicate
	// or missing events.
	for _, id := range []string{"w1", "w2"} {
		atts, err := st.Attempts(ctx, "run-oc", id)
		if err != nil {
			t.Fatal(err)
		}
		if len(atts) != 1 {
			t.Fatalf("%s attempts = %d, want exactly 1 (no duplicates)", id, len(atts))
		}
		at := atts[0]
		if !strings.HasPrefix(at.SessionID, "ses_") {
			t.Errorf("%s session id = %q, want ses_ prefix (OpenCode session id recorded)", id, at.SessionID)
		}
		if at.ServerID != srv.Base {
			t.Errorf("%s server id = %q, want %q", id, at.ServerID, srv.Base)
		}
	}
	atts, _ := st.Attempts(ctx, "run-oc", "w1")
	if atts[0].Status != "done" {
		t.Errorf("w1 attempt status = %s, want done", atts[0].Status)
	}
	atts, _ = st.Attempts(ctx, "run-oc", "w2")
	if atts[0].Status != "aborted" {
		t.Errorf("w2 attempt status = %s, want aborted", atts[0].Status)
	}

	// w1's work is real.
	data, err := os.ReadFile(filepath.Join(proj, "alpha.txt"))
	if err != nil {
		t.Fatalf("alpha.txt missing: %v", err)
	}
	if got := strings.TrimSpace(string(data)); got != "CORRAL-OC1" {
		t.Errorf("alpha.txt = %q, want CORRAL-OC1", got)
	}
}

func TestOpenCodeEvidenceGates(t *testing.T) {
	livetest.SkipIfDisabled(t)
	if _, err := exec.LookPath("opencode"); err != nil {
		t.Skip("opencode binary not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	proj, err := os.MkdirTemp("", "corral-gates-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(proj) })
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"commit", "-q", "--allow-empty", "-m", "init"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = proj
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	srv, err := spike.StartServer(ctx, proj, 0, os.Stderr)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(srv.Stop)

	st, err := store.Open(filepath.Join(t.TempDir(), "gates.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	oc := ocx.New(srv.Base, proj)
	drv := ocxadapter.New(oc, ocxadapter.Options{PollInterval: 400 * time.Millisecond})
	t.Cleanup(func() { drv.Close() })

	// w1 writes a fixed marker and passes a grep gate deterministically.
	// w2 writes a marker that the gate deliberately rejects, so the node
	// fails verification permanently and its dependent becomes blocked.
	eng := verify.New(proj)
	w1 := ocNode("w1", "Create a file named gate.txt with some content. Do not run any other commands.")
	w1.Verification = &graph.Verification{Kind: "command", Command: []string{"test", "-s", "gate.txt"}}
	w1.RetryPolicy = graph.RetryPolicy{MaxRetries: 1, Backoff: 5 * time.Second}

	w2 := ocNode("w2", "Create a file named bad.txt containing exactly one line: WRONG-CONTENT. Do not run any other commands.")
	w2.Verification = &graph.Verification{Kind: "command", Command: []string{"grep", "-q", "CORRAL-GATE-OK", "bad.txt"}}
	w2.RetryPolicy = graph.RetryPolicy{MaxRetries: 1, Backoff: 5 * time.Second}

	w3 := ocNode("w3", "Create a file named never.txt containing one line: NEVER.")
	w3.DependsOn = []graph.NodeID{"w2"}
	w3.Verification = &graph.Verification{Kind: "command", Command: []string{"test", "-f", "never.txt"}}

	s := sched.New(st, drv, &sched.EngineVerifier{Eng: eng}, clock.Real{}, sched.Options{Concurrency: 3})
	h, err := s.Create(ctx, "run-gates", &graph.Graph{Nodes: []*graph.Node{w1, w2, w3}})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx, 250*time.Millisecond); err != nil {
		for _, id := range []string{"w1", "w2", "w3"} {
			if st2, _ := h.State(graph.NodeID(id)); st2 != "" {
				t.Logf("at timeout: %s -> %s", id, st2)
			}
		}
		evs, evErr := st.Events(context.Background(), "run-gates")
		if evErr == nil {
			for _, ev := range evs {
				if ev.NodeID != "" {
					t.Logf("event %d %s %s->%s", ev.Seq, ev.Type, ev.From, ev.To)
				}
			}
		}
		t.Fatalf("run: %v", err)
	}

	if st1, _ := h.State("w1"); st1 != graph.StateDone {
		t.Errorf("w1 state = %s, want done (gate passed)", st1)
	}
	if st2, _ := h.State("w2"); st2 != graph.StateFailed {
		t.Errorf("w2 state = %s, want failed (gate permanently rejected)", st2)
	}
	if st3, _ := h.State("w3"); st3 != graph.StateBlocked {
		t.Errorf("w3 state = %s, want blocked (dep failed)", st3)
	}
	// w2 attempted at most maxRetries+1 times; w3 never ran.
	atts, _ := st.Attempts(ctx, "run-gates", "w2")
	if len(atts) > 2 {
		t.Errorf("w2 attempts = %d, want <= 2 (bounded retries)", len(atts))
	}
	n, _ := st.CountAttempts(ctx, "run-gates", "w3")
	if n != 0 {
		t.Errorf("w3 attempts = %d, want 0", n)
	}
}

var _ = adapter.StatusRunning
