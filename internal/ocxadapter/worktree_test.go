package ocxadapter_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"corral/internal/clock"
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

// TestOpenCodeWorktreeMerge runs the full isolation pipeline against a
// real OpenCode server: two workers in separate worktrees, an approval
// gate, then a merge; the main checkout stays clean until approval.
func TestOpenCodeWorktreeMerge(t *testing.T) {
	livetest.SkipIfDisabled(t)
	if _, err := exec.LookPath("opencode"); err != nil {
		t.Skip("opencode binary not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	proj, err := os.MkdirTemp("", "corral-wtmerge-")
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
	srv, err := spike.StartServer(ctx, proj, os.Stderr)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(srv.Stop)

	st, err := store.Open(filepath.Join(t.TempDir(), "wt.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	oc := ocx.New(srv.Base, proj)
	drv := ocxadapter.New(oc, ocxadapter.Options{PollInterval: 400 * time.Millisecond})
	t.Cleanup(func() { drv.Close() })
	wtm := worktree.NewManager(proj)

	w1 := ocNode("w1", "Create a file named alpha.txt with some content. Do not run any other commands.")
	w1.Role = "worker"
	w1.WriteScope = []string{"alpha.txt"}
	w1.Verification = &graph.Verification{Kind: "command", Command: []string{"test", "-s", "alpha.txt"}}
	w1.RetryPolicy = graph.RetryPolicy{MaxRetries: 1, Backoff: 5 * time.Second}

	w2 := ocNode("w2", "Create a file named beta.txt with some content. Do not run any other commands.")
	w2.Role = "worker"
	w2.WriteScope = []string{"beta.txt"}
	w2.Verification = &graph.Verification{Kind: "command", Command: []string{"test", "-s", "beta.txt"}}
	w2.RetryPolicy = graph.RetryPolicy{MaxRetries: 1, Backoff: 5 * time.Second}

	gate := &graph.Node{
		ID: "gate", Type: graph.NodeHuman, Objective: "approve",
		Priority: graph.PriorityNormal, DependsOn: []graph.NodeID{"w1", "w2"},
	}
	merge := &graph.Node{
		ID: "m", Type: graph.NodeMerge, Objective: "merge",
		Priority: graph.PriorityNormal, DependsOn: []graph.NodeID{"gate"},
		Verification: &graph.Verification{Kind: "command", Command: []string{"test", "-s", "alpha.txt", "-a", "-s", "beta.txt"}},
		RetryPolicy:  graph.RetryPolicy{MaxRetries: 1, Backoff: 5 * time.Second},
	}

	s := sched.New(st, drv, &sched.EngineVerifier{Eng: verify.New(proj)}, clock.Real{}, sched.Options{
		Concurrency: 2, Worktrees: wtm,
	})
	h, err := s.Create(ctx, "run-wtmerge", &graph.Graph{Nodes: []*graph.Node{w1, w2, gate, merge}})
	if err != nil {
		t.Fatal(err)
	}

	// Drive manually: step the scheduler, approve the gate once it runs.
	approved := false
	deadline := time.Now().Add(6 * time.Minute)
	for !h.Done() && time.Now().Before(deadline) {
		if err := h.Step(ctx); err != nil {
			t.Fatalf("step: %v", err)
		}
		if !approved {
			if gs, _ := h.State("gate"); gs == graph.StateRunning {
				// Main checkout must be clean while workers are isolated.
				out, _ := exec.Command("git", "-C", proj, "status", "--porcelain").CombinedOutput()
				if strings.TrimSpace(string(out)) != "" {
					t.Fatalf("main checkout dirty while workers isolated: %q", out)
				}
				if err := h.ApproveNode(ctx, "gate"); err != nil {
					t.Fatalf("approve: %v", err)
				}
				approved = true
				t.Log("gate approved; merge should now run")
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !approved {
		for _, id := range []string{"w1", "w2", "gate", "m"} {
			if st2, _ := h.State(graph.NodeID(id)); st2 != "" {
				t.Logf("at timeout: %s -> %s", id, st2)
			}
		}
		evs, evErr := st.Events(context.Background(), "run-wtmerge")
		if evErr == nil {
			for _, ev := range evs {
				if ev.NodeID != "" {
					t.Logf("event %d %s %s->%s att=%s", ev.Seq, ev.Type, ev.From, ev.To, ev.AttemptID)
				}
			}
		}
		t.Fatal("gate never reached running")
	}
	if !h.Done() {
		t.Fatal("run did not complete after approval")
	}

	if st1, _ := h.State("w1"); st1 != graph.StateDone {
		t.Errorf("w1 = %s, want done", st1)
	}
	if st2, _ := h.State("w2"); st2 != graph.StateDone {
		t.Errorf("w2 = %s, want done", st2)
	}
	if stm, _ := h.State("m"); stm != graph.StateDone {
		t.Errorf("merge = %s, want done", stm)
	}

	// Merged files live in the main checkout; sessions ran in worktrees.
	for _, f := range []string{"alpha.txt", "beta.txt"} {
		data, err := os.ReadFile(filepath.Join(proj, f))
		if err != nil {
			t.Fatalf("%s missing after merge: %v", f, err)
		}
		if len(strings.TrimSpace(string(data))) == 0 {
			t.Fatalf("%s empty after merge", f)
		}
	}
	// Each worker attempt recorded a distinct worktree.
	seen := map[string]bool{}
	for _, id := range []string{"w1", "w2"} {
		atts, _ := st.Attempts(ctx, "run-wtmerge", id)
		if len(atts) != 1 || atts[0].Worktree == "" {
			t.Fatalf("%s worktree not recorded: %+v", id, atts)
		}
		seen[atts[0].Worktree] = true
	}
	if len(seen) != 2 {
		t.Fatalf("workers did not get distinct worktrees: %v", seen)
	}
	// Diff artifacts captured (content-addressed).
	atts, _ := st.Attempts(ctx, "run-wtmerge", "w1")
	arts, _ := st.Artifacts(ctx, "run-wtmerge", atts[0].ID)
	if len(arts) == 0 || arts[0].Name != "diff" || arts[0].Hash == "" {
		t.Fatalf("diff artifact missing: %+v", arts)
	}
}
