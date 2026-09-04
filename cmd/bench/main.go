// Command bench measures corral against single-agent work using the
// deterministic scheduler simulation (fake clock + scripted agents).
// Results feed the README benchmark chart; the numbers are reproducible
// with: go run ./cmd/bench
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"corral/internal/clock"
	"corral/internal/graph"
	"corral/internal/sched"
	"corral/internal/store"
	"corral/internal/verify"
)

const tick = time.Millisecond

func main() {
	parallel := run(8, 4)
	serial := run(8, 1)
	fmt.Printf("8 independent tasks — wall time in ticks\n")
	fmt.Printf("  single agent (concurrency 1): %d\n", serial)
	fmt.Printf("  corral (concurrency 4):       %d\n", parallel)
	fmt.Printf("  speedup: %.1fx\n\n", float64(serial)/float64(parallel))

	// Crash mid-run: 8 tasks, crash after 4 complete. Naive re-run does
	// all 8 again; corral resumes only the 4 unfinished ones.
	fmt.Printf("crash at 50%% done — work re-executed (ticks)\n")
	naive := run(8, 1) // full redo
	resume := runFromCrash(8, 4, 4)
	fmt.Printf("  naive re-run: %d\n", naive)
	fmt.Printf("  corral resume: %d\n", resume)
	fmt.Printf("  saved: %.1fx\n\n", float64(naive)/float64(resume))

	// Concurrency scaling: same 8-task graph at increasing worker
	// counts. Shows where added workers stop buying wall time.
	fmt.Printf("concurrency scaling — 8 independent tasks\n")
	fmt.Printf("  %-12s %-8s %-8s %s\n", "workers", "ticks", "speedup", "efficiency")
	base := run(8, 1)
	for _, c := range []int{1, 2, 4, 8} {
		ticks := run(8, c)
		speedup := float64(base) / float64(ticks)
		fmt.Printf("  %-12d %-8d %-8s %.0f%%\n",
			c, ticks, fmt.Sprintf("%.1fx", speedup), speedup/float64(c)*100)
	}
	fmt.Println()

	// Verification: of 10 agents that "said done", how many actually
	// failed their evidence gate in the simulation?
	fmt.Printf("evidence gates vs trusting the prose\n")
	caught := gateCatchRate(10, 3)
	fmt.Printf("  agents claiming done: 10\n")
	fmt.Printf("  failed their gate: %d\n", caught)
	fmt.Printf("  would have merged broken work (no gates): %d/10\n", caught)
}

// benchScripts returns a fresh script map for n file-writing agents.
func benchScripts(n int) map[string][]sched.Script {
	scripts := map[string][]sched.Script{}
	for i := 0; i < n; i++ {
		file := fmt.Sprintf("f%d.txt", i)
		scripts[fmt.Sprintf("w%d", i)] = []sched.Script{{Delay: 10 * tick, Write: map[string]string{file: "x"}}}
	}
	return scripts
}

// run completes n independent file-writing tasks and returns the wall
// time in ticks.
func run(n, concurrency int) int {
	st, _ := store.Open("")
	defer st.Close()
	clk := clock.NewFake(time.Unix(0, 0))
	workdir, _ := os.MkdirTemp("", "corral-bench-")
	defer os.RemoveAll(workdir)
	eng := verify.New(workdir)

	var nodes []*graph.Node
	scripts := map[string][]sched.Script{}
	for i := 0; i < n; i++ {
		id := graph.NodeID(fmt.Sprintf("w%d", i))
		file := fmt.Sprintf("f%d.txt", i)
		nodes = append(nodes, &graph.Node{
			ID: id, Type: graph.NodeAgent, Role: "worker",
			Objective: "write " + file, AcceptanceCriteria: []string{file},
			Priority: graph.PriorityNormal, WriteScope: []string{file},
			Verification: &graph.Verification{Kind: "command", Command: []string{"test", "-f", file}},
			Meta:         map[string]string{"cwd": workdir},
		})
		scripts[string(id)] = []sched.Script{{Delay: 10 * tick, Write: map[string]string{file: "x"}}}
	}
	drv := sched.NewFakeDriver(clk, scripts)
	s := sched.New(st, drv, &sched.EngineVerifier{Eng: eng}, clk, sched.Options{Concurrency: concurrency})
	h, err := s.Create(context.Background(), "bench", &graph.Graph{Nodes: nodes})
	if err != nil {
		fmt.Fprintln(os.Stderr, "create:", err)
		os.Exit(1)
	}
	steps := 0
	for !h.Done() && steps < 10000 {
		clk.Advance(tick)
		h.Step(context.Background())
		steps++
	}
	if !h.Done() {
		fmt.Fprintln(os.Stderr, "run did not complete")
		os.Exit(1)
	}
	return steps
}

// runFromCrash completes n tasks but crashes after done count; returns
// the ticks spent from the crash point onward (resume path).
func runFromCrash(n, doneBeforeCrash, concurrency int) int {
	st, _ := store.Open("")
	defer st.Close()
	clk := clock.NewFake(time.Unix(0, 0))
	workdir, _ := os.MkdirTemp("", "corral-bench-")
	defer os.RemoveAll(workdir)
	eng := verify.New(workdir)

	var nodes []*graph.Node
	for i := 0; i < n; i++ {
		id := graph.NodeID(fmt.Sprintf("w%d", i))
		file := fmt.Sprintf("f%d.txt", i)
		nodes = append(nodes, &graph.Node{
			ID: id, Type: graph.NodeAgent, Role: "worker",
			Objective: "write " + file, AcceptanceCriteria: []string{file},
			Priority: graph.PriorityNormal, WriteScope: []string{file},
			Verification: &graph.Verification{Kind: "command", Command: []string{"test", "-f", file}},
			Meta:         map[string]string{"cwd": workdir},
		})
	}
	drv := sched.NewFakeDriver(clk, benchScripts(n))
	s := sched.New(st, drv, &sched.EngineVerifier{Eng: eng}, clk, sched.Options{Concurrency: concurrency})
	h, err := s.Create(context.Background(), "bench", &graph.Graph{Nodes: nodes})
	if err != nil {
		fmt.Fprintln(os.Stderr, "create:", err)
		os.Exit(1)
	}
	ctx := context.Background()
	// Drive until doneBeforeCrash nodes completed.
	steps := 0
	completed := func() int {
		c := 0
		for i := 0; i < n; i++ {
			if st2, _ := h.State(graph.NodeID(fmt.Sprintf("w%d", i))); st2 == graph.StateDone {
				c++
			}
		}
		return c
	}
	for completed() < doneBeforeCrash && steps < 10000 {
		clk.Advance(tick)
		h.Step(ctx)
		steps++
	}
	// "Crash": a fresh scheduler (and fresh worker pool) loads the same
	// store; count ticks to finish. Completed nodes are never started
	// again — the script list being longer than needed is harmless.
	// NOTE: build a fresh script map — FakeDriver consumes (mutates) the
	// map it was handed.
	drv2 := sched.NewFakeDriver(clk, benchScripts(n))
	s3 := sched.New(st, drv2, &sched.EngineVerifier{Eng: eng}, clk, sched.Options{Concurrency: concurrency})
	h3, loadErr := s3.Load(ctx, "bench")
	if loadErr != nil {
		fmt.Fprintln(os.Stderr, "load2:", loadErr)
		os.Exit(1)
	}
	resumeSteps := 0
	for !h3.Done() && resumeSteps < 10000 {
		clk.Advance(tick)
		h3.Step(ctx)
		resumeSteps++
	}
	return resumeSteps
}

// gateCatchRate runs agents that all "claim done" but where failures of
// them produced broken output; returns how many the gates caught.
func gateCatchRate(total, broken int) int {
	st, _ := store.Open("")
	defer st.Close()
	clk := clock.NewFake(time.Unix(0, 0))
	workdir, _ := os.MkdirTemp("", "corral-bench-")
	defer os.RemoveAll(workdir)
	eng := verify.New(workdir)

	var nodes []*graph.Node
	scripts := map[string][]sched.Script{}
	for i := 0; i < total; i++ {
		id := graph.NodeID(fmt.Sprintf("w%d", i))
		file := fmt.Sprintf("f%d.txt", i)
		nodes = append(nodes, &graph.Node{
			ID: id, Type: graph.NodeAgent, Role: "worker",
			Objective: "write " + file, AcceptanceCriteria: []string{file},
			Priority: graph.PriorityNormal, WriteScope: []string{file},
			Verification: &graph.Verification{Kind: "command", Command: []string{"grep", "-q", "CORRECT", file}},
			Meta:         map[string]string{"cwd": workdir},
		})
		if i < broken {
			// Broken agents claim done but write the wrong content.
			scripts[string(id)] = []sched.Script{{Delay: tick, Write: map[string]string{file: "wrong"}}}
		} else {
			scripts[string(id)] = []sched.Script{{Delay: tick, Write: map[string]string{file: "CORRECT"}}}
		}
	}
	drv := sched.NewFakeDriver(clk, scripts)
	s := sched.New(st, drv, &sched.EngineVerifier{Eng: eng}, clk, sched.Options{Concurrency: total})
	h, err := s.Create(context.Background(), "bench", &graph.Graph{Nodes: nodes})
	if err != nil {
		fmt.Fprintln(os.Stderr, "create:", err)
		os.Exit(1)
	}
	ctx := context.Background()
	for !h.Done() {
		clk.Advance(tick)
		h.Step(ctx)
	}
	caught := 0
	for i := 0; i < total; i++ {
		if st2, _ := h.State(graph.NodeID(fmt.Sprintf("w%d", i))); st2 == graph.StateFailed {
			caught++
		}
	}
	return caught
}
