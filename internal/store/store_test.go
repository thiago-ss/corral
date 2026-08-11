package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"corral/internal/graph"
)

func testGraph(t *testing.T) *graph.Graph {
	t.Helper()
	return &graph.Graph{Nodes: []*graph.Node{
		{ID: "a", Type: graph.NodeAgent, Objective: "o", AcceptanceCriteria: []string{"c"}, Priority: graph.PriorityNormal},
		{ID: "b", Type: graph.NodeAgent, Objective: "o", AcceptanceCriteria: []string{"c"}, Priority: graph.PriorityNormal, DependsOn: []graph.NodeID{"a"}},
	}}
}

func open(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func now() time.Time { return time.UnixMilli(1786000000000) }

func TestCreateRunAndReplay(t *testing.T) {
	st := open(t)
	ctx := context.Background()
	if err := st.CreateRun(ctx, "r1", testGraph(t), false, now()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendTransition(ctx, "r1", "a", graph.StatePending, graph.StateReady, "", now()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendTransition(ctx, "r1", "a", graph.StateReady, graph.StateLeased, "", now()); err != nil {
		t.Fatal(err)
	}
	evs, err := st.Events(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 { // run + 2 transitions
		t.Fatalf("events = %d, want 3", len(evs))
	}
	if evs[0].Seq != 1 || evs[1].Seq != 2 || evs[2].Seq != 3 {
		t.Fatalf("seq not monotonic: %d %d %d", evs[0].Seq, evs[1].Seq, evs[2].Seq)
	}
	if evs[1].From != graph.StatePending || evs[1].To != graph.StateReady {
		t.Fatalf("transition wrong: %s -> %s", evs[1].From, evs[1].To)
	}
	states, err := st.NodeStates(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if states["a"] != graph.StateLeased {
		t.Fatalf("materialized a = %s, want leased", states["a"])
	}
	if states["b"] != graph.StatePending {
		t.Fatalf("materialized b = %s, want pending", states["b"])
	}
}

func TestLeaseAtomicity(t *testing.T) {
	st := open(t)
	ctx := context.Background()
	if err := st.CreateRun(ctx, "r1", testGraph(t), false, now()); err != nil {
		t.Fatal(err)
	}
	ok, err := st.AcquireLease(ctx, "r1", "a", "h1", time.Minute, now())
	if err != nil || !ok {
		t.Fatalf("first lease failed: ok=%v err=%v", ok, err)
	}
	// Second holder must be rejected while held.
	ok, err = st.AcquireLease(ctx, "r1", "a", "h2", time.Minute, now())
	if err != nil || ok {
		t.Fatalf("second holder leased: ok=%v err=%v", ok, err)
	}
	// A different node is unaffected.
	ok, err = st.AcquireLease(ctx, "r1", "b", "h2", time.Minute, now())
	if err != nil || !ok {
		t.Fatalf("unrelated node not leasable: ok=%v err=%v", ok, err)
	}
	// After expiry, a new holder may lease.
	ok, err = st.AcquireLease(ctx, "r1", "a", "h2", time.Minute, now().Add(2*time.Minute))
	if err != nil || !ok {
		t.Fatalf("expired lease not reusable: ok=%v err=%v", ok, err)
	}
	// Release by a non-holder must not clear the lease.
	if err := st.ReleaseLease(ctx, "r1", "b", "nobody", now()); err != nil {
		t.Fatal(err)
	}
	ok, err = st.AcquireLease(ctx, "r1", "b", "h3", time.Minute, now())
	if err != nil || ok {
		t.Fatalf("lease cleared by wrong holder: ok=%v err=%v", ok, err)
	}
	// Release by the holder frees the node.
	if err := st.ReleaseLease(ctx, "r1", "b", "h2", now()); err != nil {
		t.Fatal(err)
	}
	ok, err = st.AcquireLease(ctx, "r1", "b", "h3", time.Minute, now())
	if err != nil || !ok {
		t.Fatalf("holder release did not free node: ok=%v err=%v", ok, err)
	}
}

func TestAttemptsUniquePerNode(t *testing.T) {
	st := open(t)
	ctx := context.Background()
	if err := st.CreateRun(ctx, "r1", testGraph(t), false, now()); err != nil {
		t.Fatal(err)
	}
	a := Attempt{ID: "a/1", RunID: "r1", NodeID: "a", No: 1, Status: "running"}
	if err := st.RecordAttempt(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordAttempt(ctx, Attempt{ID: "a/2", RunID: "r1", NodeID: "a", No: 2, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	// Duplicate attempt number for the same node must fail.
	if err := st.RecordAttempt(ctx, Attempt{ID: "a/2b", RunID: "r1", NodeID: "a", No: 2, Status: "running"}); err == nil {
		t.Fatal("duplicate (node, no) accepted")
	}
	atts, err := st.Attempts(ctx, "r1", "a")
	if err != nil {
		t.Fatal(err)
	}
	if len(atts) != 2 || atts[0].ID != "a/1" || atts[1].ID != "a/2" {
		t.Fatalf("attempts wrong: %+v", atts)
	}
}

// TestMigrateAddsAutoApproveColumn opens a database created with the
// pre-auto-approve schema and verifies the column is added, so existing
// deployments keep working after an upgrade.
func TestMigrateAddsAutoApproveColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	oldDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldDB.Exec(`CREATE TABLE runs (
		id TEXT PRIMARY KEY,
		graph TEXT NOT NULL,
		status TEXT NOT NULL,
		created_at INTEGER NOT NULL
	);`); err != nil {
		t.Fatal(err)
	}
	oldDB.Close()

	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	ctx := context.Background()
	if err := st.CreateRun(ctx, "r1", testGraph(t), now()); err != nil {
		t.Fatal(err)
	}
	ru, err := st.Run(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if ru.AutoApproveGates {
		t.Fatal("migrated run should default autoApproveGates to false")
	}
}

func TestAutoApproveGatesPersisted(t *testing.T) {
	st := open(t)
	ctx := context.Background()
	// Default run: flag off.
	if err := st.CreateRun(ctx, "off", testGraph(t), now()); err != nil {
		t.Fatal(err)
	}
	// Explicit run: flag on.
	if err := st.CreateRunWithOpts(ctx, "on", testGraph(t), true, now()); err != nil {
		t.Fatal(err)
	}
	off, err := st.Run(ctx, "off")
	if err != nil {
		t.Fatal(err)
	}
	if off.AutoApproveGates {
		t.Fatal("default run has autoApproveGates set")
	}
	on, err := st.Run(ctx, "on")
	if err != nil {
		t.Fatal(err)
	}
	if !on.AutoApproveGates {
		t.Fatal("autoApproveGates not persisted on the run")
	}
	// ListRuns carries the flag too.
	runs, err := st.ListRuns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Fatalf("runs = %d, want 2", len(runs))
	}
	for _, r := range runs {
		want := r.ID == "on"
		if r.AutoApproveGates != want {
			t.Fatalf("run %s autoApproveGates = %v, want %v", r.ID, r.AutoApproveGates, want)
		}
	}
}

func TestMarkInterrupted(t *testing.T) {
	st := open(t)
	ctx := context.Background()
	if err := st.CreateRun(ctx, "r1", testGraph(t), false, now()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a/1", "b/1"} {
		if err := st.RecordAttempt(ctx, Attempt{ID: id, RunID: "r1", NodeID: string(id[0]), No: 1, Status: "running"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.RecordAttempt(ctx, Attempt{ID: "a/0", RunID: "r1", NodeID: "a", No: 0, Status: "done"}); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkInterrupted(ctx, "r1", "a"); err != nil {
		t.Fatal(err)
	}
	atts, _ := st.Attempts(ctx, "r1", "a")
	for _, at := range atts {
		if at.ID == "a/0" && at.Status != "done" {
			t.Fatalf("completed attempt flipped: %+v", at)
		}
	}
	for _, at := range atts {
		if at.ID == "a/1" && at.Status != "interrupted" {
			t.Fatalf("running attempt not interrupted: %+v", at)
		}
	}
}
