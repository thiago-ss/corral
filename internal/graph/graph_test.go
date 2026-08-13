package graph

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func agent(id NodeID, deps ...NodeID) *Node {
	return &Node{
		ID:                 id,
		Type:               NodeAgent,
		Objective:          "do " + string(id),
		AcceptanceCriteria: []string{"criterion for " + string(id)},
		Priority:           PriorityNormal,
		DependsOn:          deps,
		RetryPolicy:        RetryPolicy{MaxRetries: 2, Backoff: time.Second},
		Budget:             Budget{MaxDuration: 10 * time.Minute},
	}
}

func check(id NodeID, deps ...NodeID) *Node {
	n := agent(id, deps...)
	n.Type = NodeCheck
	n.AcceptanceCriteria = nil
	n.Verification = &Verification{Kind: "command", Command: []string{"true"}}
	return n
}

func TestValidateAcceptsValidGraph(t *testing.T) {
	g := &Graph{Nodes: []*Node{agent("a"), agent("b"), agent("c", "a", "b")}}
	if err := Validate(g); err != nil {
		t.Fatalf("valid graph rejected: %v", err)
	}
}

func TestValidateRejectsCycle(t *testing.T) {
	cases := []struct {
		name string
		deps map[NodeID][]NodeID
	}{
		{"direct", map[NodeID][]NodeID{"a": {"b"}, "b": {"a"}}},
		{"indirect", map[NodeID][]NodeID{"a": {"b"}, "b": {"c"}, "c": {"a"}}},
		{"self", map[NodeID][]NodeID{"a": {"a"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var nodes []*Node
			for id, deps := range tc.deps {
				nodes = append(nodes, agent(id, deps...))
			}
			if err := Validate(&Graph{Nodes: nodes}); err == nil {
				t.Fatal("cycle accepted")
			}
		})
	}
}

func TestValidateRejectsMissingDependency(t *testing.T) {
	g := &Graph{Nodes: []*Node{agent("a", "ghost")}}
	if err := Validate(g); err == nil {
		t.Fatal("missing dependency accepted")
	}
}

func TestValidateRejectsDuplicateAndEmptyIDs(t *testing.T) {
	g := &Graph{Nodes: []*Node{agent("a"), agent("a")}}
	if err := Validate(g); err == nil {
		t.Fatal("duplicate id accepted")
	}
	g = &Graph{Nodes: []*Node{agent("")}}
	if err := Validate(g); err == nil {
		t.Fatal("empty id accepted")
	}
}

func TestValidateRejectsUnknownAgentRoleAndUnsafeScope(t *testing.T) {
	for _, tc := range []struct {
		name  string
		role  string
		scope []string
	}{
		{"unknown role", "admin", []string{"a.txt"}},
		{"absolute scope", "worker", []string{"/tmp/a.txt"}},
		{"parent scope", "worker", []string{"../a.txt"}},
		{"wildcard path", "worker", []string{"src/*.go"}},
		{"reviewer scope", "reviewer", []string{"a.txt"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := agent("a")
			n.Role, n.WriteScope = tc.role, tc.scope
			if err := Validate(&Graph{Nodes: []*Node{n}}); err == nil {
				t.Fatal("unsafe agent accepted")
			}
		})
	}
}

func TestValidateRejectsMissingAcceptanceCriteria(t *testing.T) {
	n := agent("a")
	n.AcceptanceCriteria = nil
	g := &Graph{Nodes: []*Node{n}}
	if err := Validate(g); err == nil {
		t.Fatal("agent node without acceptance criteria accepted")
	}
	// Non-agent nodes may omit criteria.
	n2 := check("c")
	if err := Validate(&Graph{Nodes: []*Node{n2}}); err != nil {
		t.Fatalf("check node without criteria rejected: %v", err)
	}
}

func TestValidateRejectsExcessiveFanOut(t *testing.T) {
	var nodes []*Node
	nodes = append(nodes, agent("hub"))
	for i := 0; i <= MaxFanOut; i++ {
		nodes = append(nodes, agent(NodeID(fmt.Sprintf("leaf%d", i)), "hub"))
	}
	if err := Validate(&Graph{Nodes: nodes}); err == nil {
		t.Fatalf("fan-out of %d accepted (limit %d)", MaxFanOut+1, MaxFanOut)
	}
}

func TestValidateRejectsDuplicateDependencyAndBadTypes(t *testing.T) {
	g := &Graph{Nodes: []*Node{agent("a", "b", "b"), agent("b")}}
	if err := Validate(g); err == nil {
		t.Fatal("duplicate dependency accepted")
	}
	n := agent("x")
	n.Type = NodeType("nonsense")
	if err := Validate(&Graph{Nodes: []*Node{n}}); err == nil {
		t.Fatal("invalid node type accepted")
	}
}

func TestTrackerIllegalTransitions(t *testing.T) {
	g := &Graph{Nodes: []*Node{agent("a")}}
	tr, err := NewTracker(g)
	if err != nil {
		t.Fatal(err)
	}
	// Each pair is checked with the tracker genuinely in the source state,
	// reached through legal transitions.
	illegal := [][2]State{
		{StatePending, StateRunning},
		{StatePending, StateVerifying},
		{StatePending, StateDone},
		{StatePending, StateFailed},
		{StateReady, StateRunning},
		{StateReady, StateVerifying},
		{StateReady, StateDone},
		{StateLeased, StateDone},
		{StateLeased, StateVerifying},
		{StateRunning, StateReady},
		{StateRunning, StateDone},
		{StateVerifying, StateReady},
		{StateVerifying, StateLeased},
		{StateVerifying, StateRunning},
		{StateRetryWait, StateRunning},
		{StateRetryWait, StateVerifying},
		{StateBlocked, StateRunning},
		{StateBlocked, StateLeased},
		{StateDone, StateReady},
		{StateDone, StateRunning},
		{StateFailed, StateReady},
		{StateCanceled, StateRunning},
	}
	for _, trn := range illegal {
		t.Run(fmt.Sprintf("%s_to_%s", trn[0], trn[1]), func(t *testing.T) {
			advanceTo(t, tr, "a", trn[0])
			if err := tr.Transit("a", trn[0], trn[1]); err == nil {
				t.Errorf("illegal transition %s -> %s accepted", trn[0], trn[1])
			}
		})
	}
}

// advanceTo walks the tracker to the target state using only legal
// transitions (Set resets to pending between cases).
func advanceTo(t *testing.T, tr *Tracker, id NodeID, to State) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("advance: %v", err)
		}
	}
	if err := tr.Set(id, StatePending); err != nil {
		t.Fatalf("reset: %v", err)
	}
	toVerifying := func() {
		must(tr.Transit(id, StatePending, StateReady))
		must(tr.Transit(id, StateReady, StateLeased))
		must(tr.Transit(id, StateLeased, StateRunning))
		must(tr.Transit(id, StateRunning, StateVerifying))
	}
	switch to {
	case StatePending:
		return
	case StateReady:
		must(tr.Transit(id, StatePending, StateReady))
	case StateLeased:
		must(tr.Transit(id, StatePending, StateReady))
		must(tr.Transit(id, StateReady, StateLeased))
	case StateRunning:
		must(tr.Transit(id, StatePending, StateReady))
		must(tr.Transit(id, StateReady, StateLeased))
		must(tr.Transit(id, StateLeased, StateRunning))
	case StateVerifying:
		toVerifying()
	case StateRetryWait:
		toVerifying()
		must(tr.Transit(id, StateVerifying, StateRetryWait))
	case StateBlocked:
		must(tr.Transit(id, StatePending, StateBlocked))
	case StateDone:
		toVerifying()
		must(tr.Transit(id, StateVerifying, StateDone))
	case StateFailed:
		toVerifying()
		must(tr.Transit(id, StateVerifying, StateFailed))
	case StateCanceled:
		must(tr.Transit(id, StatePending, StateReady))
		must(tr.Transit(id, StateReady, StateLeased))
		must(tr.Transit(id, StateLeased, StateRunning))
		must(tr.Transit(id, StateRunning, StateCanceled))
	}
}

func TestTrackerLifecycle(t *testing.T) {
	g := &Graph{Nodes: []*Node{agent("a")}}
	tr, _ := NewTracker(g)
	steps := []State{StateReady, StateLeased, StateRunning, StateVerifying, StateDone}
	for _, want := range steps {
		if err := tr.Transit("a", "", want); err != nil {
			t.Fatalf("legal transition to %s rejected: %v", want, err)
		}
	}
}

func TestTrackerFromCheck(t *testing.T) {
	g := &Graph{Nodes: []*Node{agent("a")}}
	tr, _ := NewTracker(g)
	if err := tr.Transit("a", StateRunning, StateDone); err == nil {
		t.Fatal("from-check should reject when state is pending")
	}
	if err := tr.Transit("a", StatePending, StateReady); err != nil {
		t.Fatalf("from-check with correct from rejected: %v", err)
	}
	if err := tr.Transit("a", StatePending, StateReady); err == nil {
		t.Fatal("same transition twice should fail on from-check")
	}
}

func TestComputeReadyDeterministicForkJoin(t *testing.T) {
	// Same logical graph, two insertion orders. Ready order must be identical.
	base := []*Node{
		agent("a"),
		agent("b"),
		agent("c", "a", "b"),
		agent("d", "a"),
	}
	orders := [][]*Node{
		base,
		{base[2], base[3], base[0], base[1]},
	}
	var results [][]NodeID
	for _, nodes := range orders {
		g := &Graph{Nodes: nodes}
		tr, err := NewTracker(g)
		if err != nil {
			t.Fatal(err)
		}
		ready, _ := ComputeReady(g, tr)
		var ids []NodeID
		for _, n := range ready {
			ids = append(ids, n.ID)
		}
		results = append(results, ids)
	}
	if !reflect.DeepEqual(results[0], results[1]) {
		t.Fatalf("ready order not deterministic: %v vs %v", results[0], results[1])
	}
	if got, want := results[0], []NodeID{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ready = %v, want %v", got, want)
	}
}

func TestComputeReadyPriorityAndTiebreak(t *testing.T) {
	hi := agent("hi")
	hi.Priority = PriorityHigh
	norm := agent("norm")
	lo := agent("lo")
	lo.Priority = PriorityLow
	g := &Graph{Nodes: []*Node{norm, lo, hi}}
	tr, _ := NewTracker(g)
	ready, _ := ComputeReady(g, tr)
	if got, want := []NodeID{ready[0].ID, ready[1].ID, ready[2].ID}, []NodeID{"hi", "norm", "lo"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("priority order wrong: %v want %v", got, want)
	}
}

func TestComputeReadyFanInWaitsForEveryPredecessor(t *testing.T) {
	g := &Graph{Nodes: []*Node{agent("a"), agent("b"), agent("c", "a", "b")}}
	tr, _ := NewTracker(g)
	// a done, b pending: c must not be ready.
	if err := tr.Set("a", StateDone); err != nil {
		t.Fatal(err)
	}
	ready, _ := ComputeReady(g, tr)
	if len(ready) != 1 || ready[0].ID != "b" {
		t.Fatalf("c became ready with b unfinished; ready=%v", nodeIDs(ready))
	}
	if err := tr.Set("b", StateDone); err != nil {
		t.Fatal(err)
	}
	ready, _ = ComputeReady(g, tr)
	if len(ready) != 1 || ready[0].ID != "c" {
		t.Fatalf("c not ready after all deps done; ready=%v", nodeIDs(ready))
	}
}

func TestComputeReadyBlockedByFailedDependency(t *testing.T) {
	g := &Graph{Nodes: []*Node{agent("a"), agent("b", "a"), agent("c", "b")}}
	tr, _ := NewTracker(g)
	if err := tr.Set("a", StateFailed); err != nil {
		t.Fatal(err)
	}
	ready, blocked := ComputeReady(g, tr)
	if len(ready) != 0 {
		t.Fatalf("ready should be empty, got %v", nodeIDs(ready))
	}
	if len(blocked) != 2 { // b (directly), c (transitively)
		t.Fatalf("blocked = %v, want [b c]", nodeIDs(blocked))
	}
	if blocked[0].ID != "b" || blocked[1].ID != "c" {
		t.Fatalf("blocked order wrong: %v", nodeIDs(blocked))
	}
}

func nodeIDs(ns []*Node) []NodeID {
	out := make([]NodeID, len(ns))
	for i, n := range ns {
		out[i] = n.ID
	}
	return out
}

func TestProposalValidation(t *testing.T) {
	g := &Graph{Nodes: []*Node{agent("a"), agent("b"), agent("c")}}
	good := Proposal{Proposer: "planner-1", Op: OpAddNode, Node: agent("d", "a")}
	if err := ValidateProposal(g, good); err != nil {
		t.Fatalf("valid add rejected: %v", err)
	}
	// Add a node that depends on a missing node.
	badDep := Proposal{Proposer: "planner-1", Op: OpAddNode, Node: agent("e", "ghost")}
	if err := ValidateProposal(g, badDep); err == nil {
		t.Fatal("add with missing dep accepted")
	}
	// Add an edge that creates a cycle: a -> b, b -> c, add c -> a.
	cycGraph := &Graph{Nodes: []*Node{agent("a", "b"), agent("b", "c"), agent("c")}}
	cyc := Proposal{Proposer: "planner-1", Op: OpAddEdge, From: "c", To: "a"}
	if err := ValidateProposal(cycGraph, cyc); err == nil {
		t.Fatal("cycle-introducing edge accepted")
	}
	// Edge to unknown node.
	missing := Proposal{Proposer: "planner-1", Op: OpAddEdge, From: "a", To: "zzz"}
	if err := ValidateProposal(g, missing); err == nil {
		t.Fatal("edge to unknown node accepted")
	}
	// Duplicate edge (c -> a already exists).
	dupGraph := &Graph{Nodes: []*Node{agent("a"), agent("b"), agent("c", "a")}}
	dup := Proposal{Proposer: "planner-1", Op: OpAddEdge, From: "c", To: "a"}
	if err := ValidateProposal(dupGraph, dup); err == nil {
		t.Fatal("duplicate edge accepted")
	}
	// Remove node that is still depended on.
	rmGraph := &Graph{Nodes: []*Node{agent("a"), agent("b", "a")}}
	rm := Proposal{Proposer: "planner-1", Op: OpRemoveNode, Target: "a"}
	if err := ValidateProposal(rmGraph, rm); err == nil {
		t.Fatal("removing depended-on node accepted")
	}
	// Add node missing acceptance criteria.
	noCrit := agent("f")
	noCrit.AcceptanceCriteria = nil
	p := Proposal{Proposer: "planner-1", Op: OpAddNode, Node: noCrit}
	if err := ValidateProposal(g, p); err == nil {
		t.Fatal("agent node without criteria accepted via proposal")
	}
}

type testIdentity struct{ canApply bool }

func (i testIdentity) CanApplyGraphChanges() bool { return i.canApply }

func TestProposalApplicationRequiresIdentity(t *testing.T) {
	g := &Graph{Nodes: []*Node{agent("a"), agent("b")}}

	// An agent (planner) can propose...
	p := Proposal{Proposer: "planner-1", Op: OpAddNode, Node: agent("d", "a")}
	if err := ValidateProposal(g, p); err != nil {
		t.Fatalf("agent proposal rejected: %v", err)
	}
	// ...but cannot apply.
	agentApplier := NewApplier(g, testIdentity{canApply: false})
	if err := agentApplier.Apply(p); err == nil {
		t.Fatal("agent applied graph change without authorization")
	}
	if len(g.Nodes) != 2 {
		t.Fatal("graph mutated by unauthorized apply")
	}

	// The scheduler identity can apply; graph and version change.
	schedApplier := NewApplier(g, testIdentity{canApply: true})
	if err := schedApplier.Apply(p); err != nil {
		t.Fatalf("authorized apply failed: %v", err)
	}
	if len(g.Nodes) != 3 || g.Version != 1 {
		t.Fatalf("graph not updated: nodes=%d version=%d", len(g.Nodes), g.Version)
	}
}

func TestApplierRejectsInvalidMutations(t *testing.T) {
	g := &Graph{Nodes: []*Node{agent("a"), agent("b", "a")}}
	applier := NewApplier(g, testIdentity{canApply: true})
	// Removing node still depended on must fail and leave graph unchanged.
	if err := applier.Apply(Proposal{Proposer: "sched", Op: OpRemoveNode, Target: "a"}); err == nil {
		t.Fatal("removal of depended-on node applied")
	}
	if len(g.Nodes) != 2 {
		t.Fatal("graph mutated by rejected apply")
	}
	// Adding a cycle-introducing edge must fail.
	if err := applier.Apply(Proposal{Proposer: "sched", Op: OpAddEdge, From: "b", To: "a"}); err == nil {
		t.Fatal("cycle-introducing edge applied")
	}
}

func TestSetRestoresOnlyRecoverableStates(t *testing.T) {
	g := &Graph{Nodes: []*Node{agent("a")}}
	tr, _ := NewTracker(g)
	if err := tr.Set("a", StateRunning); err == nil {
		t.Fatal("Set to running accepted; only recoverable states allowed")
	}
	if err := tr.Set("a", StateReady); err != nil {
		t.Fatalf("Set to ready rejected: %v", err)
	}
	if err := tr.Set("a", StateDone); err != nil {
		t.Fatalf("Set to done rejected: %v", err)
	}
}
