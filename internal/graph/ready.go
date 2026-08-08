package graph

import "sort"

// ComputeReady returns nodes that may be scheduled now, in deterministic
// order: priority (higher first), then node ID. A node is ready when it is
// pending and every dependency is done.
//
// Nodes that can never run are returned in blocked: a pending node whose
// dependency chain contains a failed or canceled state (propagated
// transitively). Both slices are duplicate-free and stable for the same
// graph+states, regardless of node insertion order.
func ComputeReady(g *Graph, t *Tracker) (ready []*Node, blocked []*Node) {
	byID := map[NodeID]*Node{}
	for _, n := range g.Nodes {
		byID[n.ID] = n
	}
	stateOf := func(id NodeID) (State, bool) { return t.State(id) }

	// dead reports whether the dependency chain rooted at id ends in
	// failure/cancellation (or a missing node), meaning id can never be
	// satisfied even if other deps complete.
	deadMemo := map[NodeID]bool{}
	var dead func(id NodeID) bool
	dead = func(id NodeID) bool {
		if v, ok := deadMemo[id]; ok {
			return v
		}
		s, ok := stateOf(id)
		if !ok {
			deadMemo[id] = true
			return true
		}
		// Blocked counts as un-completable for dependencies: the dependent
		// cannot run until the block is resolved manually.
		if s == StateFailed || s == StateCanceled || s == StateBlocked {
			deadMemo[id] = true
			return true
		}
		n, ok := byID[id]
		if !ok {
			deadMemo[id] = true
			return true
		}
		for _, dep := range n.DependsOn {
			if dead(dep) {
				deadMemo[id] = true
				return true
			}
		}
		deadMemo[id] = false
		return false
	}

	for _, n := range g.Nodes {
		s, ok := stateOf(n.ID)
		if !ok || (s != StatePending && s != StateReady) {
			continue
		}
		unrunnable := false
		for _, dep := range n.DependsOn {
			ds, ok := stateOf(dep)
			if !ok {
				unrunnable = true
				break
			}
			if ds == StateFailed || ds == StateCanceled || ds == StateBlocked {
				unrunnable = true
				break
			}
			if ds != StateDone {
				unrunnable = true
				break
			}
		}
		if unrunnable {
			if dead(n.ID) {
				blocked = append(blocked, n)
			}
			continue
		}
		ready = append(ready, n)
	}
	order := func(ss []*Node) {
		sort.SliceStable(ss, func(i, j int) bool {
			if ss[i].Priority != ss[j].Priority {
				return ss[i].Priority > ss[j].Priority
			}
			return ss[i].ID < ss[j].ID
		})
	}
	order(ready)
	order(blocked)
	return ready, blocked
}
