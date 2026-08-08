package graph

import (
	"fmt"
	"sync"
)

// State is the lifecycle state of a single node.
type State string

const (
	StatePending   State = "pending"    // initial; waiting for dependencies
	StateReady     State = "ready"      // dependencies satisfied; schedulable
	StateLeased    State = "leased"     // claimed by a worker lease
	StateRunning   State = "running"    // attempt in flight
	StateVerifying State = "verifying"  // evidence gate in progress
	StateDone      State = "done"       // terminal: evidence passed
	StateRetryWait State = "retry_wait" // backoff before retry
	StateBlocked   State = "blocked"    // waiting on human intervention
	StateFailed    State = "failed"     // terminal: exhausted or unrecoverable
	StateCanceled  State = "canceled"   // terminal: aborted by operator
)

func (s State) Terminal() bool {
	return s == StateDone || s == StateFailed || s == StateCanceled
}

func (s State) Active() bool {
	return s == StateLeased || s == StateRunning || s == StateVerifying || s == StateRetryWait
}

// transitions is the legal transition table. Every transition not listed is
// illegal and rejected by Tracker.Transit.
var transitions = map[State]map[State]bool{
	StatePending: {
		StateReady:    true,
		StateBlocked:  true,
		StateCanceled: true,
	},
	StateReady: {
		StateLeased:   true,
		StateBlocked:  true,
		StateCanceled: true,
	},
	StateLeased: {
		StateRunning:  true, // worker starts the attempt
		StateReady:    true, // lease expired before start
		StateBlocked:  true,
		StateCanceled: true,
	},
	StateRunning: {
		StateVerifying: true, // execution finished; evidence gate starts
		StateBlocked:   true, // e.g. permission request pending
		StateFailed:    true, // execution error
		StateCanceled:  true,
	},
	StateVerifying: {
		StateDone:      true, // evidence passed
		StateRetryWait: true, // evidence failed; retry scheduled
		StateFailed:    true, // evidence failed permanently / budget exhausted
		StateBlocked:   true,
	},
	StateRetryWait: {
		StateReady:    true, // backoff elapsed
		StateBlocked:  true,
		StateFailed:   true,
		StateCanceled: true,
	},
	StateBlocked: {
		StateReady:    true, // human resolved: retry
		StateCanceled: true, // human kills the node
	},
}

// Tracker holds the state of every node in a run and enforces the state
// machine. It is not persisted; Task 2 replaces its backing store with
// SQLite and leases.
type Tracker struct {
	mu    sync.Mutex
	state map[NodeID]State
}

func NewTracker(g *Graph) (*Tracker, error) {
	if err := Validate(g); err != nil {
		return nil, err
	}
	t := &Tracker{state: map[NodeID]State{}}
	for _, n := range g.Nodes {
		t.state[n.ID] = StatePending
	}
	return t, nil
}

func (t *Tracker) State(id NodeID) (State, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, ok := t.state[id]
	return s, ok
}

// Transit moves a node from wantFrom to to. wantFrom "" skips the from-check
// (used by authoritative external events such as crash recovery).
func (t *Tracker) Transit(id NodeID, wantFrom, to State) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	from, ok := t.state[id]
	if !ok {
		return fmt.Errorf("state: unknown node %q", id)
	}
	if wantFrom != "" && from != wantFrom {
		return fmt.Errorf("state: illegal transition %s %q -> %q (current state is %q)", id, wantFrom, to, from)
	}
	if !transitions[from][to] {
		return fmt.Errorf("state: illegal transition %s %q -> %q", id, from, to)
	}
	t.state[id] = to
	return nil
}

// Set places a node into a state without a from-check. Intended for
// reconciliation during restart only.
func (t *Tracker) Set(id NodeID, to State) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.state[id]; !ok {
		return fmt.Errorf("state: unknown node %q", id)
	}
	if to.Terminal() || to == StatePending || to == StateReady || to == StateBlocked {
		t.state[id] = to
		return nil
	}
	return fmt.Errorf("state: cannot restore node %q to non-recoverable state %q", id, to)
}
