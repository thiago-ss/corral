package graph

import "fmt"

// Proposals are the only way to change a graph after approval. Agents and
// other sessions can create and validate proposals, but only an authorized
// applier may apply them: the graph never mutates itself.
type Op string

const (
	OpAddNode     Op = "add_node"
	OpRemoveNode  Op = "remove_node"
	OpAddEdge     Op = "add_edge"
	OpRemoveEdge  Op = "remove_edge"
	OpSetPriority Op = "set_priority"
	OpSetRetry    Op = "set_retry"
	OpSetBudget   Op = "set_budget"
)

type Proposal struct {
	Proposer string // identity of the session proposing the change
	Reason   string
	Op       Op
	Node     *Node  // for OpAddNode
	Target   NodeID // for the other ops
	From, To NodeID // for edge ops
	Priority Priority
	Retry    RetryPolicy
	Budget   Budget
}

// CanApplyGraphChanges marks identities allowed to apply proposals. The
// scheduler is the only holder in a run; agent sessions never get one.
type Identity interface {
	CanApplyGraphChanges() bool
}

// Applier mutates a graph. Implementations are expected to re-validate the
// whole graph after each application.
type Applier interface {
	Apply(p Proposal) error
}

// NewApplier returns the single enforcement point for the rule "agents may
// propose graph changes but cannot apply them directly": Apply succeeds
// only when who.CanApplyGraphChanges() is true. The scheduler holds the
// authorized identity; agent sessions never do. Not safe for concurrent
// use; the scheduler serializes applications.
func NewApplier(g *Graph, who Identity) Applier {
	return &gatedApplier{g: g, who: who}
}

type gatedApplier struct {
	g   *Graph
	who Identity
}

func (a *gatedApplier) Apply(p Proposal) error {
	if !a.who.CanApplyGraphChanges() {
		return fmt.Errorf("proposal: %s is not authorized to apply graph changes", p.Proposer)
	}
	if err := ValidateProposal(a.g, p); err != nil {
		return err
	}
	work := clone(a.g)
	switch p.Op {
	case OpAddNode:
		work.Nodes = append(work.Nodes, p.Node)
	case OpRemoveNode:
		kept := work.Nodes[:0]
		for _, n := range work.Nodes {
			if n.ID != p.Target {
				kept = append(kept, n)
			}
		}
		work.Nodes = kept
	case OpAddEdge:
		from, _ := nodeByID(work, p.From)
		from.DependsOn = append(from.DependsOn, p.To)
	case OpRemoveEdge:
		from, _ := nodeByID(work, p.From)
		for i, dep := range from.DependsOn {
			if dep == p.To {
				from.DependsOn = append(from.DependsOn[:i], from.DependsOn[i+1:]...)
				break
			}
		}
	case OpSetPriority:
		n, _ := nodeByID(work, p.Target)
		n.Priority = p.Priority
	case OpSetRetry:
		n, _ := nodeByID(work, p.Target)
		n.RetryPolicy = p.Retry
	case OpSetBudget:
		n, _ := nodeByID(work, p.Target)
		n.Budget = p.Budget
	}
	if err := Validate(work); err != nil {
		return fmt.Errorf("proposal: application rejected: %w", err)
	}
	a.g.Nodes = work.Nodes
	a.g.Version++
	return nil
}

// ValidateProposal checks a proposal structurally against the current graph.
// It does not require an authorized identity: proposing is open to anyone.
func ValidateProposal(g *Graph, p Proposal) error {
	if p.Proposer == "" {
		return fmt.Errorf("proposal: missing proposer")
	}
	switch p.Op {
	case OpAddNode:
		if p.Node == nil {
			return fmt.Errorf("proposal: add_node requires a node")
		}
		for _, n := range g.Nodes {
			if n.ID == p.Node.ID {
				return fmt.Errorf("proposal: node %s already exists", p.Node.ID)
			}
		}
		if err := validateNode(p.Node); err != nil {
			return fmt.Errorf("proposal: %w", err)
		}
		for _, dep := range p.Node.DependsOn {
			if _, ok := nodeByID(g, dep); !ok {
				return fmt.Errorf("proposal: node %s depends on missing node %s", p.Node.ID, dep)
			}
		}
		return nil
	case OpRemoveNode:
		if _, ok := nodeByID(g, p.Target); !ok {
			return fmt.Errorf("proposal: unknown node %s", p.Target)
		}
		for _, n := range g.Nodes {
			for _, dep := range n.DependsOn {
				if dep == p.Target {
					return fmt.Errorf("proposal: node %s is depended on by %s; remove the edge first", p.Target, n.ID)
				}
			}
		}
		return nil
	case OpAddEdge:
		from, ok1 := nodeByID(g, p.From)
		_, ok2 := nodeByID(g, p.To)
		if !ok1 || !ok2 {
			return fmt.Errorf("proposal: edge endpoints unknown (%s -> %s)", p.From, p.To)
		}
		if p.From == p.To {
			return fmt.Errorf("proposal: self-edge on %s", p.From)
		}
		for _, dep := range from.DependsOn {
			if dep == p.To {
				return fmt.Errorf("proposal: edge %s -> %s already exists", p.From, p.To)
			}
		}
		if fanOut(g, p.To) >= MaxFanOut {
			return fmt.Errorf("proposal: fan-out of %s would exceed %d", p.To, MaxFanOut)
		}
		// Applying the edge must not create a cycle.
		fork := clone(g)
		f, _ := nodeByID(fork, p.From)
		f.DependsOn = append(f.DependsOn, p.To)
		if err := Validate(fork); err != nil {
			return fmt.Errorf("proposal: edge %s -> %s rejected: %w", p.From, p.To, err)
		}
		return nil
	case OpRemoveEdge:
		from, ok1 := nodeByID(g, p.From)
		if !ok1 {
			return fmt.Errorf("proposal: unknown node %s", p.From)
		}
		for _, dep := range from.DependsOn {
			if dep == p.To {
				return nil
			}
		}
		return fmt.Errorf("proposal: edge %s -> %s does not exist", p.From, p.To)
	case OpSetPriority:
		if _, ok := nodeByID(g, p.Target); !ok {
			return fmt.Errorf("proposal: unknown node %s", p.Target)
		}
		if p.Priority < 0 {
			return fmt.Errorf("proposal: negative priority")
		}
		return nil
	case OpSetRetry:
		if _, ok := nodeByID(g, p.Target); !ok {
			return fmt.Errorf("proposal: unknown node %s", p.Target)
		}
		if p.Retry.MaxRetries < 0 || p.Retry.Backoff < 0 {
			return fmt.Errorf("proposal: negative retry policy")
		}
		return nil
	case OpSetBudget:
		if _, ok := nodeByID(g, p.Target); !ok {
			return fmt.Errorf("proposal: unknown node %s", p.Target)
		}
		if p.Budget.MaxDuration < 0 || p.Budget.MaxTokens < 0 || p.Budget.MaxCost < 0 {
			return fmt.Errorf("proposal: negative budget")
		}
		return nil
	default:
		return fmt.Errorf("proposal: unknown op %q", p.Op)
	}
}

func nodeByID(g *Graph, id NodeID) (*Node, bool) {
	for _, n := range g.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return nil, false
}

func fanOut(g *Graph, id NodeID) int {
	n := 0
	for _, node := range g.Nodes {
		for _, dep := range node.DependsOn {
			if dep == id {
				n++
			}
		}
	}
	return n
}

func clone(g *Graph) *Graph {
	out := &Graph{Version: g.Version}
	for _, n := range g.Nodes {
		cp := *n
		cp.DependsOn = append([]NodeID(nil), n.DependsOn...)
		cp.AcceptanceCriteria = append([]string(nil), n.AcceptanceCriteria...)
		cp.InputArtifacts = append([]ArtifactRef(nil), n.InputArtifacts...)
		cp.OutputArtifacts = append([]ArtifactRef(nil), n.OutputArtifacts...)
		cp.WriteScope = append([]string(nil), n.WriteScope...)
		if n.Verification != nil {
			v := *n.Verification
			v.Command = append([]string(nil), n.Verification.Command...)
			cp.Verification = &v
		}
		out.Nodes = append(out.Nodes, &cp)
	}
	return out
}
