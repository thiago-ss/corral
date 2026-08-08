package graph

import (
	"fmt"
	"sort"
	"strings"
)

// Validate checks the whole graph for structural errors:
// unknown/missing/duplicate dependencies, cycles, duplicate or empty node
// IDs, missing acceptance criteria on agent nodes, excessive fan-out, and
// content limits.
func Validate(g *Graph) error {
	if g == nil {
		return fmt.Errorf("graph: nil graph")
	}
	if len(g.Nodes) > MaxNodes {
		return fmt.Errorf("graph: %d nodes exceeds limit %d", len(g.Nodes), MaxNodes)
	}
	byID := map[NodeID]*Node{}
	fanIn := map[NodeID]int{} // number of dependents per node
	for i, n := range g.Nodes {
		if err := validateNode(n); err != nil {
			return fmt.Errorf("graph: node %s: %w", nodeLabel(n, i), err)
		}
		if prev, dup := byID[n.ID]; dup {
			return fmt.Errorf("graph: duplicate node id %q (at %s)", n.ID, prev.ID)
		}
		byID[n.ID] = n
	}
	for _, n := range g.Nodes {
		seen := map[NodeID]bool{}
		for _, dep := range n.DependsOn {
			if dep == n.ID {
				return fmt.Errorf("graph: node %s depends on itself", n.ID)
			}
			if _, ok := byID[dep]; !ok {
				return fmt.Errorf("graph: node %s depends on missing node %q", n.ID, dep)
			}
			if seen[dep] {
				return fmt.Errorf("graph: node %s has duplicate dependency %q", n.ID, dep)
			}
			seen[dep] = true
			fanIn[dep]++
			if fanIn[dep] > MaxFanOut {
				return fmt.Errorf("graph: node %s fan-out %d exceeds limit %d", dep, fanIn[dep], MaxFanOut)
			}
		}
	}
	if err := detectCycles(g, byID); err != nil {
		return err
	}
	return nil
}

func validateNode(n *Node) error {
	if n == nil {
		return fmt.Errorf("nil node")
	}
	if n.ID == "" {
		return fmt.Errorf("empty node id")
	}
	switch n.Type {
	case NodeAgent, NodeCheck, NodeMerge, NodeHuman:
	default:
		return fmt.Errorf("invalid node type %q", n.Type)
	}
	if len(n.Objective) > maxObjectiveLen {
		return fmt.Errorf("objective exceeds %d chars", maxObjectiveLen)
	}
	if n.Type == NodeAgent {
		if len(n.AcceptanceCriteria) == 0 {
			return fmt.Errorf("agent node missing acceptance criteria")
		}
		for _, c := range n.AcceptanceCriteria {
			if strings.TrimSpace(c) == "" {
				return fmt.Errorf("empty acceptance criterion")
			}
			if len(c) > maxCriteriaLen {
				return fmt.Errorf("criterion exceeds %d chars", maxCriteriaLen)
			}
		}
		if len(n.AcceptanceCriteria) > maxCriteriaCount {
			return fmt.Errorf("%d acceptance criteria exceeds limit %d", len(n.AcceptanceCriteria), maxCriteriaCount)
		}
	}
	if n.Type == NodeMerge {
		if n.Verification == nil || n.Verification.Kind != "command" {
			return fmt.Errorf("merge node requires a command verification (post-merge checks)")
		}
	}
	if n.Type == NodeCheck {
		if n.Verification == nil || n.Verification.Kind != "command" {
			return fmt.Errorf("check node requires a command verification")
		}
	}
	if n.Priority < 0 {
		return fmt.Errorf("negative priority")
	}
	if n.RetryPolicy.MaxRetries < 0 {
		return fmt.Errorf("negative max retries")
	}
	if n.RetryPolicy.Backoff < 0 {
		return fmt.Errorf("negative backoff")
	}
	if n.Budget.MaxDuration < 0 || n.Budget.MaxTokens < 0 || n.Budget.MaxCost < 0 {
		return fmt.Errorf("negative budget")
	}
	if v := n.Verification; v != nil {
		switch v.Kind {
		case "command", "json_schema", "reviewer":
		default:
			return fmt.Errorf("invalid verification kind %q", v.Kind)
		}
		if v.Kind == "command" && len(v.Command) == 0 {
			return fmt.Errorf("command verification missing command")
		}
		if v.Kind == "reviewer" && v.Reviewer == "" {
			return fmt.Errorf("reviewer verification missing reviewer node")
		}
	}
	return nil
}

func nodeLabel(n *Node, i int) string {
	if n == nil {
		return fmt.Sprintf("index %d", i)
	}
	return string(n.ID)
}

// detectCycles runs DFS-based cycle detection and returns a deterministic
// error naming the first cycle found (stable node order).
func detectCycles(g *Graph, byID map[NodeID]*Node) error {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	order := make([]*Node, len(g.Nodes))
	copy(order, g.Nodes)
	sort.SliceStable(order, func(i, j int) bool { return order[i].ID < order[j].ID })

	color := map[NodeID]int{}
	stack := []NodeID{}
	var visit func(n *Node) error
	visit = func(n *Node) error {
		color[n.ID] = gray
		stack = append(stack, n.ID)
		deps := append([]NodeID(nil), n.DependsOn...)
		sort.Slice(deps, func(i, j int) bool { return deps[i] < deps[j] })
		for _, dep := range deps {
			switch color[dep] {
			case white:
				if err := visit(byID[dep]); err != nil {
					return err
				}
			case gray:
				idx := 0
				for i, id := range stack {
					if id == dep {
						idx = i
						break
					}
				}
				cycle := append(append([]NodeID{}, stack[idx:]...), dep)
				return fmt.Errorf("graph: dependency cycle: %s", strings.Join(ids(cycle), " -> "))
			}
		}
		stack = stack[:len(stack)-1]
		color[n.ID] = black
		return nil
	}
	for _, n := range order {
		if color[n.ID] == white {
			if err := visit(n); err != nil {
				return err
			}
		}
	}
	return nil
}

func ids(ss []NodeID) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = string(s)
	}
	return out
}
