package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"corral/internal/graph"
	"corral/internal/ocx"
)

// OpenCodePlanner turns a goal statement into a validated corral graph by
// asking a read-only planner agent session to emit the graph as JSON.
// The session runs without execution tools (no bash/edit), the response
// is parsed progressively while it streams, and failed sessions are
// aborted so nothing keeps burning tokens.
type OpenCodePlanner struct {
	oc      *ocx.Client
	Model   string
	Timeout time.Duration
}

// NewOpenCodePlanner wires a planner to an OpenCode client.
func NewOpenCodePlanner(oc *ocx.Client, model string, timeout time.Duration) *OpenCodePlanner {
	return &OpenCodePlanner{oc: oc, Model: model, Timeout: timeout}
}

// planTools keeps the planner read-only: reading the repo is fine,
// anything that executes or writes is disabled. This prevents tool loops
// that stall planning.
var planTools = map[string]bool{
	"bash":        false,
	"edit":        false,
	"write":       false,
	"apply_patch": false,
	"webfetch":    false,
	"websearch":   false,
	"task":        false,
	"todowrite":   false,
	"question":    false,
	"skill":       false,
	"lsp":         false,
}

var errNoGraph = errors.New("planner produced no valid graph")

// Plan runs up to two attempts: the first with the normal prompt, a
// stricter retry only when the model answered but its JSON was
// unparseable. Timeouts and session errors fail fast (no retry), so a
// slow model cannot double the wait.
func (p *OpenCodePlanner) Plan(ctx context.Context, goal string) (*graph.Graph, error) {
	if p.Timeout <= 0 {
		p.Timeout = 5 * time.Minute
	}
	g, err := p.planOnce(ctx, goal, false)
	if err == nil {
		return g, nil
	}
	if !errors.Is(err, errNoGraph) {
		return nil, err
	}
	return p.planOnce(ctx, goal, true)
}

func (p *OpenCodePlanner) planOnce(ctx context.Context, goal string, strict bool) (*graph.Graph, error) {
	sess, err := p.oc.CreateSession(ctx, "corral/planner")
	if err != nil {
		return nil, fmt.Errorf("planner session: %w", err)
	}
	prompt := planPrompt(goal)
	if strict {
		prompt = "Your previous response contained no valid graph JSON. " +
			"Return ONLY the JSON object described above, nothing else. " + prompt
	}
	if err := p.oc.PromptAsyncWithTools(ctx, sess.ID, prompt, p.Model, planTools); err != nil {
		return nil, fmt.Errorf("planner prompt: %w", err)
	}
	deadline := time.Now().Add(p.Timeout)
	for {
		select {
		case <-ctx.Done():
			_ = p.oc.Abort(ctx, sess.ID)
			return nil, ctx.Err()
		default:
		}
		if msgs, err := p.oc.Messages(ctx, sess.ID, 8); err == nil {
			// Parse progressively: as soon as a complete, valid graph
			// object is in the stream, return it — no need to wait for
			// the session to finish.
			if g, terminal, errName := extractGraphFromMessages(msgs); g != nil {
				return g, nil
			} else if terminal {
				if errName != "" {
					return nil, fmt.Errorf("planner session error: %s", errName)
				}
				return nil, fmt.Errorf("%w (model answered without graph JSON)", errNoGraph)
			}
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(400 * time.Millisecond)
	}
	// Timed out: kill the session so it stops generating, and fail fast.
	_ = p.oc.Abort(ctx, sess.ID)
	return nil, fmt.Errorf("planner timed out after %s (no terminal response)", p.Timeout)
}

// extractGraphFromMessages scans assistant text parts (newest message
// first) for a complete graph, and reports whether the newest assistant
// message is terminal (finished or errored).
func extractGraphFromMessages(msgs []ocx.Message) (g *graph.Graph, terminal bool, errName string) {
	var newest *ocx.Message
	for i := range msgs {
		m := &msgs[i]
		if m.Info.Role != "assistant" {
			continue
		}
		for _, part := range m.Parts {
			var p2 struct {
				Type string `json:"type"`
				Text string `json:"text"`
			}
			if json.Unmarshal(part, &p2) != nil || p2.Type != "text" {
				continue
			}
			if g, err := parseGraphFromResponse(p2.Text); err == nil {
				return g, false, ""
			}
		}
		newest = m
	}
	if newest == nil {
		return nil, false, ""
	}
	if newest.Info.Error != nil {
		return nil, true, errorName(newest.Info.Error)
	}
	if newest.Info.Finish != nil {
		return nil, true, ""
	}
	return nil, false, ""
}

func errorName(raw *json.RawMessage) string {
	if raw == nil {
		return ""
	}
	var e struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(*raw, &e); err != nil {
		return ""
	}
	return e.Name
}

// parseGraphFromResponse extracts the first object that looks like a
// corral graph (has a "nodes" array) from model prose and normalizes it.
func parseGraphFromResponse(text string) (*graph.Graph, error) {
	idx := strings.Index(text, "{")
	for idx >= 0 {
		end := findJSONEnd(text, idx)
		if end > 0 {
			var probe struct {
				Nodes json.RawMessage `json:"nodes"`
			}
			if json.Unmarshal([]byte(text[idx:end]), &probe) == nil && len(probe.Nodes) > 0 {
				if g, err := extractGraph(probe.Nodes); err == nil {
					return g, nil
				}
			}
		}
		next := strings.Index(text[idx+1:], "{")
		if next < 0 {
			break
		}
		idx = idx + 1 + next
	}
	return nil, fmt.Errorf("no valid graph JSON found in planner response")
}

// extractGraph normalizes a model-produced nodes array into a canonical,
// validated corral graph. Models routinely invent field names; common
// aliases are mapped (goal/action -> objective, check/command ->
// verification, dependencies -> dependsOn, worker/reviewer types ->
// agent roles).
func extractGraph(raw json.RawMessage) (*graph.Graph, error) {
	var nodes []map[string]any
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return nil, err
	}
	g := &graph.Graph{Version: 1}
	for _, rn := range nodes {
		n, err := normalizeNode(rn)
		if err != nil {
			return nil, err
		}
		g.Nodes = append(g.Nodes, n)
	}
	if err := graph.Validate(g); err != nil {
		return nil, err
	}
	return g, nil
}

func str(v any) string {
	s, _ := v.(string)
	return strings.TrimSpace(s)
}

func strList(v any) []string {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case []any:
		var out []string
		for _, e := range t {
			if s := str(e); s != "" {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return t
	case string:
		if t != "" {
			return []string{t}
		}
	}
	return nil
}

func intOf(v any, def int) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case string:
		if n, err := parseInt(t); err == nil {
			return n
		}
	}
	return def
}

func normalizeNode(rn map[string]any) (*graph.Node, error) {
	id := str(rn["id"])
	if id == "" {
		return nil, fmt.Errorf("node without id")
	}
	typ := str(rn["type"])
	if typ == "" {
		typ = "agent"
	}
	n := &graph.Node{ID: graph.NodeID(id), Priority: graph.Priority(intOf(rn["priority"], 50))}
	switch typ {
	case "worker", "reviewer":
		n.Type = graph.NodeAgent
		n.Role = typ
	case "agent":
		n.Type = graph.NodeAgent
		n.Role = str(rn["role"])
	case "check":
		n.Type = graph.NodeCheck
	case "merge":
		n.Type = graph.NodeMerge
	case "human_gate", "gate", "approval":
		n.Type = graph.NodeHuman
	default:
		return nil, fmt.Errorf("node %s: unknown type %q", id, typ)
	}
	n.Objective = str(rn["objective"])
	if n.Objective == "" {
		n.Objective = str(rn["goal"])
	}
	if n.Objective == "" {
		n.Objective = str(rn["action"])
	}
	n.AcceptanceCriteria = strList(rn["acceptanceCriteria"])
	if n.Type == graph.NodeAgent && len(n.AcceptanceCriteria) == 0 && n.Objective != "" {
		n.AcceptanceCriteria = []string{n.Objective}
	}
	for _, d := range strList(rn["dependsOn"]) {
		n.DependsOn = append(n.DependsOn, graph.NodeID(d))
	}
	if len(n.DependsOn) == 0 {
		for _, d := range strList(rn["dependencies"]) {
			n.DependsOn = append(n.DependsOn, graph.NodeID(d))
		}
	}
	n.WriteScope = strList(rn["writeScope"])
	if len(n.WriteScope) == 0 {
		n.WriteScope = strList(rn["write"])
	}
	n.Role = str(rn["role"])
	if n.Role == "" && n.Type == graph.NodeAgent && len(n.WriteScope) > 0 {
		n.Role = "worker"
	}
	if m, ok := rn["retryPolicy"].(map[string]any); ok {
		if v, ok := m["maxRetries"]; ok {
			n.RetryPolicy.MaxRetries = intOf(v, 0)
		}
		if v, ok := m["backoffSeconds"]; ok {
			n.RetryPolicy.Backoff = time.Duration(intOf(v, 0)) * time.Second
		}
	}
	if m, ok := rn["budget"].(map[string]any); ok {
		if v, ok := m["maxDurationSeconds"]; ok {
			n.Budget.MaxDuration = time.Duration(intOf(v, 0)) * time.Second
		}
	}
	// Verification: canonical object, or a command alias.
	if v, ok := rn["verification"].(map[string]any); ok {
		n.Verification = normalizeVerification(v)
	}
	if n.Verification == nil {
		if cmd, ok := rn["command"]; ok {
			n.Verification = &graph.Verification{Kind: "command", Command: strList(cmd)}
		} else if check, ok := rn["check"]; ok {
			if m, ok := check.(map[string]any); ok {
				n.Verification = normalizeVerification(m)
			}
		}
	}
	return n, nil
}

func normalizeVerification(v map[string]any) *graph.Verification {
	kind := str(v["kind"])
	if kind == "" {
		if _, ok := v["schema"]; ok {
			kind = "json_schema"
		} else {
			kind = "command"
		}
	}
	ver := &graph.Verification{Kind: kind}
	ver.Command = strList(v["command"])
	ver.Schema = str(v["schema"])
	ver.Target = str(v["target"])
	ver.Reviewer = graph.NodeID(str(v["reviewer"]))
	if kind == "command" && len(ver.Command) == 0 {
		// e.g. "grep -q marker file" shorthand
		if cmd := str(v["cmd"]); cmd != "" {
			ver.Command = strings.Fields(cmd)
		}
	}
	return ver
}

func parseInt(s string) (int, error) {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			if n > 0 {
				break
			}
			continue
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

// findJSONEnd returns the index just past the balanced brace starting at
// start (which must point at '{'), or -1 when unbalanced.
func findJSONEnd(s string, start int) int {
	depth := 0
	inStr := false
	esc := false
	for i := start; i < len(s); i++ {
		c := s[i]
		if inStr {
			if esc {
				esc = false
				continue
			}
			if c == '\\' {
				esc = true
				continue
			}
			if c == '"' {
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return -1
}

func planPrompt(goal string) string {
	return `You are the corral planner. Produce a task graph for the goal below.
The graph is JSON matching exactly this schema:

{
  "version": 1,
  "nodes": [
    {
      "id": "string (unique, e.g. w1, c1, gate, merge)",
      "type": "agent | check | merge | human_gate",
      "objective": "string: instruction for the worker agent",
      "acceptanceCriteria": ["string"],   // required for agent nodes
      "role": "worker | reviewer",        // agent nodes only
      "priority": 50,
      "dependsOn": ["other node ids"],
      "writeScope": ["paths the agent may write, e.g. file.txt or dir/"],  // workers
      "verification": {                    // evidence gate; agents may omit (diff gate)
        "kind": "command | json_schema | reviewer",
        "command": ["cmd", "arg1"],        // for command kind, runs in the worker's worktree
        "target": "file.json",             // for json_schema
        "schema": "{...}",                 // for json_schema
        "reviewer": "reviewer-node-id"     // for reviewer
      },
      "retryPolicy": {"maxRetries": 1, "backoffSeconds": 5},
      "budget": {"maxDurationSeconds": 600}
    }
  ]
}

Rules:
- check nodes: type "check", command verification only, they verify worker output.
- human gates: type "human_gate", no verification, they gate a merge.
- merge nodes: type "merge", depend on a human gate, command verification only.
- Every worker (role "worker") must have a writeScope and a verification command that checks its output.
- A merge node must come after a human gate; checks run inside the worker's worktree.
- ids: use short unique ids like "w1", "c1", "gate", "merge".
- Return ONLY the JSON object, no prose, no markdown fences.

Example for "add a linter":
{"version":1,"nodes":[
 {"id":"w1","type":"agent","role":"worker","objective":"add a golangci-lint config","acceptanceCriteria":["config exists"],"priority":50,"writeScope":[".golangci.yml"],"verification":{"kind":"command","command":["test","-f",".golangci.yml"]},"retryPolicy":{"maxRetries":1,"backoffSeconds":5}},
 {"id":"c1","type":"check","objective":"verify lint config","priority":50,"dependsOn":["w1"],"verification":{"kind":"command","command":["golangci-lint","run","--config",".golangci.yml"]},"retryPolicy":{"maxRetries":1,"backoffSeconds":5}},
 {"id":"gate","type":"human_gate","objective":"approve","priority":50,"dependsOn":["c1"]},
 {"id":"merge","type":"merge","objective":"merge accepted work","priority":50,"dependsOn":["gate"],"verification":{"kind":"command","command":["git","diff","--check"]},"retryPolicy":{"maxRetries":1,"backoffSeconds":5}}
]}

Goal: ` + goal
}
