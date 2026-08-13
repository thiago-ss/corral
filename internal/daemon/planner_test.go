package daemon

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"corral/internal/ocx"
)

const validGraph = `{
  "version": 1,
  "nodes": [
    {"id": "w1", "type": "agent", "role": "worker", "objective": "write a.txt",
     "acceptanceCriteria": ["a.txt exists"], "priority": 50, "writeScope": ["a.txt"],
     "verification": {"kind": "command", "command": ["test", "-f", "a.txt"]},
     "retryPolicy": {"maxRetries": 1}}
  ]
}`

func TestParseGraphFromResponse(t *testing.T) {
	// Prose around a fenced JSON block.
	text := "Here is the graph:\n```json\n" + validGraph + "\n```\nHope this helps."
	g, err := parseGraphFromResponse(text)
	if err != nil {
		t.Fatalf("fenced graph: %v", err)
	}
	if len(g.Nodes) != 1 || g.Nodes[0].ID != "w1" {
		t.Fatalf("wrong graph: %+v", g.Nodes)
	}

	// Bare JSON at the end.
	g2, err := parseGraphFromResponse("sure, " + validGraph)
	if err != nil {
		t.Fatalf("bare graph: %v", err)
	}
	if len(g2.Nodes) != 1 {
		t.Fatalf("wrong graph: %+v", g2.Nodes)
	}

	// Invalid graph must be rejected (dependency cycle).
	cyc := strings.Replace(validGraph, `"dependsOn"`, `"dependsOn"`, 1)
	cyc = strings.Replace(cyc, `"retryPolicy": {"maxRetries": 1}`, `"dependsOn": ["w1"], "retryPolicy": {"maxRetries": 1}`, 1)
	if _, err := parseGraphFromResponse(cyc); err == nil {
		t.Fatal("cyclic graph accepted")
	}

	// Unknown node type must be rejected.
	badType := strings.Replace(validGraph, `"type": "agent"`, `"type": "dancer"`, 1)
	if _, err := parseGraphFromResponse(badType); err == nil {
		t.Fatal("unknown node type accepted")
	}

	// No graph at all.
	if _, err := parseGraphFromResponse("I cannot plan this."); err == nil {
		t.Fatal("no-graph response accepted")
	}

	// Invalid JSON followed by a valid object (first parse attempt fails,
	// later one succeeds).
	mixed := `{"nodes": }` + "\n" + validGraph
	g3, err := parseGraphFromResponse(mixed)
	if err != nil {
		t.Fatalf("mixed response: %v", err)
	}
	if len(g3.Nodes) != 1 {
		t.Fatalf("wrong graph: %+v", g3.Nodes)
	}
}

func TestPlannerToolsFailClosed(t *testing.T) {
	if planTools["*"] || !planTools["read"] || !planTools["glob"] || len(planTools) != 3 {
		t.Fatalf("planner tools = %#v, want wildcard deny plus read/glob", planTools)
	}
}

func TestFindJSONEnd(t *testing.T) {
	s := `{"a": {"b": [1, {"c": "} { "}]}} tail`
	if end := findJSONEnd(s, 0); end != len(s)-5 {
		t.Fatalf("end = %d, want %d", end, len(s)-5)
	}
	if end := findJSONEnd(`{unbalanced`, 0); end != -1 {
		t.Fatalf("unbalanced should be -1, got %d", end)
	}
}

func testMsgs(role string, text string, finish *string) []ocx.Message {
	part := json.RawMessage(fmt.Sprintf(`{"type":"text","text":%s}`, strconv.Quote(text)))
	info := ocx.MessageInfo{Role: role, Finish: finish}
	return []ocx.Message{{Info: info, Parts: []json.RawMessage{part}}}
}

// TestExtractGraphFromMessages covers the progressive-parse contract:
// a graph mid-stream is returned before terminal state; terminal state
// is reported when no graph exists; session errors surface by name.
func TestExtractGraphFromMessages(t *testing.T) {
	// Complete graph in a still-streaming message -> returned immediately.
	streaming := func() *string { return nil }()
	g, term, errName := extractGraphFromMessages(testMsgs("assistant", "here:\n"+validGraph, streaming))
	if g == nil || term || errName != "" {
		t.Fatalf("streaming graph not extracted: g=%v term=%v err=%q", g != nil, term, errName)
	}

	// Terminal without graph -> terminal flagged, no error name.
	done := "stop"
	g, term, errName = extractGraphFromMessages(testMsgs("assistant", "I could not plan this.", &done))
	if g != nil || !term || errName != "" {
		t.Fatalf("terminal no-graph wrong: g=%v term=%v err=%q", g != nil, term, errName)
	}

	// Still streaming without a graph -> neither.
	g, term, _ = extractGraphFromMessages(testMsgs("assistant", "thinking...", streaming))
	if g != nil || term {
		t.Fatalf("streaming no-graph wrong: term=%v", term)
	}

	// Session error surfaces the error name.
	errMsg := json.RawMessage(`{"name":"MessageAbortedError","data":{}}`)
	m := ocx.Message{Info: ocx.MessageInfo{Role: "assistant", Error: &errMsg}}
	g, term, errName = extractGraphFromMessages([]ocx.Message{m})
	if g != nil || !term || errName != "MessageAbortedError" {
		t.Fatalf("error message wrong: g=%v term=%v err=%q", g != nil, term, errName)
	}

	// No assistant message yet -> nothing.
	g, term, _ = extractGraphFromMessages(testMsgs("user", "goal", streaming))
	if g != nil || term {
		t.Fatalf("no-assistant wrong: term=%v", term)
	}
}
