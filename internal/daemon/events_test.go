package daemon_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"corral/internal/graph"
	"corral/internal/sched"
	"corral/internal/store"
)

// readSSE reads one server-sent event record from r, returning its id and
// data, or the heartbeat flag when the line is a comment.
func readSSE(t *testing.T, r *bufio.Reader) (id int64, data string, ping bool) {
	t.Helper()
	var recID string
	var dataLines []string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read sse: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if recID != "" || len(dataLines) > 0 {
				n, _ := strconv.ParseInt(recID, 10, 64)
				return n, strings.Join(dataLines, "\n"), false
			}
			continue
		}
		switch {
		case strings.HasPrefix(line, ": "):
			return 0, "", true
		case strings.HasPrefix(line, "id: "):
			recID = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			dataLines = append(dataLines, strings.TrimPrefix(line, "data: "))
		}
	}
}

func TestEventsSSEStreamsFullRun(t *testing.T) {
	a, _, _, drv := setupDaemon(t, "")
	drv.SetScript("w1", sched.Script{Delay: 200 * time.Millisecond, Write: map[string]string{"a.txt": "A1"}})
	g := &graph.Graph{Nodes: []*graph.Node{workerNode("w1", "a.txt", "A1")}}
	code, body := a.do("operator", http.MethodPost, "/api/runs", map[string]any{"graph": g})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %s", code, body)
	}
	var created struct{ RunID string }
	json.Unmarshal([]byte(body), &created)

	resp, err := http.Get(a.base + "/api/runs/" + created.RunID + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}

	r := bufio.NewReader(resp.Body)
	var seqs []int64
	var types []string
	toStates := map[string]bool{}
	deadline := time.Now().Add(20 * time.Second)
	completed := false
	for time.Now().Before(deadline) {
		id, data, ping := readSSE(t, r)
		if ping {
			continue
		}
		var ev store.Event
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			t.Fatalf("bad event json: %v: %s", err, data)
		}
		if ev.Seq != id {
			t.Fatalf("id %d does not match event seq %d", id, ev.Seq)
		}
		seqs = append(seqs, ev.Seq)
		types = append(types, string(ev.Type))
		if ev.To != "" {
			toStates[string(ev.To)] = true
		}
		if ev.Type == store.EventRun && strings.Contains(data, "completed") {
			completed = true
			break
		}
	}
	if !completed {
		t.Fatalf("run completion never streamed; types = %v", types)
	}
	for i, s := range seqs {
		if s != int64(i+1) {
			t.Fatalf("seq[%d] = %d, want %d (contiguous from 1)", i, s, i+1)
		}
	}
	for _, want := range []string{"ready", "leased", "running", "verifying", "done"} {
		if !toStates[want] {
			t.Fatalf("missing %s transition in stream (got %v)", want, toStates)
		}
	}
	if types[0] != "run" {
		t.Fatalf("first event type = %q, want run", types[0])
	}
}

func TestEventsSSEAfterCursor(t *testing.T) {
	a, _, _, drv := setupDaemon(t, "")
	drv.SetScript("w1", sched.Script{Delay: 150 * time.Millisecond, Write: map[string]string{"a.txt": "A1"}})
	g := &graph.Graph{Nodes: []*graph.Node{workerNode("w1", "a.txt", "A1")}}
	code, body := a.do("operator", http.MethodPost, "/api/runs", map[string]any{"graph": g})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %s", code, body)
	}
	var created struct{ RunID string }
	json.Unmarshal([]byte(body), &created)

	// Replay-only: connect with a cursor and only the events past it must
	// be emitted.
	resp, err := http.Get(fmt.Sprintf("%s/api/runs/%s/events?after=5", a.base, created.RunID))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	first := true
	var minSeq int64
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		_, data, ping := readSSE(t, r)
		if ping {
			continue
		}
		var ev store.Event
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			t.Fatalf("bad event: %v", err)
		}
		if first {
			minSeq = ev.Seq
			first = false
		}
		if ev.Seq <= 5 {
			t.Fatalf("event seq %d emitted despite after=5", ev.Seq)
		}
		if ev.Type == store.EventRun && strings.Contains(data, "completed") {
			break
		}
	}
	if first {
		t.Fatal("no events received")
	}
	if minSeq != 6 {
		t.Fatalf("first event seq = %d, want 6", minSeq)
	}
}

func TestEventsSSEHeartbeats(t *testing.T) {
	a, d, st, _ := setupDaemon(t, "")
	d.SetEventHeartbeat(100 * time.Millisecond)
	const runID = "run_heartbeat"
	if err := st.CreateRun(context.Background(), runID, &graph.Graph{Nodes: []*graph.Node{workerNode("w1", "a.txt", "A1")}}, false, time.Now()); err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(a.base + "/api/runs/" + runID + "/events?after=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	r := bufio.NewReader(resp.Body)
	pings := 0
	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, _, ping := readSSE(t, r)
		if ping {
			pings++
		}
	}
	if pings == 0 {
		t.Fatal("no heartbeats received")
	}
}

func TestEventsSSEReconnectReplaysMissedEvents(t *testing.T) {
	a, _, st, _ := setupDaemon(t, "")
	ctx := context.Background()
	const runID = "run_reconnect"
	if err := st.CreateRun(ctx, runID, &graph.Graph{Nodes: []*graph.Node{workerNode("w1", "a.txt", "A1")}}, false, time.Now()); err != nil {
		t.Fatal(err)
	}

	firstCtx, cancel := context.WithCancel(ctx)
	request, _ := http.NewRequestWithContext(firstCtx, http.MethodGet, a.base+"/api/runs/"+runID+"/events", nil)
	response, err := a.cli.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	id, _, ping := readSSE(t, bufio.NewReader(response.Body))
	if ping || id != 1 {
		t.Fatalf("first stream record: id=%d ping=%v, want event 1", id, ping)
	}
	cancel()
	response.Body.Close()

	for i := 2; i <= 3; i++ {
		if _, err := st.AppendEvent(ctx, runID, "w1", store.EventGraph, "", "", "", fmt.Sprintf(`{"version":%d}`, i), time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	response, err = http.Get(a.base + "/api/runs/" + runID + "/events?after=1")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	for want := int64(2); want <= 3; want++ {
		id, data, ping := readSSE(t, reader)
		if ping || id != want {
			t.Fatalf("replayed record: id=%d ping=%v, want event %d", id, ping, want)
		}
		var event store.Event
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			t.Fatal(err)
		}
		if event.Seq != want || event.RunID != runID {
			t.Fatalf("replayed event = %+v", event)
		}
	}
}

func TestEventsSSEUsesBearerAuthLikeOtherReadEndpoints(t *testing.T) {
	a, _, st, _ := setupDaemon(t, "sekret")
	const runID = "run_auth"
	if err := st.CreateRun(context.Background(), runID, &graph.Graph{Nodes: []*graph.Node{workerNode("w1", "a.txt", "A1")}}, false, time.Now()); err != nil {
		t.Fatal(err)
	}

	request, _ := http.NewRequest(http.MethodGet, a.base+"/api/runs/"+runID+"/events", nil)
	response, err := a.cli.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("without bearer: %d, want 401", response.StatusCode)
	}

	request, _ = http.NewRequest(http.MethodGet, a.base+"/api/runs/"+runID+"/events", nil)
	request.Header.Set("Authorization", "Bearer sekret")
	response, err = a.cli.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("with bearer: %d, want 200", response.StatusCode)
	}
	id, _, ping := readSSE(t, bufio.NewReader(response.Body))
	if ping || id != 1 {
		t.Fatalf("authorized record: id=%d ping=%v, want event 1", id, ping)
	}
}

func TestEventsSSERejectsInvalidCursor(t *testing.T) {
	a, _, st, _ := setupDaemon(t, "")
	const runID = "run_cursor"
	if err := st.CreateRun(context.Background(), runID, &graph.Graph{Nodes: []*graph.Node{workerNode("w1", "a.txt", "A1")}}, false, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, after := range []string{"-1", "abc"} {
		code, body := a.do("operator", http.MethodGet, "/api/runs/"+runID+"/events?after="+after, nil)
		if code != http.StatusBadRequest {
			t.Fatalf("after=%q: %d %s, want 400", after, code, body)
		}
	}
}

func TestEventsSSEUnknownRun(t *testing.T) {
	a, _, _, _ := setupDaemon(t, "")
	code, body := a.do("operator", http.MethodGet, "/api/runs/nope/events", nil)
	if code != http.StatusNotFound {
		t.Fatalf("unknown run: %d %s, want 404", code, body)
	}
}

func TestEventsSSEDoesNotAffectJSONEndpoints(t *testing.T) {
	a, _, st, drv := setupDaemon(t, "")
	drv.SetScript("w1", sched.Script{Delay: 150 * time.Millisecond, Write: map[string]string{"a.txt": "A1"}})
	g := &graph.Graph{Nodes: []*graph.Node{workerNode("w1", "a.txt", "A1")}}
	code, body := a.do("operator", http.MethodPost, "/api/runs", map[string]any{"graph": g})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %s", code, body)
	}
	var created struct{ RunID string }
	json.Unmarshal([]byte(body), &created)

	// Open (and hold) an SSE connection while the run executes; the JSON
	// endpoints must keep serving shape-stable responses regardless.
	resp, err := http.Get(a.base + "/api/runs/" + created.RunID + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// The run detail must still be served and shape-stable while the
	// stream is open.
	a.waitState(t, "", created.RunID, "w1", graph.StateDone, 20*time.Second)
	code, body = a.do("operator", http.MethodGet, "/api/runs/"+created.RunID, nil)
	if code != http.StatusOK || !strings.Contains(body, `"done":true`) {
		t.Fatalf("run detail broken: %d %s", code, body)
	}
	if _, err := st.Run(context.Background(), created.RunID); err != nil {
		t.Fatalf("store run read failed: %v", err)
	}
}
