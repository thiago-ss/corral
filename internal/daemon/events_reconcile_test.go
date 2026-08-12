package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"corral/internal/graph"
	"corral/internal/store"
)

func TestEventHeartbeatReconcilesDroppedWakeup(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	g := &graph.Graph{Nodes: []*graph.Node{{
		ID: "w", Type: graph.NodeAgent, Objective: "work", AcceptanceCriteria: []string{"done"},
	}}}
	if err := st.CreateRun(ctx, "r", g, false, time.Now()); err != nil {
		t.Fatal(err)
	}

	// Deliberately omit the store-to-broker pump. The durable event below has
	// no live wakeup, so only heartbeat cursor reconciliation can deliver it.
	d := &Daemon{st: st, broker: newBroker(), eventHeartbeat: 20 * time.Millisecond}
	srv := httptest.NewServer(d.Handler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL + "/api/runs/r/events?after=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := st.AppendEvent(ctx, "r", "w", store.EventGraph, "", "", "", `{"version":2}`, time.Now()); err != nil {
		t.Fatal(err)
	}

	reader := bufio.NewReader(resp.Body)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "data: ") {
			var event store.Event
			if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data: "))), &event); err != nil {
				t.Fatal(err)
			}
			if event.Seq != 2 {
				t.Fatalf("event seq = %s, want 2", strconv.FormatInt(event.Seq, 10))
			}
			return
		}
	}
	t.Fatal("durable event was not reconciled on heartbeat")
}
