package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

type fakeAPI struct {
	runs      []RunSummary
	detail    *RunDetail
	actions   []string
	listErr   error
	detailErr error
}

func (f *fakeAPI) ListRuns(ctx context.Context) ([]RunSummary, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.runs, nil
}

func (f *fakeAPI) GetRun(ctx context.Context, runID string) (*RunDetail, error) {
	if f.detailErr != nil {
		return nil, f.detailErr
	}
	return f.detail, nil
}

func (f *fakeAPI) act(label string) {
	f.actions = append(f.actions, label)
}

func (f *fakeAPI) Approve(ctx context.Context, r, n string) error { f.act("approve:" + n); return nil }
func (f *fakeAPI) Reject(ctx context.Context, r, n string) error  { f.act("reject:" + n); return nil }
func (f *fakeAPI) Cancel(ctx context.Context, r, n string) error  { f.act("cancel:" + n); return nil }
func (f *fakeAPI) Retry(ctx context.Context, r, n string) error   { f.act("retry:" + n); return nil }
func (f *fakeAPI) Steer(ctx context.Context, r, n, m string) error {
	f.act("steer:" + n + ":" + m)
	return nil
}
func (f *fakeAPI) RespondPermission(ctx context.Context, r, n, pid string, allow bool) error {
	f.act(fmt.Sprintf("perm:%s:%s:%v", pid, n, allow))
	return nil
}

func sampleDetail() *RunDetail {
	return &RunDetail{
		RunID:  "run_1",
		Status: "active",
		Graph: struct{ Nodes []GraphNode }{Nodes: []GraphNode{
			{ID: "w1", Type: "agent", Role: "worker", Objective: "write a.txt", Priority: 50,
				WriteScope: []string{"a.txt"}, DependsOn: nil,
				Verification: &struct {
					Kind    string   `json:"kind"`
					Command []string `json:"command,omitempty"`
				}{Kind: "command", Command: []string{"test", "-f", "a.txt"}}},
			{ID: "gate", Type: "human_gate", Objective: "approve", Priority: 50, DependsOn: []string{"w1"}},
			{ID: "m", Type: "merge", Objective: "merge", Priority: 50, DependsOn: []string{"gate"},
				Verification: &struct {
					Kind    string   `json:"kind"`
					Command []string `json:"command,omitempty"`
				}{Kind: "command", Command: []string{"true"}}},
		}},
		States: map[string]string{"w1": "done", "gate": "running", "m": "pending"},
		Attempts: map[string][]AttemptView{
			"w1":   {{ID: "run_1/w1/1", No: 1, Status: "done", SessionID: "ses_x", Worktree: "/tmp/wt/w1", Evidence: `{"exit":0}`}},
			"gate": {{ID: "run_1/gate/1", No: 1, Status: "running"}},
		},
		Events: []EventView{{Seq: 1, Type: "transition", NodeID: "w1", From: "pending", To: "done"}},
	}
}

func key(s string) tea.Msg { return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)} }

// send simulates the tea runtime: run the returned command and deliver
// its message back into the model.
func send(t *testing.T, m *Model, msg tea.Msg) {
	t.Helper()
	_, cmd := m.Update(msg)
	if cmd != nil {
		if r := cmd(); r != nil {
			send(t, m, r)
		}
	}
}

func TestListAndNavigation(t *testing.T) {
	api := &fakeAPI{runs: []RunSummary{{ID: "run_1", Status: "active", States: map[string]string{"w1": "running"}}}, detail: sampleDetail()}
	m := New(api, context.Background())

	// Initial tick loads the run list.
	mm, cmd := m.Update(tickMsg{})
	if cmd == nil {
		t.Fatal("expected tick command")
	}
	if _, cmd = m.Update(fetchMsg{runs: api.runs}); cmd != nil {
		t.Fatal("unexpected cmd from fetch")
	}
	_ = mm
	if len(m.runs) != 1 || m.runs[0].ID != "run_1" {
		t.Fatalf("runs not loaded: %+v", m.runs)
	}
	view := m.View()
	if !strings.Contains(view, "run_1") || !strings.Contains(view, "w1:running") {
		t.Fatalf("list view missing content:\n%s", view)
	}

	// Enter opens the detail view and fetches it.
	m.Update(key("enter"))
	if m.mode != modeDetail {
		t.Fatalf("mode = %d, want detail", m.mode)
	}
	m.Update(fetchRunMsg{detail: api.detail})
	if m.detail == nil || m.detail.RunID != "run_1" {
		t.Fatal("detail not loaded")
	}
	view = m.View()
	for _, want := range []string{"run_1", "w1", "gate", "m", "dag", "← gate", "p50"} {
		if !strings.Contains(view, want) {
			t.Fatalf("detail view missing %q:\n%s", want, view)
		}
	}
	// The node line shows attempts and session/worktree in inspect.
	m.Update(key("i"))
	view = m.View()
	for _, want := range []string{"ses_x", "worktree", "attempts", "evidence"} {
		if !strings.Contains(view, want) {
			t.Fatalf("inspect view missing %q:\n%s", want, view)
		}
	}
	m.Update(key("esc"))
	if m.mode != modeDetail {
		t.Fatalf("esc from inspect: mode = %d", m.mode)
	}
	m.Update(key("esc"))
	if m.mode != modeList {
		t.Fatalf("esc from detail: mode = %d", m.mode)
	}
}

func TestNodeActions(t *testing.T) {
	api := &fakeAPI{}
	m := New(api, context.Background())
	m.runs = []RunSummary{{ID: "run_1"}}
	m.selectedID = "run_1"
	m.detail = sampleDetail()
	m.mode = modeDetail
	m.nodeCursor = 1 // gate

	send(t, m, key("a"))
	send(t, m, key("r"))
	send(t, m, key("t"))
	// cancel uses same key path
	send(t, m, key("c"))
	want := []string{"approve:gate", "reject:gate", "retry:gate", "cancel:gate"}
	if fmt.Sprint(api.actions) != fmt.Sprint(want) {
		t.Fatalf("actions = %v, want %v", api.actions, want)
	}

	// Steer: 's' opens input, typing + enter sends.
	send(t, m, key("s"))
	if m.mode != modeSteer {
		t.Fatalf("mode = %d, want steer", m.mode)
	}
	for _, r := range "focus on alpha" {
		m.Update(key(string(r)))
	}
	if m.steerInput != "focus on alpha" {
		t.Fatalf("steer input = %q", m.steerInput)
	}
	send(t, m, key("enter"))
	if len(api.actions) != 5 || api.actions[4] != "steer:gate:focus on alpha" {
		t.Fatalf("steer action = %v", api.actions)
	}
}

func TestPermissionRespond(t *testing.T) {
	d := sampleDetail()
	// w1 is blocked on a permission request carried by its transition.
	d.States["w1"] = "blocked"
	d.Events = append(d.Events, EventView{
		Seq: 2, NodeID: "w1", Type: "transition", From: "running", To: "blocked",
		Payload: json.RawMessage(`{"reason":"permission","permissionID":"perm-9"}`),
	})
	api := &fakeAPI{}
	m := New(api, context.Background())
	m.runs = []RunSummary{{ID: "run_1"}}
	m.selectedID = "run_1"
	m.detail = d
	m.mode = modeDetail
	m.nodeCursor = 0 // w1

	// The pending permission is surfaced in the detail view.
	view := m.View()
	for _, want := range []string{"perm:perm-9", "p allow perm", "d deny perm"} {
		if !strings.Contains(view, want) {
			t.Fatalf("detail view missing %q:\n%s", want, view)
		}
	}

	send(t, m, key("p"))
	send(t, m, key("d"))
	want := []string{"perm:perm-9:w1:true", "perm:perm-9:w1:false"}
	if fmt.Sprint(api.actions) != fmt.Sprint(want) {
		t.Fatalf("actions = %v, want %v", api.actions, want)
	}

	// 'p'/'d' are no-ops on a node with no pending permission (the gate).
	m.nodeCursor = 1
	send(t, m, key("p"))
	send(t, m, key("d"))
	if len(api.actions) != 2 {
		t.Fatalf("permission keys acted on non-permission node: %v", api.actions)
	}
}

func TestEmptyState(t *testing.T) {
	m := New(&fakeAPI{}, context.Background())
	m.Update(fetchMsg{})
	if !strings.Contains(m.View(), "no runs yet") {
		t.Fatalf("empty view wrong:\n%s", m.View())
	}
}

func TestFetchErrorShown(t *testing.T) {
	api := &fakeAPI{listErr: fmt.Errorf("connection refused")}
	m := New(api, context.Background())
	m.Update(fetchMsg{err: api.listErr})
	if m.err == nil {
		t.Fatal("error not recorded")
	}
	if !strings.Contains(m.View(), "connection refused") {
		t.Fatalf("error not rendered:\n%s", m.View())
	}
}

var _ = time.Second
