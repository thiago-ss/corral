package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

type fakeAPI struct {
	mu          sync.Mutex
	runs        []RunSummary
	detail      *RunDetail
	actions     []string
	listErr     error
	detailErr   error
	tailErr     error
	tail        []string
	stream      func(context.Context, string, int64, func(EventView) error) error
	streamAfter []int64
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

func (f *fakeAPI) StreamEvents(ctx context.Context, runID string, after int64, emit func(EventView) error) error {
	f.mu.Lock()
	f.streamAfter = append(f.streamAfter, after)
	stream := f.stream
	f.mu.Unlock()
	if stream != nil {
		return stream(ctx, runID, after, emit)
	}
	return fmt.Errorf("stream unavailable")
}

func (f *fakeAPI) streamCursors() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.streamAfter...)
}

func (f *fakeAPI) Tail(ctx context.Context, runID, nodeID string, lines int) ([]string, error) {
	if f.tailErr != nil {
		return nil, f.tailErr
	}
	return f.tail, nil
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

func runCmd(t *testing.T, cmd tea.Cmd) {
	t.Helper()
	if cmd == nil {
		return
	}
	msg := cmd()
	if batch, ok := msg.(tea.BatchMsg); ok {
		for _, nested := range batch {
			runCmd(t, nested)
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

func TestResolvedPermissionIsNotExposedOrResent(t *testing.T) {
	d := sampleDetail()
	d.States["w1"] = "done"
	d.Events = append(d.Events, EventView{
		Seq: 2, NodeID: "w1", Type: "transition", From: "running", To: "blocked",
		Payload: json.RawMessage(`{"reason":"permission","permissionID":"perm-old"}`),
	})
	d.Events = append(d.Events, EventView{
		Seq: 3, NodeID: "w1", Type: "transition", From: "blocked", To: "running",
	})

	api := &fakeAPI{}
	m := New(api, context.Background())
	m.selectedID, m.detail, m.mode = "run_1", d, modeDetail
	if pid, ok := m.pendingPermission("w1"); ok || pid != "" {
		t.Fatalf("resolved permission exposed: %q, %v", pid, ok)
	}
	if strings.Contains(m.View(), "perm-old") {
		t.Fatalf("resolved permission rendered:\n%s", m.View())
	}
	send(t, m, key("p"))
	send(t, m, key("d"))
	if len(api.actions) != 0 {
		t.Fatalf("resolved permission resent: %v", api.actions)
	}
}

func TestOnlyLatestBlockedTransitionCanExposePermission(t *testing.T) {
	d := sampleDetail()
	d.States["w1"] = "blocked"
	d.Events = append(d.Events,
		EventView{Seq: 2, NodeID: "w1", Type: "transition", From: "running", To: "blocked",
			Payload: json.RawMessage(`{"reason":"permission","permissionID":"perm-old"}`)},
		EventView{Seq: 3, NodeID: "w1", Type: "transition", From: "ready", To: "blocked",
			Payload: json.RawMessage(`{"reason":"dependency_failed"}`)},
	)
	m := New(&fakeAPI{}, context.Background())
	m.selectedID, m.detail, m.mode = "run_1", d, modeDetail
	if pid, ok := m.pendingPermission("w1"); ok || pid != "" {
		t.Fatalf("stale permission exposed after newer block: %q, %v", pid, ok)
	}
}

func TestEmptyState(t *testing.T) {
	m := New(&fakeAPI{}, context.Background())
	m.Update(fetchMsg{})
	if !strings.Contains(m.View(), "no runs yet") {
		t.Fatalf("empty view wrong:\n%s", m.View())
	}
}

func TestLiveAttemptTail(t *testing.T) {
	api := &fakeAPI{detail: sampleDetail(), tail: []string{"alpha", "beta", "gamma"}}
	m := New(api, context.Background())
	m.selectedID = "run_1"
	m.detail = sampleDetail()
	m.mode = modeDetail
	m.nodeCursor = 1 // gate
	m.Update(key("i"))
	if m.mode != modeInspect || m.tailNode != "gate" {
		t.Fatalf("inspect mode = %d tailNode=%q", m.mode, m.tailNode)
	}
	// Simulate the tail fetch result.
	m.Update(tailMsg{runID: "run_1", node: "gate", lines: api.tail})
	view := m.View()
	for _, want := range []string{"live tail", "alpha", "beta", "gamma"} {
		if !strings.Contains(view, want) {
			t.Fatalf("inspect view missing %q:\n%s", want, view)
		}
	}
	// Back clears the tail so it stops being fetched.
	m.Update(key("esc"))
	if m.tailNode != "" || len(m.tail) != 0 {
		t.Fatalf("tail not cleared on back: node=%q tail=%v", m.tailNode, m.tail)
	}
}

func TestAttentionOnGateAndFailure(t *testing.T) {
	var got []string
	api := &fakeAPI{detail: sampleDetail()}
	m := New(api, context.Background())
	m.notify = func(title, body string) { got = append(got, title+" | "+body) }
	m.selectedID = "run_1"
	m.detail = sampleDetail()
	m.detail.States["gate"] = "pending"
	m.seedAttentionStates()
	m.streamAttempted = true

	// Gate transitions into running → gate attention fires.
	next := sampleDetail()
	_, cmd := m.Update(fetchRunMsg{detail: next})
	runCmd(t, cmd)
	if len(got) != 1 || !strings.Contains(got[0], "gate") {
		t.Fatalf("gate attention not fired: %v", got)
	}

	// Same state again: no duplicate.
	_, cmd = m.Update(fetchRunMsg{detail: next})
	runCmd(t, cmd)
	if len(got) != 1 {
		t.Fatalf("duplicate attention fired: %v", got)
	}

	// A node fails → failure attention fires.
	detail := sampleDetail()
	detail.States["w1"] = "failed"
	_, cmd = m.Update(fetchRunMsg{detail: detail})
	runCmd(t, cmd)
	if len(got) != 2 || !strings.Contains(got[1], "failed") {
		t.Fatalf("failure attention not fired: %v", got)
	}
}

func TestAttentionDisabledByDefault(t *testing.T) {
	api := &fakeAPI{detail: sampleDetail()}
	m := New(api, context.Background())
	m.selectedID = "run_1"
	m.detail = sampleDetail()
	m.detail.States["gate"] = "pending"
	m.seedAttentionStates()
	m.streamAttempted = true
	_, cmd := m.Update(fetchRunMsg{detail: sampleDetail()})
	if cmd != nil {
		t.Fatal("attention should be disabled unless EnableAttention is called")
	}
}

func TestBudgetBarInDetail(t *testing.T) {
	d := sampleDetail()
	d.Graph.Nodes[0].Budget.MaxDuration = int64(2 * time.Second)
	d.Attempts["w1"] = []AttemptView{{ID: "w1/1", No: 1, Status: "done", StartedAt: int64Ptr(1000), FinishedAt: int64Ptr(2000)}}
	m := New(&fakeAPI{detail: d}, context.Background())
	m.selectedID = "run_1"
	m.detail = d
	m.mode = modeDetail
	view := m.View()
	if !strings.Contains(view, "progress") {
		t.Fatalf("detail view missing progress bar:\n%s", view)
	}
	if !strings.Contains(view, "1s/2s") {
		t.Fatalf("detail view missing budget usage:\n%s", view)
	}
	if !strings.Contains(view, "wall elapsed") {
		t.Fatalf("detail view must label timestamp-only duration as wall elapsed:\n%s", view)
	}
}

func TestBudgetBarExcludesPermissionWaitFromProviderRuntime(t *testing.T) {
	d := sampleDetail()
	n := &d.Graph.Nodes[0]
	n.Budget.MaxDuration = int64(10 * time.Second)
	d.Attempts["w1"] = []AttemptView{{
		ID: "run_1/w1/1", No: 1, Status: "done",
		StartedAt: int64Ptr(1_000), FinishedAt: int64Ptr(26_000),
	}}
	d.Events = []EventView{
		{Seq: 1, NodeID: "w1", Type: "attempt", AttemptID: "run_1/w1/1", CreatedAt: 1_000},
		{Seq: 2, NodeID: "w1", Type: "transition", From: "running", To: "blocked", CreatedAt: 3_000},
		{Seq: 3, NodeID: "w1", Type: "transition", From: "blocked", To: "ready", CreatedAt: 23_000},
		{Seq: 4, NodeID: "w1", Type: "transition", From: "ready", To: "leased", CreatedAt: 23_000},
		{Seq: 5, NodeID: "w1", Type: "transition", From: "leased", To: "running", CreatedAt: 23_000},
		{Seq: 6, NodeID: "w1", Type: "transition", From: "running", To: "verifying", CreatedAt: 26_000},
	}
	m := New(&fakeAPI{}, context.Background())
	m.detail = d
	bar := m.nodeBudgetBar(*n, d.Attempts["w1"], "done")
	if !strings.Contains(bar, "runtime 5s/10s") {
		t.Fatalf("permission wait counted as provider runtime: %q", bar)
	}
}

func TestElapsedActiveAttemptUsesCurrentTime(t *testing.T) {
	start := time.Now().Add(-2 * time.Second).UnixMilli()
	got := elapsed(start, nil)
	if got == "0ms" {
		t.Fatal("active attempt elapsed time must advance")
	}
}

func TestInspectSanitizesProviderTerminalControls(t *testing.T) {
	d := sampleDetail()
	d.States["w1"] = "running"
	d.Graph.Nodes[0].Objective = "safe\x1b]52;c;Y2xpcGJvYXJk\a\x1b[31mred\x1b[0m"
	d.Attempts["w1"][0].Status = "run\x1b]52;c;c3RhdHVz\a\x1b[31mning\x1b[0m"
	d.Attempts["w1"][0].Evidence = "proof\x1b]0;title\a"
	m := New(&fakeAPI{}, context.Background())
	m.selectedID, m.inspectNode, m.detail, m.mode = "run_1", "w1", d, modeInspect
	m.tail = []string{"tail\x1b]52;c;ZXZpbA==\a\x1b[2Jvisible"}
	view := m.View()
	for _, unsafe := range []string{"\x1b]52", "\x1b[31m", "\x1b[2J", "\a"} {
		if strings.Contains(view, unsafe) {
			t.Fatalf("view retained terminal control %q: %q", unsafe, view)
		}
	}
	for _, want := range []string{"safe", "red", "proof", "tail", "visible"} {
		if !strings.Contains(view, want) {
			t.Fatalf("view lost safe text %q: %q", want, view)
		}
	}
}

func TestCursorTargetsRenderedGraphOrder(t *testing.T) {
	d := sampleDetail()
	// Input order differs from the stable ID order rendered by viewDetail.
	d.Graph.Nodes = []GraphNode{d.Graph.Nodes[2], d.Graph.Nodes[0], d.Graph.Nodes[1]}
	api := &fakeAPI{}
	m := New(api, context.Background())
	m.selectedID, m.detail, m.mode, m.nodeCursor = "run_1", d, modeDetail, 0
	if selected, ok := m.SelectedNode(); !ok || selected != "m" {
		t.Fatalf("selected node = %q, %v; want first rendered node m", selected, ok)
	}
	_, cmd := m.Update(key("a"))
	if cmd == nil {
		t.Fatal("approve command missing")
	}
	cmd()
	if len(api.actions) != 1 || api.actions[0] != "approve:m" {
		t.Fatalf("cursor action = %v, want approve:m", api.actions)
	}
}

func TestBudgetBarUsesHighestUtilizationAndDoesNotFillDone(t *testing.T) {
	d := sampleDetail()
	n := &d.Graph.Nodes[0]
	n.Budget.MaxDuration = int64(100 * time.Second)
	n.Budget.MaxTokens = 100
	n.Budget.MaxCost = 10
	d.Attempts["w1"] = []AttemptView{{
		ID: "run_1/w1/1", No: 1, Status: "done",
		StartedAt: int64Ptr(1_000), FinishedAt: int64Ptr(11_000),
		Tokens: 75, Cost: 2,
	}}
	m := New(&fakeAPI{}, context.Background())
	m.selectedID, m.detail, m.mode = "run_1", d, modeDetail
	bar := m.nodeBudgetBar(*n, d.Attempts["w1"], "done")
	if !strings.Contains(bar, "tokens") {
		t.Fatalf("dominant token budget not labeled: %q", bar)
	}
	if got := strings.Count(bar, "█"); got != 6 {
		t.Fatalf("done node filled %d/8 cells, want actual 75%% (6/8): %q", got, bar)
	}
}

func TestDetailViewHandlesEmptyGraph(t *testing.T) {
	d := &RunDetail{RunID: "empty", States: map[string]string{}, Attempts: map[string][]AttemptView{}}
	m := New(&fakeAPI{}, context.Background())
	m.selectedID, m.detail, m.mode = "empty", d, modeDetail
	if got := m.View(); !strings.Contains(got, "no nodes") {
		t.Fatalf("empty graph view missing empty state:\n%s", got)
	}
}

func TestAttentionExecutesOutsideUpdate(t *testing.T) {
	var got []string
	m := New(&fakeAPI{}, context.Background())
	m.notify = func(title, body string) { got = append(got, title+" | "+body) }
	m.selectedID = "run_1"
	m.detail = sampleDetail()
	m.detail.States["gate"] = "pending"
	m.seedAttentionStates()
	m.streamAttempted = true
	_, cmd := m.Update(fetchRunMsg{detail: sampleDetail()})
	if len(got) != 0 {
		t.Fatalf("notification blocked Update: %v", got)
	}
	if cmd == nil {
		t.Fatal("transition did not return async attention command")
	}
	_ = cmd()
	if len(got) != 1 || !strings.Contains(got[0], "gate") {
		t.Fatalf("attention command did not notify: %v", got)
	}
}

func TestEventUpdatesStateIncrementallyAndAdvancesCursor(t *testing.T) {
	d := sampleDetail()
	d.States["w1"] = "running"
	d.Events = []EventView{{Seq: 4, Type: "transition", NodeID: "w1", From: "leased", To: "running"}}
	m := New(&fakeAPI{}, context.Background())
	m.selectedID, m.detail, m.mode, m.eventCursor = "run_1", d, modeDetail, 4
	items := make(chan eventStreamItem)
	m.streamItems = items

	event := EventView{Seq: 5, RunID: "run_1", Type: "transition", NodeID: "w1", From: "running", To: "blocked",
		Payload: json.RawMessage(`{"reason":"permission","permissionID":"perm-live"}`)}
	_, cmd := m.Update(eventStreamItemMsg{runID: "run_1", items: items, item: eventStreamItem{event: &event}})
	if m.eventCursor != 5 || m.detail.States["w1"] != "blocked" {
		t.Fatalf("incremental state cursor=%d state=%q", m.eventCursor, m.detail.States["w1"])
	}
	if pid, ok := m.pendingPermission("w1"); !ok || pid != "perm-live" {
		t.Fatalf("incremental permission = %q, %v", pid, ok)
	}
	if cmd == nil {
		t.Fatal("stream listener was not continued")
	}
}

func TestFirstStreamEventGapFallsBackWithoutApplying(t *testing.T) {
	d := sampleDetail()
	d.Events = nil
	d.States["w1"] = "pending"
	m := New(&fakeAPI{}, context.Background())
	m.selectedID, m.detail, m.mode = "run_1", d, modeDetail
	items := make(chan eventStreamItem)
	m.streamItems = items
	m.streamCancel = func() {}
	event := EventView{Seq: 2, RunID: "run_1", Type: "transition", NodeID: "w1", From: "pending", To: "running"}

	_, cmd := m.Update(eventStreamItemMsg{runID: "run_1", items: items, item: eventStreamItem{event: &event}})
	if m.eventCursor != 0 || m.detail.States["w1"] != "pending" {
		t.Fatalf("gapped event applied: cursor=%d state=%q", m.eventCursor, m.detail.States["w1"])
	}
	if m.streamItems != nil || cmd == nil {
		t.Fatal("gapped event did not stop stream and request full refresh")
	}
}

func TestStaleFullRefreshDoesNotOverwriteNewerEventState(t *testing.T) {
	current := sampleDetail()
	current.Events = []EventView{{Seq: 5, Type: "transition", NodeID: "w1", From: "leased", To: "running"}}
	current.States["w1"] = "running"
	m := New(&fakeAPI{}, context.Background())
	m.selectedID, m.detail, m.mode, m.eventCursor = "run_1", current, modeDetail, 5

	stale := sampleDetail()
	stale.Events = []EventView{{Seq: 4, Type: "transition", NodeID: "w1", From: "ready", To: "leased"}}
	stale.States["w1"] = "leased"
	m.Update(fetchRunMsg{runID: "run_1", detail: stale})
	if m.detail.States["w1"] != "running" || m.eventCursor != 5 {
		t.Fatalf("stale refresh regressed state: cursor=%d state=%q", m.eventCursor, m.detail.States["w1"])
	}
}

func TestDroppedStreamFallsBackToFullRefreshAndReconnectsFromCursor(t *testing.T) {
	api := &fakeAPI{detail: sampleDetail()}
	m := New(api, context.Background())
	m.selectedID, m.detail, m.mode, m.eventCursor = "run_1", sampleDetail(), modeDetail, 9
	items := make(chan eventStreamItem)
	m.streamItems = items
	canceled := false
	m.streamCancel = func() { canceled = true }

	_, cmd := m.Update(eventStreamItemMsg{runID: "run_1", items: items, item: eventStreamItem{err: fmt.Errorf("dropped")}})
	if !canceled || m.streamItems != nil || cmd == nil {
		t.Fatalf("drop did not stop stream/fallback: canceled=%v items=%v cmd=%v", canceled, m.streamItems, cmd)
	}
	msg := cmd()
	refresh, ok := msg.(fetchRunMsg)
	if !ok || refresh.detail == nil || refresh.runID != "run_1" {
		t.Fatalf("fallback = %#v, want full run refresh", msg)
	}

	_, cmd = m.Update(tickMsg(time.Now()))
	batch, ok := cmd().(tea.BatchMsg)
	if !ok {
		t.Fatalf("reconnect tick = %T, want batch", cmd())
	}
	var ready eventStreamReadyMsg
	for _, nested := range batch {
		if nested == nil {
			continue
		}
		if msg := nested(); msg != nil {
			if value, ok := msg.(eventStreamReadyMsg); ok {
				ready = value
			}
		}
	}
	if ready.items == nil {
		t.Fatal("fallback tick did not reconnect stream")
	}
	ready.cancel()
	deadline := time.Now().Add(time.Second)
	for len(api.streamCursors()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cursors := api.streamCursors()
	if len(cursors) == 0 || cursors[len(cursors)-1] != 9 {
		t.Fatalf("reconnect cursors = %v, want last cursor 9", cursors)
	}
}

func TestHealthyStreamPreventsOneSecondFullPolling(t *testing.T) {
	m := New(&fakeAPI{}, context.Background())
	m.selectedID, m.detail, m.mode = "run_1", sampleDetail(), modeDetail
	m.streamItems = make(chan eventStreamItem)
	_, cmd := m.Update(tickMsg(time.Now()))
	if _, ok := cmd().(fetchRunMsg); ok {
		t.Fatal("healthy event stream still triggered one-second full polling")
	}
}

func TestWaitingRunKeepsStreamForExternalResolution(t *testing.T) {
	d := sampleDetail()
	d.Status = "waiting"
	d.Done = true
	m := New(&fakeAPI{}, context.Background())
	m.selectedID, m.detail, m.mode, m.eventCursor = "run_1", d, modeDetail, 4
	items := make(chan eventStreamItem)
	m.streamItems = items
	canceled := false
	m.streamCancel = func() { canceled = true }
	event := EventView{Seq: 5, RunID: "run_1", Type: "run", Payload: json.RawMessage(`{"status":"waiting"}`)}
	_, cmd := m.Update(eventStreamItemMsg{runID: "run_1", items: items, item: eventStreamItem{event: &event}})
	if canceled || m.streamItems == nil {
		t.Fatal("waiting run stopped stream needed for later permission resolution")
	}
	if cmd == nil {
		t.Fatal("waiting run did not keep stream listener")
	}
}

func TestTerminalRunEventStopsWithoutAnotherStreamWait(t *testing.T) {
	d := sampleDetail()
	d.States["w1"] = "done"
	m := New(&fakeAPI{detail: d}, context.Background())
	m.selectedID, m.detail, m.mode, m.eventCursor = "run_1", d, modeDetail, 4
	items := make(chan eventStreamItem)
	m.streamItems = items
	m.streamCancel = func() {}
	event := EventView{Seq: 5, RunID: "run_1", Type: "run", Payload: json.RawMessage(`{"status":"completed"}`)}

	_, cmd := m.Update(eventStreamItemMsg{runID: "run_1", items: items, item: eventStreamItem{event: &event}})
	if m.streamItems != nil || !m.runTerminal() {
		t.Fatal("terminal run event did not stop stream")
	}
	if _, ok := cmd().(tea.BatchMsg); ok {
		t.Fatal("terminal event enqueued another wait on stopped stream")
	}
}

func TestInitialWaitingRunStartsEventStream(t *testing.T) {
	d := sampleDetail()
	d.Status = "waiting"
	d.Done = true
	m := New(&fakeAPI{}, context.Background())
	m.selectedID, m.mode = "run_1", modeDetail
	_, cmd := m.Update(fetchRunMsg{runID: "run_1", detail: d})
	if cmd == nil || !m.streamConnecting {
		t.Fatal("initial waiting run did not start event stream")
	}
}

func TestWaitingRunDoesNotRenderDoneMarker(t *testing.T) {
	d := sampleDetail()
	d.Status = "waiting"
	d.Done = true
	m := New(&fakeAPI{}, context.Background())
	m.selectedID, m.detail, m.mode = "run_1", d, modeDetail
	if got := m.View(); strings.Contains(got, "✓ done") {
		t.Fatalf("waiting run rendered terminal marker:\n%s", got)
	}
}

func int64Ptr(v int64) *int64 { return &v }

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
