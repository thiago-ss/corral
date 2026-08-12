package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

type tickMsg time.Time

func tickCmd(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(t time.Time) tea.Msg { return tickMsg(t) })
}

type fetchMsg struct {
	runs []RunSummary
	err  error
}

type fetchRunMsg struct {
	runID  string
	detail *RunDetail
	err    error
}

type tailMsg struct {
	runID string
	node  string
	lines []string
	err   error
}

type eventStreamItem struct {
	event *EventView
	err   error
}

type eventStreamReadyMsg struct {
	runID  string
	items  <-chan eventStreamItem
	cancel context.CancelFunc
}

type eventStreamItemMsg struct {
	runID string
	items <-chan eventStreamItem
	item  eventStreamItem
}

type actionMsg struct {
	label string
	err   error
}

type viewMode int

const (
	modeList viewMode = iota
	modeDetail
	modeInspect
	modeSteer
)

// Model is the TUI state machine. All state transitions happen in
// Update, which makes it fully testable without a terminal.
type Model struct {
	api  API
	tick time.Duration
	ctx  context.Context
	mode viewMode

	runs        []RunSummary
	cursor      int
	selectedID  string
	detail      *RunDetail
	nodeCursor  int
	inspectNode string

	steerNode  string
	steerInput string

	// tail holds the last-fetched live attempt tail for the inspected
	// node. tailNode is set when the inspect view is active.
	tailNode string
	tail     []string

	// Durable event stream state. eventCursor is the greatest sequence
	// incorporated into detail. Full polling is used only while this stream
	// is unavailable, plus the initial/action refreshes.
	eventCursor      int64
	streamRun        string
	streamItems      <-chan eventStreamItem
	streamCancel     context.CancelFunc
	streamConnecting bool
	streamAttempted  bool

	// prevStates records the last seen node states so attention only
	// fires on transitions (gate awaiting approval, node failed).
	prevStates map[string]string
	// notify delivers terminal attention; overridden in tests.
	notify func(title, body string)

	status string
	err    error

	width, height int
	lastFetch     time.Time
}

func New(api API, ctx context.Context) *Model {
	return &Model{api: api, ctx: ctx, tick: time.Second}
}

// EnableAttention arms terminal attention notifications (bell + desktop
// notification) for gates awaiting approval and failed nodes. The model
// ships with attention disabled so unit tests stay side-effect free; the
// real TUI calls this once at startup.
func (m *Model) EnableAttention() { m.notify = NotifyAttention }

func (m *Model) Init() tea.Cmd {
	return tea.Batch(fetchRunsCmd(m), tickCmd(m.tick))
}

func fetchRunsCmd(m *Model) tea.Cmd {
	return func() tea.Msg {
		runs, err := m.api.ListRuns(m.ctx)
		return fetchMsg{runs: runs, err: err}
	}
}

func fetchRunCmd(m *Model) tea.Cmd {
	runID := m.selectedID
	return func() tea.Msg {
		d, err := m.api.GetRun(m.ctx, runID)
		return fetchRunMsg{runID: runID, detail: d, err: err}
	}
}

func fetchTailCmd(m *Model) tea.Cmd {
	runID, node := m.selectedID, m.tailNode
	return func() tea.Msg {
		lines, err := m.api.Tail(m.ctx, runID, node, 40)
		return tailMsg{runID: runID, node: node, lines: lines, err: err}
	}
}

func startEventStreamCmd(m *Model) tea.Cmd {
	api, parent := m.api, m.ctx
	runID, after := m.selectedID, m.eventCursor
	return func() tea.Msg {
		ctx, cancel := context.WithCancel(parent)
		items := make(chan eventStreamItem, 64)
		go func() {
			err := api.StreamEvents(ctx, runID, after, func(event EventView) error {
				select {
				case items <- eventStreamItem{event: &event}:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			select {
			case items <- eventStreamItem{err: err}:
			case <-ctx.Done():
			}
			close(items)
		}()
		return eventStreamReadyMsg{runID: runID, items: items, cancel: cancel}
	}
}

func waitEventStreamCmd(runID string, items <-chan eventStreamItem) tea.Cmd {
	return func() tea.Msg {
		item, ok := <-items
		if !ok {
			item.err = fmt.Errorf("event stream closed")
		}
		return eventStreamItemMsg{runID: runID, items: items, item: item}
	}
}

func actionCmd(m *Model, label string, fn func(context.Context) error) tea.Cmd {
	return func() tea.Msg {
		return actionMsg{label: label, err: fn(m.ctx)}
	}
}

func (m *Model) nodeAt(idx int) (string, bool) {
	if m.detail == nil || idx < 0 || idx >= len(m.detail.Graph.Nodes) {
		return "", false
	}
	return m.detail.Graph.Nodes[idx].ID, true
}

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch v := msg.(type) {
	case tickMsg:
		m.lastFetch = time.Time(v)
		cmds := []tea.Cmd{tickCmd(m.tick)}
		switch m.mode {
		case modeList:
			cmds = append(cmds, fetchRunsCmd(m))
		case modeDetail, modeInspect:
			if m.detail == nil || (!m.runTerminal() && m.streamItems == nil && !m.streamConnecting) {
				// Initial/full refresh and fallback polling while SSE is down.
				cmds = append(cmds, fetchRunCmd(m))
				if m.detail != nil && !m.runTerminal() {
					m.streamConnecting = true
					m.streamAttempted = true
					cmds = append(cmds, startEventStreamCmd(m))
				}
			}
			if m.mode == modeInspect && m.tailNode != "" {
				cmds = append(cmds, fetchTailCmd(m))
			}
		}
		return m, tea.Batch(cmds...)

	case fetchMsg:
		if v.err != nil {
			m.err = v.err
		} else {
			m.runs = v.runs
			m.err = nil
			if m.cursor >= len(m.runs) {
				m.cursor = 0
			}
		}
		return m, nil

	case fetchRunMsg:
		if v.runID != "" && v.runID != m.selectedID {
			return m, nil
		}
		if v.err != nil || v.detail == nil {
			if v.err != nil {
				m.err = v.err
			}
		} else {
			initial := m.detail == nil || m.detail.RunID != v.detail.RunID
			m.detail = v.detail
			m.err = nil
			for _, event := range m.detail.Events {
				if event.Seq > m.eventCursor {
					m.eventCursor = event.Seq
				}
			}
			if m.nodeCursor >= len(m.detail.Graph.Nodes) {
				m.nodeCursor = 0
			}
			var cmds []tea.Cmd
			if initial {
				m.seedAttentionStates()
			} else if cmd := m.checkAttention(); cmd != nil {
				cmds = append(cmds, cmd)
			}
			if !m.runTerminal() && m.streamItems == nil && !m.streamConnecting && !m.streamAttempted {
				m.streamConnecting = true
				m.streamAttempted = true
				cmds = append(cmds, startEventStreamCmd(m))
			}
			return m, tea.Batch(cmds...)
		}
		return m, nil

	case tailMsg:
		if v.err == nil && v.runID == m.selectedID && v.node == m.tailNode {
			m.tail = v.lines
		}
		return m, nil

	case eventStreamReadyMsg:
		if v.runID != m.selectedID || (m.mode != modeDetail && m.mode != modeInspect) {
			v.cancel()
			return m, nil
		}
		if m.streamCancel != nil {
			m.streamCancel()
		}
		m.streamRun = v.runID
		m.streamItems = v.items
		m.streamCancel = v.cancel
		m.streamConnecting = false
		return m, waitEventStreamCmd(v.runID, v.items)

	case eventStreamItemMsg:
		if v.runID != m.selectedID || v.items != m.streamItems {
			return m, nil
		}
		if v.item.err != nil {
			m.stopEventStream()
			m.status = "event stream unavailable; polling"
			if m.detail != nil && !m.runTerminal() {
				return m, fetchRunCmd(m)
			}
			return m, nil
		}
		if v.item.event == nil {
			return m, waitEventStreamCmd(v.runID, v.items)
		}
		event := *v.item.event
		if event.RunID != "" && event.RunID != m.selectedID {
			m.stopEventStream()
			m.status = "event stream mismatched run; polling"
			return m, fetchRunCmd(m)
		}
		if event.Seq <= m.eventCursor {
			return m, waitEventStreamCmd(v.runID, v.items)
		}
		if m.eventCursor > 0 && event.Seq != m.eventCursor+1 {
			m.stopEventStream()
			m.status = "event stream gap; polling"
			return m, fetchRunCmd(m)
		}
		needsRefresh := m.applyEvent(event)
		cmds := []tea.Cmd{waitEventStreamCmd(v.runID, v.items)}
		if cmd := m.checkAttention(); cmd != nil {
			cmds = append(cmds, cmd)
		}
		if needsRefresh {
			cmds = append(cmds, fetchRunCmd(m))
		}
		return m, tea.Batch(cmds...)

	case actionMsg:
		m.status = v.label
		if v.err != nil {
			m.err = v.err
		}
		// Refresh immediately after an action.
		if m.mode == modeDetail || m.mode == modeInspect {
			return m, fetchRunCmd(m)
		}
		return m, fetchRunsCmd(m)

	case tea.WindowSizeMsg:
		m.width, m.height = v.Width, v.Height
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(v)

	default:
		return m, nil
	}
}

func (m *Model) handleKey(k tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch k.String() {
	case "ctrl+c", "q":
		if m.mode == modeList {
			return m, tea.Quit
		}
		m.back()
		return m, nil
	case "esc":
		m.back()
		return m, nil

	case "enter":
		switch m.mode {
		case modeList:
			if m.cursor < len(m.runs) {
				m.stopEventStream()
				m.selectedID = m.runs[m.cursor].ID
				m.mode = modeDetail
				m.nodeCursor = 0
				m.detail = nil
				m.eventCursor = 0
				m.streamAttempted = false
				return m, fetchRunCmd(m)
			}
		case modeSteer:
			return m, actionCmd(m, "steered "+m.steerNode, func(ctx context.Context) error {
				return m.api.Steer(ctx, m.selectedID, m.steerNode, m.steerInput)
			})
		}
		return m, nil

	case "j", "down":
		m.move(1)
	case "k", "up":
		m.move(-1)
	case "g":
		m.moveTo(0)
	case "G":
		m.moveTo(1 << 30)
	}

	switch m.mode {
	case modeDetail:
		switch k.String() {
		case "i":
			if id, ok := m.nodeAt(m.nodeCursor); ok {
				m.inspectNode = id
				m.tailNode = id
				m.tail = nil
				m.mode = modeInspect
				return m, fetchTailCmd(m)
			}
		case "a":
			return m, m.nodeAction("approved", m.api.Approve)
		case "r":
			return m, m.nodeAction("rejected", m.api.Reject)
		case "c":
			return m, m.nodeAction("canceled", m.api.Cancel)
		case "t":
			return m, m.nodeAction("retried", m.api.Retry)
		case "p":
			return m, m.permissionAction("allowed", true)
		case "d":
			return m, m.permissionAction("denied", false)
		case "s":
			if id, ok := m.nodeAt(m.nodeCursor); ok {
				m.steerNode = id
				m.steerInput = ""
				m.mode = modeSteer
			}
		}
	case modeSteer:
		switch k.String() {
		case "backspace":
			if len(m.steerInput) > 0 {
				m.steerInput = m.steerInput[:len(m.steerInput)-1]
			}
		default:
			if len(k.Runes) > 0 && k.Runes[0] >= 32 {
				m.steerInput += string(k.Runes)
			}
		}
	}
	return m, nil
}

func (m *Model) nodeAction(label string, fn func(context.Context, string, string) error) tea.Cmd {
	id, ok := m.nodeAt(m.nodeCursor)
	if !ok {
		return nil
	}
	runID := m.selectedID
	m.status = fmt.Sprintf("%s %s/%s", label, runID, id)
	return actionCmd(m, label, func(ctx context.Context) error {
		return fn(ctx, runID, id)
	})
}

// pendingPermission returns the permission id the node is currently blocked
// on, if its latest blocked transition carried a permission request.
func (m *Model) pendingPermission(nodeID string) (string, bool) {
	if m.detail == nil || m.detail.States[nodeID] != "blocked" {
		return "", false
	}
	for i := len(m.detail.Events) - 1; i >= 0; i-- {
		ev := m.detail.Events[i]
		if ev.NodeID != nodeID || ev.Type != "transition" || ev.To != "blocked" {
			continue
		}
		var p struct {
			Reason       string `json:"reason"`
			PermissionID string `json:"permissionID"`
		}
		if json.Unmarshal(ev.Payload, &p) == nil && p.Reason == "permission" && p.PermissionID != "" {
			return p.PermissionID, true
		}
		// Only the latest transition into blocked describes why the
		// current blocked state exists. Never fall back to an older request.
		return "", false
	}
	return "", false
}

// permissionAction answers the pending permission of the node under the
// cursor; it is a no-op unless the node is blocked on a permission.
func (m *Model) permissionAction(label string, allow bool) tea.Cmd {
	id, ok := m.nodeAt(m.nodeCursor)
	if !ok {
		return nil
	}
	pid, ok := m.pendingPermission(id)
	if !ok {
		return nil
	}
	runID := m.selectedID
	m.status = fmt.Sprintf("%s permission %s/%s", label, runID, id)
	return actionCmd(m, label, func(ctx context.Context) error {
		return m.api.RespondPermission(ctx, runID, id, pid, allow)
	})
}

func (m *Model) back() {
	switch m.mode {
	case modeInspect:
		m.mode = modeDetail
		m.tailNode = ""
		m.tail = nil
	case modeSteer:
		m.mode = modeDetail
	case modeDetail:
		m.stopEventStream()
		m.mode = modeList
		m.detail = nil
		m.eventCursor = 0
		m.streamAttempted = false
	}
}

func (m *Model) move(delta int) {
	switch m.mode {
	case modeList:
		m.cursor = clamp(m.cursor+delta, 0, len(m.runs)-1)
	case modeDetail, modeInspect:
		if m.detail == nil {
			return
		}
		if m.mode == modeInspect {
			// keep inspect on the node; move between nodes too
		}
		m.nodeCursor = clamp(m.nodeCursor+delta, 0, len(m.detail.Graph.Nodes)-1)
		if m.mode == modeInspect {
			if id, ok := m.nodeAt(m.nodeCursor); ok {
				m.inspectNode = id
				m.tailNode = id
				m.tail = nil
			}
		}
	}
}

func (m *Model) moveTo(idx int) {
	switch m.mode {
	case modeList:
		m.cursor = clamp(idx, 0, len(m.runs)-1)
	case modeDetail, modeInspect:
		if m.detail == nil {
			return
		}
		m.nodeCursor = clamp(idx, 0, len(m.detail.Graph.Nodes)-1)
		if m.mode == modeInspect {
			if id, ok := m.nodeAt(m.nodeCursor); ok {
				m.inspectNode = id
				m.tailNode = id
				m.tail = nil
			}
		}
	}
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if hi < lo || v > hi {
		return hi
	}
	return v
}

// SelectedNode returns the node under the cursor (for tests).
func (m *Model) SelectedNode() (string, bool) { return m.nodeAt(m.nodeCursor) }

// CurrentRun returns the selected run id (for tests).
func (m *Model) CurrentRun() string { return m.selectedID }

func (m *Model) stopEventStream() {
	if m.streamCancel != nil {
		m.streamCancel()
	}
	m.streamRun = ""
	m.streamItems = nil
	m.streamCancel = nil
	m.streamConnecting = false
}

func (m *Model) runTerminal() bool {
	return m.detail != nil && (m.detail.Status == "completed" || m.detail.Status == "canceled")
}

func (m *Model) seedAttentionStates() {
	if m.detail == nil {
		return
	}
	if m.prevStates == nil {
		m.prevStates = map[string]string{}
	}
	for _, node := range m.detail.Graph.Nodes {
		m.prevStates[m.detail.RunID+"/"+node.ID] = m.detail.States[node.ID]
	}
}

// applyEvent incrementally materializes the state carried by one raw
// durable store.Event. It returns true when a full detail refresh is
// useful for data not present in the event (attempt rows or graph edits).
func (m *Model) applyEvent(event EventView) bool {
	if m.detail == nil {
		return true
	}
	m.eventCursor = event.Seq
	m.detail.Events = append(m.detail.Events, event)
	switch event.Type {
	case "transition", "recovery":
		if event.NodeID != "" && event.To != "" {
			if m.detail.States == nil {
				m.detail.States = map[string]string{}
			}
			m.detail.States[event.NodeID] = event.To
		}
		return false
	case "run":
		var payload struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(event.Payload, &payload) == nil && payload.Status != "" {
			m.detail.Status = payload.Status
			m.detail.Done = payload.Status != "active" && payload.Status != "created"
		}
		// Attempt finalization is durable before the terminal/waiting run
		// event. Refresh once here so transcript metadata and usage settle.
		if m.runTerminal() {
			m.stopEventStream()
		}
		return true
	case "attempt", "verdict", "graph":
		return true
	default:
		return false
	}
}

// checkAttention fires a terminal attention notification when a node's
// state demands it: a human gate awaiting approval (running) or any node
// that fails. Each condition is announced once per transition.
func (m *Model) checkAttention() tea.Cmd {
	if m.detail == nil {
		return nil
	}
	if m.prevStates == nil {
		m.prevStates = map[string]string{}
	}
	var cmds []tea.Cmd
	for _, n := range m.detail.Graph.Nodes {
		cur := m.detail.States[n.ID]
		key := m.detail.RunID + "/" + n.ID
		prev := m.prevStates[key]
		m.prevStates[key] = cur
		if m.notify == nil {
			continue
		}
		var title, body string
		switch {
		case cur == "running" && prev != "running" && n.Type == "human_gate":
			title = "corral: gate awaits approval"
			body = fmt.Sprintf("gate %s on run %s awaits approval", n.ID, m.detail.RunID)
		case cur == "failed" && prev != "failed":
			title = "corral: node failed"
			body = fmt.Sprintf("node %s on run %s failed", n.ID, m.detail.RunID)
		}
		if title != "" {
			notify := m.notify
			titleCopy, bodyCopy := title, body
			cmds = append(cmds, func() tea.Msg {
				notify(titleCopy, bodyCopy)
				return nil
			})
		}
	}
	if len(cmds) == 1 {
		return cmds[0]
	}
	if len(cmds) > 1 {
		return tea.Batch(cmds...)
	}
	return nil
}
