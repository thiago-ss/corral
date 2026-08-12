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
	detail *RunDetail
	err    error
}

type tailMsg struct {
	node  string
	lines []string
	err   error
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

	// prevStates records the last seen node states so attention only
	// fires on transitions (gate awaiting approval, node failed).
	prevStates map[string]string
	// notified remembers which attention conditions have already been
	// announced, keyed by runID/nodeID/condition.
	notified map[string]bool
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
	return func() tea.Msg {
		d, err := m.api.GetRun(m.ctx, m.selectedID)
		return fetchRunMsg{detail: d, err: err}
	}
}

func fetchTailCmd(m *Model) tea.Cmd {
	return func() tea.Msg {
		lines, err := m.api.Tail(m.ctx, m.selectedID, m.tailNode, 40)
		return tailMsg{node: m.tailNode, lines: lines, err: err}
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
		cmd := tickCmd(m.tick)
		switch m.mode {
		case modeList:
			return m, tea.Batch(cmd, fetchRunsCmd(m))
		case modeDetail, modeInspect:
			cmds := []tea.Cmd{cmd, fetchRunCmd(m)}
			if m.mode == modeInspect && m.tailNode != "" {
				cmds = append(cmds, fetchTailCmd(m))
			}
			return m, tea.Batch(cmds...)
		}
		return m, cmd

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
		if v.err != nil || v.detail == nil {
			if v.err != nil {
				m.err = v.err
			}
		} else {
			m.detail = v.detail
			m.err = nil
			if m.nodeCursor >= len(m.detail.Graph.Nodes) {
				m.nodeCursor = 0
			}
			m.checkAttention()
		}
		return m, nil

	case tailMsg:
		if v.err == nil && v.node == m.tailNode {
			m.tail = v.lines
		}
		return m, nil

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
				m.selectedID = m.runs[m.cursor].ID
				m.mode = modeDetail
				m.nodeCursor = 0
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
				return m, nil
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
	if m.detail == nil {
		return "", false
	}
	pid := ""
	for _, ev := range m.detail.Events {
		if ev.NodeID != nodeID || ev.To != "blocked" || len(ev.Payload) == 0 {
			continue
		}
		var p struct {
			Reason       string `json:"reason"`
			PermissionID string `json:"permissionID"`
		}
		if json.Unmarshal(ev.Payload, &p) == nil && p.Reason == "permission" && p.PermissionID != "" {
			pid = p.PermissionID
		}
	}
	if pid == "" {
		return "", false
	}
	return pid, true
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
		m.mode = modeList
		m.detail = nil
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

// checkAttention fires a terminal attention notification when a node's
// state demands it: a human gate awaiting approval (running) or any node
// that fails. Each condition is announced once per transition.
func (m *Model) checkAttention() {
	if m.detail == nil || m.notify == nil {
		return
	}
	if m.prevStates == nil {
		m.prevStates = map[string]string{}
	}
	if m.notified == nil {
		m.notified = map[string]bool{}
	}
	for _, n := range m.detail.Graph.Nodes {
		cur := m.detail.States[n.ID]
		prev := m.prevStates[n.ID]
		m.prevStates[n.ID] = cur
		key := m.detail.RunID + "/" + n.ID

		// Drop remembered conditions that no longer hold, so a later
		// recurrence (retry, re-opened gate) announces again.
		if cur != "running" {
			delete(m.notified, key+"/gate")
		}
		if cur != "failed" {
			delete(m.notified, key+"/failed")
		}

		if cur == "running" && prev != "running" && n.Type == "human_gate" && !m.notified[key+"/gate"] {
			m.notified[key+"/gate"] = true
			m.notify("corral: gate awaits approval",
				fmt.Sprintf("gate %s on run %s awaits approval", n.ID, m.detail.RunID))
		}
		if cur == "failed" && prev != "failed" && !m.notified[key+"/failed"] {
			m.notified[key+"/failed"] = true
			m.notify("corral: node failed",
				fmt.Sprintf("node %s on run %s failed", n.ID, m.detail.RunID))
		}
	}
}
