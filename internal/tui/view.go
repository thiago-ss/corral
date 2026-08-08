package tui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

var (
	styleHeader   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	styleSelected = lipgloss.NewStyle().Background(lipgloss.Color("237"))
	styleDim      = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	styleTitle    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("39"))
	styleError    = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	styleOK       = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	styleMuted    = lipgloss.NewStyle().Foreground(lipgloss.Color("244"))
)

func stateColor(s string) lipgloss.Style {
	switch s {
	case "done":
		return styleOK
	case "running", "verifying":
		return lipgloss.NewStyle().Foreground(lipgloss.Color("39")) // cyan/blue
	case "leased", "retry_wait", "ready":
		return lipgloss.NewStyle().Foreground(lipgloss.Color("214")) // yellow
	case "blocked":
		return lipgloss.NewStyle().Foreground(lipgloss.Color("220")) // amber
	case "failed":
		return lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	case "canceled":
		return lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	default: // pending
		return styleMuted
	}
}

func (m *Model) View() string {
	switch m.mode {
	case modeList:
		return m.viewList()
	case modeDetail:
		return m.viewDetail()
	case modeInspect:
		return m.viewInspect()
	case modeSteer:
		return m.viewSteer()
	}
	return ""
}

func (m *Model) viewList() string {
	var b strings.Builder
	b.WriteString(styleHeader.Render("corral — runs") + "\n\n")
	if len(m.runs) == 0 {
		b.WriteString(styleDim.Render("no runs yet; start one from OpenCode (corral_start) or the daemon API") + "\n")
	} else {
		for i, r := range m.runs {
			line := fmt.Sprintf(" %-28s %-10s %s", r.ID, r.Status, r.StateSummary())
			if i == m.cursor {
				line = styleSelected.Render(line)
			}
			if r.Done {
				line += styleOK.Render(" ✓")
			}
			b.WriteString(line + "\n")
		}
	}
	b.WriteString("\n" + m.footer("↑/↓ select · enter detail · q quit"))
	return b.String()
}

func (m *Model) viewDetail() string {
	if m.detail == nil {
		return "loading…"
	}
	var b strings.Builder
	b.WriteString(styleTitle.Render(fmt.Sprintf("run %s  [%s]", m.detail.RunID, m.detail.Status)))
	if m.detail.Done {
		b.WriteString(styleOK.Render(" ✓ done"))
	}
	b.WriteString("\n\n")

	// DAG view: nodes in dependency order, arrows between them.
	b.WriteString(styleDim.Render("dag") + "\n")
	nodes := m.detail.Graph.Nodes
	byID := map[string]GraphNode{}
	for _, n := range nodes {
		byID[n.ID] = n
	}
	// Topological-ish order: by state then id for stable rendering.
	order := append([]GraphNode(nil), nodes...)
	sort.SliceStable(order, func(i, j int) bool { return order[i].ID < order[j].ID })
	indeg := map[string]int{}
	for _, n := range order {
		indeg[n.ID] = len(n.DependsOn)
	}
	for _, n := range order {
		line := "  " + m.nodeLine(n, indeg[n.ID])
		if n.ID == m.detail.Graph.Nodes[m.nodeCursor].ID {
			line = styleSelected.Render(line)
		}
		b.WriteString(line + "\n")
		for _, dep := range n.DependsOn {
			if dn, ok := byID[dep]; ok {
				b.WriteString(styleDim.Render(fmt.Sprintf("      ← %s (%s)\n", dep, m.detail.States[dep])))
				_ = dn
			}
		}
	}
	b.WriteString("\n" + m.footer("↑/↓ node · a approve · r reject · c cancel · t retry · s steer · i inspect · esc back · q quit"))
	return b.String()
}

func (m *Model) nodeLine(n GraphNode, deps int) string {
	state := m.detail.States[n.ID]
	st := stateColor(state).Render(pad(state, 10))
	typ := styleDim.Render(n.Type)
	prio := styleDim.Render(fmt.Sprintf("p%d", n.Priority))
	atts := m.detail.Attempts[n.ID]
	attempts := styleDim.Render(fmt.Sprintf("%d att", len(atts)))
	depsS := ""
	if deps > 0 {
		depsS = styleDim.Render(fmt.Sprintf("(%d deps)", deps))
	}
	return fmt.Sprintf("%-12s %s %-7s %s %s %s", n.ID, st, typ, prio, attempts, depsS)
}

func (m *Model) viewInspect() string {
	if m.detail == nil {
		return "loading…"
	}
	var b strings.Builder
	n := m.findNode(m.inspectNode)
	if n == nil {
		return "node gone"
	}
	state := m.detail.States[n.ID]
	b.WriteString(styleTitle.Render(fmt.Sprintf("node %s  %s", n.ID, stateColor(state).Render(state))) + "\n\n")
	b.WriteString(styleDim.Render("objective: ") + n.Objective + "\n")
	if n.Role != "" {
		b.WriteString(styleDim.Render("role: ") + n.Role + "\n")
	}
	if len(n.WriteScope) > 0 {
		b.WriteString(styleDim.Render("write scope: ") + strings.Join(n.WriteScope, ", ") + "\n")
	}
	if n.Verification != nil {
		b.WriteString(styleDim.Render("verification: ") + n.Verification.Kind + " " + strings.Join(n.Verification.Command, " ") + "\n")
	}
	b.WriteString("\n" + styleDim.Render("attempts") + "\n")
	for _, at := range m.detail.Attempts[n.ID] {
		b.WriteString(fmt.Sprintf("  #%d %-10s", at.No, stateColor(at.Status).Render(at.Status)))
		if at.SessionID != "" {
			b.WriteString(styleMuted.Render(" session=" + at.SessionID))
		}
		if at.Worktree != "" {
			b.WriteString(styleMuted.Render(" worktree=" + shortPath(at.Worktree)))
		}
		if at.StartedAt != nil {
			b.WriteString(styleMuted.Render(" " + elapsed(*at.StartedAt, at.FinishedAt)))
		}
		b.WriteString("\n")
		if at.Cost > 0 || at.Tokens > 0 {
			b.WriteString(styleMuted.Render(fmt.Sprintf("     $%.4f  %d tok\n", at.Cost, at.Tokens)))
		}
		if at.Evidence != "" {
			b.WriteString(styleMuted.Render("     evidence: "+shortLine(at.Evidence, 90)) + "\n")
		}
	}
	b.WriteString("\n" + m.footer("esc back · ↑/↓ navigate · a/r/c/t/s act"))
	return b.String()
}

func (m *Model) viewSteer() string {
	var b strings.Builder
	b.WriteString(styleTitle.Render(fmt.Sprintf("steer %s/%s", m.selectedID, m.steerNode)) + "\n\n")
	b.WriteString("message: " + m.steerInput + "▌\n\n")
	b.WriteString(m.footer("type message · enter send · esc cancel"))
	return b.String()
}

func (m *Model) footer(keys string) string {
	var b strings.Builder
	b.WriteString(styleDim.Render("── " + keys))
	if m.status != "" {
		b.WriteString(styleOK.Render("  · " + m.status))
	}
	if m.err != nil {
		b.WriteString(styleError.Render("  · " + m.err.Error()))
	}
	return b.String()
}

func (m *Model) findNode(id string) *GraphNode {
	if m.detail == nil {
		return nil
	}
	for i := range m.detail.Graph.Nodes {
		if m.detail.Graph.Nodes[i].ID == id {
			return &m.detail.Graph.Nodes[i]
		}
	}
	return nil
}

func (r RunSummary) StateSummary() string {
	var parts []string
	for _, id := range sortedKeys(r.States) {
		parts = append(parts, fmt.Sprintf("%s:%s", id, stateColor(r.States[id]).Render(r.States[id])))
	}
	return strings.Join(parts, " ")
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func pad(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}

func shortLine(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func shortPath(p string) string {
	if len(p) <= 48 {
		return p
	}
	return "…" + p[len(p)-48:]
}

func elapsed(start int64, end *int64) string {
	e := start
	if end != nil {
		e = *end
	}
	d := time.Duration(e-start) * time.Millisecond
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return d.Round(time.Second).String()
}
