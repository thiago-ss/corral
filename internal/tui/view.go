package tui

import (
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
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

// progressBar renders a horizontal bar of the given width with the filled
// fraction (0..1). Empty fill uses a bright color, so near-full usage is
// easy to spot.
func progressBar(frac float64, width int) string {
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	filled := int(frac*float64(width) + 0.5)
	body := strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
	if frac >= 1 {
		return styleError.Render(body)
	}
	if frac >= 0.85 {
		return lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Render(body)
	}
	return styleOK.Render(body)
}

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

func stripUnsafeControls(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '\n', '\t':
			return r
		}
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, s)
}

func safeText(s string) string {
	return stripUnsafeControls(ansi.Strip(s))
}

func (m *Model) viewList() string {
	var b strings.Builder
	b.WriteString(styleHeader.Render("corral — runs") + "\n\n")
	if len(m.runs) == 0 {
		b.WriteString(styleDim.Render("no runs yet; start one from OpenCode (corral_start) or the daemon API") + "\n")
	} else {
		for i, r := range m.runs {
			line := fmt.Sprintf(" %-28s %-10s %s", safeText(r.ID), safeText(r.Status), r.StateSummary())
			if i == m.cursor {
				line = styleSelected.Render(line)
			}
			if r.Status == "completed" {
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
	b.WriteString(styleTitle.Render(fmt.Sprintf("run %s  [%s]", safeText(m.detail.RunID), safeText(m.detail.Status))))
	if m.detail.Status == "completed" {
		b.WriteString(styleOK.Render(" ✓ done"))
	}
	// Overall run progress: done nodes / total.
	total := len(m.detail.Graph.Nodes)
	if total > 0 {
		done := 0
		for _, n := range m.detail.Graph.Nodes {
			if m.detail.States[n.ID] == "done" {
				done++
			}
		}
		b.WriteString("\n" + styleDim.Render("progress ") + progressBar(float64(done)/float64(total), 20) +
			styleMuted.Render(fmt.Sprintf(" %d/%d done", done, total)))
	}
	b.WriteString("\n\n")

	// DAG view: nodes in dependency order, arrows between them.
	b.WriteString(styleDim.Render("dag") + "\n")
	nodes := m.detail.Graph.Nodes
	if len(nodes) == 0 {
		b.WriteString(styleMuted.Render("  no nodes") + "\n")
	}
	byID := map[string]GraphNode{}
	for _, n := range nodes {
		byID[n.ID] = n
	}
	indeg := map[string]int{}
	for _, n := range nodes {
		indeg[n.ID] = len(n.DependsOn)
	}
	selected, _ := m.nodeAt(m.nodeCursor)
	for _, n := range nodes {
		line := "  " + m.nodeLine(n, indeg[n.ID])
		if n.ID == selected {
			line = styleSelected.Render(line)
		}
		b.WriteString(line + "\n")
		for _, dep := range n.DependsOn {
			if dn, ok := byID[dep]; ok {
				b.WriteString(styleDim.Render(fmt.Sprintf("      ← %s (%s)\n", safeText(dep), safeText(m.detail.States[dep]))))
				_ = dn
			}
		}
	}
	b.WriteString("\n" + m.footer("↑/↓ node · a approve · r reject · c cancel · t retry · s steer · p allow perm · d deny perm · i inspect · esc back · q quit"))
	return b.String()
}

func (m *Model) nodeLine(n GraphNode, deps int) string {
	state := m.detail.States[n.ID]
	st := stateColor(state).Render(pad(safeText(state), 10))
	typ := styleDim.Render(safeText(n.Type))
	prio := styleDim.Render(fmt.Sprintf("p%d", n.Priority))
	atts := m.detail.Attempts[n.ID]
	attempts := styleDim.Render(fmt.Sprintf("%d att", len(atts)))
	depsS := ""
	if deps > 0 {
		depsS = styleDim.Render(fmt.Sprintf("(%d deps)", deps))
	}
	permS := ""
	if state == "blocked" {
		if pid, tool, _, ok := m.pendingPermissionDetails(n.ID); ok {
			permS = styleTitle.Render(fmt.Sprintf("perm:%s", safeText(pid)))
			if tool != "" {
				permS += styleMuted.Render(" " + safeText(tool))
			}
		}
	}
	bar := m.nodeBudgetBar(n, atts, state)
	return fmt.Sprintf("%-12s %s %-7s %s %s %s %s %s", safeText(n.ID), st, typ, prio, attempts, depsS, bar, permS)
}

// nodeBudgetBar renders a compact budget-usage bar for a node. The
// dominant budget dimension (provider runtime, tokens, or cost) drives the
// bar, with the used/limit figures beside it. Nodes without a budget show "".
func (m *Model) nodeBudgetBar(n GraphNode, atts []AttemptView, _ string) string {
	maxDur := n.Budget.MaxDuration
	maxTok := n.Budget.MaxTokens
	maxCost := n.Budget.MaxCost
	if maxDur <= 0 && maxTok <= 0 && maxCost <= 0 {
		return ""
	}
	// Used figures, from the most recent attempt for runtime and the sum for
	// tokens/cost.
	var usedDur time.Duration
	timeLabel := "wall elapsed"
	usedTok := 0
	usedCost := 0.0
	if len(atts) > 0 {
		at := atts[len(atts)-1]
		if runtime, exact := m.attemptRuntime(n.ID, at); exact {
			usedDur = runtime
			timeLabel = "runtime"
		} else if at.StartedAt != nil {
			start := time.UnixMilli(*at.StartedAt)
			end := m.now()
			if at.FinishedAt != nil {
				end = time.UnixMilli(*at.FinishedAt)
			}
			if end.After(start) {
				usedDur = end.Sub(start)
			}
		}
	}
	for _, at := range atts {
		usedTok += at.Tokens
		usedCost += at.Cost
	}
	// Choose the highest-utilization configured dimension. Completion is
	// lifecycle progress, not budget consumption, so done nodes retain
	// their actual utilization.
	type usage struct {
		fraction float64
		label    string
	}
	var dominant usage
	consider := func(candidate usage) {
		if dominant.label == "" || candidate.fraction > dominant.fraction {
			dominant = candidate
		}
	}
	if maxDur > 0 {
		consider(usage{
			fraction: float64(usedDur) / float64(time.Duration(maxDur)),
			label:    fmt.Sprintf("%s %s/%s", timeLabel, usedDur.Round(time.Second), time.Duration(maxDur).Round(time.Second)),
		})
	}
	if maxTok > 0 {
		consider(usage{
			fraction: float64(usedTok) / float64(maxTok),
			label:    fmt.Sprintf("tokens %d/%d", usedTok, maxTok),
		})
	}
	if maxCost > 0 {
		consider(usage{
			fraction: usedCost / maxCost,
			label:    fmt.Sprintf("cost $%.2f/$%.2f", usedCost, maxCost),
		})
	}
	return progressBar(dominant.fraction, 8) + " " + styleMuted.Render(dominant.label)
}

// attemptRuntime sums intervals in which the provider session was running.
// Durable transition events make permission-blocked intervals visible, so
// those waits do not consume the displayed runtime budget. Older/incomplete
// event histories return exact=false and are shown explicitly as wall elapsed.
func (m *Model) attemptRuntime(nodeID string, at AttemptView) (time.Duration, bool) {
	if m.detail == nil || at.StartedAt == nil {
		return 0, false
	}
	var startSeq int64
	for _, event := range m.detail.Events {
		if event.Type == "attempt" && event.AttemptID == at.ID {
			startSeq = event.Seq
			break
		}
	}
	if startSeq == 0 {
		return 0, false
	}

	start := *at.StartedAt
	end := m.now().UnixMilli()
	if at.FinishedAt != nil {
		end = *at.FinishedAt
	}
	if end < start {
		end = start
	}

	active := true
	activeSince := start
	var usedMillis int64
	for _, event := range m.detail.Events {
		if event.Seq <= startSeq || event.NodeID != nodeID || event.Type != "transition" || event.CreatedAt < start || event.CreatedAt > end {
			continue
		}
		if active && event.From == "running" && event.To != "running" {
			if event.CreatedAt > activeSince {
				usedMillis += event.CreatedAt - activeSince
			}
			active = false
			continue
		}
		if !active && event.To == "running" {
			active = true
			activeSince = event.CreatedAt
		}
	}
	if active && end > activeSince {
		usedMillis += end - activeSince
	}
	return time.Duration(usedMillis) * time.Millisecond, true
}

// now returns the model's wall-clock reference (last tick time, or real
// time before the first tick) for elapsed computations.
func (m *Model) now() time.Time {
	if m.lastFetch.IsZero() {
		return time.Now()
	}
	return m.lastFetch
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
	b.WriteString(styleTitle.Render(fmt.Sprintf("node %s  %s", safeText(n.ID), stateColor(state).Render(safeText(state)))) + "\n\n")
	b.WriteString(styleDim.Render("objective: ") + safeText(n.Objective) + "\n")
	if n.Role != "" {
		b.WriteString(styleDim.Render("role: ") + safeText(n.Role) + "\n")
	}
	if len(n.WriteScope) > 0 {
		scopes := make([]string, len(n.WriteScope))
		for i, scope := range n.WriteScope {
			scopes[i] = safeText(scope)
		}
		b.WriteString(styleDim.Render("write scope: ") + strings.Join(scopes, ", ") + "\n")
	}
	if n.Verification != nil {
		command := make([]string, len(n.Verification.Command))
		for i, arg := range n.Verification.Command {
			command[i] = safeText(arg)
		}
		b.WriteString(styleDim.Render("verification: ") + safeText(n.Verification.Kind) + " " + strings.Join(command, " ") + "\n")
	}
	if pid, tool, input, ok := m.pendingPermissionDetails(n.ID); ok {
		b.WriteString(styleDim.Render("permission: ") + styleTitle.Render(safeText(pid)))
		if tool != "" {
			b.WriteString(styleMuted.Render(" tool=" + safeText(tool)))
		}
		if input != "" {
			b.WriteString(styleMuted.Render(" input=" + shortLine(safeText(input), 100)))
		}
		b.WriteString(styleMuted.Render(" pending — p allow · d deny") + "\n")
	}
	b.WriteString("\n" + styleDim.Render("attempts") + "\n")
	for _, at := range m.detail.Attempts[n.ID] {
		b.WriteString(fmt.Sprintf("  #%d %-10s", at.No, stateColor(at.Status).Render(safeText(at.Status))))
		if at.SessionID != "" {
			b.WriteString(styleMuted.Render(" session=" + safeText(at.SessionID)))
		}
		if at.Worktree != "" {
			b.WriteString(styleMuted.Render(" worktree=" + shortPath(safeText(at.Worktree))))
		}
		if at.StartedAt != nil {
			b.WriteString(styleMuted.Render(" " + elapsed(*at.StartedAt, at.FinishedAt)))
		}
		b.WriteString("\n")
		if at.Cost > 0 || at.Tokens > 0 {
			b.WriteString(styleMuted.Render(fmt.Sprintf("     $%.4f  %d tok\n", at.Cost, at.Tokens)))
		}
		if at.Evidence != "" {
			b.WriteString(styleMuted.Render("     evidence: "+shortLine(safeText(at.Evidence), 90)) + "\n")
		}
	}
	// Live attempt tail for the current (running) attempt.
	if state == "running" || state == "verifying" {
		b.WriteString("\n" + styleDim.Render("live tail") + "\n")
		if len(m.tail) == 0 {
			b.WriteString(styleMuted.Render("  (no output yet)\n"))
		} else {
			for _, ln := range m.tail {
				b.WriteString(styleMuted.Render("  "+shortLine(safeText(ln), 90)) + "\n")
			}
		}
	}
	b.WriteString("\n" + m.footer("esc back · ↑/↓ navigate · a/r/c/t/s act · p/d respond perm"))
	return b.String()
}

func (m *Model) viewSteer() string {
	var b strings.Builder
	b.WriteString(styleTitle.Render(fmt.Sprintf("steer %s/%s", safeText(m.selectedID), safeText(m.steerNode))) + "\n\n")
	b.WriteString("message: " + safeText(m.steerInput) + "▌\n\n")
	b.WriteString(m.footer("type message · enter send · esc cancel"))
	return b.String()
}

func (m *Model) footer(keys string) string {
	var b strings.Builder
	b.WriteString(styleDim.Render("── " + keys))
	if m.status != "" {
		b.WriteString(styleOK.Render("  · " + safeText(m.status)))
	}
	if m.err != nil {
		b.WriteString(styleError.Render("  · " + safeText(m.err.Error())))
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
		parts = append(parts, fmt.Sprintf("%s:%s", safeText(id), stateColor(r.States[id]).Render(safeText(r.States[id]))))
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
	e := time.Now().UnixMilli()
	if end != nil {
		e = *end
	}
	d := time.Duration(e-start) * time.Millisecond
	if d < 0 {
		d = 0
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return d.Round(time.Second).String()
}
