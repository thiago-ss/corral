// Package claudeadapter maps the generic adapter contract onto Claude Code
// CLI/SDK sessions. Each attempt spawns a headless `claude -p` process
// (stream-json output) in the attempt's working directory and streams its
// transcript in; a watcher emits exactly one Completion per attempt, using
// the process exit as the reconciliation fallback when the terminal result
// event is missed. Permission prompts are forwarded through a local unix
// socket broker to the driver's --permission-prompt-tool MCP helper.
//
// The package is self-contained: it does not know about the scheduler and
// is not wired into cmd/corral.
package claudeadapter

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"corral/internal/adapter"
)

// Options configures the Claude driver.
type Options struct {
	// PollInterval is the fallback status-poll period. Completions are
	// primarily driven by the process event stream; the poll re-checks
	// terminal state in case an event was dropped.
	PollInterval time.Duration
	// Model overrides the default model for sessions ("" = driver default).
	Model string
	// Command is the claude binary to spawn ("" = "claude" on PATH).
	Command string
	// PermissionSocket overrides the unix socket path used by the
	// permission broker ("" = auto in the temp dir).
	PermissionSocket string
	// DisablePermissions skips wiring the --permission-prompt-tool MCP
	// helper and its broker socket. Permission prompts are then still
	// tracked from the event stream but cannot be answered.
	DisablePermissions bool
}

func (o Options) poll() time.Duration {
	if o.PollInterval <= 0 {
		return time.Second
	}
	return o.PollInterval
}

func (o Options) command() string {
	if o.Command != "" {
		return o.Command
	}
	return "claude"
}

// Driver implements adapter.Driver and adapter.Stepper for Claude Code.
type Driver struct {
	opts Options

	mu          sync.Mutex
	attempts    map[string]*attempt // attemptID -> rec
	bySession   map[string]*attempt // sessionID -> rec
	completions chan adapter.Completion
	spawn       spawnFunc

	brokerOnce sync.Once
	broker     *permissionBroker
	brokerErr  error
	closed     bool
}

// attempt tracks one live claude session.
type attempt struct {
	d         *Driver
	proc      process
	attemptID string
	nodeID    string
	sessionID string // current id reported by Claude
	launchID  string // immutable --session-id used by the helper environment
	cwd       string

	aborted   atomic.Bool
	completed atomic.Bool
	stopped   bool // session closed by the driver; suppress completion
	terminal  bool // a terminal event (result/error/exit) was seen
	subtype   string
	errMsg    string
	exited    bool
	exitErr   error
	events    chan streamEvent
	exitedCh  chan error // scanner -> watcher: the process Wait result
	cancel    context.CancelFunc

	mu         sync.Mutex
	transcript []adapter.Message
	permission string // pending permission request id ("" = none); at most one request is admitted
}

func New(opts Options) *Driver {
	return &Driver{
		opts:        opts,
		attempts:    map[string]*attempt{},
		bySession:   map[string]*attempt{},
		completions: make(chan adapter.Completion, 64),
		spawn:       spawnCLI,
	}
}

// Close tears the driver down, killing any live sessions and the
// permission broker.
func (d *Driver) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	ats := make([]*attempt, 0, len(d.attempts))
	for _, at := range d.attempts {
		ats = append(ats, at)
	}
	d.mu.Unlock()
	for _, at := range ats {
		at.cancel()
	}
	if d.broker != nil {
		d.broker.close()
	}
}

// Start creates a headless claude session for the attempt, sends the
// objective, and starts a watcher that completes the attempt from the
// event stream with process-exit as fallback.
func (d *Driver) Start(ctx context.Context, a adapter.Attempt) (adapter.Session, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, fmt.Errorf("start claude: driver is closed")
	}
	if _, exists := d.attempts[a.ID]; exists {
		d.mu.Unlock()
		return nil, fmt.Errorf("start claude: attempt %q already started", a.ID)
	}
	if !d.opts.DisablePermissions {
		if err := d.startBroker(); err != nil {
			d.mu.Unlock()
			return nil, fmt.Errorf("start claude permission broker: %w", err)
		}
	}
	spec := d.specFor(a)
	proc, err := d.spawn(ctx, spec)
	if err != nil {
		d.mu.Unlock()
		return nil, fmt.Errorf("start claude: %w", err)
	}
	atCtx, cancel := context.WithCancel(context.Background())
	at := &attempt{
		d:         d,
		proc:      proc,
		attemptID: a.ID,
		nodeID:    a.NodeID,
		sessionID: spec.sessionID,
		launchID:  spec.sessionID,
		cwd:       a.Cwd,
		events:    make(chan streamEvent, 256),
		exitedCh:  make(chan error, 1),
		cancel:    cancel,
	}
	d.attempts[a.ID] = at
	d.bySession[spec.sessionID] = at
	d.mu.Unlock()

	go d.scan(at)
	go at.watch(atCtx)
	return &session{at: at}, nil
}

// specFor builds the claude invocation for an attempt.
func (d *Driver) specFor(a adapter.Attempt) spawnSpec {
	sid := newUUID()
	args := []string{
		"-p", promptFor(a),
		"--output-format", "stream-json",
		"--verbose",
		"--include-partial-messages",
		"--session-id", sid,
	}
	if model := d.modelFor(a); model != "" {
		args = append(args, "--model", model)
	}
	if tools := allowedTools(a); len(tools) > 0 {
		if !d.opts.DisablePermissions && d.broker != nil {
			tools = append(tools, permissionToolName)
		}
		args = append(args, "--allowedTools")
		args = append(args, tools...)
	}
	env := os.Environ()
	if !d.opts.DisablePermissions && d.broker != nil {
		cfg, _ := json.Marshal(mcpConfig{
			Servers: map[string]mcpServerConfig{
				permissionServerName: {
					Command: helperExecutable(),
					Args:    []string{helperFlag},
					Env: map[string]string{
						"CORRAL_CLAUDE_BROKER":     d.broker.path,
						"CORRAL_CLAUDE_ATTEMPT_ID": a.ID,
						"CORRAL_CLAUDE_SESSION_ID": sid,
					},
				},
			},
		})
		args = append(args, "--mcp-config", string(cfg))
		args = append(args, "--permission-prompt-tool", permissionToolName)
		env = append(env,
			"CORRAL_CLAUDE_BROKER="+d.broker.path,
			"CORRAL_CLAUDE_ATTEMPT_ID="+a.ID,
			"CORRAL_CLAUDE_SESSION_ID="+sid,
		)
	}
	return spawnSpec{
		command:   d.opts.command(),
		args:      args,
		env:       env,
		dir:       a.Cwd,
		sessionID: sid,
	}
}

func (d *Driver) modelFor(a adapter.Attempt) string {
	if a.Model != "" {
		return a.Model
	}
	return d.opts.Model
}

// Step drains completed attempts (non-blocking).
func (d *Driver) Step(_ context.Context, _ time.Time) []adapter.Completion {
	var out []adapter.Completion
	for {
		select {
		case c := <-d.completions:
			out = append(out, c)
		default:
			return out
		}
	}
}

// attemptBySession returns the attempt owning a session id, if any.
func (d *Driver) attemptBySession(sessionID string) *attempt {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.bySession[sessionID]
}

func (d *Driver) attemptByID(attemptID string) *attempt {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.attempts[attemptID]
}

// updateSessionID atomically remaps an attempt when Claude's init event
// reports a session id different from the requested --session-id.
func (d *Driver) updateSessionID(at *attempt, sessionID string) {
	if sessionID == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	at.mu.Lock()
	oldID := at.sessionID
	at.sessionID = sessionID
	at.mu.Unlock()
	if oldID != sessionID {
		delete(d.bySession, oldID)
	}
	d.bySession[sessionID] = at
}

func (at *attempt) cleanup() {
	at.d.mu.Lock()
	delete(at.d.attempts, at.attemptID)
	at.mu.Lock()
	sessionID := at.sessionID
	at.mu.Unlock()
	delete(at.d.bySession, sessionID)
	at.d.mu.Unlock()
}

// scan reads the claude stream-json output until EOF, dispatches each
// event to the attempt, then reports the process exit to the watcher. The
// exit is delivered through the watcher (not handled here) so the terminal
// decision always sees the full transcript in order.
func (d *Driver) scan(at *attempt) {
	sc := bufio.NewScanner(at.proc.stdout())
	for sc.Scan() {
		var ev streamEvent
		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			continue // startup noise / unknown line
		}
		select {
		case at.events <- ev:
		default: // dropped; the process exit covers it
		}
	}
	select {
	case at.exitedCh <- at.proc.wait():
	default: // watcher already gone (driver closed)
	}
}

// watch drives an attempt to a terminal state. The event stream is the
// primary signal; a poll ticker is the reconciliation fallback.
func (at *attempt) watch(ctx context.Context) {
	poll := time.NewTicker(at.d.opts.poll())
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			at.terminate()
			at.cleanup()
			return
		case <-poll.C:
			at.d.maybeComplete(context.Background(), at)
		case ev := <-at.events:
			at.handleEvent(ev)
		case err := <-at.exitedCh:
			at.drainEvents()
			at.onExit(err)
			return
		}
	}
}

// drainEvents processes every event queued ahead of the exit signal so the
// terminal decision reflects the full transcript.
func (at *attempt) drainEvents() {
	for {
		select {
		case ev := <-at.events:
			at.handleEvent(ev)
		default:
			return
		}
	}
}

func (at *attempt) handleEvent(ev streamEvent) {
	switch ev.Type {
	case "system":
		if ev.Subtype != "init" {
			return
		}
		if ev.SessionID != "" {
			at.d.updateSessionID(at, ev.SessionID)
		}
	case "assistant":
		at.appendMessage(ev.Message, "assistant")
	case "user":
		if rid := permissionRequest(ev.Message); rid != "" {
			at.mu.Lock()
			if at.permission == "" {
				at.permission = rid
			}
			at.mu.Unlock()
			return
		}
		at.appendMessage(ev.Message, "user")
	case "result":
		at.mu.Lock()
		at.terminal = true
		at.subtype = ev.Subtype
		at.mu.Unlock()
		at.d.maybeComplete(context.Background(), at)
	case "stream_error":
		at.mu.Lock()
		at.terminal = true
		if ev.Error != "" {
			at.errMsg = ev.Error
		}
		at.mu.Unlock()
		at.d.maybeComplete(context.Background(), at)
	}
}

// onExit marks the process exit as the terminal event. It is the
// reconciliation fallback when the result event was dropped.
func (at *attempt) onExit(err error) {
	at.mu.Lock()
	if at.stopped {
		at.mu.Unlock()
		return
	}
	at.terminal = true
	at.exited = true
	at.exitErr = err
	at.mu.Unlock()
	at.d.maybeComplete(context.Background(), at)
}

// terminate hard-kills a live process on driver shutdown.
func (at *attempt) terminate() {
	at.mu.Lock()
	at.stopped = true
	at.mu.Unlock()
	select {
	case <-at.proc.done():
		return
	default:
	}
	_ = at.proc.signal(syscall.SIGKILL)
	select {
	case <-at.proc.done():
	case <-time.After(2 * time.Second):
	}
}

// appendMessage accumulates one transcript entry from a stream message.
func (at *attempt) appendMessage(raw json.RawMessage, role string) {
	var m sdkMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return
	}
	am := adapter.Message{Role: role, Finish: m.Stop, Cost: m.Cost}
	if m.Usage != nil {
		am.Tokens = m.Usage.InputTokens + m.Usage.OutputTokens
	}
	for _, c := range m.Content {
		var b contentBlock
		if json.Unmarshal(c, &b) != nil {
			continue
		}
		switch b.Type {
		case "text":
			am.Text += b.Text
		case "tool_result":
			am.Text += toolResultText(b.Content)
		}
	}
	at.mu.Lock()
	at.transcript = append(at.transcript, am)
	at.mu.Unlock()
}

func (at *attempt) snapshot() []adapter.Message {
	at.mu.Lock()
	defer at.mu.Unlock()
	out := make([]adapter.Message, len(at.transcript))
	copy(out, at.transcript)
	return out
}

func (at *attempt) currentSessionID() string {
	at.mu.Lock()
	defer at.mu.Unlock()
	return at.sessionID
}

// terminalStatus decides whether the attempt reached a terminal state and
// what adapter.Status it maps to. Aborted attempts are terminal
// immediately; otherwise a result/error/exit event must have been seen.
func (at *attempt) terminalStatus() (adapter.Status, bool) {
	if at.aborted.Load() {
		return adapter.StatusAborted, true
	}
	at.mu.Lock()
	defer at.mu.Unlock()
	if !at.terminal {
		return "", false
	}
	if at.subtype == "success" {
		return adapter.StatusIdle, true
	}
	if at.errMsg != "" || at.subtype != "" {
		return adapter.StatusError, true
	}
	if at.exited {
		if at.exitErr == nil && len(at.transcript) > 0 {
			return adapter.StatusIdle, true
		}
		return adapter.StatusError, true
	}
	return adapter.StatusIdle, true
}

// maybeComplete emits a completion exactly once per attempt, guarded
// against duplicate or missing events.
func (d *Driver) maybeComplete(ctx context.Context, at *attempt) {
	status, ok := at.terminalStatus()
	if !ok {
		return
	}
	if !at.completed.CompareAndSwap(false, true) {
		return // duplicate event; already handled
	}
	c := adapter.Completion{
		AttemptID: at.attemptID,
		SessionID: at.currentSessionID(),
		Status:    status,
		Messages:  at.snapshot(),
	}
	select {
	case d.completions <- c:
	default:
		log.Printf("claudeadapter: completion channel full; dropping %s", at.attemptID)
	}
}

// session implements adapter.Session and adapter.PermissionSession for a
// live claude session.
type session struct {
	at *attempt
}

func (s *session) ID() string {
	s.at.mu.Lock()
	defer s.at.mu.Unlock()
	return s.at.sessionID
}
func (s *session) ServerID() string { return s.at.d.opts.command() }

// Send is unsupported: each claude attempt is one headless process running
// its prompt to completion, so there is no live process to steer.
func (s *session) Send(_ context.Context, text string) error {
	return fmt.Errorf("claudeadapter: cannot send %q to session %s: sessions run one prompt to completion", text, s.ID())
}

func (s *session) Abort(ctx context.Context) error {
	s.at.aborted.Store(true)
	select {
	case <-s.at.proc.done():
		return nil // already exited
	default:
	}
	// SIGTERM is Claude Code's graceful stop (it aborts the turn, runs
	// SessionEnd hooks and exits 143); SIGKILL is the hard fallback.
	if err := s.at.proc.signal(syscall.SIGTERM); err != nil {
		return err
	}
	select {
	case <-s.at.proc.done():
		return nil
	case <-ctx.Done():
		_ = s.at.proc.signal(syscall.SIGKILL)
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return s.at.proc.signal(syscall.SIGKILL)
	}
}

func (s *session) Status(_ context.Context) (adapter.Status, error) {
	if s.at.aborted.Load() {
		return adapter.StatusAborted, nil
	}
	if st, ok := s.at.terminalStatus(); ok {
		return st, nil
	}
	return adapter.StatusRunning, nil
}

func (s *session) Messages(_ context.Context) ([]adapter.Message, error) {
	return s.at.snapshot(), nil
}

func (s *session) Close(context.Context) error {
	s.at.cancel()
	return nil
}

func (s *session) PendingPermission(_ context.Context) (string, bool, error) {
	s.at.mu.Lock()
	defer s.at.mu.Unlock()
	if s.at.permission != "" {
		return s.at.permission, true, nil
	}
	return "", false, nil
}

func (s *session) RespondPermission(ctx context.Context, id string, allow bool) error {
	s.at.mu.Lock()
	if s.at.permission == "" {
		s.at.mu.Unlock()
		return fmt.Errorf("claudeadapter: no pending permission request")
	}
	if s.at.permission != id {
		pending := s.at.permission
		s.at.mu.Unlock()
		return fmt.Errorf("claudeadapter: permission %q is not pending (want %q)", id, pending)
	}
	s.at.mu.Unlock()
	if err := s.at.d.respondPermission(ctx, s.at, id, allow); err != nil {
		return err
	}
	s.at.mu.Lock()
	if s.at.permission == id {
		s.at.permission = "" // resolved; the scheduler resumes automatically
	}
	s.at.mu.Unlock()
	return nil
}

// respondPermission delivers a decision through the broker. When no helper
// is waiting for the id (e.g. a stream-only tracked prompt) the request is
// treated as resolved.
func (d *Driver) respondPermission(ctx context.Context, at *attempt, id string, allow bool) error {
	if d.broker == nil {
		return nil
	}
	return d.broker.respond(ctx, at, id, allow)
}

// promptFor builds the headless prompt for an attempt, mirroring ocx.
func promptFor(a adapter.Attempt) string {
	var b strings.Builder
	if a.Role != "" {
		b.WriteString("(role: " + a.Role + ")\n")
	}
	b.WriteString(a.Objective)
	if a.Feedback != "" {
		b.WriteString("\n\nPrevious attempt was rejected. Fix these issues:\n" + a.Feedback)
	}
	return b.String()
}

// allowedTools maps an attempt's write scope onto Claude Code permission
// rules: read-only tools are always approved and scoped Edit rules cover
// each writable path, so in-scope work runs without prompts.
func allowedTools(a adapter.Attempt) []string {
	tools := []string{"Read", "Glob", "Grep"}
	for _, p := range a.WriteScope {
		tools = append(tools, "Edit("+p+")", "Write("+p+")")
	}
	return tools
}

// newUUID returns a v4 UUID used as the claude --session-id.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Non-crypto fallback (still RFC4122-shaped).
		for i := range b {
			b[i] = byte(i * 7)
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ---------------------------------------------------------------------------
// Claude Code stream-json protocol types.

type streamEvent struct {
	Type      string          `json:"type"`
	Subtype   string          `json:"subtype"`
	SessionID string          `json:"session_id"`
	Message   json.RawMessage `json:"message"`
	Result    string          `json:"result"`
	Error     string          `json:"error"`
	Event     json.RawMessage `json:"event"`
}

type sdkMessage struct {
	Role    string            `json:"role"`
	Content []json.RawMessage `json:"content"`
	Usage   *sdkUsage         `json:"usage"`
	Cost    float64           `json:"cost"`
	Stop    string            `json:"stop_reason"`
}

type sdkUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type contentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	ToolUseID string          `json:"tool_use_id"`
	Content   json.RawMessage `json:"content"`
	IsError   bool            `json:"is_error"`
	State     string          `json:"state"`
	ToolName  string          `json:"tool_name"`
	RequestID string          `json:"request_id"`
	ID        string          `json:"id"`
}

// toolResultText flattens a tool_result content field, which may be a plain
// string or an array of text blocks.
func toolResultText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) != nil {
		return ""
	}
	var b strings.Builder
	for _, bl := range blocks {
		if bl.Type == "text" {
			b.WriteString(bl.Text)
		}
	}
	return b.String()
}

// permissionRequest returns a pending permission request id from a user
// message, or "" when the message carries none.
func permissionRequest(raw json.RawMessage) string {
	var m sdkMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}
	for _, c := range m.Content {
		var b contentBlock
		if json.Unmarshal(c, &b) != nil || b.Type != "permission_request" {
			continue
		}
		if b.State != "needs_response" && b.State != "" {
			continue
		}
		if b.RequestID != "" {
			return b.RequestID
		}
		if b.ID != "" {
			return b.ID
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Process abstraction. The default implementation spawns the claude binary;
// tests substitute an in-process fake speaking the same stream-json protocol.

type process interface {
	stdout() io.Reader
	done() <-chan struct{} // closed once the process has exited
	wait() error           // Wait result; valid once done() is closed
	signal(os.Signal) error
}

type spawnSpec struct {
	command   string
	args      []string
	env       []string
	dir       string
	sessionID string
}

type spawnFunc func(ctx context.Context, spec spawnSpec) (process, error)

type cliProcess struct {
	cmd     *exec.Cmd
	out     io.ReadCloser
	doneCh  chan struct{}
	waitMu  sync.Mutex
	waitErr error
}

func (p *cliProcess) stdout() io.Reader { return p.out }
func (p *cliProcess) done() <-chan struct{} {
	return p.doneCh
}
func (p *cliProcess) wait() error {
	<-p.doneCh
	p.waitMu.Lock()
	defer p.waitMu.Unlock()
	return p.waitErr
}
func (p *cliProcess) signal(sig os.Signal) error {
	return p.cmd.Process.Signal(sig)
}

func spawnCLI(ctx context.Context, spec spawnSpec) (process, error) {
	cmd := exec.CommandContext(ctx, spec.command, spec.args...)
	cmd.Dir = spec.dir
	cmd.Env = spec.env
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	errw, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	// The prompt comes from argv; never stream to stdin, and drain stderr
	// so a chatty CLI cannot deadlock the process.
	_ = in.Close()
	go io.Copy(io.Discard, errw)
	p := &cliProcess{cmd: cmd, out: out, doneCh: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		p.waitMu.Lock()
		p.waitErr = err
		p.waitMu.Unlock()
		close(p.doneCh)
	}()
	return p, nil
}
