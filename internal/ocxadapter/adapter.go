// Package ocxadapter maps the generic adapter contract onto OpenCode
// sessions (Task 3). Sessions are created and prompted asynchronously; a
// shared SSE event stream dispatches terminal events to per-attempt
// watchers, with status polling as the reconciliation fallback. Every
// attempt emits exactly one Completion, guarded against duplicate events.
package ocxadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"corral/internal/adapter"
	"corral/internal/ocx"
)

type Options struct {
	// PollInterval is the fallback status-poll period when events are
	// missed or the stream is down.
	PollInterval time.Duration
	// StreamReadyTimeout bounds how long the first Start waits for an SSE
	// subscription before proceeding with REST reconciliation (default 1s).
	StreamReadyTimeout time.Duration
	// Model is the driver default when Attempt.Model is empty
	// ("" leaves model selection to the server).
	Model string
}

func (o Options) poll() time.Duration {
	if o.PollInterval <= 0 {
		return time.Second
	}
	return o.PollInterval
}

func (o Options) streamReadyTimeout() time.Duration {
	if o.StreamReadyTimeout <= 0 {
		return time.Second
	}
	return o.StreamReadyTimeout
}

func (o Options) permissionPoll() time.Duration {
	poll := o.poll()
	if poll < 100*time.Millisecond {
		return 100 * time.Millisecond
	}
	if poll > time.Second {
		return time.Second
	}
	return poll
}

const permissionRequestTimeout = 500 * time.Millisecond

// Driver implements adapter.Driver and adapter.Stepper for OpenCode.
type Driver struct {
	oc   *ocx.Client
	opts Options

	mu          sync.Mutex
	attempts    map[string]*attempt    // attemptID -> rec
	bySession   map[string]*attempt    // sessionID -> rec
	seen        map[string]struct{}    // all successfully reserved attempt IDs
	clients     map[string]*ocx.Client // cwd -> client (worktrees)
	completions []adapter.Completion

	streamOnce     sync.Once
	streamReady    chan struct{}
	readyOnce      sync.Once
	streamCtx      context.Context
	streamCancel   context.CancelFunc
	permissionWake chan struct{}
	closed         bool
}

type attempt struct {
	d          *Driver
	oc         *ocx.Client // client bound to the attempt's directory
	attemptID  string
	nodeID     string
	sessionID  string
	aborted    atomic.Bool
	completed  bool
	events     chan ocx.Event // routed from the shared stream
	cancel     context.CancelFunc
	mu         sync.Mutex
	permission string // pending permission id ("" = none)
}

func New(oc *ocx.Client, opts Options) *Driver {
	return &Driver{
		oc:             oc,
		opts:           opts,
		attempts:       map[string]*attempt{},
		bySession:      map[string]*attempt{},
		seen:           map[string]struct{}{},
		clients:        map[string]*ocx.Client{},
		streamReady:    make(chan struct{}),
		permissionWake: make(chan struct{}, 1),
	}
}

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
	if d.streamCancel != nil {
		d.streamCancel()
	}
	d.readyOnce.Do(func() { close(d.streamReady) })
	for _, at := range ats {
		at.cancel()
	}
	var wg sync.WaitGroup
	for _, at := range ats {
		wg.Add(1)
		go func(at *attempt) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = at.oc.Abort(ctx, at.sessionID)
		}(at)
	}
	wg.Wait()
}

// clientFor returns the client bound to a directory (the attempt's
// worktree when isolated), creating it on first use.
func (d *Driver) clientFor(cwd string) *ocx.Client {
	if cwd == "" {
		return d.oc
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, ok := d.clients[cwd]; ok {
		return c
	}
	c := ocx.New(d.oc.Base(), cwd)
	d.clients[cwd] = c
	return c
}

// Start creates an OpenCode session, sends the objective, and starts a
// watcher that completes the attempt via events + polling fallback.
func (d *Driver) Start(ctx context.Context, a adapter.Attempt) (adapter.Session, error) {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, fmt.Errorf("start OpenCode: driver is closed")
	}
	if _, exists := d.seen[a.ID]; exists {
		d.mu.Unlock()
		return nil, fmt.Errorf("start OpenCode: attempt %q already started", a.ID)
	}
	d.seen[a.ID] = struct{}{}
	d.mu.Unlock()
	reserved := true
	defer func() {
		if !reserved {
			return
		}
		d.mu.Lock()
		delete(d.seen, a.ID)
		d.mu.Unlock()
	}()

	client := d.clientFor(a.Cwd)
	title := "corral/" + a.NodeID
	sess, err := client.CreateSession(ctx, title)
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}
	atCtx, atCancel := context.WithCancel(context.Background())
	at := &attempt{
		d:         d,
		oc:        client,
		attemptID: a.ID,
		nodeID:    a.NodeID,
		sessionID: sess.ID,
		events:    make(chan ocx.Event, 32),
		cancel:    atCancel,
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = client.Abort(cleanupCtx, sess.ID)
		cancel()
		return nil, fmt.Errorf("start OpenCode: driver is closed")
	}
	d.attempts[a.ID] = at
	d.bySession[sess.ID] = at
	d.startStream(context.Background())
	d.mu.Unlock()
	readyTimer := time.NewTimer(d.opts.streamReadyTimeout())
	select {
	case <-d.streamReady:
	case <-readyTimer.C:
		// REST transcript/status polling completes attempts, while the durable
		// permission poll recovers prompts missed before a later SSE reconnect.
		// Open the shared gate so an unavailable stream delays only the first
		// Start instead of serially delaying every attempt.
		d.readyOnce.Do(func() { close(d.streamReady) })
	case <-ctx.Done():
		if !readyTimer.Stop() {
			select {
			case <-readyTimer.C:
			default:
			}
		}
		atCancel()
		at.cleanup()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = client.Abort(cleanupCtx, sess.ID)
		cancel()
		return nil, fmt.Errorf("start OpenCode event stream: %w", ctx.Err())
	}
	if !readyTimer.Stop() {
		select {
		case <-readyTimer.C:
		default:
		}
	}
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		atCancel()
		at.cleanup()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = client.Abort(cleanupCtx, sess.ID)
		cancel()
		return nil, fmt.Errorf("start OpenCode: driver is closed")
	}

	prompt := promptFor(a)
	model := a.Model
	if model == "" {
		model = d.opts.Model
	}
	if err := client.PromptAsync(ctx, sess.ID, prompt, model); err != nil {
		atCancel()
		at.cleanup()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = client.Abort(cleanupCtx, sess.ID)
		cancel()
		return nil, fmt.Errorf("prompt: %w", err)
	}
	d.wakePermissionReconcile()
	d.mu.Lock()
	closed = d.closed
	d.mu.Unlock()
	if closed {
		atCancel()
		at.cleanup()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = client.Abort(cleanupCtx, sess.ID)
		cancel()
		return nil, fmt.Errorf("start OpenCode: driver closed during prompt")
	}
	reserved = false
	go at.watch(atCtx)
	return &session{oc: client, at: at}, nil
}

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

// Step drains completed attempts (non-blocking).
func (d *Driver) Step(_ context.Context, _ time.Time) []adapter.Completion {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := d.completions
	d.completions = nil
	return out
}

// startStream opens the shared SSE stream exactly once and dispatches
// terminal events to the attempt that owns the session.
func (d *Driver) startStream(ctx context.Context) {
	d.streamOnce.Do(func() {
		sc, cancel := context.WithCancel(ctx)
		d.streamCtx = sc
		d.streamCancel = cancel
		go d.reconcilePermissions(sc)
		go func() {
			err := d.oc.StreamEventsReady(sc, func() {
				d.readyOnce.Do(func() { close(d.streamReady) })
			}, func(ev ocx.Event) {
				var p struct {
					SessionID string `json:"sessionID"`
				}
				if err := ev.UnmarshalProps(&p); err != nil || p.SessionID == "" {
					return
				}
				switch ev.Type {
				case "session.idle", "session.error",
					"permission.asked", "permission.v2.asked",
					"permission.replied", "permission.v2.replied",
					"permission.updated": // pre-1.18 compatibility
					d.mu.Lock()
					at := d.bySession[p.SessionID]
					d.mu.Unlock()
					if at == nil {
						return
					}
					if strings.HasPrefix(ev.Type, "permission.") {
						// Record permission events inline. PendingPermission also polls the
						// durable endpoint, covering reconnect gaps and dropped events.
						at.handleEvent(ev)
						return
					}
					select {
					case at.events <- ev:
					default: // dropped; the poll fallback covers it
					}
				}
			})
			if err != nil && err != context.Canceled && sc.Err() == nil {
				log.Printf("ocxadapter: event stream ended: %v", err)
			}
		}()
	})
}

// wakePermissionReconcile asks the shared background poller to query pending
// permissions without putting provider I/O on the scheduler's hot path.
func (d *Driver) wakePermissionReconcile() {
	select {
	case d.permissionWake <- struct{}{}:
	default:
	}
}

// reconcilePermissions continuously polls OpenCode's durable permission list.
// It is shared by all attempts and clients, so PendingPermission remains a
// local, non-blocking scheduler query even while the SSE stream reconnects.
func (d *Driver) reconcilePermissions(ctx context.Context) {
	d.reconcilePermissionsOnce(ctx)
	ticker := time.NewTicker(d.opts.permissionPoll())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-d.permissionWake:
		}
		d.reconcilePermissionsOnce(ctx)
	}
}

func (d *Driver) reconcilePermissionsOnce(ctx context.Context) {
	d.mu.Lock()
	groups := make(map[*ocx.Client][]*attempt)
	for _, at := range d.attempts {
		groups[at.oc] = append(groups[at.oc], at)
	}
	d.mu.Unlock()

	var wg sync.WaitGroup
	for client, attempts := range groups {
		client, attempts := client, attempts
		wg.Add(1)
		go func() {
			defer wg.Done()
			pollCtx, cancel := context.WithTimeout(ctx, permissionRequestTimeout)
			defer cancel()
			requests, err := client.PendingPermissions(pollCtx)
			if err != nil {
				return // keep last-known state; retry on the next shared poll
			}
			pending := make(map[string]string)
			for _, request := range requests {
				if request.SessionID == "" || request.ID == "" {
					continue
				}
				if current := pending[request.SessionID]; current == "" || request.ID < current {
					pending[request.SessionID] = request.ID
				}
			}
			for _, at := range attempts {
				at.mu.Lock()
				at.permission = pending[at.sessionID]
				at.mu.Unlock()
			}
		}()
	}
	wg.Wait()
}

// watch completes the attempt when the session reaches a terminal state,
// using the event stream as primary signal and polling as fallback.
func (at *attempt) watch(ctx context.Context) {
	defer at.cleanup()
	poll := time.NewTicker(at.d.opts.poll())
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			at.d.maybeComplete(ctx, at)
		case ev := <-at.events:
			at.handleEvent(ev)
		}
	}
}

func (at *attempt) handleEvent(ev ocx.Event) {
	switch ev.Type {
	case "permission.asked", "permission.v2.asked", "permission.updated":
		var p struct {
			ID string `json:"id"`
		}
		if err := ev.UnmarshalProps(&p); err == nil && p.ID != "" {
			at.mu.Lock()
			at.permission = p.ID
			at.mu.Unlock()
			return
		}
	case "permission.replied", "permission.v2.replied":
		var p struct {
			RequestID string `json:"requestID"`
		}
		if err := ev.UnmarshalProps(&p); err == nil && p.RequestID != "" {
			at.mu.Lock()
			if at.permission == p.RequestID {
				at.permission = ""
			}
			at.mu.Unlock()
			return
		}
	}
	at.d.maybeComplete(context.Background(), at)
}

func (at *attempt) cleanup() {
	at.d.mu.Lock()
	delete(at.d.attempts, at.attemptID)
	delete(at.d.bySession, at.sessionID)
	at.d.mu.Unlock()
}

// maybeComplete fetches the session transcript and emits a completion if
// the attempt reached a terminal state. It is idempotent: exactly one
// Completion per attempt is ever emitted.
func (d *Driver) maybeComplete(ctx context.Context, at *attempt) {
	d.mu.Lock()
	if at.completed {
		d.mu.Unlock()
		return
	}
	d.mu.Unlock()

	msgs, err := at.oc.Messages(ctx, at.sessionID, 0)
	if err != nil {
		return // transient; retried on next poll/event
	}
	term, status := terminalStatus(msgs, at.aborted.Load())
	if !term {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if at.completed {
		return // duplicate event; already handled
	}
	at.completed = true
	c := adapter.Completion{
		AttemptID: at.attemptID,
		SessionID: at.sessionID,
		Status:    status,
		Messages:  toAdapterMessages(msgs),
	}
	if status == adapter.StatusError {
		c.Err = terminalError(msgs)
	}
	d.completions = append(d.completions, c)
	at.cancel()
}

func terminalError(msgs []ocx.Message) error {
	for i := len(msgs) - 1; i >= 0; i-- {
		message := msgs[i]
		if message.Info.Role != "assistant" {
			continue
		}
		if message.Info.Error != nil {
			if name := errorName(message.Info.Error); name != "" {
				return fmt.Errorf("OpenCode assistant error: %s", name)
			}
			return fmt.Errorf("OpenCode assistant error")
		}
		if message.Info.Finish != nil && *message.Info.Finish != "" {
			return fmt.Errorf("OpenCode finish reason: %s", *message.Info.Finish)
		}
		break
	}
	return fmt.Errorf("OpenCode attempt failed")
}

// terminalStatus decides whether the transcript shows a terminal attempt
// and what adapter.Status it maps to. Aborted attempts are terminal
// immediately; otherwise the last assistant message must be finished or
// errored.
func terminalStatus(msgs []ocx.Message, aborted bool) (bool, adapter.Status) {
	if aborted {
		return true, adapter.StatusAborted
	}
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if m.Info.Role != "assistant" {
			continue
		}
		if m.Info.Error != nil {
			name := errorName(m.Info.Error)
			if name == "MessageAbortedError" {
				return true, adapter.StatusAborted
			}
			return true, adapter.StatusError
		}
		if m.Info.Finish != nil {
			switch *m.Info.Finish {
			case "", "tool-calls":
				return false, "" // still streaming or entering a tool step
			case "stop":
				return true, adapter.StatusIdle
			default:
				return true, adapter.StatusError
			}
		}
		return false, "" // still streaming
	}
	return false, ""
}

func toAdapterMessages(msgs []ocx.Message) []adapter.Message {
	var out []adapter.Message
	for _, m := range msgs {
		am := adapter.Message{Role: m.Info.Role}
		if m.Info.Finish != nil {
			am.Finish = *m.Info.Finish
		}
		if m.Info.Error != nil {
			am.Error = errorName(m.Info.Error)
		}
		am.Cost = m.Info.Cost
		am.Tokens = m.Info.Tokens.Total
		for _, p := range m.Parts {
			var part struct {
				Type string `json:"type"`
				Text string `json:"text"`
			}
			if json.Unmarshal(p, &part) == nil && part.Type == "text" {
				am.Text += part.Text
			}
		}
		if m.Info.Summary != nil {
			for _, df := range m.Info.Summary.Diffs {
				am.Diffs = append(am.Diffs, adapter.Diff{
					File:      df.File,
					Patch:     df.Patch,
					Additions: df.Additions,
					Deletions: df.Deletions,
					Status:    df.Status,
				})
			}
		}
		out = append(out, am)
	}
	return out
}

func errorName(raw *json.RawMessage) string {
	if raw == nil {
		return ""
	}
	var e struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(*raw, &e); err != nil {
		return ""
	}
	return e.Name
}

// session implements adapter.Session for a live OpenCode session.
type session struct {
	oc *ocx.Client
	at *attempt
}

func (s *session) ID() string       { return s.at.sessionID }
func (s *session) ServerID() string { return s.oc.Base() }
func (s *session) Close(context.Context) error {
	s.at.cancel()
	return nil
}
func (s *session) Send(ctx context.Context, text string) error {
	return s.oc.PromptAsync(ctx, s.at.sessionID, text, "")
}
func (s *session) Abort(ctx context.Context) error {
	if err := s.oc.Abort(ctx, s.at.sessionID); err != nil {
		return err
	}
	s.at.aborted.Store(true)
	return nil
}
func (s *session) Status(ctx context.Context) (adapter.Status, error) {
	statuses, err := s.oc.SessionStatus(ctx)
	if err != nil {
		return "", err
	}
	if st, ok := statuses[s.at.sessionID]; ok && st.Type == "busy" {
		return adapter.StatusRunning, nil
	}
	if s.at.aborted.Load() {
		return adapter.StatusAborted, nil
	}
	return adapter.StatusIdle, nil
}
func (s *session) Messages(ctx context.Context) ([]adapter.Message, error) {
	msgs, err := s.oc.Messages(ctx, s.at.sessionID, 0)
	if err != nil {
		return nil, err
	}
	return toAdapterMessages(msgs), nil
}

func (s *session) PendingPermission(_ context.Context) (string, bool, error) {
	s.at.mu.Lock()
	defer s.at.mu.Unlock()
	return s.at.permission, s.at.permission != "", nil
}

func (s *session) RespondPermission(ctx context.Context, id string, allow bool) error {
	pending, ok, err := s.PendingPermission(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("ocxadapter: no pending permission request")
	}
	if pending != id {
		return fmt.Errorf("ocxadapter: permission %q is not pending (want %q)", id, pending)
	}
	reply := "reject"
	if allow {
		reply = "once"
	}
	if err := s.oc.RespondPermission(ctx, id, reply); err != nil {
		return err
	}
	s.at.mu.Lock()
	if s.at.permission == id {
		s.at.permission = "" // resolved; resume continues automatically
	}
	s.at.mu.Unlock()
	return nil
}
