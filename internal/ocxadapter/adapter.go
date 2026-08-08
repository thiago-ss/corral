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
	// Model overrides the default model for sessions ("" = server default).
	Model string
}

func (o Options) poll() time.Duration {
	if o.PollInterval <= 0 {
		return time.Second
	}
	return o.PollInterval
}

// Driver implements adapter.Driver and adapter.Stepper for OpenCode.
type Driver struct {
	oc   *ocx.Client
	opts Options

	mu          sync.Mutex
	attempts    map[string]*attempt    // attemptID -> rec
	bySession   map[string]*attempt    // sessionID -> rec
	clients     map[string]*ocx.Client // cwd -> client (worktrees)
	completions chan adapter.Completion

	streamOnce   sync.Once
	streamCtx    context.Context
	streamCancel context.CancelFunc
	closed       bool
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
		oc:          oc,
		opts:        opts,
		attempts:    map[string]*attempt{},
		bySession:   map[string]*attempt{},
		clients:     map[string]*ocx.Client{},
		completions: make(chan adapter.Completion, 64),
	}
}

func (d *Driver) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	d.closed = true
	d.mu.Unlock()
	if d.streamCancel != nil {
		d.streamCancel()
	}
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
	client := d.clientFor(a.Cwd)
	title := "corral/" + a.NodeID
	sess, err := client.CreateSession(ctx, title)
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}
	prompt := promptFor(a)
	if err := client.PromptAsync(ctx, sess.ID, prompt, d.opts.Model); err != nil {
		return nil, fmt.Errorf("prompt: %w", err)
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
	d.attempts[a.ID] = at
	d.bySession[sess.ID] = at
	d.mu.Unlock()

	d.startStream(atCtx)
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

// startStream opens the shared SSE stream exactly once and dispatches
// terminal events to the attempt that owns the session.
func (d *Driver) startStream(ctx context.Context) {
	d.streamOnce.Do(func() {
		sc, cancel := context.WithCancel(ctx)
		d.streamCtx = sc
		d.streamCancel = cancel
		go func() {
			err := d.oc.StreamEvents(sc, func(ev ocx.Event) {
				var p struct {
					SessionID string `json:"sessionID"`
				}
				if err := ev.UnmarshalProps(&p); err != nil || p.SessionID == "" {
					return
				}
				switch ev.Type {
				case "session.idle", "session.error", "permission.updated":
					d.mu.Lock()
					at := d.bySession[p.SessionID]
					d.mu.Unlock()
					if at == nil {
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

// watch completes the attempt when the session reaches a terminal state,
// using the event stream as primary signal and polling as fallback.
func (at *attempt) watch(ctx context.Context) {
	poll := time.NewTicker(at.d.opts.poll())
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			at.cleanup()
			return
		case <-poll.C:
			at.d.maybeComplete(ctx, at)
		case ev := <-at.events:
			at.handleEvent(ev)
		}
	}
}

func (at *attempt) handleEvent(ev ocx.Event) {
	if ev.Type == "permission.updated" {
		var p struct {
			ID   string `json:"id"`
			Type string `json:"type"`
		}
		if err := ev.UnmarshalProps(&p); err == nil && p.ID != "" {
			at.mu.Lock()
			at.permission = p.ID
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
	select {
	case d.completions <- c:
	default:
		log.Printf("ocxadapter: completion channel full; dropping %s", at.attemptID)
	}
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
			return true, adapter.StatusIdle
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
	s.at.aborted.Store(true)
	return s.oc.Abort(ctx, s.at.sessionID)
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
	if s.at.permission != "" {
		return s.at.permission, true, nil
	}
	return "", false, nil
}

func (s *session) RespondPermission(ctx context.Context, id string, allow bool) error {
	response := "deny"
	if allow {
		response = "allow"
	}
	s.at.mu.Lock()
	if s.at.permission == id {
		s.at.permission = "" // resolved; resume continues automatically
	}
	s.at.mu.Unlock()
	return s.oc.RespondPermission(ctx, s.at.sessionID, id, response)
}
