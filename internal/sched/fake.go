package sched

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"corral/internal/adapter"
	"corral/internal/clock"
	"corral/internal/graph"
)

// Script describes one fake attempt for a node. Each call to Start
// consumes the next script; when scripts are exhausted a default script
// (zero delay, no messages) is used.
type Script struct {
	Delay      time.Duration
	Messages   []adapter.Message
	Write      map[string]string // file -> content, written into a.Cwd at completion
	SessionID  string            // optional fixed session id
	Err        error             // terminal execution error
	Permission string            // pending permission id while running; resolved via RespondPermission
}

// FakeDriver simulates agents deterministically: completions are produced
// by Step, driven by the fake clock, never by goroutines.
type FakeDriver struct {
	clk      clock.Clock
	mu       sync.Mutex
	sessions map[string]*fakeSession
	scripts  map[string][]Script
	seq      int
	// Feedback records the feedback string each attempt received.
	Feedback map[string][]string
}

type fakeSession struct {
	driver     *FakeDriver
	attemptID  string
	nodeID     string
	id         string
	script     Script
	start      time.Time
	aborted    bool
	cwd        string
	permission string // pending permission id ("" = none)
}

func NewFakeDriver(clk clock.Clock, scripts map[string][]Script) *FakeDriver {
	return &FakeDriver{clk: clk, sessions: map[string]*fakeSession{}, scripts: scripts, Feedback: map[string][]string{}}
}

func (f *FakeDriver) Start(_ context.Context, a adapter.Attempt) (adapter.Session, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	if a.Feedback != "" {
		f.Feedback[a.NodeID] = append(f.Feedback[a.NodeID], a.Feedback)
	}
	var script Script
	if ss := f.scripts[a.NodeID]; len(ss) > 0 {
		script = ss[0]
		f.scripts[a.NodeID] = ss[1:]
	}
	id := script.SessionID
	if id == "" {
		id = fmt.Sprintf("ses-fake-%s-%d", a.NodeID, f.seq)
	}
	s := &fakeSession{
		driver:     f,
		attemptID:  a.ID,
		nodeID:     a.NodeID,
		id:         id,
		script:     script,
		start:      f.clk.Now(),
		cwd:        a.Cwd,
		permission: script.Permission,
	}
	f.sessions[a.ID] = s
	return s, nil
}

func (f *FakeDriver) Step(_ context.Context, now time.Time) []adapter.Completion {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []string
	for id := range f.sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var out []adapter.Completion
	for _, id := range ids {
		s := f.sessions[id]
		if !s.aborted && now.Sub(s.start) < s.script.Delay {
			continue
		}
		delete(f.sessions, id)
		if s.script.Write != nil {
			for file, content := range s.script.Write {
				if s.cwd != "" {
					_ = os.MkdirAll(filepath.Dir(filepath.Join(s.cwd, file)), 0o755)
					_ = os.WriteFile(filepath.Join(s.cwd, file), []byte(content), 0o644)
				}
			}
		}
		if s.permission != "" && !s.aborted {
			continue // still awaiting permission; not complete
		}
		c := adapter.Completion{
			AttemptID: s.attemptID,
			SessionID: s.id,
		}
		switch {
		case s.aborted:
			c.Status = adapter.StatusAborted
		case s.script.Err != nil:
			c.Status = adapter.StatusError
			c.Err = s.script.Err
		default:
			c.Status = adapter.StatusIdle
			c.Messages = s.script.Messages
		}
		out = append(out, c)
	}
	return out
}

// SetScript assigns the script used by the next attempt of a node
// (replaces the remaining scripts).
func (f *FakeDriver) SetScript(nodeID string, script Script) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.scripts == nil {
		f.scripts = map[string][]Script{}
	}
	f.scripts[nodeID] = []Script{script}
}

// AppendScript queues a script at the end of a node's script list.
func (f *FakeDriver) AppendScript(nodeID string, script Script) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.scripts == nil {
		f.scripts = map[string][]Script{}
	}
	f.scripts[nodeID] = append(f.scripts[nodeID], script)
}

// Cancel aborts an in-flight fake session (used by tests for cancel paths).
func (f *FakeDriver) Cancel(attemptID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.sessions[attemptID]; ok {
		s.aborted = true
	}
}

func (s *fakeSession) ID() string { return s.id }

func (s *fakeSession) ServerID() string { return "fake" }

func (s *fakeSession) Send(context.Context, string) error { return nil }

func (s *fakeSession) Abort(context.Context) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	s.aborted = true
	return nil
}

func (s *fakeSession) Status(context.Context) (adapter.Status, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	if s.aborted {
		return adapter.StatusAborted, nil
	}
	return adapter.StatusRunning, nil
}

func (s *fakeSession) Messages(context.Context) ([]adapter.Message, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	if s.aborted {
		return nil, nil
	}
	return s.script.Messages, nil
}

func (s *fakeSession) Close(context.Context) error { return nil }

func (s *fakeSession) PendingPermission(_ context.Context) (string, bool, error) {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	if s.permission != "" {
		return s.permission, true, nil
	}
	return "", false, nil
}

func (s *fakeSession) RespondPermission(_ context.Context, id string, allow bool) error {
	s.driver.mu.Lock()
	defer s.driver.mu.Unlock()
	if s.permission == id {
		s.permission = ""
	}
	return nil
}

// FakeVerifier returns scripted verdicts per node, one per attempt; the
// last verdict repeats for any extra attempts.
type FakeVerifier struct {
	mu       sync.Mutex
	verdicts map[string][]Verdict
	def      Verdict
}

func NewFakeVerifier(verdicts map[string][]Verdict, def Verdict) *FakeVerifier {
	return &FakeVerifier{verdicts: verdicts, def: def}
}

func (v *FakeVerifier) Verdict(_ context.Context, n *graph.Node, attemptNo int, _ string, _ []adapter.Message) (Verdict, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	seq := v.verdicts[string(n.ID)]
	idx := attemptNo - 1
	if idx < 0 {
		idx = 0
	}
	if idx < len(seq) {
		return seq[idx], nil
	}
	return v.def, nil
}
