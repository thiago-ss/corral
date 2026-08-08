package spike

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"corral/internal/ocx"
)

const (
	TitleFast   = "w1-fast"
	TitleSlow   = "w2-slow"
	TitleMed    = "w3-medium"
	PromptFast  = "Create a file named alpha.txt containing exactly one line: CORRAL-A1. Do not run any other commands."
	PromptSlow  = "Append one line to beta.txt every second, 30 lines total, numbered 1 to 30, using bash. Keep going until the loop finishes. Do not stop early."
	PromptMed   = "Create two files named gamma-1.txt and gamma-2.txt, each containing exactly one line: CORRAL-G. Then run: ls *.txt"
	PromptAfter = "Append one line to alpha.txt containing exactly: CORRAL-B2. Do not run any other commands."
)

type SessionResult struct {
	mu         sync.Mutex
	Title      string
	SessionID  string
	BusySeen   bool
	Aborted    bool
	AbortError string
	Finished   bool
	Finish     string
	Files      []string
	Cost       float64
	Tokens     int
	FirstBusy  time.Time
	IdleAt     time.Time
}

func (r *SessionResult) busySeen() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.BusySeen }
func (r *SessionResult) idleZero() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.IdleAt.IsZero() }
func (r *SessionResult) setBusy(t time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.BusySeen {
		r.BusySeen = true
		r.FirstBusy = t
	}
}
func (r *SessionResult) setIdle(t time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.IdleAt.IsZero() {
		r.IdleAt = t
	}
}
func (r *SessionResult) setAborted() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Aborted = true
	r.AbortError = "MessageAbortedError"
}

type Result struct {
	Sessions       []*SessionResult
	PeakConcurrent int
	DiffCaptured   map[string][]string
	Reconcile      *ReconcileResult
	FollowUp       *FollowUpResult
}

type ReconcileResult struct {
	Found         int
	PerSession    map[string]ReconcileSession
	DuplicateDone bool
}

type ReconcileSession struct {
	SessionID       string
	State           string
	CompletedRuns   int
	AbortedRuns     int
	LatestFinish    string
	LatestErrorName string
}

type FollowUpResult struct {
	SessionID string
	Finished  bool
	Files     []string
}

type Options struct {
	Model string
	Log   func(format string, args ...any)
}

func (o *Options) logf(format string, args ...any) {
	if o.Log != nil {
		o.Log(format, args...)
	}
}

func Run(ctx context.Context, c *ocx.Client, projDir string, opts Options) (*Result, error) {
	res := &Result{
		Sessions:     []*SessionResult{},
		DiffCaptured: map[string][]string{},
	}

	// Phase 1: create three sessions concurrently.
	opts.logf("phase 1: creating 3 sessions")
	created := make([]*ocx.Session, 3)
	titles := []string{TitleFast, TitleSlow, TitleMed}
	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := range titles {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := c.CreateSession(ctx, titles[i])
			created[i], errs[i] = &s, err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("create session %d: %w", i, err)
		}
	}

	// Phase 2: fire all three prompts asynchronously, concurrently.
	opts.logf("phase 2: firing async prompts")
	prompts := []string{PromptFast, PromptSlow, PromptMed}
	for i := range created {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = c.PromptAsync(ctx, created[i].ID, prompts[i], "")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("prompt %d: %w", i, err)
		}
	}

	byID := map[string]*SessionResult{}
	for i := range created {
		r := &SessionResult{Title: titles[i], SessionID: created[i].ID}
		res.Sessions = append(res.Sessions, r)
		byID[r.SessionID] = r
	}

	// Phase 3: stream events + poll status until w1 and w3 are idle or deadline.
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	var betaLoopAt time.Time
	var betaMu sync.Mutex
	setBetaLoop := func() {
		betaMu.Lock()
		defer betaMu.Unlock()
		if betaLoopAt.IsZero() {
			betaLoopAt = time.Now()
		}
	}
	betaLoopSince := func(d time.Duration) bool {
		betaMu.Lock()
		defer betaMu.Unlock()
		return !betaLoopAt.IsZero() && time.Since(betaLoopAt) >= d
	}
	go c.StreamEvents(streamCtx, func(ev ocx.Event) {
		switch ev.Type {
		case "session.status":
			var p struct {
				SessionID string `json:"sessionID"`
				Status    struct {
					Type string `json:"type"`
				} `json:"status"`
			}
			if err := ev.UnmarshalProps(&p); err != nil {
				return
			}
			if r := byID[p.SessionID]; r != nil && p.Status.Type == "busy" {
				r.setBusy(time.Now())
			}
		case "session.idle":
			var p struct {
				SessionID string `json:"sessionID"`
			}
			if err := ev.UnmarshalProps(&p); err != nil {
				return
			}
			if r := byID[p.SessionID]; r != nil {
				r.setIdle(time.Now())
			}
		case "session.error":
			var p struct {
				SessionID string `json:"sessionID"`
				Error     struct {
					Name string `json:"name"`
				} `json:"error"`
			}
			if err := ev.UnmarshalProps(&p); err != nil {
				return
			}
			if r := byID[p.SessionID]; r != nil && p.Error.Name == "MessageAbortedError" {
				r.setAborted()
			}
		case "message.part.updated":
			var p struct {
				SessionID string `json:"sessionID"`
				Part      struct {
					Type  string `json:"type"`
					Tool  string `json:"tool"`
					State struct {
						Status string `json:"status"`
						Input  struct {
							Command string `json:"command"`
						} `json:"input"`
					} `json:"state"`
				} `json:"part"`
			}
			if err := ev.UnmarshalProps(&p); err != nil {
				return
			}
			if p.Part.Type == "tool" && p.Part.Tool == "bash" && p.Part.State.Status == "running" &&
				strings.Contains(p.Part.State.Input.Command, "beta.txt") {
				setBetaLoop()
			}
		}
	})

	// Status polling loop: track peak concurrency, abort w2 mid-run, then settle.
	deadline := time.Now().Add(420 * time.Second)
	aborted := false
	settled := func() bool {
		for _, r := range res.Sessions {
			if r.idleZero() {
				return false
			}
		}
		return true
	}
	opts.logf("phase 3: waiting for busy states (abort 3s into beta.txt loop, settle by deadline)")
	for time.Now().Before(deadline) {
		statuses, err := c.SessionStatus(ctx)
		if err != nil {
			return nil, fmt.Errorf("status poll: %w", err)
		}
		busy := 0
		for sid, st := range statuses {
			if r := byID[sid]; r != nil {
				if st.Type == "busy" {
					r.setBusy(time.Now())
					busy++
				}
				if st.Type == "idle" {
					r.setIdle(time.Now())
				}
			}
		}
		if busy > res.PeakConcurrent {
			res.PeakConcurrent = busy
		}
		if !aborted && betaLoopSince(3*time.Second) {
			opts.logf("phase 4: aborting %s 3s into beta.txt loop", TitleSlow)
			if err := c.Abort(ctx, byTitle(res.Sessions, TitleSlow).SessionID); err != nil {
				return nil, fmt.Errorf("abort: %w", err)
			}
			aborted = true
		}
		if aborted && settled() {
			break
		}
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	cancelStream()
	if !aborted {
		return nil, fmt.Errorf("scenario timeout before abort fired")
	}
	for _, r := range res.Sessions {
		if r.IdleAt.IsZero() {
			return nil, fmt.Errorf("scenario timeout: %s never reached idle", r.Title)
		}
	}

	// Phase 5: capture messages, diffs, costs.
	opts.logf("phase 5: capturing messages and diffs")
	for _, r := range res.Sessions {
		msgs, err := c.Messages(ctx, r.SessionID, 0)
		if err != nil {
			return nil, fmt.Errorf("messages %s: %w", r.Title, err)
		}
		for _, m := range msgs {
			if m.Info.Role == "assistant" {
				r.Cost += m.Info.Cost
				r.Tokens += m.Info.Tokens.Total
				if m.Info.Finish != nil {
					r.Finished = true
					r.Finish = *m.Info.Finish
				}
				if m.Info.Error != nil && r.AbortError == "" {
					r.AbortError = errorName(m.Info.Error)
				}
			}
			if m.Info.Role == "user" && m.Info.Summary != nil {
				for _, d := range m.Info.Summary.Diffs {
					res.DiffCaptured[r.Title] = append(res.DiffCaptured[r.Title], d.File)
				}
			}
		}
		r.Files = append(r.Files, res.DiffCaptured[r.Title]...)
		sort.Strings(r.Files)
		sort.Strings(res.DiffCaptured[r.Title])
	}

	// Phase 6: restart simulation - fresh client reconciles from REST only.
	opts.logf("phase 6: fresh client reconciles (restart simulation)")
	rc, err := Reconcile(ctx, c, projDir)
	if err != nil {
		return nil, err
	}
	res.Reconcile = rc

	// Phase 7: fresh client continues control - follow-up prompt.
	opts.logf("phase 7: follow-up control on fresh client")
	fu, err := FollowUp(ctx, c, projDir, byTitle(res.Sessions, TitleFast).SessionID)
	if err != nil {
		return nil, err
	}
	res.FollowUp = fu

	// Phase 8: cleanup.
	opts.logf("phase 8: cleanup (delete sessions)")
	for _, s := range res.Sessions {
		if err := c.DeleteSession(ctx, s.SessionID); err != nil {
			opts.logf("  delete %s: %v", s.Title, err)
		}
	}
	return res, nil
}

func byTitle(ss []*SessionResult, title string) *SessionResult {
	for _, s := range ss {
		if s.Title == title {
			return s
		}
	}
	return nil
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

func Reconcile(ctx context.Context, c *ocx.Client, projDir string) (*ReconcileResult, error) {
	sessions, err := c.ListSessions(ctx)
	if err != nil {
		return nil, err
	}
	statuses, err := c.SessionStatus(ctx)
	if err != nil {
		return nil, err
	}
	res := &ReconcileResult{
		PerSession: map[string]ReconcileSession{},
	}
	for _, s := range sessions {
		rs := ReconcileSession{SessionID: s.ID}
		msgs, err := c.Messages(ctx, s.ID, 0)
		if err != nil {
			return nil, fmt.Errorf("reconcile messages %s: %w", s.ID, err)
		}
		for _, m := range msgs {
			if m.Info.Role != "assistant" {
				continue
			}
			if m.Info.Error != nil {
				rs.AbortedRuns++
				rs.LatestErrorName = errorName(m.Info.Error)
			} else if m.Info.Finish != nil && *m.Info.Finish == "stop" {
				rs.CompletedRuns++
				rs.LatestFinish = *m.Info.Finish
			} else {
				rs.LatestFinish = ""
			}
		}
		if st, ok := statuses[s.ID]; ok {
			switch st.Type {
			case "busy", "retry":
				rs.State = "running"
			default:
				rs.State = "idle"
			}
		} else {
			rs.State = "idle"
		}
		res.PerSession[s.ID] = rs
		res.Found++
		if rs.CompletedRuns > 1 {
			res.DuplicateDone = true
		}
	}
	return res, nil
}

func FollowUp(ctx context.Context, c *ocx.Client, projDir, sid string) (*FollowUpResult, error) {
	if err := c.PromptAsync(ctx, sid, PromptAfter, ""); err != nil {
		return nil, err
	}
	// Wait for the run to start (busy entry appears), then for it to end
	// (entry disappears: /session/status omits idle sessions).
	started := false
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		statuses, err := c.SessionStatus(ctx)
		if err != nil {
			return nil, err
		}
		st, ok := statuses[sid]
		if !started && ok {
			started = true
		}
		if started && (!ok || st.Type == "idle") {
			break
		}
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	// Fallback: if we never observed the session as busy, give it a grace
	// window, then confirm completion from the transcript.
	if !started {
		time.Sleep(5 * time.Second)
	}
	msgs, err := c.Messages(ctx, sid, 5)
	if err != nil {
		return nil, err
	}
	res := &FollowUpResult{SessionID: sid}
	for _, m := range msgs {
		if m.Info.Role == "assistant" && m.Info.Finish != nil {
			res.Finished = true
		}
		if m.Info.Role == "user" && m.Info.Summary != nil {
			for _, d := range m.Info.Summary.Diffs {
				res.Files = append(res.Files, d.File)
			}
		}
	}
	// verify append actually happened
	data, err := os.ReadFile(filepath.Join(projDir, "alpha.txt"))
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	var hasB2 bool
	for _, l := range lines {
		if strings.TrimSpace(l) == "CORRAL-B2" {
			hasB2 = true
		}
	}
	if !hasB2 {
		return nil, fmt.Errorf("follow-up append missing; alpha.txt=%q", string(data))
	}
	var hasA1 bool
	for _, l := range lines {
		if strings.TrimSpace(l) == "CORRAL-A1" {
			hasA1 = true
		}
	}
	if !hasA1 || !hasB2 {
		return nil, fmt.Errorf("alpha.txt content wrong: %q", string(data))
	}
	return res, nil
}
