// Package ocxreviewer implements the verify.Reviewer seam on top of
// OpenCode sessions (Task 4's reviewer gate). A reviewer session receives
// the completed attempt's evidence — objective, prior feedback, transcript,
// the recorded diff artifact and any check results — and must conclude with
// an explicit verdict: APPROVED or CHANGES_REQUESTED plus a note. The note
// becomes the gate feedback when the verdict is not approved, so the worker
// knows exactly what to fix. Sessions use the named reviewer agent with all
// tools denied and evaluate only the supplied evidence.
package ocxreviewer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"corral/internal/adapter"
	"corral/internal/ocx"
	"corral/internal/verify"
)

// Options tunes reviewer sessions. Zero values select sane defaults.
type Options struct {
	// Model overrides the default model for reviewer sessions
	// ("" = the OpenCode server default).
	Model string
	// Timeout bounds a single review session (default 5m).
	Timeout time.Duration
	// PollInterval is the transcript poll period while the session runs
	// (default 400ms).
	PollInterval time.Duration
}

func (o Options) timeout() time.Duration {
	if o.Timeout <= 0 {
		return 5 * time.Minute
	}
	return o.Timeout
}

func (o Options) poll() time.Duration {
	if o.PollInterval <= 0 {
		return 400 * time.Millisecond
	}
	return o.PollInterval
}

const reviewerAgent = "corral-reviewer"

// denyTools returns OpenCode's prompt-level wildcard deny. OpenCode converts
// this entry into a wildcard permission rule, covering built-in, plugin, MCP,
// and future tools without relying on the incomplete experimental ID list.
func denyTools() map[string]bool {
	return map[string]bool{"*": false}
}

// Driver implements verify.Reviewer for OpenCode sessions.
type Driver struct {
	oc   *ocx.Client
	opts Options
}

func New(oc *ocx.Client, opts Options) *Driver {
	return &Driver{oc: oc, opts: opts}
}

// Review runs a reviewer session for the attempt's evidence, waits for the
// session to reach idle, and parses the verdict from the transcript.
func (d *Driver) Review(ctx context.Context, req verify.ReviewRequest) (bool, string, error) {
	// Reviewer agent configuration belongs to the daemon's main OpenCode
	// project and is not guaranteed to exist in generated Git worktrees. The
	// reviewer is evidence-only with all tools denied, so it does not need a
	// session bound to the attempt worktree.
	client := d.oc
	reviewTools := denyTools()

	title := "corral/review/" + req.Attempt.NodeID
	sess, err := client.CreateSession(ctx, title)
	if err != nil {
		return false, "", fmt.Errorf("review session: %w", err)
	}
	prompt := promptFor(req)
	if err := client.PromptAsyncAgentWithTools(ctx, sess.ID, prompt, d.opts.Model, reviewerAgent, reviewTools); err != nil {
		abortReview(client, sess.ID)
		return false, "", fmt.Errorf("review prompt: %w", err)
	}

	deadline := time.Now().Add(d.opts.timeout())
	var lastPollErr error
	for {
		select {
		case <-ctx.Done():
			abortReview(client, sess.ID)
			return false, "", ctx.Err()
		default:
		}
		msgs, err := client.Messages(ctx, sess.ID, 0)
		if err != nil {
			lastPollErr = err
		} else if term, errName := terminal(msgs); term {
			if errName != "" {
				return false, "", fmt.Errorf("review session error: %s", errName)
			}
			return verdict(msgs)
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-time.After(d.opts.poll()):
		case <-ctx.Done():
			abortReview(client, sess.ID)
			return false, "", ctx.Err()
		}
	}

	// Timed out: kill the session so it stops generating, and fail fast.
	abortReview(client, sess.ID)
	if lastPollErr != nil {
		return false, "", fmt.Errorf("review timed out after %s (last poll: %v)", d.opts.timeout(), lastPollErr)
	}
	return false, "", fmt.Errorf("review timed out after %s (no terminal response)", d.opts.timeout())
}

func abortReview(client *ocx.Client, sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = client.Abort(ctx, sessionID)
}

// terminal reports whether the newest assistant message ended the session,
// returning the error name when it terminated with a session error.
func terminal(msgs []ocx.Message) (bool, string) {
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if m.Info.Role != "assistant" {
			continue
		}
		if m.Info.Error != nil {
			return true, errorName(m.Info.Error)
		}
		if m.Info.Finish != nil {
			switch *m.Info.Finish {
			case "", "tool-calls":
				return false, ""
			case "stop":
				return true, ""
			default:
				return true, "finish reason: " + *m.Info.Finish
			}
		}
		return false, ""
	}
	return false, ""
}

// verdict parses only the newest assistant response. Text parts are joined in
// their original order because OpenCode may split one continuous response
// across parts; older assistant messages can never supply a stale verdict.
func verdict(msgs []ocx.Message) (bool, string, error) {
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if m.Info.Role != "assistant" {
			continue
		}
		var text strings.Builder
		for _, part := range m.Parts {
			var p struct {
				Type string `json:"type"`
				Text string `json:"text"`
			}
			if json.Unmarshal(part, &p) != nil || p.Type != "text" {
				continue
			}
			text.WriteString(p.Text)
		}
		if approved, note, ok := parseVerdict(text.String()); ok {
			return approved, note, nil
		}
		return false, "", fmt.Errorf("reviewer produced no explicit verdict")
	}
	return false, "", fmt.Errorf("reviewer produced no explicit verdict")
}

// parseVerdict accepts exactly two lines: a case-sensitive verdict followed
// by a non-empty "Note: ..." line. Rejecting surrounding prose, legacy
// spellings, and extra lines prevents a verdict word inside commentary from
// being mistaken for the reviewer's decision.
func parseVerdict(text string) (approved bool, note string, ok bool) {
	text = strings.TrimSuffix(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	lines := strings.Split(text, "\n")
	if len(lines) != 2 {
		return false, "", false
	}
	switch lines[0] {
	case "APPROVED":
		approved = true
	case "CHANGES_REQUESTED":
		approved = false
	default:
		return false, "", false
	}
	const notePrefix = "Note: "
	if !strings.HasPrefix(lines[1], notePrefix) {
		return false, "", false
	}
	note = strings.TrimSpace(strings.TrimPrefix(lines[1], notePrefix))
	if note == "" {
		return false, "", false
	}
	note = truncate(note, 2000)
	return approved, note, true
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// promptFor builds the reviewer prompt from the attempt's evidence.
func promptFor(req verify.ReviewRequest) string {
	var b strings.Builder
	b.WriteString("You are a corral reviewer. Review the completed attempt's evidence below and return a verdict.\n")
	b.WriteString("\nOBJECTIVE:\n" + req.Attempt.Objective)
	if req.Attempt.Role != "" {
		b.WriteString("\n\nATTEMPT ROLE: " + req.Attempt.Role)
	}
	if req.Worktree != "" {
		b.WriteString("\n\nWORKTREE: " + req.Worktree)
	}
	if req.Feedback != "" {
		b.WriteString("\n\nPRIOR FEEDBACK (from a rejected attempt; confirm it is addressed):\n" + req.Feedback)
	}
	if d := diffArtifact(req.Messages); d != "" {
		b.WriteString("\n\nEVIDENCE — RECORDED DIFF ARTIFACT (file changes from the attempt):\n" + d)
	}
	if c := checkResults(req.Messages); c != "" {
		b.WriteString("\n\nEVIDENCE — CHECK RESULTS (command gates that already ran):\n" + c)
	}
	if t := transcript(req.Messages); t != "" {
		b.WriteString("\n\nEVIDENCE — ATTEMPT TRANSCRIPT:\n" + t)
	}
	b.WriteString(`
VERDICT:
Reply with EXACTLY these two lines and nothing else.

APPROVED
Note: <one-paragraph justification citing the evidence>

or

CHANGES_REQUESTED
Note: <what is missing or wrong; concrete, actionable feedback the worker can act on>
`)
	return b.String()
}

// diffArtifact renders the file changes the attempt recorded (the diff
// artifact evidence) into a compact text block.
func diffArtifact(msgs []adapter.Message) string {
	var b strings.Builder
	for _, m := range msgs {
		for _, d := range m.Diffs {
			fmt.Fprintf(&b, "--- %s (%s) +%d -%d\n", d.File, d.Status, d.Additions, d.Deletions)
			if d.Patch != "" {
				b.WriteString(d.Patch)
				b.WriteString("\n")
			}
		}
	}
	return strings.TrimSpace(b.String())
}

// checkResults renders command-gate results carried in message Meta
// (exit/stdout/stderr) as check-result evidence.
func checkResults(msgs []adapter.Message) string {
	var b strings.Builder
	for i, m := range msgs {
		if m.Meta["exit"] == "" {
			continue
		}
		fmt.Fprintf(&b, "# check %d: exit=%s\n", i, m.Meta["exit"])
		if out := m.Meta["stdout"]; out != "" {
			b.WriteString("stdout:\n" + tail(out, 2000) + "\n")
		}
		if errOut := m.Meta["stderr"]; errOut != "" {
			b.WriteString("stderr:\n" + tail(errOut, 2000) + "\n")
		}
	}
	return strings.TrimSpace(b.String())
}

// transcript renders the attempt's user/assistant messages.
func transcript(msgs []adapter.Message) string {
	var b strings.Builder
	for _, m := range msgs {
		text := strings.TrimSpace(m.Text)
		if text == "" {
			continue
		}
		fmt.Fprintf(&b, "%s: %s\n\n", m.Role, text)
	}
	return strings.TrimSpace(b.String())
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
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
