// Package verify implements the evidence gates that surround agent nodes:
// command checks, JSON-schema validation of produced artifacts, and
// reviewer sessions. Prose alone can never mark an attempt complete.
package verify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v5"

	"corral/internal/adapter"
	"corral/internal/graph"
)

// Result of one evidence gate. Feedback is the focused, worker-readable
// reason the gate failed (empty when Pass).
type Result struct {
	Pass     bool
	Feedback string
	Evidence string // JSON document: command output, schema errors, reviewer note
}

// CommandRunner executes check commands. Injectable for deterministic tests.
type CommandRunner interface {
	Run(ctx context.Context, dir string, command []string, timeout time.Duration) (exit int, stdout, stderr string, err error)
}

// ExecRunner runs commands with exec.CommandContext.
type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, dir string, command []string, timeout time.Duration) (int, string, string, error) {
	if len(command) == 0 {
		return 0, "", "", fmt.Errorf("empty command")
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil && cmd.ProcessState == nil {
		// The command never started (missing binary, bad path, permission
		// denied). Surface the real error instead of a phantom exit 0,
		// which would let an evidence gate pass on a check that never ran.
		return 0, stdout.String(), stderr.String(), err
	}
	exit := 0
	if cmd.ProcessState != nil {
		exit = cmd.ProcessState.ExitCode()
	}
	if err != nil && ctx.Err() == context.DeadlineExceeded {
		return exit, stdout.String(), stderr.String(), fmt.Errorf("command timed out after %s", timeout)
	}
	return exit, stdout.String(), stderr.String(), nil
}

// ReviewRequest is what a reviewer session receives.
type ReviewRequest struct {
	Attempt  adapter.Attempt
	Worktree string
	Feedback string // prior gate failures, if any
	Messages []adapter.Message
}

// Reviewer runs a review session that must conclude APPROVED.
type Reviewer interface {
	Review(ctx context.Context, req ReviewRequest) (approved bool, note string, err error)
}

// Engine dispatches verification by the node's declared method.
type Engine struct {
	Root     string // project root; worktree root when nodes share one
	Runner   CommandRunner
	Reviewer Reviewer
}

func New(root string) *Engine {
	return &Engine{Root: root, Runner: ExecRunner{}}
}

const DefaultCommandTimeout = 2 * time.Minute

// Verify runs the evidence gate declared on n.
func (e *Engine) Verify(ctx context.Context, n *graph.Node, worktree string, attemptNo int, msgs []adapter.Message) (Result, error) {
	if worktree == "" {
		worktree = e.Root
	}
	if n.Type == graph.NodeCheck {
		return e.verifyCheck(ctx, n, worktree, msgs)
	}
	v := n.Verification
	if v == nil {
		return verifyDefault(msgs)
	}
	switch v.Kind {
	case "command":
		return e.verifyCommand(ctx, n, worktree, v)
	case "json_schema":
		return e.verifyJSONSchema(n, worktree, v)
	case "reviewer":
		return e.verifyReviewer(ctx, n, worktree, attemptNo, msgs)
	default:
		return Result{}, fmt.Errorf("verify: unknown kind %q", v.Kind)
	}
}

// verifyCheck evaluates a check node: the evidence is its own command run.
// The command exit code and output are carried in message Meta.
func (e *Engine) verifyCheck(ctx context.Context, n *graph.Node, worktree string, msgs []adapter.Message) (Result, error) {
	v := n.Verification
	if v == nil || v.Kind != "command" {
		return Result{}, fmt.Errorf("verify: check node %s requires a command verification", n.ID)
	}
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if m.Role != "assistant" {
			continue
		}
		exit := m.Meta["exit"]
		if exit == "0" {
			return Result{Pass: true, Evidence: fmt.Sprintf(`{"command":%q,"exit":0}`, strings.Join(v.Command, " "))}, nil
		}
		feedback := m.Meta["stderr"]
		if feedback == "" {
			feedback = m.Meta["stdout"]
		}
		if feedback == "" {
			feedback = fmt.Sprintf("check exited with code %s", exit)
		}
		return Result{
			Pass:     false,
			Feedback: feedback,
			Evidence: fmt.Sprintf(`{"command":%q,"exit":%s,"stdout":%q,"stderr":%q}`,
				strings.Join(v.Command, " "), exit, m.Meta["stdout"], m.Meta["stderr"]),
		}, nil
	}
	return Result{}, fmt.Errorf("verify: check node %s has no execution result", n.ID)
}

func (e *Engine) verifyCommand(ctx context.Context, n *graph.Node, worktree string, v *graph.Verification) (Result, error) {
	timeout := DefaultCommandTimeout
	if n.Budget.MaxDuration > 0 {
		timeout = n.Budget.MaxDuration
	}
	exit, stdout, stderr, err := e.Runner.Run(ctx, worktree, v.Command, timeout)
	if err != nil && exit == 0 {
		return Result{}, fmt.Errorf("verify: command execution failed: %w", err)
	}
	evidence, _ := json.Marshal(map[string]any{
		"command": v.Command, "exit": exit, "stdout": tail(stdout, 4000), "stderr": tail(stderr, 4000),
	})
	if exit == 0 {
		return Result{Pass: true, Evidence: string(evidence)}, nil
	}
	feedback := tail(stderr, 2000)
	if strings.TrimSpace(feedback) == "" {
		feedback = tail(stdout, 2000)
	}
	if strings.TrimSpace(feedback) == "" {
		feedback = fmt.Sprintf("verification command exited with code %d", exit)
	}
	return Result{Pass: false, Feedback: feedback, Evidence: string(evidence)}, nil
}

func (e *Engine) verifyJSONSchema(n *graph.Node, worktree string, v *graph.Verification) (Result, error) {
	if v.Target == "" {
		return Result{}, fmt.Errorf("verify: json_schema gate requires a target file")
	}
	path := filepath.Join(worktree, v.Target)
	data, err := os.ReadFile(path)
	if err != nil {
		return Result{
			Pass:     false,
			Feedback: fmt.Sprintf("target artifact %s not found: %v", v.Target, err),
			Evidence: fmt.Sprintf(`{"target":%q}`, v.Target),
		}, nil
	}
	var doc any
	if err := json.Unmarshal(data, &doc); err != nil {
		return Result{
			Pass:     false,
			Feedback: fmt.Sprintf("target artifact %s is not valid JSON: %v", v.Target, err),
			Evidence: fmt.Sprintf(`{"target":%q}`, v.Target),
		}, nil
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("gate.json", strings.NewReader(v.Schema)); err != nil {
		return Result{}, fmt.Errorf("verify: invalid gate schema: %w", err)
	}
	schema, err := compiler.Compile("gate.json")
	if err != nil {
		return Result{}, fmt.Errorf("verify: invalid gate schema: %w", err)
	}
	if err := schema.Validate(doc); err != nil {
		feedback := firstValidationErrors(err, 5)
		return Result{Pass: false, Feedback: feedback, Evidence: feedback}, nil
	}
	return Result{Pass: true, Evidence: fmt.Sprintf(`{"target":%q,"valid":true}`, v.Target)}, nil
}

func (e *Engine) verifyReviewer(ctx context.Context, n *graph.Node, worktree string, attemptNo int, msgs []adapter.Message) (Result, error) {
	if e.Reviewer == nil {
		return Result{}, fmt.Errorf("verify: no reviewer configured for node %s", n.ID)
	}
	req := ReviewRequest{
		Attempt: adapter.Attempt{
			ID:        string(n.ID) + fmt.Sprintf("/%d", attemptNo),
			NodeID:    string(n.ID),
			Objective: n.Objective,
			Role:      n.Role,
			Cwd:       worktree,
		},
		Worktree: worktree,
		Messages: msgs,
	}
	approved, note, err := e.Reviewer.Review(ctx, req)
	if err != nil {
		return Result{}, fmt.Errorf("verify: reviewer failed: %w", err)
	}
	evidence, _ := json.Marshal(map[string]any{"approved": approved, "note": note})
	if approved {
		return Result{Pass: true, Evidence: string(evidence)}, nil
	}
	feedback := note
	if strings.TrimSpace(feedback) == "" {
		feedback = "reviewer rejected the work"
	}
	return Result{Pass: false, Feedback: feedback, Evidence: string(evidence)}, nil
}

// verifyDefault is the gate when a node declares no verification method:
// agent prose alone can never mark work complete, so at least one file
// change (diff) is required.
func verifyDefault(msgs []adapter.Message) (Result, error) {
	for _, m := range msgs {
		if m.Role == "user" && len(m.Diffs) > 0 {
			return Result{Pass: true, Evidence: `{"gate":"default","diffs":true}`}, nil
		}
	}
	return Result{
		Pass:     false,
		Feedback: "attempt produced no file changes; deliver the required output artifact",
		Evidence: `{"gate":"default","diffs":false}`,
	}, nil
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}

func firstValidationErrors(err error, max int) string {
	var out []string
	ve, ok := err.(*jsonschema.ValidationError)
	if !ok {
		return err.Error()
	}
	var walk func(e *jsonschema.ValidationError, depth int)
	walk = func(e *jsonschema.ValidationError, depth int) {
		if len(out) >= max || depth > 3 {
			return
		}
		if e.Message != "" {
			out = append(out, fmt.Sprintf("%v: %s", e.InstanceLocation, e.Message))
		}
		for _, c := range e.Causes {
			walk(c, depth+1)
		}
	}
	walk(ve, 0)
	if len(out) == 0 {
		out = append(out, ve.Error())
	}
	return strings.Join(out, "; ")
}
