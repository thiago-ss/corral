// Package adapter defines the generic executor contract that corral's
// scheduler uses to run nodes. OpenCode is the production-wired
// implementation; the standalone Claude Code adapter satisfies the same
// contract. Nothing in this package depends on either provider.
package adapter

import (
	"context"
	"time"
)

// Attempt describes one execution of a node. AttemptID is unique per
// attempt (node + run counter); it must be stable across controller
// restarts so reconciliation can attribute sessions.
type Attempt struct {
	ID                 string
	NodeID             string
	Objective          string
	Role               string // "worker" | "reviewer" | "merger" | "planner"
	Model              string // optional provider override; "" = driver default
	Cwd                string // worktree or checkout the session runs in
	WriteScope         []string
	InputFiles         []string
	Feedback           string // focused feedback from the previous attempt's failed verification
	MaxDurationSeconds int
}

// Status is the liveness state of a session.
type Status string

const (
	StatusPending Status = "pending" // accepted, not yet executing
	StatusRunning Status = "running" // attempt in flight
	StatusIdle    Status = "idle"    // finished responding; evidence pending
	StatusAborted Status = "aborted" // canceled by operator or budget
	StatusError   Status = "error"   // session terminated with an error
)

// Diff is one file change produced by an attempt.
type Diff struct {
	File      string
	Patch     string
	Additions int
	Deletions int
	Status    string
}

// Message is one transcript entry of a session.
type Message struct {
	Role   string // "user" | "assistant"
	Text   string
	Finish string // "" while running, "stop" on normal end
	Error  string // error name, "" when none
	Cost   float64
	Tokens int
	Diffs  []Diff
	// Meta carries driver-specific evidence (e.g. "exit", "stdout",
	// "stderr" for check-node command runs).
	Meta map[string]string
}

// Session is a handle to one live or historical worker session.
type Session interface {
	ID() string
	// ServerID identifies the provider server instance (e.g. the OpenCode
	// server base URL) the session lives on.
	ServerID() string
	Send(context.Context, string) error
	Abort(context.Context) error
	Status(context.Context) (Status, error)
	Messages(context.Context) ([]Message, error)
	Close(context.Context) error
}

// Driver starts sessions. Implementations must guarantee that duplicate
// or missing completion events cannot produce duplicate completions:
// drivers are idempotent per Attempt ID, and the scheduler relies on
// Status()/Messages() reconciliation after restarts.
type Driver interface {
	Start(ctx context.Context, a Attempt) (Session, error)
}

// Completion is emitted exactly once per attempt by drivers.
type Completion struct {
	AttemptID string
	SessionID string
	Status    Status
	Messages  []Message
	Err       error
	Budget    bool // aborted because the node budget expired
}

// Stepper is implemented by drivers that produce completions on demand
// (cooperative fake agents) or drain asynchronously-arriving completions
// (async drivers such as OpenCode). The scheduler calls Step once per
// tick; implementations must return without blocking.
type Stepper interface {
	Step(ctx context.Context, now time.Time) []Completion
}

// PermissionSession is implemented by drivers whose sessions can pause on
// permission requests. When a permission is pending the scheduler moves
// the node to blocked (explicit state) and resumes it automatically once
// the permission is resolved.
type PermissionSession interface {
	// PendingPermission returns the pending permission id, if any.
	PendingPermission(ctx context.Context) (id string, ok bool, err error)
	// RespondPermission approves (allow=true) or denies a pending
	// permission request.
	RespondPermission(ctx context.Context, id string, allow bool) error
}

// Event is a live stream item from a session, used for progress display
// and for triggering verification once the attempt reaches idle.
type Event struct {
	SessionID string
	Type      string // "status" | "message" | "diff" | "error"
	Status    Status
	Message   *Message
	Diff      *Diff
	Error     string
}
