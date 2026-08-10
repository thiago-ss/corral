// Package store persists corral runs in SQLite. The event log is the
// durable record of everything that happened; nodes and attempts tables are
// materialized views that can be rebuilt from the log (event replay).
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	_ "modernc.org/sqlite"

	"corral/internal/graph"
)

type EventType string

const (
	EventTransition EventType = "transition" // state change: from -> to
	EventRecovery   EventType = "recovery"   // forced restore after restart (applied via Set)
	EventAttempt    EventType = "attempt"    // start/finish/abort/interrupt of an attempt
	EventVerdict    EventType = "verdict"    // verification outcome
	EventRetry      EventType = "retry"      // retry scheduled
	EventRun        EventType = "run"        // run lifecycle (created/completed)
	EventGraph      EventType = "graph"      // graph version change
)

type Event struct {
	Seq       int64           `json:"seq"`
	RunID     string          `json:"runID"`
	NodeID    string          `json:"nodeID,omitempty"`
	Type      EventType       `json:"type"`
	From      graph.State     `json:"from,omitempty"`
	To        graph.State     `json:"to,omitempty"`
	AttemptID string          `json:"attemptID,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	CreatedAt int64           `json:"createdAt"` // ms epoch
}

type Attempt struct {
	ID         string  `json:"id"`
	RunID      string  `json:"runID"`
	NodeID     string  `json:"nodeID"`
	No         int     `json:"no"`
	Status     string  `json:"status"` // leased|running|verifying|done|failed|aborted|interrupted
	ServerID   string  `json:"serverID,omitempty"`
	SessionID  string  `json:"sessionID,omitempty"`
	Worktree   string  `json:"worktree,omitempty"`
	Branch     string  `json:"branch,omitempty"`
	StartedAt  *int64  `json:"startedAt,omitempty"`
	FinishedAt *int64  `json:"finishedAt,omitempty"`
	Evidence   string  `json:"evidence,omitempty"`
	Cost       float64 `json:"cost"`
	Tokens     int     `json:"tokens"`
}

type Run struct {
	ID               string       `json:"id"`
	Graph            *graph.Graph `json:"graph"`
	Status           string       `json:"status"` // active|completed|canceled
	AutoApproveGates bool         `json:"autoApproveGates"`
	CreatedAt        int64        `json:"createdAt"`
}

// NodeRow is the materialized per-node state.
type NodeRow struct {
	NodeID      string
	State       graph.State
	Attempts    int
	RetriesLeft int
	LeasedBy    string
	LeasedUntil int64
	UpdatedAt   int64
}

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	dsn := path
	if path == "" {
		dsn = "file:corral?mode=memory&cache=shared"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if path != "" {
		// WAL gives readers a consistent snapshot during concurrent writes.
		if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
			db.Close()
			return nil, fmt.Errorf("wal: %w", err)
		}
	}
	for _, pragma := range []string{
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
		"PRAGMA synchronous=NORMAL",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func migrate(db *sql.DB) error {
	schema := `
CREATE TABLE IF NOT EXISTS runs (
	id        TEXT PRIMARY KEY,
	graph     TEXT NOT NULL,
	status    TEXT NOT NULL,
	auto_approve_gates INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	run_id TEXT NOT NULL REFERENCES runs(id),
	seq INTEGER NOT NULL,
	node_id TEXT NOT NULL DEFAULT '',
	etype TEXT NOT NULL,
	from_state TEXT NOT NULL DEFAULT '',
	to_state TEXT NOT NULL DEFAULT '',
	attempt_id TEXT NOT NULL DEFAULT '',
	payload TEXT NOT NULL DEFAULT '',
	created_at INTEGER NOT NULL,
	UNIQUE(run_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, seq);
CREATE TABLE IF NOT EXISTS nodes (
	run_id TEXT NOT NULL REFERENCES runs(id),
	node_id TEXT NOT NULL,
	state TEXT NOT NULL,
	attempts INTEGER NOT NULL DEFAULT 0,
	retries_left INTEGER NOT NULL DEFAULT 0,
	leased_by TEXT NOT NULL DEFAULT '',
	leased_until INTEGER NOT NULL DEFAULT 0,
	updated_at INTEGER NOT NULL,
	PRIMARY KEY(run_id, node_id)
);
CREATE TABLE IF NOT EXISTS attempts (
	attempt_id TEXT PRIMARY KEY,
	run_id TEXT NOT NULL REFERENCES runs(id),
	node_id TEXT NOT NULL,
	no INTEGER NOT NULL,
	status TEXT NOT NULL,
	server_id TEXT NOT NULL DEFAULT '',
	session_id TEXT NOT NULL DEFAULT '',
	worktree TEXT NOT NULL DEFAULT '',
	branch TEXT NOT NULL DEFAULT '',
	started_at INTEGER,
	finished_at INTEGER,
	evidence TEXT NOT NULL DEFAULT '',
	cost REAL NOT NULL DEFAULT 0,
	tokens INTEGER NOT NULL DEFAULT 0,
	UNIQUE(run_id, node_id, no)
);
CREATE TABLE IF NOT EXISTS artifacts (
	run_id TEXT NOT NULL REFERENCES runs(id),
	attempt_id TEXT NOT NULL,
	node_id TEXT NOT NULL,
	name TEXT NOT NULL,
	hash TEXT NOT NULL,
	path TEXT NOT NULL DEFAULT '',
	content TEXT NOT NULL DEFAULT '',
	PRIMARY KEY(run_id, attempt_id, name)
);`
	if _, err := db.Exec(schema); err != nil {
		return err
	}
	// Migrate pre-existing databases: the auto-approve column is added
	// when it is missing (CREATE TABLE IF NOT EXISTS does not alter an
	// existing table).
	ok, err := columnExists(db, "runs", "auto_approve_gates")
	if err != nil {
		return err
	}
	if !ok {
		if _, err := db.Exec("ALTER TABLE runs ADD COLUMN auto_approve_gates INTEGER NOT NULL DEFAULT 0"); err != nil {
			return err
		}
	}
	return nil
}

// columnExists reports whether table has column.
func columnExists(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (s *Store) CreateRun(ctx context.Context, runID string, g *graph.Graph, now time.Time) error {
	return s.createRun(ctx, runID, g, false, now)
}

// CreateRunWithOpts creates a run with scheduler options (auto-approving
// human gates) persisted on the run row.
func (s *Store) CreateRunWithOpts(ctx context.Context, runID string, g *graph.Graph, autoApproveGates bool, now time.Time) error {
	return s.createRun(ctx, runID, g, autoApproveGates, now)
}

func (s *Store) createRun(ctx context.Context, runID string, g *graph.Graph, autoApproveGates bool, now time.Time) error {
	gj, err := json.Marshal(g)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	autoApprove := 0
	if autoApproveGates {
		autoApprove = 1
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO runs(id, graph, status, auto_approve_gates, created_at) VALUES(?, ?, 'active', ?, ?)`,
		runID, string(gj), autoApprove, now.UnixMilli()); err != nil {
		return err
	}
	for _, n := range g.Nodes {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO nodes(run_id, node_id, state, retries_left, updated_at) VALUES(?, ?, 'pending', ?, ?)`,
			runID, n.ID, n.RetryPolicy.MaxRetries, now.UnixMilli()); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO events(run_id, seq, etype, payload, created_at) VALUES(?, 1, 'run', '{"status":"created"}', ?)`,
		runID, now.UnixMilli()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Run(ctx context.Context, runID string) (*Run, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, graph, status, auto_approve_gates, created_at FROM runs WHERE id = ?`, runID)
	var r Run
	var gj string
	var autoApprove int
	if err := row.Scan(&r.ID, &gj, &r.Status, &autoApprove, &r.CreatedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(gj), &r.Graph); err != nil {
		return nil, fmt.Errorf("decode graph: %w", err)
	}
	r.AutoApproveGates = autoApprove != 0
	return &r, nil
}

// ListRuns returns all runs with their status.
func (s *Store) ListRuns(ctx context.Context) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, graph, status, auto_approve_gates, created_at FROM runs ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		var r Run
		var gj string
		var autoApprove int
		if err := rows.Scan(&r.ID, &gj, &r.Status, &autoApprove, &r.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(gj), &r.Graph); err != nil {
			return nil, fmt.Errorf("decode graph: %w", err)
		}
		r.AutoApproveGates = autoApprove != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) CompleteRun(ctx context.Context, runID string, status string, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status = ? WHERE id = ?`, status, runID); err != nil {
		return err
	}
	if _, err := appendEventTx(ctx, tx, runID, 0, "", EventRun, "", "", "", "", `{"status":"`+status+`"}`, now); err != nil {
		return err
	}
	return tx.Commit()
}

// AppendTransition records a state change and updates the materialized
// nodes table atomically.
func (s *Store) AppendTransition(ctx context.Context, runID string, nodeID string, from, to graph.State, payload string, now time.Time) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	seq, err := appendEventTx(ctx, tx, runID, 0, nodeID, EventTransition, string(from), string(to), "", "", payload, now)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE nodes SET state = ?, updated_at = ? WHERE run_id = ? AND node_id = ?`,
		string(to), now.UnixMilli(), runID, nodeID); err != nil {
		return 0, err
	}
	return seq, tx.Commit()
}

func (s *Store) AppendEvent(ctx context.Context, runID, nodeID string, typ EventType, from, to graph.State, attemptID, payload string, now time.Time) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	seq, err := appendEventTx(ctx, tx, runID, 0, nodeID, typ, string(from), string(to), attemptID, "", payload, now)
	if err != nil {
		return 0, err
	}
	return seq, tx.Commit()
}

// appendEventTx computes the next per-run sequence and inserts the event.
func appendEventTx(ctx context.Context, tx *sql.Tx, runID string, forceSeq int64, nodeID string, typ EventType, from, to, attemptID, _, payload string, now time.Time) (int64, error) {
	var seq int64
	if forceSeq > 0 {
		seq = forceSeq
	} else {
		if err := tx.QueryRowContext(ctx,
			`SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE run_id = ?`, runID).Scan(&seq); err != nil {
			return 0, err
		}
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO events(run_id, seq, node_id, etype, from_state, to_state, attempt_id, payload, created_at)
		 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		runID, seq, nodeID, string(typ), from, to, attemptID, Redact(payload), now.UnixMilli()); err != nil {
		return 0, err
	}
	return seq, nil
}

// Events returns the full event log of a run in sequence order.
func (s *Store) Events(ctx context.Context, runID string) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq, node_id, etype, from_state, to_state, attempt_id, payload, created_at
		 FROM events WHERE run_id = ? ORDER BY seq`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		var nodeID, typ, from, to, attemptID, payload string
		if err := rows.Scan(&e.Seq, &nodeID, &typ, &from, &to, &attemptID, &payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.RunID = runID
		e.NodeID = nodeID
		e.Type = EventType(typ)
		e.From = graph.State(from)
		e.To = graph.State(to)
		e.AttemptID = attemptID
		if payload != "" {
			e.Payload = json.RawMessage(payload)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// NodeStates reads the materialized states.
func (s *Store) NodeStates(ctx context.Context, runID string) (map[graph.NodeID]graph.State, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT node_id, state FROM nodes WHERE run_id = ?`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[graph.NodeID]graph.State{}
	for rows.Next() {
		var id, st string
		if err := rows.Scan(&id, &st); err != nil {
			return nil, err
		}
		out[graph.NodeID(id)] = graph.State(st)
	}
	return out, rows.Err()
}

func (s *Store) NodeRow(ctx context.Context, runID, nodeID string) (*NodeRow, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT node_id, state, attempts, retries_left, leased_by, leased_until, updated_at
		 FROM nodes WHERE run_id = ? AND node_id = ?`, runID, nodeID)
	var r NodeRow
	var st string
	if err := row.Scan(&r.NodeID, &st, &r.Attempts, &r.RetriesLeft, &r.LeasedBy, &r.LeasedUntil, &r.UpdatedAt); err != nil {
		return nil, err
	}
	r.State = graph.State(st)
	return &r, nil
}

// AcquireLease atomically leases a node that has no live lease (or whose
// lease expired). Returns false when the node is currently leased.
func (s *Store) AcquireLease(ctx context.Context, runID, nodeID, holder string, ttl time.Duration, now time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE nodes SET leased_by = ?, leased_until = ?, updated_at = ?
		 WHERE run_id = ? AND node_id = ? AND (leased_by = '' OR leased_until < ?)`,
		holder, now.Add(ttl).UnixMilli(), now.UnixMilli(), runID, nodeID, now.UnixMilli())
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// ReleaseLease releases a lease held by holder (only if still held).
func (s *Store) ReleaseLease(ctx context.Context, runID, nodeID, holder string, now time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE nodes SET leased_by = '', leased_until = 0, updated_at = ?
		 WHERE run_id = ? AND node_id = ? AND leased_by = ?`,
		now.UnixMilli(), runID, nodeID, holder)
	return err
}

// ClearLease releases a lease regardless of holder (crash recovery).
func (s *Store) ClearLease(ctx context.Context, runID, nodeID string, now time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE nodes SET leased_by = '', leased_until = 0, updated_at = ?
		 WHERE run_id = ? AND node_id = ?`,
		now.UnixMilli(), runID, nodeID)
	return err
}

// RecordAttempt inserts or updates an attempt row.
func (s *Store) RecordAttempt(ctx context.Context, a Attempt) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO attempts(attempt_id, run_id, node_id, no, status, server_id, session_id, worktree, branch, started_at, finished_at, evidence, cost, tokens)
		 VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(attempt_id) DO UPDATE SET
		   status = excluded.status, session_id = excluded.session_id,
		   worktree = excluded.worktree, branch = excluded.branch,
		   finished_at = excluded.finished_at, evidence = excluded.evidence,
		   cost = excluded.cost, tokens = excluded.tokens`,
		a.ID, a.RunID, a.NodeID, a.No, a.Status, a.ServerID, a.SessionID, a.Worktree, a.Branch, a.StartedAt, a.FinishedAt, Redact(a.Evidence), a.Cost, a.Tokens)
	return err
}

// Attempts lists attempts for a node in order.
func (s *Store) Attempts(ctx context.Context, runID, nodeID string) ([]Attempt, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT attempt_id, run_id, node_id, no, status, server_id, session_id, worktree, branch, started_at, finished_at, evidence, cost, tokens
		 FROM attempts WHERE run_id = ? AND node_id = ? ORDER BY no`, runID, nodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.ID, &a.RunID, &a.NodeID, &a.No, &a.Status, &a.ServerID, &a.SessionID, &a.Worktree, &a.Branch, &a.StartedAt, &a.FinishedAt, &a.Evidence, &a.Cost, &a.Tokens); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// MarkInterrupted flips non-terminal attempts of a node to interrupted.
func (s *Store) MarkInterrupted(ctx context.Context, runID, nodeID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE attempts SET status = 'interrupted'
		 WHERE run_id = ? AND node_id = ? AND status IN ('leased','running','verifying')`,
		runID, nodeID)
	return err
}

// CountAttempts returns the number of attempts for a node.
func (s *Store) CountAttempts(ctx context.Context, runID, nodeID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM attempts WHERE run_id = ? AND node_id = ?`, runID, nodeID).Scan(&n)
	return n, err
}

// NodeCost sums cost and tokens across all attempts of a node.
func (s *Store) NodeCost(ctx context.Context, runID, nodeID string) (float64, int, error) {
	var cost float64
	var tokens int
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(cost), 0), COALESCE(SUM(tokens), 0) FROM attempts WHERE run_id = ? AND node_id = ?`,
		runID, nodeID).Scan(&cost, &tokens)
	return cost, tokens, err
}

// SetNodeState updates the materialized node state without appending an
// event (used only for forced recovery restores).
func (s *Store) SetNodeState(ctx context.Context, runID, nodeID string, st graph.State, now time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE nodes SET state = ?, updated_at = ? WHERE run_id = ? AND node_id = ?`,
		string(st), now.UnixMilli(), runID, nodeID)
	return err
}

// Artifact is a content-addressed output produced by an attempt.
type Artifact struct {
	RunID     string `json:"runID"`
	AttemptID string `json:"attemptID"`
	NodeID    string `json:"nodeID"`
	Name      string `json:"name"`
	Hash      string `json:"hash"`
	Path      string `json:"path"`
	Content   string `json:"content"`
}

// RecordArtifact stores (or replaces) a named artifact for an attempt.
func (s *Store) RecordArtifact(ctx context.Context, a Artifact) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO artifacts(run_id, attempt_id, node_id, name, hash, path, content)
		 VALUES(?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(run_id, attempt_id, name) DO UPDATE SET
		   hash = excluded.hash, path = excluded.path, content = excluded.content`,
		a.RunID, a.AttemptID, a.NodeID, a.Name, a.Hash, a.Path, Redact(a.Content))
	return err
}

// Artifacts lists artifacts for an attempt.
func (s *Store) Artifacts(ctx context.Context, runID, attemptID string) ([]Artifact, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT run_id, attempt_id, node_id, name, hash, path, content
		 FROM artifacts WHERE run_id = ? AND attempt_id = ?`, runID, attemptID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Artifact
	for rows.Next() {
		var a Artifact
		if err := rows.Scan(&a.RunID, &a.AttemptID, &a.NodeID, &a.Name, &a.Hash, &a.Path, &a.Content); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// Redact strips common secret patterns before content is persisted to the
// event log, evidence columns and artifacts, so secrets never leak into
// durable records, logs or audit exports.
func Redact(s string) string {
	if s == "" {
		return s
	}
	repl := []struct{ re, to string }{
		{`(?i)bearer\s+[A-Za-z0-9._~+/=-]+`, "bearer [REDACTED]"},
		{`(?i)api[_-]?key["']?\s*[:=]\s*["']?[A-Za-z0-9._~+/=-]{6,}`, "apiKey [REDACTED]"},
		{`(?i)(password|passwd|secret|token|authorization)["']?\s*[:=]\s*["']?[A-Za-z0-9._~+/=-]{6,}`, "$1 [REDACTED]"},
		{`sk-[A-Za-z0-9]{16,}`, "sk-[REDACTED]"},
	}
	out := s
	for _, r := range repl {
		re := regexp.MustCompile(r.re)
		out = re.ReplaceAllString(out, r.to)
	}
	return out
}

// SetRetriesLeft updates the retry budget.
func (s *Store) SetRetriesLeft(ctx context.Context, runID, nodeID string, n int, now time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE nodes SET retries_left = ?, updated_at = ? WHERE run_id = ? AND node_id = ?`,
		n, now.UnixMilli(), runID, nodeID)
	return err
}
