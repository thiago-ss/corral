// Package tui renders the corral companion terminal UI: a live dashboard
// over the daemon HTTP API with runs, DAG status, attempts, evidence and
// node actions.
package tui

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// DTOs mirror the daemon API payloads.

type RunSummary struct {
	ID     string            `json:"id"`
	Status string            `json:"status"`
	States map[string]string `json:"states"`
	Done   bool              `json:"done"`
}

type GraphNode struct {
	ID           string   `json:"id"`
	Type         string   `json:"type"`
	Objective    string   `json:"objective"`
	Role         string   `json:"role,omitempty"`
	Priority     int      `json:"priority"`
	DependsOn    []string `json:"dependsOn,omitempty"`
	WriteScope   []string `json:"writeScope,omitempty"`
	Verification *struct {
		Kind    string   `json:"kind"`
		Command []string `json:"command,omitempty"`
	} `json:"verification,omitempty"`
	RetryPolicy struct {
		MaxRetries int `json:"maxRetries"`
	} `json:"retryPolicy"`
	Budget struct {
		MaxDuration int64   `json:"maxDuration"` // nanoseconds (time.Duration)
		MaxTokens   int     `json:"maxTokens,omitempty"`
		MaxCost     float64 `json:"maxCost,omitempty"`
	} `json:"budget"`
}

type AttemptView struct {
	ID         string  `json:"id"`
	No         int     `json:"no"`
	Status     string  `json:"status"`
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

type EventView struct {
	Seq       int64           `json:"seq"`
	NodeID    string          `json:"nodeID,omitempty"`
	Type      string          `json:"type"`
	From      string          `json:"from,omitempty"`
	To        string          `json:"to,omitempty"`
	AttemptID string          `json:"attemptID,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	CreatedAt int64           `json:"createdAt"`
}

type RunDetail struct {
	RunID    string                      `json:"runID"`
	Status   string                      `json:"status"`
	Graph    struct{ Nodes []GraphNode } `json:"graph"`
	States   map[string]string           `json:"states"`
	Done     bool                        `json:"done"`
	Attempts map[string][]AttemptView    `json:"attempts"`
	Events   []EventView                 `json:"events"`
}

// API is the daemon surface the TUI needs.
type API interface {
	ListRuns(ctx context.Context) ([]RunSummary, error)
	GetRun(ctx context.Context, runID string) (*RunDetail, error)
	Tail(ctx context.Context, runID, nodeID string, lines int) ([]string, error)
	Approve(ctx context.Context, runID, nodeID string) error
	Reject(ctx context.Context, runID, nodeID string) error
	Cancel(ctx context.Context, runID, nodeID string) error
	Retry(ctx context.Context, runID, nodeID string) error
	Steer(ctx context.Context, runID, nodeID, message string) error
	RespondPermission(ctx context.Context, runID, nodeID, permissionID string, allow bool) error
}

// Client talks to a corral daemon over HTTP.
type Client struct {
	Base string
	Key  string
	HTTP *http.Client
	Role string
}

func NewClient(base, key string) *Client {
	return &Client{Base: base, Key: key, HTTP: &http.Client{Timeout: 15 * time.Second}, Role: "operator"}
}

// Do performs a raw API call (used by export and tools).
func (c *Client) Do(ctx context.Context, method, path string, body any, out any) error {
	return c.do(ctx, method, path, body, out)
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Corral-Role", c.Role)
	if c.Key != "" {
		req.Header.Set("Authorization", "Bearer "+c.Key)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, truncate(string(data), 200))
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

func (c *Client) ListRuns(ctx context.Context) ([]RunSummary, error) {
	var out []RunSummary
	err := c.do(ctx, http.MethodGet, "/api/runs", nil, &out)
	return out, err
}

func (c *Client) GetRun(ctx context.Context, runID string) (*RunDetail, error) {
	var out RunDetail
	err := c.do(ctx, http.MethodGet, "/api/runs/"+runID, nil, &out)
	return &out, err
}

func (c *Client) Tail(ctx context.Context, runID, nodeID string, lines int) ([]string, error) {
	var out struct {
		Lines []string `json:"lines"`
	}
	path := fmt.Sprintf("/api/runs/%s/tail?node=%s&lines=%d", runID, url.QueryEscape(nodeID), lines)
	err := c.do(ctx, http.MethodGet, path, nil, &out)
	return out.Lines, err
}

func (c *Client) nodeAction(ctx context.Context, path, runID, nodeID string) error {
	return c.do(ctx, http.MethodPost, path, map[string]string{"nodeID": nodeID}, nil)
}

func (c *Client) Approve(ctx context.Context, runID, nodeID string) error {
	return c.nodeAction(ctx, "/api/runs/"+runID+"/approve", runID, nodeID)
}
func (c *Client) Reject(ctx context.Context, runID, nodeID string) error {
	return c.nodeAction(ctx, "/api/runs/"+runID+"/reject", runID, nodeID)
}
func (c *Client) Cancel(ctx context.Context, runID, nodeID string) error {
	return c.nodeAction(ctx, "/api/runs/"+runID+"/cancel", runID, nodeID)
}
func (c *Client) Retry(ctx context.Context, runID, nodeID string) error {
	return c.nodeAction(ctx, "/api/runs/"+runID+"/retry", runID, nodeID)
}
func (c *Client) Steer(ctx context.Context, runID, nodeID, message string) error {
	return c.do(ctx, http.MethodPost, "/api/runs/"+runID+"/steer", map[string]string{"nodeID": nodeID, "message": message}, nil)
}
func (c *Client) RespondPermission(ctx context.Context, runID, nodeID, permissionID string, allow bool) error {
	return c.do(ctx, http.MethodPost, "/api/runs/"+runID+"/permission",
		map[string]any{"nodeID": nodeID, "permissionID": permissionID, "allow": allow}, nil)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
