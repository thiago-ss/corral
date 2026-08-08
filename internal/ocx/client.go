package ocx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	base string
	http *http.Client
	dir  string
}

func New(base, directory string) *Client {
	return &Client{
		base: strings.TrimSuffix(base, "/"),
		http: &http.Client{Timeout: 120 * time.Second},
		dir:  directory,
	}
}

func (c *Client) Base() string { return c.base }

func (c *Client) do(ctx context.Context, method, path string, query url.Values, body any, out any) (int, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rdr = bytes.NewReader(b)
	}
	u := c.base + path
	if query != nil {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	if resp.StatusCode >= 400 {
		return resp.StatusCode, fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, truncate(raw, 500))
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode %s: %w", path, err)
		}
	}
	return resp.StatusCode, nil
}

func (c *Client) CreateSession(ctx context.Context, title string) (Session, error) {
	var s Session
	_, err := c.do(ctx, http.MethodPost, "/session", url.Values{"directory": {c.dir}}, map[string]string{"title": title}, &s)
	return s, err
}

func (c *Client) PromptAsync(ctx context.Context, sid, text, model string) error {
	body := map[string]any{
		"parts": []map[string]string{{"type": "text", "text": text}},
	}
	if model != "" {
		body["model"] = model
	}
	_, err := c.do(ctx, http.MethodPost, "/session/"+sid+"/prompt_async", url.Values{"directory": {c.dir}}, body, nil)
	return err
}

func (c *Client) ListSessions(ctx context.Context) ([]Session, error) {
	var ss []Session
	_, err := c.do(ctx, http.MethodGet, "/session", url.Values{"directory": {c.dir}}, nil, &ss)
	return ss, err
}

func (c *Client) SessionStatus(ctx context.Context) (map[string]SessionStatus, error) {
	m := map[string]SessionStatus{}
	_, err := c.do(ctx, http.MethodGet, "/session/status", url.Values{"directory": {c.dir}}, nil, &m)
	return m, err
}

func (c *Client) Messages(ctx context.Context, sid string, limit int) ([]Message, error) {
	q := url.Values{"directory": {c.dir}}
	if limit > 0 {
		q.Set("limit", fmt.Sprint(limit))
	}
	var ms []Message
	_, err := c.do(ctx, http.MethodGet, "/session/"+sid+"/message", q, nil, &ms)
	return ms, err
}

func (c *Client) Abort(ctx context.Context, sid string) error {
	_, err := c.do(ctx, http.MethodPost, "/session/"+sid+"/abort", url.Values{"directory": {c.dir}}, nil, nil)
	return err
}

// RespondPermission answers a pending permission request.
func (c *Client) RespondPermission(ctx context.Context, sid, permissionID, response string) error {
	_, err := c.do(ctx, http.MethodPost, "/session/"+sid+"/permissions/"+permissionID,
		url.Values{"directory": {c.dir}}, map[string]any{"response": response}, nil)
	return err
}

func (c *Client) DeleteSession(ctx context.Context, sid string) error {
	_, err := c.do(ctx, http.MethodDelete, "/session/"+sid, url.Values{"directory": {c.dir}}, nil, nil)
	return err
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
