package ocx

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type EventHandler func(Event)

// streamClient has no overall timeout; the request context controls
// cancellation so long-lived SSE streams survive indefinitely.
var streamClient = &http.Client{}

func (c *Client) StreamEvents(ctx context.Context, handler EventHandler) error {
	return c.StreamEventsReady(ctx, nil, handler)
}

// StreamEventsReady is StreamEvents with a callback invoked after each
// successful HTTP subscription, before any event records are read.
func (c *Client) StreamEventsReady(ctx context.Context, ready func(), handler EventHandler) error {
	for {
		err := c.streamOnce(ctx, ready, handler)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
		if err != nil {
			fmt.Printf("event stream error, reconnecting in 2s: %v\n", err)
		}
	}
}

func (c *Client) streamOnce(ctx context.Context, ready func(), handler EventHandler) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/global/event", nil)
	if err != nil {
		return err
	}
	// The default client has an overall 120s timeout which would kill a
	// long-lived stream; the stream needs its own client without one.
	resp, err := streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("event stream: %s", resp.Status)
	}
	if ready != nil {
		ready()
	}
	br := bufio.NewReader(resp.Body)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var frame struct {
			Directory string          `json:"directory"`
			Project   string          `json:"project"`
			Payload   json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &frame); err != nil {
			fmt.Printf("bad event frame: %v\n", err)
			continue
		}
		var ev Event
		if err := json.Unmarshal(frame.Payload, &ev); err != nil {
			fmt.Printf("bad event payload: %v\n", err)
			continue
		}
		ev.Directory = frame.Directory
		ev.Project = frame.Project
		handler(ev)
	}
}

func (ev Event) UnmarshalProps(v any) error {
	return json.Unmarshal(ev.Properties, v)
}
