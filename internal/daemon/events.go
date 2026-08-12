package daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"corral/internal/store"
)

// defaultEventHeartbeat is the idle keep-alive interval for the SSE stream.
const defaultEventHeartbeat = 15 * time.Second

// SetEventHeartbeat overrides the SSE heartbeat interval (default 15s).
func (d *Daemon) SetEventHeartbeat(interval time.Duration) {
	if interval <= 0 {
		interval = defaultEventHeartbeat
	}
	d.eventHeartbeatMu.Lock()
	d.eventHeartbeat = interval
	d.eventHeartbeatMu.Unlock()
}

func (d *Daemon) eventHeartbeatInterval() time.Duration {
	d.eventHeartbeatMu.RLock()
	defer d.eventHeartbeatMu.RUnlock()
	return d.eventHeartbeat
}

// handleEvents streams the run's event log as server-sent events. The
// ?after=<seq> query cursor selects only events with seq greater than the
// cursor; persisted events are replayed first and live transitions (and
// heartbeats) follow on the broker. A terminal run event ends the stream after
// delivery; reconnecting with its sequence cursor is safe.
func (d *Daemon) handleEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	runID := r.PathValue("id")
	after, err := parseEventCursor(r.URL.Query().Get("after"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	run, err := d.st.Run(ctx, runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Subscribe before reading the durable snapshot. An event committed during
	// the read queues a notification, and every notification is reconciled from
	// the store by sequence, so overlap is deduplicated and gaps are impossible.
	ch, unsubscribe := d.broker.Subscribe(runID)
	defer unsubscribe()
	replay, err := d.st.EventsAfter(ctx, runID, after)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	frames, err := encodeEventFrames(replay, after)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	last := after
	terminal := run.Status == "completed" || run.Status == "canceled"
	for _, frame := range frames {
		if _, err := w.Write(frame.data); err != nil {
			return
		}
		fl.Flush()
		last = frame.event.Seq
		terminal = terminal || terminalRunEvent(frame.event)
	}
	if len(frames) == 0 {
		fl.Flush()
	}
	if terminal {
		return
	}

	hb := time.NewTicker(d.eventHeartbeatInterval())
	defer hb.Stop()
	flushDurable := func() (bool, error) {
		events, err := d.st.EventsAfter(ctx, runID, last)
		if err != nil {
			return false, err
		}
		for _, ev := range events {
			if ev.Seq <= last {
				continue
			}
			if err := writeEventSSE(w, fl, ev); err != nil {
				return false, err
			}
			last = ev.Seq
			if terminalRunEvent(ev) {
				return true, nil
			}
		}
		return false, nil
	}

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
			// The broker is a wake-up path, not the source of truth. Reading the
			// durable log here preserves order even when concurrent commits notify
			// out of order.
			terminal, err := flushDurable()
			if err != nil || terminal {
				return // headers are committed; reconnect replays from last id
			}
		case <-hb.C:
			// A best-effort store or broker wakeup may be dropped when buffers
			// overflow. Heartbeats also reconcile the durable cursor so a quiet
			// stream never waits forever for another event.
			terminal, err := flushDurable()
			if err != nil || terminal {
				return
			}
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			fl.Flush()
		case <-ctx.Done():
			return
		}
	}
}

func parseEventCursor(raw string) (int64, error) {
	if raw == "" {
		return 0, nil
	}
	after, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || after < 0 {
		return 0, fmt.Errorf("after must be a non-negative integer")
	}
	return after, nil
}

type eventFrame struct {
	event store.Event
	data  []byte
}

func encodeEventFrames(events []store.Event, after int64) ([]eventFrame, error) {
	frames := make([]eventFrame, 0, len(events))
	for _, ev := range events {
		if ev.Seq <= after {
			continue
		}
		var buf bytes.Buffer
		if err := writeEventSSE(&buf, nil, ev); err != nil {
			return nil, err
		}
		frames = append(frames, eventFrame{event: ev, data: buf.Bytes()})
	}
	return frames, nil
}

func terminalRunEvent(ev store.Event) bool {
	if ev.Type != store.EventRun {
		return false
	}
	var payload struct {
		Status string `json:"status"`
	}
	if json.Unmarshal(ev.Payload, &payload) != nil {
		return false
	}
	return payload.Status == "completed" || payload.Status == "canceled"
}

// writeEventSSE frames one event as an SSE record with a resume id.
func writeEventSSE(w io.Writer, fl http.Flusher, ev store.Event) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\ndata: %s\n\n", ev.Seq, b); err != nil {
		return err
	}
	if fl != nil {
		fl.Flush()
	}
	return nil
}
