package daemon

import (
	"sync"

	"corral/internal/store"
)

// broker is the in-daemon live-event hub. The store notifies it of every
// committed event; it fans those out to per-run subscribers (SSE clients).
//
// Subscribers are removed on disconnect via the returned unsubscribe func.
// A subscriber whose buffer overflows is dropped so the scheduler never
// blocks on a slow consumer; the client reconnects with an after cursor and
// the store replays the gap.
type broker struct {
	mu     sync.Mutex
	subs   map[string]map[*subscriber]struct{}
	closed bool
}

type subscriber struct {
	runID string
	ch    chan store.Event
}

const subscriberBuffer = 256

func newBroker() *broker {
	return &broker{subs: map[string]map[*subscriber]struct{}{}}
}

// Subscribe registers a subscriber for runID. Events for that run are
// delivered on the returned channel; the caller must call the returned
// func when done (e.g. on request context cancellation).
func (b *broker) Subscribe(runID string) (<-chan store.Event, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		ch := make(chan store.Event)
		close(ch)
		return ch, func() {}
	}
	if b.subs[runID] == nil {
		b.subs[runID] = map[*subscriber]struct{}{}
	}
	sub := &subscriber{runID: runID, ch: make(chan store.Event, subscriberBuffer)}
	b.subs[runID][sub] = struct{}{}
	return sub.ch, func() { b.unsubscribe(sub) }
}

func (b *broker) unsubscribe(sub *subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	subs := b.subs[sub.runID]
	if _, ok := subs[sub]; ok {
		delete(subs, sub)
		close(sub.ch)
		if len(subs) == 0 {
			delete(b.subs, sub.runID)
		}
	}
}

// Publish delivers an event to every subscriber of its run. It never
// blocks: slow consumers are dropped rather than stalling the writer.
func (b *broker) Publish(ev store.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	subs := b.subs[ev.RunID]
	for sub := range subs {
		select {
		case sub.ch <- ev:
		default:
			delete(subs, sub)
			close(sub.ch)
		}
	}
	if len(subs) == 0 {
		delete(b.subs, ev.RunID)
	}
}

// Close shuts the broker down, closing every subscriber channel. Publish
// after Close is a no-op.
func (b *broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for _, subs := range b.subs {
		for sub := range subs {
			close(sub.ch)
		}
	}
	b.subs = map[string]map[*subscriber]struct{}{}
}

// SubscriberCount reports how many subscribers a run currently has
// (used by tests to assert cleanup on disconnect).
func (b *broker) SubscriberCount(runID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs[runID])
}
