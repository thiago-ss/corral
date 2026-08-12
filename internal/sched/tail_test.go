package sched

import (
	"context"
	"testing"
	"time"

	"corral/internal/adapter"
	"corral/internal/graph"
)

type blockingMessagesSession struct {
	entered chan struct{}
	release chan struct{}
}

func (s *blockingMessagesSession) ID() string       { return "session" }
func (s *blockingMessagesSession) ServerID() string { return "server" }
func (s *blockingMessagesSession) Send(context.Context, string) error {
	return nil
}
func (s *blockingMessagesSession) Abort(context.Context) error { return nil }
func (s *blockingMessagesSession) Status(context.Context) (adapter.Status, error) {
	return adapter.StatusRunning, nil
}
func (s *blockingMessagesSession) Messages(context.Context) ([]adapter.Message, error) {
	close(s.entered)
	<-s.release
	return []adapter.Message{{Text: "tail"}}, nil
}
func (s *blockingMessagesSession) Close(context.Context) error { return nil }

func TestTailDoesNotHoldRunMutexWhileFetchingMessages(t *testing.T) {
	sess := &blockingMessagesSession{entered: make(chan struct{}), release: make(chan struct{})}
	h := &RunHandle{sessions: map[graph.NodeID]*sessionRec{
		"node": {nodeID: "node", sess: sess},
	}}
	tailDone := make(chan error, 1)
	go func() {
		_, err := h.Tail(context.Background(), "node", 10)
		tailDone <- err
	}()
	select {
	case <-sess.entered:
	case <-time.After(time.Second):
		t.Fatal("Tail did not call Messages")
	}

	mutexAvailable := make(chan struct{})
	go func() {
		_ = h.ActiveSessions()
		close(mutexAvailable)
	}()
	select {
	case <-mutexAvailable:
		// Good: Messages is still blocked, but scheduler mutex is free.
	case <-time.After(100 * time.Millisecond):
		close(sess.release)
		t.Fatal("Tail held RunHandle mutex across Messages I/O")
	}
	close(sess.release)
	if err := <-tailDone; err != nil {
		t.Fatal(err)
	}
}
