package daemon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"corral/internal/clock"
	"corral/internal/graph"
	"corral/internal/sched"
	"corral/internal/store"
	"corral/internal/verify"
	"corral/internal/worktree"
)

func TestBrokerRoutesByRun(t *testing.T) {
	b := newBroker()
	ch1, unsub1 := b.Subscribe("r1")
	defer unsub1()
	ch1b, unsub1b := b.Subscribe("r1")
	defer unsub1b()
	ch2, unsub2 := b.Subscribe("r2")
	defer unsub2()

	b.Publish(store.Event{Seq: 1, RunID: "r1", Type: store.EventRun})

	for _, ch := range []<-chan store.Event{ch1, ch1b} {
		select {
		case ev := <-ch:
			if ev.Seq != 1 || ev.RunID != "r1" {
				t.Fatalf("r1 subscriber got %+v", ev)
			}
		default:
			t.Fatal("r1 subscriber missed event")
		}
	}
	select {
	case ev := <-ch2:
		t.Fatalf("r2 subscriber got r1 event %+v", ev)
	default:
	}
}

func TestBrokerUnsubscribeCleansUp(t *testing.T) {
	b := newBroker()
	_, unsub := b.Subscribe("r1")
	if b.SubscriberCount("r1") != 1 {
		t.Fatalf("count = %d, want 1", b.SubscriberCount("r1"))
	}
	unsub()
	if b.SubscriberCount("r1") != 0 {
		t.Fatalf("count after unsub = %d, want 0", b.SubscriberCount("r1"))
	}
	b.Publish(store.Event{Seq: 1, RunID: "r1"}) // must not panic or deliver
}

func TestBrokerDropsSlowSubscriber(t *testing.T) {
	b := newBroker()
	ch, _ := b.Subscribe("r1")
	for i := 0; i < subscriberBuffer; i++ {
		b.Publish(store.Event{Seq: int64(i + 1), RunID: "r1"})
	}
	b.Publish(store.Event{Seq: 1000, RunID: "r1"})
	if n := b.SubscriberCount("r1"); n != 0 {
		t.Fatalf("slow subscriber not dropped: count = %d", n)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("dropped subscriber channel was not closed")
		}
	}
}

func TestBrokerCloseClosesSubscribers(t *testing.T) {
	b := newBroker()
	ch, _ := b.Subscribe("r1")
	b.Close()
	if _, ok := <-ch; ok {
		t.Fatal("subscriber channel not closed after Close")
	}
	b.Publish(store.Event{Seq: 1, RunID: "r1"}) // no-op after close
	ch2, _ := b.Subscribe("r1")
	if _, ok := <-ch2; ok {
		t.Fatal("Subscribe after Close returned an open channel")
	}
}

func TestBrokerPublishUnsubscribeRace(t *testing.T) {
	b := newBroker()
	for i := 0; i < 500; i++ {
		_, unsubscribe := b.Subscribe("r1")
		done := make(chan struct{})
		go func(seq int64) {
			defer close(done)
			b.Publish(store.Event{Seq: seq, RunID: "r1"})
		}(int64(i + 1))
		unsubscribe()
		<-done
	}
	if got := b.SubscriberCount("r1"); got != 0 {
		t.Fatalf("subscriber count = %d, want 0", got)
	}
}

func TestClosingOneDaemonDoesNotDetachAnother(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "d.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	clk := clock.Real{}
	s := sched.New(st, sched.NewFakeDriver(clk, nil), &sched.EngineVerifier{Eng: verify.New(t.TempDir())}, clk, sched.Options{})
	one := New(st, s, nil, t.TempDir(), "")
	two := New(st, s, nil, t.TempDir(), "")
	t.Cleanup(two.Close)
	one.Close()

	events, unsubscribe := two.broker.Subscribe("r1")
	defer unsubscribe()
	if err := st.CreateRun(context.Background(), "r1", &graph.Graph{Nodes: []*graph.Node{{
		ID: "w1", Type: graph.NodeAgent, Objective: "o", AcceptanceCriteria: []string{"c"}, Priority: graph.PriorityNormal,
	}}}, false, time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		if event.Seq != 1 || event.RunID != "r1" {
			t.Fatalf("second daemon got %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("closing first daemon detached second daemon")
	}
}

func TestSSESubscriberCleanupOnDisconnect(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "d.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	repo := t.TempDir()
	for _, args := range [][]string{
		{"init", "-q", "-b", "main"},
		{"-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git: %v: %s", err, out)
		}
	}
	clk := clock.Real{}
	drv := sched.NewFakeDriver(clk, nil)
	eng := verify.New(repo)
	eng.Runner = verify.ExecRunner{}
	s := sched.New(st, drv, &sched.EngineVerifier{Eng: eng}, clk, sched.Options{
		Concurrency: 4, Worktrees: worktree.NewManager(repo),
	})
	d := New(st, s, nil, repo, "")
	t.Cleanup(d.Close)
	srv := httptest.NewServer(d.Handler())
	t.Cleanup(srv.Close)

	ctx := context.Background()
	h, err := s.Create(ctx, "run_cleanup", &graph.Graph{Nodes: []*graph.Node{
		{ID: "w1", Type: graph.NodeAgent, Role: "worker", Objective: "o",
			AcceptanceCriteria: []string{"c"}, Priority: graph.PriorityNormal,
			Meta:         map[string]string{"cwd": repo},
			Verification: &graph.Verification{Kind: "command", Command: []string{"true"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	go h.Run(ctx, 50*time.Millisecond)

	reqCtx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(reqCtx, http.MethodGet, srv.URL+"/api/runs/run_cleanup/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return d.broker.SubscriberCount("run_cleanup") == 1 }, "subscriber registered")
	cancel()
	waitFor(t, func() bool { return d.broker.SubscriberCount("run_cleanup") == 0 }, "subscriber cleaned up on disconnect")
	resp.Body.Close()
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}
