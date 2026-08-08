// Package clock provides a time abstraction so the scheduler can run
// against wall time in production and against a deterministic fake clock
// in simulations.
package clock

import (
	"sync"
	"time"
)

type Clock interface {
	Now() time.Time
	// After returns a channel that fires after d. For Real it is a wall
	// timer; for Fake it fires when Advance crosses the deadline.
	After(d time.Duration) <-chan time.Time
}

// Real is the wall-clock implementation.
type Real struct{}

func (Real) Now() time.Time                         { return time.Now() }
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Fake is a manually advanced clock. Timers registered with After fire when
// Advance passes their deadline. It is safe for concurrent use.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	timers []fakeTimer
}

type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
}

func NewFake(start time.Time) *Fake {
	return &Fake{now: start}
}

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan time.Time, 1)
	f.timers = append(f.timers, fakeTimer{deadline: f.now.Add(d), ch: ch})
	return ch
}

// Advance moves the fake clock forward by d and fires due timers.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	remaining := f.timers[:0]
	for _, t := range f.timers {
		if !t.deadline.After(f.now) {
			select {
			case t.ch <- f.now:
			default:
			}
			continue
		}
		remaining = append(remaining, t)
	}
	f.timers = remaining
}
