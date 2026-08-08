package spike_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"corral/internal/livetest"
	"corral/internal/ocx"
	"corral/internal/spike"
)

func TestTransportSpike(t *testing.T) {
	livetest.SkipIfDisabled(t)
	if _, err := exec.LookPath("opencode"); err != nil {
		t.Skip("opencode binary not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	proj, err := os.MkdirTemp("", "corral-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(proj) })
	gitInit(t, proj)

	srv, err := spike.StartServer(ctx, proj, 0, os.Stderr)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(srv.Stop)

	logf := func(f string, a ...any) { t.Logf(f, a...) }
	res, err := spike.Run(ctx, ocx.New(srv.Base, proj), proj, spike.Options{Log: logf})
	if err != nil {
		t.Fatalf("scenario: %v", err)
	}

	by := map[string]*spike.SessionResult{}
	for _, s := range res.Sessions {
		by[s.Title] = s
	}

	// 1. Three independent sessions launched concurrently, all observed busy.
	for _, title := range []string{spike.TitleFast, spike.TitleSlow, spike.TitleMed} {
		if !by[title].BusySeen {
			t.Errorf("%s: never observed busy", title)
		}
	}
	if res.PeakConcurrent < 2 {
		t.Errorf("expected >=2 concurrent busy sessions, got %d", res.PeakConcurrent)
	}

	// 2. Cancellation: w2 aborted mid-run with error marker and partial output.
	slow := by[spike.TitleSlow]
	if !slow.Aborted || slow.AbortError != "MessageAbortedError" {
		t.Errorf("w2 not aborted cleanly: aborted=%v err=%q", slow.Aborted, slow.AbortError)
	}
	beta, err := os.ReadFile(filepath.Join(proj, "beta.txt"))
	if err != nil {
		t.Fatalf("beta.txt missing after abort: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(beta)), "\n")
	if len(lines) >= 30 {
		t.Errorf("abort had no effect: %d lines in beta.txt", len(lines))
	}
	if len(lines) == 0 {
		t.Errorf("abort fired too early: beta.txt empty")
	}
	if slow.Finish != "" {
		t.Errorf("aborted attempt should have no finish marker, got %q", slow.Finish)
	}

	// 3. Fast/medium sessions completed with terminal state and files.
	for _, title := range []string{spike.TitleFast, spike.TitleMed} {
		s := by[title]
		if !s.Finished {
			t.Errorf("%s: never finished", title)
			continue
		}
		if s.Finish != "stop" {
			t.Errorf("%s: expected finish=stop, got %q", title, s.Finish)
		}
	}
	alpha, err := os.ReadFile(filepath.Join(proj, "alpha.txt"))
	if err != nil {
		t.Fatalf("alpha.txt missing: %v", err)
	}
	// Exactly one CORRAL-A1 line proves no duplicate execution after the
	// simulated restart (the follow-up appended CORRAL-B2 afterwards).
	if got := countLines(string(alpha), "CORRAL-A1"); got != 1 {
		t.Errorf("alpha.txt has %d CORRAL-A1 lines, want exactly 1: %q", got, string(alpha))
	}
	for _, f := range []string{"gamma-1.txt", "gamma-2.txt"} {
		data, err := os.ReadFile(filepath.Join(proj, f))
		if err != nil {
			t.Fatalf("%s missing: %v", f, err)
		}
		if got := strings.TrimSpace(string(data)); got != "CORRAL-G" {
			t.Errorf("%s = %q, want CORRAL-G", f, got)
		}
	}

	// 4. Diffs captured from message summaries.
	for _, title := range []string{spike.TitleFast, spike.TitleMed} {
		if len(res.DiffCaptured[title]) == 0 {
			t.Errorf("%s: no diffs captured", title)
		}
	}
	if !contains(res.DiffCaptured[spike.TitleFast], "alpha.txt") {
		t.Errorf("alpha.txt not in captured diffs: %v", res.DiffCaptured[spike.TitleFast])
	}

	// 5. Restart reconciliation: no duplicate completion, correct states.
	if res.Reconcile.Found != 3 {
		t.Errorf("reconcile found %d sessions, want 3", res.Reconcile.Found)
	}
	if res.Reconcile.DuplicateDone {
		t.Error("duplicate completed attempts detected")
	}
	fastID := by[spike.TitleFast].SessionID
	rs := res.Reconcile.PerSession[fastID]
	if rs.CompletedRuns != 1 {
		t.Errorf("fast session completed %d runs, want 1 (no dup after restart)", rs.CompletedRuns)
	}
	rsSlow := res.Reconcile.PerSession[by[spike.TitleSlow].SessionID]
	if rsSlow.AbortedRuns == 0 {
		t.Error("aborted run not visible in reconciliation")
	}

	// 6. Fresh client retains control (follow-up prompt succeeded).
	if res.FollowUp == nil || !res.FollowUp.Finished {
		t.Error("follow-up control prompt did not finish")
	}
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

func countLines(text, want string) int {
	n := 0
	for _, l := range strings.Split(text, "\n") {
		if strings.TrimSpace(l) == want {
			n++
		}
	}
	return n
}

func gitInit(t *testing.T, dir string) {
	t.Helper()
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"commit", "-q", "--allow-empty", "-m", "init"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
}
