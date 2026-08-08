package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"corral/internal/ocx"
	"corral/internal/spike"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tmp, err := os.MkdirTemp("", "corral-spike-")
	if err != nil {
		fatal(err)
	}
	defer os.RemoveAll(tmp)
	if err := gitInit(tmp); err != nil {
		fatal(err)
	}
	fmt.Printf("project: %s\n", tmp)

	srv, err := spike.StartServer(ctx, tmp, 0, os.Stderr)
	if err != nil {
		fatal(err)
	}
	defer srv.Stop()
	fmt.Printf("server:  %s\n", srv.Base)

	c := ocx.New(srv.Base, tmp)
	res, err := spike.Run(ctx, c, tmp, spike.Options{Log: func(f string, a ...any) { fmt.Printf("  "+f+"\n", a...) }})
	if err != nil {
		fatal(err)
	}
	printReport(res)
}

func printReport(res *spike.Result) {
	fmt.Println()
	fmt.Println("== report ==")
	for _, s := range res.Sessions {
		st := "completed"
		if s.Aborted {
			st = "aborted"
		}
		fmt.Printf("  %-12s %-10s busy=%v idle=%-18s finish=%-12s files=%v cost=$%.4f tokens=%d\n",
			s.Title, st, s.BusySeen, s.IdleAt.Format("15:04:05.000"), orEmpty(s.Finish), s.Files, s.Cost, s.Tokens)
	}
	fmt.Printf("  peak concurrent busy: %d\n", res.PeakConcurrent)
	for _, s := range res.Sessions {
		fmt.Printf("  diffs[%s]: %v\n", s.Title, res.DiffCaptured[s.Title])
	}
	fmt.Println("  reconcile:")
	for sid, rs := range res.Reconcile.PerSession {
		fmt.Printf("    %s state=%s completed=%d aborted=%d finish=%q err=%q\n",
			sid, rs.State, rs.CompletedRuns, rs.AbortedRuns, rs.LatestFinish, rs.LatestErrorName)
	}
	fmt.Printf("    sessions found: %d  duplicate-done: %v\n", res.Reconcile.Found, res.Reconcile.DuplicateDone)
	fmt.Printf("  follow-up: %s finished=%v files=%v\n", res.FollowUp.SessionID, res.FollowUp.Finished, res.FollowUp.Files)
}

func orEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func gitInit(dir string) error {
	cmd := exec.Command("git", "init", "-q", "-b", "main")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git init: %w: %s", err, out)
	}
	cmd = exec.Command("git", "commit", "-q", "--allow-empty", "-m", "init")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git commit: %w: %s", err, out)
	}
	return nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "spike:", err)
	os.Exit(1)
}

var _ = filepath.Join
