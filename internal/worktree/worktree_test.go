package worktree

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func gitInit(t *testing.T, dir string) {
	t.Helper()
	for _, args := range [][]string{
		{"init", "-q", "-b", "main"},
		{"-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
}

// resolved returns the canonical (symlink-resolved) form of p, the form
// git reports on macOS (/var vs /private/var).
func resolved(t *testing.T, p string) string {
	t.Helper()
	r, err := filepath.EvalSymlinks(p)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestManagerLifecycle(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)

	path, err := m.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if _, err := os.Stat(filepath.Join(path, ".git")); err != nil {
		t.Fatalf("worktree missing: %v", err)
	}
	// Uncommitted change in the worktree.
	if err := os.WriteFile(filepath.Join(path, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	patch, err := m.Diff(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if patch == "" {
		t.Fatal("empty diff")
	}
	files, err := m.Files(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0] != "a.txt" {
		t.Fatalf("files = %v", files)
	}
	if HashContent(patch) != HashContent(patch) {
		t.Fatal("content addressing inconsistent")
	}
	if HashContent("x") == HashContent("y") {
		t.Fatal("hash collision")
	}

	// Main checkout must be untouched while the worktree holds changes.
	if _, err := os.Stat(filepath.Join(repo, "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("main checkout corrupted: %v", err)
	}

	// Commit + merge into main.
	if err := m.CommitWorktree(ctx, path); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := m.MergeBranch(ctx, "corral/r1/w1/1"); err != nil {
		t.Fatalf("merge: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(repo, "a.txt"))
	if err != nil {
		t.Fatalf("merged file missing: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("merged content = %q", data)
	}

	if err := m.Remove(ctx, "corral/r1/w1/1"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("worktree not removed")
	}
}

func TestRemovePreservesIgnoredContent(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)
	if err := os.WriteFile(filepath.Join(repo, ".gitignore"), []byte("secret.env\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.git(ctx, repo, "add", ".gitignore"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.git(ctx, repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "ignore"); err != nil {
		t.Fatal(err)
	}
	branch := "corral/r1/w1/1"
	path, err := m.Add(ctx, branch)
	if err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(path, "secret.env")
	if err := os.WriteFile(secret, []byte("keep me"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.Remove(ctx, branch); err == nil || !strings.Contains(err.Error(), "dirty worktree") {
		t.Fatalf("Remove ignored-content error = %v", err)
	}
	if got, err := os.ReadFile(secret); err != nil || string(got) != "keep me" {
		t.Fatalf("ignored content lost: %q, %v", got, err)
	}
}

func TestCommitWorktreePropagatesCommitFailureAndCanRetry(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)

	path, err := m.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	hook := filepath.Join(repo, ".git", "hooks", "pre-commit")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := m.CommitWorktree(ctx, path); err == nil {
		t.Fatal("CommitWorktree hid a real git commit failure")
	}
	status, err := m.git(ctx, path, "status", "--porcelain=v1")
	if err != nil {
		t.Fatal(err)
	}
	if status != "A  a.txt\n" {
		t.Fatalf("failed commit did not preserve staged work: %q", status)
	}
	mainStatus, err := m.git(ctx, repo, "status", "--porcelain=v1")
	if err != nil {
		t.Fatal(err)
	}
	if mainStatus != "" {
		t.Fatalf("failed worktree commit dirtied main: %q", mainStatus)
	}

	if err := os.Remove(hook); err != nil {
		t.Fatal(err)
	}
	if err := m.CommitWorktree(ctx, path); err != nil {
		t.Fatalf("retry commit: %v", err)
	}
	if err := m.MergeBranch(ctx, "corral/r1/w1/1"); err != nil {
		t.Fatalf("merge after retry: %v", err)
	}
}

func TestCommitWorktreeAllowsNothingToCommit(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)

	path, err := m.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.CommitWorktree(ctx, path); err != nil {
		t.Fatalf("empty commit: %v", err)
	}
}

func TestMergeBranchAbortsConflictAndCanRetry(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)

	if err := os.WriteFile(filepath.Join(repo, "conflict.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.git(ctx, repo, "add", "conflict.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.git(ctx, repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "base"); err != nil {
		t.Fatal(err)
	}

	path, err := m.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "conflict.txt"), []byte("branch\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := m.CommitWorktree(ctx, path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "conflict.txt"), []byte("main\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.git(ctx, repo, "add", "conflict.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.git(ctx, repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "main change"); err != nil {
		t.Fatal(err)
	}
	mainHead, err := m.git(ctx, repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}

	if err := m.MergeBranch(ctx, "corral/r1/w1/1"); err == nil {
		t.Fatal("conflicting merge unexpectedly succeeded")
	}
	assertCleanMainWithoutMerge(t, m, ctx, mainHead)

	// Fold main into the worktree branch, making a retry conflict-free.
	if _, err := m.git(ctx, path,
		"-c", "user.name=t", "-c", "user.email=t@t",
		"merge", "-q", "-s", "ours", "-m", "resolve main", "main"); err != nil {
		t.Fatal(err)
	}
	if err := m.MergeBranch(ctx, "corral/r1/w1/1"); err != nil {
		t.Fatalf("merge retry: %v", err)
	}
}

func TestMergeBranchAbortsFailedMergeCommitAndCanRetry(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)

	path, err := m.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := m.CommitWorktree(ctx, path); err != nil {
		t.Fatal(err)
	}
	hook := filepath.Join(repo, ".git", "hooks", "pre-merge-commit")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	mainHead, err := m.git(ctx, repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}

	if err := m.MergeBranch(ctx, "corral/r1/w1/1"); err == nil {
		t.Fatal("merge commit hook failure was hidden")
	}
	assertCleanMainWithoutMerge(t, m, ctx, mainHead)

	if err := os.Remove(hook); err != nil {
		t.Fatal(err)
	}
	if err := m.MergeBranch(ctx, "corral/r1/w1/1"); err != nil {
		t.Fatalf("merge retry: %v", err)
	}
}

func assertCleanMainWithoutMerge(t *testing.T, m *Manager, ctx context.Context, wantHead string) {
	t.Helper()
	if _, code, err := m.gitExit(ctx, m.repo, "rev-parse", "--verify", "--quiet", "MERGE_HEAD"); err != nil {
		t.Fatal(err)
	} else if code == 0 {
		t.Fatal("failed merge left MERGE_HEAD active")
	}
	status, err := m.git(ctx, m.repo, "status", "--porcelain=v1")
	if err != nil {
		t.Fatal(err)
	}
	if status != "" {
		t.Fatalf("failed merge left main dirty: %q", status)
	}
	head, err := m.git(ctx, m.repo, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if head != wantHead {
		t.Fatalf("failed merge moved main HEAD: got %q want %q", head, wantHead)
	}
}

func TestManagerList(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)

	if infos, err := m.List(ctx); err != nil {
		t.Fatalf("list: %v", err)
	} else if len(infos) != 0 {
		t.Fatalf("List = %v, want none", infos)
	}

	path, err := m.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	infos, err := m.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 {
		t.Fatalf("List = %v, want 1 worktree", infos)
	}
	info := infos[0]
	if info.Branch != "corral/r1/w1/1" {
		t.Fatalf("branch = %q", info.Branch)
	}
	if info.Path != resolved(t, path) {
		t.Fatalf("path = %q, want %q", info.Path, resolved(t, path))
	}
	if info.Head == "" {
		t.Fatal("head empty")
	}
	if info.Mtime.IsZero() {
		t.Fatal("mtime zero")
	}
	if !info.Dirty {
		t.Fatal("dirty worktree reported clean")
	}
	if info.Detached || info.Locked || info.Orphaned {
		t.Fatalf("unexpected flags: %+v", info)
	}
	// The main checkout is never listed.
	for _, i := range infos {
		if i.Path == repo {
			t.Fatalf("main checkout listed: %+v", i)
		}
	}
}

func TestManagerPruneMerged(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)

	path, err := m.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := m.CommitWorktree(ctx, path); err != nil {
		t.Fatal(err)
	}
	if err := m.MergeBranch(ctx, "corral/r1/w1/1"); err != nil {
		t.Fatal(err)
	}
	merged, err := m.BranchMerged(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if !merged {
		t.Fatal("merged branch should report merged")
	}
	want := resolved(t, path)

	pruned, err := m.Prune(ctx, 0, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(pruned) != 1 || pruned[0] != want {
		t.Fatalf("pruned = %v, want [%s]", pruned, want)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("worktree dir still exists")
	}
	if infos, err := m.List(ctx); err != nil || len(infos) != 0 {
		t.Fatalf("List after prune = %v (err %v)", infos, err)
	}
	// Main checkout keeps the merged content and is untouched.
	data, err := os.ReadFile(filepath.Join(repo, "a.txt"))
	if err != nil || string(data) != "hello" {
		t.Fatalf("main checkout wrong: %v %q", err, data)
	}
}

func TestManagerPruneSkipsDirtyMerged(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)

	path, err := m.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "merged.txt"), []byte("merged"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := m.CommitWorktree(ctx, path); err != nil {
		t.Fatal(err)
	}
	if err := m.MergeBranch(ctx, "corral/r1/w1/1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "unfinished.txt"), []byte("keep me"), 0o644); err != nil {
		t.Fatal(err)
	}

	pruned, err := m.Prune(ctx, 0, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(pruned) != 0 {
		t.Fatalf("dirty merged worktree pruned: %v", pruned)
	}
	if data, err := os.ReadFile(filepath.Join(path, "unfinished.txt")); err != nil || string(data) != "keep me" {
		t.Fatalf("uncommitted work lost: %q (%v)", data, err)
	}
}

func TestManagerPruneStale(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)

	path, err := m.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := m.CommitWorktree(ctx, path); err != nil {
		t.Fatal(err)
	}
	now := time.Now()

	// An unmerged worktree survives --prune alone (age criterion off).
	if pruned, err := m.Prune(ctx, 0, now); err != nil || len(pruned) != 0 {
		t.Fatalf("prune without stale removed %v (err %v)", pruned, err)
	}
	// It survives --prune --stale while fresh.
	if pruned, err := m.Prune(ctx, 24*time.Hour, now); err != nil || len(pruned) != 0 {
		t.Fatalf("fresh prune removed %v (err %v)", pruned, err)
	}
	// Idle past the threshold: removed, but its branch is kept.
	want := resolved(t, path)
	pruned, err := m.Prune(ctx, 24*time.Hour, now.Add(30*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(pruned) != 1 || pruned[0] != want {
		t.Fatalf("stale pruned = %v, want [%s]", pruned, want)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("stale worktree dir still exists")
	}
	if _, code, _ := m.gitExit(ctx, m.repo, "rev-parse", "--verify", "--quiet", "refs/heads/corral/r1/w1/1"); code != 0 {
		t.Fatal("unmerged stale branch should be kept")
	}
}

func TestManagerPruneSkipsDirtyStale(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)

	path, err := m.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "unfinished.txt"), []byte("keep me"), 0o644); err != nil {
		t.Fatal(err)
	}

	pruned, err := m.Prune(ctx, time.Hour, time.Now().Add(30*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(pruned) != 0 {
		t.Fatalf("dirty stale worktree pruned: %v", pruned)
	}
	if _, err := os.Stat(filepath.Join(path, "unfinished.txt")); err != nil {
		t.Fatalf("uncommitted work lost: %v", err)
	}
}

func TestManagerPruneSkipsIgnoredContent(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)
	if err := os.WriteFile(filepath.Join(repo, ".gitignore"), []byte("secret.env\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.git(ctx, repo, "add", ".gitignore"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.git(ctx, repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "ignore secret"); err != nil {
		t.Fatal(err)
	}
	path, err := m.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(path, "secret.env")
	if err := os.WriteFile(secret, []byte("keep me"), 0o600); err != nil {
		t.Fatal(err)
	}
	pruned, err := m.Prune(ctx, time.Hour, time.Now().Add(30*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(pruned) != 0 {
		t.Fatalf("worktree with ignored content pruned: %v", pruned)
	}
	if got, err := os.ReadFile(secret); err != nil || string(got) != "keep me" {
		t.Fatalf("ignored content lost: %q, %v", got, err)
	}
}

func TestManagerPruneSkipsLocked(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)

	path, err := m.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m.git(ctx, m.repo, "worktree", "lock", "--reason", "active run", path); err != nil {
		t.Fatal(err)
	}
	if err := m.CommitWorktree(ctx, path); err != nil {
		t.Fatal(err)
	}
	if err := m.MergeBranch(ctx, "corral/r1/w1/1"); err != nil {
		t.Fatal(err)
	}
	// Merged, but locked: never pruned.
	if pruned, err := m.Prune(ctx, 0, time.Now()); err != nil || len(pruned) != 0 {
		t.Fatalf("locked worktree pruned: %v (err %v)", pruned, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("locked worktree dir removed: %v", err)
	}
}

func TestManagerPruneOrphaned(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)

	path, err := m.Add(ctx, "corral/r1/w1/1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	// The admin entry still exists until a prune sweeps it.
	if _, err := m.Prune(ctx, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if infos, err := m.List(ctx); err != nil || len(infos) != 0 {
		t.Fatalf("orphaned worktree not cleaned: %v (err %v)", infos, err)
	}
}

func TestManagerBranchMergedMissing(t *testing.T) {
	repo := t.TempDir()
	gitInit(t, repo)
	ctx := context.Background()
	m := NewManager(repo)

	merged, err := m.BranchMerged(ctx, "corral/r1/never/1")
	if err != nil {
		t.Fatal(err)
	}
	if !merged {
		t.Fatal("missing branch should report merged (safe to prune)")
	}
}

func TestScopesOverlap(t *testing.T) {
	cases := []struct {
		a, b []string
		want bool
	}{
		{[]string{"a.txt"}, []string{"a.txt"}, true},
		{[]string{"a.txt"}, []string{"b.txt"}, false},
		{[]string{"src/"}, []string{"src/x/y.go"}, true},
		{[]string{"src/x/y.go"}, []string{"src/"}, true},
		{[]string{"src/x"}, []string{"src/xerox"}, false},
		{[]string{}, []string{"anything"}, true},
		{[]string{"*"}, []string{"anything"}, true},
		{[]string{"src/"}, []string{"docs/"}, false},
		{[]string{"a/b/c"}, []string{"a/b"}, true},
	}
	for i, tc := range cases {
		if got := ScopesOverlap(tc.a, tc.b); got != tc.want {
			t.Errorf("case %d: ScopesOverlap(%v, %v) = %v, want %v", i, tc.a, tc.b, got, tc.want)
		}
	}
}
