package worktree

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
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
