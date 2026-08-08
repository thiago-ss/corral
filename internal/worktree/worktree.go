// Package worktree isolates writable node execution from the main
// checkout: every writing node runs in its own git worktree on its own
// branch, diffs are captured as content-addressed artifacts, and merge
// nodes fold branches back into the main checkout.
package worktree

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Manager owns worktrees for one repository.
type Manager struct {
	repo string // main checkout
	dir  string // parent directory holding worktrees
}

func NewManager(repo string) *Manager {
	return &Manager{
		repo: repo,
		dir:  filepath.Join(filepath.Dir(repo), filepath.Base(repo)+".corral-worktrees"),
	}
}

// Repo returns the main checkout path.
func (m *Manager) Repo() string { return m.repo }

// NodeIsWriting decides isolation from a node's type and role.
func NodeIsWriting(nodeType, role string) bool {
	switch nodeType {
	case "agent":
		return role == "" || role == "worker"
	case "check", "merge", "human_gate":
		return false
	}
	return false
}

func (m *Manager) git(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %v: %w: %s", args, err, tail(out, 400))
	}
	return string(out), nil
}

// Add creates a worktree for branch at a fresh path, based on the main
// checkout HEAD.
func (m *Manager) Add(ctx context.Context, branch string) (string, error) {
	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(m.dir, branch)
	if _, err := os.Stat(path); err == nil {
		return "", fmt.Errorf("worktree %s already exists", path)
	}
	if _, err := m.git(ctx, m.repo, "worktree", "add", "-b", branch, path, "HEAD"); err != nil {
		return "", err
	}
	return path, nil
}

// Diff computes the working-tree diff against HEAD (patch format).
// Untracked files are included via intent-to-add.
func (m *Manager) Diff(ctx context.Context, worktree string) (patch string, err error) {
	if _, err := m.git(ctx, worktree, "add", "-A", "--intent-to-add"); err != nil {
		return "", err
	}
	return m.git(ctx, worktree, "diff", "HEAD")
}

// Files lists changed file paths in a worktree vs HEAD.
func (m *Manager) Files(ctx context.Context, worktree string) ([]string, error) {
	out, err := m.git(ctx, worktree, "diff", "--name-only", "HEAD")
	if err != nil {
		return nil, err
	}
	var files []string
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		if l != "" {
			files = append(files, l)
		}
	}
	return files, nil
}

// HashContent returns the content address of a patch.
func HashContent(content string) string {
	h := sha256.Sum256([]byte(content))
	return hex.EncodeToString(h[:])
}

// CommitWorktree commits all worktree changes on its branch.
func (m *Manager) CommitWorktree(ctx context.Context, worktree string) error {
	if _, err := m.git(ctx, worktree, "add", "-A"); err != nil {
		return err
	}
	if _, err := m.git(ctx, worktree, "-c", "user.name=corral", "-c", "user.email=corral@local", "commit", "-q", "-m", "corral: work"); err != nil {
		// Nothing to commit is fine.
		return nil
	}
	return nil
}

// MergeBranch fast-ensures a branch is merged into the main checkout with
// a non-fast-forward merge commit.
func (m *Manager) MergeBranch(ctx context.Context, branch string) error {
	main, err := m.MainBranch(ctx)
	if err != nil {
		return err
	}
	if _, err := m.git(ctx, m.repo, "checkout", "-q", main); err != nil {
		return err
	}
	out, err := m.git(ctx, m.repo, "merge", "--no-ff", "-m", "corral: merge "+branch, branch)
	if err != nil {
		return fmt.Errorf("merge %s: %w: %s", branch, err, tail([]byte(out), 400))
	}
	return nil
}

// MainBranch returns the currently checked-out branch of the main repo.
func (m *Manager) MainBranch(ctx context.Context) (string, error) {
	out, err := m.git(ctx, m.repo, "branch", "--show-current")
	if err != nil {
		return "", err
	}
	b := strings.TrimSpace(out)
	if b == "" {
		return "", fmt.Errorf("main checkout is not on a branch")
	}
	return b, nil
}

// Remove deletes a worktree. Failed worktrees are kept for inspection by
// design; callers invoke Remove only after successful merge/cleanup.
func (m *Manager) Remove(ctx context.Context, branch string) error {
	path := filepath.Join(m.dir, branch)
	if _, err := os.Stat(path); err != nil {
		return nil // already gone
	}
	if _, err := m.git(ctx, m.repo, "worktree", "remove", "--force", path); err != nil {
		return err
	}
	_, _ = m.git(ctx, m.repo, "branch", "-D", branch)
	return nil
}

// ScopesOverlap reports whether two declared write scopes can conflict.
// Empty or "*" scopes mean the whole repository (a writing node with no
// declared scope collides with everything). Paths overlap when one is a
// prefix of the other at a path boundary.
func ScopesOverlap(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return true
	}
	for _, x := range a {
		for _, y := range b {
			if scopesTouch(x, y) {
				return true
			}
		}
	}
	return false
}

func scopesTouch(x, y string) bool {
	x = strings.TrimSpace(x)
	y = strings.TrimSpace(y)
	if x == "" || y == "" || x == "*" || y == "*" {
		return true
	}
	x = filepath.Clean(x)
	y = filepath.Clean(y)
	if x == "." || y == "." {
		return true
	}
	if x == y {
		return true
	}
	if strings.HasPrefix(x, y+string(filepath.Separator)) {
		return true
	}
	if strings.HasPrefix(y, x+string(filepath.Separator)) {
		return true
	}
	return false
}

func tail(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[len(b)-n:])
}
