// Package worktree isolates writable node execution from the main
// checkout: every writing node runs in its own git worktree on its own
// branch, diffs are captured as content-addressed artifacts, and merge
// nodes fold branches back into the main checkout.
package worktree

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
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

// gitExit runs git like git but returns the exit code instead of wrapping
// non-zero exits as errors (for commands whose exit code is meaningful,
// e.g. merge-base --is-ancestor).
func (m *Manager) gitExit(ctx context.Context, dir string, args ...string) (string, int, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), 0, nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return string(out), ee.ExitCode(), nil
	}
	return "", -1, err
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
	// Determine the no-op case explicitly. A non-zero commit can also mean
	// a hook, signer, or filesystem failure; those errors must reach the
	// caller so the staged work can be retried.
	out, code, err := m.gitExit(ctx, worktree, "diff", "--cached", "--quiet", "--exit-code", "HEAD", "--")
	if err != nil {
		return err
	}
	switch code {
	case 0:
		return nil
	case 1:
		// Staged changes exist; commit them below.
	default:
		return fmt.Errorf("inspect staged work: git diff exited %d: %s", code, tail([]byte(out), 400))
	}
	if _, err := m.git(ctx, worktree, "-c", "user.name=corral", "-c", "user.email=corral@local", "commit", "-q", "-m", "corral: work"); err != nil {
		return err
	}
	return nil
}

// MergeBranch fast-ensures a branch is merged into the main checkout with
// a non-fast-forward merge commit. The merge commit uses the corral
// identity so machines without a global git identity still work.
func (m *Manager) MergeBranch(ctx context.Context, branch string) error {
	main, err := m.MainBranch(ctx)
	if err != nil {
		return err
	}
	if _, err := m.git(ctx, m.repo, "checkout", "-q", main); err != nil {
		return err
	}
	_, err = m.git(ctx, m.repo,
		"-c", "user.name=corral", "-c", "user.email=corral@local",
		"merge", "--no-ff", "-m", "corral: merge "+branch, branch)
	if err != nil {
		mergeErr := fmt.Errorf("merge %s: %w", branch, err)
		if abortErr := m.abortMerge(); abortErr != nil {
			return fmt.Errorf("%w; abort failed: %v", mergeErr, abortErr)
		}
		return mergeErr
	}
	return nil
}

// abortMerge restores the main checkout after a merge that entered merge
// state and then failed (for example, a conflict or merge-commit hook).
// Cleanup uses its own bounded context so caller cancellation cannot strand
// the repository in an active merge.
func (m *Manager) abortMerge() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, code, err := m.gitExit(ctx, m.repo, "rev-parse", "--verify", "--quiet", "MERGE_HEAD")
	if err != nil {
		return err
	}
	if code != 0 {
		return nil
	}
	_, err = m.git(ctx, m.repo, "merge", "--abort")
	return err
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

// Remove deletes a clean worktree. Any tracked, untracked, or ignored content
// keeps the worktree for inspection, even after its branch was merged.
func (m *Manager) Remove(ctx context.Context, branch string) error {
	path := filepath.Join(m.dir, branch)
	if _, err := os.Stat(path); err != nil {
		return nil // already gone
	}
	status, err := m.git(ctx, path, "status", "--porcelain=v1", "--untracked-files=normal", "--ignored=matching")
	if err != nil {
		return err
	}
	if strings.TrimSpace(status) != "" {
		return fmt.Errorf("refuse to remove dirty worktree %s", path)
	}
	if _, err := m.git(ctx, m.repo, "worktree", "remove", path); err != nil {
		return err
	}
	_, _ = m.git(ctx, m.repo, "branch", "-D", branch)
	return nil
}

// WorktreeInfo describes one attempt worktree owned by this manager.
type WorktreeInfo struct {
	Path     string
	Branch   string // branch name without the refs/heads/ prefix
	Head     string // full commit hash the worktree is on
	Mtime    time.Time
	Locked   bool
	Detached bool
	Orphaned bool // admin entry whose working directory is already gone
	Dirty    bool // tracked, staged, untracked, or ignored content not recorded in HEAD
}

// List returns the attempt worktrees registered under the manager's
// sibling .corral-worktrees directory. Entries come from `git worktree
// list --porcelain`; the main checkout is never included. Mtime is the
// most recent change to the worktree directory or its gitdir HEAD/index.
func (m *Manager) List(ctx context.Context) ([]WorktreeInfo, error) {
	out, err := m.git(ctx, m.repo, "worktree", "list", "--porcelain")
	if err != nil {
		return nil, err
	}
	var infos []WorktreeInfo
	for _, block := range strings.Split(out, "\n\n") {
		info, ok := parseWorktreeBlock(block)
		if !ok || !insideDir(m.dir, info.Path) {
			continue
		}
		info.Mtime = lastActivity(info.Path)
		// Ignored files still carry user data. Treat them as dirty so automatic
		// pruning never deletes content merely because .gitignore hides it.
		status, err := m.git(ctx, info.Path, "status", "--porcelain=v1", "--untracked-files=normal", "--ignored=matching")
		if err != nil {
			return nil, err
		}
		info.Dirty = strings.TrimSpace(status) != ""
		infos = append(infos, info)
	}
	return infos, nil
}

func parseWorktreeBlock(block string) (WorktreeInfo, bool) {
	var info WorktreeInfo
	for _, line := range strings.Split(block, "\n") {
		switch {
		case strings.HasPrefix(line, "worktree "):
			info.Path = strings.TrimSpace(strings.TrimPrefix(line, "worktree "))
		case strings.HasPrefix(line, "HEAD "):
			info.Head = strings.TrimSpace(strings.TrimPrefix(line, "HEAD "))
		case strings.HasPrefix(line, "branch refs/heads/"):
			info.Branch = strings.TrimPrefix(line, "branch refs/heads/")
		case line == "detached":
			info.Detached = true
		case line == "locked" || strings.HasPrefix(line, "locked "):
			info.Locked = true
		case strings.HasPrefix(line, "prunable"):
			info.Orphaned = true
		}
	}
	return info, info.Path != ""
}

// insideDir reports whether path lives strictly below dir. Both sides are
// resolved through symlinks because git reports canonical paths while the
// caller may hold a symlinked one (e.g. /var vs /private/var on macOS).
func insideDir(dir, path string) bool {
	dir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return false
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(dir, path)
	if err != nil {
		return false
	}
	return rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// lastActivity returns the most recent modification time among the
// worktree directory and its gitdir HEAD/index files — a proxy for the
// last time anything changed in the worktree.
func lastActivity(path string) time.Time {
	var latest time.Time
	if fi, err := os.Stat(path); err == nil {
		latest = fi.ModTime()
	}
	if gd := worktreeGitDir(path); gd != "" {
		for _, f := range []string{"HEAD", "index"} {
			if fi, err := os.Stat(filepath.Join(gd, f)); err == nil && fi.ModTime().After(latest) {
				latest = fi.ModTime()
			}
		}
	}
	return latest
}

// worktreeGitDir resolves the git directory backing a linked worktree by
// reading its .git file ("gitdir: <path>"). Empty when unavailable.
func worktreeGitDir(path string) string {
	data, err := os.ReadFile(filepath.Join(path, ".git"))
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if line, ok := strings.CutPrefix(line, "gitdir: "); ok {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

// BranchMerged reports whether branch has been folded into the main
// checkout branch with its own commits, or no longer exists. A branch
// still pointing at the main tip is not "merged": its worktree may hold
// uncommitted work. Neither check touches the main checkout.
func (m *Manager) BranchMerged(ctx context.Context, branch string) (bool, error) {
	if _, code, err := m.gitExit(ctx, m.repo, "rev-parse", "--verify", "--quiet", "refs/heads/"+branch); err != nil {
		return false, err
	} else if code != 0 {
		return true, nil // branch removed; its worktree is orphaned
	}
	main, err := m.MainBranch(ctx)
	if err != nil {
		return false, err
	}
	branchTip, err := m.git(ctx, m.repo, "rev-parse", "--verify", "refs/heads/"+branch)
	if err != nil {
		return false, err
	}
	mainTip, err := m.git(ctx, m.repo, "rev-parse", "--verify", "refs/heads/"+main)
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(branchTip) == strings.TrimSpace(mainTip) {
		return false, nil // no unique commits; uncommitted work may still live in the worktree
	}
	_, code, err := m.gitExit(ctx, m.repo, "merge-base", "--is-ancestor", "refs/heads/"+branch, "refs/heads/"+main)
	if err != nil {
		return false, err
	}
	return code == 0, nil
}

// Prune removes clean worktrees that are safe to drop: their branch has
// been merged into the main checkout branch or no longer exists, or (when
// staleAfter > 0) their last activity is older than staleAfter. Dirty,
// locked, and detached worktrees are always skipped; orphaned git admin
// entries are pruned first via `git worktree prune`. The main checkout is
// never touched. Returns the paths removed.
func (m *Manager) Prune(ctx context.Context, staleAfter time.Duration, now time.Time) ([]string, error) {
	if _, err := m.git(ctx, m.repo, "worktree", "prune"); err != nil {
		return nil, err
	}
	infos, err := m.List(ctx)
	if err != nil {
		return nil, err
	}
	var pruned []string
	for _, info := range infos {
		if info.Dirty || info.Locked || info.Detached || info.Orphaned {
			continue
		}
		merged, err := m.BranchMerged(ctx, info.Branch)
		if err != nil {
			return pruned, err
		}
		stale := staleAfter > 0 && now.Sub(info.Mtime) > staleAfter
		if !merged && !stale {
			continue
		}
		if err := m.removeInfo(ctx, info, merged); err != nil {
			return pruned, fmt.Errorf("prune %s: %w", info.Path, err)
		}
		pruned = append(pruned, info.Path)
	}
	return pruned, nil
}

// removeInfo removes a worktree by path. A merged branch is deleted too;
// an unmerged stale branch is kept so its commits stay recoverable.
func (m *Manager) removeInfo(ctx context.Context, info WorktreeInfo, merged bool) error {
	// No --force here: if content appears after List's safety check, Git must
	// refuse instead of deleting an agent's newly-written work.
	if _, err := m.git(ctx, m.repo, "worktree", "remove", info.Path); err != nil {
		return err
	}
	if merged && info.Branch != "" {
		_, _ = m.git(ctx, m.repo, "branch", "-D", info.Branch)
	}
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
