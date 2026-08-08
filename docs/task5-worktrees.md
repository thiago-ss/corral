# Task 5 — Writable-work isolation

Status: **DONE** — every writing node runs in its own git worktree; write
scopes serialize conflicting writers; diffs become content-addressed
artifacts; failed worktrees are kept; merges require checks + human
approval. Verified deterministically and against a real OpenCode server.

## Deliverables

- `internal/worktree` — git worktree manager:
  - `Add`: branch-per-attempt worktree (`corral/<run>/<node>/<no>`), based
    at main HEAD, created in a sibling `.corral-worktrees` directory so
    the main checkout stays pristine.
  - `Diff` (with intent-to-add so untracked files appear), `Files`,
    `HashContent` (sha256 content addressing), `CommitWorktree`,
    `MergeBranch` (no-ff into main), `Remove`, `MainBranch`, `Repo`.
  - `ScopesOverlap`: path-boundary prefix semantics; empty/`*` scope
    collides with everything.
- `sched`:
  - Writing nodes (role `worker` or unlabeled agents) get a worktree per
    attempt; `Attempt.Cwd` and the attempt row record it (new
    `worktree`/`branch` columns).
  - Declared scope collisions defer scheduling: a candidate whose scope
    overlaps an active writing session stays ready (re-selected next
    step), enforced only when isolation is enabled.
  - On completion, the worktree diff is stored as a content-addressed
    artifact (attempt + node + hash + patch) — including failed attempts.
  - Check nodes run in the worktree of their completed writing dep
    (`checkCwd`), so checks verify the actual worker output.
  - `human_gate` nodes park in `running` until `ApproveNode` (→ done) or
    `RejectNode` (→ blocked); merge nodes never start before approval
    because their dependencies must be done.
  - Merge nodes commit + merge the branches of all transitively-completed
    writing deps into main, run their command verification in main, and
    prune consumed worktrees on success; merge conflicts surface as
    failed verification (retry per policy).
  - Verifier now receives the attempt's cwd so command gates run inside
    the worker's worktree.
- `graph`: merge and check nodes must declare a command verification.
- `graph/ready.go`: `blocked` deps now propagate to dependents (they can
  never complete until manually resolved) — fixes runs not settling after
  a rejected gate.
- `ocxadapter`: sessions bind to the attempt's directory (per-cwd client
  map), so real agents work inside worktrees.
- `store`: `worktree`/`branch` on attempts; `artifacts` table
  (content-addressed); fixed `ON CONFLICT` update paths silently
  clearing worktree/branch.

## Acceptance verification

| Criterion | Evidence |
|---|---|
| Every writing node gets a unique worktree | Real test: w1/w2 attempts record distinct worktrees; `TestWorkersIsolatedAndMainUntouched` |
| Write-scope collisions block scheduling | `TestScopeCollisionDefersConcurrentWriter`: w2 leased only after w1 done |
| Diff becomes content-addressed artifact | `TestDiffArtifactContentAddressed` (hash = sha256(patch)); real test asserts artifact rows |
| Failed worktree remains for inspection | `TestFailedWorktreeRetainedForInspection` |
| Merge runs only after checks pass + approval | `TestMergeRunsOnlyAfterApproval`: merge has 0 attempts before approval; after approval it folds the branch into main and prunes; `TestRejectedGateBlocksMerge`: rejection → gate+merge blocked, run settles `waiting` |
| Parallel workers don't corrupt main checkout | `TestOpenCodeWorktreeMerge` (real OpenCode): main checkout `git status` clean while workers run isolated; merged files present after approval |

## Notes

- Empty write scope on a writing node = whole repository (collides with
  everything); declare scopes to enable parallelism.
- Worktrees are pruned only on successful merge; failed/abandoned
  worktrees stay on disk for inspection (`git worktree prune` at run
  cleanup is future work).
- Retries allocate a fresh worktree per attempt (deterministic redo).
