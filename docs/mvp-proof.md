# MVP proof

Status: **PASSED** — the five-node demonstration pipeline runs end-to-end
through a real OpenCode server with durable persistence, isolation and
approval-gated merging.

## The demo

`internal/daemon` E2E (and `internal/ocxadapter`/`internal/sched` suites)
exercise the plan's five-node proof:

1. **Planner produces an approved graph** — `OpenCodePlanner` turns a goal
   into a validated graph (live: `w1 → c1 → gate → merge`, normalization
   recovers model drift); review via `GET /api/runs/{id}`; run starts only
   via the API (orchestrator role).
2. **Two workers execute concurrently in separate worktrees** — each
   attempt gets its own `git worktree` + branch; sessions bind to them;
   the main checkout stays clean (`git status` asserted during the run).
3. **One worker fails verification and retries** — command gates (grep
   markers) reject/accept deterministically; focused feedback flows into
   the next attempt; retry counts are bounded.
4. **Reviewer validates both outputs** — check nodes run their gates
   inside the worker's worktree; merge runs only after every dependency
   is done.
5. **Merge waits for approval** — the human gate parks in `running`;
   `corral_approve` → merge commits + no-ff merges branches into main,
   runs its own verification, prunes consumed worktrees; rejection blocks
   the merge and the run settles `waiting`.

**Scheduler restarts mid-run without duplicate execution** — crash/restart
test: completed nodes never re-run (attempt counts stay 1), interrupted
attempts resume exactly once, the event log replays to the exact
materialized state.

**Final tests pass; event replay shows complete provenance** — every
transition/attempt/verdict/retry is an event with a monotonic per-run
sequence; the audit export (`corral export`) contains graph, states,
events, attempts (with server/session/worktree/branch ids) and
content-addressed diff artifacts, with secrets redacted at the store.

## Where each proof element lives

| proof | test |
|---|---|
| planner → graph | `TestPlannerSmoke` (live), `TestParseGraphFromResponse` |
| approval-gated start | `TestRoleEnforcement`, `TestDaemonEndToEndRealOpenCode` |
| parallel workers, isolated | `TestOpenCodeWorktreeMerge` (real), `TestWorkersIsolatedAndMainUntouched` |
| fail → retry → pass | `TestFailThenPassAfterRetryWithFeedback`, `TestOpenCodeEvidenceGates` |
| reviewer/checks gate | `TestMergeRunsOnlyAfterApproval`, `TestRejectedGateBlocksMerge` |
| restart without duplicates | `TestCrashRestartNoDuplicateAttempts`, `TestLoadRecoversVerifyingNode` |
| provenance | `TestAuditExport`, `TestSixNodeForkJoinDeterministic` (replay equality) |

## Out of scope (deferred)

Distributed workers, autonomous deployment, graph cycles, knowledge
graphs, automatic graph mutation, human-in-the-loop merge UI beyond the
TUI/plugin actions.
