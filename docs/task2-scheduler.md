# Task 2 — Durable scheduler with fake agents

Status: **DONE** — foundation checkpoint reached: core tests pass, event
replay reproduces graph state, crash/restart test passes.

## Deliverables

- `internal/clock` — `Clock` abstraction: `Real` (wall time) and `Fake`
  (manual `Advance`, deterministic timers).
- `internal/store` — SQLite (WAL, `modernc.org/sqlite`, pure Go):
  - `runs` (graph JSON, status), `events` (monotonic per-run `seq` log),
    `nodes` (materialized states, retry budget, leases), `attempts`
    (unique per `(run, node, no)`).
  - Event types: `transition`, `recovery`, `attempt`, `verdict`, `retry`,
    `run`, `graph`.
  - `AcquireLease` is atomic on the holder (`leased_by`/`leased_until`);
    expired leases are re-acquirable. `ReleaseLease` is holder-scoped,
    `ClearLease` is for crash recovery.
- `internal/sched` — the control loop:
  - One deterministic `Step()` per tick; all state mutation happens inside
    it (cooperative simulation with the fake driver, async results channel
    for real drivers).
  - Concurrency limit, priority ordering with **aging** (boost per
    saturated step, capped; only waiting nodes accrue).
  - Retry policy: `verifying → retry_wait → ready` on failed evidence while
    retries remain; `verifying → failed` when exhausted.
  - Time budget: deadline exceeded → driver abort → `running → failed`.
  - Restart (`Load`): events replayed into a fresh `Tracker` (validating
    the machine trace), interrupted attempts marked, leases cleared, nodes
    restored to `ready` — exactly one re-execution each; completed nodes
    are never re-run.
- `internal/sched/fake.go` — `FakeDriver` (scripted sessions, deterministic
  sorted completions, abort support) and `FakeVerifier` (scripted verdicts).

## Acceptance verification

| Criterion | Evidence |
|---|---|
| Fan-out executes concurrently | Six-node fork/join: b and c both leased before either completes |
| Fan-in waits for every predecessor | f leased only after d **and** e done |
| Priority + aging prevents starvation | Chain-arrival test: low-priority `lo` runs last without aging, before `h6` with aging (boost 20/tick) |
| Restart resumes without duplicate attempts | Crash after a done + b,c running: a,d run once; b,c exactly once more (interrupted + resume); replay ≡ materialized state |
| Deterministic simulation | All tests use the fake clock; fork/join completion order stable across `-count=10` |
| Leases | Single-holder atomicity, expiry reuse, holder-scoped release (store tests) |

## Notes

- `ComputeReady` treats `pending` and `ready` as schedulable (retry
  restores nodes to `ready`).
- Recovery events are applied with `Tracker.Set` (not `Transit`), keeping
  the transition log strictly legal.
- Aging state is in-memory (resets on restart); recorded as acceptable
  trade-off for Task 2.
- Run status persists (`active` → `completed`) with an `EventRun` marker.
