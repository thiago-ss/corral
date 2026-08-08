# Task 1 — Graph contract and state machine

Status: **DONE** — all acceptance criteria verified by unit tests.

## Deliverables

- `internal/graph/graph.go` — typed schema: `Node` (objective, acceptance criteria, role/model, priority, dependencies, input/output artifacts, write scope, verification method, retry policy, time/token/cost budget), `Graph`, limits (`MaxNodes=100`, `MaxFanOut=8`).
- `internal/graph/validate.go` — rejects: cycles (deterministic DFS, sorted iteration), missing/duplicate/self dependencies, duplicate/empty IDs, agent nodes without acceptance criteria, excessive fan-out, invalid types, bad verification/budget/retry values.
- `internal/graph/state.go` — `Tracker` enforcing the plan's state machine:

  ```
  pending → ready → leased → running → verifying → done
             ├→ retry_wait → ready
             └→ blocked | failed | canceled
  ```

  Full legal-transition table; every other transition returns an error.
  `Set()` restores only recoverable states (pending/ready/blocked/terminal)
  for restart reconciliation.
- `internal/graph/ready.go` — `ComputeReady`: ready = pending + all deps done;
  blocked = pending nodes whose dep chain transitively contains
  failed/canceled. Deterministic order: priority desc, then node ID —
  stable across insertion orders.
- `internal/graph/proposal.go` — `Proposal` ops (add/remove node, add/remove
  edge, set priority/retry/budget), `ValidateProposal` (open to any agent),
  and `NewApplier` — the single gated application point keyed on
  `Identity.CanApplyGraphChanges()`. Agents can propose; only the scheduler
  identity applies.
- `internal/adapter/adapter.go` — generic executor contract (frozen):
  `Attempt`, `Session`, `Driver`, `Status`, `Message`, `Diff`, `Event`.
  OpenCode implementation lands in Task 3.

## Notes

- Done is not a blocker: `Terminal()` includes done, so readiness logic
  distinguishes done from failed/canceled explicitly.
- `retry_wait → failed` is legal (supersede/cancel during backoff), and
  `verifying → failed` carries permanent failure after retries exhaust.
- Graph mutations re-validate the whole graph before committing (clone,
  validate, swap, version++).
