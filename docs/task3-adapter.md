# Task 3 — OpenCode adapter

Status: **DONE** — the generic adapter contract is mapped onto real OpenCode
sessions; integration test covers parallel execution and cancellation.

## Deliverables

- `internal/ocxadapter` — `Driver` implementing `adapter.Driver` +
  `adapter.Stepper`:
  - `Start`: `POST /session` (title `corral/<node>`), async prompt with the
    node objective (+ role prefix, optional model override), records the
    OpenCode session; returns an `adapter.Session` handle.
  - Completion detection: a shared `/global/event` SSE stream (opened once
    per driver) dispatches `session.idle` / `session.error` to the owning
    attempt's event channel; a per-attempt poller (`PollInterval`, default
    1s) queries `/session/status` + `/session/:id/message` as the
    reconciliation fallback when events are missed or the stream drops.
  - Exactly-once completion: `attempt.completed` guard (checked before the
    transcript fetch and again under the driver mutex before emission);
    duplicate events (e.g. repeated `session.idle`) cannot emit twice, and
    the scheduler asserts exactly one attempt row per node.
  - Terminal classification from the transcript: aborted flag or
    `MessageAbortedError` → `StatusAborted`; other assistant error →
    `StatusError`; `finish:"stop"` → `StatusIdle`; no finished message →
    still running (poll again).
  - `Abort` → `POST /session/:id/abort`; `Messages` → adapter messages
    with text parts, costs/tokens, and user-summary diffs (patch format).
- Contract additions (adapter v0.2): `adapter.Completion`,
  `adapter.Stepper`, `Session.ServerID()`.
- `sched`: drains `adapter.Stepper` into the results channel; records
  `ServerID` per attempt (`store.Attempt.ServerID`, new `server_id`
  column); new `RunHandle.CancelNode` (operator cancel → driver abort →
  `running → canceled`).

## Acceptance verification

| Criterion | Evidence |
|---|---|
| Start, stream, message, inspect, cancel sessions | Integration test: sessions created + prompted; SSE stream dispatches terminal events; transcripts fetched via `Messages`; `CancelNode` aborts mid-run |
| Record OpenCode server/session IDs per attempt | Asserted: `session_id` has `ses_` prefix, `server_id` == server base URL |
| Event stream + status polling fallback | Both paths implemented; polling is the completion path when events are dropped (same `maybeComplete` logic) |
| Duplicate/missing events cannot duplicate completion | Exactly 1 attempt row per node asserted; `completed` guard + scheduler drop of unknown/duplicate attempt results |
| Two-node parallel run + cancellation test | Both sessions observed busy concurrently (peak ≥ 2); w1 done with real file output; w2 canceled with `aborted` attempt |

## Notes

- `PromptAsync` gained an optional `model` parameter (`ocx`); spike call
  sites updated.
- Watchers exit via attempt cancel / driver `Close`; the shared stream
  goroutine runs until `Close`.
- Run: `go test ./internal/ocxadapter -run TestOpenCodeAdapterParallelAndCancel -v`
