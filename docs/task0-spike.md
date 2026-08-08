# Task 0 — OpenCode transport spike: findings and verdict

Status: **GO** — transport contract proven viable for the corral scheduler.

## Acceptance criteria

| Criterion | Result | Evidence |
|---|---|---|
| Launch 3 independent sessions concurrently | PASS | 3 sessions created + prompted in parallel; peak concurrent busy = 3 in a single status poll |
| Receive events and terminal states | PASS | SSE delivers `session.status` (busy/idle/retry), `session.idle`, `message.updated`, `message.part.updated` (text/tool/patch/reasoning/step-start/step-finish), `session.diff`, `session.updated`, `session.error` |
| Cancel one session | PASS | `POST /session/:id/abort` → `session.error` with `MessageAbortedError`, then `session.status: idle`; killed assistant message has `finish: null` + `error`; partial file proves mid-run kill (beta.txt stopped at < 30 lines) |
| Restart controller and reconcile remaining sessions | PASS | Fresh client re-derived per-session state from `GET /session`, `/session/status`, `/session/:id/message`; exactly 1 completed run for done sessions, 1 aborted run for the killed session, no duplicate completion |
| Capture session IDs, messages, diffs | PASS | Session IDs + full transcripts + git-format patch diffs captured from message summaries |
| Automated integration test | PASS | `go test ./internal/spike -run TestTransportSpike` (18-22s, stable across 4 runs) |

## Verified wire contract (OpenCode 1.18.14)

- Sessions are **project-scoped**: `POST /session?directory=<proj>`. One worktree per project — **all sessions in a project share the same worktree** (see risks).
- Async prompting: `POST /session/:id/prompt_async` → `204`, non-blocking.
- SSE stream: `GET /global/event`. **Every frame is wrapped**: `{"directory", "project", "payload": {id, type, properties}}` — the payload wrapper is easy to miss (this spike's first bug). `server.heartbeat` ~2-5s; first frame `server.connected`.
- Terminal detection: `session.status {busy|idle|retry}` + `session.idle`. `session.idle` fires even for aborted sessions (twice sometimes).
- **`GET /session/status` omits idle sessions from the map.** A missing entry means "idle *or not yet started*" — a fresh prompt must first be observed busy before missing = idle is trustworthy (spike bug #2).
- Cancellation: `POST /session/:id/abort` → `200`. Aborted attempt = assistant message with `finish: null` + `error: MessageAbortedError`; `session.error` event carries the same.
- Diffs: authoritative source is the **user-message `summary.diffs`** (`{file, patch, additions, deletions, status}`, git patch format). `GET /session/:id/diff` returns `[]` after completion — do not rely on it.
- The stream also carries **`sync` events** (`type: "sync"`, `syncEvent: {seq, aggregateID}`) — a monotonic, per-aggregate durable event log. Promising as a replay cursor for Task 2/3; no REST endpoint to fetch missed sync events was found, so gap recovery must use message/status polling.
- `file.edited` / `file.watcher.updated` fire for **agent tool edits only, not raw bash writes** — unusable as a generic write trigger.
- Tool lifecycle visible in `message.part.updated`: `tool` parts with `state {pending → running → completed}` + `input`/`output` — usable for live progress and evidence capture.

## Risks and design notes for Tasks 1-3

1. **Shared worktree per project.** Two corral sessions in the same project see each other's file edits; diff summaries are project-wide, not session-scoped. Task 5's separate-worktree-per-worker design is therefore required, and the adapter must be told which worktree a session runs in.
2. **Model latency is variable** (5-40s of reasoning before the first tool call). Abort triggers must key off observed execution state (tool part running), not wall-clock.
3. **Idle ≠ done.** `session.idle`/missing status is a liveness signal only. Completion requires evidence: assistant `finish: "stop"` + user-message diff summary. Matches the plan's "session.idle never means success".
4. **Status polling fallback works** but only for busy/retry; pairing poll-till-busy with event idle is the reliable pattern.
5. **Event gap on disconnect**: no replay endpoint for sync events; recovery = re-poll sessions/messages/status and reconcile (proven in phase 6/7 of the spike).
6. `opencode serve` startup can take > 30s cold (plugin load); health-check with retries before connecting (spike bug #3).

## Artifacts

- `internal/ocx` — transport client (client.go, events.go, types.go) to be hardened into the Task 3 adapter contract.
- `internal/spike` — scenario engine + integration test + embedded server launcher.
- `cmd/spike` — human-readable runner (`go run ./cmd/spike`).
- Run: `go test ./internal/spike -run TestTransportSpike -v`

**Go/no-go:** GO. Proceed to Task 1 (graph contract + state machine) with `internal/ocx` as the basis for the OpenCode adapter.
