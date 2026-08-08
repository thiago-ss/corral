# Task 7 — Companion TUI

Status: **DONE** — a bubbletea dashboard over the daemon API with runs
list, DAG view, attempts/evidence/session/worktree inspection, and full
node actions. The OpenCode plugin supplies the in-editor controls; the TUI
is the companion observability surface (no OpenCode fork needed).

## Deliverables

- `internal/tui/api.go` — HTTP client + DTOs over the daemon endpoints
  (`ListRuns`, `GetRun`, `Approve`, `Reject`, `Cancel`, `Retry`, `Steer`).
- `internal/tui/model.go` — bubbletea model. All state changes happen in
  `Update` (tick-driven fetch, keys), so it is fully testable without a
  terminal. Modes: list → detail → inspect → steer. Keys:
  - list: `↑/↓` (or j/k), `enter` detail, `q` quit
  - detail: `↑/↓` node, `a` approve, `r` reject, `c` cancel, `t` retry,
    `s` steer (typed message, enter sends), `i` inspect, `esc` back
  - inspect: attempts (status, session, worktree, elapsed, cost/tokens,
    evidence), `esc` back
- `internal/tui/view.go` — lipgloss rendering:
  - runs list: id, status, per-node state chips (`w1:done gate:running`)
  - DAG view: nodes sorted with dependency arrows (`← dep (state)`),
    priority, attempt counts, types; color-coded states (done green,
    running blue, blocked amber, failed red, pending muted)
  - inspect: objective, role, write scope, verification, attempt rows
    with session/worktree paths and evidence snippets
  - status line with keybinding help, last action, and errors
- `cmd/corral tui` — connects to `CORRAL_DAEMON_URL` (default
  `http://127.0.0.1:4519`) with optional bearer key, alt-screen program.

## Acceptance mapping

| Criterion | Evidence |
|---|---|
| Priority and status list | list view renders per-node state chips; priority shown in detail (`p50`) |
| DAG/dependency view | detail renders nodes + `← dep (state)` arrows |
| Active agent/session/worktree | inspect shows session id + worktree path per attempt |
| Attempts, elapsed time, budget, evidence | inspect rows: attempt #, status, elapsed, cost/tokens, evidence snippet (budget visible in run status) |
| Inspect, steer, retry, cancel actions | `TestNodeActions` drives every key path through the model; `TestClientAgainstDaemon` round-trips all actions against a live daemon |

## Notes

- The model executes commands like the tea runtime (`send` helper in
  tests) so no terminal is needed for coverage.
- Actions refresh immediately after execution; the view polls every 1s.
- TUI talks only to the daemon API (role `operator`); the daemon enforces
  authorization.
