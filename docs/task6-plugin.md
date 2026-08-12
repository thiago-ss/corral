# Task 6 — OpenCode plugin and agent roles

Status: **DONE** — users drive runs from inside OpenCode through a thin
plugin; the daemon enforces role-based controls server-side; the full
flow is verified end-to-end against a real OpenCode server.

## Deliverables

- `internal/daemon` — HTTP control API (`corral daemon`):
  - `POST /api/plan` — goal → validated graph via a read-only planner
    agent session. The planner prompts with the graph schema + a concrete
    example, and `extractGraph` **normalizes** model drift (goal/action →
    objective, check/command → verification, worker/reviewer types →
    agent roles, default acceptance criteria) before `graph.Validate`.
  - `POST /api/runs` — start a run from a graph; run loops live on the
    daemon context (not request context — bug found and fixed) and
    persist via SQLite. Accepts `autoApproveGates` (stored on the run and
    exposed by `GET /api/runs/{id}`); gates remain explicit, and the flag
    authorizes the orchestrator to call the normal approval endpoint.
  - `GET /api/runs`, `GET /api/runs/{id}` — follow execution (states,
    attempts, event log).
  - `GET /api/runs/{id}/watch` — Server-Sent Events stream of run deltas
    (node transitions, gates awaiting approval, run done) from an `after`
    cursor; powers `corral_watch`.
  - `approve` / `reject` / `cancel` / `retry` / `steer` per node —
    including `RetryNode` (blocked→ready, retry_wait→ready, failed→ready
    operator override with retry budget reset) and run-loop restart after
    un-settling a settled run.
  - Role enforcement: `X-Corral-Role` header; create/approve/cancel/
    retry/steer require operator or orchestrator; plan requires
    planner/orchestrator; workers/reviewers/mergers are read-only.
    Optional bearer-token auth.
  - `Resume` rehydrates active/waiting runs on daemon restart.
- `cmd/corral` — `corral daemon [--port 4519] [--key TOKEN]`: embeds
  `opencode serve`, wires store + adapter + worktrees + verifier.
- `.opencode/tools/corral.ts` — the thin plugin: `corral_plan`,
  `corral_start` (accepts the raw graph *or* the full `corral_plan` output,
  unwrapping a leading `{"graph": ...}` wrapper, plus an optional
  `autoApproveGates` flag), `corral_status`, `corral_watch` (blocks on the
  daemon SSE stream and returns the first run delta — node transition, gate
  awaiting approval, or run done — or times out), `corral_approve`,
  `corral_reject`, `corral_cancel`, `corral_retry`, `corral_steer`, calling
  the daemon and mapping the session agent (`corral-*`) to a role.
- `example/opencode.json` — agent role configuration using OpenCode's
  per-agent permissions: orchestrator (deny edit/bash, allow corral_*),
  planner (read-only + corral_plan), worker (ask edits/bash), reviewer
  (deny edit; bash allow only `git diff/status/log`, tests), merger (deny
  edit; bash allow only `git status/log/diff`, ask merge/checkout/branch).

## Acceptance verification

| Criterion | Evidence |
|---|---|
| corral_plan..corral_steer exposed | plugin tools + daemon endpoints; all exercised via API tests |
| Roles via per-agent permissions | `example/opencode.json` (validated JSON); server-side role gates tested (worker/planner create → 403, orchestrator → 201, unknown role → 403, auth 401/ok) |
| Submit goal, review graph, approve, follow execution — no DB/daemon internals | `TestDaemonEndToEndRealOpenCode`: graph submitted via API, gate observed running via API, main checkout verified clean while isolated, approve via API, merge completes, content merged, run `done:true` |
| Planner works | `TestPlannerSmoke` (live): 4-node graph `w1 → c1 → gate → merge` produced and validated; unit tests for extraction/normalization/JSON-end finding |

## Notes

- Planner smoke is tolerant: if the model fails to emit a parseable graph
  it logs and skips (nondeterministic LLM output); normalization keeps
  drift recoverable.
- The plugin's role fallback is `operator` for unknown agents (human).
- `steer` sends a message into the running session (agent sees it as a
  follow-up instruction).
- Run: `go test ./internal/daemon -v` (includes the real-OpenCode E2E).
