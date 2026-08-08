# Task 8 — Hardening and packaging

Status: **DONE** — all acceptance criteria covered with tests.

## Deliverables

- **Permission requests → explicit blocked state**
  - `adapter.PermissionSession` (optional interface): `PendingPermission`,
    `RespondPermission`.
  - `ocxadapter` tracks `permission.updated` events per session and answers
    via `POST /session/:id/permissions/:permissionID`.
  - Scheduler: a pending permission moves the node `running → blocked`
    (payload carries `permissionID`) and the session is *suspended* — its
    eventual completion still resolves the attempt (machine walk
    `blocked→ready→leased→running→verifying`). Resolved permissions
    resume automatically (run re-activates if it had settled); budget
    deadline is re-armed with remaining time.
  - Daemon: `POST /api/runs/{id}/permission {nodeID, permissionID, allow}`.
- **Budgets and circuit breakers**
  - Run-level budget: `RunMaxTokens` / `RunMaxCost` — accumulated per
    finished attempt; once exceeded, pending nodes block
    (`{"reason":"run budget exceeded"}`).
  - Circuit breaker: `BreakerMaxFailures` within `BreakerWindow` —
    tripped, it blocks pending nodes (`{"reason":"circuit breaker"}`);
    operator retry resets it. Checks run after completions drain, before
    new work starts (fixed ordering bug where new nodes could start the
    step after a budget/breaker trip).
  - Per-node time/token/cost budgets from Tasks 2/4 unchanged.
- **Secrets hygiene**: `store.Redact` strips bearer tokens, api keys,
  passwords/secrets/tokens and `sk-…` patterns at the persistence
  boundary (attempt evidence, event payloads, artifact content). Verified
  end-to-end: nothing round-trips into the DB.
- **corral doctor**: checks opencode version (≥1.18.0), git repository,
  daemon health, plugin presence, config; exits non-zero on problems.
- **OpenAPI contract**: `GET /doc` serves the daemon OpenAPI document;
  `TestOpenAPIContract` asserts every registered route is documented and
  live responses validate against the document schemas (jsonschema).
- **One-command init**: `corral init` verifies the git repo, creates
  `.corral/`, generates a 0600 API key (idempotent), writes config, and
  prints next steps.
- **Audit export**: `GET /api/runs/{id}/export` and `corral export
  <runID> [--out file]` — full provenance (graph, states, event log,
  attempts with sessions/worktrees/branches, content-addressed
  artifacts), redacted at the store.

## Acceptance verification

| Criterion | Evidence |
|---|---|
| Permission requests → explicit blocked | `TestPermissionRequestBlocksExplicitly` (blocked + suspended + auto-resume + single done attempt); `TestPermissionThroughAPI` via daemon endpoint |
| Run/node budgets + circuit breakers | `TestCircuitBreakerStopsNewWork` (z never ran, blocked), `TestRunBudgetBlocksNewWork` (n2 blocked, 0 attempts), existing node budgets |
| OpenCode version checked by doctor | `TestDoctorPassesWithDaemonUp` / `TestDoctorReportsProblems` (opencode check line; version parsing unit test) |
| API contract vs OpenAPI | `TestOpenAPIContract` (routes ⊆ doc, live responses schema-validated) |
| Secrets excluded from logs/artifacts | `TestRedact` + `TestSecretsNeverPersisted` (evidence/artifact/event all clean) |
| One-command local initialization | `TestInitCmd` (key format 0600, idempotent), `TestInitFailsOutsideGit` |
| Full audit export | `TestAuditExport` (events, attempts, artifacts w/ hashes, states, graph) |

## Notes

- `corral doctor` runs checks against the live environment; a down
  daemon or missing plugin is reported (exit 1).
- The plugin + `example/opencode.json` are the OpenCode-facing half of
  the loop (Task 6); Task 8 adds the ops tooling around the daemon.
