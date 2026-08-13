# Changelog

All notable changes are documented here. Dates use ISO 8601 format.

## [Unreleased] — 2026-08-13

### Added

- Durable run-event streaming over Server-Sent Events, with reconnect-safe
  cursors and a live TUI dashboard for graph state, attempts, evidence,
  transcripts, budgets, permissions, and operator actions.
- Standalone Claude Code adapter with streamed transcripts, usage/cost
  accounting, scoped permission mediation, and abort/retry handling.
- Permission-aware scheduler state: pending provider permissions pause attempt
  budgets, move nodes to `blocked`, expose the requested tool/input to
  operators, and resume only after an explicit decision.
- Worktree inspection and cleanup improvements, including preservation of dirty
  worktrees and safe pruning of merged or stale clean worktrees.
- Workflow diagrams and a visual trust-loop/brand system in `docs/` and the
  repository root.

### Changed

- Agent authorization is now fail-closed. Managed OpenCode roles refresh on
  `corral init`, worktree sessions receive the managed policy, and provider
  prompts select the role-specific agent.
- Gate pre-authorization is operator-only; model agents cannot grant that
  authority to themselves.
- Graph validation accepts only supported agent roles and safe relative write
  scopes. Scheduler verification rejects changes outside the declared scope.
- Planner and reviewer tool permissions default to deny, while workers require
  approval for edits and shell commands.
- API-key material is kept in `.corral/api.key` instead of being duplicated in
  project-readable config metadata.

### Fixed

- TUI event ordering, reconnect, terminal-run, and attention behavior.
- Attempt identity collisions across runs and event-subscriber loss after
  overload.
- Retry feedback, reviewer read-only boundaries, and provider stream/usage
  preservation.

## [0.2.0]

See the [v0.2.0 release](https://github.com/thiago-ss/corral/releases/tag/v0.2.0)
for the previous baseline.
