# corral — durable graph/loop orchestration for OpenCode

[![ci](https://github.com/thiago-ss/corral/actions/workflows/ci.yml/badge.svg)](https://github.com/thiago-ss/corral/actions/workflows/ci.yml)
[![release](https://github.com/thiago-ss/corral/actions/workflows/release.yml/badge.svg)](https://github.com/thiago-ss/corral/actions/workflows/release.yml)

corral is a durable task-graph orchestrator for agent runs. You describe a
run as a DAG of nodes (agents, checks, human gates, merges); a companion
daemon schedules them against OpenCode sessions with SQLite persistence,
evidence-gated verification, retries, budgets, writable-work isolation
(git worktrees) and a human approval gate before merging — and it survives
restarts without duplicating work.

```
OpenCode plugin (corral_*) ──▶ corral daemon ──▶ SQLite event log
TUI (corral tui)          ──▶  │                (durable source of truth)
                                ├── task graph + state machine
                                ├── node loop (leases, priority+aging)
                                └── OpenCode adapter (sessions, SSE, cancel)
```

## Install

```sh
# macOS / Linux (installs to ~/.local/bin)
curl -fsSL https://raw.githubusercontent.com/thiago-ss/corral/main/scripts/install.sh | sh

# or build from source
go install github.com/thiago-ss/corral/cmd/corral@latest
```

Requires: `opencode` ≥ 1.18 on your PATH, and a git repository to
orchestrate work in.

## Updating

```sh
corral update     # downloads the latest release, sanity-checks it, replaces the binary
corral version    # show the installed version
```

`corral update` refuses to downgrade (newer local build than the latest
release) and validates the download by running it before replacing the
binary. Built from source? `git pull && make install`.

## Quickstart

```sh
# ONE command: git check + API key + OpenCode plugin + agent roles +
# detached daemon + health/doctor report
corral up

# follow runs in the terminal dashboard (reads the key itself)
corral tui

# environment check / full audit export
corral doctor
corral export <runID> --out audit.json
```

`corral up` writes `.corral/` (API key, config, daemon log), installs the
plugin into `.opencode/tools/corral.ts`, merges the five agent roles
(planner, orchestrator, worker, reviewer, merger) into `opencode.json`
(preserving any existing config), starts the daemon detached, and runs
`corral doctor` — all idempotent.

Then restart OpenCode and the workflow is ready (below).

### From inside OpenCode

1. `corral up` did the setup — restart OpenCode so the tools/agents load.
2. In OpenCode, switch to the `corral-planner` agent and ask for a plan:
   the `corral_plan` tool returns a graph JSON; review it.
3. Switch to `corral-orchestrator` and `corral_start` with the graph.
4. Follow execution with `corral_status`; approve the human gate with
   `corral_approve`; steer/cancel/retry nodes as needed.
5. Watch the merge node fold accepted branches into main.

## State machine

```
pending → ready → leased → running → verifying → done
             ├→ retry_wait → ready
             └→ blocked | failed | canceled
```

`sidle`/`session.idle` never means success: completion requires evidence
(command gate, JSON-schema gate, reviewer gate, or produced diffs — agent
prose alone fails). Failed verification returns focused feedback into the
next attempt. Runs with a blocked node settle as `waiting` (daemon stays
up); operator actions (approve/reject/retry/steer/permission) re-activate.

## Commands

| command | purpose |
|---|---|
| `corral daemon [--port 4519] [--key TOKEN]` | scheduler control API + embedded opencode server |
| `corral tui` | companion dashboard: runs list, DAG, inspect, actions |
| `corral init` | local initialization (git check, `.corral/`, API key, plugin + agent config install) |
| `corral up` | init-if-needed + detached daemon + health wait + doctor |
| `corral doctor` | opencode version, git, daemon, plugin, config checks |
| `corral export <runID> [--out file]` | full audit export (events, attempts, content-addressed artifacts) |

Daemon API (OpenAPI at `GET /doc`): `/api/plan`, `/api/runs`,
`/api/runs/{id}` (+ `/approve`, `/reject`, `/cancel`, `/retry`, `/steer`,
`/permission`, `/export`). Roles via `X-Corral-Role`.

## Layout

```
cmd/corral        CLI (daemon, tui, init, doctor, export)
internal/adapter  generic executor contract (OpenCode first, others pluggable)
internal/ocx      OpenCode HTTP/SSE client
internal/ocxadapter  OpenCode driver (per-attempt sessions, events+polling)
internal/graph    schema, validation, state machine, ready computation, proposals
internal/store    SQLite: event log (replay), nodes, attempts, leases, artifacts
internal/sched    durable scheduler: leases, priority+aging, retries, budgets,
                  worktrees, merge/gate nodes, circuit breaker
internal/verify   evidence gates: command, json_schema, reviewer, prose rejection
internal/worktree git worktree manager, scope collisions, content-addressed diffs
internal/daemon   HTTP control API, role enforcement, planner, OpenAPI, export
internal/tui      bubbletea dashboard
internal/spike    Task-0 transport spike (live server tests)
```

## Verification

- Deterministic scheduler tests (fake clock, scripted agents): fork/join,
  priority+aging, retry loops, budgets, crash/restart replay, isolation,
  permission blocking, circuit breaker.
- Real-OpenCode integration tests: transport spike, parallel + cancel,
  evidence gates, worktree merge with approval, daemon E2E, live planner.
- `go test ./... -p 1 -count=1` — run the suite serially: every
  opencode-dependent package spawns its own embedded `opencode serve`, so
  parallel package runs contend on the local model provider. `-race` clean.
