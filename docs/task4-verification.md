# Task 4 — Verification and retry loops

Status: **DONE** — evidence gates surround every agent node; retries carry
focused feedback; budgets bound retries; prose alone never completes work.

## Deliverables

- `internal/verify` — the evidence gate engine:
  - **command**: runs `Verification.Command` in the worktree (default
    timeout 2m, or the node's time budget); exit 0 passes; stdout/stderr
    tail becomes focused feedback + JSON evidence. `CommandRunner`
    injectable for deterministic tests.
  - **json_schema**: validates the artifact at `Verification.Target`
    (relative to the worktree) against the schema (santhosh-tekuri
    jsonschema); missing/invalid files and schema violations produce
    concrete feedback (first 5 validation errors).
  - **reviewer**: `internal/ocxreviewer` implements the injectable
    `Reviewer` seam on top of OpenCode: a read-only LLM session receives the
    attempt's evidence (objective, prior feedback, transcript, recorded diff
    artifact and check results), must return exactly `APPROVED` or
    `CHANGES_REQUESTED` followed by a required `Note:` line; the change-request
    note is the feedback. `CORRAL_REVIEWER_MODEL` overrides the session model
    using OpenCode's `provider/model` format.
  - **default gate** (no method declared): an attempt must have produced
    at least one diff — agent prose alone cannot mark work complete.
  - **check nodes**: verdict derived from their own command run carried in
    message Meta (`exit`/`stdout`/`stderr`).
- `sched`: `EngineVerifier` adapts the engine to the scheduler; failed
  verdicts store focused feedback and it is injected into the next
  attempt (`adapter.Attempt.Feedback`, appended to the OpenCode prompt);
  check nodes execute inline via `Options.CheckRunner` (no agent
  session); cost/token budgets bound retries (`MaxTokens`/`MaxCost`
  checked against accumulated attempt totals before retrying);
  a run whose nodes are all terminal-or-blocked **settles** with status
  `waiting` instead of hanging; `RunHandle.Resume` re-activates it.
- `adapter` v0.3: `Attempt.Feedback`, `Message.Meta`.
- Bug fixed: `http.Client.Timeout: 120s` was killing the long-lived SSE
  stream every 2 minutes — the stream now uses a dedicated client whose
  lifetime is controlled by the request context only.

## Acceptance verification

| Criterion | Evidence |
|---|---|
| Command, JSON-schema, reviewer checks | `verify` unit tests: pass/fail + feedback for each kind; check-node verdict from Meta |
| Reviewer approves / requests changes with a note | `ocxreviewer` tests: scripted fake LLM server covers exact APPROVED and CHANGES_REQUESTED verdicts with notes, malformed verdicts, session errors, missing verdicts, timeout; `TestOpenCodeReviewerLive` runs a real review session (gated on `CORRAL_LIVE`) |
| Failed verification returns focused feedback | `TestFailThenPassAfterRetryWithFeedback`: gate 1 stderr reaches attempt 2 verbatim |
| Retry count, timeout, budget bounded | retries from policy; time budget aborts (Task 2); `TestTokenBudgetBoundsRetries` stops retries after MaxTokens consumed |
| Exhausted node becomes blocked or failed | `TestPermanentFailureBlocksDownstream`: failed, dependent blocked, run settles `waiting`, dependent never ran |
| Agent prose alone cannot mark complete | `TestProseAloneFails` |
| Intentionally failing task passes after one retry | `TestFailThenPassAfterRetryWithFeedback`; real-OpenCode `TestOpenCodeEvidenceGates` (grep gates: pass + deliberate permanent failure → blocked dependent) |

## Notes

- A run ending with a blocked node is `waiting` (settled, `Done()==true`)
  so the scheduler never hangs; `Resume` re-activates it after human
  intervention (used by later tasks).
- `TestOpenCodeEvidenceGates` is deterministic because the gates grep for
  fixed markers the prompt demands, independent of model behavior.
- Reviewer sessions run as the named `corral-reviewer` agent with an agent-level
  wildcard deny at both named-agent and prompt permission layers,
  review recorded diffs and command results from the evidence
  prompt, and stay bound to the main OpenCode project where that named agent
  is configured (they never access the attempt worktree). They poll the
  transcript to idle and parse the verdict from the last assistant message.
  The verdict must contain exactly two lines:
  `APPROVED`/`CHANGES_REQUESTED` then a non-empty `Note:` line; anything else
  fails the gate with a parse error.
