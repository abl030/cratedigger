---
date: 2026-04-25
topic: importer-queue
---

# Importer Queue Requirements

## Problem Frame

Issue #147 exposed a web timeout symptom, but the real problem is broader:
beets-mutating import work is reachable through multiple paths, including the
web UI and the automation pipeline. The current design relies on a growing set
of advisory locks and state-machine rules to keep those paths from colliding.
That has made import behavior expensive to understand, refactor, and trust.

The product direction is to replace scattered beets-import ownership with one
queue-backed importer. Web actions and automation should submit import work to
the same owner, and that owner should run beets-mutating work serially.

This is not just a web responsiveness patch. The force-import false-toast and UI
freeze should be fixed as a consequence of moving imports out of request
handlers and into a shared importer queue.

Current affected areas include `web/routes/pipeline.py`,
`web/routes/imports.py`, `lib/download.py`, `lib/import_dispatch.py`, and
`docs/advisory-locks.md`.

---

## Actors

- A1. Operator: Uses the web UI to review wrong matches and trigger force or
  manual imports, often in batches.
- A2. Automation pipeline: Finds completed downloads and submits ready-to-import
  work without user interaction.
- A3. Importer: The single owner of beets-mutating import work.
- A4. Web UI: Shows queue state and lets the operator keep working while import
  jobs run.
- A5. Planner/implementer: Needs a clear target architecture that avoids adding
  another temporary queue beside the existing lock model.

---

## Key Flows

- F1. Web force-import queueing
  - **Trigger:** A1 force-imports one or more wrong-match rows from the web UI.
  - **Actors:** A1, A3, A4
  - **Steps:** A4 validates the cheap user-facing inputs, submits import work to
    A3, shows queued/running/completed/failed state, and refreshes wrong-match
    data when jobs complete.
  - **Outcome:** The web request returns quickly, the UI remains responsive, and
    the operator can see that work is progressing.
  - **Covered by:** R1, R2, R3, R5, R8

- F2. Automation import queueing
  - **Trigger:** A2 determines that a downloaded album is ready for beets import.
  - **Actors:** A2, A3
  - **Steps:** A2 submits the ready-to-import work to A3 instead of invoking the
    beets import path directly. A3 eventually runs the import and records the
    final result where existing pipeline views can observe it.
  - **Outcome:** Automation and web imports cannot mutate beets concurrently
    through separate paths.
  - **Covered by:** R1, R4, R6, R7

- F3. Import execution
  - **Trigger:** A3 has queued import work.
  - **Actors:** A3
  - **Steps:** A3 takes the next eligible job, marks it running, performs the
    beets-mutating import work serially, records success or failure, and moves
    to the next job.
  - **Outcome:** Beets mutation has one owner, one execution lane, and
    observable job state.
  - **Covered by:** R1, R3, R4, R6, R9

- F4. Queue visibility
  - **Trigger:** A1 or A4 needs feedback while jobs are queued or running.
  - **Actors:** A1, A4
  - **Steps:** A4 reads queue state, shows aggregate progress, and shows per-row
    state where a row corresponds to queued work.
  - **Outcome:** Long-running batches do not look frozen or failed while work is
    actually progressing.
  - **Covered by:** R2, R5, R8

---

## Requirements

**Queue ownership**

- R1. All beets-mutating import work must flow through one importer owner rather
  than being invoked independently by web handlers and automation code.
- R2. Web force-import and manual-import actions must enqueue work and return
  quickly; they must not block HTTP request handlers for the duration of audio
  checks, spectral work, conversion, or beets import.
- R3. The importer must expose enough job state for the UI to distinguish
  queued, running, completed, and failed work.
- R4. The automation pipeline must submit ready-to-import work to the same
  importer path used by the web UI.

**Operator feedback**

- R5. The Wrong Matches UI must show visible feedback for queued/running import
  work so a batch of force-imports does not appear dead.
- R6. Completed import work must continue to update the existing durable source
  of truth, including request status and download/import history, so existing
  pipeline views remain meaningful.
- R7. Import failures must be visible as job results and must preserve enough
  message/detail for the operator or pipeline logs to explain what happened.

**Concurrency and simplification**

- R8. The importer must run beets-mutating execution serially. Parallelism may be
  introduced only outside the beets-mutation lane.
- R9. A later staged design may run preflight or spectral work in parallel, but
  final beets mutation must remain single-lane unless beets parallel write
  safety is deliberately proven.
- R10. Once web and automation both use the importer, existing advisory-lock and
  import-state complexity should be reviewed for deletion or simplification
  rather than preserved by default.
- R11. Queue semantics must prevent accidental duplicate web submissions from
  creating duplicate import work for the same source/request.

---

## Acceptance Examples

- AE1. **Covers R1, R2, R3, R5.** Given a force-import that takes 80 seconds,
  when the operator starts it from Wrong Matches, the web request returns
  quickly, the row shows queued or running state, other web interactions still
  work, and the row eventually shows completion or failure.
- AE2. **Covers R1, R4, R8.** Given the automation pipeline is ready to import
  one release while the web UI has queued another release, when both jobs exist,
  beets mutation runs through one importer lane rather than two independent
  import paths.
- AE3. **Covers R5, R7.** Given a batch of many force-imports, when some jobs are
  queued, some are running, and one fails, the UI can show aggregate progress
  and a failure message without requiring the operator to inspect server logs.
- AE4. **Covers R10.** Given both web and automation have migrated to the shared
  importer, when reviewing `docs/advisory-locks.md` and related call sites, any
  lock whose only purpose was cross-entrypoint beets import concurrency is a
  candidate for removal.

---

## Success Criteria

- Operators can enqueue force/manual imports from the UI without freezing the
  web interface or receiving false failure toasts for successful imports.
- The automation pipeline and web UI no longer have separate beets-mutating
  import paths.
- Import progress and failures are observable from the UI and logs.
- The architecture is simpler to explain: beets import mutation has one owner.
- A planner can turn this document into an implementation plan without inventing
  product behavior, queue semantics, or migration intent.

---

## Scope Boundaries

- This replaces the web-only in-memory async job idea as the desired final
  direction for issue #147.
- This does not require proving beets is parallel-write safe. The initial
  assumption is the opposite: beets mutation is serialized.
- This does not remove all pipeline state. Search, download, request status, and
  import history remain meaningful durable concepts.
- This does not require the first implementation to parallelize spectral or
  preflight work.
- This does not require changing the UI into a full queue-management product.
  The UI only needs enough visibility to make queued work understandable.
- This does not require deleting existing advisory locks in the same first step.
  Deletion should happen after the shared importer owns the relevant paths.

---

## Key Decisions

- Use a shared importer queue rather than a web-only queue: the bug is a symptom
  of scattered import ownership, not only a slow request handler.
- Serialize beets mutation: current code does not prove different-release beets
  writes are globally safe, and the desired architecture should not depend on
  that assumption.
- Keep queue state visible to the UI: batching many force-imports needs explicit
  feedback so the operator can tell queued work from a dead UI.
- Treat lock simplification as a goal after migration: the queue should reduce
  the need for cross-entrypoint import locks, not become another layer added on
  top forever.

---

## Dependencies / Assumptions

- The existing durable import result surfaces, such as request status and
  download/import history, remain the source of truth after a job completes.
- The importer needs to be reachable by both web UI code and automation code.
- The first implementation may keep existing import internals intact behind the
  queue while ownership is moved.
- Duplicate submission handling is needed because operators may click multiple
  force-import rows quickly or retry while jobs are still visible.

---

## Outstanding Questions

### Resolve Before Planning

- None.

### Deferred to Planning

- [Affects R1, R3][Technical] Decide whether the queue state is DB-backed from
  day one or introduced behind a smaller service boundary first.
- [Affects R4, R10][Technical] Identify the safest migration sequence for moving
  automation from direct import execution to queue submission.
- [Affects R5][Technical] Decide whether queue visibility appears only in Wrong
  Matches at first or as a global queue indicator.
- [Affects R8, R9][Needs research] Identify which parts of the current import
  path are pure/preflight work and which parts must stay inside the serialized
  beets-mutation lane.
- [Affects R10][Technical] Inventory which advisory locks or state-machine paths
  become redundant after the importer owns both web and automation imports.

---

## Next Steps

-> /ce-plan for structured implementation planning.
