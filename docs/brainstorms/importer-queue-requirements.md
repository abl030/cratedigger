---
date: 2026-04-25
topic: importer-queue
updated: 2026-04-25
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

After the shared queue exists, the next direction is to split import work into
two stages: async preview/decision workers do CPU-heavy validation, spectral
analysis, measurements, and would-import decisions; the serial beets importer
only consumes jobs whose preview finished as importable. This should make the
beets lane faster and easier to reason about without trying to prove beets can
be written in parallel.

This is not just a web responsiveness patch. The force-import false-toast and UI
freeze should be fixed as a consequence of moving imports out of request
handlers and into a shared importer queue.

Current affected areas include `web/routes/pipeline.py`,
`web/routes/imports.py`, `lib/download.py`, `lib/import_dispatch.py`, and
`docs/advisory-locks.md`.

The UI direction for the follow-on work is intentionally narrow: add a queue
subview under Recents. It should show the beets import timeline in importable
order, with the next import at the top. Async preview state enriches each row
with values and color/status changes as it lands; it does not need highly live
streaming updates or a separate admin dashboard.

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
- A6. Async preview worker: Performs validation, spectral analysis,
  measurement, and preview decisions before the beets importer claims a job.
- A7. Deployer/operator: Tunes async worker concurrency on doc2 based on load,
  swap pressure, and queue throughput.

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

- F5. Async preview readiness
  - **Trigger:** A queued import job lacks a completed preview decision.
  - **Actors:** A3, A6
  - **Steps:** A6 claims preview work, validates files, runs spectral analysis
    and measurements, persists the preview values and verdict, and marks the job
    importable, rejected, or errored.
  - **Outcome:** CPU-heavy decision work happens before the serial beets lane,
    and the final importer only sees jobs that are ready to import.
  - **Covered by:** R8, R9, R12, R13, R14, R15

- F6. Recents queue view
  - **Trigger:** A1 opens Recents to see what will import next.
  - **Actors:** A1, A4
  - **Steps:** A4 shows a single import-order timeline, with the next
    beets-importable job at the top, and enriches rows with preview values,
    verdict colors, and failure messages as they are available.
  - **Outcome:** The operator can see queue order and preview outcomes without
    treating queue monitoring as a separate administration product.
  - **Covered by:** R3, R5, R16, R17

- F7. Wrong Matches preview backfill
  - **Trigger:** A7 intentionally runs a maintenance backfill after deploy or
    after preview-cache behavior changes.
  - **Actors:** A1, A6, A7
  - **Steps:** Existing Wrong Matches rows with resolvable files are previewed
    and audited; historical rows without files and already-queued imports are
    not swept by this command.
  - **Outcome:** The visible Wrong Matches backlog can be reduced or annotated
    once, while normal queued imports continue to be discovered by the preview
    worker path.
  - **Covered by:** R18, R19

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
- R9. Preflight, spectral, measurement, and preview-decision work may run in
  parallel, but final beets mutation must remain single-lane unless beets
  parallel write safety is deliberately proven.
- R10. Once web and automation both use the importer, existing advisory-lock and
  import-state complexity should be reviewed for deletion or simplification
  rather than preserved by default.
- R11. Queue semantics must prevent accidental duplicate web submissions from
  creating duplicate import work for the same source/request.

**Async preview stage**

- R12. Queued import jobs must have a preview/decision state that distinguishes
  at least waiting, running, would-import, confident-reject, and uncertain/error.
- R13. Async preview workers must persist validation, spectral, measurement, and
  preview-decision values in durable audit state before a job becomes
  beets-importable.
- R14. The beets importer must only claim jobs whose preview state is complete
  and importable.
- R15. Preview confident-reject, uncertain, or error outcomes must fail or
  resolve the import job with a clear operator-facing message, preserve audit
  detail, and denylist the source when attribution exists and the failure
  belongs to that downloaded source.

**Recents queue visibility**

- R16. Queue visualization must live under Recents as a queue subview, not as a
  separate global admin dashboard.
- R17. The Recents queue subview must show a single beets import timeline sorted
  by importable order, with the next serial import at the top; preview values
  and row color/status should fill in when async preview completes, without
  requiring highly live streaming behavior.

**Backfill and operations**

- R18. Existing Wrong Matches rows with resolvable files must be previewable by
  an explicit one-shot backfill path, not by an always-on historical scanner.
- R19. Queued import jobs must be continuously rediscovered by the normal async
  preview worker path so restarts do not lose readiness work.
- R20. Async preview worker concurrency must be deployment-tunable, default to
  two workers on doc2, and be adjusted only after monitoring CPU, swap pressure,
  and queue throughput.

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
- AE5. **Covers R12, R13, R14.** Given a queued automation import lacks preview
  values, when an async preview worker completes with `would_import`, the values
  and verdict are visible in durable state and the beets importer can claim that
  job on its next pass.
- AE6. **Covers R15, R17.** Given async preview finishes as uncertain/error,
  when the operator opens the Recents queue subview, the row shows a clear
  failure message and does not enter the beets importable lane.
- AE7. **Covers R16, R17.** Given several queued jobs have mixed preview states,
  when the operator opens Recents, the queue subview shows one import-order
  timeline with the next beets import at the top and row color/status indicating
  preview progress or outcome.
- AE8. **Covers R18, R19, R20.** Given the feature is deployed on doc2, when the
  operator runs the one-shot Wrong Matches backfill with the default worker cap,
  existing wrong-match files are previewed while normal queued import jobs remain
  handled by the continuously rediscovered worker path.

---

## Success Criteria

- Operators can enqueue force/manual imports from the UI without freezing the
  web interface or receiving false failure toasts for successful imports.
- The automation pipeline and web UI no longer have separate beets-mutating
  import paths.
- Import progress and failures are observable from the UI and logs.
- The architecture is simpler to explain: beets import mutation has one owner.
- CPU-heavy preview work can be parallelized without making beets writes
  parallel or racing the beets importer against the preview workers.
- Recents gives the operator a clear next-import timeline with preview outcomes
  and errors visible without building a full queue administration surface.
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
- The original shared-queue pass did not require parallel spectral or preflight
  work. The follow-on async preview stage does parallelize that work, but only
  before the serialized beets lane.
- This does not require changing the UI into a full queue-management product.
  The UI only needs a Recents queue subview with import-order rows and preview
  enrichment.
- This does not require deleting existing advisory locks in the same first step.
  Deletion should happen after the shared importer owns the relevant paths.
- This does not require prepared import staging. Durable converted artifacts,
  staging cleanup, and artifact lifecycle should remain future work unless
  conversion is measured as a real bottleneck.
- This does not require an always-on historical preview scanner. Historical
  Wrong Matches backfill is an explicit maintenance action.

---

## Key Decisions

- Use a shared importer queue rather than a web-only queue: the bug is a symptom
  of scattered import ownership, not only a slow request handler.
- Serialize beets mutation: current code does not prove different-release beets
  writes are globally safe, and the desired architecture should not depend on
  that assumption.
- Keep queue state visible to the UI: batching many force-imports needs explicit
  feedback so the operator can tell queued work from a dead UI.
- Use a two-stage import pipeline for the follow-on work: async preview workers
  determine import readiness first, and the serial beets importer waits for
  preview-ready importable jobs instead of racing to do the same CPU work.
- Put queue visualization under Recents: the operator primarily wants to see
  what will import next and why rows changed color, not manage a separate
  queue-control dashboard.
- Default async worker concurrency conservatively: doc2 has spare CPU in normal
  samples but meaningful swap usage, so start with two workers and tune through
  deployment configuration only after monitoring.
- Keep prepared staging out of v1: async preview cache is the valuable first
  step; durable converted artifacts should be added only if conversion time is
  proven to slow the serial lane materially.
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
- The unified import preview path can evaluate real folders and typed values,
  so async workers should reuse that decision vocabulary instead of creating a
  second simulator.
- doc2 deployment starts with two async preview workers and treats higher
  concurrency as an operational tuning decision based on observed CPU, memory,
  swap, disk, and queue-drain behavior.

---

## Outstanding Questions

### Resolve Before Planning

- None.

### Deferred to Planning

- [Affects R12-R15][Technical] Decide the exact durable state shape for preview
  readiness, preview audit values, and job failure messages.
- [Affects R14][Technical] Decide how the beets importer orders and claims only
  preview-ready importable jobs while preserving existing queue ordering.
- [Affects R16, R17][Technical] Decide the smallest Recents subview data
  contract that can render import order, preview values, colors, and errors.
- [Affects R18][Technical] Decide the operator command or one-shot mode for
  Wrong Matches backfill and how it reports skipped rows.
- [Affects R10][Technical] Inventory which advisory locks or state-machine paths
  become redundant after the importer owns both web and automation imports.
- [Affects R20][Operational] Define the deploy verification pattern for worker
  count changes, including which doc2 metrics to check before increasing beyond
  the default of two workers.

### Post-Implementation Status

The async preview queue plan implemented the R12-R20 follow-on work:

- Durable preview state now lives on `import_jobs`.
- Async preview workers claim waiting jobs, persist no-mutation preview audit,
  and mark only `would_import` jobs importable.
- The serial importer claims only queued jobs whose preview is complete and
  importable.
- Recents has a Queue subview backed by `/api/import-jobs/timeline`.
- Wrong Matches preview backfill is an explicit operator command.
- The NixOS module starts `cratedigger-import-preview-worker.service` with a
  deployment-tunable worker count that defaults to two when
  `services.cratedigger.importer.preview.enable = true`.
- The async preview gate is opt-in for backward compatibility. When preview is
  disabled, application and database defaults mark new jobs
  `preview_status='would_import'` with `preview_message='Preview gate disabled'`
  so the serial importer drains without preview workers.
- Nix prestart now renders the shared `config.ini` atomically because importer,
  preview, web, and timer-driven services can start concurrently after DB
  migrations.

Remaining long-tail work is not required for the feature to deploy:

- Review and remove redundant advisory locks after production observation.
- Add durable prepared staging or converted preview artifacts only if measured
  conversion time keeps the serial beets lane slow.
- Add richer queue management controls, such as retry/cancel/manual-review for
  uncertain preview results, only if the Recents Queue view is not enough.
- Tune `services.cratedigger.importer.previewWorkers` beyond two only after
  observing CPU, swap, disk, and queue-drain behavior on doc2.

Tracking issue: <https://github.com/abl030/cratedigger/issues/169>.

---

## Next Steps

-> /ce-plan for structured implementation planning.
