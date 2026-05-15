---
date: 2026-05-15
topic: importer-never-measures
---

# Importer Never Measures — Preview Owns Candidate Evidence

## Summary

Make the preview worker the only producer of candidate evidence. Delete the
toggle that lets imports bypass preview, and replace the importer's
hard-fail-on-missing-evidence with a requeue to `queued` so the preview worker
recovers the row on its next claim. The importer and the preview worker
consult the same load-plus-snapshot function and trust its answer.

---

## Problem Frame

The 2026-05-14 evidence-boundary refactor made preview a producer of durable
candidate evidence and made the importer recompute decisions from that
evidence. Two seams remain wrong.

The Nix option `services.cratedigger.importer.preview.enable` defaults to off
and, when off, marks newly enqueued jobs importable immediately — bypassing
preview entirely. Two parallel pipelines exist: one with evidence, one
without. Every `enqueue_import_job`, fake, and test branches on the flag.
The dual-mode surface adds carrying cost forever, and production has only
ever wanted preview on.

When the importer drains a job and the cheap snapshot guard fails (or the
evidence row is missing), `lib/import_dispatch.py` returns
`DISPATCH_CODE_CANDIDATE_EVIDENCE_UNAVAILABLE` and the job hard-fails. The
2026-05-14 brainstorm's R6 specified that the active path "must recompute or
backfill evidence" first and only fail closed if recompute itself cannot
complete. The current code skips the recompute step entirely. The
consequence: roughly 700 legacy Wrong Matches rows whose `download_log`
predates migration 017 cannot be force-imported through the evidence-aware
path, because they have no evidence row and the importer will not produce
one. The recompute step belongs in the preview worker — the only producer
of candidate evidence in the system — not as a second measurement codepath
inside the importer.

---

## Actors

- A1. Operator: Triggers force-import on Wrong Matches rows; expects the
  job to drive to completion without manual intervention.
- A2. Preview worker (`cratedigger-import-preview-worker`): Claims `queued`
  jobs, ensures candidate evidence exists and is snapshot-valid, marks the
  job ready for the importer.
- A3. Importer worker (`cratedigger-importer`): Claims ready jobs, recomputes
  the decision against valid evidence, mutates beets serially.

---

## Key Flows

- F1. Force-import of a row with no candidate evidence (legacy population)
  - **Trigger:** Operator clicks force-import on a Wrong Matches row whose
    originating `download_log` predates migration 017.
  - **Actors:** A1, A2, A3
  - **Steps:** Force-import enqueues an `import_job` with status `queued`.
    Preview worker claims it, calls the shared load-plus-snapshot function,
    sees no evidence row, measures, persists evidence, marks the job ready.
    Importer claims the ready job, loads valid evidence, recomputes the
    decision, mutates beets.
  - **Outcome:** Legacy rows flow through the same pipeline as new rows.
    No legacy branch exists in the importer.
  - **Covered by:** R1, R2, R3, R5, R6, R7

- F2. Importer encounters missing or stale evidence
  - **Trigger:** Importer claims a ready job; the shared load-plus-snapshot
    function returns `None` (missing, stale, or incomplete).
  - **Actors:** A2, A3
  - **Steps:** Importer flips the job's status back to `queued` and stops.
    Preview worker claims the requeued job on its next tick, measures,
    persists, marks ready. Importer reclaims and proceeds.
  - **Outcome:** Importer never measures. Stale or missing evidence is
    recovered through the existing preview pathway.
  - **Covered by:** R3, R4, R5

- F3. Preview worker re-claims a job whose evidence is still valid
  - **Trigger:** Preview worker claims a `queued` job whose evidence row
    already exists and whose snapshot still matches the candidate folder.
  - **Actors:** A2
  - **Steps:** Preview calls the shared load-plus-snapshot function; it
    returns valid evidence; preview marks the job ready without invoking
    any measurement helper.
  - **Outcome:** Requeue → re-claim is free when nothing has changed.
  - **Covered by:** R4, R5

---

## Requirements

**Preview is mandatory**

- R1. Every enqueued `import_job` must pass through the preview worker.
  The option `services.cratedigger.importer.preview.enable` and its
  environment variable `CRATEDIGGER_IMPORT_PREVIEW_ENABLE` are deleted.
- R2. `enqueue_import_job` (and its test fakes) must not branch on a
  preview-enabled flag. There is one enqueue path; new jobs are written
  to the queue in a state the preview worker will claim.

**Importer never measures**

- R3. When the importer drains a job and the shared load-plus-snapshot
  function returns `None` (evidence missing, stale, incomplete, or
  otherwise invalid), the importer flips the job's status back to
  `queued`. It does not invoke any candidate measurement helper —
  spectral analysis, V0 probing, candidate bitrate probing,
  `run_preimport_gates`, or equivalents.
- R4. A single shared function is the source of truth for evidence
  validity. Both the importer (before mutation) and the preview worker
  (before measurement) call it; neither side reimplements the snapshot
  check.

**Preview is idempotent on valid evidence**

- R5. On claiming a job, the preview worker must consult the shared
  load-plus-snapshot function before invoking measurement. Valid
  evidence → mark the job ready, skip measurement. `None` → measure,
  persist evidence, mark the job ready.

**Pipeline shape**

- R6. The dispatch branch that runs `inspect_local_files` /
  `run_preimport_gates` directly (the path that fires when no
  `import_job_id` and no `download_log_id` is supplied) is deleted.
  Every import enters dispatch with an `import_job_id`;
  `download_log_id` may additionally accompany it for Wrong Matches
  force-imports to scope candidate evidence to the originating download.
- R7. The 2026-05-14 brainstorm's R6 fail-closed clause is superseded
  for the candidate-evidence path. Recompute lives in preview;
  importer-side "fail closed when evidence missing" is replaced by
  requeue. Cleanup-side fail-closed semantics (Wrong Matches triage)
  are unchanged.

---

## Acceptance Examples

- AE1. **Covers R1, R2.** Given the cratedigger codebase after the
  refactor, when grepping for `CRATEDIGGER_IMPORT_PREVIEW_ENABLE`,
  `import_preview_enabled_from_env`, `services.cratedigger.importer.preview.enable`,
  or `preview_enabled` parameters in `lib/`, `scripts/`, `tests/`, or
  `nix/`, no matches remain.
- AE2. **Covers R3, R4, R5.** Given an `import_job` whose candidate
  evidence row is missing, when the importer claims it, the importer
  flips its status back to `queued` and returns without calling any
  measurement helper. On the next preview-worker tick the worker
  measures, persists evidence, and marks the job ready. The importer
  reclaims and proceeds.
- AE3. **Covers R3, R4, R5.** Given an `import_job` whose evidence row
  exists but whose cheap snapshot no longer matches the candidate
  folder, when the importer claims it, the same requeue → re-measure
  cycle as AE2 fires. The importer does not measure on either pass.
- AE4. **Covers R4, R5.** Given an `import_job` marked ready whose
  evidence is still valid, when the preview worker re-claims it after a
  requeue, the worker marks the job ready again without invoking any
  measurement helper.
- AE5. **Covers R6.** Given any production caller of dispatch, when it
  invokes `dispatch_import_from_db`, it supplies an `import_job_id`.
  The legacy `run_preimport_gates` branch in
  `_dispatch_import_from_db_locked` is unreachable and deleted.
- AE6. **Covers R1, R5, R7.** Given a force-import on a
  pre-migration-017 `download_log` row (no existing evidence), when the
  operator triggers it, the job flows
  `queued` → preview measures → ready → importer mutates, identically
  to a freshly previewed job. There is no separate legacy code path.

---

## Success Criteria

- A force-import on any Wrong Matches row drives to completion without
  operator intervention or hand-rolled measurement. The ~700 legacy rows
  flow through the same pipeline as new ones.
- The importer's code surface measures nothing. Pyright and grep cannot
  find a call from the importer worker to spectral analysis, V0 probing,
  candidate bitrate probing, or `run_preimport_gates` after the refactor.
- A downstream agent picking up `ce-plan` can describe the data flow as:
  enqueue → preview (idempotent) → importer (mutation only), with one
  shared validity function — without inventing toggles, alternate
  branches, or recompute-in-importer semantics.
- The preview-enable toggle is absent from `nix/module.nix`, the Python
  code, and the test surface.

---

## Scope Boundaries

- Proactive backfill of the ~700 wrong-match rows is out of scope. They
  are measured on demand at first force-import.
- The Wrong Matches cleanup / triage path is not touched. It already
  routes through `decide_wrong_match_cleanup`, which has its own
  evidence acquisition.
- Content-hash or audio-fingerprint upgrades to the snapshot guard are
  out of scope. The existing `audio_snapshot_matches` check (sorted
  paths, sizes, mtimes, extensions) remains the cheap check.
- Cross-request or cross-release candidate-evidence reuse is out of
  scope.
- The evidence-authorized harness mutation mode
  (`--quality-evidence-action-file`) is unchanged. This brainstorm
  governs *how candidate evidence reaches the mutation boundary*, not
  what happens inside the mutation.
- The automation (non-force) import path already enqueues `import_job`
  rows. Once the preview toggle is gone, those rows pass through preview
  unconditionally; no automation-specific work is in scope beyond the
  toggle deletion.

---

## Key Decisions

- **Importer never measures.** Inlining recompute in the importer would
  be marginally less code in one place but duplicates measurement
  paths and ties up the serial mutation lane on slow work. Preview owns
  measurement, period.
- **Single shared validity function.** Both importer and preview call
  the same load-plus-snapshot function. Staleness logic is not
  duplicated; the answer is the same on both sides by construction.
- **Requeue is a plain status flip.** No new state, no retry counters,
  no new tables. If preview's measurement legitimately fails (files
  vanished, ffmpeg crash), the existing preview-failure path marks the
  job failed and the cycle ends.
- **Delete the preview-enable toggle outright.** The two-mode surface
  costs more than the configurability earns. Production has only ever
  wanted preview on, and the off-branch existed for backward-compatible
  rollout that is now complete.

---

## Dependencies / Assumptions

- The shared function (today `lib.quality_evidence.load_candidate_evidence_for_source`)
  already returns `None` consistently for missing, stale, and incomplete
  evidence. Verified during brainstorm; planning to re-verify and decide
  whether it stays the canonical entry point or gets a thin wrapper.
- The preview worker's exception handling already marks a job failed on
  legitimate measurement failure (file vanished, ffmpeg error). This is
  the natural exit from a requeue → re-measure cycle that cannot make
  progress.
- Migration 017 is deployed. No new schema changes are anticipated; this
  is a code-shape refactor on top of existing tables.

---

## Outstanding Questions

### Deferred to Planning

- [Affects R3][Technical] What status / column transition exactly
  constitutes "flip back to `queued`"? The preview worker's claim query
  defines the answer (clear `preview_status`, reset `claimed_at`, etc.).
- [Affects R5][Technical] Where does the preview-worker front-gate
  live — at the top of `execute_preview_job`, inside
  `process_claimed_preview_job`, or a small wrapper? Today
  `execute_preview_job` unconditionally calls
  `preview_import_from_path(..., persist_candidate_evidence=True)`.
- [Affects R6][Technical] Audit every caller of
  `dispatch_import_from_db` (web routes, CLI subcommands, importer
  worker, automation poller) to confirm `import_job_id` is always set
  in production after the refactor. The legacy direct path is only safe
  to delete once that audit is clean.
