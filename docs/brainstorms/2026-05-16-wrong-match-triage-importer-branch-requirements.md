---
date: 2026-05-16
topic: wrong-match-triage-importer-branch
---

# Wrong Match Triage as an Importer Branch

## Summary

Retire Wrong Matches triage as an independent cleanup system. High-distance
reject handling and bulk cleanup both consume explicit persisted evidence FKs,
ask the unified pipeline decider what force import would do, and delete only
candidates that remain confident rejects. Existing Wrong Matches rows are
backfilled exactly once during this work; runtime cleanup has no legacy
fallback or evidence-backfill path.

---

## Problem Frame

Issue #259 originally targeted a stale decision surface that predated the
current evidence/decision architecture. Wrong Matches triage kept its own
cleanup result type, wrote its own audit blob, and could decide destructive
cleanup after the fact. That was dangerous because the decision could be made
from whichever evidence happened to be attached to the historical row, not
necessarily the rich evidence produced by the actual preview/import job.

PR #261 changed the baseline substantially. Evidence is now
content-addressed, preview is measurement-only, reject paths no longer
re-measure, and Wrong Matches cleanup can walk the evidence FK chain. That
makes the old cleanup path safer, but it does not remove the product problem:
Wrong Matches still has a separate triage/cleanup authority alongside the
importer. The remaining work is to collapse that authority, not to redo the
evidence-canonical cleanup PR already merged.

---

## Actors

- A1. Operator: Uses Wrong Matches to review high-distance rejected folders and
  wants one trustworthy cleanup action that cannot delete importable material.
- A2. Importer: Owns action-time decisions and filesystem mutation after
  candidate evidence exists.
- A3. Wrong Matches UI / CLI surfaces: Let the operator inspect, force import,
  dismiss, or bulk-clean the whole current Wrong Matches queue.
- A4. Evidence store: Holds candidate/current album evidence produced before
  any cleanup decision is made.

---

## Key Flows

- F1. New high-distance candidate reaches the importer
  - **Trigger:** A candidate passes measurement but beets validation rejects it
    as a high-distance match.
  - **Actors:** A2, A4
  - **Steps:** The importer reads the already-persisted candidate evidence and
    current evidence, asks the unified decider what force import would do, then
    branches on that answer.
  - **Outcome:** A candidate that would not import even in force mode is
    deleted and cleared; every other candidate is preserved for manual review.
  - **Covered by:** R1, R2, R3, R4

- F2. Operator runs bulk Wrong Matches triage
  - **Trigger:** The operator clicks or runs the bulk triage action against the
    current Wrong Matches queue.
  - **Actors:** A1, A3, A4
  - **Steps:** The action iterates every current wrong-match row, loads
    existing candidate evidence, asks the same force-mode question, deletes
    confident rejects, keeps would-import rows, and skips rows whose evidence is
    missing or stale.
  - **Outcome:** The queue shrinks without creating new evidence or relying on
    stored historical triage verdicts.
  - **Covered by:** R5, R6, R7, R8

- F3. Operator reviews history after cleanup changes
  - **Trigger:** The operator opens Recents, History, or album detail views
    after this refactor.
  - **Actors:** A1, A3
  - **Steps:** The UI describes wrong-match rows from stable download/import
    facts such as the rejection scenario and evidence-backed outcome, not from
    new `wrong_match_triage` audit blobs.
  - **Outcome:** Historical rows remain readable, but new cleanup does not write
    a second active decision record.
  - **Covered by:** R9, R10

---

## Requirements

**Importer-owned high-distance cleanup**

- R1. High-distance handling must be a branch of the importer decision flow, not
  a separate post-hoc triage system.
- R2. The branch must use candidate/current evidence addressed by explicit
  evidence FKs. It must not infer evidence from legacy sibling/latest-import-job
  fallbacks.
- R3. The branch must ask the unified pipeline decider the same force-mode
  question force import would ask for the candidate.
- R4. If the force-mode answer is a confident reject, the candidate source may
  be deleted and the wrong-match row cleared. If the answer is would-import,
  uncertain, missing evidence, or stale evidence, the candidate must be kept for
  manual review.

**Bulk Wrong Matches triage**

- R5. Wrong Matches must expose one bulk triage action that applies the same
  force-mode decision rule to the whole current Wrong Matches queue.
- R6. Before replacement cleanup is enabled, the current Wrong Matches queue is
  backfilled exactly once so rows have explicit evidence FKs. Runtime bulk
  cleanup is a read-only evidence consumer: it must not create evidence, run
  preview, run measurement, or use legacy sibling/latest-import-job inference.
- R7. Bulk triage must return an operator-readable summary that distinguishes
  at least deleted confident rejects, kept would-import rows, kept uncertain
  rows, and skipped evidence-unavailable rows.
- R8. Bulk triage must not persist new active triage verdicts into
  `download_log.validation_result`. Existing historical triage blobs may remain
  decodable for audit/history views.
- R8a. The web bulk endpoint must require an explicit
  `confirm_all_wrong_matches: true` confirmation flag. The CLI equivalent is
  `wrong-match-triage --apply`; narrower request/limit/all scopes are not part
  of the replacement bulk action.
- R8b. Cleanup must serialize deletion per source row/path/request. Before
  deleting, the service must acquire a DB/advisory lock, recheck active import
  jobs inside that lock, and skip rows with active jobs referencing the same
  `download_log_id`, failed/source path, request, or source directory. Failed
  force-import cleanup may pass its own `import_job_id` as an ignore value, but
  other active jobs still block deletion.

**Surface cleanup**

- R9. The old per-row/backfill triage model must stop being an active operator
  workflow. Per-row delete, group delete, and heuristic delete routes must be
  removed as destructive cleanup surfaces; the confirmed full-queue bulk action
  is the only operator-triggered destructive Wrong Matches cleanup path.
- R10. Recents, History, and album-detail displays must not depend on newly
  written `wrong_match_triage` blobs. They may continue to render historical
  blobs for old rows, but new cleanup status should come from stable row facts
  and the actual cleanup outcome.
- R11. CLI and web operator surfaces must stay symmetric for the replacement
  bulk action: both should expose the same capability and report the same
  outcome categories.

**Regression coverage**

- R12. The live-loss shape from issue #259 must be pinned as a regression: a
  sparse candidate row must not authorize deletion when richer candidate
  evidence exists for the actual import/preview path.
- R13. Tests must prove the simulator-style full pipeline decision and the
  evidence-backed path agree for the affected album shape.
- R14. Tests must prove no new code path writes
  `validation_result.wrong_match_triage` for active cleanup.

---

## Acceptance Examples

- AE1. **Covers R1, R2, R3, R4.** Given a newly rejected high-distance candidate
  whose existing evidence says force import would still reject it, when the
  importer handles the rejection, the source folder is deleted and the row is
  cleared without running measurement or preview.
- AE2. **Covers R1, R3, R4.** Given a newly rejected high-distance candidate
  whose existing evidence says force import would import it, when the importer
  handles the rejection, the source folder remains available for manual review.
- AE3. **Covers R5, R6, R7.** Given a Wrong Matches queue containing confident
  rejects, would-import rows, uncertain rows, and rows with no usable evidence,
  when the operator runs bulk triage, only confident rejects are deleted and the
  response counts every other category separately.
- AE4. **Covers R6, R8, R14.** Given a wrong-match row whose evidence is missing
  after the one-time backfill, when bulk triage reaches it, the row is skipped
  and no preview result, measurement result, evidence row, or
  `wrong_match_triage` blob is written.
- AE5. **Covers R10.** Given an old download-history row that already contains a
  historical `wrong_match_triage` blob, when the operator opens history, the row
  remains readable; given a new cleanup row, the UI does not require that blob
  to explain what happened.
- AE6. **Covers R12, R13.** Given the Technicolour Sleep / Mountain Goats Flux
  shape from issue #259, when the evidence-backed path evaluates the candidate,
  it must not reproduce the sparse-evidence deletion that caused the live loss.

---

## Success Criteria

- Wrong Matches no longer has an independent cleanup decider that can drift from
  importer behavior.
- A downstream planner can describe the new shape as one decision question:
  "would this import if forced?" If no, delete; otherwise keep.
- Bulk cleanup reduces the accumulated Wrong Matches queue without producing new
  evidence, re-running preview, or trusting old triage audit blobs.
- The issue #259 live-loss shape is covered by regression tests through both
  the pure decision surface and the evidence-backed orchestration path.

---

## Scope Boundaries

- Do not redo PR #261 work: content-addressed evidence, FK evidence lookup,
  `measurement.py`, preview-measures-only, and reject-path no-remeasurement are
  baseline assumptions.
- Do not recover the 64 already-deleted FLAC folders in this work. That is a
  separate data-recovery/requeue task.
- Do not change quality policy, beets distance thresholds, spectral thresholds,
  V0 policy, or verified-lossless behavior.
- Do not use legacy sibling/latest-import-job evidence inference for Wrong
  Matches cleanup.
- Do not add runtime evidence backfill to bulk cleanup. The current Wrong
  Matches queue is backfilled once as a rollout/implementation step.
- Do not remove historical decode/render support for old
  `wrong_match_triage` blobs unless planning proves every reader can drop it
  safely.
- Do not add a persistent post-run Wrong Matches summary panel in this pass.
  Toast-level feedback plus the API/CLI summary is acceptable; richer UI
  feedback can be revisited if operators need it.

---

## Key Decisions

- Collapse the surface, not just the bug. PR #261 made cleanup safer, but the
  remaining risk is the existence of an active triage authority outside the
  importer.
- Treat bulk triage as destructive but conservative. Operator-triggered cleanup
  may delete files, but only when the unified force-mode decider says the
  candidate would not import.
- Backfill current Wrong Matches evidence exactly once during rollout. The
  cleanup code assumes explicit FKs and stays simple: no legacy fallback, no
  runtime evidence creation, no preview, and no measurement.
- Delete legacy active cleanup code rather than wrapping it. The replacement
  does not keep `lib/wrong_match_cleanup_decision.py`,
  `lib/wrong_match_triage.py`, or the preview/backfill CLI command as active
  compatibility surfaces.
- Preserve old audit readability while stopping new audit writes. Historical
  rows are useful for explaining past behavior, but new active behavior should
  not depend on a second decision record.

---

## Dependencies / Assumptions

- PR #261 is merged and is the baseline for planning.
- The unified decision authority is `full_pipeline_decision_from_evidence` in
  `lib/quality.py`.
- Current code still contains active Wrong Matches cleanup/triage surfaces,
  including `lib/wrong_match_cleanup_decision.py`,
  `lib/wrong_match_triage.py`, web Wrong Matches delete/triage paths, CLI
  wrong-match triage/backfill commands, and history rendering for
  `wrong_match_triage`.
- It is acceptable for the replacement bulk action to be destructive after an
  explicit operator trigger, as long as the result summary is clear and the
  deletion rule is conservative.

---

## Outstanding Questions

### Resolve Before Planning

- None.

### Deferred to Planning

- [Affects R9, R11][Technical] Decide the exact CLI/API/service result shape
  for the replacement whole-queue bulk action while preserving CLI/web parity.
- [Affects R10][Technical] Decide how much historical
  `wrong_match_triage` rendering can stay as compatibility code, and which new
  fields the UI should prefer for fresh rows.
- [Affects R4, R5][Technical] Removed during chat review: per-row delete actions
  are out of scope as destructive cleanup. Operators use the bulk action plus
  force import/review controls.
