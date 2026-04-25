---
date: 2026-04-25
topic: import-preview
---

# Unified Import Preview Requirements

## Problem Frame

Wrong Matches triage needs to know whether a rejected candidate would actually
survive the import pipeline before the operator spends time converging it. The
current tools are close but split: `pipeline-cli quality` and the Decisions tab
simulate typed scenarios, while `harness/import_one.py` owns the real file-based
quality comparison but its current `--dry-run` exits before that comparison.

The goal is one authoritative import-preview capability. It must support real
folders, where Cratedigger inspects files and runs spectral work, and synthetic
override inputs, where an operator or UI can type values to simulate a
scenario. Both modes should use the same decision pipeline so wrong-match
triage, the CLI, the API, and the Decisions UI cannot drift apart.

---

## Actors

- A1. Operator: Reviews Wrong Matches, previews import outcomes, and converges
  only candidates that are likely to improve the library.
- A2. Wrong Matches workflow: Uses preview results to keep importable
  candidates and discard clear rejects.
- A3. Import decision pipeline: The shared decision owner for preimport gates,
  spectral/transcode decisions, quality comparison, and post-import gate
  simulation.
- A4. CLI/API/UI surfaces: Expose the same preview behavior for live folders
  and typed simulations without adding separate decision logic.
- A5. Implementer: Needs a clean seam that removes duplicated decision setup
  instead of adding another parallel importer.

---

## Key Flows

- F1. Real-folder preview
  - **Trigger:** A1 or A2 asks whether a wrong-match folder would import.
  - **Actors:** A1, A2, A3, A4
  - **Steps:** Resolve the folder, inspect local files, run the same preimport
    gates that force/manual import uses, run spectral work when the gates call
    for it, run any conversion needed for a faithful quality comparison in an
    isolated workspace, and return a preview verdict without mutating beets,
    the source folder, or pipeline request state.
  - **Outcome:** The caller knows whether the candidate would import,
    confidently reject, or remain uncertain.
  - **Covered by:** R1, R2, R3, R5, R6, R7, R8

- F2. Override-value simulation
  - **Trigger:** A1 opens the Decisions UI or runs the CLI with explicit
    values such as bitrate, format, VBR status, spectral grade, and existing
    quality fields.
  - **Actors:** A1, A3, A4
  - **Steps:** Parse typed values into the same measured-decision input shape
    used by real-folder preview, run the shared decision pipeline, and return
    the same preview result shape.
  - **Outcome:** Hypothetical scenarios and real-folder previews explain
    decisions with the same stages and vocabulary.
  - **Covered by:** R1, R4, R6, R9

- F3. Wrong-match triage consumer
  - **Trigger:** A2 evaluates one or more wrong-match candidates.
  - **Actors:** A1, A2, A3
  - **Steps:** For each actionable wrong-match row, run real-folder preview.
    If the verdict would import, keep or stage the row for converge. If the
    verdict is a clear reject, delete the source folder and clear the
    actionable failed-path pointer. If preview errors or cannot be trusted,
    leave the row visible with a reason.
  - **Outcome:** Wrong Matches gets smaller without deleting potentially useful
    uncertain candidates.
  - **Covered by:** R2, R5, R7, R8, R10

---

## Requirements

**Single decision source**

- R1. Import preview must use the same decision pipeline as actual import for
  quality comparison, transcode handling, verified-lossless handling, target
  format handling, and post-import gate simulation.
- R2. Real-folder preview must include the same preimport gates used by
  force/manual import, including nested-layout rejection, audio validation, and
  spectral transcode detection.
- R3. `harness/import_one.py --dry-run` must run far enough to produce the same
  import decision and `ImportResult` measurement fields as a real import would
  before beets mutation begins.
- R4. Typed override simulation must use the same measured-decision reducer as
  real-folder preview, not a second hand-written model.

**Surfaces and result shape**

- R5. The preview capability must expose a CLI mode that can preview a real
  folder for a request or download-log row.
- R6. The preview capability must expose an API mode that supports both real
  folder preview and typed override simulation.
- R7. Preview results must distinguish at least three outcomes: would import,
  confident reject, and uncertain/error.
- R8. Real-folder preview must not mutate the source folder, beets library,
  album request status, spectral request state, denylist, import queue, or
  download log.
- R9. Existing Decisions UI simulation behavior must remain available, but its
  implementation should call through the unified preview/decision seam.

**Wrong Matches use**

- R10. Wrong-match triage may delete files and clear failed-path pointers only
  for confident rejects; uncertain/error previews must remain visible.
- R11. Preview explanations must expose the relevant stage chain so the
  operator can see whether a candidate failed preimport, spectral, quality
  comparison, or post-import quality gate.

---

## Acceptance Examples

- AE1. **Covers R1, R2, R3, R7, R8.** Given a wrong-match MP3 folder whose
  spectral analysis would reject it as a transcode, when real-folder preview
  runs, the result is a confident reject with a spectral-stage explanation, no
  request state changes, no denylist entry, and no source deletion.
- AE2. **Covers R1, R3, R8.** Given a FLAC wrong-match folder that needs
  conversion before quality comparison, when preview runs, conversion happens
  only in an isolated workspace and the original folder remains unchanged.
- AE3. **Covers R4, R6, R9.** Given equivalent typed values submitted through
  the API and the Decisions UI, when both are evaluated, they return the same
  stage chain and final preview verdict as a direct call to the shared decision
  reducer.
- AE4. **Covers R5, R7, R11.** Given an operator runs the CLI against a
  download-log row, when preview completes, the output clearly says whether the
  row would import, confidently reject, or remain uncertain, with the measured
  new/existing quality values included.
- AE5. **Covers R10.** Given wrong-match triage sees one clear reject and one
  preview error, when cleanup runs, only the clear reject is deleted and
  cleared; the error case remains visible for manual review.

---

## Success Criteria

- Operators can safely preview real wrong-match folders before converge without
  risking beets or source-folder mutation.
- The Decisions UI, CLI simulations, real-folder preview, and wrong-match
  triage all explain decisions through the same stage names and result shape.
- The codebase has less duplicated quality/import decision setup than it has
  today; future changes to import decisions have one obvious place to update.
- A planner or implementer can proceed without inventing product behavior,
  cleanup policy, or API/CLI surface semantics.

---

## Scope Boundaries

- This does not change the actual import policy, quality thresholds, spectral
  thresholds, or beets matching thresholds.
- This does not make preview an import job and does not add another
  beets-mutating worker.
- This does not delete anything from the beets library.
- This does not make uncertain preview failures eligible for automatic deletion.
- This does not require caching or storing spectral analysis results next to
  download attempts.
- This does require automatic triage for newly created Wrong Matches rows in the
  download import path, but only through the conservative unified preview seam:
  confident cleanup-eligible rejects can be deleted and cleared; would-import
  and uncertain candidates remain visible.
- This does not require replacing every historical simulator test in one pass;
  compatibility wrappers can remain while delegating to the shared decision
  path.

---

## Key Decisions

- Build one preview seam rather than another simulator: the core value is
  eliminating drift between "would import" answers.
- Let real-folder preview do spectral work again even if a later converge will
  repeat it: duplicated work is safer than threading precomputed spectral into
  the real import path before the decision seam is clean.
- Treat `--dry-run` as a real import preview, not an early conversion dry run:
  it should stop before mutation, not before the decision.
- Preserve synthetic override simulation: it is still useful for debugging,
  Decisions UI scenarios, and operator reasoning without real files.
- Keep wrong-match cleanup conservative: only confident rejects are eligible for
  deletion.

---

## Dependencies / Assumptions

- `harness/import_one.py` remains the owner of file-based spectral,
  conversion, and quality-measurement behavior before beets import.
- `lib.quality` remains the home for pure quality/import decision reducers.
- Real-folder preview can use temporary working directories for destructive or
  measurement-only conversion steps.
- Existing `ImportResult` JSON is the right audit/result shape to preserve and
  expose from preview.
- The first implementation may keep compatibility endpoints such as
  `/api/pipeline/simulate` as wrappers while routing their logic through the new
  seam.

---

## Outstanding Questions

### Resolve Before Planning

- None.

### Deferred to Planning

- [Affects R1, R3][Technical] Decide the exact pure decision reducer shape so
  both `full_pipeline_decision()` and `harness/import_one.py` can delegate
  without circular imports or dual-module loading.
- [Affects R2, R8][Technical] Decide how real-folder preview disables
  preimport side effects while still running the same gates.
- [Affects R5, R6][Technical] Decide whether the first API surface is a new
  endpoint or a compatibility expansion of `/api/pipeline/simulate`.
- [Affects R10][Technical] Decide where the ingestion-time triage hook runs and
  how it persists audit state after clearing actionable `failed_path` pointers.

---

## Next Steps

-> /ce-plan for structured implementation planning.
