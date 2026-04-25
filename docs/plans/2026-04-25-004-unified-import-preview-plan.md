---
title: "feat: Unified import preview and dry-run decision path"
type: feat
status: completed
date: 2026-04-25
origin: docs/brainstorms/import-preview-requirements.md
---

# feat: Unified import preview and dry-run decision path

## Overview

Create one authoritative import-preview path that answers "would this import?"
for both real files and typed simulation values. The work should fix
`harness/import_one.py --dry-run` so it runs through the real quality decision
before beets mutation, extract shared measured-decision logic so the harness
and simulator cannot drift, and expose thin CLI/API wrappers over the same
service.

The immediate product payoff is safer Wrong Matches triage: candidates that
would import can stay available for converge, while clear rejects can be
deleted and cleared without manually force-importing them first.

---

## Problem Frame

Wrong Matches contains folders that failed match validation but may still be
good enough when reviewed manually. Today the operator can force-import them,
but force-import is the expensive way to discover that many candidates fail
spectral or quality comparison anyway. The repo already has a typed simulator
(`full_pipeline_decision()`), a CLI/UI surface for hypothetical scenarios, and
the real harness decision code in `harness/import_one.py`, but these are not one
cohesive preview capability (see origin: `docs/brainstorms/import-preview-requirements.md`).

The plan is to make the actual import decision pipeline previewable without
beets mutation or source-folder mutation, then route synthetic simulation and
wrong-match triage through that same decision seam.

---

## Requirements Trace

- R1. Import preview uses the same decision pipeline as actual import for
  quality comparison, transcode handling, verified-lossless handling, target
  format handling, and post-import gate simulation.
- R2. Real-folder preview includes force/manual preimport gates: nested layout,
  audio validation, and spectral transcode detection.
- R3. `harness/import_one.py --dry-run` runs far enough to emit the same
  decision and measurement fields as a real import before beets mutation.
- R4. Typed override simulation uses the same measured-decision reducer as
  real-folder preview.
- R5. The CLI can preview a real folder by request or download-log row.
- R6. The API supports both real-folder preview and typed override simulation.
- R7. Preview results distinguish would-import, confident-reject, and
  uncertain/error outcomes.
- R8. Real-folder preview does not mutate source folders, beets, request state,
  spectral request state, denylist, import queue, or download log.
- R9. The Decisions UI simulation remains available while delegating through
  the unified seam.
- R10. Wrong-match triage deletes and clears only confident rejects.
- R11. Preview explanations expose the relevant decision stage chain.

**Origin actors:** A1 Operator, A2 Wrong Matches workflow, A3 Import decision
pipeline, A4 CLI/API/UI surfaces, A5 Implementer

**Origin flows:** F1 Real-folder preview, F2 Override-value simulation, F3
Wrong-match triage consumer

**Origin acceptance examples:** AE1 spectral reject preview, AE2 FLAC temp
conversion preview, AE3 override/API/UI equivalence, AE4 CLI output, AE5
conservative cleanup

---

## Scope Boundaries

- Do not change import policy, distance thresholds, spectral thresholds, rank
  policy, or beets matching.
- Do not create a second decision tree beside `lib.quality` and the harness.
  Compatibility wrappers may remain, but they must delegate to the shared seam.
- Do not add another beets-mutating worker or direct beets import entry point.
- Do not delete anything from the beets library.
- Do not persist preview-side spectral measurements to `album_requests`,
  `download_log`, or denylists.
- Do not make uncertain/error previews eligible for automatic cleanup.
- Do not require spectral caching as part of this work.

### Deferred to Follow-Up Work

- A fully asynchronous UI batch-triage runner. This plan exposes a
  single-candidate API, CLI batch command, and synchronous ingestion-time
  triage for newly created Wrong Matches rows; if the UI needs long-running
  multi-row triage, it should be queued or incrementally polled rather than run
  inside one page request.

---

## Context & Research

### Relevant Code and Patterns

- `CLAUDE.md` and `.claude/rules/code-quality.md` require import/quality
  decisions to stay pure where possible and forbid parallel subprocess paths.
- `lib.quality.full_pipeline_decision()` powers the current Decisions UI and
  `pipeline-cli quality` simulator, including stage names consumed by tests and
  the web contract.
- `harness/import_one.py` owns real file measurement: spectral analysis,
  conversion, new/existing measurements, `quality_decision_stage()`, and
  `ImportResult` emission.
- `harness/import_one.py --dry-run` currently exits before the quality
  comparison, so it is not a true import preview.
- `lib.import_dispatch.dispatch_import_core()` is the only production caller
  that launches `import_one.py`; any preview subprocess invocation should share
  a launcher extracted from this path.
- `lib.import_dispatch.dispatch_import_from_db()` already wraps force/manual
  import in `inspect_local_files()` and `run_preimport_gates()`. Preview should
  reuse the same gates with side effects disabled.
- `lib.preimport.run_preimport_gates()` can avoid DB persistence by not
  receiving a DB/request pair, while still running audio and spectral checks.
- `web/routes/pipeline.py::get_pipeline_simulate` and
  `tests/test_web_server.py::TestPipelineRouteDirectEquivalence` already prove
  the route returns the same result as `full_pipeline_decision()`.
- `scripts/pipeline_cli.py::cmd_quality` is the current CLI simulator. New CLI
  commands should preserve this operator workflow or provide a compatible
  replacement.
- `lib.wrong_matches.cleanup_wrong_match_source()` and
  `dismiss_wrong_match_source()` already centralize wrong-match delete/clear
  and non-deleting dismissal behavior.

### Institutional Learnings

- No `docs/solutions/` directory exists in this checkout.
- `.claude/memory/project_audio_quality_types.md` records why measurement-rich
  `ImportResult` fields and `pipeline-cli quality` exist: quality decisions
  need to be debuggable from structured measurements.
- `.claude/memory/feedback_use_nix_shell.md` records that Python commands and
  tests should run through `nix-shell` so audio/import dependencies are present.

### External References

- None. This is a repo-local refactor and feature over established import,
  simulator, and web-route patterns.

---

## Key Technical Decisions

- Extract shared measured-decision logic into `lib.quality`: project rules
  already designate it as the home for pure import/quality decisions.
- Keep `full_pipeline_decision()` as a compatibility adapter: it should build
  the new input shape and call the shared reducer so existing tests, routes, and
  UI code keep their contract while duplication is removed.
- Leave thin compatibility shims in `harness/import_one.py` where tests import
  existing helper names, but have the real logic delegate to `lib.quality`.
- Redefine `--dry-run` as "preview through decision, stop before mutation": the
  current early-exit behavior does not answer the operator's question.
- Run dry-run conversions in an isolated workspace: FLAC preview needs actual
  converted bitrate measurements, but source folders under `failed_imports/`
  must not be touched.
- Extract the `import_one.py` subprocess launcher from `dispatch_import_core()`:
  both real import and real-file preview need to call the same subprocess
  protocol without duplicating command construction.
- Make preview side-effect-free by default: pass no DB to preimport gates, skip
  `update_pipeline_db`, skip beets import, skip postflight, skip media scans,
  and avoid import queue writes.
- Classify cleanup eligibility outside the reducer: the preview reducer should
  decide import/reject/uncertain; wrong-match cleanup policy should remain in
  wrong-match triage code.

---

## Open Questions

### Resolved During Planning

- Should preview accept precomputed spectral from the DB? No. The first version
  measures real files when previewing real folders and accepts explicit typed
  spectral values only in override simulation mode.
- Should `--dry-run` keep its current early conversion dry-run meaning? No. It
  should become the authoritative no-mutation import preview.
- Should preview persist measured spectral to request rows? No. That would make
  preview stateful and risk claiming failed files are on disk.
- Should automatic ingestion-time triage run on new Wrong Matches rows?
  Yes. The download rejection path now invokes the conservative triage helper
  immediately after the rejected `download_log` row is created. Bad-file
  scenarios (`audio_corrupt`, `spectral_reject`) remain outside wrong-match
  cleanup policy.

### Deferred to Implementation

- Exact dataclass names for the measured-decision input/result. Choose names
  that fit `lib.quality` conventions and keep JSON serialization simple.
- Exact CLI flag shape for override mode. Prefer a `--values-json` escape hatch
  plus common convenience flags rather than a fragile giant positional command.
- Exact API endpoint name. A new `/api/import-preview` is likely clearer, while
  `/api/pipeline/simulate` can remain as a compatibility wrapper.
- Whether single-candidate wrong-match triage is exposed in the first PR's UI or
  only as CLI/API. Avoid a blocking multi-row web request either way.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for
> review, not implementation specification. The implementing agent should treat
> it as context, not code to reproduce.*

```mermaid
flowchart TD
    Values[Typed override values] --> Adapter[Preview input adapter]
    Folder[Request + real folder] --> Gates[inspect_local_files + run_preimport_gates no DB side effects]
    Gates --> DryRun[import_one.py --dry-run in isolated workspace]
    DryRun --> Measurements[ImportResult measurements]
    Adapter --> Measurements
    Measurements --> Reducer[lib.quality measured decision reducer]
    Reducer --> Result[Preview result: would_import / confident_reject / uncertain]
    Result --> CLI[pipeline-cli import-preview]
    Result --> API[/api/import-preview and /api/pipeline/simulate wrapper]
    Result --> WM[Wrong-match triage cleanup policy]
```

---

## Implementation Units

- U1. **Characterize Current Contracts and Desired Preview Outcomes**

**Goal:** Add failing or pending characterization around the current gaps so the
refactor has a safety net before moving logic.

**Requirements:** R1, R3, R4, R7, R8, R9, R11

**Dependencies:** None

**Files:**
- Modify: `tests/test_import_one_stages.py`
- Create: `tests/test_import_preview.py`
- Modify: `tests/test_quality_decisions.py`
- Modify: `tests/test_web_server.py`
- Modify: `tests/test_pipeline_cli.py`

**Approach:**
- Pin that current `--dry-run` is insufficient by adding the expected new
  behavior as RED tests: dry run reaches quality decision and emits
  `new_measurement`, `existing_measurement`, and a real decision.
- Add direct-equivalence tests for the new measured-decision reducer once its
  expected shape is sketched in tests.
- Add side-effect assertions for preview: no request status update, no
  download log write, no denylist write, and no source-folder mutation.
- Preserve existing `/api/pipeline/simulate` contract tests so compatibility
  remains visible during refactor.

**Execution note:** Characterization-first. This area has multiple historical
quality regressions, and project rules require orchestration slices for new
pipeline paths.

**Patterns to follow:**
- `tests/test_web_server.py::TestPipelineRouteDirectEquivalence`
- `tests/test_quality_decisions.py` subTest decision matrices
- `tests/test_dispatch_from_db.py` source-preservation assertions

**Test scenarios:**
- Happy path: dry-run import preview of an importable measured candidate returns
  `would_import`.
- Edge case: dry-run downgrade returns confident reject and does not call the
  beets harness import step.
- Error path: malformed or missing preview input returns uncertain/error rather
  than cleanup-eligible reject.
- Integration: `/api/pipeline/simulate` still matches direct library output.

**Verification:**
- Tests fail against the current early-exit dry-run behavior before the
  implementation units below are applied.

---

- U2. **Extract the Shared Measured Decision Reducer**

**Goal:** Remove duplicated measured-decision setup between `full_pipeline_decision()`
and `harness/import_one.py`.

**Requirements:** R1, R4, R7, R9, R11

**Dependencies:** U1

**Files:**
- Modify: `lib/quality.py`
- Modify: `harness/import_one.py`
- Modify: `tests/test_quality_decisions.py`
- Modify: `tests/test_import_one_stages.py`
- Modify: `tests/test_simulator_scenarios.py`
- Modify: `tests/test_quality_classification.py`

**Approach:**
- Introduce typed pure inputs/results in `lib.quality` for "measured import
  decision" data: new measurement, existing measurement, transcode flag,
  target-format context, preimport stage outcomes, and post-import gate inputs.
- Move or wrap pure helpers currently duplicated or harness-local, especially
  existing-measurement construction and quality decision stage mapping.
- Refactor `full_pipeline_decision()` so it translates legacy simulator kwargs
  into the new input shape, calls the reducer, and returns the existing dict
  shape.
- Refactor `harness/import_one.py` to build the same measured input from real
  file measurements and call the reducer before deciding whether to proceed.
- Keep compatibility helper names in the harness where tests or imports rely on
  them, but delegate to `lib.quality`.

**Patterns to follow:**
- `lib.quality.ImportResult` typed Struct patterns
- `tests/test_quality_decisions.py::TestFullPipelineDecision`
- `tests/test_import_one_stages.py::TestQualityDecisionStage`

**Test scenarios:**
- Happy path: override simulation and harness-measured input produce the same
  import decision for equivalent measurements.
- Edge case: existing CBR spectral override clamps avg/median exactly as the
  current harness behavior requires.
- Edge case: existing VBR spectral state does not clobber real avg/median.
- Error path: missing existing measurement remains a first-import decision.
- Integration: existing simulator scenario tests remain green without changing
  their expected stage names.

**Verification:**
- `full_pipeline_decision()` remains API-compatible while its core branch logic
  is delegated to the new reducer.

---

- U3. **Make `import_one.py --dry-run` a True No-Mutation Preview**

**Goal:** Let the real file-based harness run through spectral, conversion, and
quality comparison without touching the source folder, beets library, or DB.

**Requirements:** R1, R3, R7, R8, R11

**Dependencies:** U2

**Files:**
- Modify: `harness/import_one.py`
- Create: `tests/test_import_one_preview.py`
- Modify: `tests/test_conversion_e2e.py`
- Modify: `tests/helpers.py`

**Approach:**
- Change dry-run execution to operate on an isolated temporary copy of the
  source folder before any conversion or cleanup-prone step.
- Allow conversion to run normally inside the temp workspace so FLAC preview can
  measure real post-conversion bitrate.
- Stop after the shared measured decision reducer returns, before any beets
  import, postflight verification, DB update, source cleanup, target cleanup, or
  media-server trigger.
- Emit the normal `ImportResult` sentinel with decision, conversion details,
  spectral detail, new/existing measurements, and a clear dry-run/preview marker.
- Ensure temp workspace cleanup happens even when conversion or measurement
  fails.

**Patterns to follow:**
- `harness/import_one.py::_emit_and_exit`
- `tests/test_conversion_e2e.py` dry-run/source-preservation tests
- `tests/test_import_result.py` ImportResult serialization tests

**Test scenarios:**
- Happy path: MP3 preview computes new measurements and exits before beets
  import.
- Happy path: FLAC preview converts only in the temp workspace and reports
  post-conversion measurement.
- Edge case: `--preserve-source` plus `--dry-run` leaves both original source
  and temp cleanup in the expected state.
- Error path: conversion failure in preview emits a terminal result without DB
  updates.
- Integration: source directory checksums or file lists remain unchanged after
  preview.

**Verification:**
- No dry-run test calls the beets harness import path or `update_pipeline_db()`.

---

- U4. **Add the Import Preview Service and Shared Harness Runner**

**Goal:** Provide one library entry point for real-folder preview and typed-value
preview, with no duplicate subprocess construction.

**Requirements:** R1, R2, R5, R6, R7, R8, R10, R11

**Dependencies:** U2, U3

**Files:**
- Create: `lib/import_preview.py`
- Modify: `lib/import_dispatch.py`
- Modify: `lib/preimport.py`
- Modify: `tests/test_import_preview.py`
- Modify: `tests/test_dispatch_core.py`
- Modify: `tests/test_dispatch_from_db.py`
- Modify: `tests/fakes.py`

**Approach:**
- Extract import-one subprocess command construction and sentinel parsing from
  `dispatch_import_core()` into a shared runner used by both dispatch and
  preview. Dispatch remains the owner of mutation; the runner only executes the
  harness protocol.
- Add `preview_import_from_path()` that resolves request context, repairs and
  inspects files, rejects nested layouts consistently with force/manual import,
  runs `run_preimport_gates()` with side effects disabled, computes any
  effective existing override needed for the harness, and calls the shared
  runner with dry-run enabled.
- Add `preview_import_from_values()` that accepts typed override values and
  calls the shared measured-decision reducer through the same result-normalizing
  code path.
- Normalize both modes into a typed preview result with stage chain,
  `would_import`, `confident_reject`, `uncertain`, `cleanup_eligible`,
  `ImportResult` fields when available, and human-readable reason/detail.
- Keep cleanup policy conservative: only preimport rejects and terminal
  downgrade/transcode-downgrade style outcomes are cleanup-eligible; errors,
  missing paths, parse failures, lock contention, and unknown decisions are
  uncertain.

**Patterns to follow:**
- `lib.import_dispatch.DispatchOutcome`
- `lib.import_service.parse_import_result_stdout`
- `lib.wrong_matches.WrongMatchCleanupResult`
- `lib.preimport.PreImportGateResult`

**Test scenarios:**
- Happy path: real-folder preview returns would-import and does not write DB
  state.
- Happy path: override-value preview returns the same reducer output as direct
  `full_pipeline_decision()` for equivalent inputs.
- Edge case: nested real folder returns confident reject before harness runner.
- Edge case: spectral reject returns confident reject and does not denylist.
- Error path: harness emits no JSON and preview returns uncertain/error.
- Integration: dispatch import still constructs the same command flags as before
  when not in preview mode.

**Verification:**
- There is exactly one shared helper responsible for launching `import_one.py`.

---

- U5. **Expose Unified CLI and API Surfaces**

**Goal:** Make the preview usable by operators and keep existing simulator
surfaces compatible.

**Requirements:** R5, R6, R7, R9, R11

**Dependencies:** U4

**Files:**
- Modify: `scripts/pipeline_cli.py`
- Modify: `web/routes/pipeline.py`
- Modify: `web/routes/imports.py`
- Modify: `web/js/decisions.js`
- Modify: `tests/test_pipeline_cli.py`
- Modify: `tests/test_web_server.py`
- Modify: `tests/test_js_decisions.mjs`

**Approach:**
- Add `pipeline-cli import-preview` with real-file modes:
  `--request-id --path` and `--download-log-id`, plus JSON output for tooling.
- Add override simulation mode using a structured payload (`--values-json`) and
  common convenience flags for the values operators already use in the Decisions
  UI.
- Add a new API route, likely `/api/import-preview`, that accepts either
  real-folder/download-log input or typed override values and returns the common
  preview result shape.
- Refactor `/api/pipeline/simulate` to call the same preview/value adapter while
  preserving its current response shape for the Decisions UI.
- Update `cmd_quality` only where useful: keep its human-friendly common
  scenario output, but route scenario evaluation through the new value-preview
  adapter.

**Patterns to follow:**
- `scripts/pipeline_cli.py::cmd_force_import`
- `scripts/pipeline_cli.py::cmd_quality`
- `web/routes/pipeline.py::get_pipeline_simulate`
- `tests/test_web_server.py::TestPipelineRouteDirectEquivalence`

**Test scenarios:**
- Happy path: CLI `--download-log-id` resolves the failed path and prints a
  would-import/confident-reject verdict.
- Happy path: CLI `--values-json` returns the same preview result as direct
  library invocation.
- Edge case: API rejects requests that mix path mode and values mode
  ambiguously.
- Error path: API returns uncertain/error JSON for missing folders without
  clearing wrong-match rows.
- Integration: `/api/pipeline/simulate` remains backward compatible with the
  Decisions UI query parameters.

**Verification:**
- The Decisions tab still renders existing scenario output, and direct route
  equivalence tests remain green.

---

- U6. **Add Conservative Wrong-Match Triage Consumer**

**Goal:** Use the unified preview to delete only clear wrong-match rejects while
leaving importable and uncertain candidates available for operator review.

**Requirements:** R2, R7, R8, R10, R11

**Dependencies:** U4, U5

**Files:**
- Create: `lib/wrong_match_triage.py`
- Modify: `lib/wrong_matches.py`
- Modify: `web/routes/imports.py`
- Modify: `scripts/pipeline_cli.py`
- Modify: `tests/test_wrong_matches_cleanup.py`
- Create: `tests/test_wrong_match_triage.py`
- Modify: `tests/test_web_server.py`
- Modify: `tests/test_pipeline_cli.py`

**Approach:**
- Add a small triage helper that takes a `download_log_id`, runs
  real-folder preview, and applies cleanup only when the preview result is a
  confident reject with `cleanup_eligible=True`.
- Call that helper synchronously from the download rejection path immediately
  after the rejected `download_log` row is written, excluding bad-file
  scenarios that are not Wrong Matches candidates.
- Persist each triage action and reason under
  `download_log.validation_result.wrong_match_triage` so cleanup decisions can
  be audited after the row leaves the actionable Wrong Matches view.
- Keep would-import rows visible/actionable; do not dismiss or delete them.
  They are already "staged" in Wrong Matches for converge.
- Keep uncertain/error rows visible and return the preview reason so the
  operator can decide manually.
- Expose a CLI batch mode for operational cleanup, such as triaging all
  actionable rows for a request or a bounded number of rows. The CLI can be
  long-running; it should print counts and per-row reasons.
- Expose a single-candidate API first. Avoid a blocking multi-row UI route until
  the execution model is explicit.

**Patterns to follow:**
- `lib.wrong_matches.cleanup_wrong_match_source`
- `web/routes/imports.py` wrong-match delete/converge endpoint patterns
- `docs/plans/2026-04-25-003-wrong-matches-converge-workflow-plan.md`

**Test scenarios:**
- Happy path: confident spectral reject deletes folder and clears failed-path
  pointers.
- Happy path: would-import preview leaves folder and wrong-match row intact.
- Edge case: preview error leaves folder and row intact.
- Edge case: missing folder clears only when classified as stale-path cleanup,
  not as a successful preview reject.
- Integration: API response includes per-row action, reason, and cleanup result
  without requiring a page reload.

**Verification:**
- Triage never deletes a candidate unless the preview result says
  `cleanup_eligible=True`.

---

## System-Wide Impact

- **Interaction graph:** `pipeline-cli`, `web/routes/pipeline.py`,
  `web/routes/imports.py`, Wrong Matches cleanup, `lib.import_dispatch`, and
  `harness/import_one.py` all become consumers of the preview/decision seam.
- **Error propagation:** Preview errors return uncertain/error verdicts and
  must not masquerade as confident rejects.
- **State lifecycle risks:** Preview must not persist request status, spectral
  fields, denylists, import jobs, or download-log rows. Triage cleanup is the
  only allowed DB/file side effect, and only after preview classification.
- **API surface parity:** CLI, API, and Decisions UI should expose the same
  stage names and verdict categories, even if their human presentation differs.
- **Integration coverage:** Real-folder preview needs orchestration tests with
  fake DB plus mocked external audio/beets edges; pure reducer tests alone are
  not enough.
- **Unchanged invariants:** Actual imports still go through the importer queue
  and `dispatch_import_core()`. Preview never imports to beets, never calls
  raw `beet import`, and never deletes beets-library files.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Dry-run conversion mutates the wrong-match source folder | Always preview from a temp copy and assert source file lists remain unchanged |
| Simulator and harness continue to drift | Put measured-decision logic in `lib.quality` and make both paths delegate |
| A second import-one subprocess launcher appears | Extract one shared runner from `dispatch_import_core()` and reuse it |
| Preview accidentally persists spectral or denylist state | Run preimport gates without DB side effects and add fake DB assertions |
| API batch preview blocks web requests for minutes | Start with single-candidate API and CLI batch; queue or poll any future UI batch runner |
| Confident reject classification deletes useful files | Cleanup only when preview is terminal and explicit `cleanup_eligible=True`; uncertain results stay visible |
| Dual module loading breaks enum/type comparisons | Import through `lib.quality` consistently and keep direct-equivalence route tests |

---

## Documentation / Operational Notes

- Update `docs/quality-ranks.md` to describe `pipeline-cli import-preview` and
  the relationship to `pipeline-cli quality`.
- Update `docs/webui-primer.md` if the Decisions tab or Wrong Matches adds a
  visible preview/triage action.
- Note in operator docs that preview may run spectral and conversion work, so
  real-folder preview is not a cheap metadata-only command.
- Run Python verification through `nix-shell --run "..."` so audio and DB
  dependencies are available.

---

## Sources & References

- **Origin document:** `docs/brainstorms/import-preview-requirements.md`
- Existing simulator: `lib/quality.py::full_pipeline_decision`
- Existing CLI simulator: `scripts/pipeline_cli.py::cmd_quality`
- Existing simulation API: `web/routes/pipeline.py::get_pipeline_simulate`
- Real import harness: `harness/import_one.py`
- Import dispatcher: `lib/import_dispatch.py::dispatch_import_core`
- Preimport gates: `lib/preimport.py::run_preimport_gates`
- Wrong-match cleanup helpers: `lib/wrong_matches.py`
- Related plan: `docs/plans/2026-04-25-003-wrong-matches-converge-workflow-plan.md`
