---
title: "refactor: Reuse preview evidence for import mutation"
type: refactor
status: active
date: 2026-05-15
origin: docs/brainstorms/2026-05-14-quality-evidence-decision-boundary-requirements.md
supersedes: docs/plans/2026-05-14-001-refactor-quality-evidence-decision-boundary-plan.md
---

# refactor: Reuse preview evidence for import mutation

## Summary

The first evidence/decision-boundary PR made progress on safety: mutating
import now rejects stale preview candidates before Beets mutation when stored
candidate evidence and fresh current evidence prove the candidate is worse.

It did not complete the refactor's performance invariant. Snapshot-valid
candidate evidence from async preview is currently used only as an early reject
gate. If the candidate is allowed, import still falls through to the normal
`harness/import_one.py` path, which reruns candidate spectral analysis, V0
probing, bitrate measurement, verified-lossless source probing, and quality
decision work. That violates R28-R32 and AE10 in the requirements.

This plan fixes the architecture: async preview owns candidate measurement;
import owns fresh decision computation and Beets mutation. If the candidate
fileset snapshot still matches, import reuses stored candidate evidence and
must not remeasure the candidate.

## Problem Frame

The intended model is simple:

- candidate evidence is durable work product;
- decisions are ephemeral and recomputed at action time;
- current evidence is refreshed/backfilled at action time;
- Beets mutation only happens after the fresh evidence-pair decision allows it.

The current branch has the first and third pieces partially in place, but the
mutation path remains coupled to the measuring harness. The consequence is a
safe-but-wasteful flow:

1. async preview measures candidate and stores evidence;
2. importer validates that evidence and recomputes the decision;
3. if the decision allows import, `import_one.py` measures the same unchanged
   candidate again before mutating Beets.

The corrective refactor must make "recompute the decision" mean exactly that:
recompute the policy output from existing evidence. It must not imply
remeasuring unchanged candidate files.

## Current Shape

The current PR branch adds these useful pieces:

- `lib/quality.py::AlbumQualityEvidence` and
  `full_pipeline_decision_from_evidence`
- `lib/quality_evidence.py` snapshot, candidate/current evidence builders, and
  load/backfill helpers
- migration `migrations/017_album_quality_evidence.sql`
- `scripts/import_preview_worker.py` opt-in candidate evidence persistence
- `lib/import_dispatch.py` import-time evidence rejection before subprocess
- `lib/wrong_match_cleanup_decision.py` cleanup recomputation service shell

The remaining duplication points are:

- `lib/import_preview.py::preview_import_from_path` measures the candidate
  through preimport gates and `run_import_one(..., dry_run=True)`.
- `lib/import_dispatch.py::_dispatch_import_from_db_locked` still runs
  `inspect_local_files` and `run_preimport_gates` before evidence loading on
  force/manual import.
- `lib/download.py::_process_beets_validation` still runs
  `run_preimport_gates` before dispatch on automation import.
- `lib/import_dispatch.py::dispatch_import_core` recomputes the evidence
  decision, but allowed candidates still call normal `run_import_one`.
- `harness/import_one.py` still combines candidate measurement, quality
  decision, source materialization, Beets mutation, postflight validation, and
  result emission in one path.
- `lib/wrong_match_cleanup_decision.py` recomputes cleanup by calling preview
  again rather than using snapshot-valid `download_log_candidate` evidence.

## Scope

In scope:

- Reuse snapshot-valid candidate evidence for automation, manual import, force
  import, and Wrong Matches cleanup.
- Recompute import/cleanup decisions at action time from candidate evidence,
  current evidence, policy/config, and narrow action context.
- Add a mutation-only or evidence-authorized harness mode that does not run
  candidate measurement functions when candidate evidence is valid.
- Add tests that fail if snapshot-valid preview evidence causes import to call
  candidate measurement functions again.
- Preserve force import as distance bypass only.
- Preserve existing quality policy behavior and simulator outcomes.

Out of scope:

- Caching converted audio artifacts from preview. Import may still perform
  conversions needed to materialize final files for Beets; those conversions are
  not evidence recomputation.
- Retuning quality thresholds, spectral thresholds, codec ranks, provisional
  lossless policy, or verified-lossless policy.
- Cross-request or cross-release evidence reuse.
- Content hashes or audio fingerprints beyond the current cheap fileset
  snapshot.
- A provenance UI. Provenance must be stored and testable, but UI surfacing can
  remain minimal.

## Requirements Traceability

- R3, R11, R22: preview/triage verdicts remain audit-only and cannot authorize
  mutation or cleanup.
- R6, R20, R23: missing/stale/incomplete evidence recomputes or fails closed;
  decisions are action-time.
- R21: force import only bypasses Beets distance/match gating.
- R24, R32: action outputs record reused/recomputed/backfilled evidence and
  fallback reasons.
- R28, R29: snapshot-valid candidate evidence is reused; unchanged candidates
  are not remeasured.
- R30, R31: async preview produces the evidence artifact import consumes, and
  Beets mutation is separable from candidate measurement.
- AE1, AE7, AE8, AE10: stale preview cannot mutate Beets, snapshot mismatch
  recomputes/fails closed, poisoned legacy blobs are ignored, and valid preview
  evidence prevents duplicate candidate measurement.

## Key Decisions

- Keep policy authority in `lib/quality.py`. Import and cleanup call
  `full_pipeline_decision_from_evidence`; the harness does not recompute the
  quality decision in evidence-backed mutation mode.
- Introduce one import-time evidence acquisition service. It loads
  candidate/current evidence, validates snapshots, recomputes/backfills only
  when required, and returns action provenance.
- Treat a valid candidate evidence row as proof that preview's expensive
  candidate gates passed for that exact fileset. If the snapshot is unchanged,
  force/manual and automation paths skip candidate preimport measurement.
- Add an evidence-authorized harness mode rather than a separate Beets mutation
  subprocess. Reuse `build_import_one_command` / `run_import_one` plumbing, but
  pass an action-time evidence plan generated by dispatch, not a stored preview
  result.
- The evidence-authorized harness mode still validates the source snapshot
  immediately before mutation. A last-mile mismatch fails closed without Beets
  mutation.
- Materialization is distinct from measurement. Import may convert/copy/remove
  files to prepare the final Beets payload, but those steps must not feed a new
  candidate quality decision when evidence is valid.
- Legacy `preview_status='would_import'` remains compatibility/audit language
  only. Active readiness should move toward neutral wording such as
  `evidence_ready`.

## Existing Patterns To Follow

- `lib.quality` pure reducer style for quality decisions.
- `msgspec.Struct` for subprocess/JSONB boundary payloads in `lib/quality.py`.
- `lib.import_dispatch.run_import_one` and `build_import_one_command` as the
  subprocess seam.
- `lib.quality_evidence.load_candidate_evidence_for_source` for owner-scoped
  snapshot validation.
- `tests/helpers.py::make_album_quality_evidence`,
  `tests/fakes.py::FakePipelineDB`, and dispatch test helpers for
  production-shaped test rows.
- `docs/beets-primer.md`: preserve the Beets wrapper protocol and test harness
  action safety.
- `docs/solutions/testing/contract-test-mocks-must-mirror-production-shape.md`
  and `docs/solutions/testing/mocked-contract-tests-miss-helper-mirror-integration-bugs.md`:
  add integration slices, not only mocks.

## Implementation Units

### U1. Add RED No-Remeasurement Regressions

**Goal:** Make the current gap fail before refactoring it.

**Requirements:** R28, R29, R30, R31, R32, AE10.

**Files:**

- Modify: `tests/test_dispatch_core.py`
- Modify: `tests/test_dispatch_from_db.py`
- Modify: `tests/test_import_queue.py`
- Modify: `tests/test_import_one_stages.py`
- Modify: `tests/test_integration_slices.py`

**Approach:**

- Add an allowed-import evidence test in `tests/test_dispatch_core.py` where
  candidate evidence is snapshot-valid and the fresh evidence decision imports.
  The test must assert the legacy measuring `run_import_one` path is not used
  for candidate measurement once the mutation-only seam exists.
- Add non-contended force/manual tests in `tests/test_dispatch_from_db.py`
  proving snapshot-valid `download_log_candidate` or `import_job_candidate`
  evidence skips `inspect_local_files` and `run_preimport_gates`.
- Add automation queue coverage proving a previewed automation job reuses
  `import_job_candidate` evidence and does not rerun candidate preimport gates.
- Add harness-stage tests that patch or sentinel candidate measurement helpers
  in `harness/import_one.py` and fail if evidence-backed mutation calls
  spectral analysis, V0 probe generation, candidate bitrate probing, or quality
  decision helpers.
- Add one integration slice for preview worker -> evidence store -> importer
  with valid evidence, asserting the fresh decision runs and candidate
  measurement does not rerun.

**Test Scenarios:**

- Valid preview evidence + unchanged files + importable decision -> no
  candidate measurement calls before Beets mutation.
- Valid preview evidence + stronger current evidence -> import rejects without
  Beets mutation and without candidate remeasurement.
- Stale candidate snapshot -> candidate evidence recomputes; if recompute
  fails, import fails closed.
- Legacy preview `would_import` row with no relational evidence -> action path
  recomputes evidence and records provenance.

### U2. Create Evidence Acquisition and Provenance Service

**Goal:** Centralize "load, validate, reuse, recompute, or fail" logic so
preview, import, and cleanup do not each invent evidence handling.

**Requirements:** R4, R5, R6, R20, R23, R24, R28, R29, R32, AE1, AE7, AE10.

**Files:**

- Modify: `lib/quality_evidence.py`
- Create or modify: `lib/import_evidence.py`
- Modify: `lib/pipeline_db.py`
- Modify: `tests/test_quality_evidence.py`
- Modify: `tests/test_pipeline_db.py`
- Modify: `tests/fakes.py`

**Approach:**

- Add an action-facing result type that reports:
  - candidate evidence status: `reused`, `recomputed`, `missing`, `stale`,
    `incomplete`, `failed`
  - current evidence status: `loaded`, `backfilled`, `missing`, `failed`
  - snapshot guard result
  - fallback reason
- Implement `ensure_candidate_evidence_for_action`:
  - first load owner-scoped evidence and validate the source snapshot;
  - if valid, return it as `reused`;
  - if missing/stale/incomplete and recompute is allowed, call the existing
    measurement path to build new evidence and persist it;
  - if recompute fails for a mutating/destructive action, return fail-closed
    provenance.
- Implement `ensure_current_evidence_for_action`:
  - load `request_current` evidence;
  - validate it against the current Beets album path when an album exists;
  - backfill from Beets if missing, stale, or incomplete;
  - fail closed if current album exists but evidence cannot be obtained.
- Keep legacy scalar reads limited to documented one-time seeding. They are not
  decision inputs once relational evidence exists.

**Test Scenarios:**

- Reused candidate evidence returns `candidate_status='reused'` and does not
  call the measurement builder.
- Stale snapshot calls the measurement builder exactly once and records
  `candidate_status='recomputed'`.
- Measurement failure returns a fail-closed result with the reason preserved.
- Current evidence with matching Beets files loads; stale or missing current
  evidence backfills from Beets.
- Fake DB and real DB produce the same owner/snapshot/provenance behavior.

### U3. Route Import Orchestration Through Evidence Acquisition

**Goal:** Make force/manual and automation import skip candidate preimport
measurement when valid candidate evidence exists.

**Requirements:** R6, R20, R21, R24, R28, R29, R30, AE1, AE2, AE7, AE10.

**Files:**

- Modify: `lib/import_dispatch.py`
- Modify: `scripts/importer.py`
- Modify: `lib/download.py`
- Modify: `tests/test_dispatch_from_db.py`
- Modify: `tests/test_dispatch_core.py`
- Modify: `tests/test_import_queue.py`
- Modify: `tests/test_integration_slices.py`

**Approach:**

- In `dispatch_import_from_db`, check owner-scoped candidate evidence before
  running force/manual preimport gates. If the evidence is snapshot-valid, build
  `DownloadInfo` from stored evidence plus source attribution and skip
  `inspect_local_files` / `run_preimport_gates`.
- If candidate evidence is missing/stale/incomplete, run the shared evidence
  acquisition service to recompute it. That recompute may use current
  measurement paths because evidence is not valid.
- In automation, move the same evidence check before
  `lib.download._process_beets_validation` calls `run_preimport_gates`. A
  previewed `import_job_candidate` with a matching snapshot skips preimport
  measurement.
- `dispatch_import_core` receives an evidence acquisition result, not just raw
  owner IDs. It runs `full_pipeline_decision_from_evidence` once from those
  evidence objects.
- Keep Beets distance/match validation separate. Force bypasses only that gate;
  evidence validation and decision recomputation remain identical across
  force/manual/automation.

**Test Scenarios:**

- Force import with valid `download_log_candidate` evidence bypasses distance
  and skips candidate preimport gates, then recomputes the decision.
- Manual import with valid `import_job_candidate` evidence skips candidate
  preimport gates.
- Automation import with valid preview evidence skips automation preimport
  gates.
- Missing evidence falls back to action-time measurement and persists the new
  candidate evidence.
- Stale evidence fails closed if recomputation cannot complete.

### U4. Add Evidence-Authorized Mutation Mode To The Harness

**Goal:** Split the mutating harness path from candidate measurement and
quality-decision recomputation.

**Requirements:** R20, R21, R28, R29, R30, R31, R32, AE1, AE7, AE8, AE10.

**Files:**

- Modify: `harness/import_one.py`
- Modify: `lib/import_dispatch.py`
- Modify: `lib/quality.py`
- Modify: `tests/test_import_one_stages.py`
- Modify: `tests/test_dispatch_core.py`
- Modify: `tests/test_dispatch_from_db.py`

**Approach:**

- Define a `msgspec.Struct` action payload, generated at import time by
  dispatch, not persisted preview. It should include:
  - candidate evidence snapshot/files and measurement summary;
  - current evidence summary used for the action decision;
  - the action-time decision result from `full_pipeline_decision_from_evidence`;
  - target/materialization inputs required to prepare the Beets payload;
  - provenance statuses from U2.
- Extend `build_import_one_command` and `run_import_one` with an
  evidence-authorized action file flag. Do not revive
  `--preview-import-result-file`.
- In `harness/import_one.py`, parse the action payload and enter a dedicated
  evidence-backed branch:
  - validate the candidate fileset snapshot immediately;
  - fail closed before mutation if the snapshot mismatches;
  - skip candidate spectral analysis, V0 probing, candidate bitrate probing,
    verified-lossless source probing, and quality-decision helpers;
  - perform only materialization needed for Beets import, such as required
    conversions, target conversion, source cleanup, and duplicate guard;
  - run `run_import`;
  - perform postflight validation;
  - emit an `ImportResult` seeded from the action-time evidence/decision plus
    postflight data.
- Keep current dry-run/preview measurement behavior for evidence creation.
  The new path is for action-time mutation when evidence is already valid.

**Important Boundary:**

Conversions needed to create the final files are still allowed in the
evidence-backed branch. They are materialization, not decision evidence. The
harness must not use their measured bitrate/spectral/V0 output to change the
already recomputed action decision. Post-import current evidence is measured
from final Beets files by dispatch after mutation.

**Test Scenarios:**

- Evidence-backed harness mode does not call spectral analysis, V0 probe,
  candidate bitrate probing, `determine_verified_lossless`,
  `provisional_lossless_decision`, or `quality_decision_stage`.
- Evidence-backed harness mode fails closed before `run_import` when the source
  snapshot mismatches.
- Evidence-backed harness mode still performs required target conversion and
  source cleanup for accepted imports.
- The old `--preview-import-result-file` remains absent from CLI help and
  parser behavior.
- Subprocess decoding keeps `errors="replace"` on all import runner paths.

### U5. Refresh Current Evidence After Mutation Without Copying Candidate Facts

**Goal:** Ensure successful imports update current evidence from final Beets
files while carrying forward only valid source-proof classification.

**Requirements:** R5, R15, R16, R17, R18, R19, R24, AE3.

**Files:**

- Modify: `lib/import_dispatch.py`
- Modify: `lib/quality_evidence.py`
- Modify: `tests/test_quality_evidence.py`
- Modify: `tests/test_dispatch_core.py`
- Modify: `tests/test_conversion_e2e.py`
- Modify: `tests/test_pipeline_db.py`

**Approach:**

- Keep `request_current` evidence measured from Beets final files after import.
- Add explicit source-proof carry-forward logic:
  - if the accepted candidate evidence proves `verified_lossless=true`, carry
    that proof provenance into current evidence;
  - do not copy candidate codec/container/bitrate/spectral/V0 facts into
    current evidence after conversion;
  - lossy backfill cannot flip an existing true proof false.
- Record provenance for current evidence status: loaded, backfilled, refreshed,
  proof-carried-forward, or failed.

**Test Scenarios:**

- Lossless source imported to Opus stores current Opus facts and true source
  proof provenance.
- Later lossy current backfill preserves the true proof and does not recompute
  it false.
- Imported MP3 candidate cannot create or alter verified-lossless proof.
- Post-import current evidence write failure records a failed refresh result
  without treating candidate evidence as current evidence.

### U6. Route Wrong Matches Cleanup Through Evidence, Not Preview

**Goal:** Make cleanup reuse snapshot-valid `download_log_candidate` evidence
instead of rerunning preview when evidence is already valid.

**Requirements:** R6, R22, R23, R24, R28, R29, R32, AE6, AE7, AE9.

**Files:**

- Modify: `lib/wrong_match_cleanup_decision.py`
- Modify: `web/routes/imports.py`
- Modify: `scripts/importer.py`
- Modify: `tests/test_import_queue.py`
- Modify: `tests/test_web_server.py`
- Modify: `tests/test_wrong_matches_cleanup.py`
- Modify: `tests/test_quality_evidence.py`

**Approach:**

- Replace `preview_import_from_download_log` as the normal cleanup authority
  with the shared evidence acquisition service.
- For cleanup:
  - use `download_log.validation_result.failed_path` only to locate files;
  - load/validate `download_log_candidate` evidence;
  - recompute candidate evidence only when missing/stale/incomplete;
  - load/backfill current evidence;
  - run the cleanup decision from the evidence pair and cleanup context;
  - fail uncertain when required evidence cannot be obtained.
- Keep preview rerun only as an evidence recomputation mechanism when no valid
  candidate evidence exists, never as stored verdict authority.
- Preserve failed force-import cleanup routing through the same service.

**Test Scenarios:**

- Wrong Matches delete with valid candidate evidence does not call preview and
  records `candidate_status='reused'`.
- Cleanup with stale candidate snapshot recomputes evidence or leaves files
  visible if recomputation fails.
- Bulk cleanup, converge cleanup, direct delete, and failed force-import cleanup
  all use the same decision service.
- Stored `cleanup_eligible=true` or `would_import=false` preview verdict cannot
  delete files without fresh evidence.

### U7. Neutralize Active Preview Readiness Language

**Goal:** Make queue state and API wording stop implying preview authorization.

**Requirements:** R22, R24, R30, AE5, AE8.

**Files:**

- Modify: `lib/import_queue.py`
- Modify: `lib/pipeline_db.py`
- Modify: `scripts/import_preview_worker.py`
- Modify: `scripts/importer.py`
- Modify: `web/js/recents.js`
- Modify: `tests/test_import_queue.py`
- Modify: `tests/test_js_recents.mjs`
- Modify: `docs/pipeline-db-schema.md`

**Approach:**

- Add a neutral active readiness status, such as `evidence_ready` or
  `preview_complete`.
- Keep legacy `preview_status='would_import'` claimable for existing rows, but
  treat it as compatibility/audit only.
- Update worker messages to say "evidence ready" or "ready for final check"
  rather than "would import" for active jobs.
- Preserve historical display text where it is explicitly showing an old audit
  verdict.

**Test Scenarios:**

- New preview worker success writes neutral readiness status.
- Importer claims neutral readiness and legacy `would_import` compatibility
  rows.
- Stored preview verdict text remains visible in history but is not used as
  authority.
- Recents/queue JS renders the new status without implying import approval.

## Sequencing

1. Land U1 first. The no-remeasurement tests must fail on the current branch.
2. Implement U2 so import and cleanup share evidence acquisition/provenance.
3. Implement U3 so force/manual/automation stop premeasuring valid candidates.
4. Implement U4 so accepted imports use evidence-authorized mutation instead of
   the normal measuring harness path.
5. Implement U5 to ensure successful imports refresh current evidence from
   final Beets files and preserve only valid proof provenance.
6. Implement U6 so cleanup gets the same evidence reuse/fail-closed semantics.
7. Implement U7 after the behavior is correct; it removes misleading active
   terminology and protects future readers.

U1-U4 are ship-blocking for the async-preview efficiency invariant. U5-U6 are
ship-blocking for correctness of current evidence and cleanup. U7 is not
behavioral authority, but should land in the same PR if it stays small.

## Verification Strategy

- Run focused Python tests after each unit:
  - `tests.test_quality_evidence`
  - `tests.test_quality_decisions`
  - `tests.test_import_preview`
  - `tests.test_dispatch_core`
  - `tests.test_dispatch_from_db`
  - `tests.test_import_queue`
  - `tests.test_import_one_stages`
  - `tests.test_web_server.TestWrongMatchesContract`
  - `tests.test_wrong_matches_cleanup`
- Run simulator scenarios after reducer/evidence changes:
  - `tests.test_simulator_scenarios`
  - `tests.test_js_decisions`
- Run conversion and harness tests after U4/U5:
  - `tests.test_conversion_e2e`
  - `tests.test_import_one_stages`
- Run integration slices before PR review:
  - `tests.test_integration_slices`
- Run full checks before merge:
  - `python3 -m unittest discover tests -v`
  - JS sweep for `tests/test_js_*.mjs`
  - `pyright` on touched Python paths, then broader pyright if existing noise is
    not blocking.

## Risks

| Risk | Mitigation |
| --- | --- |
| Mutation-only harness accidentally trusts stale preview authority | The action payload is generated at import time from fresh evidence decision, and the harness validates the candidate snapshot immediately before mutation. |
| Skipping preimport gates hides corrupt files | Snapshot-valid evidence means preview already measured that exact fileset. Stale/missing evidence recomputes through the measurement path. Tests must prove recompute on mismatch. |
| Target conversion changes final facts | Treat conversion as materialization. Refresh current evidence from final Beets files after import; do not use conversion measurements to change the import decision. |
| Harness mode forks into a second import implementation | Extend the existing `run_import_one` subprocess seam and share `run_import`, postflight validation, duplicate guard, cleanup, and result emission. |
| Mocks miss boundary drift | Add integration slices for preview worker -> evidence store -> importer and for all destructive cleanup paths. |
| Legacy rows without relational evidence stop draining | Missing evidence path recomputes candidate evidence at action time; legacy preview/status fields remain compatibility only. |

## Rollout Notes

- Deploy migration `017_album_quality_evidence.sql` before workers that require
  evidence-backed import.
- Stop old preview/import workers before deploying code that changes queue
  readiness semantics.
- Watch importer logs for evidence fail-closed reasons after deploy. These are
  missing/stale evidence bugs to investigate, not permission to trust old
  preview decisions.
- If U4 is not complete, do not call the refactor complete. A PR that only
  rejects stale candidates before `import_one.py` is safe-but-wasteful and does
  not satisfy R28-R32.

## Open Questions

- Should the neutral readiness status be `evidence_ready` or
  `preview_complete`? This affects display wording only; behavior must not wait
  on the name.
- Should action-time recomputation for missing candidate evidence use
  `preview_import_from_path` as an internal measurement helper initially, or
  should measurement be extracted into a smaller evidence builder first? The
  long-term direction is the smaller builder; the implementation may choose the
  least risky intermediate step if tests enforce no remeasurement when evidence
  is valid.
