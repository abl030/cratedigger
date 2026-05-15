---
title: "refactor: Preview never decides — symmetric closure of the evidence/decision boundary"
type: refactor
status: active
date: 2026-05-16
origin: docs/brainstorms/2026-05-16-preview-never-decides-requirements.md
extends: docs/plans/2026-05-15-002-refactor-importer-never-measures-plan.md
---

# Preview Never Decides — Symmetric Closure of the Evidence/Decision Boundary

## Summary

The third leg of the evidence/decision refactor: split `lib/preimport.py::run_preimport_gates` into a pure measurement helper and a pure decision function in `lib/quality.py`, reclassify every preview-side verdict-emitting exit as either an `evidence_ready` row carrying typed facts or a `measurement_failed` outcome that self-heals the parent request to `wanted`, fix `validate_audio`'s stderr-policy false-positive, and recover the currently-stuck rows on deploy. Closes #250 (stuck-forever symptom), #251 (`validate_audio` false-positives), and #252 (preview-decides architectural smell) on the same merge — three faces of the same pathology.

---

## Problem Frame

The 2026-05-14 evidence/decision-boundary refactor and the 2026-05-15 importer-never-measures refactor enforced the importer half of the architectural contract strictly, but the symmetric preview half was specified only implicitly. The implementation honored the literal R5 (`"measure, persist evidence, mark the job ready"`) but the fused measure-and-decide function `lib/preimport.py::run_preimport_gates` continued to set `valid=False` + `scenario` on audio_corrupt / bad_audio_hash / spectral_reject, which `lib/import_preview.py:380-393` translates into `verdict="confident_reject"` and `preview_status='uncertain'`. Roughly a dozen other preview exits (`nested_layout`, `evidence_empty_fileset`, `evidence_invalid_snapshot`, materialization errors) are decisions under a different name. Observable today: 315 import_jobs sit in `preview_status='uncertain'`, the parent `album_request` rows are locked in `status='downloading'`, and the poll loop refuses to resume search because an "active" import job exists. See origin for the full architectural narrative.

---

## Requirements

- R1. Preview emits exactly two terminal outputs per claimed job: `evidence_ready` and `measurement_failed`. No `verdict='confident_reject'`, no `verdict='uncertain'`, no dangling `preview_status='uncertain'`. (see origin: R1)
- R2. `run_preimport_gates` is split into a pure measurement helper (in `lib/preimport.py`) and a pure decision function `preimport_decide` (in `lib/quality.py`). Neither writes a verdict, denylists, or mutates a job. (see origin: R2)
- R3. The importer worker is the sole caller of `preimport_decide`. The preview worker imports and calls only the measurement helper. (see origin: R3)
- R4. Folder-shape facts (layout, file count, filetype band) become persisted evidence on the candidate row. The decision function decides what to do with each. (see origin: R4)
- R5. Preview exits that today emit `verdict='uncertain'` because the source is unmeasurable become `measurement_failed` with a typed reason. (see origin: R5)
- R6. When the importer rejects a candidate on the basis of evidence, it writes a `download_log` row, denylists the source user where the existing rule applies, marks the import job `status='failed'`, and finalizes the parent request to `status='wanted'`. (see origin: R6)
- R7. When preview emits `measurement_failed`, the same self-healing finalization fires. For the request-not-found subcase, the helper writes the log and marks the job failed but skips request transition (there is nothing to finalize). (see origin: R7)
- R8. `preview_status='uncertain'` is deleted as a state any production code path can write. Historical rows are not migrated; the constant `PREVIEW_FAILURE_STATUS` in `scripts/import_preview_worker.py` is retired. (see origin: R8)
- R9. The deploy lands a one-time recovery sweep migration that frees the currently-stuck rows. After deploy, zero `import_job` rows remain in `preview_status='uncertain'`, and the parent requests are each in a poll-advanceable state. (see origin: R9)
- R10. `validate_audio` (in `lib/util.py`) rejects only when ffmpeg's exit code is non-zero. Metadata-only stderr (BOM warnings, mjpeg APP-segment warnings, mp3float backstep recoveries) no longer counts as corruption. Closes #251. *(Folded into this plan from #251 mid-brainstorm; no corresponding R-ID exists in the origin doc — see origin Dependencies / Assumptions for the coordinated-dependency framing this plan supersedes.)*

**Origin actors:** A1 (Operator), A2 (Preview worker), A3 (Importer worker)
**Origin flows:** F1 (Healthy candidate → preview without verdict), F2 (Suspect candidate → importer reject + self-heal), F3 (Measurement itself fails → preview self-heals), F4 (315 stuck rows recovered on deploy)
**Origin acceptance examples:** AE1 (covers R1-R3), AE2 (R1, R3, R6), AE3 (R1, R4), AE4 (R1, R4), AE5 (R1, R5, R7), AE6 (R7), AE7 (R8), AE8 (R9)

---

## Scope Boundaries

- Importer-side dispatch path, `load_candidate_evidence_for_source` snapshot guard, requeue-to-preview mechanism, and Wrong Matches cleanup — all inherited from 2026-05-14 / 2026-05-15 and not retouched.
- Content-hash or audio-fingerprint upgrades to the snapshot guard, cross-request candidate-evidence reuse, and changes to the per-user 5-strikes cooldown rule — out of scope.
- Proactive backfill of the ~700 legacy Wrong Matches rows that predate migration 017 — out of scope; they are measured on demand at first force-import.
- Tuning of quality thresholds, spectral grade bands, or V0 probe parameters — out of scope.

### Deferred to Follow-Up Work

- Full per-file decode diagnostic detail across replays (beyond the per-row `decode_ok` bool on `album_quality_evidence_files`). The current plan stores a single boolean per file in the snapshot row; if we ever need richer diagnostic structure (e.g. ffmpeg's exact stderr per failed file), that lives in an auxiliary table in a future iteration.
- Marking `import_jobs.preview_status='uncertain'` deprecated at the column level (e.g. removing it from the `IMPORT_JOB_PREVIEW_STATUSES` frozenset entirely vs. keeping the value-but-not-the-write). U7 retires the production writers; column-deprecation in a future migration if useful.

---

## Context & Research

### Relevant Code and Patterns

- `lib/preimport.py:357-669` — `run_preimport_gates` to split (measurement lines 415-592, decision lines 594-666). `PreImportGateResult` (lines 58-81) is the existing return shape.
- `lib/import_preview.py:276-571` — 13 verdict-emitting exits to reclassify across two entry points. In `process_claimed_preview_job`'s helpers: lines 276, 288, 298, 316, 327, 355, 384, 465, 492, 503. In `preview_import_from_download_log` (force/manual UI entry): lines 537, 546, 556, 566.
- `scripts/import_preview_worker.py:45, 333-431` — `PREVIEW_FAILURE_STATUS = "uncertain"` constant and the lifecycle paths that write it.
- `lib/quality.py:2081-2702` — pure decision function precedent: `spectral_import_decision`, `import_quality_decision`, `quality_gate_decision`, `dispatch_action`, `transcode_detection`. Pattern: function in `lib/quality.py` with no DB/IO; subTest table in `tests/test_quality_decisions.py`.
- `lib/quality.py:842-913` — `AlbumQualityEvidence` `msgspec.Struct` to extend.
- `lib/quality.py:768-790` — `AlbumQualityEvidenceFile` to extend with `decode_ok`.
- `lib/quality_evidence.py:170-187` — `audio_snapshot_matches` snapshot guard (per-file equality on `relative_path, size_bytes, extension, container, codec`; `mtime_ns` excluded).
- `lib/quality_evidence.py:315, 410, 483` — `evidence_from_import_result`, `persist_candidate_evidence_from_import_result`, `load_candidate_evidence_for_source`.
- `lib/pipeline_db.py:1703-1880` — `upsert_album_quality_evidence`, `load_album_quality_evidence`.
- `lib/beets_album_op.py:78-110` — `BeetsOpFailure` (`Literal` reason + free-text detail + frozen `msgspec.Struct`) — the canonical typed-failure precedent for `MeasurementFailure`.
- `lib/transitions.py:293-313` — `finalize_request(db, request_id, transition)` — the single shared finalize-to-wanted helper. `RequestTransition.to_wanted_fields(...)` is the builder.
- `lib/import_dispatch.py:850-918` — `_record_rejection_and_maybe_requeue` — the existing helper that writes `download_log`, calls `db.add_denylist`, calls `transitions.finalize_request`. The new preview measurement_failed path and the new importer-side preimport-reject path will both delegate through this helper (or a sibling that shares its internals).
- `lib/download.py:2337-2389` — `_poll_one_active_download`. Line 2371 is the poll-loop's active-import-job guard; once the importer marks the job `status='failed'` and finalizes the request to `wanted`, the guard releases on the next tick.
- `lib/util.py:363` — the `validate_audio` `or stderr` bug. #251's one-line fix lands here.
- `migrations/017_album_quality_evidence.sql` — current schema to extend; pattern for the new 019 migration.
- `migrations/018_neutral_import_job_preview_ready.sql:45-67` — recovery-sweep precedent for the U7 020 migration.
- `migrations/008_bad_audio_hashes.sql:18` — FK shape for `matched_bad_audio_hash_id REFERENCES bad_audio_hashes(id) ON DELETE SET NULL`.
- `migrations/009_curator_ban_outcome.sql:11-12` — precedent for adding a new `download_log.outcome` CHECK constraint value (the new `'measurement_failed'` outcome).
- `lib/migrator.py` — SQL-only migration runner; recovery sweep MUST be a numbered SQL file, no Python sweep precedent.
- `tests/fakes.py::FakePipelineDB` (lines 638+, evidence CRUD at 1097-1186, 1822-1830) — existing fake; will need new methods for the new evidence persistence helper and the new `MeasurementFailure` reasons.
- `tests/test_quality_decisions.py::TestSpectralImportDecision` — subTest table pattern for `TestPreimportDecide`.
- `tests/test_integration_slices.py` — pattern for the orchestration slices that prove self-healing finalize fires through real code.

### Institutional Learnings

- `docs/solutions/testing/contract-test-mocks-must-mirror-production-shape.md` — mocked DB rows in contract tests must use the production `msgspec.Struct` types (`AlbumQualityEvidence`, `MeasurementFailure`, real `datetime`/`UUID`), not synthetic dicts. The new evidence fields will hit this trap on contract-style tests if mocks aren't shape-correct.
- `docs/solutions/testing/mocked-contract-tests-miss-helper-mirror-integration-bugs.md` — the new pure measurement helper needs at least one integration slice in `tests/test_integration_slices.py` that exercises it through real `lib/quality_evidence.py` persistence — mocking the helper alone hides serialization drift.
- `.claude/rules/code-quality.md` § Pipeline Bug Reproduction — Red/Green on Real Code Paths — the self-healing finalize path needs an orchestration test that calls the real reject helper with mocked-state-matching-live-#250-scenarios. RED reproduces the stuck-forever pattern; GREEN proves the request reaches `wanted`.
- `.claude/rules/code-quality.md` § Wire-boundary types — `MeasurementFailure` is a wire-boundary type (written to `import_jobs.preview_result` JSONB, read by the recents UI). MUST be `msgspec.Struct` with strict typing and a RED test that feeds the wrong type at the boundary and asserts `msgspec.ValidationError`.
- `.claude/rules/pipeline-db.md` — schema lives only in numbered `migrations/NNN_*.sql` files; no DDL in `PipelineDB` methods. Both U1 (019) and U7's recovery sweep (020) follow this.

### External References

- None used. This is a focused internal refactor on a well-patterned area.

---

## Key Technical Decisions

- **Extend `AlbumQualityEvidence` with typed columns, not JSONB.** Mirrors migration 017's deliberate "explode the legacy JSONB blob into typed columns" decision. Pure-decision functions take typed structs, not `dict.get()`-with-coercion. New columns: `audio_corrupt`, `matched_bad_audio_hash_id`, `matched_bad_audio_hash_path`, `folder_layout`, `filetype_band`, `audio_file_count`; per-file `decode_ok` on `album_quality_evidence_files`. (see origin: R4 Deferred to Planning)
- **`MeasurementFailure` is a `msgspec.Struct`.** Mirrors `lib/beets_album_op.py::BeetsOpFailure:78-110`: `Literal` reason + free-text detail + source_path; frozen. Carried in `import_jobs.preview_result` (JSONB) and `download_log.validation_result` (JSONB). Strict at the boundary per the wire-boundary rule. Reason taxonomy: `snapshot_stale`, `source_vanished`, `materialization_error`, `measurement_crashed`, `evidence_persist_failed`, `request_not_found`, `missing_release_id`, `download_log_not_found`, `missing_failed_path` (10 values; covers both the `process_claimed_preview_job` exits and the `preview_import_from_download_log` force/manual-import exits). U5 may surface an 11th reason during reclassification if the inventory misses an exit.
- **New `download_log.outcome='measurement_failed'` value.** Added via migration 019 (alongside the schema extension) following the `migrations/009_curator_ban_outcome.sql` precedent. Lets ops grep the recents UI for measurement-failed rejections distinctly from slskd transfer failures.
- **Decision function `preimport_decide` lives in `lib/quality.py`** next to the existing pure-decision family. Same `subTest` table pattern as `TestSpectralImportDecision`.
- **Recovery sweep is a SQL migration.** Modelled on `migrations/018_neutral_import_job_preview_ready.sql:45-67`. Idempotent UPDATE gated on `preview_status='uncertain'`. No Python sweep — there is no `cratedigger-db-migrate.service`-invoked Python precedent in the repo, and adding one is an architectural break for one-time work.
- **Pre-claim sanity-check exits fold into `measurement_failed` with a no-finalize subcase.** Request-not-found, missing MBID, path-missing — preview can't measure, so the outcome is `measurement_failed` with a distinct reason. For `request_not_found` specifically, the self-healing helper writes the log and marks the job failed but skips the request transition (there is nothing to finalize). The other two have a parent request and self-heal normally.
- **Two rejection-finalize entry points sharing a sub-helper.** The existing importer-side `lib/import_dispatch.py::_record_rejection_and_maybe_requeue` takes a `DownloadInfo` populated from an in-flight slskd transfer (filetype, bitrate, soulseek_username, spectral, v0 probe, etc.). Preview's `measurement_failed` path has no slskd context — there is no transfer in flight when measurement fails. Keep `_record_rejection_and_maybe_requeue` as the importer-side entry point with its existing `DownloadInfo` signature. Add a sibling `_record_preview_measurement_failed(db, request_id, import_job_id, payload: MeasurementFailure, ..., denylist_rule_applies: bool)` for the preview path. Both delegate to a shared private `_finalize_request_and_log_rejection` sub-helper that owns the four steps (download_log row, denylist where applicable, request → wanted via `transitions.finalize_request`, job → failed). The sub-helper is the single source of truth enforceable via grep; the two entry points just shape their inputs into the sub-helper's signature.
- **`#251` fix lands before the split (U2 before U3).** Otherwise the new measurement helper inherits a buggy validator and the U7 recovery sweep re-sticks the 90 audio_corrupt rows on re-measurement.

---

## Open Questions

### Resolved During Planning

- Names for the split halves → measurement helper stays in `lib/preimport.py` (as `measure_preimport_state` or kept-as-`run_preimport_gates`-minus-decision); decision moves to `lib/quality.py` as `preimport_decide`.
- Persistence shape for new evidence facts → typed columns on `AlbumQualityEvidence` (see KTDs).
- `measurement_failed` typed shape → `msgspec.Struct MeasurementFailure` mirroring `BeetsOpFailure`.
- Recovery sweep mechanism → SQL migration modelled on 018.
- Finalize-to-wanted helper → `lib/transitions.py::finalize_request`; the new preview measurement_failed path and the new importer preimport-reject path both go through one shared rejection-finalize helper that calls it.
- Whether `audio_corrupt` is a column or a derived view → column. Decision function reads typed data; derivation in the function would force `dict.get()`-style access from the snapshot files.

### Deferred to Implementation

- Whether `folder_layout` is a denormalised column or derived in the persistence helper from `EXISTS(snapshot file path contains '/')`. The current plan keeps it as a typed column for symmetry with `audio_corrupt`; if the persistence path makes it cleaner to derive, that's acceptable provided the decision function still reads a single bool.
- Final shape of the shared rejection-finalize helper signature — whether to extend `_record_rejection_and_maybe_requeue` or extract a sibling. The contract is unchanged; the file boundary is a refactor judgment call once U6 lands.
- Whether the recovery sweep migration also re-runs the `validate_audio` fix's positive cases proactively (it doesn't need to — once preview re-claims the requeued rows under the new contract, they'll re-measure correctly).

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

```text
                ┌────────────────────────────────┐
                │   import_job claimed by        │
                │   preview worker                │
                └────────────┬───────────────────┘
                             │
                             ▼
                ┌────────────────────────────────┐
                │  measure_preimport_state(...)   │
                │  (lib/preimport.py — pure        │
                │   measurement, no decision,      │
                │   no DB writes, no denylist)     │
                └────────────┬───────────────────┘
                             │
                ┌────────────┴─────────────┐
                │                          │
       facts produced              measurement crashed /
       (audio_corrupt,             source vanished /
        spectral, V0,              snapshot stale
        layout, etc.)                       │
                │                          ▼
                ▼              ┌──────────────────────────┐
   ┌─────────────────────┐     │ MeasurementFailure(     │
   │ persist evidence →   │     │   reason=...,           │
   │   evidence_ready    │     │   detail=...,           │
   │   (preview done)    │     │   source_path=...)      │
   └────────────┬────────┘     └────────────┬─────────────┘
                │                           │
                │                           ▼
                │              ┌──────────────────────────┐
                │              │ self-healing finalize:   │
                │              │  - download_log row      │
                │              │  - denylist (if rule)    │
                │              │  - job → status=failed   │
                │              │  - request → wanted      │
                │              │    (skipped if request_  │
                │              │     not_found)           │
                │              └──────────────────────────┘
                ▼
   ┌─────────────────────┐
   │ importer claims     │
   │ evidence_ready job  │
   └────────┬────────────┘
            │
            ▼
   ┌─────────────────────────────────────┐
   │ preimport_decide(measurement, cfg,  │
   │   existing) → "accept" | "reject"   │
   │ (lib/quality.py — pure, no IO)      │
   └────────┬────────────────────────────┘
            │
   ┌────────┴────────┐
   │                 │
accept             reject
   │                 │
   ▼                 ▼
beets import   same self-healing finalize:
               - download_log row
               - denylist (if rule)
               - job → status=failed
               - request → wanted
```

The horizontal symmetry — preview's `measurement_failed` and importer's `reject` route through the same self-healing finalize helper — is what makes #250's "stuck forever" pathology impossible by construction.

---

## Implementation Units

### U1. Migration 019 + Struct extension: new evidence facts

**Goal:** Add the typed columns that preview must persist (`audio_corrupt`, `matched_bad_audio_hash_id` + path, `folder_layout`, `filetype_band`, `audio_file_count`, per-file `decode_ok`) plus the new `download_log.outcome='measurement_failed'` value. Plumb them through the `AlbumQualityEvidence` Struct, the CRUD helpers, and `FakePipelineDB`.

**Requirements:** R4 (folder-shape facts as evidence), R7 (measurement_failed outcome value)

**Dependencies:** None.

**Files:**
- Create: `migrations/019_preview_evidence_facts.sql`
- Modify: `lib/quality.py` (AlbumQualityEvidence, AlbumQualityEvidenceFile, AudioQualityMeasurement-adjacent Structs as needed)
- Modify: `lib/pipeline_db.py` (`upsert_album_quality_evidence`, `_album_quality_evidence_from_row`, `log_download`'s outcome validation)
- Modify: `lib/quality_evidence.py` (`evidence_from_import_result`, `persist_candidate_evidence_from_import_result` — populate new fields)
- Modify: `lib/import_queue.py` (extend `IMPORT_JOB_PREVIEW_STATUSES` with `'measurement_failed'`; replace `IMPORT_JOB_PREVIEW_UNCERTAIN` membership in `IMPORT_JOB_PREVIEW_FAILURE_STATUSES` with `'measurement_failed'`)
- Modify: `tests/fakes.py::FakePipelineDB` (mirror the new fields in the in-memory store; accept the new `preview_status` value)
- Test: `tests/test_migrator.py` (assert 019 applies cleanly)
- Test: `tests/test_pipeline_db.py` (upsert + load round-trip the new fields; CHECK constraints accept the new `preview_status` and `outcome` values)
- Test: `tests/test_fakes.py` (FakePipelineDB matches real PipelineDB shape)

**Approach:**
- 019 adds the columns + two CHECK constraint extensions: (a) `download_log.outcome` gains `'measurement_failed'` (see `migrations/009_curator_ban_outcome.sql:11-12` for the precedent), and (b) `import_jobs.preview_status` gains `'measurement_failed'` (see `migrations/018_neutral_import_job_preview_ready.sql:15-27` for the precedent of extending this exact constraint). Without (b), every preview write of `preview_status='measurement_failed'` in U5/U4 raises a CHECK violation on deploy. Default values are conservative and chosen to keep the U3 `PreimportMeasurement` Struct's typed fields non-Optional: `audio_corrupt BOOLEAN NOT NULL DEFAULT FALSE`, `folder_layout TEXT NOT NULL DEFAULT 'flat'` (with CHECK constraint accepting only `'flat'` or `'nested'`), `audio_file_count INTEGER NOT NULL DEFAULT 0`, `filetype_band TEXT NOT NULL DEFAULT ''`, `decode_ok BOOLEAN NOT NULL DEFAULT TRUE` (per-file). Legacy evidence rows decoded into the new Struct shape pick up these defaults — the decision function reads them as facts (e.g. `folder_layout='flat'` + `audio_file_count=0` is the explicit "empty inventory" signal that AE4 requires). `matched_bad_audio_hash_id` and `matched_bad_audio_hash_path` are nullable because they represent an optional FK relationship.
- `audio_file_count INTEGER NOT NULL DEFAULT 0` — the empty-fileset case becomes an explicit fact rather than a snapshot-validation failure. Relax `AlbumQualityEvidence.storage_validation_errors` (`lib/quality.py:876-899`) to allow `files=[]` when `audio_file_count=0`.
- `filetype_band TEXT` — small classification (`flac` / `mp3` / `mixed_lossless` / `mixed_lossy` / `mixed`), normalised from `LocalFileInspection.filetype`'s comma-separated string.
- The Struct field types match the SQL exactly; pyright must be clean.
- This unit is rails only — no behavior change yet. Preview still calls the old `run_preimport_gates`; the new fields default safely.

**Patterns to follow:**
- `migrations/017_album_quality_evidence.sql` for column shape, CHECK constraints, and the two-table parent/child layout.
- `lib/pipeline_db.py:1703-1880` for the CTE-based upsert + load shape.
- `tests/test_pipeline_db.py` for round-trip test patterns on the existing evidence columns.

**Test scenarios:**
- Happy path: migration 019 applies cleanly on a fresh DB; `schema_migrations` shows 019.
- Happy path: `upsert_album_quality_evidence` + `load_album_quality_evidence` round-trip an evidence row with every new field populated. Covers AE3, AE4.
- Edge case: `audio_file_count=0` with empty `files=[]` round-trips without raising `storage_validation_errors`. Covers AE4.
- Edge case: `matched_bad_audio_hash_id` NULL when no hash matched; non-NULL when a `bad_audio_hashes.id` value is set. Cascade on delete drops to NULL.
- Edge case: `folder_layout` CHECK rejects any value not in `('flat', 'nested')`.
- Edge case: `download_log.outcome='measurement_failed'` is accepted by the CHECK; unknown outcomes still rejected.
- Integration: `FakePipelineDB` upsert + load matches real PipelineDB for the same Struct input.

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_migrator tests.test_pipeline_db tests.test_fakes -v"` passes.
- Pyright clean on every touched file.
- `grep -n "audio_corrupt\|folder_layout\|filetype_band\|audio_file_count\|decode_ok" lib/quality.py lib/pipeline_db.py lib/quality_evidence.py tests/fakes.py` shows the new fields plumbed through every layer.

---

### U2. Fix `validate_audio` stderr false-positives (#251)

**Goal:** `validate_audio` rejects only on `ffmpeg returncode != 0`. Metadata-only stderr (BOM warnings, mjpeg APP-segment warnings, `mp3float invalid new backstep -1`) no longer counts as audio corruption.

**Requirements:** R10 (closes #251). Lands before U3 so the new measurement helper inherits a fixed validator and U7's recovery sweep doesn't re-stick the 90 audio_corrupt rows.

**Dependencies:** None.

**Files:**
- Modify: `lib/util.py:363` — drop the `or stderr` clause.
- Modify: `lib/util.py` — retire `_IGNORABLE_AUDIO_VALIDATION_STDERR` and its associated logic (no longer needed once stderr is informational only) OR keep it but stop consulting it — implementer's choice; the brainstorm called out option A as simplest.
- Test: `tests/test_util.py` (or wherever `validate_audio` tests live) — add regression cases for the four false-positive patterns + assert real corruption still rejects.

**Approach:**
- One-line behavior change: `if ffmpeg_returncode != 0 or stderr:` → `if ffmpeg_returncode != 0:`. ffmpeg's exit code is the authoritative signal for decode failure.
- Before deleting `_is_ignorable_audio_validation_stderr` (line 353) or the `_IGNORABLE_AUDIO_VALIDATION_STDERR` constant, audit what patterns each accumulated. Two existing seams already filter known-ignorable stderr: (a) the MD5-fix path (lines 342-347) that logs "ignoring metadata-only stderr after MD5 fix" and continues, and (b) the explicit ignorable-stderr filter (line 353) that gates whether line 363 is reached. Each pattern was added for a specific failure mode (see commit history on `lib/util.py`). Either verify each ignorable pattern has `rc=0` in practice (in which case the dropped `or stderr` clause makes them moot and the filter+constant can be retired), or keep the filter as a defense-in-depth seam against future ffmpeg versions that change exit-code semantics. Default: retire both the constant and the filter unless the audit surfaces a documented case where ffmpeg returns rc=0 but the audio is genuinely undecodable.
- Add regression tests for each documented false-positive pattern (#251's table): `mp3float invalid new backstep`, `Incorrect BOM value Error reading lyrics`, `Incorrect BOM value Error reading comment frame`, `mjpeg unable to decode APP fields`. Each must produce `ValidationResult` with `corrupt_files=[]`.
- Add regression tests for real corruption (`invalid sync code / invalid frame header / decode_frame() failed`, `illegal residual coding method 2`, `invalid residual`) — these have `rc!=0` and MUST still reject. Pull a small fixture from one of the live-rejected #251 examples if convenient.

**Execution note:** Test-first. Reproduce each false-positive as a failing test (RED) against the current `or stderr` code, then drop the clause (GREEN), then ensure the real-corruption tests still GREEN.

**Patterns to follow:**
- `tests/test_util.py` for existing `validate_audio` test scaffolding.
- The brainstorm's #251 reference for the false-positive vs real-corruption matrix.

**Test scenarios:**
- Happy path: `ffmpeg rc=0` with empty stderr → `corrupt_files=[]`.
- Happy path: `ffmpeg rc=0` with `mp3float invalid new backstep -1` stderr → `corrupt_files=[]` (RED before fix; GREEN after).
- Happy path: `ffmpeg rc=0` with `Incorrect BOM value` stderr → `corrupt_files=[]`.
- Happy path: `ffmpeg rc=0` with `mjpeg unable to decode APP fields` stderr → `corrupt_files=[]`.
- Error path: `ffmpeg rc=1` with `invalid sync code` stderr → `corrupt_files=[path]`.
- Error path: `ffmpeg rc=1` with `illegal residual coding method 2` stderr → `corrupt_files=[path]`.

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_util -v"` passes.
- Manually checking one of #251's stuck rows by running validate_audio against its source folder shows no rejection (will become observable after U7).

---

### U3. Split `run_preimport_gates` + add `preimport_decide`

**Goal:** Decompose the fused measure-and-decide function into (a) a pure measurement helper in `lib/preimport.py` and (b) a pure decision function `preimport_decide` in `lib/quality.py`. Neither writes a verdict, denylists, or mutates a job. The measurement helper returns a typed result that maps cleanly onto the new `AlbumQualityEvidence` fields from U1.

**Requirements:** R2 (split), R3 (importer is sole caller of decision function — wired in U6).

**Dependencies:** U1 (new evidence fields), U2 (clean validator).

**Files:**
- Modify: `lib/preimport.py` — extract decision logic out of `run_preimport_gates`; the remaining function (or its replacement `measure_preimport_state`) returns facts only. Keep the same callable name if every caller will be updated in this unit; otherwise introduce `measure_preimport_state` as the new entry point and leave a thin `run_preimport_gates` that callers will be migrated off in U5/U6.
- Modify: `lib/preimport.py::PreImportGateResult` — strip decision fields (`valid`, `scenario`, `detail` may stay if reframed, but `valid` must go), keep measurement fields (`corrupt_files`, `download_spectral`, `existing_spectral`, `existing_min_bitrate`, `matched_bad_hash_id`, `matched_bad_track_path`, plus new: layout, file_count, filetype_band).
- Create / Modify: `lib/quality.py` — add `preimport_decide(measurement, cfg, existing_evidence) -> PreimportDecision` near the existing pure-decision family.
- Test: `tests/test_quality_decisions.py::TestPreimportDecide` — subTest table covering every decision branch (accept, reject_audio_corrupt, reject_bad_hash, reject_spectral, reject_nested_layout, reject_empty_fileset).
- Test: `tests/test_preimport.py` (or similar) — measurement helper produces correct facts for fixture inputs.

**Approach:**
- The decision function consumes the measurement result + `QualityRankConfig` + `existing_evidence` (the album's current `AlbumQualityEvidence`, if any). It returns a typed `PreimportDecision` (likely a dataclass with `decision: Literal["accept", "reject"]`, `reason: str | None`, `detail: str | None`). The reason taxonomy mirrors the current `scenario` strings (`audio_corrupt`, `bad_audio_hash`, `spectral_reject`, `nested_layout`, `empty_fileset`).
- The measurement helper does not call `db.add_denylist`. The denylist write moves to U6 (importer's reject path) and U4's preview-side measurement_failed helper.
- `_persist_spectral_state` (today at `lib/preimport.py:657-666`, the issue #90 propagation that writes `album_requests.current_spectral_*` so the next attempt has up-to-date comparison data) moves into the evidence-persistence helper that fires on `evidence_ready`. This preserves the existing semantics: spectral state propagates on every claim where measurement completes, regardless of whether the importer subsequently accepts or rejects. Co-locating with evidence persistence is the natural mapping under the new contract because evidence persistence is the durable record of "we measured this candidate." Do NOT move spectral state persistence into the importer reject branch only — that silently regresses the issue #90 fix for accept-with-suspect-grade-upgrade and import_no_exist cases.
- The existing `lib/preimport.py::_analyze_existing` lookup either stays in the measurement helper (it's reading existing evidence to colocate spectral measurement) or moves to a load step at the importer side. Pick whichever keeps the measurement helper purer; the criterion is "no DB writes," not "no DB reads" — reading existing evidence is fine.
- U3 is the keystone refactor: the function decomposition itself. Behavior change is deferred until U5 (preview calls the new helper, no longer translates `valid=False` to verdict) and U6 (importer calls `preimport_decide`, performs the side effects). Backward compatibility during U3 is achieved by having the old call sites (preview, automation) continue to call the old `run_preimport_gates` until U5/U6 — or by introducing the new functions alongside the old one and only deleting the old after U6.
- **Caller audit before the legacy bridge is deleted.** As a pre-step in U3, run `grep -rn 'run_preimport_gates\|PreImportGateResult\|\.valid\b\|\.scenario\b' lib/ scripts/ tests/` and enumerate every caller that consumes `PreImportGateResult.valid` or `.scenario`. List each caller in U3's Files section before declaring the field deletion safe. Known callers from prior research: `lib/import_preview.py:366`, `lib/download.py:1200` (auto-import path), and any force-import or manual-import dispatch sites that still consume the result struct. Force-import and manual-import paths must be named explicitly even if their dispatch flow is "unchanged" — they still consume the struct shape. The legacy bridge stays alive until every consumer is migrated to either the measurement helper or `preimport_decide`.

**Execution note:** Strict subTest TDD on `preimport_decide`. Each branch is one row in `TestPreimportDecide.CASES`.

**Technical design:** *Optional pseudo-code sketch — directional guidance.*

```text
PreimportMeasurement (Struct, no decision):
  corrupt_files: list[str]
  audio_corrupt: bool                  # cardinality(corrupt_files) > 0
  matched_bad_hash_id: int | None
  matched_bad_track_path: str | None
  download_spectral: SpectralMeasurement | None
  existing_spectral: SpectralMeasurement | None
  existing_min_bitrate: int | None
  folder_layout: Literal["flat", "nested"]
  audio_file_count: int
  filetype_band: str
  min_bitrate_kbps: int | None
  is_vbr: bool

preimport_decide(m, cfg, existing_evidence) -> PreimportDecision:
  if m.audio_corrupt:        return reject("audio_corrupt", detail)
  if m.matched_bad_hash_id:  return reject("bad_audio_hash", detail)
  if m.folder_layout == "nested": return reject("nested_layout", detail)
  if m.audio_file_count == 0:     return reject("empty_fileset", detail)
  if spectral_import_decision(m.download_spectral, existing) == "reject":
     return reject("spectral_reject", detail)
  return accept()
```

**Patterns to follow:**
- `lib/quality.py::spectral_import_decision:2081-2116` — function shape (no IO, returns Literal/Enum).
- `tests/test_quality_decisions.py::TestSpectralImportDecision:136-172` — subTest CASES table.

**Test scenarios:**
- Happy path: clean candidate (no corruption, no bad hash, flat layout, files present, spectral grade allows) → `accept`.
- Edge case: `audio_corrupt=True` → `reject("audio_corrupt")`.
- Edge case: `matched_bad_hash_id=42` → `reject("bad_audio_hash")`.
- Edge case: `folder_layout="nested"` → `reject("nested_layout")`.
- Edge case: `audio_file_count=0` → `reject("empty_fileset")`. Covers AE4.
- Edge case: spectral grade `likely_transcode` + no improvement over existing → `reject("spectral_reject")`. Covers AE2.
- Edge case: spectral grade `likely_transcode` but bitrate-improves on existing → `accept`.
- Edge case: existing evidence None (first time we've seen this album) + suspect spectral → policy-dependent; mirror the current `spectral_import_decision` "import_no_exist" branch.
- Measurement helper: takes a fixture folder with one corrupt MP3 → returns `audio_corrupt=True, corrupt_files=[path]`.
- Measurement helper: nested fixture → returns `folder_layout="nested"`.
- Measurement helper: empty fixture → returns `audio_file_count=0`.

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_quality_decisions tests.test_preimport -v"` passes.
- `grep -n "result.valid = False\|result.scenario =" lib/preimport.py` returns no matches (decision fields gone from the measurement helper) — OR returns only matches inside the old `run_preimport_gates` function if it's still around as a temporary bridge for U5/U6.
- Pyright clean.
- `preimport_decide` has no `db.`, no `import ... persist`, no `cfg.add_denylist` — pure function.

---

### U4. `MeasurementFailure` rails + shared self-healing rejection-finalize helper

**Goal:** Introduce the `MeasurementFailure` `msgspec.Struct`, the new `download_log.outcome='measurement_failed'` write path, and the shared helper that finalizes a rejection (whether from preview's measurement_failed or the importer's preimport_decide reject) into `(download_log row + denylist where applicable + job → failed + request → wanted)`. The helper is the single source of self-healing behavior; both preview (U5) and importer (U6) delegate to it.

**Requirements:** R6, R7 (self-healing on both sides; the request-not-found subcase skips request transition).

**Dependencies:** U1 (download_log.outcome='measurement_failed' available), U2 (clean validator so `audio_corrupt` is trustworthy), U3 (PreimportDecision shape settled — the helper accepts either a `MeasurementFailure` or a `PreimportDecision` reject).

**Files:**
- Modify: `lib/quality.py` (or new `lib/preimport_failure.py` if it grows) — add `MeasurementFailure` Struct.
- Modify: `lib/import_dispatch.py::_record_rejection_and_maybe_requeue:850-918` — extract the four side-effect steps (download_log row, denylist where applicable, request → wanted, job → failed) into a shared private `_finalize_request_and_log_rejection` sub-helper. `_record_rejection_and_maybe_requeue` continues to be the importer-side entry point with its `DownloadInfo` signature, now delegating to the sub-helper. Add a sibling entry point `_record_preview_measurement_failed(db, request_id, import_job_id, payload: MeasurementFailure, ..., denylist_rule_applies: bool)` that shapes its (no-slskd-context) inputs into the same sub-helper signature. Preview rows write NULL for the slskd-only `download_log` fields (username, bitrate, filetype, etc.) — verify `log_download` accepts that.
- Modify: `lib/pipeline_db.py::log_download` — accept `outcome='measurement_failed'` (gated by the new CHECK from U1).
- Modify: `tests/fakes.py::FakePipelineDB` — `log_download` accepts the new outcome; add a tracking helper for assertions (`db.assert_log(outcome="measurement_failed", reason=...)`).
- Test: `tests/test_preimport_failure.py` (or extend existing) — `MeasurementFailure` Struct round-trips through `msgspec.json.encode` → `msgspec.convert`.
- Test: `tests/test_preimport_failure.py` — wire-boundary RED test: feeding `reason="not_a_real_enum_value"` raises `msgspec.ValidationError`.
- Test: `tests/test_integration_slices.py` — slice that calls the shared helper with a `MeasurementFailure(reason="source_vanished", ...)` payload and asserts the four side effects fire (download_log row, denylist write where rule applies, job → failed, request → wanted).
- Test: `tests/test_integration_slices.py` — slice for the `request_not_found` subcase: helper writes the log + marks job failed but SKIPS the request transition.

**Approach:**
- `MeasurementFailure` is `frozen=True`, fields: `reason: MeasurementFailureReason` (a `Literal` of the 7 starting taxonomy strings — see KTD), `detail: str`, `source_path: str`.
- Shared helper signature (sketch): `finalize_rejection(db, request_id: int | None, import_job_id: int, payload: MeasurementFailure | PreimportDecision, *, denylist_rule_applies: bool) -> None`. When `request_id is None` (request_not_found subcase), the helper writes the log + marks job failed but does NOT call `finalize_request`.
- Denylist policy stays exactly as today (per-user 5-strikes via `lib/cooldowns.py`); the helper's `denylist_rule_applies` arg is computed by the caller, not by the helper.
- The `download_log` write uses the existing `lib/pipeline_db.py::log_download` plumbing — only the `outcome` value changes (new `measurement_failed`) and the JSONB `validation_result` carries the typed `MeasurementFailure`.

**Patterns to follow:**
- `lib/beets_album_op.py::BeetsOpFailure:78-110` — the typed-failure Struct shape.
- `lib/import_dispatch.py::_record_rejection_and_maybe_requeue:850-918` — current rejection-finalize implementation; the new helper either is this function refactored or a sibling that calls into it.
- `lib/transitions.py::finalize_request:293-313` — the finalize-to-wanted call.

**Test scenarios:**
- Happy path: `MeasurementFailure(reason="source_vanished", ...)` serialises to JSON and decodes back via `msgspec.convert` to an identical Struct.
- Wire-boundary RED: feeding `reason="invalid_value_not_in_literal"` to `msgspec.convert` raises `msgspec.ValidationError`. (Required by .claude/rules/code-quality.md wire-boundary rule.)
- Happy path (orchestration): helper invoked with `MeasurementFailure(reason="snapshot_stale", ...)` writes a `download_log` row with `outcome='measurement_failed'`, marks the import job `status='failed'`, finalizes the parent request to `status='wanted'`. Denylist conditional on the rule. Covers AE5.
- Edge case (orchestration): helper invoked with `request_id=None` (request_not_found subcase) writes the log + marks job failed but does NOT call `finalize_request`. No request transition.
- Edge case: helper called for the importer reject path (with a `PreimportDecision(reject, reason="audio_corrupt", ...)` payload) writes the same shape of `download_log` row that today's `_record_rejection_and_maybe_requeue` writes — i.e., this refactor preserves behavior, only the entry surface changes.
- Integration slice: full preview self-heal scenario — preview claims a job, measurement crashes, helper fires, request returns to `wanted`, poll loop on next tick re-runs search.
- Integration slice (issue #90 propagation guard): preview claims a job whose audio measures as suspect spectral but importer accepts (upgrade case); assert `album_requests.current_spectral_grade` and `current_spectral_bitrate` are written by the evidence-persistence path on `evidence_ready`, not gated on a reject decision. Same assertion for the import_no_exist case (no existing-album evidence). Guards against the propagation-regression failure mode the architectural shift would otherwise introduce.

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_preimport_failure tests.test_integration_slices -v"` passes.
- `grep -n "outcome='measurement_failed'" lib/` shows the helper as the only producer.
- Pyright clean.

---

### U5. Reclassify every preview verdict-emitting exit

**Goal:** Replace every `verdict="confident_reject"` and `verdict="uncertain"` exit in `lib/import_preview.py` with either (a) an `evidence_ready` outcome carrying the new facts from U1, or (b) a `measurement_failed` outcome routed through U4's helper. Retire the `PREVIEW_FAILURE_STATUS = "uncertain"` constant in `scripts/import_preview_worker.py` — preview now writes either `'evidence_ready'` or `'error'` (existing crash state) or `'measurement_failed'` (new). Preview never emits a verdict.

**Requirements:** R1 (only two outcomes), R5 (unmeasurable exits become measurement_failed), R8 (uncertain retired from production writes).

**Dependencies:** U3 (measurement helper exists), U4 (MeasurementFailure + helper exist).

**Files:**
- Modify: `lib/import_preview.py:276-571` — every `_preview_result(verdict=...)` call across both entry points (13 exits total). Per the code-surface inventory:

  **In `process_claimed_preview_job` and helpers:**
  - Line 276 (request_not_found) → `measurement_failed(reason="request_not_found")`, no-finalize subcase.
  - Line 288 (no MBID) → `measurement_failed(reason="missing_release_id")`.
  - Line 298 (path missing) → `measurement_failed(reason="source_vanished")`.
  - Line 316 (snapshot OSError) → `measurement_failed(reason="snapshot_stale")`.
  - Line 327 (empty source snapshot) → measure & persist `audio_file_count=0` evidence; mark `evidence_ready`. Importer rejects on `empty_fileset`. Covers AE4.
  - Line 355 (nested layout) → persist `folder_layout="nested"` evidence; mark `evidence_ready`. Importer rejects on `nested_layout`. Covers AE3.
  - Line 384 (preimport_decide reject — but in this unit preview no longer makes that decision; measurement persists facts, importer decides) → persist evidence; mark `evidence_ready`. Importer rejects via U6.
  - Line 465 (snapshot mismatch after retry) → `measurement_failed(reason="snapshot_stale")`. Covers AE5.
  - Line 492 (evidence_status != "ready" after persist) → `measurement_failed(reason="evidence_persist_failed")`.
  - Line 503 (verdict pass-through from `_classify_import_result`) — examine what this does today; if it ever emitted `confident_reject`/`uncertain`, route through the same reclassification.

  **In `preview_import_from_download_log` (force/manual-import UI entry point):**
  - Line 537 (download_log row not found) → `measurement_failed(reason="download_log_not_found")`. No-finalize subcase: no parent request to transition.
  - Line 546 (download_log row has no `request_id`) → `measurement_failed(reason="request_not_found")`. No-finalize subcase.
  - Line 556 (download_log row has no `failed_path`) → `measurement_failed(reason="missing_failed_path")`. No-finalize subcase.
  - Line 566 (failed_path no longer exists on disk) → `measurement_failed(reason="source_vanished")`. Finalizes the parent request to `wanted` when one exists.
- Modify: `scripts/import_preview_worker.py:45` — delete `PREVIEW_FAILURE_STATUS = "uncertain"` constant. Replace its uses (lines 420, 426) with either the new `measurement_failed` flow (via U4's helper) or the existing `'error'` crash status, whichever fits the trigger semantically.
- Modify: `scripts/import_preview_worker.py::_mark_automation_preview_blocked:333-356` — delete or refactor. The "automation preview is blocked, leave job queued" semantics no longer exist; preview emits measurement_failed instead.
- Modify: `lib/import_queue.py:31, 39, 44` — `IMPORT_JOB_PREVIEW_UNCERTAIN` constant and the frozenset memberships. Retire the constant; remove from frozensets as appropriate.
- Modify: `web/js/recents.js:116-133` — add an explicit `previewBadge` branch for `preview_status === 'measurement_failed'` returning a label like `'measurement failed'` with the `badge-failed` class (or a new class if a distinct color is desired). Extend the border-color logic at lines 131-133 to color `measurement_failed` rejections consistently with `confident_reject`. Without this edit, every `measurement_failed` row renders with a blank pill and default border because the existing handlers return `undefined` for unrecognised values.
- Modify: `tests/test_import_preview.py`, `tests/test_import_queue.py`, `tests/fakes.py` — update every assertion that today expects `verdict="uncertain"` or `verdict="confident_reject"` from preview. After this unit, those values are never produced by preview.
- Test: `tests/test_js_util.mjs` — add a unit test that calls the `previewBadge` / border-color helpers with `preview_status='measurement_failed'` and asserts non-empty label + non-default styling.
- Test: `tests/test_integration_slices.py` — slice for each of the 13 reclassified exits, end-to-end: trigger → preview claim → expected outcome (evidence_ready with facts OR measurement_failed with reason).

**Approach:**
- For every reclassified exit, the trigger condition is unchanged; only the outcome shape changes.
- The `evidence_ready` branches (lines 327, 355, 384) require the measurement helper from U3 to be wired so its facts are written to the evidence row before marking the job `evidence_ready`. The importer then reads those facts in U6 and decides.
- The `measurement_failed` branches all go through U4's shared helper, which handles the four side effects (log, denylist where applicable, job-failed, request→wanted-or-skip).
- After this unit, `grep -n "verdict=\"confident_reject\"\|verdict=\"uncertain\"" lib/import_preview.py` returns zero matches. `grep -n "PREVIEW_FAILURE_STATUS" scripts/ lib/` returns zero matches.

**Execution note:** Land each of the 10 exits as a separate sub-step of the same commit if reviewer hygiene allows, or split into two commits (sanity-checks first at 276/288/298; content-related exits second at 316/327/355/384/465/492/503). One PR, atomic deletion of `PREVIEW_FAILURE_STATUS` once all exits are reclassified.

**Patterns to follow:**
- The U4 helper's signature determines what each exit passes in.
- For evidence-ready branches: `lib/quality_evidence.py::persist_candidate_evidence_from_import_result` is the persistence rail; populate the new fact fields from U1's Struct extension.

**Test scenarios:**
- Covers AE1. After this unit, grep for `verdict=` in `lib/import_preview.py` returns no production-write matches; grep for `preview_status='uncertain'` in `lib/` and `scripts/` returns no production-write matches.
- Covers AE3. Nested-audio fixture → preview persists `folder_layout='nested'`, job marked `evidence_ready`, importer claims, decides reject (verified in U6), self-healing finalize fires.
- Covers AE4. Empty-folder fixture → preview persists `audio_file_count=0`, job marked `evidence_ready`, importer decides reject.
- Covers AE5. Source folder moved mid-measure → `measurement_failed(reason="snapshot_stale")`, request → `wanted`.
- Covers AE6. ffmpeg ENOENT during measurement → `measurement_failed(reason="source_vanished")`, request → `wanted`.
- Edge case (request_not_found): import_job exists but parent request_id no longer in `album_requests` → helper writes the log + marks job failed but does NOT finalize a request. No exception.
- Edge case (missing MBID): import_job for a request with `mb_release_id IS NULL` → `measurement_failed(reason="missing_release_id")`, request → `wanted` so the operator (or a future logic branch) can resolve.
- Integration slice: full `measurement_failed` scenario from claim to next-poll-tick-resumes-search.

**Verification:**
- `grep -rn "verdict=\"confident_reject\"\|verdict=\"uncertain\"" lib/import_preview.py` — zero matches.
- `grep -rn "PREVIEW_FAILURE_STATUS\|IMPORT_JOB_PREVIEW_UNCERTAIN" lib/ scripts/` — zero matches.
- `grep -n "preview_status.*=.*'uncertain'" lib/ scripts/` — zero production-write matches.
- All test files update without skipped tests.
- `nix-shell --run "bash scripts/run_tests.sh"` clean.
- Pyright clean.

---

### U6. Importer wires `preimport_decide` + self-heal on reject

**Goal:** The importer worker, on claiming an `evidence_ready` job, calls `preimport_decide` against the persisted evidence + cfg + existing-album evidence. On `accept`, normal beets mutation proceeds. On `reject`, the importer fires U4's shared self-healing helper (download_log + denylist + job-failed + request → wanted). The importer is the sole caller of `preimport_decide` — `grep` confirms.

**Requirements:** R3 (importer is sole caller of decision function), R6 (importer self-heals on reject).

**Dependencies:** U3 (`preimport_decide` exists), U4 (rejection helper exists), U5 (evidence rows now carry the facts the decision function reads).

**Files:**
- Modify: `lib/import_dispatch.py` — in the dispatch path that runs after evidence is loaded but before beets mutation, call `preimport_decide(measurement, cfg, existing_evidence)`. On `accept`, continue. On `reject`, route through U4's helper. Today's quality-gate path (`_check_quality_gate_core`) stays — `preimport_decide` is upstream of it for the preimport-class rejections (audio_corrupt, bad_audio_hash, nested_layout, empty_fileset, spectral_reject).
- Modify: `scripts/importer.py` if needed for plumbing (the worker already calls dispatch).
- Modify: `lib/preimport.py` — if the legacy `run_preimport_gates` function is still alive at this point as a temporary bridge from U3, delete it. The measurement helper is the only entry point post-U6.
- Test: `tests/test_dispatch_from_db.py` — `preimport_decide` reject branch fires the rejection helper; accept branch proceeds to quality-gate.
- Test: `tests/test_integration_slices.py` — full slice: importer claims an `evidence_ready` job whose evidence carries `audio_corrupt=true`, decides reject, self-heals.

**Approach:**
- The new call site sits inside `_dispatch_import_from_db_locked` between evidence load (post-U2 of the 2026-05-15 plan) and the existing quality-gate / mutation steps.
- The evidence row is the input. The decision function does not re-measure. If evidence is missing/stale, the existing requeue-to-preview mechanism (5/15 R2) already handles it — this unit does not retouch that path.
- After this unit, the preview worker has zero callers of `preimport_decide`, and `grep -n "preimport_decide" lib/import_preview.py scripts/import_preview_worker.py` is empty. The importer is the only caller in production code.

**Patterns to follow:**
- 2026-05-15 plan's U2 (`requeue_import_job_to_preview` integration) for the dispatch-layer call-site shape.
- `lib/import_dispatch.py::dispatch_import_core` for evidence-loading + decision-routing within the advisory-lock region.

**Test scenarios:**
- Happy path: evidence with `audio_corrupt=false`, clean spectral, flat layout → `preimport_decide → accept` → quality-gate fires → beets mutation. Existing test coverage of the accept path still passes.
- Reject path: evidence with `audio_corrupt=true` → `preimport_decide → reject("audio_corrupt")` → rejection helper fires → `download_log` row, denylist (5-strikes), job `status='failed'`, request → `wanted`. Covers AE2.
- Reject path: evidence with `folder_layout='nested'` → reject("nested_layout") → helper fires. Covers AE3.
- Reject path: evidence with `audio_file_count=0` → reject("empty_fileset") → helper fires. Covers AE4.
- Reject path: evidence with suspect spectral that fails `spectral_import_decision` → reject("spectral_reject") → helper fires.
- Integration slice: claim evidence_ready job → decide → reject → poll loop on next tick observes request in `wanted` and re-runs search.
- Grep verification: `grep -rn "preimport_decide" lib/ scripts/` shows it imported only by `lib/import_dispatch.py` (and the tests).

**Verification:**
- `nix-shell --run "bash scripts/run_tests.sh"` clean.
- `grep -rn "preimport_decide" lib/import_preview.py scripts/import_preview_worker.py` — zero matches.
- Pyright clean.
- `grep -rn "run_preimport_gates" lib/ scripts/` — only the new measurement helper name if renamed, or zero matches if the legacy name was deleted.

---

### U7. Recovery sweep migration (020) + grep-clean retirement

**Goal:** A one-time SQL migration that flips every existing `import_jobs.preview_status='uncertain'` row back to a state the preview worker re-claims, so the 315 stuck rows recover through the new path on first deploy. Plus final grep-clean of any references to the retired constants and a confirmation that production code emits exactly two preview outcomes.

**Requirements:** R8 (production no longer writes uncertain), R9 (recovery sweep frees stuck rows).

**Dependencies:** U2 (validate_audio fixed so 90 audio_corrupt rows don't re-stick), U3 (measurement helper exists), U4 (rejection helper exists), U5 (preview no longer writes uncertain), U6 (importer decides on facts).

**Files:**
- Create: `migrations/020_recover_stuck_preview_uncertain_jobs.sql`
- Modify: any remaining test files that mocked the legacy `'uncertain'` state.
- Test: `tests/test_migrator.py` — assert 020 applies cleanly + idempotent (running twice flips nothing the second time).
- Test: `tests/test_integration_slices.py` — pre-seed an `import_jobs` row with `preview_status='uncertain'`, run the migration, assert the row is in a state the preview-worker claim query selects.

**Approach:**
- Pattern follows `migrations/018_neutral_import_job_preview_ready.sql:45-67` exactly:

```sql
UPDATE import_jobs
SET preview_status = 'waiting',
    preview_result = NULL,
    preview_message = 'Recovered by preview-never-decides refactor (020)',
    preview_error = NULL,
    preview_worker_id = NULL,
    preview_started_at = NULL,
    preview_heartbeat_at = NULL,
    preview_completed_at = NULL,
    importable_at = NULL,
    updated_at = NOW()
WHERE status = 'queued'
  AND preview_status = 'uncertain';
```

- On deploy, preview workers claim these rows on their next tick. Each row is re-measured under the new contract:
  - Audio that previously hit #251's stderr false-positive now measures clean (U2 fix). The job marks `evidence_ready` with `audio_corrupt=false`. Importer accepts; beets imports.
  - Audio that genuinely failed `spectral_import_decision` measures clean facts (the spectral measurement was correct under the old contract too). The job marks `evidence_ready`. Importer decides reject; self-healing finalize fires; request returns to `wanted`; search resumes.
  - Genuinely corrupt audio (rc!=0) marks `audio_corrupt=true`. Importer rejects; self-healing finalize fires; request returns to `wanted`.
- Result: zero rows remain in `preview_status='uncertain'` after the post-deploy preview-worker tick. All 315 parent requests transition to either `imported` or `wanted` (poll-advanceable).
- Final grep-clean: `grep -rn "uncertain" lib/ scripts/ tests/` — any remaining matches should be in migration files (historical) or test assertions that exercise the migration. No production write paths remain.

**Patterns to follow:**
- `migrations/018_neutral_import_job_preview_ready.sql:45-67`
- `migrations/006_normalize_legacy_terminal_preview_jobs.sql`

**Test scenarios:**
- Happy path: migration 020 applies cleanly; rows in `preview_status='uncertain'` flip to `preview_status='waiting'` with `preview_result=NULL` etc.
- Edge case: idempotency — running the migration twice doesn't double-flip anything (the WHERE clause guard).
- Edge case: rows already in a different `preview_status` (e.g. `'evidence_ready'`, `'error'`) are untouched.
- Integration slice: pre-seed a stuck row, apply migration, claim via the preview worker query, observe re-measurement → evidence_ready → importer decision. Covers AE8.
- Manual verification post-deploy: the count of `import_jobs` in `preview_status='uncertain'` drains to zero over a measurement-campaign window — typically 30-60 minutes given current preview-worker concurrency and per-album measurement cost (ffmpeg + sox spectral takes seconds-to-tens-of-seconds per album, and 315 rows is a sustained drain rather than a single-tick burst). Monitor with `pipeline-cli query --json "SELECT preview_status, COUNT(*) FROM import_jobs WHERE created_at > <deploy_time> GROUP BY preview_status"` every 5 minutes for the first hour. Final state: all 315 parent requests are in (`imported`, `wanted`, `manual`); none locked in `downloading` with an inactive import job.

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_migrator tests.test_integration_slices -v"` passes.
- `grep -rn "PREVIEW_FAILURE_STATUS\|IMPORT_JOB_PREVIEW_UNCERTAIN" lib/ scripts/ tests/` — zero matches (or only matches in migration-history docs / 020's own test).
- Post-deploy: zero rows in `preview_status='uncertain'`.

---

## System-Wide Impact

- **Interaction graph:** Preview worker (`scripts/import_preview_worker.py`) calls a new measurement helper instead of the fused gates; importer (`scripts/importer.py` via `lib/import_dispatch.py`) calls `preimport_decide` and the shared rejection-finalize helper. `lib/transitions.py::finalize_request` continues to be the single finalize-to-wanted entry point. The poll loop (`lib/download.py::_poll_one_active_download`) gains correctness via the same observable contract (job not active → poll proceeds → search resumes).
- **Error propagation:** Measurement helper exceptions surface as `MeasurementFailure(reason="measurement_crashed", ...)` rather than uncaught crashes. The rejection-finalize helper is the same shape on both sides; one log row per terminal outcome.
- **State lifecycle risks:** The recovery sweep at U7 is a one-time SQL UPDATE; idempotent guard prevents double-application. Mid-deploy the preview worker may be claiming a job whose row is being updated by the migration — `cratedigger-db-migrate.service` runs before the workers start (see CLAUDE.md § Database migrations), so this race is impossible by deployment order **provided the preview worker's systemd unit has `Requires=cratedigger-db-migrate.service` and `After=cratedigger-db-migrate.service` like the importer and web units**. Verify in `nix/module.nix` before deploy; if missing, add the dependency in U1 (alongside the migration that needs it). Restart-if-changed=false on both the importer and preview worker (already in place from 2026-05-15) means in-flight work isn't killed on deploy; any `running` job is recovered via existing requeue mechanisms.
- **API surface parity:** No web routes added or changed. No CLI subcommand additions in this plan (the existing `pipeline-cli show <id>` already surfaces the evidence columns via JSON; new fields appear automatically once Structs are extended). No new operator action introduced, so the CLI ⇄ API symmetry rule (CLAUDE.md § CLI ⇄ API surface symmetry) has nothing to fan out.
- **Integration coverage:** U4, U5, U6, U7 each include integration slices in `tests/test_integration_slices.py` proving real-code-path behavior. The pure decision function is unit-tested with subTest tables (U3) and exercised end-to-end via the slices (U5/U6). The shared self-healing helper is the single seam — one orchestration test per failure-class proves the four side effects fire correctly.
- **Unchanged invariants:** `load_candidate_evidence_for_source` snapshot guard semantics unchanged; per-user 5-strikes denylist rule unchanged; Wrong Matches cleanup path unchanged; force-import dispatch contract unchanged (force-import enqueues an `import_job` and flows through the same preview → importer pipeline). Quality-gate decisions (`spectral_import_decision`, `import_quality_decision`, `quality_gate_decision`) unchanged — `preimport_decide` is upstream of them, not a replacement.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| The split in U3 introduces a temporary bridge where both old `run_preimport_gates` and new `measure_preimport_state` coexist; if U5/U6 don't fully migrate every caller, both code paths persist into production. | U7's final grep-clean is the enforceable gate. PR cannot merge if `grep` finds remaining old callers. The legacy `run_preimport_gates` is deleted in U6 (or U7) once `grep -rn "run_preimport_gates" lib/ scripts/` is empty. |
| U2 changes `validate_audio` policy; some pre-existing test fixture that asserted rejection on `rc=0+stderr` could be a real positive we're now silently accepting. | The brainstorm's #251 evidence is direct: every `rc=0` case sampled was a false positive. U2's test scenarios pin every documented false-positive pattern as a regression. Any real-corruption case must have `rc!=0`. If a fixture flips, audit it carefully. |
| The recovery sweep at U7 re-measures 315 rows on deploy; if the new code path has any unfixed bug, all 315 produce wrong outcomes in a single tick window. | Land U7 last, after every other unit's tests are clean. Run the full integration slice suite (U4, U5, U6, U7) before deploy. Post-deploy, monitor the live count via `pipeline-cli query "SELECT preview_status, COUNT(*) FROM import_jobs WHERE created_at > <deploy_time> GROUP BY preview_status"` for the first 30 minutes. If anomalies surface, the rollback shape is "the rows are already in `'waiting'` — let preview retry, fix forward." |
| 90 audio_corrupt-stuck rows depend on U2 landing in the same deploy as U7. If a partial deploy ships U7 but not U2, those 90 re-stick under the buggy validator. | U2 and U7 are in the same PR. Deploy is atomic at the flake-input level. No partial-merge path exists. |
| `MeasurementFailure` is a new wire-boundary type; if downstream consumers (recents UI in `web/`) don't handle the new shape, they crash or render blank pills on display. | U5 explicitly modifies `web/js/recents.js` to add `measurement_failed` badge and border handlers, and adds a `tests/test_js_util.mjs` unit test that exercises the new value. Backend contract: `preview_result` JSONB shape is additive — existing keys (`verdict`, `decision`, `reason`, `detail`, `stage_chain`) remain present where the old code path populated them; new `MeasurementFailure`-shaped rows carry `reason`, `detail`, `source_path` plus a synthesised top-level `verdict='measurement_failed'` for frontend grep-compatibility. Manual verification by loading the recents tab against a test DB with one `MeasurementFailure` row before merging. |
| The new evidence columns (U1) default to safe values, but if a real evidence row predating U1 is loaded and decoded into the new Struct shape, the defaults must be backward-compatible. | `audio_corrupt=FALSE`, `decode_ok=TRUE`, `audio_file_count=0` are the SQL defaults. Decoding a legacy row into the extended Struct populates these defaults. The decision function reads them defensively (e.g. `audio_corrupt=False` + `audio_file_count=0` is a valid "empty evidence" shape that the decision function rejects on empty_fileset, which matches the brainstorm AE4). |

---

## Documentation / Operational Notes

- **No downstream wrapper changes required.** Unlike 2026-05-15 (which deleted `services.cratedigger.importer.preview.enable`), this refactor does not remove a Nix option. No coordinated `~/nixosconfig/modules/nixos/services/cratedigger.nix` edit needed before flake-input bump.
- **Deploy ordering is standard.** Per `.claude/rules/deploy.md`: push cratedigger code → `nix flake update cratedigger-src` on doc1 → `nixos-rebuild switch` on doc2. `cratedigger-db-migrate.service` runs both migration 019 (schema) and 020 (recovery sweep) before workers start.
- **Pre-deploy backup recommended.** Migration 019 is additive (new columns with defaults, no destructive changes). Migration 020 is a state-flip on `import_jobs`. Per `.claude/rules/deploy.md`: `ssh doc2 'pg_dump -h 192.168.100.11 -U cratedigger cratedigger' > /tmp/cratedigger_backup_$(date +%Y%m%d_%H%M%S).sql` before deploy.
- **VM check before deploy.** No `nix/module.nix` changes are expected in this plan, but if any service-level changes are made (e.g. for the worker's measurement-failure logging surface), run `nix build .#checks.x86_64-linux.moduleVm` first.
- **Post-deploy verification commands:**
  ```bash
  ssh doc2 'pipeline-cli query --json "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 3"'
  ssh doc2 'pipeline-cli query --json "SELECT preview_status, COUNT(*) FROM import_jobs GROUP BY preview_status"'
  ssh doc2 'pipeline-cli query --json "SELECT outcome, COUNT(*) FROM download_log WHERE created_at > NOW() - INTERVAL ''1 hour'' GROUP BY outcome"'
  ```
  Expected within one preview-worker tick: zero rows in `preview_status='uncertain'`; new `outcome='measurement_failed'` rows appearing where source-vanished / snapshot-stale scenarios fire.
- **Closes three issues on merge.** PR description should reference #250, #251, #252 explicitly with "Closes" so they auto-close on merge.

---

## Sources & References

- **Origin document:** `docs/brainstorms/2026-05-16-preview-never-decides-requirements.md`
- **Extends:** `docs/plans/2026-05-15-002-refactor-importer-never-measures-plan.md` (and its origin `docs/brainstorms/2026-05-15-importer-never-measures-requirements.md`)
- **Parent in series:** `docs/plans/2026-05-14-001-refactor-quality-evidence-decision-boundary-plan.md`
- **Related issues:** #250 (stuck-forever symptom), #251 (`validate_audio` false-positives), #252 (preview-decides architectural smell)
- **Related code:** `lib/preimport.py`, `lib/import_preview.py`, `lib/quality.py`, `lib/quality_evidence.py`, `lib/pipeline_db.py`, `lib/import_dispatch.py`, `lib/transitions.py`, `lib/util.py`, `scripts/import_preview_worker.py`, `scripts/importer.py`
- **Migration precedents:** `migrations/017_album_quality_evidence.sql`, `migrations/018_neutral_import_job_preview_ready.sql`, `migrations/009_curator_ban_outcome.sql`
- **Test infrastructure:** `tests/test_quality_decisions.py`, `tests/test_integration_slices.py`, `tests/fakes.py::FakePipelineDB`, `tests/test_pipeline_db.py`
