---
date: 2026-05-17
topic: propagate-source-evidence-on-transcode
---

# Propagate Source-Side Evidence on Transcoded Imports

## Problem Frame

`propagate_candidate_evidence_to_current` (lib/quality_evidence.py:795) writes the
library-side `album_quality_evidence` row after an import succeeds. For
**renamed-only** imports it propagates the candidate's full measurement payload
(spectral grade, V0 lineage, bad-audio-hash matches, verified-lossless proof).
For **transcoded** imports (FLAC → V0 / Opus) it propagates only
`verified_lossless_proof`; spectral, V0 lineage, and bad-audio-hash fields are
zeroed.

The original reasoning was "spectral/V0 describe source audio that's gone after
transcode." That reasoning is internally inconsistent: `verified_lossless_proof`
also describes the source's provenance, and we keep it across transcode because
lineage matters. The current rule effectively says **proof-of-good survives
transcode; proof-of-compromised does not**.

The asymmetry produces a concrete bug. Live reproducer: request 3779 (Lil Wayne
— *Da Drought 3*). A transcoded-FLAC import landed at 16:06; a second
identical-quality FLAC arrived at 18:32. Wrong-match cleanup triage called
`full_pipeline_decision_from_evidence` against the library row, found the
on-disk evidence had NULL spectral/V0 fields, fell through to
`provisional_lossless_upgrade` → `kept_would_import`, and the folder stayed in
the queue for manual review even though the operator had already imported an
identical source 31 minutes earlier.

## Scope

Reverse the propagation asymmetry so transcoded imports also carry the
source-side evidence fields forward to the library row, for **all** transcoded
imports (not gated on source codec). Forward-only — existing library rows are
not backfilled.

## Requirements

**Propagation policy**

- R1. `propagate_candidate_evidence_to_current` must propagate `spectral_grade`
  and `spectral_bitrate_kbps` from the candidate measurement to the library
  measurement on transcoded imports.
- R2. It must propagate `v0_source_lineage` and the
  `v0_min_bitrate_kbps` / `v0_avg_bitrate_kbps` / `v0_median_bitrate_kbps` trio
  from the candidate evidence to the library evidence on transcoded imports.
- R3. It must propagate `matched_bad_audio_hash_id` and
  `matched_bad_audio_hash_path` from the candidate evidence to the library
  evidence on transcoded imports.
- R4. The docstring at lib/quality_evidence.py:825-847 must be updated to
  reflect the reversed rule and re-state the lineage semantic: these fields now
  describe the upstream source audio at import time, not the on-disk file.

**Semantic contract**

- R5. `current_spectral_grade` / `current_spectral_bitrate` on `album_requests`
  (mirrored from the library evidence row by `lib/import_service.py`) now
  describe the source the library row was made from, not the file on disk. Any
  downstream consumer that assumed "file at source_path" semantics inherits the
  new contract; no consumer requires special-casing today.
- R6. Source-replacement must overwrite stale evidence. When a clean
  lossless-source candidate is force-imported over a previously-transcoded
  library row, the new candidate's evidence propagation must overwrite the
  stale `likely_transcode` / `lossless_source` fields with the new candidate's
  values. (This is the existing behaviour of the upsert + propagation pair; the
  requirement is to verify, not to change.)

**Forward-only**

- R7. No backfill of existing transcoded library rows. They retain their
  current NULL spectral/V0/bad-hash fields until the album is re-imported or
  force-imported, at which point the new policy applies.
- R8. The asymmetry between pre-change and post-change library rows is an
  accepted known wart. Wrong-match triage continues to return
  `kept_would_import` against pre-change transcoded library rows until they
  are touched.

## Acceptance Examples

- **AE1. Same-source duplicate is rejected by triage** (covers R1, R2).
  Given a transcoded-FLAC library row imported under the new policy with
  `spectral_grade=likely_transcode`, `spectral_bitrate=128`,
  `v0_source_lineage=lossless_source`, `v0_avg=215`, and a new candidate
  arrives with the same source-side evidence,
  `full_pipeline_decision_from_evidence(import_mode="force")` must return a
  reject-class decision (e.g. `lossless_source_not_better`,
  `suspect_lossless_downgrade`) rather than `provisional_lossless_upgrade`.
  Wrong-match cleanup triage classifies the outcome as `confident_reject` and
  the folder becomes cleanup-eligible.

- **AE2. Lossy candidate against transcoded-FLAC row is locked out** (covers
  R2). Given the same library row as AE1, a lossy MP3 V0 candidate must
  trigger `lossless_source_locked` at the provisional-lossless gate
  (lib/quality.py:2341-2357), producing `confident_reject` and cleanup
  eligibility. This is a behaviour change relative to today's NULL
  v0_source_lineage — the lock fires where previously the candidate fell
  through to `provisional_lossless_upgrade`.

- **AE3. Bad-rip hash propagates** (covers R3). Given a candidate evidence
  row with a non-NULL `matched_bad_audio_hash_id` that imports as a transcoded
  FLAC → V0, the resulting library evidence row must carry the same
  `matched_bad_audio_hash_id` and `matched_bad_audio_hash_path`. A subsequent
  candidate matching the same bad-audio-hash must be detected against the
  library row.

- **AE4. Source-replacement overwrites** (covers R6). Given a library row
  with `spectral_grade=likely_transcode` (propagated from a compromised
  source), when a clean genuine FLAC force-imports over it, the resulting
  library evidence row must reflect `spectral_grade=genuine` (or the new
  source's actual grade), not the stale `likely_transcode`.

- **AE5. Renamed-only path unchanged** (regression guard). Given a
  lossless-stored-as-lossless import (FLAC kept as FLAC), the propagated
  library row must carry the same evidence fields as today. No behaviour
  change on the rename-only path.

## Test Obligations

Per `.claude/rules/code-quality.md` taxonomy:

- **Pure tests** (`tests/test_quality_evidence.py` or equivalent): assert
  field-by-field propagation for transcoded imports — spectral, V0 trio, bad
  hash — and a regression assertion that rename-only behaviour is unchanged.
- **Live-bug reproduction** in
  `tests/test_quality_classification.py::TestLiveBugReproductions`: the
  Lil Wayne — *Da Drought 3* (request 3779) scenario as AE1, plus its parity
  exercise through `TestLiveBugReproductionsThroughEvidencePipeline`.
- **Integration slice** in `tests/test_integration_slices.py`: round-trip
  through `propagate_candidate_evidence_to_current` then a triage call against
  the resulting library row, asserting the `kept_would_import` → reject
  transition.
- **Search-planner regression**: extend an existing
  `compute_effective_override_bitrate` test to cover the case where a
  transcoded library row now carries a `spectral_grade=likely_transcode` /
  `spectral_bitrate=128` pair, asserting the MP3 V0 override drops from
  container (~225) to spectral (128) — matching today's rename-only behaviour.

## Scope Boundaries

- Do not backfill existing transcoded library rows.
- Do not narrow search-plan tiers based on `v0_source_lineage=lossless_source`.
  That follow-up is the natural shape of the buckets work (see
  `docs/brainstorms/quality-bucket-system-requirements.md` — once buckets are
  in, search asks only for `verified`-bucket candidates when the existing row
  is `verified`, which dissolves the wasted-search window noted below).
- Do not change `full_pipeline_decision_from_evidence` or any pure decider in
  `lib/quality.py`. The deciders already do the right thing when both sides
  have comparable evidence; this change just makes the library side carry the
  evidence forward.
- Do not change UI copy or wrong-match triage display labels. The audit row
  already carries `verdict`, `preview_decision`, and `stage_chain` — these
  will surface the new outcomes (e.g. `lossless_source_locked`) without
  additional plumbing.

## Key Decisions

- **Symmetric over scoped.** Propagate the source-side fields for all
  transcoded imports, not just lossless-source candidates. The asymmetry is
  rename-only vs transcoded, not lossless vs lossy; the symmetric fix is the
  coherent one.
- **Forward-only over backfill.** Backfill via the candidate-evidence FK
  chain (`download_log.candidate_evidence_id` → `album_quality_evidence`) was
  considered. Rejected for this change because (a) the bug is operator-
  visible only on new wrong-matches, (b) backfill compounds risk on the
  search-planner semantic shift, (c) organic re-touch over time closes the
  asymmetry without a sweep. Revisit if false-positive triage keeps remain
  noisy after a few weeks.
- **Lossless-source lock is accepted, not loosened.** The propagation change
  causes `lossless_source_locked` to fire on transcoded-from-FLAC library
  rows for any lossy candidate. That is the intended behaviour under both the
  current spectral-distrust posture and the future bucket model — a transcoded
  copy of a lossless source still has lossless lineage, and only another
  lossless source can override that lineage.

## Dependencies / Assumptions

- The current `provisional_lossless_decision` already returns
  `lossless_source_locked` correctly when `existing_probe` is a comparable
  lossless-source V0 probe (lib/quality.py:2341-2357). No deciders change.
- `lib/import_service.py:135-136` already writes `spectral_grade` from the
  library measurement back to `album_requests.current_spectral_grade`. The
  new fields flow through this existing path without additional plumbing.
- `compute_effective_override_bitrate` (lib/quality.py:2821) already
  consumes `spectral_grade` correctly via `SPECTRAL_TRANSCODE_GRADES`. Its
  behaviour change for transcoded library rows is the same shift that already
  happens for rename-only library rows today; this change makes the two paths
  consistent.

## Known Consequence: Temporary Wasted-Search Window

Until the buckets work narrows search tiers based on existing-bucket, search
will continue to ask Soulseek for lossy candidates against transcoded-FLAC
library rows. Those candidates come back, hit triage, get
`lossless_source_locked`, and get cleanup-deleted. Net: more slskd churn and
more wrong-matches queue entries that auto-delete. No different files on disk.

This window closes when search-plan tier selection moves to the bucket model
(`docs/brainstorms/quality-bucket-system-requirements.md` R1, R6).

## Outstanding Questions

### Resolve Before Planning

- None.

### Deferred to Planning

- [Affects R1, R2, R3][Technical] Identify the outer-row construction site in
  `propagate_candidate_evidence_to_current` for `v0_source_lineage`, the v0
  bitrate trio, and the bad-audio-hash pair; current docstring suggests the
  inner `AudioQualityMeasurement` build at lib/quality_evidence.py:879-894
  handles spectral but the v0/bad-hash fields live on the outer
  `AlbumQualityEvidence` row.
- [Affects R6][Technical] Confirm via test that the
  `upsert_album_quality_evidence` + `propagate` flow overwrites stale fields
  on source-replacement rather than preserving them via partial update.

## Next Steps

→ Use `/ce-plan` for structured implementation planning. The change is small
   enough (one function plus tests) that planning will be light; the main
   planning work is the test matrix in §Test Obligations and the outer-row vs
   inner-measurement field-location question in §Deferred to Planning.
