---
date: 2026-05-17
topic: propagate-source-evidence-on-transcode
---

# Propagate Source-Side Evidence on Lossless-Source Transcoded Imports + Narrow Search on Lossless-Source Lock

## Problem Frame

Two coupled gaps in the current pipeline:

1. **`propagate_candidate_evidence_to_current` strips spectral / V0 / bad-hash
   fields on every transcoded import**, including the FLAC → V0/Opus case
   where the source is lossless and its source-side evidence is meaningful for
   future candidates. The function only preserves `verified_lossless_proof`.
2. **`lossless_source_locked` rejections don't narrow the search plan.** Even
   though triage classifies a lossy candidate against a lossless-source
   library row as `confident_reject`, the search planner keeps asking
   Soulseek for the same album with no filetype filter on the next cycle.
   New peers serve the same lossy file, it's downloaded, rejected again,
   indefinitely.

Both gaps are visible in the live reproducer: request 3779 (Lil Wayne —
*Da Drought 3*). A transcoded-FLAC import landed at 16:06 UTC; a second
identical-quality FLAC arrived at 18:32. Wrong-match cleanup triage called
`full_pipeline_decision_from_evidence(import_mode="force")`, found the
on-disk evidence row had NULL `spectral_grade` / `v0_metric` /
`matched_bad_audio_hash_id`, fell through to `provisional_lossless_upgrade` →
`kept_would_import`. Even if propagation had been correct, the search planner
would still keep asking for the album with no filetype narrowing — the lock
would fire repeatedly until peer cooldowns burn out.

The two changes belong in the same PR because they are the same architectural
intent: **once a library row carries lossless-source evidence, the system
should both (a) recognize that evidence on the next candidate, and (b) stop
searching for non-lossless candidates that can never override it.** Shipping
(a) without (b) creates a wasted-cycle window.

This is consistent with the direction in
`docs/brainstorms/quality-bucket-system-requirements.md` — under buckets, the
lock dissolves into bucket-comparison and the search narrowing is just
"search same-bucket-or-above." This PR is the transitional step that mimics
that behavior inside the current spectral system without waiting for the
full buckets rewrite.

## Scope

**In scope:**
- Gate `propagate_candidate_evidence_to_current` source-side propagation on
  whether the candidate source is a lossless codec. Lossless-source
  transcodes propagate spectral / V0 / bad-hash; non-lossless transcodes
  keep today's strip behavior.
- When `lossless_source_locked` fires (importer worker AND wrong-match
  cleanup triage), narrow the request's `search_filetype_override` to
  `"lossless"` so future cycles only ask for lossless candidates.

**Out of scope:**
- No backfill of existing transcoded library rows.
- No decider changes in `lib/quality.py` (the existing
  `provisional_lossless_decision` already handles the lock correctly given
  the right inputs).
- No UI / copy / triage-display changes.
- No `import_service.py` changes; downstream `current_spectral_*` flow
  inherits the narrower policy naturally.
- No force-import-bypass-the-lock affordance. Operator escape hatches stay
  as-is (bad-rip ban + manual evidence deletion).
- No new CLI / web API surface; this is automated pipeline behavior, not an
  operator action.

## Requirements

**Propagation policy (lossless-source gated)**

- R1. `propagate_candidate_evidence_to_current` must propagate
  `spectral_grade`, `spectral_bitrate_kbps`, `v0_metric`,
  `matched_bad_audio_hash_id`, and `matched_bad_audio_hash_path` from the
  candidate to the library evidence row when the candidate is a lossless
  source (codec in `LOSSLESS_CODECS = {"flac", "alac", "wav"}`).
- R2. For non-lossless transcoded imports (e.g., MP3 → Opus), the function
  must continue to strip those fields onto NULL — the source's spectral /
  V0 lineage is not meaningfully comparable across future candidates and
  storing it provides no decision value.
- R3. For renamed-only imports (any source codec), the function must
  continue to propagate all source-side fields — today's behavior unchanged.
- R4. `verified_lossless_proof` and `verified_lossless` continue to
  propagate in all cases — today's behavior unchanged.
- R5. The function's `target_format` parameter is unused after the gate is
  re-shaped; remove it from the signature.

**Search narrowing (lossless-source lock → lossless-only override)**

- R6. A new pure helper in `lib/quality.py` —
  `narrow_override_on_lossless_source_lock(current: str | None) -> str | None`
  — must return `"lossless"` when the lock fires unless the current override
  is already `"lossless"` (idempotent narrowing returns `None`).
- R7. The importer-side `lossless_source_locked` rejection in
  `lib/import_dispatch.py` must call this helper and pass the narrowed
  override through `_record_rejection_and_maybe_requeue` (precedent: the
  `downgrade` branch immediately above already does this with
  `narrow_override_on_downgrade`).
- R8. The wrong-match cleanup triage path in
  `lib/wrong_match_cleanup_service.py` must call this helper when a
  deletion completes with `preview_decision == "lossless_source_locked"`
  and persist the narrowed override on the request row.
- R9. Search-plan persistence is unaffected — `generate_search_plan`
  (`lib/search.py`) produces query strategies, not filetype slices. The
  filetype filter is applied downstream in
  `enqueue.py::effective_search_tiers` from the request's
  `search_filetype_override` column. No plan invalidation or
  `SEARCH_PLAN_GENERATOR_ID` bump needed.

**Semantic contract**

- R10. `current_spectral_grade` / `current_spectral_bitrate` on
  `album_requests` (mirrored from the library row by
  `lib/import_service.py`) inherit the narrower propagation rule naturally.
  For lossless-source transcoded library rows, these now describe the
  upstream source. For non-lossless transcoded library rows, they remain
  NULL as today.
- R11. Source-replacement (a clean lossless-source candidate force-imports
  over a previously-transcoded lossless-source library row) overwrites the
  stale fields via the existing `upsert_album_quality_evidence` ON CONFLICT
  DO UPDATE behavior. No new code; the requirement is verified via test.

**Forward-only**

- R12. No backfill of existing transcoded library rows. They retain their
  current NULL spectral / V0 / bad-hash fields until the album is
  re-imported or force-imported.
- R13. The asymmetry between pre-change and post-change library rows is an
  accepted known wart. Wrong-match triage continues to return
  `kept_would_import` against pre-change transcoded library rows until they
  are touched.

## Acceptance Examples

- **AE1. Lossless-source duplicate is rejected by triage AND search
  narrows** (covers R1, R7, R8). Given a transcoded-FLAC library row
  imported under the new policy with `spectral_grade=likely_transcode`,
  `v0_metric` carrying `lossless_source` lineage at avg 215 / min 184, when
  a second identical-quality FLAC arrives:
    - `full_pipeline_decision_from_evidence(import_mode="force")` returns
      `suspect_lossless_downgrade` (or a sibling reject-class decision).
    - Wrong-match cleanup triage classifies the outcome as
      `confident_reject` and the folder becomes cleanup-eligible.
    - After cleanup deletion, the request's `search_filetype_override` is
      `"lossless"`.

- **AE2. Lossy candidate against lossless-source library row triggers
  `lossless_source_locked` AND narrows search** (covers R7, R8). Given the
  same library row as AE1, a lossy MP3 V0 candidate must hit
  `provisional_lossless_decision`'s lock branch
  (`lib/quality.py:2341-2357`), produce `confident_reject` + cleanup
  eligibility, and the cleanup must persist
  `search_filetype_override="lossless"` on the request. The next search
  cycle for that request only asks for lossless tiers.

- **AE3. Bad-rip hash propagates on lossless-source transcoded import**
  (covers R1). Given a lossless-source candidate evidence row with a
  non-NULL `matched_bad_audio_hash_id` that imports as a transcoded
  FLAC → V0, the resulting library evidence row must carry the same
  `matched_bad_audio_hash_id` and `matched_bad_audio_hash_path`. A
  subsequent candidate matching the same bad-audio-hash is detected against
  the library row.

- **AE4. Source-replacement overwrites stale propagated evidence** (covers
  R11). Given a lossless-source-transcoded library row with
  `spectral_grade=likely_transcode`, when a clean genuine FLAC force-imports
  over it, the resulting library evidence row reflects
  `spectral_grade=genuine` and the new candidate's V0 metric and verified
  lossless proof. No stale values survive the upsert.

- **AE5. Non-lossless-source transcoded import does NOT propagate
  source-side evidence** (covers R2). Given an MP3 source candidate that
  imports as a transcoded MP3 → Opus (e.g., a low-bitrate MP3 the user
  asked the pipeline to re-encode to Opus), the resulting library evidence
  row's `spectral_grade`, `v0_metric`, `matched_bad_audio_hash_id`, and
  `matched_bad_audio_hash_path` must be NULL. Behavior unchanged from
  today.

- **AE6. Renamed-only path unchanged** (covers R3). Given a
  lossless-stored-as-lossless import (FLAC kept as FLAC) and any
  renamed-only import where source codec equals library codec, the
  propagated library row must carry the same evidence fields as today.
  Behavior unchanged.

- **AE7. Idempotent narrowing** (covers R6). Given a request whose
  `search_filetype_override` is already `"lossless"`, when
  `lossless_source_locked` fires again, the narrowing helper returns
  `None` (no-op) and the override stays `"lossless"`.

## Key Decisions

- **Lossless-source-gated, not symmetric.** The earlier framing of
  "propagate everything for all transcoded imports symmetrically" was
  scope creep. The user-stated intent is narrower: propagate source-side
  evidence specifically when the source is lossless, because that's the
  only case where the source-side spectral / V0 lineage is meaningfully
  comparable against future candidates.
- **Search narrowing is in scope, not deferred.** The propagation reversal
  alone creates a wasted-cycle window (lock fires → cleanup deletes →
  search re-asks → repeat). Closing that window with
  `search_filetype_override="lossless"` is the companion change that makes
  the propagation policy actually pay off. Without it, the lock is half a
  system.
- **`narrow_override_on_lossless_source_lock` is a pure helper at two call
  sites, not a decider change.** The deciders in `lib/quality.py` already
  return `lossless_source_locked` correctly; the new helper is rejection-
  side handling at the orchestration layer.
- **Forward-only over backfill.** Existing transcoded library rows are not
  retroactively populated. Same reasoning as before — organic re-touch
  closes the gap over time; backfill compounds risk on the search-planner
  semantic shift.
- **Aligned with the bucket model's trajectory.** Per
  `docs/brainstorms/quality-bucket-system-requirements.md` R15, spectral
  is being demoted to a bucket-modifier signal. This PR doesn't entrench
  the spectral system; it makes the spectral-era lock plus search-narrowing
  match the behavior the bucket model will produce naturally
  ("only grind up quality within the bucket or above where we are").

## Test Obligations

Per `.claude/rules/code-quality.md` taxonomy:

- **Pure tests** in `tests/test_quality_decisions.py`: new test class for
  `narrow_override_on_lossless_source_lock` covering R6 cases (None input,
  "lossless" input, lossy override input).
- **Pure / structural test** in `tests/test_quality_evidence.py` or a
  parametric extension of existing propagation tests: the lossless-source
  gate behaves as specified for the four cases — renamed-only lossless,
  renamed-only lossy, transcoded lossless source, transcoded lossy source.
- **Live-bug reproduction** in
  `tests/test_quality_classification.py::TestLiveBugReproductions` +
  `TestLiveBugReproductionsThroughEvidencePipeline`: the Lil Wayne
  (request 3779) FLAC-source same-source-duplicate scenario, with parity
  assertion that simulator and evidence-pipeline reach the same decision.
- **Integration slice** in `tests/test_integration_slices.py`: end-to-end
  propagate → wrong-match cleanup triage → assert (a) outcome is
  `OUTCOME_DELETED`, (b) the request's `search_filetype_override` ends up
  as `"lossless"`.
- **Orchestration test** for importer-side rejection in
  `lib/import_dispatch.py`: when `lossless_source_locked` fires during
  import, the request's `search_filetype_override` ends up as
  `"lossless"`. Reuses the `FakePipelineDB` pattern from existing dispatch
  tests.
- **Negative test** for AE5: a non-lossless source transcoded import does
  NOT propagate spectral / V0 / bad-hash to the library row.

## Scope Boundaries

- Do not backfill existing transcoded library rows (R12).
- Do not change `full_pipeline_decision_from_evidence` or any pure decider
  in `lib/quality.py`.
- Do not change UI / copy / triage-display.
- Do not add a force-import-bypass-the-lock affordance.
- Do not bump `SEARCH_PLAN_GENERATOR_ID` — plan output unchanged.

### Deferred to Follow-Up Work

- **Bucket-aware search-plan tier ordering** (the broader buckets work).
  This PR narrows the override at the lock site; the buckets brainstorm
  generalizes "search same-bucket-or-above" as the primary mechanism.
  When buckets land, the lock and the helper introduced here are likely
  obsolete in favor of pure bucket comparison.
- **Bad-rip ban evidence cleanup**
  (`lib/release_cleanup.py::clear_on_disk_quality_fields` doesn't null
  `current_evidence_id`). Sharpened by this PR but pre-existing and
  separate from the propagation/narrowing intent.
- **Force-import override of `lossless_source_locked`.** Operator escape
  hatch from the lock — needs a design decision (always bypass on force?
  explicit "unlock-source" CLI action?). Not in this PR.

## Dependencies / Assumptions

- `lib/quality.py::LOSSLESS_CODECS = frozenset({"flac", "alac", "wav"})`
  is the right gating set.
- `provisional_lossless_decision` already returns `lossless_source_locked`
  correctly when `existing_probe` is a comparable lossless-source V0 probe
  (`lib/quality.py:2341-2357`). No decider changes.
- `_record_rejection_and_maybe_requeue` in `lib/import_dispatch.py`
  already accepts a `search_filetype_override` argument that flows through
  to the request row update. No new transition plumbing needed.
- `db.set_request_*` patterns exist in `lib/pipeline_db.py` for updating
  the override column; specific method name to be confirmed during
  implementation.
- `generate_search_plan` in `lib/search.py` produces query strategies only;
  the filetype filter is applied downstream in `enqueue.py`, so updating
  the request's override takes effect on the next cycle without plan
  regeneration.
- `compute_effective_override_bitrate` (`lib/quality.py:2821`) already
  consumes `spectral_grade` correctly. Its behavior shift for lossless-
  source transcoded library rows is intentional and matches today's
  rename-only behavior.

## Outstanding Questions

### Resolve Before Planning

- None — research at the end of the planning bootstrap confirmed the call
  sites and the helper shape.

### Deferred to Planning

- The exact method name / signature for persisting the narrowed override
  on the request row from `lib/wrong_match_cleanup_service.py` (does an
  existing transition helper already cover this, or does it need a small
  new wrapper?).
- Whether the `is_transcode` detection block can be partially preserved
  (since the gate is now `is_transcode AND NOT source_is_lossless`) or
  re-derived inline.

## Next Steps

→ `/ce-plan` updates the existing plan file in place against this corrected
   scope. Then strict-TDD execution of the new units; the previous PR
   commits get rewound or amended to match.
