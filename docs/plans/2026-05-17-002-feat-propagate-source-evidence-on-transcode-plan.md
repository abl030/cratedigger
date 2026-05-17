---
title: "feat: propagate source-side evidence on transcoded imports"
date: 2026-05-17
deepened: 2026-05-17
type: feat
status: active
origin: docs/brainstorms/2026-05-17-propagate-source-evidence-on-transcode-requirements.md
---

# feat: Propagate Source-Side Evidence on Transcoded Imports

## Summary

Reverse the propagation asymmetry in `propagate_candidate_evidence_to_current`
(`lib/quality_evidence.py:795`). Today, transcoded imports (FLAC → V0 / Opus)
zero the library row's `spectral_grade`, `spectral_bitrate_kbps`, `v0_metric`,
`matched_bad_audio_hash_id`, and `matched_bad_audio_hash_path` while propagating
only `verified_lossless_proof`. After this change, all five fields propagate
symmetrically with the rename-only path. Forward-only; no backfill of existing
transcoded library rows. See origin: `docs/brainstorms/2026-05-17-propagate-source-evidence-on-transcode-requirements.md`.

---

## Problem Frame

Live reproducer: request 3779 (Lil Wayne — *Da Drought 3*), MBID
`244322cc-51ba-4f35-b072-f7c5888fb5ce`. A transcoded-FLAC import landed at
16:06 UTC; a second identical-quality FLAC arrived at 18:32. Wrong-match
cleanup triage (`lib/wrong_match_cleanup_service.py:330`) called
`full_pipeline_decision_from_evidence(import_mode="force")`, found the on-disk
evidence row had NULL `spectral_grade` / `v0_metric` / `matched_bad_audio_hash_id`,
and fell through to `provisional_lossless_upgrade` → `kept_would_import`. The
operator had already imported an identical source 31 minutes earlier; triage
should have classified the new candidate as `confident_reject` and cleanup-
deleted the wrong-match folder.

Root cause: `propagate_candidate_evidence_to_current` strips source-side
evidence on transcoded imports, leaving the library row blind to its own
provenance. The triage decider has comparable evidence on the candidate side
and nothing on the library side, so it cannot reject.

The brainstorm establishes the asymmetry as internally inconsistent —
`verified_lossless_proof` also describes the upstream source, but survives
transcode because lineage matters. The current rule conflates "describes source
audio" with "proof-of-good survives, proof-of-compromised does not." Reversing
makes the policy coherent.

---

## Requirements Trace

From `docs/brainstorms/2026-05-17-propagate-source-evidence-on-transcode-requirements.md`:

| Origin ID | Description | Addressed by |
|-----------|-------------|--------------|
| R1 | Propagate `spectral_grade` / `spectral_bitrate_kbps` on transcoded imports | U5 |
| R2 | Propagate `v0_metric` (single struct holding lineage + bitrate trio) on transcoded imports | U5 |
| R3 | Propagate `matched_bad_audio_hash_id` / `matched_bad_audio_hash_path` on transcoded imports | U5 |
| R4 | Update docstring at lib/quality_evidence.py:825-847 to reflect reversed rule | U5 + U7 |
| R5 | `current_spectral_grade` on `album_requests` inherits new semantic; no special-casing needed | Verified in research; no work |
| R6 | Source-replacement overwrites stale propagated evidence | U4 (regression coverage) |
| R7 | No backfill of existing transcoded library rows | Out-of-scope by decision |
| R8 | Pre-change rows remain NULL — accepted known wart | Documented in U7 |
| AE1 | Same-source duplicate is rejected by triage | U2 (parity) + U3 (round-trip) |
| AE2 | Lossy candidate against transcoded-FLAC row is locked out by `lossless_source_locked` | U2 (parity) + U3 (round-trip) |
| AE3 | Bad-rip hash propagates and matches future candidates | U1 (existing fixture has hash=99 ready to flip) |
| AE4 | Source-replacement overwrites stale evidence | U4 |
| AE5 | Renamed-only path unchanged | Regression guard via existing `test_renamed_only_flac_propagates_full_measurement_payload` |
| Search-planner regression | `compute_effective_override_bitrate` for transcoded library rows with spectral | U6 |

---

## Implementation Units

### U1. Flip TestU10 transcoded-evidence propagation assertions (test-first RED)

**Goal:** Convert the existing assertion that transcoded imports drop spectral
/ V0 / bad-hash into an assertion that they now propagate. Becomes the RED
baseline for U5's production change.

**Requirements:** AE3 (bad-rip hash by virtue of the fixture's existing
`matched_bad_audio_hash_id=99`), AE5 (sibling rename-only test untouched as
regression guard).

**Dependencies:** None.

**Files:**
- `tests/test_integration_slices.py` (modify `TestU10PostImportEvidencePropagation`)

**Approach:**
- Rename `test_transcoded_flac_to_v0_drops_spectral_and_v0_keeps_proof` →
  `test_transcoded_flac_to_v0_propagates_source_evidence`.
- Flip the five `assertIsNone` checks at the bottom of the test
  (`new_evidence.measurement.spectral_grade`,
  `new_evidence.measurement.spectral_bitrate_kbps`, `new_evidence.v0_metric`,
  `new_evidence.matched_bad_audio_hash_id`,
  `new_evidence.matched_bad_audio_hash_path`) to `assertEqual` against the
  candidate's seeded values. Keep `verified_lossless_proof` assertion as-is
  (already propagates).
- Leave `test_renamed_only_flac_propagates_full_measurement_payload`
  untouched — it stays as the AE5 regression guard.

**Execution note:** Write the assertion flip first. Run the test, watch it
fail (RED). U5 turns it green. Do not modify production code in this unit.

**Patterns to follow:** Mirror the existing sibling test's assertion shape —
the `_build_candidate_evidence` helpers in `TestU10PostImportEvidencePropagation`
already produce the right fixture; only the assertions change.

**Test scenarios:**
- Covers AE3. Transcoded FLAC → V0 with candidate
  `spectral_grade="genuine"`, `spectral_bitrate_kbps=850`,
  `v0_metric.source_lineage="lossless_source"`, `v0_metric.avg_bitrate_kbps=850`,
  `matched_bad_audio_hash_id=99`,
  `matched_bad_audio_hash_path="/some/known/bad.flac"`. After propagation,
  the library row's `measurement.spectral_grade`, `measurement.spectral_bitrate_kbps`,
  `v0_metric` (full struct equality), `matched_bad_audio_hash_id`,
  `matched_bad_audio_hash_path` must equal the candidate's values.
- Regression: `verified_lossless_proof` still propagates (no change).
- Regression: `measurement.min_bitrate_kbps` / `avg_bitrate_kbps` / `format`
  still describe the *library copy* (V0 output), not the candidate (FLAC
  source) — these are re-derived from `album_info`, not propagated.

**Verification:** Running
`nix-shell --run "python3 -m unittest tests.test_integration_slices.TestU10PostImportEvidencePropagation.test_transcoded_flac_to_v0_propagates_source_evidence -v"`
must fail with `AssertionError: None != ...` on each of the five propagated
fields before U5; pass after U5.

---

### U2. Add Lil Wayne live-bug scenario — simulator + evidence-pipeline parity (test-first RED)

**Goal:** Encode the live bug (request 3779, MBID
`244322cc-51ba-4f35-b072-f7c5888fb5ce`) as a permanent regression test.
Establishes the parity contract: the simulator decider and the
evidence-pipeline decider must reach the same outcome on the same album.

**Requirements:** AE1, AE2 (at decision level).

**Dependencies:** None.

**Files:**
- `tests/test_quality_classification.py` (extend `TestLiveBugReproductions`
  and `TestLiveBugReproductionsThroughEvidencePipeline`)

**Approach:**
- Add one test method to `TestLiveBugReproductions` named
  `test_lil_wayne_da_drought_3_transcoded_flac_rejects_duplicate_via_simulator`.
  Build the candidate facts to match the live row: FLAC container, spectral
  `likely_transcode`, spectral_bitrate `128`, v0 probe `lossless_source` avg
  `215` min `184`. Existing library facts match the transcoded-FLAC import:
  format `Opus`, codec `opus`, min `100`, avg `119`, plus the now-propagated
  source-side fields (`spectral_grade=likely_transcode`,
  `spectral_bitrate=128`, `v0_source_lineage=lossless_source`, `v0_avg=215`).
  Call `full_pipeline_decision(...)` with `import_mode="force"`. Expected
  outcome: reject-class decision (e.g. `suspect_lossless_downgrade` or
  `lossless_source_not_better`); NOT `provisional_lossless_upgrade`.
- Add the parity sibling to `TestLiveBugReproductionsThroughEvidencePipeline`
  named `test_lil_wayne_da_drought_3_transcoded_flac_rejects_duplicate_via_evidence`.
  Use `_build_candidate(...)` and `_build_current(...)` to construct
  `AlbumQualityEvidence` rows matching the same facts; call
  `full_pipeline_decision_from_evidence(candidate_evidence, current_evidence,
  facts=AlbumQualityEvidenceDecisionFacts(import_mode="force"), cfg=...)`.
  Assert the same decision and that `classify_full_pipeline_decision(decision)`
  returns `verdict="confident_reject"`.
- Both tests must reference the live row in the docstring with MBID, request
  ID, and date for future archaeology.

**Execution note:** The simulator test may already pass today — `full_pipeline_decision`
takes flat kwargs and will decide correctly when given the propagated
evidence. The evidence-pipeline test is the RED test: today, no plausible
`_build_current(...)` would carry the source-side fields (because propagation
never wrote them), so the test will encode the *intended* state and fail
until U5 makes the production path produce that state. Document this asymmetry
in the test docstring.

**Patterns to follow:** Mirror `test_mountain_goats_bride_provisional_via_evidence`
(parity sibling pattern) and `test_live_mountain_goats_flux_flac_source_vs_lossy_no_spectral`
(simulator scenario shape). Use the existing `_build_candidate` / `_build_current`
helpers; do not hand-roll `AlbumQualityEvidence` rows.

**Test scenarios:**
- Covers AE1. Simulator: transcoded-FLAC library row (propagated source
  evidence) + identical-source FLAC candidate → reject-class decision,
  `confident_reject` verdict.
- Covers AE1. Evidence pipeline parity: same inputs through
  `full_pipeline_decision_from_evidence(import_mode="force")` →
  same decision, `confident_reject` verdict.
- Covers AE2. Same library row but lossy MP3 V0 candidate (in the same
  parity-test or a sibling) → `lossless_source_locked` decision,
  `confident_reject` verdict, `cleanup_eligible=True`.

**Verification:** The simulator test passes immediately (decider already
handles the inputs correctly); the evidence-pipeline test fails RED
before U5 and passes after U5. The parity assertion (both classes return
the same outcome) catches future drift.

---

### U3. Integration slice: propagate → wrong-match triage round-trip (test-first RED)

**Goal:** Prove AE1 end-to-end at the orchestration level — propagate
transcoded-FLAC evidence on import, then exercise the real
`cleanup_wrong_match` call path against an identical-source wrong-match
candidate, and assert the triage outcome flips from `kept_would_import` to
`deleted` (`confident_reject` + cleanup eligible).

**Requirements:** AE1 (orchestration level).

**Dependencies:** None.

**Files:**
- `tests/test_integration_slices.py` (new test class
  `TestWrongMatchTriageRejectsSameSourceDuplicate` adjacent to
  `TestWrongMatchCleanupFKChainAvoidsRemeasurement`)

**Approach:**
- Reuse the `_seed(source)`, `_evidence_for(source_dir, mb_release_id)`, and
  `_patch_cfg()` builders from
  `TestWrongMatchCleanupFKChainAvoidsRemeasurement` (or extract them into
  module-level helpers if duplication starts hurting).
- Stage two source dirs on disk: a `transcoded_origin` dir representing the
  first FLAC import, and a `duplicate_source` dir representing the identical
  second arrival. Build matching `AlbumQualityEvidence` rows for both with
  `spectral_grade="likely_transcode"`, `spectral_bitrate_kbps=128`,
  `v0_metric` with `source_lineage="lossless_source"` and `avg=215`.
- Simulate the first import by calling
  `_refresh_current_evidence_after_import(...)` with the transcoded-origin
  candidate evidence and an `album_info` for a V0 library copy. Verify the
  resulting library evidence row carries the propagated source fields
  (sanity check; the real assertion is downstream).
- Seed a `download_log` row marking the duplicate-source folder as
  `rejected` (mirroring the live row 16682).
- Call `cleanup_wrong_match(db, download_log_id)` with the seeded log id.
- Assert outcome: `WrongMatchCleanupOutcome.outcome == OUTCOME_DELETED`,
  `verdict == "confident_reject"`, `cleanup_eligible == True`,
  `preview_decision` is a reject decision name (e.g. `suspect_lossless_downgrade`
  or `lossless_source_locked`).

**Execution note:** Test will be RED before U5 — without propagation, the
library row's source-side fields stay NULL, triage returns
`OUTCOME_KEPT_WOULD_IMPORT`, and the assertion against `OUTCOME_DELETED`
fails. Document this in the test docstring.

**Patterns to follow:**
- `TestWrongMatchCleanupFKChainAvoidsRemeasurement` for the
  seed/evidence/cfg-patch pattern.
- `TestU10PostImportEvidencePropagation` for staging audio dirs and calling
  `_refresh_current_evidence_after_import`.
- `FakePipelineDB` from `tests/fakes.py` for the stateful DB.

**Test scenarios:**
- Covers AE1. Two-source scenario: propagate evidence for the first
  transcoded import, then call cleanup triage on the duplicate-source
  wrong-match → `OUTCOME_DELETED`, `confident_reject`, `cleanup_eligible`.
- Negative regression: same setup but skip the propagation step (or use a
  bare candidate evidence with NULL source-side fields) → still
  `OUTCOME_KEPT_WOULD_IMPORT`. Demonstrates the propagation IS the load-
  bearing input, not some other gate.

**Verification:** Test fails RED before U5 with
`AssertionError: 'kept_would_import' != 'deleted'`; passes after U5.

---

### U4. Source-replacement overwrite slice (regression coverage)

**Goal:** Document and pin behaviour for AE4 — when a clean lossless-source
candidate force-imports over a previously-transcoded library row, the new
candidate's evidence overwrites the stale propagated fields.

**Requirements:** AE4, R6.

**Dependencies:** None.

**Files:**
- `tests/test_integration_slices.py` (new test method in
  `TestU10PostImportEvidencePropagation` or a sibling class)

**Approach:**
- Stage one library dir. Propagate evidence from a first candidate with
  compromised source (`spectral_grade="likely_transcode"`,
  `spectral_bitrate=128`, `v0_metric.source_lineage="lossless_source"`,
  `v0_avg=215`). Verify the library row reflects these values.
- Propagate evidence from a second candidate with clean genuine source
  (`spectral_grade="genuine"`, `spectral_bitrate=900`,
  `v0_metric.source_lineage="lossless_source"`, `v0_avg=900`,
  `verified_lossless_proof` populated) over the same MBID + snapshot
  fingerprint.
- Assert the library row's `measurement.spectral_grade == "genuine"` (not
  `"likely_transcode"`), `measurement.spectral_bitrate_kbps == 900`,
  `v0_metric.source_lineage == "lossless_source"` with `avg == 900`,
  `verified_lossless_proof IS NOT None`. No stale values survive.

**Execution note:** This unit may pass before U5 if the second candidate
also exercises the existing rename-only path (which propagates everything).
The unit explicitly tests the transcoded → transcoded replacement case to
exercise the new policy. If it passes immediately under U5 because
`upsert_album_quality_evidence` uses `ON CONFLICT DO UPDATE` and the upsert
overwrites by `(mb_release_id, snapshot_fingerprint)`, that is the
documented success criterion — the test serves as regression coverage,
not RED→GREEN.

**Patterns to follow:** Reuse `TestU10PostImportEvidencePropagation`'s
audio-staging and `_refresh_current_evidence_after_import` patterns.

**Test scenarios:**
- Covers AE4. Sequential propagation: compromised source first, clean
  source second → library row reflects the second.
- Edge case: the snapshot fingerprint changes between propagations
  (different file set) → both rows coexist as separate
  `album_quality_evidence` rows; the request's `current_evidence_id`
  points at the second. (Optional; only add if `_refresh_current_evidence_after_import`
  exposes this transition cleanly.)

**Verification:** Test passes after U5; documents the upsert semantic.

---

### U5. Reverse propagation policy in lib/quality_evidence.py (production change — GREEN)

**Goal:** Remove the `is_transcode` conditionals that zero the source-side
fields on the library row. Update the function docstring to reflect the
reversed rule and re-state the semantic shift.

**Requirements:** R1, R2, R3, R4.

**Dependencies:** U1, U2, U3 (all must be RED before this lands).

**Files:**
- `lib/quality_evidence.py` (modify `propagate_candidate_evidence_to_current`,
  lines 795-923)

**Approach:**
- **Inner measurement (lines 886-891).** Remove the `None if is_transcode
  else ...` conditionals on `spectral_grade` and `spectral_bitrate_kbps`.
  After the change, both fields always equal `candidate_measurement.spectral_grade`
  / `candidate_measurement.spectral_bitrate_kbps`.
- **Outer row (line 911).** Remove the `None if is_transcode else ...`
  conditional on `v0_metric`. After the change, `v0_metric` always equals
  `candidate_evidence.v0_metric` (a single `AlbumQualityV0Metric` struct
  holding lineage + min/avg/median).
- **Outer row (lines 917-918, 920-921).** Remove the `None if is_transcode
  else ...` conditionals on `matched_bad_audio_hash_id` and
  `matched_bad_audio_hash_path`. After the change, both always equal the
  candidate's values.
- **`is_transcode` flag and its inputs.** Verified at plan time: the
  detection block at lines 858-877 (and the locals `source_codec`,
  `library_codec`, `effective_target`, `effective_target_lc` that feed it)
  is referenced ONLY by the five conditionals being removed. After the
  conditional removals, the entire detection block plus its input locals
  become dead code and must be deleted in the same change. No downstream
  call path consumes `is_transcode` from this function.
- **Docstring (lines 825-847).** Rewrite the "Field policy" bullet that
  starts "Propagated when renamed-only, NULL when transcoded:" to instead
  describe the new unified rule: "Propagated in both renamed-only and
  transcoded cases — these describe the upstream source audio at import
  time, not the on-disk file. For transcoded imports, the on-disk file
  has a different spectrum and codec, but the propagated fields remain
  accurate descriptions of the source that produced it." Update the
  surrounding paragraphs to remove the now-obsolete reasoning.

**Execution note:** This is the GREEN unit. Once it lands, U1's flipped
assertions, U2's evidence-pipeline parity test, U3's triage round-trip,
and U4's source-replacement test all turn green. The simulator side of U2
was already green.

**Patterns to follow:** The rename-only path. After the change, transcoded
and rename-only branches differ only in whether `measurement.format` /
`measurement.min_bitrate_kbps` etc. come from `album_info` (always re-derived
from the library snapshot, for both cases) — the source-side fields no
longer branch on `is_transcode`.

**Test scenarios:** None added in this unit (production change). All test
coverage lives in U1-U4. Verification is "the previously-RED tests turn
GREEN."

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_integration_slices.TestU10PostImportEvidencePropagation -v"`
  passes (both U1's flipped test and U4's source-replacement test).
- `nix-shell --run "python3 -m unittest tests.test_quality_classification.TestLiveBugReproductionsThroughEvidencePipeline -v"`
  passes (including U2's parity test).
- New `TestWrongMatchTriageRejectsSameSourceDuplicate` from U3 passes.
- `nix-shell --run "bash scripts/run_tests.sh"` full suite passes — no
  regressions in adjacent decider tests.
- Pyright on `lib/quality_evidence.py` is clean.

---

### U6. Extend `TestComputeEffectiveOverrideBitrate.CASES` for transcoded library rows with spectral (search-planner coverage)

**Goal:** Pin the search-planner's override-min-bitrate behaviour for the
new world where transcoded library rows can carry `spectral_grade` /
`spectral_bitrate_kbps`. Documents the semantic shift noted in the
brainstorm's "Known Consequence: Temporary Wasted-Search Window" section.

**Requirements:** Search-planner regression (brainstorm Test Obligations
section, last bullet).

**Dependencies:** None (pure function test; independent of U5).

**Files:**
- `tests/test_quality_decisions.py` (extend `TestComputeEffectiveOverrideBitrate.CASES`)

**Approach:**
- Add subTest rows to the existing `CASES` table covering both transcoded-
  output container bitrates:
  - Opus V2 transcoded library row: `container_bitrate=100`,
    `spectral_bitrate=128`, `spectral_grade="likely_transcode"`,
    expected `100` (min wins; unchanged behaviour).
  - MP3 V0 transcoded library row: `container_bitrate=225`,
    `spectral_bitrate=128`, `spectral_grade="likely_transcode"`,
    expected `128` (spectral wins; semantic shift visible).
- The descriptions on the new rows should reference "transcoded library
  row" so the intent is obvious from the subTest label.

**Patterns to follow:** `TestComputeEffectiveOverrideBitrate.CASES` rows
already cover the (container, spectral, grade) decision matrix exhaustively;
add to the table, do not invent a new pattern.

**Test scenarios:**
- New: Opus V2 transcoded row with spectral=128/likely_transcode → 100
  (container min wins).
- New: MP3 V0 transcoded row with spectral=128/likely_transcode → 128
  (spectral min wins, demonstrating the semantic shift).
- Existing rows unchanged.

**Verification:**
`nix-shell --run "python3 -m unittest tests.test_quality_decisions.TestComputeEffectiveOverrideBitrate -v"`
passes; new rows visible in subTest output.

---

### U7. Update CLAUDE.md and acknowledge known wart

**Goal:** Update the living docs to reflect the reversed propagation policy
and document the accepted asymmetry between pre-change and post-change
transcoded library rows.

**Requirements:** R4 (extends docstring updates from U5 to project-level docs),
R8 (documents the accepted known wart).

**Dependencies:** U5 (docs follow the code change in the same PR but trail
it in the unit list for readability).

**Files:**
- `CLAUDE.md` (lines 280-285, "Evidence survives the candidate → library
  transition" paragraph in § "Decision architecture")

**Approach:**
- Rewrite the paragraph at CLAUDE.md:278-285 to describe the new rule:
  > After a successful import, `propagate_candidate_evidence_to_current` (U10)
  > inherits the candidate's full measurement payload (spectral grade, V0
  > lineage, bad-audio-hash matches, verified-lossless proof) onto the
  > library evidence row, for **both** renamed-only and transcoded imports.
  > These fields describe the upstream source audio at import time, not the
  > on-disk file. For transcoded imports the on-disk file has a different
  > spectrum and codec, but the propagated fields remain accurate
  > descriptions of the source that produced it.
- Add a one-sentence "Known wart" follow-up paragraph:
  > Library rows imported before this policy change have NULL
  > spectral/V0/bad-hash fields. They retain the old behaviour (wrong-match
  > triage cannot reject same-source duplicates against them) until they
  > are re-imported or force-imported. Forward-only by design; no backfill.
- No update needed in `.claude/rules/code-quality.md` (its "Quality
  decisions live in ONE place" section covers decision purity, not
  propagation policy — confirmed via grep during research).
- No update needed in `docs/pipeline-db-schema.md` (no mentions of the
  propagation policy — confirmed via grep).
- The lib/quality_evidence.py docstring update is owned by U5; this unit
  is project-level docs only.

**Patterns to follow:** Match the surrounding tone of CLAUDE.md's
"Decision architecture" section — concise, opinionated, with a clear
"never re-create decisions elsewhere" frame.

**Test scenarios:** Test expectation: none — docs-only change.

**Verification:**
- `grep -n "inherit only\|stay NULL\|NULL when transcoded" CLAUDE.md` returns
  no results after the update.
- `grep -n "Propagated in both" CLAUDE.md` shows the new wording.
- Manual read of the updated paragraph to confirm tone matches the section.

---

## Sequencing

```
U1  (RED: flip TestU10 transcoded assertions)
U2  (RED: Lil Wayne live-bug, simulator + evidence-pipeline parity)
U3  (RED: propagate → triage round-trip slice)
U4  (regression coverage: source-replacement overwrite)
U6  (search-planner coverage: compute_effective_override_bitrate CASES)
                    │
                    ▼
U5  (GREEN: production change — lib/quality_evidence.py + docstring)
                    │
                    ▼
U7  (docs: CLAUDE.md policy paragraph)
```

U1, U2, U3, U4, U6 are independent and can be written in any order or in
parallel. U5 must come after the RED test units to honour the strict-TDD
posture (RED visible before GREEN). U7 follows U5 in the unit list for
review readability but could technically land in the same commit; the
implementer's call.

---

## Key Technical Decisions

- **Symmetric over scoped.** Propagate source-side fields for all transcoded
  imports, not just lossless-source candidates. The asymmetry being fixed is
  rename-only vs transcoded, not lossless vs lossy — the symmetric fix is the
  coherent one. See origin: brainstorm § Key Decisions #1.
- **Forward-only over backfill.** Existing transcoded library rows are not
  retroactively populated. The bug is operator-visible only on new
  wrong-matches; organic re-touch closes the asymmetry over time;
  `migrations/021_evidence_canonical_rekey.sql` provides a clean atomic-
  backfill precedent if operator pain warrants revisiting. See origin:
  brainstorm § Key Decisions #2.
- **One production change unit, one docs unit.** Splitting the policy code
  change from the project-level docs change makes the policy shift
  auditable in a single commit and the docs propagation reviewable in a
  separate one. The function docstring belongs with the code (in U5)
  because it describes function-local behaviour; CLAUDE.md belongs with
  the docs unit (U7) because it describes project-level architecture.
- **Test-first per unit.** U1, U2, U3 are RED tests written before U5
  flips them green. U4 is regression coverage that may pass immediately
  after U5 via `ON CONFLICT DO UPDATE`. U6 is pure-function coverage
  independent of propagation. The plan structures tests as separate units
  so each commit makes its intent visible (RED-then-GREEN over fewer-
  larger-commits per user TDD preference).
- **Reuse existing builders.** All test units lean on existing infrastructure
  — `TestU10PostImportEvidencePropagation`'s audio-staging helpers,
  `TestLiveBugReproductionsThroughEvidencePipeline._build_candidate` /
  `_build_current`, `TestWrongMatchCleanupFKChainAvoidsRemeasurement`'s
  `_seed` / `_evidence_for` / `_patch_cfg`, `FakePipelineDB` from
  `tests/fakes.py`, `make_album_quality_evidence` from `tests/helpers.py`.
  No new builders; no hand-rolled msgspec.Struct construction.

---

## Test Strategy

- **Pure tests** for `compute_effective_override_bitrate` (U6) — subTest
  table extension.
- **Integration slices** for propagation (U1 — flip existing; U4 —
  source-replacement) and for the end-to-end triage round-trip (U3).
- **Parity tests** for the live bug (U2) — simulator and evidence pipeline
  must agree.
- **Regression guards** are explicit: `test_renamed_only_flac_propagates_full_measurement_payload`
  (untouched, guards AE5); existing `TestComputeEffectiveOverrideBitrate.CASES`
  rows (untouched, guards rename-only override behaviour); full test suite
  must pass after U5.

Per `.claude/rules/code-quality.md`, no new bespoke harnesses; all test
infrastructure already exists.

---

## System-Wide Impact

- **Decision layer (`lib/quality.py`).** No changes. Existing deciders
  (`provisional_lossless_decision`, `spectral_import_decision`,
  `full_pipeline_decision_from_evidence`) already handle the propagated
  evidence shapes correctly — they were written assuming evidence might
  be present on both sides; the current bug is that the library side was
  artificially empty.
- **Importer worker (`lib/import_dispatch.py`).** No changes. The importer
  reads persisted evidence and decides via the unchanged pipeline.
- **Preview worker (`lib/import_preview.py`).** No changes. Preview produces
  candidate evidence; the propagation policy reversal only affects how
  candidate evidence flows to the library row after import.
- **Search planner (`lib/download.py:1575-1610`).** Behavioural shift for
  newly-imported transcoded albums: `compute_effective_override_bitrate`
  will start returning the source spectral cliff for MP3 V0 transcoded
  library rows (e.g. drops from container ~225 to spectral 128). For
  Opus V2 transcoded rows, behaviour is unchanged (`min(100, 128) = 100`).
  Net effect: searches become slightly more permissive for new transcoded
  albums; resulting lossy candidates get locked out at triage by the new
  `lossless_source_locked` firing. Wasted slskd churn; no different files
  on disk. Closes when the bucket model lands and search tiers narrow
  based on existing-bucket (see `docs/brainstorms/quality-bucket-system-requirements.md`).
- **Wrong-match cleanup triage (`lib/wrong_match_cleanup_service.py`).**
  Intended behavioural shift: lossy candidates against transcoded-FLAC
  library rows now classify as `confident_reject` (via
  `lossless_source_locked`) and become cleanup-eligible. Same-source FLAC
  duplicates against transcoded-FLAC library rows likewise classify as
  reject (via `suspect_lossless_downgrade` or equivalent). Result: cleaner
  wrong-matches queue for new transcoded imports.
- **Web UI.** No changes. The Wrong Matches view consumes triage audit
  rows via `download_log.validation_result`; new outcomes (e.g.
  `lossless_source_locked`) already render via
  `_wrong_match_action_label` (`web/classify.py:273-290`) without
  additional plumbing.
- **Pipeline DB schema.** No changes. Forward-only; no migrations.
- **Operator workflows.** No CLI changes; no API changes; no config
  changes. The CLI ⇄ API surface symmetry rule (§ CLAUDE.md) does not
  apply — this is an internal policy reversal, not a new operator action.

---

## Scope Boundaries

In scope:
- Five `is_transcode` conditional removals in
  `propagate_candidate_evidence_to_current`.
- Docstring updates at lib/quality_evidence.py:825-847 and CLAUDE.md:278-285.
- Test additions per U1-U4, U6.
- No-op verification of `verified_lossless_proof` propagation (already
  works; explicitly tested by existing assertions in U1's flipped test).

Out of scope:
- No backfill of existing transcoded library rows (Key Decision #2).
- No changes to `full_pipeline_decision_from_evidence` or any pure decider
  in `lib/quality.py`.
- No changes to `import_service.py` or how `current_spectral_grade` flows
  from library evidence back to `album_requests` (existing path inherits
  the new semantic naturally; no special-casing).
- No UI / copy / triage-display changes.
- No `.claude/rules/code-quality.md` updates (decision purity rules
  unchanged).

### Deferred to Follow-Up Work

- **Search-plan tier narrowing on `lossless_source` lineage.** Once a
  library row carries `v0_source_lineage="lossless_source"`, the search
  plan could narrow to lossless tiers only, since any lossy candidate
  will be locked out downstream. This is the natural shape of the bucket
  work (see `docs/brainstorms/quality-bucket-system-requirements.md` R1,
  R6) and absorbs the temporary wasted-search window from this change.
  Not bundled into this PR.
- **FK-chain backfill for existing transcoded library rows.** If the
  organic re-touch rate proves too slow and false-positive triage keeps
  remain operationally noisy, walk `album_requests.current_evidence_id` →
  `download_log.candidate_evidence_id` and propagate the prior candidate's
  source-side fields to the library row. Deferred behind operator
  observation; revisit only if pain warrants.

---

## Risks and Mitigations

- **Risk: U2's simulator test passes immediately, masking incomplete RED
  baseline.** Mitigation: U2's docstring explicitly notes the simulator vs
  evidence-pipeline asymmetry. The evidence-pipeline parity test is the
  load-bearing RED; the simulator side documents the expected outcome
  shape.
- **Risk: `is_transcode` flag still referenced after conditional removals.**
  Mitigation: U5 includes an explicit local read of lines 858-877 to
  decide whether to delete the detection or preserve it with a comment.
- **Risk: `lossless_source_locked` firing more aggressively breaks a
  workflow we forgot.** Mitigation: U3's integration slice exercises the
  full triage path against a real wrong-match row, and the full test suite
  must pass after U5. Adjacent decider tests in `tests/test_quality_decisions.py`
  exercise `provisional_lossless_decision` directly and will catch
  unintended changes to the lock branch.
- **Risk: Search-planner override drop (225 → 128) on MP3 V0 transcoded
  rows causes excessive slskd churn.** Mitigation: brainstorm classifies
  this as a known-acceptable temporary window. U6 documents the shift
  with subTest rows. If the churn proves operationally painful, the
  follow-up "search-plan tier narrowing" work in `Deferred to Follow-Up Work`
  closes it.
- **Risk: Pre-change transcoded library rows generate ongoing false-
  positive triage keeps.** Mitigation: documented as R8 known wart in
  brainstorm and U7. If pain warrants, FK-chain backfill in
  `Deferred to Follow-Up Work` resolves it.

---

## Verification Checklist

Before merging:
- [ ] U1 test (`test_transcoded_flac_to_v0_propagates_source_evidence`) fails
  with `AssertionError` on each of the five propagated fields before U5;
  passes after.
- [ ] U2 evidence-pipeline test fails RED before U5; passes after; parity
  with the simulator test holds.
- [ ] U3 round-trip test fails RED before U5 with
  `'kept_would_import' != 'deleted'`; passes after.
- [ ] U4 source-replacement test passes after U5; documents the upsert
  overwrite semantic.
- [ ] U6 subTest rows visible in test output; pass.
- [ ] `nix-shell --run "bash scripts/run_tests.sh"` full suite passes; no
  unrelated regressions.
- [ ] Pyright clean on `lib/quality_evidence.py`,
  `tests/test_integration_slices.py`,
  `tests/test_quality_classification.py`,
  `tests/test_quality_decisions.py`.
- [ ] CLAUDE.md grep confirms old wording removed, new wording present.
- [ ] Pre-commit hook (`scripts/pre-commit`) passes.
