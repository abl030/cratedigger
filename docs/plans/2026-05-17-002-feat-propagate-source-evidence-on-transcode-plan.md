---
title: "feat: propagate lossless-source evidence on transcoded imports + narrow search on lossless-source lock"
date: 2026-05-17
deepened: 2026-05-17
rescoped: 2026-05-17
type: feat
status: active
origin: docs/brainstorms/2026-05-17-propagate-source-evidence-on-transcode-requirements.md
---

# feat: Propagate Lossless-Source Evidence on Transcoded Imports + Narrow Search on Lossless-Source Lock

## Summary

Two coupled changes shipped together:

1. **Narrow the propagation policy in
   `lib/quality_evidence.py::propagate_candidate_evidence_to_current`** to
   carry source-side evidence (spectral, V0, bad-hash) forward onto the
   library row **only when the candidate source is lossless** (FLAC / ALAC /
   WAV). Renamed-only behavior unchanged. Non-lossless transcodes
   (MP3 → Opus etc.) continue to strip source-side fields onto NULL.
2. **Add `narrow_override_on_lossless_source_lock` and wire it into both
   the importer rejection path and the wrong-match cleanup triage path**
   so that whenever `lossless_source_locked` fires, the request's
   `search_filetype_override` is set to `"lossless"`. Future search cycles
   only ask Soulseek for lossless candidates that can actually win against
   the existing lossless-source library row.

Forward-only; no backfill. Fixes live bug request 3779 Lil Wayne — Da
Drought 3 AND closes the wasted-search-cycle window the first version of
this plan left open.

This file replaces an earlier broader version of the plan that proposed
symmetric propagation for all transcoded imports and deferred the search
narrowing. See origin brainstorm for the rescope rationale; see git
history for the previous shape's commits.

---

## Rescope Notes (read before editing)

This plan was originally written against a broader scope ("symmetric
propagation for all transcoded imports; defer search narrowing to the
buckets work"). That scope diverged from the user's actual ask
("propagate evidence in lossless scenarios only; narrow search when
lossless_source-locked"). 8 commits already landed on the branch under
the old scope; the unit list below reflects the **corrected target
state**, not the historical sequencing.

The corrective work goes on top of the existing commits as new commits
(no rebase). Tests that pin behavior the rescope changes need to be
adjusted; some tests will be tightened, none deleted wholesale.

---

## Problem Frame

**Gap 1 (propagation):** Today's
`propagate_candidate_evidence_to_current` zeros spectral / V0 / bad-hash
on every transcoded import. For the lossless-source case (FLAC → V0/Opus)
the source-side evidence is meaningful for future candidates and should
survive transcode; this is the gap the live bug exposed.

**Gap 2 (search narrowing):** Even after Gap 1 is fixed, the
`lossless_source_locked` rejection doesn't narrow the request's search
plan. Future cycles re-ask for the same album with no filetype filter,
download the same lossy candidate from a new peer, hit the lock again,
auto-delete, repeat. The lock without narrowing is half a system.

Live reproducer: request 3779 (Lil Wayne — *Da Drought 3*),
MBID `244322cc-51ba-4f35-b072-f7c5888fb5ce`. Transcoded-FLAC import at
16:06 UTC; identical-quality FLAC at 18:32. Today triage classifies
`kept_would_import` (Gap 1). After Gap 1 fix alone, triage would reject
correctly but each future cycle would re-find lossy candidates and
re-lock — Gap 2.

This PR closes both gaps in tandem because they are the same
architectural intent: a library row's lossless-source lineage should both
(a) be recognized on the next candidate, and (b) constrain what future
candidates are even searched for.

---

## Requirements Trace

From `docs/brainstorms/2026-05-17-propagate-source-evidence-on-transcode-requirements.md`:

| Origin ID | Description | Addressed by |
|-----------|-------------|--------------|
| R1 | Propagate spectral/V0/bad-hash when source is lossless | U8 |
| R2 | Strip on non-lossless transcoded imports (unchanged) | U8 + U1 negative test |
| R3 | Renamed-only path unchanged | U8 (unchanged code path) + AE6 regression guard |
| R4 | verified_lossless_proof propagates always (unchanged) | No change; existing tests pin |
| R5 | Remove unused `target_format` param | U8 |
| R6 | New `narrow_override_on_lossless_source_lock` helper | U9 |
| R7 | Wire helper into importer-side rejection | U10 |
| R8 | Wire helper into wrong-match cleanup triage | U11 |
| R9 | No plan invalidation needed | Verified during research |
| R10 | current_spectral_grade inherits narrower rule naturally | No code change |
| R11 | Source-replacement overwrites via ON CONFLICT (existing) | U7 regression coverage |
| R12 | No backfill | Out-of-scope by decision |
| R13 | Pre-change rows remain NULL — accepted known wart | Documented in U12 |
| AE1 | Same-source duplicate rejected by triage AND search narrows | U4 + U6 |
| AE2 | Lossy candidate against lossless-source row → lock fires + narrows | U3 + U4 |
| AE3 | Bad-rip hash propagates on lossless-source transcoded import | U1 |
| AE4 | Source-replacement overwrites stale propagated evidence | U7 |
| AE5 | Non-lossless source transcoded import strips source-side evidence | U1 negative case |
| AE6 | Renamed-only path unchanged | Existing `test_renamed_only_flac_propagates_full_measurement_payload` |
| AE7 | Idempotent narrowing | U2 |

---

## Implementation Units

### U1. Adjust TestU10 transcoded propagation tests for lossless-source gate

**Goal:** The existing flipped test (`test_transcoded_flac_to_v0_propagates_source_evidence`)
still pins the correct lossless-source case. Add a new negative test
that pins the non-lossless-source case (MP3 → Opus): source-side fields
must stay NULL.

**Requirements:** AE3 (bad-hash propagates on lossless-source case; existing
fixture has matched_bad_audio_hash_id=99), AE5 (non-lossless strip).

**Dependencies:** None.

**Files:**
- `tests/test_integration_slices.py` (`TestU10PostImportEvidencePropagation`)

**Approach:**
- Keep the renamed `test_transcoded_flac_to_v0_propagates_source_evidence`
  as-is — it exercises the FLAC source → V0 library case which is
  lossless-source-gated.
- Add new test `test_transcoded_mp3_to_opus_strips_source_evidence` (or
  similar). Mirrors the existing transcoded test fixture but with
  `candidate_evidence.codec="mp3"`, `container="mp3"`, `storage_format="mp3 v0"`.
  Library copy is Opus. Asserts the resulting library row's
  `measurement.spectral_grade`, `measurement.spectral_bitrate_kbps`,
  `v0_metric`, `matched_bad_audio_hash_id`, and
  `matched_bad_audio_hash_path` are ALL NULL.

**Execution note:** Write the test first — it should fail RED before U8
lands (current code propagates the fields symmetrically), pass after U8
re-gates on source_is_lossless.

**Patterns to follow:** Existing
`test_transcoded_flac_to_v0_propagates_source_evidence` is the template;
only the candidate codec and matching assertions change.

**Test scenarios:**
- New: MP3 source transcoded to Opus → all source-side fields NULL on
  library row. Verifies AE5.
- Regression: existing FLAC → Opus propagation test still passes after U8.
- Regression: renamed-only test still passes after U8.

**Verification:** New test fails RED before U8; passes after. Existing
tests unaffected.

---

### U2. Pure tests for `narrow_override_on_lossless_source_lock`

**Goal:** Pin the helper's behavior at every input case.

**Requirements:** R6, AE7.

**Dependencies:** None.

**Files:**
- `tests/test_quality_decisions.py` (new test class
  `TestNarrowOverrideOnLosslessSourceLock`)

**Approach:**
- subTest table with rows:
  - `(None, "lossless")` — no override → narrow to lossless
  - `("mp3 v0", "lossless")` — lossy override → narrow to lossless
  - `("mp3 320", "lossless")` — lossy override → narrow to lossless
  - `("lossless,mp3 v0,mp3 320", "lossless")` — full ladder → narrow to lossless
  - `("lossless", None)` — already narrowest → no-op (returns None)
- Pattern: match `TestComputeEffectiveOverrideBitrate`'s subTest CASES
  table shape.

**Test scenarios:**
- Each row above; assertion shape:
  `assertEqual(narrow_override_on_lossless_source_lock(current), expected)`.

**Verification:** All subTest cases pass after U9 lands.

---

### U3. Importer-side rejection narrows override (orchestration test)

**Goal:** When `dispatch_import_from_db` (or its dispatch core) decides
`lossless_source_locked`, the request's `search_filetype_override` ends
up as `"lossless"` after the rejection is recorded.

**Requirements:** R7, AE2.

**Dependencies:** None.

**Files:**
- `tests/test_dispatch_core.py` (or `tests/test_integration_slices.py` if
  the existing dispatch-core scaffolding is too tied to other flows)

**Approach:**
- Use `FakePipelineDB` with a seeded request whose
  `search_filetype_override` is initially `"mp3 v0,mp3 320"` (or None).
- Drive the dispatch path with a lossy candidate (`dl_info`) that
  produces an `ImportResult` with `decision="lossless_source_locked"`.
  Mock the `import_one.py` subprocess to return the canned result, or
  use the existing dispatch-core fakes.
- Assert post-call: `db.request(request_id)["search_filetype_override"]`
  equals `"lossless"`.

**Execution note:** Test will be RED before U10 lands.

**Patterns to follow:** Existing downgrade-narrowing orchestration tests
in test_dispatch_core.py (around the `downgrade` branch's
`narrow_override_on_downgrade` call); mirror their shape.

**Test scenarios:**
- Happy path: lossy candidate + lossless-source library evidence
  → `lossless_source_locked` → override becomes `"lossless"`.
- Idempotent: if override is already `"lossless"`, stays
  `"lossless"` (no spurious DB write).

**Verification:** Test fails RED before U10; passes after.

---

### U4. Lil Wayne parity tests (simulator + evidence pipeline)

**Goal:** Pin the Lil Wayne (request 3779) FLAC-source same-source
duplicate scenario as a regression test in both
`TestLiveBugReproductions` and
`TestLiveBugReproductionsThroughEvidencePipeline`. Encode the parity
contract: simulator and evidence pipeline must reach the same decision.

**Requirements:** AE1 at the decision level.

**Dependencies:** None.

**Files:**
- `tests/test_quality_classification.py`

**Approach:**
- Existing tests from the prior plan version (already in branch history
  on commit be30928) cover this. Inspect and verify they still match the
  corrected scope — both candidate and existing-library inputs are FLAC,
  which is the lossless-source case.
- Strengthen the parity assertion: explicitly assert
  `simulator_result["stage2_import"] ==
  evidence_result["stage2_import"]`, not just hardcoded equality to
  `"suspect_lossless_downgrade"` on each side independently. Pin the
  parity contract, not the literal decision name.

**Test scenarios:**
- Same as the existing tests; assertion strengthening only.

**Verification:** Both tests pass; parity assertion fails if a future
change makes the two deciders diverge.

---

### U5. Wrong-match triage → search narrowing integration slice

**Goal:** Extend the existing
`TestWrongMatchTriageRejectsSameSourceDuplicate` (added in commit
770ccd8) so that after triage deletes the wrong-match folder, the
request's `search_filetype_override` is `"lossless"`.

**Requirements:** R8, AE1, AE2.

**Dependencies:** None.

**Files:**
- `tests/test_integration_slices.py`
  (`TestWrongMatchTriageRejectsSameSourceDuplicate`)

**Approach:**
- Add an assertion after the existing OUTCOME_DELETED assertion:
  `self.assertEqual(db.request(request_id)["search_filetype_override"],
  "lossless")`.
- Also extend the negative regression sibling test to assert the
  override is unchanged (None or whatever it was seeded with) in the
  no-propagation path.

**Execution note:** New assertion fails RED before U11 lands.

**Test scenarios:**
- Happy path: propagation + triage deletion → override is `"lossless"`.
- Negative: no propagation → override unchanged (regression).

**Verification:** Test fails RED before U11; passes after.

---

### U6. AE2 lossy-candidate triage round-trip (NEW — addresses prior P0)

**Goal:** Add a slice that exercises the lossy MP3 V0 candidate vs
lossless-source library row case end-to-end through triage. This is the
test the previous reviewer flagged as missing (P0 in the prior code
review).

**Requirements:** AE2 explicitly.

**Dependencies:** None.

**Files:**
- `tests/test_integration_slices.py` (extend
  `TestWrongMatchTriageRejectsSameSourceDuplicate` or sibling class)

**Approach:**
- Seed the same lossless-source library row from U5/U6 of the existing
  test.
- Seed a wrong-match download_log with a lossy MP3 V0 candidate
  (`codec="mp3"`, no v0_metric).
- Call `cleanup_wrong_match`.
- Assert: outcome is `OUTCOME_DELETED`, `preview_decision ==
  "lossless_source_locked"`, `verdict == "confident_reject"`, and the
  request's `search_filetype_override` is `"lossless"`.

**Test scenarios:**
- Lossy MP3 V0 candidate vs lossless-source library row →
  `lossless_source_locked` → DELETED + override narrowed.

**Verification:** Test passes after U8 + U11 land (depends on both
propagation gate behaving correctly so the library row has the V0
probe, AND the cleanup wiring narrowing).

---

### U7. Source-replacement overwrite slice (regression coverage)

**Goal:** Pin the AE4 behavior — when a clean lossless-source candidate
force-imports over a previously-transcoded lossless-source library row,
the new candidate's evidence overwrites the stale fields via the
existing `upsert_album_quality_evidence` ON CONFLICT.

**Requirements:** AE4, R11.

**Dependencies:** None.

**Files:**
- `tests/test_integration_slices.py` (existing
  `test_source_replacement_overwrites_stale_propagated_evidence`)

**Approach:**
- The test from commit 57639d2 already covers this. Verify it still
  matches the corrected scope (both candidates are FLAC, so within the
  lossless-source gate). No changes expected.

**Verification:** Test passes; no change required.

---

### U8. Production change: re-gate propagation on lossless source

**Goal:** Replace today's "always strip on transcode" policy with
"strip on transcode UNLESS source is lossless." Remove the unused
`target_format` parameter and the dead `is_transcode` detection block
where appropriate.

**Requirements:** R1, R2, R3, R4, R5.

**Dependencies:** U1.

**Files:**
- `lib/quality_evidence.py::propagate_candidate_evidence_to_current`

**Approach:**
- Re-introduce a minimal `is_transcode` detection (source codec vs
  library codec, since `target_format` param is gone). Add a
  `source_is_lossless` check: `(candidate_evidence.codec or "").lower()
  in LOSSLESS_CODECS`.
- Combine: `strip_source_fields = is_transcode and not source_is_lossless`.
- Apply the gate to the 5 fields:
  - `measurement.spectral_grade`
  - `measurement.spectral_bitrate_kbps`
  - `v0_metric`
  - `matched_bad_audio_hash_id`
  - `matched_bad_audio_hash_path`
- Leave `verified_lossless`, `verified_lossless_proof`,
  `was_converted_from` propagating unchanged.
- Update the function docstring to describe the lossless-source gate
  precisely.

**Execution note:** Production GREEN unit. Unblocks U1's negative case
(MP3 → Opus strip) and keeps U1's positive case (FLAC → Opus propagate)
passing.

**Patterns to follow:** Today's stripped-conditional pattern that the
earlier rewrite removed; restore selectively with the
`source_is_lossless` check.

**Test scenarios:** None added in this unit. Verification is U1, U7, and
U2's tests all behaving correctly.

**Verification:**
- U1's new MP3 → Opus negative case passes (strip).
- U1's existing FLAC → Opus positive case passes (propagate).
- Existing renamed-only test passes (unchanged).
- `nix-shell --run "bash scripts/run_tests.sh"` full suite passes.
- Pyright clean on `lib/quality_evidence.py`.

---

### U9. Add `narrow_override_on_lossless_source_lock` helper

**Goal:** Pure helper that returns `"lossless"` unless the override is
already `"lossless"` (in which case returns `None` for idempotent no-op).

**Requirements:** R6, AE7.

**Dependencies:** None.

**Files:**
- `lib/quality.py` (add helper near `narrow_override_on_downgrade`,
  `rejection_backfill_override`)

**Approach:**
- Function shape:
  ```
  def narrow_override_on_lossless_source_lock(current: str | None) -> str | None:
      if current == QUALITY_LOSSLESS:
          return None
      return QUALITY_LOSSLESS
  ```
- Tiny docstring referencing the brainstorm and the call sites that
  use it.

**Patterns to follow:** `narrow_override_on_downgrade` is the precedent
in shape and naming.

**Test scenarios:** Owned by U2.

**Verification:** U2's subTest cases all pass.

---

### U10. Wire helper into importer-side rejection

**Goal:** When `dispatch_import_from_db` (importer worker) processes a
`lossless_source_locked` rejection, narrow the request's
`search_filetype_override` to `"lossless"`.

**Requirements:** R7.

**Dependencies:** U9.

**Files:**
- `lib/import_dispatch.py` (the `lossless_source_locked` rejection
  branch around lines 1846-1858, plus a narrowing block analogous to
  the existing downgrade branch at 1906-1939)

**Approach:**
- In the `elif decision == "lossless_source_locked":` branch, compute:
  ```
  current_override = req_row.get("search_filetype_override")
                     if req_row else None
  narrowed_override = narrow_override_on_lossless_source_lock(
      current_override)
  ```
  (where `req_row = db.get_request(request_id)`).
- Ensure `narrowed_override` flows into the existing
  `_record_rejection_and_maybe_requeue(...,
  search_filetype_override=narrowed_override, ...)` call.
- Log the narrowing for parity with the downgrade-branch logging
  ("Narrowed search_filetype_override 'X' -> 'lossless' after
  lossless_source_locked").

**Execution note:** Production unit. Unblocks U3's test going GREEN.

**Patterns to follow:** The `downgrade` branch at lines 1906-1939 is the
exact precedent.

**Test scenarios:** Owned by U3.

**Verification:** U3 passes; full suite passes.

---

### U11. Wire helper into wrong-match cleanup triage

**Goal:** When `cleanup_wrong_match` deletes a folder with
`preview_decision == "lossless_source_locked"`, also narrow the request's
`search_filetype_override` to `"lossless"`.

**Requirements:** R8.

**Dependencies:** U9.

**Files:**
- `lib/wrong_match_cleanup_service.py` (likely
  `_perform_cleanup_deletion` or `_cleanup_wrong_match`)

**Approach:**
- After the successful deletion path (where outcome would be
  `OUTCOME_DELETED`), if `preview_decision == "lossless_source_locked"`:
  - Look up the current override on the request row.
  - Call `narrow_override_on_lossless_source_lock(current_override)`.
  - Persist the narrowed override via the appropriate
    `db.set_request_*` method (or a small wrapper if no exact match
    exists — TBD during implementation; check `lib/pipeline_db.py` for
    the right entry point).
- Audit: include the narrowed override in the cleanup audit payload
  so the Recents tab can show "search narrowed to lossless" if useful.

**Execution note:** Production unit. Unblocks U5 and U6 going GREEN.

**Patterns to follow:** Existing `cleanup_wrong_match` already persists
audit data via `db.record_wrong_match_triage(...)`; the override update
is a small additional step.

**Test scenarios:** Owned by U5 (FLAC same-source case) and U6 (lossy
MP3 case).

**Verification:** U5 and U6 pass; full suite passes.

---

### U12. Update CLAUDE.md and lib/quality_evidence.py docstring

**Goal:** Update the living docs to reflect the corrected (narrower)
propagation policy and the new search-narrowing behavior.

**Requirements:** Companion to U8 and U10-U11.

**Dependencies:** U8, U9, U10, U11.

**Files:**
- `CLAUDE.md` (the "Evidence survives the candidate → library transition"
  paragraph, currently in the post-U7 state from the earlier work)
- `lib/quality_evidence.py` (function docstring)

**Approach:**
- Rewrite the CLAUDE.md paragraph to describe:
  - Propagation rule: full payload for renamed-only; lossless-source-only
    for transcoded; non-lossless transcodes strip.
  - Search-narrowing companion: lossless_source_locked → override
    becomes "lossless" so search stops asking for non-lossless candidates.
  - Known wart paragraph stays (pre-change rows still NULL).
- Update the function docstring to match the gate's actual behavior
  and remove the symmetric-propagation framing introduced earlier.
- Also fix the stale docstring at
  `lib/import_dispatch.py:645-648` (the `_refresh_current_evidence_after_import`
  wrapper) which the project-standards reviewer flagged.
- Fix the stale class docstring at
  `tests/test_integration_slices.py:7367-7376`
  (`TestU10PostImportEvidencePropagation`) which still says "stay NULL".

**Test scenarios:** None — docs-only.

**Verification:** `grep -n "stay NULL\|inherit only" CLAUDE.md
lib/quality_evidence.py lib/import_dispatch.py
tests/test_integration_slices.py` returns no results.

---

## Sequencing

```
U1 (RED: TestU10 negative MP3 case)
U2 (pure: narrow helper subTest cases)
U3 (RED: importer-side narrowing test)
U4 (parity test strengthening)
U5 (RED: triage slice narrowing assertion)
U6 (RED: AE2 lossy candidate slice)
U7 (regression coverage check)
              │
              ▼
U8 (GREEN: propagation gate re-introduction)
U9 (GREEN: narrow helper)
              │
              ▼
U10 (GREEN: importer wiring)
U11 (GREEN: triage wiring)
              │
              ▼
U12 (docs: CLAUDE.md + docstrings)
```

U1-U7 are RED-first / regression-coverage units. U8 and U9 unblock most
of them; U10 unblocks U3; U11 unblocks U5 and U6. U12 lands docs after
behavior is verified.

---

## Key Technical Decisions

- **Lossless-source-gated, not symmetric.** The earlier symmetric scope
  was scope creep; the user's actual intent is narrower. The gate is
  `candidate_evidence.codec in LOSSLESS_CODECS` (= `{"flac", "alac",
  "wav"}`).
- **Search narrowing is in scope, not deferred.** A standalone helper
  (`narrow_override_on_lossless_source_lock`) wired into two sites
  (importer + triage) closes the wasted-cycle window the propagation
  change would otherwise create.
- **No `SEARCH_PLAN_GENERATOR_ID` bump.** Research confirmed
  `generate_search_plan` produces query strategies only; filetype
  filtering happens downstream in `enqueue.py::effective_search_tiers`
  via the request's `search_filetype_override`. Updating that column
  takes effect on the next cycle without plan regeneration.
- **Reuse existing infrastructure.** All test units extend existing
  builders (`FakePipelineDB`, `_build_candidate`/`_build_current`,
  TestU10 audio-staging) and the production code follows the existing
  downgrade-narrowing pattern.
- **Forward-only over backfill.** Existing transcoded rows stay NULL.

---

## Scope Boundaries

In scope:
- Lossless-source gate in `propagate_candidate_evidence_to_current`.
- `narrow_override_on_lossless_source_lock` helper + two call sites.
- Docstring + CLAUDE.md updates.
- Test additions per U1-U7.

Out of scope:
- No backfill of existing transcoded library rows.
- No changes to `full_pipeline_decision_from_evidence` or any pure
  decider.
- No force-import-bypass-the-lock affordance.
- No UI / copy / triage-display changes (beyond the optional audit
  payload extension in U11).
- No new CLI / web API surface.

### Deferred to Follow-Up Work

- **Bucket-aware search-plan tier ordering** (the full buckets work).
  This PR mimics the bucket behavior at the lock site only; the bucket
  rewrite generalizes it.
- **Bad-rip ban evidence cleanup**
  (`clear_on_disk_quality_fields` doesn't null `current_evidence_id`).
  Pre-existing wart sharpened by this PR but separate from the
  propagation/narrowing intent.
- **Force-import override of `lossless_source_locked`.** Operator
  escape hatch from the lock — needs a design decision.

---

## Branch History Strategy

8 commits from the previous (over-scoped) shape are already on the
branch (`feat/propagate-source-evidence-on-transcode`). The corrective
work goes on top as new commits — do not rebase or rewrite history.
Rationale: the evolution is real and the audit trail is part of the
honesty of the PR. The PR description will explain the rescope explicitly.

Each new commit's message references the corrected unit (e.g.,
`test(u1-fix): add negative MP3-source case after rescope`,
`feat(u8-fix): re-gate propagation on lossless source`, etc.) so the
delta from the earlier shape is searchable.

---

## Risks and Mitigations

- **Risk: gating logic accidentally re-strips renamed-only lossless
  cases (FLAC → FLAC).** Mitigation: U1 keeps the existing
  `test_renamed_only_flac_propagates_full_measurement_payload` and the
  existing renamed-only FLAC test as a regression guard.
- **Risk: `search_filetype_override = "lossless"` interacts badly with
  user-driven re-queue paths.** Mitigation: `resolve_user_requeue_override`
  (`lib/quality.py:84`) already preserves stricter overrides on user
  requeue. The narrowing aligns with that contract.
- **Risk: the wrong-match cleanup triage write of
  `search_filetype_override` races with the importer's update on the
  same request.** Mitigation: both update paths are guarded by
  advisory locks per the existing architecture; verify during
  implementation, add a test if a race is observable.

---

## Verification Checklist

Before merging:
- [ ] U1 negative test (MP3 → Opus strips) fails RED before U8; passes after.
- [ ] U1 positive test (FLAC → Opus propagates) passes throughout.
- [ ] U2 subTest cases for narrowing helper all pass.
- [ ] U3 importer-narrowing test fails RED before U10; passes after.
- [ ] U4 parity tests assert simulator/evidence equality (not just
  hardcoded strings on each side).
- [ ] U5 triage slice asserts override narrowing.
- [ ] U6 AE2 lossy-candidate slice passes.
- [ ] U7 source-replacement test still passes.
- [ ] `nix-shell --run "bash scripts/run_tests.sh"` full suite passes;
  no unrelated regressions.
- [ ] Pyright clean on touched files.
- [ ] CLAUDE.md grep confirms old wording removed, new wording present.
- [ ] Stale docstrings at `lib/import_dispatch.py:645-648` and
  `tests/test_integration_slices.py:7367-7376` updated.
- [ ] Pre-commit hook (`scripts/pre-commit`) passes.
