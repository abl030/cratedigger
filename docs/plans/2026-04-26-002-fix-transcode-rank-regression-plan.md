---
title: "fix: Block lower-rank transcodes over genuine"
type: fix
status: completed
date: 2026-04-26
origin: docs/brainstorms/transcode-over-genuine-rank-regression-requirements.md
---

# fix: Block lower-rank transcodes over genuine

## Overview

Cratedigger should keep the shared-spectral comparison, but it must stop using
a higher spectral floor to promote a lower real-quality transcode over a higher
real-quality genuine existing album. The fix is to add a targeted guard around
the transcode-grade candidate vs non-transcode-grade existing case: if the
candidate's real selected-metric rank is lower than the existing album's real
selected-metric rank, treat it as a downgrade before the shared-spectral clamp
can say otherwise.

The plan also makes expanded download history show both actual existing bitrate
and spectral floor so operators can inspect this class of decision without
querying `download_log.import_result` manually.

---

## Problem Frame

The Muse *Origin Of Symmetry* run imported a `likely_transcode` MP3 around
196kbps average over an existing `genuine` MP3 around 261kbps average because
`compare_quality()` compared spectral floors first: candidate ~160 vs existing
~128. That made the candidate look better even though the actual selected
bitrate rank regressed.

The product decision is intentionally narrow (see origin:
`docs/brainstorms/transcode-over-genuine-rank-regression-requirements.md`).
Bay of Biscay shaped progress should remain valid: a transcode-grade candidate
can remain eligible when the real selected-metric rank does not regress and the
shared-spectral comparison supports progress.

---

## Requirements Trace

- R1. A transcode-grade candidate must not beat a non-transcode-grade existing
  album solely because its spectral floor is higher.
- R2. If a transcode-grade candidate's real selected-metric quality rank is
  lower than the existing non-transcode-grade album's real rank, reject as a
  downgrade.
- R3. If that real rank does not regress, preserve existing shared-spectral
  behavior so Bay of Biscay and similar progress cases remain possible.
- R4. Use configured rank and metric behavior rather than hardcoded average
  bitrate or fixed kbps thresholds.
- R5. Expanded download history must show actual existing bitrate alongside any
  existing spectral floor.

**Origin actors:** A1 (Import pipeline), A2 (Operator)
**Origin flows:** F1 (Targeted transcode-vs-genuine comparison), F2 (History review after an import attempt)
**Origin acceptance examples:** AE1 (Muse-shaped downgrade), AE2 (Bay-shaped import remains eligible), AE3 (same-grade shared-spectral progress), AE4 (history shows actual and spectral existing bitrate)

---

## Scope Boundaries

- Do not remove the shared-spectral bucket.
- Do not make `genuine` an absolute trump card over every `suspect` or
  `likely_transcode` candidate.
- Do not retune `QualityRankConfig` thresholds or change the configured primary
  bitrate metric.
- Do not change spectral analysis, per-track rollup thresholds, beets matching,
  search candidate selection, or import queue behavior.
- Do not redesign Recents or expanded history; only clarify the existing
  expanded history bitrate line.

---

## Context & Research

### Relevant Code and Patterns

- `lib/quality.py::_shared_spectral_bitrates()` currently returns clamped
  bitrate inputs whenever both sides have `spectral_bitrate_kbps`.
- `lib/quality.py::compare_quality()` applies shared-spectral rank comparison
  before raw selected-metric tie-breaks.
- `lib/quality.py::measurement_rank()` and `_selected_bitrate()` already
  centralize configured metric selection and rank classification.
- `lib/quality.py::SPECTRAL_TRANSCODE_GRADES` already names the transcode-grade
  set: `suspect` and `likely_transcode`.
- `lib/quality.py::get_decision_tree()` documents the current shared-spectral
  rule in the Decisions tab; its rule text must change with the comparator.
- `tests/test_quality_decisions.py::TestCompareQualitySharedSpectralBucket`
  currently pins grade-independent shared-spectral behavior.
- `tests/test_integration_slices.py::TestBayOfBiscayUpgradeChain` pins a
  related but still-valid case where actual average also improves.
- `tests/test_simulator_scenarios.py` covers composed full-pipeline decisions
  and is the right place to prove stage 1 spectral upgrade no longer guarantees
  stage 2 import.
- `web/js/history.js::renderDownloadHistoryItem()` currently renders the
  existing spectral floor instead of the actual bitrate when both are present.
- `tests/test_js_history.mjs` already covers expanded download-history rendering.

### Institutional Learnings

- `docs/quality-verification.md` states the core intuition this fix preserves:
  genuine V0 files usually average around 240-260kbps, while transcodes often
  sit lower.
- `docs/brainstorms/quality-bucket-system-requirements.md` frames quality as
  bucket/rank first and tie-break second, which matches the targeted guard.
- No `docs/solutions/` directory exists in this repo, so there were no
  solution-learnings to carry forward.

### External References

- None. This is a repo-local decision-policy fix with established local tests.

---

## Key Technical Decisions

- Put the guard in the comparator path, not the force-import path: all import
  modes consume `import_quality_decision()` and `compare_quality()`, so the
  invariant belongs at the shared decision layer.
- Use real selected-metric ranks from `measurement_rank()` before applying the
  shared-spectral clamp: this honors `QualityRankConfig.bitrate_metric`,
  thresholds, and codec semantics.
- Trigger the guard only when the new grade is in `SPECTRAL_TRANSCODE_GRADES`
  and the existing grade is not: this blocks Muse without invalidating Bay,
  Eno, or transcode-over-transcode progress.
- Treat lower real rank as `worse`, not merely `equivalent`: the downstream
  `import_quality_decision()` already maps `worse` to `downgrade` and
  `transcode_downgrade` as appropriate.
- Update the Decisions tab model text in `get_decision_tree()` with the same
  policy so simulator/explainer output does not lie about the comparator.

---

## Open Questions

### Resolved During Planning

- Should `genuine` always trump `likely_transcode`? No. Bay of Biscay remains a
  valid import shape when the candidate's real selected-metric rank does not
  regress.
- Should the guard compare raw average kbps? No. It should compare configured
  real quality rank so `avg`, `median`, or `min` deployments stay consistent.
- Does this require external research? No. The behavior is defined by local
  pipeline policy and existing rank helpers.

### Deferred to Implementation

- Exact helper naming and placement inside `lib/quality.py`: decide while
  editing, but keep the helper small and pure.
- Exact text for the expanded history line: choose concise wording in
  `web/js/history.js`, but tests should require both actual bitrate and
  spectral floor to appear.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for
> review, not implementation specification. The implementing agent should treat
> it as context, not code to reproduce.*

```text
if new grade is suspect/likely_transcode
and existing grade is not suspect/likely_transcode:
    real_new_rank = measurement_rank(new)
    real_existing_rank = measurement_rank(existing)
    if real_new_rank < real_existing_rank:
        return worse

continue with existing shared-spectral comparison
```

Decision examples:

| Shape | Real rank movement | Spectral floor movement | Expected |
|---|---:|---:|---|
| Muse: `likely_transcode` avg 196 over `genuine` avg 261 | Down | Up | Downgrade |
| Bay: `likely_transcode` avg 179 over `genuine` avg 172 | Not lower | Up | Existing comparator may import |
| Eno: `genuine` avg 290 over `genuine` avg 128 | Up | Same | Existing shared-spectral progress remains |

---

## Implementation Units

- U1. **Add comparator characterization tests**

**Goal:** Pin the desired policy before implementation: Muse-shaped lower-rank
transcode over genuine is a downgrade, while Bay and Eno style progress remain
allowed.

**Requirements:** R1, R2, R3, R4; AE1, AE2, AE3

**Dependencies:** None

**Files:**
- Modify: `tests/test_quality_decisions.py`
- Modify: `tests/test_integration_slices.py`
- Test: `tests/test_quality_decisions.py`
- Test: `tests/test_integration_slices.py`

**Approach:**
- Add a direct `compare_quality()` / `import_quality_decision()` test for the
  Muse shape:
  `new=(MP3, likely_transcode, avg=196, spectral=160)` vs
  `existing=(MP3, genuine, avg=261, spectral=128)` expects `worse` /
  `downgrade`.
- Keep or strengthen the Bay of Biscay assertion so
  `new=(likely_transcode, avg=179, spectral=160)` over
  `existing=(genuine, avg=172, spectral=128)` remains importable.
- Keep the Eno same-grade shared-floor test green so the implementation cannot
  accidentally remove shared-spectral progress.
- Update current test wording that says shared-spectral is fully
  "grade-independent"; the new policy is grade-aware only for the
  transcode-over-non-transcode lower-rank guard.

**Execution note:** Add these tests first and confirm the Muse-shaped assertion
fails before changing production code.

**Patterns to follow:**
- `TestCompareQualitySharedSpectralBucket` table-style cases in
  `tests/test_quality_decisions.py`.
- `TestBayOfBiscayUpgradeChain` direct assertion on `import_quality_decision()`.

**Test scenarios:**
- Happy path: Muse-shaped candidate returns `compare_quality() == "worse"` and
  `import_quality_decision() == "downgrade"`.
- Happy path: Bay-shaped candidate still returns `import_quality_decision() ==
  "import"`.
- Edge case: same-grade Eno-shaped `genuine` over `genuine` still returns
  `"better"` / importable when real selected metric improves.
- Edge case: transcode-grade over transcode-grade is not affected by the new
  guard and continues through shared-spectral comparison.

**Verification:**
- The new tests document both allowed and blocked shapes before implementation.

---

- U2. **Implement the targeted rank-regression guard**

**Goal:** Modify shared quality comparison so a transcode-grade candidate cannot
beat a non-transcode-grade existing album when its real selected-metric quality
rank is lower.

**Requirements:** R1, R2, R3, R4; F1; AE1, AE2, AE3

**Dependencies:** U1

**Files:**
- Modify: `lib/quality.py`
- Test: `tests/test_quality_decisions.py`

**Approach:**
- Add a small pure helper near `compare_quality()` that identifies the guarded
  shape: new grade in `SPECTRAL_TRANSCODE_GRADES`, existing grade outside that
  set, and `measurement_rank(new, cfg) < measurement_rank(existing, cfg)`.
- Call the helper before applying `_shared_spectral_bitrates()` inside
  `compare_quality()`.
- Return `"worse"` for the guarded shape so `import_quality_decision()` maps it
  to the existing downgrade outcomes.
- Leave same-rank, better-rank, same-grade, and transcode-over-transcode paths
  on the current shared-spectral comparison.
- Update `get_decision_tree()` rule text around shared spectral comparison so
  the Decisions tab explains the new guard.

**Technical design:** Directional helper shape:

```text
candidate_is_transcode_grade(new)
existing_is_non_transcode_grade(existing)
real_rank_regresses(new, existing, cfg)
```

Keep the helper about policy, not row provenance. It should operate only on
`AudioQualityMeasurement` and `QualityRankConfig`.

**Patterns to follow:**
- `compute_effective_override_bitrate()` for grade-aware spectral policy.
- `measurement_rank()` and `_selected_bitrate()` for configured metric use.
- `SPECTRAL_TRANSCODE_GRADES` for the transcode-grade set.

**Test scenarios:**
- Happy path: candidate lower real rank is blocked before spectral floor rank
  can promote it.
- Happy path: candidate same or higher real rank continues through the existing
  comparator.
- Edge case: missing existing spectral floor still follows the existing
  non-shared-spectral path.
- Edge case: missing or unknown existing grade is treated as non-transcode for
  the guard only when the candidate is explicitly transcode-grade.

**Verification:**
- `tests/test_quality_decisions.py` passes with the new Muse, Bay, and Eno
  expectations.

---

- U3. **Cover the composed pipeline decision**

**Goal:** Prove the full simulated pipeline blocks the Muse class even when
stage 1 spectral analysis sees the candidate's spectral floor as higher.

**Requirements:** R1, R2, R3, R4; F1; AE1, AE2

**Dependencies:** U2

**Files:**
- Modify: `tests/test_simulator_scenarios.py`
- Test: `tests/test_simulator_scenarios.py`

**Approach:**
- Add Muse-shaped `AlbumState` and `DownloadScenario` fixtures, or inline them
  in a focused test if adding global fixtures would clutter the matrix.
- Assert the composed result does not import when:
  existing is `genuine`, `avg_bitrate=261`, `spectral_bitrate=128`, and the
  candidate is `likely_transcode`, `avg_bitrate=196`, `spectral_bitrate=160`.
- Assert the stage-1 spectral result can still be `import_upgrade` while
  stage-2 import comparison returns `downgrade`; this is the exact layering
  failure the Muse run exposed.
- Add or retain a Bay-shaped composed case if not already covered by the
  integration slice, so the simulator also preserves the allowed path.

**Patterns to follow:**
- Existing simulator fixtures and `simulate()` helper in
  `tests/test_simulator_scenarios.py`.
- Unter Null and Eno scenario tests that document live production incidents.

**Test scenarios:**
- Integration: Muse-shaped composed simulation returns `imported=False`,
  `stage1_spectral="import_upgrade"` or equivalent non-reject stage-1 result,
  and `stage2_import="downgrade"`.
- Integration: Bay-shaped composed simulation remains importable or, if already
  covered at integration-slice level, the simulator documents why the slice is
  the authoritative coverage.
- Edge case: existing transcode-grade album with lower spectral floor can still
  be upgraded by a higher transcode-grade candidate.

**Verification:**
- The full-pipeline simulator captures the interaction between spectral gate
  and import comparison, not only the pure comparator.

---

- U4. **Clarify expanded history bitrate display**

**Goal:** Show actual existing bitrate and existing spectral floor together in
expanded download history so operators can see when a candidate regressed real
quality.

**Requirements:** R5; F2; AE4

**Dependencies:** None

**Files:**
- Modify: `web/js/history.js`
- Test: `tests/test_js_history.mjs`

**Approach:**
- Change the `On disk (before)` display logic to prefer actual
  `existing_min_bitrate` when present, and append the spectral floor when
  `existing_spectral_bitrate` is also present.
- Preserve the current spectral-only fallback when actual existing bitrate is
  missing.
- Keep escaping behavior and row rendering patterns unchanged.

**Patterns to follow:**
- Existing `rows.push(['On disk (before)', ...])` rendering in
  `web/js/history.js`.
- Existing `tests/test_js_history.mjs` DOM-string assertions.

**Test scenarios:**
- Happy path: `existing_min_bitrate=246` and `existing_spectral_bitrate=128`
  render both `246kbps` and `~128kbps`.
- Edge case: only `existing_spectral_bitrate=128` preserves the old
  `~128kbps (spectral)` style.
- Edge case: only `existing_min_bitrate=246` preserves the plain `246kbps`
  style.

**Verification:**
- Expanded history output makes the Muse row visibly lower-quality without
  requiring database inspection.

---

- U5. **Update explanatory tests and docs references**

**Goal:** Remove stale language that says the shared-spectral bucket is fully
grade-independent and ensure the Decisions tab model explains the new policy.

**Requirements:** R1, R2, R3, R4, R5

**Dependencies:** U2, U4

**Files:**
- Modify: `lib/quality.py`
- Modify: `tests/test_quality_decisions.py`
- Modify: `tests/test_simulator_scenarios.py`
- Optional modify: `docs/quality-verification.md`

**Approach:**
- Update comments/docstrings in comparator tests from "grade-independent" to
  the narrower policy: shared spectral is generally grade-tolerant, but
  transcode-grade over non-transcode-grade cannot override lower real rank.
- Update Decisions tab rule text in `get_decision_tree()` if not already done
  in U2.
- Touch `docs/quality-verification.md` only if implementation reveals a
  current statement that would mislead operators about transcode-vs-genuine
  replacement policy.

**Patterns to follow:**
- Existing issue-specific test comments in `tests/test_simulator_scenarios.py`.
- Existing decision-tree consistency tests in `tests/test_quality_decisions.py`.

**Test scenarios:**
- Happy path: decision-tree constants/tests still pass after rule text changes.
- Test expectation: none for optional prose-only docs unless the decision-tree
  contract exposes the text in tests.

**Verification:**
- Test names, comments, and Decisions tab rules describe the intended nuanced
  policy instead of the old grade-independent rule.

---

## System-Wide Impact

- **Interaction graph:** `full_pipeline_decision()` feeds
  `import_quality_decision()`, which calls `compare_quality()`. The guard must
  affect auto, force, manual, preview, and simulator paths because they share
  this decision layer.
- **Error propagation:** No new error path; guarded comparisons return existing
  `"worse"` / `"downgrade"` outcomes.
- **State lifecycle risks:** Rejected force/manual imports should keep current
  source preservation behavior; this plan does not change cleanup or DB state
  transitions.
- **API surface parity:** The comparator behavior must match CLI simulator,
  import preview, import queue execution, and web Decisions tab explanation.
- **Integration coverage:** Unit tests alone are insufficient; the simulator
  must prove stage 1 spectral upgrade can be overridden by stage 2 downgrade.
- **Unchanged invariants:** Shared-spectral same-grade progress, Bay-shaped
  rank-preserving imports, verified-lossless preference, and transcode-over-
  transcode upgrades remain available.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Overcorrecting into "genuine always wins" | Keep Bay-shaped and Eno-shaped tests explicit and green. |
| Accidentally bypassing configured metric policy | Use `measurement_rank()` rather than raw avg/min comparisons. |
| Decisions tab drifts from code | Update `get_decision_tree()` and its tests with the comparator change. |
| UI display change confuses spectral-only rows | Preserve spectral-only and actual-only fallbacks in `tests/test_js_history.mjs`. |

---

## Documentation / Operational Notes

- No migration or rollout sequencing is required.
- After implementation, inspect recent Muse-shaped rows manually only as an
  operational confidence check; the code change should be fully covered by
  deterministic tests.
- If `docs/quality-verification.md` is updated, keep it focused on replacement
  policy rather than retuning spectral analysis.

---

## Sources & References

- **Origin document:** `docs/brainstorms/transcode-over-genuine-rank-regression-requirements.md`
- Related code: `lib/quality.py`
- Related tests: `tests/test_quality_decisions.py`,
  `tests/test_simulator_scenarios.py`, `tests/test_integration_slices.py`,
  `tests/test_js_history.mjs`
- Related UI: `web/js/history.js`
