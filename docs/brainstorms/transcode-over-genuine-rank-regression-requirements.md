---
date: 2026-04-26
topic: transcode-over-genuine-rank-regression
---

# Transcode Over Genuine Rank Regression

## Problem Frame

Cratedigger's shared-spectral comparison can let a suspect or
`likely_transcode` candidate replace an existing `genuine` album even when the
candidate's actual selected bitrate metric is in a lower quality rank. The Muse
*Origin Of Symmetry* run exposed the failure: a `likely_transcode` MP3 around
196kbps average replaced a `genuine` MP3 around 261kbps average because the
candidate's spectral floor was higher than the existing row's partial spectral
floor.

The desired behavior is narrower than "genuine always wins." Bay of Biscay
showed a sensible import where both the candidate's actual average and spectral
floor improved. The fix should preserve same-rank or better-rank progress while
blocking spectral-only promotion of a lower-rank transcode over a higher-rank
genuine existing album.

---

## Actors

- A1. Import pipeline: compares a measured download against the current beets
  album before mutation.
- A2. Operator: reviews import history and expects replacement decisions to be
  explainable from actual quality rank plus spectral evidence.

---

## Key Flows

- F1. Targeted transcode-vs-genuine comparison
  - **Trigger:** A measured candidate and an existing album both carry
    `spectral_bitrate_kbps`, and the candidate has a transcode-grade spectral
    classification while the existing album has a non-transcode-grade
    classification.
  - **Actors:** A1
  - **Steps:** Classify both measurements by the configured real selected
    bitrate metric before applying the shared-spectral clamp; if the candidate
    is in a lower real quality rank than the existing album, reject as a
    downgrade; otherwise allow the existing comparator behavior to decide.
  - **Outcome:** Spectral evidence can support real progress, but cannot
    launder a lower-rank transcode over a higher-rank genuine file.
  - **Covered by:** R1, R2, R3, R4

- F2. History review after an import attempt
  - **Trigger:** The operator expands download history for a request whose
    decision involved both actual bitrate and spectral evidence.
  - **Actors:** A2
  - **Steps:** Show the downloaded label, spectral grade/floor, and the existing
    actual bitrate without hiding it behind a spectral floor.
  - **Outcome:** The operator can see whether the actual quality rank improved,
    held, or regressed.
  - **Covered by:** R5

---

## Requirements

**Comparison Semantics**

- R1. A candidate with spectral grade `suspect` or `likely_transcode` must not
  be considered better than an existing non-transcode-grade album solely
  because its spectral floor is higher.
- R2. When the candidate is transcode-grade and the existing album is
  non-transcode-grade, the candidate must be rejected as a downgrade if its
  real selected-metric quality rank is lower than the existing album's real
  selected-metric quality rank.
- R3. When that real selected-metric rank does not regress, the existing shared
  spectral comparison may still decide the result so Bay of Biscay style
  same-rank progress remains possible.
- R4. The rule must honor the configured bitrate metric and rank thresholds,
  not hardcode average bitrate or specific kbps cutoffs.

**Operator Explainability**

- R5. Download history must not hide an existing album's actual bitrate when an
  existing spectral floor is also present; both signals should remain visible
  enough to explain the decision.

---

## Acceptance Examples

- AE1. **Covers R1, R2, R4.** Given a `likely_transcode` MP3 candidate with
  `avg_bitrate_kbps=196` and `spectral_bitrate_kbps=160`, and an existing
  `genuine` MP3 album with `avg_bitrate_kbps=261` and
  `spectral_bitrate_kbps=128`, when import quality is compared, the result is
  `downgrade`.
- AE2. **Covers R2, R3, R4.** Given a Bay of Biscay shaped candidate with
  `likely_transcode`, `avg_bitrate_kbps=179`, and `spectral_bitrate_kbps=160`,
  and an existing `genuine` MP3 album with `avg_bitrate_kbps=172` and
  `spectral_bitrate_kbps=128`, when import quality is compared, the candidate
  remains eligible for import under the existing comparator.
- AE3. **Covers R3, R4.** Given two same-grade `genuine` measurements with the
  same spectral floor but a higher real selected metric on the candidate, when
  import quality is compared, the shared-spectral tie-breaker can still allow
  the pipeline to grind upward.
- AE4. **Covers R5.** Given an existing album with `existing_min_bitrate=246`
  and `existing_spectral_bitrate=128`, when history renders the row, the
  operator can see both the actual 246kbps value and the ~128kbps spectral
  floor.

---

## Success Criteria

- Muse-shaped replacements are blocked: a lower-rank transcode-grade candidate
  cannot replace a higher-rank genuine existing album through spectral-only
  promotion.
- Bay of Biscay shaped progress remains allowed when the candidate's real
  selected-metric quality rank does not regress.
- The test suite captures both sides of the policy so future changes cannot
  flatten it into either "genuine always wins" or "spectral always wins."
- Download history gives the operator enough bitrate and spectral context to
  explain the decision without querying JSONB manually.

---

## Scope Boundaries

- Do not remove the shared-spectral bucket; keep it for equal-rank and
  same-grade progress cases such as Eno.
- Do not make `genuine` an absolute trump card over every `likely_transcode`
  candidate.
- Do not retune quality-rank thresholds or change the configured primary
  bitrate metric.
- Do not change spectral analysis itself, per-track rollup thresholds, beets
  matching, or search candidate selection.
- Do not redesign Recents or expanded history beyond exposing the actual
  existing bitrate alongside any spectral floor.

---

## Key Decisions

- Guard by real selected-metric quality rank, not raw kbps: this preserves the
  quality-bucket intent and respects operator configuration.
- Apply the guard only for transcode-grade candidate over non-transcode-grade
  existing album: this targets the Muse failure without invalidating Bay of
  Biscay or same-grade shared-spectral progress.
- Keep spectral as supporting evidence: it may break ties or support progress
  when the real rank does not regress, but it cannot override a real rank
  downgrade across the transcode/non-transcode boundary.
- Improve history explainability in parallel: the backend decision is the
  root fix, but hiding actual existing bitrate made the incident harder to
  inspect.

---

## Dependencies / Assumptions

- `QualityRankConfig.bitrate_metric` remains the source of truth for selecting
  the real metric used to classify actual quality.
- `suspect` and `likely_transcode` remain the transcode-grade set used by
  existing helper constants.
- Non-transcode-grade existing albums include `genuine`, `marginal`, missing
  grade, and unknown future grades unless planning discovers a narrower helper
  already exists.

---

## Outstanding Questions

### Resolve Before Planning

- None.

### Deferred to Planning

- [Affects R1, R2][Technical] Decide whether the guard belongs inside
  `compare_quality()` directly or in a small helper called before applying the
  shared-spectral rank comparison.
- [Affects R5][Technical] Decide whether the existing bitrate display change
  should use new API fields or only adjust the current frontend formatting.

---

## Next Steps

-> /ce-plan for structured implementation planning.
