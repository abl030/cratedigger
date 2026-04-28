---
date: 2026-04-28
topic: wrong-matches-spectral-evidence
---

# Wrong Matches Spectral Evidence

## Problem Frame

The Wrong Matches tab can present several candidate folders for the same
release. Beets distance is useful for match confidence, but it is not enough
to judge whether a candidate is worth keeping when the candidates may differ
substantially in audio quality.

The immediate goal is not to change ranking or add another preview workflow.
The operator already expects these staged wrong-match rows to have passed
preview/spectral analysis. The tab should simply surface the stored evidence
needed to eyeball candidates before using destructive actions.

## Requirements

**Candidate evidence**

- R1. Wrong Matches rows must surface stored spectral evidence for the
  candidate when available, including spectral grade and estimated spectral
  bitrate/floor.
- R2. Wrong Matches rows must surface stored V0 probe evidence for
  lossless-source candidates when available, at least the probe average.
- R3. The tab must not add a new per-row preview action or async preview flow
  for this feature; missing candidate evidence is treated as missing stored
  data, not as a trigger to run analysis from the UI.

**Bulk cleanup safety**

- R4. The top-level `Delete Lossless Opus` bulk action must not delete Wrong
  Matches groups whose exact on-disk Opus copy is spectrally suspect or
  likely transcode.
- R5. For `Delete Lossless Opus`, an on-disk spectral grade of `NULL`,
  `genuine`, or `marginal` is safe; `suspect` and `likely_transcode` are not
  safe.
- R6. The `Delete Lossless Opus` safety rule must be enforced by the backend
  endpoint, not only by frontend visibility or button filtering.

## Acceptance Examples

- AE1. **Covers R1, R2.** Given a Wrong Matches release with five candidate
  rows, when the operator expands the release, each row shows the stored
  spectral grade/floor and lossless-source V0 probe average when those values
  exist, so a high-distance but better-quality candidate can be noticed before
  Converge or delete actions run.
- AE2. **Covers R3.** Given a candidate row without stored spectral evidence,
  when the row renders, the UI does not start a new preview job or expose a
  preview button as part of this feature.
- AE3. **Covers R4, R5, R6.** Given an exact on-disk Opus copy with
  `current_spectral_grade = 'likely_transcode'`, when `Delete Lossless Opus`
  runs, that release's Wrong Matches rows are skipped even if the request is
  marked verified lossless.
- AE4. **Covers R5.** Given an exact on-disk Opus copy with
  `current_spectral_grade IS NULL`, when `Delete Lossless Opus` runs, the
  release remains eligible for cleanup.

## Success Criteria

- The operator can compare Wrong Matches candidates by audio-quality evidence
  without querying JSONB or running a separate preview command.
- Bulk cleanup no longer deletes candidate folders merely because an Opus copy
  is on disk when that on-disk copy is itself spectrally suspect.
- Planning can proceed without inventing a new ranking policy, preview worker,
  or candidate-selection heuristic.

## Scope Boundaries

- Do not change beets distance thresholds or green-row Converge logic in this
  feature.
- Do not automatically select the best candidate by spectral quality.
- Do not add a per-row preview action, background preview job, or new preview
  lifecycle to the Wrong Matches tab.
- Do not change spectral analysis thresholds or V0 probe policy.
- Do not change manual force-import semantics.

## Key Decisions

- Surface evidence rather than automate ranking: the operator wants enough
  information to eyeball candidates and decide manually.
- Assume staged Wrong Matches rows have already gone through preview/spectral
  analysis: missing UI evidence should reveal a storage/display gap, not start
  a new workflow.
- Treat null on-disk spectral state as safe for `Delete Lossless Opus`:
  existing staged rows are expected to have been previewed, while suspect and
  likely-transcode states are explicit blockers.

## Dependencies / Assumptions

- Candidate spectral and V0 probe data is available from existing persisted
  download/import audit fields for the rows this feature targets.
- The Wrong Matches group header already carries exact on-disk format,
  verified-lossless state, and current spectral state for the library copy.

## Outstanding Questions

### Resolve Before Planning

- None.

### Deferred to Planning

- [Affects R1, R2][Technical] Identify the exact stored fields that cover
  candidate spectral grade/floor and V0 probe evidence for Wrong Matches rows,
  and decide how to handle older rows where the evidence is absent.
- [Affects R4, R6][Technical] Decide whether the frontend should also hide or
  count skipped `Delete Lossless Opus` groups, in addition to the required
  backend enforcement.

## Next Steps

-> /ce-plan for structured implementation planning.
