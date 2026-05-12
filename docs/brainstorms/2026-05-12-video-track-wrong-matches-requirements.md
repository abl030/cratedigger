---
date: 2026-05-12
topic: video-track-wrong-matches
---

# Video Track Wrong Matches Requirements

## Summary

Cratedigger will automatically validate MusicBrainz releases that contain one
or more video tracks by including video tracks during matched validation, so
Wrong Matches gets normal candidate, distance, and mapping data instead of
video-induced `mbid_not_found`. Import remains an explicit operator decision in
the existing Wrong Matches flow.

---

## Problem Frame

Beets normally ignores MusicBrainz recordings marked as video. For releases
where the target MusicBrainz release includes video tracks, that can erase part
or all of the release before matching. A release that exists in MusicBrainz can
therefore surface in Cratedigger as `mbid_not_found`, with zero candidates and
no Beets distance, even though the downloaded files may be a valid audio rip of
that release.

The operator uses Wrong Matches every day and expects validation data to be
filled automatically as part of normal triage. A video-containing release should
not require a manual rerun just to populate the UI; the operator should only
need to make the usual import/delete/retry decision once normal validation data
is available.

Mixed releases matter too. If a MusicBrainz release has both audio and video
tracks, validation must consider both sides of the release rather than treating
video-aware handling as a video-only special case.

---

## Actors

- A1. Operator: Reviews Wrong Matches and decides whether a candidate should be
  imported, deleted, retried, or left for later.
- A2. Validation pipeline: Runs matched Beets validation and persists the result
  used by Wrong Matches and download history.
- A3. Beets import driver: Performs the matched Beets validation or import work
  for a specific target release.
- A4. Wrong Matches UI: Displays validation evidence and gives the operator the
  daily triage surface.
- A5. MusicBrainz metadata source: Provides release media and recording flags,
  including whether recordings are video tracks.

---

## Key Flows

- F1. Video-containing release validation
  - **Trigger:** A downloaded candidate is validated against a target
    MusicBrainz release that contains at least one video track.
  - **Actors:** A2, A3, A5
  - **Steps:** Cratedigger recognizes that the target release contains video
    tracks, runs matched Beets validation with video tracks included, validates
    against the target release, and persists the resulting candidate data,
    distance, mapping, and detail using the ordinary evidence model, plus scoped
    metadata indicating that video tracks were included.
  - **Outcome:** The row has distance-backed validation evidence instead of a
    video-induced `mbid_not_found`.
  - **Covered by:** R1, R2, R3, R4, R5, R9

- F2. Wrong Matches review
  - **Trigger:** A video-aware validation result appears in Wrong Matches.
  - **Actors:** A1, A4
  - **Steps:** The UI renders the normal candidate, distance, and mapping
    evidence, shows that video tracks were included in validation, and leaves
    the final action to the operator through the existing Wrong Matches
    decision flow.
  - **Outcome:** The operator can handle the release in the normal daily triage
    workflow without treating `mbid_not_found` as a dead end.
  - **Covered by:** R6, R7, R8, R9, R10

- F3. Pending video-aware validation
  - **Trigger:** A Wrong Matches row is waiting for automatic video-aware
    validation to populate or refresh its evidence.
  - **Actors:** A1, A2, A4
  - **Steps:** Wrong Matches distinguishes queued/running validation from a final
    validation result, prevents the stale `mbid_not_found` state from looking
    final, and refreshes into either normal candidate evidence or a diagnosable
    failure.
  - **Outcome:** The operator does not have to infer whether a row is stale,
    still processing, or ready for a decision.
  - **Covered by:** R11, R12

- F4. Import after video-aware validation
  - **Trigger:** The operator chooses to import a row that was validated with
    video tracks included.
  - **Actors:** A1, A3
  - **Steps:** The matched import uses the same video-aware release handling as
    validation, then proceeds through the existing guarded Beets import flow.
  - **Outcome:** Validation and import agree about the target release's track
    set, so import does not fail simply because video tracks are filtered during
    the final import attempt.
  - **Covered by:** R14

- F5. Video-aware validation still fails
  - **Trigger:** Video-aware validation runs, but still cannot produce a usable
    target candidate.
  - **Actors:** A2, A4
  - **Steps:** Cratedigger preserves the original failure detail and the
    video-aware retry detail instead of manufacturing a successful match.
  - **Outcome:** The row remains diagnosable as a true mismatch, missing-track
    case, source problem, metadata problem, or other non-video-filter failure.
  - **Covered by:** R15, R16

- F6. Non-video `mbid_not_found`
  - **Trigger:** The target release contains no video tracks and validation
    returns `mbid_not_found`.
  - **Actors:** A2, A4
  - **Steps:** Cratedigger leaves the non-video validation path unchanged and
    preserves the normal failure detail for diagnosis.
  - **Outcome:** This work fixes the video-track-filter case without changing
    unrelated MusicBrainz, metadata, source, or harness failures.
  - **Covered by:** R17

---

## Requirements

**Video-aware validation**

- R1. If a target MusicBrainz release contains one or more video tracks,
  Cratedigger must run matched validation with video tracks included for that
  release.
- R2. Video-aware validation must handle mixed releases by considering both
  audio and video tracks in the target release.
- R3. Video-aware validation must remain matched validation against the target
  release identity; it must not fall back to an as-is import or unverified
  metadata acceptance.
- R4. When video-aware validation finds the target release, the resulting
  validation data must include the normal candidate, distance, mapping,
  extra-track, and missing-track evidence used by Wrong Matches.
- R5. Video-aware validation must be scoped to the relevant validation attempt;
  it must not change the global Beets default for every release.

**Wrong Matches behavior**

- R6. Wrong Matches must receive and render video-aware validation results using
  the same evidence model as ordinary distance-backed wrong matches.
- R7. For a video-containing release that previously failed only because video
  tracks were filtered out, `mbid_not_found` should go away after validation and
  be replaced by normal candidate and distance data.
- R8. Wrong Matches must make it visible that video tracks were included in the
  validation context, without presenting that fact as a failure by itself.
- R9. Automatically populating video-aware validation data must not imply import
  approval; the operator still makes the final import decision through the
  existing Wrong Matches flow.
- R10. Video-aware validation results must be persisted for Wrong Matches/manual
  review even when the resulting distance is below the normal auto-import
  threshold.
- R11. Pending video-aware validation must be distinguishable from a final
  `mbid_not_found` state so the operator can tell when a row is queued, running,
  refreshed, or failed.
- R12. Wrong Matches must define which actions are available while video-aware
  validation is pending so the operator cannot accidentally act on stale
  evidence as though it were final.

**Import and audit consistency**

- R13. Video-aware handling is metadata-side support for validating and importing
  audio files against video-containing MusicBrainz releases; importing or
  managing actual video media files is out of scope.
- R14. A matched import triggered from a video-aware validation result must use
  the same video-track inclusion behavior as validation.
- R15. Download history and validation audit data must preserve enough detail to
  explain that video tracks were included for the attempt.
- R16. If video-aware validation still cannot produce a usable target candidate,
  Cratedigger must preserve a diagnosable failure instead of manufacturing a
  successful match.
- R17. Non-video `mbid_not_found` behavior must remain unchanged by this work.
- R18. For mixed releases that already produce usable target evidence through
  ordinary validation, video-aware handling must not make the row less
  diagnosable or turn a valid target match into a worse operator decision.

---

## Acceptance Examples

- AE1. **Covers R1, R4, R7.** Given a target MusicBrainz DVD release where every
  recording is marked as video, when validation runs, Cratedigger includes video
  tracks and persists a target candidate with distance-backed evidence instead
  of a zero-candidate `mbid_not_found`.
- AE2. **Covers R1, R2.** Given a target MusicBrainz release with both audio and
  video tracks, when validation runs, the resulting mapping and track-count
  evidence are based on the full target release rather than only the audio
  subset or only the video subset.
- AE3. **Covers R6, R8, R9, R10.** Given a video-aware validation result in
  Wrong Matches, when the operator opens the row, the UI shows normal candidate
  and distance evidence, indicates that video tracks were included, and waits
  for the operator's existing import/delete/retry decision even if the distance
  would normally qualify for auto-import.
- AE4. **Covers R11, R12.** Given a Wrong Matches row whose video-aware
  validation is queued or running, when the operator views the row, the UI makes
  that pending state distinct from a final `mbid_not_found` and does not present
  stale evidence as final.
- AE5. **Covers R14.** Given a row that validated successfully only because
  video tracks were included, when the operator imports it, the final matched
  import uses the same video-aware handling and does not fail because the video
  tracks disappear from Beets' target track set.
- AE6. **Covers R15, R16.** Given default validation fails with zero candidates
  and video-aware validation also cannot produce a usable target candidate, when
  validation completes, Cratedigger preserves both failure details so the row is
  diagnosable as something other than the video-track-filter case.
- AE7. **Covers R16.** Given a video-containing target release but a downloaded
  candidate that still does not match the target after video tracks are
  included, when validation completes, Wrong Matches shows the ordinary failure
  evidence, such as high distance or missing/extra tracks, instead of treating
  video-awareness as automatic success.
- AE8. **Covers R17.** Given a target release with no video tracks, when
  validation returns `mbid_not_found`, Cratedigger does not apply the
  video-aware path and leaves the failure classification unchanged.
- AE9. **Covers R18.** Given a mixed audio/video target release that already
  produces usable target evidence through ordinary validation, when video-aware
  handling applies, the row remains at least as diagnosable and does not lose
  candidate, distance, or mapping evidence the operator would otherwise have
  used.

---

## Success Criteria

- A release like Placebo's `We Come in Pieces`, whose target MusicBrainz release
  is video-track-backed, no longer appears in Wrong Matches as a zero-candidate
  dead end after normal validation.
- Wrong Matches fills out the same candidate, distance, and mapping evidence for
  video-containing releases that it already fills out for ordinary audio
  releases.
- The operator can make the final import decision from the familiar Wrong
  Matches workflow without a separate manual rerun just to populate validation
  data.
- Non-video releases and unrelated `mbid_not_found` causes keep their current
  behavior.
- The audit trail makes video-track inclusion visible enough that future
  debugging can distinguish this path from ordinary Beets validation.

---

## Scope Boundaries

- Do not globally set Beets to include video tracks for every release.
- Do not automatically import video-containing releases just because validation
  can now produce candidate data.
- Do not import or manage actual video media files; this work is metadata-side
  support for audio files matched to video-containing MusicBrainz releases.
- Do not replace Beets matching or add a parallel matcher for this case.
- Do not rework release selection, search planning, or MusicBrainz release
  choice as part of this pass.
- Do not try to solve unrelated `mbid_not_found` causes in this work.

---

## Key Decisions

- Automatic validation population: Video-aware handling belongs in normal
  validation so Wrong Matches gets filled automatically.
- Operator control at import time: The human decision remains the existing Wrong
  Matches action, not a manual rerun required before evidence appears.
- Trigger on any video track: The pathway applies when the target release has at
  least one video track, because mixed releases need correct handling too. This
  is a deliberate scope choice, but mixed releases must not regress already
  usable target evidence.
- Preserve matched Beets semantics: The path still validates and imports against
  the target release identity.
- Scoped behavior: Video-track inclusion should be per relevant attempt, not a
  permanent global Beets policy change.
- Audio files only: Video-aware validation is not a promise that Beets or
  Cratedigger can import video files as video media.

---

## Dependencies / Assumptions

- Cratedigger can determine whether the target MusicBrainz release contains
  video tracks before or during validation.
- Beets can produce normal candidate data for video-containing releases when
  video tracks are included.
- Existing Wrong Matches rendering can display normal validation results once
  candidates, distance, and mapping data are present.
- The final import path can share the same video-aware setting as validation so
  the two phases do not disagree about the target track set.

---

## Outstanding Questions

### Deferred to Planning

- [Affects R1][Technical] Where should Cratedigger determine and cache whether a
  target release contains video tracks?
- [Affects R8, R15][Technical] What exact UI and audit wording should identify
  that video tracks were included?
- [Affects R14][Technical] How should validation and final import share the
  video-aware decision so the behavior cannot drift between phases?
