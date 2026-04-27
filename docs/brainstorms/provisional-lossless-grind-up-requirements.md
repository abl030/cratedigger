---
date: 2026-04-27
topic: provisional-lossless-grind-up
---

# Provisional Lossless Grind-Up

## Problem Frame

Issue #178 exposed a gap between Cratedigger's spectral evidence and its
replacement policy. When a lossless-container source is spectrally suspect, the
pipeline can discard it because its stored or converted bitrate is not higher
than an existing lossy copy, even though the lossless-container source is the
better bet inside the same suspect quality lane.

The desired behavior is not "trust every FLAC." Spectral evidence still matters:
`suspect` and `likely_transcode` sources are not verified lossless. But spectral
analysis is not perfect, and a lossless-container source carries useful evidence
that should let the pipeline grind upward within the suspect bucket instead of
freezing on a suspect native lossy file or looping through equivalent suspect
lossless files.

The stable comparison signal for this lane is the MP3 V0 probe created from the
lossless source before final storage. Its average bitrate should be persisted
and used to compare suspect lossless-container sources. The final stored format
is a separate storage policy decision.

---

## Actors

- A1. Import pipeline: validates, probes, compares, imports, rejects, and
  requeues download attempts.
- A2. Evidence system: produces spectral evidence and V0 probe metrics for
  downloaded candidates and inspected on-disk files.
- A3. Operator: reviews history and expects provisional upgrades and rejections
  to explain the evidence that drove them.
- A4. Downstream media consumers: receive library changes after provisional
  imports even though acquisition continues.

---

## Key Flows

- F1. Provisional lossless-source upgrade
  - **Trigger:** A supported lossless-container candidate passes validation and
    spectral analysis returns `suspect` or `likely_transcode`.
  - **Actors:** A1, A2, A3, A4
  - **Steps:** Produce and persist a lossless-source V0 probe; compare the
    candidate's V0 probe average against the existing comparable source probe;
    if it meaningfully improves, import it using the configured lossless-source
    storage target, record it as provisional, denylist the source, trigger
    normal post-import side effects, and keep the request wanted.
  - **Outcome:** The library improves within the suspect lane without treating
    the request as complete.
  - **Covered by:** R1, R2, R3, R6, R8, R9, R11, R13, R14

- F2. Suspect lossless-source no-op or downgrade
  - **Trigger:** A supported lossless-container candidate has suspect spectral
    evidence but its V0 probe average does not meaningfully beat the current
    comparable source probe.
  - **Actors:** A1, A2, A3
  - **Steps:** Preserve the candidate probe and spectral evidence in history;
    reject with a distinct suspect-lossless downgrade outcome; denylist the
    source; keep searching.
  - **Outcome:** Equivalent or worse sources do not cause retry loops and do
    not overwrite the current provisional best copy.
  - **Covered by:** R1, R3, R7, R10, R12, R14

- F3. Clean lossless-source import
  - **Trigger:** A supported lossless-container candidate has `genuine` or
    `marginal` spectral evidence.
  - **Actors:** A1, A2, A3, A4
  - **Steps:** Follow the existing verified-lossless path, including the
    configured target format and completion semantics.
  - **Outcome:** Verified-enough lossless sources remain the normal completion
    path and do not become provisional.
  - **Covered by:** R6, R15

- F4. Passive native-lossy probe collection
  - **Trigger:** The normal evidence pass inspects native lossy audio.
  - **Actors:** A2, A3
  - **Steps:** Produce and persist V0 probe metrics as research evidence only.
  - **Outcome:** Future analysis can decide whether the signal is useful, but
    v1 decisions do not depend on it.
  - **Covered by:** R4, R5

---

## Requirements

**V0 Probe Evidence**

- R1. Cratedigger must persist V0 probe `min`, `avg`, and `median` metrics for
  supported lossless-container candidates before final storage conversion can
  remove the source or temporary V0 files.
- R2. The v1 decision signal for suspect lossless-source grind-up must be the
  V0 probe average bitrate. V0 probe min and median are stored for audit and
  future analysis, but they do not drive v1 grind-up decisions.
- R3. V0 probe average comparisons must reuse
  `QualityRankConfig.within_rank_tolerance_kbps` to decide whether an
  improvement is meaningful. There must be no separate probe-specific tolerance
  in v1.
- R4. Native lossy V0 probes may be collected and stored as passive research
  evidence, but they must not affect import, rejection, ranking, requeue, or
  source-selection decisions until a later explicit product decision promotes
  that signal.
- R5. Evidence records must distinguish lossless-source V0 probes from weaker
  on-disk or native-lossy research probes so future policy does not accidentally
  treat second-generation evidence as source-lineage proof.

**Comparison Semantics**

- R6. A supported lossless-container candidate with `suspect` or
  `likely_transcode` spectral evidence must not be rejected by spectral evidence
  before its V0 source probe can be produced and compared.
- R7. A suspect lossless-container candidate whose V0 probe average is within
  tolerance of, or below, the current comparable source probe must be rejected
  as a suspect-lossless downgrade.
- R8. A suspect lossless-container candidate whose V0 probe average beats the
  current comparable source probe by more than the configured tolerance must be
  accepted as a provisional lossless-source upgrade.
- R9. If the existing album lacks a comparable lossless-source V0 probe, a new
  suspect lossless-container candidate with a source probe should win inside
  the suspect lane. This state is expected to be unusual and generally means
  the operator reopened the request because the current copy is not trusted.
- R10. Equivalent-within-tolerance and worse suspect lossless-source candidates
  share the same rejection label; no separate equivalent outcome is needed.
- R11. A suspect lossless-source upgrade must remain separate from existing
  `transcode_upgrade` semantics even if both import a provisional file and keep
  searching.
- R12. A suspect lossless-source rejection must remain separate from existing
  `transcode_downgrade`, `quality_downgrade`, and `spectral_reject` semantics.

**State, Storage, and Presentation**

- R13. A provisional lossless-source upgrade must import the candidate, store it
  using the configured lossless-source storage target, mark it unverified when
  spectral evidence is suspect, denylist the source, trigger normal post-import
  notifications/rescans, and leave or requeue the request as wanted so
  acquisition continues.
- R14. The feature must apply to all supported lossless-container sources, not
  only FLAC.
- R15. `genuine` and `marginal` lossless-container candidates must stay on the
  existing verified-lossless path rather than becoming provisional.
- R16. The operator-facing badge for accepted provisional imports must be
  `Provisional`.
- R17. Download history must explain provisional outcomes using the relevant
  evidence: spectral grade/floor, V0 probe average, existing comparable probe
  when present, stored format, source denylisting, and continued searching.
- R18. Broken, unreadable, corrupt, or structurally invalid audio may still fail
  before V0 probing; validation failures are not quality-policy decisions.

---

## Acceptance Examples

- AE1. **Covers R1, R6, R8, R9, R13, R16.** Given an existing suspect native
  MP3 320 copy with no comparable lossless-source V0 probe, when a
  lossless-container candidate is spectrally suspect and produces a V0 source
  probe average of 250kbps, Cratedigger imports it as `Provisional`, stores it
  using the configured lossless-source target, marks it unverified, denylists
  the source, and keeps searching.
- AE2. **Covers R1, R2, R3, R7, R8, R10.** Given an existing provisional
  suspect lossless-source copy with V0 probe average 171kbps, when a later
  suspect lossless-container candidate has V0 probe average 228kbps, it imports
  provisionally; when later candidates have V0 probe average 171kbps or any
  value within configured tolerance of the existing probe, they reject as
  suspect-lossless downgrades.
- AE3. **Covers R3, R7, R10.** Given `within_rank_tolerance_kbps=5`, an existing
  source probe average of 171kbps, and a new suspect lossless-source probe
  average of 175kbps, the new candidate is treated as not meaningfully better
  and is rejected as a suspect-lossless downgrade.
- AE4. **Covers R4, R5.** Given a native MP3 candidate or existing on-disk lossy
  file, when the evidence pass creates a V0 research probe, that probe is
  persisted but does not change the import or rejection decision in v1.
- AE5. **Covers R15.** Given a lossless-container candidate whose spectral
  grade is `genuine` or `marginal`, when it reaches import comparison, it
  follows the existing verified-lossless path and can complete the request.
- AE6. **Covers R14.** Given supported ALAC or WAV input, when spectral marks it
  suspect, the same provisional V0 source-probe comparison applies as it would
  for FLAC.

---

## Success Criteria

- Cratedigger can keep improving suspect albums instead of freezing on a
  suspect native lossy incumbent or discarding a better suspect
  lossless-container source.
- Repeated equivalent suspect lossless-container sources no longer cause loops
  or replace the current best provisional copy.
- Operators can explain Issue #178-style history from the UI without querying
  JSONB: the relevant spectral and V0 probe evidence is visible.
- Native lossy probe collection produces useful future research data without
  silently changing current policy.
- Downstream planning does not need to invent provisional outcome semantics,
  storage policy, source denylisting, request state, or comparison thresholds.

---

## Scope Boundaries

- Do not redesign the broader quality-bucket system.
- Do not retune spectral thresholds, cliff detection, or suspect rollup rules.
- Do not promote native lossy V0 probes into decision policy in v1.
- Do not collapse provisional lossless-source outcomes into existing transcode
  outcomes.
- Do not change release identity matching, beets distance thresholds, or album
  completeness rules.
- Do not add a separate V0 probe tolerance knob.
- Do not treat suspect lossless-container sources as verified lossless.

---

## Key Decisions

- V0 probe average is the decision signal: average bitrate from a
  quality-targeted VBR encode captures the source-complexity signal that made
  the Iron & Wine example meaningful.
- `within_rank_tolerance_kbps` is reused: one existing auditable tolerance
  governs meaningful same-lane bitrate movement.
- Storage is separate from evidence: provisional imports use the configured
  lossless-source storage target, while the V0 probe remains the comparison
  artifact.
- Provisional outcomes are distinct: they are close to transcode outcomes
  operationally, but they represent a different policy and need independent
  future tuning.
- Request state stays wanted: provisional imports improve the library but do
  not satisfy the acquisition goal.
- Passive native-lossy probes are research-only: the data may be promoted
  later, but v1 cannot let it affect decisions.

---

## Dependencies / Assumptions

- Existing import flow already creates temporary V0 artifacts for
  lossless-container candidates before target conversion.
- The configured lossless-source storage target is the desired storage form for
  provisional lossless-source imports; on doc2 this is `opus 128`.
- Historical albums may lack comparable lossless-source V0 probes. New imports
  should not normally lack them once this feature is in place.
- The source/user should be denylisted after both provisional upgrades and
  suspect-lossless downgrades because repeating the same source cannot improve
  the current state immediately.

---

## Outstanding Questions

### Resolve Before Planning

- None.

### Deferred to Planning

- [Affects R1, R5, R17][Technical] Choose the durable storage shape for source
  probes and passive research probes.
- [Affects R6, R13][Technical] Identify the exact gate or import stage where
  lossless-container candidates must bypass spectral hard rejection to reach the
  V0 source-probe comparison.
- [Affects R13][Technical] Confirm the dispatch action can import, notify,
  denylist, and leave the request wanted without leaking a completed request
  state.

---

## Next Steps

-> /ce-plan for structured implementation planning.
