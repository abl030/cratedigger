---
date: 2026-04-24
topic: quality-bucket-system
---

# Quality Bucket System

## Problem Frame

Cratedigger's quality system has accumulated overlapping concepts: search filetype tiers, slskd bitrate claims, local audio inspection, verified-lossless conversion, spectral demotion, and raw bitrate comparisons. This makes it hard to reason about whether an album should be searched, accepted, replaced, or kept.

The new model should make codec-aware quality buckets the central primitive. Buckets decide quality level. Tie-breakers compare candidates within the same bucket. Storage policy decides whether verified lossless lineage is kept as source audio or converted to the preferred target.

---

## Actors

- A1. Pipeline search: selects candidate Soulseek sources before download using slskd metadata.
- A2. Import pipeline: validates release identity, inspects downloaded files, and persists the final quality assessment.
- A3. Operator: configures codec preference, bucket thresholds, and whether selected albums keep lossless source files.

---

## Key Flows

- F1. Search candidate classification
  - **Trigger:** Cratedigger receives slskd search results for a tracked release.
  - **Actors:** A1
  - **Steps:** Use slskd `extension`, `bitRate`, VBR flag, sample rate, and bit depth as a trusted-enough search-time claim; classify each candidate into a claimed bucket; prefer candidates by bucket and codec preference.
  - **Outcome:** Search can slice results by quality without hardcoded codec overrides.
  - **Covered by:** R1, R2, R3, R7

- F2. Download/import quality assessment
  - **Trigger:** A selected candidate is downloaded and reaches import processing.
  - **Actors:** A2
  - **Steps:** Beets/release validation handles release identity and track matching; local audio inspection computes the actual codec-aware bucket; slskd claim vs actual inspection mismatches are recorded as evidence.
  - **Outcome:** Cratedigger compares actual quality against existing library state using buckets and tie-breakers, not raw cross-codec bitrate.
  - **Covered by:** R4, R5, R6, R8

---

## Requirements

**Quality Model**

- R1. Cratedigger must treat quality as a codec-aware bucket, not as a universal raw bitrate.
- R2. The bucket ladder must retain roughly the current quality-rank nuance rather than collapsing to only a few broad bands.
- R3. The top bucket must be named `verified`, representing verified lossless lineage whether the album is kept lossless or converted to a preferred target.
- R4. Broken or unreadable audio must be a validation outcome, not a quality bucket.
- R5. Verified lineage and storage form must be separate concepts: `verified` is the quality bucket; `stored_as=source` or `stored_as=target` is policy/state.

**Comparison Semantics**

- R6. Bucket comparison must happen before bitrate comparison.
- R7. Inside the same bucket, codec preference must beat raw cross-codec bitrate so Opus can be preferred over MP3 even when MP3 has a higher numeric bitrate.
- R8. Inside the same bucket and same codec family, bitrate metrics may act as tie-breakers.
- R9. Album-level bucket classification should default to median bitrate, while average and minimum bitrate remain first-class signals for tie-breaks, diagnostics, and outlier detection.
- R10. The primary album bitrate metric must be configurable.
- R11. Same-bucket codec preference order must be configurable, with the operator's expected default preference of Opus over AAC over MP3.

**System Boundaries**

- R12. Search-time classification may trust slskd metadata for slicing and candidate ordering.
- R13. Import-time classification must use local inspection of downloaded files for actual quality state.
- R14. Beets validation remains responsible for release identity, MBID/Discogs identity, track matching, and album completeness; these must not become quality tie-breakers.
- R15. Spectral analysis is out of the first rewrite; if reintroduced later, it should act as a bucket modifier/demotion signal rather than a parallel quality system.

**Configuration Surface**

- R16. Bucket thresholds, primary bitrate metric, and same-bucket codec preference order must be configurable through the deployment configuration.
- R17. The UI should surface the active bucket/ranking configuration so decisions are explainable from the web app.

---

## Acceptance Examples

- AE1. **Covers R1, R6, R7.** Given an existing Opus 128 album and a new MP3 V0 candidate, when both classify as `transparent`, raw MP3 bitrate alone must not cause MP3 to replace Opus.
- AE2. **Covers R3, R5.** Given a verified FLAC source converted to Opus 128 and another verified FLAC source kept as FLAC, both must classify as `verified`; their storage forms differ but their quality bucket does not.
- AE3. **Covers R12, R13.** Given slskd claims `mp3 320` but local inspection finds MP3 192 after download, search may have selected the candidate as `transparent`, but import must record the actual bucket from local inspection.
- AE4. **Covers R9.** Given a VBR album with one very quiet low-bitrate track and otherwise high-quality tracks, median-based bucket classification should not be dragged down solely by the single low track; the low track should remain available as an outlier signal.
- AE5. **Covers R14.** Given a candidate with imperfect track matching, beets/release validation rejects or stages it independently of quality bucket comparison.
- AE6. **Covers R7, R11.** Given Opus and MP3 candidates in the same bucket, the configured codec preference can choose Opus even though MP3 has a higher numeric bitrate.

---

## Success Criteria

- Operators can explain an import decision as bucket comparison plus same-bucket tie-breaks, without tracing spectral branches or cross-codec raw bitrate math.
- Search tiers, import comparison, and UI quality language use the same bucket vocabulary.
- Downstream planning does not need to invent the product semantics for `verified`, storage policy, slskd claims, or same-bucket tie-breakers.

---

## Scope Boundaries

- Do not broaden MusicBrainz or Discogs matching behavior as part of this rewrite.
- Do not move album completeness or track matching into the quality comparator.
- Do not treat spectral analysis as required for the first version of the bucket model.
- Do not preserve codec/filetype search overrides as a separate concept; bucket-based search should replace that mental model.
- Do not use raw bitrate to compare different codec families inside the same bucket.

---

## Key Decisions

- Bucket first, tie-break second, policy flags third: this keeps quality decisions understandable while preserving nuance.
- `verified` replaces `lossless` as the top bucket name because verified lineage can be stored either as source lossless or converted target audio.
- Slskd metadata is acceptable for search-time slicing, but local inspection is authoritative after download.
- Codec preference is an explicit same-bucket tie-breaker, so operator preference such as Opus over MP3 can be represented directly.
- Median bitrate is the default album bucket classifier; average and minimum bitrate remain available for tie-breaks, warnings, and diagnostics.
- Default same-bucket codec preference should reflect codec efficiency and operator preference: Opus before AAC before MP3.

---

## Dependencies / Assumptions

- Existing rank concepts in `docs/quality-ranks.md` and `lib/quality.py` are useful prior art, but their names and responsibilities need cleanup.
- slskd search results provide enough metadata to compute a claimed bucket for most audio candidates.
- Local inspection can compute an actual bucket from downloaded files before final import decision logging.

---

## Outstanding Questions

### Resolve Before Planning

- None.

### Deferred to Planning

- [Affects R12, R13][Technical] Identify every boundary where slskd audio metadata is currently dropped before `download_log`.
- [Affects R2][Technical] Map the current `QualityRank` ladder to the new bucket names without losing behavior that existing tests intentionally pin.
- [Affects R15][Technical] Decide which current spectral hooks should be removed, disabled, or isolated behind a future demotion interface.

---

## Next Steps

-> Resume brainstorming to resolve median vs average and default codec preference, then use `/ce-plan` for structured implementation planning.
