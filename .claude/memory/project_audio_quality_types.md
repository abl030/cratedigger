---
name: Audio quality type system
description: AudioFileSpec + AudioQualityMeasurement deployed; AudioQualityState deferred — consolidate album_requests quality columns when debugging friction warrants it
type: project
---

Audio quality type system built in 3 steps (2026-04-03):

1. **AudioFileSpec** — filetype identity (codec, extension, quality). Deployed.
2. **AudioQualityMeasurement** — replaces loose scalars in decision functions AND on ImportResult. QualityInfo deleted, SpectralInfo→SpectralDetail. Deployed.
3. **pipeline-cli quality** — simulates decisions for an album. Deployed.

**Why:** User wants quality decisions to be easier to debug. Measurements make the audit trail self-documenting. The `quality` CLI command shows what would happen for hypothetical downloads.

**How to apply:** When debugging quality issues, use `pipeline-cli show <id>` and `pipeline-cli quality <id>`. See CLAUDE.md "Debugging Quality Decisions" section.

**Deferred: AudioQualityState** — consolidate 8 scattered album_requests columns (min_bitrate, prev_min_bitrate, verified_lossless, spectral_grade, spectral_bitrate, on_disk_spectral_grade, on_disk_spectral_bitrate, quality_override) into a typed object. Do this when the scattered columns actually cause a bug or confusion, not speculatively. See TODO-audio-quality.md.
