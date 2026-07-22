# Spectral calibration research: Opus

Issue #829 Phase 0. Companion to `docs/research/spectral-mp3-lame.md`. The
question here is narrower than for MP3: does *any* bitrate-proportional
lowpass ladder exist for Opus that `lib/spectral_check.py`'s cliff detector
and `LAME_LOWPASS`/`estimate_bitrate_from_cliff()` could be adapted to, or
is that mechanism architecturally inapplicable to this codec? **Expected
conclusion, checked against primary sources below: no usable ladder
exists.** Opus's CELT layer does not apply an encoder-side lowpass filter
that moves with bitrate the way LAME's `optimum_bandwidth()` does. What
moves with bitrate in Opus is *within-band precision*, not *which bands are
coded at all* — and our analyzer only measures the latter.

## Summary

- Opus's audio bandwidth (NB/MB/WB/SWB/FB) is a **discrete 5-step mode**,
  not a continuous bitrate-proportional cutoff. The step to full 20 kHz
  bandwidth (FB) happens at a very low bitrate — on the order of 12-16 kbps
  for stereo music — confirmed directly from `libopus`'s own
  `opus_encoder.c` threshold tables
  (<https://github.com/xiph/opus/blob/main/src/opus_encoder.c>).
- Above that threshold, **every bitrate our library actually contains**
  (32/48/64/96/128/192/256 kbps stereo music) selects the same FB mode.
  There is no further bandwidth narrowing as bitrate rises — the "ladder"
  the MP3 doc found bottoms out below the range that matters for music and
  is flat above it.
- The 20 kHz ceiling itself is a **fixed architectural constant**, not a
  bitrate-dependent artifact: RFC 6716 states Opus "never codes audio above
  20 kHz" full stop, and the CELT/MDCT layer achieves the lower bandwidth
  modes by *zeroing* bands in the frequency domain, not by a variable-cutoff
  filter (<https://www.rfc-editor.org/rfc/rfc6716.html>).
- Even within FB mode at low per-band bit allocation, CELT's band-energy
  preservation and **spectral/band folding** design keeps every band's
  *energy* transmitted and present, borrowing shape from lower bands when
  there aren't enough bits for independent detail
  (<https://en.wikipedia.org/wiki/CELT>). A coarse RMS-per-band detector
  like ours cannot tell "independently coded detail" apart from
  "folded/reused shape at the correct energy" — both read as full-bandwidth,
  non-silent energy.
- Net result: our cliff detector and `hf_deficit_db` metric will read
  **"genuine, no cliff" for essentially all Opus files at ≥64 kbps**,
  regardless of whether the true encode was 64 or 256 kbps. Not a detector
  bug — it measures bandwidth presence, and Opus doesn't vary that signal
  with quality the way MP3/AAC do.
- The private-tracker "spectral analysis" reference guides the MP3/AAC
  community uses for visual transcode detection do not cover Opus at all
  (checked <https://interview.orpheus.network/spectral-analysis.php> and
  <https://interviewfor.red/en/spectrals.html> — neither mentions Opus).
  Weak corroborating evidence: the community that built spectrogram-cliff
  heuristics for every other lossy codec has not produced one for Opus.

## Opus architecture in 10 lines

1. Opus (RFC 6716) is a container for two sub-codecs: **SILK** (linear
   prediction, speech-oriented, effective bandwidth up to 8 kHz) and
   **CELT** (MDCT-based, up to 20 kHz), plus a **hybrid** mode that runs
   both at once <https://www.rfc-editor.org/rfc/rfc6716.html>.
2. In hybrid mode, SILK codes 0-8 kHz and CELT codes 8 kHz upward, crossing
   over at 8 kHz — the maximum wideband (WB) frequency
   (<https://www.rfc-editor.org/rfc/rfc6716.html>).
3. The **CELT/MDCT layer always runs internally at 48 kHz**, irrespective
   of bandwidth mode; lower bandwidths are produced by zeroing
   high-frequency bands in the frequency domain, not a separate filter
   stage (<https://www.rfc-editor.org/rfc/rfc6716.html>).
4. CELT's spectrum is split into **21 fixed critical bands** up to 20 kHz
   (`eband5ms[]` in `celt/modes.c`, values `0,1,2,...,78,100` in units of
   200 Hz — band 21's edge is `100*200 Hz = 20000 Hz` exactly)
   (<https://github.com/xiph/opus/blob/main/celt/modes.c>).
5. This band layout is **fixed regardless of bitrate**. Bitrate changes how
   many bits (PVQ pulses) each band gets, not how many bands exist or where
   their edges fall.
6. Each band's **energy is coded first and separately** ("coarse energy"),
   then the normalized residual shape is coded via **Pyramid Vector
   Quantization** (<https://en.wikipedia.org/wiki/CELT>).
7. When a band gets too few bits for its own shape, CELT applies **band
   folding** — reusing lower bands' decoded shape, rescaled to the
   transmitted energy — "similar effect to spectral band replication
   (SBR)... but has much less impact on... algorithmic delay and
   computational complexity" (<https://en.wikipedia.org/wiki/CELT>).
8. Opus picks SILK-only / hybrid / CELT-only and NB/MB/WB/SWB/FB
   automatically per-frame from bitrate and a speech/music probability
   estimate (`voice_est`), unless overridden via
   `OPUS_SET_BANDWIDTH`/`OPUS_SET_MAX_BANDWIDTH`
   (<https://opus-codec.org/docs/opus_api-1.5/group__opus__encoderctls.html>).
9. For music, the mode threshold sits around equivalent-bitrate ~10 kbps —
   below every practical music bitrate — so **CELT-only is the default
   mode for music** at 32 kbps and up (`mode_thresholds[][1] = 10000` for
   mono and stereo, <https://github.com/xiph/opus/blob/main/src/opus_encoder.c>).
10. `ffmpeg`/`opusenc` default to `application=audio`, VBR on, complexity
    10 — settings a music library is almost certainly encoded with
    (<https://ffmpeg.org/ffmpeg-codecs.html>).

## Bandwidth mode thresholds — the real (coarse) ladder

RFC 6716 Table 1, the five bandwidth modes and their nominal audio
bandwidth / internal sample rate
(<https://www.rfc-editor.org/rfc/rfc6716.html>):

| Mode | Audio bandwidth | Effective sample rate |
|------|------------------|------------------------|
| NB (narrowband) | 4 kHz | 8 kHz |
| MB (medium-band) | 6 kHz | 12 kHz |
| WB (wideband) | 8 kHz | 16 kHz |
| SWB (super-wideband) | 12 kHz | 24 kHz |
| FB (fullband) | 20 kHz (hard ceiling, human-hearing rationale) | 48 kHz |

`libopus`'s automatic-bandwidth logic in `opus_encoder.c`
(<https://github.com/xiph/opus/blob/main/src/opus_encoder.c>) picks the
bandwidth from an **equivalent bitrate** (`equiv_rate`, the configured
bitrate adjusted for frame-rate overhead, VBR/CBR, and complexity — at the
defaults a music encode is likely to use, complexity 10 and VBR on,
`equiv_rate` is close to the nominal bitrate) against threshold/hysteresis
pairs, blended between "voice" and "music" tables by a per-frame
speech/music probability estimate:

```c
static const opus_int32 stereo_music_bandwidth_thresholds[8] = {
         9000,  700, /* NB<->MB */
         9000,  700, /* MB<->WB */
        11000, 1000, /* WB<->SWB */
        12000, 2000, /* SWB<->FB */
};
```

(mono music table is nearly identical: `9000/700, 9000/700, 11000/1000,
12000/2000`; "voice" tables push WB<->SWB/SWB<->FB slightly higher —
13500-14000 bps — but real music weights almost entirely toward the music
table.) Reading the SWB<->FB row: a stereo music stream reaches FB (full
20 kHz) once `equiv_rate` clears roughly **12 kbps**, with 2000 bps of
hysteresis. Mode selection (SILK-only vs. CELT-only) uses a separate, lower
threshold for music (`mode_thresholds[stereo][music] = 10000` bps, same
source).

Corroborating secondary source (Hydrogenaudio's Opus wiki page): "bandwidth
progresses from narrowband (4 kHz) at 6 kbps, through wideband and
superwideband intermediate ranges, reaching fullband (20 kHz) by
approximately 16-24 kbps for music content"
(<https://wiki.hydrogenaudio.org/index.php?title=Opus>). FFmpeg's own docs
independently confirm the low end of this behavior: "libopus forces a
wideband cutoff for bitrates < 15 kbps, unless CELT-only (application set
to `lowdelay`) mode is used" (<https://ffmpeg.org/ffmpeg-codecs.html>).

**The practical consequence: every bitrate this library is likely to
contain is already past the ladder.** 32, 48, 64, 96, 128, 192, and
256 kbps stereo music all sit far above the ~12-16 kbps FB threshold —
they all select the *same* bandwidth mode. The only place a real,
bitrate-driven bandwidth *step* exists is below ~16 kbps, a range that
essentially never appears for music (it's a voice/podcast bitrate).

## What cliff detection sees per bitrate

| Nominal bitrate (stereo music, VBR, `application=audio`) | Selected bandwidth mode | What `sox`-based 12-20 kHz slicing measures | `spectral_check.py` grade |
|---|---|---|---|
| ≤ ~10-12 kbps | SWB or lower (cutoff ≤ 12 kHz) | Real, near-total silence 12-20 kHz | Genuine cliff/HF-deficit — but not music-relevant |
| ~16-32 kbps | FB, very few bits per high band | Attenuated but present energy 12-20 kHz — folded shape at the band's true (low) energy, not silence | Likely `genuine`/borderline `marginal`; not a clean cliff |
| 64 kbps | FB | Energy present across all 21 bands; folding fills fine detail in the least-allocated high bands | `genuine` [unverified: exact `hf_deficit_db` at 64 kbps not measured against a real file for this doc — see Phase 3] |
| 96 / 128 kbps | FB | Same — Hydrogenaudio's own table calls 96-128 kbps "good quality approaching transparency" to "very close to transparency," i.e. higher per-band precision, not more bandwidth (<https://wiki.hydrogenaudio.org/index.php?title=Opus>) | `genuine` |
| 192 / 256 kbps | FB | Same bandwidth as 64 kbps; only per-band precision (PVQ pulse count) differs | `genuine` — **indistinguishable from 96 kbps by our detector** |

Mechanism behind the flat middle/right side of that table: CELT's coarse
energy for every band up to the mode's top edge is coded and transmitted
regardless of how many bits are left for shape detail
(<https://en.wikipedia.org/wiki/CELT>, PVQ/normalization description);
folding then fills in shape for starved bands using rescaled lower-band
content. Our detector measures *band RMS* — exactly the value CELT
preserves first and always — so it reads "present" even where detail is
folded/approximate. It was built to find LAME's *removed* bands (zero
content past the lowpass); it has no comparable signal for Opus's
*degraded-but-present* bands.

## The 20 kHz shelf vs. LAME cliffs

Two genuinely different mechanisms produce superficially similar-looking
"stuff stops near 20 kHz" spectrograms:

- **LAME** (see the companion MP3 doc) computes a bitrate-specific lowpass
  frequency via `optimum_bandwidth()` and applies it at encode time — the
  cutoff frequency *is* a function of bitrate, ranging from ~15.1 kHz at
  96 kbps to ~20.5 kHz at 320 kbps. That's the entire basis of
  `LAME_LOWPASS`/`estimate_bitrate_from_cliff()`.
- **Opus** has one fixed ceiling at 20 kHz for the FB mode, justified
  purely by human-hearing limits, independent of bitrate: "Opus never
  codes audio above 20 kHz, as that is the generally accepted upper limit
  of human hearing" (<https://www.rfc-editor.org/rfc/rfc6716.html>). There
  is no bitrate variable anywhere in that statement.

This has been a live complaint on Opus's own mailing list, not just a
theoretical distinction. In January 2013 a user reported that "at 96 kbps
the cutoff of the filter starts at 16kHz and is completely cut at 20kHz"
and asked for a `--lowpass` option to move it; Xiph developer Benjamin
Schwartz replied: "Opus is a single-purpose codec for audio going into
human ears. Human ears can't hear above 20 kHz, so Opus can't code higher
frequencies," and recommended FLAC for anyone who needs content above
20 kHz (<https://lists.xiph.org/pipermail/opus/2013-January/001939.html>).
The reporter's observed shape (taper from ~16 kHz, fully attenuated by
20 kHz) resembles our cliff detector's target shape, but Schwartz's reply
confirms the boundary is the fixed 20 kHz ceiling, not a 96-kbps-specific
artifact — the same shape appears at 128, 192, or 256 kbps.

A more pointed, non-primary version of the same complaint (the NamuWiki
Opus article) argues the 20 kHz ceiling is unnecessarily strict for
listeners who can hear 21-22 kHz, and that "even from a technical
standpoint, it isn't difficult to support up to 24 kHz" given the 48 kHz
internal rate (<https://en.namu.wiki/w/Opus(오디오 코덱)> —
[unverified: the specific "22-23 kHz would fix half the complaints" claim]).
Whatever the merits, it corroborates the structural point: the 20 kHz
behavior is one fixed policy decision, not a bitrate ladder.

## Implications for calibration

- **Above the SWB/FB threshold (~12-16 kbps for stereo music) — every
  practical music bitrate — spectral cliff/bitrate-ladder detection is
  architecturally inapplicable to Opus.** `estimate_bitrate_from_cliff()`
  and `LAME_LOWPASS` must never be applied to an Opus track; nothing in
  Opus's design produces the bitrate→cliff-frequency relationship that
  table encodes. The right policy for #829 is **audit-only** for Opus at
  ≥64 kbps: full-bandwidth energy presence is at best a sanity check that
  the file isn't a from-MP3 transcode carrying a visible legacy cliff
  underneath the Opus re-encode — a genuinely different, still-detectable
  signal, but not a quality/bitrate estimator.
- **Below the SWB/FB threshold**, a real, sourced, bitrate-driven boundary
  exists (12 kHz at SWB, 8 kHz at WB) and *could* drive a coarse detector,
  but only matters for Opus files under ~16 kbps — a speech/podcast
  bitrate, not a music-library one. Low priority; likely not worth building
  unless Phase 3 finds such files in the wild.
- **What our existing detector *can* still catch**: a cliff inside the
  12-20 kHz slice range on a file that is *not* near the SWB/FB boundary is
  not explainable by Opus's own architecture — only by a band-limited
  source feeding the encoder (MP3/AAC-sourced transcode-to-Opus). Legitimate
  signal, but a *source* flag, not a bitrate estimate; `LAME_LOWPASS` must
  not be reused to label it with an assumed original bitrate.

## Predictions for Phase 3 (testable)

These are falsifiable predictions Phase 3 should check against real
encodes (e.g., `ffmpeg -c:a libopus -b:a <N>k -vbr on -application audio`
at 32/48/64/96/128/192/256 kbps stereo music, run through
`lib/spectral_check.py::analyze_track`):

1. **All of 32/48/64/96/128/192/256 kbps should report `cliff_detected =
   False`**, because all of them select FB bandwidth per the thresholds
   above.
2. **`hf_deficit_db` should stay well under `HF_DEFICIT_MARGINAL = 40.0`
   for 64 kbps and above**, and should *not* shrink monotonically as
   bitrate rises the way it does for LAME — if it does, the folding
   mechanism doesn't fully mask the energy differences our slice widths
   can see, and that's a genuine finding against this doc's prediction.
3. **A cliff tripped in the 12-20 kHz range should correlate with either
   (a) an encode bitrate under ~16 kbps** (mode boundary — verify via the
   file's actual bitrate/VBR average) **or (b) a band-limited upstream
   source**, not with the music bitrate itself. A genuine music-bitrate
   (≥64 kbps) Opus file with a clean cliff correlating with *bitrate*
   rather than *source* would falsify this doc's central claim.
4. **`estimate_bitrate_from_cliff()` should never be called on an Opus
   file in practice** — Phase 3's harness should assert this (a
   fast-failing guard or generated property), not merely document it.
5. A very-low-bitrate control point (12-16 kbps, voice-ish content) should
   show a real, reproducible cliff near 12 kHz — the one place this doc
   predicts the detector's mechanism still corresponds to something real.

## Sources

- `lib/spectral_check.py` (this repo) — the code under calibration.
- `docs/research/spectral-mp3-lame.md` (this repo) — companion doc, the
  LAME ladder this doc contrasts Opus against.
- RFC 6716, "Definition of the Opus Audio Codec" — bandwidth mode table,
  20 kHz rationale, 48 kHz internal MDCT rate, hybrid crossover at 8 kHz:
  <https://www.rfc-editor.org/rfc/rfc6716.html>
- `xiph/opus` source, `src/opus_encoder.c` — `mode_thresholds`, mono/stereo
  voice/music `*_bandwidth_thresholds`, `compute_equiv_rate()`, default
  bitrate formula: <https://github.com/xiph/opus/blob/main/src/opus_encoder.c>
- `xiph/opus` source, `celt/modes.c` — `eband5ms[]` 21-band CELT layout:
  <https://github.com/xiph/opus/blob/main/celt/modes.c>
- Opus API docs — `OPUS_SET_BANDWIDTH`/`OPUS_SET_MAX_BANDWIDTH` semantics:
  <https://opus-codec.org/docs/opus_api-1.5/group__opus__encoderctls.html>
- Wikipedia, "CELT" — band energy normalization, PVQ, band folding:
  <https://en.wikipedia.org/wiki/CELT>
- Xiph mailing list, opus@xiph.org, Jan 2013 ("low pass filter frequency
  adjustable") — 96 kbps taper 16→20 kHz; Schwartz's 20 kHz reply:
  <https://lists.xiph.org/pipermail/opus/2013-January/001939.html>
- XiphWiki, "OpusFAQ" — SILK/CELT/hybrid frequency split, 20 kHz rationale:
  <https://wiki.xiph.org/OpusFAQ>
- XiphWiki, "Opus Recommended Settings" — bandwidth-threshold pointer,
  stereo/mono downmix threshold, complexity default:
  <https://wiki.xiph.org/Opus_Recommended_Settings>
- Hydrogenaudio Knowledgebase, "Opus" — bandwidth-by-bitrate progression
  for music, 64/96/128/160-192 kbps quality characterizations:
  <https://wiki.hydrogenaudio.org/index.php?title=Opus>
- FFmpeg documentation, `libopus` encoder options — `cutoff` values,
  wideband-cutoff-below-15kbps note, VBR/application defaults:
  <https://ffmpeg.org/ffmpeg-codecs.html>
- Orpheus and Redacted-lineage interview-prep spectral analysis guides —
  checked for Opus coverage, found none (MP3-only ladders documented):
  <https://interview.orpheus.network/spectral-analysis.php>,
  <https://interviewfor.red/en/spectrals.html>
- NamuWiki, "Opus (오디오 코덱)" — community criticism of the fixed 20 kHz
  ceiling; cited only for the structural point that the ceiling doesn't
  move with bitrate — the "22-23 kHz" remediation claim is unverified:
  <https://en.namu.wiki/w/Opus(오디오 코덱)> [unverified: numeric claim]
