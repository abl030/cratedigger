# Spectral calibration research — AAC

Issue #829 Phase 0. AAC is the codec family that triggered the calibration project: ordinary AAC-LC around 128 kbps rolls off at roughly 16-17 kHz, landing squarely inside the bucket our analyzer currently reserves for "this is a 128 kbps LAME MP3 wearing a lossless costume."

## Summary

`lib/spectral_check.py` has exactly one calibration table (`LAME_LOWPASS`, lines 31-40) and it is a **LAME MP3 lowpass table**, not a general lossy-codec table. Every cliff `detect_cliff()` finds is run through `estimate_bitrate_from_cliff()`, bucketed into a LAME-shaped bitrate guess, and (via `classify_track`) any file with a detected cliff is graded `"suspect"` outright — regardless of which codec produced it. AAC is the worst-hit family:

- **AAC-LC's designed bandwidth at common bitrates (96-192 kbps) overlaps the same 15-19 kHz window LAME uses for 96-160 kbps MP3.** A genuine libfdk_aac or Apple encode at 128 kbps rolls off close to where a *transcoded* MP3-128-as-AAC would land — the analyzer can't tell "codec's native ceiling" from "upstream lossy source's ceiling."
- **HE-AAC (SBR) breaks the cliff-detection premise entirely.** The transmitted core signal ends at 8-13 kHz, but the decoder synthesizes everything above from mirrored/patched copies of the core band's own harmonics. What the detector sees past the crossover is neither genuine full bandwidth nor clean silence — it's reconstructed content with its own amplitude profile, which can register as a cliff, a large deficit, or neither, because SBR's shape was never designed to imitate a natural rolloff.
- **Apple's encoder — the single most common real-world AAC source (qaac / CoreAudio / iTunes Plus / Apple Music) — publishes no cutoff table at all.** It is closed-source and empirically variable; there is no authoritative Hz-per-bitrate reference to calibrate against, unlike LAME or FDK.

## Why AAC broke cratedigger

`estimate_bitrate_from_cliff` was written against LAME's documented lowpass table (cliff at ~17.0 kHz → "this was a 128 kbps MP3"). A genuine, never-transcoded AAC-LC file at 128-160 kbps from libfdk_aac or Apple's encoder rolls off in almost exactly that same 15-17 kHz band by design, not because it was ever an MP3. The analyzer has no AAC-shaped bucket, so it runs the AAC file's real, intentional cliff through the MP3 table and reports "MP3 128 transcode" — the exact false positive this calibration project exists to fix.

## Encoder-by-encoder cutoff behavior

### ffmpeg native `aac` encoder

FFmpeg's built-in encoder (`-c:a aac`) computes its lowpass automatically unless `-cutoff` is given: *"Set cutoff frequency. If unspecified will allow the encoder to dynamically adjust the cutoff to improve clarity on low bitrates."* ([FFmpeg encoders.texi](https://github.com/FFmpeg/FFmpeg/blob/master/doc/encoders.texi)) No published fixed Hz-per-bitrate table exists the way LAME and FDK publish one; the cutoff is derived from a bitrate/bandwidth heuristic in `libavcodec/aaccoder.c` (`rate_bandwidth_multiplier`, tied to `frame_bit_rate` and sample rate) rather than a lookup table `[unverified — exact numeric thresholds not confirmed from source]`.

Quality history matters because the native encoder is a common self-transcode path: it was "considered experimental and poor quality" before a 2015 GSoC rewrite, and FFmpeg declared it stable "and ready for common use" from FFmpeg 3.0 (Feb 2016). Hydrogenaudio now ranks it **second** behind libfdk_aac: *"the native FFmpeg AAC encoder is currently the second highest-quality AAC encoder available in FFmpeg."* ([Hydrogenaudio: AAC encoders](https://wiki.hydrogenaudio.org/?title=AAC_encoders), [FFmpeg wiki: Encode/AAC](https://mirror.hjertaas.com/trac.ffmpeg.org/trac.ffmpeg.org/wiki/Encode/AAC.html))

| Bitrate | Expected cutoff behavior | Source |
|---|---|---|
| Any (default, `-cutoff 0`) | Auto-computed per bitrate/sample-rate; no fixed table published | [encoders.texi](https://github.com/FFmpeg/FFmpeg/blob/master/doc/encoders.texi) |
| Low bitrates (<96 kbps) | Encoder narrows bandwidth further to protect clarity | [encoders.texi](https://github.com/FFmpeg/FFmpeg/blob/master/doc/encoders.texi) |
| Manual override | `-cutoff <Hz>`; community guidance is against forcing it high at low bitrate — "raising it to 20000 will probably yield lower quality" | [FFmpeg wiki: Encode/AAC](https://mirror.hjertaas.com/trac.ffmpeg.org/trac.ffmpeg.org/wiki/Encode/AAC.html) |

### libfdk_aac (Fraunhofer FDK)

FDK is the only AAC encoder with a fully documented, source-level bandwidth table. Fraunhofer's header states the policy plainly: *"The FDK AAC encoder usually does not use the full frequency range of the input signal... it is not recommended to change these settings, because they are based on numerous listening tests."* ([aacenc_lib.h](https://github.com/mstorsjo/fdk-aac/blob/master/libAACenc/include/aacenc_lib.h)) The table lives in `libAACenc/src/bandwidth.cpp` (`FDKaacEnc_DetermineBandWidth`), keyed by per-channel bitrate:

| Per-channel bitrate | Mono bandwidth | Stereo+ bandwidth |
|---|---|---|
| 0 kbps | 3,700 Hz | 5,000 Hz |
| 12 kbps | 5,000 Hz | 6,400 Hz |
| 20 kbps | 6,900 Hz | 9,640 Hz |
| 28 kbps | 9,600 Hz | 13,050 Hz |
| 40 kbps | 12,060 Hz | 14,260 Hz |
| 56 kbps | 13,950 Hz | 15,500 Hz |
| 72 kbps | 14,200 Hz | 16,120 Hz |
| 96 kbps+ (CBR) | **17,000 Hz (capped)** | **17,000 Hz (capped)** |

Source: [bandwidth.cpp](https://github.com/mstorsjo/fdk-aac/blob/master/libAACenc/src/bandwidth.cpp) (transcribed from the `bandWidthTable` array), cross-referenced against [Hydrogenaudio: Fraunhofer FDK AAC](https://wiki.hydrogenaudio.org/index.php?title=Fraunhofer_FDK_AAC).

The critical, non-obvious fact: **CBR bandwidth caps at 17,000 Hz and never goes higher, however high the bitrate climbs** — the table's last two rows (96 kbps and 576,001 kbps) both map to 17,000/17,000. A 320 kbps FDK-AAC-CBR file has the *same* nominal cutoff as a 96 kbps one — a deliberate psychoacoustic choice (extra bits buy passband precision, not bandwidth). FDK-AAC never produces the near-full-bandwidth ~20.5 kHz cliff LAME 320 does, so a LAME-derived "high bitrate = wide bandwidth" expectation will always read AAC as suspicious, at every bitrate.

FDK's VBR modes (0=CBR, 1-5) use fixed bandwidths, independent of resulting bitrate:

| VBR mode | Bandwidth (mono & stereo) |
|---|---|
| VBR 1 | 13,000 Hz |
| VBR 2 | 13,000 Hz |
| VBR 3 | 15,750 Hz |
| VBR 4 | 16,500 Hz |
| VBR 5 | 19,293 Hz |

Source: [bandwidth.cpp](https://github.com/mstorsjo/fdk-aac/blob/master/libAACenc/src/bandwidth.cpp). Mode-to-approximate-bitrate mapping (e.g. VBR 3 ≈ 96 kbps/channel class) is `[unverified]` — not independently confirmed from a primary source in this pass.

The oft-repeated community line — FFmpeg wiki: *"libfdk_aac defaults to a low-pass filter of around 14kHz... use `-cutoff 18000`"* — matches the 56-96 kbps/channel **mono** rows (13,950-14,200 Hz) rather than stereo (15.5-16.1 kHz at the same bitrates), which is likely why casual command-line encodes so often land near "14 kHz" in folklore. ([FFmpeg wiki: Encode/AAC](https://mirror.hjertaas.com/trac.ffmpeg.org/trac.ffmpeg.org/wiki/Encode/AAC.html))

### Apple AAC (qaac / CoreAudio / afconvert)

This is the encoder behind the single most common real-world AAC source — iTunes Plus, Apple Music downloads, any macOS/iOS rip — and the one encoder here with **no published cutoff table**. Apple's own note (TN2271) documents bitrate control modes (CBR/ABR/CVBR/TVBR quality 0-127) but says nothing about resulting spectral bandwidth. ([Apple TN2271](https://developer.apple.com/library/archive/technotes/tn2271/_index.html))

When a qaac user asked the maintainer directly which lowpass frequencies Apple's encoder uses per bitrate/mode — the kind of table LAME and FDK both publish — the answer was unambiguous:

> "There's no way to control the bandwidth of Apple's encoder... If you want to know encoder's bandwidth, just try encoding and inspect the output yourself." — nu774 (qaac maintainer), [qaac issue #70](https://github.com/nu774/qaac/issues/70#issuecomment-699576953)

The thread closes with no empirical numbers ever supplied. Elsewhere, community observations are device-dependent folklore rather than an encoder spec — e.g. different phone SoCs re-encoding Bluetooth AAC at the same nominal 256 kbps show materially different cutoffs (~16 kHz on one chipset, none apparent on another), which speaks to the codec profile in general, not Apple's own target curve. ([Archimago's Musings](http://archimago.blogspot.com/2023/08/part-ii-comparison-of-bluetooth.html)) `[unverified]` for any specific Apple-encoder Hz-per-bitrate figure.

| Mode | Description | Source |
|---|---|---|
| CBR | Constant bitrate, minor fluctuation | [Hydrogenaudio: Apple AAC](https://wiki.hydrogenaudio.org/index.php?title=Apple_AAC) |
| ABR | Average bitrate target | [Hydrogenaudio: Apple AAC](https://wiki.hydrogenaudio.org/index.php?title=Apple_AAC) |
| CVBR | What iTunes Plus / Apple Music use — will not exceed the nominal rate (256 kbps) but can dip below it | [Hydrogenaudio: Apple AAC](https://wiki.hydrogenaudio.org/index.php?title=Apple_AAC) |
| TVBR | Quality-scale VBR (0-127), can exceed the nominal rate for complex stereo content | [Apple TN2271](https://developer.apple.com/library/archive/technotes/tn2271/_index.html) |

Reputation: *"Apple AAC... does consistently well in Hydrogenaudio listening tests"* and is *"known to be one of the highest quality medium-bitrate CBR and VBR LC AAC encoders."* ([Hydrogenaudio: Apple AAC](https://wiki.hydrogenaudio.org/index.php?title=Apple_AAC)) That's the calibration risk in one sentence: a high-quality, non-transcoded Apple encode is exactly the file we must not flag, and exactly the encoder we have the least documented ability to model.

Real-world bitrate history for this encoder: original iTunes Music Store (2003-2007) shipped 128 kbps FairPlay-DRM AAC; iTunes Plus (May 2007) introduced DRM-free 256 kbps AAC; by early 2009 256 kbps CVBR became the sole default, and Apple Music/iTunes downloads remain there today. ([Macworld: iTunes Store goes DRM-free](https://www.macworld.com/article/194277/itunestore.html); [Apple Newsroom: Apple Launches iTunes Plus](https://www.apple.com/newsroom/2007/05/30Apple-Launches-iTunes-Plus/)) A meaningful fraction of "legacy" library rips still in circulation are the older 128 kbps encodes, so both bitrate classes are plausible inputs.

## HE-AAC / SBR and what cliff detection sees

HE-AAC v1 (SBR) and HE-AAC v2 (SBR + Parametric Stereo) are not "AAC-LC with a lower cutoff" — they stack two signals: (1) a **core layer** of ordinary AAC-LC encoded at *half* the nominal output sample rate, carrying only low/mid frequencies ([Hydrogenaudio: Fraunhofer FDK AAC](https://wiki.hydrogenaudio.org/index.php?title=Fraunhofer_FDK_AAC): "the HE-AAC and HE-AACv2 profiles encode audio using AAC-LC at one half the sample rate"); (2) **SBR side info** — a compact description (spectral envelope + noise/tone floor) of what the upper band should look like, computed by the encoder from the full-band original; and (3) **decoder reconstruction** via *spectral patching* — copying adjoining QMF (Quadrature Mirror Filter) subbands from the transmitted low band up into the high band, shaped by the side info, to synthesize plausible highs. ([Wikipedia: Spectral band replication](https://en.wikipedia.org/wiki/Spectral_band_replication); [Hydrogenaudio: Spectral Band Replication](https://wiki.hydrogenaudio.org/index.php?title=Spectral_Band_Replication))

The **SBR crossover frequency** — where "real, transmitted" switches to "encoder-guided, decoder-synthesized" — moves with bitrate: lower bitrate pushes it down because that's the only way to free bits, a quality/data-rate tradeoff, not a free lunch. ([EBU Tech Review 305 — Moser](https://tech.ebu.ch/docs/techreview/trev_305-moser.pdf)) One documented example: at 64 kbps stereo, SBR splits around 7.5 kHz, handing 7.5-15 kHz to the reconstruction tool for ~1.5 kbps of side-info overhead `[unverified — single source, illustrative not universal]`. Typical nominal bitrates: HE-AAC v1 ~48-64 kbps stereo (160 kbps for 5.1); HE-AAC v2 ~24-32 kbps stereo. ([EBU Tech Review 305](https://tech.ebu.ch/docs/techreview/trev_305-moser.pdf); [Broadcast Bridge: HE-AAC](https://www.thebroadcastbridge.com/content/entry/21823/standards-high-efficiency-audio-codecs-he-aac))

**What our cliff detector actually measures on an HE-AAC file:**

- `detect_cliff()` slices only 12,000-20,000 Hz. If the SBR crossover sits below 12 kHz (typical for 48-64 kbps stereo per above), the *real* cliff — the core layer's true information boundary — is invisible to our slicer; everything in our window is 100% SBR-synthesized.
- Synthesized highs are neither a smooth natural rolloff nor silence — a patched copy of lower harmonics reshaped by an envelope. That copy can: (a) show its own secondary rolloff near the top of the SBR range, read as an unrelated cliff; (b) sit at an unnaturally healthy energy level for its real information content, UNDER-estimating `hf_deficit_db` and letting a file that should be flagged pass as `"genuine"`; or (c) show comb-like irregularities from QMF patching that neither metric characterizes.
- Net effect: cliff detection and HF-deficit scoring on HE-AAC content isn't "miscalibrated," it's **measuring the wrong thing** — a cliff found in our 12-20 kHz window may sit entirely inside the synthetic region and tell us almost nothing about real information content.

## CBR/VBR nuances

- **FDK CBR bandwidth is bitrate-*insensitive* above 96 kbps/channel** — the single biggest AAC-specific trap for a LAME-modeled bitrate-from-cutoff heuristic (where cutoff keeps climbing to 320 kbps). Expect FDK-AAC from 96 through 320 kbps CBR to cluster at the same ~17 kHz cutoff.
- **Apple CVBR (what iTunes Plus/Apple Music ship) is bitrate-capped from above, not below** — "256 kbps" is a ceiling, dipping lower on simple passages ([Hydrogenaudio: Apple AAC](https://wiki.hydrogenaudio.org/index.php?title=Apple_AAC)). A track's average bitrate can misrepresent the specific 30-second window our analyzer trims to (`trim_seconds=30` in `analyze_track`) — sharper for VBR/CVBR sources than CBR.
- **FFmpeg native `aac`'s only documented CBR/VBR distinction is that `-b:a` activates CBR** vs. `-q:a` for VBR; no separate published bandwidth table per mode — "dynamically adjust" is the entire public spec.
- **HE-AAC has no meaningful CBR/VBR distinction for bandwidth** — the crossover-vs-bitrate tradeoff dominates regardless of rate-control mode, because the core/SBR split is what's traded, not quantizer step size.
- **Streaming rips add a second layer of uncertainty on top of encoder choice**: YouTube Music serves AAC-LC at 128 kbps (itag 140) or 256 kbps (itag 141, Premium); Spotify's web player offers AAC at 128/256 kbps. A "YouTube rip" or "Spotify rip" is a genuine AAC-LC encode at one of these nominal rates, not evidence of transcoding — but which underlying encoder produced it is generally unknown/unversioned.

## Implications for calibration

**Is a per-encoder AAC ladder viable?** Partially:

- **libfdk_aac: yes.** A public, source-level bitrate→bandwidth table (above) that's stable across versions (the header calls the values deliberately fixed). A ladder keyed on FDK's CBR/VBR-mode bandwidths is directly buildable and testable.
- **ffmpeg native `aac`: partially.** No public table, but the encoder is open source and deterministic — Phase 3 can build an empirical ladder by encoding reference material at target bitrates and measuring the actual cutoff, pinning the results as the calibration table. Buildable, but requires new measurement, not a source-table transcription.
- **Apple AAC: no fixed ladder — only an empirical, re-validated one.** No published spec exists, the qaac maintainer confirms none exists, and the closed-source encoder can change silently across OS/Core Audio versions. Treat as the least stable of the three, most likely to need re-calibration after an undocumented Apple change.
- **HE-AAC/SBR: no ladder makes sense in cliff-detection terms.** The fix is a **pre-classification gate**: if the stream signals SBR (AAC object type 5/29), skip cliff-based grading for that file entirely rather than run the existing 12-20 kHz LAME-shaped detector against it (a dedicated SBR-crossover estimator is out of scope for this calibration pass).

**Distinguishability**: FDK's CBR cutoffs (14-16 kHz mono, 15.5-17 kHz stereo, hard-capping at 17 kHz) sit close enough to LAME's 128-160 kbps window that cliff position alone can't separate "genuine FDK-AAC 128" from "MP3 128 transcoded to AAC" — some other signal (container/encoder tag, or a wide "AAC-plausible" band that simply never fires `"suspect"` across that whole range) will be needed. Apple's cutoffs are undocumented, so distinguishability there can only be established empirically in Phase 3.

## Predictions for Phase 3

Testable claims — encode X at bitrate Y with real reference material, then measure whether the cliff falls in the predicted range:

| Encoder | Bitrate | Predicted cliff/cutoff range | Confidence |
|---|---|---|---|
| libfdk_aac CBR | 96 kbps | ~16,000-17,000 Hz | High — from source table |
| libfdk_aac CBR | 128 kbps | ~17,000 Hz (capped, same as 96 kbps+) | High — from source table |
| libfdk_aac CBR | 192-320 kbps | ~17,000 Hz — **no increase over 128 kbps** | High — from source table; worth pinning as a regression test |
| libfdk_aac VBR mode 3 | ~96 kbps-class | ~15,750 Hz | Medium — table confirmed, bitrate mapping unverified |
| libfdk_aac VBR mode 5 | highest VBR | ~19,293 Hz | Medium — table confirmed, bitrate mapping unverified |
| ffmpeg native `aac` | 128 kbps | Likely 15,000-17,000 Hz per community consensus, **no table guarantee** | Low — no published table |
| ffmpeg native `aac` | 256-320 kbps | Likely closer to full-band (18-20 kHz) than FDK at the same rate — no hard 17 kHz cap | Low — inference from doc language, not measurement |
| Apple AAC CVBR | ~256 kbps (iTunes Plus/Apple Music) | Unknown; plausibly wider than FDK's 17 kHz cap given reputation, but no table to predict from | Very low — explicitly undocumented; must measure real files |
| Apple AAC | 128 kbps (legacy pre-2007 iTunes) | Unknown | Very low — same caveat |
| HE-AAC v1 | 64 kbps stereo | Core/SBR crossover ~7.5-9 kHz (below our 12 kHz slice floor); 12-20 kHz window is synthetic | Medium — one concrete example, SBR mechanics well documented |
| HE-AAC v2 | 24-32 kbps stereo | Crossover pushed lower still (likely <8 kHz); nearly all our slice window is synthetic | Low — mechanics-based inference, no numeric source at this exact bitrate |

Recommended Phase 3 minimum test matrix: encode the same reference album through (a) libfdk_aac CBR at 96/128/192/320, (b) ffmpeg native `aac` at the same four rates, (c) a real Apple Music or iTunes Plus purchase (cannot be synthesized — must be sourced from an actual Apple encode), and (d) libfdk_aac HE-AAC/HE-AACv2 at 48/64/96 — then run each through the existing `analyze_track` and record the actual `cliff_freq_hz` / `hf_deficit_db` against the predictions above.

## Sources

- [FFmpeg encoders.texi — `aac`/`libfdk_aac` `-cutoff` docs](https://github.com/FFmpeg/FFmpeg/blob/master/doc/encoders.texi)
- [FFmpeg wiki: Encode/AAC](https://mirror.hjertaas.com/trac.ffmpeg.org/trac.ffmpeg.org/wiki/Encode/AAC.html)
- [Hydrogenaudio: AAC encoders (overview/ranking)](https://wiki.hydrogenaudio.org/?title=AAC_encoders)
- [Hydrogenaudio: Libavcodec AAC (native ffmpeg encoder history)](https://wiki.hydrogenaudio.org/index.php?title=Libavcodec_AAC)
- [Hydrogenaudio: Fraunhofer FDK AAC](https://wiki.hydrogenaudio.org/index.php?title=Fraunhofer_FDK_AAC)
- [fdk-aac source: `aacenc_lib.h` (AACENC_BANDWIDTH docs)](https://github.com/mstorsjo/fdk-aac/blob/master/libAACenc/include/aacenc_lib.h)
- [fdk-aac source: `bandwidth.cpp` (bandWidthTable)](https://github.com/mstorsjo/fdk-aac/blob/master/libAACenc/src/bandwidth.cpp)
- [Hydrogenaudio: Apple AAC](https://wiki.hydrogenaudio.org/index.php?title=Apple_AAC)
- [Apple Developer: TN2271 (AAC encoder bitrate control strategies)](https://developer.apple.com/library/archive/technotes/tn2271/_index.html)
- [qaac issue #70 — default lowpass frequencies in Apple encoder (none published)](https://github.com/nu774/qaac/issues/70)
- [Archimago's Musings: Bluetooth AAC encoder comparison](http://archimago.blogspot.com/2023/08/part-ii-comparison-of-bluetooth.html)
- [Wikipedia: Spectral band replication](https://en.wikipedia.org/wiki/Spectral_band_replication)
- [Wikipedia: High-Efficiency Advanced Audio Coding](https://en.wikipedia.org/wiki/High-Efficiency_Advanced_Audio_Coding)
- [Hydrogenaudio: Spectral Band Replication](https://wiki.hydrogenaudio.org/index.php?title=Spectral_Band_Replication)
- [EBU Technical Review 305 — Moser, "MPEG-4 HE-AAC v2"](https://tech.ebu.ch/docs/techreview/trev_305-moser.pdf)
- [The Broadcast Bridge: High Efficiency Audio Codecs (HE-AAC)](https://www.thebroadcastbridge.com/content/entry/21823/standards-high-efficiency-audio-codecs-he-aac)
- [Macworld: iTunes Store goes DRM-free](https://www.macworld.com/article/194277/itunestore.html)
- [Apple Newsroom: Apple Launches iTunes Plus (2007)](https://www.apple.com/newsroom/2007/05/30Apple-Launches-iTunes-Plus/)
- [Gist: Converting audio to AAC with Fraunhofer FDK AAC in FFmpeg](https://gist.github.com/ScribbleGhost/54ad17da006e8bba4a1612bd6a64571c)
- [YouTube Music format IDs (itag 140/141 AAC bitrates)](https://gist.github.com/AgentOak/34d47c65b1d28829bb17c24c04a0096f)
- Analyzer reviewed for this doc: `lib/spectral_check.py` (`LAME_LOWPASS` lines 31-40, `detect_cliff`/`estimate_bitrate_from_cliff`/`classify_track`)
