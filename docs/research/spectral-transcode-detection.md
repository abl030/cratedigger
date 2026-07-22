# Spectral transcode-detection prior art (issue #829 Phase 0)

What distinguishes a NATIVE low-bitrate encode from a TRANSCODE
(lossy→lossy or lossy→lossless), beyond a bare frequency cliff. Written
2026-07-23. Every claim carries an inline source URL; anything
unverifiable is marked `[unverified]`.

## Summary

The "fake lossless" detection industry — from the 2004 `auCDtect` console
tool to 2026's browser checkers — converges on the same base signal
cratedigger already uses: a hard spectral cliff at a codec-characteristic
frequency (https://m.afterdawn.com/article.cfm/2004/04/06/aucdtect_tool_determines_the_authenticity_of_musical_cd_records,
https://www.getspectro.app/blog/what-is-fake-lossless-audio). Tools that
grow past that baseline add secondary signals — rolloff shape, time
consistency, encoder-family-specific artifacts, and cross-checks against a
codec's own known table — because the bare cliff alone has two documented
failure modes: it is blind to a same-codec transcode whose second pass
re-applies a correct lowpass for its claimed bitrate, and legitimate
mastering (vinyl rips, cassette transfers, quiet acoustic recordings, some
encoders' own filter quirks) can look like a lossy cliff
(https://www.getspectro.app/blog/what-is-fake-lossless-audio,
https://github.com/Guillain-RDCDE/FLAC_Detective). The forensic/academic
line (MDCT coefficient statistics, Benford's-law tests) goes further but
needs bitstream-internal access sox/ffmpeg's PCM-only output cannot give
(https://link.springer.com/article/10.1007/s12559-010-9045-4).

## What cratedigger does today

`lib/spectral_check.py` slices 12–20 kHz into 500 Hz sox `sinc`+`stat` RMS
bands, flags a cliff at ≥2 consecutive slices steeper than -12 dB/kHz, and
maps the cliff frequency through a hard-coded LAME lowpass table to an
estimated source bitrate; a secondary HF-deficit signal (1–4 kHz reference
minus the top 4 slices) grades tracks without a detected cliff too. Bare
cliff + LAME table — no artifact-shape analysis, no time-consistency
check, no cross-codec table, no distinction between a native low-bitrate
encode and a transcode landing on the same cliff.

## Tool-by-tool prior art

**Lossless Audio Checker** — AES Convention Paper 9416 (Lacroix, Prime,
Remy, Derrien, 2015): detects upscaling/upsampling/AAC transcoding in
lossless containers, validated at "100% success for upscaling and
transcoding, and 91.3% for upsampling"
(https://secure.aes.org/forum/pubs/conventions/?elib=17972). A 2019 AES
Journal follow-up (Derrien, JAES 67(3):116–123) detects AAC quantization
errors in the time-frequency domain without ML, reporting "null false
positive ratios and very low false negative ratios" on 1576 files
(https://www.researchgate.net/publication/331400801_Detection_of_Genuine_Lossless_Audio_Files_Application_to_the_MPEG-AAC_Codec,
https://hal.science/hal-02055742). Open-source GUI:
https://github.com/emps/Lossless-Audio-Checker-GUI.

**auCDtect / Tau Analyzer** — searches for "the frequency cut-off... at
about 18kHz" plus reduced dynamics
(https://m.afterdawn.com/article.cfm/2004/04/06/aucdtect_tool_determines_the_authenticity_of_musical_cd_records).
Vendor claims ">95%" accuracy, but independent Head-Fi testing found it
"detected about 1/10 MP3 to FLAC conversions" and false-positived on a
legitimate FLAC ("said MPEG with high confidence"), concluding "it doesn't
do what the AES paper claimed to do"
(https://www.head-fi.org/threads/fake-flac-identification.826737/).
Hydrogenaudio hosts several threads specifically about its unreliability
(https://hydrogenaudio.org/index.php/topic,27910.0.html,
https://hydrogenaudio.org/index.php/topic,20408.0.html,
https://hydrogenaudio.org/index.php/topic,60888.msg544600.html); modern
derivatives use it only as one signal among several
(https://alessandrocomito.github.io/audiofakedetectorpro/).

**Fakin' the Funk** — Windows DJ batch tool catching 128→320 kbps
upscales via bitrate/frequency-peak analysis
(https://fakinthefunk.net/en/); lineage traces to D'Alessandro & Shi, "MP3
Bit Rate Quality Detection Through Frequency Spectrum Analysis," ACM
MM&Sec '09, pp. 57–62
(https://djtechtools.com/2018/01/30/review-fakin-funk-low-bitrate-detection-utility/).

**Spek** — pure spectrogram visualizer, no verdict; FFTW-based, default
2048-pt FFT, switchable window/size — the substrate for classic
Hydrogenaudio-style manual inspection
(https://deepwiki.com/alexkay/spek/1.2-using-spek, https://www.spek.cc/).

**Modern browser tools** (brizm.dev, LosslessRadar, Fabl, Audio Fake
Detector PRO) operationalize the same lore. brizm's checker is the most
explicit public multi-signal write-up: 4096-pt FFT/Hann window, then a
"6-signal scoring system analyzing gradient sharpness, noise floor above
cutoff, spectral sparsity, cutoff variance, intensity stereo correlation,
and cutoff position against known codec signatures"
(https://brizm.dev/lossless-audio-checker/). Audio Fake Detector PRO scans
segments "via a GDI+ LockBits spectrogram for high-frequency cutoff
behaviour, roll-off ratio, peak energy, and anomaly flags," classifying
≤16.5 kHz as FAKE and 16.5–18.5 kHz as SUSPECT, with majority voting plus
auCDtect as an extra signal (https://alessandrocomito.github.io/audiofakedetectorpro/).

**FLAC_Detective** (Guillain-RDCDE) — open-source CLI/API/beets-plugin,
"11-rule spectral analysis plus an optional CNN classifier," grouped as
cutoff-frequency detection, MP3-bitrate signature matching, and
compression-artifact checks, plus protection rules "so genuine vinyl
rips, cassette transfers and naturally quiet recordings aren't flagged"
(https://github.com/Guillain-RDCDE/FLAC_Detective). Score bands: ≤30
authentic, 31–54 warning, 55–85 suspicious, ≥86 fake_certain; the optional
CNN rule raised specificity from 80% to 95% on 11,234 authentic FLACs per
the project's own case study (same URL). Per-rule thresholds live in a
linked technical-details doc this survey could not fetch
`[unverified — not retrievable via WebFetch]`.

**Forensic MDCT-coefficient line** — detects double compression (any
recompression, even same-format/same-bitrate) via quantized-MDCT
statistics: Benford's-law distributions (Yang, Shi, Huang — first such
work, SVM) (https://digitalcommons.njit.edu/fac_pubs/6316/); a 2015
"Difference of Calibration Histogram" method tested on ~13,830 files
(https://link.springer.com/article/10.1007/s11042-015-2758-3); Bianchi et
al.'s method, comparing against a *simulated* singly-compressed reference
derived from the file itself, which can localize which portion was
recompressed
(https://jis-eurasipjournals.springeropen.com/articles/10.1186/1687-417X-2014-10);
and an AAC scale-factor-difference extension
(https://www.researchgate.net/publication/326140876_AAC_Double_Compression_Audio_Detection_Algorithm_Based_on_the_Difference_of_Scale_Factor).
All need quantized transform coefficients from inside the bitstream — not
exposed by sox/ffmpeg's PCM output — so these are forensic-grade, not
deployable with our tool stack.

**ML/CNN line** — Seichter et al.'s CNN for AAC bitrate detection
(https://www.researchgate.net/publication/303590383_AAC_encoding_detection_and_bitrate_estimation_using_a_convolutional_neural_network);
Deezer's Hennequin et al. (ICASSP), codec-independent, observing "lossy
compression leaves traces in the spectrogram... namely holes, band
frequency cuts, and clusters," 92.4% accuracy in a follow-up
(https://research.deezer.com/publication/2017/04/05/icassp-hennequin.html,
https://ieeexplore.ieee.org/iel7/7943262/7951776/07952251.pdf). Koops et
al. 2024 found naive training "obtain[s] near perfect... test set
[performance], but severely degraded performance on... codec parameters
not seen in training" because models "rel[y] solely on the training set's
codec cutoff frequency," fixed via random spectrogram masking
(https://arxiv.org/pdf/2407.21545). This warns directly against a
hand-tuned table that only covers LAME's bitrate/lowpass pairs.

## The fingerprint catalogue

What it looks like / cause / sox-ffmpeg computability, for each signature.

1. **Bare frequency cliff** (cratedigger today) — steep dB/kHz drop at a
   codec-characteristic frequency, from the encoder's lowpass or
   psychoacoustic model discarding inaudible HF content
   (https://wiki.hydrogenaudio.org/index.php?title=Spectrogram). Already
   implemented via `detect_cliff`.

2. **sfb21 "spray"/"bloat"** — noise-like smear just above 16 kHz, LAME
   MP3 `-V0`–`-V2` without `-Y`/lowpass, because MP3 has no scale-factor
   band above ~16 kHz, forcing inefficient bit spend there
   (https://wiki.themixingbowl.org/LAME, https://news.ycombinator.com/item?id=37850250).
   Computable: finer slices (<500 Hz) across 15.5–17 kHz looking for a
   local energy *bump* vs. neighbors, not a monotonic rolloff.

3. **Double lowpass / stacked shelves** — two distinct cliffs: a lower one
   from an original lossy encode, a higher one from a second pass. Not a
   formally-named artifact in the sources surveyed, but the direct
   consequence of the "holes, band frequency cuts, and clusters" Deezer's
   paper describes for cascaded compression
   (https://research.deezer.com/publication/2017/04/05/icassp-hennequin.html)
   `[cascade framing verified via that paper; the "double lowpass" label
   itself is unverified against a primary source]`. Computable: extend
   `detect_cliff` over the full spectrum and report every cliff found.

4. **SBR mirror/replication artifact (HE-AAC)** — high frequencies
   transposed up from low/mid bands via a QMF bank rather than coded
   directly (https://en.wikipedia.org/wiki/Spectral_band_replication),
   producing a textured band, softer/gradual at higher bitrates rather
   than a flat cliff
   (https://www.getspectro.app/blog/how-to-detect-fake-lossless)
   `[perceptual "watery" framing unverified against a primary source]`.
   NOT cheap with sox `stat` RMS bands — needs cross-correlation between
   the low band and transposed high band (real FFT-bin data). Research-
   grade, not near-term deployable.

5. **Spectral holes/clusters above the shelf** — localized dropouts
   rather than a uniformly flat floor, per Deezer's own description of
   what its CNN keys on
   (https://research.deezer.com/publication/2017/04/05/icassp-hennequin.html).
   Computable as variance/non-monotonicity across top slices instead of
   collapsing to one average (as `avg_hf_db` does today).

6. **Noise-floor character above cutoff** — true digital silence (flat,
   near-floor) means "encoder cut it, nothing there"; a low structured
   noise/dither tail means a genuine recording
   (https://brizm.dev/lossless-audio-checker/). Computable via variance
   across the topmost 2–3 slices, not just their mean — a full flatness
   measure needs real FFT bins, which band-summed RMS collapses away.

7. **Cutoff consistency across time** — an encoder's lowpass is
   time-invariant; a natural rolloff shifts slightly with the material
   (https://brizm.dev/lossless-audio-checker/). **Cratedigger's single
   30-second trim can't see this** — needs the cliff/HF-deficit computed
   on multiple windows across the track and compared for invariance.

8. **Intensity-stereo / joint-stereo correlation** — joint-stereo coding
   forces mid/side correlation above a threshold, deviating from natural
   decorrelation (https://brizm.dev/lossless-audio-checker/). Computable
   via sox M/S `remix` + HF-band correlation; moderate added cost over
   the current mono-collapsing pipeline.

9. **Pre-echo / transient backward smear** — MDCT quantization noise
   spreads backward from a transient within a coding block, because
   transient energy spans many frequencies, forcing low-bit quantization
   whose noise smears across the block on decode
   (https://en.wikipedia.org/wiki/Pre-echo). NOT sox-alone computable —
   needs onset detection plus short-window, time-localized comparison; a
   different analysis shape than whole-track averaging.

10. **MDCT double-quantization statistics** (Benford's law, calibration
    histograms) — see the forensic line above
    (https://digitalcommons.njit.edu/fac_pubs/6316/). Needs quantized
    transform coefficients from inside the bitstream; sox/ffmpeg expose
    PCM only. Out of scope for a sox/ffmpeg-based Phase 3.

11. **Cutoff-vs-declared-codec mismatch** — the cliff sits where a
    *different* codec's/bitrate's table says it should, not the file's
    own claimed codec (e.g. AAC-256 that cliffs where MP3-128 cliffs) —
    the cascaded-compression tell: "an AAC file transcoded from an MP3
    source would typically show the MP3's original lower cutoff... rather
    than AAC's characteristic behavior"
    (https://research.deezer.com/publication/2017/04/05/icassp-hennequin.html,
    https://arxiv.org/pdf/2511.11527). Directly computable from
    cratedigger's existing `cliff_freq_hz` plus the file's real
    container/codec cross-checked against per-codec tables — cratedigger
    has only ONE table today (LAME's), so it can only detect
    "MP3-shaped," never "wrong-codec-shaped."

12. **Per-codec natural spectral envelope shape** — each codec's
    loudness-reduction-before-cliff shape differs: MP3-320/MP3-128
    "elevate the loudness of the entire frequency range before cutoff
    occurs at 18 kHz and 15 kHz respectively, while AAC CBR 256 kbps
    loudness reduction happens much earlier... around 7 kHz... AAC VBR
    level 5 loudness closely matches... FLAC" (https://arxiv.org/pdf/2511.11527).
    Computable with existing band-RMS machinery but needs per-codec
    reference curves, not one reference-band-vs-HF comparison applied
    uniformly — this IS "per-codec spectral calibration."

## Within-codec native-vs-transcode analysis

**Same codec, same nominal target** (native 128 kbps MP3 vs. a 96 kbps
MP3 transcoded up and re-tagged 128): if the transcoder re-applies a
correct lowpass for the claimed target, the cliff sits at the SAME
frequency either way. LAME's table is public and deterministic — a
SourceForge bug report's `lame.c` excerpt shows `{192,18600}`,
`{224,19400}`, `{256,19700}`, `{320,20500}`
(https://sourceforge.net/p/lame/bugs/492/), matching cratedigger's own
`LAME_LOWPASS` exactly for those four bitrates. So **the bare
cliff-position test is fundamentally blind to a well-executed same-codec
re-encode** — cratedigger cannot tell "native 128" from "96, cleanly
re-lowpassed to 128's cutoff." Distinguishing signals here are NOT cliff
position: a residual lower shelf if the transcoder's lowpass was looser
than ideal (#3); sfb21-style irregularities inconsistent with a genuine
single-generation encode (#2); and, at the forensic tier, MDCT coefficient
statistics that differ between single- and double-quantized signals even
at a matching nominal cutoff (https://digitalcommons.njit.edu/fac_pubs/6316/).
Time-invariance (#7) doesn't disambiguate this case either — both a
native and a clean re-encode have a time-invariant cliff.

**Cross-codec** (MP3-128 source transcoded into AAC-256): here the cliff
DOES betray the source, because AAC-256's natural shape doesn't cliff
sharply in the 15–17 kHz range the way MP3-128 does — its loudness
reduction starts earlier (~7 kHz) but gradually
(https://arxiv.org/pdf/2511.11527). An AAC-tagged file showing an
MP3-128-shaped hard cliff at ~17 kHz is a strong, cheap "wrong codec's
fingerprint" tell (#11) — the most reliable and least expensive
native-vs-transcode signal surveyed, but it needs more than one codec's
table, which cratedigger lacks today.

**Lossy → lossless** (cratedigger's actual importer scenario: verifying
an alleged FLAC is genuinely lossless) is the easiest case, and the one
the current cliff detector is already built for — genuine lossless has no
codec-imposed cliff at all, so any clean, time-invariant, table-matching
cliff is itself the red flag, with no "which codec's shape is this"
ambiguity to resolve.

## What's deployable for cratedigger — ranked shortlist

1. **Multi-window cliff-consistency check** (#7) — reuses existing sox
   band-RMS machinery entirely; run the same slicing over N windows
   across the track instead of one 30s trim, check invariance. Highest
   value-for-effort: no new sox invocation shape, just a loop + compare.
2. **Per-codec cutoff-and-shape table cross-check** (#11/#12) — the
   highest-leverage Phase-0 item and literally what "per-codec spectral
   calibration" means: build a table per codec (not only LAME) of
   cutoff-vs-bitrate AND pre-cliff loudness shape, cross-check the file's
   actual codec against ALL tables, flag cross-codec mismatches.
3. **Full-spectrum multi-cliff scan** (#3) — extend `detect_cliff` to the
   full spectrum, return every cliff found; answers "cascaded through two
   lossy passes," which the current single-cliff detector cannot
   represent even in principle.
4. **HF noise-floor variance, not just mean** (#6) — cheap addition:
   variance across the topmost 2–3 slices alongside their average, to
   separate a hard digital floor from a low structured noise tail.
5. **sfb21 bump detector** (#2) — narrow 15.5–17 kHz local-bump check vs.
   neighbors; closes a named false-negative gap (un-lowpassed LAME
   `-V0`–`-V2`) a pure-cliff approach misses by construction.
6. **Stereo mid/side correlation above cutoff** (#8) — moderate added
   cost (M/S remix step); useful secondary signal, lower priority than
   1–5.

**Not deployable near-term:** SBR-mirror cross-correlation (#4) and
pre-echo/transient smear (#9) need real FFT-bin or time-localized
analysis, not sox `stat` band-RMS; MDCT/Benford's-law statistics (#10)
need bitstream-internal access sox/ffmpeg can't provide. Record as a
follow-up research spike only if the Phase 3 calibration corpus shows
residual false negatives the deployable set can't explain — don't build
speculatively now.

## Sources

- https://secure.aes.org/forum/pubs/conventions/?elib=17972
- https://github.com/emps/Lossless-Audio-Checker-GUI
- https://www.researchgate.net/publication/331400801_Detection_of_Genuine_Lossless_Audio_Files_Application_to_the_MPEG-AAC_Codec
- https://hal.science/hal-02055742
- https://m.afterdawn.com/article.cfm/2004/04/06/aucdtect_tool_determines_the_authenticity_of_musical_cd_records
- https://www.head-fi.org/threads/fake-flac-identification.826737/
- https://hydrogenaudio.org/index.php/topic,27910.0.html
- https://hydrogenaudio.org/index.php/topic,20408.0.html
- https://hydrogenaudio.org/index.php/topic,60888.msg544600.html
- https://fakinthefunk.net/en/
- https://djtechtools.com/2018/01/30/review-fakin-funk-low-bitrate-detection-utility/
- https://deepwiki.com/alexkay/spek/1.2-using-spek
- https://www.spek.cc/
- https://brizm.dev/lossless-audio-checker/
- https://www.getspectro.app/blog/what-is-fake-lossless-audio
- https://www.getspectro.app/blog/how-to-detect-fake-lossless
- https://alessandrocomito.github.io/audiofakedetectorpro/
- https://github.com/Guillain-RDCDE/FLAC_Detective
- https://wiki.themixingbowl.org/LAME
- https://news.ycombinator.com/item?id=37850250
- https://wiki.hydrogenaudio.org/index.php?title=Spectrogram
- https://sourceforge.net/p/lame/bugs/492/
- https://en.wikipedia.org/wiki/Spectral_band_replication
- https://en.wikipedia.org/wiki/Pre-echo
- https://research.deezer.com/publication/2017/04/05/icassp-hennequin.html
- https://ieeexplore.ieee.org/iel7/7943262/7951776/07952251.pdf
- https://arxiv.org/pdf/2407.21545
- https://arxiv.org/pdf/2511.11527
- https://digitalcommons.njit.edu/fac_pubs/6316/
- https://link.springer.com/article/10.1007/s11042-015-2758-3
- https://jis-eurasipjournals.springeropen.com/articles/10.1186/1687-417X-2014-10
- https://www.researchgate.net/publication/326140876_AAC_Double_Compression_Audio_Detection_Algorithm_Based_on_the_Difference_of_Scale_Factor
- https://www.researchgate.net/publication/303590383_AAC_encoding_detection_and_bitrate_estimation_using_a_convolutional_neural_network
