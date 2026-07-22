# Spectral calibration research: WMA (Windows Media Audio)

Part of issue #829 Phase 0. Scope: assess whether Windows Media Audio
(Standard / Pro / Lossless / Voice) can be given a calibrated cutoff→bitrate
mapping analogous to `lib/spectral_check.py::LAME_LOWPASS`, and whether it
belongs in the Phase 2 encode matrix at all.

**Bottom line up front: WMA should be DROPPED from the Phase 2 encode
matrix.** See "Recommendation" below for full reasoning; short version: the
only Linux-available encoder (`ffmpeg`'s `wmav1`/`wmav2`) is not the encoder
that produced real WMA files in the wild, so any matrix built would
calibrate the wrong thing, and the affected population is small and shrinking.

## How our analyzer works (context)

`lib/spectral_check.py` measures RMS energy in 500 Hz slices from 12 kHz to
20 kHz, looks for a steep multi-slice dB/kHz drop (`detect_cliff`,
`CLIFF_THRESHOLD_DB_PER_KHZ = -12.0`), and maps the cliff frequency to an
estimated source bitrate via `LAME_LOWPASS` — a table taken from LAME's own
source code (comment: "from source code"), i.e. it encodes exactly one
encoder's internal lowpass-filter design decisions. WMA/M4A/ALAC files are
decoded once via `ffmpeg` to WAV before the same sox pipeline runs
(`_ffmpeg_to_wav`); there is no codec-aware branch — every file, regardless
of source codec, gets bucketed through the LAME table. This is exactly the
cross-codec miscalibration issue #829 exists to fix.

## WMA family taxonomy

Windows Media Audio is Microsoft's umbrella name for several unrelated
codecs sharing the ASF container and a `wma` extension convention:

| Variant | Type | Introduced | Notes |
|---|---|---|---|
| WMA (v1) | lossy | Aug 1999 | First release ([Wikipedia](https://en.wikipedia.org/wiki/Windows_Media_Audio)) |
| WMA (v2) | lossy | 1999 | Minor bitstream syntax changes over v1 ([Wikipedia](https://en.wikipedia.org/wiki/Windows_Media_Audio)) |
| WMA 7 / 8 / **9 ("WMA 9 Standard")** | lossy | 2000 / 2001 / 2003 | Same family, iterative encoder-only improvements; v9.1/9.2/10 stay bitstream-compatible with v9 decoders ([Wikipedia](https://en.wikipedia.org/wiki/Windows_Media_Audio)) |
| WMA Pro (9 Pro / 10 Pro) | lossy | 2003 | Multichannel (up to 7.1), up to 24-bit/96kHz, improved entropy coding ([Wikipedia](https://en.wikipedia.org/wiki/Windows_Media_Audio)) |
| WMA Lossless | **lossless** | 2003 | Unpublished bitstream spec, reverse-engineered; compression ratio ~1.7:1–3:1 ([Wikipedia](https://en.wikipedia.org/wiki/Windows_Media_Audio); [Hydrogenaudio](https://wiki.hydrogenaudio.org/index.php?title=Windows_Media_Audio)) |
| WMA Voice | lossy (speech) | 2003 | Mono, ≤22.05 kHz, CBR only, ≤20 kbit/s, speech/music auto-switching ([Wikipedia](https://en.wikipedia.org/wiki/Windows_Media_Audio)) |

"WMA 9 Standard" in period ripper UIs (Windows Media Player 9/10,
~2003–2008) refers to the plain lossy WMA v9 codec — almost certainly the
variant behind most legacy WMA files an archivist would encounter, since
WMP defaulted to it for CD ripping.

Codec internals, relevant to why cliff-detection may not transfer cleanly
from LAME: WMA Standard is an MDCT transform codec like MP3/AAC/Vorbis, but
uses **5 block sizes** (128–2048 samples) versus MP3/AAC's 2, does
mid/side stereo coding, and below ~17 kbit/s substitutes line-spectral-pair
coding and below ~33 kbit/s substitutes noise coding for parts of the
spectrum instead of a plain filter cutoff ([Wikipedia](https://en.wikipedia.org/wiki/Windows_Media_Audio)). That is a materially
different HF-discard mechanism than LAME's fixed per-bitrate lowpass filter
— there is no a priori reason WMA's rolloff shape should resemble LAME's.

Microsoft's own release claim was "near-CD-quality at 64 kbit/s" / "MP3
quality at half the bitrate" — independent listening tests debunked this
for WMA Standard specifically ([Hydrogenaudio](https://wiki.hydrogenaudio.org/index.php?title=Windows_Media_Audio)). WMA Pro, by contrast,
"scored as statistically tied with top encoders" in blind tests
([Hydrogenaudio](https://wiki.hydrogenaudio.org/index.php?title=Windows_Media_Audio)) — but WMA Pro is essentially absent from consumer
rips (see Prevalence).

## Cutoff behavior per variant/bitrate — best-effort table

**This is the thin part of the doc, as expected.** Unlike LAME (a public,
source-derived lowpass table) or AAC (Hydrogenaudio-maintained encoder
comparison pages), there is no equivalent published bitrate→cutoff-frequency
table for WMA anywhere we could find — not Hydrogenaudio's wiki, not
Microsoft's developer docs, not forum lore. Repeated targeted searches
(Hydrogenaudio wiki/forum, Microsoft Learn, empegBBS, AnandTech, multimedia.cx)
turned up bitrate *ranges* and *marketing claims* but no third-party spectral
measurement of WMA Standard's actual rolloff frequency at specific bitrates.

| Bitrate | Claimed/observed behavior | Confidence |
|---|---|---|
| 64 kbit/s | Microsoft called this "CD quality"; forum listeners at the time called that false and reported "too many artifacts," judging 96 kbit/s the practical floor ([empegBBS](https://empegbbs.com/ubbthreads.php/ubb/printthread/Board/1/main/5778/type/thread)) | Anecdotal only, no measured cutoff Hz |
| 96–128 kbit/s | No documented cutoff frequency found. Generic lossy-codec forum lore treats ~16 kHz as a rule-of-thumb cutoff "at 128kbps" but every citable instance of that figure is about MP3/LAME, not WMA — sources conflate the two by analogy, not measurement ([forum synthesis, unverified for WMA specifically]) | [unverified] |
| 160–192 kbit/s | No documented cutoff frequency found | [unverified] |
| WMA Pro (any rate) | No cutoff data found; low-bitrate WMA Pro mode exists down to ~48 kbit/s for multichannel content (NBC Olympics example) but is not a consumer CD-rip shape ([Wikipedia](https://en.wikipedia.org/wiki/Windows_Media_Audio)) | [unverified] |
| WMA Lossless | Not applicable — lossless, no cutoff (bit-exact reconstruction is the claim) ([Wikipedia](https://en.wikipedia.org/wiki/Windows_Media_Audio_9_Lossless) via search synthesis) | N/A |
| WMA Voice | Band-limited to speech range by design (low-pass + high-pass filtering outside speech band) — a real cutoff exists but is a speech-bandwidth artifact, not a bitrate-quality signal, and this variant essentially never appears as a music-file rip ([Wikipedia](https://en.wikipedia.org/wiki/Windows_Media_Audio)) | Design-documented, not spectrally measured by us |

We could not find a single hydrogenaudio-wiki-style page, Microsoft
white paper, or forum spectrogram thread that pins an actual measured
rolloff frequency to a specific WMA Standard bitrate the way the LAME page
does for MP3. This absence is itself the key finding of this section.

## What ffmpeg can encode, and how representative it is

- `ffmpeg` ships **native encoders for `wmav1` and `wmav2`** (WMA v1/v2,
  i.e. the original 1999-era codec, not "WMA 9"), confirmed via FFmpeg's own
  source/doxygen (`libavcodec/wmadec.c`, `wma.h`: "1 = 0x160 (WMAV1), 2 =
  0x161 (WMAV2)") and mailing-list `ffmpeg -codecs` dumps showing both as
  encode+decode capable ([FFmpeg doxygen](https://ffmpeg.org/doxygen/0.6/wmadec_8c.html); [ffmpeg-user mailing list](https://ffmpeg.org/pipermail/ffmpeg-user/2013-August/016651.html)).
- **No WMA Pro or WMA Lossless encoder exists in ffmpeg, or anywhere on
  Linux** — decode only for both. fre:ac's own docs state Linux builds
  can't produce WMA Lossless output because "it doesn't look like any WMA
  Lossless encoder exists besides the Microsoft one included with Windows"
  ([SourceForge fre:ac thread](https://sourceforge.net/p/bonkenc/discussion/85470/thread/fe31a0ff4f/); [ffmpeg WMA decoder history](https://news.slashdot.org/story/02/10/28/1331251/ffmpeg-free-softwares-wma-decoder)).
- **`wmav1`/`wmav2` quality reputation is poor.** FFmpeg's own encoding-guide
  content (mirrored, since the canonical `trac.ffmpeg.org` page is behind
  bot-protection at fetch time) ranks encoders "high to low" as `libopus >
  libvorbis >= libfdk_aac > aac > libmp3lame >= eac3/ac3 > libtwolame >
  vorbis > mp2 > wmav2/wmav1` and states plainly that "the wmav1/wmav2
  encoder does not reach transparency at any bitrate" ([mirror: williamyaps](https://williamyaps.blogspot.com/2017/02/ffmpeg-quality-compare.html); [mirror: evilsoup](https://evilsoup.wordpress.com/2013/02/10/general-ffmpeg-encoding-guide-2/)).
- **The representativeness gap is the load-bearing fact for this doc.**
  ffmpeg's `wmav2` is a clean-room reimplementation of the old WMA v1/v2
  bitstream — not Microsoft's WMA 9 encoder (the one bundled with WMP 9/10
  that produced the vast majority of real "WMA 9 Standard" rips), and
  definitely not WMA Pro/Lossless (neither encodable on Linux at all). Any
  Phase-2-style ladder built on doc2/Linux would characterize **ffmpeg's own
  encoder's HF-discard behavior**, not the Microsoft encoder that produced
  the population this project models — a different failure mode from the
  AAC/Opus docs in this series, where ffmpeg's native/libfdk encoders are at
  least *real, shipping* implementations of the target format.

## Prevalence in the wild

- WMA (specifically WMA Standard v9, "WMA 9 Standard" in WMP's UI) was
  Windows Media Player's **default rip format for years in the 2000s**,
  common among casual Windows users ripping CDs with the out-of-the-box
  tool rather than a dedicated MP3 encoder ([search synthesis, AnandTech-era discussion](https://forums.anandtech.com/threads/whats-the-deal-with-wma-files-these-days.1296482/post-11225827)).
- Contemporary file-sharing-scene sentiment treated WMA as the mark of a
  casual/non-audiophile ripper: a period AnandTech thread is quoted as
  "Default format for Media Centre rips, so any noobs posting songs often
  do so in WMA" ([AnandTech forum](https://forums.anandtech.com/threads/whats-the-deal-with-wma-files-these-days.1296482/post-11225827)). Dedicated "scene" traders on
  Napster/Kazaa/LimeWire/Soulseek-era networks skewed MP3 (universal
  compatibility, non-Windows-only container) and, later, lossless — WMA's
  ASF container was itself a compatibility tax outside Windows. We could not
  find any source quantifying WMA's share of Soulseek traffic specifically,
  in 2026 or any other year — **[unverified]**, an inference from cultural
  context, not a measurement.
- Practical implication: any WMA files this archive encounters are almost
  certainly legacy WMP rips from ~2000–2008 at 64–192 kbit/s CBR/VBR,
  essentially never WMA Pro (near-zero consumer software wrote it) and
  never WMA Lossless in meaningful volume (niche, Windows-only, and itself
  hard to decode correctly — "several releases of Windows 10 had faulty
  decoders" per Hydrogenaudio ([Hydrogenaudio](https://wiki.hydrogenaudio.org/index.php?title=Windows_Media_Audio))). The population is old, finite,
  and can only shrink as it's replaced by better-sourced re-rips —
  consistent with cratedigger's own upgrade mandate.

## Implications for calibration

1. **No ground-truth reference exists to calibrate against even in
   principle**, absent building one from real WMA9-encoded material — and
   the only "encoder" Linux can drive (`ffmpeg wmav2`) is not that material's
   producer (see above). A matrix built by encoding our ground-truth FLACs
   through `ffmpeg wmav2` would tell us about `wmav2`, not about "WMA 9
   Standard," so it would not answer the calibration question issue #829
   asks.
2. **The codec's internal HF-discard mechanism differs structurally from
   LAME's.** WMA Standard trades spectral resolution via block-size
   switching, LSP coding below ~17 kbit/s, and noise substitution below
   ~33 kbit/s, rather than a single fixed lowpass filter per bitrate tier
   ([Wikipedia](https://en.wikipedia.org/wiki/Windows_Media_Audio)). There is no structural reason to expect a clean,
   LAME-shaped single-frequency cliff at all, let alone one that lines up
   with LAME's specific frequency thresholds.
3. **Would a real WMP9-produced WMA9 128 kbit/s file be misread by our
   `LAME_LOWPASS` table today?** Qualitatively: almost certainly yes — the
   table's boundaries are LAME's internal filter choices, not a validated
   cross-codec mapping; any agreement would be coincidental. We cannot say
   *how wrong*, or in which direction, without measuring real Microsoft-
   encoder WMA files, a different acquisition problem than the
   ffmpeg-encode-a-ladder approach Phase 2 uses for every other codec here
   ([unverified] — no source gives measured WMA cutoff frequencies to
   compare against the LAME table's 17000 Hz→128 boundary). Net: the
   achievable outcome for WMA is not "a calibrated table with confidence
   bounds" (the Phase 2/3 goal elsewhere) but "a documented decision to
   treat WMA as non-decision-grade" — already the taxonomy's own fallback
   for "lossy non-MP3".

## Recommendation: drop WMA from the Phase 2 encode matrix

**Drop it.** Reasoning, weighted by cost vs. expected value:

- The corpus/encoder mismatch means a Phase 2 WMA ladder would calibrate the
  wrong encoder. Spending budget on it produces a table that doesn't
  describe the files it's meant to protect against — worse than doing
  nothing, since a wrong-but-present table looks authoritative later.
- WMA Pro and WMA Lossless can't be encoded on Linux at all, so at most 1 of
  4 lossy variants (plain WMA Standard) could even be attempted, and that
  attempt is the mismatched one above.
- Prevalence is low, skewed toward the population cratedigger already wants
  to upgrade away from (legacy 64–192 kbit/s WMP rips), and can only shrink
  over time — unlike AAC/Opus/Vorbis, live formats worth calibrating well.
- The issue text itself hedges this codec uniquely — "WMA if ffmpeg encodes
  it sanely" — the only conditional phrasing in the Phase 2 codec list.
- Dropping WMA creates no gap: per the three-domain taxonomy, "lossy
  non-MP3" (WMA included) already defaults to audit-only/fail-closed
  absent a validated table. WMA just stays in that bucket permanently
  instead of graduating out — the honest state of our knowledge.

**What "drop" should mean for Phase 4/5:** treat WMA (all variants) as a
codec where spectral evidence is measured (cliff detection still runs — a
genuine near-DC-to-0dB cliff is still evidence of something, e.g. a
lossless-container fake) but is **not decision-grade for bitrate estimation
or transcode verdicts** — no `LAME_LOWPASS`-style bucket, no
`likely_transcode` stamp derived from a WMA cliff. This is exactly the
audit-only/fail-closed treatment #829 already specifies for uncalibrated
lossy-non-MP3 codecs; WMA needs no bespoke code path beyond correct
classification into that treatment.

Confidence: **high** for "don't build a matrix via ffmpeg" (the
encoder-mismatch argument is structural, not probabilistic); **medium** for
the prevalence argument (no hard Soulseek telemetry exists — leans on
cultural/historical inference).

## Predictions for Phase 3 (if the operator overrides this and includes WMA anyway)

If a future decision reverses this and an `ffmpeg wmav2` ladder gets built
anyway, predictions worth pinning now so Phase 3 can check them:

1. **The cliff detector will behave less cleanly on WMA than on LAME MP3.**
   Because WMA's HF discard spreads across block-size switching and noise
   substitution rather than one lowpass filter, expect more tracks to land
   in `marginal` (gradual HF deficit, no qualifying cliff) than
   `suspect`-via-cliff, especially at mid bitrates (128–192 kbit/s).
2. **Any cliff frequency measured from `ffmpeg`-encoded WMA describes
   `wmav2`'s filter design, not WMA9's** — should NOT be merged into or
   compared against real-world WMA measurements without that caveat.
3. **Low bitrates (64–96 kbit/s) are where `wmav2`'s "never reaches
   transparency" reputation should show up most visibly** — expect early,
   broad-band HF loss rather than a clean cliff, possibly triggering
   suspect grades via `HF_DEFICIT_SUSPECT` (60 dB) rather than `detect_cliff`.
4. **WMA Pro/Lossless cannot be added to any matrix on Linux** — a hard
   tooling constraint, not a probabilistic guess.
5. If real WMP9 files are later sourced for genuine ground truth (e.g.
   pulled live off Soulseek rather than re-encoded locally), expect the
   true WMA9 curve to sit close to, but not exactly on, the LAME curve at
   mid bitrates (contemporary tests rated WMA roughly comparable to
   LAME-era MP3) — and diverge more at low bitrates where WMA's
   LSP/noise-substitution mechanisms have no LAME analogue.

## Sources

- https://wiki.hydrogenaudio.org/index.php?title=Windows_Media_Audio
- https://en.wikipedia.org/wiki/Windows_Media_Audio
- https://en.wikipedia.org/wiki/Windows_Media_Audio_9_Lossless
- https://wiki.multimedia.cx/index.php/Windows_Media_Audio_9
- https://ffmpeg.org/doxygen/0.6/wmadec_8c.html
- https://ffmpeg.org/doxygen/1.2/wma_8h_source.html
- https://ffmpeg.org/pipermail/ffmpeg-user/2013-August/016651.html
- https://williamyaps.blogspot.com/2017/02/ffmpeg-quality-compare.html
- https://evilsoup.wordpress.com/2013/02/10/general-ffmpeg-encoding-guide-2/
- https://sourceforge.net/p/bonkenc/discussion/85470/thread/fe31a0ff4f/
- https://news.slashdot.org/story/02/10/28/1331251/ffmpeg-free-softwares-wma-decoder
- https://empegbbs.com/ubbthreads.php/ubb/printthread/Board/1/main/5778/type/thread
- https://forums.anandtech.com/threads/whats-the-deal-with-wma-files-these-days.1296482/post-11225827
- https://www.nch.com.au/kb/10098.html
- https://listening-tests.freetzi.com/html/Multiformat_128kbps_public_listening_test_results.htm
- GitHub issue #829 (this repo) — problem statement and Phase 2 plan text, quoted for context on the operator's own hedge language ("WMA if ffmpeg encodes it sanely")
