# Spectral calibration — empirical findings (issue #829)

The discoveries of the per-codec spectral calibration campaign, compiled so a
future session can pick the work up cold. This is the empirical companion to
the six per-codec research docs in this directory (`spectral-mp3-lame.md`,
`spectral-aac.md`, `spectral-opus.md`, `spectral-vorbis.md`, `spectral-wma.md`,
`spectral-transcode-detection.md`): those record what the literature and
encoder sources predict; this records what 21,306 measured encodes actually
showed, and the detection model those measurements forced.

**Status**: current through the round-1 blind test and its rework
(2026-07-24). A round-2 blind validation is pending; update this document
with its results. Detailed chronology and per-phase records live on issue
#829; raw data and one-shot analysis scripts (uncommitted, per scope.md) live
under `/mnt/virtio/Music/calibration-tmp/` on shared storage.

## The evidence base

- **Ground truth**: 34 verified-lossless albums (~402 tracks) across a
  deliberately wide spectrum, including false-positive trap material:
  pre-1975 masters, early-digital recordings, lo-fi/shoegaze, ambient/drone,
  dense metal, loudness-war pop, full-spectrum electronic. Admission bar:
  every file ffprobe-verified FLAC + production analyzer grade `genuine` with
  zero cliffs + a 20–22 kHz ultrasonic audit (see "Ground-truth methodology"
  below). Four albums turned out to be genuine 96 kHz hi-res masters.
- **Encode matrix**: 49 variants per track (LAME CBR/VBR ladders, ffmpeg-native
  AAC, libfdk CBR/VBR/HE-AAC, Opus, Vorbis by quality level, a normalized
  16/44.1 FLAC control, six second-generation fraud shapes) plus 4 genuine
  Apple CoreAudio modes (qaac 2.89 / CoreAudioToolbox 7.10.9.0 on a dedicated
  Windows VM) — 21,306 files, all measured with the production primitives
  from `lib/spectral_check.py`, capturing grade, cliff Hz, estimate, HF
  deficit, reference dB, and the raw 16-slice dB vector per file.
- **Extension measurements**: four 20–22 kHz slices over the window-relevant
  variants; three additional 30-second windows (offsets 60/120/180 s) over the
  launder-relevant variants.

## Per-codec findings

### MP3 (LAME)

- Our `LAME_LOWPASS` table is a byte-exact transcription of LAME's own
  `optimum_bandwidth()` source array and has been stable since LAME 3.90.
  The MP3 calibration is faithful — **to LAME specifically** (see non-LAME
  below).
- **The detector reads one tier low, systematically.** `detect_cliff` reports
  the first slice of the steep run, ~500–1000 Hz below the encoder's actual
  lowpass. Measured cliff medians (detector space): CBR-96 → 14500, 128 →
  15500, 160 → 16500, 192 → 18000, 224/256 → 19000, 320 → 19500 (the last
  three only visible with the 20–22 kHz extension). Consequence: **bucket
  boundaries must be derived in detector space from measured medians, not
  from encoder specs** — through the spec-derived table, CBR-192 buckets as
  160 for 75% of tracks.
- **Window truncation bites earlier than the specs suggest**: CBR-224/256 are
  84–89% invisible in the production 12–20 kHz window (lowpass 19.4/19.7 kHz
  needs two steep slices inside the window), and CBR-64's 11 kHz lowpass sits
  *below* the window floor — no cliff, caught only by the deficit metric.
- **Cliff detection is material-dependent**: quiet/dark albums often produce
  no detectable cliff at low bitrates (nothing to cut). Assertion rates at
  128 kbps: ~89% of tracks on the training corpus, but far lower on quiet
  holdout material. Therefore: **a detected cliff supports a ±1-tier band
  assertion (92–100% within ±1); an absent cliff asserts nothing** — never
  "unbounded quality," simply no evidence.
- V-presets map into the same content classes for free: V2 measures
  identically to CBR-192 (~18000 Hz — which matches its real ~190 kbps
  content class), V4/V6 land in the 128–160 band, V0 is mostly cliff-free
  (unfiltered under the default `vbr_mtrh` engine).

### MP3 (non-LAME) — the encoder-identity hole

Xing/Helix applies a **fixed 16 kHz lowpass at any bitrate** — a genuine
Helix 320 reads as ~128-class through any LAME-shaped table. Shine applies no
lowpass at all (invisible to bandwidth analysis). Fraunhofer's behavior is
forum-anecdotal only. No Linux-packaged encoder exists to build a control arm
(Shine/Helix are not in nixpkgs); encoder identity is header-sniffable
(LAME/Xing info tags) and should be captured as evidence if this hole ever
needs closing. Fail direction is conservative (under-estimates quality).

### AAC — three encoders, three behaviors, one honest semantic

- **libfdk**: CBR bandwidth caps at ~17 kHz from 96 kbps/channel upward (its
  own source table) — measured: FDK 192/256/320 all cliff at exactly 16500 Hz
  median (91% of tracks) and read as "MP3 128" through the LAME table. FDK's
  cutoff is an *identity signature*, not a quality ladder.
- **ffmpeg-native aac**: a rising empirical ladder (96 → 15500, 128 → 17000,
  192 → 18250, 256/320 → mostly no cliff) **with a dynamic cutoff** — on
  dense/loud material the cutoff climbs ≥18 kHz. This dynamism matters twice:
  it makes the ladder probabilistic, and it creates a thin-evidence fraud
  class (see residuals).
- **Apple CoreAudio** (the dominant real-world source): publishes no cutoff
  table (confirmed by the qaac maintainer). Measured: legacy CBR-128 is
  visible and — coincidentally — correctly bucketed (median 16500 → est 128);
  ABR-192/TVBR-91 are 75% invisible; **CVBR-256 (iTunes Plus / Apple Music)
  is 98% no-cliff with a grade distribution statistically identical to the
  lossless controls, and carries real 20–22 kHz energy** — it is spectrally
  indistinguishable from lossless in every band we can measure.
- **The pooled floor table**: an AAC cliff anywhere in 13000–18000 Hz is
  consistent with encoder-rates from 96 to 320 kbps across all three
  families; only ≥18500 Hz lifts the floor to ~190-class. Therefore **AAC
  cliff evidence asserts a content floor, never a bitrate, and is never a
  transcode accusation** (cliffs are native AAC behavior). Below 13 kHz is
  junk-class.

### Opus — audit-only, proven

libopus reaches fullband (20 kHz) at ~12 kbps equivalent for stereo music;
every music-relevant bitrate selects identical bandwidth, and CELT's
band-energy preservation + spectral folding keeps *measured* energy in every
band regardless of actual coding precision. Measured: every bitrate ≥32 kbps
grades statistically identically to the lossless controls (band-RMS cannot
see Opus quality at all). The one real boundary (SWB→FB near ~12–16 kbps)
sits below music bitrates. **No spectral quality inference is possible for
Opus; audit-only.**

### Vorbis

Source-extracted quality ladder holds through q4 (detector medians: q0 →
14500, q2 → 16000, q3 → 17000, q4 → 18500, same one-tier detector bias as
LAME). **q5 — the Spotify Normal tier — cuts at 20.1 kHz, past the production
window**: 82% no-cliff without the extension, 50% visible with it. q6+ has no
encoder lowpass at all. Where the old LAME-shaped table did catch Vorbis
cliffs it over-estimated one-directionally (Vorbis keeps more top-end per
kbps than LAME; q4's real 128 kbps read as est-192). ffmpeg's `-q:a -1`
behaves like ~q3 (encoder-mapping artifact, not a ladder violation).

### HE-AAC (SBR)

**Worse than predicted: HE-AACv1 at 64 kbps reads as lossless** — 100%
no-cliff with control-like grades, because everything in the analysis window
is SBR-synthesized content sitting at plausible energy. HE-AACv2-32 is caught
by the deficit metric (91% suspect). The mandatory consequence: **detect SBR
via AAC object type (5/29) and exempt those files from cliff-based grading
entirely** — a pre-classification gate, not a calibration problem.

### WMA

Dropped from calibration permanently: no published cutoff table exists
anywhere, the encoder that made real-world WMA files (WMP9-era Microsoft) has
no Linux implementation, and ffmpeg's `wmav2` is a clean-room 1999-codec
reimplementation whose ladder would calibrate the wrong encoder. Audit-only
forever; a cliff on a supposedly-lossless file remains meaningful regardless
of codec.

## The HF-deficit metric

Control (genuine lossless) deficit distribution: p50 = 48 dB, p95 = 65,
p99 = 69, max = 78. The legacy thresholds (marginal 40 / suspect 60) flag the
*median* genuine track as marginal and 14% as suspect — the trap albums did
exactly their job (Bee Thousand 20/20 tracks non-genuine at up to 78 dB, all
genuine). Re-derived thresholds: **marginal 65 / suspect 69** → 5.5%/1.5%
track-level false positives, ≈0 album-level after the ≥60% aggregation, while
retaining 58–79% of the real deficit-only catches (CBR-64, HE-AACv2-32,
AAC-96, Apple-128). The metric's honest role is narrow: the backstop for
sub-window junk with no visible cliff — and it is one of the two legs that
expose AAC→MP3 launders (the cliff leg alone exposes only 6/34 albums there;
deficit raises it to 31/34).

## Ceiling detection and the launder problem

The blind spot the campaign existed to close: fake FLACs made from codecs the
window can't see (opus→flac, vorbis-q5→flac were **completely invisible** to
the production detector). Findings, in the order they were forced:

1. **Absolute ultrasonic thresholds do not work.** sox's sinc band filters
   leak ~35–60 dB of apparent energy from massive sub-20 kHz content into the
   20–22 kHz bands, so a hard codec ceiling never reads as silence relative
   to the 1–4 kHz reference. Every usable form is *relative/local*: a step
   across the ceiling boundary, not a level below a floor.
2. **The working detector is album-level, not track-level.** Per-track
   ceiling steps drown in material variance (quiet tracks have nothing to
   step from). Averaging the album's spectral profile first, then measuring
   one sliding step (max drop across any 19–21.5 kHz boundary) amplifies the
   encoder-constant ceiling and washes out material noise. Companion signal:
   ceiling-position consistency across tracks (encoder ceilings are
   position-constant; natural rolloffs scatter).
3. **Multiple windows, union aggregation.** One 30-second trim undersamples
   quiet-intro material. Evaluating the album-profile legs on each window
   (offsets 0/60/120/180 s) and denying on any trip closed most of the
   remaining gap. Best-window selection (rather than union) was a wash.
4. **Natural near-Nyquist rolloff is real**: with the 20–22 kHz extension,
   10% of genuine control tracks show "cliffs" at ~20 kHz. Consequence —
   **split semantics**: high-band cliffs (≥ ~19250 Hz) inform upward tier
   discrimination only and never reject; launder detection for lossless
   containers uses the ceiling-step/consistency/affirmative machinery above.
5. **The fundamental ambiguity — mastering ceilings.** Six of 34 genuine
   control albums carry real spectral ceilings from their recording chains:
   Gould's 1982 Goldberg Variations (early all-digital ADC), Kind of Blue's
   1987 CD master, Tabula Rasa (ECM 1984), Loveless, Feed Me Weird Things,
   and a 1991 cassette-sourced master. A mastering-chain lowpass is
   *physically identical* to a codec ceiling; no spectral feature will ever
   split them. This forced the proof-semantics change below.

**Training-final gate performance** (union-over-windows: in-window-cliff leg,
ceiling leg step ≥15 dB with position-consistency ≥0.5, affirmative-content
leg inband−ultrasonic ≤35 dB): opus→flac denied 34/34, mp3-128→flac 33/34,
vorbis-q5→flac 33/34; genuine controls denied 6/34 — every one an
early-digital/band-limited master, i.e. denials that are *factually correct*
statements about the recordings.

## Proof semantics (operator-approved)

The ceiling ambiguity means a flag cannot mean "fake — reject." It means
"full-band lossless provenance is not spectrally provable." Approved model,
implemented as a stricter promotion bar on the **existing**
provisional-lossless → verified-lossless transition (no new states):

> Promotion to verified lossless requires: (a) no in-window transcode cliff,
> (b) no album-level ceiling flag, and (c) **affirmative ultrasonic content**
> — the burden flips from "nothing suspicious found" to "positive evidence of
> full-band content." A failed leg never rejects, denylists, or accuses: the
> album imports normally, stays provisional, and surfaces in triage.

Under these semantics the fraud bar becomes "zero fraud albums *receive
proof*" — achievable, and honest even on the false side: a 1982 digital
master genuinely has no spectral proof of full-band content.

## Documented residual classes (accepted, spectrally unsolvable)

1. **Apple CVBR-256-sourced launders** — real ultrasonic energy, no cliff, no
   ceiling; indistinguishable from lossless in every measured band. Lowest
   perceptual severity of any fraud class. (No cheap slice-shape discriminator
   exists: distributions overlap the controls almost completely.)
2. **Quiet / no-HF material** (Grouper class) — albums with near-zero HF
   content anywhere provide no spectral evidence in either direction;
   launders of them are undetectable and near-lossless by construction.
   Automated exemption formulas for this class misfire; classification is
   analyst work.
3. **Dynamic-cutoff AAC→MP3 launders on dense material** — when ffmpeg-AAC's
   cutoff climbs ≥18 kHz, the transcode retains near-full bandwidth and both
   the band and deficit legs read thin.

## Fraud-shape detectability (training corpus, album level)

| Fraud shape | Detected by | Rate |
|---|---|---|
| mp3-128 → FLAC | in-window cliff | 33/34 |
| mp3-128 → AAC-256 | in-window cliff (source betrayed) | ~34/34 |
| mp3-192 → mp3-320 (same-codec) | surviving source cliff | 34/34 |
| aac-128 → mp3-320 | deficit + band legs combined | 31/34 |
| opus-96 → FLAC | ceiling step + affirmative content | 34/34 |
| vorbis-q5 → FLAC | ceiling/affirmative/cliff union | 33/34 |
| apple-256 → FLAC (inferred) | — | residual |

## Ground-truth methodology findings

- **The verified-lossless admission bar works**: it caught a real fake at
  corpus admission (a "FLAC" mixtape with 27/29 tracks cliffing at 17.5 kHz —
  lossy-native, upscaled by someone upstream).
- **The ultrasonic audit closes the circularity risk**: ground truth admitted
  under the *old* detector could in principle contain the launder classes the
  old detector can't see — but every such class leaves a ≤20.1 kHz ceiling,
  and 0/34 albums showed one. "Carries real content above 20 kHz" is the
  property calibration needs, and it held corpus-wide.
- **`validate_audio`'s rc=0 gap** (issue #835): ffmpeg's default error
  resilience conceals recoverable frame corruption behind a zero exit code —
  a corrupt FLAC passed the production audio gate and imported; `flac -t`
  fails the same file. One corpus track was excluded for this; the prod
  library likely holds more of the class.
- **Real-world sample rates**: 4 of 34 wild-sourced "CD" albums were genuine
  96 kHz hi-res masters. Matrix encoding normalized everything to 16/44.1.

## What round 2 must confirm (pending)

The round-1 blind test failed the original single-window/absolute-threshold
launder arm (its constants had never been checked against training frauds —
the out-of-sample test did its job) and drove items 1–3 of the ceiling
findings. The reworked gate above is training-qualified only. Round 2: a
fresh blind cohort (25 albums seeded, acquiring), scored once by the frozen
v2.1 scorer; results belong in this document, especially (a) whether the
union gate's fraud denials generalize, (b) whether control denials stay
confined to the real-ceiling master class, and (c) the false-flag cost on
fresh genuine material.

## Data and reproduction

All under `/mnt/virtio/Music/calibration-tmp/` (temporary instance;
ownership-ordered teardown checklist in
`docs/plans/2026-07-22-001-feat-829-spectral-calibration-plan.md`):
`encodes*/manifest.tsv` (what was encoded), `measurements/results*.tsv`
(per-file grade/cliff/estimate/deficit/ref + 16-slice vectors),
`measurements/extended*.tsv` (20–22 kHz slices), `measurements/multiwin.tsv`
(offset windows), `measurements/*.py` (one-shot sweep/analysis/scoring
scripts, uncommitted by design), `versions.txt` (encoder provenance +
exclusions). The Phase 3 prediction scorecard and per-phase records are
comments on issue #829.
