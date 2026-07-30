# Spectral calibration — empirical findings (issue #829)

The discoveries of the per-codec spectral calibration campaign, compiled so a
future session can pick the work up cold. This is the empirical companion to
the six per-codec research docs in this directory (`spectral-mp3-lame.md`,
`spectral-aac.md`, `spectral-opus.md`, `spectral-vorbis.md`, `spectral-wma.md`,
`spectral-transcode-detection.md`): those record what the literature and
encoder sources predict; this records what **60,102 measurements** over real
encodes actually showed, and the detection model they forced.

**Status**: the research campaign is **COMPLETE and four-arm validated**
(concluded 2026-07-27; issue #829 itself stays open for Phase 5
implementation). Every codec family has been measured on four independent album
cohorts — TRAINING (34 albums), ROUND-1 (15), ROUND-2 (27), ROUND-3 (24), 100
albums total. The verified-lossless proof gate (scorer v3) passed a fresh,
deliberately hostile blind cohort. The two remaining Phase 4 verification
sweeps were dropped by operator decision and implementation began.

**Implementation is a separate document.** The PR sequence, constants to code,
authority citations, and known residuals-to-ship-with live in
`docs/plans/2026-07-27-001-feat-829-phase5-implementation-plan.md`. Do not
duplicate that plan here; this file is the *evidence*. As of 2026-07-27,
Phase 5 PR1 (evidence primitive capture — migration 065, the additive extension
slices, `ultrasonic_deficit_db`) has shipped and PR2 onward has not.

Per-phase chronology lives on issue #829. Raw data and one-shot analysis
scripts (uncommitted, per `.claude/rules/scope.md`) live under
`/mnt/virtio/Music/calibration-tmp/` on shared storage.

---

## The evidence base

### The four arms

| arm | albums | tracks | encodes | role |
|---|---:|---:|---:|---|
| TRAINING | 34 | ~402 | 21,306 | every constant is derived here |
| ROUND-1 (spent) | 15 | — | 8,575 | first blind validation — FAILED |
| ROUND-2 (spent) | 27 | 337 | 16,513 | second blind validation — FAILED |
| ROUND-3 (spent) | 24 | 220 | 10,780 | third blind validation — **PASSED** |

Those four encode counts sum to 57,174. Adding **2,928 genuine Apple CoreAudio
encodes**, measured across the three holdout cohorts (700 / 1,348 / 880) so
that Apple — the last training-only codec family — became four-arm, gives the
final total: **100 albums, 60,102 production-primitive measurements.**

Separate from that total, the multi-window sweeps (offsets 60/120/180 s) live
in their own files. Round-1's was generated retroactively for the v3
qualification (2,100 measurements), because v2.1's window machinery post-dated
that round.

**Blind protocol.** Each holdout cohort was seeded, acquired, and sealed with
no measurement or classification until a scorer was frozen; it was then scored
exactly **once**. Blindness is single-use — round-1, round-2 and round-3
blindness are all spent, and any constant fitted to a spent arm is fitted to a
test set. Round 3's blind-set assembly (`build_holdout3_src.py`) carries an
automated contamination guard: any album slug already present in the training,
round-1 or round-2 measurement files is excluded by construction. This was
added because round 2 had to exclude two albums by hand (Daft Punk *RAM*,
Simon & Garfunkel *Bridge*) after discovering they were training members.

### Cohort composition matters more than cohort size

The single most important lesson about the evidence base: **the model's
failures are a function of how much high-frequency-poor material the cohort
contains**, and the arms differ enormously.

| arm | HF-poor share |
|---|---:|
| TRAINING (34) | ~6% |
| ROUND-2 (27) | 26% |
| ROUND-3 (24) | **46%** |

Round 3 was deliberately over-weighted toward the round-2 failure class
(quiet / ambient / solo-piano / drone), because a fix aimed at HF-poor material
cannot be validated on a cohort that barely contains any. Round-1's HF-poor
share is not stated in the sources.

### Ground truth

The TRAINING arm is 34 verified-lossless albums (~402 tracks) across a
deliberately wide spectrum, including false-positive trap material: pre-1975
masters, early-digital recordings, lo-fi/shoegaze, ambient/drone, dense metal,
loudness-war pop, full-spectrum electronic. Admission bar: every file
ffprobe-verified FLAC + production analyzer grade `genuine` with zero cliffs +
a 20–22 kHz ultrasonic audit (see "Ground-truth methodology" below). Four
albums turned out to be genuine 96 kHz hi-res masters.

### Encode matrix

49 variants per track (LAME CBR/VBR ladders, ffmpeg-native AAC, libfdk
CBR/VBR/HE-AAC, Opus, Vorbis by quality level, a normalized 16/44.1 FLAC
control, six second-generation fraud shapes) plus 4 genuine Apple CoreAudio
modes (qaac 2.89 / CoreAudioToolbox 7.10.9.0 on a dedicated Windows VM —
byte-identical toolchain across all four arms). Every file was measured with
the production primitives from `lib/spectral_check.py`, capturing grade, cliff
Hz, estimate, HF deficit, reference dB, and the raw 16-slice dB vector per
file.

### Extension and window measurements

Four 20–22 kHz slices over the window-relevant variants, and three additional
30-second windows (offsets 60/120/180 s) over the launder-relevant variants.
The production analysis window (12–20 kHz, sixteen 500 Hz slices) was **not**
widened: the extension slices (20000/20500/21000/21500 Hz) are an additive
capture that never feeds `detect_cliff`, because widening the detector input
would shift every historical cliff detection. The `U` statistic below averages
the three slices at 20500/21000/21500 Hz against a 1–4 kHz reference band.

---

## Campaign narrative (short)

Read this once for orientation; everything load-bearing is stated as a finding
elsewhere in this document.

1. **Phase 0–3** — six literature/source research docs, a temporary calibration
   instance, the encode matrix, and the first full measurement pass. The
   spec-derived tables were replaced by measured, detector-space tables.
2. **Phase 4 model, round-1 blind test: FAIL.** Six fraud classes produced
   album-level false accepts. Root cause: the ultrasonic hard-floor threshold
   (75 dB) was derived from the ground-truth audit but never checked against
   the *training frauds*. Controls were clean (0/15 false flags) — the
   conservative direction held.
3. **Rework (training-side only)** — the ceiling-step detector replaced the
   absolute ultrasonic floor; two-signal lossless-container flagging;
   ±1-tier band assertions with explicit "no cliff asserts nothing"; then
   multi-window union aggregation and the **v2.1** freeze.
4. **Round-2 blind test: FAIL.** 12 FLAC-container false accepts, every one on
   an HF-poor album. The failure had a single measurable cause and no promotion
   fell outside a documented residual class.
5. **Two candidate fixes falsified on training data** before either shipped
   (see "Falsified hypotheses"). The second yielded a strikingly reproducible
   constant that was nonetheless the wrong model.
6. **Scorer v3 derived, qualified on three arms, FROZEN** before any round-3
   measurement. The affirmative leg became level-invariant.
7. **Round-3 blind test: PROOF BAR PASS**, on the hardest cohort the model had
   faced (46% HF-poor). Zero FLAC-container launders received proof.
8. **Four-arm per-codec ladder validation** — the lossy half, which is what the
   issue was actually opened for, finally spent the three holdout arms. Vorbis
   replicated exactly; the AAC content floor was confirmed; Opus audit-only was
   confirmed; and cutoff Hz was **falsified** as a cross-codec currency.
9. **Apple/CoreAudio completed on all four arms**, and the corpus-composition
   figures that motivated the codec priorities were corrected.
10. **Operator dropped the two remaining verification gates**; Phase 5
    implementation began.

---

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
- **Detector-space buckets** (cliff Hz → nominal kbps class), measured:
  `<15000→96 | <16000→128 | <17250→160 | <18250→192 | <19250→256 | ≥19250→320`.
- **Window truncation bites earlier than the specs suggest**: CBR-224/256 are
  84–89% invisible in the production 12–20 kHz window (lowpass 19.4/19.7 kHz
  needs two steep slices inside the window), and CBR-64's 11 kHz lowpass sits
  *below* the window floor — no cliff, caught only by the deficit metric.
- **Cliff detection is material-dependent**: quiet/dark albums often produce
  no detectable cliff at low bitrates (nothing to cut). Therefore: **a detected
  cliff supports a ±1-tier band assertion; an absent cliff asserts nothing** —
  never "unbounded quality," simply no evidence.
- **Band-assertion accuracy, ROUND-2 (the redesign's first blind test)**:
  ±1-tier 94–100% across the whole CBR ladder (cbr96/128 100%, cbr160 98%,
  cbr320 97%, cbr192 96%, cbr224/256 94%), with 147–263 of 337 tracks
  asserting per variant. This is a clear improvement over round 1's
  point-bucket scoring, and "no cliff asserts nothing" behaved as intended.
- **V-presets** map into the same content classes for free: V2 measures
  identically to CBR-192 (~18000 Hz — which matches its real ~190 kbps content
  class), V4/V6 land in the 128–160 band, V0 is mostly cliff-free (unfiltered
  under the default `vbr_mtrh` engine). **V2's ±1-tier accuracy degrades with
  the cohort's HF-poor share**: 93% (training) → 87% (round 2) → 78%
  (round 3). See "The lossy side is unqualified on HF-poor material".

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
  own source table). Measured on **all four arms**: FDK 192/256/320 all cliff
  at a 16500 Hz median, i.e. they read as "MP3 128" through the LAME table.
  FDK's cutoff is an *identity signature*, not a quality ladder.
- **ffmpeg-native aac**: a rising empirical ladder (96 → 15500, 128 → 17000,
  192 → 18250, 256/320 → mostly no cliff) **with a dynamic cutoff** — on
  dense/loud material the cutoff climbs ≥18 kHz. This dynamism matters twice:
  it makes the ladder probabilistic, and it creates a thin-evidence fraud
  class (residual class 3).
- **Apple CoreAudio** (the dominant real-world source): publishes no cutoff
  table (confirmed by the qaac maintainer). Four-arm measurement
  (% no-cliff / median cliff Hz):

  | variant | TRAINING | ROUND-1 | ROUND-2 | ROUND-3 |
  |---|---|---|---|---|
  | apple-cbr128 | 16% / 16500 | 13% / 17000 | 27% / 17000 | 29% / 16500 |
  | apple-abr192 | 75% / 18500 | 80% / 18500 | 82% / 18250 | 74% / 18000 |
  | apple-tvbr91 | 75% / 18500 | 80% / 18500 | 82% / 18000 | 77% / 18000 |
  | apple-cvbr256 | 98% / 18000 | 99% / 18500 | 96% / 16000 | 98% / 17500 |

  Legacy CBR-128 is visible and stable (and coincidentally bucketed correctly
  by the LAME table); ABR-192 / TVBR-91 are ~75–82% invisible; **CVBR-256
  (iTunes Plus / Apple Music) is invisible** — see residual class 1.
- **The pooled content floor**: an AAC cliff anywhere in 13000–18000 Hz is
  consistent with encoder-rates from 96 to 320 kbps across all three encoder
  families; only ≥18500 Hz lifts the floor to ~190-class; below 13 kHz is
  junk-class. Share of all AAC cliffs (ffmpeg-native + libfdk + Apple) landing
  in 13–18 kHz, with the Apple holdout encodes included:

  | TRAINING | ROUND-1 | ROUND-2 | ROUND-3 |
  |---|---|---|---|
  | 95% | 94% | 96% | 95% |

  (An earlier pass, before the Apple encodes were added to the holdout arms,
  read 95 / 98 / 98 / 98 on the same statistic. The table above is the final
  four-arm figure.) Pooled, the encoder settings producing a 13–18 kHz cliff
  are `fdk-cbr128` (985), `fdk-cbr256` (978), `fdk-cbr320` (975), `fdk-cbr192`
  (969), `apple-cbr128` (884), `aacffm-96` (832), `fdk-vbr3` (580),
  `fdk-cbr96` (573), `aacffm-128` (315), `apple-abr192` (115).
- Therefore: **AAC cliff evidence asserts a content floor, never a bitrate, and
  is never a transcode accusation** (cliffs are native AAC behavior). This was
  fitted on training and is now **measured on three independent holdouts**.

### Opus — audit-only, proven on four arms

libopus reaches fullband (20 kHz) at ~12 kbps equivalent for stereo music;
every music-relevant bitrate selects identical bandwidth, and CELT's
band-energy preservation + spectral folding keeps *measured* energy in every
band regardless of actual coding precision. Measured (% no-cliff / median HF
deficit):

| variant | TRAINING | ROUND-1 | ROUND-2 | ROUND-3 |
|---|---|---|---|---|
| opus-32 | 94% / 45 dB | 97% / 44 | 94% / 44 | 96% / 43 |
| opus-96 | 99% / 46 dB | 100% / 45 | 99% / 44 | 99% / 43 |
| opus-256 | 99% / 48 dB | 100% / 46 | 99% / 46 | 99% / 44 |
| **control-flac1644** | **99% / 48 dB** | **100% / 47** | **99% / 47** | **99% / 44** |

Opus ≥32 kbps is statistically indistinguishable from genuine lossless on every
arm; only opus-12 separates (deficit 57–64 dB), and the one real bandwidth
boundary (SWB→FB near ~12–16 kbps) sits below music bitrates. **No spectral
quality inference is possible for Opus; audit-only, unconditional.**

### Vorbis — the ladder replicates exactly

Source-extracted quality ladder, median cliff Hz, all four arms:

| variant | Phase 4 (fitted on TRAINING) | TRAINING | ROUND-1 | ROUND-2 | ROUND-3 |
|---|---|---|---|---|---|
| vorbis-q0 | 14500 | 14500 | 14500 | 14500 | 14500 |
| vorbis-q2 | 16000 | 16000 | 16000 | 16000 | 16000 |
| vorbis-q3 | 17000 | 17000 | 17000 | 17000 | 17000 |
| vorbis-q4 | 18500 | 18500 | 18500 | 18500 | 18500 |

Four-for-four on three independent arms — **Vorbis q0–q4 is decision-grade with
its own table**, confirmed rather than asserted. The same one-tier detector bias
as LAME applies. Measured detector-space ladder (cliff Hz → nominal kbps class):
`<15250→64 | <16500→96 | <17750→112 | <19000→128 | ≥19000→160`.

**q5 — the Spotify Normal tier — cuts at 20.1 kHz, past the production
window**: 82–91% no-cliff in-window across the arms, visible only with the
20–22 kHz extension. q6+ has no encoder lowpass at all. Where the old
LAME-shaped table did catch Vorbis cliffs it over-estimated one-directionally
(Vorbis keeps more top-end per kbps than LAME; q4's real 128 kbps read as
est-192). ffmpeg's `-q:a -1` behaves like ~q3 (encoder-mapping artifact, not a
ladder violation).

### HE-AAC (SBR) — the pre-classification gate is mandatory

**Worse than predicted: HE-AACv1 at 64 kbps reads as lossless** — `fdk-he1-64`
measures 96–100% no-cliff on **every arm**, because everything in the analysis
window is SBR-synthesized content sitting at plausible energy. `fdk-he2-32` is
66–71% no-cliff (and on training, 91% of its tracks were caught as suspect by
the deficit metric). The mandatory consequence: **detect SBR via AAC object
type (5/29) and exempt those files from cliff-based grading entirely** — a
pre-classification gate, not a calibration problem.

### WMA

Dropped from calibration permanently: no published cutoff table exists
anywhere, the encoder that made real-world WMA files (WMP9-era Microsoft) has
no Linux implementation, and ffmpeg's `wmav2` is a clean-room 1999-codec
reimplementation whose ladder would calibrate the wrong encoder. Audit-only
forever; a cliff on a supposedly-lossless file remains meaningful regardless
of codec.

---

## Cross-codec comparison — cutoff Hz is not a currency

This is the load-bearing negative result of the whole campaign, and it directly
concerns the live defect that opened issue #829 (download 37946, an AAC
candidate graded against an MP3 existing through a shared spectral clamp).

**Phase 4 proposed cutoff Hz as the common currency. The four-arm data
falsifies it.** For each measured cliff band, the top classes each codec's own
ladder infers:

| measured cliff | MP3 says | VORBIS says | AAC says |
|---|---|---|---|
| 13.0–15.0 kHz | cbr96, v6, cbr128 | q0, q4, q5 | cbr96, cbr128, vbr3 |
| 15.0–16.5 kHz | cbr128, v6, v4 | q2, q0, q4 | cbr128, cbr96, vbr3 |
| **16.5–18.0 kHz** | **cbr160, v4, v2** | q-1, q3, q4 | **cbr256, cbr320** |
| 18.0–19.25 kHz | cbr192, v2, cbr224 | q4, q5, q6 | abr192, tvbr91 |

A cliff at 17 kHz means **~160 kbps in MP3 and 256–320 kbps in AAC** — the same
number, a factor of two apart in true quality. There is no monotone mapping
from cutoff Hz to quality that holds across codecs. Cutoff Hz is a *per-codec
measurement*, not a currency, and the shared clamp cannot be fixed by
re-scaling — it has to be gated off.

**But comparison IS valid in inferred-class space.** Measured over all four arms
pooled, comparing MP3 against Vorbis by each side's own inferred class rather
than by cutoff Hz:

| comparison | pairs | ordering correct |
|---|---:|---:|
| MP3 ↔ Vorbis, both sides cliffed | 9,713 | **98%** |
| …at ≥64 kbps class gaps | — | 99% |
| …below 64 kbps class gaps | — | 96% |
| within-codec control, MP3 | 4,118 | 99% |
| within-codec control, Vorbis | 3,860 | 99% |
| **no-call — at least one side had no cliff** | **28,251 (74%)** | — |

The binding limit is **coverage, not accuracy**. The resulting rule:

> Compare in inferred-class space, never in cutoff space, and only when BOTH
> sides have an invertible ladder (MP3, Vorbis q0–q4). AAC contributes a
> one-sided floor. Opus and HE-AAC contribute nothing. **No ladder on either
> side ⇒ no comparison — unknown, not equal.**

The #827 parity-property domain extension must therefore encode "cross-codec
spectral comparison is undefined and fails closed", not a translation table.

### The four Phase 3 questions, with four-arm answers

1. **Stable bitrate→cutoff mapping?** MP3 yes, Vorbis yes (q0–q4), AAC no
   (floor only), Opus no, HE-AAC no.
2. **Native-low-bitrate vs transcoded, within codec?** Only where a ladder
   exists (MP3, Vorbis).
3. **False-positive rate on band-limited lossless?** 1–8% at the re-thresholded
   deficit, vs 4–17% before.
4. **Common currency for cross-codec comparison?** **None exists. Fail closed.**

---

## The HF-deficit metric

Training control (genuine lossless) deficit distribution: p50 = 48 dB,
p95 = 65, p99 = 69, max = 78. The legacy thresholds (marginal 40 / suspect 60)
flag the *median* genuine track as marginal — the trap albums did exactly their
job (Bee Thousand 20/20 tracks non-genuine at up to 78 dB, all genuine).

**Re-derived thresholds: marginal 65 / suspect 69.** Training-side, that gave
5.5% / 1.5% track-level false positives, ≈0 album-level after the ≥60%
aggregation, while retaining 58–79% of the real deficit-only catches (CBR-64,
HE-AACv2-32, AAC-96, Apple-128).

**Validated on four arms** — control false-flag rate (the old 40/60 column
reproduces the training track-level figures above, so these are track-level):

| arm | ≥65 (marginal) | ≥69 (suspect) | old 40 / 60 |
|---|---|---|---|
| TRAINING | 5% | 1% | 75% / 14% |
| ROUND-1 | 2% | 0% | 73% / 4% |
| ROUND-2 | 11% | 8% | 80% / 17% |
| ROUND-3 | 2% | 1% | 68% / 9% |

The old thresholds flagged the majority of genuine lossless on every arm.
65/69 holds up, with round 2 the outlier at 11% / 8% (that arm is 26% HF-poor).

Round 2 also produced a clean live demonstration of the old thresholds' harm:
two genuine FLAC arrivals (Gas *Pop*, Víkingur Ólafsson *Johann Sebastian
Bach*) were stamped `likely_transcode` while simultaneously being
verified-lossless-proofed. Per-track audit with the production primitives:
**0/7 and 0/35 tracks cliff**. The grade was entirely the mis-thresholded
deficit on HF-poor material (62–88 dB). Both are genuine and were kept.

The metric's honest role is narrow: the backstop for sub-window junk with no
visible cliff — and it is one of the two legs that expose AAC→MP3 launders (the
cliff leg alone exposes only 6/34 training albums; adding the deficit leg
raises it to 31/34).

---

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
   containers uses the album-level ceiling/ultrasonic machinery instead.
5. **The fundamental ambiguity — mastering ceilings.** Six of 34 genuine
   training albums carry real spectral ceilings from their recording chains:
   Gould's 1982 Goldberg Variations (early all-digital ADC), Kind of Blue's
   1987 CD master, Tabula Rasa (ECM 1984), Loveless, Feed Me Weird Things,
   and a 1991 cassette-sourced master. A mastering-chain lowpass is
   *physically identical* to a codec ceiling; no spectral feature will ever
   split them. This forced the proof-semantics change below.
6. **Level-invariance is not optional** (the round-2 lesson, below). In
   *absolute* dB, a quiet record's genuine ultrasonic content is
   indistinguishable from a loud record's launder leakage. Any ultrasonic
   statistic must be normalised against the album's own reference band.

### v2.1 and why it failed round 2

v2.1 was the union-over-windows gate with three legs: the in-window cliff leg,
the ceiling leg (step ≥15 dB with position-consistency ≥0.5), and a **relative
affirmative-content leg**:

```
A = mean(12–14 kHz) − mean(20.5–22 kHz)        deny promotion if A > 35
```

Training-final performance: opus→flac denied 34/34, mp3-128→flac 33/34,
vorbis-q5→flac 33/34; genuine controls denied 6/34 — every one an
early-digital/band-limited master.

**Round 2 scored it once and it failed**: 12 FLAC-container false accepts
(mp3128→flac promoted 2, opus96→flac 3, vorbisq5→flac 7), against denials of
25/27, 24/27 and 20/27. Controls denied 5/27 (18.5%) vs training 6/34 (17.6%),
so the control side generalized fine.

**Every false accept sat on an HF-poor album, with zero overlap.** Ranking the
27 albums by their own control-FLAC HF deficit against how many of the three
launder classes each slipped gives an unbroken monotone dose-response:

| album (control) | HF deficit dB | launder classes slipped |
|---|---:|---|
| Víkingur Ólafsson — Bach | 70.0 | **3/3** |
| Gas — Pop | 67.8 | **3/3** |
| Aphex Twin — SAW II | 54.3 | 2/3 |
| Ye — MBDTF | 52.0 | 1/3 |
| Floating Points — Elaenia | 52.0 | 1/3 |
| Tim Hecker — Harmony in UV | 51.9 | 1/3 |
| Max Richter — Blue Notebooks | 50.8 | 1/3 |
| Vashti Bunyan … Orbital (20 albums) | 49.8 → 36.2 | **0/3** — none |

Slipped ≥1 class: n=7, deficit 50.8–70.0. Slipped none: n=20, deficit
36.2–49.8. The gap (49.8 → 50.8) is unbroken and there are no exceptions in
either direction.

**Mechanism:** on HF-poor material the 12–14 kHz reference band the affirmative
leg measures against is itself weak, so the ceiling and affirmative legs have
no dynamic range left to discriminate a codec ceiling from the album's own
rolloff. The gate was being asked a question its inputs could not answer, and
it answered "promote".

**The defect was semantic, not numeric.** `A`'s own 35 dB threshold separates
cleanly on *both* cohorts — training promoted-max 32.1 vs denied-min 35.9;
round-2 promoted-max 33.4 vs denied-min 35.2. The problem is that `A < 35` was
being read as "affirmative content present → promote" when it can equally mean
"this album never had the dynamic range to show a gap", and those two cases
must not be indistinguishable when promotion grants terminal proof.

### Scorer v3 — the level-invariant ultrasonic deficit (FROZEN)

v3 replaces the relative affirmative leg with an **ultrasonic deficit
normalised against the album's own midband**:

```
U = mean_over_tracks[ ref_db(1–4 kHz) − mean(20.5–22 kHz) ]     deny if U ≥ 62
```

Normalising by the album's own reference removes the mastering-level term that
sank v2.1. `U` is computed from the candidate alone (all production ever has),
and both reference bands sit below every codec cutoff in play, so `U` is stable
across launder classes. **Cliff and ceiling legs are unchanged**; legs are still
evaluated per window (offsets 0/60/120/180) and unioned — any window tripping
any leg denies promotion.

**The threshold came from the binding constraint, not from a sweep.** For
launder albums where this leg is the *last line* (cliff and ceiling legs both
silent), the album is denied iff max-over-windows `U ≥ T`, so `T` must sit below
the minimum such value:

| arm | binding minimum `U` | album |
|---|---:|---|
| TRAINING (34) | **67.9** | t-vorbisq5-flac / Radiohead |
| ROUND-2 (27) | **66.7** | t-vorbisq5-flac / Aphex Twin |

Inter-arm spread is 1.2 dB. **T = 62** sits 4.7 dB below the tightest observed
value — roughly 4× that spread. The naive choice would have been wrong: the
largest training-clean threshold is T=67, and **T=67 leaks on round 2**
(vorbisq5 / Aphex Twin). The training boundary has no margin, which is exactly
why the binding-constraint form was used instead of the sweep edge.

### Four-arm qualification of v3

| arm | HF-poor share | FLAC launders denied (mp3128 · opus96 · vorbisq5) | controls denied | PROOF BAR |
|---|---:|---|---:|---|
| TRAINING (34) | ~6% | 34/34 · 34/34 · 34/34 | 13/34 (38%) | **CLEAN** |
| ROUND-1 (15, spent) | — | 15/15 · 15/15 · 15/15 | 4/15 (27%) | **CLEAN** |
| ROUND-2 (27, spent) | 26% | 27/27 · 27/27 · 27/27 | 10/27 (37%) | **CLEAN** |
| **ROUND-3 (24, fresh blind)** | **46%** | **24/24 · 24/24 · 24/24** | **9/24 (37.5%)** | **CLEAN** |

Round 2 went from 12 FLAC-container false accepts to zero, and round 3 — the
cohort built specifically to break this — produced zero false accepts across
all three launder classes. Genuine-denial cost came in exactly as predicted
(37.5% vs 38% training and 37% round 2; no drift).

Round-3's 9 denied controls: Björk, Dire Straits, Fugazi, Kate Bush, Philip
Glass, Steve Reich/Kronos Quartet, Sunn O))), Swans, Talk Talk.

**A denial is not a rejection.** The album imports normally, carries no spectral
verified-lossless proof, and surfaces in triage. Under the approved semantics
that is the correct outcome for material whose full-band provenance genuinely
is not spectrally provable.

### Verdict reporting separates two bars

v2.1 conflated them into one exit code. v3 reports:

- **PROOF BAR (blocking)** — a FLAC-container launder receiving verified-lossless
  proof. Archivist-critical.
- **LOSSY-SIDE (non-blocking)** — an MP3-container fraud whose band assertion
  fails to sit below its container class. These are MP3s; they can never receive
  lossless proof, so a miss is a weaker claim, not a false grant of terminal
  status.

---

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

Authority: *"right. this is kind of our idea with provisional lossless
source."* — https://github.com/abl030/cratedigger/issues/829#issuecomment-5056061513

Under these semantics the fraud bar becomes "zero fraud albums *receive
proof*" — achievable, and honest even on the false side: a 1982 digital
master genuinely has no spectral proof of full-band content.

### What "verified lossless" actually claims (reframed 2026-07-30)

The Apple arm falsified the unqualified version of that bar. "Zero fraud
albums receive proof" holds for the three classes it was measured against and
**fails at ~91% for Apple CVBR-256 → FLAC**. The stamp cannot honestly mean
"proven bit-faithful to a lossless source."

Operator decision: **keep the name, bound the claim.**

> *"verified lossless inasmuch as we can — we still call it verified lossless
> but at least now we know what we can't know"*

So `verified_lossless` means, precisely:

> **No evidence of lossy origin was found by the tests we have.** Not "this is
> bit-faithful to a lossless source." The tests are the in-window cliff, the
> album ceiling, and the ultrasonic deficit; their measured competence is
> recorded in this document, class by class.

This is not a weakening dressed up as a definition — it is the strongest claim
the evidence supports, and it is strictly more useful than an unqualified one
because its failure modes are *enumerated* rather than unknown. The three
things it buys:

- **A false proof is now a bounded risk, not an open one.** The one class that
  defeats the gate is named, measured, and is the lowest perceptual severity of
  any (a near-transparent source). We know what we cannot know.
- **The archivist invariant survives.** The system still never auto-decides
  anything irreversible on this: a denial withholds proof, never rejects,
  denylists or accuses.
- **It stays falsifiable.** Any future discriminator that separates the Apple
  class moves the boundary, and this document is where the boundary is written
  down. The V0/Opus probe axis has already been tried and failed.

Operator surfaces must not imply more than this. Copy that reads as
"guaranteed bit-perfect" is wrong; copy that reads as "we found nothing
suspicious, and here is what we can and cannot detect" is right.

---

## Verdict tiers — the gate's output must not be binary

Operator challenge, and it landed: *"do we detect these FLAC re-encodes as bad,
or do we just lump these and the 37% into unverified? because that's not really
useful. are they good? who knows."* Correct as a criticism of the **output**,
not of the model: `denied` currently spans everything from *proven transcode* to
*quiet piano record we cannot certify*, and v3 already computes which legs
fired before throwing that information away.

Tier definitions (frozen v3 legs and thresholds; nothing is fitted here — this
is purely descriptive):

| tier | meaning |
|---|---|
| 1 | in-window cliff fired (≥60% of tracks cliff ≤19 kHz) |
| 2 | ceiling **and** no-ultrasonic together |
| 3 | ceiling only |
| 4 | no-ultrasonic only |
| 5 | nothing fired → PROOF |

Pooled over all four arms — **100 genuine albums vs 300 launders**:

```
class                   1      2      3      4      5     n
GENUINE                 .     18      2     16     64   100
mp3128                 91      .      .      9      .   100
opus96                  .     97      .      3      .   100
vorbisq5                8     58      1     33      .   100
```

Per arm:

| arm | GENUINE (1/2/3/4/5) | mp3128 | opus96 | vorbisq5 |
|---|---|---|---|---|
| TRAINING (34) | 0/6/0/7/21 | 33/0/0/1/0 | 0/34/0/0/0 | 4/20/1/9/0 |
| ROUND-1 (15) | 0/4/0/0/11 | 15/0/0/0/0 | 0/15/0/0/0 | 2/10/0/3/0 |
| ROUND-2 (27) | 0/4/1/5/17 | 25/0/0/2/0 | 0/24/0/3/0 | 0/18/0/9/0 |
| ROUND-3 (24) | 0/4/1/4/15 | 18/0/0/6/0 | 0/24/0/0/0 | 2/10/0/12/0 |

What the tiers are worth:

- **Tier 1 is a positive detection, not an absence of proof.** 0/100 false
  positives on genuine albums across four independent arms, catching 91/100
  mp3-128→FLAC — the commonest real-world fake. This is the same cliff signal
  production already stamps as `likely_transcode`.
- **Tier 2 is strong evidence, not proof.** 155 launders vs 18 genuine. It
  catches 97% of opus-96 and 58% of vorbis-q5 launders — but early-digital and
  band-limited masters genuinely live there (round-3: Björk, Fugazi, Kate Bush,
  Dire Straits, Talk Talk), which is exactly why it must never become an
  accusation.
- **Tier 4 is the honest "who knows".** 45 launders vs 16 genuine. This is the
  only bucket where the operator's "are they good? who knows" is the truthful
  answer — and it is 16 of 100 genuine albums, not the whole ~36% denial cohort.
- **Tier 5 remains absolute: 0 of 300 launders ever reached proof** on any arm.

### Base-rate caveat — load-bearing, repeat it wherever these rates appear

**This corpus is 1 genuine : 3 launders *by construction*.** Every number above
is a per-class detection rate, **not a posterior odds**. In the real library the
base rate is overwhelmingly genuine — peers actually share 49.2% mp3 / 48.3%
flac / 1.7% AAC / 0.43% opus — so **tier 2 in production means "worth a look",
not "probably fake"**. Any operator-facing copy that implies otherwise will
slander the archive.

---

## Falsified hypotheses

These are as valuable as the confirmed findings and are recorded so no future
session re-derives them. Note the recurring shape: **a plausible constant that
was never checked against the training launder arm.** Item 1 shipped that way
and round 1 caught it; item 3 was the second instance and the training check
caught it before it shipped.

1. **Absolute ultrasonic hard floor (75 dB) — falsified by round 1.**
   Derived from the ground-truth audit but never checked against training
   frauds. sox sinc band leakage puts ~35–60 dB of apparent energy above a
   codec ceiling, so the floor never fires. Simultaneously the split semantics
   excluded ≥19250 Hz cliffs from flagging — exactly where opus/vorbis ceilings
   cliff. Two individually-sensible rules whose intersection was a corridor for
   the launders.
2. **The relative affirmative leg `A = mean(12–14k) − mean(20.5–22k)`, deny if
   `A > 35` — falsified by round 2.** Not because 35 was the wrong number (it
   separates cleanly on both cohorts) but because `A < 35` conflates "affirmative
   content present" with "no dynamic range to show a gap". Replaced by the
   level-invariant `U`.
3. **Control-FLAC album-mean HF deficit ≥ ~50 dB as a competence precondition —
   falsified on training before it shipped.** Round 2's dose-response table is a
   perfect zero-overlap separator *within that cohort* and it was tempting to
   threshold at 50 and ship. But training's only launder-promoting album is
   Grouper, whose deficit is **44.0 — rank 21 of 33** in that ranking (the arm
   is 34 albums; the sources do not explain the one-album difference). Twenty
   training albums have a *higher* deficit and promote nothing:

   ```
   threshold 50 dB: denies 10/33 training controls (30%), catches Grouper: False
   threshold 55 dB: denies  2/33 training controls ( 6%), catches Grouper: False
   threshold 65 dB: denies  1/33 training controls ( 3%), catches Grouper: False
   ```

   The dose-response is real within round 2 and still explains its failures, but
   it is **not a transferable law**. Had it been fitted from round-2 data and
   shipped, it would have regressed on the one training album that already
   demonstrated the class.
4. **A global launder leakage floor — falsified, but it yielded a reproducible
   constant.** Structural-headroom model: if a launder's ultrasonic band
   collapses to a fixed leakage floor `F`, then `A_max = ref − F`, and any album
   with `ref ≤ F + 35` is structurally incapable of tripping the leg. The floor
   is strikingly reproducible across independent cohorts — launder ultrasonic
   p90 = **−95.1 dB** (training) vs **−95.2 dB** (round 2) — but the resulting
   precondition (`ref > −60.2`) declares 25/34 training and 16/27 round-2 albums
   incompetent and its membership does not match the observed promotion sets.
   Launder ultrasonic level is partly signal-dependent (launder ultra p50 ≈
   −110 dB vs control ultra p50 ≈ −87 dB), not a fixed absolute.
5. **Taking the sweep edge as the threshold — falsified in derivation.** The
   largest training-clean value for `U` is T=67, and T=67 **leaks on round 2**.
   The binding-constraint derivation (below the tightest observed
   last-line minimum, by ≈4× the inter-arm spread) is what produced T=62.
6. **Cutoff Hz as a cross-codec currency — falsified on four arms.** See
   "Cross-codec comparison" above. Comparison is valid only in inferred-class
   space, and only where both sides have an invertible ladder.
7. **An automated residual-class exemption formula — removed on principle.**
   The v2.1 scorer self-test caught it misfiring two different ways. The verdict
   now names every promoted fraud album; classification against the documented
   residual classes is analyst work reported to the operator, never automated.

---

## Documented residual classes (accepted, spectrally unsolvable)

1. **Apple CVBR-256-sourced launders — MEASURED 2026-07-30, and the gate does
   not catch them.** Full result and method: `calibration-data/apple-arm/`.

   The `.m4a` statistics below were always four-arm, and they showed
   CVBR-256 indistinguishable from lossless in every measured band:

   | | TRAINING | ROUND-1 | ROUND-2 | ROUND-3 |
   |---|---|---|---|---|
   | apple-cvbr256 | 98% no-cliff / 49 dB | 99% / 48 | 96% / 47 | 98% / 45 |
   | **control-flac1644** | **99% no-cliff / 48 dB** | **100% / 47** | **99% / 47** | **99% / 44** |

   **But the FLAC-container launder `t-apple256-flac` was never run through the
   gate** — it exists only in `probe_pair.tsv.gz`, and the frozen scorer's
   `FLAC_FRAUDS` set is mp3-128 / opus-96 / vorbis-q5. "Survives the v3 gate"
   was an inference from the table above, not a result. A dedicated arm now
   measures it:

   | | T = 62 | T = 59.5 |
   |---|---:|---:|
   | launders reaching PROOF (n=17) | **10** | **10** |
   | genuine controls reaching PROOF | 11 | 10 |
   | conditional P(launder proof \| genuine proof) | 91% | **100%** |

   At `T = 59.5` the launder proof-set is byte-for-byte the same album set as
   the genuine proof-set — **zero discriminating power**. Pooled across two
   arms: 91–92% conditional false accept. The seven denials are denials of
   their *genuine originals* (quiet/HF-poor masters), not of the laundering.
   Lowering the threshold does not help.

   Mechanism: Apple CVBR-256 applies essentially no lowpass in the measured
   band — 2.1 dB down at 21.5 kHz relative to each album's own reference — so
   no leg has anything to see. Paired U delta, n=37: mean +1.88 dB, median
   +0.57 dB, against a four-arm safety margin of 2.04 dB.

   CVBR-256 is the dominant real-world AAC population (iTunes Plus / Apple
   Music). It remains the lowest perceptual severity of any fraud class
   (near-transparent source), and no cheap discriminator is known — the
   V0/Opus probe axis fails against it too. The 402 training pairs were
   deleted with the encode trees; the new arm's 37 pairs are committed.
2. **Quiet / no-HF material (the "Grouper class")** — albums with near-zero HF
   content anywhere provide no spectral evidence in either direction. Round 2
   showed this is **not a curiosity of two Grouper rows: it is 26% of a
   realistic archive cohort** (and 46% of round 3's deliberately-loaded one).
   v3 solved the *proof-gate* half of this class by making the ultrasonic leg
   level-invariant, so it no longer causes false accepts. The *lossy-side* half
   is open (below). Automated exemption formulas for this class misfire;
   classification is analyst work.
3. **Dynamic-cutoff AAC→MP3 launders on dense material** — when ffmpeg-AAC's
   cutoff climbs ≥18 kHz, the transcode retains near-full bandwidth and both the
   band and deficit legs read thin. Round 2's two misses (Gojira 33% of tracks
   exposed, The Killers 36%) are both dense/loud with *low* control deficit
   (45.7 / 41.9) — a different mechanism from class 2, as designed.

---

## Fraud-shape detectability — four arms, album level

FLAC-container launders under the frozen v3 gate (denied = did not receive
proof):

| fraud shape | detected by | TRAINING | ROUND-1 | ROUND-2 | ROUND-3 |
|---|---|---|---|---|---|
| mp3-128 → FLAC | in-window cliff (tier 1, 91% pooled) | 34/34 | 15/15 | 27/27 | 24/24 |
| opus-96 → FLAC | ceiling + no-ultrasonic (tier 2, 97% pooled) | 34/34 | 15/15 | 27/27 | 24/24 |
| vorbis-q5 → FLAC | ceiling / no-ultrasonic / cliff union | 34/34 | 15/15 | 27/27 | 24/24 |
| apple-256 → FLAC | **nothing — 10/17 reach proof** (dedicated arm, 2026-07-30) | not in arm | not in arm | not in arm | not in arm |

The Apple row was `—` across all four arms because **the class was never in
them**; it was added and measured separately. See `calibration-data/apple-arm/`
and residual class 1. The three rows above it are the classes the zero-false-
accept bar was actually established against.

MP3-container frauds (exposed = the band assertion sits below the container
class). Non-blocking — these can never receive lossless proof:

| fraud shape | TRAINING | ROUND-1 | ROUND-2 | ROUND-3 |
|---|---|---|---|---|
| mp3-192 → mp3-320 (same-codec, surviving source cliff) | 34/34 | 15/15 | 27/27 | **23/24** |
| aac-128 → mp3-320 (deficit + band legs combined) | 31/34 | 14/15 | 25/27 | **18/24** |

### The lossy side is unqualified on HF-poor material

Round 3 is the first cohort that stressed this, and it degraded. The six
`t-aac128-mp3320` misses were Sarah Davachi (0% of tracks exposed), William
Basinski (27%), Hiroshi Yoshimura (33%), Portishead (36%), Bohren (44%),
Biosphere (45%); the single `t-mp3192-mp3320` miss was Bohren (44%).

**Five of the six are HF-poor arm albums** — the same root mechanism that sank
the proof gate in round 2, now showing up on the lossy side: where an album has
little high-frequency content, a cliff-based band assertion has nothing to
detect, and the `hf_deficit ≥ 69` backstop is too blunt to compensate. The
margin is thin, not just the failures: Björk sits exactly on the 50% boundary,
with Death (56%) and Talk Talk (67%) close behind. `lame-v2` ±1-tier accuracy
fell to 78% (vs 87% round-2, 93% training) by the same mechanism.

Interpretation: the band-assertion machinery was calibrated and validated on
cohorts that were 94–74% full-band material. It is not wrong so much as
**unqualified for HF-poor input**, exactly as the affirmative leg was before v3.
The archivist-critical consequence is nil — no fraud received terminal proof —
but "we asserted a bitrate band" is a weaker claim on this material than the
earlier arms implied. The fix wants the same competence-precondition treatment
the affirmative leg received (derive on training, qualify, validate), and it
should probably **assert nothing rather than assert weakly**, consistent with
the existing "no cliff asserts nothing" rule.

**Portishead's `t-aac128-mp3320` miss (36%) is unexplained.** It is not HF-poor
and it fits none of the three documented residual classes.

---

## Ground-truth methodology findings

- **The verified-lossless admission bar works**: it caught a real fake at
  corpus admission (a "FLAC" mixtape with 27/29 tracks cliffing at 17.5 kHz —
  lossy-native, upscaled by someone upstream).
- **The ultrasonic audit closes the circularity risk**: ground truth admitted
  under the *old* detector could in principle contain the launder classes the
  old detector can't see — but every such class leaves a ≤20.1 kHz ceiling,
  and 0/34 training albums showed one. "Carries real content above 20 kHz" is
  the property calibration needs, and it held corpus-wide.
- **`validate_audio`'s rc=0 gap** (issue #835): ffmpeg's default error
  resilience conceals recoverable frame corruption behind a zero exit code —
  a corrupt FLAC passed the production audio gate and imported; `flac -t`
  fails the same file. One corpus track was excluded for this; the prod
  library likely holds more of the class.
- **Real-world sample rates**: 4 of 34 wild-sourced "CD" albums were genuine
  96 kHz hi-res masters. Matrix encoding normalized everything to 16/44.1.
- **Smoke-test the blind pipeline against one real album before the run.** The
  round-3 smoke test earned its keep: it caught a real defect — this beets
  library stores item paths *relative* to the library root, which the blind-tree
  builder was treating as absolute, so `makedirs` created stray directories
  outside the staging tree.

---

## Real-world corpus composition (corrected)

An earlier reading of `download_log.actual_filetype` put Opus at 21% of
Soulseek candidates. **That was wrong**: `actual_filetype` is *post-conversion*,
and `was_converted=True, flac→opus` accounts for 7,249 of those rows — the
pipeline's own `verified_lossless_target = opus 128` output, not peer content.

What peers actually share (`slskd_filetype`, n=17,706 where recorded):

| shared | share of known |
|---|---:|
| mp3 | 49.2% |
| flac | 48.3% |
| **m4a / AAC** | **1.7%** |
| opus | 0.43% |
| ogg / wav | 0.26% |

Soulseek is ~half MP3, half FLAC, and **AAC is ~4× more prevalent than Opus** —
the opposite of the earlier framing, and why completing the Apple/CoreAudio arm
was the right call.

---

## Standing caveats and open items

- **Existing `verified_lossless` stamps remain proofs under the OLD model's
  assumptions** and are NOT re-scored by this project. The verified-lossless
  re-measurement sweep (15,495 proof rows / 7,175 albums) and the read-only
  classification sweep of prod `failed_imports/` (5.7 GB, 205 mixed-codec files)
  were both dropped.
  Authority: *"1 and 2 don't really buy us anything, lets behin the coding
  ound"* — https://github.com/abl030/cratedigger/issues/829#issuecomment-5086552837
  The standing caveat now persists indefinitely rather than being resolved by a
  sweep. If the operator later wants the number, the sweep is still available
  and unchanged.
- **93% of existing verified-lossless proofs cannot be backfilled with the new
  ultrasonic statistic**, because their source FLAC was converted away
  (`verified_lossless_target = opus 128`) and the row wears its *source's*
  spectral by design (R19). Measured on prod 2026-07-27: 15,222 lossless-derived
  evidence rows across 6,346 albums, 14,391 of them carrying verified-lossless
  proof, against 15,501 proof rows total. The consequences this forces on the
  proof-gate implementation — three distinguishable meanings of
  `ultrasonic_deficit_db IS NULL`, no retroactive demotion, and operator
  surfaces saying which model proved a row — are specified in the Phase 5 plan,
  § "PR3 hard constraint".
- **Lossy-side band assertions on HF-poor material** — open, non-blocking; see
  above.
- **Portishead's unexplained `t-aac128-mp3320` miss** — open.
- **The mixed-codec prod quarantine** (5.7 GB, 20 dirs, 205 files — 126 flac /
  83 opus / 79 mp3) is the natural real-world adversarial check for the lossy
  verdicts. Never run; dropped with the gates above.
- **No-cliff asserts nothing.** The high end of every ladder is invisible. This
  is a permanent property, not a defect.

---

## Data and reproduction

All under `/mnt/virtio/Music/calibration-tmp/` on shared storage. The temporary
calibration instance was still running as of 2026-07-27 and is kept alive until
Phase 5 PR5; the ownership-ordered teardown checklist (which must run *before*
the calib DB is dropped, or leftover slskd files become permanently unreapable)
is in `docs/plans/2026-07-22-001-feat-829-spectral-calibration-plan.md` and
`docs/plans/2026-07-27-001-feat-829-phase5-implementation-plan.md` § 5.

| path | contents |
|---|---|
| `encodes*/manifest.tsv` | what was encoded, per arm |
| `measurements/results*.tsv` | per-file grade / cliff / estimate / deficit / ref + 16-slice vectors, all four arms |
| `measurements/extended*.tsv` | 20–22 kHz extension slices |
| `measurements/multiwin*.tsv` | per-window sweeps (offsets 60/120/180) |
| `measurements/score_v3.py` | **frozen** scorer, `T_ultra=62`; its `_window_legs` / `gate` are the shape to port |
| `measurements/run_round3.sh` | one-command blind-round pipeline (six stages, idempotent, resumable) |
| `measurements/build_holdout3_src.py` | blind-set assembly + automated contamination guard |
| `measurements/scorecard-round*.txt` | the single scoring pass per blind round |
| `versions.txt` | encoder provenance + exclusions |

Analysis scripts are one-shots and are deliberately uncommitted
(`.claude/rules/scope.md`). The encode matrices (~371 GB) are prunable once the
per-codec tables are committed into the six research docs.
