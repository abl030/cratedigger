# Spectral calibration data — the measured record behind the per-codec model

This directory is the **primary evidence** for every per-codec spectral
constant in `lib/quality/spectral_interpretation.py`. It is immutable: it
records what was measured in July 2026, not what is true now.

The derived analysis lives in the sibling research docs
(`../spectral-calibration-findings.md` and the five per-codec documents).
This directory is the substrate those documents were computed from, kept so
that any future claim about the model can be checked against the data rather
than against a summary of it.

## Sub-directories

Each is a self-contained measurement record with its own README carrying the
exact commands, tool versions and column layouts. **The 2026-07-30 directories
are deliberately descriptive and draw no conclusions; the 2026-07-31
directories DO draw conclusions**, and each states its verdict up front.

| directory | what it holds | measured |
|---|---|---|
| `apple-arm/` | the Apple CVBR-256 → FLAC gate test that closed residual #1 | 2026-07-30 |
| `launder-matrix/` | 31 encoder variants × 19 albums, built as real launders, measured through the production analyzer and scored with the frozen gate | 2026-07-30 |
| `derrien/` | a validated numpy port of Derrien's MDCT-lattice AAC-transcode detector, plus paired and wild measurement arms | 2026-07-30 |
| `floor-analysis/` | re-reading of the four committed arms plus the launder matrix — preconditions, bitrate floors, a three-state framing | 2026-07-30 |
| `provenance/` | read-only AccurateRip / CUETools DB / MusicBrainz DiscID probes over the library's FLAC albums — **its AR/CTDB results are superseded by `provenance-round2/`** | 2026-07-30 |
| `homogeneity/` | within-album track-to-track dispersion as a discriminator — **VERDICT: no**, falsified with the sign inverted | 2026-07-31 |
| `shape-analysis/` | the slice vector as a rolloff shape — **VERDICT: no**; SNR > 1 ⇔ the gate already catches it | 2026-07-31 |
| `derrien-refinement/` | which Derrien statistics are deployable — **VERDICT: yes, partial**; a parameter-free offset-concentration rule closes the whole Apple/CoreAudio family | 2026-07-31 |
| `provenance-round2/` | the corrected AccurateRip / CTDB verification — **VERDICT: partial**; 27 of 42 lossless albums bit-verified, a positive-only badge tier | 2026-07-31 |

## Provenance

Issue #829. The spectral subsystem's cliff→bitrate table
(`lib/spectral_check.py::LAME_LOWPASS`) is calibrated to exactly one encoder,
LAME MP3, but the attempt spectral audit measures **every** codec and persists
the result as decision-facing evidence. A live AAC was consequently read as an
"MP3 128 transcode" (download 37946). Rather than add a narrow codec gate, the
operator chose empirical calibration: a ground-truth FLAC corpus, a full
encoder×bitrate matrix, and per-codec verdicts.

**60,102 production-primitive measurements across four independent arms:**

| arm | albums | rows | role |
|---|---:|---:|---|
| TRAINING | 34 | 21,306 | the arm the model was fitted on |
| ROUND-1 | 15 | 9,275 | first blind holdout |
| ROUND-2 | 27 | 17,861 | second blind holdout |
| ROUND-3 | 24 | 11,660 | fresh blind holdout — the arm the proof gate passed |

Each arm encoded the same ground-truth FLACs through the full matrix (LAME,
ffmpeg-native AAC, libfdk AAC incl. HE-AAC, **Apple CoreAudio** via qaac,
Opus, Vorbis) plus second-generation fraud shapes, then measured every file
with the **production analyzer**, capturing raw slice vectors rather than only
the bucketed output.

**The proof gate was validated against THREE FLAC-container fraud classes,
not four** — `t-mp3128-flac`, `t-opus96-flac`, `t-vorbisq5-flac`, which is
what the frozen scorer's `FLAC_FRAUDS` set contains. `t-apple256-flac` was
built once for the V0 probe experiment and appears only in
`probe_pair.tsv.gz`; it was never spectrally measured here and never entered
the gate evaluation. An earlier version of this README listed it among the
fraud classes, which overstated what had been tested. **See `apple-arm/` —
that gap is now measured, and the gate does not catch the class.** The
lossy-container shapes (`t-aac128-mp3320`, `t-mp3128-aac256`,
`t-mp3192-mp3320`) are present in `results*`/`extended*` but cannot receive
lossless proof in any case.

Holdout arms were sealed until the scorer being tested was frozen. Round 1
**failed**, and that failure is part of this record — see
`scorecard-round2.txt` and `scorecard-round3.txt`.

## Files

Large tables are gzipped (27 MB → 2.4 MB); `gunzip -c` them. **All TSVs are
headerless.** Column layouts below are taken from the producing scripts, not
inferred.

### `results{,-holdout,-holdout2,-holdout3}.tsv.gz` — the core record

One row per measured file. 27 columns:

| idx | field |
|---:|---|
| 0 | album slug |
| 1 | variant (encoder + setting identifier, e.g. `lame-cbr64`, `apple-cvbr256`) |
| 2 | codec family |
| 3 | encoder |
| 4 | setting |
| 5 | source path *(historical — the encode tree was deleted at teardown)* |
| 6 | grade (`genuine` / `marginal` / `suspect` / `likely_transcode` / `error`) |
| 7 | cliff Hz — **empty when no cliff was detected** |
| 8 | estimated kbps from the LAME table — empty when no cliff |
| 9 | HF-deficit dB |
| 10 | reference dB (1–4 kHz level) |
| 11–26 | sixteen 500 Hz slice levels, 12000 → 19500 Hz |

`control-flac1644` is the genuine-lossless control variant.

### `extended{,-holdout,-holdout2,-holdout3}.tsv.gz` — ultrasonic extension

`variant, path, s20000, s20500, s21000, s21500` — the four slices above the
production window. The ultrasonic proof leg is computed from these.

### `multiwin{,-holdout,-holdout2,-holdout3}.tsv.gz` — per-window sweeps

`variant, path, offset_seconds, ref_db, twenty slices (12000 → 21500)`.
Offsets 0/60/120/180 s. The proof gate evaluates its legs per window and
unions them, so a fraud that hides in one window is still caught.

### `probe_pair.tsv.gz` — the V0/Opus re-encode probe experiment

`arm, variant, album_slug, source_path, duration_s, v0_kbps, opus_kbps`.
5,670 files. **A negative result, kept deliberately**: only mp3-128 separates,
and that is the one class the cliff leg already catches. Apple CVBR-256
survives this axis too, because at that rate the source is near-transparent so
content *complexity* — which re-encode probes measure — is preserved. Recorded
so the idea is not re-proposed from scratch.

### `gt-audit-{manifest,results}.tsv` — ground-truth audit

The ultrasonic audit of the corpus itself: 0 of 34 albums showed a ceiling
signature, and four are genuine 96 kHz hi-res. This is what licenses the
corpus as ground truth.

### `score_v3.py.frozen` — the frozen scorer

Frozen 2026-07-26. This exact code produced the tier table and the round-3
blind pass. PR3 ports its `_window_legs` / `gate` shape into production; it is
kept as the artifact that port is verified against, not as machinery to run.

The `.frozen` suffix is deliberate and load-bearing: it keeps the file out of
Pyright, Ruff and Vulture. This is an ad-hoc analysis script preserved as
evidence, and it does not type-check cleanly. **Do not "fix" it** — its whole
value is being byte-identical to the code that produced the validated results.
Rename it to `.py` only in a scratch copy if you need to run it.

### `corpus-manifest.json` — what was measured

115 rows, **every one carrying an exact MusicBrainz release ID**. The corpus
FLACs themselves were deleted at teardown (operator decision, 2026-07-29):
the pipeline's entire purpose is acquiring exact pressings, so the manifest is
the reacquisition path. Note the honest caveat — the corpus was chosen to
include long-tail and band-limited material, and reacquisition depends on
peers still having it.

### `tables.md`, `scorecard-round2.txt`, `scorecard-round3.txt`, `seed_ids.txt`, `round3_reqs.json`

Derived summaries and the corpus seeds, as generated at the time.

## What this data established

Constants and their homes in production:

- **MP3 detector-space buckets** — `<15000→96 | <16000→128 | <17250→160 |
  <18250→192 | <19250→256 | ≥19250→320`. Detector space, *not* encoder-lowpass
  space: `detect_cliff` reports roughly one tier below the encoder's lowpass,
  which is why the shipped `LAME_LOWPASS` table systematically under-rates MP3s.
- **Vorbis q0–q4 ladder** — `<15250→64 | <16500→96 | <17750→112 | <19000→128 |
  ≥19000→160`. Medians replicated exactly on all four arms.
- **AAC content floor** — a cliff in 13000–18000 Hz supports only a 96-class
  floor; ≥18500 lifts to ~190. 94–96% of all AAC cliffs on every arm land in
  13–18 kHz, produced by everything from 96 to 320 kbps across all three
  encoder families. **An AAC cliff is never a transcode accusation.**
- **Opus ≥32k** — statistically indistinguishable from genuine lossless on
  every arm. Audit-only, unconditional.
- **HE-AAC (SBR)** — `fdk-he1-64` reads 96–100% no-cliff: a 64 kbps file that
  looks lossless. The object-type pre-gate is not optional.
- **HF-deficit thresholds** 65 / 69 dB, replacing 40 / 60. The old thresholds
  flagged the majority of genuine lossless.
- **Ultrasonic proof leg** —
  `U = mean_over_tracks[ref_db(1–4 kHz) − mean(20.5–22 kHz)]`, deny promotion
  when `U ≥ 62`.
- **Cross-codec comparison is refused, not rescaled.** A 17 kHz cliff means
  ~160 kbps in MP3 and 256–320 in AAC. Cutoff Hz is a per-codec measurement,
  not a currency. Comparison is valid only in inferred-class space, only
  between invertible ladders (98% ordering accuracy over 9,713 MP3↔Vorbis
  pairs).

**Base-rate caveat, load-bearing for any copy derived from this data:** the
corpus is 1 genuine : 3 launders *by construction*. Detection rates here are
per-class, **not** posterior odds. Real peer-shared content is roughly 49% mp3
/ 48% flac / 2% AAC / 0.4% opus.

## Known residuals

1. **Apple CVBR-256 → FLAC defeats the proof gate — MEASURED 2026-07-30, see
   `apple-arm/`.** 10 of 17 launders reach proof; at `T = 59.5` the launder
   proof-set is byte-for-byte identical to the genuine proof-set, i.e. zero
   discriminating power. Pooled conditional false-accept 91–92% across two
   arms. Mechanism: Apple CVBR-256 applies essentially no lowpass in the
   measured band — 2.1 dB down at 21.5 kHz — so no leg has anything to see.
   This entry previously described the same conclusion as an inference from
   `.m4a` statistics across four arms; it was never gate-tested until now,
   and the measured result is worse than the inference implied.
   Still the lowest perceptual severity of any fraud class (near-transparent
   source), and no cheap discriminator is known — the V0/Opus probe axis
   (`probe_pair.tsv.gz`) fails against it too.
   **Update 2026-07-31: no *spectral* discriminator exists, and this is now
   proved twice over (`homogeneity/`, `shape-analysis/`) — but the class is
   closable off the spectrum. `derrien-refinement/` scores the Apple family
   against the already-measured Derrien detector for the first time (the
   launder-matrix union table excluded `qaac-*` by a hardcoded `ORDER` list)
   and finds 0 of 10 Apple launders survive the spectral ∪ Derrien union, plus
   a parameter-free offset-concentration rule with an analytic false-positive
   floor of ~0.0023 albums per 5000.**
2. **Lossy-side band assertions are weak on HF-poor material.**
3. **No-cliff asserts nothing** — the high end of every ladder is invisible.
   A permanent property, not a defect.

## What was not kept

The encode matrices (~584 GB apparent) and the ground-truth FLACs (38 GB) were
deleted at teardown. Every measurement derived from them is here; the audio is
not. Re-deriving a *new* statistic from the original audio would require
reacquiring the corpus from `corpus-manifest.json` and re-encoding.
