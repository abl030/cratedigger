# The Apple arm — closing the one gap the four-arm campaign left open

Measured 2026-07-30. This directory turns residual #1 from an **inference**
into a **measurement**, and the answer is worse than the inference implied.

## The question it answers

The July 2026 calibration validated the verified-lossless proof gate against
**three** FLAC-container fraud classes — `t-mp3128-flac`, `t-opus96-flac`,
`t-vorbisq5-flac` — on four independent arms, with zero false accepts.

`t-apple256-flac` was listed as a fourth class in the plan and in the parent
README, but it was never run through the gate: it exists only in
`../probe_pair.tsv.gz`, the V0/Opus re-encode experiment, and is absent from
every spectral measurement file. The frozen scorer's `FLAC_FRAUDS` set is the
three classes above. "Apple CVBR-256 survives the v3 gate" was therefore an
inference from `.m4a` statistics, not a result.

## The answer

**The gate does not catch Apple-CVBR-256 → FLAC laundering.**

Single-window mode (= production, see the Phase 5 plan §1.5b):

| | T = 62 | T = 59.5 |
|---|---:|---:|
| Apple launders reaching PROOF (arm A, n=17) | **10** | **10** |
| genuine controls reaching PROOF (arm A) | 11 | 10 |
| conditional P(launder proof \| genuine proof) | 91% | **100%** |
| pooled conditional, arms A+B | 22/24 (92%) | 20/22 (91%) |

At `T = 59.5` the launder proof-set is **byte-for-byte the same album set** as
the genuine proof-set — the gate has zero discriminating power against this
class. The seven launders it denies are denied because their *genuine
originals* are also denied (quiet, HF-poor masters), not because they were
laundered. Lowering the threshold does not help; the same albums pass.

Against the validated classes, same scorer, same mode:

| class | albums | promoted @62 | promoted @59.5 |
|---|---:|---:|---:|
| `t-mp3128-flac` | 100 (4 arms) | 0 | 0 |
| `t-opus96-flac` | 100 (4 arms) | 0 | 0 |
| `t-vorbisq5-flac` | 100 (4 arms) | 1 | 0 |
| **`t-apple256-flac`** | **17 (1 arm)** | **10** | **10** |

## Why — the mechanism

**Apple CVBR-256 applies essentially no lowpass in the measured band.** Mean
level relative to each album's own 1–4 kHz reference, 17 albums:

| band | genuine | launder |
|---|---:|---:|
| 19.5 kHz | −46.7 dB | −47.9 dB |
| 21.5 kHz | −55.4 dB | −57.5 dB |

2.1 dB down at 21.5 kHz. There is nothing for the cliff, ceiling or ultrasonic
leg to see. Production grades all 17 launder albums `genuine`; the cliff leg
never fires (max 0.364 of tracks cliffed vs the 0.60 threshold); the ceiling
leg fires on 2, the same 2 it fires on for their genuine originals.

Paired U delta (launder − genuine), n=37 pooled: mean **+1.88 dB**, median
**+0.57 dB**, with |Δ| < 1 dB on 19 of 37 albums. Compare the gate's entire
four-arm safety margin: 2.04 dB.

## Two findings that constrain the implementation

**1. Ultrasonic deficit is not comparable across decode paths.** One control
would not reproduce its stored DB value — the single row with
`was_converted_from='alac'`. Isolated: the *same bits* measure **50.26 through
`_ffmpeg_to_wav` @48 kHz versus 47.17 sox-native @44.1 kHz — a +3.09 dB
skew**, larger than the gate's whole margin. A carried or propagated
`ultrasonic_deficit_db` from a non-sox-native container is not on the same
scale as a native FLAC measurement, and must not be gated against the same
threshold. The other 16 controls reproduce to 1e-7.

**2. A 16-bit launder is the adversarial case.** The 24-bit twin
(`*-apple24.*`) measures up to **+13.8 dB higher U** on quiet material
(Tabula Rasa 69.5 → 83.3), because 16-bit requantization noise refills the
ultrasonic band AAC emptied. Gate verdicts are identical (10/17 either way),
so the headline is depth-invariant — but the realistic "looks like a CD rip"
launder is strictly the harder one to catch.

## Harness trust

Run in single-window mode over the four committed arms, this harness
reproduces the published 34/100 genuine-denial figure **exactly**. Arm A's
false-denial cost is comparable to calibration (7/17 here vs 34/100 there), so
the arm is not anomalous — the launder result is not an artifact of an unusual
album set.

## Files

`results-*.tsv.gz` / `extended-*.tsv.gz` are in the parent directory's format
(see `../README.md` for column layouts). `albums-*.json` carry the production
`AlbumResult` facts; `gate-*.json` carry per-album U, fired legs and verdicts
at both thresholds.

| suffix | arm |
|---|---|
| `-apple` | arm A — 17 seeded corpus albums, 215 tracks, verified-lossless ground truth |
| `-apple2` | arm B — 20 FLAC albums from the `wrong_matches/` tree, each qualified as a genuine control first |
| `-apple24` | 24-bit launder control over arm A |

`manifest-armA.json`, `manifest-armB.json` and `armB-album-identity.tsv`
identify the source albums. Arm B albums are wrong *pressings*, which does not
matter here — the test needs genuine lossless audio, not a specific release.

The three `*.py.frozen` drivers are the exact scripts that produced this.
The `.frozen` suffix keeps them out of Pyright/Ruff/Vulture, same rule as
`../score_v3.py.frozen`: they are evidence, not maintained source. **Do not
"fix" them.**

## Reproduction

- **Encoder:** `C:\Tools\qaac\qaac64.exe -s -v 256 -o <out>.m4a <in>.wav`
  (`-v` = `--cvbr`), qaac 2.89 / CoreAudioToolbox 7.10.9.0 — byte-identical to
  the toolchain the training arm used. Output verified AAC-LC, 252–299 kbps,
  mean 273.
- **Chain:** library FLAC → ffmpeg PCM WAV (native rate/depth, no resample) →
  qaac → ffmpeg `-sample_fmt s16 -c:a flac`. 44.1 kHz sources stay 44.1; the
  three hi-res sources are resampled to 48 kHz by CoreAudio, as a real Apple
  launder would be.
- **Measurement:** `lib.spectral_check.analyze_album` called **unmodified**,
  with `_get_band_rms` wrapped by a pass-through recorder so the scorer's raw
  slice vectors come from the same sox calls production makes. Nothing
  re-implemented.
- **Gate:** byte-identical copy of `../score_v3.py.frozen`
  (md5 `b1eb515c4b0af702913d831ba045dafa`), its `load`/`gate`/`_window_legs`
  imported and driven. No multiwin file, so single-window.

## What this does not say

It does not say the gate is worthless. It catches the three common
real-world FLAC-container frauds essentially perfectly, and nothing in
production catches them today. It says the proof has one **known, measured,
unaddressed** blind spot, and that the honest description of what
`verified_lossless` means has to account for it.
