# The launder matrix — one corpus, many encoders, measured as real launders

Measured 2026-07-30. **Descriptive record only.** This directory says what was
built, what was run, and what the numbers came out as. It does not draw a
conclusion, and no conclusion should be inferred from the ordering or emphasis
of anything below. The research it belongs to is unfinished.

## What was built

One ground-truth corpus of **19 library FLAC albums / 230 tracks / 1044 min**,
pushed through a registry of encoder variants and decoded back to FLAC, so that
every non-control row is an **actual launder** — encoded, decoded, re-wrapped —
rather than a native lossy file whose statistics are read as if it were one.

Chain, identical for every non-control variant (`variants.py.frozen`):

```
source FLAC --WAV_ARGV--> PCM WAV (native rate/depth, NO -ar resample)
            --argv------> lossy intermediate
            --DECODE_ARGV-> 16-bit FLAC
```

`DECODE_ARGV` forces `-sample_fmt s16`. That choice is recorded in the
registry: the 24-bit twin on the Apple arm measured up to +13.8 dB higher
ultrasonic deficit, so 16-bit is the harder case to detect (see
`../apple-arm/README.md`).

## Variants

**31 variant IDs were measured**, of which two are controls (`genuine`,
`null-flac`) and 29 apply a lossy step. 27 of the 29 have ≥17-album coverage;
`ffmpeg-q0` and `opus-128` exist only in the 3-album pilot.

`FINAL_REPORT.txt` tabulates **24** of them (`genuine`, `null-flac` and 22
lossy) — that is `analyze_local.py.frozen`'s hardcoded `ORDER` list, which
excludes the five `qaac-*` Apple variants (reported separately by
`apple_report.py.frozen` / `apple_deployable.py.frozen`) and the two
pilot-only IDs. The full set is in `variants.py.frozen` and
`variant-index.json.gz`.

| family | variants | encoder |
|---|---|---|
| control | `genuine`, `null-flac` | none / ffmpeg WAV round trip |
| MP3 | `lame-v0`, `lame-v2`, `lame-cbr128/192/256/320` | LAME 64bits 3.100 |
| MP3 (ffmpeg front-end) | `ffmpeg-q0` *(pilot only)* | ffmpeg 8.1.2 libmp3lame |
| AAC | `aacffm-128/192/256/320` | ffmpeg 8.1.2 native `aac` |
| AAC (Apple) | `qaac-cbr128`, `qaac-abr192`, `qaac-tvbr91`, `qaac-cvbr256`, `qaac-cvbr320` | qaac 2.89 / CoreAudioToolbox 7.10.9.0 |
| Vorbis | `vorbis-q4/q5/q6/q7/q8/q10` | ffmpeg 8.1.2 libvorbis |
| Opus | `opus-96/192/256`, `opus-128` *(pilot only)* | ffmpeg 8.1.2 libopus |
| WMA | `wma-128/192/320` | ffmpeg 8.1.2 `wmav2` |

`variant-index.json.gz` is the provenance record, merged from the
per-variant `index.json` files that lived in the (disposable) encode tree. Per
variant it carries the `argv` template, the `encoder_version` string as the
binary reported it, and — per album — the fully rendered example command, the
output sample rate, the per-stage timings, and the genuine↔launder file map.

### Measured properties of the toolchain, recorded because they bound the data

- **`libfdk_aac` is not in this ffmpeg** (licensing). HE-AAC / SBR could not be
  built. `aacffm-*` is ffmpeg's native encoder; `qaac-*` is Apple CoreAudio;
  they are different encoders.
- **libopus always encodes at 48 kHz**, so an Opus launder decodes to a 48 kHz
  FLAC even from a 44.1 kHz source. The 20.5–21.5 kHz measurement band
  therefore sits far below Nyquist for Opus rows and at the edge for 44.1 kHz
  rows. Sample rate is entangled in every Opus comparison here.
- **`wmav2` is ffmpeg's clean-room reimplementation**, not Microsoft's encoder,
  and it overshoots the requested rate: `-b:a 128k` measured ~138 kbps,
  `-b:a 192k` ~276 kbps, `-b:a 320k` ~551 kbps. The variant label is the
  request, not the result. `wmav2` also cannot encode the two 96 kHz albums, so
  the `wma-*` rows cover 17 albums, not 19 (tag `localw`).
- **`aacffm-320` has a content-dependent ceiling**: on sparse material the
  encoder clamped near ~243 kbps and collapsed onto `aacffm-256` (Grouper, 60 s:
  242.8 vs 242.5 kbps); on dense material it reached a higher rate (Autechre,
  60 s: 344.5 vs 283.3 kbps).
- **`lame -V0` prints that it disables the polyphase lowpass** (stock LAME
  3.100 behaviour).
- **`lame -V0` and `ffmpeg -q:a 0 -c:a libmp3lame` are nearly, but not
  exactly, the same bitstream.** Re-verified during this capture over the three
  pilot albums (30 tracks): **27 of 30 decode to bit-identical PCM**; the other
  3 (album 8923) differ only in trailing length by ≈0.0008 s (~35 samples).
  `RUNBOOK.md` states them as bit-identical; the measurement above is the
  narrower true statement.
- **The `null-flac` control is a true null for 16-bit sources.** Re-verified
  during this capture: **197 of 197 tracks from the seventeen 16-bit albums
  decode to PCM bit-identical to their source.** The 33 tracks from the two
  24-bit/96 kHz albums (8920, 8931) are *not* bit-identical at native depth —
  the chain's `-sample_fmt s16` step requantizes them — which is why those two
  albums must be scored against their own `null-flac`, not against `genuine`.

## Corpus

`manifest.json` — 19 albums, built by `mkmanifest.py.frozen` from library
paths. Slug format used throughout is `{request_id}--{album dir basename}`.

| request_id | album | tracks | sr | bits |
|---:|---|---:|---:|---:|
| 8916 | Daft Punk — Random Access Memories | 13 | 44100 | 16 |
| 8917 | Aphex Twin — Syro | 12 | 44100 | 16 |
| 8918 | Autechre — Tri Repetae | 10 | 44100 | 16 |
| 8919 | Squarepusher — Feed Me Weird Things | 12 | 44100 | 16 |
| 8920 | M83 — Hurry Up, We're Dreaming | 22 | **96000** | **24** |
| 8921 | Red Hot Chili Peppers — Californication | 15 | 44100 | 16 |
| 8922 | Metallica — Death Magnetic | 10 | 44100 | 16 |
| 8923 | Converge — Jane Doe | 12 | 44100 | 16 |
| 8924 | Arvo Pärt — Tabula Rasa | 4 | 44100 | 16 |
| 8925 | Bach / Gould — The Goldberg Variations | 32 | 44100 | 16 |
| 8926 | Miles Davis — Kind of Blue | 5 | 44100 | 16 |
| 8928 | GoGo Penguin — Man Made Object | 10 | 44100 | 16 |
| 8929 | The Velvet Underground & Nico | 11 | 44100 | 16 |
| 8930 | Simon & Garfunkel — Bridge Over Troubled Water | 11 | 44100 | 16 |
| 8931 | Fleetwood Mac — Rumours | 11 | **96000** | **24** |
| 8932 | My Bloody Valentine — Loveless | 11 | 44100 | 16 |
| 8933 | Grouper — Ruins | 8 | 44100 | 16 |
| 8934 | Duster — Stratosphere | 17 | 44100 | 16 |
| 8935 | Brian Eno — Ambient 1: Music for Airports | 4 | 44100 | 16 |

The two 96 kHz/24-bit albums carry a resampling and a requantisation confound
and are excluded from most reported aggregates; where they appear they are
labelled `96k/24`.

## Tags

A "tag" is one measurement run. Row counts are track rows in `results-*`.

| tag | albums | variants | track rows | what it is |
|---|---:|---:|---:|---|
| `local` | 19 | 21 | 4830 | the main local-encoder arm (MP3/AAC-ffmpeg/Vorbis/Opus + controls) |
| `localw` | 17 | 5 | 985 | the `wmav2` arm — 17 albums because wmav2 cannot encode 96 kHz |
| `apple` | 19 | 7 | 1610 | the five `qaac-*` Apple CoreAudio variants + controls |
| `pilot` | 3 | 8 | 240 | the first shakedown run |
| `qaacchk` | 1 | 2 | 16 | a single-album qaac smoke check |
| | | | **7681** | total |

## Files and their exact column layouts

Large tables are gzipped; `gunzip -c` them.
**`results-*` and `extended-*` are HEADERLESS** (they merge directly with the
parent directory's `results*.tsv.gz` / `extended*.tsv.gz`).
**`tracks-*` and `derrien-*` DO carry a header row.**

### `results-<tag>.tsv.gz` — 27 columns, headerless

Written by `measure.py.frozen`. One row per measured track.

| idx | field |
|---:|---|
| 0 | slug (`{request_id}--{album dir basename}`) |
| 1 | variant id |
| 2 | codec family as the production analyzer reported it for that track |
| 3 | encoder (from the variant registry) |
| 4 | setting (from the variant registry) |
| 5 | measured file path *(library path for `genuine`; disposable encode tree otherwise)* |
| 6 | grade (`genuine` / `marginal` / `suspect` / `likely_transcode` / `error`) |
| 7 | cliff Hz — **empty when no cliff was detected** |
| 8 | estimated kbps from the LAME table — empty when no cliff |
| 9 | HF-deficit dB |
| 10 | reference dB (1–4 kHz level) |
| 11–26 | sixteen 500 Hz slice levels, 12000 → 19500 Hz |

### `extended-<tag>.tsv.gz` — 6 columns, headerless

`variant, path, s20000, s20500, s21000, s21500` — the four slices above the
production window. Empty string where the slice was not measurable.

### `tracks-<tag>.tsv.gz` — 14 columns, **with header**

Written by `score.py.frozen`. Columns 0–10 are `results-*` columns 0–10 in the
same order, then:

| idx | field |
|---:|---|
| 11 | `derrien_proba` — Derrien statistic for that file (empty if not run) |
| 12 | `derrien_z` |
| 13 | `derrien_offset` — recovered AAC frame offset |

### `derrien-<tag>.tsv.gz` — 16 columns, **with header**

Written by `derrien.py.frozen`. One row per (file, frame-selection mode).

`variant, slug, request_id, mode, path, sr, ch, dur, proba, median, std, z,
offset, pmode, secs, err`

- `mode` — frame-selection mode passed to the detector (`high` throughout;
  `low` was used only in the standalone `../derrien/` arm)
- `proba` — the detector statistic at the best offset; `median` / `std` are over
  the offset sweep; `z = (proba - median) / std`
- `offset` — the argmax offset in samples (0–1023)
- `pmode` — which channel combination produced the reported value (`LR` / `MS`)
- `secs` — wall seconds for that file; `err` — exception text, empty on success.
  **Rows with a non-empty `err` are excluded by every downstream script.**

### `albums-<tag>.json` — production `AlbumResult` facts

One object per (variant, album): `slug`, `variant`, `request_id`, `folder`,
`grade`, `suspect_pct`, `cliff_hz`, `codec_family`, `ultrasonic_deficit_db`,
`estimated_bitrate_kbps`, `n_tracks`, `n_error`. These are the values
`lib.spectral_check.analyze_album` returned, unmodified.

### `gate-<tag>.json` — frozen-gate verdicts

One object per (variant, album):

| key | meaning |
|---|---|
| `variant`, `slug`, `n` | identity, track count |
| `denied@62`, `legs@62` | frozen-gate verdict and fired legs at `T = 62` |
| `denied@59.5`, `legs@59.5` | same at `T = 59.5` |
| `U_win0` | ultrasonic deficit recomputed from the window-0 slice vectors |
| `U_prod` | `ultrasonic_deficit_db` as production reported it |
| `prod_grade`, `prod_cliff`, `prod_codec_family` | production's own album facts |
| `cliff_frac<=19k` | fraction of tracks with an in-window cliff |
| `derrien_proba_max`, `derrien_proba_med`, `derrien_z_max`, `derrien_offsets`, `derrien_n` | present only where Derrien ran for that (variant, album) |

Leg names are `cliff`, `ceiling`, `no-ultrasonic`.

### `FINAL_REPORT.txt`, `SCORE_local.txt`

`FINAL_REPORT.txt` is `analyze_local.py.frozen`'s output over the `local` +
`localw` tags: gate escape rates per variant, per-album PROOF/DENY grids,
Derrien control thresholds, Derrien recall, pooled operating points, and the
gate∪Derrien union table. `SCORE_local.txt` is `score.py.frozen`'s per-album
verdict dump for the same tag. Both are the reports as generated; some of their
headings are phrased as judgements ("lower is better for us"). Read them as the
run's own output, not as a settled position.

### `RUNBOOK.md`

The harness runbook as written during the run, including the measured cost
figures (~15 min wall and ~6 GB disk per variant, Derrien dominating at ~49 s
CPU per track). It refers to scratch paths that no longer need to exist.

## How it was run

All three stages are idempotent and resumable; re-running skips completed work.

```
build.py    --variants a,b,c [--albums 8918,8923] [--workers 4]
measure.py  --tag T --variants a,b,c [--albums ...] [--workers 6]
derrien.py  --tag T --variants a,b,c [--albums ...] [--workers 24]
score.py    --tag T [--control genuine]
analyze_local.py --tag local [--extra-tags localw]
```

Everything ran from the repository root inside `nix-shell`.

- **Measurement calls `lib.spectral_check.analyze_album` unmodified.**
  `_get_band_rms` is wrapped by a pass-through recorder so the raw slice vectors
  come from the same sox invocations production makes. Nothing is
  re-implemented; `measure.py.frozen` raises if the recorder and the analyzer
  disagree on track count.
- **The gate is the frozen scorer.** `score.py.frozen` imports
  `../score_v3.py.frozen`'s `load` / `gate` / `_window_legs` from a
  byte-identical copy (md5 `b1eb515c4b0af702913d831ba045dafa`, re-verified at
  capture time) and drives them. No multiwin file is supplied, so the gate runs
  **single-window** — window 0, the production 30 s-from-start window.
- **Derrien** runs `../derrien/aacdet.py.frozen` unmodified, on every variant
  including non-AAC ones.

## What is NOT here

**The 163 GB of launder audio.** It lived at `/mnt/virtio/Music/matrix/` on
doc2, one directory per variant, `<variant>/<request_id>/NNN.flac`. It is
**disposable** — every measurement derived from it is in this directory, and
the manifest plus `variant-index.json.gz` are the rebuild recipe. Rebuilding
costs roughly 5 min and 6 GB per variant per the recorded timings.

The source corpus is the live beets library at the paths in `manifest.json`.

## Frozen scripts

Every `.py.frozen` file is the exact script that produced the data.
**The `.frozen` suffix is deliberate and load-bearing** — it keeps these files
out of Pyright, Ruff and Vulture. They are evidence, not maintained source, and
they do not type-check cleanly. **Do not "fix" them.** Rename a scratch copy to
`.py` if you need to run one.

| file | role |
|---|---|
| `variants.py.frozen` | the variant registry — every `argv` template |
| `mkmanifest.py.frozen` | corpus manifest builder |
| `build.py.frozen` | encode/decode driver (local + remote-qaac) |
| `measure.py.frozen` | production-analyzer measurement → `results/extended/albums` |
| `derrien.py.frozen` | Derrien detector sweep → `derrien-*` |
| `score.py.frozen` | frozen-gate scoring → `gate-*`, `tracks-*` |
| `analyze_local.py.frozen` | the `FINAL_REPORT.txt` report |
| `apple_report.py.frozen`, `apple_deployable.py.frozen` | the Apple-family reports |
| `run_local_builds.sh`, `run_local_t3.sh`, `run_derr_final.sh` | batch drivers |

## Unexplored directions

Open questions, with the data that would answer them. **These are not
recommendations and carry no prediction that any of them will work.** They are
a map of where nobody has looked yet.

Context for why these are interesting rather than a conclusion about them: on
every instrument tried so far, **album identity dominates codec effect**.
Measured: `../floor-analysis/OUT_q2run3.txt` records genuine-lossless
`d18_195` spanning min 27.3 / median 46.8 / max 70.3 across the 100 genuine
control albums of the four committed calibration arms — a **43.0 dB spread**.
Computed from `../floor-analysis/feats.json.gz` for the proof leg's own
statistic, window-0 `U` over those same 100 albums spans **25.54 to 81.53 dB,
a 55.99 dB range**. Paired per-album codec effects on `abs_bU` are a few dB by
comparison (`../floor-analysis/OUT_q1b.txt`, median paired delta: Apple
CVBR-256 −3.6 dB, `lame-v0` −4.6, `vorbis-q9` −4.6). Items 1 and 3 below are
the two directions that would use album identity as a control rather than
fight it as a confound.

1. **Within-album homogeneity.** Every statistic used so far is computed
   per-track and then aggregated. The **variance across tracks within an
   album** has not been examined. A laundered album went through one encoder
   at one setting; a genuine album's tracks are related only by mastering.
   Whether that difference is measurable is untested. Data: per-track vectors
   already exist for every album × variant here (`results-*`, `extended-*`,
   `tracks-*`) and in the four committed arms. `feats.json.gz` already carries
   a `U_sd` column that nothing has used.
2. **The slice vectors as shapes.** Every leg reduces the 16–20 band-RMS values
   to a single scalar — cliff position, ceiling step, ultrasonic deficit, HF
   deficit. The vector itself, as a rolloff *shape*, has never been examined.
   Data: ~60,000 measurements in the committed arms plus the 7,681 rows here,
   all carrying full slice vectors.
3. **Cross-candidate agreement per request.** The pipeline re-downloads the
   same album from many peers — one live request has 226 candidates from 170
   distinct peers. Whether independent copies sharing fine spectral structure
   says something about the *source* rather than the file is untested.
   Related: the convergence-signal proposal in the issue #829 comments
   (2026-07-27), which was never operationalised. Data:
   `album_quality_evidence` joined through `download_log` per request, in the
   live pipeline DB.
4. **The V0 probe data.** Prior work concluded it "clusters copies but does not
   rank them" and set it aside for lacking an ordering (`../probe_pair.tsv.gz`,
   a deliberately-kept negative result). Whether the clustering *alone* carries
   information was not pursued. Data: `v0_min_bitrate_kbps` /
   `v0_avg_bitrate_kbps` / `v0_median_bitrate_kbps` on thousands of live
   evidence rows.
5. **HE-AAC / SBR is unmeasured on this host.** `libfdk_aac` is absent, so no
   SBR launder could be built here at all. The four committed arms contain
   `fdk-he1-64` rows from an earlier toolchain; nothing in this matrix does.
6. **The Apple family above CVBR-256 was measured but never reported
   alongside the rest.** `qaac-cbr128`, `qaac-abr192`, `qaac-tvbr91`,
   `qaac-cvbr256` and `qaac-cvbr320` are all in `gate-apple.json` and
   `results-apple.tsv.gz` at full 19-album coverage, but `FINAL_REPORT.txt`'s
   tables exclude them by construction.
7. **The pilot-only variants.** `ffmpeg-q0` and `opus-128` were measured on
   3 albums and never extended. Whether the near-bit-identity of
   `lame-v0` / `ffmpeg-q0` holds beyond those 30 tracks is unknown.
