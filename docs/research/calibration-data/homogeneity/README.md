# Within-album homogeneity — does laundering compress a album's track-to-track spread?

Measured 2026-07-31. **This directory draws conclusions.** It answers open
question 1 of `../launder-matrix/README.md` § "Unexplored directions".

## The question and the verdict

**Question.** Every statistic in this research is computed per track and then
aggregated. A laundered album went through *one* encoder at *one* setting; a
genuine album's tracks are related only by mastering. Does the **dispersion
across tracks within an album** therefore separate laundered from genuine?

**VERDICT: NO — and the hypothesis is falsified with its sign inverted.**

1. Laundering does not *compress* dispersion. Where it moves dispersion at all
   it **inflates** it: `lame-v0` +0.84 dB and `vorbis-q6/7/8` +1.4 to +1.7 dB
   median paired `U_sd`, with 88–100 % of albums moving up.
2. On the classes that actually escape the frozen proof gate — `aacffm-256`,
   `aacffm-320`, `qaac-cvbr256`, `qaac-cvbr320` — the effect is **absent in
   both directions**. Median paired `U_sd` delta: −0.012, −0.036, −0.003,
   +0.011 dB. A **paired oracle** that is handed the album's own genuine twin
   (which production never has) sits at a coin flip: 8/17, 7/17, 8/17, 9/17
   albums up, p = 1.000, 0.629, 1.000, 1.000.
3. A grouped leave-one-album-out multivariate model over **33 dispersion
   features** scores **AUC 0.508** against the escape class — chance.
4. The reason is the same one that defeats every level statistic here:
   **album identity dominates second moments exactly as it dominates levels.**
   Genuine `U_sd` spans 1.88 → 21.36 dB across the 137-album genuine corpus,
   a 19.5 dB range, against a ≤1.7 dB launder effect.

**The permanent methodological lesson (record this, not the AUC tables).** The
null-feature control `ref_sd` — the dispersion of the 1–4 kHz reference level,
which a transparent encoder *cannot* move — has a median paired delta of
±0.01 dB on every variant, i.e. laundering provably does nothing to it, **and
it still scores AUC 0.60**. That 0.60 is pure corpus composition. Any AUC in
the 0.55–0.65 band on this corpus must be read against that floor, not against
0.5. Several of the "promising" dispersion statistics (`U_sd` 0.56–0.65,
`shape_mpd_hi` 0.54–0.72) never clear their own null feature.

## How it was run

From the repository root, in the pinned dev shell. Python 3.14.6, numpy 2.5.1,
scipy 1.18.0, scikit-learn 1.8.0.

```bash
nix-shell --run "python3 build.py"      # -> tracks.json.gz
nix-shell --run "python3 disp.py"       # -> disp.json.gz
nix-shell --run "python3 cohorts.py"    # -> OUT_cohorts.txt
nix-shell --run "python3 paired.py"     # -> OUT_paired.txt
nix-shell --run "python3 eval.py"       # -> OUT_auc.txt
nix-shell --run "python3 report.py"     # -> OUT_report.txt
nix-shell --run "python3 report2.py"    # -> OUT_report2.txt
nix-shell --run "python3 oracle.py"     # -> OUT_oracle.txt
```

`report.py`, `report2.py` and `oracle.py` do `import eval as E`, so a scratch
copy must have every `.py.frozen` renamed back to `.py` and be run from its own
directory. The scripts are read-only over `../` and write only into their own
scratch directory.

## Cohorts

Everything is read from the four committed calibration arms plus the two
2026-07-30 launder corpora. `build.py.frozen` joins `results*` (16 slices,
12000–19500 Hz) to `extended*` (4 slices, 20000–21500 Hz) into one 20-point
vector per track, then `disp.py.frozen` reduces each (dataset, album, variant)
group to dispersion statistics.

| cohort | albums | genuine baseline variant |
|---|---:|---|
| `ARM` — TRAIN + R1 + R2 + R3 | 100 | `control-flac1644` |
| `MTX` — launder matrix `local` tag | 17 (19 − two 96 k/24) | `genuine` |
| `APPB` — apple-arm arm B | 20 | `control-flac1644` |
| **genuine reference corpus** | **137** | |

Slug overlap is zero between `ARM` and each of the other two; `MTX` and the
apple-arm A tag share all 17 albums, which is why arm A is excluded from the
genuine pool and only arm B contributes (`OUT_cohorts.txt`). The two 96 kHz /
24-bit albums (8920, 8931) are excluded everywhere — they carry a resampling
and a requantisation confound.

The **escape class** is the ten launder variants that reach proof under the
frozen gate: `lame-v0`, `aacffm-256`, `aacffm-320`, `vorbis-q10`, `vorbis-q8`,
`vorbis-q7`, `vorbis-q6`, `qaac-cvbr256`, `qaac-cvbr320`, `t-apple256-flac`.
Variants marked `*` in the tables (`lame-cbr128*`, `t-mp3128-flac*`,
`t-vorbisq5-flac*`) are already caught by the gate and appear for context only.

## Key results

### The null-feature control — read this before any AUC below

`ref_sd` cannot be moved by laundering. `OUT_report.txt`:

```
  lame-v0        n= 17 medianPairedDelta=+0.0001 dB   AUC_vs_pool=0.607
  aacffm-256     n= 17 medianPairedDelta=-0.0110 dB   AUC_vs_pool=0.602
  qaac-cvbr256   n= 17 medianPairedDelta=-0.0039 dB   AUC_vs_pool=0.606
  vorbis-q10     n= 17 medianPairedDelta=-0.0030 dB   AUC_vs_pool=0.606
```

Mean AUC over the escape class for `ref_sd` is **0.592** (`OUT_auc.txt`), which
places it 22nd of 35 candidate statistics — above thirteen of them, including
`U_iqr`, `U_mad`, `d16_18_sd` and `shape_mpd_all`.

### Paired oracle ceiling — the non-deployable upper bound

`OUT_oracle.txt`. `up/n` = albums whose dispersion increased under laundering;
`p` = two-sided exact binomial against 0.5. 50 % means the mechanism carries no
information *even with perfect per-album calibration*.

| variant | `U_sd` | `d18_195_sd` | `shape_mpd_hi` |
|---|---|---|---|
| `lame-v0` | 15/17 88 % p=0.002 | 15/17 88 % p=0.002 | 16/17 94 % p<.001 |
| `vorbis-q6` | 12/17 71 % p=0.143 | 17/17 100 % p<.001 | 17/17 100 % p<.001 |
| `vorbis-q8` | 12/17 71 % p=0.143 | 16/17 94 % p<.001 | 17/17 100 % p<.001 |
| `vorbis-q10` | 11/17 65 % p=0.332 | 9/17 53 % p=1.000 | 15/17 88 % p=0.002 |
| **`aacffm-256`** | **8/17 47 % p=1.000** | 3/17 18 % p=0.013 | 8/17 47 % p=1.000 |
| **`aacffm-320`** | **7/17 41 % p=0.629** | 5/17 29 % p=0.143 | 6/17 35 % p=0.332 |
| **`qaac-cvbr256`** | **8/17 47 % p=1.000** | 10/17 59 % p=0.629 | 11/17 65 % p=0.332 |
| **`qaac-cvbr320`** | **9/17 53 % p=1.000** | 11/17 65 % p=0.332 | 12/17 71 % p=0.143 |
| `t-apple256-flac` | 23/35 66 % p=0.090 | 24/35 69 % p=0.041 | 26/35 74 % p=0.006 |

The four bolded AAC rows are the escape classes with no spectral handle
anywhere else in this research, and the oracle cannot tell them apart either.

### The confound, measured

```
  U_sd           n=137  min=1.88 p10=3.59 median=6.60 p90=11.31 max=21.36  spread=19.48
  shape_mpd_hi   n=137  min=2.82 p10=4.93 median=8.42 p90=14.31 max=26.09  spread=23.28
  d18_195_sd     n=137  min=1.50 p10=3.85 median=7.17 p90=11.91 max=21.32  spread=19.82

  median paired launder effect on U_sd (escape class)
    lame-v0       +0.839 dB      aacffm-256    -0.012 dB      qaac-cvbr256  -0.003 dB
    vorbis-q6     +1.737 dB      aacffm-320    -0.036 dB      qaac-cvbr320  +0.011 dB
    vorbis-q7     +1.447 dB      vorbis-q10    +0.600 dB      t-apple256    +0.538 dB
    vorbis-q8     +1.400 dB
```

### Deployable operating table — one global threshold, LOAO

`OUT_report.txt`. Threshold fitted on the genuine reference corpus *excluding
the album under test*; no per-album calibration anywhere. `k8_shape_mpd_ul` is
the best statistic in the whole battery (mean escape AUC 0.668) and it buys
nothing usable:

```
alpha  genuineFP(alb)   lame-v0   aacffm-256  aacffm-320  qaac-cvbr256 qaac-cvbr320
0.01       2/116   1.7%   2/14        1/14        1/14        2/14         2/14
0.05       7/116   6.0%   3/14        2/14        2/14        2/14         2/14
0.10      13/116  11.2%   5/14        3/14        3/14        3/14         3/14
0.20      24/116  20.7%   6/14        5/14        5/14        6/14         6/14
```

The original hypothesis direction — **low** dispersion means launder — is
strictly worse than chance on every variant (`OUT_report.txt`, last two
tables): at α = 0.10 with 10.2 % genuine FP, `U_sd` recalls 0–2 of 17 launder
albums on every escape class.

### Cross-corpus false positives are worse than nominal

`OUT_report2.txt` section 1. Reference quantiles fitted on the 100 `ARM`
albums, applied to the 37 out-of-corpus genuine albums — the real deployment
condition, which the within-corpus LOAO number understates:

```
statistic                a=0.01  a=0.05  a=0.10  a=0.20
U_sd                       0.0%    8.1%   13.5%   21.6%    (nominal 1/5/10/20%)
shape_mpd_hi               0.0%    8.1%   10.8%   21.6%
rat_mpdhi_over_mpdlo       0.0%    0.0%    5.4%   24.3%
```

### The conditional table — the only population that matters

A new deny leg only matters on albums the gate would otherwise **promote**.
`OUT_report2.txt` section 2, on the 98 of 137 genuine albums the gate proves:

```
=== rat_mpdhi_over_mpdlo   (best-behaved statistic on this population)
alpha  newFP/proved     aacffm-256   aacffm-320   qaac-cvbr256  qaac-cvbr320
0.05      5/98   5.1%     0/11         0/11          0/10          0/10
0.10     11/98  11.2%     2/11         2/11          2/10          2/10
0.20     21/98  21.4%     5/11         5/11          6/10          6/10
```

At a 5 % new false-positive cost on already-proved genuine albums, the leg
catches **zero** AAC launders.

### Multivariate ceiling

`OUT_report.txt`, grouped leave-one-album-out logistic regression, 33 features,
136 genuine / 188 escape-launder albums:

```
pooled AUC (escape class vs genuine, grouped-LOAO) = 0.508
  genuine FP   5.1% -> escape recall  12.2%
  genuine FP  19.9% -> escape recall  26.1%
  per-variant recall at genuine FP=5.1%:
    lame-v0 18%  aacffm-256 0%  aacffm-320 0%  vorbis-q10 0%  vorbis-q8 29%
    vorbis-q7 29%  vorbis-q6 35%  qaac-cvbr256 6%  qaac-cvbr320 12%
    t-apple256-flac 3%
```

## Conclusions

1. **The within-album homogeneity axis is closed.** It is not a tuning
   problem, a threshold problem or a feature-engineering problem: on the two
   AAC families that defeat the spectral gate the paired oracle is at chance,
   and the paired oracle is an upper bound no deployable rule can exceed.
2. **Where the mechanism exists at all it points the other way.** `lame-v0`
   and the mid-quality Vorbis ladder *increase* within-album spread, because
   their per-track lowpass placement is content-dependent. That is a real
   effect, it is just not available on the classes that matter, and those
   classes are already caught by the cliff leg.
3. **Album identity dominates second moments as it dominates levels.** This
   is now measured on both. The 19.5 dB genuine `U_sd` range mirrors the
   55.99 dB genuine window-0 `U` range recorded in
   `../floor-analysis/README.md`; the same confound, one moment up.
4. **The null-feature control is the durable artifact of this thread.**
   `ref_sd` is unmovable by construction and scores 0.60. Any future
   discriminator proposed on this corpus owes a null-feature control before
   its AUC is read as signal. This is a cheap check and it retires whole
   families of proposal in one run.
5. **Nothing here changes production.** No constant, threshold or leg is
   derived from this directory.

## Files

`.py.frozen` files are the exact scripts that produced the data. **The
`.frozen` suffix is deliberate and load-bearing** — it keeps them out of
Pyright, Ruff and Vulture. They are evidence, not maintained source, and they
carry hardcoded absolute scratch paths. **Do not "fix" them.** Rename a scratch
copy to `.py` if you need to run one.

| file | role |
|---|---|
| `build.py.frozen` | joins `results*` + `extended*` across all 11 source tags into per-track 20-point vectors → `tracks.json.gz` |
| `disp.py.frozen` | per-(dataset, album, variant) dispersion reduction → `disp.json.gz` |
| `cohorts.py.frozen` | cohort definition, corpus-overlap and `null-flac` degeneracy checks, n-sensitivity |
| `paired.py.frozen` | paired genuine-vs-launder mechanism table, per corpus group |
| `eval.py.frozen` | cohorts + LOAO machinery + the AUC sweep (imported by the three report scripts) |
| `report.py.frozen` | LOAO operating tables, the `ref_sd` null-feature control, the multivariate ceiling |
| `report2.py.frozen` | cross-corpus FP, the gate-conditional table, per-variant multivariate recall |
| `oracle.py.frozen` | the paired-oracle ceiling and the genuine dispersion spread |

| output | produced by |
|---|---|
| `OUT_cohorts.txt` | `cohorts.py.frozen` |
| `OUT_paired.txt` | `paired.py.frozen` |
| `OUT_auc.txt` | `eval.py.frozen` |
| `OUT_report.txt` | `report.py.frozen` |
| `OUT_report2.txt` | `report2.py.frozen` |
| `OUT_oracle.txt` | `oracle.py.frozen` |

`OUT_cohorts.txt` was captured by re-running the frozen script over the same
inputs at commit time; the other five are the run's own transcripts. Every OUT
file's first line is the dev shell's own banner.

The two derived tables are the bulk of this directory (`tracks.json.gz` 4.9 MB,
`disp.json.gz` 3.7 MB, both already at maximum gzip). They are **fully
regenerable** from the committed measurement tables in `../` by
`build.py.frozen` then `disp.py.frozen` — kept because re-deriving them is the
only way to re-check any number below, and because a reader should not have to
trust that the derivation was the one described.

### `tracks.json.gz` — per-track feature table

A JSON list, one object per measured track, written by `build.py.frozen`:

| key | meaning |
|---|---|
| `ds` | source tag — `TRAIN`, `R1`, `R2`, `R3`, `LOCAL`, `LOCALW`, `MAPPLE`, `PILOT`, `APPLEA`, `APPLEB`, `APPLE24` |
| `slug`, `variant`, `path`, `grade` | identity, copied from the `results*` row |
| `cliff` | cliff Hz, or `null` when no cliff was detected |
| `hfdef` | HF-deficit dB |
| `ref` | 1–4 kHz reference level dB |
| `slices` | the 20 slice levels, 12000 → 21500 Hz |
| `U` | `ref − mean(20500…21500)` — the proof leg's own statistic |
| `d12_14`, `d14_16`, `d16_18`, `d18_195`, `d20_215` | band deficits below `ref` |
| `drop_hi` | `mean(18500…19500) − mean(20500…21500)` |
| `slope_mid` | `(mean(12000…14000) − mean(18500…19500)) / 11` |
| `r12000` … `r21500` | per-slice deficit `ref − slice[k]`, 20 keys |

### `disp.json.gz` — per-(dataset, album, variant) dispersion table

A JSON list written by `disp.py.frozen`. Keys `ds`, `slug`, `variant`, `n`
(track count), then, for each of the 30 scalars in `tracks.json.gz`
(`U`, the five band deficits, `drop_hi`, `slope_mid`, `ref`, `hfdef`, and the
20 `r<Hz>` values), six dispersion measures suffixed `_sd`, `_iqr`, `_mad`,
`_rng`, `_mean`, `_med` — e.g. `U_sd`, `d18_195_iqr`, `r21500_rng`.

Shape dispersion over the ref-normalized slice vector, for each of four bands
`all` (12000–21500), `hi` (≥18000), `ul` (≥20000), `lo` (≤16000):

| key | meaning |
|---|---|
| `shape_mpd_<band>` | mean pairwise Euclidean distance between the album's track vectors, normalized by √dim |
| `shape_sdmean_<band>` | mean over bands of the per-band standard deviation |
| `shapec_mpd_<band>` | same as `shape_mpd`, after removing each track's own mean level in that band |
| `shapec_sdmean_<band>` | same as `shape_sdmean`, mean-centred |

Track-count controls, present only when the album has at least `K` tracks —
mean over 200 random size-`K` track subsets, so every album is compared at
identical `n`:

`k4_U_sd`, `k4_d18_195_sd`, `k4_drop_hi_sd`, `k4_shape_mpd_ul`,
`k4_shape_mpd_hi`, and the same five with the `k8_` prefix.

Six ratios and three differences (`rat_*`, `dif_*`) are derived at evaluation
time by `eval.py.frozen::add_derived`, not stored.

## What is NOT here

No audio. This directory is derived entirely from the committed measurement
tables in `../`, `../launder-matrix/` and `../apple-arm/`; those directories
document what happened to the audio.
