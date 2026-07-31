# Derrien refinement — what is actually deployable from the MDCT-lattice detector

Measured 2026-07-31. **This directory draws conclusions.** It re-analyses the
measurements committed in `../derrien/` (the validated numpy port of Derrien's
JAES-2018 MDCT-lattice AAC-transcode detector) and in
`../launder-matrix/derrien-*.tsv.gz`, and asks which of its statistics survive
contact with a deployable rule.

## The question and the verdict

**Question.** `../derrien/` established that the detector separates Apple
CVBR-256 from genuine on a paired arm, and `../launder-matrix/FINAL_REPORT.txt`
§ D used a pooled `proba` threshold. Three things were never settled: whether
the never-scored **NAC probe** column adds anything; whether the **`mode=low`**
frame selection adds anything; and whether any of the thresholds are
**corpus-supported or two-album artifacts**.

**VERDICT: YES, partial — one parameter-free rule is deployable at proof
grade, and it closes the entire Apple/CoreAudio family.**

| finding | status |
|---|---|
| NAC probe | **dead.** Track AUC 0.760 against `proba` 0.995 / `z` 0.993. No combination beats `proba` alone. |
| `mode=low` frame selection | **dead.** Track AUC 0.644 (`proba`) / 0.659 (`z`) against 0.995 / 0.993 for `mode=high`. `proba` at `mode=low` saturates at exactly 1.0000 on *both* classes. |
| pooled `proba` threshold | **a two-album artifact.** The zero-FP λ = 0.1082 is set by album 8918 alone; max / median-album ratio **5.012**; removing two albums halves it. |
| pooled `z` threshold | **corpus-supported but only triage grade.** max / median-album ratio **1.114**; 16 of 17 albums must be removed before λ halves. But an honest Gumbel tail fit prices `z > 6.914` at **~492 false positives per 5000 albums**. `z > 12` is the conservative operating point (0/1136 wild tracks, 0/17 genuine). |
| **offset concentration** | **THE deployable finding.** ≥4 tracks of an album recovering the same MDCT frame offset. **No tunable threshold at all.** Analytic FP floor **~0.0023 albums per 5000**. Catches **100 %** of `qaac-cvbr256`, `qaac-cvbr320`, `qaac-tvbr91`, `qaac-abr192` and 16/17 of `qaac-cbr128`. Reproduced on an independent second Apple build. Mechanism exact. |

**PREMISE CORRECTION.** `../launder-matrix/FINAL_REPORT.txt` § D — the
gate ∪ Derrien union table — contains **no `qaac-*` row**. Its producing
script's hardcoded `ORDER` list excludes the five Apple variants (documented in
`../launder-matrix/README.md`), so **the Apple class was never scored against
the Derrien detector at all**. Scored here for the first time: the *existing*
pooled rule already closes it completely. **0 of 10** Apple launder albums
survive the spectral ∪ Derrien union, and 17/17 Apple albums are flagged by
every rule in the ladder including the parameter-free one.

The residual is therefore narrower than it looked. `ffmpeg`-native AAC 256/320
remains roughly half-invisible (best union: 5/10 and 4/10 survive), and
`lame-v0` / `vorbis-q10` carry no AAC lattice at all — the detector cannot see
them by construction.

## The mechanism, exactly

```
qaac / CoreAudio primes 2112 samples  ->  2112 mod 1024 = 64  ->  lattice at 1024 - 64 = 960
ffmpeg's native AAC   primes 1024 samples                     ->  lattice at 0
a genuine album's tracks share no such constant                -> offsets uniform over 0..1023
```

`q3f_out.txt`:

```
  matrix qaac-cvbr256          n= 197  modal offset 960  in 192/197 tracks ( 97.5%)   distinct offsets: 5
  derrien-arm qaac -s -v 256   n= 182  modal offset 960  in 177/182 tracks ( 97.3%)   distinct offsets: 5
  qaacchk qaac-cvbr256         n=   8  modal offset 960  in 8/8 tracks (100.0%)       distinct offsets: 1
  ffmpeg aacffm-256            n= 197  modal offset   0  in  57/197 tracks ( 28.9%)   distinct offsets: 127
  ffmpeg aacffm-128            n= 197  modal offset   0  in  14/197 tracks (  7.1%)   distinct offsets: 160
  genuine                      n= 197  modal offset 803  in   3/197 tracks (  1.5%)   distinct offsets: 182
  lame-v0                      n= 197  modal offset 982  in   3/197 tracks (  1.5%)   distinct offsets: 178
```

The two Apple rows are **independent builds** — the launder-matrix `apple` tag
and the `../derrien/` paired arm's own `qaac -s -v 256` encode — and both land
on 960 in ~97 % of tracks. This is the one cross-corpus replication in the
Apple family that is not the same bits twice (contrast
`../shape-analysis/README.md` § "corpus facts", where the matrix and apple-arm A
spectral datasets turn out to be byte-identical).

## The offset-concentration rule and its false-positive floor

**Rule:** flag an album when **k ≥ 4 of its tracks recover the same MDCT frame
offset**. There is no threshold to fit — 1024 is the MDCT lattice size and 4 is
an integer count.

Genuine offsets are uniform (`q3d_out.txt`):

```
genuine offsets, n=197: distinct=182  max repeat=3
  chi^2 over 32 equal bins = 19.5 on 31 df (E[chi^2]=31) -> consistent with uniform
  offsets 0 or 960 among genuine tracks: 1/197
```

`max repeat = 3` is over the **pooled** 197 offsets across all 17 albums; the
within-album coincidence count is **0/17 at k ≥ 2** (`q3c_out.txt` § D). The
analytic floor under uniformity:

```
  expected genuine albums (of 17) with k>=2: 1.331   k>=3: 0.00760   observed: 0 and 0
  extrapolated to a 5000-album library at the corpus mean 11.6 tracks/album:
    k>=2 rule: ~322.3 FP albums   k>=3 rule: ~1.049 FP albums   k>=4 rule: ~0.00231 FP albums
    a 32-track album:  k>=3 -> 4.73e-03,  k>=4 -> 3.35e-05
```

Coverage (`q3c_out.txt` § D, `alb k>=4` column):

| variant | k ≥ 3 | k ≥ 4 | mean mode-share |
|---|---|---|---|
| `genuine` | 0/17 | 0/17 | 0.111 |
| `null-flac` | 0/17 | 0/17 | 0.111 |
| `qaac-cvbr256` | 17/17 | **17/17** | 0.974 |
| `qaac-cvbr320` | 17/17 | **17/17** | 0.972 |
| `qaac-tvbr91` | 17/17 | **17/17** | 0.960 |
| `qaac-abr192` | 17/17 | **17/17** | 0.956 |
| `qaac-cbr128` | 17/17 | **16/17** | 0.861 |
| `aacffm-256` | 7/17 | 4/17 | 0.287 |
| `aacffm-320` | 5/17 | 4/17 | 0.279 |
| `aacffm-192` | 5/17 | 2/17 | 0.211 |
| `aacffm-128` | 1/17 | 1/17 | 0.167 |
| `lame-v0`, `lame-cbr320`, `vorbis-q10`, `opus-256`, `wma-320` | 0/17 | 0/17 | 0.115–0.117 |
| `vorbis-q4` | 1/17 | 1/17 | 0.125 |

The single `vorbis-q4` hit is the only non-AAC flag anywhere in the ladder.

## Why `z > 6.914` is triage grade, not proof grade

The pooled zero-false-positive threshold `z > 6.914` is exactly the *maximum
observed* genuine track z over 197 tracks — an in-sample maximum, which is not
a false-positive rate. Fitting a Gumbel to the 17 genuine **album-max** z values
(`q3d_out.txt`):

```
  Gumbel fit to the 17 genuine ALBUM-MAX z values: mu=5.598 beta=0.580
  (album maxima observed: 4.58 4.80 4.97 5.17 5.28 5.54 5.78 5.98 6.21 6.28
                          6.30 6.47 6.51 6.64 6.66 6.77 6.91)
    P(album-max z >  6.9140) = 9.84e-02  -> expected FP albums per 5000:  492.1
    P(album-max z >  7.2597) = 5.55e-02  -> expected FP albums per 5000:  277.5
    P(album-max z >  8.2968) = 9.52e-03  -> expected FP albums per 5000:   47.6
    P(album-max z > 10.3710) = 2.68e-04  -> expected FP albums per 5000:    1.3
    P(album-max z > 12.0000) = 1.62e-05  -> expected FP albums per 5000:    0.1
```

Corroborated empirically on the wild arm — 1,136 unlabelled real peer FLACs
from the quarantine trees (`q3e_out.txt`):

```
  z >  6.914: genuine control 0/197 (0.00%)   wild 14/1136 (1.23%)
  z >  8.300: genuine control 0/197 (0.00%)   wild  1/1136 (0.09%)
  z > 12.000: genuine control 0/197 (0.00%)   wild  0/1136 (0.00%)
```

A separate GPD tail fit at four thresholds (`q3e_out.txt`, top) puts
`z > 6.914` between 0 and 256 FP albums per 5000 depending on the threshold
choice `u` — the instability itself is the finding. `z > 12` is stable under
every fit and under the wild arm.

## Threshold stability — why `proba` was rejected and `z` retained

`q3b_out.txt` § A:

```
proba: pooled zero-FP lambda over all 17 albums = 0.1082
  LOO lambda range across the 17 held-out fits: 0.1046 .. 0.1082  (spread 3.3%)
  albums that must be removed before lambda halves: 2  (removed: ['8918', '8919'])
  top-5 genuine albums by max: 8918=0.1082, 8919=0.1046, 8917=0.0252, 8930=0.0228, 8929=0.0225
  ratio max/2nd = 1.034   max/median-album = 5.012

z: pooled zero-FP lambda over all 17 albums = 6.9140
  LOO lambda range across the 17 held-out fits: 6.7660 .. 6.9140  (spread 2.1%)
  albums that must be removed before lambda halves: 16
  top-5 genuine albums by max: 8930=6.9140, 8923=6.7660, 8929=6.6560, 8925=6.6420, 8934=6.5120
  ratio max/2nd = 1.022   max/median-album = 1.114
```

The `proba` λ is held up by exactly two albums (8918 Tri Repetae, 8919 Feed Me
Weird Things) whose genuine maxima are 4–5× every other album's; the third
highest is 0.0252. A threshold with that shape is not a property of the
population. The `z` λ sits 11 % above the median album — a real distribution.

`q3b_out.txt` § B, wild-arm firing rate at those λ:

```
  proba  L=0.1082   tracks over:    0/1136 ( 0.0%)   folders with >=1 over: 0/115 ( 0.0%)
  z      L=6.9140   tracks over:   14/1136 ( 1.2%)   folders with >=1 over: 11/115 ( 9.6%)
```

## The NAC probe and `mode=low` — both dead

`q1_out.txt`, paired arm, 15 albums, 182 genuine + 182 Apple CVBR-256 tracks:

```
--- TRACK-LEVEL AUC (launder = positive) ---
  proba          AUC=0.9952
  z              AUC=0.9933
  nac_z          AUC=0.7597
  proba*z        AUC=0.9987
--- COMBINED SCORES ---
  nac_z*z        AUC=0.9928      nac_z+z       AUC=0.9954
  nac_z*proba    AUC=0.9967      min(nac_z,z)  AUC=0.7605      z/nac_z  AUC=0.8776
```

Every NAC combination is at or below `proba*z`, and the two that lean on
`nac_z` most (`min`, `z/nac_z`) collapse toward the NAC probe's own 0.76.

`q2_out.txt`, `mode=high` vs `mode=low` frame selection:

```
  proba HIGH  GEN med=0.0180 max=0.1082  |  LAU med=0.1198 max=0.2207  AUC=0.9952
  proba LOW   GEN med=0.0315 max=1.0000  |  LAU med=0.0980 max=1.0000  AUC=0.6437
  z HIGH      GEN med=4.4750 max=6.7660  |  LAU med=28.5980 max=31.1340  AUC=0.9933
  z LOW       GEN med=4.3230 max=9.7380  |  LAU med=5.6020 max=30.0250  AUC=0.6586
```

`proba` at `mode=low` reaches exactly 1.0000 on both classes — a saturated
statistic, not a discriminator. The offset-agreement between the two modes is
a real signal (36.3 % exact on Apple against 0.5 % genuine, chance = 0.1 %) but
scores only AUC 0.679 on its own and adds nothing to `mode=high` alone.

## The final ladder and the gate union

`q3f_out.txt`. FP evidence: 197 genuine control tracks / 17 albums under the
album rule, 1,136 wild peer FLACs in 115 folders, and the analytic uniform-offset
floor.

```
rule                            genFP  wild fold  analytic FP/5000   aacffm-256   aacffm-320 qaac-cvbr256 qaac-cvbr320   apple-arm2
BASELINE proba 2nd-trk > 0.05     0/17        n/a               n/a         2/17         1/17        17/17        17/17        15/15
z max > 6.914  (naive 0-FP)       0/17     11/115       z-tail only        12/17        12/17        17/17        17/17        15/15
z max > 12.0                      0/17      0/115       z-tail only         4/17         5/17        17/17        17/17        15/15
offset k>=3                       0/17      1/115             1.049         7/17         5/17        17/17        17/17        15/15
offset k>=4                       0/17      1/115             0.002         4/17         4/17        17/17        17/17        15/15
offset k>=4 OR z>12.0             0/17      1/115             0.002         6/17         7/17        17/17        17/17        15/15
offset k>=3 OR z>8.297            0/17      1/115             1.049        10/17         9/17        17/17        17/17        15/15
```

Union with the frozen spectral gate at T = 62, over the 11 genuine-provable
44.1 k/16 albums. `esc` is how many launder albums reach PROOF spectrally;
the cells are how many of those **also** escape Derrien (lower is better):

```
variant          esc   BASELINE proba 2nd>0.05      offset k>=4    offset k>=4 OR z>12.0   offset k>=4 OR z>6.914
aacffm-256      10/11            8/10 ( 80.0%)     7/10 ( 70.0%)          5/10 ( 50.0%)            3/10 ( 30.0%)
aacffm-320      10/11            9/10 ( 90.0%)     7/10 ( 70.0%)          4/10 ( 40.0%)            2/10 ( 20.0%)
qaac-cvbr256    10/11            0/10 (  0.0%)     0/10 (  0.0%)          0/10 (  0.0%)            0/10 (  0.0%)
qaac-cvbr320    10/11            0/10 (  0.0%)     0/10 (  0.0%)          0/10 (  0.0%)            0/10 (  0.0%)
genuine         11/11           11/11 (100.0%)    11/11 (100.0%)         11/11 (100.0%)           11/11 (100.0%)
null-flac       11/11           11/11 (100.0%)    11/11 (100.0%)         11/11 (100.0%)           11/11 (100.0%)
```

`qaac-tvbr91`, `qaac-abr192` and `qaac-cbr128` have `esc = 0/11` — the spectral
gate already denies every one of them, so Derrien is not needed there.

## How it was run

From the repository root, in the pinned dev shell. Python 3.14.6 standard
library only (no numpy dependency in this thread). Every script is read-only
over `../`.

```bash
nix-shell --run "python3 load.py"             # -> all.tsv (committed here as all.tsv.gz)
nix-shell --run "python3 explore.py"          # -> explore_out.txt
nix-shell --run "python3 q1_nac.py"           # -> q1_out.txt
nix-shell --run "python3 q2_mode.py"          # -> q2_out.txt
nix-shell --run "python3 q3_frontier.py"      # -> q3_out.txt
nix-shell --run "python3 q3b_stability.py"    # -> q3b_out.txt
nix-shell --run "python3 q3c_offset.py"       # -> q3c_out.txt
nix-shell --run "python3 q3d_tail.py"         # -> q3d_out.txt
nix-shell --run "python3 q3e_final.py"        # -> q3e_out.txt
nix-shell --run "python3 q3f_consolidate.py"  # -> q3f_out.txt
```

`explore_out.txt`, `q1_out.txt`, `q3b_out.txt` and `q3d_out.txt` were captured
by re-running the frozen scripts over `all.tsv` at commit time (the analysis
session printed them to the terminal without redirecting); the other four are
the run's own transcripts. All ten scripts are deterministic over `all.tsv`,
which is itself deterministic over the committed `../derrien/res_*.tsv.gz` and
`../launder-matrix/derrien-*.tsv.gz`.

Every script does `sys.path.insert(0, ...)` on its scratch directory and imports
`engine`, so a scratch copy must have every `.py.frozen` renamed back to `.py`
and the two hardcoded absolute paths in `engine.py.frozen` / `load.py.frozen`
pointed at it.

## Cohorts

| name | contents |
|---|---|
| `ALB17` | the 17 launder-matrix albums with a genuine Derrien baseline (44.1 kHz / 16 bit) — `8916`–`8935` less the two hi-res |
| `ALB15` | the 15 of those the `../derrien/` paired arm covers (less `8930`, `8935`) |
| wild arm | 1,136 unlabelled real peer FLACs in 115 folders from `../derrien/res_wild_high.tsv.gz` |

Derrien cannot analyse 96 kHz input, so the two hi-res albums have no genuine
baseline anywhere and are excluded throughout.

## Files

`.py.frozen` files are the exact scripts that produced the data. **The
`.frozen` suffix is deliberate and load-bearing** — it keeps them out of
Pyright, Ruff and Vulture. They are evidence, not maintained source. **Do not
"fix" them.**

| file | role |
|---|---|
| `load.py.frozen` | normalizes every Derrien measurement in `../derrien/` and `../launder-matrix/` into one long table → `all.tsv` |
| `engine.py.frozen` | shared cohort definitions, track scores, album aggregators, and the leave-one-album-out evaluator that enforces the deployability rule (one global threshold, album statistic from the album's own tracks, LOO-calibrated) |
| `explore.py.frozen` | per-variant track distributions of `proba` and `z`, and the genuine per-album maxima → `explore_out.txt` |
| `q1_nac.py.frozen` | Q1 — does the never-scored NAC probe separate anything → `q1_out.txt` |
| `q2_mode.py.frozen` | Q2 — `mode=high` vs `mode=low`, offset agreement, LOO album eval → `q2_out.txt` |
| `q3_frontier.py.frozen` | Q3 — score × aggregator × threshold frontier → `q3_out.txt` |
| `q3b_stability.py.frozen` | Q3a — LOO threshold stability, wild firing rate, gate union with the z rule → `q3b_out.txt` |
| `q3c_offset.py.frozen` | Q3b — album offset concentration, the combined frontier, LOAO → `q3c_out.txt` |
| `q3d_tail.py.frozen` | Q3c — Gumbel tail fit on genuine album-max z, and the analytic offset FP floor → `q3d_out.txt` |
| `q3e_final.py.frozen` | Q3d — GPD tail fit, wild corroboration, the final operating frontier → `q3e_out.txt` |
| `q3f_consolidate.py.frozen` | Q3e — reproducibility across the two Apple builds, the final ladder, the gate union → `q3f_out.txt` |

### `all.tsv.gz` — the normalized long table

Written by `load.py.frozen`, **with a header row**, gzipped for commit. One row
per successfully measured (file, frame-selection mode); rows with a non-empty
`err` in the source are dropped.

| column | meaning |
|---|---|
| `src` | source filename — `res_gt_high.tsv.gz`, `res_gt_low.tsv.gz`, `res_wild_high.tsv.gz`, or `derrien-<tag>.tsv.gz` |
| `arm` | `gt` (the `../derrien/` paired arm), `wild`, or `mx:<tag>` for a launder-matrix tag |
| `variant` | variant id; the paired arm's launder class is normalized to `gtarm-qaac-cvbr256` |
| `rid` | request id (album), empty on the wild arm |
| `mode` | `high` or `low` frame selection |
| `path` | measured file path |
| `sr`, `dur` | sample rate, duration seconds |
| `proba` | the detector statistic at the best offset |
| `median`, `std` | median and standard deviation over the offset sweep |
| `z` | `(proba − median) / std` |
| `offset` | argmax MDCT frame offset in samples, 0–1023 |
| `pmode` | channel combination that produced the value (`LR` / `MS`) |
| `nac_z` | the NAC probe statistic, populated only on the `gt` arm |

## Conclusions

1. **Ship-grade finding: the offset-concentration rule.** `k ≥ 4` tracks of an
   album sharing an MDCT frame offset is parameter-free, has an analytic false
   positive floor of ~0.0023 albums per 5000, fires on 0/17 genuine and 0/115
   wild folders that are not already suspicious, and catches 100 % of four
   Apple CoreAudio variants plus 16/17 of the fifth. It is the only statistic
   in this research whose false-positive rate is a *calculation* rather than a
   fitted threshold.
2. **The Apple/CoreAudio family is closable at proof grade.** This is the
   direct answer to residual 1 of `../README.md` § "Known residuals" and to
   `../apple-arm/`: the spectral gate genuinely cannot see Apple CVBR-256, and
   it does not have to. 0 of 10 Apple launders survive the spectral ∪ Derrien
   union.
3. **The union table in `../launder-matrix/FINAL_REPORT.txt` § D understates
   what was already measured**, because its `ORDER` list silently dropped the
   Apple variants. Nothing new had to be measured to close them — only scored.
4. **Two statistics are retired.** The NAC probe column and the `mode=low`
   sweep cost roughly half the detector's runtime and contribute nothing. Any
   future production port should compute `mode=high` only and drop NAC.
5. **`z` is a triage signal, not a proof signal.** The naive zero-FP threshold
   is an in-sample maximum; honest extrapolation prices it at ~490 false
   positives per 5000 albums. `z > 12` is the conservative operating point.
6. **The permanently open spectral residual is now precisely named:**
   `lame-v0`, `vorbis-q10`, and roughly half of `ffmpeg`-native AAC 256/320.
   The first two carry no AAC lattice, so Derrien cannot help; the third is
   half-visible because ffmpeg's priming does not concentrate offsets the way
   CoreAudio's does.
7. **Nothing here changes production.** No constant, threshold or leg is
   derived into shipped code by this directory. It identifies what a future
   detector leg would be, and prices it.

## What is NOT here

No audio, and no re-measurement of the detector. The detector implementation,
its validation against the JAES-2018 reference, and the raw measurement arms
live in `../derrien/`; the launder-matrix Derrien sweeps live in
`../launder-matrix/`. This directory only re-analyses those tables.
