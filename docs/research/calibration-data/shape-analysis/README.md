# The slice vector as a shape — is there information the scalar legs threw away?

Measured 2026-07-31. **This directory draws conclusions.** It answers open
question 2 of `../launder-matrix/README.md` § "Unexplored directions".

## The question and the verdict

**Question.** Every leg of the frozen proof gate reduces the 16–20 band-RMS
values to one scalar — cliff position, ceiling step, ultrasonic deficit, HF
deficit. The *rolloff shape itself* has never been examined. Twenty-four
level-invariant shape features were built over the 20-point vector and
evaluated against the classes that escape the gate.

**VERDICT: NO. Within the production spectral instrument the transparent-escape
class is unreachable — not under-tuned, unreachable.**

1. **The mechanism is not there.** After level normalization the AAC/Apple
   paired shape delta is **≤ 0.24 dB in all 20 bands**, on 197 paired tracks
   across 17 albums. Vorbis-q6 moves −7.65 dB at 20 kHz on the same
   normalization; `aacffm-192`, which the gate already catches, moves
   −26.08 dB.
2. **The signal-to-noise ratio settles it.** Codec shape displacement against
   genuine-album-to-genuine-album shape distance, in the same 20-dim anchored
   space: `aacffm-256` **0.031**, `aacffm-320` **0.036**, `qaac-cvbr256`
   **0.041**, `qaac-cvbr320` **0.040**, `t-apple256-flac` **0.077 / 0.086**.
   The codec's footprint is 25–40× *smaller* than the ordinary distance
   between two different genuine albums, and **0.0 %** of genuine album pairs
   are closer together than the median codec displacement.
3. **A black box does no better.** Leave-one-album-out logistic regression on
   the full 19-dim adjacent-band-difference vector — no threshold, no
   legibility constraint — scores **AUC 0.408–0.590** on the six AAC/Apple
   escape classes while scoring **1.000** on `lame-cbr320` and `aacffm-192`
   and 0.902 on `vorbis-q5`.
4. **The structural law.** Every class the frozen gate catches has shape
   SNR > 1; every class that escapes has SNR < 1. Transparency is now a
   *measured property of the production measurement granularity*, not a
   suspicion: at these rates the encoder leaves nothing in a 500 Hz-resolution
   band-RMS vector for any function of that vector to find.

Two corpus facts recorded here because they bound what "independent
replication" can mean on the Apple family:

- **`launder-matrix/qaac-cvbr256` and `apple-arm/t-apple256-flac` (arm A) are
  the same dataset under two names.** 215 of 215 paired measurements are
  byte-identical — max |slice difference| and max |ref difference| are both
  exactly `0.000e+00`. qaac is deterministic and both runs used the same
  corpus and the same argv. The only album-independent Apple evidence is
  **arm B, 20 albums**.
- **The Apple shape signal concentrates on albums that are useless.** The
  six matrix albums whose *genuine* twin is already DENIED by the gate carry
  median shape displacement 2.363 (max 4.101); the eleven whose genuine twin
  reaches proof — the only population a new discriminator could help — carry
  median 0.160 (max 1.600). `corr(d_shape, genuine U) = +0.62`: the
  displacement tracks how HF-poor the album already is.

## How it was run

From the repository root, in the pinned dev shell. Python 3.14.6, numpy 2.5.1,
scipy 1.18.0. Every script is read-only over `../`.

```bash
nix-shell --run "python3 load.py"        # corpus join, row counts
nix-shell --run "python3 pair.py"        # -> OUT_pair.txt
nix-shell --run "python3 integrity.py"   # -> OUT_integrity.txt
nix-shell --run "python3 effect.py"      # -> OUT_effect.txt
nix-shell --run "python3 eval.py"        # -> OUT_eval.txt  + loo_zero_fp.json
nix-shell --run "python3 eval2.py"       # -> OUT_eval2.txt
nix-shell --run "python3 eval3.py"       # -> OUT_eval3.txt
nix-shell --run "python3 final.py"       # -> OUT_final.txt
nix-shell --run "python3 snr.py"         # -> OUT_snr.txt
nix-shell --run "python3 repro.py"       # -> OUT_repro.txt
nix-shell --run "python3 dup.py"         # -> OUT_dup.txt
nix-shell --run "python3 tail.py"        # -> OUT_tail.txt
nix-shell --run "python3 gate.py"        # -> OUT_gate.txt
```

`eval.py`, `eval2.py`, `eval3.py`, `final.py`, `snr.py`, `repro.py`, `tail.py`
and `gate.py` import each other and `feat` / `load`, so a scratch copy must have
every `.py.frozen` renamed back to `.py`.

## The feature battery

`feat.py.frozen` builds 24 features from `n[k] = slice[k] − ref`, each either a
difference of bands, a second difference, or a residual from a fit — i.e. every
one is invariant to the album's absolute level:

| group | features |
|---|---|
| slopes | `slope_lo` (12→16 k), `slope_mid` (16→19.5 k), `slope_top` (19.5→21.5 k), `knee_mid_lo`, `knee_top_mid` |
| curvature | `curv_all`, `curv_top`, `curv_min`, `curv_max` |
| roughness | `rough_all`, `rough_top`, `rough_lo`, `rough_ratio` |
| fit residuals | `resid_lin`, `resid_iso` (isotonic / PAVA), `quad_c`, `resid_quad` |
| top-octave character | `top_sd`, `top_span`, `maxdrop`, `maxdrop_top` |
| level context (not shape; kept as the ceiling reference) | `_U`, `_d18_195`, `_drop_hi` |

## Key results

### 1. Paired mechanism, per band (`OUT_pair.txt`)

197 paired tracks / 17 albums, 44.1 k/16 only. `s[k] = n[k] − n[12 kHz]` is pure
shape. Median over tracks of (launder − genuine), dB:

```
variant        n      12.0   14.0   16.0   18.0   19.0   19.5   20.0   20.5   21.0   21.5
aacffm-256    197     0.00   0.00   0.02   0.02   0.05   0.03   0.04   0.11   0.12   0.24
aacffm-320    197     0.00  -0.01   0.00   0.00   0.02   0.02   0.04   0.11   0.12   0.18
qaac-cvbr256  197     0.00  -0.01  -0.02  -0.02  -0.01  -0.00  -0.07  -0.05  -0.04  -0.04
qaac-cvbr320  197     0.00  -0.01  -0.02  -0.03  -0.04  -0.04  -0.09  -0.08  -0.08  -0.08
null-flac     197     0.00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   0.00
lame-v0       197     0.00   0.01  -0.07  -0.37  -0.89  -1.51  -2.09  -2.50  -3.52  -4.75
vorbis-q6     197     0.00   0.05   0.12  -0.10  -2.01  -5.09  -7.65  -5.58  -5.59  -6.97
--- caught by the gate, for contrast ---
lame-cbr320   197     0.00   0.00   0.00   0.00  -0.01  -0.52 -11.62 -24.92 -20.55 -16.43
aacffm-192    197     0.00   0.00   0.01   0.03  -1.78 -24.35 -26.08 -22.92 -19.52 -15.63
opus-256      197     0.00  -0.01   0.01  -0.00  -0.09  -0.60 -10.48 -22.89 -19.68 -16.88
```

The four transparent AAC rows are inside ±0.24 dB everywhere. `null-flac` — the
bitwise-null control — is exactly 0.00, which is what proves the pipeline is
measuring what it claims to.

Independent replication on the apple-arm's own corpora (`OUT_final.txt` § 1b):
arm A 215 tracks, arm B 303 tracks, arm 24-bit 215 tracks; every `s[k]` median
delta is within −0.71 to 0.00 dB, and arm B's single largest value (−0.71 at
21.5 kHz) is the only one past −0.31.

### 2. Signal-to-noise — the decisive number (`OUT_snr.txt`)

```
genuine albums: 114   genuine-vs-genuine shape distance (RMS dB/band over 20 bands)
  p10=2.373  median=6.461  p90=14.636

variant              n  d_codec med  d_codec p90   SNR med   pct of genuine pairs closer
aacffm-256          17        0.201        0.448     0.031                          0.0%
aacffm-320          17        0.234        0.453     0.036                          0.0%
qaac-cvbr256        17        0.265        2.668     0.041                          0.0%
qaac-cvbr320        17        0.259        2.700     0.040                          0.0%
apple256-flac(A)    17        0.497        2.882     0.077                          0.0%
apple256-flac(B)    20        0.557        3.932     0.086                          0.0%
vorbis-q10          17        1.179        2.552     0.183                          1.3%
lame-v0             17        2.878        5.933     0.445                         15.0%
vorbis-q8           17        3.902        5.985     0.604                         26.2%
vorbis-q7           17        4.221        6.516     0.653                         29.9%
vorbis-q6           17        4.393        7.542     0.680                         31.6%
--- caught by the gate ---
vorbis-q5           17        7.727       11.847     1.196                         59.5%
lame-cbr320         17        9.680       13.537     1.498                         73.3%
aacffm-192          17       12.003       14.893     1.858                         83.9%
```

**SNR > 1 ⇔ the gate catches it. SNR < 1 ⇔ it escapes.** The three caught
classes are the three above 1.0; every escape class is below. The boundary is
not a coincidence of thresholds — it is the point at which the codec's
footprint becomes comparable to ordinary album-to-album variation.

### 3. Effect sizes against the genuine cross-album spread (`OUT_effect.txt`)

Median paired delta divided by the genuine cross-album SD, 156 pooled genuine
control albums. For `qaac-cvbr256` / `qaac-cvbr320` the largest of all 24
features is **0.092 / 0.064** SD (`rough_top`, `resid_lin`); `aacffm-256` /
`aacffm-320` peak at **0.073 / 0.075** SD (`curv_top`). By contrast
`vorbis-q6` reaches −1.30 SD (`knee_mid_lo`) and +0.81 SD (`maxdrop_top`).

### 4. Deployability — one global threshold, leave-one-album-out

`OUT_eval.txt` prices a zero-FP-on-fold global threshold on 114 deduplicated
genuine control albums; `OUT_eval2.txt` adds AUC and quantile operating points;
`loo_zero_fp.json` is the machine-readable recall matrix keyed by
`(feature, direction)`.

`OUT_eval3.txt` § C is the decisive control. Every raw recall number is
contaminated, because the 17 launder albums are a fixed subset of the 114
genuine controls: a feature on which those particular albums happen to sit high
"detects" them with no codec information at all. The control runs the identical
LOO rule over **those same albums' own genuine measurements**, and
**NET = recall(launder) − recall(genuine twin)** is the only part the codec
produced. At the 5 % album FP budget the NET for `aacffm-256`, `aacffm-320`,
`qaac-cvbr256`, `qaac-cvbr320` and both apple-arm targets **never exceeds
+3 of 17** across all 24 features (the single largest is `qaac-cvbr320` on
`curv_top`; most cells are 0 or ±1) — while on the same rule `aacffm-192`
reaches +17/17 on `maxdrop` and `lame-cbr320` reaches +14/17 on `resid_lin`.

`OUT_eval3.txt` § D, the black-box ceiling:

```
variant             n_alb   LOO AUC   recall@5%FP  recall@10%FP
lame-v0                17     0.647          5/17          5/17
aacffm-256             17     0.423          1/17          2/17
aacffm-320             17     0.436          1/17          2/17
vorbis-q10             17     0.570          5/17          5/17
vorbis-q8              17     0.757          4/17         11/17
vorbis-q7              17     0.771          7/17         11/17
vorbis-q6              17     0.830         11/17         11/17
qaac-cvbr256           17     0.577          2/17          5/17
qaac-cvbr320           17     0.590          1/17          3/17
apple256-flac(A)       17     0.408          0/17          1/17
apple256-flac(B)       20     0.517          0/20          3/20
lame-cbr320            17     1.000         17/17         17/17
aacffm-192             17     1.000         17/17         17/17
vorbis-q5              17     0.902         12/17         13/17
```

Three of the six AAC/Apple rows score **below 0.5** — the model is
anti-correlated, i.e. fitting corpus noise.

### 5. Corpus integrity (`OUT_integrity.txt`, `OUT_dup.txt`, `OUT_repro.txt`)

- **28 distinct genuine albums appear in more than one corpus.** `eval.py`
  deduplicates by track fingerprint before pricing false positives, so no
  physical album is both a control and a recall target in the same fold.
- **The four committed arms store slice dB rounded to 0.1**, while the matrix
  and apple-arm store full precision. Roughness and curvature are differences
  of differences, so this is a real perturbation; quantified by re-rounding
  748 full-precision genuine rows, the worst bias is −0.0065 dB (`curv_min`)
  and the worst RMS deviation 0.066 dB — an order of magnitude below every
  effect discussed above.
- **The decode-path audit** (`OUT_final.txt` § 0) confirms every row used is a
  FLAC measured through the sox-native path; the 10,608 native-lossy rows
  (`.mp3`/`.opus`/`.ogg`/`.m4a`) are excluded, because the README-documented
  `_ffmpeg_to_wav @48k` path skews ultrasonic statistics by +3.09 dB.
- **`OUT_repro.txt`** reproduces `../apple-arm/README.md`'s published numbers
  from this harness (19.5 kHz genuine −46.73 / launder −47.87 against the
  README's −46.7 / −47.9; pooled A+B paired `U` delta mean +1.88, median
  +0.57, |d| < 1 on 19/37) — harness fidelity, checked before any conclusion
  was drawn.
- **`OUT_dup.txt`** is the 215/215 byte-identity finding above.

### 6. Where the Apple signal lives (`OUT_tail.txt`, `OUT_gate.txt`)

```
albums whose GENUINE twin reaches proof (the only population a discriminator can help): n=11
   their shape displacement: median 0.160  max 1.600
albums whose genuine twin is already DENIED (not provable at all): n=6
   their shape displacement: median 2.363  max 4.101
```

`corr(d_shape, genuine U) = +0.620` (matrix) and `+0.599` (apple-arm A). The
albums that move are Tabula Rasa (4.101), Kind of Blue (2.641), the Goldberg
Variations (2.477), Ambient 1 (2.249) — HF-poor acoustic and ambient material
whose genuine twin the gate already denies.

## Conclusions

1. **The shape axis is closed for the transparent-escape class.** Not a
   threshold problem: the mechanism is ≤0.24 dB, the SNR is 25–40× below the
   genuine confound, the NET recall is zero and the unconstrained black box is
   at or below chance. Two independent falsifications now exist for this
   class — this one and `../homogeneity/`.
2. **SNR > 1 is the structural boundary of the production instrument.** The
   frozen gate catches exactly the classes whose shape displacement exceeds
   ordinary album-to-album shape distance. That is a statement about the
   500 Hz band-RMS representation, not about the gate's tuning, and it is why
   no further scalar or vector function of the same measurement will move the
   AAC 256/320 or Apple CVBR classes.
3. **Any future Apple claim must be scoped to arm B.** The matrix and
   apple-arm A Apple datasets are the same 215 measurements. Statements of the
   form "replicated on two arms" are false for arm A and true only for the
   20-album arm B.
4. **The Apple shape displacement, where it exists, is on unprovable albums.**
   It correlates with how HF-poor the album already is, so it appears exactly
   where the gate already denies the genuine twin — no reachable operating
   point exists.
5. **Nothing here changes production.** No constant, threshold or leg is
   derived from this directory. The one durable engineering artifact is the
   SNR framing: a proposed spectral discriminator can be priced in one run by
   comparing its codec displacement against the genuine cross-album distance
   in the same space, before any threshold is discussed.

## Files

`.py.frozen` files are the exact scripts that produced the data. **The
`.frozen` suffix is deliberate and load-bearing** — it keeps them out of
Pyright, Ruff and Vulture. They are evidence, not maintained source, and they
carry hardcoded absolute scratch paths. **Do not "fix" them.**

| file | role |
|---|---|
| `load.py.frozen` | joins `results*` + `extended*` into 20-point vectors across all 12 source tags of `../`, `../launder-matrix/` and `../apple-arm/` |
| `feat.py.frozen` | the 24-feature level-invariant shape battery |
| `pair.py.frozen` | step 1 — paired per-band mechanism check → `OUT_pair.txt` |
| `integrity.py.frozen` | cross-corpus album overlap + 0.1 dB storage-rounding perturbation → `OUT_integrity.txt` |
| `effect.py.frozen` | step 2 — paired effect size against the genuine cross-album SD → `OUT_effect.txt` |
| `eval.py.frozen` | step 3 — LOAO zero-FP global-threshold table → `OUT_eval.txt`, `loo_zero_fp.json` |
| `eval2.py.frozen` | step 3b — AUC + LOAO operating curves at real FP budgets → `OUT_eval2.txt` |
| `eval3.py.frozen` | step 3c — the NULL-RECALL control and the black-box ceiling → `OUT_eval3.txt` |
| `final.py.frozen` | step 4 — decode-path audit, apple-arm paired replication, track-level tables → `OUT_final.txt` |
| `snr.py.frozen` | step 5 — codec shape displacement vs genuine cross-album shape distance → `OUT_snr.txt` |
| `repro.py.frozen` | harness fidelity against published `../apple-arm/README.md` numbers → `OUT_repro.txt` |
| `dup.py.frozen` | are the two Apple datasets independent encodes or the same bits → `OUT_dup.txt` |
| `tail.py.frozen` | which albums actually show an Apple displacement → `OUT_tail.txt` |
| `gate.py.frozen` | are those albums provable in the first place → `OUT_gate.txt` |

Every OUT file's first line is the dev shell's own banner.

### `loo_zero_fp.json`

A JSON object keyed by the string `"('<feature>', '<dir>')"` where `dir` is
`hi` or `lo`, mapping to a 14-element list of `"<hits>/<n>"` recall strings in
this fixed target order:

```
lame-v0, aacffm-256, aacffm-320, vorbis-q10, vorbis-q8, vorbis-q7, vorbis-q6,
qaac-cvbr256, qaac-cvbr320, apple256-flac(A), apple256-flac(B),
lame-cbr320, aacffm-192, vorbis-q5
```

The 11th entry has denominator 20 (apple-arm B); the rest are 17. Read it
together with `OUT_eval3.txt` § C — these are the **contaminated** recalls, and
the NET table is the one that means something.

## What is NOT here

No audio and no re-measurement. This directory is derived entirely from the
committed measurement tables in `../`, `../launder-matrix/` and
`../apple-arm/`.
