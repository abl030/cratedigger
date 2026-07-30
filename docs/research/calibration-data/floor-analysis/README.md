# Floor and precondition analysis — re-reading the committed arms plus the new matrix

Run 2026-07-30. **Descriptive record only.** This directory records a set of
questions that were asked of the already-committed calibration arms and the new
launder matrix, the exact scripts that asked them, and the raw output. It draws
no conclusion and proposes no change; the analysis is unfinished.

## What was done

Nothing new was measured. Two feature files were derived from data that already
exists — the four committed calibration arms in the parent directory and the
new launder matrix in `../launder-matrix/` — and a sequence of small scripts
(`q1a` … `q3b`) interrogated them. Each script's stdout is saved verbatim as
`OUT_<name>.txt`.

Three broad questions were asked. The names are the scripts' own.

- **Q1 — preconditions.** The gate's ultrasonic leg denies some genuine
  lossless albums (albums with little natural ultrasonic content). Is there a
  *precondition* — a property of the album, independent of codec — under which
  the ultrasonic leg's verdict should be treated as uninformative ("untestable")
  rather than as a denial? Scripts `q1a`–`q1i`, `q1split`, `q1final`.
- **Q2 — floors.** Given a measurement, what is the highest bitrate the
  evidence can *rule out* — i.e. what lower bound on quality can be asserted,
  rather than what class can be guessed? Estimated leave-one-album-out.
  Scripts `q2feat`, `q2look`, `q2run`, `q2run2`, `q2run3`, `q2u`, `q2wall`.
- **Q3 — a three-state framing.** PROVED / BOUNDED / DENIED (+ UNTESTABLE)
  instead of a binary gate, tabulated over both corpora. Scripts `q3`, `q3b`.

## Inputs

`load.py.frozen` reads the committed arm tables and the matrix tables through
one loader, and re-implements the gate's leg logic (`window_legs`, `gate`) in a
form that can be driven per-window. Constants there mirror the frozen scorer:
`S_STEP = 15.0`, `C_CONS = 0.5`, `INWINDOW_CLIFF_HZ = 19000`,
`INWINDOW_FRAC = 0.6`.

| dataset key | source |
|---|---|
| `TRAIN`, `R1`, `R2`, `R3` | `../results{,-holdout,-holdout2,-holdout3}.tsv.gz` + matching `extended*` + `multiwin*` |
| `LOCAL`, `LOCALW`, `APPLE` | `../launder-matrix/results-{local,localw,apple}.tsv.gz` + matching `extended-*` |

The arms have multiwin data (four windows at 0/60/120/180 s); the matrix does
not, so matrix rows are window-0 only. Where the two are compared, the
comparison is made in **window-0 (single-window) mode**, which is also what
production does.

## The two feature files

### `feats.json.gz` — 2251 rows, one per (dataset, album, variant)

`{'TRAIN': 578, 'R1': 240, 'R2': 432, 'R3': 384, 'LOCAL': 399, 'LOCALW': 85,
'APPLE': 133}`. Built by `feat.py.frozen` from the **20 slice values**
(16 production + 4 extension) of window 0. Every value is the mean over the
album's tracks unless stated.

| key | meaning |
|---|---|
| `ds`, `slug`, `variant`, `n` | dataset, album slug, variant id, track count |
| `ref` | 1–4 kHz reference level, dB |
| `abs_b12_14`, `abs_b14_16`, `abs_b16_18`, `abs_b18_195`, `abs_b20_215`, `abs_bU` | absolute mean level in that band, dB |
| `D_b12_14` … `D_bU` | the same bands as a **deficit** below `ref` (`ref − level`) |
| `U` | `ref − mean(20.5–21.5 kHz)` — the proof leg's own statistic |
| `U_sd` | per-track standard deviation of `U` within the album |
| `slope_mid` | dB per 500 Hz over 12–19.5 kHz |
| `drop_hi` | 18–19.5 kHz level minus 20.5–21.5 kHz level |
| `step`, `cons` | ceiling-leg step size and cross-track consensus, window 0 |
| `cliff_frac` | fraction of tracks with a cliff ≤19 kHz |
| `cliff_med` | median cliff Hz over tracks that had one (`null` if none) |
| `hfdef` | mean HF-deficit dB |
| `denied@62`, `legs@62`, `denied@59.5`, `legs@59.5` | **multiwin** gate verdict (arms only have >1 window) |
| `w0denied@62`, `w0legs@62`, `w0denied@59.5`, `w0legs@59.5` | **single-window** verdict, comparable across both corpora |

Band index ranges, for reference: `b12_14` = 12000–14000, `b14_16` =
14500–16000, `b16_18` = 16500–18000, `b18_195` = 18500–19500, `b20_215` =
20000–21500, `bU` = 20500–21500.

### `q2feats.json.gz` — 5917 rows, one per (dataset, album, variant)

`{'TRAIN': 1802, 'R1': 795, 'R2': 1431, 'R3': 1272, 'LOCAL': 399,
'LOCALW': 85, 'APPLE': 133}`. Built by `q2feat.py.frozen` from the **16
production slices only**, so it covers every variant including ones with no
extension data.

| key | meaning |
|---|---|
| `ds`, `slug`, `variant`, `n` | as above |
| `d12_14`, `d14_16`, `d16_18`, `d18_195` | band deficits below the 1–4 kHz reference |
| `rel18` | 18–19.5 kHz level relative to the 12–14 kHz level |
| `edge_med`, `edge_p25` | median / 25th-percentile top-of-window edge frequency |
| `cliff_med`, `cliff_min`, `cliff_frac` | cliff statistics |
| `hfdef` | mean HF deficit |
| `kbps` | the variant's nominal bitrate label, for scoring floor correctness |

## Output files

Each `OUT_<name>.txt` is the verbatim stdout of `<name>.py.frozen`. All were
run from the repository root as `nix-shell --run "python3 <name>.py"` with the
output redirected.

| file | what the script printed |
|---|---|
| `OUT_q1a.txt` | distribution (min/p10/med/p90/max) of every feature, split by gate outcome class |
| `OUT_q1b.txt` | paired `abs_bU`, each album's own genuine vs its launder, per fraud class, 100 arm albums |
| `OUT_q1c.txt` | baseline window-0 gate counts on the arms |
| `OUT_q1d.txt` | correlation of `U` with the preserved-band features on genuine albums |
| `OUT_q1e.txt` | which leg combinations fire, per class |
| `OUT_q1f.txt` | multiwin gate + a codec-invariant midband-deficit precondition |
| `OUT_q1g.txt` | sweep of the ultrasonic threshold `T` as the baseline knob |
| `OUT_q1h.txt` | the two candidate precondition features validated per arm and per class |
| `OUT_q1i.txt` | the no-wall precondition swept over the full variant set, arms + matrix |
| `OUT_q1split.txt` | the same, split by launder severity (near-transparent vs aggressive) |
| `OUT_q1final.txt` | the consolidated Q1 run: (A) shipped multiwin gate on 100 arm albums, (B) window-0 gate over the full variant set |
| `OUT_q2look.txt` | per-variant feature medians, sorted by nominal bitrate |
| `OUT_q2run.txt` | leave-one-album-out floor estimator, `rel18` only |
| `OUT_q2run2.txt` | LOO floor from ref-normalised absolute-deficit statistics, three codec universes |
| `OUT_q2run3.txt` | floor conditioned on affirmative cliff evidence, plus the album-heterogeneity diagnosis |
| `OUT_q2u.txt` | LOO floor from `U` itself |
| `OUT_q2wall.txt` | what floor a wall (ceiling-leg) signature supports |
| `OUT_q3.txt` | three-state table on the 100 arm albums |
| `OUT_q3b.txt` | three-state table on the new 19-album matrix, wide codec set |

## Measured observations worth carrying forward

Stated as measurements with their source, **not** as conclusions.

- **Album identity dominates codec effect on these statistics.**
  `OUT_q2run3.txt`: genuine-lossless `d18_195` across the 100 arm albums spans
  **min 27.3 / median 46.8 / max 70.3 — a 43.0 dB spread**. Computed from
  `feats.json.gz` for the proof leg's own statistic, window-0 `U` over the same
  100 genuine control albums spans **25.54 to 81.53 dB, a 55.99 dB range**.
  Paired per-album codec effects on `abs_bU` are a few dB by comparison
  (`OUT_q1b.txt` median deltas: `t-mp3128-flac` −22.0, `apple-cvbr256` −3.6,
  `lame-v0` −4.6, `vorbis-q9` −4.6).
- **68 of 100 albums look "better" as opus-32 than the median genuine album
  does as genuine**, on `d18_195` (`OUT_q2run3.txt`). The same file records 4/100
  for opus-12 and 40/100 for fdk-he1-64.
- The **leave-one-out floor estimators report high correctness** on their own
  terms (`OUT_q2run.txt` 5296/5300; `OUT_q2run2.txt` 5271/5300;
  `OUT_q2u.txt` 1629/1634; `OUT_q2wall.txt` 611/612) — but read the floors
  themselves, which are frequently the bottom of the ladder (96 kbps).
  `OUT_q3.txt` records where the violations concentrate: two held-out albums
  account for 30 of them.

## Frozen scripts

Every `.py.frozen` file is the exact script that produced the matching
`OUT_*.txt`. **The `.frozen` suffix is deliberate and load-bearing** — it keeps
these files out of Pyright, Ruff and Vulture. They are evidence, not maintained
source, and they carry hardcoded absolute scratch paths. **Do not "fix" them.**
`load.py.frozen`'s `CAL` constant points at this repository's
`docs/research/calibration-data`; its `MTX` constant points at a scratch path
that no longer needs to exist — repoint it at
`docs/research/calibration-data/launder-matrix` (and un-gzip, or teach `_open`
about the new suffixes) to re-run anything here.

## Unexplored directions

Open questions with the data that would answer them. **Not recommendations,
and no prediction that any of them will work.**

1. **Within-album homogeneity.** Every statistic in `feats.json.gz` except
   `U_sd` is a per-track value aggregated to an album mean, and `U_sd` is
   computed but never read by any script here. A laundered album went through
   one encoder at one setting; a genuine album's tracks are related only by
   mastering. Whether that difference is measurable is untested. Data: the
   per-track vectors behind these features already exist in the parent
   directory and in `../launder-matrix/`.
2. **The slice vectors as shapes.** Every feature in both feature files is a
   scalar reduction of the 16–20 band-RMS values — a band mean, a deficit, a
   slope, a step. The vector itself, as a rolloff *shape*, has never been
   examined. Data: ~60,000 measurements in the committed arms plus 7,681 rows
   in `../launder-matrix/`, all carrying full slice vectors.
3. **Cross-candidate agreement per request.** Nothing here uses the fact that
   the pipeline holds many independent peer copies of the same album — one live
   request has 226 candidates from 170 distinct peers. Whether independent
   copies sharing fine spectral structure says something about the *source*
   rather than the file is untested. Related: the convergence-signal proposal
   in the issue #829 comments (2026-07-27), never operationalised. Data:
   `album_quality_evidence` joined through `download_log` per request, in the
   live pipeline DB.
4. **The V0 probe data.** Prior work concluded it "clusters copies but does not
   rank them" and set it aside for lacking an ordering (`../probe_pair.tsv.gz`).
   Whether the clustering alone carries information was not pursued. Data:
   `v0_min_bitrate_kbps` / `v0_avg_bitrate_kbps` / `v0_median_bitrate_kbps` on
   thousands of live evidence rows.
5. **`multiwin` is unused for the matrix.** The arms carry four windows; the
   matrix carries one. Every cross-corpus comparison here is therefore made in
   single-window mode. What the extra windows would show on the matrix is
   unmeasured — the audio is gone, but rebuilding is a documented recipe
   (`../launder-matrix/README.md`).
6. **The three-state framing was tabulated and stopped.** `OUT_q3.txt` /
   `OUT_q3b.txt` are the only output; no threshold, boundary or policy was
   derived from them, and the `UNTESTABLE` rule they use
   (`nocliff & step < 8`) is one point on a sweep, chosen inside the script.

Items 1 and 3 are the two directions that would use album identity as a
control rather than fight it as a confound.
