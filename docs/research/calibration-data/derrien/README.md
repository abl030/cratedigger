# The Derrien port — an MDCT-lattice AAC-transcode detector, ported and measured

Measured 2026-07-30. **Descriptive record only.** This directory records that
a published AAC-transcode detector was ported to numpy, validated against the
author's own reference implementation, and run over Cratedigger corpora. It
draws no conclusion about whether the detector should be adopted, and none
should be inferred.

## What this is

Olivier Derrien's detector recovers the **MDCT frame lattice** an AAC encoder
left behind. AAC quantises MDCT coefficients on a fixed 1024-sample frame grid;
after decoding to PCM the grid is invisible in the waveform, but re-analysing
the PCM at the *right* offset recovers scalefactor-band structure that a signal
which was never AAC-encoded does not have. The statistic is a probability over
a sweep of all 1024 candidate offsets; a genuine file's sweep is flat, an
AAC-derived file's sweep has a spike at the true offset.

`aacdet.py.frozen` is a numpy port of the author's Matlab, plus a Yang-style
NAC (non-zero-MDCT-coefficient) frame-grid probe as a second, independent
statistic.

## Provenance and citation

`jaes2018_code.zip` is **the author's own reference implementation**,
downloaded publicly from <http://potion.prism.cnrs.fr/JAES2018.html>. It
contains four unmodified Matlab files: `main.m`, `MDCT.m`, `detect_aac.m`,
`init_aac.m`. `jaes2018_results.pdf` is the results archive published on the
same page. Both are redistributed here as the reference the port is checked
against.

`oracle.m` is **ours** — a small driver that calls the author's unmodified
`detect_aac.m` / `init_aac.m` / `MDCT.m` over a fixed set of offsets so the
port can be compared to it. Extract `jaes2018_code.zip` into a directory,
put `oracle.m` beside it, and run under Octave.

**Papers are cited, not redistributed** (copyright). Four were consulted:

- O. Derrien, "Detection of Genuine Lossless Audio Files: Application to the
  MPEG-AAC Codec", *J. Audio Eng. Soc.*, vol. 67, no. 3, pp. 116–123 (2019).
  The detector implemented here.
- "Lossless Audio Checker: A Software for the Detection of Upscaling,
  Upsampling, and Transcoding in Lossless Musical Tracks."
- R. Yang et al., on non-zero-MDCT-coefficient (NAC) frame-grid detection —
  the basis of the second probe in `aacdet.py.frozen`.
- "Analysing decompressed audio with the Inverse Decoder."

## Validation — re-run at capture time, not merely reported

Two independent checks, both re-run on 2026-07-30 during this capture and both
passing. Commands are exactly as run, from the repository root.

**1. Self-validation of the port's mathematics** — `validate.py.frozen`:

```
nix-shell --run "python3 validate.py"
```

```
PASS  MDCT == direct DCT-IV definition (up to constant)  ratio=1.000000 maxdev=1.37e-10
PASS  TDAC perfect reconstruction (sine window)  max abs err=1.12e-12
PASS  tau(s) formula is conservative vs the true null (never anti-conservative)
        worst rel err=0.224 over widths [4, 8, 12, 16, 20, 24, 28, 32]
PASS  synthetic AAC codec round trip -> detector peaks at the true offset
        argmax=313 true=313 peak=0.138 median=0.0111 z=29.6
PASS  white-noise control stays below the paper's lambda=0.031  max=0.0194 median=0.0114
REPORT  NAC probe on the synthetic lattice: best=425 (true=313) z=3.87
        noise control NAC z=3.43
OVERALL: PASS
```

**2. Vectorised port vs a literal Matlab transcription, on real audio** —
`parity.py.frozen` against `naive.py.frozen` (a deliberately unoptimised,
statement-by-statement transcription of `detect_aac.m`):

```
nix-shell --run "python3 parity.py"
...
worst |diff| over all probes: 0.000e+00
PARITY PASS
```

Fourteen probes over a genuine/launder pair, every one identical to the last
printed digit.

**3. Octave oracle — recorded, not re-runnable here.** `oracle_cmp.py.frozen`
compared the port against Octave running the author's unmodified Matlab, over
5 offsets on two files. The Octave reference outputs survive as
`oct_genuine.txt` / `oct_launder.txt` (`offset<TAB>probability`); the WAV
inputs those numbers were computed from do not, so this check cannot be
reproduced from what is committed. The reported result at the time was a worst
absolute difference of **3.3e-16**. Checks 1 and 2 above are the reproducible
half of the validation.

Environment: `pyenv.txt` records the exact Nix store paths of the Python
interpreter and every site-packages entry used for these runs.

## Data — what was measured

Two corpora, both measured through `measure.py.frozen`.

**The paired ground-truth arm.** 19 library FLAC albums (the same corpus as
`../launder-matrix/`, `manifest.json` there) and an Apple CoreAudio CVBR-256
launder of each track, built by `build_launders.py.frozen`:

```
library FLAC -> ffmpeg PCM WAV (native rate/depth) -> scp to the Windows VM
             -> qaac64 -s -v 256 -> scp back -> ffmpeg -sample_fmt s16 -c:a flac
```

215 genuine tracks + 215 launder tracks. **33 of each error out**: those are
the two 96 kHz albums (8920 = 22 tracks, 8931 = 11) — `init_aac` raises
`ValueError` for any sample rate other than 32 / 44 / 48 kHz, so **the
detector cannot score 96 kHz input at all**, and those albums have no genuine
Derrien baseline.

**The wild arm.** 1500 FLAC files taken from the two protected quarantine
trees on doc2, `/mnt/virtio/music/slskd/wrong_matches` (934 rows) and
`/mnt/virtio/music/slskd/failed_imports` (566 rows). 364 error out. These are
real peer-shared files of unknown provenance — negatives only in the sense
that nothing labels them.

## Files and their exact column layouts

`res_*.tsv.gz` all share one **16-column layout, with a header row**:

| idx | field | meaning |
|---:|---|---|
| 0 | `cls` | `genuine` / `launder` / `wild:wrong_matches` / `wild:failed_imports` |
| 1 | `rid` | corpus request_id (empty for wild rows) |
| 2 | `mode` | frame-selection mode — `high` (highest-energy frames) or `low` |
| 3 | `path` | measured file |
| 4 | `sr` | sample rate |
| 5 | `ch` | channels |
| 6 | `dur` | duration, seconds |
| 7 | `proba` | the detector statistic at the best offset |
| 8 | `median` | median of the statistic over the offset sweep |
| 9 | `std` | standard deviation over the sweep |
| 10 | `z` | `(proba − median) / std` |
| 11 | `offset` | argmax offset in samples, 0–1023 |
| 12 | `pmode` | channel combination that produced the reported value — `LR` or `MS` |
| 13 | `nac_z` | the NAC probe's z-score (populated only in `res_gt_high`) |
| 14 | `secs` | wall seconds for that file |
| 15 | `err` | exception text; **empty on success** |

**Every downstream script drops rows with a non-empty `err`.** Do the same.

| file | rows | contents |
|---|---:|---|
| `res_gt_high.tsv.gz` | 430 | paired arm, `mode=high`, 215 genuine + 215 launder, 33 errors, NAC probe populated on 397 |
| `res_gt_low.tsv.gz` | 430 | the same files at `mode=low`, no NAC probe |
| `res_wild_high.tsv.gz` | 1500 | wild arm, `mode=high`, 364 errors |

`oct_genuine.txt` / `oct_launder.txt` — the Octave oracle's output,
`offset<TAB>probability`, 5 offsets each.

## Cross-references to the other measurement

The Derrien detector was **also** run across the whole launder matrix — every
variant, AAC or not. Those results live in `../launder-matrix/` as
`derrien-*.tsv.gz` (same statistic, slightly different column set) and are
summarised in that directory's `FINAL_REPORT.txt` sections B–E. That report
records, among other things, that the genuine baseline is strongly
album-dependent (max probability 0.0180 on one album vs 0.1082 on another) and
that the in-sample per-album calibrated columns have zero false positives **by
construction** — its leave-one-out row is the honest floor for that rule. Read
those tables from the report itself.

## Cost

Recorded during the run: roughly **49 s of CPU per track** at the default
`nb_win=8`, `nb_sf=8`, single-threaded. `profile_cost.py.frozen` measures where
that goes and how it scales with `nb_win` / `nb_sf`.

## What is NOT here

The launder audio for the paired arm lived at
`/mnt/virtio/Music/derrien-arm/` on doc2 (`index.json` + `launder/<rid>/NNN.flac`).
It is disposable; `build_launders.py.frozen` is the rebuild recipe. The genuine
side is the live beets library.

The Octave oracle's WAV inputs are gone (see above).

## Frozen scripts

Every `.py.frozen` file is the exact script that produced or validated the
data. **The `.frozen` suffix is deliberate and load-bearing** — it keeps these
files out of Pyright, Ruff and Vulture. They are evidence, not maintained
source. **Do not "fix" them.** `oracle.m` is Matlab/Octave and carries no
suffix.

| file | role |
|---|---|
| `aacdet.py.frozen` | the port: Derrien detector + NAC probe |
| `naive.py.frozen` | literal statement-by-statement transcription of `detect_aac.m` |
| `validate.py.frozen` | mathematical self-validation (MDCT, TDAC, tau, synthetic lattice, noise control) |
| `parity.py.frozen` | vectorised vs naive on real audio |
| `oracle_cmp.py.frozen` + `oracle.m` | port vs the author's unmodified Matlab under Octave |
| `build_launders.py.frozen` | the paired Apple CVBR-256 arm |
| `measure.py.frozen` | batch measurement → `res_*.tsv` |
| `score.py.frozen`, `stats.py.frozen`, `final.py.frozen` | scoring, statistic comparison, consolidated table |
| `dupcheck.py.frozen` | do independent copies of a track recover the same frame offset? |
| `gain.py.frozen` | how much post-decode gain does the statistic tolerate? |
| `topz.py.frozen`, `outliers.py.frozen` | highest-scoring negatives / candidate false positives |
| `pair.py.frozen` | single genuine/launder pair probe |
| `profile_cost.py.frozen` | per-track cost and its scaling |

## Unexplored directions

Open questions with the data that would answer them. **Not recommendations,
and no prediction that any of them will work.**

1. **The NAC probe was populated on 397 rows and then never scored.**
   `res_gt_high.tsv.gz` column 13 is the only place it exists. `validate.py`
   reports it as `REPORT`, not `PASS` — on the synthetic lattice it picked
   offset 425 against a true 313, and its z-score on the noise control (3.43)
   was close to its z-score on the signal (3.87). Whether it separates
   anything on real audio is untested.
2. **`mode=low` was measured and never compared to `mode=high`.**
   `res_gt_low.tsv.gz` is a complete second measurement of the same 430 files
   under a different frame-selection rule. No script in this directory reads
   both.
3. **The wild arm has no labels.** 1136 successfully-measured real peer files
   sit in `res_wild_high.tsv.gz` with no ground truth. What would label them is
   an open question in itself — `../provenance/` is one attempt at an
   independent labelling axis.
4. **Offset agreement across independent copies.** `dupcheck.py.frozen` asks
   whether two copies of the same track recover the same frame offset (chance
   agreement would be ~0.1% over 1024 offsets). It was written; its output is
   not among the saved artifacts. The pipeline holds many independent copies of
   the same album from different peers, which is the natural corpus for it.
5. **96 kHz is out of scope for the detector as ported.** `init_aac` accepts
   only 32 / 44 / 48 kHz band tables. Whether a hi-res file could be
   downsampled and scored, and what that would mean, is untested.
6. **Per-album baselines.** The strong album-dependence of the genuine baseline
   recorded in `../launder-matrix/FINAL_REPORT.txt` was measured but not
   explained. Whether it tracks anything measurable about the master is
   unexamined.
