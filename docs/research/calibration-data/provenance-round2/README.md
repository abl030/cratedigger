# Provenance round 2 — the corrected AccurateRip / CUETools verification

Measured 2026-07-31. **This directory draws conclusions**, and it **supersedes
the AccurateRip and CTDB results in `../provenance/`** (2026-07-30). Three
implementation bugs in the round-1 probes are diagnosed and fixed here; the
round-1 directory's ARv1/ARv2 numbers are wrong and its CTDB verification never
ran.

## The question and the verdict

**Question.** Not "what does this audio look like?" but **"is this exact audio
a known CD rip?"** — a cryptographic question, answered by two public
databases keyed on the disc TOC reconstructed from the track split.

**VERDICT: PARTIAL — the axis is real, it works, and it is narrow.**

- **25 of 38 FLAC album directories (66 %) are now bit-verified**, up from
  9 partial in round 1. Every one of the 25 is a **strong** match — a CTDB
  whole-disc CRC or all tracks matching AccurateRip, not a single lucky track.
- **27 of 42** counting the library's four ALAC albums, two of which match at
  a nonzero read offset.
- **The ceiling is 42 albums — 0.49 % of the 8,490-album library.** 27 of
  those are already verified, so at most 15 more could ever be added by
  perfecting this axis. The albums that remain unverifiable include the six
  with no TOC in either database — which are exactly the long-tail pressings
  this archive exists for.
- **Therefore: a positive-only badge tier, never load-bearing.** A match is
  strong positive evidence about the *bits*. A non-match is evidence of
  nothing at all, and on 99.5 % of the library there is nothing to ask.

## The three bugs

### (A) The AccurateRip response was parsed with the wrong field semantics

An AccurateRip `dBAR` block is, little-endian, per response block:

```
header : uint8 num_tracks, uint32 ar_id1, uint32 ar_id2, uint32 freedb_id
track  : uint8 confidence, uint32 checksum, uint32 checksum_450     x num_tracks
```

`checksum` holds **either the ARv1 or the ARv2 value — the database does not
record which**. `checksum_450` is the CRC over CD frame 450 only, used for
read-offset detection, and is **never** a track checksum.

Round 1 compared our computed ARv2 against the `checksum_450` column. **Zero
ARv2 matches was therefore structurally guaranteed**, which is exactly what
`../provenance/README.md` § "Unexplored directions" item 1 reported as an
unexplained anomaly. Reference: `arver` 1.5.0, `arver/disc/database.py` — the
`Track` docstring and `make_dict`, which keys only on `checksum`.

`diag2.py.frozen` is the direct proof: it computes ARv1, ARv2 and the frame-450
CRC for four albums and reports which AccurateRip field each lands in.

Consequence: **4 albums are ARv2-only in the database** and were invisible in
round 1 — the Goldberg Variations (32/32), Stratosphere (17/17), Fake Our
Deaths (13/13), Man Made Object (10/10).

### (B) Track-1 off-by-one in the checksum window

AccurateRip skips the first 5 CD sectors of track 1 and the last 5 of the final
track. The correct lower bound is `5 * 588 = 2940` samples skipped, i.e. the
sum starts at 1-based sample index 2941 — round 1 started one sample later.

**This is invisible whenever CD frame 2939 is silence**, which is why round 1's
25-of-25 `arver` cross-check passed: both albums it tested begin with digital
silence. It is not invisible on albums that do not.

Fixed, the whole-library run gains a track on six albums:

```
1991 - End on End                                v1 16→17  v2 16→17
1998 - Stratosphere                              v2 16→17
2001 - Jane Doe                                  v1 11→12  v2 11→12
2013 - Random Access Memories                    v1 12→13  v2 12→13
2016 - Man Made Object                           v2  9→10
The Rough Guide to Australian Aboriginal Music   v1 15→16  v2 15→16
```

Each of those six moves from "all but track 1" to **all tracks** — which is the
difference between a weak and a strong claim. Pooled: ARv1 93 → 97 tracks,
ARv2 163 → 169.

Re-cross-checked against `arver` 1.5.0's C extension (`arver/audio/_audio.c`)
on five albums — the Goldberg Variations, Stratosphere, Fake Our Deaths, Death
Magnetic, Man Made Object — **82 of 82 tracks now match on both ARv1 and ARv2**
(`out/arver_*.json`, produced by `arver_ref2.py.frozen`).

### (C) float64 promotion in the read-offset scan

The read-offset search builds prefix sums over the whole disc so each candidate
offset costs O(1). Round 1 (and a first draft here) wrote

```python
s0 = np.concatenate(([0], np.cumsum(w)))          # w is uint64
```

`np.concatenate` with a Python `0` promotes the result to **float64**, whose
53-bit mantissa silently loses precision above 2^53 — and these sums reach
~2^60. Every candidate offset was therefore evaluated against a corrupted
prefix sum.

`offsetscan.py.frozen` keeps everything in `uint64` reduced mod 2^32
(`prefix_mod`), widens the radius to ±5000 samples, and probes three tracks
instead of one. Albums with a candidate offset: **3 → 12**. Confirmed all-track
matches at a single constant offset:

```
1984 - Tabula Rasa            offset  -78   v1  4/4   v2  0/4    conf   1
1992 - Slanted and Enchanted  offset  -30   v1 14/14  v2 14/14   conf   1
1995 - Tri Repetae            offset -222   v1 10/10  v2 10/10   conf  44
1996 - Feed Me Weird Things   offset +102   v1 12/12  v2 12/12   conf  65
1999 - Californication        offset +697   v1 15/15  v2 15/15   conf 200
1999 - Terror Twilight        offset   +6   v1 11/11  v2 11/11   conf  95
2000 - The Pointless Gift     offset +664   v1 12/12  v2 12/12   conf   1
2006 - Ten Lives              offset +667   v1 12/12  v2  0/12   conf   2
2014 - Ruins                  offset +108   v1  7/8   v2  8/8    conf  38
2014 - Syro                   offset +667   v1 12/12  v2 12/12   conf 200
```

**9 albums are rescued *only* by the read-offset scan** (Tabula Rasa also has a
CTDB whole-disc match, so it is not counted as offset-only). A constant offset
across every track of a disc is the signature of a drive read offset, and it is
strong evidence rather than a coincidence: Californication at +697 matches all
15 tracks on both checksum versions at confidence 200.

Round 1 reported "the offset search rescued nothing: 0 albums matched only
after a shift". That was the float64 bug.

## The CTDB convention, reverse-engineered

Round 1 surveyed CUETools DB coverage but never verified against it
(`../provenance/README.md` § "Unexplored directions" item 2), because CTDB
publishes no CRC specification.

It was derived empirically from **Death Magnetic** — bit-verified against
AccurateRip at confidence 200 on all 10 tracks on both versions, with a CTDB
top entry at confidence 8026, so whatever convention CTDB uses our PCM must
reproduce it. `ctdb_convention.py.frozen` tries eight candidate conventions per
track (plain zlib, `init0`, inverted, byte-swapped, zero-prefixed, AR-style
5-sector trim, skip-zeroes) and `ctdb_edge.py.frozen` brute-forces the
lead/tail trim in whole sectors.

**The rule:**

> CTDB `trackcrcs` and `crc32` are plain `zlib.crc32` over the disc PCM
> restricted to samples `[stride, total − stride − (total mod stride))`,
> with `stride = 5880` samples = 10 CD sectors. Per-track CRCs are that
> trimmed image intersected with each track, so middle tracks are untrimmed.

`ctdb_rule.py.frozen` validates it on four albums. The host used throughout is
`db.cue.tools`; the alternative `db.cuetools.net` was not usable here (TLS
certificate), and the hostname is a `provlib.ctdb_fetch_cached` default.

Result: **12 albums match CTDB's whole-disc CRC-32 exactly**, and 16 have at
least one submitted rip agreeing on at least one track
(`ctdb_entrycheck.py.frozen` → `out/ctdb_entry.json`). The whole-disc match is
the strongest single claim available anywhere in this research: it means our
bytes are identical to a rip another person submitted, confidence up to 12,412
(Random Access Memories).

## Results

`out/FINAL_REPORT.txt`, verbatim summary:

```
=== SUCCESS METRIC (38 FLAC album dirs) ===
  BEFORE (frozen run): 9 albums with >=1 ARv1 track at offset 0;
                       ARv2 0 tracks everywhere; CTDB never verified.
  AR offset 0, >=1 track matched     : 13
     of which ARv2-only (parser fix) : 4
  AR offset 0, ALL tracks matched    : 13
  CTDB, single submitted rip >=1 trk : 16
  CTDB whole-disc CRC32 exact        : 12
  rescued ONLY by AR read-offset     : 9
  ANY positive bit-verified match    : 25 (66% of 38)
     of which STRONG (whole-disc or ALL tracks): 25
```

The 13 unverified FLAC albums break down as: **4 not sector-aligned** (Rumours
and Hurry Up We're Dreaming are 96 kHz/24-bit hi-res with no CD TOC at all;
Loveless and Hecklers Choice have track lengths that are not whole sectors),
**6 whose TOC is in neither database**, and **3 whose TOC is present but whose
audio matched nothing** (A Long Time Coming, Voices of Gondwana, New Light New
Hope) — a different pressing, a read offset beyond ±5000 samples, or a
non-CD source; this run does not distinguish those.

### ALAC

`out/FINAL_REPORT.txt`'s ALAC section reports **0 of 4** — but it only counts
offset-0 matches, because it was rendered before the ALAC read-offset scan.
`alac_offset.py.frozen` → `out/alac_offsets.json` rescues two:

```
2006 - Half-Truths & Indiscretions_ The Anthology   offset +680  v1 14/14  conf  1
1999 - Re-Rewind (The Crowd Say Bo Selecta)         offset   +6  v1  3/3  v2 3/3  conf 29
```

**27 of the 42 lossless albums are therefore bit-verified.** The FINAL_REPORT's
ALAC line is left as generated; this paragraph is the correction.

### The ceiling

`census.py.frozen` reads the beets library SQLite read-only and counts the
population that could **ever** match a rip checksum database — only lossless,
44.1 kHz stereo, sector-aligned track splits are reachable:

**42 lossless albums out of 8,490 — 0.49 % of the library** (38 FLAC + 4 ALAC).
Its stdout was not captured during the run; the count is reproducible by
re-running the frozen script, and it is consistent with the format census in
`../provenance/README.md` § 4 (FLAC 38, ALAC 4).

## How it was run

From the repository root, in the pinned dev shell (Python 3.14.6, numpy 2.5.1,
`flac` 1.5.0, `ffmpeg` 8.1.2). `arver_ref2.py.frozen` is the exception: `arver`
is not in the dev shell, so it ran under a throwaway `pip install arver==1.5.0`
venv on Python 3.13.13. That venv is **not committed**.

```bash
nix-shell --run "python3 census.py"           # library ceiling
nix-shell --run "python3 diag2.py"            # prove the checksum/checksum_450 field split
nix-shell --run "python3 ctdb_convention.py"  # derive the CTDB CRC convention
nix-shell --run "python3 ctdb_edge.py"        # brute-force the lead/tail trim
nix-shell --run "python3 ctdb_rule.py"        # validate the derived rule on 4 albums
nix-shell --run "python3 fullrun2.py"         # -> out/fullrun2.json, out/fullrun2.log
nix-shell --run "python3 offsetcheck.py"      # all-track confirmation of candidate offsets
nix-shell --run "python3 offsetscan.py"       # -> out/offsets.json (exact uint64 scan)
nix-shell --run "python3 ctdb_entrycheck.py"  # -> out/ctdb_entry.json
nix-shell --run "python3 alacrun.py"          # -> out/alac.json
nix-shell --run "python3 alac_offset.py"      # -> out/alac_offsets.json
nix-shell --run "python3 report.py"           # -> out/FINAL_REPORT.txt
./venv/bin/python arver_ref2.py "<album dir>" # -> out/arver_<album>.json
```

Every script imports `provlib` from its own directory, so a scratch copy must
have each `.py.frozen` renamed back to `.py`. All network access is cached:
`provlib.fetch_ar_cached` and `provlib.ctdb_fetch_cached` write into a local
`cache/ar` and `cache/ctdb`, with a 0.7 s inter-request delay and a
`cratedigger-provenance-research/1.0` user agent. **The cache is not
committed** — it is 696 KB of rebuildable HTTP responses (42 AccurateRip blobs,
37 CTDB XML documents) and re-running any script repopulates it.

Everything is **read-only**. No repository file, database row or library file is
touched.

## Files

`.py.frozen` files are the exact scripts that produced the data. **The
`.frozen` suffix is deliberate and load-bearing** — it keeps them out of
Pyright, Ruff and Vulture. They are evidence, not maintained source, and they
carry hardcoded absolute library paths. **Do not "fix" them.**

| file | role |
|---|---|
| `provlib.py.frozen` | the corrected primitives — FLAC STREAMINFO parse, TOC build, CDDB/AccurateRip/MusicBrainz disc IDs, ARv1+ARv2, the frame-450 CRC, the **corrected** `parse_ar`, and the cached AR/CTDB fetchers |
| `diag2.py.frozen` | proves bug (A): which AccurateRip field ARv1 / ARv2 / frame-450 actually land in |
| `ctdb_convention.py.frozen` | derives the CTDB CRC convention from Death Magnetic against eight candidates |
| `ctdb_edge.py.frozen` | brute-forces the CTDB lead/tail trim in whole sectors |
| `ctdb_rule.py.frozen` | validates the derived stride-5880 rule on four albums |
| `fullrun2.py.frozen` | the corrected whole-library run over all 38 FLAC album dirs |
| `offsetcheck.py.frozen` | `ar_at()` — ARv1/ARv2 of an arbitrary window, used to confirm a candidate offset across **every** track |
| `offsetscan.py.frozen` | fixes bug (C): exact `uint64` mod-2^32 prefix sums, ±5000 samples, three probe tracks |
| `ctdb_entrycheck.py.frozen` | per-entry CTDB agreement — does ONE submitted rip agree with us on all tracks |
| `census.py.frozen` | library-ceiling census off the beets SQLite |
| `alacrun.py.frozen` | extends the probe to the four ALAC albums |
| `alac_offset.py.frozen` | ALAC read-offset scan |
| `report.py.frozen` | renders `out/FINAL_REPORT.txt` |
| `arver_ref2.py.frozen` | emits `arver` 1.5.0 C-extension ARv1/ARv2 for one album as JSON (runs under the uncommitted venv) |

### `out/`

| file | what it is |
|---|---|
| `FINAL_REPORT.txt` | the final table and success metric, verbatim as generated. Columns: `#`, album, `trk`, `al` (sector-aligned), `ARblk` (AccurateRip response blocks), `ARv1`/`ARv2` (tracks matched at offset 0), `ARcnf` (max AR confidence), `CTent` (CTDB entries), `CT1rip` (best single submitted rip's track agreement), `CTcnf`, then the best overall claim |
| `fullrun2.json` | one record per FLAC album — identity, `mb_discid`, `toc`, AR block count, per-version hit counts and confidences, CTDB entry count / track hits / whole-disc CRC and match, plus `per_track` with each track's ARv1, ARv2, CTDB CRC and hit flags |
| `fullrun2.log` | that run's per-album stdout, `per_track` and `toc` elided |
| `offsets.json` | `{album: [{offset, v1, v2, n, max_conf}, …]}` — every confirmed candidate read offset, scored across all tracks |
| `ctdb_entry.json` | `{album: {n, best_entry_agree, best_entry_conf, best_entry_id, disc_match, disc_conf}}` |
| `alac.json` | the four ALAC albums at offset 0, same record shape as `fullrun2.json` minus `per_track` |
| `alac_offsets.json` | `{album: [{offset, v1, v2, n, max_conf}, …]}` for the ALAC read-offset scan |
| `arver_<album>.json` | `[{n, v1, v2}, …]` from the `arver` 1.5.0 C extension — the independent reference for the 82-track cross-check |
| `fullrun2_v1bug.json`, `offsets_v1bug.json`, `offsets.log`, `offsetscan.log` | the **pre-fix** runs and their logs, kept as the audit trail for bugs (B) and (C). Diffing `fullrun2_v1bug.json` against `fullrun2.json` reproduces the six-album track-1 table above; `offsets_v1bug.json` has 3 candidate albums against the corrected scan's 12 |

## Conclusions

1. **The axis works, and round 1 measured it wrong.** All three failures were
   implementation, not concept: a mis-parsed database column, a one-sample
   window bound, and a silent numpy dtype promotion. Two of the three were
   invisible to the round-1 validation because the validation albums happened
   not to exercise them — bug (B) hides whenever track 1 starts with silence,
   and the 25-of-25 `arver` cross-check used two such albums.
2. **A CTDB whole-disc CRC-32 match is the strongest provenance claim in this
   research.** It is byte identity with another person's submitted rip, at
   confidences up to 12,412. Twelve albums have one.
3. **A constant nonzero read offset across every track is a real signature**,
   not a coincidence, and it accounts for 9 of the 25 verified albums. Any
   future implementation must scan offsets, in exact integer arithmetic.
4. **The ceiling is 0.49 % of the library and 27 of the reachable 42 are
   already verified.** Perfecting this axis could add at most 15 albums. That
   is the whole case, and it is why this is a badge, not a gate.
5. **A non-match must never deny anything.** Six of the unverified albums have
   no TOC in either database — long-tail Australian indie and small pressings,
   precisely the material the archive exists to preserve. Wiring a non-match
   into any decision would penalise exactly the collection this pipeline is
   for.
6. **Positive-only badge tier, never load-bearing.** If this ships, it renders
   as "verified against N independent rips" on the 27 albums that have it, and
   is silent everywhere else. It contributes nothing to accept/reject.
7. **Nothing here changes production.** No constant, threshold or decision is
   derived into shipped code by this directory.

## What is NOT here

- **`cache/`** — 696 KB of AccurateRip `.bin` blobs and CTDB XML. Rebuildable
  by re-running any script; the fetchers are cache-first with a 0.7 s delay.
- **`arver_src/`** — the extracted `arver` 1.5.0 package. Third-party GPL
  source; it is cited, not vendored. The two files that matter are
  `arver/disc/database.py` (the `Track` docstring and `make_dict`, which
  settle bug (A)) and `arver/audio/_audio.c` (`sum_from = 5*588`, which
  settles bug (B)). Recreate with `pip install arver==1.5.0`.
- **`venv/`** — the throwaway Python 3.13 venv that `arver_ref2.py.frozen`
  runs under.
- **The audio.** Everything here reads the live beets library at
  `/mnt/virtio/Music/Beets` read-only.
