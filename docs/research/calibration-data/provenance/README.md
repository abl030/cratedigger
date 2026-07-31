# Provenance probes — can a file's origin be established without looking at its spectrum?

Run 2026-07-30. **Descriptive record only.** This directory records a set of
read-only probes against external rip databases and MusicBrainz, and what they
returned on the current library. It draws no conclusion about whether any of
this belongs in the pipeline, and none should be inferred.

## The idea being probed

Every spectral instrument in this research asks "what does this audio look
like?". These probes ask a different question: **"is this exact audio a known
CD rip?"** — a cryptographic, not statistical, question.

A CD's table of contents can be reconstructed from track-split rip files
(track lengths in 588-sample sectors). That TOC identifies a disc pressing.
Two public databases keyed on it hold checksums submitted by rippers:

- **AccurateRip** — per-track checksums (ARv1 and ARv2) with a confidence count.
- **CUETools DB (CTDB)** — per-disc and per-track CRCs, also with confidence,
  plus fuzzy TOC matching.

MusicBrainz stores the same TOC as a **DiscID**, attached to specific releases.

A match on any of these is positive evidence about the *bits*: this audio is
byte-identical to what N other people got off that pressing. A non-match is
not evidence of anything in particular.

Everything here is **read-only**. No repository file, database row or library
file is touched.

## What was measured

Three probes, all over the library's **38 FLAC album directories** at
`/mnt/virtio/Music/Beets`.

### 1. AccurateRip verification — `fullrun.py.frozen` → `fullrun.out`

For each album: reconstruct the TOC from FLAC STREAMINFO sample counts,
fetch the AccurateRip block for it, compute ARv1 and ARv2 per track at offset 0,
and — if nothing matched — search read offsets ±3000 samples using prefix sums
(O(1) per candidate offset).

```
== 38 albums | AR TOC hit 25 | audio-proved at offset 0 9 | proved only after offset shift 0
```

- **4 albums are not sector-aligned** (Rumours, Hurry Up We're Dreaming,
  Loveless, Hecklers Choice) — their track lengths are not whole sectors, so
  they are not CD track splits at all and no TOC can be built.
- **9 albums' TOCs are not in AccurateRip.**
- **25 albums have an AccurateRip block.** Of those, **9 have at least one
  track matching ARv1 at offset 0**: Ambient 1 (4/4), Rough Guide to
  Australian Aboriginal Music (15/16), Jane Doe (11/12), Random Access
  Memories (12/13), Death Magnetic (10/10), Kind of Blue (5/5), Shady Lane
  (9/9), End on End (16/17), The Velvet Underground & Nico (11/11).
- **ARv2 matched 0 tracks on every album**, including the nine where ARv1
  matched. See the open questions below.
- **The offset search rescued nothing**: 0 albums matched only after a shift.

An album counts as "proved" here if *any* track matched — the per-album
fractions above are the honest detail.

### 2. MusicBrainz DiscID — `mbdisc.py.frozen` → `mbdisc.out`

Compute the MB DiscID from the reconstructed TOC and check whether it is
attached to the release the file is already tagged with (`MUSICBRAINZ_ALBUMID`,
looked up through the MB mirror).

```
20/34 albums: computed DiscID present on the matched MB release
              (27 releases have any discid)
```

34 = the 38 minus the 4 non-sector-aligned. 7 of the 34 matched releases carry
no DiscID at all, so the practical denominator is 27, of which 20 matched.

### 3. AccurateRip + CTDB coverage survey — `survey.py.frozen` → `survey.out`

Per album: sector alignment, whether the TOC is in AccurateRip, how many CTDB
blocks came back, and CTDB's top confidence.

```
sector-aligned 34/38; AR TOC hit 25; CTDB TOC hit 28
```

CTDB confidences on matched albums range from single digits to 8411
(Californication).

### 4. Library-wide sector-alignment survey — `align.py.frozen` → `align.out`

A coarser probe straight off the beets library DB: for each album, are *all*
tracks whole numbers of 588-sample sectors? Derived from `items.length`
(seconds, float), so it is a survey, not a proof — `fullrun.py.frozen` uses
exact FLAC sample counts for the FLAC subset.

```
format          albums  all-tracks sector-aligned    pct
Opus              7257                       2800  38.6%
MP3               1127                        401  35.6%
AAC                 48                         18  37.5%
FLAC                38                         19  50.0%
OGG                 10                          8  80.0%
MIXED                5                          3  60.0%
ALAC                 4                          3  75.0%
Windows Media        1                          0   0.0%
TOTAL             8490                       3252  38.3%
```

## Implementation validation

The AccurateRip checksum implementation in `arverify.py.frozen` is an
independent numpy implementation. It was cross-checked against **`arver`
1.5.0**, whose checksums come from a C extension — a genuinely separate
implementation.

Re-run at capture time; the transcript is `arver_crosscheck.out`:

```
Metallica — Death Magnetic              TOTAL match=10 mismatch=0
Red Hot Chili Peppers — Californication TOTAL match=15 mismatch=0
```

25 of 25 tracks match on **both ARv1 and ARv2**. Californication is one of the
albums that matched *nothing* in the database, so this also shows that its
non-match is a property of the rip or the database comparison, not of the
checksum arithmetic.

`arver`'s own CLI could not run here (it imports `cdio` for physical CD
access), so the cross-check imports `arver.audio.checksums.get_checksums`
directly. `arver_ref.py.frozen` runs under the scratch venv's Python 3.12 with
`LD_LIBRARY_PATH` pointed at a zlib store path; `arver_crosscheck.py.frozen`
runs under the repo `nix-shell` and compares. The venv itself is **not
committed** — recreate it with `pip install arver==1.5.0`.

## Files

| file | what it is |
|---|---|
| `fullrun.out` | the AccurateRip verification survey, 38 albums, one line each |
| `mbdisc.out` | the MusicBrainz DiscID probe, one line per album |
| `survey.out` | alignment + AR + CTDB coverage, one line per album |
| `align.out` | library-wide sector-alignment survey, by format |
| `arver_crosscheck.out` | our checksums vs the `arver` C reference, 25 tracks |

`fullrun.out` line format: `<album name, 46 chars>  AR blocks=<n>  v1=<ok>/<n>
v2=<ok>/<n>[ offset-match at [...] samples]`, or one of
`not sector-aligned -> not a CD track split` / `TOC not in AccurateRip`.

`survey.out` columns: `album`, `trk` (track count), `align` (`yes` or
`<n>bad`), `AR` (`<n>blk` / `miss` / `-`), `CTDBconf` (top confidence, `miss`,
`err`, or `-`).

## Frozen scripts

Every `.py.frozen` file is the exact probe that produced the matching output.
**The `.frozen` suffix is deliberate and load-bearing** — it keeps these files
out of Pyright, Ruff and Vulture. They are evidence, not maintained source, and
they carry hardcoded absolute paths and a hardcoded mirror address. **Do not
"fix" them.**

| file | role |
|---|---|
| `toc_probe.py.frozen` | the primitives — FLAC STREAMINFO parse, TOC build, CDDB id, AccurateRip id, MusicBrainz DiscID, CTDB lookup |
| `arverify.py.frozen` | ARv1/ARv2 checksums (numpy) + AccurateRip fetch/parse |
| `aroffset.py.frozen` | prefix-sum read-offset search for a single album |
| `fullrun.py.frozen` | the whole-library AccurateRip survey |
| `survey.py.frozen` | alignment + AR + CTDB coverage survey |
| `mbdisc.py.frozen` | MusicBrainz DiscID probe against the MB mirror |
| `align.py.frozen` | library-wide sector-alignment survey off the beets DB |
| `multi.py.frozen` | multi-album AR + CTDB detail for named directories |
| `ctdbcheck.py.frozen`, `ctdbcheck2.py.frozen` | per-track CTDB CRC comparison, two CRC conventions |
| `diag.py.frozen` | raw AccurateRip block dump for one album |
| `beetsprobe.py.frozen` | what precision the beets `items.length` column carries |
| `arver_ref.py.frozen`, `arver_crosscheck.py.frozen` | the `arver` C-reference cross-check (written during this capture) |

## Unexplored directions

Open questions with the data that would answer them. **Not recommendations,
and no prediction that any of them will work.**

1. **ARv2 matched nothing, anywhere.** Our ARv2 values agree exactly with the
   `arver` C reference on 25 tracks, so the arithmetic is right — which leaves
   the block parsing in `parse_ar` (the ARv2 column offset within an
   AccurateRip block), or the database, as the place to look. Until that is
   resolved, `fullrun.out`'s v2 column carries no information. Data:
   `diag.py.frozen` dumps raw blocks; `arver`'s own `disc/database.py` parses
   the same binary.
2. **CTDB was surveyed for coverage but never verified against.** 28 of 38
   albums have a CTDB entry, some with very high confidence, and
   `ctdbcheck.py.frozen` / `ctdbcheck2.py.frozen` exist to compare per-track
   CRCs under two different CRC conventions — but no whole-library CTDB
   verification was run, and neither script's output was saved. CTDB is also
   the more forgiving database (fuzzy TOC matching, offset tolerance).
3. **The coverage ceiling is unmeasured beyond FLAC.** These probes only ran on
   the 38 FLAC album dirs. `align.out` says 3252 of 8490 library albums are
   sector-aligned across all formats — but a lossy album can never match a
   checksum database, so what the reachable population actually is (lossless
   albums, sector-aligned, TOC present in AR or CTDB) has not been counted.
4. **A non-match means nothing yet.** 16 of the 25 AR-hit albums matched no
   track. Whether that is a different pressing, a different read offset beyond
   ±3000 samples, gapless/HTOA differences, or a genuinely non-CD source is
   not distinguished by anything here. `aroffset.py.frozen` searches offsets
   for a single album and could be widened.
5. **DiscID non-matches were not chased.** 7 of 27 releases with a DiscID did
   not match ours. That could be a wrong pressing in the tag, a different
   release in the same group, or a TOC construction difference. `mbdisc.out`
   names them.
6. **Nothing here has been joined to the spectral evidence.** The 9
   AR-verified albums are, by construction, a small set of files with an
   external byte-level provenance claim. What their spectral evidence looks
   like — and whether any spectral instrument agrees with the databases — has
   not been checked. Data: `album_quality_evidence` in the live pipeline DB,
   keyed by path.
7. **The pipeline's own corpus was not probed.** These ran over the beets
   library only. The quarantine trees (`wrong_matches/`, `failed_imports/`)
   hold real peer files of unknown provenance and are exactly where an
   independent labelling axis would be worth having — `../derrien/` measured
   1500 of them with no labels at all.
