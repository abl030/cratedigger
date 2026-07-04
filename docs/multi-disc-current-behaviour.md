# Multi-disc current behaviour

*Investigation unit U9 of PR2 of search-plan iteration 2. The deliverable
for U9 is this document; U10 reads it as input and uses the "U10 design
recommendation" section to decide in-scope vs. follow-up.*

## Summary

Multi-disc support already exists end-to-end as a **per-disc** pipeline.
`album_tracks.disc_number` is preserved at enqueue time, `AlbumRecord`
exposes `release.media` (one `MediaRecord` per disc) derived from those
rows, and `try_multi_enqueue` runs the matcher per-disc using only that
disc's tracks. The generator and the search-cache layer remain
disc-agnostic — they see one album, one track-title list, one set of
peer dirs. The matcher's strict count gate therefore compares
per-disc track counts against per-disc file counts, and slskd's search
responses naturally bucket files under their immediate parent directory
(`Aerial\CD 1` is distinct from `Aerial\CD 2`).

The **Kate Bush Aerial pattern** is not actually a count-gate bug. The
flow already lines up: `try_enqueue` fails on the flat 16-track gate,
`try_multi_enqueue` fires, partitions tracks per `mediumNumber`, and for
each disc finds the corresponding subfolder candidate. What fails today
is upstream of the matcher — the **search-stage filtering**: a peer's
`Aerial\CD 1\` files only ever surface as a candidate dir if a search
result for query `aerial` (or one of the plan's variants) includes one of
its tracks. Because the wave-based fan-out then ranks/browses dirs whose
`audio_file_count` is in the right ballpark, sibling subfolders are
correctly retained. The aggregation U10 anticipates building is already
present — just structured as per-disc matching with disk-prefixed file
naming downstream, rather than as parent-folder aggregation in a single
match call.

What is **genuinely missing** is the case where `try_enqueue` (the
flat pass) catches a peer hosting a flat 16-file Aerial dir with a
non-MB-canonical pressing (e.g. 18 files because the peer dropped two
hidden tracks). The strict count gate at `matching.py:502` will reject
it. But that is the normal pressing-identity invariant doing its job,
not a multi-disc bug.

## Enqueue path

Multi-disc requests preserve disc structure in three layers:

* **`album_tracks` schema** (`migrations/001_initial.sql`, columns
  populated by `PipelineDB.set_tracks` at `lib/pipeline_db.py:2912-2925`).
  The column is `disc_number INTEGER` and `set_tracks` reads
  `t.get("disc_number", 1)` per track. `get_tracks` returns rows
  `ORDER BY disc_number, track_number` (`pipeline_db.py:2927-2934`), so
  iteration order is pressing order.
* **`AlbumRecord.from_db_row`** (`album_source.py:69-149`) groups the
  fetched track rows into `discs: dict[int, list[track]]` keyed by
  `t["disc_number"]`, then builds one `MediaRecord(medium_number,
  medium_format, track_count)` per disc. `release.media` is therefore
  always populated correctly for multi-disc MB releases.
* **`DatabaseSource.get_tracks`** (`album_source.py:206-228`) maps each
  row to a normalised dict with `mediumNumber: t["disc_number"]`,
  feeding the matcher's track-record shape.

Verified against the live DB on doc2 — `pipeline-cli query` for
`album_requests` joined to `album_tracks GROUP BY ar.id HAVING
array_length(array_agg(DISTINCT at.disc_number), 1) > 1` returns
multiple multi-disc rows including Kate Bush *Aerial* (req 1859, 16
tracks across discs 1–2), Decemberists *The Crane Wife* (48 tracks
across 6 discs), Bruce Springsteen *Born to Run 30th Anniversary*
(40 tracks across 4 discs), Eluvium *Life Through Bombardment Vol. 1*
(47 tracks across 14 discs).

`release_snapshot.py::_tracks_titles_and_artists` (lines 116-161) sorts
by `(disc_number, track_number)` before extracting titles. The output
is a **flat** `tuple[str, ...]` — the generator never sees disc
boundaries. This is fine because the generator's per-track slots use
distinctiveness ranking (U7) over the whole album, not per-disc cuts.

## Generator behaviour

`generate_search_plan` (`lib/search.py:1555`) and the three branch
plans (`_generate_normal_plan`, `_generate_va_plan`,
`_generate_selftitled_plan`) emit **one query family per album**, not
per-disc queries. A `grep -in "disc\|cd\b\|medium" lib/search.py`
(filtering out `discriminat*` / `discuss*` matches) returns nothing —
zero disc-awareness in the generator. `track_titles` is consumed as a
flat sequence for the `track_<idx>_artist` fallback slots and for the
distinctiveness scoring in `select_distinctive_track_titles`.

For Aerial (16 tracks, discs 1–2) the generator emits the same slot
mix as a single-disc 16-track album: `default`, `literal`,
`literal_flac`, `unwild_year` (and conditionally `unwild_rg_year`,
`catalog_number`), then up to four `track_<n>_artist` fallback slots
drawn from the flat 16-title pool. No `default + " CD 1"` style
per-disc emission exists or is planned.

## Search execution + slskd response shape

`_collect_search_results` in `cratedigger.py:525-655` waits for slskd
to deliver search responses for a submitted query and feeds them into
`_build_search_cache` (`cratedigger.py:203-253`). The decisive line is
`cratedigger.py:239`:

```python
file_dir = file["filename"].rsplit("\\", 1)[0]
```

Each file's parent path becomes its `file_dir`. So a peer holding
`Aerial\CD 1\01 - Pi.flac` produces `file_dir = "Aerial\\CD 1"` and a
sibling `Aerial\CD 2\01 - Prelude.flac` produces `file_dir =
"Aerial\\CD 2"`. The two subfolders appear in
`cache_entries[username][filetype]` as **distinct entries** — they are
sibling candidate dirs to the matcher, not children of a "Aerial"
parent.

slskd's search response is a **flat list of (username, files-with-full-paths)**.
It is NOT a directory tree. There is no parent-folder grouping in the
search-result wire format; we synthesise the directory bucketing on
the consumer side from the rsplit above.

`lib/browse.py::_browse_one` (lines 373-400) calls
`slskd_client.users.directory(username=user, directory=file_dir)` — this
fetches the listing for **exactly one specified directory**. It does
not walk children, does not return siblings, does not surface a tree.
If the matcher wants to know what dirs live under `Aerial\`, it must
either receive them in the search response (which today it does — both
`Aerial\CD 1` and `Aerial\CD 2` show up as `file_dirs` because the
search hit files inside each) or perform additional browse calls
(which today it does not).

`rank_candidate_dirs` (`browse.py:350-370`) ranks the
search-discovered dirs by album-title and artist-name substring match.
Both `Aerial\CD 1` and `Aerial\CD 2` contain "Aerial" so they rank
equally — both make it into the dirs-to-try list.

## Matcher

`check_for_match` (`lib/matching.py:318-569`) is the strict count gate.
For the multi-disc flow the key facts are:

* `track_num = len(tracks)` (line 360) — this is the **per-disc**
  count when called from `try_multi_enqueue`, and the **flat
  whole-album** count when called from `try_enqueue`. The function
  is the same; the caller decides the granularity.
* The asymmetric pre-filter (lines 381-402) compares
  `search_count > 2 * track_num` against cached audio-count per dir.
  For Aerial disc 1 with `track_num=7` it rejects dirs with more
  than 14 audio files; for disc 2 with `track_num=9` it rejects dirs
  with more than 18 files. A peer's `Aerial\CD 1` (7 files) is
  retained for the disc-1 pass, `Aerial\CD 2` (9 files) is retained
  for the disc-2 pass.
* The strict count gate at line 502:

  ```python
  if tracks_info["count"] != track_num or tracks_info["filetype"] == "":
      … reject
  ```

  This is **equality**, not >=. For the per-disc call this is
  exactly the right shape: disc 1 has 7 tracks, the matcher asks
  "does `Aerial\CD 1` contain exactly 7 audio files of one filetype?"
  Yes → continue to filename score and cross-check.

* If `try_enqueue` (the flat 16-track call) gets a peer whose
  `Aerial\CD 1` dir has 7 audio files, the count gate rejects (7 !=
  16). Good — that's a per-disc subset, not the album. The
  whole-album flat path correctly cannot accept a single-disc dir as
  the whole album.

* `try_enqueue` succeeds on the flat path only if a peer hosts a
  single dir with exactly 16 audio files (e.g. `Aerial\` with the
  whole album flattened). That's the single-folder layout.

* If `try_enqueue` fails AND `len(release.media) > 1`, control flows
  to `try_multi_enqueue` (`lib/enqueue.py:1411-1414`). It loops over
  `release.media`, partitions `all_tracks` per `mediumNumber`
  (lines 1090-1099), and runs `_iter_wave_matches` per disc against
  the same `user_dirs` mapping. Each disc finds its own match.

* When all discs match (`count_found == total`, line 1173), the
  per-disc `_planned_downloads` lists are concatenated into one
  `planned_downloads` list. Each `DownloadFile` is tagged with
  `disk_no` and `disk_count` (lines 1199-1201) so the staging layer
  can disk-prefix filenames.

## Staging + import (downstream of the matcher)

`lib/staged_album.py::staged_filename` (lines 29-34) prepends `Disk N - `
to the filename when `disk_count > 1`. So a multi-disc download lands
in a single staging directory under `/Incoming/auto-import/` with
filenames like `Disk 1 - 01 - Pi.flac`, `Disk 1 - 02 - Bertie.flac`,
…, `Disk 2 - 01 - Prelude.flac`, …. This is the auto-import flatten
that the `preimport_nested` gate (`lib/quality/gates.py`) is
expected to find pre-flattened. Force-import and manual-import paths
go to `/Incoming/post-validation/` and would trip the `nested_layout`
gate if presented with raw subfolders.

The beets harness sees a flat staging dir with disk-numbered prefixes
and does its match against the full multi-disc release on MB. Beets'
tagging then writes the files into the library with the configured
per-disc path template.

## Known gap (the Kate Bush Aerial pattern, reconstructed)

The "Aerial gap" described in the iter2 brainstorm and plan is **not**
the per-disc gate failing on a `Aerial/CD 1/` + `Aerial/CD 2/` peer —
that path works today as described above, provided the **search query
itself surfaces files from both CD subfolders**.

The actual failure mode appears to be one of:

1. **Search response doesn't include both subfolders.** The slskd
   peer indexer returns files matching the query terms; if the query
   is for "Kate Bush Aerial" and the peer's CD 2 folder is named
   differently (e.g. `Aerial Disc 2 Sky of Honey`), the search hit
   for `aerial` might pick up CD 1 files only. The matcher then sees
   only `Aerial\CD 1` as a candidate dir, the per-disc count gate
   passes for disc 1 but disc 2 has no candidate dir, and
   `try_multi_enqueue` returns `matched=False`. **Probability: high.**

2. **Peer organises by combined parent only.** A peer hosting a flat
   `Kate Bush - Aerial\` dir with all 16 files concatenated under
   one folder triggers `try_enqueue` to accept (16 == 16). But the
   strict count gate is also `tracks_info["filetype"] != ""` (line
   502), and `album_track_num` (lines 227-270) returns
   `filetype == ""` if files mix codecs. So a peer with
   `CD 1\01.flac, CD 2\01.flac, sleeve.jpg` produces 16+1 audio
   files at the parent and the gate rejects. Same outcome from a
   different cause.

3. **Pre-filter `2*track_num` excludes the parent.** For Aerial-16,
   any parent dir with >32 audio files is pre-filter-skipped. Peers
   bundling Aerial inside a larger Kate Bush discography would hit
   this. The parent `Kate Bush\` dir is correctly excluded, but
   per-disc subfolders nested deeper (e.g. `Kate Bush\Aerial\CD 1\`)
   should still surface from the search response with file_dir =
   `Kate Bush\Aerial\CD 1`.

The U10 brainstorm's proposed "matcher detects sibling subdir pattern
under common parent and aggregates" would only help case (1) — and
even then only if `lib/browse.py` is taught to *additionally* browse
the parent of the matched dir, find unindexed siblings via
`slskd.users.directory`, and surface them. That is a **browse-side**
change, not a matcher-side change.

## Other multi-disc edge cases visible in code

* **Alternative naming (`Disc 1`, `Digital Media 01`, `Side A`).**
  Today the search-cache layer doesn't care about subdir naming — it
  just buckets files by their immediate parent. Any naming works
  end-to-end as long as both subfolder names contain at least one
  filename that the search query matches. The U10 regex
  `(?i)(disc|cd|digital media|side)\s*\d+` is only needed if U10
  builds explicit sibling discovery (case 1 above).
* **Pregap tracks.** MB media often lists a pregap track at
  `track_number=0`. The `_tracks_titles_and_artists` sort is
  `(disc_number, track_number)`, so pregap (`(1, 0)`) sorts before
  track 1 — good. But `album_tracks.track_number` is an INTEGER NOT
  NULL, and `set_tracks` reads `t["track_number"]` — a NULL or 0
  pregap would either crash or sort first. No special handling in
  the matcher.
* **Hidden bonus / data tracks.** `audio_file_matches` is the
  filetype gate inside `album_track_num`, and data tracks
  (`.iso, .pdf, .nfo`) are filtered out. So a peer's
  `Aerial\CD 1\07 - A Coral Room.flac, Aerial\CD 1\info.nfo`
  produces audio count 7, matching disc 1's expected 7. Good.
* **Single-disc requests in a peer subfolder.** A peer who organises
  even single-disc albums as `Artist\Album\Disc 1\` (some do, for
  consistency) produces `file_dir = Artist\Album\Disc 1`. The
  single-disc request goes through `try_enqueue` only (since
  `len(release.media) == 1`). If the count matches and the strict
  filename ratio passes, accepts. No problem.
* **Discs of different sizes that look like one combined disc.** A
  9 + 7 = 16-track release that a peer flattens into a 16-file
  `Aerial\` dir is accepted by `try_enqueue` (16 == 16). The track
  title cross-check (`_track_titles_cross_check` in `lib/util.py:395`)
  then has to find all 16 titles among the 16 files. Pass → accept,
  good. The staging layer doesn't know this peer was flat, but
  because all 16 `DownloadFile` records get `disk_no=None` from the
  flat path, `staged_filename` doesn't prepend `Disk N -`. Beets
  re-assigns disc/track numbers on tag-write. No data loss, but the
  filenames don't carry the disc structure.

## U10 design recommendation

* **(a) Matcher-side aggregation viable with no generator-output
  changes?** **No.** The matcher today already does per-disc matching
  via `try_multi_enqueue`, which is the equivalent of aggregation
  expressed differently. The remaining failure modes (search response
  not surfacing both subfolders, peers hiding discs behind extra
  parent dirs) require **browse-side** changes — specifically, a new
  step that on per-disc match failure does
  `slskd.users.directory(parent_of_matched_dir)` to enumerate
  siblings and feed them back into the matcher. That's a change to
  `lib/browse.py` and the wave-fan-out machinery, not a matcher-side
  helper.

* **(b) Estimated production LOC.** **~400-600 LOC** if the
  browse-side change is built (new helper that detects "match found
  for disc N of M, no candidate for the remaining N-1 discs",
  browses the parent dir, applies the disc regex, requeues the
  per-disc match against the discovered siblings). Plus the bookkeeping
  to make sure (i) the discovered siblings flow into the search-cache
  layer with the right shape, (ii) the wave-cap and peer-rank still
  apply, (iii) negative-cache entries for the original failure are
  invalidated. Substantially exceeds the 300 LOC budget in U10's
  in-scope criteria.

* **(c) Changes to `lib/browse.py`?** **Yes** — see above. The
  current `_browse_one` is purely "fetch listing for this exact
  dir"; the sibling-discovery flow needs a new "browse parent and
  enumerate matching children" code path. That violates U10's
  in-scope criterion (c).

**Recommended shape — defer U10's body to a follow-up plan, land the
RED test scaffold in PR2.** The brainstorm and plan already
contemplate this fallback ("If any of these fail, U10's body slips to
a follow-up plan; the test scaffold lands in PR2 anyway as RED
placeholders"). Concretely for PR2:

* Add a single RED integration slice in
  `tests/test_integration_slices.py::TestMultiDiscSiblingDiscoverySlice`
  that seeds `FakeSlskdAPI` with the case-1 Aerial pattern (search
  response returns only `Aerial\CD 1\` files; peer also has
  `Aerial\CD 2\` on disk but slskd didn't return it; under
  `users.directory("Kate Bush\\Aerial\\")` slskd would return both).
  Assert the matcher today returns `matched=False`; the test stays
  RED awaiting the follow-up plan's implementation.
* Optionally add a sibling-naming-variant RED scaffold parameterised
  over `Disc 1`, `CD 1`, `Digital Media 01`, `Side A`.
* No `SEARCH_PLAN_GENERATOR_ID` re-home. U8 keeps the bump.

The follow-up plan can then either:

1. **Browse-side sibling discovery** — `lib/browse.py` adds a
   `discover_siblings(username, matched_dir, expected_disc_count)`
   helper that uses `slskd.users.directory` against the parent and
   filters children by the disc regex. `try_multi_enqueue` invokes
   it once per cycle per request when partial-match is detected.
2. **Per-disc query emission** — generator emits a
   `default_disc_<n>` slot variant when the snapshot has multiple
   discs (titles from disc N only, query `<artist> <title> CD <n>`
   style). This would shift the work upstream to the search layer
   and bypass the browse-side problem entirely, but would bump
   `SEARCH_PLAN_GENERATOR_ID` and invalidate all multi-disc plans.

Option (1) is smaller and more targeted; (2) is more architecturally
clean but more disruptive. The follow-up plan owns that call.

## Sources

* `lib/matching.py` lines 318-569 — `check_for_match`, strict count
  gate at 502.
* `lib/enqueue.py` lines 1073-1410 — `try_multi_enqueue`,
  `_try_filetype`, per-disc partition at 1090-1099.
* `lib/browse.py` lines 350-400 — `rank_candidate_dirs`,
  `_browse_one`.
* `lib/search.py` line 1555 — `generate_search_plan` (no disc logic).
* `lib/release_snapshot.py` lines 106-161 — `_tracks_titles_and_artists`.
* `cratedigger.py` lines 203-253 — `_build_search_cache` (file_dir
  bucketing).
* `album_source.py` lines 23-149, 206-228 — `MediaRecord`,
  `AlbumRecord.from_db_row`, `DatabaseSource.get_tracks`.
* `lib/staged_album.py` lines 29-34 — `staged_filename` disk prefix.
* `lib/pipeline_db.py` lines 2912-2934 — `set_tracks`, `get_tracks`.
* `tests/test_enqueue_fanout.py` line 1499 —
  `test_multi_disc_per_disc_uses_warm_cache_across_discs` (existing
  multi-disc fixture pattern).
* Live DB on doc2 — multi-disc wanted requests including Aerial
  (1859), Decemberists *The Crane Wife* (2266), Springsteen *Born to
  Run 30th* (2272), Eluvium *Life Through Bombardment* (3133).
