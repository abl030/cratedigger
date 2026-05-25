---
date: 2026-05-25
topic: search-plan-post-deploy-findings
status: findings-only (no recommendations)
---

# Search-Plan Post-Deploy Findings (2026-05-19-1 → 2026-05-25)

## Scope

What was deployed: the search-plan entropy and matcher pre-filter work
shipped under `SEARCH_PLAN_GENERATOR_ID = "search-plan/2026-05-19-1"` on
2026-05-19 ~22:40 AWST (PR #289). Origin: `docs/brainstorms/2026-05-19-search-plan-entropy-requirements.md`.

Window analysed: 5.5 days, from 2026-05-20 12:00 UTC to 2026-05-25 ~00:00 UTC.

This document captures observations only — query results, distributional
shape, and per-case drill-down evidence from slskd probes. It records what
the data shows, not what should be done about it.

---

## Headline metrics (5.5 days, 15,396 searches)

| Outcome | Count | Share |
|---|---|---|
| `found` (matched + enqueued) | 519 | 3.4% |
| `no_match` (results returned, none scored as acceptable) | 8,625 | 56.0% |
| `no_results` (slskd returned 0 results) | 6,223 | 40.4% |
| `error` | 29 | 0.2% |
| `empty_query` / `timeout` | 0 | 0% |

Final-state distribution (slskd terminal state):

| Final state | Count |
|---|---|
| `Completed, TimedOut` | 15,258 |
| `Completed, ResponseLimitReached` | 69 |
| `InProgress` | 30 |
| `(null)` | 29 |
| `Completed, Errored` | 9 |
| `Queued` | 1 |

Pre-filter telemetry (introduced by R3, migration 025):
- 605 searches recorded at least one pre-filter skip.
- 40,394 cumulative pre-filter skips across the window.

Saturation: 69 of 15,396 searches (0.45%) ended in `ResponseLimitReached`.
Pre-deploy reference (per origin brainstorm): ~3% (≈600 / 20,000 / 7d).
No search in the window ended in `FileLimitReached`.

## Result-count bucket distribution

| `result_count` bucket | Searches | Found | Avg `result_count` |
|---|---|---|---|
| 1000 (cap) | 11 | 0 | 1000.1 |
| 950–999 | 62 | 1 | 994.1 |
| 500–949 | 39 | 5 | 708.0 |
| 100–499 | 569 | 41 | 190.6 |
| 20–99 | 1,262 | 157 | 46.8 |
| 1–19 | 7,201 | 315 | 4.8 |
| 0 | 6,226 | 0 | 0.0 |

Conversion rate by bucket (`found / searches`):
- 20–99 results: 12.4% (highest)
- 1–19: 4.4%
- 100–499: 7.2%
- 500–949: 12.8% (small N)
- 950–999: 1.6%
- ≥ 1000: 0%

## Per-strategy data

| `plan_strategy` | Searches | `no_results` | `no_match` | `found` | Avg `result_count` | Searches ≥ 950 |
|---|---|---|---|---|---|---|
| `literal_lossless` | 2,093 | 1,803 | 288 | 1 | 0.7 | 0 |
| `track_0_artist` | 2,025 | 525 | 1,352 | 142 | 17.4 | 8 |
| `default` | 1,966 | 507 | 1,361 | 77 | 37.5 | 22 |
| `unwild_year` | 1,940 | 851 | 1,040 | 49 | 8.5 | 0 |
| `track_1_artist` | 1,846 | 448 | 1,321 | 77 | 24.8 | 9 |
| `literal_flac` | 1,772 | 1,092 | 659 | 21 | 17.6 | 9 |
| `literal` | 1,728 | 481 | 1,169 | 76 | 25.3 | 4 |
| `track_2_artist` | 1,663 | 387 | 1,216 | 60 | 26.4 | 17 |
| `selftitled_artist_track_0` | 137 | 27 | 99 | 11 | 28.0 | 0 |
| `selftitled_artist_track_0_flac` | 128 | 88 | 38 | 2 | 9.7 | 0 |
| `selftitled_artist_year` | 100 | 17 | 80 | 3 | 63.5 | 4 |
| `unwild_rg_year` | 2 | 0 | 2 | 0 | 315.5 | 0 |

## Wanted-cohort composition (832 wanted requests)

Classified by post-deploy search behaviour (`searches >= 5` required to classify):

| Category | Definition | Requests | Searches consumed |
|---|---|---|---|
| **B — candidates every search, never matched** | `with_cands > 0 AND found_outcomes = 0` | **422** | 7,459 |
| E — mixed | rest | 146 | 2,592 |
| D — found-but-no-import | `found_outcomes > 0` (search hit, import didn't land) | 145 | 2,750 |
| A — zero-results dominant | `zero_results / searches > 0.8` | 119 | 2,088 |
| Resolved | moved out of `wanted` since 2026-05-20 | 95 | 513 |
| Unclassified | `searches < 5` | — | — |

Cohort B track-count distribution (447 requests with `searches >= 5 AND with_cands > 0 AND found_outcomes = 0`):

| Track count band | Requests | Share of cohort B |
|---|---|---|
| 1 | 28 | 6.3% |
| 2–4 | 115 | 25.7% |
| 5–12 | 180 | 40.3% |
| 13–20 | 83 | 18.6% |
| 21+ | 41 | 9.2% |

## Various Artists sub-cohort

| Group | Requests with searches in window | Searches | Avg `result_count` | Near-cap (≥ 950) | Found |
|---|---|---|---|---|---|
| `artist_name IN ('Various', 'Various Artists')` | 34 | 484 | 73.3 | 19 | 26 |
| All other artists | 893 | 14,934 | 17.9 | 54 | 494 |

Saturation rate: VA cohort 3.9% (19/484), non-VA cohort 0.36% (54/14,934).
VA accounts for 26% (19/73) of all near-cap searches while representing
3.7% (34/927) of active-searching wanted requests.

Sample VA queries observed at `result_count >= 500`:

| Strategy | Query | Result count | Outcome |
|---|---|---|---|
| `default` | `So Why` | 993–1,000 | no_match |
| `literal_flac` | `So Why FLAC` | 996–1,000 | no_match |
| `default` | `Axel F` | 998 | no_match |
| `literal_flac` | `Axel F FLAC` | 894–1,000 | no_match |
| `track_2_artist` | `Yourself Merry Little Christmas` | 998–999 | no_match |
| `track_1_artist` | `Christmas Song Chestnuts Roasting` | 720–729 | no_match |
| `track_2_artist` | `What Gonna Do` | 996–1,001 | no_match |
| `track_2_artist` | `It Alright Only Bleeding` | 632–636 | no_match |
| `track_2_artist` | `I Need Your Love` | 993–1,000 | no_match |

Pattern observable in these queries:
- Artist tokens `Various` / `Various Artists` do not appear in the
  emitted queries.
- Track-fallback queries for VA requests do not contain an artist token
  prefix.
- Track titles in track-fallback queries are missing words that appear
  in the full MB track title (e.g. "Have Yourself a Merry Little
  Christmas" → `Yourself Merry Little Christmas`; "What Are You Gonna
  Do" → `What Gonna Do`; "(The) Christmas Song (Chestnuts Roasting on an
  Open Fire)" → `Christmas Song Chestnuts Roasting`).

## `release_group_year` backfill state (831 wanted requests)

| State | Count |
|---|---|
| `release_group_year` populated | 723 |
| `release_group_year` NULL | 109 |
| Has `mb_release_group_id` | 761 |
| `release_group_year` differs from `year` | 24 |
| `release_group_year` equals `year` | 545 |

Of the 109 NULL-rg_year wanted requests:
- 9 have a numeric `mb_release_group_id` (Discogs master ID format,
  e.g. Kid A's `"21501"`).
- 78 have a populated `mb_release_group_id` (UUID or numeric) but
  `release_group_year` NULL.
- 31 have NULL `mb_release_group_id` entirely.

Request 1868 (Radiohead / Kid A) — called out by the May 19 brainstorm
as the canonical reissue case (`year=2008`, expected `release_group_year=2000`) —
has `release_group_year` NULL and a numeric `mb_release_group_id="21501"`.
Of its 18 post-deploy searches, none used the `unwild_rg_year` strategy.
Across all 15,396 searches in the window, only 2 used `unwild_rg_year`.

## Specific case drill-downs (cohort B, slskd-probed)

Twelve representative cohort-B requests probed by querying slskd
directly with alternate patterns and inspecting which peer folders the
matcher saw on production searches.

| ID | Artist / Album | Pattern observed |
|---|---|---|
| 1868 | Radiohead / Kid A | Multiple Kid A folders found on production search (e.g. user `sayaka`: `Radiohead - 2000 - Kid A {2008,...,24-192,VInyl}`, 10 audio files; user `justsomeguy9182`: `music\Radiohead\Kid A [Album] (2008)`, 11 files). Request `total_tracks=14`. Strict count gate at `lib/matching.py:451` rejects (`fc != track_num`). `unwild_rg_year` slot did not fire (rg_year NULL). |
| 455 | Bon Iver / 22, a Million | Multiple peer folders served standard 10-track release (`matched_tracks=10`, `avg_ratio≈0.91`). Request `total_tracks=12` (10 + 2-track Disc 2). Strict count gate rejected. |
| 4473 | Bright Eyes / I'm Wide Awake, It's Morning | Peers served 10-track release at `mt=10/10, ar=0.96`. Request `total_tracks=14` (10 + 4-track bonus disc). Strict count gate rejected. Unicode apostrophe in title normalised correctly by `_normalize_title`. |
| 1752 | Okkervil River / The Stage Names | Peers served standard 9-track release. Request `total_tracks=10` (includes iTunes-bonus "Love to a Monster"). Strict count gate rejected; slskd probes did not surface any peer with the 10-track variant. |
| 3526 | Darwin Deez / Darwin Deez | Peers served US 10-track release. Request `total_tracks=11` (US+UK "Bad Day" bonus). |
| 379 | The National / Sad Songs for Dirty Lovers | Request points at 2021 Remaster MBID (`year=2021`, `release_group_year=2003`). Peers served original 12-track 2003 release at `mt=12/12, ar=0.90`. Remaster has 19 tracks (12 + 7 bonus). Strict count gate rejected. |
| 1859 | Kate Bush / Aerial | Peers `minorsecond, yuljk` served the album as `CD 1`/`CD 2` subfolders (9 files each). Matcher browsed leaf directories with `fc=9`. Request `total_tracks=18`. Other peers (`eddie666, cafrune, amyslskduser`) served the album in a flat layout; slskd browses of those peers in this investigation returned mixed results, with several producing `fc=0` apparently due to non-audio subdirs (`Artwork/`). |
| 497 | The National / Conversation 16 | Request `total_tracks=1` (single). All peers serve the track only as part of the 11-track *High Violet* album. No standalone single-file folder observed. |
| 350 | Tony Sheridan / My Bonnie | Request `total_tracks=2` (1961 single). Track only available bundled on 14-track Beatles compilations (`Early Tapes Of`, `First Recordings 50th Anniversary`). No standalone 2-track folder observed. |
| 117 | Ben Folds Five / Philosophy | Request `total_tracks=1` (1996 promo single). Track only served on the 12-track 1995 self-titled debut. No standalone folder observed. |
| 2257 | Big Thief / Dragon New Warm Mountain (demo) | Request `total_tracks=1` (album-cut demo). Peers serve only the 20-track regular album. No standalone demo folder observed. |
| 70 | Various Artists / Here Comes Trouble | Request `total_tracks=60` (3-disc compilation). Query "Here Comes Trouble" hits Bad Company / Scatterbrain / Reggae compilations as false positives. The Trouble On Vinyl release at `fc=6` is a different release ("Remixes LP"). No correct pressing observed on slskd. |

Aggregated cohort-B classification across the 12 probed:

| Class | Cases | Description |
|---|---|---|
| Bonus-track / wrong-pressing where peers serve a sibling release | 6 | 3526, 455, 4473, 1752, 379, 1868 (partial) |
| 1-track / promo / demo with no standalone folder on the network | 4 | 497, 350, 117, 2257 |
| Multi-disc subfolder layout split across `CD 1`/`CD 2` siblings | 1 | 1859 (some overlap with 1868) |
| Compilation with VA query collapse | 1 | 70 |
| Year-mismatched MBID (`rg_year` ≠ `year`) | 1 | 379 (also bonus-track) |
| Discogs numeric ID backfill gap | 1 | 1868 |

## Specific case drill-downs (cohort A, slskd-probed)

Seventeen representative cohort-A requests (≥ 70% zero-result rate)
probed by querying slskd with artist-only, alternate-spelling,
track-only, and label queries.

| ID | Artist / Album | Artist-only probe result | Classification |
|---|---|---|---|
| 4628 | Russian Winters / Last Battles | 43 files on network, 0 are the requested band (false positives: "Russian Futurists", "My Dying Bride Russian Tribute") | REAL_GAP |
| 4627 | Russian Winters / Give Up The Ghost | same — band absent | REAL_GAP |
| 973 | Spit Syndicate / The Future's Bright | 164 files — later studio LPs present, requested mixtape not | REAL_GAP for this release |
| 1716 | Carol Chell / Sing a Song of Playschool | 8 files — only comp tracks from 1972 Bang On A Drum / Play School | REAL_GAP for this release |
| 654 | Red Jezebel / Joyful Possibilities | 141 files but all false-positive ("Andi Almqvist Red Room", "Red Molly Jezebel", "Dee Clark Red Riding Hood") | REAL_GAP |
| 587 | The Fergusons / El Presidente | 26 files but all are a single track ("Everything's Gone Bad") on Triple J Hottest 100 vol. 10 compilations | REAL_GAP for this album |
| 585 | The Fergusons / Four Piece Demos | same — same single Triple J comp track only | REAL_GAP |
| 1694 | MONGEEYA / Listen to This. (2025) | 0 files network-wide | REAL_GAP |
| 1467 | Cam Butler / Healing Feelings | 79 files — Silber Sounds Halloween, Silent Ballet vol 9 comp appearances only | REAL_GAP for this album |
| 596 | The Panics / Songs From Another Room | 142 files for "The Panics Cruel Guards" (different album); requested album absent | REAL_GAP for this album |
| 1302 | Vagenius / Vagenius | 1 file ("00 Vagenius — Take it to The Maxx" SXSW 2005 sampler) | REAL_GAP for this album |
| 3814 | Marie Wilson / Heartbreak | 282 files but all coincidental (Sharon Marie/Brian Wilson, Snoop+Teena Marie) | REAL_GAP |
| 3718 | Jimmy Stewart (12) / Live At The Rainbow Hotel | 0 files for the search shape | REAL_GAP |
| 1330 | A Shelter in the Desert / [reSound] | 70 files all false-positive (Fatboy Slim, Rolling Stones bootlegs, Nonsun) | REAL_GAP |
| 1193 | firebrandboy / Songs for Cake | 15 files — chiptune compilations / Bandcamp comps only | REAL_GAP for this album |
| 1368 | Tetrafide Percussion / New Electronic | 0 files; alternate album "Lords of Ajembe" also 0 | REAL_GAP |
| 522 | Phoebe Bridgers / The Face \| 1975 Tribute Concert | "Punisher" → 3,157 files (artist well-seeded); requested album is 1-track ("Girls", The 1975 cover) and IS findable under the filename `Phoebe Bridgers - Punish Me / 12 Girls (The 1975 Cover, The Face Session)` | QUERY_TOO_NARROW + METADATA_DRIFT (1-track release) |

Cohort A summary across 17 probed:
- 16 classified REAL_GAP (94% — album genuinely absent on Soulseek)
- 1 classified recoverable (5.9% — Phoebe Bridgers 522, 1-track release)

Composition skew: Australian indie (Cam Butler, The Panics, Spit
Syndicate, The Fergusons, Marie Wilson, MONGEEYA, Red Jezebel),
early-2000s demos/EPs, niche compilations (Carol Chell BBC Play School
1972), and one-band-no-results (Russian Winters Toronto post-rock).

## Generator behaviour — observed query shape examples

Request 1868 (Radiohead / Kid A) emitted queries (from `search_log`):

| Strategy | Query |
|---|---|
| `default` | `*adiohead Kid A` |
| `literal` | `Radiohead Kid A` |
| `literal_flac` | `Radiohead Kid A FLAC` |
| `literal_lossless` | `Radiohead Kid A lossless` |
| `unwild_year` | `Radiohead Kid A 2008` |
| `track_0_artist` | `*adiohead Motion Picture Soundtrack` |
| `track_1_artist` | `*adiohead How Disappear Completely` |
| `track_2_artist` | `*adiohead Everything Right Place` |

Observations:
- `track_0_artist` query is the title of MB track 7 of the 14-track 2008
  release ("Motion Picture Soundtrack"). The track ordering at ordinal 0
  in the request's `album_tracks` does not match the album's track 1.
- `track_1_artist` query has `How Disappear Completely` — original MB
  track is "How to Disappear Completely". Token `to` was dropped.
- `track_2_artist` query has `Everything Right Place` — original MB
  track is "Everything in Its Right Place". Tokens `in` and `Its` were
  dropped.
- `unwild_year` uses 2008 (the request's MBID year). Brainstorm
  observed that `Radiohead Kid 2000` returns ~hundreds of results vs
  ~6–10 for `Radiohead Kid 2008`; no slot in the deployed plan emits a
  2000-anchored query for this request.

Sample candidate JSONB for request 1868 / `unwild_year` (`result_count=6`):
- All 17 scored candidates have `matched_tracks=0` and `avg_ratio=0.0`.
- Real Kid A folder examples among them:
  - `sayaka`: `Radiohead - 2000 - Kid A {2008, 5284821-AB; 5284831-CD, 24-192, VInyl}`, FLAC 24/192, `file_count=10`, `total_tracks=14`.
  - `justsomeguy9182`: `music\Radiohead\Kid A [Album] (2008)`, FLAC, `file_count=11`, `total_tracks=14`.
  - `geekmusicwtf`: `music\Radiohead\Kid A (2008)`, FLAC, `file_count=1`, `total_tracks=14`.

## Generator ID and plan state

| Setting | Value |
|---|---|
| `SEARCH_PLAN_GENERATOR_ID` (in `lib/search.py:35`) | `"search-plan/2026-05-19-1"` |
| Deployed `SEARCH_PLAN_GENERATOR_ID` on doc2 | `"search-plan/2026-05-19-1"` |
| Pre-deploy generator (per origin doc) | unspecified literal; entropy work bumped from prior id |

Plan cycle counters observed on the chronically-searching cohort:
- Most cohort-B requests have `plan_cycle_count = 2–4` after 5.5 days,
  consistent with full plan execution per ~5-min cycle and cursor
  wrapping each completed plan.

## Cohort imported-rate by creation date

| Cohort | Total | Still wanted | Imported | % still wanted |
|---|---|---|---|---|
| Created before April 2026 | 370 | 96 | 273 | 25.9% |
| Created April 2026 | 1,867 | 400 | 1,465 | 21.4% |
| Created May 2026 (pre-deploy) | 2,099 | 327 | 1,762 | 15.6% |
| Created post-deploy (2026-05-20+) | 33 | 9 | 24 | 27.3% |

Aggregate: 832 of 4,369 (19.0%) currently `wanted`. Manual: 1.
Downloading: 8.

## Tooling / observability surface used in this investigation

What was already present (used heavily):
- `pipeline-cli query --json` for raw-SQL diagnostics.
- `pipeline-cli show <request_id>` for human-readable per-request rendering.
- `search_log.candidates` JSONB column.
- `search_log.final_state` column.
- `search_log.pre_filter_skip_count` column (from R3).
- `pipeline-cli search-plan show / saturation / dry-run` subcommands.

What was inferred via JSONB introspection because no dedicated column exists:
- Whether a `no_match` outcome was caused by strict-count rejection vs
  low `avg_ratio` vs all-skipped-pre-filter vs denylisted-user — all
  required walking `candidates` JSONB entries or joining `album_requests`
  / `album_tracks`.
- Whether `result_count` reflects a natural-small result set or a
  saturated/truncated set — currently inferrable only via `final_state`
  string parsing.
- Query token count and distinctiveness — currently requires substring
  parsing of `query` text.
- Multi-disc track-count mismatch — currently requires JOIN against the
  current `album_tracks` row count.

What was inferred via slskd HTTP probing because no historical state is kept:
- Whether the artist's catalog is present on the Soulseek network at all
  (artist-only probe).
- Whether peers serve the same release in different pressings.
- Whether the requested album is in a subfolder layout the matcher
  cannot aggregate.

---

## Open questions surfaced by the data

These are observations only — items where the data shows divergence
between expectation and outcome but where the cause is not established
by this document.

- Why `track_X_artist` queries for some requests contain track titles
  that do not appear at the track's ordinal position in the request's
  `album_tracks` (Kid A example).
- Whether the low-entropy stopword set (currently `the`, `you`, `from`,
  `and`, and apparently `have`, `are`) is the right cut-list given that
  track-fallback queries are losing distinctive opening words.
- Whether the wildcard-first-token rule produces partial-character masks
  (`Death Cab for Cutie` → `*eath *ab *utie` was reported by one
  investigator probe) or full-token drops.
- Whether the rg_year backfill silently fails on numeric
  `mb_release_group_id` values (Discogs master IDs) or whether the
  78-NULL cohort has a different cause.
- Whether the `selftitled_artist_track_0_flac` slot's low conversion
  (2/128) is a generator problem or a matcher problem (its parent
  `selftitled_artist_track_0` converted at 8%).
- Whether the post-browse strict-count gate at `lib/matching.py:451`
  contributes more rejections than the pre-filter at `:350`.

---

## Sources

- Primary: `pipeline-cli query` against the live PostgreSQL pipeline DB
  (`192.168.100.11:5432`).
- slskd HTTP API at `http://localhost:5030` (doc2-local).
- `docs/brainstorms/2026-05-19-search-plan-entropy-requirements.md` — origin spec.
- `docs/plans/2026-05-19-001-feat-search-plan-entropy-and-matcher-prefilter-plan.md` — deployed plan.
- `docs/pipeline-db-schema.md` — schema reference.
- `lib/search.py`, `lib/matching.py`, `web/mb.py`, `scripts/backfill_release_group_year.py` — code paths inspected.
