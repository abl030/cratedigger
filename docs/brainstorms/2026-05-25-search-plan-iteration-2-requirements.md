---
date: 2026-05-25
topic: search-plan-iteration-2-and-request-observability
---

# Search Plan Iteration 2 + Request Observability Subsystem

## Summary

Second iteration of the search-plan generator built on 5.5 days of
post-deploy data from the 2026-05-19-1 entropy work, plus a new
operator-facing observability subsystem that surfaces unfindable albums,
metadata-quality gaps, and per-search forensics through a single
composable API.

This iteration is two coupled changes that ship together. First, a set
of generator and schema changes (kill `literal_lossless`, add
catalog-number + track_3 slots, distinctiveness-ranked track selection,
VA-specific strategy mix backed by a new per-track-artist column,
stopword-strip cleanup, multi-disc subfolder investigation).
Second, a new "request observability" subsystem — three operator-facing
APIs (unfindable cohort, field-level data-quality telemetry, search-log
forensics) composed into one triage view, with a per-route discoverability
endpoint that makes the whole API surface introspectable.

Crucially, this iteration does **not** loosen the matcher's strict
count gate. The 5.5-day data shows ~280 requests (the dominant cohort B
sub-pattern) are starving because peers serve sibling pressings of the
same release group. The instinct to "fix" the matcher to accept those
sibling pressings would violate cratedigger's pressing-level curation
invariant. The right answer is *operator visibility*: surface the
"wrong-pressing available" state so the operator can decide whether to
Replace to the available sibling. The sibling-aware surfacer itself is
deferred to a separate future brainstorm.

The system also never auto-throttles search cadence — cratedigger is an
archival watch process whose entire value depends on catching the
moment a fresh peer arrives with a long-tail album. Detection of
unfindable cohorts is purely descriptive metadata, never an input to
search behaviour.

---

## Problem Frame

See findings document for full evidence: `docs/brainstorms/2026-05-25-search-plan-post-deploy-findings.md`.

The May 19 entropy work delivered its primary objective — saturation
collapsed from ~3% to 0.45%. But 5.5 days of post-deploy data reveal a
different dominant failure mode:

- 422 of 832 still-wanted requests (51%) sit in cohort B — the matcher
  receives candidates on every search and rejects 100% of them. Drill-down
  shows ~50% of this is bonus-track / wrong-pressing (peers serve a
  sibling release of the same release-group; strict count gate rejects),
  ~10% is multi-disc subfolder layout (peers split into `CD 1`/`CD 2`,
  matcher only sees leaf), ~6% is 1-track release requests (the track
  exists only inside parent-album folders), and the rest spans VA
  collapse and other shapes.
- 119 of 832 (14%) sit in cohort A — zero results dominant. 94%
  confirmed by direct slskd probing to be genuine network gaps (artist
  not on the network at all, or artist present but this specific
  release never shared). 1 of 17 probed was recoverable (Phoebe Bridgers
  *The Face Tribute Concert* — 1-track release findable under the track
  filename rather than the editorial album title).
- The Various Artists sub-cohort (34 requests, 3.7% of active) accounts
  for 26% of all near-cap saturation events — 10× the non-VA rate.
  Compound causes: artist tokens stripped as low-entropy (`Various` and
  `Various Artists` land in the stopword set), track-fallback queries
  emit title-only (artist-prepend got eaten), and stopword strip is
  removing distinctive opening words from track titles (`Have Yourself
  a Merry Little Christmas` → `Yourself Merry Little Christmas`).
- `literal_lossless` strategy ran 2,093 times in 5.5 days and produced
  1 successful match (99.95% zero-result). Peers do not tag folders
  with the literal token "lossless".
- 78 wanted requests have `mb_release_group_id` set but
  `release_group_year` NULL — 9 are numeric Discogs master IDs that
  the backfill silently 404s against the MB mirror; the rest are
  cases where the rg_id resolution failed and is invisible to the
  operator. The `unwild_rg_year` slot ran 2 times total across all
  15,396 searches because the data feeding it is sparse.
- The investigation needed many SQL JOINs because no column directly
  records *why* a `no_match` outcome happened — every per-request
  triage required walking `candidates` JSONB to reconstruct rejection
  cause.

The data narrows the problem cleanly into three axes:

1. **Generator entropy still has visible bugs** — wrong track titles
   at track_0, stopword strips beyond the documented set, dead slot
   (`literal_lossless`), VA query collapse.
2. **The matcher's strict gates are correct, but their reject reasons
   are invisible** — operators have no surface to see "this album is
   on the network in a sibling pressing" or "this album has never
   appeared and the artist isn't on Soulseek either."
3. **Backfilled metadata has invisible gaps** — fields that should
   feed strategy slots are NULL for reasons the system never records,
   and the operator can't see which requests are running degraded
   plans because their data is incomplete.

The brainstorm leading to this document covered eight findings; this
requirements doc captures the convergence position on each.

---

## Core Invariants

These are non-negotiable product positions that govern every decision
below. Future readers should not re-litigate these without explicit
operator sign-off.

- **Strict-pressing curation.** A request points at a specific MB or
  Discogs release. The matcher must never substitute a sibling
  release/pressing automatically. Multiple pressings of the same album
  are intentional in the operator's collection; the system never
  decides "close enough."
- **Never stop searching.** Cratedigger is an archival watch process.
  The system never auto-throttles or auto-resolves a request based on
  search behaviour. Cadence stays constant forever. Detection of
  unfindable cohorts is descriptive metadata only — never an input to
  search behaviour.
- **Dual-source uniform, no adapter code.** Any external-metadata
  field populated from MusicBrainz or Discogs is populated to a single
  column in a single shape, by code that fills it identically from
  either source. Backfills are 100%. Generator and matcher read one
  column with one shape regardless of source.
- **API now, UI later.** Every operator-facing surface in this
  iteration ships as a JSON API + a `pipeline-cli` subcommand.
  Dashboard / web-UI / notification consumption is a separate
  iteration in its own right.
- **Single source of truth.** Constants like the stopword set live in
  exactly one place in the codebase. Any change to such a constant is
  a single-source-of-truth edit; all callers read from the canonical
  definition.
- **Build it right.** No incremental "MB now, Discogs later" splits.
  No adapter code bridging two shapes. No "we'll backfill 80% and
  surface the rest later." When a new field or subsystem ships, it
  ships uniformly across sources, fully backfilled, with the operator
  surface attached.

---

## Requirements

### Generator and matcher

- **R1. Retire `literal_lossless` strategy slot.** Drop the slot from
  the plan generator. Bump `SEARCH_PLAN_GENERATOR_ID`. Evidence: 99.95%
  zero-result over 2,093 searches; the literal token "lossless" does
  not appear in peer folder names in any meaningful frequency.

- **R2. Add `catalog_number` strategy slot.** When the request's
  metadata exposes a catalog number from either MB (`labels[*].catalog-number`)
  or Discogs (`labels[*].catno`), the generator emits a slot with the
  catalog number as a raw token plus the artist token. Catalog numbers
  are high-entropy by construction and frequently appear in peer
  folder names for vinyl rips, box sets, and audiophile pressings —
  the cases where the operator most cares about pressing-specific
  identity. Slot is a no-op when the request lacks a catalog number;
  this is acceptable behaviour. Optional length filter (`>=4 chars`)
  to avoid collisions on short generic numbers; final cutoff decided
  during planning.

- **R3. Add `track_3_artist` strategy slot.** Extends the existing
  `track_N_artist` slot family. Same construction shape as
  `track_0_artist`. Fills the gap when track_0 is generic and
  track_1/2 don't disambiguate.

- **R4. Distinctiveness-ranked track selection for track-fallback
  slots.** Replace MB-track-ordinal-based selection (`track_0_artist`
  uses MB track index 0) with a simple distinctiveness scoring over
  all the album's track titles. The N most distinctive titles become
  the track-fallback slots; the slot names retain `track_0_artist /
  _1 / _2 / _3` but reference distinctive-rank, not MB-ordinal.
  Scoring stays deliberately dumb — no IDF, no ML, no corpus
  statistics. Candidate shapes (planning chooses):
  - `len(longest_token) * num_tokens` with a small generic-titles
    blacklist (`Intro`, `Outro`, `Untitled`, `Overture`, `Theme`,
    `Soundtrack`, `Motion Picture`).
  - Or any other small heuristic that demonstrably picks "Everything
    in Its Right Place" over "Motion Picture Soundtrack" for Kid A
    without special-casing.

- **R5. Stopword strip cleanup.** Trace every code path that strips
  tokens from artist names and track titles. Collapse them to one
  canonical function reading from one canonical constant
  (`STOPWORDS = {"the", "you", "from", "and"}` — the documented set
  from the May 19 work). The current generator output reveals
  additional tokens being stripped (`to`, `in`, `Its`, `Have`, `a`,
  `Are`, `You` — not all in the documented set). The fix is structural:
  one strip function, one constant, all callers route through it.
  No change to the set's *contents* in this iteration; the change is
  that the set's enforcement becomes consistent.

- **R6. Wildcard-all-tokens stays.** The current behaviour of
  wildcarding the first character of every token (`The Beatles` →
  `*he *eatles`, `Death Cab for Cutie` → `*eath *ab *or *utie`) is
  **by design**, not a bug. Rationale: Soulseek's per-token index
  may filter any individual token, and we don't know which.
  All-token wildcarding maximises probability of getting through
  regardless of which token is banned. Single-token wildcarding
  leaves the un-wildcarded tokens at risk of silent index dropouts.
  This iteration adds an inline comment to the wildcard helper plus
  a section in `CLAUDE.md` documenting the rationale so future
  readers don't repeat the agent's misinterpretation.

- **R7. Multi-disc subfolder aggregation: investigation phase first.**
  The current multi-disc handling in `lib/matching.py` and adjacent
  code is legacy from the Soularr era and has no documented behaviour
  or test coverage on this iteration's terms. Before any redesign,
  planning must produce an investigation document tracing the end-
  to-end multi-disc flow: how `album_tracks` rows are populated for
  multi-disc releases, what queries the generator emits for them
  (per-disc, combined, both), how `track_num` is computed at match
  time (per-disc, combined), and what fixtures/tests exist. Output:
  a small investigation document attached to the resulting plan.

- **R8. Multi-disc subfolder aggregation: redesign (scope-conditional).**
  After R7 lands, this iteration's plan extends the matcher to
  recognise sibling subdirectories under a common parent matching
  `(?i)(disc|cd|digital media|side)\s*\d+` and treat them as one
  logical album for the count gate. This is same-MBID, same-pressing,
  layout-only aggregation — not pressing substitution. Specific
  design (per-disc subqueries vs. parent-folder aggregation vs.
  matcher-time aggregation) is decided post-R7; planning writes the
  design before code lands.

- **R9. Single `SEARCH_PLAN_GENERATOR_ID` bump.** All generator-output
  changes (R1-R4, R5, R7-R8 if redesign lands in scope, and the
  per-track-artist VA changes from R12-R13) ride one ID bump. Bumping
  mid-implementation would generate intermediate plans the next bump
  invalidates.

### Per-track-artist schema (enables VA strategy mix)

- **R10. Add `album_tracks.track_artist TEXT` column.** Stores the
  per-track artist credit. NULL means "unresolved" or "not credited"
  — distinguished via R15 (field-resolution side table). One column,
  one shape, regardless of source.

- **R11. Dual-source uniform backfill of `track_artist`.** Single
  backfill script populates the column for every existing
  `album_tracks` row from whichever mirror has the credit:
  - MB-sourced requests: hit the MB mirror's recording credit data
    via the release endpoint.
  - Discogs-sourced requests: hit the Discogs mirror's master/release
    endpoint for per-track artist credits.
  - Both write to the same column in the same shape. No conditional
    branches in consumer code.
  - Resolution outcomes (success / failure / reason) record into the
    field-resolutions side table from R15.
  - Backfill is 100% — every row attempted regardless of source.

- **R12. VA detection via primary-artist-credit identity.** Detect
  Various Artists requests by the upstream mirror's primary-artist
  identity, not by string matching on `artist_name`:
  - MB: primary-artist-credit MBID = `89ad4ac3-39f7-470e-963a-56509c546377`.
  - Discogs: primary artist ID = `194` ("Various").
  - Detection records as a stable boolean column or generator-time
    derivation, planning chooses. Either way, the detection rule is
    centralised — one function, one identity check per source, no
    string-match fallbacks.

- **R13. VA-specific strategy mix.** When the VA flag is set, the
  generator's strategy mix changes:
  - **Drop** `default`, `literal`, `literal_flac` slots (they
    collapse to title-only after artist-token stripping and saturate
    on common compilation titles).
  - **Keep** `unwild_year` (high entropy when present).
  - **Add** `<track_artist> <track_title>` queries for the 2-3
    most distinctive tracks (using the distinctiveness scoring from
    R4, but on per-track-artist+title compounds, not just titles).
  - **Add** a compilation-series slot when the title contains
    volume/series markers (`Vol`, `Volume`, `#`, Roman numerals at
    end). The volume number becomes its own high-entropy token.
  - Bumped under R9.

### Field-level data-quality telemetry

- **R14. Add `album_request_field_resolutions` side table.** Side
  table (not per-field columns on `album_requests`) tracking resolution
  attempts for every external-metadata field cratedigger fills from
  MB or Discogs. Shape (final schema decided in planning):
  - `request_id INTEGER NOT NULL` (FK).
  - `field_name TEXT NOT NULL` (e.g. `release_group_id`,
    `release_group_year`, `track_artist`, `catalog_number`).
  - `resolved_at TIMESTAMPTZ` — when the most recent attempt happened.
  - `status TEXT` — `resolved`, `unresolved_404`, `unresolved_malformed`,
    `unresolved_mirror_unavailable`, `unresolved_field_missing_upstream`,
    others as discovered.
  - `reason_code TEXT NULLABLE` — human-meaningful free-form
    descriptor.
  - `attempts INTEGER` — count of resolution attempts to date.
  - UNIQUE `(request_id, field_name)` — one row per request/field
    pair; attempts increments on retry, status overwrites.

- **R15. Fields tracked by R14 in this iteration:**
  - `mb_release_group_id` (the 31 NULL-rg_id requests).
  - `release_group_year` (the 78 NULL-rg_year requests).
  - `track_artist` (new from R10, per-track granularity is
    request-level rollup: "any track unresolved" suffices for
    operator triage).
  - `catalog_number` (new from R2).
  - Schema is extensible — additional fields added by writing one
    resolver function and one side-table row at enqueue time.

- **R16. Inline field resolution at enqueue time, proceed-with-NULL.**
  `web/routes/pipeline.py::post_pipeline_add` and the CLI add path
  resolve every R15 field from the appropriate mirror at request
  creation time. Resolution failures **do not block** the request
  from landing in `wanted`. Failures record into the side table with
  a status code. The generator handles NULL fields gracefully (skips
  slots that depend on them) and the operator surface (R23) makes
  the cohort of degraded-plan requests visible.

- **R17. Dual-source uniform backfill of field resolutions.** Single
  backfill script populates R14 for every existing wanted request,
  filling whatever's NULL today:
  - For numeric `mb_release_group_id` values (Discogs master IDs),
    hit the Discogs mirror's `master/{id}?fmt=json` endpoint, take
    `year`, write to `release_group_year`.
  - For UUID `mb_release_group_id` values, hit the MB mirror's
    `release-group/{uuid}` endpoint.
  - For NULL `mb_release_group_id` entirely, first resolve the rg_id
    from the release_id (both sources expose this), then resolve
    rg_year.
  - All outcomes (success / failure / reason) record into the field-
    resolutions side table.
  - Backfill is idempotent — re-runnable, only updates unresolved
    rows.

### Unfindable cohort detection

- **R18. Catalog-probe-grounded unfindable detection.** A periodic
  artist-only probe runs against slskd for each request in the
  candidate cohort. Cadence (planning chooses): roughly weekly per
  cohort member, scheduled to spread load. The probe's purpose is
  **detection signal enrichment**, not search behaviour change —
  results feed the categorisation in R19, nothing else.

- **R19. 4-category unfindable taxonomy.** The unfindable
  categorisation surfaces requests with structured reasons:
  - `artist_absent` — artist-only probe returns no genuine matches
    across K consecutive probes (planning chooses K, likely 2-3).
    Operator action: source the artist's catalog yourself, Replace
    via Discogs, or accept loss.
  - `album_absent_artist_present` — artist has ≥N catalog files on
    the network, but the requested album has never appeared across
    M plan cycles with zero candidate matches. Operator action:
    source this specific album yourself.
  - `one_track_structural` — `total_tracks == 1` on the request.
    The track may exist as part of parent-album folders on the
    network, but per the strict-pressing invariant (Core Invariants)
    cratedigger never extracts it. Operator action: source yourself
    or Replace to the parent-album MBID. Detected at enqueue, not
    via probe.
  - `wrong_pressing_available` — search-log analysis shows the
    request has consistently received candidates with high
    `matched_tracks` and `avg_ratio` but mismatched `file_count`
    (the strict-count-gate rejection pattern). Indicates a sibling
    release of the same release-group is on the network. This
    category bridges into a future sibling-aware surfacer (out of
    scope here; see Scope Boundaries) but the detection lands now.

- **R20. Unfindable detection is purely descriptive.** No detection
  outcome ever changes search behaviour. Cadence stays constant.
  Cohort membership is metadata only, consumed by the API in R23.
  This is a non-negotiable invariant — future code reviewers should
  reject any change that ties search throttling to unfindable
  status.

- **R21. Long-tail-rescue event capture.** When a request transitions
  from any unfindable category to `imported`, the system records
  the event with timestamp, prior unfindable category, time-in-
  unfindable-state, and import details. This event is first-class
  audit data, queryable through the triage API in R28. The
  operator-facing celebration UX (notifications, dashboard banners,
  whatever) is out of scope for this iteration but the data lands
  now so future UI work consumes a populated audit trail.

### Search-log forensics columns

- **R22. `search_log.rejection_reason TEXT`** populated at log time
  with the reason a `no_match` outcome happened. Enum-shaped:
  `strict_count_mismatch`, `avg_ratio_low`, `all_skipped_pre_filter`,
  `bitrate_below_min`, `denylisted_user`, `cooldown`,
  `cap_truncation_no_survivors`, others as discovered. Trivial code
  cost at the matcher's reject points.

- **R23. `search_log.result_count_uncapped INTEGER`** records the
  pre-cap result-set size from slskd. Today's `result_count` is
  post-cap (silently capped at 1000); operators cannot distinguish
  "saturated and truncated" from "natural-small."

- **R24. `search_log.query_token_count INTEGER` and
  `search_log.query_distinct_token_count INTEGER`** record the
  number of tokens and distinct tokens in the emitted query. Cheap
  proxy for query entropy.

- **R25. `search_log.expected_track_count INTEGER`** snapshots the
  request's track count at search time. Surfaces multi-disc /
  bonus-track mismatches without JOINing the live `album_tracks`
  table (which moves with re-imports).

- **R26. `search_log.matcher_score_top1 FLOAT`** records the top
  scored candidate's `(matched_tracks, avg_ratio)` composite score.
  Lets operators see "rejection magnitude" — how close did we come?

- **R27. `search_log.query_template TEXT`** records the template
  shape that produced the query (e.g. `"{artist} {track_N}"`,
  `"{artist} {catalog_number}"`). Distinguishes "this template
  underperforms" from "this template fired against generic inputs."

- **R28. `album_requests.failure_class TEXT`** records the 5-bucket
  classification (`A_zero_results_dominant`, `B_cands_never_match`,
  `D_found_but_no_import`, `E_mixed`, `resolved`) at plan-wrap time.
  Materialised so the triage subsystem doesn't recompute it on
  every read.

- **R29. `request_search_summary` view.** A SQL view aggregating per-
  request rollup over `search_log`: total searches, with_cands,
  found_count, near_cap_count, first_strategy_with_cands,
  dominant_failure_reason. Powers the triage subsystem in R31.

### Triage subsystem (composable observability)

- **R30. One composable triage subsystem.** Three operator-facing
  data domains (unfindable cohort from R18-R21, field-quality from
  R14-R17, search-forensics from R22-R29) are exposed through one
  triage API that composes all three into one coherent view.
  Underlying data lives in three places; the API does the
  composition.

- **R31. CLI: `pipeline-cli triage <request_id>`.** Per-request view
  surfacing: current status, last N searches with rejection reasons,
  unfindable categorisation (if any), data-quality field-resolution
  status, search summary stats. Single human-readable rendering.

- **R32. CLI: `pipeline-cli triage list --filter=<filter>`.** Cohort
  listing. Filter values: `unfindable`, `unfindable:artist_absent`,
  `unfindable:album_absent_artist_present`, etc.; `data-quality`,
  `data-quality:rg_year`, `data-quality:track_artist`, etc.;
  `search-not-converting` (cohort B classification); `all` (any
  flagged condition).

- **R33. API endpoints mirroring R31 and R32.** `GET /api/triage/<id>`
  and `GET /api/triage/list?filter=<filter>`. Same JSON shape. Same
  CLI ⇄ API symmetry as the rest of the repo per
  `.claude/rules/code-quality.md` § "CLI ⇄ API Surface Symmetry."
  Registered in `TestRouteContractAudit.CLASSIFIED_ROUTES`.

### API discoverability

- **R34. `GET /api/_index` endpoint.** Self-documenting API surface.
  Introspects `Handler._FUNC_GET_ROUTES`, `_FUNC_POST_ROUTES`,
  `_FUNC_GET_PATTERNS` plus the Pydantic request-model classes for
  POST routes. Returns JSON: route path, method, classification
  bucket, request model name (when applicable), description string.
  No OpenAPI, no Swagger UI — structured introspection of what the
  dispatch tables already expose.

- **R35. CLI: `pipeline-cli routes`.** Mirror of R34 for CLI. Walks
  argparse subparsers, emits subcommand list with args and
  descriptions. Same data shape as R34's API output.

- **R36. Description metadata on every route.** Every registered
  route (GET, POST, pattern-route) carries a one-line description
  string. The existing `TestRouteContractAudit.CLASSIFIED_ROUTES`
  audit is extended to fail when a registered route lacks a
  description. This enforces R34's usefulness — no untitled
  endpoints can ship.

---

## Acceptance Examples

- **AE1. Covers R1, R2, R3, R4, R5, R9.** Given Kid A request 1868
  (year=2008, release_group_year backfilled to 2000 via R17) under
  the new generator, a single cycle of the plan contains no
  `literal_lossless` slot, contains a `catalog_number` slot with
  Kid A's pressing catalog number as a token, contains
  `track_0_artist` and `track_3_artist` slots with distinctiveness-
  ranked track titles (not "Motion Picture Soundtrack"), and emits
  no track-fallback query missing words from the stopword set
  beyond `the / you / from / and`.

- **AE2. Covers R10, R11, R12, R13.** Given a VA compilation request
  (artist primary credit MBID = Various-Artists or Discogs ID 194)
  with backfilled `track_artist` data, the generator emits no
  `default`, `literal`, or `literal_flac` slots. It emits at least
  one `<track_artist> <track_title>` slot (e.g. for a Christmas
  compilation, `Nat King Cole (The) Christmas Song` or similar).
  When the title contains volume markers (`Vol 05`), a compilation-
  series slot fires.

- **AE3. Covers R7, R8.** Before any code changes to multi-disc
  matching land, planning produces an investigation document
  attached to the plan describing the current multi-disc flow end-
  to-end. After the redesign lands, given Kate Bush *Aerial* request
  1859 (`total_tracks=18`) and a peer serving `Aerial/CD 1/` (9
  files) + `Aerial/CD 2/` (9 files), the matcher recognises the
  sibling structure, aggregates to `fc=18`, and accepts the
  candidate.

- **AE4. Covers R14, R15, R16, R17.** Given Kid A request 1868 with
  numeric `mb_release_group_id="21501"` (Discogs master ID),
  after the backfill runs, `release_group_year=2000` is populated
  and one row in `album_request_field_resolutions` records
  `field_name='release_group_year', status='resolved'`. Given a
  request whose Discogs master entry has no `year` field, the
  request lands with `release_group_year=NULL` but a row in the
  side table records `status='unresolved_field_missing_upstream'`.
  The next plan cycle skips `unwild_rg_year` for that request
  without retrying resolution.

- **AE5. Covers R18, R19, R20.** Given Russian Winters request 4628
  (zero results across 19 cycles), after 2-3 weekly artist-only
  probes return no genuine matches, the request is categorised
  `artist_absent` in the unfindable surface. Cadence does **not**
  change — the next plan cycle fires on schedule. The request stays
  in `wanted`.

- **AE6. Covers R21.** Given Russian Winters request 4628 (currently
  categorised `artist_absent`), if a future peer arrives and the
  next search produces a successful match and import, the system
  records a long-tail-rescue event with timestamp, prior category
  (`artist_absent`), time-in-state (weeks), and import details.
  The event is queryable through the triage API.

- **AE7. Covers R22, R23, R24, R25, R26, R27, R28, R29.** Given any
  `search_log` row created after deploy, the row contains
  `rejection_reason`, `result_count_uncapped`, `query_token_count`,
  `query_distinct_token_count`, `expected_track_count`,
  `matcher_score_top1` (when candidates exist), and `query_template`
  populated. Given a wanted request that completed a plan cycle
  post-deploy, `album_requests.failure_class` is populated.

- **AE8. Covers R30, R31, R32, R33.** `pipeline-cli triage 1868`
  produces a single rendering combining Kid A's status, last 20
  searches with rejection reasons, unfindable categorisation
  (`wrong_pressing_available`), data-quality status (one resolved
  rg_year row), and search summary stats. `pipeline-cli triage
  list --filter=unfindable:wrong_pressing_available` lists every
  request in that category. `GET /api/triage/1868` returns the same
  data as JSON. `GET /api/triage/list?filter=unfindable` lists
  every unfindable cohort member.

- **AE9. Covers R34, R35, R36.** `curl /api/_index | jq` returns
  every registered route with path, method, classification, request
  model name (for POST), and one-line description.
  `pipeline-cli routes` returns every subcommand. The contract
  audit `TestRouteContractAudit` fails if a registered route lacks
  description metadata.

---

## Success Criteria

- The 422-request cohort B shrinks materially within 2-3 weeks of
  deploy, driven by: catalog-number slot finding pressing-specific
  rips, multi-disc subfolder aggregation accepting Aerial-shaped
  layouts, distinctiveness-ranked track selection unblocking Kid A
  shape cases. Specific target left for planning; "the cohort
  measurably shrinks and the wrong-pressing-available subset is
  surfaced for operator action" is the qualitative bar.
- The Various Artists sub-cohort's saturation rate drops from 3.9%
  toward parity with the non-VA rate (0.36%). At minimum, the
  near-cap saturation count for VA requests in 5.5 days drops below
  5 (from current 19).
- The unfindable cohort surface returns a populated cohort within
  one week of deploy. Every cohort A request from the findings doc
  (Russian Winters, Carol Chell, MONGEEYA, etc.) is categorised.
- The data-quality cohort surface returns a populated cohort. Every
  current NULL-rg_year wanted request either resolves on backfill
  or carries a `status` row explaining why.
- Operators can run `pipeline-cli triage <id>` for any wanted
  request and see rejection reasons, unfindable status, data-quality
  status, and search summary stats in one rendering — no SQL
  required.
- `curl /api/_index | jq` returns the full API surface with
  description metadata on every route.
- `SEARCH_PLAN_GENERATOR_ID` bumps once. No mid-iteration plan
  invalidation churn.

---

## Scope Boundaries

### Out of scope (this iteration)

- **Release-group-aware sibling-pressing surfacer + operator Replace
  flow.** The `wrong_pressing_available` category from R19 detects
  the cohort but does not surface "we have the 10-track 2010 standard
  pressing available — Replace?" recommendations. That sibling-aware
  surfacer is a separate brainstorm in its own right. This iteration
  ships detection only.
- **UI consumption of the new APIs.** Every new surface ships as
  API + CLI only. Dashboard filters, web-UI long-tail-rescue
  notifications, operator triage views in the browser — all separate
  iteration.
- **Aggressive stopword removal (the "B.2" path).** R5 fixes the
  current stopword-strip inconsistency but does not change the
  stopword set's contents. Removing the set entirely or extending it
  remains for a future iteration with its own evidence.
- **Wildcard-mask redesign.** R6 captures the rationale; no change to
  the behaviour is in scope.
- **Matcher count-gate loosening.** The strict-pressing invariant
  forbids automatic substitution of sibling pressings. No matcher
  loosening lands in this iteration.
- **Search cadence throttling on unfindable status.** The never-stop-
  searching invariant forbids this. R20 captures the constraint
  explicitly.
- **Beyond-the-investigation multi-disc redesign.** If R7's
  investigation reveals deeper-than-expected complexity, R8's
  redesign may slip to a follow-up iteration. The investigation is
  unconditional in this iteration; the redesign is conditional on
  the investigation's findings.
- **Catalog-number slot's discriminator length cutoff** (planning
  picks a final value).
- **Distinctiveness scoring formula's exact shape** (R4 picks one
  during planning).
- **Per-track-artist resolution for non-VA requests** is implicit in
  R11 (the backfill is dual-source uniform across all requests with
  per-track credit data) but is not a requirement of this iteration.
  Generator integration for non-VA `track_artist` use is a future
  iteration's problem.

### Explicitly deferred concerns

- The 145-request cohort D (`found_but_no_import`) is a different
  failure mode from the cohort A/B/C work this iteration addresses.
  Out of scope here; lives in a separate brainstorm.

---

## Key Decisions

- **Strict-pressing curation is the product identity, not a tunable.**
  The 422-request matcher-rejection cohort would have been "fixed"
  by loosening the count gate. That fix would corrupt the operator's
  curated collection. The right answer is operator visibility, not
  matcher loosening. The future sibling-aware surfacer is a real
  workstream; this iteration ships the detection that surfaces the
  cohort to it.
- **Never stop searching is the product purpose, not a tunable.**
  Cratedigger is fundamentally an archival watch process. The 119-
  request cohort A would have been "fixed" by throttling search
  cadence for unfindable requests. That fix would defeat the
  product's value proposition (catching long-tail peers when they
  arrive). The right answer is descriptive metadata, not cadence
  control.
- **Dual-source uniform, no adapter code.** The MB-vs-Discogs split
  in the data layer should be invisible to consumer code. Backfills
  fill the same column in the same shape regardless of source.
  Generator and matcher don't branch on source. This rule applies
  to `track_artist`, `release_group_year`, `catalog_number`, and
  every future external-metadata field.
- **API now, UI later, as a strict scope split.** Conflating the
  two leads to half-built APIs that are shaped to specific UI
  needs. Building API-first with no UI consumer ensures the API is
  composable and complete.
- **Single source of truth for shared constants.** The stopword
  set's "4 words is what 4 words means" got eroded by drift across
  multiple call sites. The fix is structural: one constant, one
  function, all callers.
- **Wildcard-all-tokens is a defensive design, not a bug.** Future
  code reviewers will see the all-token wildcard and want to "fix"
  it. The brainstorm captures the rationale; the code captures it
  in a comment; `CLAUDE.md` captures it as a documented invariant.
- **One observability subsystem, three data domains.** Unfindable
  cohort + field-quality + search-forensics share an operator-facing
  shape: "tell me what's wrong with this request, and let me find
  the cohort of requests with this kind of wrong." Three separate
  APIs would force the operator to know which surface to query;
  one composable subsystem hides the data-domain split.
- **API discoverability is a transversal concern, not a deferred
  one.** Adding 4+ new endpoints in this iteration without
  introspectability widens the gap. The `/api/_index` endpoint is
  cheap and prevents the gap from getting worse.
- **Investigation precedes redesign for multi-disc.** The legacy
  Soularr-era code is unknown enough that committing to a redesign
  shape upfront would be premature. An investigation document attached
  to the plan is the gating artefact.
- **One `SEARCH_PLAN_GENERATOR_ID` bump.** Multiple bumps mid-
  iteration produce intermediate plans the later bumps invalidate.
  Plan invalidation has a real (if bounded) cost; one bump amortises
  it.

---

## Dependencies / Assumptions

- The Discogs mirror at `discogs.ablz.au` exposes per-track artist
  credits on the master/release endpoints. **Assumption to verify
  during planning** — the schema is documented but the field
  population rate is unknown. If per-track artist coverage on the
  Discogs side is too sparse, the VA strategy mix's dependency on
  `track_artist` for Discogs-sourced VA requests degrades to NULL
  and the side table records `unresolved_field_missing_upstream`.
  The system handles this gracefully but the impact on Discogs-
  sourced VA requests would be larger than planned.
- The Discogs mirror exposes `year` on `master/<id>` for numeric
  rg_id resolution. **Assumption to verify** — should be reliable
  for masters with releases, but unique one-off Discogs releases
  without an associated master would have NULL `year`.
- The MB mirror's release-group endpoint reliably exposes the
  first-release year for UUID rg_ids. Already implicit in the May
  19 work; this iteration extends the same code path.
- Slskd's per-token wildcard behaviour stays consistent across
  the slskd-api version we ship. R6 codifies our assumption that
  per-token banning is a real-enough concern to justify all-token
  wildcarding; if a future slskd version changes this, R6's
  rationale needs revisiting.
- The existing dispatch tables (`Handler._FUNC_GET_ROUTES`,
  `_FUNC_POST_ROUTES`, `_FUNC_GET_PATTERNS`) are introspectable from
  Python without runtime cost penalties. R34's `/api/_index` walks
  them on each request; if call volume becomes a concern, the
  endpoint can cache.
- The 5.5-day post-deploy data window is representative. Specifically,
  the 422-request cohort B size and the cohort A 94%-real-gap rate
  are not artefacts of a specific week's network state. Two weeks
  of post-deploy data would confirm; this iteration assumes the
  signals are stable.

---

## Outstanding Questions

Resolved during the brainstorm dialogue. No blockers remain for
planning. Items that planning chooses (not unresolved product
questions):

- The exact distinctiveness-scoring formula for R4 (kept simple by
  product directive; planning picks among a small set of dumb
  heuristics).
- The catalog-number length cutoff for R2 (planning picks; "≥4
  characters" is the working starting point).
- The K and M thresholds for unfindable categorisation in R19
  (planning picks; "K=2 weekly probes" and "M=3 plan cycles" are
  working starting points).
- The exact schema column shape for the field-resolutions side table
  in R14 (planning finalises; the prose captures the necessary
  fields).
- Whether the multi-disc redesign (R8) lands in this iteration or
  slips to a follow-up depends on the R7 investigation's findings.
  Scope is reserved either way.
- The exact shape of the long-tail-rescue event audit (R21) —
  planning chooses whether it's a row in a new table, a JSONB blob
  on the import_jobs row, or another shape. Data captured is
  fixed; storage shape is flexible.

---

## Sources / References

- **Findings document:** `docs/brainstorms/2026-05-25-search-plan-post-deploy-findings.md`
- **Origin of the previous iteration:** `docs/brainstorms/2026-05-19-search-plan-entropy-requirements.md`
- **Origin plan document:** `docs/plans/2026-05-19-001-feat-search-plan-entropy-and-matcher-prefilter-plan.md`
- **Persisted-plan rollout:** `docs/persisted-search-plans-rollout.md`
- **Pipeline DB schema:** `docs/pipeline-db-schema.md`
- **MusicBrainz mirror:** `docs/musicbrainz-mirror.md`
- **Discogs mirror:** `docs/discogs-mirror.md`
- **Related (out-of-scope) brainstorm to anchor:** future sibling-
  pressing surfacer brainstorm (not yet written; will consume the
  `wrong_pressing_available` category from R19 when started).
- **Related rules:**
  - `CLAUDE.md` — strict-pressing curation invariant, CLI ⇄ API
    surface symmetry pattern.
  - `.claude/rules/code-quality.md` — Pydantic on route bodies,
    msgspec for wire boundaries, CLI ⇄ API symmetry, contract test
    audit.
  - `.claude/rules/deploy.md` — flake input update flow, db-migrate
    unit ordering.
- **Related code paths to inspect / extend in planning:**
  - `lib/search.py` (generator, `SEARCH_PLAN_GENERATOR_ID`).
  - `lib/matching.py` (strict count gate, multi-disc handling).
  - `web/mb.py`, `web/discogs.py` (mirror clients).
  - `web/routes/pipeline.py` (enqueue path, triage endpoints).
  - `scripts/pipeline_cli.py` (CLI subcommands).
  - `lib/pipeline_db.py` (data access, schema methods).
  - `migrations/` (new migrations for R10, R14, R22-R28).
  - `tests/test_web_server.py::TestRouteContractAudit.CLASSIFIED_ROUTES`
    (audit extended in R36).
