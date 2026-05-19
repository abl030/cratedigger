---
date: 2026-05-19
topic: search-plan-entropy-and-matcher-prefilter
---

# Search Plan Entropy and Matcher Pre-filter

## Summary

Restructure the search-plan generator to produce per-album entropy that
actually finds music, and reshape a matcher pre-filter that today fires
asymmetrically and silently discards legitimate track-fallback results.
Investment in the matcher's permissiveness also seeds the Redis browse
cache with more peer-folder data, which is the substrate for the future
cross-pressing matcher (out of scope here; explicitly enabled by the
choices in this doc).

---

## Problem Frame

The persisted search-plan system (rolled out 2026-05-08) is running cleanly
— ~20k searches/7d, plans persist, cursor advances — but a hard read of the
data shows ~600 / 20k searches saturating at slskd's
`ResponseLimitReached` (1000 user cap) or `FileLimitReached` (50,000 file
cap). The saturation rate alone (3%) is unalarming. What is alarming is the
per-request distribution: a small set of curated requests are
**continuously** saturating with zero downloads.

Concrete starving cases (14d window):
- Radiohead — Kid A: 43 search attempts, 25 saturated, **0 found**, status
  still `wanted`. Has existing 128kbps MP3 in beets; pipeline cannot
  upgrade.
- Bon Iver — 22, a Million: 47 attempts, 23 saturated, 0 found.
- Willow — Willow (self-titled, 2007): 37 attempts, 18 saturated, 0 found.
- Mountains — Mountains Mountains Mountains: 8 attempts, 10 saturated,
  2 found, none imported.
- Darren Hanlon — I Will Love You at All: 43 attempts, 6 saturated, 0
  found.

Manual slskd probes prove every one of these albums is **on the network in
sufficient quantity to find**:
- Kid A FLAC: 422 unique Kid A directories across 8+ users carrying the
  2021 Mnesia box (34 tracks), and many users carrying the original 2000
  release.
- Bon Iver 22 a Million with literal title: 138 responses, no saturation.
- Willow self-titled: literal first-track query "When the Sea Called Our
  Names" returned **exactly 2 responses, both pointing at the correct
  album folder** on cpm4 and special_user. A direct browse of those
  users confirmed both serve the full 4-track Willow (2007) - Willow
  directory (MP3 only — the album is genuinely scarce in lossless, but
  the matcher would have rejected it on quality grounds, not by silently
  skipping it).
- Darren Hanlon: literal title returned **one** response, a perfect
  10-track FLAC match.

Demographic confirmation: **100% of requests in the pipeline want
lossless** (3716 with default config, 215 lossless-only, 269+ requests
with lossless as the top tier in a multi-tier override). Zero requests
exclude FLAC. There is no cohort for whom format-hint slots are
inappropriate.

So the pipeline is not failing because the network is empty. It is failing
because:

1. **The generator emits low-entropy queries that saturate.** Five of ten
   plan slots are the same default query repeated. Short-token drop strips
   the most distinguishing characters of short-titled albums ("Kid A" →
   `*adiohead Kid`, "22, a Million" → `*on *ver Million`). Self-titled
   albums collapse to one token (`*illow`, `*ountains`). Title-only track
   fallbacks drop the artist and match every soundtrack on Soulseek.

2. **The matcher pre-filter fires asymmetrically and silently discards
   track-fallback results.** A pre-filter in `check_for_match` rejects any
   candidate dir where `abs(search_count - track_num) > 2`. Track-fallback
   queries by definition return one file per user (the matching track),
   so for any album with ≥4 tracks the pre-filter unconditionally skips
   and **negative-caches** every user. Three Willow track searches in
   the DB returned 2 / 4 / 6 results and **zero candidates evaluated
   each time**. The strategy slot fires; the matcher never looks at the
   data. The pre-filter's true purpose is junk-dir guarding ("user has a
   dump folder with 5000 files, don't browse it for a 4-track album"),
   not tight-fit filtering. The bidirectional comparison was always
   over-strict.

3. **Format/year context is under-used as entropy.** Adding the literal
   string "FLAC" or the original release year to a wildcarded query
   collapsed saturated 1000-response results to 138–200. Year is currently
   one slot in ten and uses the MB release year (sometimes a reissue
   year), not the release-group year users actually file by. Kid A is
   filed by Soulseek users overwhelmingly as 2000 (original release);
   our request carries 2008 (US reissue), so `Radiohead Kid 2008`
   returns ~6 results while `Radiohead Kid 2000` returns hundreds.

The observability we built in the persisted-plan rollout
(`final_state`, `candidates` JSONB, per-slot stats in
`search-plan show`) is what made this investigation possible — that
infrastructure is keeping its end of the bargain. The generator and
matcher are not.

---

## Requirements

**Matcher pre-filter — bleed-stop**

- R1. The `check_for_match` pre-filter at `lib/matching.py` must flip
  from bidirectional (`abs(search_count - track_num) > 2`) to
  asymmetric (`search_count > 2 * track_num`). This single-line change
  preserves the optimisation's true value (don't waste a browse on a
  user whose dir clearly contains too much stuff to be the album) while
  killing the false-positive case (don't skip a user just because the
  search returned only the matching track, not the whole folder). The
  `cached_codecs <= 1` guard stays — it protects multi-codec dirs from
  being skipped. As a bonus dividend, this change also bypasses the
  pre-filter for track-fallback queries automatically (`search_count = 1`
  is well below `2 * track_num` for any multi-track album), so no
  variant context needs to be threaded into the matcher.
- R2. **Dissolved.** Investigation confirmed
  `ctx.negative_matches` is a per-cycle in-memory `set` on
  `CratediggerContext`, wiped fresh every 5-minute cycle. No permanent
  leak exists; no cache-cleanup work is needed. The brainstorm's
  original wipe-vs-scope question is moot.
- R3. Search forensics must distinguish "matcher pre-filter skipped"
  from "matcher scored, rejected" — but bounded against storage growth.
  Shape: add `pre_filter_skip_count INTEGER` as a dedicated column on
  `search_log` (aggregable, ~4 bytes/row), and allow up to ~5 flagged
  skip rows to ride alongside scored candidates inside the existing
  candidates JSONB blob (samples for inspection, not exhaustive lists).
  Operators investigating "is the pre-filter doing real work?" use the
  scalar; operators investigating "why was peer X skipped?" use the
  sample. Reconstructing the full skip list from a single search is not
  a goal — peers are still browsable, the next cycle would capture
  them fresh.

**Generator — entropy structure**

- R4. The five-slot default repetition must be replaced. Each of those
  slots should be a distinct strategy (literal-title, format-hint,
  release-group-year, track-with-artist, etc.), not the same query
  five times. One default slot is sufficient.
- R5. Remove the short-token drop entirely. The current rule strips
  tokens ≤ 2 chars unconditionally, destroying the most distinguishing
  characters of short-titled albums ("A" in "Kid A", "22" in "22, a
  Million", "Why" in "So Why?"). Replace with: rank tokens by
  distinctiveness and cap total token count only — never drop a token
  because it is short. The historical rationale ("Soulseek drops short
  tokens") is false; the original AFI failure was an artist-name issue,
  not a length issue.
- R6. Add a "literal full title, no wildcarding" strategy as a
  first-class plan slot. Empirical evidence: this strategy is
  dramatically more focused than the wildcarded default for ≥4 of the 6
  albums probed.
- R7. Add format-hint strategy slots (`literal-title + FLAC`,
  `literal-title + lossless`) as unconditional members of the plan
  rotation. **No conditional logic, no log lookback, no statefulness in
  the generator.** Every request in the pipeline wants lossless (zero
  requests exclude FLAC); format-hint slots are appropriate for every
  request. For niche releases where one slot returns zero, the parallel
  agnostic slot catches the single available peer; the cost is bounded
  at one slot per cycle per niche release. The generator stays a pure
  function of the snapshot.
- R8. Track-fallback queries must prepend the artist name. Title-only
  track queries produce convincing false positives (e.g. Kid A's "Motion
  Picture Soundtrack" matches a Matador OST 10/14 tracks). Artist token
  is the disambiguating signal.
- R9. Persist release-group year on `album_requests` and use it in the
  generator. Specifics:
    - Add `release_group_year INTEGER NULL` column on `album_requests`.
    - Backfill on deploy by fetching release-group metadata from the
      local MB mirror for every request with `mb_release_group_id IS
      NOT NULL AND release_group_year IS NULL`. ~4000 requests, all
      local I/O. Never defer cheap backfills.
    - Enqueue path fetches release-group year going forward, so new
      requests land with it populated.
    - Generator emits a conditional extra year-anchored slot **only**
      when `year IS NOT NULL AND release_group_year IS NOT NULL AND
      release_group_year != year`. When years match, no extra slot
      (would be a duplicate of `unwild_year`). Plan size grows by
      exactly 1 for reissues; unchanged for everything else.

**Self-titled / collision cases**

- R10. When `normalize(artist_name) == normalize(album_title)` OR one
  is a token-subset of the other (catches Willow / Willow,
  Mountains / Mountains Mountains Mountains, etc.), detect the request
  as self-titled and route through a dedicated strategy mix. The
  default wildcarded query for these collapses to a single token
  (`*illow`, `*ountains`) and is structurally unable to disambiguate.
- R11. The self-titled strategy mix must include at least one slot
  that does not rely on artist+title overlap. At minimum: `artist + year`
  and `artist + first-track-title`. The Willow probe showed
  track-name-with-artist is the working path; the generator must emit it.

**Observability — close the loop**

- R12. Ship the data and CLI surface for saturation telemetry; defer
  the dashboard UI to the existing
  `docs/brainstorms/2026-05-09-search-plan-per-request-dashboard-requirements.md`.
  Specifically:
    - The `pre_filter_skip_count` column from R3 lands.
    - A new aggregator method on `PipelineDB` exposes per-request
      saturation rate (rows where `final_state LIKE '%LimitReached%'`
      / total rows) for a request over a window.
    - A `pipeline-cli search-plan saturation <id>` subcommand surfaces
      saturation rate, pre-filter skip count, and starving-cohort
      membership for the operator.
    - CLI ⇄ API symmetry: same data is exposed via the request's
      existing JSON endpoint so the future dashboard UI work has the
      data ready.

**Simulator — Simulator-First TDD for the generator**

- R13. Add a `pipeline-cli search-plan dry-run <request_id>`
  subcommand that loads the request's snapshot, runs the new generator
  against it without persisting, and prints the resulting plan items.
  Lets operators (and the deploy process) validate new generator output
  against starving albums before bumping `SEARCH_PLAN_GENERATOR_ID`
  for real. Matches the "Simulator-First TDD" pattern from
  `.claude/rules/code-quality.md`. Pure read; no side effects on the
  active plan or request.

---

## Acceptance Examples

- AE1. **Covers R1.** Given a request whose `album_tracks` row count is
  4, when the matcher's pre-filter sees a user with `search_count = 1`
  (typical for a track-fallback query result), it does NOT skip the
  user — `1 > 2 * 4` is false. The user's dir is browsed and the full
  album is evaluated by `album_match`. The same request's matcher,
  given a user with `search_count = 50` from a junk dir, still skips
  pre-browse — `50 > 8`.
- AE2. **Covers R4, R5, R6, R8.** Given Radiohead Kid A request 1868
  with a freshly-regenerated plan under the new generator, when one
  cycle of the plan runs, the executor emits at least one query
  containing the literal token "A" preserved, at least one query
  containing a format hint, at least one track-fallback query that
  prepends "Radiohead", and **no** five-fold repetition of the same
  canonical query key.
- AE3. **Covers R7.** Given Darren Hanlon — I Will Love You at All
  (request 1640, niche release with 1 lossless peer on the network),
  when one cycle of the new plan runs, both the format-hinted slots
  (`Hanlon I Will Love You at All FLAC`) and the format-agnostic
  literal-title slot fire. The agnostic slot returns the 1 peer and the
  matcher evaluates it; the format-hinted slots may return zero
  results without harm. Both slots execute regardless of the request's
  prior cycle history — no conditional gating.
- AE4. **Covers R9.** Given a request where `year = 2008` and
  `release_group_year = 2000` (Kid A reissue), when the generator runs,
  the plan contains both an `unwild_year` slot anchored on 2008 and a
  separate slot anchored on 2000. Given a request where year and
  release-group year match, the plan size is unchanged from current
  behaviour — no duplicate slot.
- AE5. **Covers R10, R11.** Given Willow request 2125 (artist = "Willow",
  title = "Willow"), the generator detects self-titled, and the
  emitted plan contains at least one `artist + first-track-title` slot
  ("Willow When the Sea Called Our Names" or similar) that does NOT
  collapse to a single wildcarded token.
- AE6. **Covers R13.** Given any request id, when an operator runs
  `pipeline-cli search-plan dry-run <id>`, the new generator's plan
  items are printed to stdout without writing to `search_plans` or
  `plan_items` or affecting the request's `active_plan_id` cursor.

---

## Success Criteria

- The currently-starving cohort (≥ 5 saturated searches + 0 finds in 14d)
  drops by at least 50% within two weeks of deploy. Radiohead Kid A and
  Bon Iver 22 a Million specifically reach `imported` or surface real
  candidates with measurable beets distance.
- Zero search_log rows where `outcome='no_match'` AND `variant LIKE 'track%'`
  AND `result_count > 0` AND `candidates = '[]'` in the 14d following
  deploy. (Today: 100% of such rows are empty.)
- Operators can identify a starving request and its failure mode from
  `pipeline-cli show <id>` alone — generator-emitted queries, slskd
  `final_state`, AND pre-filter skip count are all visible.
- `pipeline-cli search-plan dry-run` produces materially different
  plans for at least Radiohead Kid A, Bon Iver 22 a Million, Willow
  self-titled, and Darren Hanlon I Will Love You at All before deploy
  — confirming the generator changes are reaching the right releases.
- No regression in the conversion rate of normal (non-saturated,
  non-track-fallback) searches.
- Redis browse cache coverage increases — the asymmetric pre-filter's
  more permissive shape feeds more peer-folder data into the cache,
  which seeds the future cross-pressing matcher.

---

## Scope Boundaries

- Not in scope: rebuilding the `album_match` filename-similarity scoring.
  R1 fixes the pre-filter; the downstream ratio/cross-check logic is left
  alone unless a starving case proves it also broken.
- Not in scope: a new query-DSL or generator framework. Changes layer on
  `lib/search.py::generate_search_plan` in place, bumping
  `SEARCH_PLAN_GENERATOR_ID` to invalidate old plans.
- Not in scope: the cross-pressing matcher itself. The asymmetric
  pre-filter (R1) and persisted `release_group_year` (R9) are
  investments that make that future system possible, but its design and
  implementation belong in their own brainstorm.
- Not in scope: catalog-mining / "find every Radiohead release this user
  has" browse strategy. The pipeline is request-driven; this work
  improves per-request search.
- Not in scope: changing slskd's `responseLimit` / `fileLimit` config.
  Saturation is the symptom; raising limits would not help focus.
- Not in scope: pipeline-level retry/cooldown policy on starving
  requests. R12 surfaces the data; a separate brainstorm can decide
  what to do with chronically-starving requests.
- Not in scope: the per-request dashboard UI for saturation/skip
  visualisation. R12 ships the data + CLI; UI lives in
  `docs/brainstorms/2026-05-09-search-plan-per-request-dashboard-requirements.md`.
- Not in scope: bonus-track and multi-disc handling. The matcher's
  per-folder strict-count gate still rejects 14-track 2-disc albums
  stored as `/CD1/` + `/CD2/` and 13-track albums with a bonus track in
  the same folder. Known limitations, separate problems.
- Not in scope: the Willow self-titled "genuinely rare in lossless on
  Soulseek" case. R10 + R11 give it a fighting chance via track-name
  + artist queries; if no FLAC exists in the network, the pipeline
  correctly stays in `wanted`. Acceptable behaviour.

---

## Key Decisions

- **Forward-only via plan-generator ID bump.** Bumping
  `SEARCH_PLAN_GENERATOR_ID` invalidates existing plans for affected
  requests on the next cycle. No backfill, no migration of old plan
  rows. Matches the repo's "request is source of truth, everything
  else is derived" invariant.
- **Matcher fix lands before or with generator changes, never after.**
  Shipping new track-fallback variants while the pre-filter still
  silently discards them would waste a deploy and obscure whether the
  generator changes helped. Order matters.
- **Pre-filter is asymmetric, not variant-aware.** A single-line code
  change (bidirectional → `search_count > 2 * track_num`) is preferred
  over plumbing variant context through `find_download` into the
  matcher. The matcher stays unaware of search strategy; the threshold
  doubles the album's track count rather than fitting it tightly.
  Bonus: track-fallback bug fixed without special-casing.
- **`2n` over `n+2`.** The pre-filter's purpose is "junk-dir guard",
  not "tight-fit filter". `2n` captures more peer-folder data into
  Redis (cached for 7 days, ~3 searches/release/day cadence means first
  cycle pays the browse cost, subsequent cycles cheap), which is the
  substrate for the future cross-pressing matcher.
- **Format hints are universal, not conditional.** 100% of pipeline
  requests want lossless; no cohort exists for whom format-hint slots
  are inappropriate. The generator stays pure (no log lookback, no
  state). Niche releases get one or two slots that return zero — bounded
  cost.
- **Wildcarding stays.** Empirically necessary for catalog-style
  searches on artists where Soulseek's index doesn't return literal
  matches (the original Beatles → `*eatles` rationale still holds). We
  are not removing wildcarding; we are removing **wildcarding combined
  with token-dropping that destroys entropy**.
- **Release-group year is data, not derivation.** Persist
  `release_group_year` as a real column and backfill on deploy. Don't
  defer cheap backfills. Conditional extra slot when years differ; no
  extra slot when they match (avoid the same wasteful repetition
  pattern we are killing in R4).
- **R3 forensic encoding: scalar count + sample in JSONB.** A dedicated
  `INTEGER` column for aggregable skip count; a small (~5 row) sample
  flagged inside the existing candidates JSONB for inspection.
  Resolves the storage-growth-vs-debuggability tension by capping per-row
  forensic size.
- **JSONB is the right shape for forensic samples** (consumed wholesale,
  not queried by inner fields). The broader question of "should
  cratedigger migrate away from JSONB more aggressively" is its own
  conversation — out of scope here.

---

## Dependencies / Assumptions

- Depends on the persisted search-plan infrastructure already in place
  (`lib/search.py::generate_search_plan`,
  `lib/search_plan_service.SearchPlanService`,
  `SEARCH_PLAN_GENERATOR_ID` constant).
- Depends on the `search_log.final_state` and `candidates` JSONB columns
  rolled out in migrations 010 + 014.
- Depends on the 7-day Redis browse cache (
  `docs/plans/2026-05-05-001-feat-peer-cache-redis-migration-plan.md`) —
  the `2n` pre-filter shape is acceptable cost only because cached
  browses make subsequent cycles cheap.
- Assumes slskd's `ResponseLimitReached` / `FileLimitReached` state
  strings are stable across the slskd-api versions we ship.
- **Assumption to verify during implementation:** the MB mirror's
  release-group fetch path exposes the first-release year directly. If
  it instead requires "list all releases in this release-group, take
  min(year)", the backfill query is slightly more elaborate but still
  cheap (all local mirror I/O). Either way the implementation is
  bounded.

---

## Outstanding Questions

All planning-time questions from the original brainstorm have been
resolved through dialogue. No blockers remain. The implementation plan
is at `docs/plans/2026-05-19-NNN-feat-search-plan-entropy-and-matcher-prefilter-plan.md`.
