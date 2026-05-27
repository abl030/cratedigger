---
date: 2026-05-27
topic: youtube-music-album-resolver
---

# YouTube Music Album Resolver API

## Summary

A read-only HTTP + CLI surface that takes any MB release MBID or Discogs release ID, auto-widens to the release group, and returns the set of matching YouTube Music album entities — each annotated with beets distance scores against every MB pressing in that release group. The API surfaces evidence; it never decides which pressing is "the" match.

---

## Problem Frame

Cratedigger holds rich pressing-level identity for every album in the operator's curated catalogue — distinct MBIDs for distinct release-group siblings (2000 UK 10-track Kid A vs 2008 14-track reissue vs 2017 OKNOTOK box). YouTube Music is a useful adjacent surface for many things the operator might want to do with that catalogue: previewing albums before pulling the trigger on a Soulseek search, sanity-checking whether a release is genuinely out-of-print or just hard-to-find on P2P, sharing the album with someone who doesn't pirate, cross-referencing archival pressings against the streaming world's view of the same release group. None of those downstream uses exist today, and the operator wants to keep them out of scope for now — but every one of them needs the same lower-level capability: given a release identifier from cratedigger's world, what is the corresponding album on YouTube Music?

The complexity lives in the mismatch between the two data models. YouTube Music does carry multiple pressings as separate entities (each gets its own stable `browseId` and paired `OLAK5uy_` audioPlaylistId), but its metadata surface is thin — title, artist, year, track count, ordered track titles and durations. No UPC, no catalog number, no ISRC, no label. There is no published MBID ↔ YT Music release-level mapping at meaningful scale. The operator's strict-pressing invariant says siblings must never be silently collapsed; YouTube Music's editorial clustering (`other_versions[]`) doesn't necessarily match MusicBrainz release groups. A naive "give me the YT Music URL for this MBID" answer would either lie (collapse to an arbitrary sibling) or refuse useful information ("no exact match, sorry"). The right primitive is one that surfaces the full picture — every YT sibling, every MB sibling, the beets distance between each pair — and lets the caller decide what to do with it.

---

## Key Flows

- F1. Resolve a release identifier to a scored YT Music matrix
  - **Trigger:** caller hits the API endpoint or the CLI subcommand with an MBID or Discogs ID
  - **Actors:** the calling service (web UI, automation, ad-hoc CLI invocation)
  - **Steps:**
    1. Server resolves the input to a release-group MBID (for MB-side inputs) or a Discogs master ID (for Discogs-side inputs)
    2. If a cached matrix exists for that identifier and no refresh was requested → return the cached matrix
    3. Otherwise, enumerate MB siblings (or Discogs siblings) for the release group from the local mirror
    4. Pick a representative MB release as the seed; query YouTube Music search; take the top album result's `browseId`
    5. Call `get_album(browseId)`; the YT-side sibling set is the seed result plus every entry in its `other_versions[]` array
    6. Call `get_album` once per YT sibling to obtain each one's track list with durations
    7. For each (YT sibling × MB release in the group) pair, synthesize a beets-compatible item list from the YT track data and call `compute_beets_distance(items_override=…, mbid=<mb_release>)`
    8. Persist the matrix (one row per `(release_group, yt_browseId)` pair); return the matrix
  - **Outcome:** caller receives the full scored matrix — every YT sibling, every MB sibling, every pairwise distance
  - **Covered by:** R1, R2, R3, R4, R5, R6, R7, R9, R10, R12

- F2. Refresh a cached matrix
  - **Trigger:** caller passes a refresh flag (or hits a sibling refresh endpoint)
  - **Actors:** the operator (debugging a known-wrong cached result), or downstream automation that observed staleness
  - **Steps:** identical to F1 from step 3 onward; the cached rows for the given release-group are overwritten in place
  - **Outcome:** the persisted matrix reflects the live state of YT Music as of the refresh
  - **Covered by:** R13, R14

- F3. Degraded mode — YouTube Music unreachable
  - **Trigger:** YT Music returns 4xx/5xx, times out, or `ytmusicapi` raises a network exception
  - **Actors:** the calling service
  - **Steps:**
    1. If a cached matrix exists for the requested release group → return it with a flag indicating the freshness fall-back
    2. Otherwise, return an explicit `youtube_unavailable` outcome with no matrix
  - **Outcome:** the caller has unambiguous information about whether the result is fresh, stale-but-cached, or unavailable
  - **Covered by:** R16

---

## Requirements

**Input and resolution**

- R1. The API accepts any of: an MB release MBID, an MB release-group MBID, a Discogs release ID, or a Discogs master ID as input. Release-level identifiers are auto-widened server-side to their release-group / master before the resolve flow proceeds.
- R2. From the resolved release-group (or master), the server enumerates all MB sibling pressings (or Discogs sibling releases) from the local mirror. This set is the MB-side scoring axis used throughout the rest of the flow.

**YouTube Music enumeration**

- R3. The server queries YouTube Music anonymously — no Google account, no OAuth, no cookies. The `ytmusicapi` library is used in its unauthenticated mode, with `search` and `get_album` as the only endpoints exercised.
- R4. The YT-side sibling set is built via search-then-expand: one search query against a representative MB release, then expansion via the seed result's `other_versions[]` array. The full set is `{seed_result} ∪ other_versions[]`. The seed-pick heuristic and tiebreaks when search returns multiple plausible candidates are deferred to planning.
- R5. Each YT Music sibling is fetched once via `get_album` to obtain its track list with durations (required for beets distance scoring). Per-album `get_album` calls are cached so a release group with N YT siblings costs exactly 1 search + N `get_album` calls on the first resolve, and zero YT Music traffic on every resolve thereafter.

**Distance scoring**

- R6. The existing `compute_beets_distance` service (`lib/beets_distance.py`) is extended with one optional parameter — provisionally `items_override: list[ItemRecord] | None = None` — that accepts a caller-provided list of items. When `items_override` is supplied, the function skips its download_log-loading and filesystem-reading branch and scores the provided items directly against the candidate MBID.
- R7. For each (YT Music sibling × MB release in the release group) pair, the server synthesizes a beets-compatible item list from the YT track data — track title, length, track number, artist, album — and calls `compute_beets_distance(items_override=…, mbid=<mb_release>)`. The full per-pair `BeetsDistanceResult` is preserved in the response.
- R8. The Replace-picker's existing call path through `compute_beets_distance` (where `items_override` is absent) remains untouched. The current download_log-driven scoring behavior is preserved bit-for-bit.

**API contract and output shape**

- R9. The API decides nothing. It returns the full matrix as-is: a list of YT Music sibling releases, each annotated with the per-MBID distance results from R7. The choice of "which pressing is the right one" — if any — belongs to the caller.
- R10. Each returned YT Music release entry carries its `browseId`, its `audioPlaylistId` (the `OLAK5uy_…` form), the constructed public URL (`music.youtube.com/playlist?list=…`), `year`, `trackCount`, and the track list used for scoring.
- R11. When the seed search returns nothing, or the expanded YT-side set is empty, the API returns an empty list of YT Music releases together with an explicit outcome indicator. "Not found on YouTube Music" is a normal response shape, not an error.

**Persistence**

- R12. Resolved matrices are persisted in a new normalized table — one row per `(release_group_identifier, yt_browseId)` pair. Each row carries the YT-side metadata from R10 and the per-MBID distance array as a JSONB column. The table accommodates both MB release-group identifiers and Discogs master identifiers as keying axes.
- R13. The cache is content-addressed by the release-group identifier and has no TTL. Since YT Music `browseId` / `OLAK5uy_` IDs are stable per release, cached rows live forever absent explicit refresh.
- R14. An operator-triggered refresh path — a `refresh=true` parameter or a sibling endpoint — re-fetches from YT Music and overwrites the cached rows for the given release group.

**CLI ⇄ API symmetry**

- R15. Both surfaces are present: a `pipeline-cli` subcommand and a web API endpoint, both wrapping the same service-layer method. Outcome → exit-code / outcome → HTTP-status mappings follow the existing convention (200/0 success; 404/2 not found; 409/4 wrong state; 503/5 transient; etc.). The route is added to `TestRouteContractAudit.CLASSIFIED_ROUTES` per the existing audit gate.

**Failure modes and observability**

- R16. When YT Music is unreachable (network failure, 403, 429, captcha challenge) and a cached matrix exists for the requested release group, the API returns the cached result with a flag indicating the freshness fall-back. When no cached matrix exists, the API returns a `youtube_unavailable` outcome with no matrix payload. The cache absorbs the realistic transient-throttling failure mode.
- R17. When `compute_beets_distance` returns a non-`ok` outcome for any (YT_release × MB_release) pair (e.g. an MB lookup failure for one sibling), that entry in the matrix carries the failure outcome verbatim instead of a distance value. Partial results remain useful — one missing pair doesn't invalidate the rest.

---

## Acceptance Examples

- AE1. **Covers R1, R2, R9, R10.** Given a release group with 3 MB siblings (MBIDs A, B, C) and YT Music has 2 sibling albums in `other_versions[]` (browseIds X and Y), when the API is called with MBID A as the input, the response contains 2 YT Music release entries (X and Y), each carrying 3 distance results — one for each of A, B, C — total 6 pairwise distances.

- AE2. **Covers R11.** Given a release group for which YT Music search returns no results at all (a long-tail Australian indie 7" with no streaming presence), when the API is called, the response is an empty list of YT Music releases together with an outcome indicating "not found on YouTube Music." This is a 200 response, not an error.

- AE3. **Covers R1.** Given an MB release MBID as input, when the API is called, the server auto-widens to the release group and returns the matrix for the entire group. The caller's specific MBID is one of several entries in each YT release's distance array; the caller can filter to it client-side if they care about their exact pressing.

- AE4. **Covers R1, R2.** Given a Discogs release ID as input, when the API is called, the server auto-widens to the Discogs master and returns the matrix for all releases in that master, scored against YT Music siblings the same way.

- AE5. **Covers R13, R14.** Given a previously-resolved release group with a cached matrix, when the API is called with no refresh flag, the response is served from cache and no YT Music traffic occurs. When the same call is made with `refresh=true`, YT Music is re-queried and the cached rows are overwritten.

- AE6. **Covers R16.** Given that YT Music is throwing 429 throttling errors and a cached matrix exists for the requested release group, when the API is called, the cached result is returned with a flag indicating the result is from cache and YT Music was unreachable. When no cached matrix exists, the response carries the `youtube_unavailable` outcome instead.

- AE7. **Covers R17.** Given YT Music has 3 sibling albums and the local MB mirror is missing one of the 4 MB siblings in the release group (data drift), when the API is called, each YT release's distance array contains 3 successful distance results and 1 entry with the `mb_lookup_failed` outcome verbatim from `compute_beets_distance`.

- AE8. **Covers R8.** Given the Replace picker calls `compute_beets_distance(download_log_id=X, mbid=Y)` with no `items_override`, when the function executes, its behavior is identical to the pre-change implementation — the download_log row is loaded, the on-disk failed_path is read, fingerprints are computed, and the distance is returned. No new code path is exercised.

---

## Success Criteria

- Any caller (web UI, automation, ad-hoc CLI) can resolve any MBID or Discogs ID to a faithful matrix of YT Music albums plus per-pressing beets distance scores with a single HTTP or CLI call.
- The cache absorbs steady-state requests: repeated calls for an already-resolved release group do not touch YouTube Music. Re-resolves are an explicit operator action, not a side effect.
- The operator's real Google account never authenticates to the service. The deploy-day failure mode is per-IP throttling, not account suspension.
- When YT Music genuinely has no entry for a release group (the archival long-tail this product is built for), the API returns a clean empty result with a clear outcome indicator. The caller knows the difference between "not on YT Music" and "we couldn't reach YT Music."
- The Replace-picker's distance-scoring behavior is preserved without regression; `items_override` is purely additive.
- Downstream consumers (preview-on-YouTube buttons, out-of-print signals, library decoration, archival cross-reference) can be built on top of this API without the resolver itself needing changes.
- `ce-plan` consuming this document does not need to invent product behavior. Every product decision is documented; only implementation specifics (exact table/column names, route URL shape, schema migration ordering, error-message wording) remain.

---

## Scope Boundaries

- The downstream consumers of this API are deliberately out of scope. The API is the artifact; what calls it is separate work.
- Real YouTube playlists created via `playlists.insert` are not built. The artifact is the existing YT Music album URL, never a synthesized playlist.
- `watch_videos`-style URLs constructed from individual video IDs are not built. Same reason: different artifact.
- Track-by-track video search and matching is not done. Resolution happens at the album-entity level only — if YT Music has no album entity for the release group, the response is empty regardless of whether individual tracks exist as standalone videos.
- OAuth, Google Cloud Console flows, and browser-cookie authentication are not implemented. Anonymous YT Music access is the only supported mode.
- The operator's real Google account is never authenticated to the service. Burner-account + OAuth is documented in `## Key Decisions` as the only escape hatch if anonymous access degrades to unviability in the future — it is not implemented in v1.
- MusicBrainz `release-url` relationship-type lookups (the generic "free streaming" relationship that can carry YT Music URLs) are not consulted. Coverage at the release level is sparse and not maintained for this purpose.
- TTL-based stale invalidation is not implemented. The cache lives forever absent explicit operator-triggered refresh.
- No web UI work (button placements, library decoration, browse-tab integration, Decisions-tab integration). Frontend wiring against this API is downstream of this brainstorm.
- No backfill of existing `album_requests` rows with YT Music URLs at deploy time. The cache populates on demand, per the single-operator no-backfill-scripts invariant.

---

## Key Decisions

- **Anonymous YT Music access via `ytmusicapi`, no Google account involved.** Research found no meaningful documented history of account suspensions for `ytmusicapi` usage — a single empty 2020 GitHub issue with no follow-up in six years. The cookie / OAuth-against-real-account paths concentrate the (small) downside risk on the operator's primary identity for marginal benefit. Per-IP throttling (429 / captcha) is the realistic failure mode, and the cache absorbs it. If anonymous access degrades in the future, the documented escape hatch is OAuth via a clean burner Google account (separate recovery email, never used from the operator's primary devices) — not the operator's real account, not cookie auth.
- **Extend `compute_beets_distance` with `items_override` rather than extract a `score_items_against_release` helper.** The existing service is already the right shape: one optional parameter that bypasses the download_log + filesystem branch is the minimal change. Helper extraction would force structural refactoring of working code without changing the substance of the scoring. The Replace-picker call path stays untouched.
- **Normalized table — one row per (release_group, YT sibling) — over a single JSONB blob or fully-exploded `(yt × mb)` rows.** Keeps YT siblings as queryable first-class entities (queries like "show me all YT releases we've found that score ≤ 0.15 against any MBID in our DB" become real SQL) while keeping per-MBID scoring detail as JSONB on the row. The single-JSONB-blob shape flattens YT identity away; the fully-exploded shape over-normalizes for the cost.
- **Auto-widen any release-level input to release group / master.** Cratedigger's own data (`album_requests`, browse routes) is keyed on release-level MBIDs and Discogs release IDs, not release groups / masters. Forcing every caller to widen first would be pure boilerplate. The matrix returned still identifies each MBID individually so callers can filter to their input.
- **Surface evidence, decide nothing.** Aligns with the archivist-frame invariant that the system never auto-decides anything irreversible (CLAUDE.md R3). The caller — which may be a human picker UI, an automation script, or a downstream service applying its own threshold — owns the "is this the right pressing?" decision. The API's job is to be a faithful reporter of what YouTube Music has and how it scores against the operator's catalogue.
- **Single-operator scope; no backfill at deploy, no compat shims, no retry-window machinery.** Per the project's single-operator invariant (CLAUDE.md § "Single-operator, no backwards-compat"): the cache populates on demand, schema migration is forward-only, no one-shot infrastructure accumulates.

---

## Dependencies / Assumptions

- The `ytmusicapi` Python library (currently v1.12+) is added to the Nix dev shell and the production environment. Verified: not currently present in the dev shell — will need to be added during planning.
- Beets's `assign_items` + `distance` primitives produce meaningful distance values when fed synthetic `Item` instances populated with `(title, length, track number, artist, album)` but lacking `(path, media, format)`. This is expected because beets distance is metadata-driven and the existing `compute_beets_distance` already builds an `AlbumInfo` from MB JSON with no path/media/format on the candidate side. Actual score quality on synthetic items is unvalidated — flagged as a Deferred-to-Planning question below.
- YT Music's `other_versions[]` field is populated for albums that have known sibling editions and is empty for albums that don't. Empty arrays are handled as "this is the only edition YT Music has for the release group."
- YT Music's `browseId` / `OLAK5uy_` IDs are stable per release across `ytmusicapi` versions and over time — verified by the research scan against the library's docs (same example pairs appear across multi-year version history).
- Anonymous YT Music access remains viable for read-only `search` + `get_album` in 2026. The realistic failure mode is per-IP throttling, absorbed by the cache. The recovery path is the burner-OAuth fallback documented in Key Decisions.
- The local MB and Discogs mirrors expose release-group and master-level enumeration of sibling releases. Verified: the existing browse routes (`web/routes/browse.py`) do this for the UI.
- The local MB mirror's release records carry the track-list-with-durations needed to build `AlbumInfo` for beets distance scoring. Verified by the existing `_build_album_info` in `lib/beets_distance.py`.
- The existing `BeetsDistanceCache` protocol (Redis-backed in production, in-process dict in tests) can be reused for caching `get_album` responses. Verified by reading the existing service.
- MusicBrainz's `release-url` "free streaming" relationship type is not used as a coverage hint. Sparse and not maintained for this use case.

---

## Outstanding Questions

### Deferred to Planning

- [Affects R6, R7][Needs research] How well does beets's `distance()` perform on synthetic items lacking `(path, media, format)` and built only from title + length + track number + artist + album? Validate against a small set of test release groups during planning — one popular (heavy YT Music presence, label-uploaded), one moderately obscure (likely Topic-uploaded), one archival (likely absent from YT Music entirely). If scores are noisier than the disk-backed Replace-picker path, the mitigation is to filter on individual distance components (track-title distance, track-count delta) rather than the aggregate.
- [Affects R4][Technical] What's the right seed-pick heuristic when MB search returns multiple plausible top candidates on the YT Music side (artist disambiguation, "Live at…" vs studio, compilation vs original)? Provisional answer: pick the YT result whose `year` + `trackCount` are closest to the MB seed release's; fall back to YT Music's top-ranked result if no clear winner. Validate during planning.
- [Affects R12][Technical] Should the persisted row's track-list snapshot include track durations, or store the full beets-style item record used at scoring time? Affects the ability to re-score (e.g. against a new MBID added to the release group later) without re-fetching from YT Music. Decide during schema design.
- [Affects R5][Technical] How long should anonymous `ytmusicapi` calls retry / back off on throttling before failing fast and returning `youtube_unavailable`? Provisional answer: at most one retry with short jitter, then fall through to the cached-or-unavailable path. Tune during implementation if observed behavior warrants.
- [Affects R15][Technical] Exact CLI subcommand name and HTTP route URL for the resolver and refresh endpoints. Provisional shapes: `pipeline-cli youtube-album <id>` / `GET /api/youtube/album?mbid=…`. Settle during planning.
