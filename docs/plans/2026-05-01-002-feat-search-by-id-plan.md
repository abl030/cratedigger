---
title: "feat: Search-by-ID — direct artist-view entry on Browse tab"
type: feat
status: active
date: 2026-05-01
deepened: 2026-05-01
origin: docs/brainstorms/search-by-id-requirements.md
---

# Search-by-ID — Direct Artist-View Entry on Browse Tab

## Overview

Add a third search-mode on the Browse tab — sibling to artist / release / label — that takes a pasted MBID, Discogs release ID, Discogs master ID, MB release-group MBID, or any of the corresponding URLs, and drops the user straight into the existing artist view with the parent group auto-expanded and the specific leaf (when one exists) ringed and scrolled into view. When the resolved artist is Various Artists, the artist view is bypassed in favour of a single-release / single-master detail card that reuses the existing release-detail component.

This routes through the existing `/api/add` flow (already accepts both `mb_release_id` and `discogs_release_id`), inherits all existing artist-view affordances (pipeline-status badges, library badges, disambiguation tab), and avoids creating a parallel "add by ID" path.

---

## Problem Frame

The Web UI's only path to "add to pipeline" today is name search → artist view → release row → Add. The user routinely hits cases where this is broken or noisy — VA compilations (#199), long titles with punctuation, pre-1990s reissues with multiple Discogs pressings of the same MB release group, niche-label / Bandcamp-only material. In all of these cases the user already has the canonical ID from MB or a Discogs collection page; there is no good reason to make them search.

The CLI (`pipeline-cli add <mbid|discogs-id>`) handles this end-to-end. The Web UI doesn't. The brainstorm pressure-tested two shapes (a bypass-the-artist-view direct-add input vs. a search-type that resolves into the existing artist view) and chose the latter — it gives the user pressing-disambiguation context they came to the UI for, dissolves the "already in pipeline" question (the artist view shows pipeline status natively), and fits the no-parallel-paths rule.

The brainstorm also measured both VA paths: Discogs `/api/artists/194` 404s (artist row genuinely doesn't exist in the mirror — `artist_id=194` is a sentinel referenced by 1.3M `release_artist` rows but has no row in `artist`), and MB VA's first page of release groups takes 23.3s. Caching cannot make either viewable. The VA fallback is therefore necessary on both sources.

---

## Requirements Trace

Carried verbatim from `docs/brainstorms/search-by-id-requirements.md`:

- R1. Search-by-ID is a new search-type sibling to artist / release / label.
- R2. Client-side **format detection**: 36-char dashed UUID → MB family; ≤12 digits → Discogs family; else inline error.
- R3. URL-stripping covers `/release/<uuid|id>`, `/release-group/<uuid>`, `/master/<id>` (with optional slug). URL path disambiguates type.
- R4. Bare-ID resolver tries leaf endpoint (`release`) first; on 404 falls back to group endpoint (`release-group` / `master`).
- R5. MB release MBID → artist view at primary artist, release-group expanded, that release ringed.
- R6. MB release-group MBID → artist view at primary artist, release-group expanded, no leaf ringed.
- R7. Discogs release ID → artist view at primary artist, master expanded (or masterless row), that release ringed.
- R8. Discogs master ID → artist view at primary artist, master expanded, no leaf ringed.
- R9. Resolution failure → inline error at input; no navigation.
- R10. Existing VA guard at `web/js/browse.js:108` extends to search-by-ID with a different fallback: render a single-release / single-master detail card.
- R11. VA detection: MB artist ID = `89ad4ac3-39f7-470e-963a-56509c546377`; Discogs `artists[0].id == 194`.
- R12. Fallback card reuses the existing release-detail component; for group VA inputs lists pressings/releases under the group.
- R13. Redis preload of VA discographies is rejected (volume + speed both prohibitive).
- R14. Persistent ring class (working name `search-target`): 2px coloured border + subtle bg tint.
- R15. Ring is persistent until close-artist-view / switch-search-mode / paste-another-ID.
- R16. `scrollIntoView({ behavior: 'smooth', block: 'center' })` after auto-expand so target row is in DOM.
- R17. Add `data-release-id` to `.release` rows in `web/js/discography.js:151` so the resolver can find the row.
- R18. Group inputs: auto-expand alone is the indicator; no master-row ring.
- R19. Already-in-pipeline behaviour is unchanged — artist view shows pipeline-status badge natively.

---

## Scope Boundaries

**In scope:**
- Search-by-ID input as a new search-type on the Browse tab.
- Client-side format detection + URL stripping for MB and Discogs, releases and groups, with URL path disambiguating type.
- Backend resolver endpoint covering all four input kinds, with leaf-first / group-fallback for bare IDs.
- Artist-view auto-expand of the parent group, persistent ring + scroll-into-view on the leaf when one exists. `data-release-id` on `.release` rows.
- VA fallback to single-release / single-master detail card on both sources.

### Deferred for later

- Detecting Discogs URLs from non-canonical surfaces (`/sell/release/<id>`, marketplace, mobile share links).
- "Recent direct adds" history list under the input.

### Outside this product's identity

- Bypass-the-artist-view direct-add (the original framing of #200). Routing through artist view is the deliberate choice — it gives the user pressing-disambiguation context they came to the UI for in the first place.
- Bulk paste of multiple IDs.

---

## Context & Research

### Relevant Code and Patterns

- `web/js/browse.js:71-93` — `setSearchType()` is the existing pattern to extend; new mode `'id'` slots in alongside `artist`/`release`/`label`.
- `web/js/browse.js:96-124` — `openBrowseArtist()` is the canonical entry point into the artist view; the VA short-circuit at line 108 is the convention to extend.
- `web/js/browse.js:175-206` — `loadBrowseDiscography()` fetches `/api/artist/<aid>` or `/api/discogs/artist/<aid>` and renders. The post-render hook for "after discography is in DOM, find the master row and click-to-expand it" lands here.
- `web/js/discography.js:125-174` — `loadReleaseGroup()` / release rendering. The `<div class="releases" id="rel-${rg.id}">` container is the auto-expand target. The `<div class="release" onclick="...toggleReleaseDetail('${rel.id}')">` row at line 151 is the ring target — needs a stable selector (R17).
- `web/js/discography.js:225-303` — `toggleReleaseDetail()` body, where the per-release detail render is **inline today**. U6 extracts this into a reusable helper before U5 can call it standalone.
- `web/routes/browse.py` — existing artist endpoints (`get_artist`, `get_discogs_artist`); the new resolver lives here. Route registration via `_FUNC_GET_PATTERNS` in `web/server.py:225` (regex routes) or `_FUNC_GET_ROUTES:217` (exact paths).
- `web/discogs.py` — `_get`, `get_artist_releases` (cached `discogs:artist:{id}:releases`), `get_master_releases` (`web/discogs.py:251-279`), masterless release shape (`web/discogs.py:215-228`). `get_release` for the leaf.
- `web/mb.py` — release / release-group / artist resolution helpers, `get_artist_name`.
- `web/cache.py` — `memoize_meta()` for the `meta:` namespace (24h TTL by default for MB-side keys, see `TTL_MB`); the resolver should cache its lookups under e.g. `mb:resolve:<mbid>` and `discogs:resolve:<id>`.
- `web/routes/pipeline.py:445-472, 509` — existing `/api/add` already accepts both `mb_release_id` and `discogs_release_id`; the resolver does NOT need to call this — the artist view's existing add buttons do.
- `tests/test_js_util.mjs` — runner for pure-JS unit tests (node `--check` + assertion harness). Extend in place — do not invent a new test file.
- `tests/test_web_server.py:606` `TestRouteContractAudit` — every new GET route must be listed in `CLASSIFIED_ROUTES` or this test fails. `tests/test_web_server.py:2453` `TestBrowseRouteContracts` — pattern for new contract tests with `_assert_required_fields`.

### Institutional Learnings

- `docs/solutions/` was scanned; nothing directly applicable to this feature. Closest adjacent learning is the discogs-api connection-pool fix (`docs/plans/2026-04-29-002`) — the resolver relies on the pool already, no new pressure.
- The existing VA short-circuit (`web/js/browse.js:108`) is itself a learning: VA degrades the artist view past usability and was already explicitly walled off. The same convention applies here, with a different fallback rendering.

### External References

None — vanilla JS + http.server + Redis + Postgres mirrors are well-established here, and the brainstorm has the timing data already verified against live mirrors.

---

## Key Technical Decisions

- **Single resolver endpoint, not four.** One `GET /api/browse/resolve` that takes the raw ID + optional `kind` hint from URL parsing, with leaf-first / group-fallback when `kind` is unspecified. Avoids four near-identical endpoints; the kind hint from URL stripping makes the common URL-paste case a single hop.
- **Resolver returns metadata only — no nested release/master detail payloads.** The frontend follows up by calling existing endpoints (`/api/artist/<id>`, `/api/discogs/artist/<id>`, or for VA fallback `/api/discogs/release/<id>` / equivalent MB) to render. Keeps the resolver focused; reuses fetch-on-input cache layer that's already in place.
- **`is_va` is computed server-side, not client-side.** The resolver knows the resolved artist ID; the VA constants live in one place (Python) rather than being duplicated in JS. Frontend just branches on `data.is_va`.
- **Ring is a CSS class, not an inline style.** New `.search-target` rule in `web/index.html` (the only stylesheet — vanilla project, no build step). Class is added to the matched `.release` row from JS via `data-release-id` selector.
- **Ring persistence is observed at `state.searchTargetId`.** A single module-level state field tracks the currently-ringed leaf. Cleared in `closeBrowseArtist()`, `setSearchType()`, and on the next paste. Re-applied if `loadDiscography()` re-renders mid-state (e.g. after an add-to-pipeline mutation invalidates the cache).
- **Discogs masterless releases ring in place.** They render as standalone rows in the discography (`web/discogs.py:215-228`), with `id="release-<n>"` then stripped to bare in the API. The frontend just searches for the `data-release-id="<bareId>"` row regardless of master/masterless.
- **Bare-MB-UUID fallback uses the MB mirror's release endpoint and falls back to release-group on 404.** Two ~50ms hops worst case. No client-side disambiguation needed; the resolver handles it.
- **Add `data-release-id` early as a no-op scaffolding commit (U1).** Lets U4 land cleanly without entangling behavioral and structural changes.

---

## Open Questions

### Resolved During Planning

- **Resolver endpoint shape.** One endpoint with optional `kind` hint, returning `{source, kind, artist_id, artist_name, is_va, expand_id, leaf_id}`. (See Key Technical Decisions.)
- **VA-detection location.** Server-side, with `is_va` boolean in response. (See Key Technical Decisions.)
- **Ring persistence model.** Module-level `state.searchTargetId`; cleared on close/mode-switch/re-paste; re-applied on re-render.

### Deferred to Implementation

- **MB release-group VA-fallback render path (Plan A vs Plan B).** U5 carries an explicit measurement step before deciding. The brainstorm's 23s number was for the MB artist→release-group-list endpoint; the path U5 hits is release-group→releases, which has not been measured. U5's verification block has the curl to run and the threshold rules.
- **CSS ring colour.** "2px coloured border + subtle bg tint" is the spec. Existing palette in `web/index.html` includes `#6a9` (accent green, used by `.tab.active`), `#6af` (link blue), and `#1a4a2a` (badge-new bg). Pick one; the implementer's call.
- **Resolver cache TTL.** Defaults to `TTL_MB` (24h). 24h is right for stable IDs; verify no edge case (e.g., a release renamed in MB) requires shorter. Default to 24h unless implementation surfaces a reason. Document the choice with an inline comment near the cache call so future TTL refactors don't quietly mutate behaviour.
- **Performance assertion (Success Criterion: < 200ms paste-to-render for the URL/leaf-hit path).** Not test-gated; covered by manual smoke. If the manual smoke shows resolver latency > 500ms, treat as a regression and investigate before declaring U3 done.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

**Decision matrix — pasted input → render path:**

| Input form | Source | URL-disambiguated kind? | Resolver action | Render |
|---|---|---|---|---|
| `https://musicbrainz.org/release/<uuid>` | MB | release | fetch release | artist-view, RG expanded, release ringed |
| `https://musicbrainz.org/release-group/<uuid>` | MB | release-group | fetch release-group | artist-view, RG expanded, no ring |
| `https://www.discogs.com/release/<id>[-slug]` | Discogs | release | fetch release | artist-view, master expanded (or masterless row), release ringed |
| `https://www.discogs.com/master/<id>[-slug]` | Discogs | master | fetch master | artist-view, master expanded, no ring |
| Bare UUID (MB) | MB | unknown | fetch release; on 404 fetch release-group | as above per resolved kind |
| Bare digits (Discogs) | Discogs | unknown | fetch release; on 404 fetch master | as above per resolved kind |
| Any of the above where resolved artist is VA | either | n/a | resolve as above; set `is_va=true` | bypass artist view; render single-release / single-master detail card |
| Garbage / unrecognised | n/a | n/a | client-side detection rejects before submit | inline "not a recognised ID" error |

**Sequence — happy path (Discogs release URL paste, non-VA):**

```
User pastes URL into search-by-ID input
  └─ JS: parseId() → {family:'discogs', kind:'release', id:'32457180'}
     └─ JS: GET /api/browse/resolve?id=32457180&kind=release&source=discogs
        └─ resolver: discogs_api.get_release(32457180)
           └─ returns {source:'discogs', kind:'release', artist_id:'1234',
                       artist_name:'Foo', is_va:false, expand_id:'<master_id>',
                       leaf_id:'32457180'}
     └─ JS: state.searchTargetId = '32457180'
     └─ JS: openBrowseArtist('1234', 'Foo')
        └─ loadDiscography() → renders discography → detects searchTargetId →
           expands master row 'expand_id' → finds [data-release-id='32457180'] →
           adds .search-target class → scrollIntoView({block:'center'})
```

**Sequence — VA branch:**

```
... resolver returns is_va:true, leaf_id:'32457180' (or master id) ...
JS: skip openBrowseArtist; call existing /api/discogs/release/32457180
JS: render the response in a single-release detail card overlay
    (reuses the existing release-detail component the artist view renders
    when a master row is expanded; same Add button)
```

---

## Implementation Units

- U1. **Add `data-release-id` to discography release rows (scaffolding)**

**Goal:** Add a stable selector to `.release` rows so the search-by-ID flow can find a specific row to ring. No behavioural change.

**Requirements:** R17

**Dependencies:** None.

**Files:**
- Modify: `web/js/discography.js`

**Approach:**
- In the release-row template at `web/js/discography.js:151`, add `data-release-id="${rel.id}"` to the outer `<div class="release">`. Use the same ID that's already passed to `toggleReleaseDetail(...)`.
- Apply the same attribute to the masterless-release row in the existing artist-view fallback branch and to bootleg rows so all `.release` rows are uniformly addressable.
- Verify no existing CSS or JS query selector collides with `[data-release-id]`. (Grep confirmed zero current uses across `web/js/*.js` during plan deepening; a regression check is still wise.)
- The attribute uniqueness invariant is **per rendered discography**, not global. Bootleg rows in different artist views may share IDs across artists; within one artist's discography render, IDs are unique.

**Patterns to follow:**
- Existing `id="rel-${rg.id}"` on `.releases` containers and `id="reldet-${rel.id}"` on detail containers.

**Test scenarios:**
Test expectation: none -- pure markup attribute, no behavioural change. Observable presence is exercised end-to-end by U4 in the artist-view smoke.

**Verification:**
- Browse any non-VA artist; inspect the DOM and confirm every `.release` row carries `data-release-id` matching the value passed to `toggleReleaseDetail`.
- `node --check web/js/discography.js` passes.

---

- U2. **JS utility: ID parser + URL stripper**

**Goal:** A pure function that takes pasted text and returns either `{family, kind, id}` or `null`. Handles bare UUIDs, bare digits, and the four supported URL forms with optional slug.

**Requirements:** R2, R3

**Dependencies:** None.

**Files:**
- Modify: `web/js/util.js` (or create `web/js/search_id.js` if util.js gets crowded — implementer's call)
- Test: `tests/test_js_util.mjs`

**Approach:**
- Single exported function, e.g. `parsePastedId(text) -> {family: 'mb'|'discogs', kind: 'release'|'release-group'|'master'|'unknown', id: string} | null`.
- Trim whitespace, lowercase a UUID, extract the canonical ID from URL forms with regex.
- The `kind === 'unknown'` case is the bare-ID case where the URL didn't disambiguate; the resolver does the leaf-first / group-fallback in that case.
- Keep the function pure and side-effect-free so it tests cleanly under node.

**Execution note:** Implement test-first — pure-function TDD with the existing `tests/test_js_util.mjs` harness.

**Patterns to follow:**
- Existing pure utilities in `web/js/util.js` (e.g. `detectSource`, `externalReleaseUrl`).
- Existing test conventions in `tests/test_js_util.mjs`: `assertEqual(actual, expected, msg)`.

**Test scenarios:**
- Happy path: bare 36-char dashed UUID → `{family: 'mb', kind: 'unknown', id: '<lower-uuid>'}`.
- Happy path: bare digits up to 12 chars → `{family: 'discogs', kind: 'unknown', id: '<digits>'}`.
- Happy path: `https://musicbrainz.org/release/<uuid>` → `{family: 'mb', kind: 'release', id: '<uuid>'}`.
- Happy path: `https://musicbrainz.org/release-group/<uuid>` → `{family: 'mb', kind: 'release-group', id: '<uuid>'}`.
- Happy path: `https://www.discogs.com/release/32457180` → `{family: 'discogs', kind: 'release', id: '32457180'}`.
- Happy path: `https://www.discogs.com/release/32457180-Various-Rock-Christmas-The-Very-Best-Of` → same.
- Happy path: `https://www.discogs.com/master/3673686` → `{family: 'discogs', kind: 'master', id: '3673686'}`.
- Happy path: `https://www.discogs.com/master/3673686-Slug-Words` → same.
- Edge case: leading/trailing whitespace stripped before detection.
- Edge case: UUID with uppercase letters — normalised to lowercase.
- Edge case: URL without `https://` (bare `musicbrainz.org/release/<uuid>`) — accepted.
- Edge case: URL with trailing slash or `?utm_*` querystring — id correctly extracted.
- Edge case: URL with multiple matching path segments (e.g. embedded `/release/` in slug) — first match wins, no false positive.
- Edge case: URL with fragment (`#disc1`) after the slug — fragment stripped, ID correctly extracted.
- Error path: garbage text (`"hello world"`) → `null`.
- Error path: non-canonical MB URL (`mbid.eu/<uuid>`, `https://beta.musicbrainz.org/release/<uuid>`) → `null` (deferred per Scope Boundaries; verifies we don't accept it accidentally).
- Error path: UUID without dashes (32 hex chars) → `null`.
- Error path: 13-digit numeric → `null` (Discogs IDs are bounded ≤12).
- Error path: mixed alphanumeric (`abc123`) → `null`.
- Error path: empty string → `null`.

**Verification:**
- `node tests/test_js_util.mjs` passes with all new scenarios.
- `node --check web/js/util.js` (or new module) passes.

---

- U3. **Backend resolver endpoint `/api/browse/resolve`**

**Goal:** Single GET endpoint that takes a parsed ID + optional kind hint and returns `{source, kind, artist_id, artist_name, is_va, expand_id, leaf_id}`. Implements leaf-first / group-fallback when kind is unspecified.

**Requirements:** R4, R5, R6, R7, R8, R9, R11

**Dependencies:** None (independent of U1, U2).

**Files:**
- Modify: `web/routes/browse.py` (add `resolve_id` handler + entry in `_FUNC_GET_ROUTES` registration).
- Modify: `web/server.py:217` (route registration; add `/api/browse/resolve` to `_FUNC_GET_ROUTES`).
- Test: `tests/test_web_server.py` (extend `TestBrowseRouteContracts` or add `TestSearchByIdResolveContract`; add classification entry to `TestRouteContractAudit.CLASSIFIED_ROUTES`).

**Approach:**
- Route shape: `GET /api/browse/resolve?source=<mb|discogs>&id=<...>&kind=<release|release-group|master>` (kind optional).
- Source is required (the JS already knows from format detection); avoids the resolver having to detect format itself.
- For Discogs: when `kind == 'release'` (or unspecified), call `discogs_api.get_release(id)`; read `master_id` and `artists[0].id`. If 404 and `kind` was unspecified, fall back to `discogs_api.get_master(id)` (or `get_master_releases` already in `web/discogs.py:251`).
- For MB: when `kind == 'release'` (or unspecified), call MB release lookup (`web/mb.py`); read release-group ID and primary artist ID. On 404 unspecified-fallback, call MB release-group lookup.
- VA detection in one place: after resolution, set `is_va = (source=='mb' and artist_id == VA_MBID) or (source=='discogs' and artist_id == 194)`. Define VA constants at module scope or in `web/mb.py` / `web/discogs.py` (single source of truth, not duplicated).
- For Discogs masterless releases: `expand_id` is the bare release ID (same as `leaf_id`); the frontend handles the masterless render path correctly because the existing artist view already renders masterless rows as standalone `.release` entries (`web/discogs.py:215-228`).
- For group inputs (kind == release-group / master): `leaf_id = null`, `expand_id = id`.
- Cache resolver responses via `memoize_meta()` keyed by `resolve:<source>:<id>` with `TTL_MB`. IDs are stable; cache is safe.
- Failure: 404 from both endpoints attempted → JSON 404 with `{error: 'not_found', message: '...'}`. Network/upstream error → 502.

**Execution note:** Start with a failing contract test in `TestBrowseRouteContracts` asserting all `REQUIRED_FIELDS`. Also confirm `TestRouteContractAudit` fails on the unclassified route (RED) before adding the entry to `CLASSIFIED_ROUTES` (GREEN). The audit is the strictest contract gate in the repo; making the RED/GREEN explicit prevents shipping an un-audited route.

**Patterns to follow:**
- Existing route registration in `web/server.py:217-225`.
- Existing handler signature in `web/routes/browse.py` (e.g., `get_discogs_artist(h, params, artist_id)`).
- `_assert_required_fields` + `REQUIRED_FIELDS` set in `tests/test_web_server.py:2453` `TestBrowseRouteContracts`.
- `patch("web.routes.browse.discogs_api")` and `patch("web.server.mb_api")` patterns already used by browse contract tests.

**Test scenarios:**
- Happy path: `?source=mb&id=<release-mbid>&kind=release` → returns `kind: 'release'`, `artist_id`, `artist_name`, `expand_id` (release-group MBID), `leaf_id == id`, `is_va: false`.
- Happy path: `?source=mb&id=<rg-mbid>&kind=release-group` → returns `kind: 'release-group'`, `expand_id == id`, `leaf_id: null`.
- Happy path: `?source=discogs&id=32457180&kind=release` → returns `kind: 'release'`, `expand_id` (master_id), `leaf_id == id`, `is_va: true` (Various sentinel).
- Happy path: `?source=discogs&id=3673686&kind=master` → returns `kind: 'master'`, `expand_id == id`, `leaf_id: null`.
- Edge case: `?source=discogs&id=<masterless-release>&kind=release` (master_id = null) → returns `kind: 'release'`, `expand_id == leaf_id == id`.
- Edge case: kind omitted, MB UUID happens to be a release-group → resolver tries release endpoint, gets 404, retries release-group endpoint, returns `kind: 'release-group'`. Verify only two upstream calls (no thrash).
- Edge case: kind omitted, Discogs digit happens to be a master ID → resolver tries release, 404, retries master, returns `kind: 'master'`.
- Edge case: kind hint **honored** — `?kind=release` with an ID that 404s on the release endpoint returns 404 immediately and does NOT probe the group endpoint. This guards the URL-disambiguation optimisation from regressing into dead code.
- Edge case: VA on MB — artist_id matches `89ad4ac3-39f7-470e-963a-56509c546377` → `is_va: true`.
- Edge case: Discogs ID-namespace assumption — a bare digit that exists as both a release and a master (different entities sharing the same numeric ID across tables) — verified during implementation to be impossible per Discogs schema. If implementation surfaces a counter-example, document it; otherwise the leaf-first order is unambiguous.
- Error path: ID not found in either endpoint → 404 response with `error: 'not_found'`.
- Error path: missing `id` parameter → 400.
- Error path: missing `source` parameter → 400.
- Error path: invalid `source` (`?source=apple`) → 400.
- Error path: discogs_api or mb_api raises non-404 exception → 502 (or whatever the existing convention is for upstream failure in browse routes — match existing patterns).
- Integration: `TestRouteContractAudit` passes — the new route is in `CLASSIFIED_ROUTES`.
- Integration: response includes every field in `REQUIRED_FIELDS = {"source","kind","artist_id","artist_name","is_va","expand_id","leaf_id"}`.

**Verification:**
- `nix-shell --run "python3 -m unittest tests.test_web_server.TestBrowseRouteContracts -v"` passes.
- `nix-shell --run "python3 -m unittest tests.test_web_server.TestRouteContractAudit -v"` passes (no unclassified route).
- Manual: `curl 'http://localhost:8085/api/browse/resolve?source=discogs&id=32457180&kind=release'` returns the expected payload with `is_va: true`.

---

- U4. **Frontend: search-by-ID mode + resolve-and-navigate flow**

**Goal:** Wire up the third search-mode in the Browse tab, take pasted input, call the resolver, drive the artist view to auto-expand the parent group, ring the leaf, and scroll into view. Implements the non-VA branch of the feature.

**Requirements:** R1, R5, R6, R7, R8, R9, R14, R15, R16, R18

**Dependencies:** U1 (`data-release-id`), U2 (parser), U3 (resolver endpoint).

**Files:**
- Modify: `web/index.html` (add `.search-target` CSS rule; add the new search-type button next to existing artist/release/label buttons).
- Modify: `web/js/browse.js` (extend `setSearchType` to accept `'id'`; add `state.searchTargetId` field; add paste-handler that calls resolver; clear `searchTargetId` in `closeBrowseArtist` and `setSearchType` and on next paste).
- Modify: `web/js/discography.js` (after `loadReleaseGroup` / discography render, if `state.searchTargetId` is set and the parent group matches the resolver's `expand_id`, programmatically expand the group, find the `.release[data-release-id="<id>"]` row, add `.search-target`, `scrollIntoView`).
- Modify: `web/js/state.js` (add `searchTargetId: null` field).
- Modify: `web/js/main.js` (event handler for the new search-type button; paste-handler for the id input).

**Approach:**
- New search-type `'id'` selectable via the same button-row pattern in `setSearchType` (`web/js/browse.js:71-92`). Placeholder updated per R1.
- Paste handler (on input or button-click): trim, run `parsePastedId()`, if null show inline error and bail; otherwise call `/api/browse/resolve` with the parsed `{source, kind, id}`.
- On resolver success and `is_va: false`: store `state.searchTargetId = response.leaf_id` (may be null for group inputs), store `state.searchExpandId = response.expand_id`, then call `openBrowseArtist(response.artist_id, response.artist_name)`. The post-discography-render hook in `discography.js` does the expand + ring + scroll.
- On resolver success and `is_va: true`: dispatch to U5's VA-fallback render (don't call `openBrowseArtist`).
- On resolver failure: show inline error at the input.
- Ring application happens after the master is expanded, so the leaf row is in the DOM. Use `requestAnimationFrame` or the existing `toggleReleaseGroup` callback chain to know when expansion is done.
- Persistence (R15): the ring class stays applied as long as `state.searchTargetId` is set. Cleared in `closeBrowseArtist`, in `setSearchType`, and at the start of the next paste-handler invocation.
- Re-application on re-render: when `loadDiscography` re-renders (e.g. after an add-to-pipeline mutation invalidates the artist cache), the post-render hook re-runs and re-applies the ring if `searchTargetId` is still set.
- CSS rule in `web/index.html`: `.release.search-target { border: 2px solid <accent>; background: <subtle-tint>; }` — implementer chooses concrete colours from the existing palette.

**Patterns to follow:**
- `setSearchType` button-row + placeholder pattern (`web/js/browse.js:71-92`).
- Existing in-flight token pattern (`searchArtistsRequestToken` in `web/js/browse.js:101`) for race protection — apply the same idiom to the resolver call so a fast double-paste doesn't render the wrong target.
- `invalidateBrowseArtist()` (`web/js/browse.js:139-143`) is the existing cache-invalidation entry — re-render triggers the hook, ring re-applies.

**Test scenarios:**
- Happy path (Playwright smoke): paste a non-VA Discogs release URL into the id input → artist view opens → master row auto-expands → matching release row has `.search-target` class → row is scrolled into the viewport. Verify no other rows have the class.
- Happy path (Playwright smoke): paste an MB release MBID (bare) → artist view opens with the release-group expanded and the release ringed.
- Happy path (Playwright smoke): paste a Discogs master URL → artist view opens with master expanded; no row carries `.search-target` (R18).
- Happy path (Playwright smoke): paste an MB release-group MBID → artist view opens with that release-group expanded; no ring.
- Edge case (Playwright smoke): paste a Discogs masterless release ID → artist view shows masterless row carrying `.search-target`.
- Edge case (Playwright smoke): after a successful paste-and-ring, switch search-type back to artist → ring is cleared, `state.searchTargetId === null`.
- Edge case (Playwright smoke): close the artist view (back button or close affordance) → ring is cleared.
- Edge case (Playwright smoke): switch to Library or Pipeline tab and back to Browse → if the artist view was open with a ring, the ring is still applied on return (state survives tab switches; only explicit close / mode-switch / re-paste clears it).
- Edge case (Playwright smoke): paste ID A, immediately paste ID B before resolver returns A → only B's ring is applied (in-flight token discards A's response).
- Edge case (Playwright smoke): after add-to-pipeline mutates and invalidates the artist cache, re-render runs and the ring is re-applied to the same row.
- Error path (Playwright smoke): paste garbage text → inline "not a recognised ID" error; no resolver call.
- Error path (Playwright smoke): paste a valid-looking ID that doesn't exist (resolver 404) → inline error at input; no navigation.

**Verification:**
- `node --check web/js/browse.js web/js/discography.js web/js/state.js web/js/main.js` passes.
- All Playwright smoke scenarios above pass against `https://music.ablz.au` after `ssh doc2 'sudo systemctl restart cratedigger-web'`.
- Visual: ring is unambiguous — a brief manual look confirms the styling distinguishes the target without making other rows look broken.

---

- U6. **Extract release-detail render into a reusable helper (refactor)**

**Goal:** Pull the inline release-detail render body out of `toggleReleaseDetail()` (`web/js/discography.js:225-303`) into a standalone exported helper so U5 can render the same card outside the artist-view expand context. Pure refactor — no behavioural change to the existing artist-view expand path.

**Requirements:** Precondition for R12 (single-release / single-master detail card reuses the existing component).

**Dependencies:** None (independent of U1–U4; can land first or last among U1–U4 — only U5 depends on it).

**Files:**
- Modify: `web/js/discography.js` (extract helper, e.g. `renderReleaseDetail(container, releaseData)`; have `toggleReleaseDetail()` call it).
- Test: `tests/test_js_util.mjs` is not appropriate (helper touches DOM). Coverage is observable via U5 + the existing artist-view expand smoke.

**Approach:**
- Identify the section of `toggleReleaseDetail()` that takes a fetched release payload and writes the inner HTML of `<div class="release-detail" id="reldet-${rel.id}">`. Lift it into a helper `renderReleaseDetail(targetEl, releaseData, opts)` where `opts` includes things like `showHeader: bool` for the standalone-card case.
- `toggleReleaseDetail()` becomes: fetch → call the helper → wire up the close affordance.
- No change to network calls, no change to the rendered HTML for the existing expand path. Verify visually that the artist-view release-detail expand looks identical to before.

**Execution note:** Characterization-first — run the existing artist-view expand path before and after the refactor and compare output. Any diff means the refactor changed behaviour and should be reverted.

**Patterns to follow:**
- Pure-extract refactor pattern from prior plans (no behavioural change).

**Test scenarios:**
Test expectation: none -- pure refactor, equivalence proven by characterization (existing artist-view expand renders identically before and after). Behavioural coverage of the helper lands with U5.

**Verification:**
- `node --check web/js/discography.js` passes.
- Manual: open any non-VA Discogs artist in Browse, expand a master row, click into a release row. The detail card renders identically to before this commit. Diff the rendered HTML if uncertain.
- Manual: same for an MB artist's release-group expansion.

---

- U5. **VA fallback card**

**Goal:** When the resolver returns `is_va: true`, bypass the artist view and render a single-release / single-master detail card with the existing Add-to-pipeline button. Reuses the helper extracted in U6.

**Requirements:** R10, R11, R12

**Dependencies:** U3 (resolver returns `is_va`), U4 (frontend dispatch into this branch), U6 (release-detail helper extracted).

**Files:**
- Modify: `web/js/browse.js` (VA branch in the resolver-callback flow; new render entry that hides search results, hides artist view, shows the standalone fallback container; populates it via the helper from U6).
- Modify: `web/index.html` (container div for the standalone VA fallback card if there isn't an existing one to repurpose; add `id="va-fallback"` or similar; minimal CSS).
- Optional modify: `web/routes/browse.py` if the existing per-release detail endpoints don't return everything needed for the standalone render — usually they do; verify before adding.

**Approach:**
- For Discogs leaf VA: fetch `/api/discogs/release/<id>` (existing route, ~20ms) and pass the response to `renderReleaseDetail()` (U6) in the fallback container.
- For Discogs master VA: fetch `/api/discogs/master/<id>` (existing route) and render the master title + a list of pressings, each row carrying an Add-to-pipeline button via the existing pressing-row component.
- For MB release leaf VA: fetch `/api/release/<mbid>` (existing route, registered at `web/routes/browse.py` via regex `^/api/release/([a-f0-9-]+)$`, handler `get_release`).
- **MB release-group VA — measure before deciding.** The brainstorm's 23s number was for the MB artist→release-group-list endpoint, NOT the release-group→releases endpoint that would be hit here. Before implementing the MB release-group VA branch, run:
  ```
  ssh doc2 'curl -s -o /dev/null -w "MB RG VA releases HTTP %{http_code} time %{time_total}s\n" "http://192.168.1.35:5200/ws/2/release?release-group=<some-VA-RG-MBID>&limit=25&fmt=json"'
  ```
  using a known VA-credited release-group MBID (find one via `/api/search?q=Rock+Christmas&type=release` or similar). If the timing is acceptable (< 2s), implement Plan A: list pressings under the release-group with Add buttons. If it's > 5s or times out, implement Plan B: render only the release-group title + Add-to-pipeline button keyed off the release-group MBID, with a "View on MusicBrainz" external link.
- The fallback card has a clear "Various Artists — bypassed artist view because [VA explanation]" header so the user knows why they didn't get the normal flow.
- A close affordance on the card returns to whatever was previously visible (search results, or empty Browse state).
- **Serial-ordering note**: U5 modifies `web/js/browse.js`, which U4 also modifies. U4 must land before U5 so the `is_va` dispatch site exists for U5 to extend. Do not branch U5 in parallel with U4.

**Patterns to follow:**
- `renderReleaseDetail()` from U6.
- Existing VA toast at `web/js/browse.js:108` is the convention this extends; the toast goes away in the resolver-driven VA path because we can render something instead of giving up.
- Existing add-to-pipeline button pattern from `web/js/browse.js` / `discography.js`.

**Test scenarios:**
- Happy path (Playwright smoke): paste Discogs release ID `32457180` (Rock Christmas comp) → VA fallback card renders with title, format, track list, Add button. Add button is functional (clicking it adds to pipeline; verify via `pipeline-cli show <id>` after).
- Happy path (Playwright smoke): paste a Discogs master VA ID → VA fallback shows the master title + a list of pressings; each pressing has an Add button.
- Happy path (Playwright smoke): paste an MB release MBID where `artist == VA_MBID` → VA fallback card renders.
- Happy path (Playwright smoke): paste an MB release-group MBID where `artist == VA_MBID` → either Plan A (pressings list) or Plan B (title + external link) renders depending on the measurement above. Decision and rationale recorded in the commit message.
- Edge case (Playwright smoke): close the VA fallback card → returns to previous Browse state (search results or empty state).
- Edge case (Playwright smoke): click Add on a VA fallback pressing already in the pipeline → existing duplicate-handling kicks in (the Add button's existing behaviour) — verify it shows the existing-pipeline state, not an error.
- Error path (Playwright smoke): if the MB RG VA endpoint stalls past 30s (the http.server default timeout), the request fails cleanly and Plan B's external link is shown — no spinning forever, no orphaned modal.
- Integration (Python contract test, in U3): a non-VA release that happens to credit Discogs `artists[0].id == 194` as primary (treated as VA by the resolver). Verify the resolver returns `is_va: true` and document this as expected behaviour — Discogs uses 194 as the canonical Various sentinel, so legitimate non-VA credits to artist 194 don't exist in the dump.

**Verification:**
- `node --check web/js/browse.js` passes (U6 already verified `discography.js`).
- All Playwright smoke scenarios above pass.
- A successful add-to-pipeline from the VA fallback card creates an `album_requests` row with the correct `discogs_release_id` or `mb_release_id` — verify with `ssh doc2 'pipeline-cli show <new-id>'` matching the pasted ID.

---

## System-Wide Impact

- **Interaction graph:** New endpoint at `/api/browse/resolve` joins the existing `_FUNC_GET_ROUTES` table; route audit must be updated. Frontend resolver-driven flow piggybacks on existing `openBrowseArtist` and the discography render pipeline. No changes to `/api/add` or any pipeline-DB-touching code.
- **Error propagation:** Resolver 404/502 surfaces as inline error at the input — no toast, no navigation. Network errors during the post-resolve fetches (artist view / VA card) follow existing artist-view error patterns (loading spinner → "Failed to load" message).
- **State lifecycle risks:** `state.searchTargetId` is module-level state. Forgetting to clear it on close-artist-view / mode-switch / re-paste would cause stale rings on subsequent renders. Mitigated by clearing in three explicit sites (R15) and verified by Playwright smoke (close-artist-view edge case).
- **API surface parity:** No change to existing API; one new GET. The `_FUNC_GET_PATTERNS` regex table is unchanged (new route is exact-path).
- **Integration coverage:** The Playwright smoke scenarios in U4 and U5 are the cross-layer coverage — they exercise the parser (U2), resolver (U3), discography render (U1), and ring application (U4) together. JS unit tests alone cannot prove the data-release-id selector finds the right row after a real discography render.
- **Unchanged invariants:** `/api/add` contract is unchanged. `openBrowseArtist` and the existing VA-toast code path (clicking a VA artist link) remain intact — the new resolver-driven VA path is a separate code branch keyed off `is_va` from the resolver. Existing browse, library, recents, pipeline tabs are untouched.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| MB release-group VA-fallback render path (R12) hits the same 23s slow path the Brainstorm measured for the artist-view release-group endpoint | U5 has Plan A / Plan B; verify in-flight at implementation time. Plan B (single-release-detail + external link) is acceptable for the VA-RG case if the endpoint is genuinely slow. |
| `data-release-id` collides with existing CSS or JS selectors | Grep for any current `data-release-id` usage before U1; the attribute is new to the codebase per the brainstorm scan. CSS attribute selectors `[data-release-id]` are unused. |
| Bare-ID fallback (release-then-group) doubles upstream calls in the unspecified-kind case | Worst-case ~50ms total; acceptable. Cache hit on second paste of the same ID makes both subsequent attempts trivial. |
| Ring not re-applied after a cache-invalidating mutation (e.g. add-to-pipeline) | Re-render hook re-checks `state.searchTargetId`; covered in U4 test scenarios. |
| Two pastes in flight race the discography render | Existing `searchArtistsRequestToken` idiom in `web/js/browse.js:101`; apply the same pattern to the resolver call. Covered in U4 test scenarios. |
| Resolver cache stale after MB release rename | 24h `TTL_MB` is the existing default for MB-side `meta:` keys. MB releases rarely change identity once minted; acceptable. |
| MB release-group VA endpoint stalls past 30s http.server worker timeout (U5 Plan A) | Measure first (U5's spike). If timing is borderline, fall through to Plan B (title + external link). U5 error-path scenario covers the timeout case explicitly. |
| Resolver cache key uses shared `meta:` namespace; if `TTL_MB` shifts globally, resolver TTL silently follows | Document the resolver's TTL choice as an inline comment at the cache call site so future TTL refactors know about the dependency. |
| Discogs `artists[0].id == 194` VA detection is fragile if a non-VA release ever credits artist 194 as primary | Verified during plan deepening — 194 is the canonical Discogs Various sentinel and has no real `artist` row, so legitimate non-VA primary credits cannot exist. Test scenario added in U3 makes the assumption explicit. |

---

## Documentation / Operational Notes

- **No NixOS module changes.** This is a pure repo change; deploy via the standard flake-input flow described in `CLAUDE.md` (push → `nix flake update cratedigger-src` on doc1 → `nixos-rebuild switch` on doc2). `cratedigger-web` restarts automatically on switch.
- **No DB migrations** — the resolver is read-only against the existing mirrors; pipeline DB is untouched.
- **No new dependencies** in `nix-shell` or `web/cache.py`.
- **Manual smoke list after deploy:** run the U4 + U5 Playwright scenarios against `music.ablz.au`. Use small / obscure artists to avoid 23s mirror waits per the playwright-test-artists memory.
- **Issue 200 closes** when U1–U5 are merged and the Acceptance items in the issue are exercised.

---

## Sources & References

- **Origin document:** `docs/brainstorms/search-by-id-requirements.md`
- Related issue: #200
- Related issue (referenced motivator): #199 (VA search broken)
- Related code (always cite by path + symbol):
  - `web/routes/browse.py` — artist endpoints + new resolver target
  - `web/js/browse.js` — `setSearchType`, `openBrowseArtist`, VA guard at line 108
  - `web/js/discography.js` — release-row template, expand mechanics
  - `web/discogs.py` — `get_release`, `get_master_releases`, masterless release shape
  - `web/cache.py` — `memoize_meta`, `TTL_MB`
  - `tests/test_web_server.py:606` `TestRouteContractAudit`, `:2453` `TestBrowseRouteContracts`
  - `tests/test_js_util.mjs` — pure-JS test harness
- External: none used; brainstorm has the verified live-mirror timing data.
