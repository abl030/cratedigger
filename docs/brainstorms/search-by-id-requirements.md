---
date: 2026-05-01
topic: search-by-id
issue: 200
---

# Search-by-ID — Direct Add Path on Browse Tab

## Problem Frame

The Web UI's only "add to pipeline" path today is name search → artist view → release row → Add. This is broken or noisy in several cases the user routinely hits:

- Various Artists / compilation search is fundamentally unusable (#199).
- Long titles with punctuation (`...I Care Because You Do`, `Fly or Die // Fly or Die`) return wrong-album top hits.
- Pre-1990s reissues with multiple Discogs pressings of the same MB release group.
- Bandcamp / niche labels not well-indexed in either mirror's ranking.

In all of these the user already knows the canonical ID — they're cross-referencing MB or a Discogs collection page. The CLI path (`pipeline-cli add <mbid|discogs-id>`) handles this end-to-end; the Web UI doesn't. `web/routes/pipeline.py` `/api/add` already accepts both `mb_release_id` and `discogs_release_id` (`web/routes/pipeline.py:445-472, 509`), so this is a frontend addition.

## Shape

**Search-by-ID is a third search mode**, sitting under the existing Search bar alongside artist / release / label search. Pasted input resolves into the **existing artist view** rather than into a parallel "add by ID" UI — the same UI surface the user already uses to pick releases and add them to the pipeline. The artist view auto-expands the parent of the pasted ID (master for Discogs, release-group for MB) and applies a persistent ring around the specific pasted ID inside it.

Both leaf-level IDs (specific pressings / releases) and group-level IDs (masters / release-groups) are accepted in v1. Leaf inputs are climbed to their parent before rendering; group inputs render at the parent directly with no leaf to ring.

This avoids a parallel add path (matches the no-parallel-paths rule), inherits all the existing artist-view affordances (pipeline status badges, library badges, disambiguation tab), and dissolves the "already in pipeline" question — the artist view already shows that.

## Requirements

### ID input

- R1. Search-by-ID is exposed as a new search-type under the existing search bar, sibling to artist / release / label. Selection rule + placeholder string mirror the existing `setSearchType()` pattern (`web/js/browse.js:71-92`). Placeholder text shows the four shapes explicitly (e.g., "MBID, Discogs release/master ID, or URL").
- R2. Client-side **format detection** classifies pasted text after URL-stripping into one of two families. **Type disambiguation** within a family (release vs release-group / release vs master) is resolved by the server resolver, not the client:
  - 36-char UUID with dashes → MB UUID family (release MBID *or* release-group MBID).
  - Pure digits, ≤ 12 chars → Discogs numeric family (release ID *or* master ID).
  - Anything else → inline "not a recognised ID", no submit.
- R3. URL-stripping covers (at minimum), with the URL path itself disambiguating type when present so the resolver does not need to probe both endpoints:
  - `https://musicbrainz.org/release/<uuid>` → MB release MBID.
  - `https://musicbrainz.org/release-group/<uuid>` → MB release-group MBID.
  - `https://www.discogs.com/release/<id>` and `https://www.discogs.com/release/<id>-Slug-Words` → Discogs release ID.
  - `https://www.discogs.com/master/<id>` and `https://www.discogs.com/master/<id>-Slug-Words` → Discogs master ID.
- R4. When a bare ID is pasted (no URL), the resolver tries the leaf endpoint first (`release` for both sources) and falls back to the group endpoint (`release-group` / `master`) on 404. Two upstream hops in the fallback case is acceptable (single Discogs release fetch ~20 ms; MB similar). The resolver returns the resolved type in its response so the frontend knows whether to ring a leaf or render at the group with no leaf.

### Resolution flow

- R5. **MB release MBID** (leaf) → fetch the release from the MB mirror, read its release-group ID and primary artist ID. Drop into artist view at the artist ID; auto-expand the release group; ring the specific release MBID within the expansion.
- R6. **MB release-group MBID** (group) → fetch the release-group from the MB mirror, read its primary artist ID. Drop into artist view at the artist ID; auto-expand the release group; no leaf to ring (the master/group row itself receives no extra ring — its open state is the indicator).
- R7. **Discogs release ID** (leaf) → fetch the release from the Discogs mirror, read its `master_id` and primary `artists[0].id`.
  - If `master_id` is non-null → drop into artist view at the primary artist ID; auto-expand that master; ring the specific release ID within the expansion.
  - If `master_id` is null (masterless) → drop into artist view at the primary artist ID; the masterless release appears as a standalone row in the discography (existing behaviour, `web/discogs.py:215-228`); ring that row.
- R8. **Discogs master ID** (group) → fetch the master from the Discogs mirror, read its primary artist ID. Drop into artist view at the artist ID; auto-expand that master; no leaf to ring.
- R9. Resolution failure (404 from both endpoints attempted, network error) shows the upstream error inline at the input. The user stays on the Browse tab; no navigation.

### Various Artists fallback

- R10. The existing VA guard at `web/js/browse.js:108` (toast + force release-search when the user clicks a VA artist link) is extended to fire at search-by-ID resolution as well, but with a different fallback: render a **single-release / single-master detail card** for the pasted ID, with the existing Add-to-pipeline button, instead of the artist view.
- R11. VA detection:
  - **MB**: resolved artist ID equals the canonical VA MBID (`89ad4ac3-39f7-470e-963a-56509c546377`).
  - **Discogs**: `artists[0].id == 194` on the release or master payload (Discogs uses 194 as the "Various" sentinel; `/api/artists/194` 404s — the artist row genuinely doesn't exist in the mirror, confirmed against the live DB).
- R12. The fallback card reuses the existing release-detail component the artist view already renders when a master row is expanded. For master / release-group group inputs whose primary artist is VA, the card lists the pressings under the master (Discogs) or releases under the release-group (MB) directly, since there is no artist view to descend through. No new component beyond the existing release-detail.
- R13. The Redis preload of VA discographies (raised during brainstorm) is rejected. Discogs VA: 1.3M rows credited to `artist_id=194`, with no `artist` row to render against. MB VA: first page of release groups takes 23.3 s against the live mirror. Caching does not make either viewable; the fallback above is the right answer.

### Highlight UX (the ring)

- R14. The "ring" is a CSS class (working name `search-target`) added to the matched leaf row inside the discography. Visual: 2 px coloured border + subtle background tint, distinct from but consistent with the existing release/release-detail styling in `web/js/discography.js:151-159`.
- R15. The ring is **persistent** — it stays applied until the user navigates away (closes the artist view, switches search mode, or pastes another ID). It is not a flash-and-fade.
- R16. On render, the ringed row is scrolled into view (`scrollIntoView({ behavior: 'smooth', block: 'center' })`). The auto-expand of the parent master/release-group happens before the scroll so the leaf row exists in the DOM by the time we scroll.
- R17. The discography templating in `web/js/discography.js:151` does not currently put a stable `id` on the `.release` row (only an onclick handler with the release ID). Adding `data-release-id="${rel.id}"` (or equivalent) so the search-by-ID resolver can find the row to ring is in scope.
- R18. For group-level inputs (master / release-group), the auto-expand alone is the indicator — no ring on the master row itself. The user pasted "show me this group", and "this group is now open" answers that.

### Already-in-pipeline behaviour

- R19. No special-case logic. The artist view (or the VA fallback card) already shows the pipeline-status badge for that release. If it's already added, the user sees the existing state and a disabled / "View in pipeline" affordance, identical to today's Browse tab.

## Success Criteria

- Pasting MB release MBID `c1f6a2c9-bcba-4e69-96f5-233c85b2830a` opens The Wiggles' artist view, the matching release group is auto-expanded, and the specific release is ringed and scrolled into view.
- Pasting an MB release-group MBID opens the artist view with the release group expanded; no leaf is ringed.
- Pasting Discogs release ID `32457180` opens the VA fallback card for Rock Christmas (compilation), Add button active.
- Pasting `https://www.discogs.com/release/32457180-Various-Rock-Christmas-The-Very-Best-Of` strips and resolves identically to the bare ID (URL path is the type signal — no fallback probe needed).
- Pasting `https://musicbrainz.org/release/c1f6a2c9-bcba-4e69-96f5-233c85b2830a` strips and resolves identically to the bare MBID.
- Pasting `https://www.discogs.com/master/3673686` opens the artist view with master 3673686 auto-expanded; no leaf ringed.
- Pasting `https://musicbrainz.org/release-group/<uuid>` opens the artist view with that release group auto-expanded; no leaf ringed.
- Pasting a bare MB UUID that's a release-group (not a release) succeeds via the leaf-then-group fallback: the resolver tries the release endpoint, gets 404, retries the release-group endpoint, and renders the group-input flow.
- Pasting a non-VA Discogs release ID with a master opens the artist view, auto-expanded at the master, with that pressing ringed and scrolled into view.
- Pasting a masterless Discogs release ID opens the artist view with the masterless row ringed.
- The ring persists until the user closes the artist view, switches search mode, or pastes another ID.
- Resolution time for the URL-or-leaf-hit path stays under 200 ms wall-clock from paste-blur to artist-view render (single Discogs release fetch is ~20 ms; artist-view fetch is the dominant cost and is the existing path). Bare-ID fallback paths (release tried, then group) stay under ~250 ms.

## Scope Boundaries

**In scope:**
- Search-by-ID input as a new search type on the Browse tab.
- Client-side format detection + URL stripping for MB and Discogs, releases and masters/release-groups, including URL-paths that disambiguate type.
- Resolver: MB release MBID → release-group + artist; MB release-group MBID → artist; Discogs release ID → master + artist; Discogs master ID → artist. Bare-ID fallback (release first, group on 404) for the cases where the URL path didn't disambiguate.
- Artist-view auto-expand of the parent group, and persistent ring + scroll-into-view on the leaf when one exists. `data-release-id` on `.release` rows so the ring can be applied.
- VA fallback to single-release / single-master detail card on both sources.

**Deferred for later:**
- Detecting Discogs URLs from non-canonical surfaces (`/sell/release/<id>`, marketplace, mobile share links).
- "Recent direct adds" history list under the input.

**Outside this product's identity:**
- Bypass-the-artist-view direct-add (the original framing of #200). Routing through artist view is the deliberate choice — it gives the user pressing-disambiguation context they came to the UI for in the first place.
- Bulk paste of multiple IDs.

## Dependencies / Assumptions

- `web/routes/pipeline.py` `/api/add` already accepts both `mb_release_id` and `discogs_release_id`. **Verified** at `web/routes/pipeline.py:445-472, 509`.
- The Discogs mirror returns `master_id` and `artists` on `/api/releases/{id}`. **Verified** against the live DB (`32457180` returns `master_id: 3673686`, `artists[0].id: 194`).
- The Discogs mirror does NOT serve `artist_id=194` (no row in `artist` table; `/api/artists/194` returns 404). **Verified** against the live DB. This is what makes the VA fallback necessary on the Discogs side.
- The MB mirror's VA artist release-group endpoint is genuinely slow (23.3 s for first 100 release groups). **Verified** against the live mirror. This is what makes the VA fallback necessary on the MB side.
- The `openBrowseArtist` short-circuit at `web/js/browse.js:108` is the established convention for VA-on-MB; extending it to fire on resolution rather than just on artist-link click is consistent with that convention.

## Open Questions

None blocking. The two questions raised in earlier drafts are resolved:
- Master / release-group input is **in scope** (R3, R4, R6, R8).
- Highlight UX is **persistent ring + scroll-into-view** (R14–R18).

## File Pointers

- `web/js/browse.js` — search-type wiring (`setSearchType`, lines 71-92), VA guard at line 108, artist-view open/close.
- `web/js/discography.js:151-159` — `.release` row template; needs `data-release-id` for the ring; `<div class="releases" id="rel-${rg.id}">` is the auto-expand target.
- `web/js/main.js` — search-bar event handling.
- `web/discogs.py` — `_get`, `get_artist_releases` (cached at `discogs:artist:{artist_id}:releases`), masterless release shape (`web/discogs.py:215-228`), `get_master_releases` (`web/discogs.py:251-279`).
- `web/mb.py` — release / release-group / artist resolution.
- `web/routes/browse.py` — artist endpoints (`get_artist`, `get_discogs_artist`); add new search-by-ID resolution endpoint here.
- `web/routes/pipeline.py:445-472, 509` — existing `/api/add` accepting both ID types.
- `scripts/pipeline_cli.py:108-249` — CLI add path; reference for resolver behaviour.
