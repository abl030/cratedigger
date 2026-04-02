# Phase 4: Combined Browse Tab

## Goal
Merge Search, Disambiguate, and Library tabs into one "Browse" tab with unified search and per-artist sub-views.

## Layout

```
[Search: "Search artists or albums..."]  [Artist | Album toggle]
[Results list]

--- On artist click: ---
[Artist Name]
[Sub-nav: Discography | Analysis | Library]
[Sub-view content]
```

## Sub-views (swap without re-fetching — data cached in Redis)

1. **Discography** — existing `loadArtist()` logic (release groups by type, pressings, add button)
2. **Analysis** — existing disambiguate rendering (coverage tiers, unique tracks)
3. **Library** — existing library artist rendering (quality info, tracks, upgrade/delete)

## Album search flow

1. User toggles to "Album" mode, types "all hail west texas"
2. `GET /api/search?q=...&type=release` → release groups with artist info
3. Click result → loads combined artist view, could auto-expand matching release group

## JS architecture

- `currentArtist = { id, name, disambiguation }` — set when user clicks an artist
- `browseSubView = 'discography' | 'analysis' | 'library'` — sub-nav state
- `switchSubView(view)` — swaps visible content, lazy-fetches sub-view data if not loaded yet
- Small in-page JS object caches fetched data per artist (avoids flicker on sub-nav switches within same page load — Redis handles cross-device)

## Tabs after merge

Remove: Search, Disambiguate, Library (3 tabs)
Add: Browse (1 tab)
Keep: Recents, Pipeline, Decisions, Manual

## Implementation notes

- Keep existing rendering functions mostly intact — `loadArtist()`, disambiguate rendering, library artist rendering become the three sub-view renderers
- Re-parent them into the Browse tab's sub-view container
- Single search input with debounce, type toggle changes which API param is sent
