// @ts-check

/**
 * Long-tail triage worklist (Pipeline sub-view) — the list shell.
 *
 * U3: band tabs (with live counts), a search box, and the row list. Fetches
 * `GET /api/pipeline/long-tail` once (KTD2 — one server-banded fetch, all
 * tab/search filtering happens client-side over that single payload),
 * derives the band tab set from the bands present in the cohort, and
 * renders the rows for the selected band.
 *
 * The in-place per-row action console (U4/U5/U6, and the #481 item 1
 * `ConsoleState` consolidation) lives in `./long_tail_console.js` — split
 * out by #522. The two modules share a deliberate, SAFE runtime-only
 * ES-module cycle: `loadLongTail` (below) prunes `consoleStates` via
 * `consolePrune` when a row leaves the cohort, and `onLongTailSearchInput`
 * (below) restores expanded consoles via `restoreLongTailConsoles` after a
 * search repaint wipes the list DOM. Every cross-reference is inside a
 * function body, never at module top-level, so neither module's top-level
 * evaluation can observe the other mid-load — `node --check` and the mjs
 * test suite both confirm no TDZ error at import.
 *
 * Pure / DOM-free helpers (band ordering, tab derivation, in-band search
 * filtering, cross-band match count) are exported via `__test__` for the
 * Node unit suite. Rendering and fetch live alongside them but never leak
 * into the pure helpers.
 *
 * Shape mirrors `web/js/search_plan.js` / `web/js/recents.js`:
 * `// @ts-check`, ES6 module, JSDoc on exports, the
 * `export const __test__ = {…}` named-object test convention.
 */

import { state, API } from './state.js';
import {
  esc,
  sourceLabel,
} from './util.js';
import {
  consolePrune,
  consoleStates,
  restoreLongTailConsoles,
} from './long_tail_console.js';

/**
 * The `Missing` band sentinel — a `wanted` request with no clean
 * beets-library album for its `mb_release_id`. Matches the lowercase
 * `band` value U1 stamps on each row.
 *
 * @type {string}
 */
export const MISSING_BAND = 'missing';

/**
 * Canonical band ordering for the tab strip: `Missing` first, then the
 * on-disk bands ascending by `QualityRank` value. `unknown` (in-library
 * but unclassifiable) sits right after `missing` — it is a distinct
 * mechanism (present-but-rank-unknown vs. absent-from-membership) and
 * reads as "lowest known quality" for triage purposes.
 *
 * Ascending QualityRank: poor < acceptable < good < excellent <
 * transparent < lossless (LOSSLESS > TRANSPARENT > EXCELLENT > … per the
 * codec-aware rank gate, so highest-quality bands sort last).
 *
 * @type {string[]}
 */
export const BAND_ORDER = [
  'missing',
  'unknown',
  'poor',
  'acceptable',
  'good',
  'excellent',
  'transparent',
  'lossless',
];

/**
 * Human-friendly label for a band value. Capitalises the lowercase
 * `band` the API emits. Unknown / future band values fall back to the
 * raw value so a new rank tier is never silently swallowed.
 *
 * @param {string|null|undefined} band
 * @returns {string}
 */
export function bandLabel(band) {
  const b = String(band || '').toLowerCase();
  if (!b) return '?';
  return b.charAt(0).toUpperCase() + b.slice(1);
}

/**
 * Sort key for a band — its index in {@link BAND_ORDER}, or a large
 * sentinel for bands not in the canonical list (so unrecognised bands
 * sort to the end in stable, alphabetical order via the tie-break).
 *
 * @param {string} band
 * @returns {number}
 */
function bandSortKey(band) {
  const idx = BAND_ORDER.indexOf(band);
  return idx === -1 ? BAND_ORDER.length : idx;
}

/**
 * Derive the band-tab set from the fetched cohort. Pure / DOM-free.
 *
 * Returns one entry per band actually present in `rows`, ordered
 * `Missing` first then ascending by `QualityRank` value
 * ({@link BAND_ORDER}), each carrying a live `count`. Bands with zero
 * rows are omitted — the tab strip only shows bands that exist in the
 * cohort. Unrecognised band values sort to the end (alphabetically
 * among themselves) rather than being dropped.
 *
 * @param {Array<Object>} rows
 * @returns {Array<{band: string, label: string, count: number}>}
 */
export function deriveBandTabs(rows) {
  const counts = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    const band = String((row && row.band) || '').toLowerCase();
    if (!band) continue;
    counts.set(band, (counts.get(band) || 0) + 1);
  }
  const bands = [...counts.keys()];
  bands.sort((a, b) => {
    const ka = bandSortKey(a);
    const kb = bandSortKey(b);
    if (ka !== kb) return ka - kb;
    return a.localeCompare(b);
  });
  return bands.map((band) => ({
    band,
    label: bandLabel(band),
    count: counts.get(band) || 0,
  }));
}

/**
 * Pick the default band tab for a freshly-fetched cohort. Pure.
 *
 * Prefers `Missing` (the operator's usual entry point — the most-stuck
 * cohort) when present, else the first band in canonical order. Returns
 * `null` for an empty cohort (no tabs to select).
 *
 * @param {Array<{band: string}>} tabs  Output of {@link deriveBandTabs}.
 * @returns {string|null}
 */
export function defaultBand(tabs) {
  if (!Array.isArray(tabs) || tabs.length === 0) return null;
  const missing = tabs.find((t) => t.band === MISSING_BAND);
  if (missing) return missing.band;
  return tabs[0].band;
}

/**
 * Lower-cased "artist album" haystack for one row. Pure.
 *
 * @param {Object} row
 * @returns {string}
 */
function rowHaystack(row) {
  const artist = (row && row.artist_name) || '';
  const album = (row && row.album_title) || '';
  return `${artist} ${album}`.toLowerCase();
}

/**
 * Does a row match the search query (artist / album substring)? Pure.
 * Empty/whitespace query matches everything.
 *
 * @param {Object} row
 * @param {string} query
 * @returns {boolean}
 */
function rowMatchesQuery(row, query) {
  const q = String(query || '').trim().toLowerCase();
  if (!q) return true;
  return rowHaystack(row).includes(q);
}

/**
 * Filter the cohort to the rows shown in the list: the selected band,
 * narrowed by the search query (artist / album substring). Pure /
 * DOM-free. A null/empty `band` matches no rows (no tab selected); an
 * empty query keeps every in-band row.
 *
 * @param {Array<Object>} rows
 * @param {string|null} band
 * @param {string} query
 * @returns {Array<Object>}
 */
export function filterRows(rows, band, query) {
  const target = String(band || '').toLowerCase();
  if (!target) return [];
  return (Array.isArray(rows) ? rows : []).filter((row) => {
    const rowBand = String((row && row.band) || '').toLowerCase();
    if (rowBand !== target) return false;
    return rowMatchesQuery(row, query);
  });
}

/**
 * Count rows matching the search query in bands OTHER than the selected
 * one. Pure. Drives the "N matches in other bands" hint shown when the
 * in-band result is empty/sparse so the operator isn't dead-ended by a
 * search that only hits a different band. A blank query returns 0 (no
 * hint when not searching).
 *
 * @param {Array<Object>} rows
 * @param {string|null} band
 * @param {string} query
 * @returns {number}
 */
export function countOtherBandMatches(rows, band, query) {
  const q = String(query || '').trim();
  if (!q) return 0;
  const target = String(band || '').toLowerCase();
  let n = 0;
  for (const row of Array.isArray(rows) ? rows : []) {
    const rowBand = String((row && row.band) || '').toLowerCase();
    if (rowBand === target) continue;
    if (rowMatchesQuery(row, q)) n += 1;
  }
  return n;
}

// --- Rendering -------------------------------------------------------

/**
 * Render the band tab strip from the derived tabs. The selected band
 * gets `active-status`. Each tab shows its live count.
 *
 * @param {Array<{band: string, label: string, count: number}>} tabs
 * @param {string|null} selected
 * @returns {string}
 */
function renderBandTabs(tabs, selected) {
  if (!tabs.length) return '';
  const buttons = tabs.map((t) => {
    const active = t.band === selected ? ' active-status' : '';
    return `<button class="p-btn lt-band-tab${active}" type="button" onclick="window.setLongTailBand('${esc(t.band)}')">${esc(t.label)} <span class="lt-band-count">${t.count}</span></button>`;
  }).join('');
  return `<div class="lt-band-tabs">${buttons}</div>`;
}

/**
 * Render the search box. The current query is reflected as the input
 * value so a re-render preserves what the operator typed. The
 * `oninput` handler routes through `window.onLongTailSearchInput`,
 * which debounces + stamps the in-flight token.
 *
 * @param {string} query
 * @returns {string}
 */
function renderSearchBox(query) {
  return `<div class="lt-search">
    <input type="text" id="lt-search-input" class="lt-search-input"
      placeholder="Filter this band by artist or album…"
      value="${esc(query)}"
      oninput="window.onLongTailSearchInput(this.value)">
  </div>`;
}

/**
 * Render one worklist row. Clickable — the `.lt-item` click toggles the
 * (empty for now) `.lt-detail` console container via
 * `window.toggleLongTailDetail`. U4 fills the detail in.
 *
 * @param {Object} row
 * @returns {string}
 */
export function renderLongTailRow(row) {
  const id = row.id;
  const artist = row.artist_name || 'Unknown';
  const album = row.album_title || '';
  const year = row.year || '?';
  // Per-row band badge. We reuse the shared `badge-rank-*` colour classes
  // (the badges.js reuse target named in the plan) but render the band
  // *name* as the chip text — the worklist's organising axis is the band,
  // not "in library". `missing` (no library copy) gets the wanted-blue
  // chip; every on-disk band gets its rank colour. We deliberately do NOT
  // append a format/bitrate suffix here: the row carries `target_format`
  // (the request's intent), not the on-disk codec, so a suffix would
  // mislabel the actual file. The band name + colour is the honest
  // signal; the console (U4) surfaces the exact on-disk format.
  const band = String(row.band || '').toLowerCase();
  const bandCls = band === MISSING_BAND ? 'badge-wanted' : `badge-rank-${esc(band)}`;
  const bandBadge = `<span class="badge ${bandCls}">${esc(bandLabel(band))}</span>`;
  const flight = row.in_flight_rescue
    ? '<span class="badge badge-new">rescue running</span>'
    : '';
  // Meta = the pressing-disambiguation triple: year · MB/Discogs · N tracks.
  // The mirror label is derived from the release id (UUID → MusicBrainz,
  // numeric → Discogs), not the low-signal pipeline `source` column.
  const srcLabel = sourceLabel(row.mb_release_id);
  const srcChip = srcLabel
    ? `<span class="lt-meta-chip">${esc(srcLabel)}</span>` : '';
  const tc = (row.track_count != null) ? Number(row.track_count) : null;
  const tracksChip = (tc != null && tc > 0)
    ? `<span class="lt-meta-chip">${tc} track${tc === 1 ? '' : 's'}</span>`
    : '';
  return `
    <div class="lt-item" onclick="window.toggleLongTailDetail(${id})">
      <div class="p-top">
        <div>
          <div class="p-title">${esc(album)} ${bandBadge}${flight}</div>
          <div class="p-artist">${esc(artist)}</div>
        </div>
        <div class="p-row-actions"><span style="font-size:0.75em;color:#666;">#${id}</span></div>
      </div>
      <div class="p-meta">
        <span>${esc(String(year))}</span>
        ${srcChip}
        ${tracksChip}
      </div>
    </div>
    <div class="lt-detail" id="lt-detail-${id}"></div>
  `;
}

/**
 * Render the list body for the currently-selected band + query. Returns
 * one of three explicit states so the area is never blank:
 *   * empty-cohort  → no `wanted` rows at all.
 *   * empty-band    → a search filtered the selected band to zero (the
 *     tab stays visible); shows the "N matches in other bands" hint.
 *   * rows          → the matching rows.
 *
 * @param {Array<Object>} rows  Full cohort.
 * @param {string|null} band    Selected band.
 * @param {string} query        Search query.
 * @returns {string}
 */
function renderListBody(rows, band, query) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return '<div class="loading">No wanted releases in the long tail.</div>';
  }
  const shown = filterRows(rows, band, query);
  if (shown.length === 0) {
    const other = countOtherBandMatches(rows, band, query);
    const hint = other > 0
      ? `<div class="lt-empty-hint">${other} match${other === 1 ? '' : 'es'} in other bands.</div>`
      : '';
    const label = bandLabel(band);
    return `<div class="lt-empty-band">
      <div class="loading">No ${esc(label)} releases match${query ? ` “${esc(query)}”` : ''}.</div>
      ${hint}
    </div>`;
  }
  return shown.map(renderLongTailRow).join('');
}

/**
 * Render the whole long-tail sub-view from the current
 * `state.longTail`. Writes into `#pipeline-content` beneath the Pipeline
 * nav (the caller supplies the nav prefix). Pure-ish: reads state,
 * returns the inner HTML body (no nav).
 *
 * @returns {string}
 */
export function renderLongTailBody() {
  const lt = state.longTail;
  const rows = Array.isArray(lt.rows) ? lt.rows : [];
  const tabs = deriveBandTabs(rows);
  // Display band: fall back to the default when the selected band is absent
  // (e.g. a refetch dropped it). Pure — does NOT write state.longTail.band;
  // the canonical default is set by loadLongTail / setLongTailBand.
  let band = lt.band;
  if (band == null || !tabs.some((t) => t.band === band)) {
    band = defaultBand(tabs);
  }
  const tabStrip = renderBandTabs(tabs, band);
  const searchBox = tabs.length ? renderSearchBox(lt.query) : '';
  const body = renderListBody(rows, band, lt.query);
  return `<div class="lt-worklist">
    ${tabStrip}
    ${searchBox}
    <div class="lt-list" id="lt-list">${body}</div>
  </div>`;
}

// --- Fetch + dispatch -----------------------------------------------

/**
 * Module-scoped in-flight token for the worklist fetch. Bumped on every
 * `loadLongTail` call; a fetch whose token is stale when it resolves is
 * discarded before it renders (browse.js stale-token pattern). Shared
 * with the search debounce so a search-triggered re-render and a fresh
 * fetch can't clobber each other.
 *
 * @type {number}
 */
let longTailRequestToken = 0;

/**
 * Debounce timer handle for the search box. Cleared on each keystroke.
 *
 * @type {number|null}
 */
let longTailSearchTimer = null;

/**
 * Debounce window (ms) for the search box, matching the browse-tab
 * search debounce.
 *
 * @type {number}
 */
const LONG_TAIL_SEARCH_DEBOUNCE_MS = 300;

/**
 * Fetch the banded `wanted` cohort and render it. Stamps an in-flight
 * token so a slow response superseded by a newer load is discarded
 * before it paints.
 *
 * @returns {Promise<void>}
 */
export async function loadLongTail() {
  const token = ++longTailRequestToken;
  const el = (typeof document !== 'undefined')
    ? document.getElementById('pipeline-content')
    : null;
  if (el) {
    // Transient loading affordance — distinct from the empty-cohort
    // state. We paint the body only (no nav): pipeline.js owns
    // `renderPipelineNav` and the success path routes back through
    // `renderPipeline` which re-emits it, so there is never a navless
    // *permanent* state. Duplicating the nav here would be a parallel
    // code path.
    el.innerHTML = '<div class="loading">Loading long tail…</div>';
  }
  try {
    const r = await fetch(`${API}/api/pipeline/long-tail`);
    if (token !== longTailRequestToken) return;
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    if (token !== longTailRequestToken) return;
    const rows = Array.isArray(data.results) ? data.results : [];
    state.longTail.rows = rows;
    // Prune console state for rows that left the wanted cohort — imported
    // out-of-band, replaced, deleted. One map, one prune function (#481
    // item 1) — the fresh-cohort intersect.
    const cohortIds = new Set(rows.map((r) => r && r.id));
    consolePrune(consoleStates, (id) => cohortIds.has(id));
    const tabs = deriveBandTabs(rows);
    // Pick a default band when none selected or the prior selection is
    // gone from the new cohort.
    if (state.longTail.band == null
        || !tabs.some((t) => t.band === state.longTail.band)) {
      state.longTail.band = defaultBand(tabs);
    }
    renderLongTail();
  } catch (e) {
    if (token !== longTailRequestToken) return;
    if (el) {
      el.innerHTML = '<div class="loading">Failed to load long tail.</div>';
    }
  }
}

/**
 * Re-render the long-tail sub-view from cached state without refetching.
 * Delegates to pipeline.js's render dispatcher so the Pipeline nav is
 * emitted consistently with the other sub-views. Bound at call time to
 * avoid a static circular import.
 *
 * @returns {void}
 */
export function renderLongTail() {
  if (typeof window === 'undefined') return;
  const fn = /** @type {undefined | (() => void)} */ (
    /** @type {any} */ (window).renderPipeline);
  if (typeof fn === 'function') fn();
}

/**
 * Select a band tab. Mutates `state.longTail.band` and re-renders. The
 * search query is preserved across band switches (the operator may want
 * the same filter applied to a different band; the "N in other bands"
 * hint already nudges them here).
 *
 * @param {string} band
 * @returns {void}
 */
export function setLongTailBand(band) {
  state.longTail.band = String(band || '').toLowerCase() || null;
  renderLongTail();
}

/**
 * Search-box input handler. Debounced + stamped with the in-flight token
 * so a stale debounced render (from a superseded keystroke or a fetch
 * that has since started) is discarded before it paints. Re-renders only
 * the list body in place so the input keeps focus + caret position.
 *
 * @param {string} value
 * @returns {void}
 */
export function onLongTailSearchInput(value) {
  state.longTail.query = String(value == null ? '' : value);
  const token = longTailRequestToken;
  if (longTailSearchTimer != null && typeof clearTimeout === 'function') {
    clearTimeout(longTailSearchTimer);
  }
  const run = () => {
    // Stale-guard: a fresh fetch (loadLongTail) bumps the token; if it
    // has moved on, that fetch's render will repaint — skip this one.
    if (token !== longTailRequestToken) return;
    const listEl = (typeof document !== 'undefined')
      ? document.getElementById('lt-list')
      : null;
    if (!listEl) return;
    const rows = Array.isArray(state.longTail.rows) ? state.longTail.rows : [];
    // Re-render only the list body; the tabs + input stay put so the
    // search box keeps focus and the caret position.
    listEl.innerHTML = renderListBody(rows, state.longTail.band, state.longTail.query);
    // The body wipe destroyed any expanded console DOM — restore (#398).
    restoreLongTailConsoles();
  };
  if (typeof setTimeout === 'function') {
    longTailSearchTimer = /** @type {any} */ (
      setTimeout(run, LONG_TAIL_SEARCH_DEBOUNCE_MS));
  } else {
    run();
  }
}


export const __test__ = {
  MISSING_BAND,
  BAND_ORDER,
  bandLabel,
  deriveBandTabs,
  defaultBand,
  filterRows,
  countOtherBandMatches,
  renderLongTailRow,
  renderLongTailBody,
};
