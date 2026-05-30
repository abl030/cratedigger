// @ts-check

/**
 * Long-tail triage worklist (Pipeline sub-view).
 *
 * U3: the list shell — band tabs (with live counts), a search box, and
 * the row list. Fetches `GET /api/pipeline/long-tail` once (KTD2 — one
 * server-banded fetch, all tab/search filtering happens client-side over
 * that single payload), derives the band tab set from the bands present
 * in the cohort, and renders the rows for the selected band.
 *
 * U4 (this unit): the in-place action console. Selecting a row expands
 * its `.lt-detail` container into a band-aware evidence console with five
 * panels, each loading INDEPENDENTLY (per-panel loading + error states)
 * so a slow / failing `GET /api/release-group/<rg>` (the 15s MB-mirror
 * timeout) never blanks the console or silently drops another panel:
 *
 *   1. Why-unfindable  ← `GET /api/triage/<id>` (unfindable category +
 *      reason + search-forensics rollup; "not yet categorised" rendered
 *      distinctly from an error).
 *   2. Soulseek peers  ← `GET /api/pipeline/<id>` `last_search.top_candidates`
 *      (reuses `renderForensicBlock`; a few rows with a "show all" toggle).
 *   3. Recent rescues  ← `GET /api/pipeline/<id>` `history` rows where
 *      `source==='youtube'` ("rescue running" / "last rescue failed: …").
 *   4. Sibling pressings ← `GET /api/release-group/<rg>` (rg taken from the
 *      pipeline-detail `request.mb_release_group_id`; skipped for rows
 *      without one, e.g. Discogs-sourced).
 *   5. YouTube matrix  ← the four-state shell. Defaults to `never_run`
 *      with a "Check YouTube" button (U5 wires the actual resolver call —
 *      U4 must NOT auto-call the slow, side-effectful resolver GET).
 *
 * U5 (this unit): the two-step YouTube rescue flow.
 *
 *   1. "Check YouTube" (`checkYoutube`) — replaces U4's placeholder. Calls
 *      the SLOW, SIDE-EFFECTFUL resolver GET
 *      (`GET /api/youtube-album?identifier=<mb_release_id>`), disables the
 *      button + shows in-progress, GUARDS double-fire (a module-scoped
 *      `resolveInFlight` Set keyed by request id — a second click while
 *      outstanding fires nothing), and STAMPS the fetch with a per-row
 *      console token so a stale result (operator collapsed the console or
 *      moved to another row) never paints. On return the YouTube panel is
 *      re-rendered via `youtubeSectionState`: `resolved_with_matrix` →
 *      pickable rescue targets; `resolved_empty` → "not on YouTube Music —
 *      re-check" (rescue affordance HIDDEN); `resolver_failed` → error +
 *      Retry; cached-but-stale → matrix with a "cached" note.
 *   2. Pick + confirm + submit — clicking a matrix target's "Rescue from
 *      this" (`pickYoutubeRescue`) opens a Promise-based confirm overlay
 *      (mirrors `web/js/replace_picker.js`'s `.confirm-overlay` shell +
 *      backdrop-click-cancel). On confirm →
 *      `POST /api/pipeline/<id>/youtube-rescue {browse_id}`. Every ingest
 *      outcome maps to specific console copy (`rescueOutcomeCopy`). On
 *      `accepted` the row is marked in-flight and a SINGLE-ROW refetch
 *      (`GET /api/pipeline/long-tail?id=<id>`) patches just that cohort
 *      row (KTD8 — never optimistically move the row to a band; the
 *      importer owns the transition, KTD4).
 *
 * accept-sibling / set-intent / re-search remain U6.
 *
 * Pure / DOM-free helpers (band ordering, tab derivation, in-band search
 * filtering, cross-band match count, YouTube state classifier, console
 * emphasis selector) are exported via `__test__` for the Node unit suite.
 * The classifier + emphasis selector live in `util.js` (the shared pure
 * home) and are re-exported through `__test__` here for convenience.
 * Rendering and fetch live alongside them but never leak into the pure
 * helpers.
 *
 * Shape mirrors `web/js/search_plan.js` / `web/js/recents.js`:
 * `// @ts-check`, ES6 module, JSDoc on exports, the
 * `export const __test__ = {…}` named-object test convention.
 */

import { state, API, toast } from './state.js';
import {
  esc,
  jsArg,
  renderForensicBlock,
  youtubeSectionState,
  consoleEmphasis,
  awstDateTime,
} from './util.js';

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
  const unfindable = row.unfindable_category
    ? `<span class="lt-meta-chip" title="unfindable category">${esc(row.unfindable_category)}</span>`
    : '';
  const src = row.source ? `<span class="lt-meta-chip">${esc(row.source)}</span>` : '';
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
        ${src}
        ${unfindable}
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
  // If the selected band is no longer present (e.g. after a refetch
  // dropped it), fall back to the default.
  let band = lt.band;
  if (band == null || !tabs.some((t) => t.band === band)) {
    band = defaultBand(tabs);
    lt.band = band;
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
  };
  if (typeof setTimeout === 'function') {
    longTailSearchTimer = /** @type {any} */ (
      setTimeout(run, LONG_TAIL_SEARCH_DEBOUNCE_MS));
  } else {
    run();
  }
}

// --- Action console (U4) --------------------------------------------
//
// Five evidence panels rendered in place, each loading independently. The
// pure render helpers below take already-fetched data (or an error
// sentinel) and return an HTML string; the DOM-side loaders fetch each
// source and patch only that panel's container. A per-row token guard
// discards a console fetch that resolves after the operator has clicked a
// different row (or re-collapsed this one).

/**
 * Per-row console-fetch token. Bumped every time a row's console is
 * (re)opened so a slow panel fetch that resolves against a stale console
 * is discarded before it paints. Keyed by `album_requests.id`.
 *
 * @type {Map<number, number>}
 */
const consoleTokens = new Map();

/**
 * Visible-row cap for the Soulseek peers panel before the "show all"
 * expansion. The peers list shows a few rows so the action buttons (U5/U6)
 * stay reachable without scrolling past a long candidate table.
 *
 * @type {number}
 */
const PEERS_VISIBLE_CAP = 5;

/**
 * Format an ISO timestamp for the console, defensively. Falls back to the
 * raw string when it can't be parsed (never throws into a render).
 *
 * @param {string|null|undefined} iso
 * @returns {string}
 */
function consoleTimestamp(iso) {
  if (!iso) return '';
  try {
    return awstDateTime(String(iso));
  } catch (_e) {
    return String(iso);
  }
}

/**
 * A small panel scaffold: a titled section with a body. Used by every
 * panel so the console reads consistently. Pure.
 *
 * @param {string} name   Stable slug for the panel container id.
 * @param {number} id     album_requests.id (namespaces the container).
 * @param {string} title  Panel heading.
 * @param {string} body   Inner HTML (already escaped where needed).
 * @param {boolean} [lead]  When true, flag the panel as the console's
 *   lead panel (band-aware emphasis).
 * @returns {string}
 */
function renderPanel(name, id, title, body, lead) {
  const leadCls = lead ? ' lt-panel-lead' : '';
  return `<div class="lt-panel lt-panel-${esc(name)}${leadCls}" id="lt-panel-${esc(name)}-${id}">
    <div class="lt-panel-title">${esc(title)}</div>
    <div class="lt-panel-body">${body}</div>
  </div>`;
}

/**
 * Per-panel loading affordance (distinct from empty/error). Pure.
 *
 * @param {string} label
 * @returns {string}
 */
function renderPanelLoading(label) {
  return `<div class="lt-panel-loading">Loading ${esc(label)}…</div>`;
}

/**
 * Per-panel error affordance — the independent-load contract: one panel's
 * fetch failing renders THIS, never blanking the console or dropping
 * another panel. Pure.
 *
 * @param {string} label
 * @returns {string}
 */
function renderPanelError(label) {
  return `<div class="lt-panel-error">Couldn't load ${esc(label)}. <span class="lt-panel-error-hint">(other panels are unaffected)</span></div>`;
}

/**
 * Render the why-unfindable panel body from a triage payload. Pure.
 *
 * Three states:
 *   * `triage == null` → caller passed nothing yet (defensive; the loader
 *     uses the loading/error affordances instead).
 *   * `triage.unfindable == null` → NOT yet categorised. Rendered as an
 *     explicit "detection runs daily" state — not an error, not blank
 *     (R7).
 *   * categorised → the category + a search-forensics rollup.
 *
 * @param {Object|null} triage  The `TriageResult` payload.
 * @returns {string}
 */
function renderUnfindableBody(triage) {
  if (!triage || typeof triage !== 'object') {
    return '<div class="lt-panel-empty">No triage data.</div>';
  }
  const unfindable = triage.unfindable;
  const sf = triage.search_forensics || {};
  const rollupParts = [];
  if (sf.total_searches != null) rollupParts.push(`${sf.total_searches} searches`);
  if (sf.with_cands_count != null) rollupParts.push(`${sf.with_cands_count} with candidates`);
  if (sf.zero_results_count != null) rollupParts.push(`${sf.zero_results_count} zero-result`);
  if (sf.dominant_rejection_reason) rollupParts.push(`dominant reject: ${sf.dominant_rejection_reason}`);
  const lastAt = sf.last_search_at ? consoleTimestamp(sf.last_search_at) : '';
  const rollup = rollupParts.length
    ? `<div class="lt-rollup">${esc(rollupParts.join(' · '))}${lastAt ? ` · last ${esc(lastAt)}` : ''}</div>`
    : '';
  if (!unfindable || unfindable.category == null) {
    // Not-yet-categorised — daily detection state, distinct from an error.
    return `<div class="lt-uncategorised">
      <span class="lt-meta-chip">not yet categorised</span>
      <span class="lt-uncategorised-note">detection runs daily</span>
    </div>${rollup}`;
  }
  const catAt = unfindable.categorised_at ? consoleTimestamp(unfindable.categorised_at) : '';
  const probe = (unfindable.last_artist_probe_match_count != null)
    ? `<div class="lt-rollup">artist probe: ${unfindable.last_artist_probe_match_count} match${unfindable.last_artist_probe_match_count === 1 ? '' : 'es'}${unfindable.last_artist_probe_at ? ` · ${esc(consoleTimestamp(unfindable.last_artist_probe_at))}` : ''}</div>`
    : '';
  return `<div class="lt-unfindable-cat">
      <span class="badge badge-manual">${esc(String(unfindable.category))}</span>
      ${catAt ? `<span class="lt-meta-chip">categorised ${esc(catAt)}</span>` : ''}
    </div>${rollup}${probe}`;
}

/**
 * Pluck the YouTube rescue rows out of a pipeline-detail history list.
 * Pure. Returns the `source==='youtube'` rows newest-first (the history
 * already arrives newest-first; we preserve order).
 *
 * @param {Array<Object>|null|undefined} history
 * @returns {Array<Object>}
 */
function youtubeHistoryRows(history) {
  return (Array.isArray(history) ? history : [])
    .filter((h) => h && h.source === 'youtube');
}

/**
 * Extract the classified failure reason for a terminal `youtube_failed`
 * download-history row. Pure. The reason is persisted in the
 * `youtube_metadata` JSONB blob (`reason`) by the ingest worker's terminal
 * write — see `lib/youtube_ingest_service.py::YoutubeIngestMetadata.reason`.
 * Falls back to the row's `error_message` then `verdict` (the shared
 * download-history-view fields) and finally a generic sentinel so the
 * panel never shows an empty reason.
 *
 * @param {Object} row  A `source==='youtube'` download-history row.
 * @returns {string}
 */
function youtubeFailureReason(row) {
  const meta = row && row.youtube_metadata;
  if (meta && typeof meta === 'object' && meta.reason) {
    return String(meta.reason);
  }
  if (row && row.error_message) return String(row.error_message);
  if (row && row.verdict) return String(row.verdict);
  return 'unknown';
}

/**
 * Render the "recent rescue attempts" panel body. Pure.
 *
 * Reads the `source==='youtube'` history rows (KTD4 — rescues never move
 * the request row; their state lives in `download_log`). Surfaces:
 *   * "rescue running" when an active `youtube_running` row exists, OR the
 *     worklist row carried `in_flight_rescue` (the same predicate).
 *   * "last rescue failed: <reason>" when the latest terminal youtube row
 *     is `youtube_failed` SPECIFICALLY (distinct from `youtube_success`).
 *   * the recent attempts list otherwise.
 *
 * @param {Array<Object>|null|undefined} history  Pipeline-detail history.
 * @param {boolean} inFlightFlag  The worklist row's `in_flight_rescue`.
 * @returns {string}
 */
function renderRescuesBody(history, inFlightFlag) {
  const rows = youtubeHistoryRows(history);
  const running = rows.find((h) => h.outcome === 'youtube_running');
  if (running || inFlightFlag) {
    return `<div class="lt-rescue-status"><span class="badge badge-new">rescue running</span>${running && running.created_at ? `<span class="lt-meta-chip">since ${esc(consoleTimestamp(running.created_at))}</span>` : ''}</div>`;
  }
  // The latest TERMINAL youtube row (youtube_failed / youtube_success).
  const terminal = rows.find(
    (h) => h.outcome === 'youtube_failed' || h.outcome === 'youtube_success');
  if (terminal && terminal.outcome === 'youtube_failed') {
    const reason = youtubeFailureReason(terminal);
    return `<div class="lt-rescue-status"><span class="badge badge-manual">last rescue failed</span> <span class="lt-rescue-reason">${esc(reason)}</span></div>`;
  }
  if (rows.length === 0) {
    return '<div class="lt-panel-empty">No rescue attempts yet.</div>';
  }
  // Some succeeded / mixed — list the recent attempts.
  const items = rows.slice(0, 5).map((h) => {
    const when = h.created_at ? consoleTimestamp(h.created_at) : '';
    return `<div class="lt-rescue-item">${esc(String(h.outcome || '?'))}${when ? ` · ${esc(when)}` : ''}</div>`;
  }).join('');
  return `<div class="lt-rescue-list">${items}</div>`;
}

/**
 * Render the Soulseek peers panel body. Reuses `renderForensicBlock` for
 * the candidates table; caps the visible rows with a "show all" toggle so
 * the action buttons stay reachable (IA per the plan). Pure.
 *
 * @param {Object|null|undefined} lastSearch  `last_search` payload.
 * @param {number} id  album_requests.id (namespaces the show-all toggle).
 * @returns {string}
 */
function renderPeersBody(lastSearch, id) {
  if (!lastSearch) {
    return renderForensicBlock(null);
  }
  const cands = Array.isArray(lastSearch.top_candidates)
    ? lastSearch.top_candidates : [];
  if (cands.length <= PEERS_VISIBLE_CAP) {
    return renderForensicBlock(/** @type {any} */ (lastSearch));
  }
  // Cap the visible candidates; offer a "show all" toggle that swaps in
  // the full block. The capped + full blocks are both pre-rendered; the
  // toggle flips which is shown without a refetch.
  const capped = { ...lastSearch, top_candidates: cands.slice(0, PEERS_VISIBLE_CAP) };
  const cappedHtml = renderForensicBlock(/** @type {any} */ (capped));
  const fullHtml = renderForensicBlock(/** @type {any} */ (lastSearch));
  return `<div class="lt-peers" id="lt-peers-${id}">
    <div class="lt-peers-capped">${cappedHtml}
      <button class="lt-link-btn" type="button" onclick="event.stopPropagation(); window.toggleLongTailPeers(${id})">show all ${cands.length} peers</button>
    </div>
    <div class="lt-peers-full" style="display:none;">${fullHtml}
      <button class="lt-link-btn" type="button" onclick="event.stopPropagation(); window.toggleLongTailPeers(${id})">show fewer</button>
    </div>
  </div>`;
}

/**
 * Toggle the capped ⇄ full Soulseek peers view in place. No refetch — both
 * blocks are already rendered; this flips visibility.
 *
 * @param {number} id  album_requests.id
 * @returns {void}
 */
export function toggleLongTailPeers(id) {
  if (typeof document === 'undefined') return;
  const wrap = document.getElementById(`lt-peers-${id}`);
  if (!wrap) return;
  const capped = /** @type {HTMLElement|null} */ (wrap.querySelector('.lt-peers-capped'));
  const full = /** @type {HTMLElement|null} */ (wrap.querySelector('.lt-peers-full'));
  if (!capped || !full) return;
  const showFull = full.style.display === 'none';
  full.style.display = showFull ? 'block' : 'none';
  capped.style.display = showFull ? 'none' : 'block';
}

/**
 * Render one sibling-pressing row for the siblings panel. Pure.
 *
 * @param {Object} rel  A release row from `/api/release-group/<rg>`.
 * @returns {string}
 */
function renderSiblingRow(rel) {
  const title = rel.title || '?';
  const meta = [rel.country, (rel.date || '').slice(0, 4), rel.format,
    (rel.track_count != null ? `${rel.track_count}t` : '')]
    .filter((x) => x).join(' · ');
  const inLib = rel.in_library
    ? `<span class="badge badge-rank-${esc(String(rel.library_rank || 'library').toLowerCase())}">in library</span>`
    : '';
  const pStatus = rel.pipeline_status
    ? `<span class="badge badge-${esc(String(rel.pipeline_status))}">${esc(String(rel.pipeline_status))}</span>`
    : '';
  return `<div class="lt-sibling">
    <span class="lt-sibling-title">${esc(String(title))}</span>
    <span class="lt-sibling-meta">${esc(meta)}</span>
    ${inLib}${pStatus}
  </div>`;
}

/**
 * Render the sibling-pressings panel body from a release-group payload.
 * Pure. Evidence-only in U4 — the accept-sibling action is U6.
 *
 * @param {Object|null|undefined} rgData  `{releases:[...]}`.
 * @returns {string}
 */
function renderSiblingsBody(rgData) {
  const releases = (rgData && Array.isArray(rgData.releases)) ? rgData.releases : [];
  if (releases.length === 0) {
    return '<div class="lt-panel-empty">No sibling pressings found.</div>';
  }
  return releases.map(renderSiblingRow).join('');
}

/**
 * @typedef {Object} YoutubeRescueTarget
 * @property {string} yt_browse_id  The YT Music album browseId — the value
 *   the rescue submit (`POST .../youtube-rescue`) takes as `browse_id`.
 * @property {number|null} year
 * @property {number|null} track_count
 * @property {number|null} best_distance  Lowest `ok` beets distance across
 *   the release's `distances[]`, or `null` when none scored.
 */

/**
 * Lowest `ok` beets distance across a YT release's `distances[]`. Pure /
 * DOM-free. Returns `null` when no distance row scored `ok` (so callers can
 * render "no distance" without a special-case branch).
 *
 * @param {{distances?: Array<{outcome?: string, distance?: number}>|null}} rel
 * @returns {number|null}
 */
export function youtubeBestDistance(rel) {
  const dists = (rel && Array.isArray(rel.distances)) ? rel.distances : [];
  let best = /** @type {number|null} */ (null);
  for (const d of dists) {
    if (!d || d.outcome !== 'ok' || typeof d.distance !== 'number') continue;
    if (best == null || d.distance < best) best = d.distance;
  }
  return best;
}

/**
 * Extract the pickable rescue targets from a resolver result. Pure /
 * DOM-free. Each target carries its `yt_browse_id` (the value the rescue
 * submit needs) plus display meta (year / track_count / best distance).
 *
 * Only `resolved_with_matrix` yields targets — `resolved_empty` (ok with
 * zero releases), `resolver_failed`, and `never_run` (null) all yield an
 * empty list, so the rescue affordance is hidden (R9 / R10: the operator
 * picks a target, the system never auto-picks; "not on YouTube Music"
 * offers nothing to pick). A release missing a `yt_browse_id` is dropped —
 * it cannot be a rescue target without the id the submit requires.
 *
 * @param {{outcome?: string, youtube_releases?: Array<Object>|null, from_cache?: boolean, error_message?: string|null}|null|undefined} result
 * @returns {YoutubeRescueTarget[]}
 */
export function youtubeRescueTargets(result) {
  const cls = youtubeSectionState(result);
  if (cls.state !== 'resolved_with_matrix') return [];
  const releases = (result && Array.isArray(result.youtube_releases))
    ? result.youtube_releases : [];
  /** @type {YoutubeRescueTarget[]} */
  const targets = [];
  for (const rel of releases) {
    const browseId = rel && rel.yt_browse_id ? String(rel.yt_browse_id) : '';
    if (!browseId) continue;  // no id → not a pickable target.
    targets.push({
      yt_browse_id: browseId,
      year: (rel.year != null) ? Number(rel.year) : null,
      track_count: (rel.track_count != null) ? Number(rel.track_count) : null,
      best_distance: youtubeBestDistance(rel),
    });
  }
  return targets;
}

/**
 * Render the YouTube panel body for a given section state + payload. Pure.
 *
 * Renders all four states. In `resolved_with_matrix` each release is a
 * PICKABLE rescue target (U5): a "Rescue from this" button carrying the
 * release's `yt_browse_id` opens the confirm step
 * (`window.pickYoutubeRescue(id, browse_id)`). `resolved_empty` HIDES the
 * rescue affordance and shows "not on YouTube Music — re-check"
 * (R9 / R10 — nothing to pick). `never_run` / `resolver_failed` render the
 * "Check YouTube" / retry button wired to `window.checkYoutube(id)` (U5's
 * real resolver handler, replacing U4's placeholder). The console must NOT
 * auto-call the slow, side-effectful resolver GET — the panel opens in
 * `never_run` until the operator clicks.
 *
 * @param {{outcome?: string, youtube_releases?: Array<Object>|null, from_cache?: boolean, error_message?: string|null}|null} result
 *   A cached resolver result, or `null` for the default never-run state.
 * @param {number} id  album_requests.id
 * @returns {string}
 */
function renderYoutubeBody(result, id) {
  const cls = youtubeSectionState(result);
  const checkLabel = (cls.state === 'resolver_failed') ? 'Retry'
    : (cls.state === 'resolved_empty') ? 'Re-check'
    : 'Check YouTube';
  const checkBtn = `<button class="lt-yt-check" type="button" onclick="event.stopPropagation(); window.checkYoutube(${id})">${checkLabel}</button>`;
  if (cls.state === 'never_run') {
    return `<div class="lt-yt lt-yt-never-run">
      <div class="lt-yt-prompt">Resolve this release against YouTube Music.</div>
      ${checkBtn}
    </div>`;
  }
  if (cls.state === 'resolver_failed') {
    return `<div class="lt-yt lt-yt-failed">
      <div class="lt-yt-msg">${esc(cls.message)}</div>
      ${checkBtn}
    </div>`;
  }
  if (cls.state === 'resolved_empty') {
    // Not on YouTube Music — HIDE the rescue affordance (nothing to pick),
    // offer only a re-check.
    return `<div class="lt-yt lt-yt-empty">
      <div class="lt-yt-msg">${esc(cls.message)}</div>
      ${checkBtn}
    </div>`;
  }
  // resolved_with_matrix — each release is a pickable rescue target (U5).
  const staleFlag = cls.stale
    ? `<div class="lt-yt-stale">${esc(cls.message)}</div>`
    : '';
  const targets = youtubeRescueTargets(result);
  const rows = targets.map((t) => {
    const bestStr = (t.best_distance != null)
      ? `dist ${t.best_distance.toFixed(3)}` : 'no distance';
    const tc = (t.track_count != null) ? `${t.track_count}t` : '';
    const yr = (t.year != null) ? String(t.year) : '';
    const meta = [yr, tc, bestStr].filter((x) => x).join(' · ');
    // The browse id is embedded via jsArg so a confirm picks the exact
    // target the operator clicked (R10 — never auto-picked).
    return `<div class="lt-yt-row">
      <span class="lt-yt-id">${esc(t.yt_browse_id)}</span>
      <span class="lt-yt-meta">${esc(meta)}</span>
      <button class="lt-yt-rescue" type="button" onclick="event.stopPropagation(); window.pickYoutubeRescue(${id}, ${jsArg(t.yt_browse_id)})">Rescue from this</button>
    </div>`;
  }).join('');
  return `<div class="lt-yt lt-yt-matrix">
    ${staleFlag}
    <div class="lt-yt-rows">${rows}</div>
    ${checkBtn}
  </div>`;
}

/**
 * Render the full console shell synchronously on open. Each panel starts
 * in its loading state; the independent loaders patch them as their
 * fetches settle. Band-aware emphasis (`consoleEmphasis`) decides which
 * panel leads: `Missing` / unfindable rows lead with why-unfindable;
 * on-disk rows lead with the band-vs-intent header. Pure.
 *
 * @param {Object} row  The worklist row (carries band, source, intent).
 * @returns {string}
 */
function renderConsoleShell(row) {
  const id = row.id;
  const emphasis = consoleEmphasis(row);
  const leadUnfindable = emphasis.lead === 'unfindable';
  // Band-vs-intent header for on-disk rows (R8). `band` is the on-disk
  // band; `target_format` is the request's intent. The console surfaces
  // the comparison; the exact on-disk codec is in the peers/quality data.
  const band = String(row.band || '').toLowerCase();
  const intent = row.target_format || (row.min_bitrate ? `${row.min_bitrate}k` : 'default');
  const bandVsIntent = !leadUnfindable
    ? `<div class="lt-band-intent lt-panel-lead">
        <div class="lt-panel-title">Quality vs intent</div>
        <div class="lt-panel-body">on disk: <strong>${esc(bandLabel(band))}</strong> · intent: <strong>${esc(String(intent))}</strong></div>
      </div>`
    : '';
  const unfindablePanel = renderPanel(
    'unfindable', id, 'Why unfindable',
    renderPanelLoading('triage'), leadUnfindable);
  const peersPanel = renderPanel(
    'peers', id, 'Soulseek peers seen', renderPanelLoading('peers'), false);
  const rescuesPanel = renderPanel(
    'rescues', id, 'Recent rescue attempts', renderPanelLoading('rescues'), false);
  const siblingsPanel = renderPanel(
    'siblings', id, 'Sibling pressings', renderPanelLoading('siblings'), false);
  // The YouTube panel opens in `never_run` immediately — no fetch (U4 must
  // not auto-call the side-effectful resolver GET).
  const youtubePanel = renderPanel(
    'youtube', id, 'YouTube Music', renderYoutubeBody(null, id), false);
  // Order: lead panel first. For Missing/unfindable, why-unfindable leads;
  // for on-disk rows, the band-vs-intent header leads, then why-unfindable.
  const ordered = leadUnfindable
    ? [unfindablePanel, peersPanel, rescuesPanel, siblingsPanel, youtubePanel]
    : [bandVsIntent, unfindablePanel, peersPanel, rescuesPanel, siblingsPanel, youtubePanel];
  return `<div class="lt-console">${ordered.join('')}</div>`;
}

/**
 * Patch one panel's body in place, guarded by the row's console token so a
 * stale fetch (operator moved on) doesn't paint. DOM-side.
 *
 * @param {number} id     album_requests.id
 * @param {string} name   Panel slug.
 * @param {number} token  The token captured when the console opened.
 * @param {string} html   New body HTML.
 * @returns {void}
 */
function patchPanel(id, name, token, html) {
  if (typeof document === 'undefined') return;
  if (consoleTokens.get(id) !== token) return;  // stale console — discard.
  const panel = document.getElementById(`lt-panel-${name}-${id}`);
  if (!panel) return;
  const body = panel.querySelector('.lt-panel-body');
  if (body) body.innerHTML = html;
}

/**
 * Load the why-unfindable panel from `GET /api/triage/<id>`. Independent —
 * a failure renders only this panel's error affordance.
 *
 * @param {number} id
 * @param {number} token
 * @returns {Promise<void>}
 */
async function loadUnfindablePanel(id, token) {
  try {
    const r = await fetch(`${API}/api/triage/${id}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    patchPanel(id, 'unfindable', token, renderUnfindableBody(data));
  } catch (_e) {
    patchPanel(id, 'unfindable', token, renderPanelError('triage'));
  }
}

/**
 * Load the peers + rescues panels from one `GET /api/pipeline/<id>` fetch,
 * and kick off the (independent) sibling-pressings fetch using the
 * release-group id off the detail payload. Peers + rescues are patched
 * from this fetch; siblings is fired separately so a slow release-group
 * (the 15s MB-mirror timeout) blocks only its own panel.
 *
 * @param {number} id
 * @param {number} token
 * @param {boolean} inFlightFlag  The worklist row's `in_flight_rescue`.
 * @returns {Promise<void>}
 */
async function loadPipelinePanels(id, token, inFlightFlag) {
  let rgId = null;
  try {
    const r = await fetch(`${API}/api/pipeline/${id}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    patchPanel(id, 'peers', token, renderPeersBody(data.last_search, id));
    patchPanel(id, 'rescues', token, renderRescuesBody(data.history, inFlightFlag));
    const req = data.request || {};
    rgId = req.mb_release_group_id || null;
  } catch (_e) {
    patchPanel(id, 'peers', token, renderPanelError('peers'));
    patchPanel(id, 'rescues', token, renderPanelError('rescue history'));
  }
  // Siblings is dependent on the rg id but loads independently: if the
  // detail fetch failed (rgId null) we render the "no rg" state; otherwise
  // we fire the release-group fetch on its own so its latency is isolated.
  loadSiblingsPanel(id, token, rgId);
}

/**
 * Load the sibling-pressings panel from `GET /api/release-group/<rg>`.
 * Independent — a slow / failing release-group renders only this panel's
 * error affordance and never blocks the others. Rows without a release
 * group (Discogs-sourced, or a legacy MB row with none) render an explicit
 * "no sibling data" state rather than firing a doomed fetch (KTD7).
 *
 * @param {number} id
 * @param {number} token
 * @param {string|null} rgId  The request's `mb_release_group_id`, or null.
 * @returns {Promise<void>}
 */
async function loadSiblingsPanel(id, token, rgId) {
  if (!rgId) {
    patchPanel(id, 'siblings', token,
      '<div class="lt-panel-empty">No release group — sibling pressings unavailable for this request.</div>');
    return;
  }
  try {
    const r = await fetch(`${API}/api/release-group/${encodeURIComponent(rgId)}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    patchPanel(id, 'siblings', token, renderSiblingsBody(data));
  } catch (_e) {
    patchPanel(id, 'siblings', token, renderPanelError('sibling pressings'));
  }
}

/**
 * Look up the cached worklist row for an id (band-aware console needs the
 * row's band / source / intent). Returns `null` when the cohort isn't
 * loaded or the id is gone.
 *
 * @param {number} id
 * @returns {Object|null}
 */
function consoleRow(id) {
  const rows = Array.isArray(state.longTail.rows) ? state.longTail.rows : [];
  return rows.find((r) => r && r.id === id) || null;
}

/**
 * Toggle the in-place action console for one row (U4). Opening renders the
 * console shell synchronously (band-aware), then fires the evidence panels
 * INDEPENDENTLY — each patches its own container as it settles, so a slow
 * or failing panel never blanks the console or drops another panel. A
 * per-row token stamps the fetch so a stale console (operator clicked
 * another row, or re-collapsed this one) is discarded before it paints.
 *
 * @param {number} id  album_requests.id
 * @returns {void}
 */
export function toggleLongTailDetail(id) {
  if (typeof document === 'undefined') return;
  const el = document.getElementById(`lt-detail-${id}`);
  if (!el) return;
  if (el.classList.contains('open')) {
    el.classList.remove('open');
    // Bump the token so any in-flight panel fetch is discarded on resolve.
    consoleTokens.set(id, (consoleTokens.get(id) || 0) + 1);
    return;
  }
  const token = (consoleTokens.get(id) || 0) + 1;
  consoleTokens.set(id, token);
  const row = consoleRow(id) || { id };
  el.innerHTML = renderConsoleShell(row);
  el.classList.add('open');
  const inFlightFlag = !!row.in_flight_rescue;
  // Independent panel loads — no Promise.all, no shared await. One panel's
  // rejection can't reject another's promise (each loader try/catches and
  // patches its own error affordance). The YouTube panel is already in its
  // never_run state from the shell (no fetch in U4).
  loadUnfindablePanel(id, token);
  loadPipelinePanels(id, token, inFlightFlag);
}

// --- Console rescue flow (U5) ----------------------------------------
//
// Two steps: "Check YouTube" runs the slow side-effectful resolver GET and
// re-renders the YouTube panel with pickable rescue targets; picking a
// target opens a confirm overlay and submits the rescue. Both steps are
// double-fire-guarded (a module-scoped Set keyed by request id). The
// resolver result is stamped with the row's console token so a stale result
// (operator moved on) is discarded before it paints.

/**
 * @typedef {Object} RescueCopy
 * @property {string} title    Short headline for the outcome.
 * @property {string} detail   One-line operator-facing explanation.
 * @property {'success'|'error'} tone  Drives toast styling (error → red).
 */

/**
 * Map a YouTube-rescue ingest outcome to specific operator-facing console
 * copy. Pure / DOM-free. Keys mirror the EXACT outcome vocabulary in
 * `lib/youtube_ingest_service.py` / `web/routes/youtube.py`
 * (`OUTCOME_HTTP_STATUS`): `accepted`, `request_not_found`, `wrong_state`,
 * `in_flight`, `no_resolver_mapping`, `track_count_precheck_failed`,
 * `transient`.
 *
 * `result` is the parsed rescue-submit response body — its `outcome`
 * selects the copy; `download_log_id` / `detail` decorate the `in_flight`
 * and `track_count_precheck_failed` messages with the specifics the
 * backend returns. An unknown outcome falls back to a generic error so a
 * future backend value never renders blank.
 *
 * @param {{outcome?: string, download_log_id?: number|null, detail?: string|null, error?: string|null}|null|undefined} result
 * @returns {RescueCopy}
 */
export function rescueOutcomeCopy(result) {
  const outcome = String((result && result.outcome) || '');
  const detail = (result && result.detail) ? String(result.detail) : '';
  const logId = (result && result.download_log_id != null)
    ? result.download_log_id : null;
  switch (outcome) {
    case 'accepted':
      return {
        title: 'Rescue queued',
        detail: logId != null
          ? `Rescue queued (download_log #${logId}). The importer owns the import; the row updates when it lands.`
          : 'Rescue queued. The importer owns the import; the row updates when it lands.',
        tone: 'success',
      };
    case 'in_flight':
      return {
        title: 'Rescue already running',
        detail: logId != null
          ? `A rescue is already running for this request (download_log #${logId}).`
          : 'A rescue is already running for this request.',
        tone: 'error',
      };
    case 'wrong_state':
      return {
        title: 'Request changed',
        detail: 'This request is no longer wanted/manual — refresh and try again.',
        tone: 'error',
      };
    case 'no_resolver_mapping':
      return {
        title: 'No resolver mapping',
        detail: 'No cached YouTube mapping for this release — re-run Check YouTube first.',
        tone: 'error',
      };
    case 'track_count_precheck_failed':
      return {
        title: 'Track-count mismatch',
        detail: detail
          ? `Track-count precheck failed: ${detail}`
          : 'Track-count precheck failed: the resolver and MB mirror disagree — refresh and re-check.',
        tone: 'error',
      };
    case 'transient':
      return {
        title: 'Temporary failure',
        detail: detail
          ? `Temporary failure: ${detail}. Retry.`
          : 'Temporary failure (DB / mirror hiccup). Retry.',
        tone: 'error',
      };
    case 'request_not_found':
      return {
        title: 'Request not found',
        detail: 'This request no longer exists — refresh.',
        tone: 'error',
      };
    default:
      return {
        title: 'Rescue failed',
        detail: detail || (result && result.error
          ? String(result.error)
          : `Unexpected outcome: ${outcome || 'unknown'}.`),
        tone: 'error',
      };
  }
}

/**
 * Request ids with an outstanding resolver GET. Guards the slow,
 * side-effectful Check-YouTube call against double-fire: a second click
 * while one is outstanding fires nothing.
 *
 * @type {Set<number>}
 */
const resolveInFlight = new Set();

/**
 * Request ids with an outstanding rescue-submit POST. Guards the confirm →
 * submit step against double-fire.
 *
 * @type {Set<number>}
 */
const submitInFlight = new Set();

/**
 * Double-fire predicate for the resolver GET. Pure. `true` when a Check
 * YouTube call may START for this id (none outstanding); `false` when one
 * is already in flight (the click is suppressed).
 *
 * @param {Set<number>} inFlight  The in-flight id set.
 * @param {number} id
 * @returns {boolean}
 */
export function canStartInFlight(inFlight, id) {
  return !inFlight.has(id);
}

/**
 * Re-render just the YouTube panel body for one row's open console, guarded
 * by the row's console token so a stale result doesn't paint. DOM-side.
 *
 * @param {number} id
 * @param {number} token  The console token captured when the resolve fired.
 * @param {{outcome?: string, youtube_releases?: Array<Object>|null, from_cache?: boolean, error_message?: string|null}|null} result
 * @returns {void}
 */
function patchYoutubePanel(id, token, result) {
  patchPanel(id, 'youtube', token, renderYoutubeBody(result, id));
}

/**
 * "Check YouTube" handler (U5) — replaces U4's placeholder toast. Runs the
 * slow, side-effectful resolver GET for the row's `mb_release_id`, then
 * re-renders the YouTube panel with the fresh classification.
 *
 * Guards:
 *   * Double-fire — a module-scoped `resolveInFlight` Set keyed by request
 *     id; a second click while outstanding returns immediately.
 *   * Stale result — the result is only painted if the row's console token
 *     still matches the one captured when the fetch fired (operator may
 *     have collapsed the console or clicked another row meanwhile).
 *   * Disabled button — the live "Check YouTube" / "Retry" button is
 *     disabled + relabelled while outstanding so the operator sees progress
 *     and can't re-click it.
 *
 * The resolver identifier is the request's `mb_release_id` (an MB release
 * MBID or a Discogs release id) — the same id the resolver's
 * `?identifier=` query takes. A row without one cannot be resolved; the
 * panel shows that explicitly rather than firing a doomed fetch.
 *
 * @param {number} id  album_requests.id
 * @returns {Promise<void>}
 */
export async function checkYoutube(id) {
  if (!canStartInFlight(resolveInFlight, id)) return;  // double-fire guard.
  const row = consoleRow(id);
  const identifier = row && row.mb_release_id ? String(row.mb_release_id) : '';
  const token = consoleTokens.get(id) || 0;
  if (!identifier) {
    patchYoutubePanel(id, token, /** @type {any} */ (
      { outcome: 'transient', error_message: 'No release identifier on this request.' }));
    return;
  }
  resolveInFlight.add(id);
  setYoutubeChecking(id);
  try {
    const r = await fetch(
      `${API}/api/youtube-album?identifier=${encodeURIComponent(identifier)}`);
    // 404/503 still carry a typed body; the classifier maps any non-`ok`
    // outcome to `resolver_failed`, so we read the body regardless of
    // status and only fall back to a synthetic failure if the body is
    // unreadable.
    let result;
    try {
      result = await r.json();
    } catch (_e) {
      result = { outcome: 'transient', error_message: `HTTP ${r.status}` };
    }
    patchYoutubePanel(id, token, result);
  } catch (_e) {
    patchYoutubePanel(id, token, /** @type {any} */ (
      { outcome: 'transient', error_message: 'Could not reach the resolver. Retry.' }));
  } finally {
    resolveInFlight.delete(id);
  }
}

/**
 * Swap the YouTube panel into an in-progress state while the resolver GET
 * is outstanding (disabled button + spinner copy). DOM-side; no-op when the
 * panel isn't mounted (Node tests / collapsed console).
 *
 * @param {number} id
 * @returns {void}
 */
function setYoutubeChecking(id) {
  if (typeof document === 'undefined') return;
  const panel = document.getElementById(`lt-panel-youtube-${id}`);
  if (!panel) return;
  const body = panel.querySelector('.lt-panel-body');
  if (!body) return;
  body.innerHTML = `<div class="lt-yt lt-yt-checking">
    <div class="lt-yt-msg">Resolving against YouTube Music…</div>
    <button class="lt-yt-check" type="button" disabled>Checking…</button>
  </div>`;
}

/**
 * Render the rescue confirm dialog body. Pure. Mirrors the
 * `.confirm-box` shell used by `replace_picker.js` so the visual language
 * matches the only other destructive-confirm in the app.
 *
 * @param {number} id        album_requests.id
 * @param {string} browseId  The YT Music album browseId being submitted.
 * @param {Object|null} row  The worklist row (for the album label).
 * @returns {string}
 */
export function renderRescueConfirm(id, browseId, row) {
  const album = (row && row.album_title) ? String(row.album_title) : `request #${id}`;
  const artist = (row && row.artist_name) ? String(row.artist_name) : '';
  const label = artist ? `${artist} — ${album}` : album;
  return `<div class="confirm-box" role="dialog" aria-modal="true">
    <h3>Rescue from YouTube Music?</h3>
    <p>Queue a YouTube-Music rescue for:<br><strong>${esc(label)}</strong></p>
    <p>Target album:<br><code>${esc(browseId)}</code></p>
    <p style="font-size:0.85em;color:#999;">The request stays <code>wanted</code> until the
    importer lands the rescue (minutes later) — the row won't move bands immediately.</p>
    <div class="actions">
      <button class="btn" id="lt-rescue-cancel">Cancel</button>
      <button class="btn p-btn" id="lt-rescue-confirm">Rescue</button>
    </div>
  </div>`;
}

/**
 * The dedicated mount node for the rescue confirm overlay. Reuses the
 * shared `replace-picker-modal` host (same pattern as the Replace picker —
 * one modal host, wiped on close).
 *
 * @returns {HTMLElement|null}
 */
function rescueModalHost() {
  if (typeof document === 'undefined') return null;
  return document.getElementById('replace-picker-modal');
}

/**
 * Open the Promise-based rescue confirm overlay. Mirrors
 * `replace_picker.js`'s overlay shell: a full-screen `.confirm-overlay`
 * backdrop (click-to-cancel) wrapping the `.confirm-box`. Resolves `true`
 * on confirm, `false` on cancel / backdrop click. DOM-side.
 *
 * @param {number} id
 * @param {string} browseId
 * @param {Object|null} row
 * @returns {Promise<boolean>}
 */
function confirmRescue(id, browseId, row) {
  const host = rescueModalHost();
  if (!host) return Promise.resolve(false);
  return new Promise((resolve) => {
    let settled = false;
    /** @param {boolean} ok */
    function close(ok) {
      if (settled) return;
      settled = true;
      host.style.display = 'none';
      host.innerHTML = '';
      resolve(ok);
    }
    host.innerHTML = `<div class="confirm-overlay">${renderRescueConfirm(id, browseId, row)}</div>`;
    host.style.display = '';
    const overlay = host.querySelector('.confirm-overlay');
    if (overlay) {
      overlay.addEventListener('click', (event) => {
        if (event.target === overlay) close(false);  // backdrop-click cancel.
      });
    }
    const cancel = host.querySelector('#lt-rescue-cancel');
    if (cancel) cancel.addEventListener('click', () => close(false));
    const confirm = host.querySelector('#lt-rescue-confirm');
    if (confirm) confirm.addEventListener('click', () => close(true));
  });
}

/**
 * Pick a rescue target (U5) — opens the confirm overlay, and on confirm
 * submits the rescue. Double-fire-guarded on the submit step.
 *
 * @param {number} id        album_requests.id
 * @param {string} browseId  The chosen target's `yt_browse_id`.
 * @returns {Promise<void>}
 */
export async function pickYoutubeRescue(id, browseId) {
  const row = consoleRow(id);
  const ok = await confirmRescue(id, browseId, row);
  if (!ok) return;
  await submitYoutubeRescue(id, browseId);
}

/**
 * Submit the rescue (`POST /api/pipeline/<id>/youtube-rescue {browse_id}`)
 * and map the outcome to console copy. On `accepted`, mark the row
 * in-flight and refetch JUST that row (KTD8 — single-row patch, no
 * full-cohort re-band, no optimistic band move; the importer owns the
 * transition, KTD4). Every other outcome surfaces its specific copy as a
 * toast. Double-fire-guarded.
 *
 * @param {number} id
 * @param {string} browseId
 * @returns {Promise<void>}
 */
async function submitYoutubeRescue(id, browseId) {
  if (!canStartInFlight(submitInFlight, id)) return;  // double-fire guard.
  submitInFlight.add(id);
  try {
    let result;
    try {
      const r = await fetch(`${API}/api/pipeline/${id}/youtube-rescue`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ browse_id: browseId }),
      });
      result = await r.json();
    } catch (_e) {
      result = { outcome: 'transient', error_message: 'Submit failed — retry.' };
    }
    const copy = rescueOutcomeCopy(result);
    if (typeof toast === 'function') {
      toast(`${copy.title}: ${copy.detail}`, copy.tone === 'error');
    }
    if (result && result.outcome === 'accepted') {
      // KTD8: mark the row in-flight locally for an immediate signal, then
      // refetch just this row's authoritative band/flags and patch it in.
      markRowInFlight(id);
      await refetchLongTailRow(id);
    }
  } finally {
    submitInFlight.delete(id);
  }
}

/**
 * Mark one cohort row `in_flight_rescue` in place (optimistic local signal
 * only — NOT a band move). Pure-ish: mutates `state.longTail.rows`. The
 * authoritative flags arrive via {@link refetchLongTailRow}.
 *
 * @param {number} id
 * @returns {void}
 */
function markRowInFlight(id) {
  const rows = Array.isArray(state.longTail.rows) ? state.longTail.rows : [];
  const row = rows.find((r) => r && r.id === id);
  if (row) row.in_flight_rescue = true;
}

/**
 * Single-row refetch + patch (KTD8). Fetches just this request's
 * authoritative banded row via `GET /api/pipeline/long-tail?id=<id>` and
 * replaces it in `state.longTail.rows`, then re-renders the list. NO
 * full-cohort re-band (the heaviest read in the app), NO optimistic band
 * move — the row stays in `Missing` until the importer completes (KTD4).
 *
 * A 404 (the row left the `wanted` worklist — e.g. already imported)
 * removes it from the cohort. A failed fetch is non-fatal: the local
 * in-flight mark from {@link markRowInFlight} already gives the operator a
 * signal; the next Refresh reconciles.
 *
 * @param {number} id
 * @returns {Promise<void>}
 */
async function refetchLongTailRow(id) {
  let data;
  try {
    const r = await fetch(`${API}/api/pipeline/long-tail?id=${encodeURIComponent(String(id))}`);
    if (r.status === 404) {
      removeRowFromCohort(id);
      renderLongTail();
      return;
    }
    if (!r.ok) return;
    data = await r.json();
  } catch (_e) {
    return;  // non-fatal — local in-flight mark stands; Refresh reconciles.
  }
  const fresh = data && data.result;
  if (!fresh) return;
  patchRowInCohort(id, fresh);
  renderLongTail();
}

/**
 * Replace one row in the cohort with a freshly-refetched authoritative row.
 * Pure-ish: mutates `state.longTail.rows`. No-op when the cohort isn't
 * loaded or the id is gone (a concurrent full refetch already reconciled).
 *
 * @param {number} id
 * @param {Object} fresh  The refetched `LongTailRow`.
 * @returns {void}
 */
function patchRowInCohort(id, fresh) {
  const rows = Array.isArray(state.longTail.rows) ? state.longTail.rows : null;
  if (!rows) return;
  const idx = rows.findIndex((r) => r && r.id === id);
  if (idx === -1) return;
  rows[idx] = fresh;
}

/**
 * Drop one row from the cohort (the single-row refetch 404'd — the row left
 * the `wanted` worklist). Pure-ish: mutates `state.longTail.rows`.
 *
 * @param {number} id
 * @returns {void}
 */
function removeRowFromCohort(id) {
  const rows = Array.isArray(state.longTail.rows) ? state.longTail.rows : null;
  if (!rows) return;
  const idx = rows.findIndex((r) => r && r.id === id);
  if (idx !== -1) rows.splice(idx, 1);
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
  // U4 — console pure render helpers + re-exported util classifiers.
  youtubeSectionState,
  consoleEmphasis,
  renderConsoleShell,
  renderUnfindableBody,
  renderPeersBody,
  renderRescuesBody,
  renderSiblingsBody,
  renderYoutubeBody,
  renderPanelError,
  youtubeHistoryRows,
  youtubeFailureReason,
  PEERS_VISIBLE_CAP,
  // U5 — two-step rescue flow pure helpers.
  youtubeBestDistance,
  youtubeRescueTargets,
  rescueOutcomeCopy,
  canStartInFlight,
  renderRescueConfirm,
};
