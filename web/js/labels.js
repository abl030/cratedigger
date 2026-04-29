// @ts-check

/**
 * Label search + label-detail rendering for the browse tab.
 *
 * Phase A wires Discogs only. The page is structured so a Phase B
 * MusicBrainz adapter is a route swap, not a redesign — the
 * `LabelEntity` and release-row contracts the route layer returns
 * are source-agnostic and read identically here.
 *
 * Pure helpers (`applyLabelFilters`, `sortByYearDesc`, `parseYear`,
 * `buildLabelSearchUrl`) are testable via Node — see
 * `tests/test_js_util.mjs`. Render helpers are DOM-bound and
 * verified via playwright.
 */

import { state, API, toast } from './state.js';
import { esc } from './util.js';
import { renderTypedSections } from './grouping.js';
import { renderStatusBadges } from './badges.js';
import { loadReleaseGroup } from './discography.js';

/**
 * Threshold above which the initial label-detail fetch defaults to
 * `include_sublabels=false`. Per U1+U2 EXPLAIN findings, mega-labels
 * (UMG, Sony, etc.) with full sub-label rollup take 30+ seconds. For
 * boutique labels (Hymen 659, Warp ~3000) this never trips.
 */
export const BIG_LABEL_THRESHOLD = 1000;

/**
 * Soft limit on releases rendered in the body. The route returns up
 * to 100 today; if the upstream ever exceeds this we render the first
 * `MAX_RENDERED` and surface a hint.
 */
export const MAX_RENDERED = 100;

/**
 * Build the URL for `/api/discogs/label/search`.
 * Pure for testability.
 * @param {string} q
 * @returns {string}
 */
export function buildLabelSearchUrl(q) {
  return `/api/discogs/label/search?q=${encodeURIComponent(q)}`;
}

/**
 * Parse the `release.date` field (Discogs `released`) into a year.
 * Accepts "2003", "2003-04", "2003-04-15"; returns null for missing
 * or unparseable values.
 * @param {string|null|undefined} dateStr
 * @returns {number|null}
 */
export function parseYear(dateStr) {
  if (!dateStr) return null;
  const m = String(dateStr).match(/^(\d{4})/);
  if (!m) return null;
  const y = Number(m[1]);
  return Number.isFinite(y) ? y : null;
}

/**
 * @typedef {Object} LabelFilters
 * @property {number|null} [yearMin]
 * @property {number|null} [yearMax]
 * @property {string} [format] - Substring match against `release.format`
 * @property {boolean} [hideHeld] - When true, exclude rows with `in_library === true`
 */

/**
 * Pure filter predicate over label release rows. No DOM access.
 *
 * Year filter is inclusive on both ends. When either yearMin or
 * yearMax is set, rows with no parseable year are dropped (you cannot
 * place an undated release inside a year range). When no year filter
 * is active, undated rows survive.
 *
 * Format filter is a case-insensitive substring match on the joined
 * `release.format` string (e.g. "LP, Album" matches both "LP" and
 * "Album"). Empty string means no filter.
 *
 * @param {Array<Object>} releases
 * @param {LabelFilters} filters
 * @returns {Array<Object>}
 */
export function applyLabelFilters(releases, filters) {
  const yMin = (filters && filters.yearMin != null) ? Number(filters.yearMin) : null;
  const yMax = (filters && filters.yearMax != null) ? Number(filters.yearMax) : null;
  const yearActive = yMin != null || yMax != null;
  const fmtRaw = (filters && filters.format) ? String(filters.format).trim() : '';
  const fmt = fmtRaw.toLowerCase();
  const hideHeld = !!(filters && filters.hideHeld);

  return releases.filter((r) => {
    if (hideHeld && r.in_library === true) return false;
    if (fmt) {
      const f = String(r.format || '').toLowerCase();
      if (!f.includes(fmt)) return false;
    }
    if (yearActive) {
      const y = parseYear(r.date);
      if (y == null) return false;
      if (yMin != null && y < yMin) return false;
      if (yMax != null && y > yMax) return false;
    }
    return true;
  });
}

/**
 * Stable sort by year descending. Rows with no parseable year sort
 * to the end. Stable across equal years (preserves input order).
 *
 * Returns a new array — does not mutate input.
 *
 * @param {Array<Object>} releases
 * @returns {Array<Object>}
 */
export function sortByYearDesc(releases) {
  return releases
    .map((r, i) => ({ r, i, y: parseYear(r.date) }))
    .sort((a, b) => {
      // Missing year always sorts last (regardless of direction).
      if (a.y == null && b.y == null) return a.i - b.i;
      if (a.y == null) return 1;
      if (b.y == null) return -1;
      if (a.y !== b.y) return b.y - a.y; // desc
      return a.i - b.i; // stable
    })
    .map((entry) => entry.r);
}

/**
 * Distinct format keys for the filter dropdown. Splits joined Discogs
 * format strings ("LP, Album, Repress") on commas and de-dupes.
 * Returns lowercase tokens for use as substring filters; the UI
 * renders them title-cased.
 * @param {Array<Object>} releases
 * @returns {string[]}
 */
export function distinctFormats(releases) {
  /** @type {Set<string>} */
  const set = new Set();
  for (const r of releases) {
    const raw = String(r.format || '').trim();
    if (!raw) continue;
    for (const part of raw.split(',')) {
      const tok = part.trim();
      if (tok) set.add(tok);
    }
  }
  return [...set].sort((a, b) => a.localeCompare(b));
}

// ─────────────────────────────────────────────────────────────────────
// DOM-bound functions below — verified via playwright, not unit tests.
// ─────────────────────────────────────────────────────────────────────

/**
 * Search for labels via `/api/discogs/label/search`.
 * @param {string} query
 * @returns {Promise<Array<Object>>}
 */
export async function searchLabels(query) {
  const url = `${API}${buildLabelSearchUrl(query)}`;
  try {
    const r = await fetch(url);
    if (!r.ok) return [];
    const data = await r.json();
    return Array.isArray(data.results) ? data.results : [];
  } catch (_e) {
    return [];
  }
}

/**
 * Render label search hits into a container element.
 *
 * @param {HTMLElement} containerEl
 * @param {Array<Object>} hits
 * @param {(labelId: string, labelName: string) => void} onClickHandler
 */
export function renderLabelSearchResults(containerEl, hits, onClickHandler) {
  if (!hits.length) {
    containerEl.innerHTML = '<div class="loading">No label results</div>';
    return;
  }
  // Stash handler on the element so the inline onclick can find it
  // by index without leaking globals. Mirrors the label-id-array
  // approach we use elsewhere when an onclick needs typed args.
  /** @type {any} */ (containerEl)._labelHits = hits;
  /** @type {any} */ (containerEl)._labelClick = onClickHandler;
  containerEl.innerHTML = hits.map((h, i) => {
    const country = h.country ? `<span class="artist-dis"> · ${esc(h.country)}</span>` : '';
    const parent = h.parent_label_id
      ? `<span class="badge badge-sublabel" style="margin-left:6px;">via ${esc(h.parent_label_name || 'parent')}</span>`
      : '';
    const count = (typeof h.release_count === 'number' && h.release_count > 0)
      ? `<span class="artist-dis" style="margin-left:6px;">${h.release_count} release${h.release_count === 1 ? '' : 's'}</span>`
      : '';
    return `
      <div class="artist">
        <div class="artist-header" onclick="window.openLabelDetailFromList(this.closest('.artist'), ${i})">
          <span class="artist-name">${esc(h.name || '')}</span>
          ${country}
          ${parent}
          ${count}
        </div>
      </div>`;
  }).join('');
}

/**
 * Click-resolver for a search-result row. Looks up the hit by index
 * on the parent container that `renderLabelSearchResults` annotated.
 * @param {HTMLElement} rowEl
 * @param {number} index
 */
export function openLabelDetailFromList(rowEl, index) {
  const containerEl = /** @type {any} */ (rowEl.parentElement);
  if (!containerEl || !containerEl._labelHits) return;
  const hit = containerEl._labelHits[index];
  const handler = containerEl._labelClick;
  if (!hit) return;
  if (typeof handler === 'function') {
    handler(String(hit.id), String(hit.name));
  } else {
    openLabelDetail(String(hit.id), String(hit.name));
  }
}

/**
 * Open the label detail view: hides search results + artist view,
 * shows the label-detail container, fetches and renders.
 * @param {string} labelId
 * @param {string} labelName
 */
export async function openLabelDetail(labelId, labelName) {
  state.browseLabel = { id: labelId, name: labelName };
  state.browseSubView = 'label';

  const results = document.getElementById('results');
  if (results) results.style.display = 'none';
  const browseArtist = document.getElementById('browse-artist');
  if (browseArtist) browseArtist.style.display = 'none';
  const browseLabel = document.getElementById('browse-label');
  if (browseLabel) browseLabel.style.display = 'block';

  const nameEl = document.getElementById('browse-label-name');
  if (nameEl) nameEl.textContent = labelName;

  const body = document.getElementById('browse-label-body');
  if (!body) return;
  body.innerHTML = '<div class="loading">Loading label catalogue...</div>';

  // First fetch with default include_sublabels=true. After we have
  // the label entity we may decide to retry without sub-labels for
  // very large labels.
  try {
    let payload = await loadLabelReleases(labelId, { include_sublabels: true });
    const totalCount = (payload && payload.label && payload.label.release_count) || 0;
    if (totalCount > BIG_LABEL_THRESHOLD && payload.releases && payload.releases.length === 0) {
      // Defensive: if the big-label query timed out and returned empty,
      // refetch without sub-labels. (Cheap insurance — happy path on
      // boutique labels never enters this branch.)
      payload = await loadLabelReleases(labelId, { include_sublabels: false });
    } else if (totalCount > BIG_LABEL_THRESHOLD) {
      // Surface the sub-label opt-in toggle even when the first fetch
      // succeeded — UX hint that this label has a long tail.
      state.labelFilters = state.labelFilters || {};
      /** @type {any} */ (state.labelFilters).bigLabel = true;
    }
    renderLabelDetail(body, payload);
  } catch (e) {
    body.innerHTML = '<div class="loading">Failed to load label</div>';
  }
}

/**
 * Close the label detail view; show search results.
 */
export function closeLabelDetail() {
  state.browseLabel = null;
  state.labelFilters = { yearMin: null, yearMax: null, format: '', hideHeld: false };
  const browseLabel = document.getElementById('browse-label');
  if (browseLabel) browseLabel.style.display = 'none';
  const results = document.getElementById('results');
  if (results) results.style.display = 'block';
}

/**
 * Fetch label detail + releases.
 * @param {string} labelId
 * @param {{include_sublabels?: boolean}} [opts]
 * @returns {Promise<Object>}
 */
export async function loadLabelReleases(labelId, opts = {}) {
  const includeSub = opts.include_sublabels !== false;
  const url = `${API}/api/discogs/label/${encodeURIComponent(labelId)}?include_sublabels=${includeSub}`;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return await r.json();
}

/**
 * Render the label detail page: header + filter bar + grouped body.
 * @param {HTMLElement} containerEl
 * @param {Object} payload
 */
export function renderLabelDetail(containerEl, payload) {
  const label = payload.label || {};
  const allReleases = Array.isArray(payload.releases) ? payload.releases : [];
  const totalCount = (typeof label.release_count === 'number')
    ? label.release_count : allReleases.length;
  const includeSub = payload.include_sublabels !== false;

  // Stash full release list on the container for filter re-renders.
  /** @type {any} */ (containerEl)._releases = allReleases;
  /** @type {any} */ (containerEl)._totalCount = totalCount;
  /** @type {any} */ (containerEl)._labelId = String(label.id || '');
  /** @type {any} */ (containerEl)._labelName = String(label.name || '');
  /** @type {any} */ (containerEl)._includeSub = includeSub;

  const hasAnySubLabel = allReleases.some((r) => r.sub_label_name);
  /** @type {any} */ (containerEl)._hasAnySubLabel = hasAnySubLabel;

  // Initialise / preserve filters in state. Default off everywhere.
  if (!state.labelFilters) {
    state.labelFilters = { yearMin: null, yearMax: null, format: '', hideHeld: false };
  }
  /** @type {LabelFilters} */
  const filters = state.labelFilters;

  const formats = distinctFormats(allReleases);

  // Header
  const profile = (label.profile || '').toString();
  const profileShort = profile.length > 200 ? profile.slice(0, 200) + '…' : profile;
  const parentBadge = label.parent_label_id
    ? `<span class="badge badge-sublabel">via ${esc(label.parent_label_name || 'parent')}</span>`
    : '';
  const country = label.country ? ` · ${esc(label.country)}` : '';
  const renderedNote = (allReleases.length < totalCount)
    ? `<div class="loading" style="text-align:left;padding:6px 0;color:#888;">Showing first ${allReleases.length} of ${totalCount} — pagination coming.</div>`
    : '';
  const bigLabelToggle = (totalCount > BIG_LABEL_THRESHOLD)
    ? `<label style="margin-left:10px;font-size:0.85em;color:#aaa;">
         <input type="checkbox" id="label-include-sublabels" ${includeSub ? 'checked' : ''}
                onchange="window.toggleLabelIncludeSublabels(this.checked)"> include sub-labels
       </label>`
    : '';

  const fmtOptions = ['<option value="">All formats</option>']
    .concat(formats.map((f) => {
      const sel = (filters.format && filters.format.toLowerCase() === f.toLowerCase()) ? ' selected' : '';
      return `<option value="${esc(f)}"${sel}>${esc(f)}</option>`;
    })).join('');

  const yMinVal = (filters.yearMin != null) ? String(filters.yearMin) : '';
  const yMaxVal = (filters.yearMax != null) ? String(filters.yearMax) : '';
  const hideHeldChecked = filters.hideHeld ? 'checked' : '';

  containerEl.innerHTML = `
    <div style="margin-bottom:12px;">
      <div style="display:flex;gap:10px;align-items:baseline;flex-wrap:wrap;">
        <span style="font-size:18px;font-weight:bold;">${esc(label.name || '')}</span>
        ${parentBadge}
        <span style="color:#888;font-size:0.85em;">${totalCount} release${totalCount === 1 ? '' : 's'}${country}</span>
      </div>
      ${profileShort ? `<div style="color:#888;font-size:0.85em;margin-top:6px;">${esc(profileShort)}</div>` : ''}
    </div>
    <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:10px;padding:8px;background:#1a1a1a;border-radius:6px;">
      <span style="font-size:0.8em;color:#888;">Year</span>
      <input type="number" id="label-year-min" placeholder="min" value="${yMinVal}"
             style="width:80px;padding:4px 6px;background:#222;color:#eee;border:1px solid #444;border-radius:4px;font-size:13px;"
             oninput="window.onLabelFilterChange()">
      <span style="color:#666;">–</span>
      <input type="number" id="label-year-max" placeholder="max" value="${yMaxVal}"
             style="width:80px;padding:4px 6px;background:#222;color:#eee;border:1px solid #444;border-radius:4px;font-size:13px;"
             oninput="window.onLabelFilterChange()">
      <span style="font-size:0.8em;color:#888;margin-left:8px;">Format</span>
      <select id="label-format" onchange="window.onLabelFilterChange()"
              style="padding:4px 6px;background:#222;color:#eee;border:1px solid #444;border-radius:4px;font-size:13px;">
        ${fmtOptions}
      </select>
      <label style="font-size:0.85em;color:#aaa;margin-left:8px;">
        <input type="checkbox" id="label-hide-held" ${hideHeldChecked}
               onchange="window.onLabelFilterChange()"> hide held
      </label>
      ${bigLabelToggle}
    </div>
    ${renderedNote}
    <div id="browse-label-rows"></div>
  `;

  renderLabelRows(containerEl);
}

/**
 * Re-render the rows section based on the current filter state.
 * @param {HTMLElement} containerEl
 */
export function renderLabelRows(containerEl) {
  const rows = /** @type {any} */ (containerEl)._releases || [];
  const hasAnySubLabel = /** @type {any} */ (containerEl)._hasAnySubLabel;
  /** @type {LabelFilters} */
  const filters = state.labelFilters || { yearMin: null, yearMax: null, format: '', hideHeld: false };
  const filtered = applyLabelFilters(rows, filters);
  const sorted = sortByYearDesc(filtered);
  const visible = sorted.slice(0, MAX_RENDERED);

  const body = containerEl.querySelector('#browse-label-rows');
  if (!body) return;
  if (!visible.length) {
    body.innerHTML = '<div class="loading">No releases match the current filters.</div>';
    return;
  }

  const renderRow = (rel) => {
    const year = parseYear(rel.date);
    const yearStr = year != null ? String(year) : '?';
    const subBadge = (hasAnySubLabel && rel.sub_label_name)
      ? `<span class="badge badge-sublabel" style="margin-left:6px;">via ${esc(rel.sub_label_name)}</span>`
      : '';
    const badges = renderStatusBadges(rel);
    const fmt = rel.format ? `<span class="rg-meta"> — ${esc(rel.format)}</span>` : '';
    const artist = rel.artist_name ? `<span class="rg-meta" style="color:#999;"> — ${esc(rel.artist_name)}</span>` : '';
    return `
      <div class="rg" onclick="event.stopPropagation(); window.loadReleaseGroup('${esc(String(rel.id))}', this)">
        <div>
          <span class="rg-year">${yearStr}</span>
          <span class="rg-title">${esc(rel.title || '')}</span>
          ${artist}
          ${fmt}
          ${subBadge}
          ${badges}
        </div>
        <div class="releases" id="rel-${esc(String(rel.id))}"></div>
      </div>
    `;
  };

  body.innerHTML = renderTypedSections(visible, renderRow, {
    classify: (r) => {
      const t = String(r.primary_type || '').toLowerCase();
      if (t === 'album') return 'Albums';
      if (t === 'ep') return 'EPs';
      if (t === 'single') return 'Singles';
      if (t === 'compilation' || t === 'soundtrack') return 'Compilations';
      if (t === 'live') return 'Live';
      return 'Other';
    },
    dateOf: (r) => String(r.date || ''),
    defaultOpen: 'Albums',
  });
}

/**
 * Inputs/selects in the filter bar all funnel through this handler.
 * Reads DOM, updates state, re-renders rows.
 */
export function onLabelFilterChange() {
  const containerEl = document.getElementById('browse-label-body');
  if (!containerEl) return;
  const yMinEl = /** @type {HTMLInputElement|null} */ (document.getElementById('label-year-min'));
  const yMaxEl = /** @type {HTMLInputElement|null} */ (document.getElementById('label-year-max'));
  const fmtEl = /** @type {HTMLSelectElement|null} */ (document.getElementById('label-format'));
  const hideEl = /** @type {HTMLInputElement|null} */ (document.getElementById('label-hide-held'));

  const yMinRaw = yMinEl && yMinEl.value.trim();
  const yMaxRaw = yMaxEl && yMaxEl.value.trim();
  state.labelFilters = {
    yearMin: yMinRaw ? Number(yMinRaw) : null,
    yearMax: yMaxRaw ? Number(yMaxRaw) : null,
    format: fmtEl ? fmtEl.value : '',
    hideHeld: !!(hideEl && hideEl.checked),
  };
  renderLabelRows(containerEl);
}

/**
 * Toggle the include-sublabels opt-in (only shown for big labels).
 * Triggers a refetch.
 * @param {boolean} include
 */
export async function toggleLabelIncludeSublabels(include) {
  if (!state.browseLabel) return;
  const body = document.getElementById('browse-label-body');
  if (!body) return;
  body.innerHTML = '<div class="loading">Reloading...</div>';
  try {
    const payload = await loadLabelReleases(state.browseLabel.id, { include_sublabels: include });
    renderLabelDetail(body, payload);
  } catch (_e) {
    body.innerHTML = '<div class="loading">Failed to reload label</div>';
    toast('Failed to reload label', true);
  }
}
