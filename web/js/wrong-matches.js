// @ts-check
import { API, toast } from './state.js';
import { esc, externalReleaseUrl, sourceLabel } from './util.js';

/** @type {boolean} */
let _loaded = false;
/** @type {Object | null} */
let _lastData = null;
/** @type {HTMLElement | null} */
let _lastEl = null;

const DEFAULT_CONVERGE_THRESHOLD_MILLI = 180;
const CONVERGE_THRESHOLD_KEY_PREFIX = 'wrongMatches.converge.threshold.';

/**
 * Format seconds as m:ss.
 * @param {number} s
 * @returns {string}
 */
function fmtLen(s) {
  const m = Math.floor(s / 60);
  const sec = Math.round(s % 60);
  return `${m}:${sec < 10 ? '0' : ''}${sec}`;
}

/**
 * Load and display wrong-match rejections from failed_imports.
 */
export async function loadWrongMatches() {
  if (_loaded) return;
  const el = document.getElementById('wrong-matches-content');
  if (!el) return;
  el.innerHTML = '<div class="loading">Loading wrong matches...</div>';
  try {
    const r = await fetch(`${API}/api/wrong-matches`);
    const data = await r.json();
    _loaded = true;
    renderWrongMatches(data, el);
  } catch (e) {
    el.innerHTML = '<div style="color:#f66;">Failed to load wrong matches</div>';
  }
}

/**
 * Invalidate wrong matches cache so next tab switch re-fetches.
 */
export function invalidateWrongMatches() {
  _loaded = false;
}

/**
 * @param {string} key
 * @returns {string | null}
 */
function readStorage(key) {
  try {
    const storage = globalThis.localStorage;
    return storage ? storage.getItem(key) : null;
  } catch (_e) {
    return null;
  }
}

/**
 * @param {string} key
 * @param {string} value
 */
function writeStorage(key, value) {
  try {
    const storage = globalThis.localStorage;
    if (storage) storage.setItem(key, value);
  } catch (_e) {
    // Storage can be blocked in private contexts; the UI still works.
  }
}

/**
 * @param {number|string} requestId
 * @returns {string}
 */
function thresholdStorageKey(requestId) {
  return `${CONVERGE_THRESHOLD_KEY_PREFIX}${requestId}`;
}

/**
 * Normalize a loosen threshold expressed in thousandths.
 * @param {unknown} value
 * @returns {number}
 */
function normalizeThreshold(value) {
  const raw = value == null || value === '' ? DEFAULT_CONVERGE_THRESHOLD_MILLI : value;
  const parsed = Number.parseInt(String(raw), 10);
  if (!Number.isFinite(parsed)) return DEFAULT_CONVERGE_THRESHOLD_MILLI;
  return Math.max(0, Math.min(999, parsed));
}

/**
 * @param {number|string} requestId
 * @returns {number}
 */
function thresholdForGroup(requestId) {
  return normalizeThreshold(readStorage(thresholdStorageKey(requestId)));
}

/**
 * @returns {boolean}
 */
function deleteUnmatchedOnConverge() {
  return true;
}

function rerenderWrongMatches() {
  if (_lastData && _lastEl) renderWrongMatches(_lastData, _lastEl);
}

/**
 * @param {unknown} value
 * @returns {number | null}
 */
function distanceValue(value) {
  if (value == null || typeof value === 'boolean') return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const parsed = Number.parseFloat(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * @param {any} entry
 * @param {number} thresholdMilli
 * @returns {boolean}
 */
function isConvergeGreen(entry, thresholdMilli) {
  const distance = distanceValue(entry?.distance);
  return distance != null && distance <= normalizeThreshold(thresholdMilli) / 1000;
}

/**
 * @param {any} group
 * @param {number} thresholdMilli
 * @returns {any[]}
 */
function greenEntries(group, thresholdMilli) {
  return (group.entries || []).filter((/** @type {any} */ entry) => (
    isConvergeGreen(entry, thresholdMilli)
  ));
}

/**
 * @param {number|string} requestId
 * @param {unknown} thresholdMilli
 * @param {boolean} deleteUnmatched
 * @returns {{request_id: number, threshold_milli: number, delete_unmatched: boolean}}
 */
function convergeRequestBody(requestId, thresholdMilli, deleteUnmatched) {
  return {
    request_id: Number(requestId),
    threshold_milli: normalizeThreshold(thresholdMilli),
    delete_unmatched: true,
  };
}

/**
 * @param {any} data
 * @returns {string}
 */
function convergeToast(data) {
  const queued = data.queued || 0;
  const deleted = data.deleted || 0;
  const skipped = (data.skipped || []).length;
  const parts = [`Queued ${queued} candidate${queued !== 1 ? 's' : ''}`];
  if (deleted) parts.push(`deleted ${deleted}`);
  if (skipped) parts.push(`skipped ${skipped}`);
  return parts.join(', ');
}

/**
 * @param {number} greenCount
 * @returns {string}
 */
function greenCountLabel(greenCount) {
  return `${greenCount} green`;
}

/**
 * @param {number} greenCount
 * @returns {string}
 */
function greenCountStyle(greenCount) {
  return greenCount > 0
    ? 'background:#142814;color:#6d6;border:1px solid #426b42;'
    : 'background:#2a1a1a;color:#f88;border:1px solid #5a2a2a;';
}

/**
 * @param {number} greenCount
 * @returns {string}
 */
function convergeButtonLabel(greenCount) {
  return `Converge${greenCount ? ` (${greenCount})` : ''}`;
}

/**
 * @param {boolean} green
 * @returns {string}
 */
function entryItemStyle(green) {
  return green
    ? 'background:#142014;margin:4px 0;border-color:#426b42;box-shadow:inset 3px 0 0 #6d6;'
    : 'background:#1a1a1a;margin:4px 0;';
}

/**
 * @param {boolean} green
 * @returns {string}
 */
function entryGreenBadgeStyle(green) {
  return `background:#142814;color:#6d6;border:1px solid #426b42;margin-left:8px;${green ? '' : 'display:none;'}`;
}

/**
 * @param {number|string} requestId
 * @returns {any | null}
 */
function groupByRequestId(requestId) {
  return ((_lastData && Array.isArray(_lastData.groups)) ? _lastData.groups : [])
    .find((/** @type {any} */ g) => Number(g.request_id) === Number(requestId)) || null;
}

/**
 * Update threshold-dependent UI in place so expanded groups stay open and
 * focused number inputs keep focus while the operator nudges values.
 * @param {number|string} requestId
 * @returns {boolean}
 */
function updateConvergeGroup(requestId) {
  const group = groupByRequestId(requestId);
  if (!group) return false;
  const thresholdMilli = thresholdForGroup(requestId);
  const greenCount = greenEntries(group, thresholdMilli).length;
  let touched = false;

  const badge = document.getElementById(`wm-green-count-${requestId}`);
  if (badge) {
    badge.textContent = greenCountLabel(greenCount);
    badge.style.cssText = greenCountStyle(greenCount);
    touched = true;
  }

  const btn = /** @type {HTMLButtonElement | null} */ (document.getElementById(`wm-converge-btn-${requestId}`));
  if (btn) {
    btn.disabled = greenCount === 0;
    btn.textContent = convergeButtonLabel(greenCount);
    touched = true;
  }

  for (const entry of (group.entries || [])) {
    const id = entry.download_log_id;
    const green = isConvergeGreen(entry, thresholdMilli);
    const card = document.getElementById(`wm-entry-card-${id}`);
    if (card) {
      card.style.cssText = entryItemStyle(green);
      touched = true;
    }
    const entryBadge = document.getElementById(`wm-entry-green-${id}`);
    if (entryBadge) {
      entryBadge.style.cssText = entryGreenBadgeStyle(green);
      touched = true;
    }
    const dist = document.getElementById(`wm-entry-dist-${id}`);
    if (dist) {
      dist.style.color = green ? '#6d6' : '#aaa';
      touched = true;
    }
  }

  return touched;
}

/**
 * @param {any[]} groups
 * @returns {{groups: number, entries: number}}
 */
function wrongMatchCounts(groups) {
  const visible = groups.filter((/** @type {any} */ g) => (g.pending_count || 0) > 0);
  return {
    groups: visible.length,
    entries: visible.reduce((/** @type {number} */ n, /** @type {any} */ g) => n + (g.pending_count || 0), 0),
  };
}

function updateWrongMatchesSummary() {
  if (!_lastData || !Array.isArray(_lastData.groups) || !_lastEl) return;
  const counts = wrongMatchCounts(_lastData.groups);
  if (counts.groups === 0) {
    _lastEl.innerHTML = '<div style="color:#888;padding:12px;">No wrong matches in failed_imports.</div>';
    return;
  }
  const summary = document.getElementById('wrong-matches-summary');
  if (summary) {
    summary.textContent = `${counts.groups} release${counts.groups !== 1 ? 's' : ''} · ${counts.entries} candidate${counts.entries !== 1 ? 's' : ''} pending review`;
  }
}

/**
 * Remove one release group from the current DOM without refetching/repainting
 * the whole Wrong Matches pane, preserving scroll position and neighboring
 * expanded groups.
 * @param {number|string} requestId
 */
function removeWrongMatchGroup(requestId) {
  if (_lastData && Array.isArray(_lastData.groups)) {
    _lastData.groups = _lastData.groups.filter((/** @type {any} */ g) => (
      Number(g.request_id) !== Number(requestId)
    ));
  }
  const row = document.getElementById(`wm-release-${requestId}`);
  if (row && typeof row.remove === 'function') row.remove();
  updateWrongMatchesSummary();
}

/**
 * @param {number|string} requestId
 * @param {unknown} value
 */
export function setWrongMatchConvergeThreshold(requestId, value) {
  writeStorage(thresholdStorageKey(requestId), String(normalizeThreshold(value)));
  if (!updateConvergeGroup(requestId)) rerenderWrongMatches();
}

/**
 * @param {boolean} checked
 */
export function setWrongMatchConvergeCleanup(_checked) {
  // Kept for old inline handlers after deploy; Converge now always cleans up
  // the non-green rows for the release.
}

/**
 * Render grouped wrong-match entries (issue #113).
 * Top level = one collapsed card per release; expand reveals every rejected
 * candidate that still has files on disk.
 * @param {Object} data
 * @param {HTMLElement} el
 */
function renderWrongMatches(data, el) {
  _lastData = data;
  _lastEl = el;
  /** @type {any[]} */
  const groups = (data.groups || []).filter((/** @type {any} */ g) => (g.pending_count || 0) > 0);
  if (groups.length === 0) {
    el.innerHTML = '<div style="color:#888;padding:12px;">No wrong matches in failed_imports.</div>';
    return;
  }

  const counts = wrongMatchCounts(groups);
  let html = `<div id="wrong-matches-summary" style="margin:8px 0;color:#888;">${counts.groups} release${counts.groups !== 1 ? 's' : ''} · ${counts.entries} candidate${counts.entries !== 1 ? 's' : ''} pending review</div>`;

  html += groups.map(renderGroup).join('');
  el.innerHTML = html;
}

/**
 * Return a tier color for a quality_rank name (matches library tab palette).
 * @param {string} rank
 * @returns {string}
 */
function rankColor(rank) {
  switch (rank) {
    case 'lossless':     return '#7cf';
    case 'transparent':  return '#6d6';
    case 'excellent':    return '#6d6';
    case 'good':         return '#da6';
    case 'acceptable':   return '#da6';
    case 'poor':         return '#f88';
    default:             return '#888';
  }
}

/**
 * Build the quality badge strip for a group header. Shows format + bitrate,
 * verified-lossless marker, spectral grade (when suspect/likely_transcode),
 * and the rank tier — so the user can tell at a glance whether there's
 * already a good version on disk.
 * @param {any} g
 * @returns {string}
 */
function renderQualityBadges(g) {
  // Drive the 'nothing on disk' badge off data, not the DB status.
  // A row left at status='imported' after a manual beet rm still has
  // nothing on disk, so checking status alone would swallow the signal
  // and leave the badge strip empty.
  //
  // Issues #121 / #123: the backend gates `in_library` and the
  // quality fields (`quality_label`, `min_bitrate`,
  // `current_spectral_grade`, `format`) on exact-ID match — no
  // fuzzy fallback. When `in_library=true` the quality fields will
  // be populated; when false they'll be null. `format` stays in the
  // guard because it's the fallback badge text when bitrate is null
  // (e.g. FLAC with no bitrate metadata).
  const hasOnDiskQuality = g.quality_label || g.min_bitrate
    || g.current_spectral_grade || g.format;
  if (!hasOnDiskQuality && !g.in_library) {
    return '<span class="badge" style="background:#3a2a2a;color:#f88;">nothing on disk</span>';
  }
  if (!hasOnDiskQuality) {
    // Defensive: in_library=true should imply quality fields are
    // set post-#123, but keep the empty-string return so a partial
    // dataset (e.g. beets row exists but items table is empty)
    // doesn't break the UI.
    return '';
  }

  const parts = [];
  const label = g.quality_label || (g.format ? String(g.format).toUpperCase() : null);
  if (label) {
    const color = rankColor(g.quality_rank || '');
    parts.push(`<span class="badge" style="background:#222;color:${color};border:1px solid ${color};">${esc(label)}</span>`);
  } else if (g.min_bitrate) {
    parts.push(`<span class="badge" style="background:#222;color:#aaa;">${g.min_bitrate}k</span>`);
  }
  if (g.verified_lossless) {
    parts.push('<span class="badge" style="background:#1a3a4a;color:#7cf;">verified lossless</span>');
  }
  // Spectral badge only when it's worth flagging.
  if (g.current_spectral_grade && g.current_spectral_grade !== 'genuine') {
    const sColor = g.current_spectral_grade === 'suspect' || g.current_spectral_grade === 'likely_transcode'
      ? '#f88' : '#da6';
    const suffix = g.current_spectral_bitrate ? ` (${g.current_spectral_bitrate}k)` : '';
    parts.push(`<span class="badge" style="background:#2a1a1a;color:${sColor};">${esc(g.current_spectral_grade)}${suffix}</span>`);
  }
  if (g.quality_rank) {
    const rColor = rankColor(g.quality_rank);
    parts.push(`<span class="badge" style="background:#1a1a1a;color:${rColor};font-family:monospace;font-size:0.72em;">${esc(g.quality_rank)}</span>`);
  }
  return parts.join(' ');
}

/**
 * Format an ISO timestamp as "YYYY-MM-DD HH:MM".
 * @param {string} iso
 * @returns {string}
 */
function fmtTs(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso);
    const pad = (/** @type {number} */ n) => n < 10 ? '0' + n : '' + n;
    return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  } catch (_e) {
    return iso;
  }
}

/**
 * Render the "Last import" header inside an expanded group. Shows the most
 * recent success/force_import/manual_import for the release — i.e. what's
 * actually on disk — not the newest attempt. A later rejection doesn't
 * change what beets has.
 * @param {any} d
 * @returns {string}
 */
function renderLatestImport(d) {
  if (!d) return '<div style="color:#555;font-size:0.78em;padding:4px 8px;">No successful import on disk.</div>';
  const fmtBr = d.actual_filetype ? `${String(d.actual_filetype).toUpperCase()}${d.actual_min_bitrate ? ' ' + d.actual_min_bitrate + 'k' : ''}` : '';
  return `
    <div style="background:#161616;border-left:3px solid #6d6;padding:6px 10px;margin:0 0 8px 0;font-size:0.78em;">
      <div style="color:#aaa;">
        <span style="color:#6d6;font-weight:600;">Last import: ${esc(d.outcome || '?')}</span>
        <span style="color:#666;margin-left:8px;">${esc(fmtTs(d.created_at))}</span>
      </div>
      <div style="color:#888;margin-top:2px;">
        ${d.soulseek_username ? 'user ' + esc(d.soulseek_username) : ''}
        ${fmtBr ? ' · ' + esc(fmtBr) : ''}
        ${d.beets_scenario ? ' · ' + esc(d.beets_scenario) : ''}
      </div>
    </div>`;
}

/**
 * Render one release group (collapsed by default).
 * @param {any} g - group payload
 * @returns {string}
 */
function renderGroup(g) {
  const groupId = `wm-group-${g.request_id}`;
  const count = g.pending_count || (g.entries ? g.entries.length : 0);
  const thresholdMilli = thresholdForGroup(g.request_id);
  const externalUrl = g.mb_release_id ? externalReleaseUrl(g.mb_release_id) : '';
  const releaseLabel = g.mb_release_id ? sourceLabel(g.mb_release_id) : '';
  const libBadge = g.in_library
    ? '<span class="badge" style="background:#2a4a2a;color:#6d6;">in library</span>'
    : '';
  const statusBadge = g.status && g.status !== 'imported'
    ? `<span class="badge" style="background:#2a2a3a;color:#9bf;">${esc(g.status)}</span>`
    : '';

  const header = `
    <div class="p-item" onclick="window.toggleWrongMatchGroup('${groupId}')">
      <div class="p-top">
        <div>
          <span class="p-title">${esc(g.artist)} — ${esc(g.album)}</span>
          <span class="badge badge-library">${count} candidate${count !== 1 ? 's' : ''}</span>
          ${libBadge}${statusBadge}
        </div>
      </div>
      <div class="p-meta" style="margin-top:4px;">
        ${renderQualityBadges(g)}
      </div>
      <div class="p-meta">
        ${g.mb_release_id && externalUrl && releaseLabel ? `<span>${releaseLabel}: <a href="${externalUrl}" target="_blank" style="color:#6af;" onclick="event.stopPropagation();">${esc(g.mb_release_id)}</a></span>` : ''}
      </div>
    </div>`;

  const entries = (g.entries || []).map((/** @type {any} */ e) => renderEntry(e, thresholdMilli, g.request_id)).join('');
  const latest = renderLatestImport(g.latest_import);
  const bulkActions = renderConvergeControls(g, count, thresholdMilli);

  return `<div id="wm-release-${g.request_id}" data-pending-count="${count}">
    ${header}
    <div class="p-detail" id="${groupId}">
      ${latest}
      ${bulkActions}
      <div style="padding:6px 0 0 0;">${entries}</div>
    </div>
  </div>`;
}

/**
 * Render release-level converge controls.
 * @param {any} g
 * @param {number} count
 * @param {number} thresholdMilli
 * @returns {string}
 */
function renderConvergeControls(g, count, thresholdMilli) {
  const greenCount = greenEntries(g, thresholdMilli).length;
  const disabled = greenCount === 0;
  const label = convergeButtonLabel(greenCount);
  const releaseName = esc(String(g.artist) + ' — ' + String(g.album));
  return `
    <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;margin:4px 0 0 0;padding:6px 8px;background:#151515;border:1px solid #242424;border-radius:4px;">
      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
        <label style="display:flex;align-items:center;gap:6px;color:#aaa;font-size:0.82em;">
          Loosen
          <input type="number" min="0" max="999" step="1" value="${thresholdMilli}" style="width:68px;background:#101010;color:#ddd;border:1px solid #333;border-radius:3px;padding:3px 5px;font-size:0.95em;" onclick="event.stopPropagation();" oninput="event.stopPropagation(); window.setWrongMatchConvergeThreshold(${g.request_id}, this.value)" onchange="event.stopPropagation(); window.setWrongMatchConvergeThreshold(${g.request_id}, this.value)">
        </label>
        <span id="wm-green-count-${g.request_id}" class="badge" style="${greenCountStyle(greenCount)}">${greenCountLabel(greenCount)}</span>
      </div>
      <div style="display:flex;align-items:center;gap:6px;">
        <button id="wm-converge-btn-${g.request_id}" class="p-btn" style="border-color:#6a9;color:#6a9;" ${disabled ? 'disabled' : ''} onclick="event.stopPropagation(); window.convergeWrongMatches(${g.request_id}, this)">${label}</button>
        <button class="p-btn delete" data-release-name="${releaseName}" onclick="event.stopPropagation(); window.deleteWrongMatchGroup(${g.request_id}, this)">Delete All (${count})</button>
      </div>
    </div>`;
}

/**
 * Render one rejected candidate inside a group.
 * @param {any} e - entry payload
 * @param {number} thresholdMilli
 * @param {number|string} requestId
 * @returns {string}
 */
function renderEntry(e, thresholdMilli, requestId) {
  const detailId = `wm-entry-${e.download_log_id}`;
  const distValue = distanceValue(e.distance);
  const dist = distValue != null ? distValue.toFixed(3) : '?';
  const job = e.import_job || null;
  const jobBadge = job ? `<span class="badge" style="background:#222;color:#9bf;margin-left:8px;">${esc(job.status)}</span>` : '';
  const green = isConvergeGreen(e, thresholdMilli);
  const distColor = green ? '#6d6' : '#aaa';

  const header = `
    <div id="wm-entry-card-${e.download_log_id}" class="p-item" data-request-id="${requestId}" data-distance="${distValue != null ? distValue : ''}" style="${entryItemStyle(green)}" onclick="window.toggleWrongMatchEntry('${detailId}')">
      <div class="p-top">
        <div>
          <span style="font-family:monospace;color:#aaa;">#${e.download_log_id}</span>
          <span style="color:#6a9;margin-left:8px;">${esc(e.soulseek_username || '?')}</span>
          <span id="wm-entry-green-${e.download_log_id}" class="badge" style="${entryGreenBadgeStyle(green)}">green</span>
          ${jobBadge}
        </div>
      </div>
      <div class="p-meta">
        <span id="wm-entry-dist-${e.download_log_id}" style="color:${distColor};">dist: ${dist}</span>
        <span>${esc(e.scenario || '')}</span>
      </div>
    </div>
    <div class="p-detail" id="${detailId}">
      ${renderEntryDetail(e, job)}
    </div>`;

  return header;
}

/**
 * Render expanded detail panel for one rejected candidate.
 * @param {Object} e - entry payload
 * @returns {string}
 */
function renderEntryDetail(e, job) {
  let html = '';
  const c = e.candidate;

  if (c) {
    html += `<div class="p-detail-row"><span class="p-detail-label">Matched</span><span class="p-detail-value">${esc(c.artist || '?')} — ${esc(c.album || '?')}${c.year ? ` (${c.year})` : ''}${c.country ? ` [${esc(c.country)}]` : ''}</span></div>`;
    if (c.label) html += `<div class="p-detail-row"><span class="p-detail-label">Label</span><span class="p-detail-value">${esc(c.label)}${c.catalognum ? ` / ${esc(c.catalognum)}` : ''}</span></div>`;
  }
  if (e.failed_path) {
    html += `<div class="p-detail-row"><span class="p-detail-label">Path</span><span class="p-detail-value" style="font-size:0.8em;">${esc(e.failed_path)}</span></div>`;
  }

  if (c) {
    const ALL_FIELDS = ['tracks', 'album', 'artist', 'album_id', 'year', 'country', 'label', 'catalognum', 'media', 'mediums', 'albumdisambig', 'missing_tracks', 'unmatched_tracks'];
    const bd = c.distance_breakdown || {};
    const nonZero = ALL_FIELDS.filter(f => (bd[f] || 0) > 0).sort((a, b) => (bd[b] || 0) - (bd[a] || 0));
    const zero = ALL_FIELDS.filter(f => !(bd[f] || 0));
    html += `<div style="margin-top:8px;"><span class="p-detail-label">Distance breakdown</span> <span style="color:#666;font-size:0.75em;">(total: ${e.distance != null ? e.distance.toFixed(3) : '?'})</span></div>`;
    html += '<div style="display:grid;grid-template-columns:auto 1fr auto;gap:2px 12px;font-size:0.8em;padding:4px 0 4px 8px;">';
    for (const field of nonZero) {
      const value = bd[field] || 0;
      const pct = e.distance ? Math.round((value / e.distance) * 100) : 0;
      const color = value > 0.05 ? '#f88' : '#da6';
      html += `<span style="color:#666;">${esc(field)}</span><span style="color:${color};">${value.toFixed(3)}</span><span style="color:#555;font-size:0.85em;">${pct}%</span>`;
    }
    html += '</div>';
    if (zero.length > 0) {
      html += `<div style="font-size:0.75em;color:#444;padding-left:8px;">Matched: ${zero.join(', ')}</div>`;
    }
  }

  if (c && c.mapping && c.mapping.length > 0) {
    html += `<div style="margin-top:10px;display:grid;grid-template-columns:1fr 1fr;gap:0 8px;font-size:0.78em;">`;
    html += `<div style="color:#6a9;font-weight:600;font-size:0.9em;padding-bottom:4px;">MB target</div>`;
    html += `<div style="color:#da6;font-weight:600;font-size:0.9em;padding-bottom:4px;">On disk</div>`;
    for (const m of c.mapping) {
      const mbNum = m.track?.medium_index || m.track?.index || '?';
      const mbTitle = m.track?.title || '?';
      const mbLen = m.track?.length ? fmtLen(m.track.length) : '';
      const localTitle = m.item?.title || m.item?.path || '?';
      const localLen = m.item?.length ? fmtLen(m.item.length) : '';
      const localFmt = m.item?.format ? ` ${m.item.format}` : '';
      const localBr = m.item?.bitrate ? ` ${Math.round(m.item.bitrate / 1000)}k` : '';
      const titleMatch = mbTitle.toLowerCase().replace(/\s*\(demo\)\s*/g, '').trim() === (localTitle || '').toLowerCase().trim();
      const mismatchStyle = titleMatch ? '' : 'color:#f88;';
      html += `<div style="padding:1px 0;color:#aaa;">${mbNum}. ${esc(mbTitle)} <span style="color:#555;">${mbLen}</span></div>`;
      html += `<div style="padding:1px 0;${mismatchStyle}">${esc(localTitle)}<span style="color:#555;"> ${localLen}${localFmt}${localBr}</span></div>`;
    }
    html += '</div>';
  }

  if (c && c.extra_items && c.extra_items.length > 0) {
    html += `<div style="margin-top:6px;font-size:0.78em;color:#da6;">Extra local files (${c.extra_items.length}):</div>`;
    html += '<div style="font-size:0.75em;padding-left:8px;color:#888;">';
    for (const item of c.extra_items) {
      html += `<div>${esc(item.title || item.path || '?')}</div>`;
    }
    html += '</div>';
  }

  if (c && c.extra_tracks && c.extra_tracks.length > 0) {
    html += `<div style="margin-top:6px;font-size:0.78em;color:#f88;">Missing MB tracks (${c.extra_tracks.length}):</div>`;
    html += '<div style="font-size:0.75em;padding-left:8px;color:#888;">';
    for (const t of c.extra_tracks) {
      const num = t.medium_index || t.index || t.track || '?';
      html += `<div>${num}. ${esc(t.title || '?')}</div>`;
    }
    html += '</div>';
  }

  html += '<div class="p-actions" style="margin-top:10px;">';
  const active = job && (job.status === 'queued' || job.status === 'running');
  const label = active ? job.status[0].toUpperCase() + job.status.slice(1) : 'Force Import';
  html += `<button class="p-btn" style="border-color:#6a9;color:#6a9;" ${active ? 'disabled' : ''} onclick="event.stopPropagation(); window.forceImportWrongMatch(${e.download_log_id}, this)">${label}</button>`;
  html += `<button class="p-btn delete" onclick="event.stopPropagation(); window.deleteWrongMatch(${e.download_log_id}, this)">Delete</button>`;
  html += '</div>';

  return html;
}

/**
 * Toggle a release group's expanded view.
 * @param {string} id
 */
export function toggleWrongMatchGroup(id) {
  const el = document.getElementById(id);
  if (el) el.classList.toggle('open');
}

/**
 * Toggle a single entry's expanded view.
 * @param {string} id
 */
export function toggleWrongMatchEntry(id) {
  const el = document.getElementById(id);
  if (el) el.classList.toggle('open');
}

/**
 * Re-fetch /api/wrong-matches and re-render in place. Used after any action
 * that can remove an entry or empty a whole group (force-import and delete
 * both move files off disk, which drops them from the list).
 *
 * Guarded against transient 5xx on the refresh: a failed refresh leaves the
 * DOM untouched and the cache invalidated, so the next tab switch retries
 * cleanly. Without this guard, an error payload would render as the empty
 * state and cache `_loaded = true`, erasing legitimate remaining rows.
 */
async function _refreshWrongMatches() {
  const el = document.getElementById('wrong-matches-content');
  if (!el) return;
  try {
    const fetchRes = await fetch(`${API}/api/wrong-matches`);
    if (fetchRes.ok) {
      const fresh = await fetchRes.json();
      renderWrongMatches(fresh, el);
      _loaded = true;
    }
  } catch (_refreshErr) {
    // Cache stays invalidated; next tab switch retries.
  }
}

/**
 * Poll a queued import job until it reaches a terminal state.
 * @param {number} jobId
 * @param {HTMLButtonElement} btn
 */
async function _pollImportJob(jobId, btn) {
  for (let i = 0; i < 240; i++) {
    await new Promise(resolve => setTimeout(resolve, 2000));
    try {
      const r = await fetch(`${API}/api/import-jobs/${jobId}`);
      if (!r.ok) continue;
      const data = await r.json();
      const job = data.job || {};
      if (job.status === 'queued' || job.status === 'running') {
        btn.textContent = job.status[0].toUpperCase() + job.status.slice(1);
        continue;
      }
      if (job.status === 'completed') {
        btn.textContent = 'Imported';
        btn.style.borderColor = '#6d6';
        btn.style.color = '#6d6';
        toast(job.message || 'Import completed');
        invalidateWrongMatches();
        await _refreshWrongMatches();
        return;
      }
      if (job.status === 'failed') {
        btn.textContent = 'Failed';
        btn.style.color = '#f88';
        toast(job.message || job.error || 'Import failed', true);
        invalidateWrongMatches();
        await _refreshWrongMatches();
        return;
      }
    } catch (_e) {
      // Keep polling through transient web/DB errors.
    }
  }
  btn.textContent = 'Queued';
}

export const __test__ = {
  pollImportJob: _pollImportJob,
  convergeRequestBody,
  convergeWrongMatches,
  deleteUnmatchedOnConverge,
  greenEntries,
  isConvergeGreen,
  normalizeThreshold,
  renderWrongMatches,
  setWrongMatchConvergeCleanup,
  setWrongMatchConvergeThreshold,
  thresholdForGroup,
};

/**
 * Queue every green candidate for a release and delete the rest.
 * @param {number} requestId
 * @param {HTMLButtonElement} btn
 */
export async function convergeWrongMatches(requestId, btn) {
  const group = ((_lastData && Array.isArray(_lastData.groups)) ? _lastData.groups : [])
    .find((/** @type {any} */ g) => Number(g.request_id) === Number(requestId));
  const thresholdMilli = thresholdForGroup(requestId);
  const greenCount = group ? greenEntries(group, thresholdMilli).length : 0;
  if (greenCount === 0) {
    toast('No candidates match the current loosen threshold', true);
    return;
  }

  btn.disabled = true;
  btn.textContent = 'Converging...';
  try {
    const r = await fetch(`${API}/api/wrong-matches/converge`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(convergeRequestBody(requestId, thresholdMilli, true)),
    });
    const data = await r.json();
    if (r.ok && data.status === 'ok') {
      toast(convergeToast(data));
      invalidateWrongMatches();
      if (data.group_empty) {
        removeWrongMatchGroup(requestId);
      } else {
        await _refreshWrongMatches();
      }
    } else {
      btn.disabled = false;
      btn.textContent = 'Converge';
      toast(data.message || 'Converge failed', true);
    }
  } catch (_e) {
    btn.disabled = false;
    btn.textContent = 'Converge';
    toast('Converge request failed', true);
  }
}

/**
 * Force-import a wrong match.
 * @param {number} logId
 * @param {HTMLButtonElement} btn
 */
export async function forceImportWrongMatch(logId, btn) {
  if (!confirm('Force-import this wrong match? This bypasses the distance check.')) return;
  btn.disabled = true;
  btn.textContent = 'Importing...';
  try {
    const r = await fetch(`${API}/api/pipeline/force-import`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({download_log_id: logId}),
    });
    const data = await r.json();
    if (data.status === 'queued') {
      btn.textContent = data.deduped ? 'Queued' : 'Queued';
      btn.style.borderColor = '#9bf';
      btn.style.color = '#9bf';
      toast(`Queued import: ${data.artist} - ${data.album}`);
      if (data.job_id) {
        await _pollImportJob(data.job_id, btn);
      }
    } else {
      btn.textContent = 'Failed';
      btn.style.color = '#f88';
      toast(data.message || 'Force import failed', true);
    }
  } catch (e) {
    btn.textContent = 'Error';
    toast('Force import request failed', true);
  }
}

/**
 * Delete every wrong-match candidate for one release at once.
 * @param {number} requestId
 * @param {HTMLButtonElement} btn - the clicked button; carries data-release-name for the confirm dialog
 */
export async function deleteWrongMatchGroup(requestId, btn) {
  const releaseName = btn.dataset.releaseName || 'this release';
  if (!confirm(`Delete ALL wrong-match candidates for "${releaseName}"?\nThis removes the files from disk and clears them from the review queue.`)) return;
  btn.disabled = true;
  btn.textContent = 'Deleting…';
  try {
    const r = await fetch(`${API}/api/wrong-matches/delete-group`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({request_id: requestId}),
    });
    const data = await r.json();
    if (data.status === 'ok') {
      toast(`Deleted ${data.deleted} candidate${data.deleted !== 1 ? 's' : ''} for ${releaseName}`);
      invalidateWrongMatches();
      await _refreshWrongMatches();
    } else {
      btn.textContent = 'Failed';
      toast('Delete-all failed', true);
    }
  } catch (e) {
    btn.textContent = 'Error';
    toast('Delete-all request failed', true);
  }
}

/**
 * Delete a wrong match (files + clear DB path).
 * @param {number} logId
 * @param {HTMLButtonElement} btn
 */
export async function deleteWrongMatch(logId, btn) {
  if (!confirm('Delete files and dismiss this wrong match?')) return;
  btn.disabled = true;
  btn.textContent = 'Deleting...';
  try {
    const r = await fetch(`${API}/api/wrong-matches/delete`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({download_log_id: logId}),
    });
    const data = await r.json();
    if (data.status === 'ok') {
      toast('Wrong match deleted');
      invalidateWrongMatches();
      await _refreshWrongMatches();
    } else {
      btn.textContent = 'Failed';
      toast('Delete failed', true);
    }
  } catch (e) {
    btn.textContent = 'Error';
    toast('Delete request failed', true);
  }
}
