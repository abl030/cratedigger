// @ts-check
import { API, toast } from './state.js';
import { esc, externalReleaseUrl, sourceLabel } from './util.js';
import { renderReplaceButton } from './release_actions.js';

/** @type {boolean} */
let _loaded = false;
/** @type {Object | null} */
let _lastData = null;
/** @type {HTMLElement | null} */
let _lastEl = null;
/** @type {Map<number, 'loading'|'loaded'>} */
const _entryExplorerState = new Map();

const DEFAULT_CONVERGE_THRESHOLD_MILLI = 180;
const CONVERGE_THRESHOLD_KEY_PREFIX = 'wrongMatches.converge.threshold.';
const EXPLORER_SHARED_TAG_PRIORITY = ['albumartist', 'artist', 'album', 'date', 'genre', 'catalognumber', 'label', 'comment', 'discnumber', 'totaltracks'];
const EXPLORER_TRACK_TAG_KEYS = new Set(['title', 'tracknumber']);
const MUSICBRAINZ_TAG_ENTITY_PATH = {
  musicbrainz_albumartistid: 'artist',
  musicbrainz_albumid: 'release',
  musicbrainz_artistid: 'artist',
  musicbrainz_releasegroupid: 'release-group',
  musicbrainz_releasetrackid: 'track',
  musicbrainz_trackid: 'recording',
  musicbrainz_workid: 'work',
};

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
 * Format a byte count as a short human-readable string.
 * @param {number} bytes
 * @returns {string}
 */
function fmtBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes < 0) return '';
  const units = ['B', 'KB', 'MB', 'GB'];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  const rounded = (value >= 10 || unit === 0) ? String(Math.round(value)) : value.toFixed(1);
  return `${rounded} ${units[unit]}`;
}

/**
 * @param {any} entry
 * @returns {string[]}
 */
function sourceDirsForEntry(entry) {
  return Array.isArray(entry?.source_dirs)
    ? entry.source_dirs.filter((/** @type {unknown} */ sourceDir) => (
      typeof sourceDir === 'string' && sourceDir.trim()
    ))
    : [];
}

/**
 * @param {unknown} raw
 * @returns {string[]}
 */
function cleanedTagValues(raw) {
  const values = Array.isArray(raw) ? raw : [raw];
  return values.filter((/** @type {unknown} */ value) => (
    typeof value === 'string' && value.trim()
  ));
}

/**
 * @param {Record<string, string[]>} tags
 * @param {string[]} preferred
 * @returns {string[]}
 */
function orderedTagKeys(tags, preferred = []) {
  const all = Object.keys(tags || {});
  const seen = new Set();
  const ordered = [];
  for (const key of preferred) {
    if (all.includes(key) && !seen.has(key)) {
      ordered.push(key);
      seen.add(key);
    }
  }
  for (const key of all.sort()) {
    if (!seen.has(key)) ordered.push(key);
  }
  return ordered;
}

/**
 * @param {Record<string, string[]>} tags
 * @returns {Record<string, string[]>}
 */
function visibleExplorerTags(tags) {
  /** @type {Record<string, string[]>} */
  const visible = {};
  for (const [rawKey, rawValue] of Object.entries(tags || {})) {
    const key = String(rawKey).toLowerCase();
    if (key.startsWith('replaygain_')) continue;
    const values = cleanedTagValues(rawValue);
    if (values.length === 0) continue;
    visible[key] = values;
  }
  return visible;
}

/**
 * @param {string[]|undefined} values
 * @returns {string}
 */
function tagValueText(values) {
  return Array.isArray(values) ? values.join(' · ') : '';
}

/**
 * @param {string} key
 * @param {string} value
 * @returns {string}
 */
function explorerTagValueUrl(key, value) {
  const normalizedKey = String(key || '').toLowerCase();
  const normalizedValue = String(value || '').trim();
  const mbPath = MUSICBRAINZ_TAG_ENTITY_PATH[normalizedKey];
  if (mbPath && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(normalizedValue)) {
    return `https://musicbrainz.org/${mbPath}/${normalizedValue.toLowerCase()}`;
  }
  if (/^https?:\/\//i.test(normalizedValue)) return normalizedValue;
  return '';
}

/**
 * @param {string} key
 * @param {string[]|undefined} values
 * @returns {string}
 */
function renderExplorerTagValues(key, values) {
  if (!Array.isArray(values) || values.length === 0) return '';
  return values.map((value) => {
    const url = explorerTagValueUrl(key, value);
    if (!url) return esc(value);
    return `<a href="${esc(url)}" target="_blank" rel="noopener" style="color:#6af;" onclick="event.stopPropagation();">${esc(value)}</a>`;
  }).join(' · ');
}

/**
 * @param {Record<string, string[]>} tags
 * @param {string} key
 * @returns {string}
 */
function firstTagValue(tags, key) {
  const values = tags[key];
  return Array.isArray(values) && values.length > 0 ? values[0] : '';
}

/**
 * @param {any[]} files
 * @returns {Record<string, string[]>}
 */
function sharedExplorerTags(files) {
  if (!Array.isArray(files) || files.length === 0) return {};
  const perFileTags = files.map((/** @type {any} */ file) => (
    visibleExplorerTags((file?.tags && typeof file.tags === 'object') ? file.tags : {})
  ));
  const first = perFileTags[0] || {};
  /** @type {Record<string, string[]>} */
  const shared = {};
  for (const key of orderedTagKeys(first, EXPLORER_SHARED_TAG_PRIORITY)) {
    if (EXPLORER_TRACK_TAG_KEYS.has(key)) continue;
    const firstText = tagValueText(first[key]);
    if (!firstText) continue;
    if (perFileTags.every((/** @type {Record<string, string[]>} */ fileTags) => (
      tagValueText(fileTags[key]) === firstText
    ))) {
      shared[key] = first[key];
    }
  }
  return shared;
}

/**
 * @param {Record<string, string[]>} tags
 * @returns {string}
 */
function renderExplorerTagGrid(tags) {
  const tagKeys = orderedTagKeys(tags, EXPLORER_SHARED_TAG_PRIORITY);
  if (tagKeys.length === 0) return '';
  return `
    <div style="display:grid;grid-template-columns:auto 1fr;gap:4px 10px;font-size:0.76em;margin-top:8px;">
      ${tagKeys.map((key) => (
        `<div style="color:#666;">${esc(key)}</div><div style="color:#aaa;">${renderExplorerTagValues(key, tags[key])}</div>`
      )).join('')}
    </div>`;
}

/**
 * @param {any} file
 * @returns {string}
 */
function renderWrongMatchExplorerFile(file) {
  const bits = [];
  if (file?.format) bits.push(String(file.format).toUpperCase());
  if (Number.isFinite(file?.bitrate_kbps)) bits.push(`${file.bitrate_kbps} kbps`);
  if (Number.isFinite(file?.duration_seconds)) bits.push(fmtLen(file.duration_seconds));
  if (Number.isFinite(file?.size_bytes)) bits.push(fmtBytes(file.size_bytes));

  const tags = visibleExplorerTags((file?.tags && typeof file.tags === 'object') ? file.tags : {});
  const trackNumber = firstTagValue(tags, 'tracknumber');
  const title = firstTagValue(tags, 'title') || String(file?.relative_path || file?.filename || '?');
  const summary = bits.length > 0 ? bits.join(' · ') : 'Unknown audio file';
  let html = `
    <div style="margin-top:6px;padding:8px 10px;background:#131313;border:1px solid #262626;border-radius:4px;">
      <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;">
        <div style="min-width:0;flex:1 1 220px;">
          <div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap;min-width:0;">
            ${trackNumber ? `<span style="color:#6a9;font-family:monospace;font-size:0.78em;">${esc(trackNumber)}</span>` : ''}
            <span style="color:#ddd;font-size:0.82em;min-width:0;overflow-wrap:anywhere;">${esc(title)}</span>
          </div>
          <div style="color:#666;font-size:0.74em;margin-top:2px;">${esc(summary)}</div>
        </div>`;

  if (file?.playable && file?.stream_url) {
    html += `
      <div style="flex:1 1 280px;min-width:220px;max-width:420px;">
        <audio controls preload="none" src="${esc(file.stream_url)}" style="width:100%;" onclick="event.stopPropagation();"></audio>
      </div>`;
  } else {
    html += '<div style="color:#666;font-size:0.76em;">Browser playback unavailable</div>';
  }

  html += '</div></div>';
  return html;
}

/**
 * @param {any} data
 * @returns {string}
 */
function renderWrongMatchExplorer(data) {
  const files = Array.isArray(data?.files) ? data.files : [];
  const otherFileCount = Number.isFinite(data?.other_file_count) ? data.other_file_count : 0;
  const audioFileCount = Number.isFinite(data?.audio_file_count) ? data.audio_file_count : files.length;
  const sourceDirs = sourceDirsForEntry(data);
  const sharedTags = sharedExplorerTags(files);
  const orderedBy = typeof data?.ordered_by === 'string' ? data.ordered_by : 'folder';
  let summary = '';
  if (sourceDirs.length > 0 || Object.keys(sharedTags).length > 0) {
    const parts = [];
    if (sourceDirs.length > 0) {
      parts.push(`
        <div>
          <div style="color:#666;">Downloaded as</div>
          <div style="color:#aaa;">${sourceDirs.map((dir) => esc(dir)).join('<br>')}</div>
        </div>`);
    }
    summary = `
      <div style="margin:6px 0 10px 0;">
        ${parts.length > 0 ? `<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:6px 12px;font-size:0.76em;">${parts.join('')}</div>` : ''}
        ${renderExplorerTagGrid(sharedTags)}
      </div>`;
  }
  if (files.length === 0) {
    return `${summary}<div style="color:#666;font-size:0.78em;padding:8px 0;">No audio files found in this folder.</div>`;
  }

  let html = `
    <div style="margin-top:10px;">
      ${summary}
      <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;">
        <div style="color:#888;font-size:0.78em;">${audioFileCount} track${audioFileCount === 1 ? '' : 's'} in surviving folder${orderedBy === 'matched' ? ' in matched order' : ''}</div>
        ${otherFileCount > 0 ? `<div style="color:#666;font-size:0.74em;">${otherFileCount} non-audio file${otherFileCount === 1 ? '' : 's'} hidden</div>` : ''}
      </div>
      ${files.map(renderWrongMatchExplorerFile).join('')}
    </div>`;
  return html;
}

/**
 * @param {number} logId
 * @returns {Promise<void>}
 */
async function ensureWrongMatchExplorer(logId) {
  const mount = document.getElementById(`wm-explorer-${logId}`);
  if (!mount) return;
  const state = _entryExplorerState.get(logId);
  if (state === 'loading' || state === 'loaded') return;

  _entryExplorerState.set(logId, 'loading');
  mount.innerHTML = '<div style="color:#666;font-size:0.78em;padding:8px 0;">Loading file explorer…</div>';
  try {
    const r = await fetch(`${API}/api/wrong-matches/explorer?download_log_id=${encodeURIComponent(String(logId))}`);
    const data = await r.json();
    if (!r.ok || data.status !== 'ok') {
      throw new Error(data.error || data.message || 'Explorer load failed');
    }
    mount.innerHTML = renderWrongMatchExplorer(data);
    _entryExplorerState.set(logId, 'loaded');
  } catch (_e) {
    _entryExplorerState.delete(logId);
    mount.innerHTML = `<div style="color:#f88;font-size:0.78em;padding:8px 0;">Failed to load file explorer. <button class="p-btn" style="margin-left:6px;" onclick="event.stopPropagation(); window.reloadWrongMatchExplorer(${logId})">Retry</button></div>`;
  }
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
    // U10: opt-in toggle persists in localStorage. Default: filtered.
    const includeReplaced = localStorage.getItem('wrongMatches.includeReplaced') === 'true';
    const url = `${API}/api/wrong-matches${includeReplaced ? '?include_replaced=true' : ''}`;
    const r = await fetch(url);
    const data = await r.json();
    _loaded = true;
    renderWrongMatches(data, el);
  } catch (e) {
    el.innerHTML = '<div style="color:#f66;">Failed to load wrong matches</div>';
  }
}

/**
 * Toggle "show replaced" filter (U10). Re-fetches with the new flag.
 */
export function toggleWrongMatchesReplacedFilter() {
  const current = localStorage.getItem('wrongMatches.includeReplaced') === 'true';
  localStorage.setItem('wrongMatches.includeReplaced', String(!current));
  _loaded = false;
  loadWrongMatches();
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
 * @returns {{request_id: number, threshold_milli: number, delete_unmatched: boolean}}
 */
function convergeRequestBody(requestId, thresholdMilli) {
  return {
    request_id: Number(requestId),
    threshold_milli: normalizeThreshold(thresholdMilli),
    delete_unmatched: true,
  };
}

/**
 * Format the per-candidate stored evidence cells for a wrong-match
 * entry. Pure — input is the entry payload, output is a `{format,
 * spectral, v0}` triple of short display strings. The format cell
 * reads the canonical evidence row (storage_format + min_bitrate kbps)
 * surfaced by /api/wrong-matches via album_quality_evidence; the
 * candidate row never starts a preview job from the UI (R3) and never
 * exposes a preview button.
 * @param {any} entry
 * @returns {{format: string, spectral: string, v0: string}}
 */
function formatEntryEvidence(entry) {
  const fmt = entry && typeof entry.format === 'string' && entry.format
    ? entry.format : null;
  const minBr = entry && Number.isFinite(entry.min_bitrate)
    ? entry.min_bitrate : null;
  let format = '—';
  if (fmt && minBr != null && minBr > 0) format = `${fmt} ${minBr}k`;
  else if (fmt) format = fmt;
  else if (minBr != null && minBr > 0) format = `${minBr}k`;

  const grade = entry && typeof entry.spectral_grade === 'string'
    ? entry.spectral_grade : null;
  const bitrate = entry && Number.isFinite(entry.spectral_bitrate)
    ? entry.spectral_bitrate : null;
  let spectral = '—';
  if (grade && bitrate != null) spectral = `${grade} · ${bitrate} kbps`;
  else if (grade) spectral = grade;
  else if (bitrate != null) spectral = `${bitrate} kbps`;

  const kind = entry && typeof entry.v0_probe_kind === 'string'
    ? entry.v0_probe_kind : null;
  const avg = entry && Number.isFinite(entry.v0_probe_avg_bitrate)
    ? entry.v0_probe_avg_bitrate : null;
  // Surface V0 probe data whenever it exists. Lossless-source probes
  // are the most actionable (they tell you what a transcode would cost),
  // but research probes for native-lossy / on-disk are still useful at
  // the manual-review surface where the operator wants to compare candidates.
  const v0 = (avg != null) ? `V0 ≈ ${avg} kbps` : '—';
  return { format, spectral, v0 };
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
 * @param {any} data
 * @returns {string}
 */
function cleanupSummaryToast(data) {
  const deleted = Number(data?.deleted || 0)
    + Number(data?.deleted_verified_lossless_parent || 0);
  const kept = Number(data?.kept_would_import || 0)
    + Number(data?.kept_uncertain || 0);
  const skipped = Number(data?.skipped_candidate_evidence_missing || 0)
    + Number(data?.skipped_candidate_evidence_stale || 0)
    + Number(data?.skipped_current_evidence_missing || 0)
    + Number(data?.skipped_current_evidence_stale || 0)
    + Number(data?.skipped_current_evidence_failed || 0)
    + Number(data?.skipped_active_job || 0)
    + Number(data?.skipped_invalid_row || 0)
    + Number(data?.skipped_missing_path || 0)
    + Number(data?.skipped_operational || 0)
    + Number(data?.delete_failed || 0);
  return `Deleted ${deleted} candidate${deleted === 1 ? '' : 's'}, kept ${kept}, skipped ${skipped}`;
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
 * Remove one candidate entry from the current DOM and the in-memory cache.
 * Updates the parent group's count badge; if the group hits zero candidates,
 * removes the whole group. Preserves scroll position and other expanded state.
 * @param {number|string} logId
 */
function removeWrongMatchEntry(logId) {
  const id = Number(logId);
  if (!Number.isFinite(id)) return;
  _entryExplorerState.delete(id);
  /** @type {any | null} */
  let owningGroup = null;
  if (_lastData && Array.isArray(_lastData.groups)) {
    for (const g of _lastData.groups) {
      const entries = Array.isArray(g.entries) ? g.entries : [];
      const idx = entries.findIndex((/** @type {any} */ e) => Number(e.download_log_id) === id);
      if (idx !== -1) {
        entries.splice(idx, 1);
        g.entries = entries;
        if (typeof g.pending_count === 'number') g.pending_count = Math.max(0, g.pending_count - 1);
        owningGroup = g;
        break;
      }
    }
  }
  const card = document.getElementById(`wm-entry-card-${id}`);
  if (card && typeof card.remove === 'function') card.remove();
  if (owningGroup) {
    const remaining = (owningGroup.pending_count != null)
      ? owningGroup.pending_count
      : (Array.isArray(owningGroup.entries) ? owningGroup.entries.length : 0);
    if (remaining <= 0) {
      removeWrongMatchGroup(owningGroup.request_id);
    } else {
      const release = document.getElementById(`wm-release-${owningGroup.request_id}`);
      if (release) release.setAttribute('data-pending-count', String(remaining));
      const badge = release ? release.querySelector('.badge-library') : null;
      if (badge) badge.textContent = `${remaining} candidate${remaining !== 1 ? 's' : ''}`;
      const groupDeleteBtn = document.getElementById(`wm-delete-group-btn-${owningGroup.request_id}`);
      if (groupDeleteBtn) groupDeleteBtn.textContent = `Delete All (${remaining})`;
      updateWrongMatchesSummary();
    }
  } else {
    updateWrongMatchesSummary();
  }
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
 * Render grouped wrong-match entries (issue #113).
 * Top level = one collapsed card per release; expand reveals every rejected
 * candidate that still has files on disk.
 * @param {Object} data
 * @param {HTMLElement} el
 */
function renderWrongMatches(data, el) {
  _lastData = data;
  _lastEl = el;
  _entryExplorerState.clear();
  /** @type {any[]} */
  const groups = (data.groups || []).filter((/** @type {any} */ g) => (g.pending_count || 0) > 0);
  if (groups.length === 0) {
    el.innerHTML = '<div style="color:#888;padding:12px;">No wrong matches in failed_imports.</div>';
    return;
  }

  const counts = wrongMatchCounts(groups);
  let html = `
    <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;margin:8px 0;">
      <div id="wrong-matches-summary" style="color:#888;">${counts.groups} release${counts.groups !== 1 ? 's' : ''} · ${counts.entries} candidate${counts.entries !== 1 ? 's' : ''} pending review</div>
      <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
        <button id="wm-refresh-btn" class="p-btn" style="border-color:#888;color:#888;" onclick="event.stopPropagation(); window.refreshWrongMatches(this)" title="Refetch the queue from the server">Refresh</button>
        <button id="wm-bulk-triage-btn" class="p-btn delete" ${counts.entries === 0 ? 'disabled' : ''} onclick="event.stopPropagation(); window.bulkTriageWrongMatches(this)">Cleanup Wrong Matches (${counts.entries})</button>
      </div>
    </div>`;

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
 *
 * When `latest_import` is absent the header distinguishes three states using
 * the group's `in_library` / `verified_lossless` flags so the operator knows
 * whether a new candidate has to beat anything to land.
 *
 * @param {any} d - latest_import payload, or null/undefined
 * @param {{in_library?: boolean, verified_lossless?: boolean}} [group]
 * @returns {string}
 */
function renderLatestImport(d, group) {
  if (d) {
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
  const inLibrary = !!(group && group.in_library);
  const verifiedLossless = !!(group && group.verified_lossless);
  if (inLibrary && verifiedLossless) {
    return '<div style="color:#6d6;font-size:0.78em;padding:4px 8px;">Verified-lossless copy in library — Wrong Matches against this album are cleared on the next cleanup sweep.</div>';
  }
  if (inLibrary) {
    return '<div style="color:#9bf;font-size:0.78em;padding:4px 8px;">Album already in library — any new candidate must beat current quality to import.</div>';
  }
  return '<div style="color:#555;font-size:0.78em;padding:4px 8px;">No previous import on disk.</div>';
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
  const latest = renderLatestImport(g.latest_import, g);
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
        <button id="wm-delete-group-btn-${g.request_id}" class="p-btn delete" onclick="event.stopPropagation(); window.deleteWrongMatchGroup(${g.request_id}, this)">Delete All (${count})</button>
        <button id="wm-converge-btn-${g.request_id}" class="p-btn" style="border-color:#6a9;color:#6a9;" ${disabled ? 'disabled' : ''} onclick="event.stopPropagation(); window.convergeWrongMatches(${g.request_id}, this)">${label}</button>
        ${renderReplaceButton({
          mode: 'standard',
          sourceRequestId: g.request_id,
          releaseGroupId: g.mb_release_group_id || null,
          sourceLabel: `${g.artist || ''} — ${g.album || ''}`,
        }, { className: 'p-btn', stopPropagation: true })}
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
  const evidence = formatEntryEvidence(e);

  // Rank badge mirrors the group-header palette so operators can sort
  // candidates visually. Sort order is server-side (best first); the
  // badge just reinforces it. verified_lossless gets its own marker
  // since FLAC can show up before/after we know it's actually lossless.
  const rank = typeof e.quality_rank === 'string' ? e.quality_rank : '';
  const rankBadge = rank && rank !== 'unknown'
    ? `<span class="badge" style="background:#1a1a1a;color:${rankColor(rank)};font-family:monospace;font-size:0.72em;margin-left:6px;">${esc(rank)}</span>`
    : '';
  const verifiedBadge = e.verified_lossless
    ? '<span class="badge" style="background:#1a2a1a;color:#6d6;margin-left:6px;">verified lossless</span>'
    : '';

  const header = `
    <div id="wm-entry-card-${e.download_log_id}" class="p-item" data-request-id="${requestId}" data-distance="${distValue != null ? distValue : ''}" style="${entryItemStyle(green)}" onclick="window.toggleWrongMatchEntry('${detailId}', ${e.download_log_id})">
      <div class="p-top">
        <div>
          <span style="font-family:monospace;color:#aaa;">#${e.download_log_id}</span>
          <span style="color:#6a9;margin-left:8px;">${esc(e.soulseek_username || '?')}</span>
          <span id="wm-entry-green-${e.download_log_id}" class="badge" style="${entryGreenBadgeStyle(green)}">green</span>
          ${rankBadge}${verifiedBadge}${jobBadge}
        </div>
      </div>
      <div class="p-meta">
        <span id="wm-entry-dist-${e.download_log_id}" style="color:${distColor};">dist: ${dist}</span>
        <span>${esc(e.scenario || '')}</span>
        <span style="color:#bbb;">${esc(evidence.format)}</span>
        <span style="color:#888;">spectral: ${esc(evidence.spectral)}</span>
        <span style="color:#888;">${esc(evidence.v0)}</span>
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
  const c = e.candidate;
  const sourceDirs = sourceDirsForEntry(e);

  // Action buttons up top: operators are usually here to act, not browse.
  const active = job && (job.status === 'queued' || job.status === 'running');
  const importLabel = active ? job.status[0].toUpperCase() + job.status.slice(1) : 'Force Import';
  let html = '<div class="p-actions" style="margin-bottom:10px;">';
  html += `<button class="p-btn" style="border-color:#6a9;color:#6a9;" ${active ? 'disabled' : ''} onclick="event.stopPropagation(); window.forceImportWrongMatch(${e.download_log_id}, this)">${importLabel}</button>`;
  html += `<button class="p-btn delete" ${active ? 'disabled' : ''} onclick="event.stopPropagation(); window.deleteWrongMatch(${e.download_log_id}, this)">Delete</button>`;
  html += '</div>';

  if (c) {
    html += `<div class="p-detail-row"><span class="p-detail-label">Matched</span><span class="p-detail-value">${esc(c.artist || '?')} — ${esc(c.album || '?')}${c.year ? ` (${c.year})` : ''}${c.country ? ` [${esc(c.country)}]` : ''}</span></div>`;
    if (c.label) html += `<div class="p-detail-row"><span class="p-detail-label">Label</span><span class="p-detail-value">${esc(c.label)}${c.catalognum ? ` / ${esc(c.catalognum)}` : ''}</span></div>`;
  }
  if (sourceDirs.length > 0) {
    html += `<div class="p-detail-row"><span class="p-detail-label">Downloaded as</span><span class="p-detail-value" style="font-size:0.8em;">${sourceDirs.map((dir) => esc(dir)).join('<br>')}</span></div>`;
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

  // File explorer (tags + per-file audio playback) lives behind its own
  // disclosure so the entry expand stays cheap and the playback UI doesn't
  // clutter the view. Lazy-loads on first open via the toggle handler.
  html += `
    <details class="wm-explorer-details" style="margin-top:10px;" ontoggle="window.maybeLoadWrongMatchExplorer(${e.download_log_id}, this)">
      <summary style="cursor:pointer;color:#6a9;font-weight:600;font-size:0.82em;list-style:none;">▸ File explorer &amp; playback</summary>
      <div id="wm-explorer-${e.download_log_id}" style="margin-top:4px;color:#555;font-size:0.78em;">Loading…</div>
    </details>`;

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
 * @param {number=} logId
 */
export async function toggleWrongMatchEntry(id, logId) {
  const el = document.getElementById(id);
  if (!el) return;
  el.classList.toggle('open');
  // Note: file explorer no longer auto-loads on entry expand — it lives behind
  // its own <details> disclosure inside the entry. The logId parameter is kept
  // for backward compatibility with the renderEntry call site.
  void logId;
}

/**
 * Lazy-loader for the per-entry <details>-wrapped file explorer disclosure.
 * Fires from the <details> element's ontoggle handler — loads the explorer
 * data on first open, no-ops on subsequent toggles.
 * @param {number} logId
 * @param {HTMLDetailsElement} detailsEl
 */
export async function maybeLoadWrongMatchExplorer(logId, detailsEl) {
  if (!detailsEl || !detailsEl.open) return;
  const id = Number(logId);
  if (!Number.isFinite(id)) return;
  await ensureWrongMatchExplorer(id);
}

/**
 * @param {number} logId
 */
export async function reloadWrongMatchExplorer(logId) {
  const normalized = Number(logId);
  if (!Number.isFinite(normalized)) return;
  _entryExplorerState.delete(normalized);
  await ensureWrongMatchExplorer(normalized);
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
 * Operator-triggered queue refresh — exposed for the toolbar's Refresh button.
 * @param {HTMLButtonElement=} btn
 */
export async function refreshWrongMatches(btn) {
  const originalLabel = btn ? btn.textContent : '';
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Refreshing...';
  }
  try {
    invalidateWrongMatches();
    await _refreshWrongMatches();
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalLabel || 'Refresh';
    }
  }
}

/**
 * Poll a queued import job until it reaches a terminal state.
 * @param {number} jobId
 * @param {HTMLButtonElement} btn
 * @param {number=} logId — the download_log row the import targets; used to
 *   surgically remove the row from the queue on completion. When omitted,
 *   completion just toasts and updates the button without touching the DOM.
 */
async function _pollImportJob(jobId, btn, logId) {
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
        // Import succeeded → row leaves the Wrong Matches queue. Surgical
        // remove preserves scroll position and surrounding expanded state.
        if (Number.isFinite(logId)) {
          removeWrongMatchEntry(Number(logId));
        }
        return;
      }
      if (job.status === 'failed') {
        btn.textContent = 'Failed';
        btn.style.color = '#f88';
        toast(job.message || job.error || 'Import failed', true);
        invalidateWrongMatches();
        // Don't refetch: failed imports may have cleaned up the source folder
        // (confident_reject) OR left it intact (transient failure). Either way
        // the row state is ambiguous; operator can hit Refresh if they want
        // to reconcile. Refetching on every failed import was the jarring
        // post-Force-Import refresh.
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
  bulkTriageWrongMatches,
  cleanupSummaryToast,
  convergeRequestBody,
  convergeWrongMatches,
  deleteWrongMatch,
  deleteWrongMatchGroup,
  deleteUnmatchedOnConverge,
  formatEntryEvidence,
  greenEntries,
  isConvergeGreen,
  maybeLoadWrongMatchExplorer,
  normalizeThreshold,
  refreshWrongMatches,
  reloadWrongMatchExplorer,
  removeWrongMatchEntry,
  removeWrongMatchGroup,
  renderLatestImport,
  renderWrongMatchExplorer,
  renderWrongMatches,
  setWrongMatchConvergeThreshold,
  thresholdForGroup,
  toggleWrongMatchEntry,
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
      body: JSON.stringify(convergeRequestBody(requestId, thresholdMilli)),
    });
    const data = await r.json();
    if (r.ok && data.status === 'ok') {
      toast(convergeToast(data));
      invalidateWrongMatches();
      if (data.group_empty) {
        removeWrongMatchGroup(requestId);
      } else {
        // Surgical: remove every unmatched row that actually got deleted (i.e.
        // not in the skipped list). Green rows are queued for force-import and
        // stay visible until their job-poller completes.
        const skippedIds = new Set((data.skipped || [])
          .map((/** @type {any} */ s) => Number(s.download_log_id))
          .filter((/** @type {number} */ id) => Number.isFinite(id)));
        for (const u of (data.unmatched || [])) {
          const id = Number(u.download_log_id);
          if (Number.isFinite(id) && !skippedIds.has(id)) {
            removeWrongMatchEntry(id);
          }
        }
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
        await _pollImportJob(data.job_id, btn, logId);
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
 * Delete one wrong-match source folder and remove it from review.
 * @param {number} logId
 * @param {HTMLButtonElement} btn
 */
export async function deleteWrongMatch(logId, btn) {
  if (!confirm('Delete this wrong-match source folder?')) return;
  btn.disabled = true;
  btn.textContent = 'Deleting...';
  try {
    const r = await fetch(`${API}/api/wrong-matches/delete`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({download_log_id: logId}),
    });
    const data = await r.json();
    if (r.ok && data.status === 'ok') {
      toast(data.path_missing ? 'Cleared missing wrong match' : 'Deleted wrong match');
      invalidateWrongMatches();
      removeWrongMatchEntry(logId);
    } else {
      btn.disabled = false;
      btn.textContent = 'Delete';
      toast(data.error || data.message || 'Delete failed', true);
    }
  } catch (_e) {
    btn.disabled = false;
    btn.textContent = 'Delete';
    toast('Delete request failed', true);
  }
}

/**
 * Delete every current wrong-match source folder for one release group.
 * @param {number} requestId
 * @param {HTMLButtonElement} btn
 */
export async function deleteWrongMatchGroup(requestId, btn) {
  const group = ((_lastData && Array.isArray(_lastData.groups)) ? _lastData.groups : [])
    .find((/** @type {any} */ g) => Number(g.request_id) === Number(requestId));
  const count = group ? (group.pending_count || (group.entries ? group.entries.length : 0)) : 0;
  if (!confirm(`Delete all ${count} wrong-match candidate source folders for this release?`)) return;

  btn.disabled = true;
  btn.textContent = 'Deleting...';
  try {
    const r = await fetch(`${API}/api/wrong-matches/delete-group`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({request_id: requestId}),
    });
    const data = await r.json();
    if (r.ok && (data.status === 'ok' || data.status === 'partial')) {
      const skipped = data.skipped ? ` · skipped ${data.skipped}` : '';
      const errors = data.errors ? ` · errors ${data.errors}` : '';
      toast(`Deleted ${data.deleted || 0} candidates${skipped}${errors}`);
      invalidateWrongMatches();
      if (data.status === 'ok' || (data.remaining === 0)) {
        removeWrongMatchGroup(requestId);
      } else {
        // Partial outcome: remove the rows that actually deleted, leave the
        // skipped/errored rows visible so the operator can see what failed.
        for (const result of (data.results || [])) {
          if (result && result.success && Number.isFinite(Number(result.download_log_id))) {
            removeWrongMatchEntry(result.download_log_id);
          }
        }
      }
    } else {
      btn.disabled = false;
      btn.textContent = `Delete All (${count})`;
      toast(data.error || data.message || 'Delete all failed', true);
    }
  } catch (_e) {
    btn.disabled = false;
    btn.textContent = `Delete All (${count})`;
    toast('Delete all request failed', true);
  }
}

/**
 * Run evidence-only cleanup over the full Wrong Matches queue.
 * @param {HTMLButtonElement} btn
 */
export async function bulkTriageWrongMatches(btn) {
  const groups = _lastData && Array.isArray(_lastData.groups) ? _lastData.groups : [];
  const counts = wrongMatchCounts(groups);
  if (counts.entries === 0) {
    toast('No wrong matches to clean up', true);
    return;
  }
  if (!confirm(`Process all ${counts.entries} Wrong Matches candidates?\nOnly force-mode confident rejects will be deleted.`)) return;

  btn.disabled = true;
  btn.textContent = 'Cleaning...';
  const restore = () => {
    btn.disabled = false;
    btn.textContent = `Cleanup Wrong Matches (${counts.entries})`;
  };
  try {
    const r = await fetch(`${API}/api/wrong-matches/triage`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({confirm_all_wrong_matches: true}),
    });
    const data = await r.json();
    // 202 = sweep started; 409 = one is already running. Either way a
    // sweep is in flight server-side, so poll for its result.
    if (r.status !== 202 && r.status !== 409) {
      restore();
      toast(data.error || data.message || 'Cleanup failed', true);
      return;
    }
    const status = await pollTriageStatus();
    if (status && status.state === 'completed') {
      restore();
      toast(cleanupSummaryToast(status.summary || {}));
      invalidateWrongMatches();
      await _refreshWrongMatches();
      return;
    }
    if (status && status.state === 'idle') {
      // The web service restarted mid-sweep and lost the in-memory status.
      // Deletions already performed are durable — refresh to show them.
      restore();
      toast('Sweep status lost (web service restarted) — queue may be partially cleaned', true);
      invalidateWrongMatches();
      await _refreshWrongMatches();
      return;
    }
    restore();
    toast((status && status.error) || 'Cleanup sweep failed', true);
  } catch (_e) {
    restore();
    toast('Cleanup request failed', true);
  }
}

/**
 * Poll the background sweep until it leaves the running state.
 * @returns {Promise<{state: string, summary: Object|null, error: string|null}|null>}
 */
async function pollTriageStatus() {
  // The sweep legitimately takes minutes when stale rows re-measure or
  // the queue is large; poll gently and give up only after an hour.
  const intervalMs = 3000;
  const maxPolls = 1200;
  for (let i = 0; i < maxPolls; i++) {
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
    try {
      const r = await fetch(`${API}/api/wrong-matches/triage/status`);
      if (!r.ok) continue;
      const status = await r.json();
      if (status.state !== 'running') return status;
    } catch (_e) {
      // Transient fetch failure — keep polling.
    }
  }
  return null;
}
