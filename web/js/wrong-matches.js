// @ts-check
import { API, toast } from './state.js';
import { esc, externalReleaseUrl, sourceLabel, wrongMatchExplorerFailureCopy } from './util.js';
import { renderReplaceButton } from './release_actions.js';
import { consumePendingScrollRestore } from './search_plan.js';
import {
  handleProcessingLockedConflict,
  refetchProcessingRequest,
} from './release_action_state.js';
import {
  qualityRankBadgeClass,
  qualityToneClass,
  spectralGradeBadgeClass,
  spectralGradeClass,
  spectralGradeIsAdmissible,
  spectralGradeLabel,
  spectralWithheldPresentation,
} from './quality_palette.js';

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
export function renderWrongMatchExplorer(data) {
  const files = Array.isArray(data?.files) ? data.files : [];
  const otherFileCount = Number.isFinite(data?.other_file_count) ? data.other_file_count : 0;
  const audioFileCount = Number.isFinite(data?.audio_file_count) ? data.audio_file_count : files.length;
  const sourceDirs = sourceDirsForEntry(data);
  const sharedTags = sharedExplorerTags(files);
  const orderedBy = typeof data?.ordered_by === 'string' ? data.ordered_by : 'folder';
  const partial = data?.partial === true;
  const unreadableCount = Number.isFinite(data?.unreadable_entry_count)
    ? data.unreadable_entry_count : 0;
  const unreadableReason = typeof data?.unreadable_reason === 'string'
    ? data.unreadable_reason : '';
  // Structured, not string-sniffed (same rule as
  // `partial_read_is_containment` on the Replace picker): `true` only
  // when the SERVER classified the refusal as a containment decision —
  // a symlink, socket, FIFO or device node — never derived by matching
  // words in `unreadableReason`, which is free-text diagnostics (issue
  // #1086).
  const unreadableIsContainment = data?.unreadable_is_containment === true;
  const truncatedReason = typeof data?.truncated_reason === 'string' ? data.truncated_reason.replace(/_/g, ' ') : 'limit';
  // Two different reasons a listing is incomplete, and they must not be
  // told as one: a LIMIT stopped us, or the server was REFUSED. Issue
  // #1063 — this panel is what the operator reads before deciding to
  // delete, so it must never present a refused read as an empty folder.
  const truncatedNotice = data?.truncated_reason
    ? `<div style="color:#e5a84b;font-size:0.76em;margin:6px 0;">Partial explorer result: ${esc(truncatedReason)} reached.</div>`
    : '';
  // A WORLD-FAILURE refusal (EACCES, EIO, ESTALE, …) describes a state
  // the operator can REPAIR (fix the mode, remount the share) and a
  // plain reload might just see the fix, so the notice carries its own
  // Retry. It rides on the NOTICE, not on the empty-state branch: a
  // PARTIAL listing — some files read, some refused — renders the
  // notice above a real track list and is exactly as repairable, but it
  // answers ``status: "ok"`` and so never reached the empty branch
  // (issue #1063). A CONTAINMENT refusal (a symlink, socket, FIFO or
  // device node) gets no Retry: re-fetching the same name answers the
  // same refusal every time — nothing short of the operator physically
  // replacing the entry changes it, and that is a filesystem action, not
  // a button click (issue #1086).
  const retryId = Number(data?.download_log_id);
  const retry = (Number.isFinite(retryId) && !unreadableIsContainment)
    ? ` <button class="p-btn" style="margin-left:6px;" onclick="event.stopPropagation(); window.reloadWrongMatchExplorer(${retryId})">Retry</button>`
    : '';
  // The LEAD sentence must not say "could not be read" for a containment
  // refusal — that phrasing implies a disk/permission problem a retry
  // might clear, which is exactly the wording a security decision must
  // never get (issue #1086 review). The containment/world distinction
  // was previously visible ONLY in the parenthetical reason text.
  const unreadableLead = unreadableIsContainment
    ? `${unreadableCount} entr${unreadableCount === 1 ? 'y was' : 'ies were'} refused (not read) as a containment decision`
    : `${unreadableCount} entr${unreadableCount === 1 ? 'y' : 'ies'} could not be read`;
  const unreadableNotice = unreadableCount > 0
    ? `<div style="color:#d9a441;font-size:0.76em;margin:6px 0;">${unreadableLead} — this listing is incomplete and nothing here is confirmed missing.${unreadableReason ? ` (${esc(unreadableReason)})` : ''}${retry}</div>`
    : '';
  const partialNotice = `${truncatedNotice}${unreadableNotice}`;
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
    const emptyText = unreadableCount > 0
      ? (unreadableIsContainment
          ? 'This folder\u2019s contents were refused (not read) as a containment decision, so no listing is available. This is NOT evidence that the folder is empty.'
          : 'This folder\u2019s contents could not be read, so no listing is available. This is NOT evidence that the folder is empty.')
      : partial
        ? 'No audio files were found before exploration was truncated.'
        : 'No audio files found in this folder.';
    const emptyColour = unreadableCount > 0 ? '#d9a441' : '#666';
    return `${summary}${partialNotice}<div style="color:${emptyColour};font-size:0.78em;padding:8px 0;">${emptyText}</div>`;
  }

  let html = `
    <div style="margin-top:10px;">
      ${summary}
      ${partialNotice}
      <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;">
        <div style="color:#888;font-size:0.78em;">${audioFileCount} track${audioFileCount === 1 ? '' : 's'} in surviving folder${orderedBy === 'matched' ? ' in matched order' : ''}</div>
        ${otherFileCount > 0 ? `<div style="color:#666;font-size:0.74em;">${otherFileCount} non-audio file${otherFileCount === 1 ? '' : 's'} hidden</div>` : ''}
      </div>
      ${files.map(renderWrongMatchExplorerFile).join('')}
    </div>`;
  return html;
}

/**
 * Explorer payload statuses this panel knows how to render.
 *
 * ``build_wrong_match_explorer`` emits exactly these two: ``ok`` when it
 * was allowed to look at everything, ``unavailable`` when refusals were
 * recorded and nothing was readable. Both are complete payloads that
 * ``renderWrongMatchExplorer`` turns into honest copy; anything else is
 * an error envelope.
 *
 * @type {Set<unknown>}
 */
export const EXPLORER_RENDERABLE_STATUSES = new Set(['ok', 'unavailable']);

/**
 * Did the server record a refusal the operator could go and repair?
 *
 * Keyed on the refusal COUNT, not on `status`: the server only says
 * `unavailable` when NOTHING was readable, so a partial listing — some
 * files read, some refused — answers `ok` while being exactly as
 * repairable. Caching that state meant the operator fixed the permission
 * and still needed a full page reload to see it (issue #1063).
 *
 * A `truncated_reason` is deliberately NOT repairable: retrying hits the
 * same limit, so a truncated listing stays cached.
 *
 * @param {any} data
 * @returns {boolean}
 */
export function explorerListingIsRepairable(data) {
  return Number(data?.unreadable_entry_count) > 0
    || data?.status === 'unavailable';
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
  /** @type {number|undefined} */
  let status;
  try {
    const r = await fetch(`${API}/api/wrong-matches/explorer?download_log_id=${encodeURIComponent(String(logId))}`);
    status = r.status;
    const data = await r.json();
    // ``unavailable`` is a 200 payload the server DELIBERATELY builds:
    // nothing readable plus recorded refusals. It is the honest listing,
    // not a load failure — rejecting it here sent the operator back to
    // "Failed to load file explorer" with a Retry button that can never
    // succeed on an unreadable tree, and left the authored copy below
    // unreachable (issue #1063).
    if (!r.ok || !EXPLORER_RENDERABLE_STATUSES.has(data?.status)) {
      throw new Error(data?.error || data?.message || 'Explorer load failed');
    }
    mount.innerHTML = renderWrongMatchExplorer(data);
    // Only a listing with nothing left to repair is worth caching. One
    // that recorded refusals describes a broken world the operator is
    // expected to go and fix; caching it would make reopening the
    // disclosure short-circuit on the stale answer until the whole page
    // is reloaded.
    if (explorerListingIsRepairable(data)) {
      _entryExplorerState.delete(logId);
    } else {
      _entryExplorerState.set(logId, 'loaded');
    }
  } catch (e) {
    _entryExplorerState.delete(logId);
    // Issue #1099: the whole-root refusal reaching this catch answers
    // 404, 422, or 503 — a definitive absence, a containment DECISION a
    // retry can never satisfy, or a retryable world failure — and this
    // block used to say "Failed to load file explorer" for all three
    // alike. ``wrongMatchExplorerFailureCopy`` is the one pure function
    // that turns the status into honest, status-specific copy; the
    // server's own reason (``Wrong-match files could not be read: …
    // (EACCES)``) still rides along as detail.
    const serverMessage = (e instanceof Error && e.message) ? e.message : '';
    const copy = wrongMatchExplorerFailureCopy(status, serverMessage);
    // The Retry button follows the SAME #1086 doctrine the per-entry
    // notice already applies inside a 200 payload: offer Retry only
    // where retrying could plausibly change the answer. A 422 is a
    // containment DECISION — re-fetching the same name answers the
    // same refusal every time — so it gets no Retry; 404/503/unknown
    // all stay retryable (a genuinely-missing folder can reappear, a
    // world failure can clear, and an unrecognised failure shape
    // should not silently strand the operator with no way to reload).
    const retryAllowed = status !== 422;
    const retryButton = retryAllowed
      ? ` <button class="p-btn" style="margin-left:6px;" onclick="event.stopPropagation(); window.reloadWrongMatchExplorer(${logId})">Retry</button>`
      : '';
    mount.innerHTML = `<div style="color:#f88;font-size:0.78em;padding:8px 0;">${esc(copy)}${retryButton}</div>`;
  }
}

/**
 * Load and display wrong-match rejections from failed_imports.
 */
export async function loadWrongMatches() {
  // The `finally` guarantees exactly one consume per call regardless of
  // which branch below returns early (including the `_loaded` cache
  // short-circuit, which still counts as "this tab's render is done") —
  // the search-plan back button's completion boundary
  // (search_plan.js::closeSearchPlanDetail) relies on this being the
  // true end of the Wrong Matches tab's render, not a fixed delay after
  // it starts.
  try {
    // #1106 F6: derive the toolbar's state on EVERY call, even when the
    // cached queue is not re-fetched below -- a tab switch must still
    // reflect a sweep started elsewhere (the CLI, another tab) without
    // requiring a manual Refresh first.
    await _deriveTriageButtonState();
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
      await _deriveTriageButtonState();
    } catch (e) {
      el.innerHTML = '<div style="color:#f66;">Failed to load wrong matches</div>';
    }
  } finally {
    consumePendingScrollRestore();
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
export function normalizeThreshold(value) {
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

function rerenderWrongMatches() {
  if (_lastData && _lastEl) {
    renderWrongMatches(_lastData, _lastEl);
    // Fire-and-forget: this path runs off a plain (non-async) threshold
    // input handler, and a slider nudge is low-stakes enough that it
    // does not need to block on the derive.
    void _deriveTriageButtonState();
  }
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
 * Is this entry's source folder unobservable right now?
 *
 * The server sends `path_unavailable` when its probe was REFUSED
 * (permissions, I/O) rather than answering "gone" — issue #1063. Such an
 * entry is still real; it just cannot be acted on, so every destructive
 * or importing action is disabled and it never counts as converge-green.
 * @param {any} entry
 * @returns {boolean}
 */
export function entryPathUnavailable(entry) {
  return entry?.path_unavailable === true;
}

/**
 * @param {any} entry
 * @param {number} thresholdMilli
 * @returns {boolean}
 */
export function isConvergeGreen(entry, thresholdMilli) {
  if (entryPathUnavailable(entry)) return false;
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
 * Candidates the group "Delete All" action can actually act on right now.
 *
 * Mirrors the converge button beside it (issue #1086 item 2): a candidate
 * whose source folder the server could not read is not deletable, so it is
 * excluded from the actionable count the same way it is excluded from
 * `greenEntries`. A group can be PARTIALLY unavailable — the button must
 * stay usable for the rest — so this only disables the action when NONE
 * of the group's candidates are actionable.
 * @param {any} group
 * @returns {any[]}
 */
export function actionableDeleteEntries(group) {
  return (group.entries || []).filter((/** @type {any} */ entry) => (
    !entryPathUnavailable(entry)
  ));
}

/**
 * @param {number} actionableCount
 * @param {number} totalCount
 * @returns {string}
 */
export function deleteAllButtonLabel(actionableCount, totalCount) {
  return actionableCount === totalCount
    ? `Delete All (${totalCount})`
    : `Delete All (${actionableCount} of ${totalCount})`;
}

/**
 * @param {number|string} requestId
 * @param {unknown} thresholdMilli
 * @returns {{request_id: number, threshold_milli: number, delete_unmatched: boolean}}
 */
export function convergeRequestBody(requestId, thresholdMilli) {
  return {
    request_id: Number(requestId),
    threshold_milli: normalizeThreshold(thresholdMilli),
    delete_unmatched: true,
  };
}

/**
 * Format the per-candidate stored evidence cells for a wrong-match
 * entry. Pure — input is the entry payload, output is a `{format,
 * spectral, v0}` triple of short display strings. Candidate source codec,
 * configured target contract, and temporary V0 probe are distinct facts;
 * the format cell must never label probe bitrates as target-codec measurements.
 * All values come from canonical album_quality_evidence; the
 * candidate row never starts a preview job from the UI (R3) and never
 * exposes a preview button.
 * @param {any} entry
 * @returns {{format: string, spectral: string, v0: string}}
 */
export function formatEntryEvidence(entry) {
  const fmt = entry && typeof entry.format === 'string' && entry.format
    ? entry.format : null;
  const source = entry && typeof entry.source_codec === 'string' && entry.source_codec
    ? entry.source_codec
    : entry && typeof entry.source_container === 'string' && entry.source_container
      ? entry.source_container : null;
  const target = entry && typeof entry.target_format === 'string' && entry.target_format
    ? entry.target_format : null;
  const minBr = entry && Number.isFinite(entry.min_bitrate)
    ? entry.min_bitrate : null;
  const avgBr = entry && Number.isFinite(entry.avg_bitrate)
    ? entry.avg_bitrate : null;
  let format = '—';
  if (target) {
    const contract = `${String(target).toUpperCase()} contract`;
    format = source
      ? `${String(source).toUpperCase()} → ${contract}`
      : contract;
  } else if (fmt && avgBr != null && avgBr > 0 && minBr != null && minBr > 0) {
    format = `${fmt} avg ${avgBr}k · min ${minBr}k`;
  } else if (fmt && avgBr != null && avgBr > 0) format = `${fmt} avg ${avgBr}k`;
  else if (fmt && minBr != null && minBr > 0) format = `${fmt} min ${minBr}k`;
  else if (fmt) format = fmt;
  else if (avgBr != null && avgBr > 0 && minBr != null && minBr > 0) {
    format = `avg ${avgBr}k · min ${minBr}k`;
  } else if (avgBr != null && avgBr > 0) format = `avg ${avgBr}k`;
  else if (minBr != null && minBr > 0) format = `min ${minBr}k`;

  const grade = entry && typeof entry.spectral_grade === 'string'
    ? entry.spectral_grade : null;
  const bitrate = entry && Number.isFinite(entry.spectral_bitrate)
    ? entry.spectral_bitrate : null;
  let spectral = '—';
  if (grade && bitrate != null) spectral = `${spectralGradeLabel(grade)} · ${bitrate} kbps`;
  else if (grade) spectral = spectralGradeLabel(grade);
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
 * The candidate row's spectral cell, audit-only aware (issue #829 Phase 5
 * PR4). The measured grade text stays exactly as `formatEntryEvidence`
 * composed it; only the accusing colour is withheld, and a suffix plus
 * hover explanation say why. A candidate with no evidence join carries no
 * flags, so it keeps the historical accusing render.
 * @param {any} entry - the wrong-match entry payload
 * @param {string} spectralText - `formatEntryEvidence(entry).spectral`
 * @returns {string}
 */
export function entrySpectralCell(entry, spectralText) {
  const grade = entry && entry.spectral_grade ? entry.spectral_grade : null;
  if (!grade) {
    return `<span class="${qualityToneClass('unknown')}">spectral: ${esc(spectralText)}</span>`;
  }
  if (spectralGradeIsAdmissible(grade, entry.spectral_accusation_admissible)) {
    return `<span class="${spectralGradeClass(grade)}">spectral: ${esc(spectralText)}</span>`;
  }
  const withholding = spectralWithheldPresentation(
    entry.spectral_accusation_withheld);
  return `<span class="${withholding.className}" title="${withholding.title}">`
    + `spectral: ${esc(spectralText)}${withholding.suffix}</span>`;
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
export function cleanupSummaryToast(data) {
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
function entryItemStyle(green, unavailable = false) {
  if (unavailable) {
    return 'background:#1f1a14;margin:4px 0;border-color:#8a6a2a;box-shadow:inset 3px 0 0 #d9a441;';
  }
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
    // Route through the full renderer so the toolbar (including Stop)
    // survives draining the LAST group via surgical removal, not just
    // a full page re-render (issue #1106 F7) -- then re-derive its
    // state off the server in case a sweep is running concurrently.
    renderWrongMatches(_lastData, _lastEl);
    void _deriveTriageButtonState();
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
export function removeWrongMatchEntry(logId) {
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
      const groupDeleteBtn = /** @type {HTMLButtonElement | null} */ (
        document.getElementById(`wm-delete-group-btn-${owningGroup.request_id}`)
      );
      if (groupDeleteBtn) {
        const actionableCount = actionableDeleteEntries(owningGroup).length;
        groupDeleteBtn.textContent = deleteAllButtonLabel(actionableCount, remaining);
        groupDeleteBtn.disabled = actionableCount === 0;
      }
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
export function renderWrongMatches(data, el) {
  _lastData = data;
  _lastEl = el;
  _entryExplorerState.clear();
  /** @type {any[]} */
  const groups = (data.groups || []).filter((/** @type {any} */ g) => (g.pending_count || 0) > 0);
  const counts = wrongMatchCounts(groups);
  // The toolbar (including Stop) is ALWAYS rendered, even at zero
  // pending entries (issue #1106 F7) -- a mid-sweep Refresh that drains
  // the queue to empty must not lose the Stop control while a sweep
  // (started elsewhere) is still running against rows this pane no
  // longer shows. Cleanup is already disabled at zero entries; Stop's
  // enablement is derived by the caller (`_deriveTriageButtonState`).
  let html = `
    <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;margin:8px 0;">
      <div id="wrong-matches-summary" style="color:#888;">${counts.groups} release${counts.groups !== 1 ? 's' : ''} · ${counts.entries} candidate${counts.entries !== 1 ? 's' : ''} pending review</div>
      <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
        <button id="wm-refresh-btn" class="p-btn" style="border-color:#888;color:#888;" onclick="event.stopPropagation(); window.refreshWrongMatches(this)" title="Refetch the queue from the server">Refresh</button>
        <button id="wm-bulk-triage-btn" class="p-btn delete" ${counts.entries === 0 ? 'disabled' : ''} onclick="event.stopPropagation(); window.bulkTriageWrongMatches()">Cleanup Wrong Matches (${counts.entries})</button>
        <button id="wm-bulk-triage-stop-btn" class="p-btn" style="border-color:#888;color:#888;" disabled onclick="event.stopPropagation(); window.stopWrongMatchTriage()" title="Stop the running cleanup sweep after its current row">Stop</button>
      </div>
    </div>`;

  html += groups.length === 0
    ? '<div style="color:#888;padding:12px;">No wrong matches in failed_imports.</div>'
    : groups.map(renderGroup).join('');
  el.innerHTML = html;
}

/**
 * Build the quality badge strip for a group header. Shows format + bitrate,
 * verified-lossless marker, spectral grade (when suspect/likely_transcode),
 * and the rank tier — so the user can tell at a glance whether there's
 * already a good version on disk.
 * @param {any} g
 * @returns {string}
 */
export function renderQualityBadges(g) {
  // Drive the 'nothing on disk' badge off data, not the DB status.
  // A row left at status='imported' after a manual beet rm still has
  // nothing on disk, so checking status alone would swallow the signal
  // and leave the badge strip empty.
  //
  // Issues #121 / #123: the backend gates `in_library` and the
  // quality fields (`quality_label`, `avg_bitrate`, `min_bitrate`,
  // `current_spectral_grade`, `format`) on exact-ID match — no
  // fuzzy fallback. When `in_library=true` the quality fields will
  // be populated; when false they'll be null. `format` stays in the
  // guard because it's the fallback badge text when bitrate is null
  // (e.g. FLAC with no bitrate metadata).
  const avgBr = Number.isFinite(g.avg_bitrate) && g.avg_bitrate > 0
    ? g.avg_bitrate : null;
  const minBr = Number.isFinite(g.min_bitrate) && g.min_bitrate > 0
    ? g.min_bitrate : null;
  const hasOnDiskQuality = g.quality_label || avgBr || minBr
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
    parts.push(`<span class="badge badge-quality-outline ${qualityRankBadgeClass(g.quality_rank)}">${esc(label)}</span>`);
  } else if (avgBr !== null || minBr !== null) {
    const fallback = [];
    if (avgBr !== null) fallback.push(`avg ${avgBr}k`);
    if (minBr !== null) fallback.push(`min ${minBr}k`);
    parts.push(`<span class="badge" style="background:#222;color:#aaa;">${fallback.join(' · ')}</span>`);
  }
  if (g.verified_lossless) {
    parts.push('<span class="badge badge-verified badge-rank-lossless">verified lossless</span>');
  }
  // Spectral badge only when it's worth flagging — and never as an
  // accusation when the installed copy's codec cannot support one
  // (issue #829 Phase 5 PR4). The gate below is exactly the accusing
  // case, so an audit-only family reached the red badge until now.
  if (g.current_spectral_grade && g.current_spectral_grade !== 'genuine') {
    const suffix = g.current_spectral_bitrate ? ` (${g.current_spectral_bitrate}k)` : '';
    const label = `${esc(spectralGradeLabel(g.current_spectral_grade))}${suffix}`;
    if (spectralGradeIsAdmissible(
      g.current_spectral_grade, g.current_spectral_accusation_admissible,
    )) {
      parts.push(`<span class="${spectralGradeBadgeClass(g.current_spectral_grade)}">${label}</span>`);
    } else {
      const withholding = spectralWithheldPresentation(
        g.current_spectral_accusation_withheld);
      parts.push(`<span class="${withholding.badgeClass}"`
        + ` title="${withholding.title}">${label}${withholding.suffix}</span>`);
    }
  }
  if (g.quality_rank) {
    parts.push(`<span class="badge ${qualityRankBadgeClass(g.quality_rank)}" style="font-family:monospace;font-size:0.72em;">${esc(g.quality_rank)}</span>`);
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
 * recent success/force_import or historical manual_import for the release — i.e. what's
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
export function renderLatestImport(d, group) {
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
  const actionableCount = actionableDeleteEntries(g).length;
  const deleteDisabled = actionableCount === 0;
  const deleteLabel = deleteAllButtonLabel(actionableCount, count);
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
        <button id="wm-delete-group-btn-${g.request_id}" class="p-btn delete" ${deleteDisabled ? 'disabled' : ''} onclick="event.stopPropagation(); window.deleteWrongMatchGroup(${g.request_id}, this)">${deleteLabel}</button>
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
export function renderEntry(e, thresholdMilli, requestId) {
  const detailId = `wm-entry-${e.download_log_id}`;
  const distValue = distanceValue(e.distance);
  const dist = distValue != null ? distValue.toFixed(3) : '?';
  const job = e.import_job || null;
  const jobBadge = job ? `<span class="badge" style="background:#222;color:#9bf;margin-left:8px;">${esc(job.status)}</span>` : '';
  const green = isConvergeGreen(e, thresholdMilli);
  const unavailable = entryPathUnavailable(e);
  const distColor = green ? '#6d6' : '#aaa';
  const evidence = formatEntryEvidence(e);
  const unavailableBadge = unavailable
    ? '<span class="badge" style="background:#2a2114;color:#d9a441;border:1px solid #8a6a2a;margin-left:8px;">source unavailable</span>'
    : '';

  // Rank badge mirrors the group-header palette so operators can sort
  // candidates visually. Sort order is server-side (best first); the
  // badge just reinforces it. verified_lossless gets its own marker
  // since FLAC can show up before/after we know it's actually lossless.
  const rank = typeof e.quality_rank === 'string' ? e.quality_rank : '';
  const rankBadge = rank && rank !== 'unknown'
    ? `<span class="badge ${qualityRankBadgeClass(rank)}" style="font-family:monospace;font-size:0.72em;margin-left:6px;">${esc(rank)}</span>`
    : '';
  const verifiedBadge = e.verified_lossless
    ? '<span class="badge badge-verified badge-rank-lossless" style="margin-left:6px;">verified lossless</span>'
    : '';

  const header = `
    <div id="wm-entry-card-${e.download_log_id}" class="p-item" data-request-id="${requestId}" data-distance="${distValue != null ? distValue : ''}" style="${entryItemStyle(green, unavailable)}" onclick="window.toggleWrongMatchEntry('${detailId}', ${e.download_log_id})">
      <div class="p-top">
        <div>
          <span style="font-family:monospace;color:#aaa;">#${e.download_log_id}</span>
          <span style="color:#6a9;margin-left:8px;">${esc(e.soulseek_username || '?')}</span>
          <span id="wm-entry-green-${e.download_log_id}" class="badge" style="${entryGreenBadgeStyle(green)}">green</span>
          ${unavailableBadge}${rankBadge}${verifiedBadge}${jobBadge}
        </div>
      </div>
      <div class="p-meta">
        <span id="wm-entry-dist-${e.download_log_id}" style="color:${distColor};">dist: ${dist}</span>
        <span>${esc(e.scenario || '')}</span>
        <span style="color:#bbb;">${esc(evidence.format)}</span>
        ${entrySpectralCell(e, evidence.spectral)}
        <span style="color:#888;">${esc(evidence.v0)}</span>
      </div>
    </div>
    <div class="p-detail" id="${detailId}">
      ${renderEntryDetail(e, job, requestId)}
    </div>`;

  return header;
}

/**
 * Render expanded detail panel for one rejected candidate.
 * @param {Object} e - entry payload
 * @param {number|string} requestId
 * @returns {string}
 */
function renderEntryDetail(e, job, requestId) {
  const c = e.candidate;
  const sourceDirs = sourceDirsForEntry(e);

  // Action buttons up top: operators are usually here to act, not browse.
  const active = job && ['queued', 'running', 'recovery_required'].includes(job.status);
  const unavailable = entryPathUnavailable(e);
  const importLabel = job?.status === 'recovery_required'
    ? 'Recovery required'
    : (active ? job.status[0].toUpperCase() + job.status.slice(1) : 'Force Import');
  const blocked = Boolean(active) || unavailable;
  let html = '<div class="p-actions" style="margin-bottom:10px;">';
  html += `<button class="p-btn" data-pipeline-request-id="${requestId}" style="border-color:#6a9;color:#6a9;" ${blocked ? 'disabled' : ''} onclick="event.stopPropagation(); window.forceImportWrongMatch(${e.download_log_id}, this)">${importLabel}</button>`;
  html += `<button class="p-btn delete" data-pipeline-request-id="${requestId}" ${blocked ? 'disabled' : ''} onclick="event.stopPropagation(); window.deleteWrongMatch(${e.download_log_id}, this)">Delete</button>`;
  html += '</div>';
  if (unavailable) {
    // Say what is actually true: the server could not look. Nothing here
    // claims the folder is gone, and nothing offers to delete it.
    html += `<div class="p-detail-row"><span class="p-detail-label" style="color:#d9a441;">Source</span><span class="p-detail-value" style="color:#d9a441;">Unavailable \u2014 the server could not read this folder, so it cannot be imported or deleted. It has NOT been confirmed missing.${e.path_unavailable_reason ? ` (${esc(String(e.path_unavailable_reason))})` : ''}</span></div>`;
  }

  if (c) {
    html += `<div class="p-detail-row"><span class="p-detail-label">Matched</span><span class="p-detail-value">${esc(c.artist || '?')} — ${esc(c.album || '?')}${c.year ? ` (${esc(c.year)})` : ''}${c.country ? ` [${esc(c.country)}]` : ''}</span></div>`;
    if (c.label) html += `<div class="p-detail-row"><span class="p-detail-label">Label</span><span class="p-detail-value">${esc(c.label)}${c.catalognum ? ` / ${esc(c.catalognum)}` : ''}</span></div>`;
  }
  if (sourceDirs.length > 0) {
    html += `<div class="p-detail-row"><span class="p-detail-label">Downloaded as</span><span class="p-detail-value" style="font-size:0.8em;">${sourceDirs.map((dir) => esc(dir)).join('<br>')}</span></div>`;
  }
  if (e.failed_path) {
    html += `<div class="p-detail-row"><span class="p-detail-label">Path</span><span class="p-detail-value" style="font-size:0.8em;">${esc(e.failed_path)}</span></div>`;
  }
  if (e.detail) {
    html += `<div class="p-detail-row"><span class="p-detail-label">Detail</span><span class="p-detail-value" style="font-size:0.8em;">${esc(e.detail)}</span></div>`;
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
      const localFmt = m.item?.format ? ` ${esc(m.item.format)}` : '';
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
      await _deriveTriageButtonState();
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
export async function pollImportJob(jobId, btn, logId) {
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
      if (job.status === 'recovery_required') {
        btn.textContent = 'Recovery required';
        btn.style.color = '#f88';
        toast(
          job.message
            || 'Historical recovery state; startup convergence will recheck the exact execution automatically',
          true,
        );
        invalidateWrongMatches();
        return;
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
    if (await handleProcessingLockedConflict({
      httpStatus: r.status,
      payload: data,
      control: btn,
      refetch: (requestId, generation) => refetchProcessingRequest(
        requestId,
        '',
        generation,
      ),
    })) {
      return;
    }
    if (data.status === 'queued') {
      btn.textContent = data.deduped ? 'Queued' : 'Queued';
      btn.style.borderColor = '#9bf';
      btn.style.color = '#9bf';
      toast(`Queued import: ${data.artist} - ${data.album}`);
      if (data.job_id) {
        await pollImportJob(data.job_id, btn, logId);
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
  // The pre-request actionable count (issue #1086 item 2): a failed
  // request restores the button to whatever it looked like before the
  // click, not a plain "Delete All (N)" that lies about a still-unavailable
  // candidate.
  const actionableCount = group ? actionableDeleteEntries(group).length : count;
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
    // A non-2xx group delete is NOT "nothing happened": the route reports
    // the worst outcome in the group, so N folders can already be gone
    // while one was skipped. Telling the operator "failed" and leaving a
    // stale list is the #1063 shape in the browser — always refresh, and
    // only say "failed" when nothing was deleted (issue #1063 T3.4).
    const summarised = Number.isFinite(Number(data.deleted));
    if (summarised) {
      const deleted = Number(data.deleted) || 0;
      // A pointer-only clear over a folder that was already gone is not a
      // deletion, and the headline must not call it one (issue #1063).
      const clearedMissing = Number(data.cleared_missing) || 0;
      const cleared = clearedMissing
        ? ` · cleared ${clearedMissing} already-missing` : '';
      // "unavailable" is its own bucket, never folded into skipped or
      // errors (issue #1086 item 3): a source the server could not even
      // observe is neither a policy skip nor a genuine delete failure, and
      // counting it into both used to read `skipped 1 · errors 1` for one
      // real outcome.
      const unavailableCount = Number(data.unavailable) || 0;
      const unavailable = unavailableCount ? ` · unavailable ${unavailableCount}` : '';
      const skipped = data.skipped ? ` · skipped ${data.skipped}` : '';
      const errors = data.errors ? ` · errors ${data.errors}` : '';
      const remaining = data.remaining ? ` · ${data.remaining} left` : '';
      const failed = deleted === 0 && clearedMissing === 0;
      // Anything left behind needs the operator's attention, even when
      // some folders did go: a green "all good" toast over `errors 1 ·
      // 1 left` under-signals exactly the world this issue is about.
      const incomplete = Boolean(data.errors) || Boolean(data.remaining);
      toast(
        failed
          ? `Deleted nothing${skipped}${unavailable}${errors}${remaining}`
          : `Deleted ${deleted} folder${deleted === 1 ? '' : 's'}${cleared}${skipped}${unavailable}${errors}${remaining}`,
        failed || incomplete,
      );
      invalidateWrongMatches();
      if (r.ok && (data.status === 'ok' || data.remaining === 0)) {
        removeWrongMatchGroup(requestId);
      } else {
        // Partial outcome: re-render from the server rather than surgically
        // removing rows. Surgical removal left the group's own strip stale
        // — "Delete All (2)" and "1 green" over the ONE unavailable
        // candidate that survived, which is the client/server green
        // disagreement all over again (issue #1063 F-review).
        btn.disabled = actionableCount === 0;
        btn.textContent = deleteAllButtonLabel(actionableCount, count);
        await _refreshWrongMatches();
      }
    } else {
      btn.disabled = actionableCount === 0;
      btn.textContent = deleteAllButtonLabel(actionableCount, count);
      invalidateWrongMatches();
      toast(data.error || data.message || 'Delete all failed', true);
    }
  } catch (_e) {
    btn.disabled = actionableCount === 0;
    btn.textContent = deleteAllButtonLabel(actionableCount, count);
    toast('Delete all request failed', true);
  }
}

/**
 * @returns {HTMLButtonElement | null}
 */
function _triageCleanupBtn() {
  return /** @type {HTMLButtonElement | null} */ (
    document.getElementById('wm-bulk-triage-btn'));
}

/**
 * @returns {HTMLButtonElement | null}
 */
function _triageStopBtn() {
  return /** @type {HTMLButtonElement | null} */ (
    document.getElementById('wm-bulk-triage-stop-btn'));
}

/**
 * How many Wrong Matches candidates are currently visible — drives the
 * idle Cleanup button's label/enablement off the CURRENT queue, not
 * whatever count was on screen when a sweep started (issue #1106: a
 * mid-sweep Refresh can change it before the sweep finishes).
 * @returns {number}
 */
function _currentWrongMatchEntryCount() {
  const groups = _lastData && Array.isArray(_lastData.groups) ? _lastData.groups : [];
  return wrongMatchCounts(groups).entries;
}

/**
 * Pure derivation of the triage toolbar's button shape from the sweep's
 * server-reported state and how many candidates are currently visible.
 * Node-testable without a DOM (issue #1106) — the DOM-mutating
 * `_applyTriageButtonState` below applies exactly this shape via fresh
 * `getElementById` lookups, never a node captured at an earlier point.
 * `'unknown'` is the conservative shape after a status fetch AND its
 * one bounded retry both fail (issue #1106 F4): Cleanup disables
 * (nothing here can verify it is safe to start another sweep) and Stop
 * enables (harmless — an unarmed cancel with nothing actually running
 * is a pure no-op, issue #1106 F3).
 * @param {string} state - `/api/wrong-matches/triage/status`'s `state`,
 *   `'unknown'`, or any other value to mean "not running"
 * @param {number} entryCount
 * @returns {{cleanupDisabled: boolean, cleanupLabel: string, stopDisabled: boolean, stopLabel: string}}
 */
export function triageButtonPresentation(state, entryCount) {
  if (state === 'running') {
    return {
      cleanupDisabled: true,
      cleanupLabel: 'Cleaning...',
      stopDisabled: false,
      stopLabel: 'Stop',
    };
  }
  if (state === 'unknown') {
    return {
      cleanupDisabled: true,
      cleanupLabel: 'Cleanup Wrong Matches (status unknown)',
      stopDisabled: false,
      stopLabel: 'Stop',
    };
  }
  return {
    cleanupDisabled: entryCount === 0,
    cleanupLabel: `Cleanup Wrong Matches (${entryCount})`,
    stopDisabled: true,
    stopLabel: 'Stop',
  };
}

/**
 * Apply `triageButtonPresentation(state, ...)` to whatever is CURRENTLY
 * registered under the toolbar button ids — looked up fresh every call,
 * never a node captured at click time or render time (issue #1106: a
 * mid-sweep re-render replaces the pane's innerHTML and detaches any
 * previously-captured node, which is exactly how the Stop button used to
 * get stranded disabled while a sweep kept running underneath it).
 * @param {string} state
 */
function _applyTriageButtonState(state) {
  const presentation = triageButtonPresentation(state, _currentWrongMatchEntryCount());
  const cleanupBtn = _triageCleanupBtn();
  if (cleanupBtn) {
    cleanupBtn.disabled = presentation.cleanupDisabled;
    cleanupBtn.textContent = presentation.cleanupLabel;
  }
  const stopBtn = _triageStopBtn();
  if (stopBtn) {
    stopBtn.disabled = presentation.stopDisabled;
    stopBtn.textContent = presentation.stopLabel;
  }
}

/**
 * `started_at` of the sweep currently owned by an active follower, or
 * `null` when nothing is being followed (issue #1106 F5/N3). Keyed on
 * `started_at` rather than a bare boolean: a boolean stays "true" for
 * the WHOLE terminal chain (poll -> toast -> refresh -> re-derive), so
 * a genuinely NEW sweep discovered while an OLDER chain's refresh is
 * still unwinding got no follower at all under the boolean design —
 * silently stranding "Cleaning..." forever. A claim is refused (not
 * blindly overwritten) when its value is the one already held, OR
 * lexicographically OLDER than it (ISO-8601 strings compare
 * correctly this way) — otherwise an out-of-order/delayed response
 * describing an already-superseded sweep could steal the slot from
 * the follower that owns the CURRENT one and produce duplicate
 * terminal toasts/refreshes for the same sweep. This only guards the
 * CLAIM; `_followTriageSweepToCompletion` separately verifies its own
 * claimed value against the final status before acting on it, since
 * the slot can still move on to a genuinely newer sweep while a poll
 * is in flight.
 * @type {string | null}
 */
let _triageFollowedStartedAt = null;

/**
 * Claim exclusive ownership of following one sweep. `startedAt == null`
 * (the caller could not determine it — a transient fetch failure on the
 * very first check) always claims without registering, a best-effort
 * degradation for an edge case rare enough that a possible duplicate
 * follower is an acceptable trade against under-following a real sweep.
 * @param {string | null} startedAt
 * @returns {boolean}
 */
export function claimTriageFollow(startedAt) {
  if (startedAt == null) return true;
  if (_triageFollowedStartedAt != null && startedAt <= _triageFollowedStartedAt) {
    return false;
  }
  _triageFollowedStartedAt = startedAt;
  return true;
}

/**
 * @param {string | null} startedAt
 */
export function releaseTriageFollow(startedAt) {
  if (startedAt != null && _triageFollowedStartedAt === startedAt) {
    _triageFollowedStartedAt = null;
  }
}

/**
 * Fetch `/api/wrong-matches/triage/status` once. Never throws — returns
 * `undefined` on any transport/parse failure so callers can branch on
 * "could not determine" without their own try/catch.
 * @returns {Promise<{state: string, started_at: string|null, summary: Object|null, error: string|null}|undefined>}
 */
async function _fetchTriageStatus() {
  try {
    const r = await fetch(`${API}/api/wrong-matches/triage/status`);
    if (!r.ok) return undefined;
    return await r.json();
  } catch (_e) {
    return undefined;
  }
}

/**
 * Apply the shared terminal handling for a background triage sweep once
 * it leaves the running state: restores both toolbar buttons, toasts
 * the outcome, and (except on failure, where there is nothing new to
 * fetch) refreshes the pane. Used by both the click path
 * (`bulkTriageWrongMatches`, which started the sweep) and the
 * render-time attach path (`_deriveTriageButtonState`, which discovered
 * a sweep already running — issue #1106) so both land in exactly the
 * same place.
 * @param {{state: string, summary: Object|null, error: string|null}|null} status
 * @returns {Promise<void>}
 */
async function _applyTriageTerminalState(status) {
  _applyTriageButtonState('idle');
  if (status && status.state === 'completed') {
    toast(cleanupSummaryToast(status.summary || {}));
    invalidateWrongMatches();
    await _refreshWrongMatches();
    return;
  }
  if (status && status.state === 'cancelled') {
    // Issue #1083: the operator hit Stop. summary still holds exactly
    // what ran before the stop — say so distinctly from completion.
    toast(`Cleanup stopped — ${cleanupSummaryToast(status.summary || {})}`);
    invalidateWrongMatches();
    await _refreshWrongMatches();
    return;
  }
  if (status && status.state === 'idle') {
    // The web service restarted mid-sweep and lost the in-memory status.
    // Deletions already performed are durable — refresh to show them.
    toast('Sweep status lost (web service restarted) — queue may be partially cleaned', true);
    invalidateWrongMatches();
    await _refreshWrongMatches();
    return;
  }
  toast((status && status.error) || 'Cleanup sweep failed', true);
}

/**
 * Poll one sweep to completion and apply the shared terminal handling,
 * exclusively owning it via `claimTriageFollow` (issue #1106 F5/N3).
 * Before acting on the poll's result, verifies it actually describes
 * the SAME sweep this call claimed — `claimTriageFollow` only refuses
 * an incoming claim that is stale relative to the slot; it does not
 * freeze the slot for the lifetime of this poll, so a genuinely newer
 * sweep can still take over while `pollTriageStatus()` is in flight. A
 * result naming a different `started_at` is silently skipped here —
 * whichever follower legitimately owns that sweep will report it.
 * @param {{state: string, started_at: string|null, summary: Object|null, error: string|null}} [knownStatus]
 *   an already-fetched RUNNING status, when the caller has one (the
 *   render-time derive always does, from its own status fetch);
 *   omitted for the click path, which fetches it itself so it too can
 *   determine `started_at` and claim ownership.
 * @returns {Promise<void>}
 */
async function _followTriageSweepToCompletion(knownStatus) {
  const status = knownStatus ?? await _fetchTriageStatus();
  const startedAt = status && typeof status.started_at === 'string'
    ? status.started_at : null;
  if (!claimTriageFollow(startedAt)) return;
  try {
    const finalStatus = status && status.state !== 'running'
      ? status
      : await pollTriageStatus();
    const mismatch = startedAt != null && finalStatus
      && typeof finalStatus.started_at === 'string'
      && finalStatus.started_at !== startedAt;
    if (!mismatch) {
      await _applyTriageTerminalState(finalStatus);
    }
  } finally {
    releaseTriageFollow(startedAt);
  }
}

/**
 * Apply whatever the server reported for the triage status: a
 * `running` sweep enables Stop, disables Cleanup, and ATTACHES a poll
 * — fire-and-forget, since a sweep can run for up to an hour and the
 * caller must not block on it — that lands on the exact same terminal
 * handling the click path uses, with no confirm dialog. Any other
 * status just derives the idle shape off the CURRENT candidate count.
 * @param {{state: string, started_at: string|null, summary: Object|null, error: string|null}} status
 */
function _applyTriageStatus(status) {
  if (status.state === 'running') {
    _applyTriageButtonState('running');
    void _followTriageSweepToCompletion(status);
    return;
  }
  _applyTriageButtonState('idle');
}

/**
 * Guards `retryTriageStatusOnce` to at most one in-flight retry at a
 * time (issue #1106 N7b). Without this, several renders failing their
 * OWN initial status fetch in close succession (`loadWrongMatches`'s
 * two derive calls under one transient blip, a Refresh racing a tab
 * switch, …) would each schedule their own independent ~3s timer and
 * fetch, wasting N redundant requests and risking the toolbar
 * flickering between whichever result resolves last.
 * @type {boolean}
 */
let _triageRetryInFlight = false;

/**
 * One bounded (~3s) background retry after a failed status fetch,
 * fire-and-forget so the caller (an awaited render-time derive) never
 * blocks on it (issue #1106 F4). Without this, a single transient
 * failure (a 502 while a sweep is genuinely running) stranded the
 * toolbar in whatever shape the initial render painted — Cleanup
 * enabled, Stop disabled — recoverable only by a manual Refresh,
 * exactly the reachability bug this issue exists to fix. If the retry
 * ALSO fails, paint the conservative `'unknown'` shape instead of
 * leaving the stale one.
 *
 * The toolbar is deliberately left unpainted (whatever the initial
 * render's static markup already shows) for the ~3s before this retry
 * fires (issue #1106 N7a) — accepted as benign: the worst case is an
 * operator clicking Cleanup during that window while a sweep is
 * actually running, which 409s, and the click path already treats a
 * 409 exactly like a 202 (follow the sweep that is in flight either
 * way), so the window self-corrects rather than mis-starting anything.
 * @returns {Promise<void>}
 */
export async function retryTriageStatusOnce() {
  if (_triageRetryInFlight) return;
  if (!_triageCleanupBtn() && !_triageStopBtn()) return;
  _triageRetryInFlight = true;
  try {
    await new Promise((resolve) => setTimeout(resolve, 3000));
    const status = await _fetchTriageStatus();
    // #1106 N5: `== null` catches BOTH a transport/parse failure
    // (`undefined`, from `_fetchTriageStatus`) and a literal JSON
    // `null` body (a malformed-but-successfully-parsed response) --
    // either way there is nothing safe to read `.state` off of.
    if (status == null) {
      _applyTriageButtonState('unknown');
      return;
    }
    _applyTriageStatus(status);
  } finally {
    _triageRetryInFlight = false;
  }
}

/**
 * Derive the triage toolbar's button state from the server (issue
 * #1106) — never from whichever tab happened to click Cleanup. Called,
 * and awaited, from every place that (re)renders the Wrong Matches pane
 * (`loadWrongMatches`, `_refreshWrongMatches`, `rerenderWrongMatches`).
 * No-ops (no fetch at all) when the toolbar itself was not rendered —
 * only possible before the very first render.
 * @returns {Promise<void>}
 */
async function _deriveTriageButtonState() {
  if (!_triageCleanupBtn() && !_triageStopBtn()) return;
  const status = await _fetchTriageStatus();
  // #1106 N5: `== null` catches BOTH a transport/parse failure
  // (`undefined`) and a literal JSON `null` body -- a malformed
  // payload must degrade to the retry/unknown path, not throw an
  // uncaught rejection out of this (unawaited-by-try-block, since F6)
  // call and abort the caller entirely (`loadWrongMatches()` would
  // never even reach its own queue fetch).
  if (status == null) {
    void retryTriageStatusOnce();
    return;
  }
  _applyTriageStatus(status);
}

/**
 * Run evidence-only cleanup over the full Wrong Matches queue.
 * @returns {Promise<void>}
 */
export async function bulkTriageWrongMatches() {
  const groups = _lastData && Array.isArray(_lastData.groups) ? _lastData.groups : [];
  const counts = wrongMatchCounts(groups);
  if (counts.entries === 0) {
    toast('No wrong matches to clean up', true);
    return;
  }
  if (!confirm(`Process all ${counts.entries} Wrong Matches candidates?\nOnly force-mode confident rejects will be deleted.`)) return;

  _applyTriageButtonState('running');
  try {
    const r = await fetch(`${API}/api/wrong-matches/triage`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({confirm_all_wrong_matches: true}),
    });
    const data = await r.json();
    // 202 = sweep started; 409 = one is already running. Either way a
    // sweep is in flight server-side, so follow it to completion.
    if (r.status !== 202 && r.status !== 409) {
      _applyTriageButtonState('idle');
      toast(data.error || data.message || 'Cleanup failed', true);
      return;
    }
    await _followTriageSweepToCompletion();
  } catch (_e) {
    _applyTriageButtonState('idle');
    toast('Cleanup request failed', true);
  }
}

/**
 * Request cancellation of the in-flight bulk triage sweep (issue #1083).
 * Not an error, and never toasted as one, when nothing is running or the
 * sweep already finished — whichever poll loop is following the sweep
 * (the click path or the render-time attach, issue #1106) renders
 * whatever terminal state actually lands. This button lives in the
 * browser deliberately: the panic scenario ("something is deleting the
 * wrong things, stop it") happens while watching this exact screen.
 * @returns {Promise<void>}
 */
export async function stopWrongMatchTriage() {
  const stopBtn = _triageStopBtn();
  if (stopBtn) {
    stopBtn.disabled = true;
    stopBtn.textContent = 'Stopping...';
  }
  try {
    const r = await fetch(`${API}/api/wrong-matches/triage/cancel`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({}),
    });
    if (!r.ok) {
      toast('Stop request failed', true);
      const failedStopBtn = _triageStopBtn();
      if (failedStopBtn) {
        failedStopBtn.disabled = false;
        failedStopBtn.textContent = 'Stop';
      }
      return;
    }
    // Success: normally, leave the button disabled/"Stopping..." — the
    // in-flight poll loop (started either by the click that began the
    // sweep, or by the render-time attach — issue #1106) restores both
    // buttons once the sweep reaches a terminal state. But if NOTHING
    // is currently being followed (issue #1106 N7c) -- the toolbar can
    // be in the conservative 'unknown' shape, where neither the initial
    // status check nor its one retry ever actually observed 'running',
    // so no follower was ever attached -- there is nothing left to
    // restore this button, and it would stay stuck on "Stopping..."
    // forever. Schedule one fresh derive to cover exactly that gap; a
    // follower that IS already attached owns the correct terminal
    // handling for this exact sweep and is left alone (re-deriving on
    // top of it would just flicker the button while cancellation is
    // still being observed between rows, not fix anything).
    if (_triageFollowedStartedAt === null) {
      void _deriveTriageButtonState();
    }
  } catch (_e) {
    toast('Stop request failed', true);
    const failedStopBtn = _triageStopBtn();
    if (failedStopBtn) {
      failedStopBtn.disabled = false;
      failedStopBtn.textContent = 'Stop';
    }
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
