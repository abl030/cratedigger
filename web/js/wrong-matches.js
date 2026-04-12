// @ts-check
import { API, toast } from './state.js';
import { esc, externalReleaseUrl, sourceLabel } from './util.js';

/** @type {boolean} */
let _loaded = false;

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
 * Render wrong match entries.
 * @param {Object} data
 * @param {HTMLElement} el
 */
function renderWrongMatches(data, el) {
  const entries = (data.entries || []).filter(e => e.files_exist);
  if (entries.length === 0) {
    el.innerHTML = '<div style="color:#888;padding:12px;">No wrong matches in failed_imports.</div>';
    return;
  }

  let html = `<div style="margin:8px 0;color:#888;">${entries.length} wrong match${entries.length !== 1 ? 'es' : ''} pending review</div>`;

  html += entries.map(e => {
    const dist = e.distance != null ? e.distance.toFixed(3) : '?';
    const filesBadge = e.files_exist
      ? '<span class="badge badge-library">files on disk</span>'
      : '<span class="badge badge-failed">files missing</span>';
    const upgradeBadge = e.in_library
      ? '<span class="badge" style="background:#2a4a2a;color:#6d6;">upgrade</span>'
      : '';

    return `
    <div class="p-item" onclick="window.toggleWrongMatchDetail('wm-${e.download_log_id}')">
      <div class="p-top">
        <div>
          <span class="p-title">${esc(e.artist)} — ${esc(e.album)}</span>
          ${filesBadge}${upgradeBadge}
        </div>
      </div>
      <div class="p-meta">
        <span>dist: ${dist}</span>
        <span>user: ${esc(e.soulseek_username || '?')}</span>
        <span>${esc(e.scenario || '')}</span>
      </div>
    </div>
    <div class="p-detail" id="wm-${e.download_log_id}">
      ${renderWrongMatchDetail(e)}
    </div>`;
  }).join('');

  el.innerHTML = html;
}

/**
 * Render detail panel for a wrong match entry.
 * @param {Object} e - Wrong match entry
 * @returns {string}
 */
function renderWrongMatchDetail(e) {
  let html = '';

  const c = e.candidate;

  // Candidate match info
  if (c) {
    html += `<div class="p-detail-row"><span class="p-detail-label">Matched</span><span class="p-detail-value">${esc(c.artist || '?')} — ${esc(c.album || '?')}${c.year ? ` (${c.year})` : ''}${c.country ? ` [${esc(c.country)}]` : ''}</span></div>`;
    if (c.label) html += `<div class="p-detail-row"><span class="p-detail-label">Label</span><span class="p-detail-value">${esc(c.label)}${c.catalognum ? ` / ${esc(c.catalognum)}` : ''}</span></div>`;
  }
  if (e.mb_release_id) {
    html += `<div class="p-detail-row"><span class="p-detail-label">Target (${sourceLabel(e.mb_release_id)})</span><span class="p-detail-value"><a href="${externalReleaseUrl(e.mb_release_id)}" target="_blank" style="color:#6af;font-family:monospace;font-size:0.85em;">${esc(e.mb_release_id)}</a></span></div>`;
  }
  if (e.failed_path) {
    html += `<div class="p-detail-row"><span class="p-detail-label">Path</span><span class="p-detail-value" style="font-size:0.8em;">${esc(e.failed_path)}</span></div>`;
  }

  // Distance breakdown — non-zero fields + summary of matched fields
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

  // Track mapping — two-column: MB target (left) ↔ On disk (right)
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
      // Highlight title mismatches
      const titleMatch = mbTitle.toLowerCase().replace(/\s*\(demo\)\s*/g, '').trim() === (localTitle || '').toLowerCase().trim();
      const mismatchStyle = titleMatch ? '' : 'color:#f88;';
      html += `<div style="padding:1px 0;color:#aaa;">${mbNum}. ${esc(mbTitle)} <span style="color:#555;">${mbLen}</span></div>`;
      html += `<div style="padding:1px 0;${mismatchStyle}">${esc(localTitle)}<span style="color:#555;"> ${localLen}${localFmt}${localBr}</span></div>`;
    }
    html += '</div>';
  }

  // Extra items (local files with no MB match)
  if (c && c.extra_items && c.extra_items.length > 0) {
    html += `<div style="margin-top:6px;font-size:0.78em;color:#da6;">Extra local files (${c.extra_items.length}):</div>`;
    html += '<div style="font-size:0.75em;padding-left:8px;color:#888;">';
    for (const item of c.extra_items) {
      html += `<div>${esc(item.title || item.path || '?')}</div>`;
    }
    html += '</div>';
  }

  // Extra tracks (MB tracks with no local match)
  if (c && c.extra_tracks && c.extra_tracks.length > 0) {
    html += `<div style="margin-top:6px;font-size:0.78em;color:#f88;">Missing MB tracks (${c.extra_tracks.length}):</div>`;
    html += '<div style="font-size:0.75em;padding-left:8px;color:#888;">';
    for (const t of c.extra_tracks) {
      const num = t.medium_index || t.index || t.track || '?';
      html += `<div>${num}. ${esc(t.title || '?')}</div>`;
    }
    html += '</div>';
  }

  // Actions
  html += '<div class="p-actions" style="margin-top:10px;">';
  if (e.files_exist) {
    html += `<button class="p-btn" style="border-color:#6a9;color:#6a9;" onclick="event.stopPropagation(); window.forceImportWrongMatch(${e.download_log_id}, this)">Force Import</button>`;
  }
  html += `<button class="p-btn delete" onclick="event.stopPropagation(); window.deleteWrongMatch(${e.download_log_id}, this)">Delete</button>`;
  html += '</div>';

  return html;
}

/**
 * Toggle detail visibility for a wrong match entry.
 * @param {string} id
 */
export function toggleWrongMatchDetail(id) {
  const el = document.getElementById(id);
  if (el) el.classList.toggle('open');
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
    if (data.status === 'ok') {
      btn.textContent = 'Imported';
      btn.style.borderColor = '#6d6';
      btn.style.color = '#6d6';
      toast(`Force imported: ${data.artist} - ${data.album}`);
      invalidateWrongMatches();
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
      // Remove the entry from the DOM
      const detail = document.getElementById(`wm-${logId}`);
      const item = detail?.previousElementSibling;
      if (detail) detail.remove();
      if (item) item.remove();
      toast('Wrong match deleted');
      invalidateWrongMatches();
    } else {
      btn.textContent = 'Failed';
      toast('Delete failed', true);
    }
  } catch (e) {
    btn.textContent = 'Error';
    toast('Delete request failed', true);
  }
}
