// @ts-check
import { API, state, toast, updatePipelineStatus } from './state.js';
import { esc, jsArg, overrideToIntent, normalizeReleaseId } from './util.js';
import { buildReleaseActionState } from './release_action_state.js';
import { renderActionToolbar, renderAcquireActionButton, renderRemoveFromBeetsButton, renderBadRipButton } from './release_actions.js';
import { renderStatusBadges } from './badges.js';
import { renderDownloadHistoryItem } from './history.js';
import {
  renderBeetsTrackRow, renderDetailRow, renderExternalLinkRow, toggleExpand,
} from './render_primitives.js';


function refreshAfterBeetsDeletion(albumId) {
  if (state.browseArtist) {
    delete state.browseCache[state.browseArtist.id];
  }

  const browseArtist = document.getElementById('browse-artist');
  if (browseArtist && browseArtist.style.display !== 'none' && state.browseArtist) {
    window.reloadBrowseArtist?.();
    return;
  }

  const activeTab = document.querySelector('.tab.active')?.textContent?.trim();
  if (activeTab === 'Recents') {
    window.loadRecents?.();
    return;
  }
  if (activeTab === 'Pipeline') {
    window.loadPipeline?.();
    return;
  }

  const detail = document.getElementById('lib-' + albumId);
  if (detail) {
    detail.previousElementSibling?.remove();
    detail.remove();
  }
}

/**
 * One library-album (or pipeline-only request) row with its detail
 * placeholder. Feeds the unified artist page's In-flight section
 * (issue #575 PR4); in-library rows expand via toggleLibDetail,
 * pipeline-only rows via the pipeline toggleDetail.
 * @param {Object} a - LibraryAlbumRow-shaped row from /api/library/artist
 * @returns {string}
 */
export function renderLibraryAlbumRow(a) {
  const added = a.added ? new Date(a.added * 1000 + 8 * 3600000).toISOString().slice(0, 10) : '?';
  const mbid = normalizeReleaseId(a.mb_albumid);
  const inLibrary = a.in_library !== false;
  const beetsAlbumId = a.beets_album_id ?? null;
  const pipelineId = a.pipeline_id || null;
  const detailId = inLibrary
    ? `lib-${beetsAlbumId}`
    : `lib-pipeline-${pipelineId || a.id}`;
  const detailToggle = inLibrary && beetsAlbumId
    ? `window.toggleLibDetail(${beetsAlbumId})`
    : `window.toggleDetail('${detailId}', ${pipelineId || a.id})`;

  const actionState = mbid ? buildReleaseActionState({
    id: mbid,
    in_library: inLibrary,
    beets_album_id: beetsAlbumId,
    pipeline_status: a.pipeline_status || null,
    pipeline_id: pipelineId,
    artist: a.artist || '',
    album: a.album || '',
    track_count: a.track_count || 0,
  }) : null;
  const toolbar = actionState ? renderActionToolbar(actionState, { size: 'small' }) : '';

  const badges = renderStatusBadges({
    id: mbid,
    in_library: inLibrary,
    library_format: a.formats,
    library_min_bitrate: a.min_bitrate ? Math.round(a.min_bitrate / 1000) : 0,
    library_avg_bitrate: a.avg_bitrate ? Math.floor(a.avg_bitrate / 1000) : 0,
    library_rank: a.library_rank,
    pipeline_status: a.pipeline_status,
  });
  return `
    <div class="lib-item" onclick="${detailToggle}">
      <div class="p-top">
        <div>
          <div class="p-title">${esc(a.album)}${badges}</div>
        </div>
        <div style="display:flex;align-items:center;gap:6px;">
          ${toolbar}
          <span style="font-size:0.75em;color:#666;">${a.track_count}t</span>
        </div>
      </div>
      <div class="p-meta">
        <span>${a.year || '?'}</span>
        ${a.country ? `<span>${a.country}</span>` : ''}
        ${a.type ? `<span>${a.type}</span>` : ''}
        <span>added ${added}</span>
      </div>
    </div>
    <div class="lib-detail" id="${detailId}"></div>
  `;
}

/**
 * Fetch /api/beets/album/<id> and render the deep library detail body.
 * Shared by toggleLibDetail (album rows) and toggleReleaseLibDetail
 * (the release-detail sub-panel on the unified artist page).
 * @param {number} id - Beets album ID
 * @returns {Promise<string>}
 */
async function fetchLibraryDetailBody(id) {
  const r = await fetch(`${API}/api/beets/album/${id}`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const data = await r.json();
  return renderLibraryDetailBody(data, id);
}

/**
 * Toggle the detail panel for a library album row.
 * @param {number} id - Beets album ID
 */
export async function toggleLibDetail(id) {
  const el = document.getElementById('lib-' + id);
  await toggleExpand(el, async (target) => {
    target.innerHTML = await fetchLibraryDetailBody(id);
  });
}

/**
 * Toggle the deep library detail inside an expanded release-detail panel
 * (the unified artist page's replacement for the old Library sub-view's
 * album detail). Targets the `libdet-<id>` placeholder emitted by
 * renderReleaseDetail.
 * @param {number} id - Beets album ID
 */
export async function toggleReleaseLibDetail(id) {
  const el = document.getElementById('libdet-' + id);
  await toggleExpand(el, async (target) => {
    target.innerHTML = await fetchLibraryDetailBody(id);
  });
}

/**
 * The deep library detail body: path, source link, label, tracks,
 * download history, pipeline status / min-bitrate / intent controls,
 * acquire + delete + bad-rip actions.
 * @param {Object} data - /api/beets/album/<id> payload
 * @param {number} id - Beets album ID
 * @returns {string}
 */
export function renderLibraryDetailBody(data, id) {
    const releaseId = normalizeReleaseId(data.mb_albumid);
    const releaseArg = jsArg(releaseId);
    let html = '';
    if (data.path) {
      html += renderDetailRow('Path', esc(data.path), { valueStyle: 'font-size:0.85em;word-break:break-all;' });
    }
    html += renderExternalLinkRow(releaseId);
    if (data.label) {
      html += renderDetailRow('Label', esc(data.label));
    }
    // Tracks
    if (data.tracks && data.tracks.length > 0) {
      html += '<div class="p-tracks"><div class="p-detail-label" style="margin-bottom:4px;">Tracks (' + data.tracks.length + ')</div>';
      html += data.tracks.map(renderBeetsTrackRow).join('');
      html += '</div>';
    }
    // Pipeline download history
    const history = data.download_history || [];
    if (history.length > 0) {
      html += '<div class="p-history"><div class="p-detail-label" style="margin-bottom:4px;">Download History (' + history.length + ')</div>';
      html += history.map(renderDownloadHistoryItem).join('');
      html += '</div>';
    } else if (data.pipeline_status) {
      html += renderDetailRow('Pipeline', `${data.pipeline_status} (${data.pipeline_source || '?'})`);
    }
    // Pipeline controls (status + quality override)
    if (releaseId && data.pipeline_id) {
      const pStatus = data.pipeline_status || '';
      html += `<div class="p-actions" style="margin-top:10px;">
        <span class="p-detail-label" style="line-height:28px;">Status:</span>
        <button class="p-btn ${pStatus === 'wanted' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.setLibQuality(${releaseArg}, 'wanted', null)">wanted</button>
        <button class="p-btn ${pStatus === 'imported' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.setLibQuality(${releaseArg}, 'imported', null)">imported</button>
        <button class="p-btn ${pStatus === 'manual' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.setLibQuality(${releaseArg}, 'manual', null)">manual</button>
      </div>`;
      html += `<div class="p-actions" style="margin-top:6px;">
        <span class="p-detail-label" style="line-height:28px;">Min bitrate:</span>
        <input type="number" id="lib-minbr-${id}" value="" placeholder="${data.pipeline_min_bitrate || ''}" style="width:60px;padding:2px 6px;background:#222;color:#eee;border:1px solid #444;border-radius:4px;font-size:0.8em;" onclick="event.stopPropagation()">
        <button class="p-btn" onclick="event.stopPropagation(); var v=document.getElementById('lib-minbr-${id}').value; if(v) window.setLibQuality(${releaseArg}, null, parseInt(v))">Set</button>
        <button class="p-btn" onclick="event.stopPropagation(); window.setLibQuality(${releaseArg}, 'imported', null)">Accept</button>
      </div>`;
      const currentIntent = overrideToIntent(data.target_format);
      html += `<div class="p-actions" style="margin-top:6px;">
        <span class="p-detail-label" style="line-height:28px;">Intent:</span>
        <select id="lib-intent-${id}" style="padding:2px 6px;background:#222;color:#eee;border:1px solid #444;border-radius:4px;font-size:0.8em;" onclick="event.stopPropagation()" onchange="event.stopPropagation(); window.setIntent(${data.pipeline_id}, this.value)">
          <option value="default"${currentIntent === 'default' ? ' selected' : ''}>Default</option>
          <option value="lossless"${currentIntent === 'lossless' ? ' selected' : ''}>Lossless</option>
        </select>
      </div>`;
    }
    const actionState = buildReleaseActionState({
      id: releaseId || '',
      in_library: true,
      beets_album_id: id,
      pipeline_status: data.pipeline_status || null,
      pipeline_id: data.pipeline_id || null,
      artist: data.artist || '',
      album: data.album || '',
      track_count: data.tracks ? data.tracks.length : 0,
    });

    // Acquire + Delete buttons share the same action-state seam as rows.
    html += '<div class="p-actions" style="margin-top:6px;">';
    const bitrates = (data.tracks || []).map(t => t.bitrate).filter(b => b && b > 0);
    const minBr = bitrates.length > 0 ? Math.round(Math.min(...bitrates) / 1000) : null;
    const brLabel = minBr ? ` (lowest: ${minBr}kbps)` : '';
    html += renderAcquireActionButton(actionState, {
      className: 'p-btn',
      addClassName: 'p-btn upgrade-btn',
      upgradeClassName: 'p-btn upgrade-btn',
      removeClassName: 'p-btn remove-request',
      upgradeLabel: `Upgrade${brLabel}`,
      stopPropagation: true,
      hideDisabled: true,
    });
    html += renderRemoveFromBeetsButton(actionState, {
      className: 'p-btn delete-beets',
      label: 'Delete from beets',
      stopPropagation: true,
    });
    html += renderBadRipButton(actionState, {
      className: 'p-btn delete-beets',
      stopPropagation: true,
    });
    html += '</div>';
    return html;
}

/**
 * Mark an imported album as a bad rip (issue #188).
 * The route resolves the supplying user from download_log, hashes the
 * imported tracks (tag-stripped), persists known-bad hashes, denylists
 * the user, removes from beets, and requeues. The frontend does not
 * need to know the username.
 *
 * @param {number} requestId
 * @param {string} mbid
 */
export async function banSource(requestId, mbid) {
  if (!confirm('Mark this album as a bad rip?\nThe most recent uploader will be denylisted, the album removed from beets, and the request requeued.')) return;
  try {
    const r = await fetch(`${API}/api/pipeline/ban-source`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({request_id: requestId, mb_release_id: mbid, confirm: 'BAN'}),
    });
    if (r.status === 409) {
      toast('Importer is busy with this album — try again in a moment.', true);
      return;
    }
    const data = await r.json();
    if (data.status !== 'ok') {
      toast(data.error || 'Ban failed', true);
      return;
    }
    const partial = data.partial_failures || {};
    const cleanupErrs = partial.cleanup_errors || [];
    const hashErrs = partial.hash_capture_errors || [];
    const removed = data.beets_removed ? 'removed from beets' : 'not in beets';
    const hashCount = data.hashes_recorded ?? 0;
    const hashes = `${hashCount} hash${hashCount === 1 ? '' : 'es'} recorded`;
    const head = data.username
      ? `Banned ${data.username}: ${removed}, ${hashes}, requeued.`
      : `Album ${removed} (no Soulseek user on record), ${hashes}, requeued.`;
    if (cleanupErrs.length > 0 || hashErrs.length > 0) {
      const warnings = [];
      if (cleanupErrs.length) warnings.push(`${cleanupErrs.length} beet remove failure(s)`);
      if (hashErrs.length) warnings.push(`${hashErrs.length} hash capture failure(s)`);
      console.warn('ban-source partial failures:', partial);
      toast(`${head} Warnings: ${warnings.join(', ')} — see console.`, true);
    } else {
      toast(head);
    }
  } catch (e) { toast('Ban failed', true); }
}

/**
 * Set pipeline quality/status for a release.
 * @param {string} mbid
 * @param {string|null} status
 * @param {number|null} minBitrate
 * @param {number} [detailId]
 */
export async function setLibQuality(mbid, status, minBitrate, detailId) {
  try {
    const releaseId = normalizeReleaseId(mbid) || mbid;
    const body = {mb_release_id: releaseId};
    if (status) body.status = status;
    if (minBitrate != null) body.min_bitrate = minBitrate;
    const r = await fetch(`${API}/api/pipeline/set-quality`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const data = await r.json();
    if (data.status === 'ok') {
      const parts = [];
      if (status) parts.push(status);
      if (minBitrate != null) parts.push(`min_bitrate=${minBitrate}`);
      toast(`Set ${parts.join(', ')}`);
      // Refresh the whole recents/library view to update badges
      const activeTab = document.querySelector('.tab.active');
      if (activeTab) {
        const tabText = activeTab.textContent.trim();
        if (tabText === 'Recents') window.loadRecents();
      }
    } else {
      toast(data.error || 'Failed', true);
    }
  } catch (e) { toast('Failed', true); }
}

/**
 * Queue an album for quality upgrade.
 * @param {string} mbid
 * @param {HTMLButtonElement} btn
 */
export async function upgradeAlbum(mbid, btn) {
  const releaseId = normalizeReleaseId(mbid) || mbid;
  btn.disabled = true;
  btn.textContent = 'Queuing...';
  try {
    const r = await fetch(`${API}/api/pipeline/upgrade`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({mb_release_id: releaseId}),
    });
    const data = await r.json();
    if (data.status === 'upgrade_queued') {
      btn.textContent = 'Queued';
      btn.style.borderColor = '#6a9';
      btn.style.color = '#6a9';
      updatePipelineStatus(releaseId, 'wanted', data.id);
      const br = data.min_bitrate ? ` from ${data.min_bitrate}kbps` : '';
      const tiers = data.search_filetype_override || 'default';
      toast(`Upgrade queued${br} — searching ${tiers}`);
    } else {
      btn.textContent = 'Error';
      toast(data.error || 'Upgrade failed', true);
    }
  } catch (e) {
    btn.textContent = 'Error';
    toast('Upgrade failed', true);
  }
}

/**
 * Set quality intent for a pipeline request.
 * @param {number} pipelineId
 * @param {string} intent
 */
export async function setIntent(pipelineId, intent) {
  try {
    const r = await fetch(`${API}/api/pipeline/set-intent`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id: pipelineId, intent}),
    });
    const data = await r.json();
    if (data.status === 'ok') {
      const msg = data.requeued ? `Intent: ${intent} (requeued)` : `Intent: ${intent}`;
      toast(msg);
    } else {
      toast(data.error || 'Failed to set intent', true);
    }
  } catch (e) { toast('Failed to set intent', true); }
}

/**
 * Build the confirmation overlay HTML for deleting an album from beets.
 * @param {number} id
 * @param {string} artist
 * @param {string} album
 * @param {number} trackCount
 * @param {number|null} [pipelineId]
 * @param {string} [releaseId]
 */
export function buildDeleteConfirmHtml(id, artist, album, trackCount, pipelineId = null, releaseId = '') {
  const parsedTrackCount = Number(trackCount);
  const safeTrackCount = Number.isFinite(parsedTrackCount) ? parsedTrackCount : 0;
  const pipelineNote = releaseId
    ? '<br>This also removes any matching pipeline request/history so the release is forgotten.'
    : '';
  return `
    <div class="confirm-box">
      <h3>Delete from beets?</h3>
      <p>${esc(artist)} - ${esc(album)}<br>${safeTrackCount} tracks will be permanently deleted from disk.${pipelineNote}</p>
      <div class="actions">
        <button class="p-btn" onclick="this.closest('.confirm-overlay').remove()">Cancel</button>
        <button class="p-btn delete-beets" id="confirm-delete-btn" onclick="window.executeBeetsDeletion(${id}, this, ${pipelineId ?? 'null'}, ${jsArg(releaseId)})">Yes, delete permanently</button>
      </div>
    </div>
  `;
}

/**
 * Show a confirmation overlay for deleting an album from beets.
 * @param {number} id
 * @param {string} artist
 * @param {string} album
 * @param {number} trackCount
 * @param {number|null} [pipelineId]
 * @param {string} [releaseId]
 */
export function confirmDeleteBeets(id, artist, album, trackCount, pipelineId = null, releaseId = '') {
  const overlay = document.createElement('div');
  overlay.className = 'confirm-overlay';
  overlay.innerHTML = buildDeleteConfirmHtml(
    id, artist, album, trackCount, pipelineId, releaseId,
  );
  document.body.appendChild(overlay);
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
}

/**
 * Convert the typed delete wire result into UI semantics.
 * @param {any} data
 * @returns {{message: string, error: boolean, completed: boolean}}
 */
export function describeBeetsDeletion(data) {
  const warnings = (data.notifications || []).filter(n => n.status === 'warning');
  const preserved = data.preserved_paths || [];
  const warningParts = [];
  if (preserved.length) {
    warningParts.push(`${preserved.length} unknown path${preserved.length === 1 ? '' : 's'} preserved`);
  }
  if (warnings.length) {
    const details = warnings
      .map(n => `${n.provider}: ${n.detail || 'unspecified warning'}`)
      .join('; ');
    warningParts.push(
      `${warnings.length} media notification warning${warnings.length === 1 ? '' : 's'} (${details})`,
    );
  }
  const warningSuffix = warningParts.length ? `; ${warningParts.join(', ')}` : '';
  if (data.status === 'ok') {
    const pipelineMsg = data.pipeline_deleted ? ', request removed' : '';
    return {
      message: `Deleted: ${data.artist} - ${data.album} (${data.deleted_files} tracks, ${data.deleted_artifacts} owned artifacts${pipelineMsg})${warningSuffix}`,
      error: warningParts.length > 0,
      completed: true,
    };
  }
  if (data.status === 'partial' && data.album_deleted) {
    return {
      message: `Album deleted, but pipeline request #${data.pipeline_id} remains. Retry pipeline cleanup after checking logs${warningSuffix}.`,
      error: true,
      completed: true,
    };
  }
  if (data.acknowledgement_lost) {
    return {
      message: data.detail || 'Beets acknowledgement was lost; filesystem deletion is unconfirmed, metadata may be gone, and the pipeline row was preserved for explicit recovery.',
      error: true,
      completed: false,
    };
  }
  const detail = data.detail ? `: ${data.detail}` : '';
  return {
    message: `${data.error || 'Delete failed'}${detail}`,
    error: true,
    completed: false,
  };
}

/**
 * Execute the beets deletion after confirmation.
 * @param {number} id
 * @param {HTMLButtonElement} btn
 * @param {number|null} [pipelineId]
 * @param {string} [releaseId]
 */
export async function executeBeetsDeletion(id, btn, pipelineId = null, releaseId = '') {
  btn.disabled = true;
  btn.textContent = 'Deleting...';
  try {
    const r = await fetch(`${API}/api/beets/delete`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        id,
        confirm: 'DELETE',
        purge_pipeline: true,
        pipeline_id: pipelineId,
        release_id: releaseId,
      }),
    });
    const data = await r.json();
    document.querySelector('.confirm-overlay')?.remove();
    const display = describeBeetsDeletion(data);
    if (data.status === 'ok') {
      if (data.pipeline_deleted && releaseId) {
        updatePipelineStatus(releaseId, null, null);
      }
      toast(display.message, display.error);
      refreshAfterBeetsDeletion(id);
    } else if (data.status === 'partial' && data.album_deleted) {
      toast(display.message, display.error);
      refreshAfterBeetsDeletion(id);
    } else {
      toast(display.message, display.error);
    }
  } catch (e) {
    document.querySelector('.confirm-overlay')?.remove();
    toast('Delete failed', true);
  }
}
