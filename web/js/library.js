// @ts-check
import { API, state, toast, updatePipelineStatus } from './state.js';
import { esc, jsArg, qualityLabel, overrideToIntent, externalReleaseUrl, sourceLabel } from './util.js';
import { renderTypedSections } from './grouping.js';
import { renderActionToolbar } from './release_actions.js';
import { renderStatusBadges } from './badges.js';
import { renderDownloadHistoryItem } from './history.js';


function refreshAfterBeetsDeletion(albumId) {
  if (state.browseArtist) {
    delete state.browseCache[state.browseArtist.id];
  }

  const browseArtist = document.getElementById('browse-artist');
  if (browseArtist && browseArtist.style.display !== 'none' && state.browseArtist) {
    window.switchSubView?.(state.browseSubView || 'discography');
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
 * Render library results into a target element (thin wrapper).
 * @param {HTMLElement} targetEl
 * @param {Object[]} albums
 */
export function renderLibraryResultsInto(targetEl, albums) {
  renderLibraryResults(albums, targetEl);
}

/**
 * Render library albums grouped by artist, then by release group.
 * @param {Object[]} albums
 * @param {HTMLElement} [targetEl]
 */
export function renderLibraryResults(albums, targetEl) {
  const el = targetEl || document.getElementById('library-content');
  if (albums.length === 0) {
    el.innerHTML = '<div class="loading">No results</div>';
    return;
  }
  // Group by artist
  const byArtist = {};
  for (const a of albums) {
    const artist = a.artist || 'Unknown';
    if (!byArtist[artist]) byArtist[artist] = [];
    byArtist[artist].push(a);
  }
  const artists = Object.keys(byArtist).sort((a, b) => a.localeCompare(b));
  // Sort albums within each artist by year
  for (const a of artists) {
    byArtist[a].sort((x, y) => (x.year || 0) - (y.year || 0));
  }

  // Auto-expand if only one artist
  const autoOpen = artists.length === 1;

  el.innerHTML = artists.map(artist => {
    const artistAlbums = byArtist[artist];

    // Group by release group within artist
    const rgOrder = [];
    const byRG = {};
    for (const a of artistAlbums) {
      const rgKey = a.mb_releasegroupid || ('_' + a.id);
      if (!byRG[rgKey]) {
        byRG[rgKey] = { title: a.release_group_title || a.album, year: a.year, albums: [] };
        rgOrder.push(rgKey);
      }
      byRG[rgKey].albums.push(a);
    }
    const rgCount = rgOrder.length;

    function renderAlbum(a) {
      const added = a.added ? new Date(a.added * 1000 + 8 * 3600000).toISOString().slice(0, 10) : '?';
      const mbid = a.mb_albumid || '';
      const inLibrary = a.in_library !== false;
      const beetsAlbumId = a.beets_album_id ?? null;
      const pipelineId = a.pipeline_id || null;
      const detailId = inLibrary
        ? `lib-${beetsAlbumId}`
        : `lib-pipeline-${pipelineId || a.id}`;
      const detailToggle = inLibrary && beetsAlbumId
        ? `window.toggleLibDetail(${beetsAlbumId})`
        : `window.toggleDetail('${detailId}', ${pipelineId || a.id})`;

      const toolbar = mbid ? renderActionToolbar({
        id: mbid,
        in_library: inLibrary,
        beets_album_id: beetsAlbumId,
        pipeline_status: a.pipeline_status || null,
        pipeline_id: pipelineId,
        upgrade_queued: !!a.upgrade_queued,
        artist: a.artist || '',
        album: a.album || '',
        track_count: a.track_count || 0,
      }, { size: 'small' }) : '';

      const badges = renderStatusBadges({
        id: a.mb_albumid,
        in_library: inLibrary,
        library_format: a.formats,
        library_min_bitrate: a.min_bitrate ? Math.round(a.min_bitrate / 1000) : 0,
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

    // Build per-RG display rows, then group them by Album/EP/Single/etc.
    // so an artist with mixed types renders the same sectioning the
    // Discography sub-tab uses. Within a section, sort by year.
    const rgRows = rgOrder.map(rgKey => {
      const rg = byRG[rgKey];
      const sample = rg.albums[0];
      const html = rg.albums.length === 1
        ? renderAlbum(sample)
        : `<div class="lib-rg">
             <div class="lib-rg-header" onclick="this.nextElementSibling.classList.toggle('open')">
               <span>${rg.year || '?'} ${esc(rg.title)}</span>
               <span class="lib-artist-count">${rg.albums.length} versions</span>
             </div>
             <div class="lib-rg-body">
               ${rg.albums.map(renderAlbum).join('')}
             </div>
           </div>`;
      return {
        // Fields classify() reads
        type: sample.type || '',
        secondary_types: [],
        // Sort key
        first_release_date: String(rg.year || ''),
        _html: html,
      };
    });
    const albumsHtml = renderTypedSections(rgRows, (r) => r._html);

    return `
      <div class="lib-artist">
        <div class="lib-artist-header" onclick="this.nextElementSibling.classList.toggle('open')">
          <span class="lib-artist-name">${esc(artist)}</span>
          <span class="lib-artist-count">${rgCount} release${rgCount !== 1 ? 's' : ''}</span>
        </div>
        <div class="lib-artist-body${autoOpen ? ' open' : ''}">
          ${albumsHtml}
        </div>
      </div>
    `;
  }).join('');
}

/**
 * Toggle the detail panel for a library album.
 * @param {number} id - Beets album ID
 */
export async function toggleLibDetail(id) {
  const el = document.getElementById('lib-' + id);
  if (el.classList.contains('open')) { el.classList.remove('open'); return; }
  el.innerHTML = '<div class="loading" style="padding:8px;">Loading...</div>';
  el.classList.add('open');
  try {
    const r = await fetch(`${API}/api/beets/album/${id}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    let html = '';
    if (data.path) {
      html += `<div class="p-detail-row"><span class="p-detail-label">Path</span><span class="p-detail-value" style="font-size:0.85em;word-break:break-all;">${esc(data.path)}</span></div>`;
    }
    if (data.mb_albumid) {
      const label = sourceLabel(data.mb_albumid);
      const url = externalReleaseUrl(data.mb_albumid);
      html += `<div class="p-detail-row"><span class="p-detail-label">${label}</span><span class="p-detail-value"><a href="${url}" target="_blank" rel="noopener" style="color:#6af;">${data.mb_albumid.slice(0,8)}...</a></span></div>`;
    }
    if (data.label) {
      html += `<div class="p-detail-row"><span class="p-detail-label">Label</span><span class="p-detail-value">${esc(data.label)}</span></div>`;
    }
    // Tracks
    if (data.tracks && data.tracks.length > 0) {
      html += '<div class="p-tracks"><div class="p-detail-label" style="margin-bottom:4px;">Tracks (' + data.tracks.length + ')</div>';
      html += data.tracks.map(t => {
        const dur = t.length ? `${Math.floor(t.length/60)}:${String(Math.round(t.length%60)).padStart(2,'0')}` : '';
        const br = t.bitrate ? `${Math.round(t.bitrate/1000)}kbps` : '';
        const depth = t.bitdepth && t.bitdepth > 16 ? `${t.bitdepth}bit` : '';
        const sr = t.samplerate && t.samplerate > 44100 ? `${(t.samplerate/1000).toFixed(1)}kHz` : '';
        const meta = [t.format, br, depth, sr].filter(Boolean).join(' ');
        return `<div class="lib-track">
          <span>${t.disc > 1 ? t.disc + '.' : ''}${t.track}. ${esc(t.title)} ${dur ? '<span style="color:#555;">' + dur + '</span>' : ''}</span>
          <span class="lib-track-meta">${meta}</span>
        </div>`;
      }).join('');
      html += '</div>';
    }
    // Pipeline download history
    const history = data.download_history || [];
    if (history.length > 0) {
      html += '<div class="p-history"><div class="p-detail-label" style="margin-bottom:4px;">Download History (' + history.length + ')</div>';
      html += history.map(renderDownloadHistoryItem).join('');
      html += '</div>';
    } else if (data.pipeline_status) {
      html += `<div class="p-detail-row"><span class="p-detail-label">Pipeline</span><span class="p-detail-value">${data.pipeline_status} (${data.pipeline_source || '?'})</span></div>`;
    }
    // Pipeline controls (status + quality override)
    if (data.mb_albumid && data.pipeline_id) {
      const pStatus = data.pipeline_status || '';
      html += `<div class="p-actions" style="margin-top:10px;">
        <span class="p-detail-label" style="line-height:28px;">Status:</span>
        <button class="p-btn ${pStatus === 'wanted' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.setLibQuality('${data.mb_albumid}', 'wanted', null)">wanted</button>
        <button class="p-btn ${pStatus === 'imported' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.setLibQuality('${data.mb_albumid}', 'imported', null)">imported</button>
        <button class="p-btn ${pStatus === 'manual' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.setLibQuality('${data.mb_albumid}', 'manual', null)">manual</button>
      </div>`;
      html += `<div class="p-actions" style="margin-top:6px;">
        <span class="p-detail-label" style="line-height:28px;">Min bitrate:</span>
        <input type="number" id="lib-minbr-${id}" value="" placeholder="${data.pipeline_min_bitrate || ''}" style="width:60px;padding:2px 6px;background:#222;color:#eee;border:1px solid #444;border-radius:4px;font-size:0.8em;" onclick="event.stopPropagation()">
        <button class="p-btn" onclick="event.stopPropagation(); var v=document.getElementById('lib-minbr-${id}').value; if(v) window.setLibQuality('${data.mb_albumid}', null, parseInt(v))">Set</button>
        <button class="p-btn" onclick="event.stopPropagation(); window.setLibQuality('${data.mb_albumid}', 'imported', null)">Accept</button>
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
    // Upgrade + Delete buttons
    html += '<div class="p-actions" style="margin-top:6px;">';
    if (data.mb_albumid) {
      const bitrates = (data.tracks || []).map(t => t.bitrate).filter(b => b && b > 0);
      const minBr = bitrates.length > 0 ? Math.round(Math.min(...bitrates) / 1000) : null;
      const brLabel = minBr ? ` (lowest: ${minBr}kbps)` : '';
      if (!data.upgrade_queued) {
        html += `<button class="p-btn upgrade-btn" onclick="event.stopPropagation(); window.upgradeAlbum('${data.mb_albumid}', this)">Upgrade${brLabel}</button>`;
      }
    }
    html += `<button class="p-btn delete-beets" onclick="event.stopPropagation(); window.confirmDeleteBeets(${id}, ${jsArg(data.artist || '')}, ${jsArg(data.album || '')}, ${data.tracks ? data.tracks.length : 0}, ${data.pipeline_id ?? 'null'}, ${jsArg(data.mb_albumid || '')})">Delete from beets</button>`;
    html += '</div>';
    el.innerHTML = html;
  } catch (e) { el.innerHTML = '<div class="loading" style="padding:8px;">Failed to load</div>'; }
}

/**
 * Ban a Soulseek source for an album.
 * @param {number} requestId
 * @param {string} username
 * @param {string} mbid
 */
export async function banSource(requestId, username, mbid) {
  if (!confirm(`Ban ${username} for this album?\nThis will remove files from beets and requeue for re-download.`)) return;
  try {
    const r = await fetch(`${API}/api/pipeline/ban-source`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({request_id: requestId, username, mb_release_id: mbid}),
    });
    const data = await r.json();
    if (data.status === 'ok') {
      // Issue #123 PR B: distinguish three outcomes. `cleanup_errors`
      // non-empty means at least one beet-remove selector timed out /
      // exited non-zero / raised — the denylist committed, but the
      // album may still be on disk under one of the selectors. Show
      // a warning toast so the user knows to investigate rather than
      // reporting a clean "not in beets".
      const errs = data.cleanup_errors || [];
      if (errs.length > 0) {
        const reasons = errs.map(e => e.reason).join(', ');
        toast(`Banned ${username}, but beet remove had ${errs.length} failure(s) (${reasons}). Album may still be on disk; check journalctl.`, true);
      } else {
        toast(`Banned ${username}, ${data.beets_removed ? 'removed from beets' : 'not in beets'}, requeued`);
      }
    } else {
      toast(data.error || 'Ban failed', true);
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
    const body = {mb_release_id: mbid};
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
  btn.disabled = true;
  btn.textContent = 'Queuing...';
  try {
    const r = await fetch(`${API}/api/pipeline/upgrade`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({mb_release_id: mbid}),
    });
    const data = await r.json();
    if (data.status === 'upgrade_queued') {
      btn.textContent = 'Queued';
      btn.style.borderColor = '#6a9';
      btn.style.color = '#6a9';
      updatePipelineStatus(mbid, 'wanted', data.id);
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
    if (data.status === 'ok') {
      if (data.pipeline_deleted && releaseId) {
        updatePipelineStatus(releaseId, null, null);
      }
      const pipelineMsg = data.pipeline_deleted ? ', request removed' : '';
      toast(`Deleted: ${data.artist} - ${data.album} (${data.deleted_files} files${pipelineMsg})`);
      refreshAfterBeetsDeletion(id);
    } else {
      toast(data.error || 'Delete failed', true);
    }
  } catch (e) {
    document.querySelector('.confirm-overlay')?.remove();
    toast('Delete failed', true);
  }
}
