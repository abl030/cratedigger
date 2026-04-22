// @ts-check
import { state, API, toast } from './state.js';
import { esc, awstDate, awstDateTime, qualityLabel, externalReleaseUrl, sourceLabel } from './util.js';
import { renderDownloadHistoryItem } from './history.js';

/**
 * Load pipeline data from API and render.
 * @returns {Promise<void>}
 */
export async function loadPipeline() {
  const el = document.getElementById('pipeline-content');
  el.innerHTML = '<div class="loading">Loading...</div>';
  try {
    const r = await fetch(`${API}/api/pipeline/all`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    state.pipelineData = await r.json();
    renderPipeline();
  } catch (e) { el.innerHTML = '<div class="loading">Failed to load pipeline</div>'; }
}

/**
 * Set the pipeline filter and re-render.
 * @param {string} f
 */
export function setFilter(f) {
  state.pipelineFilter = f;
  renderPipeline();
}

/**
 * Render the pipeline view from cached data.
 */
export function renderPipeline() {
  const el = document.getElementById('pipeline-content');
  const data = state.pipelineData;
  if (!data) return;
  const counts = data.counts || {};
  const total = Object.values(counts).reduce((a, b) => a + b, 0);

  let items = [];
  if (state.pipelineFilter === 'all') {
    items = [...(data.wanted || []), ...(data.downloading || []), ...(data.imported || []), ...(data.manual || [])];
  } else if (state.pipelineFilter === 'wanted') {
    // Downloading is a sub-state of wanted — same album, mid-acquisition.
    // The status badge on each row still distinguishes them visually.
    items = [...(data.wanted || []), ...(data.downloading || [])];
  } else {
    items = data[state.pipelineFilter] || [];
  }

  // Group by artist
  const byArtist = {};
  for (const item of items) {
    const artist = item.artist_name || 'Unknown';
    if (!byArtist[artist]) byArtist[artist] = [];
    byArtist[artist].push(item);
  }
  // Sort artists alphabetically, albums by year within each
  const artists = Object.keys(byArtist).sort((a, b) => a.localeCompare(b));
  for (const a of artists) {
    byArtist[a].sort((x, y) => (x.year || 0) - (y.year || 0));
  }

  // Wanted bucket includes downloading — mid-acquisition is a sub-state
  // of wanted. The status badge on each row keeps them visually distinct.
  const wantedTotal = (counts.wanted || 0) + (counts.downloading || 0);
  el.innerHTML = `
    <div class="status-card">
      <div class="status-counts">
        <div class="count ${state.pipelineFilter === 'wanted' ? 'active' : ''}" onclick="window.setFilter('wanted')">
          <div class="count-num">${wantedTotal}</div><div class="count-label">Wanted</div>
        </div>
        <div class="count ${state.pipelineFilter === 'manual' ? 'active' : ''}" onclick="window.setFilter('manual')">
          <div class="count-num">${counts.manual || 0}</div><div class="count-label">Manual</div>
        </div>
        <div class="count ${state.pipelineFilter === 'imported' ? 'active' : ''}" onclick="window.setFilter('imported')">
          <div class="count-num">${counts.imported || 0}</div><div class="count-label">Imported</div>
        </div>
        <div class="count ${state.pipelineFilter === 'all' ? 'active' : ''}" onclick="window.setFilter('all')">
          <div class="count-num">${total}</div><div class="count-label">All</div>
        </div>
      </div>
    </div>
    ${artists.map(artist => `
      <div class="p-group-header" onclick="this.nextElementSibling.classList.toggle('collapsed')">
        ${esc(artist)} <span style="color:#555;font-weight:400;">${byArtist[artist].length}</span>
      </div>
      <div class="p-group-body">
        ${byArtist[artist].map(item => renderPipelineItem(item)).join('')}
      </div>
    `).join('')}
    ${artists.length === 0 ? '<div class="loading">No items</div>' : ''}
  `;
}

/**
 * Render a single pipeline item row.
 * @param {Object} item
 * @returns {string} HTML string
 */
export function renderPipelineItem(item) {
  const statusBadge = item.status === 'wanted' ? '<span class="badge badge-wanted">wanted</span>'
    : item.status === 'downloading' ? '<span class="badge badge-downloading">downloading</span>'
    : item.status === 'imported' ? '<span class="badge badge-imported">imported</span>'
    : '<span class="badge badge-manual">manual</span>';
  const srcClass = 'src-' + (item.source || 'request');
  const year = item.year || '?';
  const fmt = item.format || '?';
  const country = item.country || '';
  const date = awstDate(item.created_at || '');
  const attempts = [];
  if (item.search_attempts) attempts.push(`${item.search_attempts} search`);
  if (item.download_attempts) attempts.push(`${item.download_attempts} dl`);
  if (item.validation_attempts) attempts.push(`${item.validation_attempts} val`);
  const attemptStr = attempts.length ? attempts.join(', ') : '';
  const dist = item.beets_distance != null ? `dist ${item.beets_distance.toFixed(3)}` : '';
  // Last download verdict for context (e.g. why a wanted album is stuck)
  const lastVerdict = item.last_verdict || '';
  const lastColor = item.last_outcome === 'success' || item.last_outcome === 'force_import'
    ? '#6d6' : item.last_outcome === 'rejected' ? '#d88' : '#aa8';

  return `
    <div class="p-item ${srcClass}" onclick="window.toggleDetail(${item.id})">
      <div class="p-top">
        <div>
          <div class="p-title">${esc(item.album_title)}${statusBadge}</div>
        </div>
        <div style="font-size:0.75em;color:#666;">#${item.id}</div>
      </div>
      <div class="p-meta">
        <span>${year}</span>
        <span>${fmt}</span>
        ${country ? `<span>${country}</span>` : ''}
        <span>${item.source}</span>
        <span>${date}</span>
        ${attemptStr ? `<span>${attemptStr}</span>` : ''}
        ${dist ? `<span>${dist}</span>` : ''}
      </div>
      ${lastVerdict ? `<div class="p-meta" style="margin-top:2px;"><span style="color:${lastColor};">last: ${esc(lastVerdict)}</span>${item.download_count > 1 ? `<span>(${item.download_count} attempts)</span>` : ''}</div>` : ''}
    </div>
    <div class="p-detail" id="detail-${item.id}"></div>
  `;
}

/**
 * Toggle detail panel for a pipeline or recents item.
 * @param {string|number} elId - DOM id for the detail panel
 * @param {number} [requestId] - album_requests.id (defaults to elId for pipeline tab)
 * @returns {Promise<void>}
 */
export async function toggleDetail(elId, requestId) {
  // elId: unique DOM id for the detail panel (e.g. 'dl-123' for recents, or numeric for pipeline)
  // requestId: album_requests.id for the API fetch (optional, defaults to elId for pipeline tab)
  const id = requestId || elId;
  const el = document.getElementById(/** @type {string} */ (elId)) || document.getElementById('detail-' + elId);
  if (!el) return;
  if (el.classList.contains('open')) {
    el.classList.remove('open');
    return;
  }
  el.innerHTML = '<div class="loading" style="padding:8px;">Loading...</div>';
  el.classList.add('open');
  try {
    const r = await fetch(`${API}/api/pipeline/${id}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const req = data.request;
    const tracks = data.tracks || [];
    const history = data.history || [];

    let html = '';
    // External link (MB or Discogs)
    if (req.mb_release_id) {
      const label = sourceLabel(req.mb_release_id);
      const url = externalReleaseUrl(req.mb_release_id);
      if (label && url) {
        html += `<div class="p-detail-row"><span class="p-detail-label">${label}</span><span class="p-detail-value"><a href="${url}" target="_blank" rel="noopener" style="color:#6af;">${req.mb_release_id.slice(0,8)}...</a></span></div>`;
      }
    }
    if (req.imported_path) {
      html += `<div class="p-detail-row"><span class="p-detail-label">Imported to</span><span class="p-detail-value" style="font-size:0.9em;">${esc(req.imported_path)}</span></div>`;
    }

    // Quality summary — show spectral reality if it differs from nominal
    const beetsTracks = data.beets_tracks || [];
    if (beetsTracks.length > 0) {
      const minBr = Math.min(...beetsTracks.filter(t => t.bitrate).map(t => t.bitrate));
      const minBrKbps = minBr ? Math.round(minBr / 1000) : 0;
      const fmt = beetsTracks[0]?.format || '';
      const nominal = minBrKbps ? qualityLabel(fmt, minBrKbps) : fmt;
      // Current spectral data describes the files currently in beets.
      // Fall back to the most recent download's measurement for older rows.
      const spectralBr =
        req.current_spectral_bitrate || req.last_download_spectral_bitrate || null;
      const spectralGrade =
        req.current_spectral_grade || req.last_download_spectral_grade || null;
      const verified = req.verified_lossless === true || req.verified_lossless === 'True';
      let qualitySummary = nominal;
      if (verified) {
        qualitySummary += ' <span style="color:#6d6;">verified lossless</span>';
      } else if (spectralGrade === 'suspect' || spectralGrade === 'likely_transcode') {
        const brStr = spectralBr ? ` ~${spectralBr}kbps` : '';
        qualitySummary += ` <span style="color:#d88;">spectral: ${spectralGrade}${brStr}</span>`;
      } else if (spectralGrade === 'genuine') {
        // Show the spectral floor even on a genuine rollup. A non-null
        // spectral_bitrate under genuine means some tracks tripped the
        // cliff detector but the album-level grade stayed below the
        // suspect-pct threshold — the 96k signal is what the shared-
        // spectral clamp in compare_quality now consults (Eno case).
        const brStr = spectralBr ? ` ~${spectralBr}kbps` : '';
        qualitySummary += ` <span style="color:#6d6;">spectral: genuine${brStr}</span>`;
      }
      html += `<div class="p-detail-row"><span class="p-detail-label">Quality</span><span class="p-detail-value">${qualitySummary}</span></div>`;
    }

    // Tracks — labeled to clarify what we're looking at
    if (beetsTracks.length > 0) {
      html += '<div class="p-tracks"><div class="p-detail-label" style="margin-bottom:4px;">In Library (' + beetsTracks.length + ' tracks)</div>';
      html += beetsTracks.map(t => {
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
    } else if (tracks.length > 0) {
      html += '<div class="p-tracks"><div class="p-detail-label" style="margin-bottom:4px;">Expected Tracks from MusicBrainz (' + tracks.length + ')</div>';
      html += tracks.map(t => {
        const dur = t.length_seconds ? `${Math.floor(t.length_seconds/60)}:${String(Math.round(t.length_seconds%60)).padStart(2,'0')}` : '';
        return `<div class="p-track">${t.disc_number > 1 ? t.disc_number + '.' : ''}${t.track_number}. ${esc(t.title)} ${dur ? '<span style="color:#555;">' + dur + '</span>' : ''}</div>`;
      }).join('');
      html += '</div>';
    }

    // Download history
    if (history.length > 0) {
      html += '<div class="p-history"><div class="p-detail-label" style="margin-bottom:4px;">Download History (' + history.length + ')</div>';
      html += history.map(renderDownloadHistoryItem).join('');
      html += '</div>';
    }

    // Status change buttons
    html += `<div class="p-actions">
      <span class="p-detail-label" style="line-height:28px;">Status:</span>
      <button class="p-btn ${req.status === 'wanted' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.updateStatus(${id}, 'wanted')">wanted</button>
      <button class="p-btn ${req.status === 'imported' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.updateStatus(${id}, 'imported')">imported</button>
      <button class="p-btn ${req.status === 'manual' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.updateStatus(${id}, 'manual')">manual</button>
      <button class="p-btn delete" onclick="event.stopPropagation(); window.deleteRequest(${id})">delete</button>
    </div>`;

    el.innerHTML = html;
  } catch (e) { el.innerHTML = '<div class="loading" style="padding:8px;">Failed to load details</div>'; }
}

/**
 * Delete a pipeline request.
 * @param {number} id
 * @returns {Promise<void>}
 */
export async function deleteRequest(id) {
  if (!confirm(`Delete pipeline request #${id}?`)) return;
  try {
    const r = await fetch(`${API}/api/pipeline/delete`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id}),
    });
    const data = await r.json();
    if (data.status === 'ok') {
      toast(`Deleted #${id}`);
      loadPipeline();
    } else {
      toast(data.error || 'Delete failed', true);
    }
  } catch (e) { toast('Delete failed', true); }
}

/**
 * Update the status of a pipeline request.
 * @param {number} id
 * @param {string} newStatus
 * @returns {Promise<void>}
 */
export async function updateStatus(id, newStatus) {
  try {
    const r = await fetch(`${API}/api/pipeline/update`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id, status: newStatus}),
    });
    const data = await r.json();
    if (data.status === 'ok') {
      toast(`#${id} → ${newStatus}`);
      loadPipeline();
    } else {
      toast(data.error || 'Update failed', true);
    }
  } catch (e) { toast('Update failed', true); }
}
