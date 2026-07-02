// @ts-check
import { state, API, toast } from './state.js';
import { esc, awstDate, qualityLabel, externalReleaseUrl, sourceLabel, manualReasonLabel, renderForensicBlock } from './util.js';
import { renderDownloadHistoryItem } from './history.js';
import { renderBadRipButton, renderReplaceButton } from './release_actions.js';
import { renderSearchPlanButton, renderSearchPlanDetail } from './search_plan.js';
import { loadLongTail, renderLongTailBody, restoreLongTailConsoles } from './long_tail.js';
import { renderPipelineDashboard as renderDashboardCards } from './pipeline_dashboard.js';

/**
 * Load pipeline data from API and render.
 * @returns {Promise<void>}
 */
export async function loadPipeline() {
  if (state.pipelineView === 'dashboard') {
    await loadPipelineDashboard();
    return;
  }
  if (state.pipelineView === 'long-tail') {
    // U3: the long-tail worklist owns its own fetch lifecycle. It paints
    // a loading affordance, fetches the banded cohort, then routes back
    // through renderPipeline (which re-emits the Pipeline nav).
    await loadLongTail();
    return;
  }
  if (state.pipelineView === 'search-plan-detail') {
    // U4: detail subview owns its own render lifecycle; openSearchPlanDetail
    // already kicked off the fetch when the subview was entered. Don't
    // clobber it with a queue-list paint.
    const ctx = state.searchPlanDetailContext;
    if (ctx && ctx.requestId) {
      await renderSearchPlanDetail(ctx.requestId);
    }
    return;
  }
  const el = document.getElementById('pipeline-content');
  el.innerHTML = `${renderPipelineNav()}<div class="loading">Loading...</div>`;
  try {
    // U10: opt-in toggle persists in localStorage. Default: filtered.
    const includeReplaced = localStorage.getItem('pipeline.includeReplaced') === 'true';
    clearPipelineSearch();
    const url = `${API}/api/pipeline/all${includeReplaced ? '?include_replaced=true' : ''}`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    state.pipelineData = await r.json();
    renderPipeline();
  } catch (e) { el.innerHTML = `${renderPipelineNav()}<div class="loading">Failed to load pipeline</div>`; }
}

/**
 * Toggle "show replaced" filter (U10). Pipeline + Wrong Matches tabs
 * persist this preference independently in localStorage.
 */
export function togglePipelineReplacedFilter() {
  const current = localStorage.getItem('pipeline.includeReplaced') === 'true';
  localStorage.setItem('pipeline.includeReplaced', String(!current));
  loadPipeline();
}

/**
 * Switch between the Pipeline subviews — queue, dashboard, or
 * search-plan-detail. The third value is the per-request inspector,
 * dispatched into `#pipeline-content` via `renderSearchPlanDetail`.
 * Unknown values fall back to `'queue'`.
 *
 * @param {string} view
 * @returns {void}
 */
export function setPipelineView(view) {
  if (view === 'dashboard') {
    state.pipelineView = 'dashboard';
    loadPipelineDashboard();
    return;
  }
  if (view === 'long-tail') {
    state.pipelineView = 'long-tail';
    // Re-render from cache if we already have a cohort (cheap band/search
    // re-paint); otherwise kick the initial fetch.
    if (state.longTail.rows) {
      renderPipeline();
    } else {
      loadLongTail();
    }
    return;
  }
  if (view === 'search-plan-detail') {
    state.pipelineView = 'search-plan-detail';
    const ctx = state.searchPlanDetailContext;
    if (ctx && ctx.requestId) {
      void renderSearchPlanDetail(ctx.requestId);
    }
    return;
  }
  state.pipelineView = 'queue';
  if (state.pipelineData) {
    renderPipeline();
  } else {
    loadPipeline();
  }
}

export function toggleCoverageMatchGraph(scope = 'hourly') {
  if (scope === 'daily') {
    state.pipelineDailyMatchGraphOpen = !state.pipelineDailyMatchGraphOpen;
  } else {
    state.pipelineHourlyMatchGraphOpen = !state.pipelineHourlyMatchGraphOpen;
    state.pipelineMatchGraphOpen = state.pipelineHourlyMatchGraphOpen;
  }
  renderDashboardCards(renderPipelineNav());
}

/**
 * Load dashboard metrics from the API and render them.
 * @returns {Promise<void>}
 */
export async function loadPipelineDashboard() {
  const el = document.getElementById('pipeline-content');
  el.innerHTML = `${renderPipelineNav()}<div class="loading">Loading...</div>`;
  try {
    const r = await fetch(`${API}/api/pipeline/dashboard`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    state.pipelineDashboardData = await r.json();
    renderDashboardCards(renderPipelineNav());
  } catch (e) {
    el.innerHTML = `${renderPipelineNav()}<div class="loading">Failed to load dashboard</div>`;
  }
}

/**
 * Set the pipeline filter and re-render.
 * @param {string} f
 */
export function setFilter(f) {
  state.pipelineFilter = f;
  // Filter and search are mutually exclusive: clicking a count card
  // abandons the active search so the clicked filter actually shows.
  clearPipelineSearch();
  renderPipeline();
}

/**
 * Reset the server-side search state (input text + results). Bumps the
 * in-flight token so a fetch resolving late discards itself.
 * @returns {void}
 */
export function clearPipelineSearch() {
  state.pipelineSearchQuery = '';
  state.pipelineSearchResults = null;
  pipelineSearchToken += 1;
  if (pipelineSearchTimer != null) clearTimeout(pipelineSearchTimer);
}

/**
 * Render the pipeline view from cached data.
 *
 * Dispatches on `state.pipelineView`:
 *   * `'queue'` (default)  → list of pipeline rows
 *   * `'dashboard'`        → metrics dashboard
 *   * `'long-tail'`        → banded long-tail triage worklist
 *   * `'search-plan-detail'` → per-request inspector (U4)
 */
export function renderPipeline() {
  const el = document.getElementById('pipeline-content');
  if (state.pipelineView === 'dashboard') {
    renderDashboardCards(renderPipelineNav());
    return;
  }
  if (state.pipelineView === 'long-tail') {
    el.innerHTML = `${renderPipelineNav()}${renderLongTailBody()}`;
    // The full-body wipe destroyed any expanded console DOM — restore the
    // operator's open consoles (#398 / KTD8 fidelity: the post-action
    // single-row patch and band switches must not collapse them).
    restoreLongTailConsoles();
    return;
  }
  if (state.pipelineView === 'search-plan-detail') {
    const ctx = state.searchPlanDetailContext;
    if (ctx && ctx.requestId) {
      void renderSearchPlanDetail(ctx.requestId);
    } else if (el) {
      // Defensive: subview entered without a context. Fall back to
      // queue so the operator is never stranded.
      state.pipelineView = 'queue';
    }
    return;
  }
  const data = state.pipelineData;
  if (!data) return;
  const counts = data.counts || {};
  const total = Object.values(counts).reduce((a, b) => a + b, 0);

  // Wanted bucket includes downloading — mid-acquisition is a sub-state
  // of wanted. The status badge on each row keeps them visually distinct.
  const wantedTotal = (counts.wanted || 0) + (counts.downloading || 0);
  el.innerHTML = `
    ${renderPipelineNav()}
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
    <div class="lt-search">
      <input type="text" id="pipeline-search-input" class="lt-search-input"
        placeholder="Search every request by artist or album…"
        value="${esc(state.pipelineSearchQuery || '')}"
        oninput="window.onPipelineSearchInput(this.value)">
    </div>
    <div id="pipeline-list">${renderPipelineListBody()}</div>
  `;
}

/**
 * Build the artist-grouped list body for the current filter (or the
 * active server-side search results). Kept separate from
 * renderPipeline so a search keystroke can repaint only #pipeline-list
 * and the input keeps focus/caret.
 *
 * @returns {string}
 */
function renderPipelineListBody() {
  const data = state.pipelineData || {};
  const searching = state.pipelineSearchResults != null;

  let items = [];
  if (searching) {
    items = state.pipelineSearchResults || [];
  } else if (state.pipelineFilter === 'all') {
    items = [...(data.wanted || []), ...(data.downloading || []), ...(data.imported || []), ...(data.manual || [])];
  } else if (state.pipelineFilter === 'wanted') {
    // Downloading is a sub-state of wanted — same album, mid-acquisition.
    // The status badge on each row still distinguishes them visually.
    items = [...(data.wanted || []), ...(data.downloading || [])];
  } else {
    items = data[state.pipelineFilter] || [];
  }

  // The imported bucket is a recency window (#426); say so whenever the
  // truncated bucket is part of the current view.
  const showsImported = !searching
    && (state.pipelineFilter === 'imported' || state.pipelineFilter === 'all');
  const truncationNote = showsImported && data.imported_truncated
    ? `<div class="loading">Showing the ${(data.imported || []).length} most recent of ${data.imported_total} imported — search above to find the rest.</div>`
    : '';

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

  return `
    ${truncationNote}
    ${artists.map(artist => `
      <div class="p-group-header" onclick="this.nextElementSibling.classList.toggle('collapsed')">
        ${esc(artist)} <span style="color:#555;font-weight:400;">${byArtist[artist].length}</span>
      </div>
      <div class="p-group-body">
        ${byArtist[artist].map(item => renderPipelineItem(item)).join('')}
      </div>
    `).join('')}
    ${artists.length === 0 ? `<div class="loading">${searching ? 'No matches' : 'No items'}</div>` : ''}
  `;
}

// Module-scoped debounce + stale-response token for the server-side
// pipeline search (#426). web.md: fetch-on-input UIs must stamp
// requests and discard stale responses before rendering.
let pipelineSearchTimer = null;
let pipelineSearchToken = 0;
const PIPELINE_SEARCH_DEBOUNCE_MS = 250;

/**
 * Handle a keystroke in the pipeline search box: debounce, fetch
 * server-side results across every status, repaint only the list body.
 * @param {string} value
 * @returns {void}
 */
export function onPipelineSearchInput(value) {
  const q = String(value == null ? '' : value);
  state.pipelineSearchQuery = q;
  if (pipelineSearchTimer != null) clearTimeout(pipelineSearchTimer);
  const repaint = () => {
    const listEl = document.getElementById('pipeline-list');
    if (listEl) listEl.innerHTML = renderPipelineListBody();
  };
  if (q.trim().length < 2) {
    state.pipelineSearchResults = null;
    repaint();
    return;
  }
  pipelineSearchTimer = /** @type {any} */ (setTimeout(async () => {
    const token = ++pipelineSearchToken;
    try {
      const r = await fetch(`${API}/api/pipeline/search?q=${encodeURIComponent(q.trim())}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      if (token !== pipelineSearchToken) return;
      state.pipelineSearchResults = data.items || [];
      repaint();
    } catch (e) {
      if (token !== pipelineSearchToken) return;
      state.pipelineSearchResults = [];
      repaint();
    }
  }, PIPELINE_SEARCH_DEBOUNCE_MS));
}

function renderPipelineNav() {
  const refreshAction = state.pipelineView === 'dashboard'
    ? 'window.loadPipelineDashboard()'
    : state.pipelineView === 'long-tail'
      ? 'window.loadLongTail()'
      : 'window.loadPipeline()';

  return `
    <div class="pipeline-subtabs">
      <button class="p-btn ${state.pipelineView === 'queue' ? 'active-status' : ''}" onclick="window.setPipelineView('queue')">Queue</button>
      <button class="p-btn ${state.pipelineView === 'dashboard' ? 'active-status' : ''}" onclick="window.setPipelineView('dashboard')">Dashboard</button>
      <button class="p-btn ${state.pipelineView === 'long-tail' ? 'active-status' : ''}" onclick="window.setPipelineView('long-tail')">Long Tail</button>
      <button class="p-btn subtab-refresh" onclick="${refreshAction}">Refresh</button>
    </div>
  `;
}

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

  // Search-plan inspector button — Pipeline rows always have a request
  // id by construction, so the conditional in renderSearchPlanButton is
  // a no-op here, but routing through the same helper keeps the toolbar
  // wiring consistent across Browse / Pipeline / Recents.
  const spBtn = renderSearchPlanButton({ pipelineId: item.id });

  return `
    <div class="p-item ${srcClass}" onclick="window.toggleDetail(${item.id})">
      <div class="p-top">
        <div>
          <div class="p-title">${esc(item.album_title)}${statusBadge}</div>
        </div>
        <div class="p-row-actions">${spBtn}<span style="font-size:0.75em;color:#666;">#${item.id}</span></div>
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
    // Manual-reason chip (e.g. search_exhausted) — surfaces the reason a
    // request landed in `manual` so the operator does not have to query
    // JSONB. Hidden when null (manually-set or pre-U7 rows).
    const reasonLabel = manualReasonLabel(data.manual_reason);
    if (reasonLabel) {
      html += `<div class="p-detail-row"><span class="p-detail-label">Manual reason</span><span class="p-detail-value"><span class="p-manual-chip">${esc(reasonLabel)}</span></span></div>`;
    }
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

    // Search forensics (last_search) — variant tag + top-3 candidates from
    // the most recent search_log row. Collapsed by default; click expands.
    html += renderForensicBlock(/** @type {any} */ (data.last_search));

    // Status change buttons
    html += `<div class="p-actions">
      <span class="p-detail-label" style="line-height:28px;">Status:</span>
      <button class="p-btn ${req.status === 'wanted' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.updateStatus(${id}, 'wanted')">wanted</button>
      <button class="p-btn ${req.status === 'imported' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.updateStatus(${id}, 'imported')">imported</button>
      <button class="p-btn ${req.status === 'manual' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.updateStatus(${id}, 'manual')">manual</button>`;
    // Bad-rip reuses the library renderer — pipelineId + releaseId are all
    // it needs from state. Hidden when either is absent (issue #188).
    html += renderBadRipButton(/** @type {any} */ ({pipelineId: id, releaseId: req.mb_release_id}), {
      className: 'p-btn delete',
      stopPropagation: true,
    });
    // Replace button — only shown when the row is not itself a frozen
    // audit row (R30 / scope boundary "re-replacing a replaced row is
    // not supported"). ``mb_release_group_id`` may be null on legacy
    // rows; the picker lazy-resolves via
    // ``POST /api/pipeline/<id>/resolve-rg`` before fetching siblings.
    if (req.status !== 'replaced') {
      html += renderReplaceButton({
        mode: 'standard',
        sourceRequestId: id,
        releaseGroupId: req.mb_release_group_id || null,
        sourceLabel: `${req.artist_name || ''} — ${req.album_title || ''}`,
      }, { className: 'p-btn', stopPropagation: true });
    }
    html += `<button class="p-btn delete" onclick="event.stopPropagation(); window.deleteRequest(${id})">delete</button>
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

export const __test__ = {
  renderPipelineListBody,
  clearPipelineSearch,
  renderPipelineNav,
};
