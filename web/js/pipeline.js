// @ts-check
import { state, API, toast } from './state.js';
import { esc, awstDate, awstDateTime, qualityLabel, externalReleaseUrl, sourceLabel, manualReasonLabel, renderForensicBlock } from './util.js';
import { renderDownloadHistoryItem } from './history.js';
import { renderBadRipButton } from './release_actions.js';

/**
 * Load pipeline data from API and render.
 * @returns {Promise<void>}
 */
export async function loadPipeline() {
  if (state.pipelineView === 'dashboard') {
    await loadPipelineDashboard();
    return;
  }
  const el = document.getElementById('pipeline-content');
  el.innerHTML = `${renderPipelineNav()}<div class="loading">Loading...</div>`;
  try {
    const r = await fetch(`${API}/api/pipeline/all`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    state.pipelineData = await r.json();
    renderPipeline();
  } catch (e) { el.innerHTML = `${renderPipelineNav()}<div class="loading">Failed to load pipeline</div>`; }
}

/**
 * Switch between the queue and dashboard Pipeline subtabs.
 * @param {string} view
 * @returns {void}
 */
export function setPipelineView(view) {
  state.pipelineView = view === 'dashboard' ? 'dashboard' : 'queue';
  if (state.pipelineView === 'dashboard') {
    loadPipelineDashboard();
    return;
  }
  if (state.pipelineData) {
    renderPipeline();
  } else {
    loadPipeline();
  }
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
    renderPipelineDashboard();
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
  renderPipeline();
}

/**
 * Render the pipeline view from cached data.
 */
export function renderPipeline() {
  const el = document.getElementById('pipeline-content');
  if (state.pipelineView === 'dashboard') {
    renderPipelineDashboard();
    return;
  }
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

function renderPipelineNav() {
  return `
    <div class="pipeline-subtabs">
      <button class="p-btn ${state.pipelineView === 'queue' ? 'active-status' : ''}" onclick="window.setPipelineView('queue')">Queue</button>
      <button class="p-btn ${state.pipelineView === 'dashboard' ? 'active-status' : ''}" onclick="window.setPipelineView('dashboard')">Dashboard</button>
      ${state.pipelineView === 'dashboard' ? '<button class="p-btn dashboard-refresh" onclick="window.loadPipelineDashboard()">Refresh</button>' : ''}
    </div>
  `;
}

function renderPipelineDashboard() {
  const el = document.getElementById('pipeline-content');
  const data = state.pipelineDashboardData;
  if (!data) {
    el.innerHTML = `${renderPipelineNav()}<div class="loading">Loading...</div>`;
    return;
  }
  const searches = /** @type {any[]} */ (data.searches?.windows || []);
  const cycles = /** @type {any[]} */ (data.cycles?.windows || []);
  const coverage = /** @type {any} */ (data.coverage || {});
  const redis = /** @type {any} */ (data.redis || {});
  const peerDirs = /** @type {any} */ (data.peer_dirs || {});
  const generated = data.generated_at ? awstDateTime(data.generated_at) : '';
  el.innerHTML = `
    ${renderPipelineNav()}
    <div class="dashboard-header">
      <div class="dashboard-title">Pipeline Dashboard</div>
      <div class="dashboard-updated">${generated}</div>
    </div>
    <div class="dashboard-grid">
      ${renderRedisCard(redis)}
      ${renderCoverageCard(coverage)}
      ${renderPeerDirCard(peerDirs)}
      ${renderSearchCard(searches)}
      ${renderCycleCard(cycles)}
      ${renderCycleOutliers(data.cycles?.outliers || [])}
      ${renderLoopSuspects(coverage.top_loop_suspects || [])}
      ${renderStaleWanted(coverage.stale_wanted || [])}
    </div>
  `;
}

function renderPeerDirCard(peerDirs) {
  const totals = peerDirs.totals || {};
  const days = /** @type {any[]} */ (peerDirs.days || []);
  return `
    <div class="dashboard-card dashboard-wide">
      <div class="dashboard-card-title">Peer/Dir First Seen</div>
      <div class="dashboard-metric-strip">
        <div class="dashboard-metric"><span>Known combos</span><strong>${formatCount(totals.known_combos)}</strong></div>
        <div class="dashboard-metric"><span>New 24h</span><strong>${formatCount(totals.new_24h)}</strong></div>
        <div class="dashboard-metric"><span>Known peers</span><strong>${formatCount(totals.known_peers)}</strong></div>
        <div class="dashboard-metric"><span>Tracked since</span><strong>${totals.tracked_since ? awstDate(totals.tracked_since) : 'n/a'}</strong></div>
      </div>
      <table class="dashboard-table">
        <thead><tr><th>Day</th><th>Combos</th><th>Peers</th><th>Dirs</th></tr></thead>
        <tbody>
          ${days.map(d => `
            <tr>
              <td>${esc(d.date || '')}</td>
              <td class="${d.new_combos ? 'metric-good' : ''}">${formatCount(d.new_combos)}</td>
              <td>${formatCount(d.new_peers)}</td>
              <td>${formatCount(d.new_dirs)}</td>
            </tr>
          `).join('')}
          ${days.length === 0 ? '<tr><td colspan="4">No peer/dir observations yet</td></tr>' : ''}
        </tbody>
      </table>
    </div>
  `;
}

function renderRedisCard(redis) {
  const statusClass = redis.status === 'ok' ? 'metric-good'
    : redis.status === 'disabled' ? 'metric-muted' : 'metric-bad';
  const max = redis.maxmemory_bytes ? formatBytes(redis.maxmemory_bytes) : 'unlimited';
  const used = redis.used_memory_bytes ? formatBytes(redis.used_memory_bytes) : 'n/a';
  const dataset = redis.used_memory_dataset_bytes ? formatBytes(redis.used_memory_dataset_bytes) : 'n/a';
  return `
    <div class="dashboard-card">
      <div class="dashboard-card-title">Redis</div>
      <div class="metric-list">
        <div class="metric-row"><span>Status</span><strong class="${statusClass}">${esc(redis.status || 'unknown')}</strong></div>
        <div class="metric-row"><span>Memory</span><strong>${used} / ${max}</strong></div>
        <div class="metric-row"><span>Utilization</span><strong>${formatPercent(redis.memory_utilization)}</strong></div>
        <div class="metric-row"><span>Dataset</span><strong>${dataset}</strong></div>
        <div class="metric-row"><span>Keys</span><strong>${formatCount(redis.key_count)}</strong></div>
        <div class="metric-row"><span>Expires</span><strong>${formatCount(redis.expires_count)}</strong></div>
        <div class="metric-row"><span>Avg TTL</span><strong>${formatHoursFromMs(redis.avg_ttl_ms)}</strong></div>
        <div class="metric-row"><span>Frag</span><strong>${formatDecimal(redis.fragmentation_ratio)}</strong></div>
      </div>
    </div>
  `;
}

function renderCoverageCard(coverage) {
  const wanted = coverage.wanted_total || 0;
  const searched24 = coverage.wanted_searched_24h || 0;
  const searched6 = coverage.wanted_searched_6h || 0;
  const stale24 = coverage.wanted_unsearched_24h || 0;
  const never = coverage.wanted_never_searched || 0;
  const searchedPct = wanted ? searched24 / wanted : 1;
  const coverageClass = stale24 === 0 ? 'metric-good' : never > 0 ? 'metric-bad' : 'metric-warn';
  return `
    <div class="dashboard-card">
      <div class="dashboard-card-title">Wanted Coverage</div>
      <div class="coverage-bar"><span style="width:${Math.max(0, Math.min(100, searchedPct * 100)).toFixed(1)}%;"></span></div>
      <div class="metric-list">
        <div class="metric-row"><span>Wanted</span><strong>${formatCount(wanted)}</strong></div>
        <div class="metric-row"><span>Searched 24h</span><strong class="${coverageClass}">${formatCount(searched24)}</strong></div>
        <div class="metric-row"><span>Searched 6h</span><strong>${formatCount(searched6)}</strong></div>
        <div class="metric-row"><span>Stale 24h</span><strong class="${stale24 ? 'metric-warn' : 'metric-good'}">${formatCount(stale24)}</strong></div>
        <div class="metric-row"><span>Never</span><strong class="${never ? 'metric-bad' : 'metric-good'}">${formatCount(never)}</strong></div>
        <div class="metric-row"><span>Top 10 share</span><strong>${formatPercent(coverage.top_10_share_24h)}</strong></div>
      </div>
    </div>
  `;
}

function renderSearchCard(windows) {
  return `
    <div class="dashboard-card dashboard-wide">
      <div class="dashboard-card-title">Search Throughput</div>
      <table class="dashboard-table">
        <thead><tr><th>Window</th><th>Searches</th><th>Requests</th><th>/hr</th><th>Median</th><th>P95</th><th>Found</th><th>No match</th><th>Empty</th><th>Resets</th><th>Errors</th></tr></thead>
        <tbody>
          ${windows.map(w => `
            <tr>
              <td>${esc(w.label)}</td>
              <td>${formatCount(w.searches)}</td>
              <td>${formatCount(w.distinct_requests)}</td>
              <td>${formatDecimal(w.searches_per_hour)}</td>
              <td>${formatDuration(w.median_elapsed_s)}</td>
              <td>${formatDuration(w.p95_elapsed_s)}</td>
              <td>${formatCount(w.outcomes?.found)}</td>
              <td>${formatCount(w.outcomes?.no_match)}</td>
              <td>${formatCount(w.outcomes?.no_results)}</td>
              <td>${formatCount(w.outcomes?.exhausted)}</td>
              <td class="${w.outcomes?.errors ? 'metric-warn' : ''}">${formatCount(w.outcomes?.errors)}</td>
            </tr>
          `).join('')}
          ${windows.length === 0 ? '<tr><td colspan="11">No search metrics</td></tr>' : ''}
        </tbody>
      </table>
    </div>
  `;
}

function renderCycleCard(windows) {
  return `
    <div class="dashboard-card dashboard-wide">
      <div class="dashboard-card-title">Cycle Times</div>
      <table class="dashboard-table">
        <thead><tr><th>Window</th><th>Cycles</th><th>Median</th><th>P95</th><th>Max</th><th>Search median</th><th>Watchdog</th><th>Queued</th><th>Done</th><th>Cache errs</th></tr></thead>
        <tbody>
          ${windows.map(w => `
            <tr>
              <td>${esc(w.label)}</td>
              <td>${formatCount(w.cycles)}</td>
              <td>${formatDuration(w.median_cycle_s)}</td>
              <td>${formatDuration(w.p95_cycle_s)}</td>
              <td>${formatDuration(w.max_cycle_s)}</td>
              <td>${formatDuration(w.median_search_s)}</td>
              <td class="${w.watchdog_kills ? 'metric-warn' : ''}">${formatCount(w.watchdog_kills)}</td>
              <td>${formatCount(w.find_download_queued)}</td>
              <td>${formatCount(w.find_download_completed)}</td>
              <td class="${w.cache_errors || w.cache_write_errors || w.cache_fuse_tripped ? 'metric-bad' : ''}">${formatCount((w.cache_errors || 0) + (w.cache_write_errors || 0) + (w.cache_fuse_tripped || 0))}</td>
            </tr>
          `).join('')}
          ${windows.length === 0 ? '<tr><td colspan="10">No cycle metrics</td></tr>' : ''}
        </tbody>
      </table>
    </div>
  `;
}

function renderCycleOutliers(rows) {
  return `
    <div class="dashboard-card dashboard-wide">
      <div class="dashboard-card-title">Cycle Outliers</div>
      <table class="dashboard-table">
        <thead><tr><th>Completed</th><th>Total</th><th>Search</th><th>Browse</th><th>Match</th><th>Watchdog</th><th>Peers</th><th>Waves</th></tr></thead>
        <tbody>
          ${rows.map(r => `
            <tr>
              <td>${awstDateTime(r.created_at || '')}</td>
              <td>${formatDuration(r.cycle_total_s)}</td>
              <td>${formatDuration(r.search_time_s)}</td>
              <td>${formatDuration(r.browse_time_s)}</td>
              <td>${formatDuration(r.match_time_s)}</td>
              <td class="${r.watchdog_kills ? 'metric-warn' : ''}">${formatCount(r.watchdog_kills)}</td>
              <td>${formatCount((r.peers_browsed || 0) + (r.peers_browsed_lazy || 0))}</td>
              <td>${formatCount(r.fanout_waves)}</td>
            </tr>
          `).join('')}
          ${rows.length === 0 ? '<tr><td colspan="8">No cycle rows yet</td></tr>' : ''}
        </tbody>
      </table>
    </div>
  `;
}

function renderLoopSuspects(rows) {
  return `
    <div class="dashboard-card dashboard-wide">
      <div class="dashboard-card-title">Loop Suspects</div>
      <table class="dashboard-table">
        <thead><tr><th>ID</th><th>Artist</th><th>Album</th><th>24h</th><th>6h</th><th>Found</th><th>No match</th><th>No results</th><th>Resets</th><th>Errors</th><th>Last</th></tr></thead>
        <tbody>
          ${rows.map(r => `
            <tr>
              <td>#${r.request_id}</td>
              <td>${esc(r.artist_name || '')}</td>
              <td>${esc(r.album_title || '')}</td>
              <td class="${r.searches_24h > 3 ? 'metric-warn' : ''}">${formatCount(r.searches_24h)}</td>
              <td>${formatCount(r.searches_6h)}</td>
              <td>${formatCount(r.found_24h)}</td>
              <td>${formatCount(r.no_match_24h)}</td>
              <td>${formatCount(r.no_results_24h)}</td>
              <td>${formatCount(r.reset_24h)}</td>
              <td class="${r.problem_24h ? 'metric-warn' : ''}">${formatCount(r.problem_24h)}</td>
              <td>${r.last_search_at ? awstDateTime(r.last_search_at) : 'never'}</td>
            </tr>
          `).join('')}
          ${rows.length === 0 ? '<tr><td colspan="11">No repeated wanted searches in 24h</td></tr>' : ''}
        </tbody>
      </table>
    </div>
  `;
}

function renderStaleWanted(rows) {
  return `
    <div class="dashboard-card dashboard-wide">
      <div class="dashboard-card-title">Stale Wanted</div>
      <table class="dashboard-table">
        <thead><tr><th>ID</th><th>Artist</th><th>Album</th><th>Last search</th><th>Age</th><th>24h</th><th>6h</th></tr></thead>
        <tbody>
          ${rows.map(r => `
            <tr>
              <td>#${r.request_id}</td>
              <td>${esc(r.artist_name || '')}</td>
              <td>${esc(r.album_title || '')}</td>
              <td>${r.last_search_at ? awstDateTime(r.last_search_at) : 'never'}</td>
              <td>${r.hours_since_search == null ? 'n/a' : `${formatDecimal(r.hours_since_search)}h`}</td>
              <td>${formatCount(r.searches_24h)}</td>
              <td>${formatCount(r.searches_6h)}</td>
            </tr>
          `).join('')}
          ${rows.length === 0 ? '<tr><td colspan="7">No wanted rows</td></tr>' : ''}
        </tbody>
      </table>
    </div>
  `;
}

function formatCount(value) {
  if (value == null || Number.isNaN(Number(value))) return '0';
  return Number(value).toLocaleString();
}

function formatDecimal(value) {
  if (value == null || Number.isNaN(Number(value))) return 'n/a';
  const n = Number(value);
  return n >= 10 ? n.toFixed(1) : n.toFixed(2);
}

function formatDuration(value) {
  if (value == null || Number.isNaN(Number(value))) return 'n/a';
  const seconds = Number(value);
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = Math.floor(seconds / 60);
  const rest = Math.round(seconds % 60);
  return `${minutes}m ${String(rest).padStart(2, '0')}s`;
}

function formatBytes(value) {
  if (value == null || Number.isNaN(Number(value))) return 'n/a';
  const bytes = Number(value);
  const units = ['B', 'KB', 'MB', 'GB'];
  let n = bytes;
  let unit = units[0];
  for (let i = 1; i < units.length && n >= 1024; i += 1) {
    n /= 1024;
    unit = units[i];
  }
  return `${n >= 10 ? n.toFixed(1) : n.toFixed(2)} ${unit}`;
}

function formatHoursFromMs(value) {
  if (value == null || Number.isNaN(Number(value))) return 'n/a';
  return `${(Number(value) / 3600000).toFixed(1)}h`;
}

function formatPercent(value) {
  if (value == null || Number.isNaN(Number(value))) return 'n/a';
  return `${(Number(value) * 100).toFixed(1)}%`;
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
