// @ts-check
import { state, API, toast } from './state.js';
import { esc, awstDate, awstDateTime, awstTime, qualityLabel, externalReleaseUrl, sourceLabel, manualReasonLabel, renderForensicBlock } from './util.js';
import { renderDownloadHistoryItem } from './history.js';
import { renderBadRipButton, renderReplaceButton } from './release_actions.js';
import { renderSearchPlanButton, renderSearchPlanDetail } from './search_plan.js';
import { loadLongTail, renderLongTailBody } from './long_tail.js';

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
  renderPipelineDashboard();
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
    renderPipelineDashboard();
    return;
  }
  if (state.pipelineView === 'long-tail') {
    el.innerHTML = `${renderPipelineNav()}${renderLongTailBody()}`;
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
  const coverageWithRates = withCoverageMatchRates(coverage, searches);
  const redis = /** @type {any} */ (data.redis || {});
  const peers = /** @type {any} */ (data.peers || {});
  const generated = data.generated_at ? awstDateTime(data.generated_at) : '';
  el.innerHTML = `
    ${renderPipelineNav()}
    <div class="dashboard-header">
      <div class="dashboard-title">Pipeline Dashboard</div>
      <div class="dashboard-updated">${generated}</div>
    </div>
    <div class="dashboard-grid">
      ${renderRedisCard(redis)}
      ${renderCoverageCard(coverageWithRates)}
      ${renderWantedTrendCard(coverageWithRates.wanted_trend || {})}
      ${renderPeersCard(peers)}
      ${renderSearchCard(searches)}
      ${renderCycleCard(cycles)}
      ${renderCycleOutliers(data.cycles?.outliers || [])}
      ${renderPeerBrowseHeavyQueries(peers)}
      ${renderLoopSuspects(coverage.top_loop_suspects || [])}
      ${renderStaleWanted(coverage.stale_wanted || [])}
    </div>
  `;
}

function renderWantedTrendCard(trend) {
  const current = trend.current_wanted == null ? null : Number(trend.current_wanted);
  const windows = Array.isArray(trend.windows) ? trend.windows : [];
  const etaWindow = windows.find(w => Number(w?.drain_per_hour) > 0 && w?.label === '24h')
    || windows.find(w => Number(w?.drain_per_hour) > 0);
  return `
    <div class="dashboard-card">
      <div class="dashboard-card-title">Wanted Trend</div>
      ${renderWantedTrendChart(trend.series_24h || [])}
      <div class="metric-list">
        <div class="metric-row"><span>Current</span><strong>${current == null ? 'n/a' : formatCount(current)}</strong></div>
        ${windows.map(w => `
          <div class="metric-row">
            <span>${esc(w.label || '')}</span>
            <strong class="${wantedTrendClass(w)}">${formatWantedTrendWindow(w)}</strong>
          </div>
        `).join('')}
        <div class="metric-row">
          <span>ETA</span>
          <strong>${etaWindow ? formatEtaHours(etaWindow.eta_hours) : 'n/a'}</strong>
        </div>
      </div>
    </div>
  `;
}

function renderWantedTrendChart(points) {
  const series = normalizeWantedTrendSeries(points);
  if (series.length < 2) {
    return `<div class="wanted-trend-chart"><div class="chart-empty">Collecting wanted snapshots</div></div>`;
  }

  const width = 240;
  const height = 64;
  const minWanted = Math.min(...series.map(p => p.wanted));
  const maxWanted = Math.max(...series.map(p => p.wanted));
  const range = Math.max(1, maxWanted - minWanted);
  const coords = series.map((point, index) => {
    const x = series.length === 1 ? width : (index / (series.length - 1)) * width;
    const y = height - ((point.wanted - minWanted) / range) * height;
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  }).join(' ');
  const area = `0,${height} ${coords} ${width},${height}`;
  const first = series[0]?.time ? awstTime(series[0].time) : '';
  const last = series[series.length - 1]?.time ? awstTime(series[series.length - 1].time) : '';
  const latest = series[series.length - 1]?.wanted;
  return `
    <div class="wanted-trend-chart">
      <div class="match-rate-chart-head"><span>Last 24 hours</span><strong>${formatCount(latest)}</strong></div>
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="Wanted backlog trend">
        <polygon class="wanted-trend-area" points="${area}"></polygon>
        <polyline class="wanted-trend-line" points="${coords}"></polyline>
      </svg>
      <div class="match-rate-chart-axis"><span>${first}</span><span>${last}</span></div>
    </div>
  `;
}

function normalizeWantedTrendSeries(points) {
  return (Array.isArray(points) ? points : []).map(point => {
    const row = point || {};
    const wanted = Number(row.wanted_total);
    return {
      time: row.sampled_at || '',
      wanted: Number.isFinite(wanted) ? wanted : 0,
    };
  }).filter(point => point.time || Number.isFinite(point.wanted));
}

function wantedTrendClass(w) {
  if (!w || w.delta == null) return 'metric-muted';
  if (w.trend === 'down') return 'metric-good';
  if (w.trend === 'up') return 'metric-warn';
  return 'metric-muted';
}

function formatWantedTrendWindow(w) {
  if (!w || w.delta == null || w.delta_per_hour == null) return 'collecting';
  const delta = Number(w.delta);
  if (delta === 0) return 'flat';
  const direction = delta < 0 ? 'down' : 'up';
  return `${direction} ${formatDecimal(Math.abs(w.delta_per_hour))}/hr (${formatSignedCount(delta)})`;
}

function renderPeersCard(peers) {
  const totals = peers.totals || {};
  const days = /** @type {any[]} */ (peers.days || []);
  return `
    <div class="dashboard-card dashboard-wide">
      <div class="dashboard-card-title">Known Peers</div>
      <div class="dashboard-metric-strip">
        <div class="dashboard-metric"><span>Known peers</span><strong>${formatCount(totals.known_peers)}</strong></div>
        <div class="dashboard-metric"><span>New 24h</span><strong>${formatCount(totals.new_24h)}</strong></div>
        <div class="dashboard-metric"><span>Seen 24h</span><strong>${formatCount(totals.seen_24h)}</strong></div>
        <div class="dashboard-metric"><span>Tracked since</span><strong>${totals.tracked_since ? awstDate(totals.tracked_since) : 'n/a'}</strong></div>
      </div>
      <table class="dashboard-table">
        <thead><tr><th>Day</th><th>New peers</th><th>Total known</th></tr></thead>
        <tbody>
          ${days.map(d => `
            <tr>
              <td>${esc(d.date || '')}</td>
              <td class="${d.new_peers ? 'metric-good' : ''}">${formatCount(d.new_peers)}</td>
              <td>${formatCount(d.total_peers)}</td>
            </tr>
          `).join('')}
          ${days.length === 0 ? '<tr><td colspan="3">No peer observations yet</td></tr>' : ''}
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
  const hourlyGraphOpen = Boolean(state.pipelineHourlyMatchGraphOpen || state.pipelineMatchGraphOpen);
  const dailyGraphOpen = Boolean(state.pipelineDailyMatchGraphOpen);
  return `
    <div class="dashboard-card">
      <div class="dashboard-card-title">Wanted Coverage</div>
      <div class="coverage-bar"><span style="width:${Math.max(0, Math.min(100, searchedPct * 100)).toFixed(1)}%;"></span></div>
      <div class="metric-list">
        <div class="metric-row"><span>Wanted</span><strong>${formatCount(wanted)}</strong></div>
        <div class="metric-row"><span>Searched 24h</span><strong class="${coverageClass}">${formatCount(searched24)}</strong></div>
        <div class="metric-row"><span>Searched 6h</span><strong>${formatCount(searched6)}</strong></div>
        <div class="metric-row metric-clickable ${hourlyGraphOpen ? 'metric-open' : ''}" onclick="window.toggleCoverageMatchGraph('hourly')" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();window.toggleCoverageMatchGraph('hourly');}" role="button" tabindex="0"><span>Match/hr 6h</span><strong class="${coverage.matches_6h ? 'metric-good' : ''}">${formatMatchRate(coverage.matches_per_hour_6h)}</strong></div>
        ${hourlyGraphOpen ? renderHourlyMatchRateChart(coverage.match_rate_series_24h || []) : ''}
        <div class="metric-row metric-clickable ${dailyGraphOpen ? 'metric-open' : ''}" onclick="window.toggleCoverageMatchGraph('daily')" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();window.toggleCoverageMatchGraph('daily');}" role="button" tabindex="0"><span>Match/hr 24h</span><strong class="${coverage.matches_24h ? 'metric-good' : ''}">${formatMatchRate(coverage.matches_per_hour_24h)}</strong></div>
        ${dailyGraphOpen ? renderDailyMatchRateChart(coverage.match_rate_series_28d || []) : ''}
        <div class="metric-row"><span>Stale 24h</span><strong class="${stale24 ? 'metric-warn' : 'metric-good'}">${formatCount(stale24)}</strong></div>
        <div class="metric-row"><span>Never</span><strong class="${never ? 'metric-bad' : 'metric-good'}">${formatCount(never)}</strong></div>
        <div class="metric-row"><span>Top 10 share</span><strong>${formatPercent(coverage.top_10_share_24h)}</strong></div>
      </div>
    </div>
  `;
}

function renderHourlyMatchRateChart(points) {
  return renderMatchRateChart(points, {
    periodLabel: 'Last 24 hours',
    unit: 'hr',
    rateKey: 'matches_per_hour',
    emptyLabel: 'No hourly match data yet',
    axis: 'time',
  });
}

function renderDailyMatchRateChart(points) {
  return renderMatchRateChart(points, {
    periodLabel: 'Last 28 days',
    unit: 'day',
    rateKey: 'matches_per_day',
    emptyLabel: 'No daily match data yet',
    axis: 'date',
  });
}

function renderMatchRateChart(points, options = {}) {
  const periodLabel = options.periodLabel || 'Last 24 hours';
  const unit = options.unit || 'hr';
  const rateKey = options.rateKey || 'matches_per_hour';
  const emptyLabel = options.emptyLabel || 'No hourly match data yet';
  const axis = options.axis || 'time';
  const series = normalizeMatchRateSeries(points, rateKey);
  if (series.length === 0) {
    return `<div class="match-rate-chart"><div class="chart-empty">${esc(emptyLabel)}</div></div>`;
  }

  const width = 240;
  const height = 64;
  const gap = 2;
  const maxRate = Math.max(1, ...series.map(p => p.rate));
  const barWidth = Math.max(2, (width - gap * (series.length - 1)) / series.length);
  const bars = series.map((point, index) => {
    const barHeight = Math.max(point.matches > 0 ? 2 : 0, (point.rate / maxRate) * height);
    const x = index * (barWidth + gap);
    const y = height - barHeight;
    const bucketLabel = point.time ? formatChartBucket(point.time, axis) : '';
    const label = bucketLabel ? `${bucketLabel} ${formatChartRate(point.rate, unit)}/${unit} (${formatCount(point.matches)})` : `${formatChartRate(point.rate, unit)}/${unit}`;
    return `<g><title>${esc(label)}</title><rect class="match-rate-bar ${point.matches ? 'active' : ''}" x="${x.toFixed(2)}" y="${y.toFixed(2)}" width="${barWidth.toFixed(2)}" height="${barHeight.toFixed(2)}"></rect></g>`;
  }).join('');
  const first = series[0]?.time ? formatChartBucket(series[0].time, axis) : '';
  const last = series[series.length - 1]?.time ? formatChartBucket(series[series.length - 1].time, axis) : '';
  return `
    <div class="match-rate-chart">
      <div class="match-rate-chart-head"><span>${esc(periodLabel)}</span><strong>peak ${formatChartRate(maxRate, unit)}/${esc(unit)}</strong></div>
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="${esc(periodLabel)} match rate">${bars}</svg>
      <div class="match-rate-chart-axis"><span>${first}</span><span>${last}</span></div>
    </div>
  `;
}

function normalizeMatchRateSeries(points, rateKey = 'matches_per_hour') {
  return (Array.isArray(points) ? points : []).map(point => {
    const row = point || {};
    const matches = Number(row.matches || 0);
    const rate = row[rateKey] == null ? matches : Number(row[rateKey]);
    return {
      time: row.bucket_start || '',
      matches,
      rate: Number.isFinite(rate) ? rate : 0,
    };
  });
}

function formatChartBucket(value, axis) {
  return axis === 'date' ? awstDate(value) : awstTime(value);
}

function formatChartRate(value, unit) {
  if (unit === 'day') return formatCount(Math.round(Number(value) || 0));
  return formatMatchRate(value);
}

function withCoverageMatchRates(coverage, windows) {
  if (
    coverage.matches_per_hour_6h != null
    && coverage.matches_per_hour_24h != null
  ) {
    return coverage;
  }

  const rates = {
    matches_24h: 0,
    matches_6h: 0,
    matches_per_hour_24h: 0,
    matches_per_hour_6h: 0,
  };
  for (const w of Array.isArray(windows) ? windows : []) {
    const hours = Number(w.hours || 0);
    const found = Number(w.outcomes?.found || 0);
    if (hours === 24) {
      rates.matches_24h = found;
      rates.matches_per_hour_24h = found / 24;
    } else if (hours === 6) {
      rates.matches_6h = found;
      rates.matches_per_hour_6h = found / 6;
    }
  }
  return {...coverage, ...rates};
}

function renderSearchCard(windows) {
  return `
    <div class="dashboard-card dashboard-wide">
      <div class="dashboard-card-title">Search Throughput</div>
      <table class="dashboard-table">
        <thead><tr><th>Window</th><th>Searches</th><th>Requests</th><th>24h Pace</th><th>Median</th><th>P95</th><th>Found</th><th>No match</th><th>Empty</th><th>Resets</th><th>Errors</th></tr></thead>
        <tbody>
          ${windows.map(w => `
            <tr>
              <td>${esc(w.label)}</td>
              <td>${formatCount(w.searches)}</td>
              <td>${formatCount(w.distinct_requests)}</td>
              <td>${formatProjected24h(w)}</td>
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
        <thead><tr><th>Completed</th><th>Total</th><th>Search</th><th>Browse</th><th>Match</th><th>Watchdog</th><th>Peer/Dirs</th><th>Waves</th></tr></thead>
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

function renderPeerBrowseHeavyQueries(peers) {
  const rows = /** @type {any[]} */ (peers.heavy_queries || []);
  const hours = Number(peers.heavy_query_hours || 24);
  return `
    <div class="dashboard-card dashboard-wide">
      <div class="dashboard-card-title">Peer/Dir Heavy Queries (${formatCount(hours)}h)</div>
      <table class="dashboard-table dashboard-query-table">
        <thead><tr><th>Searched</th><th>Req</th><th>MBID</th><th>Release</th><th>Query</th><th>Variant</th><th>Results</th><th>Peer/Dirs</th><th>Waves</th><th>Browse</th></tr></thead>
        <tbody>
          ${rows.map(r => {
            const release = [r.artist_name, r.album_title].filter(Boolean).join(' - ');
            const mbid = r.mb_release_id || '';
            return `
              <tr>
                <td>${awstDateTime(r.created_at || '')}</td>
                <td title="search_log #${formatCount(r.search_log_id)}">#${r.request_id}</td>
                <td title="${esc(mbid)}">${esc(formatShortText(mbid, 8))}</td>
                <td title="${esc(release)}">${esc(formatShortText(release, 28))}</td>
                <td class="dashboard-query-cell" title="${esc(r.query || '')}">${esc(r.query || '')}</td>
                <td>${esc(r.variant || '')}</td>
                <td>${formatCount(r.result_count)}</td>
                <td class="${r.peer_dirs > 10000 ? 'metric-warn' : ''}">${formatCount(r.peer_dirs)}</td>
                <td>${formatCount(r.fanout_waves)}</td>
                <td>${formatDuration(r.browse_time_s)}</td>
              </tr>
            `;
          }).join('')}
          ${rows.length === 0 ? '<tr><td colspan="10">No per-query peer/dir metrics yet</td></tr>' : ''}
        </tbody>
      </table>
    </div>
  `;
}

function renderLoopSuspects(rows) {
  const topRows = rows.slice(0, 3);
  return `
    <div class="dashboard-card dashboard-wide">
      <div class="dashboard-card-title">Loop Suspects</div>
      <table class="dashboard-table">
        <thead><tr><th>ID</th><th>Artist</th><th>Album</th><th>24h</th><th>Found</th><th>No match</th><th>Empty</th><th>Resets</th><th>Errors</th></tr></thead>
        <tbody>
          ${topRows.map(r => `
            <tr>
              <td>#${r.request_id}</td>
              <td title="${esc(r.artist_name || '')}">${esc(formatShortText(r.artist_name, 10))}</td>
              <td title="${esc(r.album_title || '')}">${esc(formatShortText(r.album_title, 5))}</td>
              <td class="${r.searches_24h > 3 ? 'metric-warn' : ''}">${formatCount(r.searches_24h)}</td>
              <td>${formatCount(r.found_24h)}</td>
              <td>${formatCount(r.no_match_24h)}</td>
              <td>${formatCount(r.no_results_24h)}</td>
              <td>${formatCount(r.reset_24h)}</td>
              <td class="${r.problem_24h ? 'metric-warn' : ''}">${formatCount(r.problem_24h)}</td>
            </tr>
          `).join('')}
          ${topRows.length === 0 ? '<tr><td colspan="9">No repeated wanted searches in 24h</td></tr>' : ''}
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

function formatSignedCount(value) {
  if (value == null || Number.isNaN(Number(value))) return '0';
  const n = Number(value);
  const formatted = Math.abs(n).toLocaleString();
  return n > 0 ? `+${formatted}` : n < 0 ? `-${formatted}` : '0';
}

function formatDecimal(value) {
  if (value == null || Number.isNaN(Number(value))) return 'n/a';
  const n = Number(value);
  return n >= 10 ? n.toFixed(1) : n.toFixed(2);
}

function formatEtaHours(value) {
  if (value == null || Number.isNaN(Number(value))) return 'n/a';
  const hours = Number(value);
  if (hours < 24) return `${formatDecimal(hours)}h`;
  const days = hours / 24;
  return `${days >= 10 ? days.toFixed(0) : days.toFixed(1)}d`;
}

function formatMatchRate(value) {
  if (value == null || Number.isNaN(Number(value))) return '0.00';
  const n = Number(value);
  return n >= 10 ? n.toFixed(1) : n.toFixed(2);
}

function formatShortText(value, maxLength) {
  const text = String(value || '');
  return text.length > maxLength ? text.slice(0, maxLength) : text;
}

function formatProjected24h(w) {
  if (w.searches_per_24h != null) {
    const direct = Number(w.searches_per_24h);
    if (Number.isFinite(direct)) return formatCount(Math.round(direct));
  }

  if (w.searches_per_hour != null) {
    const perHour = Number(w.searches_per_hour);
    if (Number.isFinite(perHour)) return formatCount(Math.round(perHour * 24));
  }

  const searches = Number(w.searches);
  const hours = Number(w.hours);
  if (Number.isFinite(searches) && Number.isFinite(hours) && hours > 0) {
    return formatCount(Math.round((searches / hours) * 24));
  }

  return 'n/a';
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
  formatEtaHours,
  formatWantedTrendWindow,
  normalizeMatchRateSeries,
  normalizeWantedTrendSeries,
  renderDailyMatchRateChart,
  renderCoverageCard,
  renderHourlyMatchRateChart,
  renderMatchRateChart,
  renderPeerBrowseHeavyQueries,
  renderPeersCard,
  renderPipelineListBody,
  clearPipelineSearch,
  renderPipelineNav,
  renderWantedTrendCard,
  renderWantedTrendChart,
  withCoverageMatchRates,
};
