// @ts-check
// Pipeline Dashboard cards + charts (#434) — split from pipeline.js so
// queue and dashboard concerns evolve independently. Pure render
// helpers over `state.pipelineDashboardData`; the queue module owns
// the nav strip and passes its HTML in, keeping this dependency
// one-way (pipeline.js -> pipeline_dashboard.js).
import { state } from './state.js';
import { esc, awstDate, awstDateTime, awstTime } from './util.js';


export function renderPipelineDashboard(navHtml) {
  const el = document.getElementById('pipeline-content');
  const data = state.pipelineDashboardData;
  if (!data) {
    el.innerHTML = `${navHtml}<div class="loading">Loading...</div>`;
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
    ${navHtml}
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
  renderWantedTrendCard,
  renderWantedTrendChart,
  withCoverageMatchRates,
};
