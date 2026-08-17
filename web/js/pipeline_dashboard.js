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
      ${renderDiskCoverageCard(data.disk_coverage)}
      ${renderLibraryCompletenessCard(data.library_completeness)}
      ${renderRetagDivergenceCensusCard(data.retag_divergence_census)}
      ${renderWantedTrendCard(coverageWithRates.wanted_trend || {})}
      ${renderPeersCard(peers)}
      ${renderSearchCard(searches)}
      ${renderCycleCard(cycles)}
      ${renderCycleOutliers(data.cycles?.outliers || [])}
      ${renderPeerBrowseHeavyQueries(peers)}
      ${renderLoopSuspects(coverage.top_loop_suspects || [])}
      ${renderStaleWanted(coverage.stale_wanted || [])}
      ${renderUnfindableCard(data.unfindable || {})}
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

function renderDiskCoverageCard(dc) {
  if (!dc) {
    return `
    <div class="dashboard-card">
      <div class="dashboard-card-title">Disk Coverage</div>
      <div class="metric-list">
        <div class="metric-row"><span>Beets DB</span><strong class="metric-muted">unavailable</strong></div>
      </div>
    </div>
  `;
  }
  const c = dc.counts || {};
  const drift = Array.isArray(dc.drift_rows) ? dc.drift_rows : [];
  const wantedOffDisk = (c.off_disk_by_status || {}).wanted || 0;
  const driftClass = drift.length > 0 ? 'metric-bad' : 'metric-good';
  const driftRowsHtml = drift.map(r => renderDriftRow(r)).join('');
  return `
    <div class="dashboard-card">
      <div class="dashboard-card-title">Disk Coverage</div>
      <div class="metric-list">
        <div class="metric-row"><span>On disk</span><strong>${formatCount(c.on_disk_total)} / ${formatCount(c.active_total)}</strong></div>
        <div class="metric-row"><span>Wanted, not uniquely in Beets</span><strong class="metric-muted">${formatCount(wantedOffDisk)}</strong></div>
        <div class="metric-row"><span>Imported, not uniquely in Beets</span><strong class="${driftClass}">${formatCount(drift.length)}</strong></div>
        ${driftRowsHtml}
      </div>
    </div>
  `;
}

/** Render the persisted read-only source/catalog/files census. */
function renderLibraryCompletenessCard(census) {
  const c = census || {};
  if (c.state === 'missing') {
    return `<div class="dashboard-card dashboard-wide"><div class="dashboard-card-title">Library Completeness</div><div class="metric-list"><div class="metric-row"><span>Status</span><strong class="metric-muted">no census published yet</strong></div></div></div>`;
  }
  if (c.state === 'unreadable') {
    return `<div class="dashboard-card dashboard-wide"><div class="dashboard-card-title">Library Completeness</div><div class="metric-list"><div class="metric-row"><span>Status</span><strong class="metric-bad">snapshot unreadable</strong></div><div class="metric-row"><span>Error</span><strong>${esc(c.error || '')}</strong></div></div></div>`;
  }
  const snapshot = c.snapshot || {};
  const report = snapshot.report || {};
  const counts = report.counts || {};
  const rows = Array.isArray(report.albums) ? report.albums : [];
  const findingLabels = {
    missing_source_audio: 'Missing source audio',
    catalog_drift: 'Catalog drift',
    non_audio_omitted: 'Non-audio omitted',
    unknown: 'Unknown',
  };
  const stale = retagDivergenceSnapshotIsStale(snapshot.generated_at);
  const statusClass = stale ? 'metric-warn' : report.status === 'complete' ? 'metric-good' : report.status === 'unknown' ? 'metric-warn' : 'metric-bad';
  const shown = Number.isFinite(c.albums_shown) ? c.albums_shown : rows.length;
  const total = Number.isFinite(c.albums_listed_total) ? c.albums_listed_total : rows.length;
  return `
    <div class="dashboard-card dashboard-wide">
      <div class="dashboard-card-title">Library Completeness</div>
      <div class="metric-list">
        <div class="metric-row"><span>Status</span><strong class="${statusClass}">${esc(stale ? `${report.status || 'unknown'} (stale)` : report.status || 'unknown')}</strong></div>
        <div class="metric-row"><span>Last run</span><strong>${snapshot.generated_at ? awstDateTime(snapshot.generated_at) : 'n/a'} (${formatDuration(snapshot.duration_seconds)})</strong></div>
        <div class="metric-row"><span>Audio complete</span><strong>${formatCount(counts.audio_complete)} / ${formatCount(counts.albums_scanned)}</strong></div>
        <div class="metric-row"><span>Missing source audio</span><strong class="${Number(counts.missing_source_audio) ? 'metric-bad' : 'metric-muted'}">${formatCount(counts.missing_source_audio)}</strong></div>
        <div class="metric-row"><span>Catalog drift</span><strong class="${Number(counts.catalog_drift) ? 'metric-warn' : 'metric-muted'}">${formatCount(counts.catalog_drift)}</strong></div>
        <div class="metric-row"><span>Non-audio omitted</span><strong>${formatCount(counts.non_audio_omitted)}</strong></div>
        <div class="metric-row"><span>Unknown</span><strong class="${Number(counts.unknown) ? 'metric-warn' : 'metric-muted'}">${formatCount(counts.unknown)}</strong></div>
        <div class="metric-row"><span>Exceptional albums</span><strong>${formatCount(shown)} / ${formatCount(total)}</strong></div>
        ${rows.map(row => `<div class="metric-row"><span>#${row.album_id} ${esc(row.artist || '?')} — ${esc(row.title || '?')}</span><strong>${esc((row.findings || []).map(f => `${findingLabels[f.kind] || 'Unknown'}: ${f.detail || ''}`).join('; '))}</strong></div>`).join('')}
      </div>
    </div>`;
}

/**
 * One Disk Coverage drift row: an `imported` request the dashboard cannot
 * uniquely resolve against Beets. `drift_rows` carries every not-uniquely-
 * present `imported` row regardless of cause or source (#1089 MINOR-3) — a
 * MusicBrainz merge is only ONE reason a row can drift, so the "Follow MB
 * merge" button renders only when `r.source === 'musicbrainz'` (#1089
 * MAJOR-A, review round 3). This is NOT the same as `r.mb_release_id`
 * being present: production Discogs rows duplicate the numeric id into
 * BOTH `mb_release_id` and `discogs_release_id`
 * (`ReleaseIdentity.from_strict_fields`'s own docstring), so a
 * column-truthiness gate renders the button on every Discogs-sourced
 * drift row too — `source` is derived server-side from the VALUE's shape
 * (`lib/disk_coverage_service.py`, via `ReleaseIdentity.from_strict_fields`
 * — the SAME strict, conflict-failing derivation
 * `MergeRekeyService.rekey_request`'s own admission test uses, #1089 N4
 * review round 4, so a row with a real MB UUID plus a conflicting numeric
 * Discogs id shows no button either — the service would refuse it too),
 * never from which column is non-null. A Discogs-sourced or otherwise
 * non-MB drift row still shows, just without an action this arm can never
 * resolve; a never-merged MB-sourced row KEEPS the button — clicking it
 * and landing on `not_merged` (the #8792 Slipknot Vol. 3 shape) is
 * designed UX, not a case to hide.
 *
 * The button rekeys the request's ledger onto the MusicBrainz merge survivor
 * Beets already holds — request-ledger-only, never mutates Beets. The click
 * handler (`mergeRekeyRequest`) lives in `pipeline.js`, which already owns
 * `loadPipelineDashboard`; this module stays a one-way dependency
 * (pipeline.js -> pipeline_dashboard.js, per the header comment) so it never
 * imports back from there — the button is wired through the `window.*`
 * binding in `main.js` instead, exactly like the "Refresh" button above.
 * @param {any} r
 * @returns {string}
 */
function renderDriftRow(r) {
  const ambiguousAlbumCount = Array.isArray(r.resolution?.album_ids)
    ? r.resolution.album_ids.length : 0;
  const resolution = r.resolution?.kind === 'ambiguous'
    ? `ambiguous (${formatCount(ambiguousAlbumCount)} ${ambiguousAlbumCount === 1 ? 'album' : 'albums'})`
    : 'missing';
  const action = r.source === 'musicbrainz'
    ? `
        <div class="metric-row drift-row-action">
          <button class="p-btn" onclick="window.mergeRekeyRequest(${r.id}, this)">Follow MB merge</button>
          <span class="drift-row-note" id="drift-note-${r.id}"></span>
        </div>`
    : '';
  return `
        <div class="metric-row">
          <span>#${r.id} ${esc(r.artist_name || '?')} — ${esc(r.album_title || '?')}</span>
          <strong class="metric-bad">${resolution}</strong>
        </div>${action}`;
}

/**
 * Snapshot age past which the "Last run" reads stale (#1142 review N5)
 * — a full day past the expected 24h daily cadence, well outside the
 * timer's own 30min jitter, so ordinary scheduling variance never trips
 * it; anything past this really has missed at least one scheduled run.
 * The boundary is EXCLUSIVE: exactly 36h old is still fresh.
 */
const RETAG_CENSUS_STALE_AFTER_HOURS = 36;

/**
 * Hours between `generatedAt` and `nowMs`, or `null` when `generatedAt`
 * is missing or unparsable — computed client-side, no persisted field
 * needed (#1142 review N5).
 * @param {string | null | undefined} generatedAt
 * @param {number} [nowMs]
 * @returns {number | null}
 */
function retagDivergenceSnapshotAgeHours(generatedAt, nowMs = Date.now()) {
  if (!generatedAt) return null;
  const then = Date.parse(generatedAt);
  if (Number.isNaN(then)) return null;
  return (nowMs - then) / 3600000;
}

/**
 * Whether a snapshot this old counts as stale — see
 * `RETAG_CENSUS_STALE_AFTER_HOURS` for the exact (exclusive) boundary.
 * @param {string | null | undefined} generatedAt
 * @param {number} [nowMs]
 * @returns {boolean}
 */
function retagDivergenceSnapshotIsStale(generatedAt, nowMs = Date.now()) {
  const ageHours = retagDivergenceSnapshotAgeHours(generatedAt, nowMs);
  return ageHours != null && ageHours > RETAG_CENSUS_STALE_AFTER_HOURS;
}

/**
 * Tone for the census report's own `status` field — `clean` is the
 * ONLY status that ever reads good; `divergence_found` reads bad;
 * everything else (`incomplete`, or a defensively-handled
 * `beets_unavailable` the daily writer no longer actually publishes)
 * reads warn (#1142 review N1).
 * @param {string | undefined} status
 * @returns {string}
 */
function retagDivergenceStatusTone(status) {
  if (status === 'clean') return 'metric-good';
  if (status === 'divergence_found') return 'metric-bad';
  return 'metric-warn';
}

/**
 * Render the daily whole-library retag-divergence census card (#1142) —
 * Beets DB identity vs. installed file tags. Deliberately a SEPARATE
 * card from Disk Coverage above (pipeline-ledger vs. Beets DB): the two
 * drift questions are independent and must never be conflated in the UI.
 *
 * Reads a PERSISTED snapshot (`GET /api/pipeline/dashboard` embeds it
 * read-only, per `web/routes/pipeline_dashboard.py`) — this module never
 * triggers a scan. `state` mirrors the route's own three-way honest
 * split: "missing" (no daily run has published yet), "unreadable" (a
 * corrupt snapshot file — logged server-side, never a 500), "ok" (a
 * real published report). Only non-agreeing albums are ever listed
 * (the whole-library report's own contract), so no artificial
 * client-side truncation is needed at the live population size this was
 * built for (single digits) — see the module's own PR body for the
 * measured live count.
 * @param {any} census
 * @returns {string}
 */
function renderRetagDivergenceCensusCard(census) {
  const c = census || {};
  const state = c.state || 'missing';
  if (state === 'missing') {
    return `
      <div class="dashboard-card dashboard-wide">
        <div class="dashboard-card-title">Beets DB &harr; File Tags Drift</div>
        <div class="metric-list">
          <div class="metric-row"><span>Status</span><strong class="metric-muted">no census published yet</strong></div>
        </div>
      </div>
    `;
  }
  if (state === 'unreadable') {
    return `
      <div class="dashboard-card dashboard-wide">
        <div class="dashboard-card-title">Beets DB &harr; File Tags Drift</div>
        <div class="metric-list">
          <div class="metric-row"><span>Status</span><strong class="metric-bad">snapshot unreadable</strong></div>
          <div class="metric-row"><span>Error</span><strong>${esc(c.error || '')}</strong></div>
        </div>
      </div>
    `;
  }
  const snapshot = c.snapshot || {};
  const report = snapshot.report || {};
  const counts = report.counts || {};
  const albums = Array.isArray(report.albums) ? report.albums : [];
  // The dashboard ROUTE caps how many albums it ever embeds
  // (web/routes/pipeline_dashboard.py::DASHBOARD_RETAG_CENSUS_ALBUM_CAP)
  // — albums_shown/albums_listed_total name the cap explicitly rather
  // than letting the row list silently look like the whole truth
  // (#1142 fresh review N1). Fall back to albums.length when the
  // fields are absent (older/synthetic payloads), which is exactly
  // "nothing was capped".
  const albumsShown = Number.isFinite(c.albums_shown) ? c.albums_shown : albums.length;
  const albumsListedTotal = Number.isFinite(c.albums_listed_total)
    ? c.albums_listed_total : albums.length;
  const isCapped = albumsListedTotal > albumsShown;
  const statusClass = retagDivergenceStatusTone(report.status);
  // The "Listed" count only ever means "verified clean" when the run
  // itself says `clean` — a non-clean status (e.g. `incomplete`, or a
  // defensively-handled `beets_unavailable`) reading zero listed albums
  // is NOT the same fact as a clean library, so it must never render
  // the same green as a genuine clean result (#1142 review N1).
  const listedClass = report.status === 'clean' ? 'metric-good'
    : albumsListedTotal ? 'metric-warn' : 'metric-muted';
  // Albums-scanned is only a trustworthy whole-library count when the
  // scan itself answered in full (`clean`/`divergence_found`); mute it
  // otherwise rather than presenting a possibly-partial number plainly.
  const scannedClass = report.status === 'clean' || report.status === 'divergence_found'
    ? '' : 'metric-muted';
  // A retained snapshot from a run that never happened (a missed daily
  // timer fire, or every recent run failing before publish) must say so
  // honestly rather than silently presenting yesterday's — or last
  // week's — report as current (#1142 review N5).
  const ageHours = retagDivergenceSnapshotAgeHours(snapshot.generated_at);
  const isStale = retagDivergenceSnapshotIsStale(snapshot.generated_at);
  const freshnessLabel = ageHours == null ? 'unknown'
    : isStale ? `stale — ${formatDecimal(ageHours)}h old` : 'fresh';
  const freshnessClass = ageHours == null ? 'metric-muted'
    : isStale ? 'metric-warn' : 'metric-good';
  return `
    <div class="dashboard-card dashboard-wide">
      <div class="dashboard-card-title">Beets DB &harr; File Tags Drift</div>
      <div class="metric-list">
        <div class="metric-row"><span>Last run</span><strong class="${isStale ? 'metric-warn' : ''}">${snapshot.generated_at ? awstDateTime(snapshot.generated_at) : 'n/a'}</strong></div>
        <div class="metric-row"><span>Freshness</span><strong class="${freshnessClass}">${esc(freshnessLabel)}</strong></div>
        <div class="metric-row"><span>Duration</span><strong>${formatDuration(snapshot.duration_seconds)}</strong></div>
        <div class="metric-row"><span>Result</span><strong class="${statusClass}">${esc(report.status || 'unknown')}</strong></div>
        <div class="metric-row"><span>Albums scanned</span><strong class="${scannedClass}">${formatCount(counts.albums_scanned)}</strong></div>
        <div class="metric-row"><span>Listed (non-agreeing)</span><strong class="${listedClass}">${formatCount(albumsListedTotal)}</strong></div>
        ${isCapped ? `<div class="metric-row"><span></span><strong class="metric-muted">Showing ${formatCount(albumsShown)} of ${formatCount(albumsListedTotal)}</strong></div>` : ''}
        ${albums.map(a => renderRetagDivergenceAlbumRow(a)).join('')}
      </div>
    </div>
  `;
}

/**
 * One retag-divergence album row, wrapped in an id'd container so the
 * per-album recheck handler (`pipeline.js::recheckRetagDivergenceAlbum`)
 * can patch just this row's `innerHTML` after a fresh check, instead of
 * reloading the whole dashboard.
 * @param {any} album
 * @returns {string}
 */
function renderRetagDivergenceAlbumRow(album) {
  return `
    <div class="retag-album-row" id="retag-album-${album.album_id}">
      ${renderRetagDivergenceAlbumRowInner(album)}
    </div>
  `;
}

/**
 * The inner content of one retag-divergence album row (classification
 * line + Recheck button), WITHOUT the outer id'd container
 * `renderRetagDivergenceAlbumRow` wraps it in. Exported (not just
 * `__test__`-only) because `pipeline.js::recheckRetagDivergenceAlbum`
 * calls this for real, to re-render just this row's `innerHTML` in
 * place after a fresh per-album check — never the outer container,
 * which already exists in the DOM and must keep its own `id`.
 * @param {any} album
 * @returns {string}
 */
export function renderRetagDivergenceAlbumRowInner(album) {
  const classClass = album.album_class === 'agrees' ? 'metric-good'
    : album.album_class === 'diverges' || album.album_class === 'file_tag_present_db_absent'
      ? 'metric-bad'
      : 'metric-warn';
  const items = Array.isArray(album.items) ? album.items : [];
  return `
    <div class="metric-row">
      <span>Album #${album.album_id} <code title="${esc(album.db_mb_albumid || '')}">${esc(album.db_mb_albumid || '(none)')}</code></span>
      <strong class="${classClass}">${esc(album.album_class)}</strong>
    </div>
    <div class="metric-row"><span>Items</span><strong>${formatCount(items.length)}</strong></div>
    ${items.filter(item => item.item_class !== 'agrees').map(renderRetagDivergenceItemRow).join('')}
    <div class="metric-row drift-row-action">
      <button class="p-btn" onclick="window.recheckRetagDivergenceAlbum(${album.album_id}, this)">Recheck</button>
      <span class="drift-row-note" id="retag-album-note-${album.album_id}"></span>
    </div>
  `;
}

/**
 * One non-agreeing item's identity mismatch — the core product claim
 * ("which file tag disagrees"), rendered as class + the file's own
 * mb_albumid tag (or `(none)`) + any classification detail (#1142
 * fresh review N2). Deliberately never renders `item.path`: a full
 * filesystem path is arbitrary-length operator-facing data with no
 * essential role in showing WHAT disagrees, only WHERE — omitted to
 * keep the row bounded and avoid echoing raw filesystem structure into
 * the page.
 * @param {any} item
 * @returns {string}
 */
function renderRetagDivergenceItemRow(item) {
  const tone = item.item_class === 'unreadable' ? 'metric-warn' : 'metric-bad';
  const identity = item.file_mb_albumid ? esc(item.file_mb_albumid) : '(none)';
  const detail = item.detail ? ` — ${esc(item.detail)}` : '';
  return `
    <div class="metric-row retag-item-row">
      <span>${esc(item.item_class)}: ${identity}${detail}</span>
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

/**
 * Render the "Unfindable detection" card — latest-run facts, per-outcome
 * breakdown for recent runs, and a due-backlog trend chart (#1112). Modest
 * by design: one card, no interactive toggles, mirroring the daily (not
 * 5-min) cadence of the underlying data.
 * @param {any} unfindable
 * @returns {string}
 */
function renderUnfindableCard(unfindable) {
  const runs = /** @type {any[]} */ (unfindable.recent_runs || []);
  const latest = runs.length > 0 ? runs[0] : null;
  const trend = unfindable.backlog_trend || {};
  const breakerClass = latest && latest.breaker_tripped ? 'metric-bad' : 'metric-good';
  return `
    <div class="dashboard-card dashboard-wide">
      <div class="dashboard-card-title">Unfindable Detection</div>
      ${renderUnfindableBacklogChart(trend.series || [])}
      <div class="metric-list">
        <div class="metric-row"><span>Last run</span><strong>${latest ? awstDateTime(latest.created_at) : 'never'}</strong></div>
        <div class="metric-row"><span>Cohort</span><strong>${latest ? formatCount(latest.cohort_total) : 'n/a'}</strong></div>
        <div class="metric-row"><span>Due backlog</span><strong>${latest ? formatCount(latest.due_backlog_at_start) : 'n/a'}</strong></div>
        <div class="metric-row"><span>Batch limit / processed</span><strong>${latest ? `${formatCount(latest.batch_limit)} / ${formatCount(latest.candidates_processed)}` : 'n/a'}</strong></div>
        <div class="metric-row"><span>Probes attempted</span><strong>${latest ? formatCount(latest.probes_attempted) : 'n/a'}</strong></div>
        <div class="metric-row"><span>Breaker tripped</span><strong class="${latest ? breakerClass : 'metric-muted'}">${latest ? (latest.breaker_tripped ? 'yes' : 'no') : 'n/a'}</strong></div>
      </div>
      <table class="dashboard-table">
        <thead><tr><th>Run</th><th>Probes</th><th>Categorised</th><th>Downgraded</th><th>No change</th><th>Probe failed</th><th>Breaker</th></tr></thead>
        <tbody>
          ${runs.map(r => `
            <tr>
              <td>${awstDateTime(r.created_at || '')}</td>
              <td>${formatCount(r.probes_attempted)}</td>
              <td>${formatCount(r.categorised_count)}</td>
              <td>${formatCount(r.downgraded_count)}</td>
              <td>${formatCount(r.no_change_count)}</td>
              <td class="${r.probe_failed_count ? 'metric-warn' : ''}">${formatCount(r.probe_failed_count)}</td>
              <td class="${r.breaker_tripped ? 'metric-bad' : ''}">${r.breaker_tripped ? 'yes' : 'no'}</td>
            </tr>
          `).join('')}
          ${runs.length === 0 ? '<tr><td colspan="7">No unfindable-detection runs yet</td></tr>' : ''}
        </tbody>
      </table>
    </div>
  `;
}

function renderUnfindableBacklogChart(points) {
  const series = normalizeUnfindableBacklogSeries(points);
  if (series.length < 2) {
    return `<div class="wanted-trend-chart"><div class="chart-empty">Collecting run history</div></div>`;
  }

  const width = 240;
  const height = 64;
  const minBacklog = Math.min(...series.map(p => p.backlog));
  const maxBacklog = Math.max(...series.map(p => p.backlog));
  const range = Math.max(1, maxBacklog - minBacklog);
  const coords = series.map((point, index) => {
    const x = series.length === 1 ? width : (index / (series.length - 1)) * width;
    const y = height - ((point.backlog - minBacklog) / range) * height;
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  }).join(' ');
  const area = `0,${height} ${coords} ${width},${height}`;
  const first = series[0]?.time ? awstDate(series[0].time) : '';
  const last = series[series.length - 1]?.time ? awstDate(series[series.length - 1].time) : '';
  const latest = series[series.length - 1]?.backlog;
  return `
    <div class="wanted-trend-chart">
      <div class="match-rate-chart-head"><span>Due backlog per run</span><strong>${formatCount(latest)}</strong></div>
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="Unfindable due-backlog trend">
        <polygon class="wanted-trend-area" points="${area}"></polygon>
        <polyline class="wanted-trend-line" points="${coords}"></polyline>
      </svg>
      <div class="match-rate-chart-axis"><span>${first}</span><span>${last}</span></div>
    </div>
  `;
}

function normalizeUnfindableBacklogSeries(points) {
  return (Array.isArray(points) ? points : []).map(point => {
    const row = point || {};
    const backlog = Number(row.due_backlog_at_start);
    return {
      time: row.sampled_at || '',
      backlog: Number.isFinite(backlog) ? backlog : 0,
    };
  }).filter(point => point.time || Number.isFinite(point.backlog));
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
  normalizeUnfindableBacklogSeries,
  normalizeWantedTrendSeries,
  renderDailyMatchRateChart,
  renderCoverageCard,
  renderDiskCoverageCard,
  renderLibraryCompletenessCard,
  renderDriftRow,
  renderHourlyMatchRateChart,
  renderMatchRateChart,
  renderPeerBrowseHeavyQueries,
  renderPeersCard,
  renderRetagDivergenceAlbumRow,
  renderRetagDivergenceAlbumRowInner,
  renderRetagDivergenceCensusCard,
  renderUnfindableBacklogChart,
  renderUnfindableCard,
  renderWantedTrendCard,
  renderWantedTrendChart,
  retagDivergenceSnapshotAgeHours,
  retagDivergenceSnapshotIsStale,
  withCoverageMatchRates,
};
