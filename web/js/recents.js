// @ts-check
import { state, API } from './state.js';
import { awstDate, awstTime, esc } from './util.js';
import { toggleDetail } from './pipeline.js';
import { renderSearchPlanButton } from './search_plan.js';

/**
 * Set the recents filter and reload.
 * @param {string} f
 */
export function setRecentsFilter(f) {
  state.recentsFilter = f;
  loadRecents();
}

/**
 * Switch Recents between history and import queue timeline.
 * @param {string} sub
 */
export function setRecentsSub(sub) {
  state.recentsSub = sub;
  loadRecents();
}

function renderRecentsSubnav() {
  return `<div class="pipeline-subtabs">
    <button class="p-btn ${state.recentsSub === 'history' ? 'active-status' : ''}" onclick="window.setRecentsSub('history')">History</button>
    <button class="p-btn ${state.recentsSub === 'downloading' ? 'active-status' : ''}" onclick="window.setRecentsSub('downloading')">Downloading</button>
    <button class="p-btn ${state.recentsSub === 'queue' ? 'active-status' : ''}" onclick="window.setRecentsSub('queue')">Queue</button>
    <button class="p-btn subtab-refresh" onclick="window.loadRecents()">Refresh</button>
  </div>`;
}

/**
 * Render recents items grouped by date.
 * @param {Array<Object>} items
 * @param {Object|null} [matchRates]
 * @returns {string} HTML string
 */
export function renderRecentsItems(items, matchRates = null) {
  if (items.length === 0) return '<div class="loading">No matching entries</div>';

  // Group by date (AWST)
  const byDate = {};
  for (const item of items) {
    const date = awstDate(item.created_at || '');
    if (!byDate[date]) byDate[date] = [];
    byDate[date].push(item);
  }
  const dates = Object.keys(byDate).sort().reverse();

  return dates.map((date, idx) => `
    ${renderRecentsDateHeader(date, idx === 0 ? matchRates : null)}
    ${byDate[date].map(item => {
      const time = awstTime(item.created_at || '');
      const badge = item.badge || '';
      const badgeClass = item.badge_class || '';
      const borderColor = item.border_color || '#444';
      const summary = item.summary || '';
      const badExtensions = Array.isArray(item.bad_extensions) ? item.bad_extensions : [];
      const triageSummary = item.wrong_match_triage_summary || '';
      const triageDetail = item.wrong_match_triage_detail
        || (Array.isArray(item.wrong_match_triage_stage_chain)
          ? item.wrong_match_triage_stage_chain.join(' · ')
          : '');

      // Issue #130: a `disambiguation_failure` chip surfaces post-import
      // `beet move` errors that leave the album in beets at a stale path.
      // Rendered inline next to the main badge; hover for detail.
      const disambigChip = item.disambiguation_failure
        ? `<span class="badge badge-warn" title="${esc(item.disambiguation_detail || '')}">disambig: ${esc(item.disambiguation_failure)}</span>`
        : '';
      const badExtChip = badExtensions.length
        ? `<span class="badge badge-warn" title="${esc(badExtensions.join(', '))}">bad ext: ${badExtensions.length}</span>`
        : '';
      const triageChip = triageSummary
        ? `<span class="badge badge-warn" title="${esc(triageDetail)}">triage: ${esc(triageSummary)}</span>`
        : '';

      // Search-plan inspector button — Recents rows always render the
      // button. Use the request_id (the album_requests.id) since the
      // download_log row's id (item.id) is the wrong cursor space.
      const spBtn = renderSearchPlanButton({ pipelineId: item.request_id });

      return `
        <div class="r-item" style="border-left-color:${borderColor}" onclick="window.toggleDetail('dl-${item.id}', ${item.request_id})">
          <div class="p-top">
            <div>
              <div class="p-title">${esc(item.album_title)} <span class="badge ${badgeClass}">${badge}</span>${disambigChip}${badExtChip}${triageChip}</div>
              <div class="p-artist">${esc(item.artist_name)}</div>
            </div>
            <div class="p-row-actions">${spBtn}<span style="font-size:0.75em;color:#666;">${time}</span></div>
          </div>
          <div class="p-meta">
            <span>${esc(summary)}</span>
          </div>
        </div>
        <div class="p-detail" id="dl-${item.id}"></div>
      `;
    }).join('')}
  `).join('');
}

function renderRecentsDateHeader(date, matchRates) {
  if (!matchRates) return `<div class="r-date-header">${date}</div>`;
  return `<div class="r-date-header recents-date-header">
    <span>${date}</span>
    <span class="recents-date-metrics">6h ${formatMatchRate(matchRates.matches_per_hour_6h)} match/hr · 24h ${formatMatchRate(matchRates.matches_per_hour_24h)} match/hr</span>
  </div>`;
}

function queueBadge(job, index) {
  if (job.status === 'completed') return ['completed', 'badge-new'];
  if (job.status === 'failed') return ['failed', 'badge-failed'];
  if (job.status === 'running') return ['importing', 'badge-force'];
  if (job.preview_status === 'evidence_ready') {
    return [index === 0 ? 'next check' : 'ready check', 'badge-new'];
  }
  if (job.preview_status === 'would_import') {
    return [index === 0 ? 'next legacy check' : 'legacy ready', 'badge-new'];
  }
  if (job.preview_status === 'running') return ['previewing', 'badge-warn'];
  if (job.preview_status === 'waiting') return ['waiting preview', 'badge-library'];
  if (job.preview_status === 'confident_reject') return ['preview reject', 'badge-failed'];
  if (job.preview_status === 'uncertain') return ['uncertain', 'badge-warn'];
  if (job.preview_status === 'error') return ['preview error', 'badge-failed'];
  return [job.status || 'queued', 'badge-library'];
}

function queueBorderColor(job) {
  if (job.status === 'failed' || ['confident_reject', 'error'].includes(job.preview_status)) return '#a33';
  if (job.preview_status === 'uncertain' || job.preview_status === 'running') return '#a93';
  if (job.preview_status === 'evidence_ready' || job.preview_status === 'would_import' || job.status === 'completed') return '#1a4a2a';
  if (job.status === 'running') return '#36c';
  return '#1a3a5a';
}

function queueMessage(job) {
  if (job.status === 'completed' || job.status === 'failed') {
    return job.message || job.error || job.preview_message || job.preview_error || '';
  }
  return job.preview_message || job.message || job.preview_error || job.error || '';
}

/**
 * Render import queue timeline rows.
 * @param {Array<Object>} jobs
 * @returns {string}
 */
export function renderImportQueueItems(jobs) {
  if (jobs.length === 0) return '<div class="loading">No queued imports</div>';
  return jobs.map((job, index) => {
    const [badge, badgeClass] = queueBadge(job, index);
    const title = job.album_title || `Import job ${job.id}`;
    const artist = job.artist_name || job.job_type || '';
    const message = queueMessage(job);
    const stages = job.preview_result && Array.isArray(job.preview_result.stage_chain)
      ? job.preview_result.stage_chain.join(' · ')
      : '';
    const meta = [
      job.job_type,
      job.preview_status ? `preview: ${job.preview_status}` : '',
      job.status ? `import: ${job.status}` : '',
    ].filter(Boolean).join(' · ');
    // Search-plan inspector button — Recents queue rows render the
    // button when the import job is bound to a pipeline request. Orphan
    // imports (job.request_id null) get nothing — the conditional in
    // renderSearchPlanButton handles the absent case.
    const spBtn = renderSearchPlanButton({ pipelineId: job.request_id });
    return `
      <div class="r-item" style="border-left-color:${queueBorderColor(job)}">
        <div class="p-top">
          <div>
            <div class="p-title">${esc(title)} <span class="badge ${badgeClass}">${esc(badge)}</span></div>
            <div class="p-artist">${esc(artist)}</div>
          </div>
          <div class="p-row-actions">${spBtn}<span style="font-size:0.75em;color:#666;">#${job.id}</span></div>
        </div>
        <div class="p-meta"><span>${esc(meta)}</span></div>
        ${message ? `<div class="p-meta"><span>${esc(message)}</span></div>` : ''}
        ${stages ? `<div class="p-meta"><span>${esc(stages)}</span></div>` : ''}
      </div>
    `;
  }).join('');
}

function downloadFileCounts(files) {
  const counts = { total: files.length, completed: 0, queued: 0, errored: 0 };
  for (const f of files) {
    const stateText = String(f.last_state || '');
    const size = Number(f.size || 0);
    const transferred = Number(f.bytes_transferred || 0);
    if (stateText.includes('Errored')) counts.errored += 1;
    if (stateText.includes('Queued')) counts.queued += 1;
    if (stateText.includes('Succeeded') || (size > 0 && transferred >= size)) {
      counts.completed += 1;
    }
  }
  return counts;
}

function downloadingSummary(item) {
  const active = item.active_download_state || {};
  const importJob = item.active_import_job || null;
  const files = Array.isArray(active.files) ? active.files : [];
  const counts = downloadFileCounts(files);
  const users = [...new Set(files.map(f => f.username).filter(Boolean))];
  const userSummary = users.length > 2
    ? `${users.slice(0, 2).join(', ')} +${users.length - 2}`
    : users.join(', ');
  const filetype = active.filetype || item.format || 'unknown';
  const progress = counts.total ? `${counts.completed}/${counts.total} files` : 'no file state';
  const stateParts = [];
  if (importJob) {
    const jobState = importJob.status === 'running' ? 'importing' : 'queued for import';
    stateParts.push(`${jobState} #${importJob.id}`);
  }
  if (counts.queued) stateParts.push(`${counts.queued} queued`);
  if (counts.errored) stateParts.push(`${counts.errored} errored`);
  if (active.last_progress_at) stateParts.push(`progress ${awstTime(active.last_progress_at)}`);
  if (active.enqueued_at) stateParts.push(`enqueued ${awstTime(active.enqueued_at)}`);

  return [filetype, progress, userSummary, ...stateParts].filter(Boolean).join(' · ');
}

function downloadingItemCounts(item) {
  const active = item.active_download_state || {};
  const files = Array.isArray(active.files) ? active.files : [];
  return downloadFileCounts(files);
}

function isWaitingForImport(item) {
  const active = item.active_download_state || {};
  if (item.active_import_job || active.processing_started_at) return true;

  const counts = downloadingItemCounts(item);
  return counts.total > 0
    && counts.completed >= counts.total
    && counts.queued === 0
    && counts.errored === 0;
}

function downloadingBadge(item) {
  const job = item.active_import_job || null;
  if (!job) return ['downloading', 'badge-downloading'];
  if (job.status === 'running') return ['importing', 'badge-force'];
  return ['import queued', 'badge-new'];
}

function downloadingBorderColor(item) {
  const job = item.active_import_job || null;
  if (!job) return '#1a3a5a';
  return job.status === 'running' ? '#36c' : '#1a4a2a';
}

function renderDownloadingHeader(activeCount, hiddenImportCount) {
  const activeLabel = `${activeCount} active Soulseek download${activeCount === 1 ? '' : 's'}`;
  if (!hiddenImportCount) return `<div class="r-date-header">${activeLabel}</div>`;
  const hiddenLabel = `${hiddenImportCount} complete/waiting for import hidden`;
  return `<div class="r-date-header">${activeLabel} · ${hiddenLabel}</div>`;
}

/**
 * Render current downloading pipeline rows for the Recents tab.
 * @param {Array<Object>} items
 * @returns {string}
 */
export function renderDownloadingItems(items) {
  if (items.length === 0) return '<div class="loading">No active downloads</div>';
  return items.map(item => {
    const date = item.updated_at ? awstDate(item.updated_at) : awstDate(item.created_at || '');
    const [badge, badgeClass] = downloadingBadge(item);
    // Downloading rows are pipeline_request rows — `item.id` is the
    // album_requests.id directly. Always render the inspector button.
    const spBtn = renderSearchPlanButton({ pipelineId: item.id });
    return `
      <div class="r-item" style="border-left-color:${downloadingBorderColor(item)}" onclick="window.toggleDetail('downloading-${item.id}', ${item.id})">
        <div class="p-top">
          <div>
            <div class="p-title">${esc(item.album_title)} <span class="badge ${badgeClass}">${badge}</span></div>
            <div class="p-artist">${esc(item.artist_name)}</div>
          </div>
          <div class="p-row-actions">${spBtn}<span style="font-size:0.75em;color:#666;">#${item.id}</span></div>
        </div>
        <div class="p-meta"><span>${esc(downloadingSummary(item))}</span></div>
        <div class="p-meta"><span>${date}</span>${item.last_outcome ? `<span>last: ${esc(item.last_outcome)}</span>` : ''}</div>
      </div>
      <div class="p-detail" id="downloading-${item.id}"></div>
    `;
  }).join('');
}

async function loadImportQueue() {
  const el = document.getElementById('recents-content');
  el.innerHTML = renderRecentsSubnav() + '<div class="loading">Loading...</div>';
  try {
    const r = await fetch(`${API}/api/import-jobs/timeline`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const jobs = data.jobs || [];
    el.innerHTML = renderRecentsSubnav()
      + renderImportQueueHeader(jobs, data.counts || {})
      + renderImportQueueItems(jobs);
  } catch (e) {
    el.innerHTML = renderRecentsSubnav() + '<div class="loading">Failed to load queue</div>';
  }
}

function renderImportQueueHeader(jobs, counts) {
  const queued = Number(counts.queued || 0);
  const running = Number(counts.running || 0);
  const activeTotal = queued + running || jobs.length;
  const shown = jobs.length;
  const windowText = activeTotal > shown
    ? `Showing ${shown} of ${activeTotal} active imports`
    : `${activeTotal} active import${activeTotal === 1 ? '' : 's'}`;
  const parts = [];
  if (queued) parts.push(`${queued} queued`);
  if (running) parts.push(`${running} running`);
  return `<div class="r-date-header">${[windowText, ...parts].join(' · ')}</div>`;
}

async function loadDownloading() {
  const el = document.getElementById('recents-content');
  el.innerHTML = renderRecentsSubnav() + '<div class="loading">Loading...</div>';
  try {
    let r = await fetch(`${API}/api/pipeline/downloading`);
    if (r.status === 404) r = await fetch(`${API}/api/pipeline/all`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const items = data.downloading || [];
    const activeDownloads = items.filter(item => !isWaitingForImport(item));
    const hiddenImportCount = items.length - activeDownloads.length;
    el.innerHTML = renderRecentsSubnav()
      + renderDownloadingHeader(activeDownloads.length, hiddenImportCount)
      + renderDownloadingItems(activeDownloads);
  } catch (e) {
    el.innerHTML = renderRecentsSubnav() + '<div class="loading">Failed to load downloads</div>';
  }
}

function formatMatchRate(value) {
  if (value == null || Number.isNaN(Number(value))) return '0.00';
  const rate = Number(value);
  return rate >= 10 ? rate.toFixed(1) : rate.toFixed(2);
}

function hasMatchRates(counts) {
  return counts
    && counts.matches_per_hour_6h != null
    && counts.matches_per_hour_24h != null;
}

function matchRatesFromDashboardWindows(windows) {
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
  return rates;
}

async function loadRecentsMatchRatesFallback() {
  if (hasMatchRates(state.recentsCounts)) return;
  try {
    const r = await fetch(`${API}/api/pipeline/dashboard`);
    if (!r.ok) return;
    const data = await r.json();
    state.recentsCounts = {
      ...state.recentsCounts,
      ...matchRatesFromDashboardWindows(data.searches?.windows || []),
    };
  } catch (e) {
    // Keep recents usable when the dashboard endpoint is unavailable.
  }
}

function renderRecentsCounts() {
  return `<div class="recents-counts">
    <div class="count ${state.recentsFilter === 'all' ? 'active' : ''}" onclick="window.setRecentsFilter('all')">
      <div class="count-num">${state.recentsCounts.all}</div><div class="count-label">all</div></div>
    <div class="count ${state.recentsFilter === 'imported' ? 'active' : ''}" onclick="window.setRecentsFilter('imported')">
      <div class="count-num">${state.recentsCounts.imported}</div><div class="count-label">imported</div></div>
    <div class="count ${state.recentsFilter === 'rejected' ? 'active' : ''}" onclick="window.setRecentsFilter('rejected')">
      <div class="count-num">${state.recentsCounts.rejected}</div><div class="count-label">rejected</div></div>
  </div>`;
}

/**
 * Load recents from API and render.
 * @returns {Promise<void>}
 */
export async function loadRecents() {
  const el = document.getElementById('recents-content');
  if (state.recentsSub === 'queue') {
    await loadImportQueue();
    return;
  }
  if (state.recentsSub === 'downloading') {
    await loadDownloading();
    return;
  }
  el.innerHTML = renderRecentsSubnav() + '<div class="loading">Loading...</div>';
  try {
    const filterParam = state.recentsFilter === 'all' ? '' : `?outcome=${state.recentsFilter}`;
    const r = await fetch(`${API}/api/pipeline/log${filterParam}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const items = data.log || [];
    if (data.counts) state.recentsCounts = data.counts;
    if (state.recentsFilter === 'all') await loadRecentsMatchRatesFallback();

    let html = renderRecentsSubnav() + renderRecentsCounts();
    html += renderRecentsItems(
      items,
      state.recentsFilter === 'all' ? state.recentsCounts : null,
    );
    el.innerHTML = html;
  } catch (e) { el.innerHTML = renderRecentsSubnav() + '<div class="loading">Failed to load log</div>'; }
}

export const __test__ = {
  hasMatchRates,
  matchRatesFromDashboardWindows,
  renderDownloadingItems,
  renderImportQueueItems,
  renderRecentsCounts,
  renderRecentsDateHeader,
  renderRecentsSubnav,
  renderRecentsItems,
  setRecentsSub,
};
