// @ts-check
import { state, API } from './state.js';
import { awstDate, awstTime, esc } from './util.js';
import { toggleDetail } from './pipeline.js';
import { renderEvidenceStrip } from './history.js';
import { consumePendingScrollRestore, renderSearchPlanButton } from './search_plan.js';
import { processingOwnerPresentation } from './release_action_state.js';
import { renderConvergencePrompt } from './convergence.js';
import { cdRipProofPresentation } from './cd_rip_proof.js';

const RECENTS_HISTORY_LIMIT = 500;

/**
 * Set the recents filter and reload.
 * @param {string} f
 */
export function setRecentsFilter(f) {
  state.recentsFilter = f;
  loadRecents();
}

/**
 * Switch Recents between history, active acquisition, and importer work.
 * @param {string} sub
 */
export function setRecentsSub(sub) {
  state.recentsSub = sub;
  loadRecents();
}

/**
 * Render the Recents sub-navigation (acquisition vs import).
 * @returns {string}
 */
export function renderRecentsSubnav() {
  return `<div class="pipeline-subtabs">
    <button class="p-btn ${state.recentsSub === 'history' ? 'active-status' : ''}" onclick="window.setRecentsSub('history')">History</button>
    <button class="p-btn ${state.recentsSub === 'acquisition' ? 'active-status' : ''}" onclick="window.setRecentsSub('acquisition')">Acquisition</button>
    <button class="p-btn ${state.recentsSub === 'imports' ? 'active-status' : ''}" onclick="window.setRecentsSub('imports')">Imports</button>
    <button class="p-btn subtab-refresh" onclick="window.loadRecents()">Refresh</button>
  </div>`;
}

/**
 * The pipeline-log API URL for the current Recents filter.
 * @returns {string}
 */
export function recentsLogUrl() {
  const params = new URLSearchParams();
  if (state.recentsFilter !== 'all') params.set('outcome', state.recentsFilter);
  params.set('limit', String(RECENTS_HISTORY_LIMIT));
  return `${API}/api/pipeline/log?${params.toString()}`;
}

/**
 * Flatten a triage summary into one operator-readable label.
 * @param {string} summary
 * @returns {string}
 */
export function triageLabelText(summary) {
  const normalized = String(summary || '').replace(/:/g, '').replace(/\s+/g, ' ').trim();
  return normalized ? `triage - ${normalized}` : '';
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
      const isHaveAnalysisError = item.outcome === 'have_analysis_error';
      const failureCategory = item.failure_category
        ? String(item.failure_category).replace(/_/g, ' ')
        : 'unknown analyser failure';
      const haveAnalysisDetails = isHaveAnalysisError
        ? `<div class="p-meta"><span>failure ${esc(failureCategory)}</span></div>
          <div class="p-meta"><span>HAVE ${esc(item.installed_path || 'unknown path')}</span></div>
          <div class="p-meta"><span>candidate ${esc(item.candidate_reference || 'unknown reference')}</span></div>
          <div class="p-meta"><span>${esc(item.analysis_error || 'No analyser detail recorded')}</span></div>`
        : '';

      // Issue #130: a `disambiguation_failure` chip surfaces post-import
      // `beet move` errors that leave the album in beets at a stale path.
      // Rendered inline next to the main badge; hover for detail.
      const disambigChip = item.disambiguation_failure
        ? `<span class="badge badge-warn" title="${esc(item.disambiguation_detail || '')}">disambig: ${esc(item.disambiguation_failure)}</span>`
        : '';
      const badExtChip = badExtensions.length
        ? `<span class="badge badge-warn" title="${esc(badExtensions.join(', '))}">bad ext: ${badExtensions.length}</span>`
        : '';
      // Issue #1178 (post-correction, "surface-not-reject"): a render-time
      // fact, never a pipeline verdict — the server has already decided
      // this import stands; the chip only asks the operator to look.
      const trackLengthWarningChip = item.track_length_warning
        ? `<span class="badge badge-warn" title="${esc(item.track_length_warning)}">track length</span>`
        : '';
      const triageLabel = triageSummary && !String(badge).startsWith('Triaged')
        ? `<span class="recents-triage-label" title="${esc(triageDetail)}">${esc(triageLabelText(triageSummary))}</span>`
        : '';
      const badgeDetail = triageDetail || (isHaveAnalysisError ? item.analysis_error : '');
      const badgeTitle = badgeDetail ? ` title="${esc(badgeDetail)}"` : '';
      const cdRipProof = cdRipProofPresentation(item.cd_rip_verification);
      const cdRipProofBadge = cdRipProof
        ? `<span class="badge badge-verified badge-rank-lossless badge-cd-proof" title="exact CD-rip provider match">${esc(cdRipProof.text)}</span>`
        : '';

      // Search-plan inspector button — Recents rows always render the
      // button. Use the request_id (the album_requests.id) since the
      // download_log row's id (item.id) is the wrong cursor space.
      const spBtn = renderSearchPlanButton({ pipelineId: item.request_id });

      // Glance-able IN/HAVE evidence strip (issue #575) — same numbers
      // the detail grid shows, compressed to one line. Empty for rows
      // with no measurements (download-phase failures).
      const evidence = renderEvidenceStrip(item);
      const convergencePrompt = renderConvergencePrompt(
        item.convergence, item.request_status, 'recents',
      );

      return `
        <div class="r-item" style="border-left-color:${borderColor}" onclick="window.toggleDetail('dl-${item.id}', ${item.request_id})">
          <div class="p-top">
            <div>
              <div class="p-title">${esc(item.album_title)} <span class="badge ${badgeClass}"${badgeTitle}>${badge}</span>${cdRipProofBadge}${disambigChip}${badExtChip}${trackLengthWarningChip}</div>
              <div class="p-artist">${esc(item.artist_name)}</div>
            </div>
            <div class="p-row-actions">${spBtn}<span style="font-size:0.75em;color:#666;">${time}</span></div>
          </div>
          ${haveAnalysisDetails}
          ${evidence ? `<div class="p-meta">${evidence}</div>` : ''}
          <div class="p-meta">
            ${triageLabel}
            <span class="r-summary" title="${esc(summary)}">${esc(summary)}</span>
          </div>
          ${convergencePrompt}
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

function jobCleanupChip(job) {
  const cleanup = job && job.result && typeof job.result === 'object'
    ? job.result.cleanup
    : null;
  if (!cleanup || typeof cleanup !== 'object') return '';
  if (cleanup.outcome === 'deleted' && cleanup.success) {
    const path = cleanup.deleted_path || cleanup.resolved_path || '';
    return `<span class="badge badge-library" title="${esc(path)}">source deleted</span>`;
  }
  if (cleanup.skipped || cleanup.outcome) {
    const reason = cleanup.reason || cleanup.error || cleanup.outcome || 'cleanup skipped';
    return `<span class="badge badge-warn" title="${esc(reason)}">cleanup: ${esc(cleanup.outcome || 'skipped')}</span>`;
  }
  return '';
}

/**
 * Render active importer timeline rows.
 * @param {Array<Object>} jobs
 * @returns {string}
 */
export function renderImportItems(jobs) {
  if (jobs.length === 0) return '<div class="loading">No active imports</div>';
  return jobs.map((job) => {
    const badge = job.badge || '';
    const badgeClass = job.badge_class || '';
    const title = job.album_title || `Import job ${job.id}`;
    const artist = job.artist_name || job.job_type || '';
    const message = job.summary || '';
    const stages = job.preview_result && Array.isArray(job.preview_result.stage_chain)
      ? job.preview_result.stage_chain.join(' · ')
      : '';
    const meta = [
      job.job_type,
      job.preview_status ? `preview: ${job.preview_status}` : '',
      job.status ? `import: ${job.status}` : '',
    ].filter(Boolean).join(' · ');
    const cleanupChip = jobCleanupChip(job);
    // Search-plan inspector button — Recents Imports rows render the
    // button when the import job is bound to a pipeline request. Orphan
    // imports (job.request_id null) get nothing — the conditional in
    // renderSearchPlanButton handles the absent case.
    const spBtn = renderSearchPlanButton({ pipelineId: job.request_id });
    return `
      <div class="r-item" style="border-left-color:${esc(job.border_color || '#444')}">
        <div class="p-top">
          <div>
            <div class="p-title">${esc(title)} <span class="badge ${badgeClass}">${esc(badge)}</span>${cleanupChip}</div>
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

function acquisitionSummary(item) {
  if (isYoutubeIngestItem(item)) {
    const meta = item.youtube_metadata || {};
    const parts = ['YouTube'];
    if (meta.expected_track_count) parts.push(`${meta.expected_track_count} tracks`);
    if (meta.browse_id) parts.push(`browse ${meta.browse_id}`);
    if (item.created_at) parts.push(`accepted ${awstTime(item.created_at)}`);
    return parts.join(' · ');
  }
  const processing = processingOwnerPresentation(
    item.status || item.pipeline_status || null,
    item.processing_owner || null,
  );
  if (processing) {
    return processing.jobId
      ? `${processing.label} · job #${processing.jobId}`
      : processing.label;
  }
  const active = item.active_download_state || {};
  const files = Array.isArray(active.files) ? active.files : [];
  const counts = downloadFileCounts(files);
  const users = [...new Set(files.map(f => f.username).filter(Boolean))];
  const userSummary = users.length > 2
    ? `${users.slice(0, 2).join(', ')} +${users.length - 2}`
    : users.join(', ');
  const filetype = active.filetype || item.format || 'unknown';
  const progress = counts.total ? `${counts.completed}/${counts.total} files` : 'no file state';
  const stateParts = [];
  if (counts.queued) stateParts.push(`${counts.queued} queued`);
  if (counts.errored) stateParts.push(`${counts.errored} errored`);
  if (active.last_progress_at) stateParts.push(`progress ${awstTime(active.last_progress_at)}`);
  if (active.enqueued_at) stateParts.push(`enqueued ${awstTime(active.enqueued_at)}`);

  return [filetype, progress, userSummary, ...stateParts].filter(Boolean).join(' · ');
}

function acquisitionBadge(item) {
  if (isYoutubeIngestItem(item)) return ['youtube ingest', 'badge-new'];
  const processing = processingOwnerPresentation(
    item.status || item.pipeline_status || null,
    item.processing_owner || null,
  );
  if (processing) return [processing.label, processing.badgeClass];
  return ['downloading', 'badge-downloading'];
}

function acquisitionBorderColor(item) {
  if (isYoutubeIngestItem(item)) return '#7a5a00';
  const processing = processingOwnerPresentation(
    item.status || item.pipeline_status || null,
    item.processing_owner || null,
  );
  if (!processing) return '#1a3a5a';
  if (processing.badgeClass === 'badge-failed') return '#a33';
  return processing.label === 'importing' ? '#36c' : '#1a4a2a';
}

function isYoutubeIngestItem(item) {
  return item && item.download_kind === 'youtube_ingest';
}

/**
 * Coerce a YouTube-ingest row into the shape the acquisition list renders.
 * @param {any} row
 * @returns {any}
 */
export function normalizeYoutubeIngestItem(row) {
  return {
    ...row,
    id: row.request_id,
    download_kind: 'youtube_ingest',
    active_download_state: null,
    processing_owner: null,
  };
}

function renderAcquisitionHeader(activeCount) {
  const noun = activeCount === 1 ? 'acquisition' : 'acquisitions';
  return `<div class="r-date-header">${activeCount} active ${noun}</div>`;
}

/**
 * Render current downloader, processor, and YouTube acquisition rows.
 * @param {Array<Object>} items
 * @returns {string}
 */
export function renderAcquisitionItems(items) {
  if (items.length === 0) return '<div class="loading">No active acquisitions</div>';
  return items.map(item => {
    const date = item.updated_at ? awstDate(item.updated_at) : awstDate(item.created_at || '');
    const [badge, badgeClass] = acquisitionBadge(item);
    const detailKey = isYoutubeIngestItem(item)
      ? `youtube-${item.download_log_id}`
      : String(item.id);
    // Request rows carry album_requests.id directly. YouTube rows retain the
    // request id only as navigation context and never gain processing owner.
    const spBtn = renderSearchPlanButton({ pipelineId: item.id });
    const processing = processingOwnerPresentation(
      item.status || item.pipeline_status || null,
      item.processing_owner || null,
    );
    const ownerLink = processing?.recoveryTarget && processing.jobId
      ? `<a href="${esc(processing.recoveryTarget)}" class="processing-owner-link" onclick="event.stopPropagation()">job #${processing.jobId}</a>`
      : '';
    const idText = isYoutubeIngestItem(item)
      ? `#${item.id} · YT #${item.download_log_id}`
      : `#${item.id}`;
    return `
      <div class="r-item" style="border-left-color:${acquisitionBorderColor(item)}" onclick="window.toggleDetail('acquisition-${detailKey}', ${item.id})">
        <div class="p-top">
          <div>
            <div class="p-title">${esc(item.album_title)} <span class="badge ${badgeClass}">${esc(badge)}</span></div>
            <div class="p-artist">${esc(item.artist_name)}</div>
          </div>
          <div class="p-row-actions">${ownerLink}${spBtn}<span style="font-size:0.75em;color:#666;">${idText}</span></div>
        </div>
        <div class="p-meta"><span>${esc(acquisitionSummary(item))}</span></div>
        <div class="p-meta"><span>${date}</span>${item.last_outcome ? `<span>last: ${esc(item.last_outcome)}</span>` : ''}</div>
      </div>
      <div class="p-detail" id="acquisition-${detailKey}"></div>
    `;
  }).join('');
}

async function loadImports() {
  const el = document.getElementById('recents-content');
  el.innerHTML = renderRecentsSubnav() + '<div class="loading">Loading...</div>';
  try {
    const r = await fetch(`${API}/api/import-jobs/timeline`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const jobs = data.jobs || [];
    el.innerHTML = renderRecentsSubnav()
      + renderImportsHeader(jobs, data.counts || {})
      + renderImportItems(jobs);
  } catch (e) {
    el.innerHTML = renderRecentsSubnav() + '<div class="loading">Failed to load imports</div>';
  }
}

function renderImportsHeader(jobs, counts) {
  const queued = Number(counts.queued || 0);
  const running = Number(counts.running || 0);
  const recovery = Number(counts.recovery_required || 0);
  const activeTotal = queued + running + recovery || jobs.length;
  const shown = jobs.length;
  const windowText = activeTotal > shown
    ? `Showing ${shown} of ${activeTotal} active imports`
    : `${activeTotal} active import${activeTotal === 1 ? '' : 's'}`;
  const parts = [];
  if (queued) parts.push(`${queued} queued`);
  if (running) parts.push(`${running} running`);
  if (recovery) parts.push(`${recovery} recovery required`);
  return `<div class="r-date-header">${[windowText, ...parts].join(' · ')}</div>`;
}

async function loadAcquisition() {
  const el = document.getElementById('recents-content');
  el.innerHTML = renderRecentsSubnav() + '<div class="loading">Loading...</div>';
  try {
    const r = await fetch(`${API}/api/pipeline/acquisition`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const items = data.acquisition || [];
    const youtubeItems = (data.youtube_ingest || []).map(normalizeYoutubeIngestItem);
    el.innerHTML = renderRecentsSubnav()
      + renderAcquisitionHeader(items.length + youtubeItems.length)
      + renderAcquisitionItems([...youtubeItems, ...items]);
  } catch (e) {
    el.innerHTML = renderRecentsSubnav() + '<div class="loading">Failed to load acquisitions</div>';
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

/**
 * Derive Recents' match-rate figures from the dashboard's search windows.
 * @param {any[]} windows
 * @returns {any}
 */
export function matchRatesFromDashboardWindows(windows) {
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

/**
 * Render the Recents counts strip from cached dashboard state.
 * @returns {string}
 */
export function renderRecentsCounts() {
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
  // The `finally` guarantees exactly one consume per call regardless of
  // which branch below runs or returns early — the search-plan back
  // button's completion boundary (search_plan.js::closeSearchPlanDetail)
  // relies on this being the true end of the Recents tab's render, not
  // a fixed delay after it starts.
  try {
    const el = document.getElementById('recents-content');
    if (state.recentsSub === 'imports') {
      await loadImports();
      return;
    }
    if (state.recentsSub === 'acquisition') {
      await loadAcquisition();
      return;
    }
    el.innerHTML = renderRecentsSubnav() + '<div class="loading">Loading...</div>';
    try {
      const r = await fetch(recentsLogUrl());
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
  } finally {
    consumePendingScrollRestore();
  }
}
