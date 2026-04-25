// @ts-check
import { state, API } from './state.js';
import { awstDate, awstTime, esc } from './util.js';
import { toggleDetail } from './pipeline.js';

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
  return `<div style="display:flex;gap:2px;margin-bottom:12px;">
    <button class="p-btn ${state.recentsSub === 'history' ? 'active-status' : ''}" onclick="window.setRecentsSub('history')">History</button>
    <button class="p-btn ${state.recentsSub === 'queue' ? 'active-status' : ''}" onclick="window.setRecentsSub('queue')">Queue</button>
  </div>`;
}

/**
 * Render recents items grouped by date.
 * @param {Array<Object>} items
 * @returns {string} HTML string
 */
export function renderRecentsItems(items) {
  if (items.length === 0) return '<div class="loading">No matching entries</div>';

  // Group by date (AWST)
  const byDate = {};
  for (const item of items) {
    const date = awstDate(item.created_at || '');
    if (!byDate[date]) byDate[date] = [];
    byDate[date].push(item);
  }
  const dates = Object.keys(byDate).sort().reverse();

  return dates.map(date => `
    <div class="r-date-header">${date}</div>
    ${byDate[date].map(item => {
      const time = awstTime(item.created_at || '');
      const badge = item.badge || '';
      const badgeClass = item.badge_class || '';
      const borderColor = item.border_color || '#444';
      const summary = item.summary || '';

      // Issue #130: a `disambiguation_failure` chip surfaces post-import
      // `beet move` errors that leave the album in beets at a stale path.
      // Rendered inline next to the main badge; hover for detail.
      const disambigChip = item.disambiguation_failure
        ? `<span class="badge badge-warn" title="${esc(item.disambiguation_detail || '')}">disambig: ${esc(item.disambiguation_failure)}</span>`
        : '';

      return `
        <div class="r-item" style="border-left-color:${borderColor}" onclick="window.toggleDetail('dl-${item.id}', ${item.request_id})">
          <div class="p-top">
            <div>
              <div class="p-title">${esc(item.album_title)} <span class="badge ${badgeClass}">${badge}</span>${disambigChip}</div>
              <div class="p-artist">${esc(item.artist_name)}</div>
            </div>
            <div style="font-size:0.75em;color:#666;">${time}</div>
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

function queueBadge(job, index) {
  if (job.status === 'completed') return ['completed', 'badge-new'];
  if (job.status === 'failed') return ['failed', 'badge-failed'];
  if (job.status === 'running') return ['importing', 'badge-force'];
  if (job.preview_status === 'would_import') {
    return [index === 0 ? 'next import' : 'importable', 'badge-new'];
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
  if (job.preview_status === 'would_import' || job.status === 'completed') return '#1a4a2a';
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
    return `
      <div class="r-item" style="border-left-color:${queueBorderColor(job)}">
        <div class="p-top">
          <div>
            <div class="p-title">${esc(title)} <span class="badge ${badgeClass}">${esc(badge)}</span></div>
            <div class="p-artist">${esc(artist)}</div>
          </div>
          <div style="font-size:0.75em;color:#666;">#${job.id}</div>
        </div>
        <div class="p-meta"><span>${esc(meta)}</span></div>
        ${message ? `<div class="p-meta"><span>${esc(message)}</span></div>` : ''}
        ${stages ? `<div class="p-meta"><span>${esc(stages)}</span></div>` : ''}
      </div>
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
    el.innerHTML = renderRecentsSubnav() + renderImportQueueItems(data.jobs || []);
  } catch (e) {
    el.innerHTML = renderRecentsSubnav() + '<div class="loading">Failed to load queue</div>';
  }
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
  el.innerHTML = renderRecentsSubnav() + '<div class="loading">Loading...</div>';
  try {
    const filterParam = state.recentsFilter === 'all' ? '' : `?outcome=${state.recentsFilter}`;
    const r = await fetch(`${API}/api/pipeline/log${filterParam}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const items = data.log || [];
    if (data.counts) state.recentsCounts = data.counts;

    let html = renderRecentsSubnav() + `<div style="display:flex;gap:8px;margin-bottom:8px;">
      <div class="count ${state.recentsFilter === 'all' ? 'active' : ''}" onclick="window.setRecentsFilter('all')">
        <div class="count-num">${state.recentsCounts.all}</div><div class="count-label">all</div></div>
      <div class="count ${state.recentsFilter === 'imported' ? 'active' : ''}" onclick="window.setRecentsFilter('imported')">
        <div class="count-num">${state.recentsCounts.imported}</div><div class="count-label">imported</div></div>
      <div class="count ${state.recentsFilter === 'rejected' ? 'active' : ''}" onclick="window.setRecentsFilter('rejected')">
        <div class="count-num">${state.recentsCounts.rejected}</div><div class="count-label">rejected</div></div>
    </div>`;
    html += renderRecentsItems(items);
    el.innerHTML = html;
  } catch (e) { el.innerHTML = renderRecentsSubnav() + '<div class="loading">Failed to load log</div>'; }
}

export const __test__ = {
  renderImportQueueItems,
  renderRecentsItems,
  setRecentsSub,
};
