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

/**
 * Load recents from API and render.
 * @returns {Promise<void>}
 */
export async function loadRecents() {
  const el = document.getElementById('recents-content');
  el.innerHTML = '<div class="loading">Loading...</div>';
  try {
    const filterParam = state.recentsFilter === 'all' ? '' : `?outcome=${state.recentsFilter}`;
    const r = await fetch(`${API}/api/pipeline/log${filterParam}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const items = data.log || [];
    if (data.counts) state.recentsCounts = data.counts;

    let html = `<div style="display:flex;gap:8px;margin-bottom:8px;">
      <div class="count ${state.recentsFilter === 'all' ? 'active' : ''}" onclick="window.setRecentsFilter('all')">
        <div class="count-num">${state.recentsCounts.all}</div><div class="count-label">all</div></div>
      <div class="count ${state.recentsFilter === 'imported' ? 'active' : ''}" onclick="window.setRecentsFilter('imported')">
        <div class="count-num">${state.recentsCounts.imported}</div><div class="count-label">imported</div></div>
      <div class="count ${state.recentsFilter === 'rejected' ? 'active' : ''}" onclick="window.setRecentsFilter('rejected')">
        <div class="count-num">${state.recentsCounts.rejected}</div><div class="count-label">rejected</div></div>
    </div>`;
    html += renderRecentsItems(items);
    el.innerHTML = html;
  } catch (e) { el.innerHTML = '<div class="loading">Failed to load log</div>'; }
}
