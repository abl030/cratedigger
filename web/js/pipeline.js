// @ts-check
import { state, API, toast } from './state.js';
import { esc, qualityLabel, renderForensicBlock } from './util.js';
import { renderDownloadHistoryItem } from './history.js';
import {
  renderBeetsTrackRow, renderExpectedTrackRow, renderDetailRow, renderExternalLinkRow, toggleExpand,
} from './render_primitives.js';
import { renderBadRipButton, renderReplaceButton } from './release_actions.js';
import { renderSearchPlanDetail } from './search_plan.js';
import { loadLongTail, renderLongTailBody } from './long_tail.js';
import { restoreLongTailConsoles } from './long_tail_console.js';
import { renderPipelineDashboard as renderDashboardCards } from './pipeline_dashboard.js';

const VISIBLE_HISTORY_ATTEMPTS = 10;

/**
 * Render the evidence-heavy sections of a request detail panel.
 *
 * The newest attempts stay visible because they explain the current outcome.
 * Older attempts and track inventories are useful audit context, but neither
 * should push the decision story multiple screens below the click target.
 *
 * @param {Array<Object>} history
 * @param {Array<Object>} beetsTracks
 * @param {Array<Object>} expectedTracks
 * @returns {string}
 */
export function renderRequestEvidenceSections(history, beetsTracks, expectedTracks) {
  let html = '';
  if (history.length > 0) {
    const visible = history.slice(0, VISIBLE_HISTORY_ATTEMPTS);
    const older = history.slice(VISIBLE_HISTORY_ATTEMPTS);
    html += `<div class="p-history"><div class="p-detail-label" style="margin-bottom:4px;">Download History (${history.length})</div>`;
    html += visible.map(renderDownloadHistoryItem).join('');
    if (older.length > 0) {
      const noun = older.length === 1 ? 'attempt' : 'attempts';
      html += `<details class="p-history-older"><summary>Show ${older.length} older ${noun}</summary>${older.map(renderDownloadHistoryItem).join('')}</details>`;
    }
    html += '</div>';
  }

  if (beetsTracks.length > 0) {
    html += `<details class="p-tracks"><summary class="p-detail-label">In Library (${beetsTracks.length} tracks)</summary>${beetsTracks.map(renderBeetsTrackRow).join('')}</details>`;
  } else if (expectedTracks.length > 0) {
    html += `<details class="p-tracks"><summary class="p-detail-label">Expected Tracks from MusicBrainz (${expectedTracks.length})</summary>${expectedTracks.map(renderExpectedTrackRow).join('')}</details>`;
  }
  return html;
}

/**
 * Load pipeline data from API and render.
 * @returns {Promise<void>}
 */
export async function loadPipeline() {
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
    // clobber it with a dashboard paint.
    const ctx = state.searchPlanDetailContext;
    if (ctx && ctx.requestId) {
      await renderSearchPlanDetail(ctx.requestId);
    }
    return;
  }
  state.pipelineView = 'dashboard';
  await loadPipelineDashboard();
}

/**
 * Switch between the operational Pipeline subviews — dashboard, long-tail,
 * or search-plan-detail. The third value is the per-request inspector,
 * dispatched into `#pipeline-content` via `renderSearchPlanDetail`.
 * Unknown values fall back to `'dashboard'`.
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
  state.pipelineView = 'dashboard';
  loadPipelineDashboard();
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
 * Render the pipeline view from cached data.
 *
 * Dispatches on `state.pipelineView`:
 *   * `'dashboard'` (default) → metrics dashboard
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
      // Defensive: subview entered without a context. Fall back to the
      // dashboard so the operator is never stranded.
      state.pipelineView = 'dashboard';
      void loadPipelineDashboard();
    }
    return;
  }
  state.pipelineView = 'dashboard';
  void loadPipelineDashboard();
}

function renderPipelineNav() {
  const refreshAction = state.pipelineView === 'dashboard'
    ? 'window.loadPipelineDashboard()'
    : state.pipelineView === 'long-tail'
      ? 'window.loadLongTail()'
      : 'window.loadPipeline()';

  return `
    <div class="pipeline-subtabs">
      <button class="p-btn ${state.pipelineView === 'dashboard' ? 'active-status' : ''}" onclick="window.setPipelineView('dashboard')">Dashboard</button>
      <button class="p-btn ${state.pipelineView === 'long-tail' ? 'active-status' : ''}" onclick="window.setPipelineView('long-tail')">Long Tail</button>
      <button class="p-btn subtab-refresh" onclick="${refreshAction}">Refresh</button>
    </div>
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
  await toggleExpand(el, async (target) => {
    const r = await fetch(`${API}/api/pipeline/${id}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const req = data.request;
    const tracks = data.tracks || [];
    const history = data.history || [];

    let html = '';
    // External link (MB or Discogs)
    html += renderExternalLinkRow(req.mb_release_id || '');
    if (req.imported_path) {
      html += renderDetailRow('Imported to', esc(req.imported_path), { valueStyle: 'font-size:0.9em;' });
    }

    const beetsTracks = data.beets_tracks || [];
    html += renderCurrentQualityRow(req, beetsTracks);

    html += renderRequestEvidenceSections(history, beetsTracks, tracks);

    // Search forensics (last_search) — variant tag + top-3 candidates from
    // the most recent search_log row. Collapsed by default; click expands.
    html += renderForensicBlock(/** @type {any} */ (data.last_search));

    // Status change buttons
    html += `<div class="p-actions">
      <span class="p-detail-label" style="line-height:28px;">Status:</span>
      <button class="p-btn ${req.status === 'wanted' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.updateStatus(${id}, 'wanted')">wanted</button>
      <button class="p-btn ${req.status === 'imported' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.updateStatus(${id}, 'imported')">imported</button>
      <button class="p-btn ${req.status === 'unsearchable' ? 'active-status' : ''}" onclick="event.stopPropagation(); window.updateStatus(${id}, 'unsearchable')">unsearchable</button>`;
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

    target.innerHTML = html;
  }, { errorText: 'Failed to load details' });
}

/**
 * Render the current on-disk Quality row from positive beets track bitrates.
 * The average drives the nominal VBR label; minimum remains floor/audit data.
 * @param {Object} req
 * @param {Array<Object>} beetsTracks
 * @returns {string}
 */
function renderCurrentQualityRow(req, beetsTracks) {
  if (beetsTracks.length === 0) return '';
  const positiveBitrates = beetsTracks
    .map(t => Number(t.bitrate))
    .filter(bitrate => Number.isFinite(bitrate) && bitrate > 0);
  const avgBrKbps = positiveBitrates.length > 0
    ? Math.floor(
      positiveBitrates.reduce((total, bitrate) => total + bitrate, 0)
        / positiveBitrates.length / 1000,
    )
    : 0;
  const fmt = beetsTracks[0]?.format || '';
  const nominal = avgBrKbps ? qualityLabel(fmt, avgBrKbps) : fmt;
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
  return renderDetailRow('Quality', qualitySummary);
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
  renderPipelineNav,
  renderCurrentQualityRow,
  renderRequestEvidenceSections,
};
