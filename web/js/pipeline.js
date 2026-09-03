// @ts-check
import { state, API, toast } from './state.js';
import { esc, qualityLabel, renderForensicBlock } from './util.js';
import { renderDownloadHistoryItem } from './history.js';
import {
  renderBeetsTrackRow, renderExpectedTrackRow, renderDetailRow, renderExternalLinkRow, toggleExpand,
} from './render_primitives.js';
import {
  renderBadRipButton,
  renderProcessingLockedControl,
  renderReplaceButton,
} from './release_actions.js';
import {
  buildReleaseActionState,
  handleProcessingLockedConflict,
  processingOwnerPresentation,
} from './release_action_state.js';
import { consumePendingScrollRestore, renderSearchPlanDetail } from './search_plan.js';
import { loadLongTail, renderLongTailBody } from './long_tail.js';
import { restoreLongTailConsoles } from './long_tail_console.js';
import {
  renderPipelineDashboard as renderDashboardCards,
  renderRetagDivergenceAlbumRowInner,
} from './pipeline_dashboard.js';
import {
  qualityToneClass,
  spectralGradeClass,
  spectralGradeIsAdmissible,
  spectralGradeLabel,
  spectralWithheldPresentation,
} from './quality_palette.js';

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
  // The `finally` guarantees exactly one consume per call regardless of
  // which branch below runs or returns early — the search-plan back
  // button's completion boundary (search_plan.js::closeSearchPlanDetail)
  // relies on this being the true end of the Pipeline tab's render, not
  // a fixed delay after it starts.
  try {
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
  } finally {
    consumePendingScrollRestore();
  }
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

/**
 * Render the dashboard cards from cached state into #pipeline-content.
 * The composer takes payload + target element as parameters (so its card
 * wiring is Node-testable); this is the one production call site that
 * supplies both.
 */
function renderDashboard() {
  renderDashboardCards(
    renderPipelineNav(),
    state.pipelineDashboardData,
    document.getElementById('pipeline-content'),
  );
}

export function toggleCoverageMatchGraph(scope = 'hourly') {
  if (scope === 'daily') {
    state.pipelineDailyMatchGraphOpen = !state.pipelineDailyMatchGraphOpen;
  } else {
    state.pipelineHourlyMatchGraphOpen = !state.pipelineHourlyMatchGraphOpen;
    state.pipelineMatchGraphOpen = state.pipelineHourlyMatchGraphOpen;
  }
  renderDashboard();
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
    renderDashboard();
  } catch (e) {
    el.innerHTML = `${renderPipelineNav()}<div class="loading">Failed to load dashboard</div>`;
  }
}

/**
 * Operator action (#1089): rekey an `imported` request's ledger onto the
 * MusicBrainz merge survivor Beets already holds. Called from the Disk
 * Coverage drift panel's "Follow MB merge" button
 * (`pipeline_dashboard.js::renderDriftRow`). Request-ledger-only — never
 * mutates Beets.
 *
 * On success (`outcome: "rekeyed"`) the whole dashboard reloads, so the row
 * disappears once the request is no longer off-disk. On refusal the row's
 * own inline note shows the outcome and message (never just a toast, which
 * an operator working through several drift rows could miss) and the
 * button re-arms for retry.
 *
 * @param {number} requestId
 * @param {HTMLButtonElement} btn
 * @returns {Promise<void>}
 */
export async function mergeRekeyRequest(requestId, btn) {
  const note = document.getElementById(`drift-note-${requestId}`);
  btn.disabled = true;
  btn.textContent = 'Rekeying...';
  try {
    const r = await fetch(`${API}/api/pipeline/${requestId}/merge-rekey`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: '{}',
    });
    const data = await r.json();
    if (r.ok && data.outcome === 'rekeyed') {
      toast(`Request #${requestId} rekeyed to ${data.new_release_id}`);
      void loadPipelineDashboard();
      return;
    }
    btn.disabled = false;
    btn.textContent = 'Follow MB merge';
    const message = `${data.outcome || 'refused'}: ${data.error_message || data.error || 'merge rekey refused'}`;
    if (note) {
      note.textContent = message;
      note.className = 'drift-row-note metric-bad';
    }
    toast(message, true);
  } catch (_e) {
    btn.disabled = false;
    btn.textContent = 'Follow MB merge';
    if (note) {
      note.textContent = 'Merge-rekey request failed';
      note.className = 'drift-row-note metric-bad';
    }
    toast('Merge-rekey request failed', true);
  }
}

/**
 * Operator action (#1241): toggle the incomplete mark on a request from
 * the Library Completeness card. Marked, the quality decider disregards
 * the installed copy for any candidate beets proves whole; the mark
 * clears automatically when such a candidate terminally imports.
 * @param {number} requestId
 * @param {boolean} marked - true to set the mark, false to clear it
 * @param {HTMLButtonElement} btn
 */
export async function toggleMarkIncomplete(requestId, marked, btn) {
  btn.disabled = true;
  btn.textContent = marked ? 'Marking...' : 'Clearing...';
  try {
    const r = await fetch(`${API}/api/pipeline/mark-incomplete`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id: requestId, marked}),
    });
    const data = await r.json();
    if (r.ok) {
      toast(marked
        ? `Request #${requestId} marked incomplete — the next complete candidate replaces it`
        : `Request #${requestId} incomplete mark cleared`);
      void loadPipelineDashboard();
      return;
    }
    btn.disabled = false;
    btn.textContent = marked ? 'Mark incomplete' : 'Clear incomplete mark';
    toast(`mark-incomplete refused: ${data.error || r.status}`, true);
  } catch (_e) {
    btn.disabled = false;
    btn.textContent = marked ? 'Mark incomplete' : 'Clear incomplete mark';
    toast('mark-incomplete request failed', true);
  }
}

/**
 * Operator action: request an out-of-schedule library-completeness
 * census run. The server writes the trigger file the census path unit
 * watches; the daily oneshot stays the single execution path, so the
 * snapshot (and this card) refreshes when that run completes.
 * @param {HTMLButtonElement} btn
 */
export async function refreshLibraryCensus(btn) {
  btn.disabled = true;
  btn.textContent = 'Requesting...';
  try {
    const r = await fetch(`${API}/api/pipeline/dashboard/library-census/refresh`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({}),
    });
    const data = await r.json();
    if (r.ok) {
      btn.textContent = 'Census requested';
      toast('Census run requested — the card refreshes when it completes (minutes on local mirrors)');
      return;
    }
    btn.disabled = false;
    btn.textContent = 'Run census now';
    toast(`census refresh refused: ${data.error || r.status}`, true);
  } catch (_e) {
    btn.disabled = false;
    btn.textContent = 'Run census now';
    toast('census refresh request failed', true);
  }
}

/**
 * Operator action (#1142): a cheap, explicit per-album retag-divergence
 * recheck. Called from the "Beets DB ↔ File Tags Drift" card's
 * "Recheck" button (`pipeline_dashboard.js::renderRetagDivergenceAlbumRowInner`).
 * Read-only — never mutates Beets, PostgreSQL, or the filesystem, and
 * (unlike `mergeRekeyRequest`) never reloads the whole dashboard: the
 * whole point of a per-album check is to answer for THIS row's own
 * files without re-running the ~200s whole-library scan, so a
 * successful recheck patches just this row's DOM in place with the
 * fresh classification.
 *
 * On refusal/failure the row's own inline note shows the outcome and
 * the button re-arms for retry, mirroring `mergeRekeyRequest`.
 *
 * @param {number} albumId
 * @param {HTMLButtonElement} btn
 * @returns {Promise<void>}
 */
export async function recheckRetagDivergenceAlbum(albumId, btn) {
  const note = document.getElementById(`retag-album-note-${albumId}`);
  const container = document.getElementById(`retag-album-${albumId}`);
  btn.disabled = true;
  btn.textContent = 'Rechecking...';
  try {
    const r = await fetch(`${API}/api/audit/retag-divergence/album/${albumId}`);
    const data = await r.json();
    if (r.ok) {
      if (container) {
        container.innerHTML = renderRetagDivergenceAlbumRowInner(data);
      }
      toast(`Album #${albumId} rechecked: ${data.album_class}`);
      return;
    }
    btn.disabled = false;
    btn.textContent = 'Recheck';
    const message = data.error || 'recheck failed';
    if (note) {
      note.textContent = message;
      note.className = 'drift-row-note metric-bad';
    }
    toast(message, true);
  } catch (_e) {
    btn.disabled = false;
    btn.textContent = 'Recheck';
    if (note) {
      note.textContent = 'Recheck request failed';
      note.className = 'drift-row-note metric-bad';
    }
    toast('Recheck request failed', true);
  }
}

/**
 * Operator action (#1260): write one album's file tags from its Beets DB
 * identity — the "Write tags" button on the "Beets DB ↔ File Tags
 * Drift" card. The identity the operator SAW is re-sent from the
 * button's `data-expected` attribute, so the server's compare-and-set
 * refuses a stale card. Any response carrying the re-scanned album
 * (success OR residual) patches just this row's DOM in place, exactly
 * like the Recheck button; refusals re-arm the button with the outcome
 * in the row's inline note.
 *
 * @param {number} albumId
 * @param {HTMLButtonElement} btn
 * @returns {Promise<void>}
 */
export async function syncRetagDivergenceAlbum(albumId, btn) {
  const container = document.getElementById(`retag-album-${albumId}`);
  const expected = btn.dataset.expected || '';
  btn.disabled = true;
  btn.textContent = 'Writing tags...';
  try {
    const r = await fetch(
      `${API}/api/audit/retag-divergence/album/${albumId}/sync-tags`,
      {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({expected_mb_albumid: expected}),
      },
    );
    const data = await r.json();
    const rerendered = Boolean(data && data.album && container);
    if (rerendered) {
      // Re-render from the post-sync scan — the same classification the
      // census itself would produce. This destroys the old note/button
      // nodes, so the note lookup below must happen AFTER this, and the
      // detached `btn` must not be touched (a fresh enabled button is
      // part of the re-render).
      container.innerHTML = renderRetagDivergenceAlbumRowInner(data.album);
    }
    if (r.ok) {
      toast(`Album #${albumId}: ${data.outcome}`);
      return;
    }
    if (!rerendered) {
      btn.disabled = false;
      btn.textContent = 'Write tags';
    }
    const message = `${data.outcome || 'refused'}: ${data.error_message || data.error || 'tag sync refused'}`;
    const note = document.getElementById(`retag-album-note-${albumId}`);
    if (note) {
      note.textContent = message;
      note.className = 'drift-row-note metric-bad';
    }
    toast(message, true);
  } catch (_e) {
    btn.disabled = false;
    btn.textContent = 'Write tags';
    const note = document.getElementById(`retag-album-note-${albumId}`);
    if (note) {
      note.textContent = 'Tag-sync request failed';
      note.className = 'drift-row-note metric-bad';
    }
    toast('Tag-sync request failed', true);
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
    renderDashboard();
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

/**
 * Render the pipeline view's own tab strip.
 * @returns {string}
 */
export function renderPipelineNav() {
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
 * Render lifecycle actions without offering an invalid search-stop edge.
 * @param {string|number} requestId
 * @param {string} status
 * @param {unknown} processingOwner
 * @returns {string}
 */
export function renderPipelineStatusButtons(requestId, status, processingOwner = null) {
  const processing = processingOwnerPresentation(status, processingOwner);
  if (processing) {
    return renderProcessingLockedControl(processing, Number(requestId), {
      className: 'p-btn active-status',
      descriptionSuffix: 'pipeline-status',
    });
  }
  const downloading = status === 'downloading'
    ? '<button class="p-btn active-status" disabled aria-disabled="true">downloading</button>'
    : '';
  const requestAttr = ` data-pipeline-request-id="${requestId}"`;
  const canSetUnsearchable = status === 'wanted' || status === 'unsearchable';
  const unsearchable = canSetUnsearchable
    ? `<button class="p-btn ${status === 'unsearchable' ? 'active-status' : ''}"${requestAttr} onclick="event.stopPropagation(); window.updateStatus(${requestId}, 'unsearchable')">unsearchable</button>`
    : '<button class="p-btn" disabled aria-disabled="true">unsearchable</button>';
  return `${downloading}
      <button class="p-btn ${status === 'wanted' ? 'active-status' : ''}"${requestAttr} onclick="event.stopPropagation(); window.updateStatus(${requestId}, 'wanted')">wanted</button>
      <button class="p-btn ${status === 'imported' ? 'active-status' : ''}"${requestAttr} onclick="event.stopPropagation(); window.updateStatus(${requestId}, 'imported')">imported</button>
      ${unsearchable}`;
}

/**
 * Render the typed current Beets authority returned by request detail.
 * @param {Object|null|undefined} current
 * @returns {string}
 */
export function renderCurrentLibraryRow(current) {
  if (current?.state === 'unique') {
    return renderDetailRow('Imported to', esc(current.path || ''), {
      valueStyle: 'font-size:0.9em;',
    });
  }
  if (current?.state === 'missing') {
    return renderDetailRow('Beets library', 'Not installed');
  }
  if (current?.state === 'ambiguous') {
    const ids = Array.isArray(current.album_ids) && current.album_ids.length > 0
      ? `; album IDs ${current.album_ids.join(', ')}`
      : '';
    return renderDetailRow(
      'Beets library',
      `Manual review — ambiguous (${esc(current.reason || 'unknown')}${ids})`,
    );
  }
  return renderDetailRow(
    'Beets library',
    `Unavailable — manual review (${esc(current?.reason || 'beets_unavailable')})`,
  );
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
    html += renderCurrentLibraryRow(data.current_library);

    const beetsTracks = data.beets_tracks || [];
    html += renderCurrentQualityRow(req, beetsTracks);

    html += renderRequestEvidenceSections(history, beetsTracks, tracks);

    // Search forensics (last_search) — variant tag + top-3 candidates from
    // the most recent search_log row. Collapsed by default; click expands.
    html += renderForensicBlock(/** @type {any} */ (data.last_search));

    // Status change buttons
    html += `<div class="p-actions">
      <span class="p-detail-label" style="line-height:28px;">Status:</span>
      ${renderPipelineStatusButtons(id, req.status, req.processing_owner ?? null)}`;
    const releaseId = req.mb_release_id || req.discogs_release_id || '';
    const actionState = buildReleaseActionState({
      id: releaseId,
      in_library: data.current_library?.state === 'unique',
      beets_album_id: data.current_library?.album_id ?? null,
      pipeline_status: req.status,
      pipeline_id: Number(id),
      processing_owner: req.processing_owner ?? null,
      artist: req.artist_name || '',
      album: req.album_title || '',
      track_count: tracks.length,
    });
    // Bad-rip reuses the library renderer — pipelineId + releaseId are all
    // it needs from state. Hidden when either is absent (issue #188).
    html += renderBadRipButton(actionState, {
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
        processingState: actionState,
      }, { className: 'p-btn', stopPropagation: true });
    }
    if (actionState.processingPresentation) {
      html += renderProcessingLockedControl(
        actionState.processingPresentation,
        actionState.pipelineId,
        {
          className: 'p-btn delete',
          label: 'delete',
          descriptionSuffix: 'pipeline-delete',
        },
      );
    } else {
      html += `<button class="p-btn delete" data-pipeline-request-id="${id}" onclick="event.stopPropagation(); window.deleteRequest(${id})">delete</button>`;
    }
    html += '</div>';

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
export function renderCurrentQualityRow(req, beetsTracks) {
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
  // The chain mixes two measurements, so the audit-only pair has to be the
  // one belonging to the grade it actually selected (issue #829 Phase 5
  // PR4). Reading the HAVE flag beside a last-download grade would be a
  // codec verdict on a different album.
  const fromCurrent = Boolean(req.current_spectral_grade);
  const admissible = fromCurrent
    ? req.current_spectral_accusation_admissible
    : req.last_download_spectral_accusation_admissible;
  const withheld = fromCurrent
    ? req.current_spectral_accusation_withheld
    : req.last_download_spectral_accusation_withheld;
  const verified = req.verified_lossless === true || req.verified_lossless === 'True';
  let qualitySummary = nominal;
  if (verified) {
    qualitySummary += ` <span class="${qualityToneClass('lossless')}">verified lossless</span>`;
  } else if (spectralGrade) {
    // Show every measured grade through the shared palette. Keep the spectral
    // floor even on a genuine rollup: a non-null bitrate there means some
    // tracks tripped the cliff detector below the album suspect threshold,
    // and the shared-spectral clamp still consults that floor (Eno case).
    const brStr = spectralBr ? ` ~${spectralBr}kbps` : '';
    const label = `spectral: ${esc(spectralGradeLabel(spectralGrade))}${brStr}`;
    if (spectralGradeIsAdmissible(spectralGrade, admissible)) {
      qualitySummary += ` <span class="${spectralGradeClass(spectralGrade)}">${label}</span>`;
    } else {
      const withholding = spectralWithheldPresentation(withheld);
      qualitySummary += ` <span class="${withholding.className}"`
        + ` title="${withholding.title}">${label}${withholding.suffix}</span>`;
    }
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
    if (await handleProcessingLockedConflict({
      httpStatus: r.status,
      payload: data,
    })) {
      return;
    }
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
    if (await handleProcessingLockedConflict({
      httpStatus: r.status,
      payload: data,
    })) {
      return;
    }
    if (data.status === 'ok') {
      toast(`#${id} → ${newStatus}`);
      loadPipeline();
    } else {
      toast(data.error || 'Update failed', true);
    }
  } catch (e) { toast('Update failed', true); }
}
