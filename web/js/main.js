// @ts-check

/**
 * Entry point — imports all modules, wires up event listeners,
 * exposes functions to window for onclick handlers in HTML templates.
 */

import { state } from './state.js';
import { searchArtists, cancelBrowseSearch, setSearchType, setBrowseSource, openBrowseArtist, closeBrowseArtist, switchSubView, invalidateBrowseArtist, openBrowseArtistFromCompare, toggleCompareRow, closeVaFallback } from './browse.js';
import { renderArtistDiscography, loadReleaseGroup, addRelease, toggleReleaseDetail } from './discography.js';
import { loadRecents, setRecentsFilter, setRecentsSub, renderRecentsItems } from './recents.js';
import { loadPipeline, loadPipelineDashboard, setPipelineView, setFilter, renderPipeline, toggleCoverageMatchGraph, toggleDetail, deleteRequest, updateStatus, togglePipelineReplacedFilter } from './pipeline.js';
import { loadLongTail, setLongTailBand, onLongTailSearchInput, toggleLongTailDetail, toggleLongTailPeers, checkYoutube, pickYoutubeRescue, longTailAcceptSibling, longTailSetIntent, longTailReSearch } from './long_tail.js';
import { renderLibraryResults, renderLibraryResultsInto, toggleLibDetail, banSource, setLibQuality, upgradeAlbum, setIntent, confirmDeleteBeets, executeBeetsDeletion } from './library.js';
import { loadDecisions, dsPreset, runSimulator } from './decisions.js';
import { renderDisambiguateInto, toggleDisambRGTracks, disambRemove } from './analysis.js';
import { loadWrongMatches, toggleWrongMatchGroup, toggleWrongMatchEntry, reloadWrongMatchExplorer, maybeLoadWrongMatchExplorer, refreshWrongMatches, forceImportWrongMatch, deleteWrongMatch, deleteWrongMatchGroup, bulkTriageWrongMatches, convergeWrongMatches, setWrongMatchConvergeThreshold, toggleWrongMatchesReplacedFilter } from './wrong-matches.js';
import { openLabelDetail, openLabelDetailFromList, closeLabelDetail, onLabelFilterChange, onLabelYearFilterInput, toggleLabelIncludeSublabels, goToLabelPage } from './labels.js';
import { toggleSearchPlanSummary, openSearchPlanDetail, closeSearchPlanDetail, searchPlanRegenerate, searchPlanAdvance, searchPlanLoadOlder, searchPlanRefreshDetail, searchPlanSubmitAdvance, searchPlanCancelAdvance } from './search_plan.js';
import { openReplacePicker } from './replace_picker.js';
import { invalidateActiveRgs } from './active_rgs.js';
import { toast } from './state.js';

/**
 * Replace-picker wrapper that surfaces success / failure toasts and
 * refetches the affected tab so the new request appears immediately.
 *
 * Called from inline onclick handlers in `release_actions.js`. The
 * picker's confirm-stage POST handles the network round-trip; this
 * wrapper only does post-completion UX.
 *
 * @param {import('./replace_picker.js').ReplacePickerOptions} options
 */
async function openReplacePickerAndHandle(options) {
  const result = await openReplacePicker(options);
  if (result.outcome !== 'confirmed') return;
  const { status, body } = result.response || { status: 0, body: {} };
  if (status === 200) {
    const newId = body.new_request_id;
    toast(`Replaced — new request #${newId}.`, false);
    // Replace may flip rows in/out of the active set (old row leaves,
    // new row enters — same RG so the count stays positive, but the
    // browse-tab cache needs to re-fetch so the per-MBID enable
    // logic stays accurate).
    invalidateActiveRgs();
    // Best-effort refetch on whichever tab the operator is most likely
    // on. Pipeline is the canonical viewer of an album_requests row;
    // wrong-matches and browse re-fetch on their own next interaction.
    const pipeSection = document.getElementById('pipeline-section');
    if (pipeSection && pipeSection.classList.contains('active')) {
      loadPipeline();
    }
    const manualSection = document.getElementById('manual-section');
    if (manualSection && manualSection.classList.contains('active')) {
      loadWrongMatches();
    }
  } else if (status === 409 && body && body.descendant_request_id) {
    toast(`Already replaced. New request is #${body.descendant_request_id}.`, true);
  } else if (body && body.error) {
    toast(`Replace failed: ${body.error}`, true);
  } else {
    toast(`Replace failed (HTTP ${status})`, true);
  }
}

// --- Tab management ---
const tabOrder = ['browse', 'recents', 'pipeline', 'decisions', 'manual'];

/**
 * Internal flag used by `openSearchPlanDetail`-style flows to suppress
 * the F12 detail-context reset for one `showTab` call. Without this,
 * the detail-open path would clear its own freshly-set context.
 *
 * @type {boolean}
 */
let suppressDetailReset = false;

/** @param {string} name */
function showTab(name) {
  // F12: Tab-switch reset for the search-plan detail subview. When the
  // operator is on the detail page and clicks a tab — including
  // re-clicking the Pipeline tab — clear the detail context so the
  // dispatcher renders the queue/dashboard rather than re-running the
  // (now-stale) detail render. The `suppressDetailReset` flag carves
  // out an exception for `openSearchPlanDetail`'s internally-driven
  // showTab('pipeline') call (which has just set pipelineView).
  if (!suppressDetailReset && state.pipelineView === 'search-plan-detail') {
    state.pipelineView = 'queue';
    state.searchPlanDetailContext = null;
  }
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  const tabEl = document.querySelector(`.tab:nth-child(${tabOrder.indexOf(name) + 1})`);
  if (tabEl) tabEl.classList.add('active');
  const secEl = document.getElementById(name + '-section');
  if (secEl) secEl.classList.add('active');
  if (name === 'pipeline') loadPipeline();
  if (name === 'recents') loadRecents();
  if (name === 'decisions') loadDecisions();
  if (name === 'manual') loadWrongMatches();
}

/**
 * Wrapper that runs `showTab(name)` with the F12 detail-context reset
 * suppressed. Used by flows that intentionally route through showTab
 * after setting `state.pipelineView` (e.g. `openSearchPlanDetail`).
 *
 * @param {string} name
 */
function showTabPreservingDetail(name) {
  suppressDetailReset = true;
  try {
    showTab(name);
  } finally {
    suppressDetailReset = false;
  }
}

// --- Search input (debounced) ---
const qInput = /** @type {HTMLInputElement} */ (document.getElementById('q'));
if (qInput) {
  qInput.addEventListener('input', () => {
    clearTimeout(state.searchTimer ?? undefined);
    const q = qInput.value.trim();
    // ID mode parses single-character inputs (Discogs IDs start at 1);
    // other modes need at least 2 chars before searching.
    const minLen = state.browseSearchType === 'id' ? 1 : 2;
    if (q.length < minLen) {
      cancelBrowseSearch();
      const results = document.getElementById('results');
      if (results) results.innerHTML = '';
      // Hide the VA fallback if it's open — without this, a stale fetch
      // landing post-clear renders into a card the user thought they
      // dismissed. (Token bump from cancelBrowseSearch also gates the
      // fetch via openVaFallback's isStale check.)
      const vaFallback = document.getElementById('va-fallback');
      if (vaFallback) vaFallback.style.display = 'none';
      return;
    }
    state.searchTimer = window.setTimeout(() => searchArtists(q), 300);
  });
}

// --- Expose functions to window for onclick handlers in HTML templates ---
Object.assign(window, {
  showTab,
  showTabPreservingDetail,
  setSearchType,
  setBrowseSource,
  openBrowseArtist,
  openBrowseArtistFromCompare,
  toggleCompareRow,
  closeBrowseArtist,
  closeVaFallback,
  switchSubView,
  searchArtists,
  renderArtistDiscography,
  loadReleaseGroup,
  addRelease,
  toggleReleaseDetail,
  loadRecents,
  setRecentsFilter,
  setRecentsSub,
  loadPipeline,
  loadPipelineDashboard,
  setPipelineView,
  setFilter,
  renderPipeline,
  toggleCoverageMatchGraph,
  toggleDetail,
  deleteRequest,
  updateStatus,
  // Long-tail triage worklist: nav + tab/search/list handlers (U3) plus
  // the per-row action console (U4 — toggleLongTailDetail opens the
  // evidence console; toggleLongTailPeers flips the capped/full peers
  // view). renderPipeline (already exposed) re-emits the nav for the
  // long-tail sub-view; long_tail.js calls it via window.renderPipeline.
  loadLongTail,
  setLongTailBand,
  onLongTailSearchInput,
  toggleLongTailDetail,
  toggleLongTailPeers,
  renderLibraryResults,
  renderLibraryResultsInto,
  toggleLibDetail,
  banSource,
  setLibQuality,
  upgradeAlbum,
  setIntent,
  confirmDeleteBeets,
  executeBeetsDeletion,
  loadDecisions,
  dsPreset,
  runSimulator,
  renderDisambiguateInto,
  toggleDisambRGTracks,
  disambRemove,
  loadWrongMatches,
  toggleWrongMatchGroup,
  toggleWrongMatchEntry,
  reloadWrongMatchExplorer,
  maybeLoadWrongMatchExplorer,
  refreshWrongMatches,
  forceImportWrongMatch,
  deleteWrongMatch,
  deleteWrongMatchGroup,
  bulkTriageWrongMatches,
  convergeWrongMatches,
  setWrongMatchConvergeThreshold,
  openLabelDetail,
  openLabelDetailFromList,
  closeLabelDetail,
  onLabelFilterChange,
  onLabelYearFilterInput,
  toggleLabelIncludeSublabels,
  goToLabelPage,
  // Search-plan inspector — handlers land in U3 (toggleSearchPlanSummary),
  // U4 (open/closeSearchPlanDetail + the detail-page Refresh + Load-older
  // affordances), and U5 (searchPlanRegenerate / searchPlanAdvance, still
  // stubs).
  toggleSearchPlanSummary,
  openSearchPlanDetail,
  closeSearchPlanDetail,
  searchPlanRegenerate,
  searchPlanAdvance,
  searchPlanLoadOlder,
  searchPlanRefreshDetail,
  searchPlanSubmitAdvance,
  searchPlanCancelAdvance,
  // Replace operator action — U9 binding so cross-module onclick
  // handlers in `release_actions.js` can call into the picker.
  openReplacePicker: openReplacePickerAndHandle,
  togglePipelineReplacedFilter,
  toggleWrongMatchesReplacedFilter,
  // Long-tail YouTube rescue (U5) — the two-step flow. `checkYoutube` runs
  // the slow, side-effectful resolver GET (double-fire-guarded, stale-result
  // stamped) and re-renders the YouTube panel with pickable rescue targets;
  // `pickYoutubeRescue` opens the confirm overlay for a chosen target and
  // submits the rescue, mapping every ingest outcome to specific console
  // copy. The resolver GET is NOT auto-called on console open (U4 leaves the
  // panel in `never_run` until the operator clicks).
  checkYoutube,
  pickYoutubeRescue,
  // Long-tail secondary actions (U6) — accept-a-sibling-pressing (the
  // existing Replace operator action, MB-only per KTD7; disabled with a
  // reason for Discogs requests), set-intent (lossless ⇄ default toggle via
  // the existing set-intent surface), and re-search (regenerate-plan +
  // reset-cursor via the existing search-plan/regenerate surface, honest
  // next-cycle copy). Each reuses the U5 single-row refetch helper for
  // post-action freshness; the confirmed Replace closes the console and
  // full-cohort refetches because the old row leaves the worklist.
  longTailAcceptSibling,
  longTailSetIntent,
  longTailReSearch,
  toast,
});
