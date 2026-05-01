// @ts-check

/**
 * Entry point — imports all modules, wires up event listeners,
 * exposes functions to window for onclick handlers in HTML templates.
 */

import { state } from './state.js';
import { searchArtists, cancelBrowseSearch, setSearchType, setBrowseSource, openBrowseArtist, closeBrowseArtist, switchSubView, invalidateBrowseArtist, openBrowseArtistFromCompare, toggleCompareRow, closeVaFallback } from './browse.js';
import { renderArtistDiscography, loadReleaseGroup, addRelease, toggleReleaseDetail } from './discography.js';
import { loadRecents, setRecentsFilter, setRecentsSub, renderRecentsItems } from './recents.js';
import { loadPipeline, setFilter, renderPipeline, toggleDetail, deleteRequest, updateStatus } from './pipeline.js';
import { renderLibraryResults, renderLibraryResultsInto, toggleLibDetail, banSource, setLibQuality, upgradeAlbum, setIntent, confirmDeleteBeets, executeBeetsDeletion } from './library.js';
import { loadDecisions, dsPreset, runSimulator } from './decisions.js';
import { renderDisambiguateInto, toggleDisambRGTracks, disambRemove } from './analysis.js';
import { loadManualImport, runManualImport } from './manual.js';
import { loadWrongMatches, toggleWrongMatchGroup, toggleWrongMatchEntry, forceImportWrongMatch, deleteWrongMatch, deleteWrongMatchGroup, deleteTransparentNonFlacWrongMatches, deleteLosslessOpusWrongMatches, convergeWrongMatches, setWrongMatchConvergeThreshold, setWrongMatchConvergeCleanup } from './wrong-matches.js';
import { openLabelDetail, openLabelDetailFromList, closeLabelDetail, onLabelFilterChange, onLabelYearFilterInput, toggleLabelIncludeSublabels, goToLabelPage } from './labels.js';
import { toast } from './state.js';

// --- Tab management ---
const tabOrder = ['browse', 'recents', 'pipeline', 'decisions', 'manual'];

/** @param {string} name */
function showTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  const tabEl = document.querySelector(`.tab:nth-child(${tabOrder.indexOf(name) + 1})`);
  if (tabEl) tabEl.classList.add('active');
  const secEl = document.getElementById(name + '-section');
  if (secEl) secEl.classList.add('active');
  if (name === 'pipeline') loadPipeline();
  if (name === 'recents') loadRecents();
  if (name === 'decisions') loadDecisions();
  if (name === 'manual') showManualSub(state.manualSub || 'complete');
}

/** @param {string} sub - 'complete' or 'wrong' */
function showManualSub(sub) {
  state.manualSub = sub;
  document.getElementById('manual-complete').style.display = sub === 'complete' ? 'block' : 'none';
  document.getElementById('manual-wrong').style.display = sub === 'wrong' ? 'block' : 'none';
  document.getElementById('manual-sub-complete').className = 'p-btn' + (sub === 'complete' ? ' active-status' : '');
  document.getElementById('manual-sub-wrong').className = 'p-btn' + (sub === 'wrong' ? ' active-status' : '');
  if (sub === 'complete') loadManualImport();
  if (sub === 'wrong') loadWrongMatches();
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
  setFilter,
  renderPipeline,
  toggleDetail,
  deleteRequest,
  updateStatus,
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
  loadManualImport,
  runManualImport,
  showManualSub,
  loadWrongMatches,
  toggleWrongMatchGroup,
  toggleWrongMatchEntry,
  forceImportWrongMatch,
  deleteWrongMatch,
  deleteWrongMatchGroup,
  deleteTransparentNonFlacWrongMatches,
  deleteLosslessOpusWrongMatches,
  convergeWrongMatches,
  setWrongMatchConvergeThreshold,
  setWrongMatchConvergeCleanup,
  openLabelDetail,
  openLabelDetailFromList,
  closeLabelDetail,
  onLabelFilterChange,
  onLabelYearFilterInput,
  toggleLabelIncludeSublabels,
  goToLabelPage,
  toast,
});
