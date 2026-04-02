// @ts-check

/**
 * Entry point — imports all modules, wires up event listeners,
 * exposes functions to window for onclick handlers in HTML templates.
 */

import { state } from './state.js';
import { searchArtists, setSearchType, openBrowseArtist, closeBrowseArtist, switchSubView, invalidateBrowseArtist } from './browse.js';
import { renderArtistDiscography, loadReleaseGroup, addRelease, toggleReleaseDetail } from './discography.js';
import { loadRecents, setRecentsFilter, renderRecentsItems } from './recents.js';
import { loadPipeline, setFilter, renderPipeline, toggleDetail, deleteRequest, updateStatus } from './pipeline.js';
import { renderLibraryResults, renderLibraryResultsInto, toggleLibDetail, banSource, setLibQuality, upgradeAlbum, confirmDeleteBeets, executeBeetsDeletion } from './library.js';
import { loadDecisions, dsPreset, runSimulator } from './decisions.js';
import { renderDisambiguateInto, toggleDisambRGTracks, disambRemove, disambDeleteFromLibrary } from './analysis.js';
import { loadManualImport, runManualImport } from './manual.js';
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
  if (name === 'manual') loadManualImport();
}

// --- Search input (debounced) ---
const qInput = /** @type {HTMLInputElement} */ (document.getElementById('q'));
if (qInput) {
  qInput.addEventListener('input', () => {
    clearTimeout(state.searchTimer ?? undefined);
    const q = qInput.value.trim();
    if (q.length < 2) {
      const results = document.getElementById('results');
      if (results) results.innerHTML = '';
      return;
    }
    state.searchTimer = window.setTimeout(() => searchArtists(q), 300);
  });
}

// --- Expose functions to window for onclick handlers in HTML templates ---
Object.assign(window, {
  showTab,
  setSearchType,
  openBrowseArtist,
  closeBrowseArtist,
  switchSubView,
  searchArtists,
  renderArtistDiscography,
  loadReleaseGroup,
  addRelease,
  toggleReleaseDetail,
  loadRecents,
  setRecentsFilter,
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
  confirmDeleteBeets,
  executeBeetsDeletion,
  loadDecisions,
  dsPreset,
  runSimulator,
  renderDisambiguateInto,
  toggleDisambRGTracks,
  disambRemove,
  disambDeleteFromLibrary,
  loadManualImport,
  runManualImport,
  toast,
});
