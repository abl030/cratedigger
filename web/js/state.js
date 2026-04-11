// @ts-check

/**
 * Shared application state and toast notification.
 * All modules import state from here instead of using bare globals.
 */

/** @type {{ browseSearchType: string, browseArtist: {id:string, name:string}|null, browseSubView: string, browseCache: Object, pipelineData: Object|null, pipelineFilter: string, recentsCounts: {all:number, imported:number, rejected:number}, recentsFilter: string, dsConstants: Object|null, disambData: Object|null, searchTimer: number|null, manualSub: string }} */
export const state = {
  browseSearchType: 'artist',
  browseArtist: null,
  browseSubView: 'discography',
  browseCache: {},
  pipelineData: null,
  pipelineFilter: 'wanted',
  recentsCounts: { all: 0, imported: 0, rejected: 0 },
  recentsFilter: 'all',
  dsConstants: null,
  disambData: null,
  searchTimer: null,
  manualSub: 'complete',
};

export const API = '';

/**
 * Central pipeline status store. Maps MBID → {status, id}.
 * Updated by any mutation (add, remove, upgrade, delete).
 * All rendering code should check this before using stale API data.
 * @type {Map<string, {status: string|null, id: number|null}>}
 */
export const pipelineStore = new Map();

/**
 * Update pipeline status for an MBID across all in-memory state.
 * Call after any pipeline mutation (add, remove, upgrade, delete).
 * @param {string} mbid - MusicBrainz release ID
 * @param {string|null} status - New status ('wanted', 'imported', null for removed)
 * @param {number|null} pipelineId - Pipeline request ID (null if removed)
 */
export function updatePipelineStatus(mbid, status, pipelineId) {
  // Update central store
  if (status) {
    pipelineStore.set(mbid, { status, id: pipelineId });
  } else {
    pipelineStore.delete(mbid);
  }
  // Update disambData pressings (analysis tab)
  if (state.disambData) {
    for (const rg of state.disambData.release_groups) {
      for (const p of (rg.pressings || [])) {
        if (p.release_id === mbid) {
          p.pipeline_status = status;
          p.pipeline_id = pipelineId;
        }
      }
      // Update RG-level status if this was the tracked pressing
      if (rg.pipeline_id === pipelineId || rg.release_ids?.includes(mbid)) {
        if (status) {
          rg.pipeline_status = status;
          rg.pipeline_id = pipelineId;
        } else if (rg.pipeline_id === pipelineId) {
          rg.pipeline_status = null;
          rg.pipeline_id = null;
        }
      }
    }
  }
}

/**
 * Show a toast notification.
 * @param {string} msg
 * @param {boolean} [isError]
 */
export function toast(msg, isError) {
  const t = document.getElementById('toast');
  if (!t) return;
  t.textContent = msg;
  t.className = 'toast' + (isError ? ' error' : '');
  t.style.display = 'block';
  setTimeout(() => t.style.display = 'none', 3000);
}
