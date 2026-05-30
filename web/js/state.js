// @ts-check

/**
 * Shared application state and toast notification.
 * All modules import state from here instead of using bare globals.
 */

import { normalizeReleaseId } from './util.js';
import { invalidateActiveRgs } from './active_rgs.js';

/**
 * Stash for the search-plan detail page's back button. Captured when
 * the operator clicks "Open detail →" inside an inspector summary panel
 * and consumed by `closeSearchPlanDetail` to restore the originating
 * tab + sub-view + scroll position. `null` when no detail page is open.
 *
 * @typedef {Object} SearchPlanDetailContext
 * @property {number} requestId        The pipeline request being inspected.
 * @property {string} originTab        Tab to restore — `'browse'`, `'pipeline'`, `'recents'`, etc.
 * @property {number} originScrollY    `window.scrollY` at click time.
 * @property {string|null} originSubView  Sub-view to restore (e.g. `'queue'`/`'dashboard'` on Pipeline). `null` for tabs with no sub-view.
 */

/**
 * Long-tail triage worklist state. `rows` is the full server-banded
 * `wanted` cohort fetched once (KTD2 — one banded fetch, client-side
 * tab/search filtering); `band` is the selected band tab (`null` until
 * the first fetch picks a default); `query` is the live search box value.
 *
 * @typedef {Object} LongTailState
 * @property {Array<Object>|null} rows  The fetched cohort, or `null` before the first load.
 * @property {string|null} band         Selected band tab, or `null` (no selection yet).
 * @property {string} query             Current search-box substring filter.
 */

/** @type {{ browseSource: string, browseSearchType: string, browseArtist: {id:string, name:string}|null, browseLabel: {id:string, name:string}|null, labelFilters: {yearMin:number|null, yearMax:number|null, format:string, hideHeld:boolean}, labelPage: number, browseSubView: string, browseCache: Object, pipelineData: Object|null, pipelineDashboardData: Object|null, pipelineView: string, pipelineFilter: string, pipelineMatchGraphOpen: boolean, pipelineHourlyMatchGraphOpen: boolean, pipelineDailyMatchGraphOpen: boolean, longTail: LongTailState, recentsCounts: {all:number, imported:number, rejected:number, matches_24h:number, matches_6h:number, matches_per_hour_24h:number, matches_per_hour_6h:number}, recentsFilter: string, recentsSub: 'history'|'downloading'|'queue', dsConstants: Object|null, disambData: Object|null, searchTimer: number|null, searchTargetId: string|null, searchTargetExpandId: string|null, searchTargetSource: string|null, searchPlanDetailContext: SearchPlanDetailContext|null }} */
export const state = {
  browseSource: 'mb',
  browseSearchType: 'artist',
  browseArtist: null,
  browseLabel: null,
  labelFilters: { yearMin: null, yearMax: null, format: '', hideHeld: false },
  labelPage: 1,
  browseSubView: 'discography',
  browseCache: {},
  pipelineData: null,
  pipelineDashboardData: null,
  pipelineView: 'queue',
  pipelineFilter: 'wanted',
  pipelineMatchGraphOpen: false,
  pipelineHourlyMatchGraphOpen: false,
  pipelineDailyMatchGraphOpen: false,
  // Long-tail triage worklist (U3). `rows` null until the first fetch;
  // `band` null until the cohort's default band is picked; `query` is
  // the live in-band search-box filter.
  longTail: { rows: null, band: null, query: '' },
  recentsCounts: {
    all: 0,
    imported: 0,
    rejected: 0,
    matches_24h: 0,
    matches_6h: 0,
    matches_per_hour_24h: 0,
    matches_per_hour_6h: 0,
  },
  recentsFilter: 'all',
  recentsSub: 'history',
  dsConstants: null,
  disambData: null,
  searchTimer: null,
  // Search-by-ID ring state. Cleared on closeBrowseArtist / setSearchType /
  // next paste. searchTargetId is the leaf .release[data-release-id]; null
  // for group-level inputs (master / release-group). searchTargetExpandId
  // is the parent .rg the discography post-render hook auto-expands.
  searchTargetId: null,
  searchTargetExpandId: null,
  searchTargetSource: null,
  // Search-plan detail subview back-button stash. Hydrated by
  // `openSearchPlanDetail` (U4); cleared by `closeSearchPlanDetail`.
  searchPlanDetailContext: null,
};

export const API = '';

/**
 * Central pipeline status store. Maps normalized release ID → {status, id}.
 * Updated by any mutation (add, remove, upgrade, delete).
 * All rendering code should check this before using stale API data.
 * @type {Map<string, {status: string|null, id: number|null}>}
 */
export const pipelineStore = new Map();

/**
 * Normalize the single release-id key the frontend stores pipeline state under.
 * @param {string|null|undefined} releaseId
 * @returns {string}
 */
export function pipelineStoreKey(releaseId) {
  return normalizeReleaseId(releaseId);
}

/**
 * Update pipeline status for an MBID across all in-memory state.
 * Call after any pipeline mutation (add, remove, upgrade, delete).
 * @param {string} mbid - MB UUID or numeric Discogs release ID
 * @param {string|null} status - New status ('wanted', 'imported', null for removed)
 * @param {number|null} pipelineId - Pipeline request ID (null if removed)
 */
export function updatePipelineStatus(mbid, status, pipelineId) {
  const key = pipelineStoreKey(mbid);
  if (!key) return;
  // Update central store
  if (status) {
    pipelineStore.set(key, { status, id: pipelineId });
  } else {
    pipelineStore.delete(key);
  }
  // Any pipeline mutation (add / remove / upgrade / replace) may
  // shift which release-groups have an active row — invalidate the
  // Browse-search inverted Replace button cache so the next render
  // re-fetches.
  invalidateActiveRgs();
  // Update disambData pressings (analysis tab)
  if (state.disambData) {
    for (const rg of state.disambData.release_groups) {
      for (const p of (rg.pressings || [])) {
        if (pipelineStoreKey(p.release_id) === key) {
          p.pipeline_status = status;
          p.pipeline_id = pipelineId;
        }
      }
      // Update RG-level status if this was the tracked pressing
      const releaseIds = (rg.release_ids || []).map(pipelineStoreKey);
      if (rg.pipeline_id === pipelineId || releaseIds.includes(key)) {
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
