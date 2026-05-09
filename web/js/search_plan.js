// @ts-check

/**
 * Search-plan inspector module.
 *
 * Foundation seam (U2 of feat/search-plan-dashboard): module-level cache,
 * URL builders, fetch helpers, origin-context capture/restore, and stub
 * action handlers. The user-visible UI (summary panel, detail subview,
 * action buttons) lands in U3-U5 and plugs into the same exports.
 *
 * Shape mirrors `web/js/recents.js` / `web/js/release_action_state.js`:
 * `// @ts-check`, ES6 module, JSDoc on exports, pure helpers DOM-free.
 */

import { state } from './state.js';

/**
 * Default number of `search_log` rows fetched per history page.
 * Mirrors the default on the API (`HISTORY_PAGE_DEFAULT_LIMIT` /
 * `HISTORY_PAGE_DEFAULT_LIMIT` in `lib/search_plan_service.py`).
 */
export const HISTORY_PAGE_DEFAULT_LIMIT = 50;

/**
 * @typedef {Object} SearchPlanCacheEntry
 * @property {Object} inspection      Result of `GET /search-plan`.
 * @property {Array<Object>} historyHead  Newest-first slice (e.g. last 3) for the summary view.
 * @property {number} fetchedAt       `Date.now()` at fetch time.
 */

/**
 * Module-level memoization for the inspector. Cleared per-request on
 * regenerate/advance success in U5; keys are pipeline request ids.
 *
 * Values are typed loosely (`any`) because the API responses are plain
 * dict trees decoded directly from `search_log` rows + the inspection
 * payload — typing them would just shadow the API contract enforced in
 * `tests/test_web_server.py`.
 *
 * @type {Map<number, SearchPlanCacheEntry>}
 */
export const searchPlanCache = new Map();

/**
 * @typedef {Object} HistoryUrlOptions
 * @property {number} requestId  Pipeline request id (positive integer).
 * @property {number} [limit]    Page size in [1, 200]; defaults to {@link HISTORY_PAGE_DEFAULT_LIMIT} when nullish.
 * @property {number|null} [beforeId]  Cursor — emit `before_id=<id>` when present, else omit.
 */

/**
 * Build the URL path + query string for the history endpoint.
 *
 * Pure / DOM-free / no `fetch`. Validates `requestId` is a positive
 * integer and throws `TypeError` otherwise. `beforeId` is omitted from
 * the query string when null/undefined.
 *
 * @param {HistoryUrlOptions} opts
 * @returns {string} e.g. `/api/pipeline/2566/search-plan/history?limit=50&before_id=12345`.
 */
export function buildHistoryUrl(opts) {
  const requestId = opts.requestId;
  if (!Number.isInteger(requestId) || requestId <= 0) {
    throw new TypeError(
      `buildHistoryUrl: requestId must be a positive integer (got ${JSON.stringify(requestId)})`,
    );
  }
  const limit = (opts.limit === null || opts.limit === undefined)
    ? HISTORY_PAGE_DEFAULT_LIMIT
    : opts.limit;
  const params = new URLSearchParams();
  params.set('limit', String(limit));
  if (opts.beforeId !== null && opts.beforeId !== undefined) {
    params.set('before_id', String(opts.beforeId));
  }
  return `/api/pipeline/${requestId}/search-plan/history?${params.toString()}`;
}

/**
 * @typedef {Object} OriginContextInput
 * @property {string} tab        Active tab when the operator clicked "Open detail" — `'browse'`, `'pipeline'`, `'recents'`, etc.
 * @property {number} scrollY    `window.scrollY` at click time.
 * @property {string|null} subView  Active sub-view (e.g. `'queue'` / `'dashboard'` on Pipeline). `null` for tabs with no sub-view.
 */

/**
 * Capture a back-button context from the active tab + scroll. Pure
 * round-trip with {@link restoreOriginContext}: `restore(capture(x))` ===
 * `{tab, scrollY, subView}` of the input. The wired-up call sites in U4
 * stash this on `state.searchPlanDetailContext`.
 *
 * @param {OriginContextInput} input
 * @returns {import('./state.js').SearchPlanDetailContext} Context shape stored on `state.searchPlanDetailContext`.
 */
export function captureOriginContext(input) {
  return {
    requestId: 0, // Caller patches this with the actual request id; U4 wires it in `openSearchPlanDetail`.
    originTab: input.tab,
    originScrollY: input.scrollY,
    originSubView: input.subView,
  };
}

/**
 * Inverse of {@link captureOriginContext}. Pure — no DOM, no scroll, no
 * tab switching. The actual `showTab` / `window.scrollTo` calls happen
 * in U4's `closeSearchPlanDetail`; this helper just exposes the data
 * shape so call-sites and tests share one definition.
 *
 * @param {import('./state.js').SearchPlanDetailContext} context
 * @returns {{tab: string, scrollY: number, subView: string|null}}
 */
export function restoreOriginContext(context) {
  return {
    tab: context.originTab,
    scrollY: context.originScrollY,
    subView: context.originSubView,
  };
}

/**
 * Drop the cache entry for one request. Returns the same Map so
 * callers can chain. No-op when the entry is absent.
 *
 * @param {Map<number, SearchPlanCacheEntry>} cache
 * @param {number} requestId
 * @returns {Map<number, SearchPlanCacheEntry>}
 */
export function invalidateSearchPlanCache(cache, requestId) {
  cache.delete(requestId);
  return cache;
}

/**
 * Fetch the search-plan inspection payload for one request.
 *
 * Impure (calls `fetch`). Throws on non-OK status with the response
 * text in the error message so callers can surface it via `state.toast`.
 *
 * @param {number} requestId
 * @returns {Promise<Object>} Parsed JSON body of `GET /api/pipeline/<id>/search-plan`.
 */
export async function fetchInspection(requestId) {
  if (!Number.isInteger(requestId) || requestId <= 0) {
    throw new TypeError(
      `fetchInspection: requestId must be a positive integer (got ${JSON.stringify(requestId)})`,
    );
  }
  const url = `/api/pipeline/${requestId}/search-plan`;
  const resp = await fetch(url);
  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`fetchInspection ${requestId}: HTTP ${resp.status} — ${body}`);
  }
  return resp.json();
}

/**
 * Fetch one history page for one request.
 *
 * Impure (calls `fetch`). Throws on non-OK status. The URL is built via
 * {@link buildHistoryUrl} so its validation runs first.
 *
 * @param {number} requestId
 * @param {{limit?: number, beforeId?: number|null}} [opts]
 * @returns {Promise<Object>} Parsed JSON body of `GET /search-plan/history`.
 */
export async function fetchHistoryPage(requestId, opts = {}) {
  const url = buildHistoryUrl({
    requestId,
    limit: opts.limit,
    beforeId: opts.beforeId ?? null,
  });
  const resp = await fetch(url);
  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(
      `fetchHistoryPage ${requestId}: HTTP ${resp.status} — ${body}`,
    );
  }
  return resp.json();
}

// --- Action handler stubs ---------------------------------------------
//
// Real implementations land in U3 (toggleSearchPlanSummary), U4
// (openSearchPlanDetail / closeSearchPlanDetail), and U5
// (searchPlanRegenerate / searchPlanAdvance). The window bindings exist
// now so U3-U5 don't have to re-wire `main.js`. Each stub THROWS rather
// than no-op'ing silently — a click on a wired-up button before its
// unit ships is a developer bug, not a UX feature.

/**
 * @param {number} _requestId
 * @param {Element|null} [_rowEl]
 * @returns {void}
 */
// eslint-disable-next-line no-unused-vars
export function toggleSearchPlanSummary(_requestId, _rowEl) {
  // Reference `state` so the import is not flagged unused by linters
  // before U3 wires the real implementation that reads/writes state.
  void state;
  throw new Error('toggleSearchPlanSummary: not implemented (U3 will land summary panel)');
}

/**
 * @param {number} _requestId
 * @param {Element|null} [_rowEl]
 * @returns {void}
 */
// eslint-disable-next-line no-unused-vars
export function openSearchPlanDetail(_requestId, _rowEl) {
  throw new Error('openSearchPlanDetail: not implemented (U4 will land detail subview)');
}

/**
 * @returns {void}
 */
export function closeSearchPlanDetail() {
  throw new Error('closeSearchPlanDetail: not implemented (U4 will land detail subview)');
}

/**
 * @param {number} _requestId
 * @returns {void}
 */
// eslint-disable-next-line no-unused-vars
export function searchPlanRegenerate(_requestId) {
  throw new Error('searchPlanRegenerate: not implemented (U5 will land regenerate action)');
}

/**
 * @param {number} _requestId
 * @param {{toOrdinal?: number, toStrategy?: string}} _target
 * @returns {void}
 */
// eslint-disable-next-line no-unused-vars
export function searchPlanAdvance(_requestId, _target) {
  throw new Error('searchPlanAdvance: not implemented (U5 will land advance action)');
}
