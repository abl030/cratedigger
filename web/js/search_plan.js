// @ts-check

/**
 * Search-plan inspector module.
 *
 * U2 (foundation): module-level cache, URL builders, fetch helpers,
 * origin-context capture/restore, action handler stubs.
 * U3 (this unit): real `toggleSearchPlanSummary`, the pure
 * `renderSummaryPanel` HTML producer, and `renderSearchPlanButton` —
 * the small per-row injector used by the three album-row renderers
 * (Browse/Pipeline/Recents).
 *
 * Detail navigation and the regenerate/advance handlers remain U2-stub
 * throws until U4/U5 fill them in. The summary HTML already wires the
 * button onclicks to those `window.*` exports — they just throw early.
 *
 * Shape mirrors `web/js/recents.js` / `web/js/release_action_state.js`:
 * `// @ts-check`, ES6 module, JSDoc on exports, pure helpers DOM-free.
 */

import { state } from './state.js';
import { esc, awstDateTime } from './util.js';

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

// --- U3: summary panel + per-row button injection --------------------

/**
 * Render the small per-row inspector button. Used by Browse, Pipeline,
 * and Recents row renderers. Pure / DOM-free / returns the empty string
 * when there is no `pipelineId` (the Browse-tab conditional — releases
 * with no `album_request` row don't get the button).
 *
 * The onclick wires to {@link toggleSearchPlanSummary} via
 * `window.toggleSearchPlanSummary`, which is registered in `main.js`.
 * `event.stopPropagation()` blocks the parent row's expand-on-click so
 * the inspector and the row's own detail panel can be toggled
 * independently.
 *
 * @param {{pipelineId: number|null}} input
 * @returns {string} HTML for a `<button class="sp-button">…</button>`, or `''`.
 */
export function renderSearchPlanButton(input) {
  const id = input.pipelineId;
  if (!Number.isInteger(id) || id == null || id <= 0) return '';
  return `<button class="sp-button" type="button" title="Inspect search plan" aria-label="Inspect search plan" onclick="event.stopPropagation(); window.toggleSearchPlanSummary(${id}, this.closest('.p-item, .r-item, .rg, .release'))">🔍</button>`;
}

/**
 * Format the active plan's status as a small badge.
 *
 * @param {string|null|undefined} status
 * @returns {string}
 */
function planStatusBadge(status) {
  const s = status || 'unknown';
  return `<span class="sp-status sp-status-${esc(s)}">${esc(s)}</span>`;
}

/**
 * Render an inline drift indicator when the request's plan generator id
 * does not match the running `SEARCH_PLAN_GENERATOR_ID`. Exposes both
 * ids so the operator can see what to regenerate to.
 *
 * @param {string|null|undefined} requestPlanGeneratorId
 * @param {string|null|undefined} currentGeneratorId
 * @returns {string}
 */
function renderDriftIndicator(requestPlanGeneratorId, currentGeneratorId) {
  return `<span class="sp-drift" title="The active plan was generated by an older generator id; consider regenerating.">drift: plan=${esc(requestPlanGeneratorId || '?')} current=${esc(currentGeneratorId || '?')}</span>`;
}

/**
 * Render the last-N attempts list for the summary panel. Each row shows
 * outcome + query + relative time. Uses the same `awstDateTime` helper
 * the detail/forensic blocks use elsewhere.
 *
 * @param {Array<Object>} rows
 * @returns {string}
 */
function renderRecentAttempts(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return '<div class="sp-attempts-empty">No attempts yet</div>';
  }
  const items = rows.map((row) => {
    const outcome = row.outcome || '?';
    const query = row.query || '';
    const when = row.created_at ? awstDateTime(row.created_at) : '';
    const consumed = row.attempt_consumed === true ? ' (consumed)' : '';
    return `<div class="sp-attempt sp-attempt-${esc(outcome)}">
      <span class="sp-attempt-outcome">${esc(outcome)}${esc(consumed)}</span>
      <span class="sp-attempt-query">${esc(query)}</span>
      <span class="sp-attempt-when">${esc(when)}</span>
    </div>`;
  }).join('');
  return `<div class="sp-attempts">${items}</div>`;
}

/**
 * Pure HTML producer for the summary panel.
 *
 * Inputs are the {@link fetchInspection} payload and the
 * {@link fetchHistoryPage} payload (with last-3 rows for the summary).
 * Returns one HTML string ready to drop into a `<div class="sp-summary">`.
 *
 * Handles three plan states without crashing:
 *   1. Active plan present → cursor `next_ordinal/total`, cycle, last-N
 *      attempts, drift indicator when generator ids disagree.
 *   2. Active plan present but generator id drift → same plus a visible
 *      drift indicator surfacing both ids (origin R13 / AE4).
 *   3. No active plan (deterministic-failed) → failure class +
 *      sanitised error; slot list omitted.
 *
 * Action buttons render placeholders that click into U2 stubs which
 * throw until U4/U5 land — operator feedback ships with the action,
 * not the surface.
 *
 * @param {{inspection: Object, history: Object}} args
 * @returns {string}
 */
export function renderSummaryPanel(args) {
  const inspection = args.inspection || {};
  const history = args.history || {};
  const requestId = inspection.request_id;
  const request = inspection.request || {};
  const currentness = inspection.currentness || {};
  const activePlan = inspection.active_plan;
  const currentGeneratorId = inspection.current_generator_id;

  const titleLine = `${esc(request.artist_name || '?')} — ${esc(request.album_title || '?')} <span class="sp-ref">#${esc(String(requestId ?? '?'))}</span>`;

  const reqIdAttr = (typeof requestId === 'number' && requestId > 0)
    ? requestId
    : 0;
  const closeOnclick = reqIdAttr
    ? `event.stopPropagation(); window.toggleSearchPlanSummary(${reqIdAttr}, null)`
    : `event.stopPropagation();`;

  const headerActions = `
    <div class="sp-summary-actions">
      <button class="sp-action-button" type="button" onclick="event.stopPropagation(); window.openSearchPlanDetail(${reqIdAttr}, this.closest('.sp-summary'))">Open detail →</button>
      <button class="sp-action-button" type="button" onclick="event.stopPropagation(); window.searchPlanAdvance(${reqIdAttr}, {})">Advance</button>
      <button class="sp-action-button sp-action-button-destructive" type="button" onclick="event.stopPropagation(); window.searchPlanRegenerate(${reqIdAttr})">Regenerate</button>
      <button class="sp-action-button sp-action-close" type="button" title="Close" aria-label="Close" onclick="${closeOnclick}">×</button>
    </div>`;

  // No active plan — render failure class + sanitised error if present.
  if (!activePlan) {
    const failed = inspection.latest_failed_deterministic;
    // Support BOTH shapes: the production API returns a flat plan dict
    // (per `_plan_to_dict`) with `failure_class` / `error_message` at
    // the top level; some tests construct `{plan: {...}}`. Reading both
    // covers both producers without forking code paths.
    const failurePlan = (failed && failed.plan) ? failed.plan : failed;
    const failureClass = (failurePlan && failurePlan.failure_class) || null;
    const failureError = (failurePlan && failurePlan.error_message) || null;

    let body = '';
    if (failureClass || failureError) {
      body = `<div class="sp-failure">
        <div class="sp-failure-class">Plan failure: <strong>${esc(failureClass || 'unknown')}</strong></div>
        ${failureError ? `<div class="sp-failure-error">${esc(failureError)}</div>` : ''}
      </div>`;
    } else {
      // No failure plan recorded either — surface the booleans we have.
      body = `<div class="sp-failure">
        <div class="sp-failure-class">No active plan</div>
        <div class="sp-failure-error">
          has_active_plan=${esc(String(!!currentness.has_active_plan))} ·
          has_deterministic_failure=${esc(String(!!currentness.has_deterministic_failure))} ·
          has_retryable_failure=${esc(String(!!currentness.has_retryable_failure))}
        </div>
      </div>`;
    }
    return `<div class="sp-summary-inner">
      <div class="sp-summary-header">
        <div class="sp-summary-title">${titleLine}</div>
        ${headerActions}
      </div>
      ${body}
      <div class="sp-summary-section">
        <div class="sp-section-label">Recent attempts</div>
        ${renderRecentAttempts(Array.isArray(history.rows) ? history.rows.slice(0, 3) : [])}
      </div>
    </div>`;
  }

  // Active plan present — show plan status, generator, cursor, cycle,
  // attempts, and drift indicator if applicable.
  const plan = activePlan.plan || {};
  const items = Array.isArray(activePlan.items) ? activePlan.items : [];
  const totalSlots = items.length;
  const nextOrdinal = (typeof activePlan.next_ordinal === 'number')
    ? activePlan.next_ordinal
    : '?';
  const cycleCount = (typeof activePlan.cycle_count === 'number')
    ? activePlan.cycle_count
    : '?';
  const planGeneratorId = plan.generator_id;

  const drift = (currentness.generator_id_mismatch === true)
    ? renderDriftIndicator(planGeneratorId, currentGeneratorId)
    : '';

  const status = planStatusBadge(plan.status);

  const attempts = renderRecentAttempts(
    Array.isArray(history.rows) ? history.rows.slice(0, 3) : []);

  return `<div class="sp-summary-inner">
    <div class="sp-summary-header">
      <div class="sp-summary-title">${titleLine}</div>
      ${headerActions}
    </div>
    <div class="sp-summary-meta">
      <span class="sp-summary-meta-item">${status}</span>
      <span class="sp-summary-meta-item">generator ${esc(String(planGeneratorId ?? '?'))}</span>
      ${drift}
      <span class="sp-summary-meta-item">cursor <strong>${esc(String(nextOrdinal))}/${esc(String(totalSlots))}</strong></span>
      <span class="sp-summary-meta-item">cycle <strong>${esc(String(cycleCount))}</strong></span>
    </div>
    <div class="sp-summary-section">
      <div class="sp-section-label">Recent attempts</div>
      ${attempts}
    </div>
  </div>`;
}

/**
 * Toggle the in-place summary panel for a pipeline request.
 *
 * Mirrors `web/js/pipeline.js::toggleDetail`: locate or create a sibling
 * `<div class="sp-summary" id="sp-summary-${requestId}">` adjacent to
 * the row, fetch via `Promise.all([fetchInspection, fetchHistoryPage])`
 * with `limit=3`, render via {@link renderSummaryPanel}, set innerHTML,
 * toggle `.open`. A second click closes the panel without re-fetching.
 *
 * The cache writes happen on success; subsequent open re-fetches (the
 * inspector deliberately refreshes on click per origin R17 / AE10).
 *
 * @param {number} requestId
 * @param {Element|null} [rowEl]  Caller's row element — used to anchor
 *   the summary div as a sibling. When `null` the function uses an
 *   existing `#sp-summary-<id>` if found, else no-ops.
 * @returns {Promise<void>}
 */
export async function toggleSearchPlanSummary(requestId, rowEl) {
  // Touch `state` so future cache-driven reads don't trip "unused import"
  // checks. The U5 mutation handlers will properly use it.
  void state;
  if (!Number.isInteger(requestId) || requestId <= 0) {
    throw new TypeError(
      `toggleSearchPlanSummary: requestId must be a positive integer (got ${JSON.stringify(requestId)})`,
    );
  }

  const panelId = `sp-summary-${requestId}`;
  let panel = /** @type {HTMLElement|null} */ (document.getElementById(panelId));
  if (panel) {
    // Existing panel — toggle closed if open, else proceed to refresh.
    if (panel.classList.contains('open')) {
      panel.classList.remove('open');
      return;
    }
  } else {
    // Create a new panel as a sibling immediately after the row. When no
    // row was supplied we cannot anchor the panel — bail rather than
    // float it at the document root.
    if (!rowEl || !rowEl.parentNode) return;
    panel = document.createElement('div');
    panel.className = 'sp-summary';
    panel.id = panelId;
    rowEl.parentNode.insertBefore(panel, rowEl.nextSibling);
  }

  panel.innerHTML = '<div class="sp-summary-loading">Loading search-plan…</div>';
  panel.classList.add('open');

  try {
    const [inspection, history] = await Promise.all([
      fetchInspection(requestId),
      fetchHistoryPage(requestId, { limit: 3 }),
    ]);
    searchPlanCache.set(requestId, {
      inspection,
      historyHead: Array.isArray(history.rows) ? history.rows.slice(0, 3) : [],
      fetchedAt: Date.now(),
    });
    panel.innerHTML = renderSummaryPanel({ inspection, history });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    panel.innerHTML = `<div class="sp-summary-loading">Failed to load search-plan: ${esc(msg)}</div>`;
  }
}

// --- Action handler stubs ---------------------------------------------
//
// Real implementations land in U4 (openSearchPlanDetail /
// closeSearchPlanDetail) and U5 (searchPlanRegenerate /
// searchPlanAdvance). The window bindings exist now so U4-U5 don't have
// to re-wire `main.js`. Each stub THROWS rather than no-op'ing silently
// — a click on a wired-up button before its unit ships is a developer
// bug, not a UX feature.

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
