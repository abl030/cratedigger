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

// --- U4: detail subview + back button + scroll restore ----------------

/**
 * @typedef {Object} ActiveTabSnapshot
 * @property {string} tab          One of `'browse'`, `'pipeline'`, `'recents'`, `'decisions'`, `'manual'`.
 * @property {string|null} subView Sub-view within the active tab when meaningful.
 * @property {number} scrollY      `window.scrollY` at the snapshot moment.
 */

/**
 * Read the active tab + sub-view + current scroll into a small snapshot.
 *
 * Pure-ish: reads the live DOM and `state` for the lookup but does not
 * mutate them. The caller stamps this onto `state.searchPlanDetailContext`
 * via {@link captureOriginContext} before navigating to the detail page.
 *
 * Tab name mapping mirrors `web/js/main.js` `tabOrder`. We map the
 * visible tab labels to internal names so the back button can route via
 * `showTab(name)` directly.
 *
 * @returns {ActiveTabSnapshot}
 */
export function snapshotActiveTab() {
  const labelToName = {
    browse: 'browse',
    recents: 'recents',
    pipeline: 'pipeline',
    decisions: 'decisions',
    'wrong matches': 'manual',
  };
  /** @type {string} */
  let tab = 'pipeline';
  if (typeof document !== 'undefined' && document.querySelector) {
    const activeEl = document.querySelector('.tab.active');
    const label = (activeEl && activeEl.textContent ? activeEl.textContent.trim().toLowerCase() : '');
    if (label && Object.prototype.hasOwnProperty.call(labelToName, label)) {
      tab = labelToName[/** @type {keyof typeof labelToName} */ (label)];
    }
  }
  /** @type {string|null} */
  let subView = null;
  if (tab === 'pipeline') {
    subView = state.pipelineView ?? 'queue';
  } else if (tab === 'recents') {
    subView = state.recentsSub ?? 'history';
  } else if (tab === 'browse') {
    subView = state.browseSubView ?? null;
  }
  const scrollY = (typeof window !== 'undefined' && typeof window.scrollY === 'number')
    ? window.scrollY
    : 0;
  return { tab, subView, scrollY };
}

/**
 * Open the per-request search-plan detail page under the Pipeline tab.
 *
 * Captures the originating tab + sub-view + scroll position into
 * `state.searchPlanDetailContext` so the back button can restore them,
 * flips `state.pipelineView` to `'search-plan-detail'`, and calls
 * `window.showTab('pipeline')`. The Pipeline render dispatcher reads
 * `state.pipelineView` and routes to {@link renderSearchPlanDetail}.
 *
 * Mutations: `state.searchPlanDetailContext`, `state.pipelineView`.
 *
 * @param {number} requestId   Pipeline request id (positive integer).
 * @param {Element|null} [_originEl]  Caller's row/panel — not used today,
 *   kept for future "scroll back into view" affordances.
 * @returns {void}
 */
// eslint-disable-next-line no-unused-vars
export function openSearchPlanDetail(requestId, _originEl) {
  if (!Number.isInteger(requestId) || requestId <= 0) {
    throw new TypeError(
      `openSearchPlanDetail: requestId must be a positive integer (got ${JSON.stringify(requestId)})`,
    );
  }
  const snap = snapshotActiveTab();
  const ctx = captureOriginContext({
    tab: snap.tab,
    scrollY: snap.scrollY,
    subView: snap.subView,
  });
  // captureOriginContext's stub-shape returns requestId=0 — patch it
  // with the real id (per U2 contract).
  state.searchPlanDetailContext = { ...ctx, requestId };
  state.pipelineView = 'search-plan-detail';
  if (typeof window !== 'undefined') {
    const showTab = /** @type {(name: string) => void} */ (
      /** @type {any} */ (window).showTab);
    if (typeof showTab === 'function') {
      showTab('pipeline');
      return;
    }
    // Fallback: if showTab was not registered (test harness, etc.), fire
    // the render directly.
    void renderSearchPlanDetail(requestId);
  }
}

/**
 * Close the detail page and return to the originating tab + sub-view.
 *
 * Reads `state.searchPlanDetailContext`. When null (operator refreshed,
 * lost the stash, or navigated here directly somehow), falls back to
 * the Pipeline queue view so the operator is never stranded.
 *
 * Mutations: clears `state.searchPlanDetailContext`, restores
 * `state.pipelineView` when the origin tab was Pipeline, schedules a
 * `window.scrollTo` on the next frame.
 *
 * @returns {void}
 */
export function closeSearchPlanDetail() {
  const ctx = state.searchPlanDetailContext;
  if (!ctx) {
    // No stash — fall back to the Pipeline queue without throwing.
    if (typeof console !== 'undefined' && console.warn) {
      console.warn(
        'closeSearchPlanDetail: no origin context — falling back to pipeline/queue',
      );
    }
    state.pipelineView = 'queue';
    if (typeof window !== 'undefined') {
      const showTab = /** @type {(name: string) => void} */ (
        /** @type {any} */ (window).showTab);
      if (typeof showTab === 'function') showTab('pipeline');
    }
    return;
  }
  const { originTab, originSubView, originScrollY } = ctx;
  if (originTab === 'pipeline') {
    state.pipelineView = (originSubView === 'dashboard'
      || originSubView === 'queue')
      ? originSubView
      : 'queue';
  } else {
    // Leave pipelineView alone on non-pipeline origins; the next time
    // the operator opens Pipeline they should land where they were last
    // (queue by default).
    if (state.pipelineView === 'search-plan-detail') {
      state.pipelineView = 'queue';
    }
    if (originTab === 'recents'
      && (originSubView === 'history' || originSubView === 'downloading' || originSubView === 'queue')) {
      state.recentsSub = originSubView;
    }
    if (originTab === 'browse' && typeof originSubView === 'string') {
      state.browseSubView = originSubView;
    }
  }
  state.searchPlanDetailContext = null;
  if (typeof window !== 'undefined') {
    const showTab = /** @type {(name: string) => void} */ (
      /** @type {any} */ (window).showTab);
    if (typeof showTab === 'function') {
      showTab(originTab);
    }
    const scheduler = (typeof window.requestAnimationFrame === 'function')
      ? window.requestAnimationFrame.bind(window)
      : (
        /** @param {() => void} fn */
        function fallback(fn) { setTimeout(fn, 0); return 0; }
      );
    scheduler(() => {
      if (typeof window.scrollTo === 'function') {
        window.scrollTo(0, originScrollY);
      }
    });
  }
}

/**
 * Render an inspector slot list. Pure / DOM-free.
 *
 * Highlights the slot whose ordinal matches `nextOrdinal`. Everything
 * else is rendered with a flat `.sp-slot` class. Returns the empty
 * string when `items` is not a non-empty array.
 *
 * @param {Array<Object>} items
 * @param {number} nextOrdinal
 * @returns {string}
 */
function renderSlotList(items, nextOrdinal) {
  if (!Array.isArray(items) || items.length === 0) {
    return '<div class="sp-slot-list-empty">No slots in plan</div>';
  }
  const rows = items.map((item) => {
    const ordinal = item.ordinal;
    const current = (typeof ordinal === 'number' && ordinal === nextOrdinal);
    const cls = current ? 'sp-slot sp-slot-current' : 'sp-slot';
    const strategy = item.strategy || '?';
    const query = item.query || '';
    const cqk = item.canonical_query_key || '';
    const repeat = item.repeat_group || '';
    return `<li class="${cls}">
      <span class="sp-slot-ordinal">${esc(String(ordinal ?? '?'))}</span>
      <span class="sp-slot-strategy">${esc(strategy)}</span>
      <span class="sp-slot-query">${esc(query)}</span>
      <span class="sp-slot-meta">key=${esc(cqk)}${repeat ? ` · group=${esc(repeat)}` : ''}</span>
    </li>`;
  }).join('');
  return `<ol class="sp-slot-list">${rows}</ol>`;
}

/**
 * Render one row of the plan-aware history table. Pure / DOM-free.
 *
 * The candidates JSONB is rendered into a native `<details>` block per
 * row so the operator can inspect the top-N candidates without a
 * dedicated handler.
 *
 * @param {Object} row
 * @returns {string}
 */
function renderHistoryRow(row) {
  const isLegacy = row.plan_id == null;
  const cls = isLegacy ? 'sp-history-row legacy' : 'sp-history-row';
  const isStale = row.cursor_update_status === 'stale' || (row.stale_reason != null && row.stale_reason !== '');
  const staleCls = isStale ? ' sp-history-row-stale' : '';
  const created = row.created_at ? awstDateTime(row.created_at) : '';
  const candidatesRaw = row.candidates;
  let candidatesJson = '';
  try {
    candidatesJson = candidatesRaw == null
      ? ''
      : JSON.stringify(candidatesRaw, null, 2);
  } catch (err) {
    candidatesJson = String(candidatesRaw);
  }
  const elapsed = (typeof row.elapsed_s === 'number')
    ? row.elapsed_s.toFixed(2) + 's'
    : '—';
  const peers = (typeof row.peers_browsed === 'number')
    ? String(row.peers_browsed + (row.peers_browsed_lazy || 0))
    : '—';
  const fanout = (typeof row.fanout_waves === 'number')
    ? String(row.fanout_waves)
    : '—';
  const ordinal = (row.plan_ordinal == null) ? '—' : String(row.plan_ordinal);
  const strategy = row.plan_strategy || (isLegacy ? '(legacy)' : '—');
  const cycle = (row.plan_cycle_snapshot == null) ? '—' : String(row.plan_cycle_snapshot);
  const consumed = (row.attempt_consumed == null)
    ? '—'
    : (row.attempt_consumed ? 'yes' : 'no');
  return `<tr class="${cls}${staleCls}">
    <td class="sp-history-when">${esc(created)}</td>
    <td class="sp-history-outcome">${esc(row.outcome || '?')}</td>
    <td class="sp-history-strategy">${esc(strategy)}</td>
    <td class="sp-history-ordinal">${esc(ordinal)}</td>
    <td class="sp-history-query"><code>${esc(row.query || '')}</code></td>
    <td class="sp-history-result-count">${esc(String(row.result_count ?? '—'))}</td>
    <td class="sp-history-elapsed">${esc(elapsed)}</td>
    <td class="sp-history-final-state">${esc(row.final_state || '—')}</td>
    <td class="sp-history-cursor-status">${esc(row.cursor_update_status || '—')}</td>
    <td class="sp-history-stale-reason">${esc(row.stale_reason || '—')}</td>
    <td class="sp-history-consumed">${esc(consumed)}</td>
    <td class="sp-history-cycle">${esc(cycle)}</td>
    <td class="sp-history-peers">${esc(peers)}</td>
    <td class="sp-history-fanout">${esc(fanout)}</td>
    <td class="sp-history-candidates">
      ${candidatesJson ? `<details class="sp-candidate-forensics"><summary>candidates</summary><pre>${esc(candidatesJson)}</pre></details>` : '—'}
    </td>
  </tr>`;
}

/**
 * Render the plan-aware history table including the optional "Load
 * older" affordance. Pure / DOM-free.
 *
 * @param {{rows: Array<Object>, nextBeforeId: number|null, requestId: number}} args
 * @returns {string}
 */
function renderHistoryTable(args) {
  const rows = Array.isArray(args.rows) ? args.rows : [];
  if (rows.length === 0) {
    return '<div class="sp-history-empty">No plan-aware attempts yet</div>';
  }
  const body = rows.map(renderHistoryRow).join('');
  const loader = (args.nextBeforeId != null)
    ? `<div class="sp-load-older-wrap">
        <button class="sp-load-older-button" type="button" onclick="event.stopPropagation(); window.searchPlanLoadOlder(${args.requestId}, ${args.nextBeforeId})">Load older</button>
      </div>`
    : '';
  return `<table class="sp-history-table" data-request-id="${args.requestId}">
    <thead>
      <tr>
        <th>When</th>
        <th>Outcome</th>
        <th>Strategy</th>
        <th>Ord</th>
        <th>Query</th>
        <th>#</th>
        <th>Elapsed</th>
        <th>Final state</th>
        <th>Cursor</th>
        <th>Stale</th>
        <th>Consumed</th>
        <th>Cycle</th>
        <th>Peers</th>
        <th>Fanout</th>
        <th>Forensics</th>
      </tr>
    </thead>
    <tbody class="sp-history-tbody">${body}</tbody>
  </table>${loader}`;
}

/**
 * Render the per-slot stats table from the inspection's
 * `stats.current.slots` array. Pure / DOM-free. Always labels the cache
 * attribution as "cycle-level" (origin R11 / AE6) — that label is the
 * only level the current stats tracker emits.
 *
 * @param {Object} stats
 * @returns {string}
 */
function renderSlotStats(stats) {
  if (!stats || typeof stats !== 'object') {
    return '<div class="sp-stats-empty">No stats yet</div>';
  }
  const current = stats.current || {};
  const slots = Array.isArray(current.slots) ? current.slots : [];
  if (slots.length === 0) {
    return '<div class="sp-stats-empty">No per-slot stats yet</div>';
  }
  const rows = slots.map((slot) => {
    const id = slot.identity || {};
    const ordinal = (id.plan_ordinal != null) ? id.plan_ordinal : '—';
    const strategy = id.plan_strategy || '?';
    const attempts = slot.attempts ?? 0;
    const counts = slot.outcome_counts || {};
    const found = Number(counts.found || 0);
    const noMatch = Number(counts.no_match || 0);
    const noResults = Number(counts.no_results || 0);
    const errors = Number(counts.error || 0);
    const consumed = slot.consumed_attempts ?? 0;
    const elapsedMean = (typeof slot.elapsed_s_mean === 'number')
      ? slot.elapsed_s_mean.toFixed(2) + 's'
      : '—';
    const elapsedP95 = (typeof slot.elapsed_s_p95 === 'number')
      ? slot.elapsed_s_p95.toFixed(2) + 's'
      : '—';
    const foundRate = attempts ? (found / attempts) : 0;
    const noMatchRate = attempts ? (noMatch / attempts) : 0;
    return `<tr class="sp-stats-row">
      <td>${esc(String(ordinal))}</td>
      <td>${esc(strategy)}</td>
      <td>${esc(String(attempts))}</td>
      <td>${esc(String(consumed))}</td>
      <td>${esc((foundRate * 100).toFixed(1))}%</td>
      <td>${esc((noMatchRate * 100).toFixed(1))}%</td>
      <td>${esc(String(noResults))}</td>
      <td>${esc(String(errors))}</td>
      <td>${esc(elapsedMean)}</td>
      <td>${esc(elapsedP95)}</td>
    </tr>`;
  }).join('');
  // Origin R11 / AE6: every cache stat label must read "cycle-level".
  // The stats tracker only emits cycle-level cache attribution; the
  // label is hardcoded to make the policy visible.
  const cacheLabel = current.cache_attribution_level || 'cycle-level';
  return `<div class="sp-stats-cache-label">Cache attribution: cycle-level (raw=${esc(cacheLabel)})</div>
  <table class="sp-stats-table">
    <thead>
      <tr>
        <th>Ord</th>
        <th>Strategy</th>
        <th>Attempts</th>
        <th>Consumed</th>
        <th>Found rate</th>
        <th>No-match rate</th>
        <th>No-results</th>
        <th>Errors</th>
        <th>Elapsed mean</th>
        <th>Elapsed p95</th>
      </tr>
    </thead>
    <tbody>${rows}</tbody>
  </table>`;
}

/**
 * Render the plan-health deep block — failure class, sanitised error,
 * and the active plan's provenance metadata. Pure / DOM-free.
 *
 * @param {Object} inspection
 * @returns {string}
 */
function renderPlanHealth(inspection) {
  const failedDet = inspection.latest_failed_deterministic;
  const failedTrans = inspection.latest_failed_transient;
  const activePlan = inspection.active_plan;
  const provenance = (activePlan && activePlan.plan && activePlan.plan.provenance)
    ? activePlan.plan.provenance
    : {};

  const renderFailure = (failure, label) => {
    if (!failure) return '';
    const plan = failure.plan ? failure.plan : failure;
    const klass = (plan && plan.failure_class) || 'unknown';
    const errMsg = (plan && plan.error_message) || '';
    const ts = plan && plan.created_at ? awstDateTime(plan.created_at) : '';
    return `<div class="sp-health-failure">
      <div class="sp-health-failure-label">${esc(label)} <span class="sp-health-failure-class">${esc(klass)}</span></div>
      ${ts ? `<div class="sp-health-failure-when">${esc(ts)}</div>` : ''}
      ${errMsg ? `<pre class="sp-health-failure-error">${esc(errMsg)}</pre>` : ''}
    </div>`;
  };

  // Provenance — show omitted candidates, deduped losers, dropped
  // low-entropy tokens so the operator can see why the active plan
  // ended up the size it is.
  const provKeys = ['omitted_candidates', 'deduped_losers', 'dropped_low_entropy_tokens'];
  /** @type {string[]} */
  const provLines = [];
  for (const key of provKeys) {
    const val = provenance[key];
    if (val == null) continue;
    let rendered;
    try {
      rendered = JSON.stringify(val, null, 2);
    } catch (err) {
      rendered = String(val);
    }
    provLines.push(`<div class="sp-health-prov-row">
      <span class="sp-health-prov-key">${esc(key)}</span>
      <pre class="sp-health-prov-val">${esc(rendered)}</pre>
    </div>`);
  }
  const provHtml = provLines.length
    ? `<div class="sp-health-provenance">${provLines.join('')}</div>`
    : '<div class="sp-health-provenance-empty">No provenance flags recorded</div>';

  const detHtml = renderFailure(failedDet, 'Deterministic failure');
  const transHtml = renderFailure(failedTrans, 'Transient failure');
  const failureBlock = (detHtml || transHtml)
    ? `<div class="sp-health-failures">${detHtml}${transHtml}</div>`
    : '<div class="sp-health-failures-empty">No recent failures</div>';

  return `<div class="sp-detail-section sp-health">
    <div class="sp-section-label">Plan health</div>
    ${failureBlock}
    <div class="sp-health-prov-label">Active plan provenance</div>
    ${provHtml}
  </div>`;
}

/**
 * Render the legacy `plan_id IS NULL` history collapsed by default.
 * Pure / DOM-free.
 *
 * @param {Object|null|undefined} legacyLogs
 * @returns {string}
 */
function renderLegacyHistory(legacyLogs) {
  const head = (legacyLogs && Array.isArray(legacyLogs.head)) ? legacyLogs.head : [];
  const count = (legacyLogs && typeof legacyLogs.count === 'number') ? legacyLogs.count : head.length;
  if (head.length === 0 && count === 0) {
    return `<div class="sp-detail-section sp-history-legacy-section">
      <div class="sp-section-label">Pre-rollout history (legacy)</div>
      <div class="sp-history-empty">No legacy attempts</div>
    </div>`;
  }
  // Legacy rows have a slim shape — outcome, variant, query, result count,
  // elapsed, final state. Render in the same table structure as plan-aware
  // rows but mark each row .legacy so CSS can dim them.
  const rows = head.map((row) => {
    const created = row.created_at ? awstDateTime(row.created_at) : '';
    const elapsed = (typeof row.elapsed_s === 'number')
      ? row.elapsed_s.toFixed(2) + 's'
      : '—';
    return `<tr class="sp-history-row legacy">
      <td class="sp-history-when">${esc(created)}</td>
      <td class="sp-history-outcome">${esc(row.outcome || '?')}</td>
      <td class="sp-history-strategy">${esc(row.variant || '(legacy)')}</td>
      <td class="sp-history-query"><code>${esc(row.query || '')}</code></td>
      <td class="sp-history-result-count">${esc(String(row.result_count ?? '—'))}</td>
      <td class="sp-history-elapsed">${esc(elapsed)}</td>
      <td class="sp-history-final-state">${esc(row.final_state || '—')}</td>
    </tr>`;
  }).join('');
  const summary = `Pre-rollout history (${count} row${count === 1 ? '' : 's'}; showing ${head.length})`;
  return `<div class="sp-detail-section sp-history-legacy-section">
    <details class="sp-history-legacy">
      <summary class="sp-section-label">${esc(summary)}</summary>
      <table class="sp-history-table sp-history-table-legacy">
        <thead>
          <tr>
            <th>When</th>
            <th>Outcome</th>
            <th>Variant</th>
            <th>Query</th>
            <th>#</th>
            <th>Elapsed</th>
            <th>Final state</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </details>
  </div>`;
}

/**
 * Pure HTML producer for the per-request detail page.
 *
 * Inputs:
 *   * `inspection` — `GET /search-plan` payload (plan + items + cursor +
 *     stats + legacy_logs head + currentness + failure summary).
 *   * `history` — array of newest-first `search_log` rows from
 *     `GET /search-plan/history` (plan-aware shape).
 *   * `nextBeforeId` — cursor seed for "Load older"; `null` when
 *     exhausted.
 *
 * Sections in order:
 *   1. Header (back, title, status, drift, cursor, cycle, refresh)
 *   2. Plan slot list
 *   3. Plan-aware history table + Load-older button
 *   4. Per-slot stats
 *   5. Plan health (failure classes + provenance)
 *   6. Pre-rollout history (collapsed)
 *
 * @param {{inspection: Object, history: Array<Object>, nextBeforeId: number|null}} args
 * @returns {string}
 */
export function renderDetailPage(args) {
  const inspection = args.inspection || {};
  const history = Array.isArray(args.history) ? args.history : [];
  const nextBeforeId = args.nextBeforeId == null ? null : args.nextBeforeId;
  const requestId = inspection.request_id;
  const reqIdAttr = (typeof requestId === 'number' && requestId > 0)
    ? requestId
    : 0;
  const request = inspection.request || {};
  const currentness = inspection.currentness || {};
  const activePlan = inspection.active_plan;
  const currentGeneratorId = inspection.current_generator_id;

  const titleLine = `${esc(request.artist_name || '?')} — ${esc(request.album_title || '?')} <span class="sp-ref">#${esc(String(requestId ?? '?'))}</span>`;

  const plan = (activePlan && activePlan.plan) ? activePlan.plan : {};
  const items = (activePlan && Array.isArray(activePlan.items)) ? activePlan.items : [];
  const totalSlots = items.length;
  const nextOrdinal = (activePlan && typeof activePlan.next_ordinal === 'number')
    ? activePlan.next_ordinal
    : 0;
  const cycleCount = (activePlan && typeof activePlan.cycle_count === 'number')
    ? activePlan.cycle_count
    : 0;
  const planGeneratorId = plan.generator_id;
  const planStatus = plan.status || (activePlan ? 'active' : '—');

  const drift = (currentness.generator_id_mismatch === true)
    ? renderDriftIndicator(planGeneratorId, currentGeneratorId)
    : '';

  const headerActions = `
    <div class="sp-detail-header-actions">
      <button class="sp-back-button" type="button" onclick="event.stopPropagation(); window.closeSearchPlanDetail()">← Back</button>
      <button class="sp-action-button" type="button" onclick="event.stopPropagation(); window.searchPlanRefreshDetail(${reqIdAttr})">Refresh</button>
      <button class="sp-action-button" type="button" onclick="event.stopPropagation(); window.searchPlanAdvance(${reqIdAttr}, {})">Advance</button>
      <button class="sp-action-button sp-action-button-destructive" type="button" onclick="event.stopPropagation(); window.searchPlanRegenerate(${reqIdAttr})">Regenerate</button>
    </div>`;

  const headerMeta = activePlan
    ? `<div class="sp-detail-meta">
        <span class="sp-summary-meta-item">${planStatusBadge(planStatus)}</span>
        <span class="sp-summary-meta-item">generator ${esc(String(planGeneratorId ?? '?'))}</span>
        ${drift}
        <span class="sp-summary-meta-item">cursor <strong>${esc(String(nextOrdinal))}/${esc(String(totalSlots))}</strong></span>
        <span class="sp-summary-meta-item">cycle <strong>${esc(String(cycleCount))}</strong></span>
      </div>`
    : `<div class="sp-detail-meta">
        <span class="sp-summary-meta-item sp-status sp-status-failed_deterministic">no active plan</span>
        ${drift}
      </div>`;

  const slotSection = activePlan
    ? `<div class="sp-detail-section">
        <div class="sp-section-label">Plan slots (${totalSlots})</div>
        ${renderSlotList(items, nextOrdinal)}
      </div>`
    : '';

  const historySection = `<div class="sp-detail-section">
    <div class="sp-section-label">Plan-aware attempts (${history.length})</div>
    ${renderHistoryTable({ rows: history, nextBeforeId, requestId: reqIdAttr })}
  </div>`;

  const statsSection = `<div class="sp-detail-section">
    <div class="sp-section-label">Per-slot stats</div>
    ${renderSlotStats(inspection.stats || {})}
  </div>`;

  const healthSection = renderPlanHealth(inspection);
  const legacySection = renderLegacyHistory(inspection.legacy_logs);

  return `<div class="sp-detail" data-request-id="${reqIdAttr}">
    <div class="sp-detail-header">
      <div class="sp-detail-header-left">
        <button class="sp-back-button" type="button" onclick="event.stopPropagation(); window.closeSearchPlanDetail()">← Back</button>
        <div class="sp-detail-title">${titleLine}</div>
      </div>
      ${headerActions}
    </div>
    ${headerMeta}
    ${slotSection}
    ${historySection}
    ${statsSection}
    ${healthSection}
    ${legacySection}
  </div>`;
}

/**
 * Async detail-page renderer — fetches inspection + history page in
 * parallel, builds the full HTML via {@link renderDetailPage}, and
 * paints into `#pipeline-content`.
 *
 * Pipeline.js's render dispatcher routes here when
 * `state.pipelineView === 'search-plan-detail'`.
 *
 * Side effects: writes `#pipeline-content.innerHTML`, populates
 * `searchPlanCache[requestId]`.
 *
 * @param {number} requestId
 * @returns {Promise<void>}
 */
export async function renderSearchPlanDetail(requestId) {
  if (!Number.isInteger(requestId) || requestId <= 0) {
    throw new TypeError(
      `renderSearchPlanDetail: requestId must be a positive integer (got ${JSON.stringify(requestId)})`,
    );
  }
  const el = (typeof document !== 'undefined')
    ? /** @type {HTMLElement|null} */ (document.getElementById('pipeline-content'))
    : null;
  if (el) {
    el.innerHTML = '<div class="sp-detail-loading">Loading search-plan…</div>';
  }
  try {
    const [inspection, historyPayload] = await Promise.all([
      fetchInspection(requestId),
      fetchHistoryPage(requestId, { limit: HISTORY_PAGE_DEFAULT_LIMIT }),
    ]);
    const rows = Array.isArray(historyPayload.rows) ? historyPayload.rows : [];
    const nextBeforeId = historyPayload.next_before_id == null
      ? null
      : historyPayload.next_before_id;
    searchPlanCache.set(requestId, {
      inspection,
      historyHead: rows.slice(0, 3),
      fetchedAt: Date.now(),
    });
    const html = renderDetailPage({ inspection, history: rows, nextBeforeId });
    if (el) el.innerHTML = html;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (el) {
      el.innerHTML = `<div class="sp-detail-loading">Failed to load search-plan: ${esc(msg)}</div>`;
    }
  }
}

/**
 * "Refresh" button binding — re-runs {@link renderSearchPlanDetail}.
 * Bound to `window.searchPlanRefreshDetail` in `main.js`.
 *
 * @param {number} requestId
 * @returns {Promise<void>}
 */
export async function searchPlanRefreshDetail(requestId) {
  invalidateSearchPlanCache(searchPlanCache, requestId);
  await renderSearchPlanDetail(requestId);
}

/**
 * "Load older" button binding — fetches the next history page using
 * the in-memory cursor and appends rows to the existing
 * `<tbody class="sp-history-tbody">`. Updates the cursor on the wrapper
 * button so subsequent clicks page further back, or removes the button
 * when the page exhausts.
 *
 * Mirrors `web/js/wrong-matches.js::removeWrongMatchGroup` for in-place
 * DOM mutation that preserves scroll.
 *
 * @param {number} requestId
 * @param {number} beforeId
 * @returns {Promise<void>}
 */
export async function searchPlanLoadOlder(requestId, beforeId) {
  if (!Number.isInteger(requestId) || requestId <= 0) {
    throw new TypeError(
      `searchPlanLoadOlder: requestId must be a positive integer (got ${JSON.stringify(requestId)})`,
    );
  }
  if (typeof document === 'undefined') return;
  const tbody = /** @type {HTMLElement|null} */ (
    document.querySelector(`.sp-history-table[data-request-id="${requestId}"] .sp-history-tbody`));
  if (!tbody) return;
  const wrap = /** @type {HTMLElement|null} */ (
    document.querySelector(`.sp-history-table[data-request-id="${requestId}"] ~ .sp-load-older-wrap`)
    || tbody.closest('.sp-detail')?.querySelector('.sp-load-older-wrap')
    || null);
  try {
    const page = await fetchHistoryPage(requestId, {
      limit: HISTORY_PAGE_DEFAULT_LIMIT,
      beforeId,
    });
    const rows = Array.isArray(page.rows) ? page.rows : [];
    if (rows.length > 0) {
      const html = rows.map(renderHistoryRow).join('');
      tbody.insertAdjacentHTML('beforeend', html);
    }
    const nextBeforeId = page.next_before_id == null ? null : page.next_before_id;
    if (wrap) {
      if (nextBeforeId == null) {
        wrap.remove();
      } else {
        wrap.innerHTML = `<button class="sp-load-older-button" type="button" onclick="event.stopPropagation(); window.searchPlanLoadOlder(${requestId}, ${nextBeforeId})">Load older</button>`;
      }
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (wrap) {
      wrap.innerHTML = `<div class="sp-load-older-error">Failed to load older rows: ${esc(msg)}</div>`;
    }
  }
}

// --- U5 stubs ---------------------------------------------------------
//
// Real implementations land in U5. Window bindings exist now so the
// summary + detail surfaces don't have to re-wire `main.js`.

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
