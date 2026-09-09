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

import { state, toast } from './state.js';
import { esc, awstDate, awstTime, awstDateTime } from './util.js';
import { isTabName, tabHasAsyncRender } from './tabs.js';

/**
 * Default number of `search_log` rows fetched per history page.
 *
 * WARNING: This value MUST match `HISTORY_PAGE_DEFAULT_LIMIT` in
 * `lib/search_plan_service.py` (currently line 93). These two constants
 * are intentionally duplicated — the Python value is the source of truth.
 * If you change the Python constant, update this constant to match and
 * search for all JS call sites that pass `HISTORY_PAGE_DEFAULT_LIMIT` as
 * a `limit` option to `fetchHistoryPage` / `buildHistoryUrl`.
 *
 * A cleaner approach would be to include the server's default in the
 * inspection payload (`GET /search-plan` → `history_page_default_limit`)
 * and read it at fetch time, falling back to 50 for older deployments.
 * That wiring was deferred because it would require touching
 * `lib/search_plan_inspection.py`, `web/routes/search_plan.py`, and
 * multiple JS call sites in the same PR.
 */
export const HISTORY_PAGE_DEFAULT_LIMIT = 50;

/**
 * Maximum age (ms) for a cached search-plan inspection entry.
 *
 * Entries older than this are treated as misses so long-running sessions
 * don't serve stale plan state indefinitely. 30 s is intentionally short
 * relative to the 5-minute pipeline cycle: the operator sees fresh data
 * after half a cycle at most, and the TTL is far enough out to avoid
 * redundant fetches on rapid back-and-forth navigation.
 */
export const CACHE_TTL_MS = 30_000;

/**
 * Maximum number of request-id entries kept in {@link searchPlanCache}.
 *
 * When the cache would exceed this size, the oldest entry (Map insertion
 * order = LRU since we always re-insert on write) is evicted first.
 */
const CACHE_MAX_ENTRIES = 50;

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
 * Module-scoped set of request ids whose summary fetch is currently in
 * flight. Used to dedup concurrent {@link toggleSearchPlanSummary} calls
 * (operator double-click, refresh-during-open, etc.) so we don't dispatch
 * multiple fetches and don't insert duplicate `<div class="sp-summary">`
 * sibling elements.
 *
 * @type {Set<number>}
 */
const summaryInFlight = new Set();

/**
 * Module-scoped set of request ids whose regenerate/advance POST is
 * currently in flight. Same pattern as {@link summaryInFlight} — guards
 * against rapid double-clicks queueing concurrent mutations.
 *
 * @type {Set<number>}
 */
const mutationInFlight = new Set();

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
 * @property {string|null} subView  Active sub-view (e.g. `'dashboard'` / `'long-tail'` on Pipeline). `null` for tabs with no sub-view.
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
 * tab switching. The actual `showTab` call happens in U4's
 * `closeSearchPlanDetail`; the `window.scrollTo` call happens there too
 * for a destination with no follow-up render (browse), or later, inside
 * the destination's own render function, via
 * {@link consumePendingScrollRestore}. This helper just exposes the data
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
 * Read a cache entry, treating it as a miss (and deleting it) when stale.
 *
 * Staleness is determined by {@link CACHE_TTL_MS} relative to
 * `Date.now()`. Accepts an optional `now` parameter so tests can control
 * the clock without mocking `Date`.
 *
 * @param {Map<number, SearchPlanCacheEntry>} cache
 * @param {number} requestId
 * @param {number} [now]  Override for `Date.now()`. Defaults to the real clock.
 * @returns {SearchPlanCacheEntry|undefined}
 */
export function getCacheEntry(cache, requestId, now) {
  const entry = cache.get(requestId);
  if (!entry) return undefined;
  const age = (now !== undefined ? now : Date.now()) - entry.fetchedAt;
  if (age > CACHE_TTL_MS) {
    cache.delete(requestId);
    return undefined;
  }
  return entry;
}

/**
 * Write a cache entry, evicting the oldest entry first when the cache is
 * at capacity ({@link CACHE_MAX_ENTRIES}).
 *
 * Map preserves insertion order so the first key returned by
 * `cache.keys()` is always the oldest (LRU) entry.
 *
 * @param {Map<number, SearchPlanCacheEntry>} cache
 * @param {number} requestId
 * @param {SearchPlanCacheEntry} entry
 */
export function setCacheEntry(cache, requestId, entry) {
  // Delete first so a re-insert moves the key to the end (freshest).
  cache.delete(requestId);
  if (cache.size >= CACHE_MAX_ENTRIES) {
    const oldest = cache.keys().next().value;
    if (oldest !== undefined) cache.delete(oldest);
  }
  cache.set(requestId, entry);
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
  // `latest_failed_deterministic` is the flat plan dict produced by
  // `lib/search_plan_inspection.py::_plan_to_dict` (`failure_class` /
  // `error_message` at the top level) — read it directly.
  if (!activePlan) {
    const failurePlan = inspection.latest_failed_deterministic;
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
  if (!Number.isInteger(requestId) || requestId <= 0) {
    throw new TypeError(
      `toggleSearchPlanSummary: requestId must be a positive integer (got ${JSON.stringify(requestId)})`,
    );
  }

  // Concurrent-call guard: a fetch is already in flight for this id.
  // The first call will write into the panel; the duplicate call from
  // (e.g.) a rapid second click is a no-op.
  if (summaryInFlight.has(requestId)) return;

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

  summaryInFlight.add(requestId);
  try {
    const [inspection, history] = await Promise.all([
      fetchInspection(requestId),
      fetchHistoryPage(requestId, { limit: 3 }),
    ]);
    // Closed-mid-fetch guard: if the panel was removed from the DOM
    // (operator clicked Close, dismissed a tab, etc.) skip the write so
    // we don't repaint a stale surface.
    const livePanel = /** @type {HTMLElement|null} */ (
      document.getElementById(panelId));
    if (livePanel !== panel) return;
    setCacheEntry(searchPlanCache, requestId, {
      inspection,
      historyHead: Array.isArray(history.rows) ? history.rows.slice(0, 3) : [],
      fetchedAt: Date.now(),
    });
    panel.innerHTML = renderSummaryPanel({ inspection, history });
  } catch (err) {
    // Same closed-mid-fetch guard on the error path.
    const livePanel = /** @type {HTMLElement|null} */ (
      document.getElementById(panelId));
    if (livePanel !== panel) return;
    const msg = err instanceof Error ? err.message : String(err);
    panel.innerHTML = `<div class="sp-summary-loading">Failed to load search-plan: ${esc(msg)}</div>`;
  } finally {
    summaryInFlight.delete(requestId);
  }
}

// --- U4: detail subview + back button + scroll restore ----------------

/**
 * @typedef {Object} ActiveTabSnapshot
 * @property {string} tab          A `tabs.js` internal tab name (`'browse'`, `'recents'`, `'pipeline'`, or `'manual'`).
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
 * The active tab's `data-tab-name` attribute IS the internal name
 * (`tabs.js` is the one owner of that mapping — see its module doc), so
 * there is no label to reverse-map here; an unknown or missing attribute
 * falls back to `'pipeline'`, same as before this attribute existed.
 *
 * @returns {ActiveTabSnapshot}
 */
export function snapshotActiveTab() {
  /** @type {string} */
  let tab = 'pipeline';
  if (typeof document !== 'undefined' && document.querySelector) {
    const activeEl = document.querySelector('.tab.active');
    const name = (activeEl && activeEl.getAttribute) ? activeEl.getAttribute('data-tab-name') : null;
    if (name && isTabName(name)) {
      tab = name;
    }
  }
  /** @type {string|null} */
  let subView = null;
  if (tab === 'pipeline') {
    subView = state.pipelineView ?? 'dashboard';
  } else if (tab === 'recents') {
    subView = state.recentsSub ?? 'history';
  }
  const scrollY = (typeof window !== 'undefined' && typeof window.scrollY === 'number')
    ? window.scrollY
    : 0;
  return { tab, subView, scrollY };
}

/**
 * Module-scoped scroll position awaiting restoration once the
 * destination tab's own render finishes.
 *
 * `closeSearchPlanDetail` stashes the origin scroll position here
 * instead of guessing how long the destination's re-render will take.
 * The destination's own render function ({@link consumePendingScrollRestore}'s
 * callers: `loadPipeline`, `loadRecents`, `loadWrongMatches`) reads and
 * clears it at the true end of its own render, so the restore always
 * lands after the DOM it is scrolling against, never before.
 *
 * @type {number | null}
 */
let pendingScrollRestore = null;

/**
 * Stash a scroll position for the next destination render to consume.
 *
 * @param {number} y
 * @returns {void}
 */
function stashScrollRestore(y) {
  pendingScrollRestore = y;
}

/**
 * Apply and clear a stashed scroll restoration, if one is pending.
 *
 * Called by a destination tab's render function at the exact end of its
 * own render (every branch and every early return), so a restore that
 * was never stashed is a no-op and one that was stashed always fires
 * after the DOM it targets is in its final layout. Never call this
 * before the destination has finished writing its own DOM.
 *
 * @returns {void}
 */
export function consumePendingScrollRestore() {
  if (pendingScrollRestore === null) return;
  const y = pendingScrollRestore;
  pendingScrollRestore = null;
  if (typeof window !== 'undefined' && typeof window.scrollTo === 'function') {
    window.scrollTo(0, y);
  }
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
    // Prefer the F12-aware showTab wrapper that preserves
    // `state.pipelineView === 'search-plan-detail'` across the tab
    // switch. Fall back to plain `showTab` for environments that don't
    // expose the wrapper, then to a direct render for test harnesses.
    const showTabPreserving = /** @type {(name: string) => void} */ (
      /** @type {any} */ (window).showTabPreservingDetail);
    if (typeof showTabPreserving === 'function') {
      showTabPreserving('pipeline');
      return;
    }
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
 * the Pipeline dashboard so the operator is never stranded.
 *
 * Mutations: clears `state.searchPlanDetailContext`, restores
 * `state.pipelineView` when the origin tab was Pipeline, stashes the
 * origin scroll position via {@link consumePendingScrollRestore}'s
 * module-scoped pending value for the destination's own render to
 * apply (or applies it immediately for a destination with no
 * follow-up render of its own).
 *
 * @returns {void}
 */
export function closeSearchPlanDetail() {
  // Bump the detail-page generation counter so any in-flight
  // `renderSearchPlanDetail` call skips its paint when it resolves —
  // covers the operator-clicks-Back-during-fetch race.
  bumpDetailGeneration();
  const ctx = state.searchPlanDetailContext;
  if (!ctx) {
    // No stash — fall back to the Pipeline dashboard without throwing.
    if (typeof console !== 'undefined' && console.warn) {
      console.warn(
        'closeSearchPlanDetail: no origin context — falling back to pipeline/dashboard',
      );
    }
    state.pipelineView = 'dashboard';
    if (typeof window !== 'undefined') {
      const showTab = /** @type {(name: string) => void} */ (
        /** @type {any} */ (window).showTab);
      if (typeof showTab === 'function') showTab('pipeline');
    }
    return;
  }
  const { tab, scrollY, subView } = restoreOriginContext(ctx);
  if (tab === 'pipeline') {
    state.pipelineView = (subView === 'dashboard'
      || subView === 'long-tail')
      ? subView
      : 'dashboard';
  } else {
    // Leave pipelineView alone on non-pipeline origins; the next time
    // the operator opens Pipeline they should land where they were last
    // (dashboard by default).
    if (state.pipelineView === 'search-plan-detail') {
      state.pipelineView = 'dashboard';
    }
    if (tab === 'recents'
      && (subView === 'history' || subView === 'acquisition' || subView === 'imports')) {
      state.recentsSub = subView;
    }
  }
  state.searchPlanDetailContext = null;
  if (typeof window !== 'undefined') {
    const showTab = /** @type {(name: string) => void} */ (
      /** @type {any} */ (window).showTab);
    // Stash before calling showTab, not after: `loadPipeline`,
    // `loadRecents`, and `loadWrongMatches` each consume-and-clear this
    // at the true end of their own render, so the restore lands after
    // the destination's DOM is in its final layout instead of racing a
    // fixed frame count against it (previously 5 chained
    // `requestAnimationFrame` ticks — a heuristic that a slow device or
    // a render awaiting I/O could still beat). Any other destination —
    // browse, or an unmapped tab name — gets no follow-up render from
    // `showTab` at all, so nothing else will ever consume the stash;
    // apply it immediately in that case.
    stashScrollRestore(scrollY);
    const destinationConsumesRestore = typeof showTab === 'function'
      && tabHasAsyncRender(tab);
    if (typeof showTab === 'function') {
      showTab(tab);
    }
    if (!destinationConsumesRestore) {
      consumePendingScrollRestore();
    }
  }
}

/**
 * Fetch the per-request pipeline payload that supplies the detail
 * page's "In the library now" column.
 *
 * Never throws: the search plan is meaningful whether or not Beets is
 * reachable, so a failure here degrades that one column rather than the
 * page. `GET /api/pipeline/<id>` answers 503 when the current Beets
 * authority is unavailable, which is exactly that case.
 *
 * @param {number} requestId
 * @returns {Promise<Object|null>} Parsed body, or `null` on any failure.
 */
async function fetchRequestLibrary(requestId) {
  try {
    const resp = await fetch(`/api/pipeline/${requestId}`);
    if (!resp.ok) return null;
    return await resp.json();
  } catch (err) {
    return null;
  }
}

/**
 * Which Attempts rows the detail page shows: `'interesting'` (the
 * default) or `'all'`. Module-scoped rather than on `state.js` because
 * nothing outside this module reads it and it is not part of any
 * cross-module contract — the detail page owns its own view mode.
 *
 * @type {'interesting'|'all'}
 */
let attemptsFilter = 'interesting';

/**
 * The last painted detail page's inputs, so the Interesting/All toggle
 * re-renders from memory instead of re-fetching three endpoints.
 * `searchPlanLoadOlder` appends to `rows` so a toggle after paging back
 * still sees every loaded row.
 *
 * @type {{requestId: number, inspection: Object, rows: Array<Object>, nextBeforeId: number|null, library: Object|null} | null}
 */
let detailSnapshot = null;

// --- #811: the detail page --------------------------------------------
//
// The page answers one question first — "is the quality override
// holding?" — and keeps forensics one click away. Section order:
// header, meta, Searching for (scope + library), Is the override
// holding?, Plan (slots merged with their tallies), Attempts, Plan
// health, and the pre-rollout legacy block only when it has rows.

/** The tier ladder chip appended after the configured tiers. */
const CATCH_ALL_TIER_LABEL = 'any';

/**
 * Turn a snake_case producer token into readable prose. Mechanical, so
 * a token no map anticipated still renders as words rather than as a
 * fallback branch nothing can reach.
 *
 * @param {unknown} token
 * @returns {string}
 */
function humanizeToken(token) {
  return String(token == null ? '' : token).replace(/_/g, ' ').trim();
}

/**
 * The first clause of an error message — everything up to the first
 * sentence-ending `.` or `;`. Grab errors carry a short cause followed
 * by retry bookkeeping the operator does not need inline.
 *
 * @param {unknown} message
 * @returns {string}
 */
function firstClause(message) {
  const s = String(message == null ? '' : message).trim();
  if (!s) return '';
  const cut = s.search(/[;.](\s|$)/);
  return (cut > 0 ? s.slice(0, cut) : s).trim();
}

/**
 * `MM-DD HH:MM` in AWST — the short form the dense tables use.
 *
 * @param {string|null|undefined} iso
 * @returns {string}
 */
function shortWhen(iso) {
  return iso ? awstDateTime(iso).slice(5) : '';
}

/**
 * Round a duration to whole seconds with a unit suffix. Non-numeric
 * input renders as the empty string rather than a dash — an absent
 * tally is blank in these tables, never a placeholder.
 *
 * @param {unknown} value  A duration in seconds.
 * @returns {string}
 */
function wholeSeconds(value) {
  return (typeof value === 'number' && Number.isFinite(value))
    ? `${Math.round(value)}s`
    : '';
}

/**
 * A small coloured chip.
 *
 * @param {string} text
 * @param {'good'|'warn'|'bad'|'dim'} tone
 * @param {string} [title]
 * @returns {string}
 */
function chip(text, tone, title) {
  const attr = title ? ` title="${esc(title)}"` : '';
  return `<span class="sp-chip sp-chip-${esc(tone)}"${attr}>${esc(text)}</span>`;
}

/**
 * Render the detail page's generator-drift chip.
 *
 * The generator id itself is noise on a current plan, so the detail
 * page never prints it; when the ids disagree the operator needs to
 * know that and what to regenerate to, so both ids ride in the chip's
 * `title`. `renderDriftIndicator` keeps its own inline both-ids form
 * for the summary panel, which is unchanged by issue #811.
 *
 * @param {string|null|undefined} planGeneratorId
 * @param {string|null|undefined} currentGeneratorId
 * @returns {string}
 */
function renderDriftChip(planGeneratorId, currentGeneratorId) {
  return chip(
    'plan generator out of date',
    'warn',
    `plan generator ${planGeneratorId || '?'} · current generator ${currentGeneratorId || '?'} — regenerate to pick up the current generator`,
  );
}

/**
 * Render the tier-ladder chips for the "Searching for" column.
 *
 * Ladder order is `configured_tiers` (the deployment's own ladder), with
 * any active tier the configuration does not list appended so an
 * override that names an unconfigured tier is still visible. A chip is
 * lit when the tier is in `tiers`; the trailing `any` chip is lit iff
 * `catch_all`.
 *
 * @param {Object} scope
 * @returns {string}
 */
function renderTierChips(scope) {
  const active = Array.isArray(scope.tiers) ? scope.tiers.map(String) : [];
  const configured = Array.isArray(scope.configured_tiers)
    ? scope.configured_tiers.map(String)
    : [];
  /** @type {string[]} */
  const ladder = [];
  for (const tier of configured.concat(active)) {
    if (!ladder.includes(tier)) ladder.push(tier);
  }
  const chips = ladder.map((tier) => (active.includes(tier)
    ? `<span class="sp-tier sp-tier-on">${esc(tier)}</span>`
    : `<span class="sp-tier">${esc(tier)}</span>`));
  chips.push(scope.catch_all === true
    ? `<span class="sp-tier sp-tier-on">${CATCH_ALL_TIER_LABEL}</span>`
    : `<span class="sp-tier">${CATCH_ALL_TIER_LABEL}</span>`);
  return `<div class="sp-tiers">${chips.join('')}</div>`;
}

/**
 * Where the effective tier ladder came from — `search_scope.source`.
 *
 * @param {unknown} source
 * @returns {string}
 */
function scopeSourceLabel(source) {
  if (source === 'override') return 'from the request search override';
  if (source === 'target_format') return 'from the target format';
  if (source === 'config') return 'the configured default ladder';
  return humanizeToken(source);
}

/**
 * Render the left "Searching for" column: tier chips plus the three
 * rows the operator opened the page to check.
 *
 * @param {Object} scope  `inspection.search_scope`.
 * @returns {string}
 */
function renderScopeColumn(scope) {
  const why = [scopeSourceLabel(scope.source)];
  if (scope.catch_all !== true) why.push('catch-all excluded');
  const overrideValue = scope.override
    ? `<code>${esc(String(scope.override))}</code>`
    : '<span class="sp-kv-why">none set</span>';
  const floorValue = (typeof scope.min_bitrate === 'number')
    ? `${esc(String(scope.min_bitrate))} kbps`
    : '<span class="sp-kv-why">none set</span>';
  const targetValue = scope.target_format
    ? `<code>${esc(String(scope.target_format))}</code>`
    : '<span class="sp-kv-why">none set</span>';
  return `<div class="sp-scope-col">
    ${renderTierChips(scope)}
    <dl class="sp-kv">
      <dt>Override</dt><dd>${overrideValue} <span class="sp-kv-why">· ${esc(why.join('; '))}</span></dd>
      <dt>Bitrate floor</dt><dd>${floorValue}</dd>
      <dt>Target format</dt><dd>${targetValue}</dd>
    </dl>
  </div>`;
}

/**
 * Render the right "In the library now" column from the per-request
 * pipeline payload (`GET /api/pipeline/<id>`).
 *
 * `library` is `null` when that fetch failed — the column says so and
 * the rest of the page still renders, because the search plan does not
 * depend on Beets being reachable.
 *
 * @param {Object|null} library
 * @returns {string}
 */
function renderLibraryColumn(library) {
  const label = '<div class="sp-scope-label">In the library now</div>';
  if (!library || typeof library !== 'object') {
    return `<div class="sp-scope-col">${label}
      <div class="sp-have-unavailable">library state unavailable</div>
    </div>`;
  }
  const current = library.current_library || {};
  const request = library.request || {};
  if (current.state !== 'unique') {
    const state = typeof current.state === 'string' ? current.state : 'unknown';
    return `<div class="sp-scope-col">${label}
      <div class="sp-have-unavailable">${esc(humanizeToken(state))}</div>
    </div>`;
  }
  const fmt = request.final_format
    ? String(request.final_format)
    : 'format unrecorded';
  const floor = (typeof request.min_bitrate === 'number')
    ? ` · floor ${request.min_bitrate} kbps`
    : '';
  /** @type {string[]} */
  const proof = [];
  if (request.current_spectral_grade) proof.push(String(request.current_spectral_grade));
  if (request.verified_lossless === true) proof.push('verified lossless');
  const history = Array.isArray(library.history) ? library.history : [];
  const imported = history.find((row) => row && row.outcome === 'success') || null;
  const from = imported
    ? `${esc(String(imported.soulseek_username || 'unknown peer'))} · imported ${esc(awstDate(imported.created_at))}`
    : '<span class="sp-kv-why">no recorded import</span>';
  const scenario = request.beets_scenario
    ? `<div class="sp-have-scenario">beets match ${esc(String(request.beets_scenario))}</div>`
    : '';
  return `<div class="sp-scope-col">${label}
    <div class="sp-have">
      <span class="sp-have-tag">HAVE</span>
      <span class="sp-have-fmt">${esc(fmt)}${esc(floor)}</span>
      <span class="sp-have-spec">${esc(proof.join(' · '))}</span>
      <span class="sp-have-tag">FROM</span>
      <span class="sp-have-fmt">${from}</span>
      <span class="sp-have-path">${esc(String(current.path || ''))}</span>
    </div>
    ${scenario}
  </div>`;
}

/**
 * One row of the "Is the override holding?" block.
 *
 * @param {boolean} ok
 * @param {string} bodyHtml  Pre-escaped inner HTML.
 * @returns {string}
 */
function checkRow(ok, bodyHtml) {
  const cls = ok ? 'sp-check sp-check-ok' : 'sp-check sp-check-att';
  return `<div class="${cls}"><span class="sp-check-mark">${ok ? '✓' : '!'}</span><span>${bodyHtml}</span></div>`;
}

/**
 * Render the "Is the override holding?" block from `acquisition`.
 *
 * Every row whose data is absent or empty is omitted rather than
 * rendered as a dash, and the whole block disappears when no row
 * survives — an empty checklist says nothing an operator can act on.
 *
 * @param {Object|null|undefined} acquisition
 * @param {Object} scope  `inspection.search_scope`, for naming the
 *   tiers a candidate landed on that the scope excludes.
 * @returns {string}
 */
function renderOverrideChecks(acquisition, scope) {
  if (!acquisition || typeof acquisition !== 'object') return '';
  /** @type {string[]} */
  const rows = [];

  const candidateTiers = Array.isArray(acquisition.candidate_tiers)
    ? acquisition.candidate_tiers
    : [];
  if (candidateTiers.length > 0) {
    const total = candidateTiers.reduce(
      (sum, entry) => sum + (Number(entry && entry.count) || 0), 0);
    const outside = Number(acquisition.candidates_outside_scope) || 0;
    const inScope = Array.isArray(scope.tiers) ? scope.tiers.map(String) : [];
    const offending = candidateTiers
      .map((entry) => String(entry && entry.tier))
      .filter((tier) => !inScope.includes(tier));
    const tally = candidateTiers
      .map((entry) => `${Number(entry && entry.count) || 0} ${esc(String(entry && entry.tier))}`)
      .join(' · ');
    rows.push(outside === 0
      ? checkRow(true,
        `<strong>${esc(String(total))}</strong> candidates scored · ${tally} · every scored folder was in scope`)
      : checkRow(false,
        `<strong>${esc(String(outside))}</strong> of <strong>${esc(String(total))}</strong> candidates scored outside the scope · ${tally}${offending.length ? ` · off-scope: ${offending.map(esc).join(', ')}` : ''}`));
  }

  const grabs = Array.isArray(acquisition.grabs) ? acquisition.grabs : [];
  if (grabs.length > 0) {
    const total = (typeof acquisition.grabs_total === 'number')
      ? acquisition.grabs_total
      : grabs.reduce((sum, g) => sum + (Number(g && g.count) || 0), 0);
    const allSucceeded = grabs.every((g) => g && g.last_outcome === 'success');
    const tally = grabs.map((g) => {
      const outcome = g && g.last_outcome
        ? ` (last ${esc(humanizeToken(g.last_outcome))}${g.last_at ? ` ${esc(shortWhen(g.last_at))}` : ''})`
        : '';
      return `<strong>${esc(String(Number(g && g.count) || 0))}</strong> ${esc(String(g && g.filetype))}${outcome}`;
    }).join(' · ');
    rows.push(checkRow(allSucceeded,
      `<strong>${esc(String(total))}</strong> grabs · ${tally}`));
  }

  const lastFound = acquisition.last_found;
  if (lastFound && typeof lastFound === 'object') {
    const grab = lastFound.grab;
    const grabOk = !!(grab && grab.outcome === 'success');
    const grabText = grab
      ? ` → grab ${esc(humanizeToken(grab.outcome))}${grab.error_message ? ` · ${esc(firstClause(grab.error_message))}` : ''}`
      : ' → no linked grab';
    rows.push(checkRow(grabOk,
      `Last found: <span class="sp-check-peer">${esc(String(lastFound.username || '?'))}</span> `
      + `${esc(String(lastFound.tier || '?'))} `
      + `<strong>${esc(String(lastFound.matched_tracks ?? '?'))}/${esc(String(lastFound.total_tracks ?? '?'))}</strong> `
      + `via ${esc(String(lastFound.strategy || '?'))} `
      + `<span class="sp-check-when">${esc(shortWhen(lastFound.at))}</span>${grabText}`));
  }

  const peers = Array.isArray(acquisition.peers) ? acquisition.peers : [];
  if (peers.length > 0) {
    const line = peers.map((peer) => (
      `<span class="sp-check-peer">${esc(String(peer && peer.username))}</span> `
      + `${esc(String((peer && peer.tier) || '?'))} `
      + `${esc(String((peer && peer.best_matched_tracks) ?? '?'))}/${esc(String((peer && peer.total_tracks) ?? '?'))} `
      + `<span class="sp-check-when">×${esc(String((peer && peer.attempts) ?? '?'))}</span>`
    )).join(' · ');
    rows.push(checkRow(true, `Peers seen: ${line}`));
  }

  if (rows.length === 0) return '';
  const since = (acquisition.since && acquisition.since_reason === 'last_import')
    ? `since import ${esc(awstDate(acquisition.since))}`
    : 'all history';
  return `<div class="sp-detail-section">
    <div class="sp-section-label">Is the override holding? <span class="sp-section-sub">${since}</span></div>
    <div class="sp-checks">${rows.join('')}</div>
  </div>`;
}

/**
 * Render the Plan table: every slot of the active plan with its own
 * tallies merged in.
 *
 * Stats are matched to a slot on the production identity keys —
 * `identity.plan_id` and `identity.ordinal`. The pre-#811 renderer read
 * `identity.plan_ordinal`, a key no producer writes, so every tally
 * showed as a dash. A slot with no stats yet renders with blank
 * tallies; the `plan_id` half of the match keeps a superseded plan's
 * bucket out of the active plan's rows.
 *
 * @param {{items: Array<Object>, statsSlots: Array<Object>, nextOrdinal: number, planId: unknown}} args
 * @returns {string}
 */
function renderPlanTable(args) {
  const items = Array.isArray(args.items) ? args.items : [];
  if (items.length === 0) {
    return '<div class="sp-attempts-empty">No slots in plan</div>';
  }
  const statsSlots = Array.isArray(args.statsSlots) ? args.statsSlots : [];
  const rows = items.map((item) => {
    const ordinal = item.ordinal;
    const slot = statsSlots.find((candidate) => {
      const identity = (candidate && candidate.identity) || {};
      return identity.plan_id === args.planId && identity.ordinal === ordinal;
    }) || null;
    const counts = (slot && slot.outcome_counts) || {};
    const tally = (value) => (slot ? `<td class="sp-n">${esc(String(Number(value) || 0))}</td>` : '<td class="sp-n"></td>');
    const found = Number(counts.found) || 0;
    const foundCell = slot
      ? `<td class="sp-n ${found > 0 ? 'sp-plan-found' : 'sp-plan-zero'}">${esc(String(found))}</td>`
      : '<td class="sp-n"></td>';
    const cls = (typeof ordinal === 'number' && ordinal === args.nextOrdinal)
      ? 'sp-plan-current'
      : '';
    return `<tr class="${cls}">
      <td class="sp-plan-ord">${esc(String(ordinal ?? '?'))}</td>
      <td class="sp-plan-strat">${esc(String(item.strategy || '?'))}</td>
      <td class="sp-plan-q">${esc(String(item.query || ''))}</td>
      ${tally(slot && slot.attempts)}
      ${foundCell}
      ${tally(counts.no_match)}
      ${tally(counts.no_results)}
      <td class="sp-n">${esc(slot ? wholeSeconds(slot.elapsed_s_mean) : '')}</td>
      <td class="sp-att-when">${esc(slot ? shortWhen(slot.last_seen_at) : '')}</td>
    </tr>`;
  }).join('');
  return `<div class="sp-tablewrap"><table class="sp-plan-table">
    <thead><tr>
      <th>#</th><th>Strategy</th><th>Query</th>
      <th class="sp-n">Tried</th><th class="sp-n">Found</th>
      <th class="sp-n">No match</th><th class="sp-n">Empty</th>
      <th class="sp-n">Avg</th><th>Last</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table></div>`;
}

/**
 * Whether one attempt row is worth showing in the default Attempts view.
 *
 * The unfiltered history is dominated by `no_match` / `no_results` rows
 * that scored nothing — 300 of them can sit between the operator and
 * the handful of attempts that actually found a folder. A row is
 * interesting when it found something, scored any candidate the
 * pre-filter did not skip, ended on any other outcome, went stale, or
 * did not consume its plan slot.
 *
 * @param {Object|null|undefined} row  One `search_log` row.
 * @returns {boolean}
 */
export function isInterestingAttempt(row) {
  if (!row || typeof row !== 'object') return false;
  const outcome = typeof row.outcome === 'string' ? row.outcome : '';
  if (outcome === 'found') return true;
  if (outcome !== 'no_match' && outcome !== 'no_results') return true;
  if (Array.isArray(row.candidates)
    && row.candidates.some((c) => c && c.pre_filter_skip !== true)) {
    return true;
  }
  if (row.cursor_update_status === 'stale') return true;
  if (typeof row.stale_reason === 'string' && row.stale_reason !== '') return true;
  if (row.attempt_consumed === false) return true;
  return false;
}

/**
 * Pick the candidate worth naming in an attempt's Candidates cell: the
 * best-matched folder the pre-filter actually scored.
 *
 * @param {Array<Object>} candidates
 * @returns {Object|null}
 */
function bestScoredCandidate(candidates) {
  /** @type {Object|null} */
  let best = null;
  for (const candidate of candidates) {
    if (!candidate || candidate.pre_filter_skip === true) continue;
    if (best === null) { best = candidate; continue; }
    const lhs = [Number(candidate.matched_tracks) || 0, Number(candidate.avg_ratio) || 0];
    const rhs = [Number(best.matched_tracks) || 0, Number(best.avg_ratio) || 0];
    if (lhs[0] > rhs[0] || (lhs[0] === rhs[0] && lhs[1] > rhs[1])) best = candidate;
  }
  return best;
}

/**
 * Render the Candidates cell of one attempt row: the best scored
 * folder, why it was rejected, and — when the search is linked to a
 * download — what the grab it produced actually did.
 *
 * @param {Object} row
 * @returns {string}
 */
function renderAttemptCandidates(row) {
  const candidates = Array.isArray(row.candidates) ? row.candidates : [];
  const scored = candidates.filter((c) => c && c.pre_filter_skip !== true);
  const best = bestScoredCandidate(candidates);
  /** @type {string[]} */
  const parts = [];
  if (best) {
    const repeats = scored.filter((c) => c.username === best.username).length;
    const ratio = Number(best.avg_ratio) || 0;
    parts.push(
      `<span class="sp-att-peer">${esc(String(best.username || '?'))}</span> `
      + `${esc(String(best.filetype || '?'))} `
      + `${esc(String(best.matched_tracks ?? '?'))}/${esc(String(best.total_tracks ?? '?'))}`
      + (ratio > 0 ? ` · ratio ${esc(ratio.toFixed(2))}` : '')
      + (repeats > 1 ? ` &times;${esc(String(repeats))}` : ''));
  }
  if (row.rejection_reason) {
    parts.push(`<span class="sp-att-rej">${esc(humanizeToken(row.rejection_reason))}</span>`);
  }
  if (typeof row.grab_outcome === 'string' && row.grab_outcome !== '') {
    const ok = row.grab_outcome === 'success';
    const detail = firstClause(row.grab_error_message);
    parts.push(`<span class="sp-att-arrow">→</span> `
      + `<span class="sp-att-grab${ok ? ' sp-att-grab-ok' : ''}">`
      + `grab ${esc(humanizeToken(row.grab_outcome))}${detail ? ` · ${esc(detail)}` : ''}</span>`);
  }
  return parts.join(' ');
}

/**
 * Render one attempt row. Abnormal facts are chips on the line; every
 * other telemetry column the pre-#811 table clipped off the right edge
 * lives behind the row's own `raw` expander.
 *
 * @param {Object} row
 * @returns {string}
 */
function renderAttemptRow(row) {
  const outcome = typeof row.outcome === 'string' ? row.outcome : '?';
  const found = outcome === 'found';
  const isStale = row.cursor_update_status === 'stale'
    || (typeof row.stale_reason === 'string' && row.stale_reason !== '');
  const finalState = typeof row.final_state === 'string' ? row.final_state : '';
  /** @type {string[]} */
  const abnormal = [];
  if (isStale) abnormal.push(chip('stale', 'warn', row.stale_reason || 'cursor update was stale'));
  if (row.attempt_consumed === false) abnormal.push(chip('not consumed', 'warn'));
  if (finalState && !finalState.startsWith('Completed')) {
    abnormal.push(chip(finalState, 'warn'));
  }
  const rawLines = [
    `query: ${row.query || ''}`,
    `cycle: ${row.plan_cycle_snapshot ?? ''}`,
    `cursor status: ${row.cursor_update_status || ''}`,
    `stale reason: ${row.stale_reason || ''}`,
    `consumed: ${row.attempt_consumed}`,
    `final state: ${finalState}`,
    `peers browsed: ${(Number(row.peers_browsed) || 0) + (Number(row.peers_browsed_lazy) || 0)}`,
    `fanout waves: ${row.fanout_waves ?? ''}`,
    `grab: ${row.grab_download_log_id ?? '—'} ${row.grab_outcome || ''} ${row.grab_filetype || ''} ${row.grab_soulseek_username || ''}`,
  ].join('\n');
  let candidatesJson = '';
  try {
    candidatesJson = row.candidates == null
      ? ''
      : JSON.stringify(row.candidates, null, 2);
  } catch (err) {
    candidatesJson = String(row.candidates);
  }
  return `<tr class="${found ? 'sp-att-found' : ''}">
    <td class="sp-att-when">${esc(shortWhen(row.created_at))}</td>
    <td>${chip(humanizeToken(outcome), found ? 'good' : 'dim')}</td>
    <td class="sp-att-strat">${esc(String(row.plan_strategy || row.variant || '?'))}${abnormal.join('')}</td>
    <td class="sp-n">${esc(String(row.result_count ?? ''))}</td>
    <td class="sp-att-cands">${renderAttemptCandidates(row)}</td>
    <td class="sp-n">${esc(wholeSeconds(row.elapsed_s))}</td>
    <td><details class="sp-att-raw"><summary>raw</summary><pre>${esc(rawLines)}${candidatesJson ? `\n\ncandidates: ${esc(candidatesJson)}` : ''}</pre></details></td>
  </tr>`;
}

/**
 * Render the Attempts section: the Interesting/All toggle, the filtered
 * rows, and the Load-older affordance.
 *
 * @param {{rows: Array<Object>, nextBeforeId: number|null, requestId: number, filter: string}} args
 * @returns {string}
 */
function renderAttemptsSection(args) {
  const rows = Array.isArray(args.rows) ? args.rows : [];
  const interesting = args.filter !== 'all';
  const shown = interesting ? rows.filter(isInterestingAttempt) : rows;
  const button = (mode, label) => {
    const on = (mode === 'all') === (args.filter === 'all');
    return `<button class="sp-filter-button${on ? ' sp-filter-button-on' : ''}" type="button" onclick="event.stopPropagation(); window.searchPlanSetAttemptsFilter(${args.requestId}, '${mode}')">${label}</button>`;
  };
  const filters = `<span class="sp-filters">${button('interesting', 'Interesting')}${button('all', 'All')}</span>`;
  const label = `<div class="sp-section-label">Attempts <span class="sp-section-sub">${esc(String(shown.length))} of ${esc(String(rows.length))} loaded</span>${filters}</div>`;
  if (rows.length === 0) {
    return `<div class="sp-detail-section sp-attempts-section">${label}
      <div class="sp-attempts-empty">No attempts yet</div>
    </div>`;
  }
  const body = shown.map(renderAttemptRow).join('');
  const loader = (args.nextBeforeId != null)
    ? `<div class="sp-load-older-wrap">
        <button class="sp-load-older-button" type="button" onclick="event.stopPropagation(); window.searchPlanLoadOlder(${args.requestId}, ${args.nextBeforeId})">Load older</button>
      </div>`
    : '';
  return `<div class="sp-detail-section sp-attempts-section">${label}
    <div class="sp-tablewrap"><table class="sp-attempts-table" data-request-id="${args.requestId}">
      <thead><tr>
        <th>When</th><th>Outcome</th><th>Strategy</th>
        <th class="sp-n">Results</th><th>Candidates</th>
        <th class="sp-n">Took</th><th></th>
      </tr></thead>
      <tbody class="sp-attempts-tbody">${body}</tbody>
    </table></div>${loader}
  </div>`;
}

/**
 * Render the active plan's provenance as one sentence: what the
 * generator left out, grouped by reason, plus any low-entropy tokens it
 * dropped. Grouping is mechanical over whatever reasons the generator
 * actually wrote, so a new reason renders as words rather than falling
 * into a branch that names it wrongly.
 *
 * @param {Object} provenance
 * @returns {string}
 */
function renderProvenanceSentence(provenance) {
  /** @type {string[]} */
  const clauses = [];
  const omitted = Array.isArray(provenance.omitted_candidates)
    ? provenance.omitted_candidates
    : [];
  if (omitted.length > 0) {
    /** @type {Map<string, number>} */
    const byReason = new Map();
    for (const entry of omitted) {
      const reason = humanizeToken((entry && entry.reason) || 'unspecified');
      byReason.set(reason, (byReason.get(reason) || 0) + 1);
    }
    const groups = Array.from(byReason.entries())
      .map(([reason, count]) => `${count} ${reason}`);
    clauses.push(`Left out: ${esc(groups.join(', '))}.`);
  }
  const dropped = Array.isArray(provenance.dropped_low_entropy_tokens)
    ? provenance.dropped_low_entropy_tokens
    : [];
  if (dropped.length > 0) {
    clauses.push(`Dropped low-entropy tokens: ${esc(dropped.map(String).join(', '))}.`);
  }
  return clauses.length ? `<span>${clauses.join(' ')}</span>` : '';
}

/**
 * Render the Plan health block: one quiet line, the provenance
 * sentence, and the raw provenance behind an expander.
 *
 * @param {Object} inspection
 * @param {string} driftHtml  The drift chip, or `''` when current.
 * @returns {string}
 */
function renderPlanHealth(inspection, driftHtml) {
  const activePlan = inspection.active_plan;
  const provenance = (activePlan && activePlan.plan && activePlan.plan.provenance)
    ? activePlan.plan.provenance
    : {};
  /** @type {string[]} */
  const facts = [];
  const failures = [
    ['Deterministic failure', inspection.latest_failed_deterministic],
    ['Transient failure', inspection.latest_failed_transient],
  ];
  for (const [label, failure] of failures) {
    if (!failure) continue;
    const klass = failure.failure_class || 'unknown';
    const detail = failure.error_message ? ` — ${esc(String(failure.error_message))}` : '';
    const when = failure.created_at ? ` (${esc(awstDateTime(failure.created_at))})` : '';
    facts.push(`<span class="sp-health-bad">${esc(String(label))}: ${esc(String(klass))}${detail}${when}</span>`);
  }
  if (facts.length === 0) {
    facts.push('<span class="sp-health-ok">No plan failures</span>');
  }
  facts.push(driftHtml || 'generator current');
  const superseded = (typeof inspection.superseded_count === 'number')
    ? inspection.superseded_count
    : 0;
  facts.push(`${esc(String(superseded))} superseded plan${superseded === 1 ? '' : 's'}`);

  let provenanceJson = '';
  try {
    provenanceJson = JSON.stringify(provenance, null, 2);
  } catch (err) {
    provenanceJson = String(provenance);
  }
  return `<div class="sp-detail-section">
    <div class="sp-section-label">Plan health</div>
    <div class="sp-health">
      <div class="sp-health-line">${facts.join(' · ')}</div>
      ${renderProvenanceSentence(provenance)}
      <details class="sp-att-raw"><summary>raw provenance</summary><pre>${esc(provenanceJson)}</pre></details>
    </div>
  </div>`;
}

/**
 * Render the pre-rollout (`plan_id IS NULL`) history, collapsed.
 * Returns the empty string when there are no legacy rows at all — an
 * empty collapsed section is pure noise on a page that exists to make
 * one question findable.
 *
 * @param {Object|null|undefined} legacyLogs
 * @returns {string}
 */
function renderLegacyHistory(legacyLogs) {
  const head = (legacyLogs && Array.isArray(legacyLogs.head)) ? legacyLogs.head : [];
  const count = (legacyLogs && typeof legacyLogs.count === 'number')
    ? legacyLogs.count
    : head.length;
  if (count === 0 && head.length === 0) return '';
  const rows = head.map((row) => {
    const created = row.created_at ? awstDateTime(row.created_at) : '';
    return `<tr class="sp-history-row legacy">
      <td class="sp-att-when">${esc(created)}</td>
      <td>${esc(row.outcome || '?')}</td>
      <td>${esc(row.variant || '(legacy)')}</td>
      <td><code>${esc(row.query || '')}</code></td>
      <td class="sp-n">${esc(String(row.result_count ?? ''))}</td>
      <td class="sp-n">${esc(wholeSeconds(row.elapsed_s))}</td>
      <td>${esc(row.final_state || '')}</td>
    </tr>`;
  }).join('');
  const summary = `Pre-rollout history (${count} row${count === 1 ? '' : 's'}; showing ${head.length})`;
  return `<div class="sp-detail-section sp-history-legacy-section">
    <details class="sp-history-legacy">
      <summary class="sp-section-label">${esc(summary)}</summary>
      <div class="sp-tablewrap"><table class="sp-history-table sp-history-table-legacy">
        <thead><tr>
          <th>When</th><th>Outcome</th><th>Variant</th><th>Query</th>
          <th class="sp-n">#</th><th class="sp-n">Elapsed</th><th>Final state</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table></div>
    </details>
  </div>`;
}

/**
 * Pure HTML producer for the per-request detail page.
 *
 * Inputs:
 *   * `inspection` — `GET /api/pipeline/<id>/search-plan`.
 *   * `history` — newest-first plan-aware `search_log` rows.
 *   * `nextBeforeId` — Load-older cursor; `null` when exhausted.
 *   * `library` — `GET /api/pipeline/<id>`, or `null` when that fetch
 *     failed. Supplies the "In the library now" column only.
 *
 * Sections, in order: header, meta, Searching for (scope + library),
 * Is the override holding?, Plan, Attempts, Plan health, pre-rollout
 * legacy history (omitted at zero rows).
 *
 * Reads one piece of module state, {@link attemptsFilter}, so the
 * Interesting/All toggle can repaint the whole page without threading a
 * view mode through every caller.
 *
 * @param {{inspection: Object, history: Array<Object>, nextBeforeId: number|null, library?: Object|null}} args
 * @returns {string}
 */
export function renderDetailPage(args) {
  const inspection = args.inspection || {};
  const history = Array.isArray(args.history) ? args.history : [];
  const nextBeforeId = args.nextBeforeId == null ? null : args.nextBeforeId;
  const library = args.library == null ? null : args.library;
  const requestId = inspection.request_id;
  const reqIdAttr = (typeof requestId === 'number' && requestId > 0) ? requestId : 0;
  const request = inspection.request || {};
  const currentness = inspection.currentness || {};
  const activePlan = inspection.active_plan;
  const scope = inspection.search_scope || {};

  const titleLine = `${esc(request.artist_name || '?')} — ${esc(request.album_title || '?')} <span class="sp-ref">#${esc(String(requestId ?? '?'))}</span>`;
  const statusChip = request.status
    ? `<span class="sp-status sp-status-${esc(String(request.status))}">${esc(String(request.status))}</span>`
    : '';

  const plan = (activePlan && activePlan.plan) ? activePlan.plan : {};
  const items = (activePlan && Array.isArray(activePlan.items)) ? activePlan.items : [];
  const nextOrdinal = (activePlan && typeof activePlan.next_ordinal === 'number')
    ? activePlan.next_ordinal
    : 0;
  const cycleCount = (activePlan && typeof activePlan.cycle_count === 'number')
    ? activePlan.cycle_count
    : 0;
  const drift = (currentness.generator_id_mismatch === true)
    ? renderDriftChip(plan.generator_id, inspection.current_generator_id)
    : '';

  /** @type {string[]} */
  const meta = [];
  meta.push(`<span class="sp-summary-meta-item">plan ${planStatusBadge(plan.status || (activePlan ? 'active' : 'none'))}</span>`);
  if (activePlan) {
    meta.push(`<span class="sp-summary-meta-item">cursor <strong>${esc(String(nextOrdinal))}/${esc(String(items.length))}</strong></span>`);
    meta.push(`<span class="sp-summary-meta-item">cycle <strong>${esc(String(cycleCount))}</strong></span>`);
  }
  if (typeof request.search_attempts === 'number') {
    const since = request.created_at ? ` since ${esc(awstDate(request.created_at))}` : '';
    meta.push(`<span class="sp-summary-meta-item"><strong>${esc(String(request.search_attempts))}</strong> attempts${since}</span>`);
  }
  if (request.last_attempt_at) {
    meta.push(`<span class="sp-summary-meta-item">last search <strong>${esc(awstDateTime(request.last_attempt_at))}</strong></span>`);
  }
  if (request.next_retry_after) {
    meta.push(`<span class="sp-summary-meta-item">next eligible <strong>${esc(awstTime(request.next_retry_after))}</strong></span>`);
  }
  if (drift) meta.push(drift);

  const scopeSection = `<div class="sp-detail-section">
    <div class="sp-section-label">Searching for</div>
    <div class="sp-scope">
      ${renderScopeColumn(scope)}
      ${renderLibraryColumn(library)}
    </div>
  </div>`;

  const statsSlots = (inspection.stats
    && inspection.stats.current
    && Array.isArray(inspection.stats.current.slots))
    ? inspection.stats.current.slots
    : [];

  const planSection = activePlan
    ? `<div class="sp-detail-section">
        <div class="sp-section-label">Plan <span class="sp-section-sub">${esc(String(items.length))} slots · current slot highlighted · tallies for this plan only</span></div>
        ${renderPlanTable({ items, statsSlots, nextOrdinal, planId: plan.id })}
      </div>`
    : '';

  return `<div class="sp-detail" data-request-id="${reqIdAttr}">
    <div class="sp-detail-header">
      <div class="sp-detail-header-left">
        <button class="sp-back-button" type="button" onclick="event.stopPropagation(); window.closeSearchPlanDetail()">← Back</button>
        <span class="sp-detail-title">${titleLine}</span>
        ${statusChip}
      </div>
      <div class="sp-detail-header-actions">
        <button class="sp-action-button" type="button" onclick="event.stopPropagation(); window.searchPlanRefreshDetail(${reqIdAttr})">Refresh</button>
        <button class="sp-action-button" type="button" onclick="event.stopPropagation(); window.searchPlanAdvance(${reqIdAttr}, {})">Advance</button>
        <button class="sp-action-button sp-action-button-destructive" type="button" onclick="event.stopPropagation(); window.searchPlanRegenerate(${reqIdAttr})">Regenerate</button>
      </div>
    </div>
    <div class="sp-detail-meta">${meta.join('')}</div>
    ${scopeSection}
    ${renderOverrideChecks(inspection.acquisition, scope)}
    ${planSection}
    ${renderAttemptsSection({
      rows: history,
      nextBeforeId,
      requestId: reqIdAttr,
      filter: attemptsFilter,
    })}
    ${renderPlanHealth(inspection, drift)}
    ${renderLegacyHistory(inspection.legacy_logs)}
  </div>`;
}

/**
 * Module-scoped generation counter for detail-page renders. Bumped at
 * the start of every {@link renderSearchPlanDetail} call so that an
 * in-flight render whose generation is now stale (e.g. operator clicked
 * Back, opened a different request, or kicked Refresh mid-fetch) cannot
 * clobber the visible surface when its fetches finally resolve.
 *
 * @type {number}
 */
let detailGeneration = 0;

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
  // Generation guard — only the most-recent render owns the paint. If
  // another render/back/refresh bumps `detailGeneration` while we await,
  // we skip the writes after the await resolves.
  const gen = ++detailGeneration;
  const el = (typeof document !== 'undefined')
    ? /** @type {HTMLElement|null} */ (document.getElementById('pipeline-content'))
    : null;
  if (el) {
    el.innerHTML = '<div class="sp-detail-loading">Loading search-plan…</div>';
  }
  try {
    const [inspection, historyPayload, library] = await Promise.all([
      fetchInspection(requestId),
      fetchHistoryPage(requestId, { limit: HISTORY_PAGE_DEFAULT_LIMIT }),
      fetchRequestLibrary(requestId),
    ]);
    if (gen !== detailGeneration) return;
    const rows = Array.isArray(historyPayload.rows) ? historyPayload.rows : [];
    const nextBeforeId = historyPayload.next_before_id == null
      ? null
      : historyPayload.next_before_id;
    setCacheEntry(searchPlanCache, requestId, {
      inspection,
      historyHead: rows.slice(0, 3),
      fetchedAt: Date.now(),
    });
    detailSnapshot = { requestId, inspection, rows, nextBeforeId, library };
    const html = renderDetailPage({
      inspection, history: rows, nextBeforeId, library,
    });
    if (el) el.innerHTML = html;
  } catch (err) {
    if (gen !== detailGeneration) return;
    const msg = err instanceof Error ? err.message : String(err);
    if (el) {
      el.innerHTML = `<div class="sp-detail-loading">Failed to load search-plan: ${esc(msg)}</div>`;
    }
  }
}

/**
 * Interesting/All toggle for the Attempts table. Bound to
 * `window.searchPlanSetAttemptsFilter` in `main.js`.
 *
 * Repaints from {@link detailSnapshot} — deliberately no fetch: the
 * rows are already loaded, and re-fetching would lose whatever the
 * operator paged in with Load older.
 *
 * @param {number} requestId
 * @param {string} mode  `'interesting'` or `'all'`.
 * @returns {void}
 */
export function searchPlanSetAttemptsFilter(requestId, mode) {
  if (mode !== 'interesting' && mode !== 'all') return;
  attemptsFilter = mode;
  const snapshot = detailSnapshot;
  if (!snapshot || snapshot.requestId !== requestId) return;
  if (typeof document === 'undefined') return;
  const el = /** @type {HTMLElement|null} */ (
    document.getElementById('pipeline-content'));
  if (!el) return;
  el.innerHTML = renderDetailPage({
    inspection: snapshot.inspection,
    history: snapshot.rows,
    nextBeforeId: snapshot.nextBeforeId,
    library: snapshot.library,
  });
}

/**
 * Bump the detail-page generation counter without rendering. Operator
 * Back-button clicks call this so any in-flight detail render skips its
 * paint when it resolves.
 *
 * @returns {void}
 */
function bumpDetailGeneration() {
  detailGeneration++;
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
    document.querySelector(`.sp-attempts-table[data-request-id="${requestId}"] .sp-attempts-tbody`));
  if (!tbody) return;
  const wrap = /** @type {HTMLElement|null} */ (
    document.querySelector(`.sp-attempts-table[data-request-id="${requestId}"] ~ .sp-load-older-wrap`)
    || tbody.closest('.sp-detail')?.querySelector('.sp-load-older-wrap')
    || null);
  // Double-click guard: synchronously disable the button so a rapid
  // second click can't dispatch a duplicate fetch (rows would otherwise
  // be inserted twice into the tbody). Re-enabled in `finally` only when
  // the wrap is still around — exhaustion or error replaces the markup
  // outright, so leaving the disabled flag on a removed node is fine.
  const button = /** @type {HTMLButtonElement|null} */ (
    wrap ? wrap.querySelector('button.sp-load-older-button') : null);
  if (button) {
    if (button.disabled) return;
    button.disabled = true;
  }
  try {
    const page = await fetchHistoryPage(requestId, {
      limit: HISTORY_PAGE_DEFAULT_LIMIT,
      beforeId,
    });
    const rows = Array.isArray(page.rows) ? page.rows : [];
    // Appended rows respect the active filter, and the snapshot keeps
    // every loaded row so switching to All later shows them without a
    // second fetch.
    const visible = attemptsFilter === 'all'
      ? rows
      : rows.filter(isInterestingAttempt);
    if (visible.length > 0) {
      tbody.insertAdjacentHTML('beforeend', visible.map(renderAttemptRow).join(''));
    }
    const nextBeforeId = page.next_before_id == null ? null : page.next_before_id;
    if (detailSnapshot && detailSnapshot.requestId === requestId) {
      detailSnapshot.rows = detailSnapshot.rows.concat(rows);
      detailSnapshot.nextBeforeId = nextBeforeId;
    }
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

// --- U5: regenerate / advance action handlers -----------------------
//
// Both handlers wrap the same service-layer methods the CLI uses
// (`SearchPlanService.generate_for_request`, `.advance_for_request`).
// Surfaces:
//   * regenerate: native `confirm()` (mirrors `confirm()` usage in
//     `analysis.js::disambRemove`, `pipeline.js`, `wrong-matches.js`).
//     Confirmation message includes both "cursor" and "cycle" per
//     origin R15 / AE8.
//   * advance: inline form rendered into the open summary panel or
//     detail page replacing the action toolbar (since advance needs
//     two-mode entry — strategy prefix XOR ordinal — `prompt()` is too
//     crude per Key Technical Decisions in the U5 plan).
//
// Refresh after a successful mutation: invalidate the cache and re-
// render the visible surface (detail page if active, else the open
// summary panel). `pipelineStore` is also touched on regenerate
// success when the response carries a fresh `request_status` so
// cross-module callers see the new status without a Pipeline reload.

/**
 * Regenerate confirmation message — exposed so tests can assert it
 * literally contains "cursor" and "cycle". Origin R15 / AE8 require
 * both substrings so the operator sees the consequence before clicking
 * through.
 */
export const REGENERATE_CONFIRM_MESSAGE =
  "Regenerate this request's search plan? This will reset the cursor and cycle count.";

/**
 * Re-render the inspector surface that is currently visible for one
 * request. Detail page wins when `state.searchPlanDetailContext`
 * matches; otherwise the open summary panel (if any) is rebuilt by
 * round-tripping through {@link toggleSearchPlanSummary}.
 *
 * Pulled out of the action handlers so both `searchPlanRegenerate` and
 * `searchPlanAdvance` have identical refresh semantics.
 *
 * @param {number} requestId
 * @returns {Promise<void>}
 */
async function refreshInspectorSurface(requestId) {
  invalidateSearchPlanCache(searchPlanCache, requestId);
  // Clear the summary in-flight marker so a still-running first-open
  // fetch can't block our re-open. The new toggle below will re-set
  // the marker for the duration of its own fetch.
  summaryInFlight.delete(requestId);
  const ctx = state.searchPlanDetailContext;
  if (ctx && ctx.requestId === requestId) {
    await renderSearchPlanDetail(requestId);
    return;
  }
  // Summary surface — locate the open `.sp-summary` panel for this
  // request, close it (so the next toggle re-fetches and re-renders),
  // and re-open via `toggleSearchPlanSummary(requestId, null)`. The
  // null row arg is fine: the existing panel id is found via getElementById.
  if (typeof document === 'undefined') return;
  const panel = /** @type {HTMLElement|null} */ (
    document.getElementById(`sp-summary-${requestId}`));
  if (panel) {
    // The toggle helper opens an existing closed panel — clear `.open`
    // first so the second click reopens via the fetch path.
    panel.classList.remove('open');
    await toggleSearchPlanSummary(requestId, null);
  }
}

/**
 * Read a JSON-or-empty body from a `Response` without crashing on
 * empty/invalid payloads. Returns `null` when the body is not parseable.
 *
 * @param {Response} resp
 * @returns {Promise<any>}
 */
async function readJsonOrNull(resp) {
  try {
    const text = await resp.text();
    if (!text) return null;
    return JSON.parse(text);
  } catch (err) {
    return null;
  }
}

/**
 * Operator-driven plan regeneration.
 *
 * Native `confirm()` first — bail when the operator dismisses the
 * dialog. The confirmation text MUST include both "cursor" and "cycle"
 * (origin R15 / AE8) so consequences are visible before the click.
 *
 * Body shape: `{}` — `prepend_artist: false` is the API default per
 * `web/routes/pipeline.py::post_pipeline_search_plan_regenerate`. A
 * future PR may surface the toggle on the form; v1 keeps the body
 * minimal for the smallest blast radius.
 *
 * Status-code mapping mirrors `searchPlanAdvance`:
 *   * 200 with `outcome ∈ {success, noop_active_plan_exists}` →
 *     invalidate cache + re-render the visible surface.
 *   * 404 (`request_not_found`) → toast "Request not found".
 *   * 422 (`failed_deterministic`) → toast the API's sanitised error.
 *   * 503 (`failed_transient`) → toast retry-soon.
 *   * Other → console.error + toast.
 *
 * Exposed on `window.searchPlanRegenerate` via `main.js`.
 *
 * @param {number} requestId
 * @returns {Promise<void>}
 */
export async function searchPlanRegenerate(requestId) {
  if (!Number.isInteger(requestId) || requestId <= 0) {
    throw new TypeError(
      `searchPlanRegenerate: requestId must be a positive integer (got ${JSON.stringify(requestId)})`,
    );
  }
  const confirmFn = (typeof window !== 'undefined' && typeof window.confirm === 'function')
    ? window.confirm.bind(window)
    : ((typeof globalThis !== 'undefined' && typeof globalThis.confirm === 'function')
      ? globalThis.confirm.bind(globalThis)
      : null);
  if (typeof confirmFn === 'function' && !confirmFn(REGENERATE_CONFIRM_MESSAGE)) {
    return;
  }
  // Double-click guard — a regenerate is already in flight for this id.
  if (mutationInFlight.has(requestId)) return;
  mutationInFlight.add(requestId);
  try {
    /** @type {Response} */
    let resp;
    try {
      resp = await fetch(`/api/pipeline/${requestId}/search-plan/regenerate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      toast(`Regenerate failed (network): ${msg}`, true);
      return;
    }

    const data = await readJsonOrNull(resp);
    const outcome = (data && typeof data.outcome === 'string') ? data.outcome : null;
    const errMsg = (data && typeof data.error_message === 'string')
      ? data.error_message
      : ((data && typeof data.error === 'string') ? data.error : null);

    // Success outcomes: invalidate + re-render. The regenerate API only
    // returns these two on the success path — see
    // `lib/search_plan_service.py::RESULT_SUCCESS` /
    // `RESULT_NOOP_ACTIVE_PLAN_EXISTS`.
    const successOutcomes = new Set([
      'success', 'noop_active_plan_exists',
    ]);
    if (resp.status === 200 && outcome != null && successOutcomes.has(outcome)) {
      // Cross-tab badge refresh deliberately omitted — regenerate response shape doesn't carry mb_release_id
      await refreshInspectorSurface(requestId);
      return;
    }

    // Failure outcomes — surface the API-sanitised error via toast and do
    // NOT mutate the cache so the operator sees the prior plan state.
    if (resp.status === 404) {
      toast(errMsg || 'Request not found', true);
      return;
    }
    if (resp.status === 422) {
      toast(errMsg || 'Plan generation failed (deterministic)', true);
      return;
    }
    if (resp.status === 503) {
      toast(`${errMsg || 'Plan generation retryable'} — try again`, true);
      return;
    }
    // Defensive: surface unknown errors but don't refresh.
    if (typeof console !== 'undefined' && console.error) {
      console.error(
        `searchPlanRegenerate: unexpected response`,
        { status: resp.status, data });
    }
    toast(`Regenerate failed (HTTP ${resp.status}): ${errMsg || 'unknown error'}`, true);
  } finally {
    mutationInFlight.delete(requestId);
  }
}

/**
 * @typedef {Object} AdvanceTargetInput
 * @property {string} [strategy]
 * @property {string|number} [ordinal]
 */

/**
 * @typedef {{toStrategy: string} | {toOrdinal: number}} AdvanceTarget
 */

/**
 * Pure validator — turn raw form values into a typed advance target.
 *
 * The caller (the form's Confirm button handler) reads two inputs
 * from the inline form: `strategy` (a `<select>` with a leading
 * "no choice" option) and `ordinal` (an `<input type="number">`).
 * Either is set; both raise. Empty strings count as "absent" so the
 * "no choice" option in the select doesn't accidentally win.
 *
 * @param {AdvanceTargetInput} formData
 * @returns {AdvanceTarget}
 * @throws {TypeError} On any invalid combination.
 */
export function parseAdvanceTarget(formData) {
  const fd = formData ?? {};
  const rawStrategy = fd.strategy;
  const rawOrdinal = fd.ordinal;
  const hasStrategy = (typeof rawStrategy === 'string' && rawStrategy !== '');
  const ordinalIsNonEmptyString = (typeof rawOrdinal === 'string' && rawOrdinal !== '');
  const ordinalIsNumber = (typeof rawOrdinal === 'number');
  const hasOrdinal = ordinalIsNonEmptyString || ordinalIsNumber;

  // Reject empty strategy when explicitly passed (covers `{strategy: ''}`).
  if (typeof rawStrategy === 'string' && rawStrategy === '') {
    if (!hasOrdinal) {
      throw new TypeError(
        'parseAdvanceTarget: strategy is empty and no ordinal was provided',
      );
    }
  }

  if (hasStrategy && hasOrdinal) {
    throw new TypeError(
      'parseAdvanceTarget: provide exactly one of strategy or ordinal, not both',
    );
  }
  if (!hasStrategy && !hasOrdinal) {
    throw new TypeError(
      'parseAdvanceTarget: one of strategy or ordinal is required',
    );
  }
  if (hasStrategy) {
    return { toStrategy: /** @type {string} */ (rawStrategy) };
  }
  // hasOrdinal — coerce to int and validate.
  let ord;
  if (ordinalIsNumber) {
    ord = /** @type {number} */ (rawOrdinal);
  } else {
    const parsed = Number(/** @type {string} */ (rawOrdinal));
    ord = parsed;
  }
  if (!Number.isFinite(ord) || !Number.isInteger(ord) || ord < 0) {
    throw new TypeError(
      `parseAdvanceTarget: ordinal must be a non-negative integer (got ${JSON.stringify(rawOrdinal)})`,
    );
  }
  return { toOrdinal: ord };
}

/**
 * Pure HTML producer for the advance form. Renders into the open
 * summary panel or detail page replacing the action toolbar; submission
 * picks whichever input the operator filled in.
 *
 * Inputs:
 *   * `activePlan` — `inspection.active_plan` (or null when no plan).
 *     The form uses `activePlan.items[]` to populate a unique-strategy
 *     `<select>` and to derive the ordinal max bound.
 *   * `requestId` — needed to wire the Confirm onclick.
 *
 * Layout:
 *   * `<select name="strategy">` with a leading `—` option = "no
 *     strategy choice — using ordinal instead".
 *   * `<input type="number" name="ordinal" min="0" max="N-1">` —
 *     `max` is `items.length - 1` so the operator can't tab past the
 *     last slot.
 *   * Confirm button (calls `window.searchPlanSubmitAdvance` which
 *     reads both inputs, parses via {@link parseAdvanceTarget}, and
 *     dispatches the API call).
 *   * Cancel button (calls `window.searchPlanCancelAdvance` which
 *     restores the toolbar by re-rendering the surface).
 *
 * @param {{activePlan: Object|null, requestId: number}} args
 * @returns {string}
 */
export function renderAdvanceForm(args) {
  const activePlan = args.activePlan;
  const requestId = args.requestId;
  /** @type {Array<Object>} */
  const items = (activePlan && Array.isArray(activePlan.items))
    ? activePlan.items : [];
  /** @type {Set<string>} */
  const strategies = new Set();
  for (const item of items) {
    const s = (item && typeof item.strategy === 'string') ? item.strategy : null;
    if (s) strategies.add(s);
  }
  const sortedStrategies = Array.from(strategies).sort();
  const maxOrdinal = items.length > 0 ? items.length - 1 : 0;

  const strategyOptions = ['<option value="">— (use ordinal)</option>']
    .concat(sortedStrategies.map((s) => `<option value="${esc(s)}">${esc(s)}</option>`))
    .join('');

  const reqIdAttr = (Number.isInteger(requestId) && requestId > 0) ? requestId : 0;

  return `<div class="sp-advance-form" data-request-id="${reqIdAttr}">
    <div class="sp-advance-form-row">
      <label class="sp-advance-form-label">Strategy</label>
      <select class="sp-advance-form-input" data-field="strategy">${strategyOptions}</select>
    </div>
    <div class="sp-advance-form-row">
      <label class="sp-advance-form-label">Ordinal</label>
      <input class="sp-advance-form-input" data-field="ordinal" type="number" min="0" max="${esc(String(maxOrdinal))}" placeholder="0..${esc(String(maxOrdinal))}" />
    </div>
    <div class="sp-advance-form-error" data-field="error" style="display:none;"></div>
    <div class="sp-advance-form-actions">
      <button class="sp-action-button" type="button" onclick="event.stopPropagation(); window.searchPlanSubmitAdvance(${reqIdAttr}, this.closest('.sp-advance-form'))">Confirm</button>
      <button class="sp-action-button" type="button" onclick="event.stopPropagation(); window.searchPlanCancelAdvance(${reqIdAttr})">Cancel</button>
    </div>
  </div>`;
}

/**
 * Read the inline form's two fields and dispatch the advance API call.
 * Bound to `window.searchPlanSubmitAdvance`. Does not throw — surfaces
 * errors via the form's inline error block + toast.
 *
 * @param {number} requestId
 * @param {Element|null} formEl
 * @returns {Promise<void>}
 */
export async function searchPlanSubmitAdvance(requestId, formEl) {
  if (!Number.isInteger(requestId) || requestId <= 0) return;
  const errEl = (formEl && formEl.querySelector)
    ? /** @type {HTMLElement|null} */ (formEl.querySelector('[data-field="error"]'))
    : null;
  if (errEl) {
    errEl.style.display = 'none';
    errEl.textContent = '';
  }
  const stratEl = (formEl && formEl.querySelector)
    ? /** @type {HTMLSelectElement|null} */ (formEl.querySelector('[data-field="strategy"]'))
    : null;
  const ordEl = (formEl && formEl.querySelector)
    ? /** @type {HTMLInputElement|null} */ (formEl.querySelector('[data-field="ordinal"]'))
    : null;
  /** @type {AdvanceTargetInput} */
  const formData = {};
  if (stratEl && typeof stratEl.value === 'string' && stratEl.value !== '') {
    formData.strategy = stratEl.value;
  }
  if (ordEl && typeof ordEl.value === 'string' && ordEl.value !== '') {
    formData.ordinal = ordEl.value;
  }
  /** @type {AdvanceTarget} */
  let target;
  try {
    target = parseAdvanceTarget(formData);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (errEl) {
      errEl.textContent = msg;
      errEl.style.display = '';
    } else {
      toast(msg, true);
    }
    return;
  }
  await searchPlanAdvance(requestId, target);
}

/**
 * Cancel the open advance form and restore the action toolbar by re-
 * rendering the visible inspector surface. Bound to
 * `window.searchPlanCancelAdvance`.
 *
 * @param {number} requestId
 * @returns {Promise<void>}
 */
export async function searchPlanCancelAdvance(requestId) {
  if (!Number.isInteger(requestId) || requestId <= 0) return;
  await refreshInspectorSurface(requestId);
}

/**
 * Open the inline advance form for one request. Replaces the action
 * toolbar inside the open summary panel or detail page with the form.
 * No fetch — the form reads from the cached inspection payload (so the
 * strategy `<select>` and ordinal `max` come from `activePlan.items`).
 * On Confirm the form calls `searchPlanAdvance(requestId, target)`.
 *
 * @param {number} requestId
 * @returns {Promise<void>}
 */
async function showAdvanceForm(requestId) {
  if (typeof document === 'undefined') return;
  // Pull the active plan from the cache. If the cache is empty (e.g.
  // operator clicked Advance directly on a stale surface) fall back to
  // a fresh fetch so we have items[] for the form.
  let activePlan = null;
  const cached = getCacheEntry(searchPlanCache, requestId);
  if (cached && cached.inspection && cached.inspection.active_plan) {
    activePlan = cached.inspection.active_plan;
  }
  if (!activePlan) {
    try {
      const inspection = await fetchInspection(requestId);
      activePlan = inspection.active_plan;
      // Patch the cache so a subsequent re-render sees the same data.
      const prior = getCacheEntry(searchPlanCache, requestId);
      setCacheEntry(searchPlanCache, requestId, {
        inspection,
        historyHead: prior ? prior.historyHead : [],
        fetchedAt: Date.now(),
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      toast(`Could not load plan for advance form: ${msg}`, true);
      return;
    }
  }
  if (!activePlan) {
    toast('Regenerate first — no active plan to advance', true);
    return;
  }

  const formHtml = renderAdvanceForm({ activePlan, requestId });

  // Two surfaces to consider: the open summary panel AND the detail
  // page. Replace the actions block in whichever is visible.
  // 1) Detail page actions container.
  const ctx = state.searchPlanDetailContext;
  if (ctx && ctx.requestId === requestId) {
    const detailActions = /** @type {HTMLElement|null} */ (
      document.querySelector(`.sp-detail[data-request-id="${requestId}"] .sp-detail-header-actions`));
    if (detailActions) {
      detailActions.innerHTML = formHtml;
      return;
    }
  }
  // 2) Open summary panel actions block.
  const summaryActions = /** @type {HTMLElement|null} */ (
    document.querySelector(`#sp-summary-${requestId} .sp-summary-actions`));
  if (summaryActions) {
    summaryActions.innerHTML = formHtml;
    return;
  }
  // Neither surface is visible — nothing to attach to. Surface a hint.
  toast('Open the inspector first, then click Advance', true);
}

/**
 * Operator-driven plan-cursor advance.
 *
 * Two call modes:
 *   1. No `target` (default summary/detail "Advance" button click) →
 *      open the inline advance form; submission re-enters with a
 *      typed target.
 *   2. With `target` (programmatic call from the form's Confirm
 *      handler) → POST `/search-plan/advance` with the appropriate
 *      body and process the response.
 *
 * Status-code mapping (origin R16 / AE9):
 *   * 200 (`advanced`) → cache invalidate + re-render visible surface.
 *   * 400 (input-validation, internal bug) → console.error + toast.
 *   * 404 (`request_not_found`) → toast "Request not found".
 *   * 409 (`no_active_plan`) → toast "Regenerate first — no active
 *     plan to advance".
 *   * 422 (`invalid_target`) → toast the API's forward-only / out-of-
 *     range message.
 *   * 503 (`failed_transient`) → toast retry-soon.
 *
 * @param {number} requestId
 * @param {AdvanceTarget} [target]
 * @returns {Promise<void>}
 */
export async function searchPlanAdvance(requestId, target) {
  if (!Number.isInteger(requestId) || requestId <= 0) {
    throw new TypeError(
      `searchPlanAdvance: requestId must be a positive integer (got ${JSON.stringify(requestId)})`,
    );
  }
  // No target — open the inline form (call mode 1). Form-open is a
  // UI-only flip; no in-flight guard needed (the guard is keyed to the
  // POST below).
  if (target === undefined || target === null
    || (typeof target === 'object'
      && !('toOrdinal' in target) && !('toStrategy' in target))) {
    await showAdvanceForm(requestId);
    return;
  }

  // Construct the API body. parseAdvanceTarget returns a typed XOR;
  // honour whichever key is present.
  /** @type {Record<string, any>} */
  const body = {};
  if ('toOrdinal' in target && typeof target.toOrdinal === 'number') {
    body.to_ordinal = target.toOrdinal;
  } else if ('toStrategy' in target && typeof target.toStrategy === 'string') {
    body.to_strategy = target.toStrategy;
  } else {
    if (typeof console !== 'undefined' && console.error) {
      console.error('searchPlanAdvance: malformed target', target);
    }
    toast('Internal error (advance request malformed)', true);
    return;
  }

  // Double-click guard — same as `searchPlanRegenerate`. Keyed on
  // requestId so concurrent advance POSTs for the same plan are
  // suppressed; concurrent advances for different plans still work.
  if (mutationInFlight.has(requestId)) return;
  mutationInFlight.add(requestId);
  try {
    /** @type {Response} */
    let resp;
    try {
      resp = await fetch(`/api/pipeline/${requestId}/search-plan/advance`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      toast(`Advance failed (network): ${msg}`, true);
      return;
    }

    const data = await readJsonOrNull(resp);
    const outcome = (data && typeof data.outcome === 'string') ? data.outcome : null;
    const errMsg = (data && typeof data.error_message === 'string')
      ? data.error_message
      : ((data && typeof data.error === 'string') ? data.error : null);

    if (resp.status === 200 && outcome === 'advanced') {
      await refreshInspectorSurface(requestId);
      return;
    }
    if (resp.status === 400) {
      // The form should always validate client-side, so 400 is internal.
      if (typeof console !== 'undefined' && console.error) {
        console.error('searchPlanAdvance: 400 from server (internal bug)',
          { status: resp.status, data, body });
      }
      toast(errMsg || 'Internal error (advance request malformed)', true);
      return;
    }
    if (resp.status === 404) {
      toast(errMsg || 'Request not found', true);
      return;
    }
    if (resp.status === 409) {
      toast(errMsg || 'Regenerate first — no active plan to advance', true);
      return;
    }
    if (resp.status === 422) {
      toast(errMsg || 'Invalid advance target', true);
      return;
    }
    if (resp.status === 503) {
      toast(`${errMsg || 'Plan lock contention'} — try again`, true);
      return;
    }
    if (typeof console !== 'undefined' && console.error) {
      console.error(
        'searchPlanAdvance: unexpected response',
        { status: resp.status, data });
    }
    toast(`Advance failed (HTTP ${resp.status}): ${errMsg || 'unknown error'}`, true);
  } finally {
    mutationInFlight.delete(requestId);
  }
}
