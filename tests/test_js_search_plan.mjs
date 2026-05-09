/**
 * Unit tests for web/js/search_plan.js — pure helpers (DOM-free).
 *
 * Run with: node tests/test_js_search_plan.mjs
 *
 * Mirrors `tests/test_js_util.mjs`: bare assertions, no test framework,
 * no DOM, no fetch mocking. Impure exports (`fetchInspection`,
 * `fetchHistoryPage`) and the action-handler stubs are not exercised
 * here — the URL builder and state stash/pop helpers are the testable
 * surface for U2.
 */

import {
  HISTORY_PAGE_DEFAULT_LIMIT,
  buildHistoryUrl,
  captureOriginContext,
  restoreOriginContext,
  invalidateSearchPlanCache,
  searchPlanCache,
  renderSearchPlanButton,
  renderSummaryPanel,
} from '../web/js/search_plan.js';

let passed = 0;
let failed = 0;

function assert(condition, msg) {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg}`);
  }
}

function assertEqual(actual, expected, msg) {
  if (actual === expected) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

function assertThrows(fn, errorClass, msg) {
  let caught = null;
  try {
    fn();
  } catch (err) {
    caught = err;
  }
  if (caught === null) {
    failed++;
    console.error(`  FAIL: ${msg} — expected throw, got no throw`);
  } else if (errorClass && !(caught instanceof errorClass)) {
    failed++;
    console.error(
      `  FAIL: ${msg} — expected ${errorClass.name}, got ${caught.constructor.name}: ${caught.message}`,
    );
  } else {
    passed++;
  }
}

// --- buildHistoryUrl -------------------------------------------------
console.log('buildHistoryUrl()');

assertEqual(
  buildHistoryUrl({ requestId: 2566, limit: 50, beforeId: null }),
  '/api/pipeline/2566/search-plan/history?limit=50',
  'first page (no before_id) emits limit only',
);

assertEqual(
  buildHistoryUrl({ requestId: 2566, limit: 50, beforeId: 12345 }),
  '/api/pipeline/2566/search-plan/history?limit=50&before_id=12345',
  'next page emits both limit and before_id',
);

assertThrows(
  () => buildHistoryUrl({ requestId: 0, limit: 50, beforeId: null }),
  TypeError,
  'requestId=0 throws TypeError',
);

assertThrows(
  () => buildHistoryUrl(/** @type {any} */ ({ requestId: 'abc', limit: 50, beforeId: null })),
  TypeError,
  'non-int requestId throws TypeError',
);

// Defaults — limit defaults to HISTORY_PAGE_DEFAULT_LIMIT when nullish.
assertEqual(
  buildHistoryUrl({ requestId: 1, beforeId: null }),
  `/api/pipeline/1/search-plan/history?limit=${HISTORY_PAGE_DEFAULT_LIMIT}`,
  'omitted limit defaults to HISTORY_PAGE_DEFAULT_LIMIT',
);

assertEqual(
  buildHistoryUrl({ requestId: 1, limit: undefined, beforeId: null }),
  `/api/pipeline/1/search-plan/history?limit=${HISTORY_PAGE_DEFAULT_LIMIT}`,
  'undefined limit defaults to HISTORY_PAGE_DEFAULT_LIMIT',
);

// beforeId is omitted only when null/undefined; 0 is NOT a valid cursor
// (id sequences start at 1) but we don't filter — the caller is
// responsible for not passing rubbish.
assertEqual(
  buildHistoryUrl({ requestId: 5, limit: 10 }),
  '/api/pipeline/5/search-plan/history?limit=10',
  'omitted beforeId is left out of query string',
);

assertThrows(
  () => buildHistoryUrl({ requestId: -1, limit: 50, beforeId: null }),
  TypeError,
  'negative requestId throws TypeError',
);

assertThrows(
  () => buildHistoryUrl({ requestId: 1.5, limit: 50, beforeId: null }),
  TypeError,
  'non-integer requestId throws TypeError',
);

// --- captureOriginContext / restoreOriginContext round-trip ----------
console.log('captureOriginContext() / restoreOriginContext()');

{
  const captured = captureOriginContext({ tab: 'browse', scrollY: 420, subView: null });
  assertEqual(captured.originTab, 'browse', 'capture stashes tab');
  assertEqual(captured.originScrollY, 420, 'capture stashes scrollY');
  assertEqual(captured.originSubView, null, 'capture stashes null subView');

  const restored = restoreOriginContext(captured);
  assertEqual(restored.tab, 'browse', 'restore returns tab');
  assertEqual(restored.scrollY, 420, 'restore returns scrollY');
  assertEqual(restored.subView, null, 'restore returns null subView');
}

{
  const captured = captureOriginContext({ tab: 'pipeline', scrollY: 0, subView: 'queue' });
  const restored = restoreOriginContext(captured);
  assertEqual(restored.tab, 'pipeline', 'pipeline tab round-trips');
  assertEqual(restored.scrollY, 0, 'scrollY=0 round-trips');
  assertEqual(restored.subView, 'queue', 'pipeline queue subView round-trips');
}

{
  const captured = captureOriginContext({ tab: 'recents', scrollY: 1234, subView: 'downloading' });
  const restored = restoreOriginContext(captured);
  assertEqual(restored.tab, 'recents', 'recents tab round-trips');
  assertEqual(restored.scrollY, 1234, 'large scrollY round-trips');
  assertEqual(restored.subView, 'downloading', 'recents downloading subView round-trips');
}

// --- invalidateSearchPlanCache ---------------------------------------
console.log('invalidateSearchPlanCache()');

{
  /** @type {Map<number, any>} */
  const cache = new Map();
  cache.set(1, { inspection: 'a', historyHead: [], fetchedAt: 1000 });
  cache.set(2, { inspection: 'b', historyHead: [], fetchedAt: 2000 });
  cache.set(3, { inspection: 'c', historyHead: [], fetchedAt: 3000 });

  const returned = invalidateSearchPlanCache(cache, 2);
  assert(returned === cache, 'returns the same Map for chainability');
  assertEqual(cache.size, 2, 'invalidated cache has 2 entries left');
  assert(!cache.has(2), 'requestId 2 removed');
  assert(cache.has(1), 'requestId 1 retained');
  assert(cache.has(3), 'requestId 3 retained');
}

{
  /** @type {Map<number, any>} */
  const cache = new Map();
  cache.set(7, { inspection: 'x', historyHead: [], fetchedAt: 9000 });

  // Removing an absent key is a no-op (no throw, no mutation).
  const returned = invalidateSearchPlanCache(cache, 99);
  assert(returned === cache, 'returns the same Map even when key absent');
  assertEqual(cache.size, 1, 'no-op: cache size unchanged');
  assert(cache.has(7), 'absent-key invalidation does not touch other entries');
}

// Module-level cache export is a Map and starts empty (sanity check —
// tests don't share state with the page).
console.log('searchPlanCache export');
assert(searchPlanCache instanceof Map, 'searchPlanCache is a Map');

// --- renderSearchPlanButton ------------------------------------------
console.log('renderSearchPlanButton()');

{
  // Happy path — pipelineId present yields a clickable button wired to
  // window.toggleSearchPlanSummary with stopPropagation.
  const html = renderSearchPlanButton({ pipelineId: 42 });
  assert(html.includes('class="sp-button"'), 'button uses sp-button class');
  assert(html.includes('window.toggleSearchPlanSummary(42'),
    'button onclick wires window.toggleSearchPlanSummary with the id');
  assert(html.includes('event.stopPropagation()'),
    'button onclick stops parent row propagation');
  assert(html.includes('aria-label='),
    'button carries an aria-label for accessibility');
}

{
  // Browse-row conditional — null pipelineId yields the empty string.
  assertEqual(renderSearchPlanButton({ pipelineId: null }), '',
    'pipelineId=null returns empty string (Browse-row gating)');
  assertEqual(renderSearchPlanButton({ pipelineId: 0 }), '',
    'pipelineId=0 returns empty string');
  assertEqual(renderSearchPlanButton(/** @type {any} */ ({ pipelineId: 'abc' })), '',
    'non-int pipelineId returns empty string');
  assertEqual(renderSearchPlanButton(/** @type {any} */ ({})), '',
    'missing pipelineId returns empty string');
}

// --- renderSummaryPanel ----------------------------------------------
console.log('renderSummaryPanel()');

/**
 * Build a minimal "happy path" inspection payload with N slots and a
 * customisable currentness/active_plan.
 */
function makeInspection(overrides = {}) {
  const items = overrides.items || [
    { id: 1, plan_id: 583, ordinal: 0, strategy: 'track_0', query: 'a', canonical_query_key: 'a', repeat_group: 'track_0', provenance: {} },
    { id: 2, plan_id: 583, ordinal: 1, strategy: 'track_1', query: 'b', canonical_query_key: 'b', repeat_group: 'track_1', provenance: {} },
    { id: 3, plan_id: 583, ordinal: 2, strategy: 'track_2', query: 'c', canonical_query_key: 'c', repeat_group: 'track_2', provenance: {} },
    { id: 4, plan_id: 583, ordinal: 3, strategy: 'track_3', query: 'd', canonical_query_key: 'd', repeat_group: 'track_3', provenance: {} },
  ];
  return {
    request_id: 2566,
    request: {
      id: 2566,
      status: 'wanted',
      artist_name: 'Test Artist',
      album_title: 'Test Album',
      mb_release_id: '00000000-0000-0000-0000-000000000001',
      year: 2026,
      source: 'request',
    },
    current_generator_id: '13',
    currentness: {
      is_wanted: true,
      has_active_plan: true,
      active_plan_generator_id: '13',
      current_generator_searchable: true,
      generator_id_mismatch: false,
      has_deterministic_failure: false,
      has_retryable_failure: false,
      ...(overrides.currentness || {}),
    },
    active_plan: overrides.active_plan === null ? null : {
      plan: {
        id: 583,
        request_id: 2566,
        generator_id: '13',
        status: 'active',
        failure_class: null,
        metadata_snapshot: {},
        provenance: {},
        error_message: null,
        superseded_at: null,
        superseded_by_plan_id: null,
        created_at: '2026-05-09T08:00:00Z',
        ...(overrides.plan || {}),
      },
      items,
      next_ordinal: overrides.next_ordinal ?? 2,
      cycle_count: overrides.cycle_count ?? 1,
    },
    latest_failed_deterministic: overrides.latest_failed_deterministic ?? null,
    latest_failed_transient: null,
    superseded_count: 0,
    legacy_logs: { count: 0, head: [] },
  };
}

{
  // AE2 — happy path: cursor 2/N, cycle 1, plan status, NO drift.
  const inspection = makeInspection({ next_ordinal: 2, cycle_count: 1 });
  const html = renderSummaryPanel({ inspection, history: { rows: [] } });
  assert(html.includes('cursor'), 'meta surfaces cursor label');
  assert(/<strong>2\/4<\/strong>/.test(html),
    'cursor renders as 2/4 inside a single <strong>');
  assert(/cycle\s*<strong>1<\/strong>/.test(html),
    'cycle count rendered with the slot count');
  assert(html.includes('sp-status'),
    'plan status badge is rendered');
  assert(!html.includes('sp-drift'),
    'no drift indicator when generator_id_mismatch=false');
  assert(html.includes('Test Artist'),
    'header carries the artist name');
  assert(html.includes('Test Album'),
    'header carries the album title');
}

{
  // AE4 — generator-id drift visibly marked with both ids.
  const inspection = makeInspection({
    currentness: { generator_id_mismatch: true, active_plan_generator_id: '12' },
    plan: { generator_id: '12' },
  });
  const html = renderSummaryPanel({ inspection, history: { rows: [] } });
  assert(html.includes('sp-drift'),
    'drift indicator class present when generator_id_mismatch=true');
  assert(html.includes('plan=12'),
    'drift indicator surfaces the request plan generator id');
  assert(html.includes('current=13'),
    'drift indicator surfaces the running SEARCH_PLAN_GENERATOR_ID');
}

{
  // History — last 3 attempts rendered with outcome + query + when.
  const history = {
    rows: [
      { id: 100, created_at: '2026-05-09T01:00:00Z', outcome: 'no_match', query: 'q1', attempt_consumed: true, plan_strategy: 'track_0' },
      { id: 99,  created_at: '2026-05-09T00:30:00Z', outcome: 'partial', query: 'q2', attempt_consumed: false, plan_strategy: 'track_1' },
      { id: 98,  created_at: '2026-05-09T00:00:00Z', outcome: 'success', query: 'q3', attempt_consumed: true, plan_strategy: 'track_2' },
    ],
    next_before_id: null,
  };
  const html = renderSummaryPanel({ inspection: makeInspection(), history });
  assert(html.includes('q1') && html.includes('q2') && html.includes('q3'),
    'all three queries appear in the rendered HTML');
  assert(html.includes('no_match') && html.includes('partial') && html.includes('success'),
    'all three outcomes appear in the rendered HTML');
  // awstDateTime renders as "YYYY-MM-DD HH:MM" (UTC + 8 = AWST). The first
  // row's UTC 01:00 → 09:00 AWST; assert at least one expected stamp.
  assert(html.includes('2026-05-09 09:00'),
    'first attempt relative-time stamp rendered via awstDateTime');
  // Three attempt rows, three blocks.
  const attemptCount = (html.match(/class="sp-attempt /g) || []).length
    + (html.match(/class="sp-attempt sp-/g) || []).length;
  assert(attemptCount >= 3,
    'three attempt entries rendered (any class permutation)');
}

{
  // History — fewer than 3 attempts. 1 row in, 1 row out, no crash.
  const history = {
    rows: [
      { id: 50, created_at: '2026-05-09T00:00:00Z', outcome: 'no_match', query: 'lone' },
    ],
    next_before_id: null,
  };
  const html = renderSummaryPanel({ inspection: makeInspection(), history });
  assert(html.includes('lone'), 'sole attempt query appears');
  assert(!html.includes('No attempts yet'),
    'non-empty history does not show empty-state copy');
  // No malformed HTML — the section markup remains balanced.
  const openSection = (html.match(/<div class="sp-summary-section">/g) || []).length;
  const openInner = (html.match(/<div class="sp-summary-inner">/g) || []).length;
  assert(openInner === 1, 'one .sp-summary-inner wrapper');
  assert(openSection >= 1, 'at least one .sp-summary-section');
}

{
  // No active plan (deterministic-failed): renders failure class +
  // sanitised error, omits slot list, does not crash.
  const inspection = makeInspection({
    active_plan: null,
    currentness: {
      is_wanted: true, has_active_plan: false, generator_id_mismatch: false,
      has_deterministic_failure: true, has_retryable_failure: false,
    },
    latest_failed_deterministic: {
      plan: { failure_class: 'no_runnable_query', error_message: 'metadata incomplete' },
    },
  });
  const html = renderSummaryPanel({ inspection, history: { rows: [] } });
  assert(html.includes('no_runnable_query'),
    'failure class surfaced');
  assert(html.includes('metadata incomplete'),
    'sanitised error surfaced');
  assert(html.includes('sp-failure'),
    'failure container wraps the messaging');
  assert(!/cursor\s*<strong>/.test(html),
    'no cursor metadata when active_plan is null');
}

{
  // Escape interpolation — hostile attempt query must be HTML-escaped.
  const history = {
    rows: [
      { id: 1, created_at: '2026-05-09T00:00:00Z', outcome: 'no_match',
        query: '<script>alert(1)</script>' },
    ],
    next_before_id: null,
  };
  const html = renderSummaryPanel({ inspection: makeInspection(), history });
  assert(!html.includes('<script>alert(1)</script>'),
    'raw <script> substring not present');
  assert(html.includes('&lt;script&gt;'),
    'angle brackets entity-escaped in rendered HTML');
}

// --- Summary ---------------------------------------------------------
console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
