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

// --- Summary ---------------------------------------------------------
console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
