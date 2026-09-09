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
  CACHE_TTL_MS,
  buildHistoryUrl,
  captureOriginContext,
  restoreOriginContext,
  snapshotActiveTab,
  invalidateSearchPlanCache,
  getCacheEntry,
  setCacheEntry,
  searchPlanCache,
  renderSearchPlanButton,
  renderSummaryPanel,
  renderDetailPage,
  isInterestingAttempt,
  searchPlanSetAttemptsFilter,
  renderSearchPlanDetail,
  closeSearchPlanDetail,
  consumePendingScrollRestore,
  searchPlanRefreshDetail,
  parseAdvanceTarget,
  renderAdvanceForm,
  searchPlanRegenerate,
  searchPlanAdvance,
  searchPlanLoadOlder,
  toggleSearchPlanSummary,
  REGENERATE_CONFIRM_MESSAGE,
} from '../web/js/search_plan.js';
import { state } from '../web/js/state.js';
import { tabHasAsyncRender } from '../web/js/tabs.js';

import {
  stubGlobals, suite, element, domStub,
} from './js_harness.mjs';

const t = suite(import.meta.url);

// --- buildHistoryUrl -------------------------------------------------
t.section('buildHistoryUrl()');

t.equal(
  buildHistoryUrl({ requestId: 2566, limit: 50, beforeId: null }),
  '/api/pipeline/2566/search-plan/history?limit=50',
  'first page (no before_id) emits limit only',
);

t.equal(
  buildHistoryUrl({ requestId: 2566, limit: 50, beforeId: 12345 }),
  '/api/pipeline/2566/search-plan/history?limit=50&before_id=12345',
  'next page emits both limit and before_id',
);

t.throws(
  () => buildHistoryUrl({ requestId: 0, limit: 50, beforeId: null }),
  'requestId=0 throws TypeError',
  TypeError,
);

t.throws(
  () => buildHistoryUrl(/** @type {any} */ ({ requestId: 'abc', limit: 50, beforeId: null })),
  'non-int requestId throws TypeError',
  TypeError,
);

// Defaults — limit defaults to HISTORY_PAGE_DEFAULT_LIMIT when nullish.
t.equal(
  buildHistoryUrl({ requestId: 1, beforeId: null }),
  `/api/pipeline/1/search-plan/history?limit=${HISTORY_PAGE_DEFAULT_LIMIT}`,
  'omitted limit defaults to HISTORY_PAGE_DEFAULT_LIMIT',
);

t.equal(
  buildHistoryUrl({ requestId: 1, limit: undefined, beforeId: null }),
  `/api/pipeline/1/search-plan/history?limit=${HISTORY_PAGE_DEFAULT_LIMIT}`,
  'undefined limit defaults to HISTORY_PAGE_DEFAULT_LIMIT',
);

// beforeId is omitted only when null/undefined; 0 is NOT a valid cursor
// (id sequences start at 1) but we don't filter — the caller is
// responsible for not passing rubbish.
t.equal(
  buildHistoryUrl({ requestId: 5, limit: 10 }),
  '/api/pipeline/5/search-plan/history?limit=10',
  'omitted beforeId is left out of query string',
);

t.throws(
  () => buildHistoryUrl({ requestId: -1, limit: 50, beforeId: null }),
  'negative requestId throws TypeError',
  TypeError,
);

t.throws(
  () => buildHistoryUrl({ requestId: 1.5, limit: 50, beforeId: null }),
  'non-integer requestId throws TypeError',
  TypeError,
);

// --- captureOriginContext / restoreOriginContext round-trip ----------
t.section('captureOriginContext() / restoreOriginContext()');

{
  const captured = captureOriginContext({ tab: 'browse', scrollY: 420, subView: null });
  t.equal(captured.originTab, 'browse', 'capture stashes tab');
  t.equal(captured.originScrollY, 420, 'capture stashes scrollY');
  t.equal(captured.originSubView, null, 'capture stashes null subView');

  const restored = restoreOriginContext(captured);
  t.equal(restored.tab, 'browse', 'restore returns tab');
  t.equal(restored.scrollY, 420, 'restore returns scrollY');
  t.equal(restored.subView, null, 'restore returns null subView');
}

{
  const captured = captureOriginContext({ tab: 'pipeline', scrollY: 0, subView: 'long-tail' });
  const restored = restoreOriginContext(captured);
  t.equal(restored.tab, 'pipeline', 'pipeline tab round-trips');
  t.equal(restored.scrollY, 0, 'scrollY=0 round-trips');
  t.equal(restored.subView, 'long-tail', 'pipeline long-tail subView round-trips');
}

{
  const captured = captureOriginContext({ tab: 'recents', scrollY: 1234, subView: 'acquisition' });
  const restored = restoreOriginContext(captured);
  t.equal(restored.tab, 'recents', 'recents tab round-trips');
  t.equal(restored.scrollY, 1234, 'large scrollY round-trips');
  t.equal(restored.subView, 'acquisition', 'recents acquisition subView round-trips');
}

// --- invalidateSearchPlanCache ---------------------------------------
t.section('invalidateSearchPlanCache()');

{
  /** @type {Map<number, any>} */
  const cache = new Map();
  cache.set(1, { inspection: 'a', historyHead: [], fetchedAt: 1000 });
  cache.set(2, { inspection: 'b', historyHead: [], fetchedAt: 2000 });
  cache.set(3, { inspection: 'c', historyHead: [], fetchedAt: 3000 });

  const returned = invalidateSearchPlanCache(cache, 2);
  t.ok(returned === cache, 'returns the same Map for chainability');
  t.equal(cache.size, 2, 'invalidated cache has 2 entries left');
  t.ok(!cache.has(2), 'requestId 2 removed');
  t.ok(cache.has(1), 'requestId 1 retained');
  t.ok(cache.has(3), 'requestId 3 retained');
}

{
  /** @type {Map<number, any>} */
  const cache = new Map();
  cache.set(7, { inspection: 'x', historyHead: [], fetchedAt: 9000 });

  // Removing an absent key is a no-op (no throw, no mutation).
  const returned = invalidateSearchPlanCache(cache, 99);
  t.ok(returned === cache, 'returns the same Map even when key absent');
  t.equal(cache.size, 1, 'no-op: cache size unchanged');
  t.ok(cache.has(7), 'absent-key invalidation does not touch other entries');
}

// --- getCacheEntry / setCacheEntry / CACHE_TTL_MS --------------------
t.section('getCacheEntry() + setCacheEntry()');

{
  // Stale entry treated as a miss and removed from the cache.
  /** @type {Map<number, any>} */
  const cache = new Map();
  const staleAt = Date.now() - (CACHE_TTL_MS + 1000);
  cache.set(42, { inspection: { active_plan: null }, historyHead: [], fetchedAt: staleAt });

  const result = getCacheEntry(cache, 42, Date.now());
  t.ok(result === undefined, 'stale entry returns undefined');
  t.ok(!cache.has(42), 'stale entry is deleted from cache');
}

{
  // Fresh entry is returned unchanged.
  /** @type {Map<number, any>} */
  const cache = new Map();
  const freshAt = Date.now();
  const entry = { inspection: { active_plan: { id: 1 } }, historyHead: [], fetchedAt: freshAt };
  cache.set(7, entry);

  const result = getCacheEntry(cache, 7, freshAt + 100);
  t.ok(result === entry, 'fresh entry is returned');
  t.ok(cache.has(7), 'fresh entry is not deleted');
}

{
  // Missing key returns undefined without throwing.
  /** @type {Map<number, any>} */
  const cache = new Map();
  const result = getCacheEntry(cache, 99);
  t.ok(result === undefined, 'missing key returns undefined');
}

{
  // setCacheEntry evicts oldest when at capacity (LRU via Map insertion order).
  /** @type {Map<number, any>} */
  const cache = new Map();
  const now = Date.now();
  // Fill to 50 entries (CACHE_MAX_ENTRIES).
  for (let i = 0; i < 50; i++) {
    cache.set(i, { inspection: {}, historyHead: [], fetchedAt: now });
  }
  t.equal(cache.size, 50, 'cache at capacity before 51st insert');

  // Insert a 51st entry — oldest (key=0) should be evicted.
  setCacheEntry(cache, 50, { inspection: {}, historyHead: [], fetchedAt: now });
  t.equal(cache.size, 50, 'cache still at 50 after LRU eviction');
  t.ok(!cache.has(0), 'oldest entry (key=0) was evicted');
  t.ok(cache.has(50), 'new entry (key=50) is present');
  // Second-oldest is still there.
  t.ok(cache.has(1), 'second-oldest entry (key=1) retained');
}

{
  // setCacheEntry re-insert of existing key moves it to most-recent position.
  /** @type {Map<number, any>} */
  const cache = new Map();
  const now = Date.now();
  cache.set(1, { inspection: {}, historyHead: [], fetchedAt: now });
  cache.set(2, { inspection: {}, historyHead: [], fetchedAt: now });
  // Re-insert key 1 — it should now be at the end (newest).
  setCacheEntry(cache, 1, { inspection: { refreshed: true }, historyHead: [], fetchedAt: now + 1 });
  // Fill to capacity; the eviction should remove key=2 (now oldest).
  for (let i = 3; i <= 50; i++) {
    setCacheEntry(cache, i, { inspection: {}, historyHead: [], fetchedAt: now });
  }
  // Now add entry 51 — should evict key=2.
  setCacheEntry(cache, 51, { inspection: {}, historyHead: [], fetchedAt: now });
  t.ok(!cache.has(2), 'key=2 (oldest after re-insert of key=1) was evicted');
  t.ok(cache.has(1), 'key=1 (re-inserted, now fresher) survived');
}

// Module-level cache export is a Map and starts empty (sanity check —
// tests don't share state with the page).
t.section('searchPlanCache export');
t.ok(searchPlanCache instanceof Map, 'searchPlanCache is a Map');

// --- renderSearchPlanButton ------------------------------------------
t.section('renderSearchPlanButton()');

{
  // Happy path — pipelineId present yields a clickable button wired to
  // window.toggleSearchPlanSummary with stopPropagation.
  const html = renderSearchPlanButton({ pipelineId: 42 });
  t.contains(html, 'class="sp-button"', 'button uses sp-button class');
  t.contains(html, 'window.toggleSearchPlanSummary(42',
    'button onclick wires window.toggleSearchPlanSummary with the id');
  t.contains(html, 'event.stopPropagation()',
    'button onclick stops parent row propagation');
  t.contains(html, 'aria-label=',
    'button carries an aria-label for accessibility');
}

{
  // Browse-row conditional — null pipelineId yields the empty string.
  t.equal(renderSearchPlanButton({ pipelineId: null }), '',
    'pipelineId=null returns empty string (Browse-row gating)');
  t.equal(renderSearchPlanButton({ pipelineId: 0 }), '',
    'pipelineId=0 returns empty string');
  t.equal(renderSearchPlanButton(/** @type {any} */ ({ pipelineId: 'abc' })), '',
    'non-int pipelineId returns empty string');
  t.equal(renderSearchPlanButton(/** @type {any} */ ({})), '',
    'missing pipelineId returns empty string');
}

// --- renderSummaryPanel ----------------------------------------------
t.section('renderSummaryPanel()');

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
  t.contains(html, 'cursor', 'meta surfaces cursor label');
  t.match(html, /<strong>2\/4<\/strong>/, 'cursor renders as 2/4 inside a single <strong>');
  t.match(html, /cycle\s*<strong>1<\/strong>/, 'cycle count rendered with the slot count');
  t.contains(html, 'sp-status',
    'plan status badge is rendered');
  t.excludes(html, 'sp-drift',
    'no drift indicator when generator_id_mismatch=false');
  t.contains(html, 'Test Artist',
    'header carries the artist name');
  t.contains(html, 'Test Album',
    'header carries the album title');
}

{
  // AE4 — generator-id drift visibly marked with both ids.
  const inspection = makeInspection({
    currentness: { generator_id_mismatch: true, active_plan_generator_id: '12' },
    plan: { generator_id: '12' },
  });
  const html = renderSummaryPanel({ inspection, history: { rows: [] } });
  t.contains(html, 'sp-drift',
    'drift indicator class present when generator_id_mismatch=true');
  t.contains(html, 'plan=12',
    'drift indicator surfaces the request plan generator id');
  t.contains(html, 'current=13',
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
  for (const query of ['q1', 'q2', 'q3']) {
    t.contains(html, query, `query ${query} appears in the rendered HTML`);
  }
  for (const outcome of ['no_match', 'partial', 'success']) {
    t.contains(html, outcome, `outcome ${outcome} appears in the rendered HTML`);
  }
  // awstDateTime renders as "YYYY-MM-DD HH:MM" (UTC + 8 = AWST). The first
  // row's UTC 01:00 → 09:00 AWST; assert at least one expected stamp.
  t.contains(html, '2026-05-09 09:00',
    'first attempt relative-time stamp rendered via awstDateTime');
  // Three attempt rows, three blocks.
  const attemptCount = (html.match(/class="sp-attempt /g) || []).length
    + (html.match(/class="sp-attempt sp-/g) || []).length;
  t.ok(attemptCount >= 3,
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
  t.contains(html, 'lone', 'sole attempt query appears');
  t.excludes(html, 'No attempts yet',
    'non-empty history does not show empty-state copy');
  // No malformed HTML — the section markup remains balanced.
  const openSection = (html.match(/<div class="sp-summary-section">/g) || []).length;
  const openInner = (html.match(/<div class="sp-summary-inner">/g) || []).length;
  t.ok(openInner === 1, 'one .sp-summary-inner wrapper');
  t.ok(openSection >= 1, 'at least one .sp-summary-section');
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
    // Flat plan dict, the shape lib/search_plan_inspection.py::_plan_to_dict
    // actually produces (and tests/web/test_routes_search_plan.py's
    // PLAN_ROW_REQUIRED_FIELDS pins for the real route response) - not a
    // {plan: {...}} wrapper, which no producer ever emits.
    latest_failed_deterministic: {
      failure_class: 'no_runnable_query', error_message: 'metadata incomplete',
    },
  });
  const html = renderSummaryPanel({ inspection, history: { rows: [] } });
  t.contains(html, 'no_runnable_query',
    'failure class surfaced');
  t.contains(html, 'metadata incomplete',
    'sanitised error surfaced');
  t.contains(html, 'sp-failure',
    'failure container wraps the messaging');
  t.notMatch(html, /cursor\s*<strong>/, 'no cursor metadata when active_plan is null');
}

{
  // WE-3 folded fix: a failure_class with a null error_message is a real
  // producible row, not a hypothetical one. lib/pipeline_db/search_plan.py's
  // create_failed_search_plan requires failure_class but defaults
  // error_message=None (the migrations/014_persisted_search_plans.sql
  // error_message column is nullable TEXT with no NOT NULL constraint), so
  // a plan can legitimately persist with a failure class and no message.
  // Every OTHER fixture in this file sets both fields together, which lets
  // an `if (failureClass || failureError)` -> `&&` mutant at
  // web/js/search_plan.js survive silently; this fixture is built the way
  // the real writer's own defaults would produce one, and closes that gap.
  const inspection = makeInspection({
    active_plan: null,
    currentness: {
      is_wanted: true, has_active_plan: false, generator_id_mismatch: false,
      has_deterministic_failure: true, has_retryable_failure: false,
    },
    latest_failed_deterministic: {
      failure_class: 'no_runnable_query', error_message: null,
    },
  });
  const html = renderSummaryPanel({ inspection, history: { rows: [] } });
  t.contains(html, 'no_runnable_query',
    'failure class surfaced even when error_message is null (producer default)');
  t.excludes(html, 'No active plan',
    'a real failure_class never falls through to the no-failure-recorded debug line');
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
  t.excludes(html, '<script>alert(1)</script>',
    'raw <script> substring not present');
  t.contains(html, '&lt;script&gt;',
    'angle brackets entity-escaped in rendered HTML');
}

// --- renderDetailPage / closeSearchPlanDetail / pagination -----------
//
// Issue #811 rewrite. The detail page is one composed entry
// (`renderDetailPage`) over four inputs — the inspection payload, the
// loaded history rows, the pagination cursor, and the per-request
// pipeline payload that supplies the library column. Every test below
// drives that entry, never a leaf, so the composition (section order,
// which payload field feeds which fragment) is what is under test.
t.section('renderDetailPage()');

/**
 * Per-slot stats bucket keyed the way the production payload keys it:
 * `identity = {plan_id, ordinal, strategy}`. The pre-#811 renderer read
 * `identity.plan_ordinal` / `identity.plan_strategy`, which no producer
 * has ever written — measured live on request 986, 2026-09-09.
 */
function makeSlotStats(overrides = {}) {
  const planId = overrides.plan_id ?? 583;
  return {
    request_id: 2566,
    current: {
      slots: [
        {
          identity: { plan_id: planId, ordinal: 0, strategy: 'track_0' },
          attempts: 52, consumed_attempts: 52, non_consuming_attempts: 0,
          stale_completion_attempts: 0,
          outcome_counts: { found: 1, no_match: 41, error: 0, no_results: 10 },
          elapsed_s_mean: 60.62, elapsed_s_p95: 70.07,
          result_count_mean: 1.82, browse_time_s_mean: 0.1,
          match_time_s_mean: 0.2, peers_browsed_mean: 0.0,
          fanout_waves_mean: 0.0, last_seen_at: '2026-05-09T01:00:00Z',
        },
        {
          identity: { plan_id: planId, ordinal: 1, strategy: 'track_1' },
          attempts: 33, consumed_attempts: 33, non_consuming_attempts: 0,
          stale_completion_attempts: 0,
          outcome_counts: { found: 0, no_match: 22, error: 1, no_results: 10 },
          elapsed_s_mean: 55.5, elapsed_s_p95: 70.0,
          result_count_mean: 0, browse_time_s_mean: 0.5,
          match_time_s_mean: 5.0, peers_browsed_mean: 2.0,
          fanout_waves_mean: 1.0, last_seen_at: '2026-05-09T02:00:00Z',
        },
      ],
      query_groups: [],
      legacy_bucket: null,
      cache_attribution_level: 'cycle-level',
      cache_per_search_available: false,
    },
    superseded_and_legacy: {
      slots: [], query_groups: [], legacy_bucket: null,
      cache_attribution_level: 'cycle-level', cache_per_search_available: false,
    },
  };
}

/** `search_scope` as the inspection route emits it (issue #811). */
function makeSearchScope(overrides = {}) {
  return {
    override: 'SCOPE-OVERRIDE',
    target_format: null,
    min_bitrate: 320,
    tiers: ['lossless'],
    catch_all: false,
    configured_tiers: ['lossless', 'mp3 v0', 'mp3 320', 'aac', 'opus', 'ogg'],
    source: 'override',
    ...overrides,
  };
}

/** `acquisition` as the inspection route emits it (issue #811). */
function makeAcquisition(overrides = {}) {
  return {
    since: '2026-04-06T01:59:27Z',
    since_reason: 'last_import',
    candidate_tiers: [{ tier: 'lossless', count: 14 }],
    candidates_outside_scope: 0,
    grabs: [
      { filetype: 'GRABTYPE', count: 9, last_at: '2026-06-15T03:55:00Z',
        last_outcome: 'success' },
    ],
    grabs_total: 9,
    last_found: {
      search_log_id: 4242,
      at: '2026-06-15T03:55:00Z',
      strategy: 'FOUNDSTRAT',
      username: 'FOUNDPEER',
      tier: 'lossless',
      matched_tracks: 11,
      total_tracks: 11,
      grab: {
        download_log_id: 777,
        outcome: 'success',
        error_message: null,
        at: '2026-06-15T04:01:00Z',
      },
    },
    peers: [
      { username: 'PEERLIST', tier: 'lossless', best_matched_tracks: 11,
        total_tracks: 11, attempts: 9, last_at: '2026-06-15T03:55:00Z' },
    ],
    ...overrides,
  };
}

/**
 * `GET /api/pipeline/<id>` — the third fetch the detail page makes.
 * Field names measured live against request 986 on 2026-09-09:
 * `current_library.{state,path}` and
 * `request.{final_format,min_bitrate,current_spectral_grade,
 * verified_lossless,beets_scenario}`, plus `history[]` rows carrying
 * `outcome` / `created_at` / `soulseek_username`.
 */
function makeLibraryPayload(overrides = {}) {
  return {
    request: {
      final_format: 'LIBFORMAT',
      min_bitrate: 320,
      current_spectral_grade: 'LIBGRADE',
      verified_lossless: false,
      beets_scenario: 'LIBSCENARIO',
      ...(overrides.request || {}),
    },
    current_library: {
      state: 'unique',
      release_source: 'musicbrainz',
      release_id: '00000000-0000-0000-0000-000000000001',
      album_id: 9649,
      path: '/lib/LIBPATH/2000 - Test Album',
      ...(overrides.current_library || {}),
    },
    history: overrides.history ?? [
      { outcome: 'rejected', created_at: '2026-05-01T00:00:00Z',
        soulseek_username: 'REJECTEDPEER' },
      { outcome: 'success', created_at: '2026-04-06T01:59:27Z',
        soulseek_username: 'IMPORTPEER' },
    ],
  };
}

/** A complete inspection payload for the detail page. */
function makeDetailInspection(overrides = {}) {
  const base = makeInspection(overrides);
  base.stats = overrides.stats ?? makeSlotStats();
  base.search_scope = overrides.search_scope === undefined
    ? makeSearchScope()
    : overrides.search_scope;
  base.acquisition = overrides.acquisition === undefined
    ? makeAcquisition()
    : overrides.acquisition;
  base.superseded_count = overrides.superseded_count ?? 2;
  // Distinctive generator ids so "the detail page does not print the
  // generator id unless it drifted" is a falsifiable assertion rather
  // than one that passes on a two-character string appearing nowhere.
  base.current_generator_id = overrides.current_generator_id ?? 'GENID-13';
  if (base.active_plan && base.active_plan.plan) {
    base.active_plan.plan.generator_id =
      (overrides.plan && overrides.plan.generator_id) || 'GENID-13';
  }
  if (overrides.legacy_logs !== undefined) {
    base.legacy_logs = overrides.legacy_logs;
  }
  if (overrides.latest_failed_deterministic !== undefined) {
    base.latest_failed_deterministic = overrides.latest_failed_deterministic;
  }
  if (overrides.latest_failed_transient !== undefined) {
    base.latest_failed_transient = overrides.latest_failed_transient;
  }
  if (base.active_plan && base.active_plan.plan && overrides.provenance !== undefined) {
    base.active_plan.plan.provenance = overrides.provenance;
  }
  return base;
}

/**
 * Two plan-aware history rows: one ordinary `no_match` with scored
 * candidates, one `found` carrying a linked grab. Row keys measured live
 * against `GET /api/pipeline/986/search-plan/history` on 2026-09-09.
 */
function makeHistoryRows() {
  return [
    {
      id: 12345, created_at: '2026-05-09T03:00:00Z', request_id: 2566,
      plan_id: 583, plan_item_id: 5821, plan_ordinal: 2,
      plan_strategy: 'ATT-STRAT-A', plan_canonical_query_key: 'foo',
      plan_repeat_group: 'track_2', plan_generator_id: 'GENID-13',
      execution_stage: 'accepted', attempt_consumed: true,
      cursor_update_status: 'advanced', stale_reason: null,
      plan_cycle_snapshot: 51,
      outcome: 'no_match', variant: 'track_2', query: 'ATT-QUERY-A',
      result_count: 12, elapsed_s: 4.23, final_state: 'Completed, TimedOut',
      rejection_reason: 'strict_count_mismatch',
      candidates: [
        { username: 'CANDPEER', dir: 'd1', filetype: 'lossless',
          matched_tracks: 0, total_tracks: 11, avg_ratio: 0.0,
          missing_titles: [], file_count: 17, pre_filter_skip: false },
        { username: 'CANDPEER', dir: 'd2', filetype: 'lossless',
          matched_tracks: 0, total_tracks: 11, avg_ratio: 0.0,
          missing_titles: [], file_count: 12, pre_filter_skip: false },
        { username: 'SKIPPEER', dir: 'd3', filetype: 'mp3 320',
          matched_tracks: 9, total_tracks: 11, avg_ratio: 0.9,
          missing_titles: [], file_count: 3, pre_filter_skip: true },
      ],
      browse_time_s: 1.2, match_time_s: 3.0,
      peers_browsed: 5, peers_browsed_lazy: 2, fanout_waves: 2,
      grab_download_log_id: null, grab_outcome: null, grab_filetype: null,
      grab_soulseek_username: null, grab_error_message: null, grab_at: null,
    },
    {
      id: 12340, created_at: '2026-05-09T02:00:00Z', request_id: 2566,
      plan_id: 583, plan_item_id: 5820, plan_ordinal: 1,
      plan_strategy: 'ATT-STRAT-B', plan_canonical_query_key: 'bar',
      plan_repeat_group: 'track_1', plan_generator_id: 'GENID-13',
      execution_stage: 'accepted', attempt_consumed: true,
      cursor_update_status: 'advanced', stale_reason: null,
      plan_cycle_snapshot: 50,
      outcome: 'found', variant: 'track_1', query: 'ATT-QUERY-B',
      result_count: 3, elapsed_s: 70.1, final_state: 'Completed, TimedOut',
      rejection_reason: null,
      candidates: [
        { username: 'GRABPEER', dir: 'g1', filetype: 'lossless',
          matched_tracks: 11, total_tracks: 11, avg_ratio: 0.72,
          missing_titles: [], file_count: 11, pre_filter_skip: false },
      ],
      browse_time_s: 0.5, match_time_s: 0.6,
      peers_browsed: 1, peers_browsed_lazy: 0, fanout_waves: 1,
      grab_download_log_id: 40287, grab_outcome: 'timeout',
      grab_filetype: 'flac', grab_soulseek_username: 'GRABPEER',
      grab_error_message: 'transfer timed out; 5-retry limit reached',
      grab_at: '2026-05-09T02:10:00Z',
    },
  ];
}

/** Slice `html` between two markers so a needle cannot match a neighbour. */
function fragment(html, startMarker, endMarker) {
  const from = html.indexOf(startMarker);
  if (from < 0) return '';
  const to = endMarker ? html.indexOf(endMarker, from + startMarker.length) : -1;
  return to < 0 ? html.slice(from) : html.slice(from, to);
}

{
  // Composition: every section is present, in order, and each one is fed
  // by the payload field it claims. Sentinels are unique per field so a
  // fragment reading its neighbour's argument shows up as a mis-ordered
  // or missing needle.
  const inspection = makeDetailInspection({
    items: [
      { id: 1, plan_id: 583, ordinal: 0, strategy: 'track_0', query: 'SLOT-QUERY-0', canonical_query_key: 'a', repeat_group: 'track_0', provenance: {} },
      { id: 2, plan_id: 583, ordinal: 1, strategy: 'track_1', query: 'SLOT-QUERY-1', canonical_query_key: 'b', repeat_group: 'track_1', provenance: {} },
    ],
    next_ordinal: 1,
    cycle_count: 51,
    provenance: { omitted_candidates: [{ strategy: 's', reason: 'year_unknown' }] },
    legacy_logs: { count: 12, head: [
      { id: 1, created_at: '2026-04-01T00:00:00Z', outcome: 'no_match',
        variant: 'fallback', query: 'LEGACY-QUERY', result_count: 0,
        elapsed_s: 1.0, final_state: 'Completed' },
    ] },
  });
  inspection.request.artist_name = 'TITLE-ARTIST';
  inspection.request.album_title = 'TITLE-ALBUM';
  inspection.request.status = 'REQSTATUS';
  inspection.request.search_attempts = 423;
  inspection.request.created_at = '2026-04-06T02:10:41Z';
  inspection.request.last_attempt_at = '2026-09-08T20:22:12Z';
  inspection.request.next_retry_after = '2026-09-09T00:22:12Z';
  const html = renderDetailPage({
    inspection,
    history: makeHistoryRows(),
    nextBeforeId: 12300,
    library: makeLibraryPayload(),
  });

  const order = [
    ['title artist', 'TITLE-ARTIST'],
    ['request status chip', 'REQSTATUS'],
    ['meta attempt count', '423</strong> attempts since'],
    ['scope section label', 'Searching for'],
    ['scope override value', 'SCOPE-OVERRIDE'],
    ['library column label', 'In the library now'],
    ['library path', 'LIBPATH'],
    ['override-check label', 'Is the override holding?'],
    ['override-check peer', 'FOUNDPEER'],
    ['plan slot query', 'SLOT-QUERY-0'],
    ['attempts section label', 'Attempts'],
    ['attempt strategy', 'ATT-STRAT-A'],
    ['load older', 'sp-load-older-button'],
    ['plan health label', 'Plan health'],
    ['legacy section', 'LEGACY-QUERY'],
  ];
  let previous = -1;
  let previousLabel = '(start)';
  for (const [label, needle] of order) {
    const at = html.indexOf(needle);
    t.ok(at > previous,
      `composition: ${label} renders after ${previousLabel} (at ${at}, previous ${previous})`);
    previous = at;
    previousLabel = label;
  }
}

{
  // Header — exactly ONE Back button (the pre-#811 page rendered two).
  const html = renderDetailPage({
    inspection: makeDetailInspection(),
    history: makeHistoryRows(),
    nextBeforeId: null,
    library: makeLibraryPayload(),
  });
  const backs = html.split('window.closeSearchPlanDetail()').length - 1;
  t.equal(backs, 1, 'header: exactly one Back button is rendered');
  t.contains(html, 'window.searchPlanRefreshDetail',
    'header: Refresh button wires to window.searchPlanRefreshDetail');
  t.contains(html, 'window.searchPlanAdvance(2566, {})',
    'header: Advance button wires to window.searchPlanAdvance');
  t.contains(html, 'window.searchPlanRegenerate(2566)',
    'header: Regenerate button wires to window.searchPlanRegenerate');
  t.contains(html, 'sp-detail-header-actions',
    'header: the advance form still has its action container to replace');
}

{
  // Meta — the generator id is noise unless the plan drifted.
  const clean = renderDetailPage({
    inspection: makeDetailInspection(),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  t.excludes(clean, 'GENID-13',
    'meta: generator id is NOT rendered when the plan is current');
  t.contains(clean, 'sp-detail-meta', 'meta: the meta line is rendered');
  t.contains(clean, 'cursor', 'meta: the cursor position is rendered');
  t.contains(clean, 'cycle', 'meta: the cycle count is rendered');

  const drifted = renderDetailPage({
    inspection: makeDetailInspection({
      currentness: { generator_id_mismatch: true },
      plan: { generator_id: 'GENID-12' },
    }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  t.contains(drifted, 'plan generator out of date',
    'meta: drift renders the "plan generator out of date" chip');
  t.contains(drifted, 'GENID-12',
    'meta: drift chip title carries the plan generator id');
  t.contains(drifted, 'GENID-13',
    'meta: drift chip title carries the current generator id');
}

{
  // Scope chips — ladder order, lit vs struck, `any` bound to catch_all.
  const html = renderDetailPage({
    inspection: makeDetailInspection({
      search_scope: makeSearchScope({
        tiers: ['lossless', 'mp3 v0'], catch_all: false,
      }),
    }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  const scope = fragment(html, 'Searching for', 'In the library now');
  t.contains(scope, '<span class="sp-tier sp-tier-on">lossless</span>',
    'scope: an in-scope tier chip is lit');
  t.contains(scope, '<span class="sp-tier sp-tier-on">mp3 v0</span>',
    'scope: the second in-scope tier chip is lit');
  t.contains(scope, '<span class="sp-tier">aac</span>',
    'scope: an excluded configured tier chip is struck through');
  t.contains(scope, '<span class="sp-tier">any</span>',
    'scope: the any chip is struck when catch_all is false');
  t.contains(scope, 'SCOPE-OVERRIDE', 'scope: the override value is rendered');
  t.contains(scope, '320 kbps', 'scope: the bitrate floor is rendered');
  t.contains(scope, 'Target format', 'scope: the target-format row is rendered');
  t.contains(scope, 'catch-all excluded',
    'scope: catch_all=false is stated beside the override');

  const catchAll = renderDetailPage({
    inspection: makeDetailInspection({
      search_scope: makeSearchScope({
        override: null, target_format: null, tiers: ['lossless', 'mp3 v0'],
        catch_all: true, source: 'config',
      }),
    }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  const catchScope = fragment(catchAll, 'Searching for', 'In the library now');
  t.contains(catchScope, '<span class="sp-tier sp-tier-on">any</span>',
    'scope: the any chip is lit when catch_all is true');
  t.excludes(catchScope, 'SCOPE-OVERRIDE',
    'scope: a null override renders no override value');
}

{
  // Library column — the third fetch's payload, and its failure mode.
  const unique = renderDetailPage({
    inspection: makeDetailInspection(),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  const lib = fragment(unique, 'In the library now', 'Is the override holding?');
  t.contains(lib, 'LIBFORMAT', 'library: request.final_format rendered');
  t.contains(lib, 'LIBGRADE', 'library: request.current_spectral_grade rendered');
  t.contains(lib, 'LIBSCENARIO', 'library: request.beets_scenario rendered');
  t.contains(lib, 'LIBPATH', 'library: current_library.path rendered');
  t.contains(lib, 'IMPORTPEER',
    'library: the newest success history row supplies the import peer');
  t.excludes(lib, 'REJECTEDPEER',
    'library: a non-success history row is NOT read for the import peer');
  t.contains(lib, '2026-04-06',
    'library: the success row created_at supplies the import date');

  const verified = renderDetailPage({
    inspection: makeDetailInspection(),
    history: [], nextBeforeId: null,
    library: makeLibraryPayload({ request: { verified_lossless: true } }),
  });
  t.contains(fragment(verified, 'In the library now', 'Is the override holding?'),
    'verified lossless',
    'library: verified_lossless=true is stated');
  t.excludes(fragment(unique, 'In the library now', 'Is the override holding?'),
    'verified lossless',
    'library: verified_lossless=false is not stated');

  const missing = renderDetailPage({
    inspection: makeDetailInspection(),
    history: [], nextBeforeId: null,
    library: makeLibraryPayload({ current_library: { state: 'missing', path: undefined } }),
  });
  const missingLib = fragment(missing, 'In the library now', 'Is the override holding?');
  t.contains(missingLib, 'missing',
    'library: a non-unique state renders the state word');
  t.excludes(missingLib, 'LIBPATH',
    'library: a non-unique state does not render a path');

  const failed = renderDetailPage({
    inspection: makeDetailInspection(),
    history: [], nextBeforeId: null, library: null,
  });
  t.contains(failed, 'library state unavailable',
    'library: a failed third fetch renders the unavailable copy, not a crash');
  t.contains(failed, 'sp-detail',
    'library: a failed third fetch still renders the rest of the page');
}

{
  // Override checks — the one question the page exists to answer.
  const clean = renderDetailPage({
    inspection: makeDetailInspection(),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  const checks = fragment(clean, 'Is the override holding?', 'sp-plan-table');
  t.contains(checks, 'since import 2026-04-06',
    'checks: since_reason=last_import renders "since import <date>"');
  t.contains(checks, 'sp-check-ok',
    'checks: candidates_outside_scope=0 renders the good mark');
  t.contains(checks, 'GRABTYPE', 'checks: the grab filetype tally is rendered');
  t.contains(checks, 'FOUNDPEER', 'checks: last_found peer rendered');
  t.contains(checks, 'FOUNDSTRAT', 'checks: last_found strategy rendered');
  t.contains(checks, 'PEERLIST', 'checks: the peers line is rendered');
  t.excludes(checks, 'sp-check-att',
    'checks: an all-clean acquisition renders no attention mark');

  const allHistory = renderDetailPage({
    inspection: makeDetailInspection({
      acquisition: makeAcquisition({ since: null, since_reason: 'request_created' }),
    }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  t.contains(allHistory, 'all history',
    'checks: no last import renders the "all history" scope label');

  const dirty = renderDetailPage({
    inspection: makeDetailInspection({
      acquisition: makeAcquisition({
        candidate_tiers: [
          { tier: 'lossless', count: 4 }, { tier: 'OUTOFSCOPE', count: 9 },
        ],
        candidates_outside_scope: 9,
        grabs: [{ filetype: 'GRABTYPE', count: 9, last_at: '2026-06-15T03:55:00Z',
          last_outcome: 'timeout' }],
        last_found: {
          search_log_id: 4242, at: '2026-06-15T03:55:00Z',
          strategy: 'FOUNDSTRAT', username: 'FOUNDPEER', tier: 'lossless',
          matched_tracks: 11, total_tracks: 11,
          grab: {
            download_log_id: 777, outcome: 'timeout',
            error_message: 'transfer timed out; 5-retry limit reached',
            at: '2026-06-15T04:01:00Z',
          },
        },
      }),
    }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  const dirtyChecks = fragment(dirty, 'Is the override holding?', 'sp-plan-table');
  t.contains(dirtyChecks, 'sp-check-att',
    'checks: a nonzero candidates_outside_scope raises the attention mark');
  t.contains(dirtyChecks, 'OUTOFSCOPE',
    'checks: the offending out-of-scope tier is named');
  t.contains(dirtyChecks, 'transfer timed out',
    'checks: a failed linked grab surfaces its error message');
  t.excludes(dirtyChecks, '5-retry limit',
    'checks: the grab error is shortened to its first clause');

  const sparse = renderDetailPage({
    inspection: makeDetailInspection({
      acquisition: makeAcquisition({
        candidate_tiers: [], grabs: [], grabs_total: 0,
        last_found: null, peers: [], since: null, since_reason: null,
      }),
    }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  t.excludes(sparse, 'Is the override holding?',
    'checks: the whole block is omitted when every row is empty');

  const partial = renderDetailPage({
    inspection: makeDetailInspection({
      acquisition: makeAcquisition({
        grabs: [], grabs_total: 0, last_found: null, peers: [],
      }),
    }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  const partialChecks = fragment(partial, 'Is the override holding?', 'sp-plan-table');
  t.excludes(partialChecks, 'GRABTYPE',
    'checks: an empty grabs list omits its row rather than printing a dash');
  t.excludes(partialChecks, 'FOUNDPEER',
    'checks: a null last_found omits its row rather than printing a dash');
  t.excludes(partialChecks, 'PEERLIST',
    'checks: an empty peers list omits its row rather than printing a dash');
  t.contains(partialChecks, 'candidates scored',
    'checks: the surviving candidate-tier row still renders');

  const noAcquisition = renderDetailPage({
    inspection: makeDetailInspection({ acquisition: null }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  t.excludes(noAcquisition, 'Is the override holding?',
    'checks: a null acquisition omits the block');
  t.contains(noAcquisition, 'sp-plan-table',
    'checks: a null acquisition still renders the plan table');
}

{
  // Plan table — slots merged with their tallies, matched on the
  // production identity keys `{plan_id, ordinal}`.
  const html = renderDetailPage({
    inspection: makeDetailInspection({ next_ordinal: 1 }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  const plan = fragment(html, 'sp-plan-table', 'sp-attempts-section');
  t.contains(plan, 'track_0', 'plan: slot 0 strategy rendered');
  t.contains(plan, 'sp-plan-current',
    'plan: the next_ordinal slot carries the current-row marker');
  // Slot 0's tallies come from the stats row whose identity is
  // {plan_id: 583, ordinal: 0}. If the matcher reverts to
  // identity.plan_ordinal (absent from every producer) these go blank.
  t.contains(plan, '>52<', 'plan: slot 0 Tried tally read via identity.ordinal');
  t.contains(plan, '>41<', 'plan: slot 0 No-match tally rendered');
  t.contains(plan, '>10<', 'plan: slot 0 Empty (no_results) tally rendered');
  t.contains(plan, '61s', 'plan: slot 0 elapsed_s_mean rendered as Avg');
  t.contains(plan, '05-09 09:00',
    'plan: slot 0 last_seen_at rendered short in the Last column');
  t.contains(plan, '>33<', 'plan: slot 1 Tried tally read via identity.ordinal');

  // A stats bucket whose plan_id belongs to a superseded plan must NOT
  // be merged into the active plan's rows.
  const foreign = renderDetailPage({
    inspection: makeDetailInspection({ stats: makeSlotStats({ plan_id: 999 }) }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  const foreignPlan = fragment(foreign, 'sp-plan-table', 'sp-attempts-section');
  t.excludes(foreignPlan, '>52<',
    'plan: stats for a different plan_id are not merged into this plan');
  t.contains(foreignPlan, 'track_0',
    'plan: a slot with no stats still renders, with blank tallies');

  // The retired per-slot stats table and its cache label are gone.
  t.excludes(html, 'sp-stats-table',
    'plan: the separate per-slot stats table is gone');
  t.excludes(html, 'Cache attribution',
    'plan: the cache-attribution label is gone');
  t.excludes(html, 'sp-slot-list',
    'plan: the separate slot list is gone');
}

{
  // Attempts — one line per row; abnormal facts inline, the rest behind
  // the raw expander.
  const html = renderDetailPage({
    inspection: makeDetailInspection(),
    history: makeHistoryRows(),
    nextBeforeId: null,
    library: makeLibraryPayload(),
  });
  const attempts = fragment(html, 'sp-attempts-tbody', 'Plan health');
  t.contains(attempts, 'ATT-STRAT-A', 'attempts: row strategy rendered');
  t.contains(attempts, 'CANDPEER',
    'attempts: the best scored candidate username is rendered');
  t.contains(attempts, '&times;2',
    'attempts: a repeated username renders the repeat count');
  t.contains(attempts, 'strict count mismatch',
    'attempts: rejection_reason is humanized');
  t.contains(attempts, 'grab timeout',
    'attempts: a linked grab outcome is appended to the candidates cell');
  t.contains(attempts, 'transfer timed out',
    'attempts: the grab error message first clause is appended');
  t.excludes(attempts, '5-retry limit',
    'attempts: the grab error message is shortened to its first clause');
  t.contains(attempts, 'sp-att-raw',
    'attempts: each row carries a raw expander');
  t.excludes(attempts, 'sp-att-grab-ok',
    'attempts: a non-success grab does NOT get the success class');

  const succeeded = renderDetailPage({
    inspection: makeDetailInspection(),
    history: makeHistoryRows().map((row) => (row.grab_outcome
      ? { ...row, grab_outcome: 'success', grab_error_message: null }
      : row)),
    nextBeforeId: null,
    library: makeLibraryPayload(),
  });
  t.contains(succeeded, 'sp-att-grab-ok',
    'attempts: a successful grab gets the success class');

  // Row 1 (no_match, no grab): everything abnormal-free stays inline-free
  // and the telemetry lives behind the expander.
  const firstRow = fragment(attempts, 'ATT-STRAT-A', 'sp-att-raw');
  t.excludes(firstRow, 'Completed, TimedOut',
    'attempts: a final_state starting with Completed is not rendered inline');
  t.excludes(firstRow, 'not consumed',
    'attempts: a consumed attempt renders no "not consumed" chip');
  t.excludes(firstRow, 'SKIPPEER',
    'attempts: a pre_filter_skip candidate is not chosen as the best');
  const raw = fragment(attempts, 'sp-att-raw', '</details>');
  t.contains(raw, 'peers', 'attempts raw: peers browsed behind the expander');
  t.contains(raw, 'fanout', 'attempts raw: fanout waves behind the expander');
  t.contains(raw, 'cycle', 'attempts raw: cycle snapshot behind the expander');
  t.contains(raw, 'advanced', 'attempts raw: cursor status behind the expander');
  t.contains(raw, 'ATT-QUERY-A', 'attempts raw: the query is behind the expander');
  t.contains(raw, 'SKIPPEER',
    'attempts raw: the full candidates JSON is behind the expander');
}

{
  // Attempts — the three abnormal inline chips, one world each, against
  // the same row that renders none of them above.
  const base = makeHistoryRows()[0];
  const CASES = [
    ['stale cursor', { ...base, cursor_update_status: 'stale' }, '>stale</span>'],
    ['stale reason', { ...base, stale_reason: 'plan_superseded' }, '>stale</span>'],
    ['not consumed', { ...base, attempt_consumed: false }, 'not consumed'],
    ['odd final state', { ...base, final_state: 'ODDSTATE' }, 'ODDSTATE'],
  ];
  for (const [label, row, needle] of CASES) {
    const html = renderDetailPage({
      inspection: makeDetailInspection(),
      history: [row], nextBeforeId: null, library: makeLibraryPayload(),
    });
    const inline = fragment(
      fragment(html, 'sp-attempts-tbody', 'Plan health'), 'ATT-STRAT-A', 'sp-att-raw');
    t.contains(inline, needle,
      `attempts: an abnormal ${label} attempt renders its inline chip`);
  }
}

{
  // Attempts filter — the section label counts loaded rows, the toggle is
  // wired to the window handler, and Interesting is the default view.
  const rows = makeHistoryRows();
  const boring = {
    ...rows[0],
    id: 12000, plan_strategy: 'BORING-STRAT',
    rejection_reason: null,
    candidates: [{ username: 'BORINGPEER', pre_filter_skip: true }],
  };
  const all = [...rows, boring];
  const html = renderDetailPage({
    inspection: makeDetailInspection(),
    history: all, nextBeforeId: null, library: makeLibraryPayload(),
  });
  t.contains(html, "window.searchPlanSetAttemptsFilter(2566, 'all')",
    'attempts filter: the All button wires to the window handler');
  t.contains(html, "window.searchPlanSetAttemptsFilter(2566, 'interesting')",
    'attempts filter: the Interesting button wires to the window handler');
  t.contains(html, '2 of 3 loaded',
    'attempts filter: the section label reads "N of M loaded"');
  t.contains(html, 'sp-filter-button-on',
    'attempts filter: the active filter button is marked');
  t.excludes(fragment(html, 'sp-attempts-tbody', 'Plan health'), 'BORING-STRAT',
    'attempts filter: an uninteresting row is hidden by the default view');
  t.contains(fragment(html, 'sp-attempts-tbody', 'Plan health'), 'ATT-STRAT-A',
    'attempts filter: an interesting row survives the default view');
}

{
  // Plan health — one line, then a provenance sentence, then raw.
  const clean = renderDetailPage({
    inspection: makeDetailInspection({
      provenance: {
        omitted_candidates: [
          { strategy: 'unwild_year', reason: 'year_unknown' },
          { strategy: 'unwild_rg_year', reason: 'year_unknown' },
          { strategy: 'catalog_number', reason: 'catalog_number_unknown' },
        ],
        dedupe_losers: [],
        dropped_low_entropy_tokens: [],
      },
    }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  const health = fragment(clean, 'Plan health', 'raw provenance');
  t.contains(health, 'No plan failures',
    'health: no failure plan renders the quiet line');
  t.contains(health, 'generator current',
    'health: a current generator is stated on the health line');
  t.contains(health, '2 superseded plans',
    'health: superseded_count is stated on the health line');
  t.contains(health, 'Left out: 2 year unknown, 1 catalog number unknown',
    'health: omitted_candidates are grouped by reason into one sentence');
  t.excludes(health, 'unwild_rg_year',
    'health: the raw provenance JSON is not dumped into the sentence');
  t.excludes(health, 'Dropped low-entropy tokens',
    'health: an empty dropped_low_entropy_tokens list is not mentioned');
  t.contains(clean, 'raw provenance',
    'health: the raw provenance stays behind a details expander');

  const dropped = renderDetailPage({
    inspection: makeDetailInspection({
      provenance: { omitted_candidates: [], dropped_low_entropy_tokens: ['DROPTOKEN'] },
    }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  t.contains(fragment(dropped, 'Plan health', 'raw provenance'), 'DROPTOKEN',
    'health: a non-empty dropped_low_entropy_tokens list is named');
  t.excludes(fragment(dropped, 'Plan health', 'raw provenance'), 'Left out',
    'health: an empty omitted_candidates list produces no "Left out" sentence');

  const failed = renderDetailPage({
    inspection: makeDetailInspection({
      latest_failed_deterministic: {
        id: 580, generator_id: 'GENID-13', failure_class: 'no_runnable_query',
        error_message: 'HEALTHERROR', created_at: '2026-05-08T00:00:00Z',
      },
    }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  t.contains(failed, 'no_runnable_query',
    'health: a deterministic failure class is surfaced');
  t.contains(failed, 'HEALTHERROR',
    'health: the sanitised failure error is surfaced');
  t.excludes(failed, 'No plan failures',
    'health: the quiet line is replaced when a failure exists');

  const transient = renderDetailPage({
    inspection: makeDetailInspection({
      latest_failed_transient: {
        id: 581, generator_id: 'GENID-13', failure_class: 'mirror_timeout',
        error_message: 'TRANSIENTERROR', created_at: '2026-05-08T00:00:00Z',
      },
    }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  t.contains(transient, 'mirror_timeout',
    'health: a transient failure class is surfaced too');
}

{
  // Legacy — omitted entirely at zero rows, rendered otherwise.
  const none = renderDetailPage({
    inspection: makeDetailInspection({ legacy_logs: { count: 0, head: [] } }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  t.excludes(none, 'sp-history-legacy-section',
    'legacy: the whole section is omitted when legacy_logs.count is 0');
  t.excludes(none, 'Pre-rollout history',
    'legacy: no pre-rollout heading is rendered at zero rows');

  const some = renderDetailPage({
    inspection: makeDetailInspection({
      legacy_logs: { count: 5, head: [
        { id: 9, created_at: '2026-04-09T00:00:00Z', outcome: 'no_match',
          variant: 'fallback', query: 'LEGACY-Q1', result_count: 1,
          elapsed_s: 1.5, final_state: 'Completed' },
      ] },
    }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  t.contains(some, 'sp-history-legacy-section',
    'legacy: the section renders when legacy rows exist');
  t.contains(some, 'LEGACY-Q1', 'legacy: the legacy query is rendered');
  t.contains(some, '<details class="sp-history-legacy">',
    'legacy: the legacy block stays collapsed');
}

{
  // Pagination — cursor present renders Load older wired to the handler.
  const html = renderDetailPage({
    inspection: makeDetailInspection(),
    history: makeHistoryRows(), nextBeforeId: 12300,
    library: makeLibraryPayload(),
  });
  t.contains(html, 'sp-load-older-button',
    'pagination: Load older button rendered when nextBeforeId is non-null');
  t.contains(html, 'window.searchPlanLoadOlder(2566, 12300)',
    'pagination: button onclick wires the cursor seed');

  const exhausted = renderDetailPage({
    inspection: makeDetailInspection(),
    history: makeHistoryRows(), nextBeforeId: null,
    library: makeLibraryPayload(),
  });
  t.excludes(exhausted, 'sp-load-older-button',
    'pagination: no Load older button when nextBeforeId is null');
}

{
  // Empty attempts — an empty-state instead of a headerless table.
  const html = renderDetailPage({
    inspection: makeDetailInspection({ legacy_logs: { count: 0, head: [] } }),
    history: [], nextBeforeId: null, library: makeLibraryPayload(),
  });
  t.contains(html, 'No attempts yet',
    'empty: an empty history renders the attempts empty-state');
}

{
  // A deterministic-failed request has no active plan at all. The page
  // still renders — the operator opened it to find out why.
  const html = renderDetailPage({
    inspection: makeDetailInspection({
      active_plan: null,
      currentness: { has_active_plan: false, has_deterministic_failure: true },
      latest_failed_deterministic: {
        id: 580, generator_id: 'GENID-13', failure_class: 'no_runnable_query',
        error_message: 'NOPLANERROR', created_at: '2026-05-08T00:00:00Z',
      },
      legacy_logs: { count: 0, head: [] },
    }),
    history: makeHistoryRows(), nextBeforeId: null,
    library: makeLibraryPayload(),
  });
  t.contains(html, 'sp-detail',
    'no plan: the page still renders without an active plan');
  t.excludes(html, 'sp-plan-table',
    'no plan: the Plan table is omitted when there is nothing to show');
  t.contains(html, 'NOPLANERROR',
    'no plan: the deterministic failure error is why the operator is here');
  t.contains(html, 'ATT-STRAT-A',
    'no plan: attempts still render without an active plan');
  t.contains(html, 'Searching for',
    'no plan: the scope section still renders without an active plan');
}

// --- isInterestingAttempt --------------------------------------------
t.section('isInterestingAttempt()');

{
  /** A boring row: no_match, every candidate pre-filter-skipped, consumed. */
  const boring = () => ({
    outcome: 'no_match',
    candidates: [{ username: 'x', pre_filter_skip: true }],
    cursor_update_status: 'advanced',
    stale_reason: null,
    attempt_consumed: true,
  });
  const CASES = [
    ['boring no_match is not interesting', boring(), false],
    ['boring no_results is not interesting',
      { ...boring(), outcome: 'no_results' }, false],
    ['null candidates is not interesting by itself',
      { ...boring(), candidates: null }, false],
    ['empty candidates is not interesting by itself',
      { ...boring(), candidates: [] }, false],
    ['found is interesting', { ...boring(), outcome: 'found' }, true],
    ['an outcome outside no_match/no_results is interesting',
      { ...boring(), outcome: 'error' }, true],
    ['a scored (non-skipped) candidate is interesting',
      { ...boring(), candidates: [{ username: 'x', pre_filter_skip: false }] }, true],
    ['a candidate with no pre_filter_skip key counts as scored',
      { ...boring(), candidates: [{ username: 'x' }] }, true],
    ['cursor_update_status=stale is interesting',
      { ...boring(), cursor_update_status: 'stale' }, true],
    ['a non-empty stale_reason is interesting',
      { ...boring(), stale_reason: 'plan_superseded' }, true],
    ['an empty-string stale_reason is not interesting',
      { ...boring(), stale_reason: '' }, false],
    ['attempt_consumed=false is interesting',
      { ...boring(), attempt_consumed: false }, true],
    ['a null row is not interesting', null, false],
  ];
  for (const [label, row, expected] of CASES) {
    t.equal(isInterestingAttempt(row), expected,
      `isInterestingAttempt: ${label}`);
  }
}

// --- closeSearchPlanDetail back-button restore -----------------------
t.section('snapshotActiveTab()');

{
  // The active tab's data-tab-name attribute IS the internal name --
  // tabs.js is the one owner of that mapping, so there is no label to
  // reverse-map here (issue #1355 WE4 removed the old labelToName table).
  for (const name of ['browse', 'recents', 'pipeline', 'manual']) {
    const active = element({ className: 'tab active' });
    active.setAttribute('data-tab-name', name);
    const globals = stubGlobals({
      document: domStub({}, { querySelector: () => active }),
    });
    try {
      const snap = snapshotActiveTab();
      t.equal(snap.tab, name, `snapshotActiveTab() reads '${name}' straight off data-tab-name, no label mapping`);
    } finally {
      globals.restore();
    }
  }
}

{
  // Unknown/missing data-tab-name falls back to 'pipeline', same as the
  // pre-tabs.js code's default when no label matched.
  const noAttr = element({ className: 'tab active' });
  const bogus = element({ className: 'tab active' });
  bogus.setAttribute('data-tab-name', 'decisions'); // dead labelToName entry -- never a real tab
  const cases = [
    ['no active tab in the DOM at all', domStub({}, { querySelector: () => null })],
    ['active tab carries no data-tab-name', domStub({}, { querySelector: () => noAttr })],
    ["active tab's data-tab-name is not a real tab ('decisions')", domStub({}, { querySelector: () => bogus })],
  ];
  for (const [label, doc] of cases) {
    const globals = stubGlobals({ document: doc });
    try {
      const snap = snapshotActiveTab();
      t.equal(snap.tab, 'pipeline', `snapshotActiveTab() falls back to 'pipeline' when ${label}`);
    } finally {
      globals.restore();
    }
  }
}

t.section('closeSearchPlanDetail() / openSearchPlanDetail()');

/**
 * Tiny shim -- capture window-side effects without a real DOM.
 *
 * `impl` may be async: `closeSearchPlanDetail` no longer restores
 * scroll on a fixed frame count for a pipeline/recents/manual origin --
 * it stashes and leaves consumption to the destination's own render
 * (`loadPipeline`/`loadRecents`/`loadWrongMatches`, composed pins in
 * their own test files). A caller here that wants to prove "not called
 * before completion, called exactly once after" drives real microtask
 * ticks and the real exported `consumePendingScrollRestore` standing in
 * for that destination render finishing.
 *
 * @param {(win: any, calls: {showTab: string[], scrollTo: number[]}) => void | Promise<void>} impl
 */
async function withFakeWindow(impl) {
  const calls = { showTab: /** @type {string[]} */ ([]), scrollTo: /** @type {number[]} */ ([]) };
  const prevState = state.searchPlanDetailContext;
  const prevPipelineView = state.pipelineView;
  /** @type {any} */
  const fakeWindow = {
    scrollY: 0,
    /** @param {number} _x @param {number} y */
    scrollTo(_x, y) { calls.scrollTo.push(y); },
    /** @param {string} name */
    showTab(name) { calls.showTab.push(name); },
  };
  const globals = stubGlobals({
    window: fakeWindow,
    // The module reads `document.querySelector` inside `snapshotActiveTab`,
    // which we don't invoke here for the close-side path. Stub minimally.
    document: /** @type {any} */ ({
      querySelector() { return null; },
    }),
  });
  try {
    await impl(fakeWindow, calls);
  } finally {
    // Drain any stash a block forgot to consume, so a leftover pending
    // restore can never leak into the next block's assertions -- this
    // runs against the REAL fakeWindow still installed, so a drain here
    // still records into THIS block's own `calls.scrollTo`, not a
    // future block's.
    consumePendingScrollRestore();
    state.searchPlanDetailContext = prevState;
    state.pipelineView = prevPipelineView;
    globals.restore();
  }
}

{
  // AE3: originTab='browse', originScrollY=420. Browse has no
  // follow-up render on a tab switch (its DOM is untouched), so
  // closeSearchPlanDetail restores scroll itself, immediately -- no
  // destination to defer to.
  await withFakeWindow((win, calls) => {
    state.searchPlanDetailContext = {
      requestId: 2566,
      originTab: 'browse',
      originScrollY: 420,
      originSubView: null,
    };
    closeSearchPlanDetail();
    t.ok(calls.showTab.length === 1 && calls.showTab[0] === 'browse',
      'AE3: showTab("browse") called once');
    t.ok(calls.scrollTo.length === 1 && calls.scrollTo[0] === 420,
      'AE3: window.scrollTo(0, 420) restored immediately -- browse has no destination render to wait for');
    t.ok(state.searchPlanDetailContext === null,
      'AE3: stash cleared after close');
  });
}

{
  // Origin tab is pipeline+long-tail: pipelineView restored to
  // long-tail, and -- unlike browse -- closeSearchPlanDetail must NOT
  // apply the restore itself. loadPipeline() is the real destination
  // render and owns consuming it (composed pin in
  // tests/test_js_pipeline.mjs, which drives a genuinely deferred
  // fetch). This test proves the search_plan.js half of that contract
  // directly: the stash survives real microtask ticks untouched, firing
  // the consume function -- standing in for the destination's render
  // finishing -- restores exactly once with the exact position, and a
  // second consume call is a no-op.
  await withFakeWindow(async (win, calls) => {
    state.searchPlanDetailContext = {
      requestId: 100,
      originTab: 'pipeline',
      originScrollY: 64,
      originSubView: 'long-tail',
    };
    state.pipelineView = 'search-plan-detail';
    closeSearchPlanDetail();
    t.equal(state.pipelineView, 'long-tail',
      'pipeline-origin: pipelineView restored to long-tail');
    t.ok(calls.showTab.length === 1 && calls.showTab[0] === 'pipeline',
      'pipeline-origin: showTab("pipeline") called');
    t.equal(calls.scrollTo.length, 0,
      'pipeline-origin: scrollTo not called immediately -- the destination owns consuming the stash');
    await Promise.resolve();
    await Promise.resolve();
    t.equal(calls.scrollTo.length, 0,
      'pipeline-origin: scrollTo still not called after real microtask ticks -- no timer or frame heuristic remains');
    consumePendingScrollRestore();
    t.equal(calls.scrollTo.length, 1,
      'pipeline-origin: scrollTo called exactly once once the destination render completes');
    t.equal(calls.scrollTo[0], 64,
      'pipeline-origin: restores the exact origin scroll position');
    consumePendingScrollRestore();
    t.equal(calls.scrollTo.length, 1,
      'pipeline-origin: a second consume call is a no-op -- exactly once, not at-least-once');
  });
}

{
  // Origin tab is pipeline+dashboard: pipelineView restored to dashboard.
  await withFakeWindow((win, calls) => {
    state.searchPlanDetailContext = {
      requestId: 100,
      originTab: 'pipeline',
      originScrollY: 0,
      originSubView: 'dashboard',
    };
    state.pipelineView = 'search-plan-detail';
    closeSearchPlanDetail();
    t.equal(state.pipelineView, 'dashboard',
      'pipeline-origin dashboard subView restored');
  });
}

{
  // Origin tab is recents+acquisition: restore recentsSub. Recents also
  // defers the restore to its own destination render (composed pin in
  // tests/test_js_recents.mjs).
  await withFakeWindow((win, calls) => {
    state.searchPlanDetailContext = {
      requestId: 99,
      originTab: 'recents',
      originScrollY: 100,
      originSubView: 'acquisition',
    };
    state.recentsSub = 'history';
    closeSearchPlanDetail();
    t.equal(state.recentsSub, 'acquisition',
      'recents-origin: recentsSub restored to acquisition');
    t.ok(calls.showTab[0] === 'recents',
      'recents-origin: showTab("recents") called');
    t.equal(calls.scrollTo.length, 0,
      'recents-origin: scrollTo not called immediately -- the destination owns consuming the stash');
    consumePendingScrollRestore();
    t.ok(calls.scrollTo.length === 1 && calls.scrollTo[0] === 100,
      'recents-origin: scrollTo fires with the origin scroll position once consumed');
  });
}

{
  // PR5 renamed the importer subview to Imports; Back must restore it.
  await withFakeWindow((win, calls) => {
    state.searchPlanDetailContext = {
      requestId: 101,
      originTab: 'recents',
      originScrollY: 0,
      originSubView: 'imports',
    };
    state.recentsSub = 'history';
    closeSearchPlanDetail();
    t.equal(state.recentsSub, 'imports',
      'recents-origin: Imports subview restored');
    t.ok(calls.showTab[0] === 'recents',
      'imports-origin: showTab("recents") called');
  });
}

{
  // Origin tab is manual (Wrong Matches) — the third hardcoded name in
  // the pre-tabs.js `tab === 'pipeline' || tab === 'recents' || tab ===
  // 'manual'` condition (issue #1355 WE3 residual). No case here drove
  // this tab before `tabHasAsyncRender('manual')` replaced that literal
  // condition; without this test a mutant deleting 'manual' from
  // `tabs.js`'s registry would survive every existing assertion.
  await withFakeWindow((win, calls) => {
    state.searchPlanDetailContext = {
      requestId: 202,
      originTab: 'manual',
      originScrollY: 250,
      originSubView: null,
    };
    closeSearchPlanDetail();
    t.ok(calls.showTab.length === 1 && calls.showTab[0] === 'manual',
      'manual-origin: showTab("manual") called');
    t.equal(calls.scrollTo.length, 0,
      'manual-origin: scrollTo not called immediately — loadWrongMatches owns consuming the stash');
    consumePendingScrollRestore();
    t.ok(calls.scrollTo.length === 1 && calls.scrollTo[0] === 250,
      'manual-origin: scrollTo fires with the origin scroll position once consumed');
  });
}

{
  // Composed parity check: closeSearchPlanDetail's "does the destination
  // consume the restore itself" decision must equal tabs.js's own
  // `tabHasAsyncRender` for every real tab name, not a hand-copied
  // expectation that could silently re-drift from the registry
  // (issue #1355 WE4 — this is the exact fact the WE3 residual named).
  for (const tab of ['browse', 'recents', 'pipeline', 'manual']) {
    // eslint-disable-next-line no-await-in-loop
    await withFakeWindow((win, calls) => {
      state.searchPlanDetailContext = {
        requestId: 303, originTab: tab, originScrollY: 77, originSubView: null,
      };
      closeSearchPlanDetail();
      const scrollFiredImmediately = calls.scrollTo.length === 1;
      t.notEqual(scrollFiredImmediately, tabHasAsyncRender(tab),
        `closeSearchPlanDetail('${tab}') fires scrollTo immediately iff tabHasAsyncRender('${tab}') is false (got ${tabHasAsyncRender(tab)})`);
    });
  }
}

{
  // No origin context: fallback to pipeline/dashboard, no throw. There
  // is no stashed scroll position in this path (no ctx means no
  // scrollY to restore), so no consume is expected either.
  await withFakeWindow((win, calls) => {
    state.searchPlanDetailContext = null;
    state.pipelineView = 'search-plan-detail';
    let threw = false;
    try {
      closeSearchPlanDetail();
    } catch (err) {
      threw = true;
    }
    t.ok(!threw, 'no-origin: close does not throw');
    t.equal(state.pipelineView, 'dashboard',
      'no-origin: fallback to pipelineView=dashboard');
    t.ok(calls.showTab.length === 1 && calls.showTab[0] === 'pipeline',
      'no-origin: fallback shows the pipeline tab');
    t.equal(calls.scrollTo.length, 0,
      'no-origin: no scroll position was ever stashed, so nothing restores');
  });
}

{
  // Mutant-runner finding: the stash must happen BEFORE showTab is
  // called, not merely "eventually" -- a destination whose loader
  // consumes synchronously (rather than after an await, as every real
  // loader here does today) would see nothing pending yet if the
  // ordering ever inverted. Simulate exactly that: a showTab stub that
  // consumes the instant it is invoked, standing in for a fully
  // synchronous destination render. If stashScrollRestore ever moved to
  // run after showTab(tab), this synchronous consumer would find
  // nothing pending and scrollTo would never fire.
  await withFakeWindow((win, calls) => {
    win.showTab = (name) => {
      calls.showTab.push(name);
      consumePendingScrollRestore();
    };
    state.searchPlanDetailContext = {
      requestId: 555,
      originTab: 'pipeline',
      originScrollY: 999,
      originSubView: 'dashboard',
    };
    state.pipelineView = 'search-plan-detail';
    closeSearchPlanDetail();
    t.ok(calls.scrollTo.length === 1 && calls.scrollTo[0] === 999,
      'the stash is already set by the time showTab runs, so a synchronous destination consumer restores immediately');
  });
}

// --- U5: parseAdvanceTarget ------------------------------------------
//
// Pure validator covering the eight branches required by AE9. Each
// scenario passes a synthetic `{strategy?, ordinal?}` object (mirroring
// what the form's Confirm handler reads) and asserts the typed return
// or that a typed error fires.
t.section('parseAdvanceTarget()');

t.equal(
  JSON.stringify(parseAdvanceTarget({ strategy: 'track' })),
  JSON.stringify({ toStrategy: 'track' }),
  'AE9: strategy-only input → {toStrategy}',
);

t.equal(
  JSON.stringify(parseAdvanceTarget({ ordinal: '7' })),
  JSON.stringify({ toOrdinal: 7 }),
  'AE9: ordinal-only string input → {toOrdinal}',
);

t.equal(
  JSON.stringify(parseAdvanceTarget({ ordinal: 7 })),
  JSON.stringify({ toOrdinal: 7 }),
  'AE9: ordinal-only numeric input → {toOrdinal}',
);

t.throws(
  () => parseAdvanceTarget({ strategy: 'track', ordinal: '7' }),
  'AE9: both fields populated throws TypeError',
  TypeError,
);

t.throws(
  () => parseAdvanceTarget({}),
  'AE9: neither field populated throws TypeError',
  TypeError,
);

t.throws(
  () => parseAdvanceTarget({ ordinal: 'abc' }),
  'AE9: non-numeric ordinal throws TypeError',
  TypeError,
);

t.throws(
  () => parseAdvanceTarget({ ordinal: '-1' }),
  'AE9: negative ordinal throws TypeError',
  TypeError,
);

t.throws(
  () => parseAdvanceTarget({ strategy: '' }),
  'AE9: empty-string strategy throws TypeError',
  TypeError,
);

// Defensive — ordinal is non-integer (1.5).
t.throws(
  () => parseAdvanceTarget({ ordinal: '1.5' }),
  'AE9: non-integer ordinal (1.5) throws TypeError',
  TypeError,
);

// Numeric -1 covers the {ordinal: -1} numeric branch alongside the
// string branch above.
t.throws(
  () => parseAdvanceTarget({ ordinal: -1 }),
  'AE9: numeric -1 ordinal throws TypeError',
  TypeError,
);

// --- U5: renderAdvanceForm -------------------------------------------
t.section('renderAdvanceForm()');

{
  // Pure helper test — given an active plan with 10 slots and 5 unique
  // strategies, the form HTML includes a strategy <select> with 5
  // strategy options + leading "no choice", a number input with
  // max=9, and Confirm + Cancel buttons.
  const items = [];
  for (let i = 0; i < 10; i++) {
    items.push({
      id: i + 1, plan_id: 1, ordinal: i,
      strategy: `track_${i % 5}`, query: `q${i}`,
      canonical_query_key: `cqk${i}`, repeat_group: `rg${i}`,
      provenance: {},
    });
  }
  const html = renderAdvanceForm({
    activePlan: { plan: { id: 1 }, items, next_ordinal: 0, cycle_count: 0 },
    requestId: 42,
  });
  // Strategy select with leading "no choice" option + 5 unique strategies.
  t.contains(html, '<select',
    'renderAdvanceForm: emits a <select>');
  t.contains(html, '— (use ordinal)',
    'renderAdvanceForm: leading "— (use ordinal)" option present');
  for (let i = 0; i < 5; i++) {
    t.contains(html, `>track_${i}</option>`,
      `renderAdvanceForm: strategy option for track_${i}`);
  }
  // Strategy options are de-duped (5 unique strategies, not 10).
  const optionMatches = html.match(/<option /g) || [];
  t.equal(optionMatches.length, 6,
    'renderAdvanceForm: 6 options total (5 unique strategies + leading "—")');
  // Ordinal input bounded to items.length - 1.
  t.match(html, /<input[^>]*type="number"[^>]*min="0"/,
    'renderAdvanceForm: ordinal input is type="number" min="0"');
  t.contains(html, 'max="9"',
    'renderAdvanceForm: ordinal max=N-1 (items.length - 1)');
  // Confirm + Cancel buttons.
  t.match(html, />Confirm</, 'renderAdvanceForm: Confirm button present');
  t.match(html, />Cancel</, 'renderAdvanceForm: Cancel button present');
  // Confirm wires to the submit handler with the request id.
  t.contains(html, 'window.searchPlanSubmitAdvance(42',
    'renderAdvanceForm: Confirm button wires window.searchPlanSubmitAdvance');
  // Cancel wires to cancel handler.
  t.contains(html, 'window.searchPlanCancelAdvance(42',
    'renderAdvanceForm: Cancel button wires window.searchPlanCancelAdvance');
  // form id captured for the submit handler to read inputs back.
  t.contains(html, 'class="sp-advance-form"',
    'renderAdvanceForm: form has the sp-advance-form class');
  t.contains(html, 'data-field="strategy"',
    'renderAdvanceForm: strategy input data-field marker');
  t.contains(html, 'data-field="ordinal"',
    'renderAdvanceForm: ordinal input data-field marker');
}

// --- U5: REGENERATE_CONFIRM_MESSAGE includes "cursor" + "cycle" ------
//
// Origin R15 / AE8 mandate both substrings so the operator sees
// consequences before clicking through. The literal message is
// exported so this assertion does not depend on string matching the
// source code.
t.section('REGENERATE_CONFIRM_MESSAGE');

{
  const lower = REGENERATE_CONFIRM_MESSAGE.toLowerCase();
  t.contains(lower, 'cursor',
    'AE8: regenerate confirm message includes "cursor"');
  t.contains(lower, 'cycle',
    'AE8: regenerate confirm message includes "cycle"');
  t.ok(REGENERATE_CONFIRM_MESSAGE.length > 10,
    'AE8: regenerate confirm message is non-trivially long');
}

// --- U5: searchPlanRegenerate confirm gating ------------------------
//
// The action handler MUST call window.confirm with the published
// message before dispatching a fetch. We swap window.confirm + fetch
// for shims and observe both side effects.
t.section('searchPlanRegenerate()');

/**
 * Shim helper — swap globals (window.confirm, fetch, document) before
 * invoking impl, restore afterwards.
 *
 * @param {Object} opts
 * @param {boolean} [opts.confirmReturns]
 * @param {Object} [opts.fetchResp]   Response shape to return from the shim.
 * @param {(arg: any) => Promise<void>} impl
 */
async function withFetchAndConfirmShim(opts, impl) {
  const calls = {
    confirm: /** @type {string[]} */ ([]),
    fetch: /** @type {Array<{url: string, init: any}>} */ ([]),
    toast: /** @type {Array<{msg: string, isError: boolean|undefined}>} */ ([]),
    consoleError: /** @type {any[][]} */ ([]),
  };
  const prevState = state.searchPlanDetailContext;
  const prevPipelineView = state.pipelineView;
  const confirmReturns = opts.confirmReturns ?? true;
  const fetchResp = opts.fetchResp || { ok: true, status: 200, body: {} };
  /** @type {any} */
  const fakeWindow = {
    /** @param {string} msg */
    confirm(msg) { calls.confirm.push(msg); return confirmReturns; },
    scrollY: 0,
    /** @param {() => void} fn */
    requestAnimationFrame(fn) { fn(); return 1; },
    scrollTo() {},
    showTab() {},
  };
  /** @type {any} */
  const fakeDocument = {
    getElementById() { return null; },
    querySelector() { return null; },
    querySelectorAll() { return []; },
  };
  // Patch state.toast — toast is imported from state.js, so we patch
  // the underlying function via a wrapper that the import sees. The
  // search_plan.js module captures `toast` at module-evaluation time;
  // we can't replace it after the fact. Instead, we replace
  // `globalThis.document.getElementById` so the toast() call short-
  // circuits to a no-op (the toast helper bails when the #toast
  // element is missing — see web/js/state.js).
  const prevConsoleError = console.error;
  console.error = (/** @type {any[]} */ ...args) => {
    calls.consoleError.push(args);
  };
  const globals = stubGlobals({
    window: fakeWindow,
    document: fakeDocument,
    /** @type {any} */
    fetch: (/** @type {string} */ url, /** @type {any} */ init) => {
      calls.fetch.push({ url, init });
      return Promise.resolve({
        ok: fetchResp.ok ?? true,
        status: fetchResp.status ?? 200,
        text() {
          const body = fetchResp.body == null ? '' : JSON.stringify(fetchResp.body);
          return Promise.resolve(body);
        },
        json() { return Promise.resolve(fetchResp.body); },
      });
    },
  });
  try {
    await impl(calls);
  } finally {
    state.searchPlanDetailContext = prevState;
    state.pipelineView = prevPipelineView;
    globals.restore();
    console.error = prevConsoleError;
  }
}

// AE8: confirm returns false → no fetch.
await withFetchAndConfirmShim({ confirmReturns: false }, async (calls) => {
  await searchPlanRegenerate(2566);
  t.equal(calls.fetch.length, 0,
    'AE8: confirm=false suppresses the regenerate fetch');
  t.equal(calls.confirm.length, 1,
    'AE8: confirm dialog was shown once');
  t.equal(calls.confirm[0], REGENERATE_CONFIRM_MESSAGE,
    'AE8: confirm dialog received the published message');
});

// AE8: confirm returns true + 200 success → fetch dispatched, cache cleared.
await withFetchAndConfirmShim({
  confirmReturns: true,
  fetchResp: {
    ok: true, status: 200,
    body: { request_id: 2566, outcome: 'success', plan_id: 999 },
  },
}, async (calls) => {
  // Pre-populate cache so refresh-after-success can be observed.
  searchPlanCache.set(2566, {
    inspection: { foo: 'old' }, historyHead: [], fetchedAt: 1000,
  });
  await searchPlanRegenerate(2566);
  t.equal(calls.fetch.length, 1,
    'AE8: confirm=true dispatches one fetch');
  t.ok(calls.fetch[0].url.endsWith('/search-plan/regenerate'),
    'AE8: regenerate hits the regenerate endpoint');
  t.equal(calls.fetch[0].init.method, 'POST',
    'AE8: regenerate uses POST');
  t.equal(calls.fetch[0].init.body, '{}',
    'AE8: regenerate sends an empty JSON body');
  // Cache invalidated on success — refresh-after-success contract.
  t.ok(!searchPlanCache.has(2566),
    'AE8: cache for the request is cleared on regenerate success');
});

// Refresh-after-success: cache cleared on success_noop too.
await withFetchAndConfirmShim({
  confirmReturns: true,
  fetchResp: {
    ok: true, status: 200,
    body: { request_id: 2566, outcome: 'noop_active_plan_exists', plan_id: 999 },
  },
}, async (calls) => {
  searchPlanCache.set(2566, {
    inspection: { foo: 'old' }, historyHead: [], fetchedAt: 1000,
  });
  await searchPlanRegenerate(2566);
  t.ok(!searchPlanCache.has(2566),
    'noop_active_plan_exists also invalidates the cache');
});

// Failure path — 422 (failed_deterministic). NO cache mutation.
await withFetchAndConfirmShim({
  confirmReturns: true,
  fetchResp: {
    ok: false, status: 422,
    body: {
      request_id: 2566, outcome: 'failed_deterministic',
      error_message: 'metadata incomplete',
    },
  },
}, async (calls) => {
  searchPlanCache.set(2566, {
    inspection: { foo: 'old' }, historyHead: [], fetchedAt: 1000,
  });
  await searchPlanRegenerate(2566);
  t.ok(searchPlanCache.has(2566),
    '422 failure path does NOT invalidate the cache');
});

// Failure path — 503 (failed_transient). NO cache mutation.
await withFetchAndConfirmShim({
  confirmReturns: true,
  fetchResp: {
    ok: false, status: 503,
    body: {
      request_id: 2566, outcome: 'failed_transient',
      error_message: 'lock contention',
    },
  },
}, async (calls) => {
  searchPlanCache.set(2566, {
    inspection: { foo: 'old' }, historyHead: [], fetchedAt: 1000,
  });
  await searchPlanRegenerate(2566);
  t.ok(searchPlanCache.has(2566),
    '503 failure path does NOT invalidate the cache');
});

// --- U5: searchPlanAdvance error-mapping ----------------------------
t.section('searchPlanAdvance()');

// Happy path — 200 with outcome=advanced invalidates cache.
await withFetchAndConfirmShim({
  fetchResp: {
    ok: true, status: 200,
    body: {
      request_id: 2566, outcome: 'advanced', plan_id: 999,
      previous_ordinal: 0, new_ordinal: 5, new_strategy: 'track_5',
    },
  },
}, async (calls) => {
  searchPlanCache.set(2566, {
    inspection: { foo: 'old' }, historyHead: [], fetchedAt: 1000,
  });
  await searchPlanAdvance(2566, { toOrdinal: 5 });
  t.equal(calls.fetch.length, 1,
    'advance: dispatches one fetch with a typed target');
  t.ok(calls.fetch[0].url.endsWith('/search-plan/advance'),
    'advance: hits the advance endpoint');
  t.equal(calls.fetch[0].init.method, 'POST',
    'advance: uses POST');
  t.equal(JSON.parse(calls.fetch[0].init.body).to_ordinal, 5,
    'advance: serialises toOrdinal as to_ordinal in the request body');
  t.ok(!searchPlanCache.has(2566),
    'advance: cache invalidated on outcome=advanced');
});

// AE9 — 422 with invalid_target surfaces the API message via toast and
// does NOT invalidate the cache.
await withFetchAndConfirmShim({
  fetchResp: {
    ok: false, status: 422,
    body: {
      request_id: 2566, outcome: 'invalid_target',
      error_message: 'Forward-only: ordinal 1 is before cursor 5',
    },
  },
}, async (calls) => {
  searchPlanCache.set(2566, {
    inspection: { foo: 'old' }, historyHead: [], fetchedAt: 1000,
  });
  await searchPlanAdvance(2566, { toOrdinal: 1 });
  t.ok(searchPlanCache.has(2566),
    'AE9: invalid_target does NOT invalidate the cache');
  // The fetch was still dispatched (toast happens after the response).
  t.equal(calls.fetch.length, 1,
    'AE9: invalid_target reports the fetch was dispatched');
  // body sent the correct shape.
  t.equal(JSON.parse(calls.fetch[0].init.body).to_ordinal, 1,
    'AE9: body shape preserved on the failure path');
});

// 409 (no_active_plan) — toast + no cache invalidation.
await withFetchAndConfirmShim({
  fetchResp: {
    ok: false, status: 409,
    body: {
      request_id: 2566, outcome: 'no_active_plan',
      error_message: 'No active plan; regenerate first',
    },
  },
}, async (calls) => {
  searchPlanCache.set(2566, {
    inspection: { foo: 'old' }, historyHead: [], fetchedAt: 1000,
  });
  await searchPlanAdvance(2566, { toStrategy: 'track' });
  t.ok(searchPlanCache.has(2566),
    '409 no_active_plan: cache preserved');
  t.equal(JSON.parse(calls.fetch[0].init.body).to_strategy, 'track',
    'advance with toStrategy → to_strategy in body');
});

// 404 — toast + no cache invalidation.
await withFetchAndConfirmShim({
  fetchResp: {
    ok: false, status: 404,
    body: { request_id: 9999, outcome: 'request_not_found' },
  },
}, async (calls) => {
  searchPlanCache.set(9999, {
    inspection: { foo: 'old' }, historyHead: [], fetchedAt: 1000,
  });
  await searchPlanAdvance(9999, { toOrdinal: 0 });
  t.ok(searchPlanCache.has(9999),
    '404 request_not_found: cache preserved');
});

// 503 — toast retry + no cache invalidation.
await withFetchAndConfirmShim({
  fetchResp: {
    ok: false, status: 503,
    body: {
      request_id: 2566, outcome: 'failed_transient',
      error_message: 'lock contention',
    },
  },
}, async (calls) => {
  searchPlanCache.set(2566, {
    inspection: { foo: 'old' }, historyHead: [], fetchedAt: 1000,
  });
  await searchPlanAdvance(2566, { toOrdinal: 5 });
  t.ok(searchPlanCache.has(2566),
    '503 failed_transient: cache preserved');
});

// 400 — internal bug (form-side validation should have caught this).
// We expect a console.error in addition to the toast.
await withFetchAndConfirmShim({
  fetchResp: {
    ok: false, status: 400,
    body: { error: 'exactly one of to_ordinal or to_strategy is required' },
  },
}, async (calls) => {
  searchPlanCache.set(2566, {
    inspection: { foo: 'old' }, historyHead: [], fetchedAt: 1000,
  });
  await searchPlanAdvance(2566, { toOrdinal: 5 });
  t.ok(searchPlanCache.has(2566),
    '400 internal: cache preserved');
  t.ok(calls.consoleError.length >= 1,
    '400 internal bug: console.error logged');
});

// --- U5: stubs are gone ---------------------------------------------
t.section('U5: stub-removal sanity check');

{
  // Both handlers are real — they DO NOT throw "not implemented".
  // Confirm-cancelled regenerate returns silently; advance with no
  // target now opens the form (no fetch — the form stays in the DOM
  // and waits for the operator). Both call paths must NOT match the
  // U2 stub message.
  let regenError = null;
  await withFetchAndConfirmShim({ confirmReturns: false }, async () => {
    try { await searchPlanRegenerate(2566); }
    catch (err) { regenError = err; }
  });
  t.ok(regenError === null,
    'searchPlanRegenerate: confirm-cancel returns without throwing (no stub)');

  let advError = null;
  await withFetchAndConfirmShim({}, async () => {
    try { await searchPlanAdvance(2566, { toOrdinal: 0 }); }
    catch (err) { advError = err; }
  });
  t.ok(advError === null,
    'searchPlanAdvance: real implementation does not throw "not implemented"');
}

// --- F1/F2/F3/F13: Race-condition guards -----------------------------
//
// Concurrency tests that simulate operator-driven races: clicking Back
// during a fetch, double-clicking Advance, and so on. Each test uses a
// "deferred" Promise that the test resolves manually so we can land
// events between fetches.
t.section('Race-condition guards (F1/F2/F3/F13)');

/** @returns {{promise: Promise<any>, resolve: (v: any) => void, reject: (err: any) => void}} */
function makeDeferred() {
  /** @type {(v: any) => void} */
  let resolve = () => {};
  /** @type {(err: any) => void} */
  let reject = () => {};
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/**
 * Build a fake `Response` object suitable for `fetchInspection` and
 * `fetchHistoryPage` — they call `.json()` on success and `.text()` on
 * failure. The body is the inspection / history payload (ok=true).
 *
 * @param {Object} body
 */
function fakeOkResponse(body) {
  return {
    ok: true, status: 200,
    text() { return Promise.resolve(JSON.stringify(body)); },
    json() { return Promise.resolve(body); },
  };
}

/**
 * Race-test fixture. Stubs window/document/fetch; gives the test
 * control over which fetch resolves when. Cleans up on exit.
 *
 * @param {(ctx: {
 *   document: any, window: any,
 *   fetchCalls: string[],
 *   fetchQueue: Array<{promise: Promise<any>, resolve: (v: any) => void}>,
 *   element: any,
 *   getInnerHtml: () => string,
 * }) => Promise<void>} impl
 */
async function withRaceFixture(impl) {
  const fetchCalls = /** @type {string[]} */ ([]);
  /** @type {Array<{promise: Promise<any>, resolve: (v: any) => void}>} */
  const fetchQueue = [];
  const prevState = state.searchPlanDetailContext;
  const prevPipelineView = state.pipelineView;
  /** @type {string} */
  let innerHtml = '';
  /** @type {any} */
  const element = {
    set innerHTML(v) { innerHtml = v; },
    get innerHTML() { return innerHtml; },
    classList: {
      _classes: new Set(),
      add(/** @type {string} */ c) { this._classes.add(c); },
      remove(/** @type {string} */ c) { this._classes.delete(c); },
      contains(/** @type {string} */ c) { return this._classes.has(c); },
    },
    parentNode: { insertBefore() {} },
    closest() { return null; },
    querySelector() { return null; },
    querySelectorAll() { return []; },
  };
  /** @type {any} */
  const fakeDocument = {
    /** @param {string} _id */
    getElementById(_id) { return element; },
    querySelector() { return element; },
    querySelectorAll() { return []; },
    /** @param {string} _tag */
    createElement(_tag) {
      return {
        className: '',
        id: '',
        innerHTML: '',
        classList: {
          _classes: new Set(),
          add(/** @type {string} */ c) { this._classes.add(c); },
          remove(/** @type {string} */ c) { this._classes.delete(c); },
          contains(/** @type {string} */ c) { return this._classes.has(c); },
        },
      };
    },
  };
  /** @type {any} */
  const fakeWindow = {
    scrollY: 0,
    requestAnimationFrame(/** @type {() => void} */ fn) { fn(); return 1; },
    scrollTo() {},
    showTab() {},
  };
  const globals = stubGlobals({
    window: fakeWindow,
    document: fakeDocument,
    /** @type {any} */
    fetch: (/** @type {string} */ url) => {
      fetchCalls.push(url);
      const d = makeDeferred();
      fetchQueue.push({ promise: d.promise, resolve: d.resolve });
      return d.promise;
    },
  });
  try {
    await impl({
      document: fakeDocument,
      window: fakeWindow,
      fetchCalls,
      fetchQueue,
      element,
      getInnerHtml: () => innerHtml,
    });
  } finally {
    state.searchPlanDetailContext = prevState;
    state.pipelineView = prevPipelineView;
    globals.restore();
  }
}

// F1 — renderSearchPlanDetail: Back-during-fetch must not clobber the
// restored view. We start a render for requestId=42, then start a render
// for requestId=43 (which bumps the generation counter), then resolve
// 42's fetches. The 42 paint must NOT happen.
{
  await withRaceFixture(async (ctx) => {
    const inspection42 = makeDetailInspection({ request_id: 42 });
    inspection42.request_id = 42;
    const inspection43 = makeDetailInspection({ request_id: 43 });
    inspection43.request_id = 43;
    // Fire 42's render. It enqueues three fetches: inspection, history,
    // and the per-request pipeline payload for the library column.
    const p42 = renderSearchPlanDetail(42);
    // Fire 43's render. It enqueues three more.
    const p43 = renderSearchPlanDetail(43);
    t.equal(ctx.fetchCalls.length, 6,
      'F1: each detail render dispatches three fetches');
    // Resolve 43's first (positions 3, 4 and 5 in the queue) so the page
    // becomes "the 43 view".
    ctx.fetchQueue[3].resolve(fakeOkResponse(inspection43));
    ctx.fetchQueue[4].resolve(fakeOkResponse({ rows: [], next_before_id: null }));
    ctx.fetchQueue[5].resolve(fakeOkResponse(makeLibraryPayload()));
    await p43;
    // Capture the post-43-paint HTML.
    const after43 = ctx.getInnerHtml();
    t.ok(after43.includes('sp-detail') || after43.length > 0,
      'F1: detail page rendered after 43 resolved');
    // Now resolve 42's fetches — the stale render must NOT clobber.
    ctx.fetchQueue[0].resolve(fakeOkResponse(inspection42));
    ctx.fetchQueue[1].resolve(fakeOkResponse({ rows: [], next_before_id: null }));
    ctx.fetchQueue[2].resolve(fakeOkResponse(makeLibraryPayload()));
    await p42;
    // The HTML must still match the 43 paint, not get overwritten by 42.
    t.equal(ctx.getInnerHtml(), after43,
      'F1: stale 42 render did NOT clobber the live 43 paint');
  });
}

// #811 — the Interesting/All toggle repaints from the loaded rows and
// dispatches no fetch of its own.
{
  await withRaceFixture(async (ctx) => {
    const boring = {
      ...makeHistoryRows()[0],
      id: 12000, plan_strategy: 'BORING-STRAT',
      rejection_reason: null,
      candidates: [{ username: 'BORINGPEER', pre_filter_skip: true }],
    };
    const rows = [...makeHistoryRows(), boring];
    const render = renderSearchPlanDetail(42);
    ctx.fetchQueue[0].resolve(fakeOkResponse(makeDetailInspection()));
    ctx.fetchQueue[1].resolve(fakeOkResponse({ rows, next_before_id: null }));
    ctx.fetchQueue[2].resolve(fakeOkResponse(makeLibraryPayload()));
    await render;
    const afterRender = ctx.fetchCalls.slice();
    t.anyContains(afterRender, '/api/pipeline/42/search-plan',
      'toggle prereq: the detail render fetched the inspection payload');
    t.anyContains(afterRender, '/api/pipeline/42/search-plan/history',
      'toggle prereq: the detail render fetched a history page');
    t.anyContains(afterRender, '/api/pipeline/42',
      'toggle prereq: the detail render fetched the per-request payload');
    t.excludes(ctx.getInnerHtml(), 'BORING-STRAT',
      'toggle: the default Interesting view hides an uninteresting row');

    searchPlanSetAttemptsFilter(42, 'all');
    t.deepEqual(ctx.fetchCalls, afterRender,
      'toggle: switching to All dispatches no additional fetch');
    t.contains(ctx.getInnerHtml(), 'BORING-STRAT',
      'toggle: All re-renders the cached rows including the boring one');

    searchPlanSetAttemptsFilter(42, 'interesting');
    t.deepEqual(ctx.fetchCalls, afterRender,
      'toggle: switching back to Interesting dispatches no additional fetch');
    t.excludes(ctx.getInnerHtml(), 'BORING-STRAT',
      'toggle: switching back to Interesting hides the boring row again');

    searchPlanSetAttemptsFilter(42, 'nonsense');
    t.contains(ctx.getInnerHtml(), 'ATT-STRAT-A',
      'toggle: an unrecognised mode is ignored rather than blanking the view');
  });
}

// F2 — toggleSearchPlanSummary: two concurrent calls for the same id
// must trigger exactly ONE fetch (deduplication).
{
  await withRaceFixture(async (ctx) => {
    // Override getElementById so the first call returns null (creates
    // a panel), and subsequent calls return the panel we created.
    /** @type {any} */
    let createdPanel = null;
    ctx.document.getElementById = (/** @type {string} */ id) => {
      if (id === `sp-summary-42`) return createdPanel;
      return ctx.element;
    };
    const originalCreate = ctx.document.createElement;
    ctx.document.createElement = (/** @type {string} */ tag) => {
      const el = originalCreate(tag);
      // Simulate the panel being inserted.
      createdPanel = el;
      return el;
    };
    /** @type {any} */
    const rowEl = {
      parentNode: {
        insertBefore() {},
      },
      nextSibling: null,
    };

    // Two concurrent calls — both should NOT cause four fetches.
    const p1 = toggleSearchPlanSummary(42, rowEl);
    const p2 = toggleSearchPlanSummary(42, rowEl);
    // We expect 2 fetches at most (inspection + history) — not 4.
    t.equal(ctx.fetchCalls.length, 2,
      'F2: concurrent calls dedup — exactly one fetch pair dispatched');
    // Resolve to settle promises.
    ctx.fetchQueue[0].resolve(fakeOkResponse(makeInspection()));
    ctx.fetchQueue[1].resolve(fakeOkResponse({ rows: [], next_before_id: null }));
    await Promise.all([p1, p2]);
  });
}

// F2 (closing-mid-fetch) — Open the panel; while the fetch is still
// in-flight, simulate the panel being removed from the DOM (operator
// dismissed the page / closed the panel). Resolve the fetch. The
// detached panel must NOT receive the stale innerHTML.
{
  await withRaceFixture(async (ctx) => {
    /** @type {any} */
    let createdPanel = null;
    /** @type {boolean} */
    let panelLive = true;
    ctx.document.getElementById = (/** @type {string} */ id) => {
      if (id === `sp-summary-77`) return panelLive ? createdPanel : null;
      return ctx.element;
    };
    const originalCreate = ctx.document.createElement;
    ctx.document.createElement = (/** @type {string} */ tag) => {
      const el = originalCreate(tag);
      createdPanel = el;
      return el;
    };
    /** @type {any} */
    const rowEl = {
      parentNode: { insertBefore() {} },
      nextSibling: null,
    };
    // Open call (in-flight).
    const p1 = toggleSearchPlanSummary(77, rowEl);
    t.equal(ctx.fetchCalls.length, 2,
      'F2 close: open call dispatched the initial fetch pair');
    // Capture the loading-state HTML that was set synchronously.
    const beforeResolve = createdPanel.innerHTML;
    t.contains(beforeResolve, 'Loading',
      'F2 close: panel shows loading text while fetch is in-flight');
    // Simulate the panel being detached (operator clicked Close /
    // navigated away). getElementById will now return null for this id.
    panelLive = false;
    // Resolve the fetches.
    ctx.fetchQueue[0].resolve(fakeOkResponse(makeInspection()));
    ctx.fetchQueue[1].resolve(fakeOkResponse({ rows: [], next_before_id: null }));
    await p1;
    // F2 closed-mid-fetch guard: the detached panel must NOT have been
    // overwritten with the resolved summary HTML.
    t.equal(createdPanel.innerHTML, beforeResolve,
      'F2 close: detached-during-fetch panel was NOT overwritten by stale resolve');
  });
}

// F3 — searchPlanLoadOlder: rapid double-click should dispatch one fetch.
{
  await withRaceFixture(async (ctx) => {
    /** @type {any} */
    const button = {
      disabled: false,
    };
    /** @type {any} */
    const wrap = {
      innerHTML: '',
      remove() {},
      querySelector(/** @type {string} */ sel) {
        if (sel.includes('button')) return button;
        return null;
      },
    };
    /** @type {any} */
    const tbody = {
      insertAdjacentHTML() {},
      closest() {
        return {
          querySelector(/** @type {string} */ sel) {
            if (sel.includes('sp-load-older-wrap')) return wrap;
            return null;
          },
        };
      },
    };
    ctx.document.querySelector = (/** @type {string} */ sel) => {
      if (sel.includes('sp-attempts-tbody')) return tbody;
      if (sel.includes('sp-load-older-wrap')) return wrap;
      return null;
    };
    const p1 = searchPlanLoadOlder(42, 12300);
    // The very first call should immediately dispatch a fetch AND disable
    // the button before any await yields.
    t.equal(ctx.fetchCalls.length, 1,
      'F3: first load-older click fires one fetch');
    // Now click again — must NOT dispatch a second fetch.
    const p2 = searchPlanLoadOlder(42, 12300);
    t.equal(ctx.fetchCalls.length, 1,
      'F3: second click during in-flight load-older does NOT dispatch a second fetch');
    ctx.fetchQueue[0].resolve(fakeOkResponse({ rows: [], next_before_id: null }));
    await Promise.all([p1, p2]);
  });
}

// F13 — searchPlanRegenerate: rapid double-confirm should dispatch one
// regenerate POST. Reuses the regenerate fetch shim because confirm gating
// is still required.
{
  // Re-use withFetchAndConfirmShim but mutate it to track concurrency by
  // running two calls in parallel without awaiting.
  const calls = {
    fetch: /** @type {Array<{url: string, init: any}>} */ ([]),
  };
  const prevState = state.searchPlanDetailContext;
  const prevPipelineView = state.pipelineView;
  /** @type {Array<{promise: Promise<any>, resolve: (v: any) => void}>} */
  const fetchQueue = [];
  const globals = stubGlobals({
    /** @type {any} */
    window: {
      confirm() { return true; },
      scrollY: 0,
      requestAnimationFrame(/** @type {() => void} */ fn) { fn(); return 1; },
      scrollTo() {},
      showTab() {},
    },
    /** @type {any} */
    document: {
      getElementById() { return null; },
      querySelector() { return null; },
      querySelectorAll() { return []; },
    },
    /** @type {any} */
    fetch: (/** @type {string} */ url, /** @type {any} */ init) => {
      calls.fetch.push({ url, init });
      const d = makeDeferred();
      fetchQueue.push({ promise: d.promise, resolve: d.resolve });
      return d.promise.then((body) => ({
        ok: true, status: 200,
        text() { return Promise.resolve(JSON.stringify(body)); },
        json() { return Promise.resolve(body); },
      }));
    },
  });
  try {
    const p1 = searchPlanRegenerate(2566);
    const p2 = searchPlanRegenerate(2566);
    // Only the first call should have dispatched a fetch. The second
    // is suppressed by the in-flight guard.
    t.equal(calls.fetch.length, 1,
      'F13: concurrent regenerate clicks dispatch exactly one fetch');
    fetchQueue[0].resolve({ request_id: 2566, outcome: 'success', plan_id: 999 });
    await Promise.all([p1, p2]);
  } finally {
    state.searchPlanDetailContext = prevState;
    state.pipelineView = prevPipelineView;
    globals.restore();
  }
}

// F8 — searchPlanRegenerate: 404 (request_not_found). Should toast the
// error message AND preserve the cache (no invalidation).
await withFetchAndConfirmShim({
  confirmReturns: true,
  fetchResp: {
    ok: false, status: 404,
    body: {
      request_id: 9999, outcome: 'request_not_found',
      error_message: 'Request 9999 does not exist',
    },
  },
}, async (calls) => {
  searchPlanCache.set(9999, {
    inspection: { foo: 'old' }, historyHead: [], fetchedAt: 1000,
  });
  await searchPlanRegenerate(9999);
  t.ok(searchPlanCache.has(9999),
    'F8: 404 request_not_found does NOT invalidate the cache');
  t.equal(calls.fetch.length, 1,
    'F8: 404 reports one fetch was dispatched (toast happens after)');
});

// --- F14: Testing gaps -----------------------------------------------
t.section('F14: misc test gaps');

// F14.1 — renderSummaryPanel debug-line branch: no active plan and no
// failure plan → surfaces the booleans.
{
  const inspection = makeInspection({
    active_plan: null,
    currentness: {
      is_wanted: true, has_active_plan: false, generator_id_mismatch: false,
      has_deterministic_failure: false, has_retryable_failure: false,
    },
    latest_failed_deterministic: null,
  });
  const html = renderSummaryPanel({ inspection, history: { rows: [] } });
  t.contains(html, 'No active plan',
    'F14.1: no-plan + no-failure surfaces the "No active plan" debug line');
  t.contains(html, 'has_active_plan=',
    'F14.1: debug line surfaces has_active_plan=');
  t.contains(html, 'has_deterministic_failure=',
    'F14.1: debug line surfaces has_deterministic_failure=');
  t.contains(html, 'has_retryable_failure=',
    'F14.1: debug line surfaces has_retryable_failure=');
}

// F14.2 — closeSearchPlanDetail with origin browse. Browse has no
// sub-views since the unified artist page (#575 PR4); the back button
// just restores the tab.
{
  withFakeWindow((win, calls) => {
    state.searchPlanDetailContext = {
      requestId: 42,
      originTab: 'browse',
      originScrollY: 100,
      originSubView: null,
    };
    closeSearchPlanDetail();
    t.equal(calls.showTab[0], 'browse',
      'F14.2: browse-origin restores the Browse tab');
  });
}

// F14.3 — searchPlanAdvance malformed target: target carries
// `toOrdinal` / `toStrategy` keys but with values of the wrong type.
// This hits the "malformed target" branch (after the "open the form"
// check, before the POST). Must NOT dispatch a fetch and must
// console.error.
{
  await withFetchAndConfirmShim({}, async (calls) => {
    /** @type {any} */
    const malformed = { toOrdinal: 'not-a-number', toStrategy: 12345 };
    await searchPlanAdvance(2566, malformed);
    t.equal(calls.fetch.length, 0,
      'F14.3: malformed target does NOT dispatch a fetch');
    t.ok(calls.consoleError.length >= 1,
      'F14.3: malformed target logs console.error');
  });
}

// --- F12: Tab-switch clears search-plan-detail context ---------------
//
// Imported via the live main.js — but main.js attaches showTab to the
// window only when imported in a browser. We test the behaviour
// directly by simulating: state.pipelineView='search-plan-detail' →
// showTab('browse') → expect state.pipelineView reset.
//
// main.js's showTab is a closure over imported state. We reach it by
// dynamic import after stubbing globals.
t.section('F12: tab-switch clears detail context');

{
  // Set up DOM stubs needed by main.js's showTab.
  const prevState = state.searchPlanDetailContext;
  const prevPipelineView = state.pipelineView;
  /** @type {any} */
  const fakeTabEl = { classList: { add() {}, remove() {} } };
  /** @type {any} */
  const fakeSecEl = { classList: { add() {}, remove() {} } };
  const globals = stubGlobals({
    /** @type {any} */
    window: { setTimeout: () => 0 },
    /** @type {any} */
    document: {
      querySelectorAll() {
        return {
          forEach(/** @type {(t: any) => void} */ fn) {
            fn({ classList: { remove() {} } });
          },
        };
      },
      querySelector() { return fakeTabEl; },
      getElementById(/** @type {string} */ id) {
        if (id === 'q') return null;
        return fakeSecEl;
      },
    },
  });
  try {
    // Dynamically import main.js so it sees our stubbed globals.
    // main.js wires window.showTab at import; we read it back.
    await import('../web/js/main.js');
    /** @type {any} */
    const showTab = globalThis.window.showTab;
    /** @type {any} */
    const showTabPreserving = globalThis.window.showTabPreservingDetail;
    t.ok(typeof showTab === 'function',
      'F12 prereq: main.js wires window.showTab');
    t.ok(typeof showTabPreserving === 'function',
      'F12 prereq: main.js wires window.showTabPreservingDetail');
    // Set up: pipelineView is search-plan-detail, context populated.
    state.pipelineView = 'search-plan-detail';
    state.searchPlanDetailContext = {
      requestId: 42, originTab: 'browse',
      originScrollY: 0, originSubView: null,
    };
    // Switch to a different tab.
    showTab('browse');
    t.equal(state.pipelineView, 'dashboard',
      'F12: switching away resets search-plan-detail to dashboard');
    t.ok(state.searchPlanDetailContext === null,
      'F12: detail context cleared when leaving search-plan-detail');
    // Now re-set and switch INTO pipeline directly — should also reset.
    state.pipelineView = 'search-plan-detail';
    state.searchPlanDetailContext = {
      requestId: 42, originTab: 'browse',
      originScrollY: 0, originSubView: null,
    };
    showTab('pipeline');
    t.equal(state.pipelineView, 'dashboard',
      'F12: switching into pipeline resets stuck detail state to dashboard');
    t.ok(state.searchPlanDetailContext === null,
      'F12: switching into pipeline tab clears stale detail context');
    // Verify the openSearchPlanDetail flow does NOT trip the reset:
    // showTabPreservingDetail wraps showTab and preserves the
    // freshly-set pipelineView='search-plan-detail'.
    state.pipelineView = 'search-plan-detail';
    state.searchPlanDetailContext = {
      requestId: 99, originTab: 'browse',
      originScrollY: 0, originSubView: null,
    };
    showTabPreserving('pipeline');
    t.equal(state.pipelineView, 'search-plan-detail',
      'F12: showTabPreservingDetail preserves pipelineView for openSearchPlanDetail flow');
    t.ok(state.searchPlanDetailContext !== null,
      'F12: showTabPreservingDetail preserves detail context');
  } finally {
    state.searchPlanDetailContext = prevState;
    state.pipelineView = prevPipelineView;
    globals.restore();
  }
}

// --- Summary ---------------------------------------------------------
t.done();
