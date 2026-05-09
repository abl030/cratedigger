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
  renderDetailPage,
  renderSearchPlanDetail,
  closeSearchPlanDetail,
  searchPlanRefreshDetail,
  parseAdvanceTarget,
  renderAdvanceForm,
  searchPlanRegenerate,
  searchPlanAdvance,
  REGENERATE_CONFIRM_MESSAGE,
} from '../web/js/search_plan.js';
import { state } from '../web/js/state.js';

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

// --- renderDetailPage / closeSearchPlanDetail / pagination -----------
//
// U4 scenarios. All DOM-free string-match assertions on the HTML
// returned by `renderDetailPage`, plus pure assertions on the back-
// button restore logic via tiny window/document shims (no jsdom).
console.log('renderDetailPage()');

/**
 * Build a slot-stats bucket for the inspection.stats.current.slots block.
 */
function makeSlotStats() {
  return {
    request_id: 2566,
    current: {
      slots: [
        {
          identity: { plan_ordinal: 0, plan_strategy: 'track_0' },
          attempts: 4, consumed_attempts: 3, non_consuming_attempts: 1,
          stale_completion_attempts: 0,
          outcome_counts: { found: 1, no_match: 2, error: 0, no_results: 1 },
          elapsed_s_mean: 4.21, elapsed_s_p95: 9.30,
          result_count_mean: 5.5, browse_time_s_mean: 1.1,
          match_time_s_mean: 3.0, peers_browsed_mean: 6.0,
          fanout_waves_mean: 2.0, last_seen_at: '2026-05-09T01:00:00Z',
        },
        {
          identity: { plan_ordinal: 1, plan_strategy: 'track_1' },
          attempts: 2, consumed_attempts: 2, non_consuming_attempts: 0,
          stale_completion_attempts: 0,
          outcome_counts: { found: 0, no_match: 1, error: 1, no_results: 0 },
          elapsed_s_mean: 5.5, elapsed_s_p95: 7.0,
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

/**
 * Build a complete inspection payload appropriate for the detail page.
 */
function makeDetailInspection(overrides = {}) {
  const base = makeInspection(overrides);
  base.stats = overrides.stats ?? makeSlotStats();
  if (overrides.legacy_logs !== undefined) {
    base.legacy_logs = overrides.legacy_logs;
  }
  if (overrides.latest_failed_deterministic !== undefined) {
    base.latest_failed_deterministic = overrides.latest_failed_deterministic;
  }
  if (overrides.latest_failed_transient !== undefined) {
    base.latest_failed_transient = overrides.latest_failed_transient;
  }
  // Add provenance so health-block tests have something to render.
  if (base.active_plan && base.active_plan.plan && overrides.provenance !== undefined) {
    base.active_plan.plan.provenance = overrides.provenance;
  }
  return base;
}

function makeHistoryRows() {
  return [
    {
      id: 12345, created_at: '2026-05-09T03:00:00Z', request_id: 2566,
      plan_id: 583, plan_item_id: 5821, plan_ordinal: 2,
      plan_strategy: 'track_2', plan_canonical_query_key: 'foo',
      plan_repeat_group: 'track_2', plan_generator_id: '13',
      execution_stage: 'accepted', attempt_consumed: true,
      cursor_update_status: 'advanced', stale_reason: null,
      plan_cycle_snapshot: 1,
      outcome: 'no_match', variant: 'track_2', query: 'q-current',
      result_count: 12, elapsed_s: 4.23, final_state: 'Completed',
      candidates: [{ user: 'peer-A', score: 0.9 }, { user: 'peer-B', score: 0.7 }],
      browse_time_s: 1.2, match_time_s: 3.0,
      peers_browsed: 5, peers_browsed_lazy: 2, fanout_waves: 2,
    },
    {
      id: 12340, created_at: '2026-05-09T02:00:00Z', request_id: 2566,
      plan_id: 583, plan_item_id: 5820, plan_ordinal: 1,
      plan_strategy: 'track_1', plan_canonical_query_key: 'bar',
      plan_repeat_group: 'track_1', plan_generator_id: '13',
      execution_stage: 'stale_completion', attempt_consumed: false,
      cursor_update_status: 'stale', stale_reason: 'plan_superseded',
      plan_cycle_snapshot: 0,
      outcome: 'partial', variant: 'track_1', query: 'q-stale',
      result_count: 3, elapsed_s: 1.10, final_state: 'Cancelled',
      candidates: null,
      browse_time_s: 0.5, match_time_s: 0.6,
      peers_browsed: 1, peers_browsed_lazy: 0, fanout_waves: 1,
    },
  ];
}

{
  // AE5: detail page surfaces every required telemetry column + plan
  // structure + per-slot stats + plan-health + collapsed pre-rollout.
  const inspection = makeDetailInspection({
    legacy_logs: { count: 12, head: [
      { id: 1, created_at: '2026-04-01T00:00:00Z', outcome: 'no_match',
        variant: 'fallback', query: 'old_q', result_count: 0,
        elapsed_s: 1.0, final_state: 'Completed' },
    ]},
    latest_failed_deterministic: {
      plan: { id: 580, generator_id: '13', failure_class: 'no_runnable_query',
        error_message: 'metadata incomplete', created_at: '2026-05-08T00:00:00Z' },
    },
    provenance: {
      omitted_candidates: ['weird thing'],
      deduped_losers: ['lose-1'],
      dropped_low_entropy_tokens: ['the'],
    },
  });
  const html = renderDetailPage({
    inspection,
    history: makeHistoryRows(),
    nextBeforeId: 12300,
  });
  // Plan slot list rendered, with the cursor (next_ordinal=2) highlighted.
  assert(html.includes('sp-slot-list'),
    'AE5: slot list rendered');
  assert(html.includes('sp-slot-current'),
    'AE5: cursor slot has sp-slot-current marker');
  // Plan-aware history table with telemetry columns visible.
  assert(html.includes('sp-history-table'),
    'AE5: plan-aware history table present');
  assert(html.includes('Outcome') && html.includes('Strategy')
    && html.includes('Elapsed') && html.includes('Final state')
    && html.includes('Cursor') && html.includes('Stale')
    && html.includes('Consumed') && html.includes('Cycle')
    && html.includes('Peers') && html.includes('Fanout')
    && html.includes('Forensics'),
    'AE5: history columns include outcome/strategy/elapsed/final_state/cursor/stale/consumed/cycle/peers/fanout/forensics');
  // Specific row data: ordinal 2, strategy track_2, attempt_consumed=yes.
  assert(html.includes('q-current'),
    'AE5: current attempt query rendered');
  assert(html.includes('q-stale'),
    'AE5: stale attempt query rendered');
  assert(html.includes('plan_superseded'),
    'AE5: stale_reason rendered for the second row');
  assert(html.includes('sp-history-row-stale'),
    'AE5: stale row carries the .sp-history-row-stale CSS class');
  assert(html.includes('sp-candidate-forensics'),
    'AE5: candidate forensics rendered as <details>');
  assert(html.includes('peer-A'),
    'AE5: candidate JSONB serialised inside the forensics block');
  // Per-slot stats.
  assert(html.includes('sp-stats-table'),
    'AE5: per-slot stats table rendered');
  assert(html.includes('track_0') && html.includes('track_1'),
    'AE5: per-slot stats include both strategies');
  // Plan-health block.
  assert(html.includes('sp-health'),
    'AE5: plan-health block rendered');
  assert(html.includes('no_runnable_query'),
    'AE5: failure class surfaced in plan-health');
  assert(html.includes('metadata incomplete'),
    'AE5: failure error_message surfaced');
  assert(html.includes('omitted_candidates'),
    'AE5: provenance: omitted_candidates rendered');
  assert(html.includes('deduped_losers'),
    'AE5: provenance: deduped_losers rendered');
  assert(html.includes('dropped_low_entropy_tokens'),
    'AE5: provenance: dropped_low_entropy_tokens rendered');
  // Pre-rollout legacy section, collapsed.
  assert(html.includes('sp-history-legacy-section'),
    'AE5: pre-rollout legacy section rendered');
  assert(html.includes('Pre-rollout history'),
    'AE5: pre-rollout summary text present');
  assert(html.includes('<details class="sp-history-legacy">'),
    'AE5: pre-rollout block is collapsed via <details>');
  assert(html.includes('class="sp-history-row legacy"'),
    'AE5: legacy rows tagged distinctly');
  assert(html.includes('old_q'),
    'AE5: legacy row content rendered inside the collapsed block');
}

{
  // AE6: cache stat label includes the literal substring "cycle-level".
  const inspection = makeDetailInspection();
  const html = renderDetailPage({
    inspection,
    history: makeHistoryRows(),
    nextBeforeId: null,
  });
  assert(html.includes('cycle-level'),
    'AE6: cache attribution label literally reads "cycle-level"');
  assert(html.includes('Cache attribution'),
    'AE6: cache attribution label introduces the level');
}

{
  // AE7: plan-aware and legacy rows render in clearly-distinguished
  // sections (different CSS classes, different parent sections).
  const inspection = makeDetailInspection({
    legacy_logs: { count: 5, head: [
      { id: 9, created_at: '2026-04-09T00:00:00Z', outcome: 'no_match',
        variant: 'fallback', query: 'legacy-q1', result_count: 1,
        elapsed_s: 1.5, final_state: 'Completed' },
      { id: 8, created_at: '2026-04-08T00:00:00Z', outcome: 'no_match',
        variant: 'fallback', query: 'legacy-q2', result_count: 0,
        elapsed_s: 0.9, final_state: 'Completed' },
    ]},
  });
  const html = renderDetailPage({
    inspection,
    history: makeHistoryRows(),
    nextBeforeId: null,
  });
  // Plan-aware rows live inside .sp-history-table without .legacy.
  assert(/<tr class="sp-history-row[ "]/.test(html),
    'AE7: plan-aware rows use .sp-history-row without .legacy');
  // Legacy rows live in .sp-history-legacy-section as .sp-history-row.legacy.
  assert(html.includes('class="sp-history-row legacy"'),
    'AE7: legacy rows distinguished via .legacy CSS suffix');
  assert(html.includes('sp-history-legacy-section'),
    'AE7: legacy rows live in their own section');
  // The two rendered queries must both appear, in the expected sections.
  assert(html.includes('q-current') && html.includes('legacy-q1'),
    'AE7: both plan-aware and legacy queries rendered');
}

{
  // AE10: a Refresh button is rendered and bound to the window handler.
  // The function-spec assertion (export exists, button HTML present) is
  // sufficient — fetch wiring is exercised by impure tests (not unit-runable here).
  assert(typeof renderSearchPlanDetail === 'function',
    'AE10: renderSearchPlanDetail is exported as a function');
  assert(typeof searchPlanRefreshDetail === 'function',
    'AE10: searchPlanRefreshDetail is exported (Refresh handler)');
  const html = renderDetailPage({
    inspection: makeDetailInspection(),
    history: makeHistoryRows(),
    nextBeforeId: null,
  });
  assert(html.includes('window.searchPlanRefreshDetail'),
    'AE10: Refresh button wires to window.searchPlanRefreshDetail');
  assert(/>\s*Refresh\s*</.test(html),
    'AE10: Refresh button label rendered');
}

{
  // Pagination — first render with a cursor renders a Load older button.
  const html = renderDetailPage({
    inspection: makeDetailInspection(),
    history: makeHistoryRows(),
    nextBeforeId: 12300,
  });
  assert(html.includes('sp-load-older-button'),
    'pagination: Load older button rendered when nextBeforeId is non-null');
  assert(html.includes('window.searchPlanLoadOlder(2566, 12300)'),
    'pagination: button onclick wires the cursor seed');
}

{
  // Pagination — exhausted: no Load older button.
  const html = renderDetailPage({
    inspection: makeDetailInspection(),
    history: makeHistoryRows(),
    nextBeforeId: null,
  });
  assert(!html.includes('sp-load-older-button'),
    'pagination: no Load older button when nextBeforeId is null');
}

{
  // Edge — no plan-aware history, but legacy rows exist. The page shows
  // an empty-state for plan-aware and a populated legacy section.
  const inspection = makeDetailInspection({
    legacy_logs: { count: 1, head: [
      { id: 1, created_at: '2026-04-01T00:00:00Z', outcome: 'no_match',
        variant: 'fallback', query: 'legacy-only', result_count: 1,
        elapsed_s: 1.0, final_state: 'Completed' },
    ]},
  });
  const html = renderDetailPage({
    inspection,
    history: [],
    nextBeforeId: null,
  });
  assert(html.includes('No plan-aware attempts yet'),
    'edge: empty plan-aware history shows an empty-state message');
  assert(html.includes('legacy-only'),
    'edge: legacy section still renders the legacy row');
}

{
  // Edge — both empty: empty-state for both, no errors.
  const inspection = makeDetailInspection({
    legacy_logs: { count: 0, head: [] },
  });
  const html = renderDetailPage({
    inspection,
    history: [],
    nextBeforeId: null,
  });
  assert(html.includes('No plan-aware attempts yet'),
    'both-empty: plan-aware empty-state shown');
  assert(html.includes('No legacy attempts'),
    'both-empty: legacy empty-state shown');
}

// --- closeSearchPlanDetail back-button restore -----------------------
console.log('closeSearchPlanDetail() / openSearchPlanDetail()');

/**
 * Tiny shim — capture window-side effects without a real DOM.
 */
function withFakeWindow(impl) {
  const calls = { showTab: [], scrollTo: [], rafCount: 0, scheduledScrolls: [] };
  const prevState = state.searchPlanDetailContext;
  const prevPipelineView = state.pipelineView;
  const prevWindow = globalThis.window;
  const prevDocument = globalThis.document;
  /** @type {any} */
  const fakeWindow = {
    scrollY: 0,
    /** @param {number} _x @param {number} y */
    scrollTo(_x, y) { calls.scrollTo.push(y); },
    /** @param {() => void} fn */
    requestAnimationFrame(fn) {
      calls.rafCount += 1;
      // Run the callback inline — the test then inspects calls.scrollTo.
      fn();
      return calls.rafCount;
    },
    /** @param {string} name */
    showTab(name) { calls.showTab.push(name); },
  };
  globalThis.window = fakeWindow;
  // The module reads `document.querySelector` inside `snapshotActiveTab`,
  // which we don't invoke here for the close-side path. Stub minimally.
  globalThis.document = /** @type {any} */ ({
    querySelector() { return null; },
  });
  try {
    impl(fakeWindow, calls);
  } finally {
    state.searchPlanDetailContext = prevState;
    state.pipelineView = prevPipelineView;
    if (prevWindow === undefined) delete globalThis.window;
    else globalThis.window = prevWindow;
    if (prevDocument === undefined) delete globalThis.document;
    else globalThis.document = prevDocument;
  }
}

{
  // AE3: originTab='browse', originScrollY=420 → showTab('browse'),
  // scrollTo scheduled to 420.
  withFakeWindow((win, calls) => {
    state.searchPlanDetailContext = {
      requestId: 2566,
      originTab: 'browse',
      originScrollY: 420,
      originSubView: null,
    };
    closeSearchPlanDetail();
    assert(calls.showTab.length === 1 && calls.showTab[0] === 'browse',
      'AE3: showTab("browse") called once');
    assert(calls.scrollTo.length === 1 && calls.scrollTo[0] === 420,
      'AE3: window.scrollTo(0, 420) scheduled via requestAnimationFrame');
    assert(state.searchPlanDetailContext === null,
      'AE3: stash cleared after close');
  });
}

{
  // Origin tab is pipeline+queue: pipelineView restored to queue.
  withFakeWindow((win, calls) => {
    state.searchPlanDetailContext = {
      requestId: 100,
      originTab: 'pipeline',
      originScrollY: 64,
      originSubView: 'queue',
    };
    state.pipelineView = 'search-plan-detail';
    closeSearchPlanDetail();
    assertEqual(state.pipelineView, 'queue',
      'pipeline-origin: pipelineView restored to queue');
    assert(calls.showTab.length === 1 && calls.showTab[0] === 'pipeline',
      'pipeline-origin: showTab("pipeline") called');
    assert(calls.scrollTo.length === 1 && calls.scrollTo[0] === 64,
      'pipeline-origin: scrollTo scheduled to origin scrollY');
  });
}

{
  // Origin tab is pipeline+dashboard: pipelineView restored to dashboard.
  withFakeWindow((win, calls) => {
    state.searchPlanDetailContext = {
      requestId: 100,
      originTab: 'pipeline',
      originScrollY: 0,
      originSubView: 'dashboard',
    };
    state.pipelineView = 'search-plan-detail';
    closeSearchPlanDetail();
    assertEqual(state.pipelineView, 'dashboard',
      'pipeline-origin dashboard subView restored');
  });
}

{
  // Origin tab is recents+downloading: restore recentsSub.
  withFakeWindow((win, calls) => {
    state.searchPlanDetailContext = {
      requestId: 99,
      originTab: 'recents',
      originScrollY: 100,
      originSubView: 'downloading',
    };
    state.recentsSub = 'history';
    closeSearchPlanDetail();
    assertEqual(state.recentsSub, 'downloading',
      'recents-origin: recentsSub restored to downloading');
    assert(calls.showTab[0] === 'recents',
      'recents-origin: showTab("recents") called');
  });
}

{
  // No origin context: fallback to pipeline/queue, no throw.
  withFakeWindow((win, calls) => {
    state.searchPlanDetailContext = null;
    state.pipelineView = 'search-plan-detail';
    let threw = false;
    try {
      closeSearchPlanDetail();
    } catch (err) {
      threw = true;
    }
    assert(!threw, 'no-origin: close does not throw');
    assertEqual(state.pipelineView, 'queue',
      'no-origin: fallback to pipelineView=queue');
    assert(calls.showTab.length === 1 && calls.showTab[0] === 'pipeline',
      'no-origin: fallback shows the pipeline tab');
  });
}

// --- U5: parseAdvanceTarget ------------------------------------------
//
// Pure validator covering the eight branches required by AE9. Each
// scenario passes a synthetic `{strategy?, ordinal?}` object (mirroring
// what the form's Confirm handler reads) and asserts the typed return
// or that a typed error fires.
console.log('parseAdvanceTarget()');

assertEqual(
  JSON.stringify(parseAdvanceTarget({ strategy: 'track' })),
  JSON.stringify({ toStrategy: 'track' }),
  'AE9: strategy-only input → {toStrategy}',
);

assertEqual(
  JSON.stringify(parseAdvanceTarget({ ordinal: '7' })),
  JSON.stringify({ toOrdinal: 7 }),
  'AE9: ordinal-only string input → {toOrdinal}',
);

assertEqual(
  JSON.stringify(parseAdvanceTarget({ ordinal: 7 })),
  JSON.stringify({ toOrdinal: 7 }),
  'AE9: ordinal-only numeric input → {toOrdinal}',
);

assertThrows(
  () => parseAdvanceTarget({ strategy: 'track', ordinal: '7' }),
  TypeError,
  'AE9: both fields populated throws TypeError',
);

assertThrows(
  () => parseAdvanceTarget({}),
  TypeError,
  'AE9: neither field populated throws TypeError',
);

assertThrows(
  () => parseAdvanceTarget({ ordinal: 'abc' }),
  TypeError,
  'AE9: non-numeric ordinal throws TypeError',
);

assertThrows(
  () => parseAdvanceTarget({ ordinal: '-1' }),
  TypeError,
  'AE9: negative ordinal throws TypeError',
);

assertThrows(
  () => parseAdvanceTarget({ strategy: '' }),
  TypeError,
  'AE9: empty-string strategy throws TypeError',
);

// Defensive — ordinal is non-integer (1.5).
assertThrows(
  () => parseAdvanceTarget({ ordinal: '1.5' }),
  TypeError,
  'AE9: non-integer ordinal (1.5) throws TypeError',
);

// Numeric -1 covers the {ordinal: -1} numeric branch alongside the
// string branch above.
assertThrows(
  () => parseAdvanceTarget({ ordinal: -1 }),
  TypeError,
  'AE9: numeric -1 ordinal throws TypeError',
);

// --- U5: renderAdvanceForm -------------------------------------------
console.log('renderAdvanceForm()');

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
  assert(html.includes('<select'),
    'renderAdvanceForm: emits a <select>');
  assert(html.includes('— (use ordinal)'),
    'renderAdvanceForm: leading "— (use ordinal)" option present');
  for (let i = 0; i < 5; i++) {
    assert(html.includes(`>track_${i}</option>`),
      `renderAdvanceForm: strategy option for track_${i}`);
  }
  // Strategy options are de-duped (5 unique strategies, not 10).
  const optionMatches = html.match(/<option /g) || [];
  assertEqual(optionMatches.length, 6,
    'renderAdvanceForm: 6 options total (5 unique strategies + leading "—")');
  // Ordinal input bounded to items.length - 1.
  assert(/<input[^>]*type="number"[^>]*min="0"/.test(html),
    'renderAdvanceForm: ordinal input is type="number" min="0"');
  assert(html.includes('max="9"'),
    'renderAdvanceForm: ordinal max=N-1 (items.length - 1)');
  // Confirm + Cancel buttons.
  assert(/>Confirm</.test(html),
    'renderAdvanceForm: Confirm button present');
  assert(/>Cancel</.test(html),
    'renderAdvanceForm: Cancel button present');
  // Confirm wires to the submit handler with the request id.
  assert(html.includes('window.searchPlanSubmitAdvance(42'),
    'renderAdvanceForm: Confirm button wires window.searchPlanSubmitAdvance');
  // Cancel wires to cancel handler.
  assert(html.includes('window.searchPlanCancelAdvance(42'),
    'renderAdvanceForm: Cancel button wires window.searchPlanCancelAdvance');
  // form id captured for the submit handler to read inputs back.
  assert(html.includes('class="sp-advance-form"'),
    'renderAdvanceForm: form has the sp-advance-form class');
  assert(html.includes('data-field="strategy"'),
    'renderAdvanceForm: strategy input data-field marker');
  assert(html.includes('data-field="ordinal"'),
    'renderAdvanceForm: ordinal input data-field marker');
}

// --- U5: REGENERATE_CONFIRM_MESSAGE includes "cursor" + "cycle" ------
//
// Origin R15 / AE8 mandate both substrings so the operator sees
// consequences before clicking through. The literal message is
// exported so this assertion does not depend on string matching the
// source code.
console.log('REGENERATE_CONFIRM_MESSAGE');

{
  const lower = REGENERATE_CONFIRM_MESSAGE.toLowerCase();
  assert(lower.includes('cursor'),
    'AE8: regenerate confirm message includes "cursor"');
  assert(lower.includes('cycle'),
    'AE8: regenerate confirm message includes "cycle"');
  assert(REGENERATE_CONFIRM_MESSAGE.length > 10,
    'AE8: regenerate confirm message is non-trivially long');
}

// --- U5: searchPlanRegenerate confirm gating ------------------------
//
// The action handler MUST call window.confirm with the published
// message before dispatching a fetch. We swap window.confirm + fetch
// for shims and observe both side effects.
console.log('searchPlanRegenerate()');

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
  const prevWindow = globalThis.window;
  const prevFetch = globalThis.fetch;
  const prevDocument = globalThis.document;
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
  globalThis.window = fakeWindow;
  globalThis.document = fakeDocument;
  /** @type {any} */
  globalThis.fetch = (/** @type {string} */ url, /** @type {any} */ init) => {
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
  };
  try {
    await impl(calls);
  } finally {
    state.searchPlanDetailContext = prevState;
    state.pipelineView = prevPipelineView;
    if (prevWindow === undefined) delete globalThis.window;
    else globalThis.window = prevWindow;
    if (prevDocument === undefined) delete globalThis.document;
    else globalThis.document = prevDocument;
    if (prevFetch === undefined) delete globalThis.fetch;
    else globalThis.fetch = prevFetch;
    console.error = prevConsoleError;
  }
}

// AE8: confirm returns false → no fetch.
await withFetchAndConfirmShim({ confirmReturns: false }, async (calls) => {
  await searchPlanRegenerate(2566);
  assertEqual(calls.fetch.length, 0,
    'AE8: confirm=false suppresses the regenerate fetch');
  assertEqual(calls.confirm.length, 1,
    'AE8: confirm dialog was shown once');
  assertEqual(calls.confirm[0], REGENERATE_CONFIRM_MESSAGE,
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
  assertEqual(calls.fetch.length, 1,
    'AE8: confirm=true dispatches one fetch');
  assert(calls.fetch[0].url.endsWith('/search-plan/regenerate'),
    'AE8: regenerate hits the regenerate endpoint');
  assertEqual(calls.fetch[0].init.method, 'POST',
    'AE8: regenerate uses POST');
  assertEqual(calls.fetch[0].init.body, '{}',
    'AE8: regenerate sends an empty JSON body');
  // Cache invalidated on success — refresh-after-success contract.
  assert(!searchPlanCache.has(2566),
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
  assert(!searchPlanCache.has(2566),
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
  assert(searchPlanCache.has(2566),
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
  assert(searchPlanCache.has(2566),
    '503 failure path does NOT invalidate the cache');
});

// --- U5: searchPlanAdvance error-mapping ----------------------------
console.log('searchPlanAdvance()');

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
  assertEqual(calls.fetch.length, 1,
    'advance: dispatches one fetch with a typed target');
  assert(calls.fetch[0].url.endsWith('/search-plan/advance'),
    'advance: hits the advance endpoint');
  assertEqual(calls.fetch[0].init.method, 'POST',
    'advance: uses POST');
  assertEqual(JSON.parse(calls.fetch[0].init.body).to_ordinal, 5,
    'advance: serialises toOrdinal as to_ordinal in the request body');
  assert(!searchPlanCache.has(2566),
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
  assert(searchPlanCache.has(2566),
    'AE9: invalid_target does NOT invalidate the cache');
  // The fetch was still dispatched (toast happens after the response).
  assertEqual(calls.fetch.length, 1,
    'AE9: invalid_target reports the fetch was dispatched');
  // body sent the correct shape.
  assertEqual(JSON.parse(calls.fetch[0].init.body).to_ordinal, 1,
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
  assert(searchPlanCache.has(2566),
    '409 no_active_plan: cache preserved');
  assertEqual(JSON.parse(calls.fetch[0].init.body).to_strategy, 'track',
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
  assert(searchPlanCache.has(9999),
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
  assert(searchPlanCache.has(2566),
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
  assert(searchPlanCache.has(2566),
    '400 internal: cache preserved');
  assert(calls.consoleError.length >= 1,
    '400 internal bug: console.error logged');
});

// --- U5: stubs are gone ---------------------------------------------
console.log('U5: stub-removal sanity check');

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
  assert(regenError === null,
    'searchPlanRegenerate: confirm-cancel returns without throwing (no stub)');

  let advError = null;
  await withFetchAndConfirmShim({}, async () => {
    try { await searchPlanAdvance(2566, { toOrdinal: 0 }); }
    catch (err) { advError = err; }
  });
  assert(advError === null,
    'searchPlanAdvance: real implementation does not throw "not implemented"');
}

// --- Summary ---------------------------------------------------------
console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
