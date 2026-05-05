/**
 * Unit tests for web/js/pipeline.js navigation helpers.
 * Run with: node tests/test_js_pipeline.mjs
 */

import { __test__ } from '../web/js/pipeline.js';
import { state } from '../web/js/state.js';

let passed = 0;
let failed = 0;

function assertContains(haystack, needle, msg) {
  if (haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} - '${needle}' not in output`);
  }
}

function assertExcludes(haystack, needle, msg) {
  if (!haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} - unexpectedly found '${needle}'`);
  }
}

console.log('renderPipelineNav() refreshes the queue subtab');
{
  state.pipelineView = 'queue';
  const html = __test__.renderPipelineNav();
  assertContains(html, 'window.setPipelineView(\'queue\')', 'queue tab rendered');
  assertContains(html, 'window.setPipelineView(\'dashboard\')', 'dashboard tab rendered');
  assertContains(html, 'window.loadPipeline()', 'queue refresh reloads pipeline queue');
  assertContains(html, 'subtab-refresh', 'refresh uses shared subtab layout');
  assertExcludes(html, 'window.loadPipelineDashboard()">Refresh', 'queue refresh does not load dashboard');
}

console.log('renderPipelineNav() refreshes the dashboard subtab');
{
  state.pipelineView = 'dashboard';
  const html = __test__.renderPipelineNav();
  assertContains(html, 'window.loadPipelineDashboard()', 'dashboard refresh reloads dashboard metrics');
  assertContains(html, 'subtab-refresh', 'refresh uses shared subtab layout');
}

console.log('renderCoverageCard() shows found-enqueue match rates');
{
  state.pipelineMatchGraphOpen = false;
  const html = __test__.renderCoverageCard({
    wanted_total: 10,
    wanted_searched_24h: 8,
    wanted_searched_6h: 5,
    wanted_unsearched_24h: 2,
    wanted_never_searched: 1,
    matches_24h: 24,
    matches_6h: 9,
    matches_per_hour_24h: 1,
    matches_per_hour_6h: 1.5,
    top_10_share_24h: 0.25,
  });
  assertContains(html, 'Match/hr 6h', '6h match-rate label rendered');
  assertContains(html, '>1.50</strong>', '6h match rate rendered');
  assertContains(html, 'Match/hr 24h', '24h match-rate label rendered');
  assertContains(html, '>1.00</strong>', '24h match rate rendered');
  assertContains(html, 'window.toggleCoverageMatchGraph()', '24h match rate toggles graph');
  assertExcludes(html, 'match-rate-chart', 'chart stays collapsed by default');
}

console.log('renderCoverageCard() expands a 24h match-rate chart');
{
  state.pipelineMatchGraphOpen = true;
  const html = __test__.renderCoverageCard({
    wanted_total: 10,
    wanted_searched_24h: 8,
    wanted_searched_6h: 5,
    wanted_unsearched_24h: 2,
    wanted_never_searched: 1,
    matches_24h: 3,
    matches_6h: 1,
    matches_per_hour_24h: 0.125,
    matches_per_hour_6h: 0.1666666667,
    match_rate_series_24h: [
      {bucket_start: '2026-05-05T00:00:00+00:00', matches: 0, matches_per_hour: 0},
      {bucket_start: '2026-05-05T01:00:00+00:00', matches: 3, matches_per_hour: 3},
    ],
    top_10_share_24h: 0.25,
  });
  assertContains(html, 'metric-open', 'clicked row shows open state');
  assertContains(html, 'match-rate-chart', 'chart container rendered');
  assertContains(html, '<svg', 'chart svg rendered');
  assertContains(html, 'peak 3.00/hr', 'chart peak rendered');
  assertContains(html, 'match-rate-bar active', 'nonzero bars are highlighted');
  state.pipelineMatchGraphOpen = false;
}

console.log('withCoverageMatchRates() falls back to search window found counts');
{
  const coverage = __test__.withCoverageMatchRates({
    wanted_total: 10,
    wanted_searched_24h: 8,
  }, [
    {label: '24h', hours: 24, outcomes: {found: 132}},
    {label: '6h', hours: 6, outcomes: {found: 27}},
  ]);
  if (coverage.matches_24h === 132
      && coverage.matches_6h === 27
      && coverage.matches_per_hour_24h === 5.5
      && coverage.matches_per_hour_6h === 4.5) {
    passed++;
  } else {
    failed++;
    console.error('  FAIL: coverage fallback did not derive expected match rates');
  }
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
