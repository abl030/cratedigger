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
  state.pipelineHourlyMatchGraphOpen = false;
  state.pipelineDailyMatchGraphOpen = false;
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
  assertContains(html, "window.toggleCoverageMatchGraph('hourly')", '6h match rate toggles hourly graph');
  assertContains(html, "window.toggleCoverageMatchGraph('daily')", '24h match rate toggles daily graph');
  assertExcludes(html, 'match-rate-chart', 'chart stays collapsed by default');
}

console.log('renderWantedTrendCard() shows backlog drain and ETA');
{
  const html = __test__.renderWantedTrendCard({
    current_wanted: 10,
    series_24h: [
      {sampled_at: '2026-05-05T00:00:00+00:00', wanted_total: 14},
      {sampled_at: '2026-05-05T06:00:00+00:00', wanted_total: 10},
    ],
    windows: [
      {
        label: '6h',
        delta: -4,
        delta_per_hour: -0.6667,
        drain_per_hour: 0.6667,
        eta_hours: 15,
        trend: 'down',
      },
      {
        label: '24h',
        delta: 2,
        delta_per_hour: 0.0833,
        drain_per_hour: 0,
        eta_hours: null,
        trend: 'up',
      },
    ],
  });
  assertContains(html, 'Wanted Trend', 'card title rendered');
  assertContains(html, 'Current', 'current row rendered');
  assertContains(html, '>10</strong>', 'current wanted rendered');
  assertContains(html, 'down 0.67/hr (-4)', 'drain rate rendered');
  assertContains(html, 'up 0.08/hr (+2)', 'growth rate rendered');
  assertContains(html, '15.0h', 'ETA rendered');
  assertContains(html, 'wanted-trend-line', 'sparkline rendered');
}

console.log('renderCoverageCard() expands an hourly match-rate chart under the 6h row');
{
  state.pipelineMatchGraphOpen = false;
  state.pipelineHourlyMatchGraphOpen = true;
  state.pipelineDailyMatchGraphOpen = false;
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
  state.pipelineHourlyMatchGraphOpen = false;
}

console.log('renderCoverageCard() expands a daily match-rate chart under the 24h row');
{
  state.pipelineMatchGraphOpen = false;
  state.pipelineHourlyMatchGraphOpen = false;
  state.pipelineDailyMatchGraphOpen = true;
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
    match_rate_series_28d: [
      {bucket_start: '2026-05-04T00:00:00+00:00', matches: 2, matches_per_day: 2},
      {bucket_start: '2026-05-05T00:00:00+00:00', matches: 8, matches_per_day: 8},
    ],
    top_10_share_24h: 0.25,
  });
  assertContains(html, 'Last 28 days', 'daily chart label rendered');
  assertContains(html, 'peak 8/day', 'daily chart peak rendered');
  assertContains(html, 'match-rate-bar active', 'daily nonzero bars are highlighted');
  state.pipelineDailyMatchGraphOpen = false;
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

console.log('renderPeerBrowseHeavyQueries() shows release ids and exact query tokens');
{
  const html = __test__.renderPeerBrowseHeavyQueries({
    heavy_query_hours: 24,
    heavy_queries: [
      {
        search_log_id: 88,
        request_id: 1843,
        mb_release_id: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
        artist_name: 'The Wiggles',
        album_title: 'The Wiggles',
        created_at: '2026-05-06T00:00:00+00:00',
        query: '*he *iggles 1991',
        variant: 'unwild_year',
        result_count: 1000,
        peer_dirs: 32355,
        fanout_waves: 422,
        browse_time_s: 3868,
      },
    ],
  });
  assertContains(html, 'Peer/Dir Heavy Queries (24h)', 'card title includes window');
  assertContains(html, '#1843', 'request id rendered');
  assertContains(html, 'aaaaaaaa', 'release id prefix rendered');
  assertContains(html, '*he *iggles 1991', 'exact query rendered');
  assertContains(html, '32,355', 'peer/dir count rendered');
  assertContains(html, '64m 28s', 'browse duration rendered');
}

console.log('renderPeersCard() shows totals strip and cumulative day table');
{
  const html = __test__.renderPeersCard({
    totals: {
      known_peers: 40746,
      new_24h: 312,
      seen_24h: 1894,
      tracked_since: '2026-05-08T13:50:47+00:00',
    },
    days: [
      { date: '2026-06-12', new_peers: 312, total_peers: 40746 },
      { date: '2026-06-11', new_peers: 0, total_peers: 40434 },
    ],
  });
  assertContains(html, 'Known Peers', 'card title rendered');
  assertContains(html, '40,746', 'known peer total rendered');
  assertContains(html, 'Seen 24h', 'seen-24h metric rendered');
  assertContains(html, '2026-06-11', 'zero-day row rendered');
  assertContains(html, '40,434', 'carried-forward cumulative total rendered');
}

console.log('renderPeersCard() with no observations renders the empty row');
{
  const html = __test__.renderPeersCard({ totals: {}, days: [] });
  assertContains(html, 'No peer observations yet', 'empty state rendered');
}

console.log('renderPipelineListBody() flags the imported recency window');
{
  state.pipelineFilter = 'imported';
  state.pipelineSearchResults = null;
  state.pipelineData = {
    counts: { imported: 6828 },
    imported: [],
    imported_total: 6828,
    imported_truncated: true,
  };
  const html = __test__.renderPipelineListBody();
  assertContains(html, 'most recent of 6828 imported',
    'truncation note names the full imported count');
  assertContains(html, 'search above', 'note points at the search box');
}

console.log('renderPipelineListBody() renders server search results across statuses');
{
  state.pipelineFilter = 'wanted';
  state.pipelineData = { counts: {}, wanted: [], imported: [] };
  state.pipelineSearchResults = [
    {
      id: 7, artist_name: 'The Mountain Goats', album_title: 'Tallahassee',
      status: 'imported', source: 'request', year: 2002,
      created_at: '2026-06-01T00:00:00+00:00',
      updated_at: '2026-06-02T00:00:00+00:00',
      mb_release_id: 'mbid-7', mb_release_group_id: 'rg-7',
    },
  ];
  const html = __test__.renderPipelineListBody();
  assertContains(html, 'The Mountain Goats', 'search result row rendered');
  assertExcludes(html, 'most recent of', 'no truncation note while searching');

  state.pipelineSearchResults = [];
  const emptyHtml = __test__.renderPipelineListBody();
  assertContains(emptyHtml, 'No matches', 'empty search shows No matches');
  state.pipelineSearchResults = null;
}

console.log('clearPipelineSearch() makes filters and search mutually exclusive');
{
  state.pipelineFilter = 'imported';
  state.pipelineSearchQuery = 'mountain';
  state.pipelineSearchResults = [{ id: 1 }];
  __test__.clearPipelineSearch();
  if (state.pipelineSearchQuery === '' && state.pipelineSearchResults === null) {
    passed++;
  } else {
    failed++;
    console.error('  FAIL: clearPipelineSearch did not reset search state');
  }
  const html = __test__.renderPipelineListBody();
  assertExcludes(html, 'No matches', 'list body is back in filter mode after clear');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
