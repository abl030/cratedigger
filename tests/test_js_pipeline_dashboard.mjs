/**
 * Unit tests for web/js/pipeline_dashboard.js cards + charts (#434).
 * Run with: node tests/test_js_pipeline_dashboard.mjs
 */

import { __test__ } from '../web/js/pipeline_dashboard.js';
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

console.log('renderUnfindableCard() with no runs yet renders the honest empty state (#1112)');
{
  const html = __test__.renderUnfindableCard({ recent_runs: [], backlog_trend: {} });
  assertContains(html, 'Unfindable Detection', 'card title rendered');
  assertContains(html, 'No unfindable-detection runs yet', 'empty table row rendered');
  assertContains(html, '>never</strong>', 'last-run falls back to never');
  assertContains(html, 'Collecting run history', 'chart empty state rendered');
  assertExcludes(html, '<polyline', 'no chart line drawn with < 2 samples');
}
console.log('renderUnfindableCard() shows latest-run facts, outcome breakdown, and breaker state');
{
  const html = __test__.renderUnfindableCard({
    recent_runs: [
      {
        created_at: '2026-08-12T00:10:00+00:00',
        cohort_total: 1301,
        due_backlog_at_start: 686,
        batch_limit: 240,
        // 84 candidates processed, 3 of them RESULT_REQUEST_NOT_FOUND
        // (never fired a probe) -- 81 real probes (F7, #1112).
        candidates_processed: 84,
        probes_attempted: 81,
        categorised_count: 3,
        downgraded_count: 1,
        no_change_count: 0,
        probe_failed_count: 77,
        not_due_count: 0,
        request_not_found_count: 3,
        breaker_tripped: true,
      },
      {
        created_at: '2026-08-11T00:10:00+00:00',
        cohort_total: 1301,
        due_backlog_at_start: 900,
        batch_limit: 240,
        candidates_processed: 240,
        probes_attempted: 240,
        categorised_count: 5,
        downgraded_count: 1,
        no_change_count: 210,
        probe_failed_count: 24,
        not_due_count: 0,
        request_not_found_count: 0,
        breaker_tripped: false,
      },
    ],
    backlog_trend: {
      current_backlog: 686,
      series: [
        {sampled_at: '2026-08-11T00:10:00+00:00', due_backlog_at_start: 900, candidates_processed: 240},
        {sampled_at: '2026-08-12T00:10:00+00:00', due_backlog_at_start: 686, candidates_processed: 84},
      ],
    },
  });
  assertContains(html, '1,301', 'cohort total rendered');
  assertContains(html, '686', 'due backlog rendered');
  assertContains(html, '240 / 84', 'batch limit / processed rendered for the latest run');
  assertContains(html, '>81</strong>', 'probes-attempted (real probes, excluding request_not_found) rendered for the latest run');
  assertContains(html, 'metric-bad">yes', 'breaker-tripped latest run flagged bad');
  assertContains(html, '<polyline', 'backlog trend line rendered with >= 2 samples');
  assertContains(html, 'Due backlog per run', 'chart head label rendered');
  assertContains(html, '77', 'probe-failed count for the tripped run rendered');
}
console.log('normalizeUnfindableBacklogSeries() maps due_backlog_at_start to a plottable series');
{
  const series = __test__.normalizeUnfindableBacklogSeries([
    {sampled_at: '2026-08-11T00:00:00+00:00', due_backlog_at_start: 900},
    {sampled_at: '2026-08-12T00:00:00+00:00', due_backlog_at_start: 686},
  ]);
  if (series.length === 2
      && series[0].time === '2026-08-11T00:00:00+00:00'
      && series[0].backlog === 900
      && series[1].backlog === 686) {
    passed++;
  } else {
    failed++;
    console.error('  FAIL: normalizeUnfindableBacklogSeries did not derive expected points', series);
  }
}

console.log('renderDriftRow() renders the operator merge-rekey button (#1089)');
{
  const html = __test__.renderDriftRow({
    id: 8792, artist_name: 'Slipknot', album_title: 'Vol. 3: (The Subliminal Verses)',
    status: 'imported',
  });
  assertContains(html, '#8792 Slipknot', 'row names the request id and artist');
  assertContains(html, 'Vol. 3: (The Subliminal Verses)', 'row names the album title');
  assertContains(html, 'metric-bad">imported', 'row shows the drift status');
  assertContains(html, 'window.mergeRekeyRequest(8792, this)', 'button wires the window binding with its request id');
  assertContains(html, 'Follow MB merge', 'button label rendered');
  assertContains(html, 'id="drift-note-8792"', 'inline refusal-note slot rendered for this request');
}
console.log('renderDriftRow() escapes artist/album HTML');
{
  const html = __test__.renderDriftRow({
    id: 1, artist_name: '<script>x</script>', album_title: 'A & B', status: 'imported',
  });
  assertExcludes(html, '<script>x</script>', 'artist name is escaped');
  assertContains(html, 'A &amp; B', 'album title is escaped');
}
console.log('renderDiskCoverageCard() composes one drift row per off-disk request');
{
  const html = __test__.renderDiskCoverageCard({
    counts: {on_disk_total: 9, active_total: 11, off_disk_by_status: {wanted: 1}},
    drift_rows: [
      {id: 316, artist_name: 'Rebecca Black', album_title: 'Sing It', status: 'imported'},
      {id: 8832, artist_name: 'Kim Petras', album_title: 'Detour', status: 'imported'},
    ],
  });
  assertContains(html, 'Drift (imported, missing from beets)', 'drift metric label rendered');
  assertContains(html, 'window.mergeRekeyRequest(316, this)', 'first drift row gets its own button');
  assertContains(html, 'window.mergeRekeyRequest(8832, this)', 'second drift row gets its own button');
}
console.log('renderDiskCoverageCard() renders no drift rows or buttons when nothing has drifted');
{
  const html = __test__.renderDiskCoverageCard({
    counts: {on_disk_total: 11, active_total: 11, off_disk_by_status: {}},
    drift_rows: [],
  });
  assertContains(html, 'metric-good">0', 'zero drift renders the good class');
  assertExcludes(html, 'mergeRekeyRequest', 'no button rendered with an empty drift list');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
