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

function assert(condition, msg) {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg}`);
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

console.log('renderDriftRow() renders the operator merge-rekey button for MB-sourced rows (#1089)');
{
  const html = __test__.renderDriftRow({
    id: 8792, artist_name: 'Slipknot', album_title: 'Vol. 3: (The Subliminal Verses)',
    status: 'imported', mb_release_id: 'd990b8af-0000-0000-0000-000000000000',
    discogs_release_id: null, source: 'musicbrainz',
    resolution: {kind: 'missing'},
  });
  assertContains(html, '#8792 Slipknot', 'row names the request id and artist');
  assertContains(html, 'Vol. 3: (The Subliminal Verses)', 'row names the album title');
  assertContains(html, 'metric-bad">missing', 'row truthfully names the missing resolution');
  assertContains(html, 'window.mergeRekeyRequest(8792, this)', 'button wires the window binding with its request id');
  assertContains(html, 'Follow MB merge', 'button label rendered');
  assertContains(html, 'id="drift-note-8792"', 'inline refusal-note slot rendered for this request');
}
console.log('renderDriftRow() withholds the button for non-MB-sourced rows (#1089 MAJOR-A, review round 3)');
{
  // The REAL production shape (#1089 MAJOR-A): a Discogs-sourced row
  // duplicates the numeric id into BOTH mb_release_id and
  // discogs_release_id (ReleaseIdentity.from_strict_fields's own
  // docstring) — mb_release_id truthiness alone would falsely gate the
  // button on. The server-derived `source` field is the real gate.
  const html = __test__.renderDriftRow({
    id: 1870, artist_name: 'Some Artist', album_title: 'Some Album',
    status: 'imported', mb_release_id: '1870', discogs_release_id: '1870',
    source: 'discogs', resolution: {kind: 'missing'},
  });
  assertContains(html, '#1870 Some Artist', 'row still names the request');
  assertContains(html, 'metric-bad">missing', 'row still shows its resolution');
  assertExcludes(html, 'window.mergeRekeyRequest', 'no merge-rekey button for a non-MB-sourced row');
  assertExcludes(html, 'Follow MB merge', 'no button label for a non-MB-sourced row');
  assertExcludes(html, 'drift-note-1870', 'no inline note slot without a button to write into');
}
console.log('renderDriftRow() escapes artist/album HTML');
{
  const html = __test__.renderDriftRow({
    id: 1, artist_name: '<script>x</script>', album_title: 'A & B', status: 'imported',
    mb_release_id: 'd990b8af-0000-0000-0000-000000000000', source: 'musicbrainz',
    resolution: {kind: 'missing'},
  });
  assertExcludes(html, '<script>x</script>', 'artist name is escaped');
  assertContains(html, 'A &amp; B', 'album title is escaped');
}
console.log('renderDriftRow() distinguishes ambiguity from missing by exact album cardinality');
{
  const html = __test__.renderDriftRow({
    id: 2, artist_name: 'Ambiguous Artist', album_title: 'Two Albums',
    status: 'imported', source: 'musicbrainz',
    resolution: {kind: 'ambiguous', album_ids: [7, 9], reason: 'multiple_matches'},
  });
  assertContains(html, 'ambiguous (2 albums)', 'ambiguity includes exact album cardinality');
  assertExcludes(html, 'metric-bad">missing', 'ambiguity never masquerades as missing');
}
console.log('renderDiskCoverageCard() composes one drift row per off-disk request');
{
  const html = __test__.renderDiskCoverageCard({
    counts: {on_disk_total: 9, active_total: 11, off_disk_by_status: {wanted: 1}},
    drift_rows: [
      {id: 316, artist_name: 'Rebecca Black', album_title: 'Sing It', status: 'imported',
       mb_release_id: 'd990b8af-0000-0000-0000-000000000001', source: 'musicbrainz',
       resolution: {kind: 'missing'}},
      {id: 8832, artist_name: 'Kim Petras', album_title: 'Detour', status: 'imported',
       mb_release_id: 'd990b8af-0000-0000-0000-000000000002', source: 'musicbrainz',
       resolution: {kind: 'ambiguous', album_ids: [8, 9, 10], reason: 'multiple_matches'}},
    ],
  });
  assertContains(html, 'Imported, not uniquely in Beets', 'neutral drift metric label rendered');
  assertContains(html, 'missing', 'missing resolution rendered');
  assertContains(html, 'ambiguous (3 albums)', 'ambiguous resolution rendered');
  assertContains(html, 'window.mergeRekeyRequest(316, this)', 'first drift row gets its own button');
  assertContains(html, 'window.mergeRekeyRequest(8832, this)', 'second drift row gets its own button');
}
console.log('renderDiskCoverageCard() keeps wanted coverage neutral when a wanted row is ambiguous');
{
  const html = __test__.renderDiskCoverageCard({
    counts: {on_disk_total: 9, active_total: 10, off_disk_by_status: {wanted: 1}},
    drift_rows: [],
  });
  assertContains(html, 'Wanted, not uniquely in Beets', 'wanted count does not claim missing membership');
  assertExcludes(html, 'Wanted (not yet acquired)', 'stale wanted-missing claim removed');
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

console.log('renderRetagDivergenceCensusCard() honestly shows the missing state');
{
  const html = __test__.renderRetagDivergenceCensusCard({
    state: 'missing', error: null, snapshot: null,
  });
  assertContains(html, 'Beets DB', 'card title mentions Beets DB');
  assertContains(html, 'File Tags', 'card title mentions file tags — distinct from Disk Coverage');
  assertContains(html, 'no census published yet', 'honest missing-state copy');
  assertExcludes(html, 'Disk Coverage', 'never confused with the ledger-vs-beets Disk Coverage card');
}
console.log('renderRetagDivergenceCensusCard() shows the unreadable state without crashing');
{
  const html = __test__.renderRetagDivergenceCensusCard({
    state: 'unreadable', error: 'DecodeError: bad json', snapshot: null,
  });
  assertContains(html, 'unreadable', 'unreadable state copy rendered');
  assertContains(html, 'DecodeError: bad json', 'error detail rendered');
}
console.log('renderRetagDivergenceCensusCard() renders a clean snapshot with zero listed albums');
{
  const html = __test__.renderRetagDivergenceCensusCard({
    state: 'ok', error: null,
    snapshot: {
      generated_at: '2026-08-14T09:00:00+00:00',
      duration_seconds: 196.4,
      report: {
        status: 'clean', complete: true,
        counts: {albums_scanned: 93700, items_scanned: 93700},
        albums: [],
      },
    },
  });
  assertContains(html, 'metric-good">clean', 'clean status rendered with good class');
  assertContains(html, '93,700', 'albums_scanned count rendered');
  assertExcludes(html, 'window.recheckRetagDivergenceAlbum', 'no recheck buttons with nothing listed');
}
console.log('retagDivergenceSnapshotAgeHours() computes hours since generated_at');
{
  const nowMs = Date.parse('2026-08-16T00:00:00+00:00');
  assert(
    __test__.retagDivergenceSnapshotAgeHours('2026-08-15T00:00:00+00:00', nowMs) === 24,
    '24h old is 24',
  );
  assert(
    __test__.retagDivergenceSnapshotAgeHours(null, nowMs) === null,
    'missing generated_at is null, not NaN',
  );
  assert(
    __test__.retagDivergenceSnapshotAgeHours('not a date', nowMs) === null,
    'unparsable generated_at is null',
  );
}
console.log('N5 (#1142 review) — stale boundary is exactly 36h; 36.0h is fresh, 36.01h is stale');
{
  const nowMs = Date.parse('2026-08-16T00:00:00+00:00');
  const atBoundary = new Date(nowMs - 36 * 3600000).toISOString();
  const justPastBoundary = new Date(nowMs - 36.01 * 3600000).toISOString();
  assert(
    __test__.retagDivergenceSnapshotIsStale(atBoundary, nowMs) === false,
    'exactly 36h old is NOT stale (boundary is exclusive)',
  );
  assert(
    __test__.retagDivergenceSnapshotIsStale(justPastBoundary, nowMs) === true,
    'just past 36h old IS stale',
  );
}
console.log('renderRetagDivergenceCensusCard() N5 — a fresh snapshot never reads stale');
{
  const fresh = new Date(Date.now() - 2 * 3600000).toISOString();
  const html = __test__.renderRetagDivergenceCensusCard({
    state: 'ok', error: null,
    snapshot: {
      generated_at: fresh,
      duration_seconds: 196.4,
      report: {
        status: 'clean', complete: true,
        counts: {albums_scanned: 93700},
        albums: [],
      },
    },
  });
  assertExcludes(html, 'stale', 'a 2h-old snapshot never renders any "stale" text');
  assertContains(html, 'metric-good">fresh', 'freshness row reads fresh');
}
console.log('renderRetagDivergenceCensusCard() N5 — a snapshot older than 36h reads stale with a warn tone');
{
  const stale = new Date(Date.now() - 40 * 3600000).toISOString();
  const html = __test__.renderRetagDivergenceCensusCard({
    state: 'ok', error: null,
    snapshot: {
      generated_at: stale,
      duration_seconds: 196.4,
      report: {
        status: 'clean', complete: true,
        counts: {albums_scanned: 93700},
        albums: [],
      },
    },
  });
  assertContains(html, 'metric-warn">stale', 'freshness row reads stale with warn tone');
  assertContains(html, '40.0h', 'stale label names the exact age');
}
console.log('renderLibraryCompletenessCard() groups findings into collapsed, honestly capped categories');
{
  const stale = new Date(Date.now() - 40 * 3600000).toISOString();
  const html = __test__.renderLibraryCompletenessCard({
    state: 'ok', albums_shown: 1, albums_listed_total: 2,
    snapshot: {
      generated_at: stale, duration_seconds: 2.5,
      report: {
        status: 'incomplete',
        counts: {albums_scanned: 8498, audio_complete: 8496, missing_source_audio: 2, catalog_drift: 2, unknown: 0},
        albums: [{album_id: 11782, artist: 'David Bowie', title: 'David Bowie', findings: [
          {kind: 'missing_source_audio', detail: "Don't Sit Down"},
          {kind: 'catalog_drift', detail: 'uncatalogued=1 catalogued_missing=0'},
        ]}],
      },
    },
  });
  assertContains(html, 'incomplete (stale)', 'stale completeness status is explicit');
  assertContains(html, '8,496 / 8,498', 'audio complete uses whole-library denominator');
  assertContains(html, 'Don&#39;t Sit Down', 'missing-source detail is rendered and escaped');
  assertContains(html, 'uncatalogued=1', 'catalog-drift detail is rendered');
  assertContains(html, '<summary><span>Missing source audio</span><strong class="metric-bad">2</strong></summary>', 'missing category is expandable');
  assertContains(html, '<summary><span>Catalog drift</span><strong class="metric-warn">2</strong></summary>', 'drift category is expandable');
  assertContains(html, '<summary><span>Unknown</span><strong class="metric-muted">0</strong></summary>', 'unknown category is expandable even when empty');
  assertExcludes(html, '<details open', 'all finding categories start collapsed');
  assert((html.match(/#11782/g) || []).length === 2, 'an album with two findings appears in both relevant categories');
  assert((html.match(/Showing 1 of 2/g) || []).length === 2, 'each capped category names its own shown and total counts');
  assertExcludes(html, 'Exceptional albums', 'the redundant flat exceptional total is gone');
  assertExcludes(html, 'Non-audio omitted', 'non-audio omissions are not a dashboard category');
  assertExcludes(html, 'non_audio_omitted', 'the removed raw category is not rendered');
  assertExcludes(html, '1 / 2', 'uncapped-style fractions are not shown');
}
console.log('renderLibraryCompletenessCard() keeps zero defect observations neutral');
{
  const html = __test__.renderLibraryCompletenessCard({
    state: 'ok', snapshot: {generated_at: new Date().toISOString(), duration_seconds: 1,
      report: {status: 'complete', counts: {albums_scanned: 1, audio_complete: 1, missing_source_audio: 0, catalog_drift: 0, unknown: 0}, albums: []}},
  });
  assertContains(html, 'metric-muted">0</strong>', 'zero defect counts are neutral, not celebratory');
  assertExcludes(html, 'metric-good">0</strong>', 'zero defect counts never use good tone');
}
console.log('renderRetagDivergenceCensusCard() lists a divergent album with a recheck button');
{
  const html = __test__.renderRetagDivergenceCensusCard({
    state: 'ok', error: null,
    snapshot: {
      generated_at: '2026-08-14T09:00:00+00:00',
      duration_seconds: 196.4,
      report: {
        status: 'divergence_found', complete: true,
        counts: {albums_scanned: 93700, items_scanned: 93700},
        albums: [{
          album_id: 6612, db_mb_albumid: 'd990b8af-0000-0000-0000-000000000000',
          album_class: 'diverges', item_count: 8, items: [],
        }],
      },
    },
  });
  assertContains(html, 'metric-bad">divergence_found', 'divergence status rendered with bad class');
  assertContains(html, 'id="retag-album-6612"', 'per-album container id rendered for in-place patching');
  assertContains(html, 'window.recheckRetagDivergenceAlbum(6612, this)', 'recheck button wired with the album id');
  assertContains(html, 'id="retag-album-note-6612"', 'inline note slot rendered for this album');
}
console.log('renderRetagDivergenceAlbumRowInner() #1260 — names first, raw MBIDs demoted, Write-tags wired');
{
  const html = __test__.renderRetagDivergenceAlbumRowInner({
    album_id: 16948,
    db_mb_albumid: '26693e58-02c0-4bb1-b66f-f0f44f8a234d',
    albumartist: 'Terre Thaemlitz / DJ Sprinkles',
    album: 'RA.1000',
    album_class: 'diverges', item_count: 1,
    items: [{
      item_class: 'diverges',
      path: '/library/Terre Thaemlitz/2025 - RA.1000/01 RA.1000.opus',
      file_mb_albumid: 'fdc54a6a-27c7-4936-87d7-7ab146812d4e',
      detail: null,
    }],
  });
  assertContains(html, 'Terre Thaemlitz / DJ Sprinkles — RA.1000', 'human name rendered on the album row');
  assertExcludes(html, '>26693e58-02c0-4bb1-b66f-f0f44f8a234d<', 'full album MBID never rendered as text');
  assertContains(html, 'title="26693e58-02c0-4bb1-b66f-f0f44f8a234d"', 'full album MBID one hover away');
  assertContains(html, '>26693e58…<', 'short album MBID code rendered');
  assertContains(html, '01 RA.1000.opus', 'item row names the file, not a bare UUID');
  assertContains(html, 'title="/library/Terre Thaemlitz/2025 - RA.1000/01 RA.1000.opus"', 'full item path one hover away');
  assertContains(html, 'title="fdc54a6a-27c7-4936-87d7-7ab146812d4e"', 'full file-tag MBID one hover away');
  assertContains(html, 'window.syncRetagDivergenceAlbum(16948, this)', 'Write-tags button wired with the album id');
  assertContains(html, 'data-expected="26693e58-02c0-4bb1-b66f-f0f44f8a234d"', 'Write-tags carries the compare-and-set identity');
}
console.log('renderRetagDivergenceAlbumRowInner() #1260 — no Write-tags button without a divergent item');
{
  const html = __test__.renderRetagDivergenceAlbumRowInner({
    album_id: 7,
    db_mb_albumid: '26693e58-02c0-4bb1-b66f-f0f44f8a234d',
    album_class: 'unreadable', item_count: 1,
    items: [{
      item_class: 'unreadable', path: '/library/x/01.mp3',
      file_mb_albumid: null, detail: 'OSError: EIO',
    }],
  });
  assertExcludes(html, 'window.syncRetagDivergenceAlbum', 'unreadable-only album gets no Write-tags button');
  assertContains(html, 'window.recheckRetagDivergenceAlbum(7, this)', 'recheck still offered');
}
console.log('renderRetagDivergenceAlbumRowInner() #1260 — a pre-#1260 snapshot without names still renders');
{
  const html = __test__.renderRetagDivergenceAlbumRowInner({
    album_id: 9,
    db_mb_albumid: '26693e58-02c0-4bb1-b66f-f0f44f8a234d',
    album_class: 'diverges', item_count: 1,
    items: [{
      item_class: 'diverges', path: null,
      file_mb_albumid: 'fdc54a6a-27c7-4936-87d7-7ab146812d4e', detail: null,
    }],
  });
  assertContains(html, 'Album #9 <code', 'no stray separator when names are absent');
  assertContains(html, '(unknown file)', 'missing item path degrades honestly');
}
console.log('renderRetagDivergenceCensusCard() escapes the db_mb_albumid value');
{
  const html = __test__.renderRetagDivergenceCensusCard({
    state: 'ok', error: null,
    snapshot: {
      generated_at: '2026-08-14T09:00:00+00:00',
      duration_seconds: 1.0,
      report: {
        status: 'divergence_found', complete: true,
        counts: {},
        albums: [{
          album_id: 1, db_mb_albumid: '<script>x</script>',
          album_class: 'diverges', item_count: 1, items: [],
        }],
      },
    },
  });
  assertExcludes(html, '<script>x</script>', 'db_mb_albumid is escaped');
}
console.log('renderRetagDivergenceCensusCard() N1 (fresh review) — shows "Showing N of M" when the dashboard route capped the album list');
{
  const html = __test__.renderRetagDivergenceCensusCard({
    state: 'ok', error: null,
    albums_shown: 50, albums_listed_total: 57,
    snapshot: {
      generated_at: '2026-08-14T09:00:00+00:00',
      duration_seconds: 1.0,
      report: {
        status: 'divergence_found', complete: true,
        counts: {albums_scanned: 8487},
        albums: Array.from({length: 50}, (_, i) => ({
          album_id: i + 1, db_mb_albumid: `mb-${i + 1}`,
          album_class: 'diverges', item_count: 0, items: [],
        })),
      },
    },
  });
  assertContains(html, 'Showing 50 of 57', 'capped state visibly names shown vs. total — no silent cap');
  assertContains(html, 'Listed (non-agreeing)', 'listed row label still present');
  assertContains(html, '>57<', 'listed row shows the TRUE total, not the capped shown count');
}
console.log('renderRetagDivergenceCensusCard() N1 (fresh review) — no "Showing" text when nothing was capped');
{
  const html = __test__.renderRetagDivergenceCensusCard({
    state: 'ok', error: null,
    albums_shown: 1, albums_listed_total: 1,
    snapshot: {
      generated_at: '2026-08-14T09:00:00+00:00',
      duration_seconds: 1.0,
      report: {
        status: 'divergence_found', complete: true,
        counts: {albums_scanned: 8487},
        albums: [{
          album_id: 6612, db_mb_albumid: 'd990b8af-0000-0000-0000-000000000000',
          album_class: 'diverges', item_count: 1, items: [],
        }],
      },
    },
  });
  assertExcludes(html, 'Showing', 'uncapped state never mentions "Showing" at all');
}
console.log('renderRetagDivergenceAlbumRowInner() N2 (fresh review) — shows each non-agreeing item\'s class + identity + detail');
{
  const html = __test__.renderRetagDivergenceAlbumRowInner({
    album_id: 6612, db_mb_albumid: 'd990b8af-0000-0000-0000-000000000000',
    album_class: 'diverges', item_count: 3,
    items: [
      {
        path: '/library/Slipknot/01.flac', item_class: 'diverges',
        file_mb_albumid: 'a6269e96-0000-0000-0000-000000000000', detail: null,
      },
      {
        path: '/library/Slipknot/02.flac', item_class: 'unreadable',
        file_mb_albumid: null, detail: 'OSError: permission denied',
      },
      {
        path: '/library/Slipknot/03.flac', item_class: 'agrees',
        file_mb_albumid: 'd990b8af-0000-0000-0000-000000000000', detail: null,
      },
    ],
  });
  assertContains(html, 'Items', 'item count label rendered');
  assertContains(html, '>3<', 'total item count rendered');
  assertContains(html, 'diverges', 'first non-agreeing item class rendered');
  assertContains(html, 'a6269e96-0000-0000-0000-000000000000', 'first item identity rendered');
  assertContains(html, 'unreadable', 'second non-agreeing item class rendered');
  assertContains(html, '(none)', 'a null file_mb_albumid renders as (none)');
  assertContains(html, 'OSError: permission denied', 'unreadable item detail rendered');
  assertExcludes(html, '/library/Slipknot/03.flac', 'agreeing item path never rendered — only non-agreeing items are itemized');
  // #1260 revised the #1142 N2 stance on operator request: the file NAME
  // is now the row's readable subject, and the FULL path appears only as
  // a hover title attribute — never as flowing row text.
  assertContains(html, 'title="/library/Slipknot/01.flac"', 'full non-agreeing item path is one hover away');
  assertExcludes(html, '>diverges: /library/Slipknot/01.flac', 'full path never rendered as row text');
  assertContains(html, '01.flac', 'the file name is the readable row subject');
}
console.log('renderRetagDivergenceAlbumRowInner() N2 (fresh review) — escapes XSS-looking item identity/detail');
{
  const html = __test__.renderRetagDivergenceAlbumRowInner({
    album_id: 1, db_mb_albumid: 'cafef00d',
    album_class: 'diverges', item_count: 1,
    items: [{
      path: '/library/x/01.flac', item_class: 'diverges',
      file_mb_albumid: '<script>alert(1)</script>',
      detail: '<img src=x onerror=alert(2)>',
    }],
  });
  assertExcludes(html, '<script>alert(1)</script>', 'file_mb_albumid XSS is escaped');
  assertExcludes(html, '<img src=x onerror=alert(2)>', 'detail XSS is escaped');
  assertContains(html, '&lt;script&gt;', 'file_mb_albumid renders as escaped entities');
}
console.log('renderRetagDivergenceCensusCard() N1 — an incomplete report with nothing listed never reads green');
{
  // A world the daily writer itself cannot currently produce (incomplete
  // always implies something listed for an unbounded, cursor-less scan —
  // see lib/retag_divergence_audit.py), but the frontend must stay
  // defensively correct on its own terms rather than trusting that
  // backend invariant to hold forever.
  const html = __test__.renderRetagDivergenceCensusCard({
    state: 'ok', error: null,
    snapshot: {
      generated_at: '2026-08-14T09:00:00+00:00',
      duration_seconds: 5.0,
      report: {
        status: 'incomplete', complete: true,
        counts: {albums_scanned: 0},
        albums: [],
      },
    },
  });
  assertContains(html, 'metric-warn">incomplete', 'incomplete status rendered with warn class');
  // The Listed row's own tone must not be metric-good when status isn't
  // clean, regardless of the zero count.
  const listedRowMatch = html.match(/Listed \(non-agreeing\)<\/span><strong class="([^"]*)">0/);
  assert(listedRowMatch !== null, 'Listed row with a 0 count is present');
  assert(listedRowMatch[1] !== 'metric-good',
    `Listed row must not be metric-good for status=incomplete, got ${listedRowMatch[1]}`);
  const scannedRowMatch = html.match(/Albums scanned<\/span><strong class="([^"]*)">0/);
  assert(scannedRowMatch !== null, 'Albums scanned row is present');
  assert(scannedRowMatch[1] === 'metric-muted',
    `Albums scanned must read muted (untrustworthy count) for status=incomplete, got ${JSON.stringify(scannedRowMatch[1])}`);
}
console.log('renderRetagDivergenceCensusCard() N1 — a clean report keeps Albums scanned unmuted');
{
  const html = __test__.renderRetagDivergenceCensusCard({
    state: 'ok', error: null,
    snapshot: {
      generated_at: '2026-08-14T09:00:00+00:00',
      duration_seconds: 196.4,
      report: {
        status: 'clean', complete: true,
        counts: {albums_scanned: 8487},
        albums: [],
      },
    },
  });
  const scannedRowMatch = html.match(/Albums scanned<\/span><strong class="([^"]*)">8,487/);
  assert(scannedRowMatch !== null, 'Albums scanned row with the real count is present');
  assert(scannedRowMatch[1] !== 'metric-muted',
    `Albums scanned must not read muted for a clean, trustworthy status, got ${JSON.stringify(scannedRowMatch[1])}`);
  const listedRowMatch = html.match(/Listed \(non-agreeing\)<\/span><strong class="([^"]*)">0/);
  assert(listedRowMatch[1] === 'metric-good', 'clean + zero listed still reads good');
}

console.log('renderMarkIncompleteButton() three-state contract (#1241)');
{
  // Marked row → the clear action, sending marked=false.
  const clearHtml = __test__.renderMarkIncompleteButton(
    {request_id: 310, marked_incomplete: true});
  assertContains(clearHtml, '>Clear incomplete mark</button>',
    'marked row offers the clear action');
  assertContains(clearHtml, 'window.toggleMarkIncomplete(310, false, this)',
    'clear action sends marked=false for THIS request');
  // Unmarked row → the mark action, sending marked=true.
  const markHtml = __test__.renderMarkIncompleteButton(
    {request_id: 311, marked_incomplete: false});
  assertContains(markHtml, '>Mark incomplete</button>',
    'unmarked row offers the mark action');
  assertContains(markHtml, 'window.toggleMarkIncomplete(311, true, this)',
    'mark action sends marked=true for THIS request');
  // No resolvable request → no button at all.
  assert(
    __test__.renderMarkIncompleteButton(
      {request_id: null, marked_incomplete: false}) === '',
    'a census album with no resolvable request renders no button');
  assert(
    __test__.renderMarkIncompleteButton({}) === '',
    'a row with request_id absent renders no button');
}

console.log('renderLibraryCompletenessCard() wires the mark buttons per row (#1241)');
{
  const html = __test__.renderLibraryCompletenessCard({
    state: 'ok',
    error: null,
    albums_shown: 3,
    albums_listed_total: 3,
    snapshot: {
      generated_at: new Date().toISOString(),
      duration_seconds: 196.0,
      report: {
        status: 'incomplete',
        counts: {
          albums_scanned: 3, audio_complete: 0,
          missing_source_audio: 3, catalog_drift: 0, unknown: 0,
        },
        albums: [
          {album_id: 1, artist: 'Dirt Dress', title: 'Theme Songs',
           release_id: 'rel-marked', request_id: 310,
           marked_incomplete: true,
           findings: [{kind: 'missing_source_audio', detail: 'Peter'}]},
          {album_id: 2, artist: 'Stellastarr*', title: 'Harmonies',
           release_id: 'rel-unmarked', request_id: 311,
           marked_incomplete: false,
           findings: [{kind: 'missing_source_audio', detail: 'Hidden'}]},
          {album_id: 3, artist: 'UNKLE', title: 'Psyence Fiction',
           release_id: 'rel-none', request_id: null,
           marked_incomplete: false,
           findings: [{kind: 'missing_source_audio', detail: 'Breather'}]},
        ],
      },
    },
  });
  assertContains(html, 'window.toggleMarkIncomplete(310, false, this)',
    'card wires the clear action to the marked album');
  assertContains(html, 'window.toggleMarkIncomplete(311, true, this)',
    'card wires the mark action to the unmarked album');
  assertExcludes(html, 'window.toggleMarkIncomplete(null',
    'card never wires an action to an unresolvable album');
}

console.log('renderLibraryCompletenessCard() offers Run census now in every branch');
{
  const missing = __test__.renderLibraryCompletenessCard({state: 'missing'});
  assertContains(missing, 'window.refreshLibraryCensus(this)',
    'missing branch offers the census run action');
  const unreadable = __test__.renderLibraryCompletenessCard({
    state: 'unreadable', error: 'boom',
  });
  assertContains(unreadable, 'window.refreshLibraryCensus(this)',
    'unreadable branch offers the repair action');
  const populated = __test__.renderLibraryCompletenessCard({
    state: 'ok', error: null, albums_shown: 0, albums_listed_total: 0,
    snapshot: {
      generated_at: new Date().toISOString(),
      duration_seconds: 90.0,
      report: {
        status: 'complete',
        counts: {albums_scanned: 1, audio_complete: 1,
                 missing_source_audio: 0, catalog_drift: 0, unknown: 0},
        albums: [],
      },
    },
  });
  assertContains(populated, 'window.refreshLibraryCensus(this)',
    'populated branch offers the census run action');
  assertContains(populated, 'metric-value-push',
    'Last run value clusters with the button in the value column');
}

console.log('main.js binds window.refreshLibraryCensus (the onclick dead-end guard, #1110 shape)');
{
  const prevWindow = globalThis.window;
  const prevDocument = globalThis.document;
  /** @type {any} */
  globalThis.window = { setTimeout: () => 0 };
  /** @type {any} */
  const fakeEl = { classList: { add() {}, remove() {} } };
  /** @type {any} */
  globalThis.document = {
    querySelectorAll() {
      return { forEach(/** @type {(t: any) => void} */ fn) { fn(fakeEl); } };
    },
    querySelector() { return fakeEl; },
    getElementById(/** @type {string} */ id) {
      // main.js wires a listener on #q only when present; the stub has
      // no addEventListener, so 'q' must be absent (same shape as the
      // search-plan F12 stub).
      if (id === 'q') return null;
      return fakeEl;
    },
  };
  try {
    await import('../web/js/main.js');
    /** @type {any} */
    const bound = globalThis.window.refreshLibraryCensus;
    assert(typeof bound === 'function',
      'main.js wires window.refreshLibraryCensus');
    /** @type {any} */
    const mark = globalThis.window.toggleMarkIncomplete;
    assert(typeof mark === 'function',
      'main.js wires window.toggleMarkIncomplete (#1241)');
  } finally {
    globalThis.window = prevWindow;
    globalThis.document = prevDocument;
  }
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
