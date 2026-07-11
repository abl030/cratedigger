/**
 * Unit tests for web/js/pipeline.js queue/nav/search helpers.
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

console.log('request 6039 current Quality uses average positive track bitrate');
{
  const html = __test__.renderCurrentQualityRow(
    {
      current_spectral_bitrate: null,
      last_download_spectral_bitrate: null,
      current_spectral_grade: null,
      last_download_spectral_grade: null,
      verified_lossless: false,
    },
    [
      ...Array.from({ length: 6 }, () => ({ format: 'MP3', bitrate: 320000 })),
      { format: 'MP3', bitrate: 196000 },
      { format: 'MP3', bitrate: 194000 },
    ],
  );
  assertContains(html, 'MP3 V0', 'avg 288 renders the current V0 label');
  assertExcludes(html, 'MP3 V2', 'min 194 never paints current quality');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
