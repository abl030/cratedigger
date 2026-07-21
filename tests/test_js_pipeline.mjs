/**
 * Unit tests for web/js/pipeline.js navigation/detail helpers.
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

console.log('renderPipelineNav() has operational views only');
{
  state.pipelineView = 'dashboard';
  const html = __test__.renderPipelineNav();
  assertExcludes(html, 'window.setPipelineView(\'queue\')', 'request queue tab removed');
  assertContains(html, 'window.setPipelineView(\'dashboard\')', 'dashboard tab rendered');
  assertContains(html, 'window.setPipelineView(\'long-tail\')', 'long-tail tab rendered');
  assertContains(html, 'window.loadPipelineDashboard()', 'dashboard refresh reloads metrics');
  assertContains(html, 'subtab-refresh', 'refresh uses shared subtab layout');
}
console.log('renderPipelineNav() refreshes the dashboard subtab');
{
  state.pipelineView = 'dashboard';
  const html = __test__.renderPipelineNav();
  assertContains(html, 'window.loadPipelineDashboard()', 'dashboard refresh reloads dashboard metrics');
  assertContains(html, 'subtab-refresh', 'refresh uses shared subtab layout');
}
console.log('pipeline status controls disable invalid unsearchable transitions');
{
  const imported = __test__.renderPipelineStatusButtons(42, 'imported');
  assertContains(imported, "class=\"p-btn active-status\" onclick=\"event.stopPropagation(); window.updateStatus(42, 'imported')\">imported</button>", 'imported remains visibly current');
  assertExcludes(imported, "window.updateStatus(42, 'unsearchable')", 'imported cannot invoke unsearchable');
  assertContains(imported, 'disabled aria-disabled="true">unsearchable</button>', 'invalid imported stop is disabled');

  const downloading = __test__.renderPipelineStatusButtons(42, 'downloading');
  assertContains(downloading, 'disabled aria-disabled="true">downloading</button>', 'downloading remains visibly current');
  assertExcludes(downloading, "window.updateStatus(42, 'unsearchable')", 'downloading cannot invoke unsearchable');
  assertContains(downloading, 'disabled aria-disabled="true">unsearchable</button>', 'invalid downloading stop is disabled');

  const wanted = __test__.renderPipelineStatusButtons(42, 'wanted');
  assertContains(wanted, "window.updateStatus(42, 'unsearchable')", 'wanted may become unsearchable');

  const stopped = __test__.renderPipelineStatusButtons(42, 'unsearchable');
  assertContains(stopped, "window.updateStatus(42, 'unsearchable')", 'current unsearchable state remains an active control');
  assertContains(stopped, 'class="p-btn active-status"', 'unsearchable remains visibly current');
}
console.log('request detail caps history and collapses tracks');
{
  const history = Array.from({ length: 12 }, (_, id) => ({
    id, created_at: '2026-07-13T00:00:00+00:00',
  }));
  const tracks = Array.from({ length: 18 }, (_, id) => ({ id, title: `Track ${id}` }));
  const html = __test__.renderRequestEvidenceSections(history, tracks, []);
  assertContains(html, 'Download History (12)', 'full history count remains visible');
  assertContains(html, 'Show 2 older attempts', 'only older attempts move behind disclosure');
  assertContains(html, '<details class="p-tracks"', 'library tracks are collapsed by default');
  assertContains(html, 'In Library (18 tracks)', 'track disclosure keeps its count');
}

console.log('request detail disclosure — generated count sweep');
for (let count = 0; count <= 30; count++) {
  const history = Array.from({ length: count }, (_, id) => ({
    id, created_at: '2026-07-13T00:00:00+00:00',
  }));
  const html = __test__.renderRequestEvidenceSections(history, [], []);
  const expectedOlder = Math.max(0, count - 10);
  if (expectedOlder === 0) {
    assertExcludes(html, 'older attempt', `${count} histories need no older disclosure`);
  } else {
    assertContains(html, `Show ${expectedOlder} older attempt`, `${count} histories expose exact remainder`);
  }
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

console.log('current Quality uses the shared ordered spectral palette');
for (const [grade, tone] of [
  ['likely_transcode', 'poor'],
  ['suspect', 'acceptable'],
  ['marginal', 'good'],
  ['genuine', 'lossless'],
]) {
  const html = __test__.renderCurrentQualityRow(
    {
      current_spectral_bitrate: 128,
      last_download_spectral_bitrate: null,
      current_spectral_grade: grade,
      last_download_spectral_grade: null,
      verified_lossless: false,
    },
    [{ format: 'MP3', bitrate: 192000 }],
  );
  assertContains(html, `quality-tone-${tone}`, `${grade} uses shared ${tone} tone`);
  assertContains(html, grade.replaceAll('_', ' '), `${grade} is humanized`);
  assertExcludes(html, grade.includes('_') ? grade : '__never__',
    `${grade} never leaks a raw token`);
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
