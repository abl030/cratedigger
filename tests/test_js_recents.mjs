/**
 * Unit tests for web/js/recents.js queue rendering helpers.
 * Run with: node tests/test_js_recents.mjs
 */

import { __test__ } from '../web/js/recents.js';

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

console.log('renderImportQueueItems() shows importable next row and preview detail');
{
  const html = __test__.renderImportQueueItems([{
    id: 77,
    job_type: 'force_import',
    status: 'queued',
    preview_status: 'would_import',
    artist_name: 'Broadcast',
    album_title: 'Tender Buttons',
    preview_message: 'Preview would import: import',
    preview_result: { stage_chain: ['stage2_import:import'] },
  }]);
  assertContains(html, 'Tender Buttons', 'album title rendered');
  assertContains(html, 'Broadcast', 'artist name rendered');
  assertContains(html, 'next import', 'first importable row is marked next');
  assertContains(html, 'preview: would_import', 'preview state rendered');
  assertContains(html, 'stage2_import:import', 'stage chain rendered');
}

console.log('renderImportQueueItems() shows uncertain preview failures without next styling');
{
  const html = __test__.renderImportQueueItems([{
    id: 78,
    job_type: 'manual_import',
    status: 'failed',
    preview_status: 'uncertain',
    artist_name: 'Low',
    album_title: 'Things We Lost in the Fire',
    preview_message: 'Preview failed: path_missing',
  }]);
  assertContains(html, 'uncertain', 'uncertain badge rendered');
  assertContains(html, 'Preview failed: path_missing', 'failure message rendered');
  assertExcludes(html, 'next import', 'uncertain rows are not marked next');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
