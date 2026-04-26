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

console.log('renderImportQueueItems() prefers terminal import messages over stale preview messages');
{
  const html = __test__.renderImportQueueItems([{
    id: 731,
    job_type: 'automation_import',
    status: 'failed',
    preview_status: 'would_import',
    artist_name: 'Muse',
    album_title: 'Origin Of Symmetry',
    preview_message: 'Preview gate disabled',
    message: 'Rejected: high_distance - distance=0.1611',
  }]);
  assertContains(html, 'Rejected: high_distance - distance=0.1611',
    'terminal failure message rendered');
  assertExcludes(html, 'Preview gate disabled',
    'stale preview message hidden for terminal rows');
}

console.log('renderRecentsItems() shows bad-extension postflight warning chip');
{
  const html = __test__.renderRecentsItems([{
    id: 584,
    request_id: 604,
    created_at: '2026-04-02T12:55:41+00:00',
    album_title: 'Sleeps Like a Curse',
    artist_name: 'The Panics',
    badge: 'Imported',
    badge_class: 'badge-new',
    border_color: '#1a4a2a',
    summary: 'MP3 320 · user',
    bad_extensions: ['01 One Too Many Itches.bak'],
  }]);
  assertContains(html, 'bad ext: 1', 'bad extension chip rendered');
  assertContains(html, '01 One Too Many Itches.bak',
    'bad extension filename appears in hover detail');
}

console.log('renderRecentsItems() shows wrong-match triage audit chip');
{
  const html = __test__.renderRecentsItems([{
    id: 725,
    request_id: 801,
    created_at: '2026-04-25T23:25:00+00:00',
    album_title: 'For Screening Purposes Only',
    artist_name: 'Test Icicles',
    badge: 'Rejected',
    badge_class: 'badge-rejected',
    border_color: '#a33',
    summary: 'Wrong match (dist 0.190) · moundsofass',
    wrong_match_triage_summary: 'deleted: spectral reject',
    wrong_match_triage_detail: 'action: deleted reject · stages: mp3_spectral:reject',
  }]);
  assertContains(html, 'Wrong match (dist 0.190) · moundsofass',
    'original wrong-match summary remains visible');
  assertContains(html, 'triage: deleted: spectral reject',
    'triage chip rendered next to rejected badge');
  assertContains(html, 'mp3_spectral:reject',
    'triage detail appears in hover text');
}

console.log('renderRecentsItems() escapes wrong-match triage chip fields');
{
  const html = __test__.renderRecentsItems([{
    id: 726,
    request_id: 802,
    created_at: '2026-04-25T23:25:00+00:00',
    album_title: 'Unsafe',
    artist_name: 'Artist',
    badge: 'Rejected',
    badge_class: 'badge-rejected',
    border_color: '#a33',
    summary: 'Wrong match',
    wrong_match_triage_summary: '<img src=x>',
    wrong_match_triage_detail: 'stage:<script>',
  }]);
  assertContains(html, '&lt;img src=x&gt;',
    'triage summary is escaped');
  assertContains(html, 'stage:&lt;script&gt;',
    'triage detail is escaped');
  assertExcludes(html, '<img src=x>',
    'raw triage summary is not rendered');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
