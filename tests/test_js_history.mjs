/**
 * Unit tests for web/js/history.js download-history rendering.
 * Run with: node tests/test_js_history.mjs
 */

import { renderDownloadHistoryItem } from '../web/js/history.js';

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

console.log('renderDownloadHistoryItem() shows wrong-match triage audit rows');
{
  const html = renderDownloadHistoryItem({
    outcome: 'rejected',
    soulseek_username: 'moundsofass',
    created_at: '2026-04-25T23:25:00+00:00',
    beets_distance: 0.190,
    verdict: 'Wrong match (dist 0.190)',
    wrong_match_triage_summary: 'deleted: spectral reject',
    wrong_match_triage_action: 'deleted_reject',
    wrong_match_triage_preview_verdict: 'confident_reject',
    wrong_match_triage_preview_decision: 'requeue_upgrade',
    wrong_match_triage_reason: 'requeue_upgrade',
    wrong_match_triage_stage_chain: ['mp3_spectral:reject'],
  });

  assertContains(html, 'Triage', 'triage summary label rendered');
  assertContains(html, 'deleted: spectral reject', 'triage summary rendered');
  assertContains(html, 'Preview', 'preview label rendered');
  assertContains(html, 'confident_reject / requeue_upgrade',
    'preview verdict and decision rendered');
  assertContains(html, 'mp3_spectral:reject', 'stage chain rendered');
  assertContains(html, 'Wrong match (dist 0.190)',
    'original verdict remains visible');
}

console.log('renderDownloadHistoryItem() omits empty triage rows');
{
  const html = renderDownloadHistoryItem({
    outcome: 'success',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    downloaded_label: 'MP3 320',
    verdict: 'MP3 320',
  });

  assertExcludes(html, 'Triage', 'no triage label without audit');
  assertExcludes(html, 'Preview', 'no preview label without audit');
  assertExcludes(html, 'Stages', 'no stages label without audit');
}

console.log('renderDownloadHistoryItem() escapes wrong-match triage audit values');
{
  const html = renderDownloadHistoryItem({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    verdict: 'Wrong match',
    wrong_match_triage_summary: '<img src=x>',
    wrong_match_triage_preview_verdict: 'confident<script>',
    wrong_match_triage_stage_chain: ['mp3_spectral:<reject>'],
  });

  assertContains(html, '&lt;img src=x&gt;', 'triage summary escaped');
  assertContains(html, 'confident&lt;script&gt;', 'preview verdict escaped');
  assertContains(html, 'mp3_spectral:&lt;reject&gt;', 'stage chain escaped');
  assertExcludes(html, '<img src=x>', 'raw summary not rendered');
  assertExcludes(html, 'confident<script>', 'raw preview not rendered');
}

console.log('renderDownloadHistoryItem() shows actual and spectral existing bitrate');
{
  const html = renderDownloadHistoryItem({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    existing_min_bitrate: 246,
    existing_spectral_bitrate: 128,
  });

  assertContains(html, 'On disk (before)', 'existing bitrate label rendered');
  assertContains(html, '246kbps', 'actual existing bitrate rendered');
  assertContains(html, '~128kbps', 'spectral existing bitrate rendered');
  assertContains(html, '~128kbps (spectral)', 'spectral style retained');
}

console.log('renderDownloadHistoryItem() keeps single existing bitrate styles');
{
  const spectralOnlyHtml = renderDownloadHistoryItem({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    existing_spectral_bitrate: 128,
  });
  const actualOnlyHtml = renderDownloadHistoryItem({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    existing_min_bitrate: 246,
  });

  assertContains(spectralOnlyHtml, '~128kbps (spectral)',
    'spectral-only existing bitrate keeps spectral style');
  assertExcludes(spectralOnlyHtml, '246kbps',
    'spectral-only existing bitrate does not invent actual bitrate');
  assertContains(actualOnlyHtml, '246kbps',
    'actual-only existing bitrate keeps plain style');
  assertExcludes(actualOnlyHtml, '~246kbps',
    'actual-only existing bitrate is not shown as spectral');
  assertExcludes(actualOnlyHtml, '(spectral)',
    'actual-only existing bitrate has no spectral suffix');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
