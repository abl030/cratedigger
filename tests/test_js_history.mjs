/**
 * Unit tests for web/js/history.js download-history rendering.
 * Run with: node tests/test_js_history.mjs
 */

import { renderDownloadHistoryItem, __test__ } from '../web/js/history.js';
const { formatV0Probe, formatSpectral, withWas } = __test__;

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

console.log('renderDownloadHistoryItem() shows Bitrate row with inline (was X) comparison');
{
  const html = renderDownloadHistoryItem({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    actual_min_bitrate: 192,
    existing_min_bitrate: 192,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 160,
    existing_spectral_bitrate: 96,
  });

  // Single grid, every metric on its own row. Existing data inline as "(was X)".
  assertContains(html, 'class="p-hist-grid"',
    'one consistent grid renders for every entry');
  assertContains(html, 'class="p-hist-label">Bitrate</span>',
    'Bitrate row label present');
  assertContains(html, 'class="p-hist-label">Spectral</span>',
    'Spectral row label present');
  assertContains(html, '192kbps', 'candidate bitrate rendered');
  // "(was Xkbps)" suffix puts the existing comparison on the same row.
  assertContains(html, 'class="p-hist-was">(was 192kbps)',
    'existing bitrate appears inline as (was X) on the candidate row');
  assertContains(html, '~160kbps', 'candidate spectral floor rendered');
  assertContains(html, 'class="p-hist-was">(was <span style="color:#aa8;">~96kbps</span>)',
    'existing spectral appears inline as (was ~Xkbps) on the spectral row');
}

console.log('renderDownloadHistoryItem() omits the (was X) suffix when no existing data');
{
  const html = renderDownloadHistoryItem({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    actual_min_bitrate: 192,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 160,
  });

  assertContains(html, '192kbps', 'candidate bitrate rendered');
  assertExcludes(html, '(was', 'no (was) suffix when existing data absent');
}

console.log('renderDownloadHistoryItem() renders lossless V0 probe with inline (was X) comparison');
{
  const html = renderDownloadHistoryItem({
    outcome: 'success',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_avg_bitrate: 228,
    existing_v0_probe_avg_bitrate: 171,
    final_format: 'opus 128',
    verdict: 'Provisional lossless source',
  });

  assertContains(html, 'class="p-hist-label">V0 probe</span>',
    'V0 probe row present for lossless source');
  assertContains(html, '228kbps avg', 'candidate V0 probe avg rendered');
  assertContains(html, 'class="p-hist-was">(was 171kbps avg)',
    'existing V0 probe appears inline as (was X) on the V0 probe row');
  assertContains(html, 'Stored as', 'final format label rendered');
  assertContains(html, 'opus 128', 'final format rendered');
  assertExcludes(html, '(lossless_source_v0)',
    'lossless probe omits the noisy kind suffix');
}

console.log('renderDownloadHistoryItem() falls back to existing min bitrate for V0 probe (was X) when existing has no V0 probe');
{
  // Lossless candidate upgrading a lossy library album — the existing
  // side has no V0 probe but does have a min bitrate. Show that as the
  // "(was X)" comparison rather than dropping it entirely.
  const html = renderDownloadHistoryItem({
    outcome: 'success',
    soulseek_username: 'awellregulatedabbey',
    created_at: '2026-05-19T13:43:00+00:00',
    downloaded_label: 'FLAC (converted to OPUS V0)',
    spectral_grade: 'genuine',
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_avg_bitrate: 260,
    actual_min_bitrate: 295,
    existing_min_bitrate: 192,
    final_format: 'opus 128',
  });

  assertContains(html, 'class="p-hist-label">V0 probe</span>',
    'V0 probe row present');
  assertContains(html, '260kbps avg', 'candidate V0 probe avg rendered');
  assertContains(html, 'class="p-hist-was">(was 192kbps)',
    'V0 probe (was X) falls back to existing min bitrate when no existing V0 probe');
}

console.log('renderDownloadHistoryItem() drops the V0 probe row for non-lossless candidates');
{
  // Non-lossless candidates carry a v0_probe (kind=native_lossy_research_v0)
  // in the DB so backend policy can read it, but the same number already
  // appears in the Bitrate row — rendering it twice would be redundant
  // and the "(measurement)" qualifier was misleading. Drop it from the
  // UI surface.
  const html = renderDownloadHistoryItem({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    v0_probe_kind: 'native_lossy_research_v0',
    v0_probe_avg_bitrate: 247,
    actual_min_bitrate: 232,
    final_format: 'MP3',
    downloaded_label: 'MP3 V0',
  });

  assertExcludes(html, 'V0 probe',
    'no V0 probe row when kind is not lossless_source_v0');
  assertExcludes(html, '(measurement)',
    'no leftover (measurement) suffix anywhere');
  assertContains(html, '232kbps',
    'candidate bitrate still rendered from actual_min_bitrate');
}

console.log('renderDownloadHistoryItem() keeps a consistent row vocabulary across codecs');
{
  // Same renderer, two very different rows — both should expose
  // Source, Spectral, Bitrate as the consistent vocabulary so the
  // download history reads as a uniform table.
  const losslessHtml = renderDownloadHistoryItem({
    outcome: 'success',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    downloaded_label: 'FLAC (converted to OPUS V0)',
    spectral_grade: 'genuine',
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_avg_bitrate: 260,
    actual_min_bitrate: 295,
    existing_min_bitrate: 192,
    final_format: 'opus 128',
  });
  const lossyHtml = renderDownloadHistoryItem({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    downloaded_label: 'MP3 V2',
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 160,
    v0_probe_kind: 'native_lossy_research_v0',
    v0_probe_avg_bitrate: 232,
    actual_min_bitrate: 192,
    existing_min_bitrate: 192,
    final_format: 'MP3',
  });

  for (const html of [losslessHtml, lossyHtml]) {
    assertContains(html, 'class="p-hist-label">Source</span>',
      'Source row in every entry');
    assertContains(html, 'class="p-hist-label">Spectral</span>',
      'Spectral row in every entry');
    assertContains(html, 'class="p-hist-label">Bitrate</span>',
      'Bitrate row in every entry');
  }
}

console.log('withWas() helper appends the existing comparison inline');
{
  if (withWas('100kbps', '90kbps') !== '100kbps <span class="p-hist-was">(was 90kbps)</span>') {
    failed++;
    console.error('  FAIL: withWas should append (was Y) inline');
  } else { passed++; }
  if (withWas('100kbps', null) !== '100kbps') {
    failed++;
    console.error('  FAIL: withWas should return bare value when wasValue is null');
  } else { passed++; }
  if (withWas('100kbps', undefined) !== '100kbps') {
    failed++;
    console.error('  FAIL: withWas should return bare value when wasValue is undefined');
  } else { passed++; }
}

console.log('formatSpectral() helper colors grades and prefixes the floor');
{
  if (!formatSpectral('genuine').includes('#6d6')) {
    failed++;
    console.error('  FAIL: genuine should be green (#6d6)');
  } else { passed++; }
  if (!formatSpectral('suspect').includes('#d66')) {
    failed++;
    console.error('  FAIL: suspect should be red (#d66)');
  } else { passed++; }
  if (!formatSpectral('genuine', 96).includes('~96kbps')) {
    failed++;
    console.error('  FAIL: spectral with floor should show ~96kbps');
  } else { passed++; }
  if (formatSpectral('genuine').includes('~')) {
    failed++;
    console.error('  FAIL: spectral without floor should not show ~');
  } else { passed++; }
}

console.log('formatV0Probe() helper picks the right kind suffix per source lineage');
{
  if (formatV0Probe(260, 'lossless_source_v0') !== '260kbps avg') {
    failed++;
    console.error('  FAIL: lossless probe should render bare ("260kbps avg")');
  } else { passed++; }
  // ``native_lossy_research_v0`` still produces a "(measurement)" tag
  // for any caller that uses the helper directly (e.g. debug surfaces).
  // The download-history renderer hides the V0 probe row entirely for
  // non-lossless candidates because the same number appears in Bitrate.
  if (formatV0Probe(247, 'native_lossy_research_v0') !== '247kbps avg (measurement)') {
    failed++;
    console.error('  FAIL: native_lossy_research_v0 should add "(measurement)" suffix');
  } else { passed++; }
  if (formatV0Probe(200, undefined) !== '200kbps avg') {
    failed++;
    console.error('  FAIL: missing kind should render bare');
  } else { passed++; }
  if (formatV0Probe(180, 'on_disk_research_v0') !== '180kbps avg (on_disk_research_v0)') {
    failed++;
    console.error('  FAIL: unknown kind should fall back to raw label');
  } else { passed++; }
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
