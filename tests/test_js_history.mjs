/**
 * Unit tests for web/js/history.js download-history rendering.
 * Run with: node tests/test_js_history.mjs
 */

import { renderDownloadHistoryItem, __test__ } from '../web/js/history.js';
const { formatV0Probe, formatSpectral } = __test__;

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

console.log('renderDownloadHistoryItem() shows actual and spectral existing bitrate in side-by-side comparison');
{
  const html = renderDownloadHistoryItem({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    existing_min_bitrate: 246,
    existing_spectral_bitrate: 128,
  });

  // After the side-by-side restructure, each existing fact gets its own
  // label/value pair under the "On disk (before)" header rather than
  // being concatenated into one composite line.
  assertContains(html, 'On disk (before)', 'on-disk section header rendered');
  assertContains(html, 'class="p-hist-label">Bitrate</span>',
    'existing bitrate has its own row');
  assertContains(html, '246kbps', 'actual existing bitrate rendered');
  assertContains(html, 'class="p-hist-label">Spectral</span>',
    'existing spectral has its own row');
  assertContains(html, '~128kbps', 'spectral existing bitrate rendered');
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

  assertContains(spectralOnlyHtml, '~128kbps',
    'spectral-only existing bitrate shows the ~ prefix');
  assertExcludes(spectralOnlyHtml, '246kbps',
    'spectral-only existing bitrate does not invent actual bitrate');
  assertExcludes(spectralOnlyHtml, 'class="p-hist-label">Bitrate</span>',
    'spectral-only existing has no Bitrate row');
  assertContains(actualOnlyHtml, '246kbps',
    'actual-only existing bitrate keeps plain style');
  assertExcludes(actualOnlyHtml, '~246kbps',
    'actual-only existing bitrate is not shown as spectral');
  assertExcludes(actualOnlyHtml, 'class="p-hist-label">Spectral</span>',
    'actual-only existing has no Spectral row');
}

console.log('renderDownloadHistoryItem() shows provisional V0 probe evidence');
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

  assertContains(html, 'V0 probe', 'candidate probe label rendered');
  assertContains(html, '228kbps avg', 'candidate probe avg rendered');
  // The existing probe now sits in the side-by-side "On disk (before)"
  // section using the shared formatV0Probe helper — same rendering
  // rules as the candidate side. Bare "171kbps avg" with no source-V0
  // qualifier, because the section header ("On disk") already labels it.
  assertContains(html, '171kbps avg', 'existing probe avg rendered');
  assertContains(html, 'Stored as', 'final format label rendered');
  assertContains(html, 'opus 128', 'final format rendered');
  assertExcludes(html, '(lossless_source_v0)',
    'lossless probe omits the noisy kind suffix');
}

console.log('renderDownloadHistoryItem() renders non-lossless V0 probe with "(measurement)" suffix');
{
  const html = renderDownloadHistoryItem({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    v0_probe_kind: 'native_lossy_research_v0',
    v0_probe_avg_bitrate: 247,
    final_format: 'MP3',
    downloaded_label: 'MP3 V0',
  });

  assertContains(html, 'V0 probe', 'V0 probe label rendered for non-lossless source');
  assertContains(html, '247kbps avg (measurement)',
    'non-lossless probe shows "(measurement)" to flag non-comparable provenance');
  assertExcludes(html, '(native_lossy_research_v0)',
    'raw kind string not surfaced in non-lossless probe value');
}

console.log('renderDownloadHistoryItem() separates Downloaded from On disk into side-by-side sections');
{
  const html = renderDownloadHistoryItem({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    downloaded_label: 'MP3 V0',
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 96,
    v0_probe_kind: 'native_lossy_research_v0',
    v0_probe_avg_bitrate: 247,
    existing_min_bitrate: 237,
    existing_spectral_bitrate: 92,
    final_format: 'MP3',
    beets_distance: 0.066,
  });

  // The wrapper that holds the two sections.
  assertContains(html, 'class="p-hist-sides"', 'two-side wrapper rendered');
  assertExcludes(html, 'class="p-hist-row"', 'old stacked rows are gone');

  // Section headers identify each side.
  assertContains(html, '>Downloaded<', 'Downloaded section header rendered');
  assertContains(html, '>On disk (before)<', 'On disk section header rendered');

  // Apples-to-apples: same metric labels appear on both sides where data
  // is present.
  assertContains(html, 'class="p-hist-label">V0 probe</span>',
    'V0 probe label appears on candidate side');
  assertContains(html, 'class="p-hist-value">247kbps avg (measurement)',
    'candidate V0 probe value renders with measurement suffix');
  assertContains(html, '237kbps', 'existing bitrate renders');
  assertContains(html, '~92kbps', 'existing spectral renders with floor');

  // Distance + verdict + triage stays in the common footer grid below
  // the side-by-side comparison.
  assertContains(html, 'class="p-hist-grid"', 'common footer grid rendered');
  assertContains(html, '0.066', 'distance rendered in footer');
}

console.log('renderDownloadHistoryItem() omits the On disk section when no existing data');
{
  const html = renderDownloadHistoryItem({
    outcome: 'success',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    downloaded_label: 'MP3 V2',
    spectral_grade: 'genuine',
    v0_probe_kind: 'native_lossy_research_v0',
    v0_probe_avg_bitrate: 192,
    final_format: 'MP3',
    beets_distance: 0.137,
  });

  assertContains(html, '>Downloaded<', 'Downloaded section present');
  assertExcludes(html, '>On disk (before)<',
    'On disk section absent when no existing data');
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
