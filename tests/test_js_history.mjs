/**
 * Unit tests for web/js/history.js download-history rendering.
 * Run with: node tests/test_js_history.mjs
 */

import {
  renderDownloadHistoryItem as renderDownloadHistoryFixture,
  renderEvidenceStrip as renderEvidenceFixture,
  __test__,
} from '../web/js/history.js';
import { readFileSync } from 'node:fs';
const { formatV0Probe, formatSpectral, withWas, storageFormatLabel } = __test__;

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

console.log('storageFormatLabel() preserves native codec names');
{
  assertContains(
    storageFormatLabel({ materialized_format: 'vorbis' }, ''),
    'Vorbis',
    'Vorbis is not rendered as the Ogg container or all-caps metadata',
  );
  assertContains(
    storageFormatLabel({ materialized_format: 'wma' }, ''),
    'WMA',
    'WMA keeps its native acronym',
  );
}

console.log('renderDownloadHistoryItem() shows wrong-match triage audit rows');
{
  const html = renderDownloadHistoryFixture({
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
  const html = renderDownloadHistoryFixture({
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
  const html = renderDownloadHistoryFixture({
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

console.log('renderDownloadHistoryItem() refuses to infer output from legacy bitrate columns');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    actual_min_bitrate: 192,
    existing_min_bitrate: 192,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 160,
    existing_spectral_grade: 'suspect',
    existing_spectral_bitrate: 96,
  });

  // Single grid, every metric on its own row. Existing data inline as "(was X)".
  assertContains(html, 'class="p-hist-grid"',
    'one consistent grid renders for every entry');
  assertContains(html, 'class="p-hist-label">Output</span>',
    'Output row label present');
  assertContains(html, 'class="p-hist-label">Spectral</span>',
    'Spectral row label present');
  assertExcludes(html, 'class="p-hist-value">192kbps',
    'legacy candidate minimum is not relabelled as materialized output');
  assertContains(html, '~160kbps', 'candidate spectral floor rendered');
  assertContains(html, 'suspect (~96kbps)',
    'existing spectral grade and floor appear on the spectral row');
}

console.log('renderDownloadHistoryItem() omits the (was X) suffix when no existing data');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    actual_min_bitrate: 192,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 160,
  });

  assertExcludes(html, 'class="p-hist-value">192kbps',
    'candidate bitrate is not relabelled as output');
  assertExcludes(html, '(was', 'no (was) suffix when existing data absent');
}

console.log('two-sided spectral failures remain distinct from legacy unmeasured rows');
{
  const failedHtml = renderDownloadHistoryFixture({
    outcome: 'rejected', created_at: '2026-07-12T00:00:00+00:00',
    spectral_attempted: true,
    spectral_error: 'RuntimeError: decode failed',
    existing_spectral_attempted: true, existing_spectral_grade: 'genuine',
  });
  assertContains(failedHtml, 'analysis failed', 'attempted failure is explicit');
  assertContains(failedHtml, 'RuntimeError: decode failed', 'failure detail is available');
  assertContains(failedHtml, '<details class="p-hist-forensics">',
    'spectral errors are reachable in focusable forensics');
  assertContains(failedHtml, 'Spectral IN error',
    'candidate error has a labelled forensic row');
  const strip = renderEvidenceFixture({
    spectral_attempted: true, spectral_error: 'candidate failed',
    existing_spectral_attempted: true, existing_spectral_error: 'existing failed',
  });
  assertContains(strip, 'IN', 'failure-only audit still renders in Recents');
  assertContains(strip, 'spectral failed', 'Recents keeps failure state compact');
  const legacyHtml = renderDownloadHistoryFixture({
    outcome: 'rejected', created_at: '2026-07-12T00:00:00+00:00',
  });
  assertExcludes(legacyHtml, 'analysis failed', 'legacy row stays unmeasured');
}

console.log('renderDownloadHistoryItem() surfaces HAVE analysis diagnostics');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'have_analysis_error',
    badge: 'Environment failure',
    badge_class: 'badge-warn',
    verdict: 'Installed HAVE analysis failed (permission denied). Request remains wanted; a future download will retry normally.',
    failure_category: 'permission_denied',
    analysis_error: 'PermissionError: <denied>',
    installed_path: '/mnt/Music/Beets/Low/<current>',
    candidate_reference: '/mnt/Music/Incoming/candidate&next',
    soulseek_username: 'archive-peer',
    created_at: '2026-07-16T10:00:00+00:00',
  });
  assertContains(html, 'Environment failure', 'environment badge rendered');
  assertContains(html, 'Failure category', 'failure category label rendered');
  assertContains(html, 'permission denied', 'failure category humanized');
  assertContains(html, 'Installed HAVE', 'installed path label rendered');
  assertContains(html, '/mnt/Music/Beets/Low/&lt;current&gt;', 'installed path escaped');
  assertContains(html, 'Candidate', 'candidate reference label rendered');
  assertContains(html, '/mnt/Music/Incoming/candidate&amp;next', 'candidate reference escaped');
  assertContains(html, 'PermissionError: &lt;denied&gt;', 'analysis error escaped');
  assertContains(html, 'remains wanted', 'retryable state remains prominent');
  assertExcludes(html, 'PermissionError: <denied>', 'raw analysis error not rendered');
}

console.log('legacy existing floor-only Recents labels the missing grade');
{
  const strip = renderEvidenceFixture({ existing_spectral_bitrate: 128 });
  assertContains(strip, 'ungraded (~128k)',
    'legacy HAVE floor cannot read like a complete spectral grade');
}

console.log('renderDownloadHistoryItem() labels both V0 probe sides explicitly');
{
  const html = renderDownloadHistoryFixture({
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
  assertContains(html, '>IN</span> 228kbps avg',
    'candidate V0 probe avg renders on the labelled IN side');
  assertContains(html, '>HAVE</span> 171kbps avg',
    'existing V0 probe renders on the labelled HAVE side');
  assertContains(html, 'Stored as', 'final format label rendered');
  assertContains(html, 'OPUS 128 contract', 'final format rendered as contract');
  assertExcludes(html, '(lossless_source_v0)',
    'lossless probe omits the noisy kind suffix');
}

console.log('renderDownloadHistoryItem() leaves HAVE empty without a comparable V0 probe');
{
  // Lossless candidate over a library album with no recorded V0 probe.
  // The V0-probe row must NOT borrow the existing raw min bitrate as a
  // "(was X)" — painting a V0-probe avg next to a container min reads as
  // a fake upgrade ("260kbps avg (was 192kbps)" mixes two metrics). The
  // min-vs-min comparison still renders on the Bitrate row, so nothing is
  // lost.
  const html = renderDownloadHistoryFixture({
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
  assertContains(html, '>IN</span> 260kbps avg',
    'candidate V0 probe remains on the IN side');
  assertContains(html, '>HAVE</span> —',
    'missing existing probe is explicit without borrowing its raw minimum');
  assertExcludes(html, 'class="p-hist-was">(was 192kbps)',
    'legacy minimums are not projected as materialized output');
}

console.log('renderDownloadHistoryItem() renders the V0 probe row for research probes too');
{
  // V0 probes run on EVERY candidate (native-lossy sources get a real
  // ffmpeg V0-transcode probe, kind=native_lossy_research_v0) and are
  // load-bearing for the operator — Wrong Matches has surfaced them
  // regardless of lineage all along. The "(from lossy)" qualifier keeps
  // the gold-standard lossless-source probes distinguishable. Note the
  // probe (247) is an independent measurement, NOT the container bitrate
  // (232) — the old "redundant with Bitrate" rationale was stale.
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    v0_probe_kind: 'native_lossy_research_v0',
    v0_probe_avg_bitrate: 247,
    actual_min_bitrate: 232,
    final_format: 'MP3',
    downloaded_label: 'MP3 V0',
  });

  assertContains(html, 'V0 probe',
    'V0 probe row renders for research probes');
  assertContains(html, '247kbps avg (from lossy)',
    'research probe carries the from-lossy qualifier');
  assertExcludes(html, 'class="p-hist-value">232kbps',
    'research candidate minimum is not relabelled as output');
}

console.log('renderDownloadHistoryItem() V0 side labels retain kind provenance');
{
  // dl 36660: lossless-source candidate probe (255) vs the library
  // album's native-lossy research probe (250). Both render — the
  // qualifier says which is which instead of hiding the comparison.
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'tunnik',
    created_at: '2026-07-10T23:19:10+00:00',
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_avg_bitrate: 255,
    existing_v0_probe_kind: 'native_lossy_research_v0',
    existing_v0_probe_avg_bitrate: 250,
  });
  assertContains(html, '255kbps avg', 'lossless-source probe renders bare');
  assertContains(html, '>HAVE</span> 250kbps avg (from lossy)',
    'existing research probe renders with its qualifier on HAVE');
}

console.log('renderDownloadHistoryItem() renders HAVE-only V0 provenance');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    created_at: '2026-07-15T00:00:00+00:00',
    existing_v0_probe_kind: 'on_disk_research_v0',
    existing_v0_probe_min_bitrate: 201,
    existing_v0_probe_avg_bitrate: 259,
  });
  assertContains(html, 'class="p-hist-label">V0 probe</span>',
    'HAVE-only evidence still creates the expanded V0 row');
  assertContains(html, '>IN</span> —', 'missing candidate probe is explicit');
  assertContains(html, '>HAVE</span> 259kbps avg · min 201kbps (on-disk re-encode)',
    'HAVE-only probe retains its detailed provenance');
}

console.log('renderDownloadHistoryItem() keeps a consistent row vocabulary across codecs');
{
  // Same renderer, two very different rows — both should expose
  // Source, Spectral, Bitrate as the consistent vocabulary so the
  // download history reads as a uniform table.
  const losslessHtml = renderDownloadHistoryFixture({
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
  const lossyHtml = renderDownloadHistoryFixture({
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
    assertContains(html, 'class="p-hist-label">Output</span>',
      'Output row in every entry');
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
  if (!formatSpectral('genuine').includes('quality-tone-lossless')) {
    failed++;
    console.error('  FAIL: genuine should use the brightest shared green');
  } else { passed++; }
  if (!formatSpectral('marginal').includes('quality-tone-good')) {
    failed++;
    console.error('  FAIL: marginal should use the shared yellow tone');
  } else { passed++; }
  if (!formatSpectral('suspect').includes('quality-tone-acceptable')) {
    failed++;
    console.error('  FAIL: suspect should use the shared orange tone');
  } else { passed++; }
  if (!formatSpectral('likely_transcode').includes('quality-tone-poor')) {
    failed++;
    console.error('  FAIL: likely transcode should use the shared red tone');
  } else { passed++; }
  if (!formatSpectral('genuine', 96).includes('~96kbps')) {
    failed++;
    console.error('  FAIL: spectral with floor should show ~96kbps');
  } else { passed++; }
  if (formatSpectral('genuine').includes('~')) {
    failed++;
    console.error('  FAIL: spectral without floor should not show ~');
  } else { passed++; }
  if (!formatSpectral('likely_transcode', 160).includes('likely transcode (~160kbps)')) {
    failed++;
    console.error('  FAIL: spectral grade tokens should be humanized');
  } else { passed++; }
}

console.log('renderEvidenceStrip() humanizes spectral tokens on both sides');
{
  const html = renderEvidenceFixture({
    spectral_grade: 'likely_transcode', spectral_bitrate: 160,
    existing_spectral_grade: 'likely_transcode', existing_spectral_bitrate: 128,
  });
  assertContains(html, 'likely transcode', 'humanized grade rendered');
  assertExcludes(html, 'likely_transcode', 'raw grade token never leaks');
}

console.log('formatV0Probe() helper picks the right kind suffix per source lineage');
{
  if (formatV0Probe(260, 'lossless_source_v0') !== '260kbps avg') {
    failed++;
    console.error('  FAIL: lossless probe should render bare ("260kbps avg")');
  } else { passed++; }
  // ``native_lossy_research_v0`` is a real ffmpeg V0-transcode probe of a
  // lossy source — qualified "(from lossy)" so it never reads as the
  // gold-standard lossless-source probe.
  if (formatV0Probe(247, 'native_lossy_research_v0') !== '247kbps avg (from lossy)') {
    failed++;
    console.error('  FAIL: native_lossy_research_v0 should add "(from lossy)" suffix');
  } else { passed++; }
  if (formatV0Probe(200, undefined) !== '200kbps avg') {
    failed++;
    console.error('  FAIL: missing kind should render bare');
  } else { passed++; }
  if (formatV0Probe(180, 'on_disk_research_v0') !== '180kbps avg (on-disk re-encode)') {
    failed++;
    console.error('  FAIL: on_disk_research_v0 should render the on-disk re-encode qualifier');
  } else { passed++; }
  if (formatV0Probe(180, 'future_probe_kind') !== '180kbps avg (future_probe_kind)') {
    failed++;
    console.error('  FAIL: unknown kind should fall back to raw label');
  } else { passed++; }
}

console.log('renderDownloadHistoryItem() shows "overridden" instead of the fake 0.000 distance on force imports');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'force_import',
    soulseek_username: 'pimpek1977',
    created_at: '2026-07-10T07:03:00+00:00',
    downloaded_label: 'FLAC (converted to OPUS V0)',
    beets_distance: null,
    original_beets_distance: 0.2328,
    verdict: 'Force imported after manual review',
  });

  assertContains(html, 'class="p-hist-label">Distance</span>',
    'Distance row present on force imports');
  assertContains(html, 'overridden', 'force-import distance reads overridden');
  assertContains(html, '(was 0.233)', 'force-import distance retains its origin measurement');
  assertExcludes(html, '0.000', 'the fake beets 0.000 never renders');
}

console.log('renderDownloadHistoryItem() always renders the core row vocabulary with em-dash placeholders');
{
  // A timeout row with no measurements still shows the fixed schema —
  // Source / Spectral / Bitrate / Distance — so adjacent entries stop
  // jumping shape.
  const html = renderDownloadHistoryFixture({
    outcome: 'timeout',
    soulseek_username: 'griot_not_riot',
    created_at: '2026-07-07T21:22:00+00:00',
    verdict: 'Download failed: file exceeded retry limit',
  });

  for (const label of ['Source', 'Spectral', 'Output', 'Distance']) {
    assertContains(html, `class="p-hist-label">${label}</span>`,
      `${label} row present even without data`);
  }
  assertContains(html, '—', 'unknown cells render an em-dash');
}

console.log('renderDownloadHistoryItem() header uses the server badge vocabulary');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'timeout',
    badge: 'Failed',
    badge_class: 'badge-failed',
    soulseek_username: 'testuser',
    created_at: '2026-07-07T21:22:00+00:00',
  });

  assertContains(html, 'badge badge-failed', 'server badge class on header');
  assertContains(html, '>Failed<', 'server badge label on header');
  // The raw outcome word must not appear as the status any more — the
  // list rows say "Failed", the detail block must not say "timeout".
  assertExcludes(html, '>timeout<', 'raw outcome word no longer the header status');
}

console.log('renderDownloadHistoryItem() header falls back to outcome when badge fields absent');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-07-07T21:22:00+00:00',
  });
  assertContains(html, '>rejected<', 'outcome fallback when classifier fields missing');
}

console.log('renderDownloadHistoryItem() tucks debug forensics behind a details toggle');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    verdict: 'Wrong match (dist 0.190)',
    wrong_match_triage_summary: 'deleted: spectral reject',
    wrong_match_triage_preview_verdict: 'confident_reject',
    wrong_match_triage_preview_decision: 'requeue_upgrade',
    wrong_match_triage_reason: 'requeue_upgrade',
    wrong_match_triage_stage_chain: ['mp3_spectral:reject'],
  });

  assertContains(html, '<details class="p-hist-forensics">',
    'forensics details element present');
  assertContains(html, 'mp3_spectral:reject', 'stage chain still reachable');
  // Triage (the operator-action audit) stays visible outside the toggle.
  const detailsStart = html.indexOf('<details');
  const triagePos = html.indexOf('deleted: spectral reject');
  if (triagePos !== -1 && detailsStart !== -1 && triagePos < detailsStart) {
    passed++;
  } else {
    failed++;
    console.error('  FAIL: triage summary should render before/outside the forensics toggle');
  }
  const stagesPos = html.indexOf('mp3_spectral:reject');
  if (stagesPos > detailsStart && detailsStart !== -1) {
    passed++;
  } else {
    failed++;
    console.error('  FAIL: stage chain should live inside the forensics toggle');
  }
}

console.log('renderEvidenceStrip() builds the compact IN/HAVE comparison');
{
  const strip = renderEvidenceFixture({
    downloaded_label: 'MP3 320',
    actual_min_bitrate: 245,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 160,
    existing_min_bitrate: 320,
  });
  assertContains(strip, 'class="r-evidence"', 'strip wrapper class');
  assertContains(strip, 'class="r-ev-row r-ev-in"', 'IN is a semantic grid row');
  assertContains(strip, 'class="r-ev-row r-ev-have"', 'HAVE is a semantic grid row');
  for (const slot of ['source', 'metric', 'spectral', 'v0']) {
    const count = strip.split(`r-ev-${slot}`).length - 1;
    if (count === 2) { passed++; } else {
      failed++;
      console.error(`  FAIL: ${slot} must occupy the same explicit slot in both rows; got ${count}`);
    }
  }
  assertExcludes(strip, 'r-ev-rank', 'rows do not render the permanently empty rank slot');
  assertExcludes(strip, 'r-ev-value', 'rows do not collapse back to one freeform value cell');
  assertContains(strip, 'IN', 'IN side labelled');
  assertContains(strip, 'MP3 320', 'incoming label rendered');
  assertContains(strip, 'min 245k', 'incoming measured bitrate rendered with the min label');
  assertContains(strip, '~160k', 'incoming spectral floor rendered');
  assertContains(strip, 'HAVE', 'HAVE side labelled');
  assertContains(strip, 'min 320k', 'on-disk bitrate rendered with the min label');
}

console.log('renderEvidenceStrip() keeps converted source bare in collapsed rows');
{
  const strip = renderEvidenceFixture({
    source_format: 'FLAC',
    was_converted: true,
    final_format: 'opus 128',
    comparison_basis: {
      verdict: 'better', branch: 'rank',
      new_rank: 'excellent', existing_rank: 'good',
      new_metric: 'contract', existing_metric: 'avg',
      new_value_kbps: 128, existing_value_kbps: 96,
      new_format: 'opus 128', existing_format: 'Opus',
      spectral_clamped: false, tolerance_kbps: null,
      verified_lossless_bypass: false,
    },
  });
  assertContains(strip, '>FLAC</span>', 'collapsed IN row names the measured source codec');
  assertExcludes(strip, 'FLAC →', 'collapsed row does not show a conversion arrow');
  assertExcludes(strip, 'OPUS 128', 'target/output contract is not labelled as source bitrate');
}

console.log('renderEvidenceStrip() renders canonical candidate evidence as ordinary IN');
{
  const strip = renderEvidenceFixture({
    downloaded_label: 'MP3 V2',
    source_format: 'MP3',
    source_min_bitrate: 201,
    source_avg_bitrate: 259,
    source_median_bitrate: 255,
  });
  assertContains(strip, '>IN</strong>', 'historical triage evidence keeps the normal IN row');
  assertContains(strip, '>MP3</span>', 'candidate evidence supplies the source codec');
  assertContains(strip, '>259k avg (min 201k)</span>',
    'candidate evidence supplies its average and minimum');
}

console.log('renderEvidenceStrip() returns empty string when no evidence exists');
{
  const strip = renderEvidenceFixture({
    outcome: 'timeout',
    error_message: 'remote_queue_timeout 3600s exceeded',
  });
  if (strip === '') { passed++; } else {
    failed++;
    console.error(`  FAIL: no-evidence rows should produce no strip, got '${strip}'`);
  }
}

console.log('renderEvidenceStrip() requires a number — a codec label alone is not a comparison');
{
  // Failed downloads carry downloaded_label (from slskd filetype) but no
  // measurements; a label-only strip would spam "IN MP3 HAVE —" on every
  // failure row in the list.
  const strip = renderEvidenceFixture({
    outcome: 'timeout',
    downloaded_label: 'MP3',
  });
  if (strip === '') { passed++; } else {
    failed++;
    console.error(`  FAIL: label-only rows should produce no strip, got '${strip}'`);
  }
}

console.log('Download failures blank IN and keep the complete pre-attempt HAVE row');
{
  const strip = renderEvidenceFixture({
    outcome: 'timeout',
    source_format: 'FLAC',
    source_min_bitrate: 455,
    source_avg_bitrate: 725,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 96,
    v0_probe_min_bitrate: 178,
    v0_probe_avg_bitrate: 248,
    existing_format: 'Opus',
    existing_min_bitrate: 93,
    existing_avg_bitrate: 129,
    existing_median_bitrate: 128,
    existing_spectral_grade: 'suspect',
    existing_spectral_bitrate: 96,
    existing_v0_probe_min_bitrate: 193,
    existing_v0_probe_avg_bitrate: 256,
  });
  assertContains(
    strip,
    '<strong class="r-ev-tag">IN</strong><span class="r-ev-cell r-ev-source">',
    'timeout renders the IN row',
  );
  assertContains(strip, '>—</span>', 'timeout leaves IN blank');
  assertExcludes(strip, '725k avg', 'timeout hides incoming bitrate');
  assertExcludes(strip, 'V0 248k avg', 'timeout hides incoming V0');
  assertContains(strip, 'Opus', 'timeout keeps HAVE codec');
  assertContains(strip, '129k avg (min 93k)', 'timeout keeps HAVE average and minimum');
  assertContains(strip, '~96k suspect', 'timeout keeps HAVE spectral');
  assertContains(strip, 'V0 256k avg (min 193k)', 'timeout keeps HAVE V0');
}

console.log('Import failures retain the grabbed candidate in IN');
{
  const strip = renderEvidenceFixture({
    outcome: 'failed',
    source_format: 'FLAC',
    source_min_bitrate: 455,
    source_avg_bitrate: 725,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 96,
    v0_probe_min_bitrate: 178,
    v0_probe_avg_bitrate: 248,
    existing_format: 'Opus',
    existing_min_bitrate: 93,
    existing_avg_bitrate: 129,
  });
  assertContains(strip, '725k avg (min 455k)', 'failed import keeps incoming bitrate');
  assertContains(strip, '~96k likely transcode', 'failed import keeps incoming spectral');
  assertContains(strip, 'V0 248k avg (min 178k)', 'failed import keeps incoming V0');
  assertContains(strip, '>725k avg/455k min<', 'mobile metric labels each number in place');
  const spectralCount = strip.split('~96k likely transcode').length - 1;
  if (spectralCount === 2) { passed++; } else {
    failed++;
    console.error(`  FAIL: mobile spectral keeps the full "likely transcode" wording (the column ellipsizes instead); got ${spectralCount} of 2 spans`);
  }
  assertContains(strip, 'V0 248/178k<', 'mobile V0 stays the bare pair — its label is the V0 prefix');
  assertExcludes(strip, 'a/m', 'the cryptic a/m shorthand is dead');
}

console.log('renderEvidenceStrip() keeps a CBR metric pair explicit but still collapses the V0 cell');
{
  const strip = renderEvidenceFixture({
    source_format: 'MP3',
    source_min_bitrate: 320,
    source_avg_bitrate: 320,
    existing_format: 'MP3',
    existing_min_bitrate: 192,
    existing_avg_bitrate: 192,
    existing_v0_probe_avg_bitrate: 245,
    existing_v0_probe_min_bitrate: 245,
  });
  assertContains(strip, '320k avg (min 320k)', 'desktop wording keeps both numbers');
  // A CBR metric cell keeps its avg/min pair on mobile — a bare "320k" was
  // ambiguous with a min-only measurement (issue #813 follow-up).
  assertContains(strip, '>320k avg/320k min<', 'mobile keeps the CBR metric pair explicit');
  assertExcludes(strip, '>320k</span>', 'mobile no longer collapses the CBR metric to one number');
  assertContains(strip, '>V0 245k<', 'an equal V0 pair still collapses (its prefix labels it)');
}

console.log('renderEvidenceStrip() shows the on-disk format on the HAVE side');
{
  // The Mothertongue case (#575): AAC 256 replacing unverified MP3 256.
  // Without the format, "IN M4A V0 · 256k HAVE 256k" reads as a
  // pointless re-download; the codec class WAS the upgrade.
  const strip = renderEvidenceFixture({
    downloaded_label: 'M4A V0',
    actual_min_bitrate: 256,
    spectral_grade: 'genuine',
    existing_format: 'MP3',
    existing_min_bitrate: 256,
  });
  assertContains(strip, '>MP3</span>', 'HAVE side leads with the on-disk format');
  assertContains(strip, '>min 256k</span>', 'HAVE bitrate stays min-labelled in its shared slot');
}

console.log('renderEvidenceStrip() renders a supplied pre-attempt HAVE snapshot');
{
  // Historical renderers receive only the evidence that belonged to the
  // attempt; a later current-library snapshot must never be projected here.
  const strip = renderEvidenceFixture({
    source_format: 'FLAC',
    source_min_bitrate: 455,
    source_avg_bitrate: 725,
    existing_format: 'Opus',
    existing_min_bitrate: 93,
  });
  assertContains(strip, '>Opus</span>', 'pre-attempt format populates HAVE');
  assertContains(strip, '>min 93k</span>', 'pre-attempt minimum populates HAVE');
}

console.log('renderDownloadHistoryItem() does not infer output from legacy min fields');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'japanman797',
    created_at: '2026-07-10T10:30:00+00:00',
    downloaded_label: 'M4A V0',
    actual_min_bitrate: 256,
    existing_format: 'MP3',
    existing_min_bitrate: 256,
  });
  assertContains(html, 'class="p-hist-label">Output</span>',
    'fixed output row remains present');
  assertContains(html, '<span class="p-hist-value">—</span>',
    'legacy row without materialized evidence stays honest');
}

console.log('renderDownloadHistoryItem() does not fabricate output when format is unknown');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'testuser',
    created_at: '2026-07-10T10:30:00+00:00',
    actual_min_bitrate: 320,
    existing_min_bitrate: 256,
  });
  assertContains(html, 'class="p-hist-label">Output</span>',
    'fixed output row remains present');
}

console.log('renderDownloadHistoryItem() calls only explicit quality labels contracts');
{
  const legacyMp3 = renderDownloadHistoryFixture({
    outcome: 'success',
    created_at: '2026-07-13T00:29:00+00:00',
    final_format: 'MP3',
  });
  assertContains(legacyMp3, 'Stored as', 'legacy stored format still renders');
  assertContains(legacyMp3, '>MP3<', 'bare MP3 remains a codec fact');
  assertExcludes(legacyMp3, 'MP3 contract', 'bare MP3 is not a quality contract');

  const explicitOpus = renderDownloadHistoryFixture({
    outcome: 'success',
    created_at: '2026-07-13T01:06:00+00:00',
    final_format: 'opus 128',
  });
  assertContains(explicitOpus, 'OPUS 128 contract', 'numeric target is a contract');
}

console.log('renderEvidenceStrip() stops compact V0 probes after the minimum');
{
  // Probe-kind provenance belongs to expanded details. Every compact kind
  // gets the same bounded numeric form so long qualifiers cannot overflow.
  for (const kind of [
    'lossless_source_v0',
    'native_lossy_research_v0',
    'on_disk_research_v0',
    'future_probe_kind',
  ]) {
    const strip = renderEvidenceFixture({
      downloaded_label: 'MP3 V0',
      actual_min_bitrate: 232,
      v0_probe_kind: kind,
      v0_probe_avg_bitrate: 247,
      v0_probe_min_bitrate: 224,
    });
    assertContains(strip, 'V0 247k avg (min 224k)', `${kind} keeps avg and min`);
    assertExcludes(strip, 'from lossy', `${kind} omits lossy provenance`);
    assertExcludes(strip, 'on-disk re-encode', `${kind} omits re-encode provenance`);
    assertExcludes(strip, 'future_probe_kind', `${kind} omits raw kind provenance`);
  }
}

console.log('renderEvidenceStrip() escapes injected values');
{
  const strip = renderEvidenceFixture({
    downloaded_label: '<img src=x>',
    actual_min_bitrate: 200,
  });
  assertExcludes(strip, '<img src=x>', 'raw label not rendered');
  assertContains(strip, '&lt;img src=x&gt;', 'label escaped');
}

console.log('renderEvidenceStrip() renders the persisted comparison basis when present');
{
  // Request 6039: avg 196->288 rank upgrade; min 194 on BOTH sides made the
  // legacy strip a tautology ("IN MP3 V2 . 194k HAVE MP3 194k").
  const strip = renderEvidenceFixture({
    downloaded_label: 'MP3 V2',
    actual_min_bitrate: 194,
    materialized_format: 'MP3',
    materialized_min_bitrate: 195,
    materialized_avg_bitrate: 320,
    materialized_median_bitrate: 320,
    spectral_grade: 'genuine',
    spectral_bitrate: 160,
    existing_format: 'MP3',
    existing_min_bitrate: 194,
    comparison_basis: {
      verdict: 'better', branch: 'rank',
      new_rank: 'transparent', existing_rank: 'good',
      new_metric: 'avg', existing_metric: 'avg',
      new_value_kbps: 288, existing_value_kbps: 196,
      new_format: 'MP3', existing_format: 'MP3',
      spectral_clamped: false, tolerance_kbps: null,
      verified_lossless_bypass: false,
    },
  });
  assertContains(strip, '288k avg (min 194k)',
    'IN side shows the deciding average and actual minimum');
  assertExcludes(strip, '>transparent</span>',
    'compact IN leaves decision ranks to the expanded detail');
  assertContains(strip, '196k avg (min 194k)',
    'HAVE side shows the deciding average and actual minimum');
  assertExcludes(strip, '>good</span>',
    'compact HAVE leaves decision ranks to the expanded detail');
  assertContains(strip, 'genuine', 'spectral grade chip survives');
  assertContains(strip, '~160k genuine',
    'ordinary avg basis does not suppress a distinct spectral floor');
  assertExcludes(strip, 'MP3 V2', 'min-derived label replaced by the basis');
  assertExcludes(strip, 'actual MP3',
    'materialized output stays in expanded detail instead of crowding the compact source strip');

  const detail = renderDownloadHistoryFixture({
    outcome: 'success',
    created_at: '2026-07-15T00:00:00+00:00',
    materialized_format: 'MP3',
    materialized_min_bitrate: 195,
    materialized_avg_bitrate: 320,
    materialized_median_bitrate: 320,
  });
  assertContains(detail, 'MP3 avg 320kbps · min 195kbps',
    'expanded detail retains the materialized output lineage');
}

console.log('Gas: contract, V0 proof, and materialized Opus output stay distinct');
{
  const strip = renderEvidenceFixture({
    outcome: 'force_import',
    downloaded_label: 'FLAC → OPUS 128',
    source_format: 'FLAC',
    source_min_bitrate: 742,
    source_avg_bitrate: 811,
    source_median_bitrate: 803,
    target_contract_format: 'opus 128',
    slskd_filetype: 'flac',
    actual_filetype: 'opus',
    was_converted: true,
    original_filetype: 'flac',
    actual_min_bitrate: 102,
    materialized_format: 'Opus',
    materialized_min_bitrate: 102,
    materialized_avg_bitrate: 132,
    materialized_median_bitrate: 144,
    spectral_grade: 'genuine',
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_min_bitrate: 191,
    v0_probe_avg_bitrate: 224,
    existing_format: 'MP3',
    existing_min_bitrate: 128,
    existing_spectral_grade: 'suspect',
    existing_spectral_bitrate: 128,
    existing_v0_probe_kind: 'native_lossy_research_v0',
    existing_v0_probe_avg_bitrate: 211,
    comparison_basis: {
      verdict: 'better', branch: 'rank',
      new_rank: 'transparent', existing_rank: 'acceptable',
      new_metric: 'contract', existing_metric: 'avg',
      new_value_kbps: 128, existing_value_kbps: 128,
      new_format: 'opus 128', existing_format: 'mp3',
      spectral_clamped: false, tolerance_kbps: null,
      verified_lossless_bypass: false,
    },
  });
  assertContains(strip, '>FLAC - Opus</span>',
    'IN suffixes the source codec with the selected storage codec');
  assertContains(strip, '>132k avg (min 102k)</span>',
    'IN metric stays numeric so it aligns with HAVE');
  assertExcludes(strip, 'FLAC →', 'collapsed source uses the compact suffix grammar');
  assertExcludes(strip, 'OPUS 128 contract',
    'target contract remains in expanded details instead of the source strip');
  assertContains(strip, '>MP3</span>',
    'HAVE names the pre-import codec');
  assertContains(strip, '>128k avg (min 128k)</span>',
    'HAVE uses the pre-import measurement');
  const outputCount = strip.split('132k avg (min 102k)').length - 1;
  if (outputCount === 1) { passed++; } else {
    failed++;
    console.error(`  FAIL: materialized output belongs only to IN; rendered ${outputCount} times`);
  }
  assertExcludes(strip, '>transparent</span>',
    'compact HAVE never substitutes a decision rank for measured spectral data');
  assertContains(strip, '~128k suspect',
    'HAVE keeps the existing spectral measurement after conversion');
  assertContains(strip, 'V0 224k avg', 'source V0 proof remains explicit');
  assertExcludes(strip, 'OPUS 128 min 191k', 'V0 minimum never wears an Opus label');

  const detail = renderDownloadHistoryFixture({
    outcome: 'force_import',
    soulseek_username: 'Gas-peer',
    created_at: '2026-07-13T01:06:27+00:00',
    downloaded_label: 'FLAC → OPUS 128',
    source_format: 'FLAC',
    source_min_bitrate: 742,
    source_avg_bitrate: 811,
    source_median_bitrate: 803,
    target_contract_format: 'opus 128',
    slskd_filetype: 'flac',
    actual_filetype: 'opus',
    was_converted: true,
    original_filetype: 'flac',
    actual_min_bitrate: 102,
    materialized_format: 'Opus',
    materialized_min_bitrate: 102,
    materialized_avg_bitrate: 132,
    materialized_median_bitrate: 144,
    spectral_grade: 'genuine',
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_min_bitrate: 191,
    v0_probe_avg_bitrate: 224,
    existing_format: 'MP3',
    existing_min_bitrate: 128,
    existing_spectral_grade: 'suspect',
    existing_spectral_bitrate: 128,
    existing_v0_probe_kind: 'native_lossy_research_v0',
    existing_v0_probe_avg_bitrate: 211,
    comparison_basis: {
      verdict: 'better', branch: 'rank',
      new_rank: 'transparent', existing_rank: 'acceptable',
      new_metric: 'contract', existing_metric: 'avg',
      new_value_kbps: 128, existing_value_kbps: 128,
      new_format: 'opus 128', existing_format: 'mp3',
      spectral_clamped: false, tolerance_kbps: null,
      verified_lossless_bypass: false,
    },
    final_format: 'opus 128',
    badge: 'Force imported',
    badge_class: 'badge-force',
    verdict: 'Force imported after manual review',
  });
  assertContains(detail, 'Output', 'detail grid names the materialized side');
  assertContains(detail, 'FLAC avg 811kbps · min 742kbps',
    'detail source uses downloaded source measurements');
  assertContains(detail, 'Target contract', 'detail names target policy separately');
  assertContains(detail, 'OPUS avg 132kbps · min 102kbps',
    'detail output is codec-aware');
  assertContains(detail, 'OPUS 128 contract', 'detail comparison is contract-aware');
  assertExcludes(detail, '>Min bitrate<', 'ambiguous unqualified row is gone');
}

console.log('Amaterasu Shiroi: force import HAVE stays pre-import');
{
  const strip = renderEvidenceFixture({
    outcome: 'force_import',
    source_format: 'FLAC',
    source_min_bitrate: 529,
    source_avg_bitrate: 648,
    source_median_bitrate: 642,
    target_contract_format: 'opus 128',
    was_converted: true,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 96,
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_min_bitrate: 246,
    v0_probe_avg_bitrate: 258,
    materialized_format: 'Opus',
    materialized_min_bitrate: 118,
    materialized_avg_bitrate: 124,
    materialized_median_bitrate: 122,
    existing_format: 'Opus',
    existing_min_bitrate: 90,
    existing_avg_bitrate: 101,
    existing_median_bitrate: 99,
    existing_spectral_grade: 'suspect',
    existing_spectral_bitrate: 80,
    existing_v0_probe_kind: 'lossless_source_v0',
    existing_v0_probe_min_bitrate: 201,
    existing_v0_probe_avg_bitrate: 220,
    comparison_basis: {
      verdict: 'better', branch: 'rank',
      new_rank: 'transparent', existing_rank: 'excellent',
      new_metric: 'contract', existing_metric: 'avg',
      new_value_kbps: 128, existing_value_kbps: 96,
      new_format: 'opus 128', existing_format: 'opus',
      spectral_clamped: true, tolerance_kbps: null,
      verified_lossless_bypass: false,
    },
  });
  assertContains(strip, '>FLAC - Opus</span>',
    'IN keeps the downloaded source and suffixes its storage codec');
  assertContains(strip, '>124k avg (min 118k)</span>',
    'IN metric contains only measured output bytes');
  assertContains(strip, '~96k likely transcode', 'IN keeps source spectral evidence');
  assertContains(strip, 'V0 258k avg (min 246k)', 'IN keeps its source V0 probe');
  assertContains(strip, '>OPUS</span>', 'HAVE names the pre-import copy');
  assertContains(strip, '>101k avg (min 90k)</span>',
    'HAVE is populated from pre-import bytes');
  assertExcludes(strip, '>transparent</span>',
    'HAVE does not substitute the decision rank for spectral data');
  assertExcludes(strip, '>excellent</span>', 'decision rank stays out of compact HAVE');
  // One cell renders the phrase twice (full + compact span); a leak into
  // HAVE would double that to 4.
  const spectralCount = strip.split('~96k likely transcode').length - 1;
  if (spectralCount === 2) { passed++; } else {
    failed++;
    console.error(`  FAIL: candidate spectral belongs only to IN; rendered ${spectralCount} spans, expected 2`);
  }
  const v0Count = strip.split('V0 258k avg (min 246k)').length - 1;
  if (v0Count === 1) { passed++; } else {
    failed++;
    console.error(`  FAIL: candidate V0 belongs only to IN; rendered ${v0Count} times`);
  }
  assertContains(strip, '~80k suspect', 'HAVE keeps its own spectral snapshot');
  assertContains(strip, 'V0 220k avg (min 201k)', 'HAVE keeps its own V0 snapshot');
}

console.log('Absentee Schmotime: an upgrade keeps the pre-import copy in HAVE');
{
  // Live issue #709 regression: the converted output belongs to IN, while
  // HAVE remains the MP3 snapshot which the upgrade decision replaced.
  const strip = renderEvidenceFixture({
    outcome: 'success',
    badge: 'Upgraded',
    source_format: 'FLAC',
    source_min_bitrate: 863,
    source_avg_bitrate: 967,
    source_median_bitrate: 950,
    target_contract_format: 'opus 128',
    was_converted: true,
    spectral_grade: 'genuine',
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_min_bitrate: 258,
    v0_probe_avg_bitrate: 268,
    materialized_format: 'Opus',
    materialized_min_bitrate: 127,
    materialized_avg_bitrate: 136,
    materialized_median_bitrate: 134,
    existing_format: 'MP3',
    existing_min_bitrate: 320,
    existing_avg_bitrate: 320,
    existing_median_bitrate: 320,
    existing_spectral_grade: 'genuine',
    existing_v0_probe_kind: 'native_lossy_research_v0',
    existing_v0_probe_min_bitrate: 258,
    existing_v0_probe_avg_bitrate: 268,
    comparison_basis: {
      verdict: 'equivalent', branch: 'cross_family_same_rank',
      new_rank: 'transparent', existing_rank: 'transparent',
      new_metric: 'contract', existing_metric: 'avg',
      new_value_kbps: 128, existing_value_kbps: 320,
      new_format: 'opus 128', existing_format: 'mp3',
      spectral_clamped: false, tolerance_kbps: null,
      verified_lossless_bypass: true,
    },
  });
  assertContains(strip, '>FLAC - Opus</span>', 'IN names the converted source');
  assertContains(strip, '>136k avg (min 127k)</span>', 'IN uses measured Opus output');
  assertContains(strip, '>MP3</span>', 'HAVE keeps the pre-import codec');
  assertContains(strip, '>320k avg (min 320k)</span>',
    'HAVE keeps the pre-import bitrate snapshot');
  assertExcludes(strip, '>transparent</span>',
    'compact upgrade leaves the internal rank in expanded detail');
  const incomingMetricCount = strip.split('136k avg (min 127k)').length - 1;
  if (incomingMetricCount === 1) { passed++; } else {
    failed++;
    console.error(`  FAIL: incoming output must appear once; rendered ${incomingMetricCount} times`);
  }
}

console.log('every attempted import keeps materialized IN separate from historical HAVE');
{
  for (const [outcome, badge] of [
    ['success', 'Upgraded'],
    ['success', 'Provisional'],
    ['force_import', 'Force imported'],
    ['manual_import', 'Imported'],
  ]) {
    for (const storage of ['Opus', 'MP3']) {
      for (const existing of ['MP3', 'AAC', 'Opus']) {
        for (const offset of [0, 37, 149]) {
        const incomingAvg = 121 + offset;
        const incomingMin = 101 + offset;
        const existingAvg = 211 + offset;
        const existingMin = 191 + offset;
        const strip = renderEvidenceFixture({
          outcome, badge, was_converted: true,
          source_format: 'FLAC', target_contract_format: storage === 'Opus'
            ? 'opus 128' : 'mp3 v0',
          materialized_format: storage,
          materialized_avg_bitrate: incomingAvg,
          materialized_min_bitrate: incomingMin,
          existing_format: existing,
          existing_avg_bitrate: existingAvg,
          existing_min_bitrate: existingMin,
        });
        assertContains(strip, `>${existing}</span>`,
          'generated upgrade HAVE keeps its pre-import codec');
        assertContains(strip, `>${existingAvg}k avg (min ${existingMin}k)</span>`,
          'generated upgrade HAVE keeps its pre-import measurements');
        const incomingCount = strip.split(`${incomingAvg}k avg (min ${incomingMin}k)`).length - 1;
        if (incomingCount === 1) { passed++; } else {
          failed++;
          console.error(`  FAIL: attempted output bled into HAVE (${outcome}/${storage}/${existing}/${offset})`);
        }
        }
      }
    }
  }
}

console.log('Forty Days: provisional HAVE stays the comparable on-disk copy');
{
  // Live issue #709 regression: a provisional candidate has a materialized
  // output measurement, but HAVE must remain the pre-attempt library snapshot
  // used by the decision so every IN field compares top-to-bottom.
  const strip = renderEvidenceFixture({
    outcome: 'success',
    badge: 'Provisional',
    source_format: 'FLAC',
    source_min_bitrate: 485,
    source_avg_bitrate: 600,
    source_median_bitrate: 618,
    target_contract_format: 'opus 128',
    was_converted: true,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 96,
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_min_bitrate: 200,
    v0_probe_avg_bitrate: 223,
    existing_format: 'Opus',
    existing_min_bitrate: 103,
    existing_avg_bitrate: 113,
    existing_median_bitrate: 114,
    existing_spectral_grade: 'likely_transcode',
    existing_spectral_bitrate: 96,
    existing_v0_probe_kind: 'lossless_source_v0',
    existing_v0_probe_min_bitrate: 173,
    existing_v0_probe_avg_bitrate: 207,
    materialized_format: 'Opus',
    materialized_min_bitrate: 106,
    materialized_avg_bitrate: 122,
  });
  assertContains(strip, '>FLAC - Opus</span>',
    'IN names the provisional source and selected storage codec');
  assertContains(strip, '>122k avg (min 106k)</span>',
    'IN metric contains only the measured provisional result');
  assertContains(strip, 'V0 223k avg (min 200k)', 'IN keeps source V0 evidence');
  assertContains(strip, '>Opus</span>', 'HAVE names the comparable library copy');
  assertContains(strip, '>113k avg (min 103k)</span>',
    'HAVE reports average and minimum for the pre-attempt copy');
  assertContains(strip, '~96k likely transcode', 'HAVE keeps spectral evidence');
  assertContains(strip, 'V0 207k avg (min 173k)', 'HAVE keeps its V0 probe');
  assertExcludes(strip, 'avg 122k (min 106k)',
    'candidate output does not replace provisional comparison evidence');
}

console.log('Actual Life 3: current canonical evidence fully populates triage HAVE');
{
  const strip = renderEvidenceFixture({
    outcome: 'rejected',
    badge: 'Triaged · deleted',
    source_format: 'FLAC',
    source_min_bitrate: 455,
    source_avg_bitrate: 725,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 96,
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_min_bitrate: 178,
    v0_probe_avg_bitrate: 248,
    existing_format: 'Opus',
    existing_min_bitrate: 93,
    existing_avg_bitrate: 129,
    existing_spectral_grade: 'suspect',
    existing_spectral_bitrate: 96,
    existing_v0_probe_kind: 'lossless_source_v0',
    existing_v0_probe_min_bitrate: 193,
    existing_v0_probe_avg_bitrate: 256,
  });
  assertContains(strip, '>FLAC</span>', 'retained lossless keeps the bare source label');
  assertContains(strip, '>725k avg (min 455k)</span>',
    'retained lossless metric reports average plus minimum');
  assertContains(strip, '>Opus</span>', 'triage HAVE names the current copy');
  assertContains(strip, '>129k avg (min 93k)</span>',
    'triage HAVE reports average plus minimum');
  assertContains(strip, '~96k suspect', 'triage HAVE keeps current spectral evidence');
  assertContains(strip, 'V0 256k avg (min 193k)',
    'triage HAVE keeps the canonical current V0 probe');
}

console.log('lossless storage labels distinguish V0 from retained FLAC');
{
  const v0 = renderEvidenceFixture({
    outcome: 'success', badge: 'Imported', was_converted: true,
    source_format: 'FLAC', source_min_bitrate: 600, source_avg_bitrate: 800,
    target_contract_format: 'mp3 v0',
    materialized_format: 'MP3', materialized_min_bitrate: 220,
    materialized_avg_bitrate: 245,
  });
  assertContains(v0, '>FLAC - V0</span>',
    'V0 target is suffixed to the lossless source label');
  assertContains(v0, '>245k avg (min 220k)</span>',
    'V0 target leaves the metric column numeric');
  const newImportMetricCount = v0.split('245k avg (min 220k)').length - 1;
  if (newImportMetricCount === 1) { passed++; } else {
    failed++;
    console.error(`  FAIL: first import must leave HAVE empty; output rendered ${newImportMetricCount} times`);
  }
  assertContains(v0, '>—</span>',
    'first import makes the absent pre-import HAVE explicit');

  const flac = renderEvidenceFixture({
    outcome: 'success', badge: 'New', was_converted: false,
    source_format: 'FLAC', source_min_bitrate: 455, source_avg_bitrate: 725,
  });
  assertContains(flac, '>FLAC</span>', 'retained FLAC target keeps the bare codec label');
  assertContains(flac, '>725k avg (min 455k)</span>',
    'retained FLAC metric contains only its actual bytes');
}

console.log('Iron & Wine: the temporary V0 minimum never wears the FLAC label');
{
  const strip = renderEvidenceFixture({
    outcome: 'rejected',
    downloaded_label: 'FLAC',
    filetype: 'flac',
    slskd_filetype: 'flac',
    actual_filetype: 'flac',
    actual_min_bitrate: 165,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 96,
    v0_probe_kind: 'lossless_source_v0',
    legacy_projection_version: 2,
    v0_probe_min_bitrate: 165,
    v0_probe_avg_bitrate: 171,
    existing_format: 'Opus',
    existing_min_bitrate: 114,
    existing_spectral_grade: 'likely_transcode',
    existing_v0_probe_kind: 'lossless_source_v0',
    existing_v0_probe_min_bitrate: 223,
    existing_v0_probe_avg_bitrate: 232,
  });
  assertContains(strip, '>FLAC</span>', 'source remains labelled FLAC');
  assertExcludes(strip, '>min 165k</span>', 'V0 minimum is not a FLAC measurement');
  assertContains(strip, 'V0 171k avg (min 165k)', 'candidate V0 owns its minimum');
  assertContains(strip, '>Opus</span>', 'pre-attempt existing Opus keeps its codec slot');
  assertContains(strip, '>min 114k</span>', 'pre-attempt existing Opus keeps its real floor');
  assertContains(strip, 'V0 232k avg (min 223k)', 'existing source V0 owns its minimum');

  const detail = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'donfulci',
    created_at: '2026-07-13T01:01:00+00:00',
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_min_bitrate: 165,
    v0_probe_avg_bitrate: 171,
    existing_v0_probe_kind: 'lossless_source_v0',
    existing_v0_probe_min_bitrate: 223,
    existing_v0_probe_avg_bitrate: 232,
    verdict: 'Suspect lossless source not better than on-disk copy; searching continues',
  });
  assertContains(detail, '171kbps avg · min 165kbps',
    'detail candidate V0 owns its minimum');
  assertContains(detail, '232kbps avg · min 223kbps',
    'detail existing V0 owns its minimum');
}

console.log('evidence strip CSS keeps desktop alignment and gives mobile readable aligned one-line rows');
{
  const css = readFileSync(new URL('../web/index.html', import.meta.url), 'utf8');
  assertContains(css, '.r-ev-row { display: contents; }',
    'desktop row wrappers participate in the parent grid instead of defining independent columns');
  assertContains(css, 'grid-template-columns: 3.6em minmax(4.5em, 0.8fr) minmax(12em, 1.7fr) minmax(7.5em, 1fr) minmax(9em, 1.35fr)',
    'desktop reserves aligned tag/source/metric/spectral/V0 columns');
  assertContains(css, '@media (max-width: 720px)', 'shared grid has a narrow-screen layout');
  assertContains(css, '.r-evidence { grid-template-columns: 2.9em 3.2em minmax(8.5em, max-content) minmax(3em, 1fr) max-content; column-gap: 0.45em; font-size: 12px;',
    'mobile fixes tag+source and floors the bitrate column at a labelled-pair width, so every metric cell (CBR pairs included) keeps aligned column edges');
  assertContains(css, 'font-family: system-ui,',
    'mobile uses the narrow system font so full lines fit without squeezing');
  assertContains(css, '.r-ev-cell { overflow: hidden; text-overflow: ellipsis; }',
    'squeezed cells drop end characters instead of wrapping');
  assertContains(css, '.r-ev-full { display: none; } .r-ev-compact { display: inline; }',
    'mobile swaps in the spelled-out compact wording');
  assertContains(css, '.r-ev-compact { display: none; }',
    'desktop keeps the full evidence wording');
  assertExcludes(css, 'clamp(9px', 'mobile never shrinks evidence below readable size');
  assertExcludes(css, 'grid-template-columns: max-content max-content max-content max-content max-content max-content;',
    'the six-column mobile crush cannot return');
  assertExcludes(css, 'column-gap: 1px', 'the 1px column crush cannot return');
  assertExcludes(css, 'grid-template-rows: auto auto auto;',
    'mobile does not spend three physical rows on each evidence side');
  assertContains(css, '.r-evidence .r-ev-tag { color: #d3deea; font-weight: 900; font-size: 1.08em;',
    'IN/HAVE labels are visibly prominent');
  assertContains(css, '@media (min-width: 721px) { .r-ev-v0 { padding-left: 1em; } }',
    'desktop V0 keeps a visible gutter after long spectral labels');
  assertContains(css, '.recents-triage-label { color: #d66; font-weight: 600; }',
    'secondary triage annotations stay rejection-coloured');
  assertExcludes(css, '.r-ev-cell { min-width: 0; overflow-wrap: anywhere;',
    'evidence tokens never use arbitrary mid-word wrapping');
  assertExcludes(css, 'repeat(5, minmax(0, 1fr))',
    'mobile evidence never collapses every field into equal tiny columns');
  assertExcludes(css, 'grid-template-columns: 2.8em minmax(3.4em',
    'the overlapping five-column mobile layout cannot return');
  assertExcludes(css, 'minmax(4.5em, 0.75fr)',
    'desktop does not reserve a track for the permanently empty rank slot');
  assertExcludes(css, 'minmax(8.5em, max-content) 0 minmax(3em, 1fr)',
    'mobile does not retain a zero-width track for the permanently empty rank slot');
}

console.log('renderEvidenceStrip() marks spectral-clamped rank values with ~');
{
  const strip = renderEvidenceFixture({
    actual_min_bitrate: 194,
    comparison_basis: {
      verdict: 'better', branch: 'rank',
      new_rank: 'transparent', existing_rank: 'good',
      new_metric: 'avg', existing_metric: 'avg',
      new_value_kbps: 250, existing_value_kbps: 196,
      new_format: 'MP3', existing_format: 'MP3',
      spectral_clamped: true, tolerance_kbps: null,
      verified_lossless_bypass: false,
    },
  });
  assertContains(strip, '~250k', 'clamped value gets the ~ prefix, no metric label');
  assertExcludes(strip, 'avg 250k', 'clamped value must not claim a metric');
}

console.log('renderEvidenceStrip() escapes basis strings');
{
  const strip = renderEvidenceFixture({
    actual_min_bitrate: 194,
    comparison_basis: {
      verdict: 'better', branch: 'rank',
      new_rank: '<b>x</b>', existing_rank: 'good',
      new_metric: 'avg', existing_metric: 'avg',
      new_value_kbps: 288, existing_value_kbps: 196,
      new_format: '<img src=x>', existing_format: 'MP3',
      spectral_clamped: false, tolerance_kbps: null,
      verified_lossless_bypass: false,
    },
  });
  assertExcludes(strip, '<img src=x>', 'raw basis format not rendered');
  assertExcludes(strip, '<b>x</b>', 'raw basis rank not rendered');
}

console.log('renderDownloadHistoryItem() renders a Compared row from the basis');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'dbqs',
    created_at: '2026-07-10T14:46:05+00:00',
    actual_min_bitrate: 194,
    existing_min_bitrate: 194,
    beets_distance: 0.0899,
    comparison_basis: {
      verdict: 'better', branch: 'rank',
      new_rank: 'transparent', existing_rank: 'good',
      new_metric: 'avg', existing_metric: 'avg',
      new_value_kbps: 288, existing_value_kbps: 196,
      new_format: 'MP3', existing_format: 'MP3',
      spectral_clamped: false, tolerance_kbps: null,
      verified_lossless_bypass: false,
    },
  });
  assertContains(html, 'Compared', 'Compared label rendered');
  assertContains(html, 'MP3 avg 288k · transparent', 'new side with rank');
  assertContains(html, 'MP3 avg 196k · good', 'existing side with rank');
}

console.log('renderDownloadHistoryItem() Compared row notes the verified-lossless bypass');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'dbqs',
    created_at: '2026-07-10T14:46:05+00:00',
    comparison_basis: {
      verdict: 'equivalent', branch: 'metric_tiebreak',
      new_rank: 'transparent', existing_rank: 'transparent',
      new_metric: 'avg', existing_metric: 'avg',
      new_value_kbps: 250, existing_value_kbps: 248,
      new_format: 'MP3', existing_format: 'MP3',
      spectral_clamped: false, tolerance_kbps: 5,
      verified_lossless_bypass: true,
    },
  });
  assertContains(html, 'verified lossless bypass', 'bypass annotated');
}

console.log('renderDownloadHistoryItem() omits the Compared row without a basis');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'dbqs',
    created_at: '2026-07-10T14:46:05+00:00',
    actual_min_bitrate: 194,
  });
  assertExcludes(html, 'Compared', 'no Compared row on legacy rows');
}

console.log('renderDownloadHistoryItem() leads with the verdict, red on rejections');
{
  // Request 8781 / download_log 36660: a Rejected row whose quality
  // evidence all read positive (transparent vs transparent, verified
  // lossless bypass) buried the actual rejection reason (mbid_missing)
  // as a dim line BELOW the grid — the detail view told a quality story
  // for a match failure. The verdict now renders directly under the
  // header, before the evidence grid, in the reject colour.
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    badge: 'Rejected',
    badge_class: 'badge-rejected',
    soulseek_username: 'tunnik',
    created_at: '2026-07-10T23:19:10+00:00',
    downloaded_label: 'WAV → OPUS 128',
    spectral_grade: 'genuine',
    verdict: 'mbid_missing',
    comparison_basis: {
      verdict: 'equivalent', branch: 'cross_family_same_rank',
      new_rank: 'transparent', existing_rank: 'transparent',
      new_metric: 'contract', existing_metric: 'avg',
      new_value_kbps: 128, existing_value_kbps: 256,
      new_format: 'opus 128', existing_format: 'aac',
      spectral_clamped: false, tolerance_kbps: null,
      verified_lossless_bypass: true,
    },
  });

  assertContains(html, 'p-hist-verdict-reject', 'rejected verdict gets the reject class');
  const verdictPos = html.indexOf('mbid_missing');
  const gridPos = html.indexOf('p-hist-grid');
  if (verdictPos !== -1 && gridPos !== -1 && verdictPos < gridPos) {
    passed++;
  } else {
    failed++;
    console.error('  FAIL: rejection verdict should render before the evidence grid');
  }
}

console.log('renderDownloadHistoryItem() colors the verdict red across the failure family');
{
  for (const outcome of [
    'rejected', 'failed', 'timeout', 'measurement_failed', 'user_offline', 'curator_ban',
  ]) {
    const html = renderDownloadHistoryFixture({
      outcome,
      soulseek_username: 'testuser',
      created_at: '2026-07-10T23:19:10+00:00',
      verdict: 'some failure story',
    });
    assertContains(html, 'p-hist-verdict-reject', `${outcome} verdict gets the reject class`);
  }
}

console.log('renderDownloadHistoryItem() keeps success verdicts unstyled and above the grid');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'dbqs',
    created_at: '2026-07-10T14:46:05+00:00',
    actual_min_bitrate: 194,
    verdict: 'Upgrade: MP3 V2 to MP3 320',
  });
  assertContains(html, 'p-hist-verdict', 'verdict line present on success rows');
  assertExcludes(html, 'p-hist-verdict-reject', 'success verdict keeps the default colour');
  const verdictPos = html.indexOf('Upgrade: MP3 V2 to MP3 320');
  const gridPos = html.indexOf('p-hist-grid');
  if (verdictPos !== -1 && gridPos !== -1 && verdictPos < gridPos) {
    passed++;
  } else {
    failed++;
    console.error('  FAIL: success verdict should also render before the grid');
  }
}

console.log('renderDownloadHistoryItem() surfaces beets_detail behind the forensics toggle');
{
  // mbid_not_found rows carry the explanation ("Target MBID X not in
  // candidates") in beets_detail — previously dropped on the floor.
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'tunnik',
    created_at: '2026-07-10T22:28:12+00:00',
    verdict: 'mbid_not_found',
    beets_detail: 'Target MBID 3de1b986-1b7d-4769-ba9a-5d2b398d0331 not in candidates',
  });
  assertContains(html, '<details class="p-hist-forensics">',
    'forensics toggle present when beets_detail exists');
  assertContains(html, 'Target MBID 3de1b986-1b7d-4769-ba9a-5d2b398d0331 not in candidates',
    'beets_detail reachable in forensics');
  const detailsStart = html.indexOf('<details');
  const detailPos = html.indexOf('Target MBID');
  if (detailsStart !== -1 && detailPos > detailsStart) {
    passed++;
  } else {
    failed++;
    console.error('  FAIL: beets_detail should live inside the forensics toggle');
  }
}

console.log('renderDownloadHistoryItem() omits the forensics Detail row when beets_detail repeats the verdict');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-07-10T22:28:12+00:00',
    verdict: 'audio_corrupt',
    beets_detail: 'audio_corrupt',
  });
  assertExcludes(html, '<details class="p-hist-forensics">',
    'no forensics toggle for a redundant beets_detail');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
