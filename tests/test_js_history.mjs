/**
 * Unit tests for web/js/history.js download-history rendering.
 * Run with: node tests/test_js_history.mjs
 */

import {
  renderDownloadHistoryItem as renderDownloadHistoryFixture,
  renderEvidenceStrip as renderEvidenceFixture,
  __test__,
} from '../web/js/history.js';
import { validDualProviderProof } from './fixtures/cd_rip_proof.mjs';
import { esc } from '../web/js/util.js';
import { readFileSync } from 'node:fs';

import { suite } from './js_harness.mjs';
const {
  formatV0Probe, formatSpectral, spectralChip, spectralGradeIsAdmissible,
  spectralStripCell, withWas, storageFormatLabel,
} = __test__;

const t = suite(import.meta.url);

t.section('storageFormatLabel() preserves native codec names');
{
  t.contains(
    storageFormatLabel({ materialized_format: 'vorbis' }, ''),
    'Vorbis',
    'Vorbis is not rendered as the Ogg container or all-caps metadata',
  );
  t.contains(
    storageFormatLabel({ materialized_format: 'wma' }, ''),
    'WMA',
    'WMA keeps its native acronym',
  );
}

t.section('renderDownloadHistoryItem() shows wrong-match triage audit rows');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'moundsofass',
    created_at: '2026-04-25T23:25:00+00:00',
    beets_distance: 0.190,
    verdict: 'Wrong match (dist 0.190)',
    wrong_match_triage_summary: 'download deleted: spectral reject',
    wrong_match_triage_action: 'deleted_reject',
    wrong_match_triage_preview_verdict: 'confident_reject',
    wrong_match_triage_preview_decision: 'requeue_upgrade',
    wrong_match_triage_reason: 'requeue_upgrade',
    wrong_match_triage_stage_chain: ['mp3_spectral:reject'],
  });

  t.contains(html, 'Triage', 'triage summary label rendered');
  t.contains(html, 'download deleted: spectral reject', 'triage summary rendered');
  t.contains(html, 'Preview', 'preview label rendered');
  t.contains(html, 'confident_reject / requeue_upgrade',
    'preview verdict and decision rendered');
  t.contains(html, 'mp3_spectral:reject', 'stage chain rendered');
  t.contains(html, 'Wrong match (dist 0.190)',
    'original verdict remains visible');
}

t.section('renderDownloadHistoryItem() omits empty triage rows');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    downloaded_label: 'MP3 320',
    verdict: 'MP3 320',
  });

  t.excludes(html, 'Triage', 'no triage label without audit');
  t.excludes(html, 'Preview', 'no preview label without audit');
  t.excludes(html, 'Stages', 'no stages label without audit');
}

t.section('renderDownloadHistoryItem() shows the track-length warning row (issue #1178)');
{
  const warning = "Track length contradicts the matched release: "
    + "'00 - Hidden Track.flac' is 237.6s where the release declares "
    + "15.0s for 'Lost Weekend'";
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'lwl',
    created_at: '2026-08-15T10:25:15+00:00',
    downloaded_label: 'FLAC',
    verdict: 'FLAC',
    track_length_warning: warning,
  });

  t.contains(html, '<span class="p-hist-label">Track length</span>',
    'track-length row label rendered');
  t.contains(html, `color:#ec6;">${esc(warning)}</span>`,
    'track-length row uses the same amber warning styling as Bad '
    + 'extension/Triage, with the full sentence as the value');
}

t.section('renderDownloadHistoryItem() omits the track-length row when the field is null');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    downloaded_label: 'MP3 320',
    verdict: 'MP3 320',
  });

  t.excludes(html, 'Track length', 'no track-length row without a warning');
}

t.section('renderDownloadHistoryItem() escapes wrong-match triage audit values');
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

  t.contains(html, '&lt;img src=x&gt;', 'triage summary escaped');
  t.contains(html, 'confident&lt;script&gt;', 'preview verdict escaped');
  t.contains(html, 'mp3_spectral:&lt;reject&gt;', 'stage chain escaped');
  t.excludes(html, '<img src=x>', 'raw summary not rendered');
  t.excludes(html, 'confident<script>', 'raw preview not rendered');
}

t.section('renderDownloadHistoryItem() refuses to infer output from legacy bitrate columns');
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
  t.contains(html, 'class="p-hist-grid"',
    'one consistent grid renders for every entry');
  t.contains(html, 'class="p-hist-label">Output</span>',
    'Output row label present');
  t.contains(html, 'class="p-hist-label">Spectral</span>',
    'Spectral row label present');
  t.excludes(html, 'class="p-hist-value">192kbps',
    'legacy candidate minimum is not relabelled as materialized output');
  t.contains(html, '~160kbps', 'candidate spectral floor rendered');
  t.contains(html, 'suspect (~96kbps)',
    'existing spectral grade and floor appear on the spectral row');
}

t.section('renderDownloadHistoryItem() omits the (was X) suffix when no existing data');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    actual_min_bitrate: 192,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 160,
  });

  t.excludes(html, 'class="p-hist-value">192kbps',
    'candidate bitrate is not relabelled as output');
  t.excludes(html, '(was', 'no (was) suffix when existing data absent');
}

t.section('two-sided spectral failures remain distinct from legacy unmeasured rows');
{
  const failedHtml = renderDownloadHistoryFixture({
    outcome: 'rejected', created_at: '2026-07-12T00:00:00+00:00',
    spectral_attempted: true,
    spectral_error: 'RuntimeError: decode failed',
    existing_spectral_attempted: true, existing_spectral_grade: 'genuine',
  });
  t.contains(failedHtml, 'analysis failed', 'attempted failure is explicit');
  t.contains(failedHtml, 'RuntimeError: decode failed', 'failure detail is available');
  t.contains(failedHtml, '<details class="p-hist-forensics">',
    'spectral errors are reachable in focusable forensics');
  t.contains(failedHtml, 'Spectral IN error',
    'candidate error has a labelled forensic row');
  const strip = renderEvidenceFixture({
    spectral_attempted: true, spectral_error: 'candidate failed',
    existing_spectral_attempted: true, existing_spectral_error: 'existing failed',
  });
  t.contains(strip, 'IN', 'failure-only audit still renders in Recents');
  t.contains(strip, 'spectral failed', 'Recents keeps failure state compact');
  const legacyHtml = renderDownloadHistoryFixture({
    outcome: 'rejected', created_at: '2026-07-12T00:00:00+00:00',
  });
  t.excludes(legacyHtml, 'analysis failed', 'legacy row stays unmeasured');
}

t.section('renderDownloadHistoryItem() surfaces HAVE analysis diagnostics');
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
  t.contains(html, 'Environment failure', 'environment badge rendered');
  t.contains(html, 'Failure category', 'failure category label rendered');
  t.contains(html, 'permission denied', 'failure category humanized');
  t.contains(html, 'Installed HAVE', 'installed path label rendered');
  t.contains(html, '/mnt/Music/Beets/Low/&lt;current&gt;', 'installed path escaped');
  t.contains(html, 'Candidate', 'candidate reference label rendered');
  t.contains(html, '/mnt/Music/Incoming/candidate&amp;next', 'candidate reference escaped');
  t.contains(html, 'PermissionError: &lt;denied&gt;', 'analysis error escaped');
  t.contains(html, 'remains wanted', 'retryable state remains prominent');
  t.excludes(html, 'PermissionError: <denied>', 'raw analysis error not rendered');
}

t.section('legacy existing floor-only Recents labels the missing grade');
{
  const strip = renderEvidenceFixture({ existing_spectral_bitrate: 128 });
  t.contains(strip, 'ungraded (~128k)',
    'legacy HAVE floor cannot read like a complete spectral grade');
}

t.section('renderDownloadHistoryItem() labels both V0 probe sides explicitly');
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

  t.contains(html, 'class="p-hist-label">V0 probe</span>',
    'V0 probe row present for lossless source');
  t.contains(html, '>IN</span> 228kbps avg',
    'candidate V0 probe avg renders on the labelled IN side');
  t.contains(html, '>HAVE</span> 171kbps avg',
    'existing V0 probe renders on the labelled HAVE side');
  t.contains(html, 'Stored as', 'final format label rendered');
  t.contains(html, 'OPUS 128 contract', 'final format rendered as contract');
  t.excludes(html, '(lossless_source_v0)',
    'lossless probe omits the noisy kind suffix');
}

t.section('renderDownloadHistoryItem() leaves HAVE empty without a comparable V0 probe');
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

  t.contains(html, 'class="p-hist-label">V0 probe</span>',
    'V0 probe row present');
  t.contains(html, '>IN</span> 260kbps avg',
    'candidate V0 probe remains on the IN side');
  t.contains(html, '>HAVE</span> —',
    'missing existing probe is explicit without borrowing its raw minimum');
  t.excludes(html, 'class="p-hist-was">(was 192kbps)',
    'legacy minimums are not projected as materialized output');
}

t.section('renderDownloadHistoryItem() renders the V0 probe row for research probes too');
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

  t.contains(html, 'V0 probe',
    'V0 probe row renders for research probes');
  t.contains(html, '247kbps avg (from lossy)',
    'research probe carries the from-lossy qualifier');
  t.excludes(html, 'class="p-hist-value">232kbps',
    'research candidate minimum is not relabelled as output');
}

t.section('renderDownloadHistoryItem() V0 side labels retain kind provenance');
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
  t.contains(html, '255kbps avg', 'lossless-source probe renders bare');
  t.contains(html, '>HAVE</span> 250kbps avg (from lossy)',
    'existing research probe renders with its qualifier on HAVE');
}

t.section('renderDownloadHistoryItem() renders HAVE-only V0 provenance');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    created_at: '2026-07-15T00:00:00+00:00',
    existing_v0_probe_kind: 'on_disk_research_v0',
    existing_v0_probe_min_bitrate: 201,
    existing_v0_probe_avg_bitrate: 259,
  });
  t.contains(html, 'class="p-hist-label">V0 probe</span>',
    'HAVE-only evidence still creates the expanded V0 row');
  t.contains(html, '>IN</span> —', 'missing candidate probe is explicit');
  t.contains(html, '>HAVE</span> 259kbps avg · min 201kbps (on-disk re-encode)',
    'HAVE-only probe retains its detailed provenance');
}

t.section('renderDownloadHistoryItem() keeps a consistent row vocabulary across codecs');
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
    t.contains(html, 'class="p-hist-label">Source</span>',
      'Source row in every entry');
    t.contains(html, 'class="p-hist-label">Spectral</span>',
      'Spectral row in every entry');
    t.contains(html, 'class="p-hist-label">Output</span>',
      'Output row in every entry');
  }
}

t.section('withWas() helper appends the existing comparison inline');
{
  t.equal(withWas('100kbps', '90kbps'),
    '100kbps <span class="p-hist-was">(was 90kbps)</span>',
    'withWas should append (was Y) inline');
  t.equal(withWas('100kbps', null), '100kbps',
    'withWas should return bare value when wasValue is null');
  t.equal(withWas('100kbps', undefined), '100kbps',
    'withWas should return bare value when wasValue is undefined');
}

t.section('formatSpectral() helper colors grades and prefixes the floor');
{
  t.contains(formatSpectral('genuine'), 'quality-tone-lossless',
    'genuine should use the brightest shared green');
  t.contains(formatSpectral('marginal'), 'quality-tone-good',
    'marginal should use the shared yellow tone');
  t.contains(formatSpectral('suspect'), 'quality-tone-acceptable',
    'suspect should use the shared orange tone');
  t.contains(formatSpectral('likely_transcode'), 'quality-tone-poor',
    'likely transcode should use the shared red tone');
  t.contains(formatSpectral('genuine', 96), '~96kbps',
    'spectral with floor should show ~96kbps');
  t.excludes(formatSpectral('genuine'), '~',
    'spectral without floor should not show ~');
  t.contains(formatSpectral('likely_transcode', 160), 'likely transcode (~160kbps)',
    'spectral grade tokens should be humanized');
}

t.section('renderEvidenceStrip() humanizes spectral tokens on both sides');
{
  const html = renderEvidenceFixture({
    spectral_grade: 'likely_transcode', spectral_bitrate: 160,
    existing_spectral_grade: 'likely_transcode', existing_spectral_bitrate: 128,
  });
  t.contains(html, 'likely transcode', 'humanized grade rendered');
  t.excludes(html, 'likely_transcode', 'raw grade token never leaks');
}

t.section('formatV0Probe() helper picks the right kind suffix per source lineage');
{
  t.equal(formatV0Probe(260, 'lossless_source_v0'), '260kbps avg',
    'lossless probe should render bare ("260kbps avg")');
  // ``native_lossy_research_v0`` is a real ffmpeg V0-transcode probe of a
  // lossy source — qualified "(from lossy)" so it never reads as the
  // gold-standard lossless-source probe.
  t.equal(formatV0Probe(247, 'native_lossy_research_v0'), '247kbps avg (from lossy)',
    'native_lossy_research_v0 should add "(from lossy)" suffix');
  t.equal(formatV0Probe(200, undefined), '200kbps avg',
    'missing kind should render bare');
  t.equal(formatV0Probe(180, 'on_disk_research_v0'), '180kbps avg (on-disk re-encode)',
    'on_disk_research_v0 should render the on-disk re-encode qualifier');
  t.equal(formatV0Probe(180, 'future_probe_kind'), '180kbps avg (future_probe_kind)',
    'unknown kind should fall back to raw label');
}

t.section('renderDownloadHistoryItem() shows "overridden" instead of the fake 0.000 distance on force imports');
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

  t.contains(html, 'class="p-hist-label">Distance</span>',
    'Distance row present on force imports');
  t.contains(html, 'overridden', 'force-import distance reads overridden');
  t.contains(html, '(was 0.233)', 'force-import distance retains its origin measurement');
  t.excludes(html, '0.000', 'the fake beets 0.000 never renders');
}

t.section('renderDownloadHistoryItem() always renders the core row vocabulary with em-dash placeholders');
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
    t.contains(html, `class="p-hist-label">${label}</span>`,
      `${label} row present even without data`);
  }
  t.contains(html, '—', 'unknown cells render an em-dash');
}

t.section('renderDownloadHistoryItem() header uses the server badge vocabulary');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'timeout',
    badge: 'Failed',
    badge_class: 'badge-failed',
    soulseek_username: 'testuser',
    created_at: '2026-07-07T21:22:00+00:00',
  });

  t.contains(html, 'badge badge-failed', 'server badge class on header');
  t.contains(html, '>Failed<', 'server badge label on header');
  // The raw outcome word must not appear as the status any more — the
  // list rows say "Failed", the detail block must not say "timeout".
  t.excludes(html, '>timeout<', 'raw outcome word no longer the header status');
}

t.section('renderDownloadHistoryItem() header falls back to outcome when badge fields absent');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-07-07T21:22:00+00:00',
  });
  t.contains(html, '>rejected<', 'outcome fallback when classifier fields missing');
}

t.section('renderDownloadHistoryItem() tucks debug forensics behind a details toggle');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-04-25T23:25:00+00:00',
    verdict: 'Wrong match (dist 0.190)',
    wrong_match_triage_summary: 'download deleted: spectral reject',
    wrong_match_triage_preview_verdict: 'confident_reject',
    wrong_match_triage_preview_decision: 'requeue_upgrade',
    wrong_match_triage_reason: 'requeue_upgrade',
    wrong_match_triage_stage_chain: ['mp3_spectral:reject'],
  });

  t.contains(html, '<details class="p-hist-forensics">',
    'forensics details element present');
  t.contains(html, 'mp3_spectral:reject', 'stage chain still reachable');
  // Triage (the operator-action audit) stays visible outside the toggle.
  const detailsStart = html.indexOf('<details');
  const triagePos = html.indexOf('deleted: spectral reject');
  t.ok(triagePos !== -1 && detailsStart !== -1 && triagePos < detailsStart,
    'triage summary should render before/outside the forensics toggle');
  const stagesPos = html.indexOf('mp3_spectral:reject');
  t.ok(stagesPos > detailsStart && detailsStart !== -1,
    'stage chain should live inside the forensics toggle');
}

t.section('renderEvidenceStrip() builds the compact IN/HAVE comparison');
{
  const strip = renderEvidenceFixture({
    downloaded_label: 'MP3 320',
    actual_min_bitrate: 245,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 160,
    existing_min_bitrate: 320,
  });
  t.contains(strip, 'class="r-evidence"', 'strip wrapper class');
  t.contains(strip, 'class="r-ev-row r-ev-in"', 'IN is a semantic grid row');
  t.contains(strip, 'class="r-ev-row r-ev-have"', 'HAVE is a semantic grid row');
  for (const slot of ['source', 'metric', 'spectral', 'v0']) {
    const count = strip.split(`r-ev-${slot}`).length - 1;
    t.equal(count, 2, `${slot} must occupy the same explicit slot in both rows`);
  }
  t.excludes(strip, 'r-ev-rank', 'rows do not render the permanently empty rank slot');
  t.excludes(strip, 'r-ev-value', 'rows do not collapse back to one freeform value cell');
  t.contains(strip, 'IN', 'IN side labelled');
  t.contains(strip, 'MP3 320', 'incoming label rendered');
  t.contains(strip, 'min 245k', 'incoming measured bitrate rendered with the min label');
  t.contains(strip, '~160k', 'incoming spectral floor rendered');
  t.contains(strip, 'HAVE', 'HAVE side labelled');
  t.contains(strip, 'min 320k', 'on-disk bitrate rendered with the min label');
}

t.section('renderEvidenceStrip() keeps converted source bare in collapsed rows');
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
  t.contains(strip, '>FLAC</span>', 'collapsed IN row names the measured source codec');
  t.excludes(strip, 'FLAC →', 'collapsed row does not show a conversion arrow');
  t.excludes(strip, 'OPUS 128', 'target/output contract is not labelled as source bitrate');
}

t.section('renderEvidenceStrip() labels an installed spectral-bound class truthfully');
{
  const strip = renderEvidenceFixture({
    source_format: 'MP3', actual_min_bitrate: 190,
    source_avg_bitrate: 190, source_min_bitrate: 190,
    existing_avg_bitrate: 275, existing_min_bitrate: 275,
    comparison_basis: {
      verdict: 'equivalent', branch: 'spectral_existing_bound',
      new_rank: 'acceptable', existing_rank: 'good',
      new_metric: 'avg', existing_metric: 'avg',
      new_value_kbps: 190, existing_value_kbps: 192,
      new_format: 'MP3', existing_format: 'MP3',
      spectral_clamped: true, tolerance_kbps: 5,
      verified_lossless_bypass: false,
    },
  });
  t.contains(strip, '190k avg', 'known-clean candidate keeps its raw metric');
  t.contains(strip, '~192k', 'installed class renders as a spectral floor');
  t.excludes(strip, 'avg 275k', 'installed raw VBR average does not relabel its class');
}

t.section('renderEvidenceStrip() keeps a VBR spectral class with its candidate encode');
{
  const candidateBound = renderEvidenceFixture({
    source_format: 'MP3', actual_min_bitrate: 275,
    source_avg_bitrate: 275, source_min_bitrate: 275,
    existing_avg_bitrate: 190, existing_min_bitrate: 190,
    comparison_basis: {
      verdict: 'equivalent', branch: 'spectral_candidate_bound',
      new_rank: 'acceptable', existing_rank: 'acceptable',
      new_metric: 'avg', existing_metric: 'avg',
      new_value_kbps: 192, existing_value_kbps: 190,
      new_format: 'MP3', existing_format: 'MP3',
      spectral_clamped: true, tolerance_kbps: 5,
      verified_lossless_bypass: false,
    },
  });
  t.contains(candidateBound, '~192k', 'candidate class renders as a spectral floor');
  t.excludes(candidateBound, '275k avg', 'candidate raw VBR average does not relabel its class');
}

t.section('renderEvidenceStrip() renders canonical candidate evidence as ordinary IN');
{
  const strip = renderEvidenceFixture({
    downloaded_label: 'MP3 V2',
    source_format: 'MP3',
    source_min_bitrate: 201,
    source_avg_bitrate: 259,
    source_median_bitrate: 255,
  });
  t.contains(strip, '>IN</strong>', 'historical triage evidence keeps the normal IN row');
  t.contains(strip, '>MP3</span>', 'candidate evidence supplies the source codec');
  t.contains(strip, '>259k avg (min 201k)</span>',
    'candidate evidence supplies its average and minimum');
}

t.section('renderEvidenceStrip() returns empty string when no evidence exists');
{
  const strip = renderEvidenceFixture({
    outcome: 'timeout',
    error_message: 'remote_queue_timeout 3600s exceeded',
  });
  t.equal(strip, '', 'no-evidence rows should produce no strip');
}

t.section('renderEvidenceStrip() requires a number — a codec label alone is not a comparison');
{
  // Failed downloads carry downloaded_label (from slskd filetype) but no
  // measurements; a label-only strip would spam "IN MP3 HAVE —" on every
  // failure row in the list.
  const strip = renderEvidenceFixture({
    outcome: 'timeout',
    downloaded_label: 'MP3',
  });
  t.equal(strip, '', 'label-only rows should produce no strip');
}

t.section('Download failures blank IN and keep the complete pre-attempt HAVE row');
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
  t.contains(
    strip,
    '<strong class="r-ev-tag">IN</strong><span class="r-ev-cell r-ev-source">',
    'timeout renders the IN row',
  );
  t.contains(strip, '>—</span>', 'timeout leaves IN blank');
  t.excludes(strip, '725k avg', 'timeout hides incoming bitrate');
  t.excludes(strip, 'V0 248k avg', 'timeout hides incoming V0');
  t.contains(strip, 'Opus', 'timeout keeps HAVE codec');
  t.contains(strip, '129k avg (min 93k)', 'timeout keeps HAVE average and minimum');
  t.contains(strip, '~96k suspect', 'timeout keeps HAVE spectral');
  t.contains(strip, 'V0 256k avg (min 193k)', 'timeout keeps HAVE V0');
}

t.section('Import failures retain the grabbed candidate in IN');
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
  t.contains(strip, '725k avg (min 455k)', 'failed import keeps incoming bitrate');
  t.contains(strip, '~96k likely transcode', 'failed import keeps incoming spectral');
  t.contains(strip, 'V0 248k avg (min 178k)', 'failed import keeps incoming V0');
  t.contains(strip, '>725k avg/455k min<', 'mobile metric labels each number in place');
  const spectralCount = strip.split('~96k likely transcode').length - 1;
  t.equal(spectralCount, 2,
    'mobile spectral keeps the full "likely transcode" wording (the column ellipsizes instead)');
  t.contains(strip, 'V0 248/178k<', 'mobile V0 stays the bare pair — its label is the V0 prefix');
  t.excludes(strip, 'a/m', 'the cryptic a/m shorthand is dead');
}

t.section('renderEvidenceStrip() keeps a CBR metric pair explicit but still collapses the V0 cell');
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
  t.contains(strip, '320k avg (min 320k)', 'desktop wording keeps both numbers');
  // A CBR metric cell keeps its avg/min pair on mobile — a bare "320k" was
  // ambiguous with a min-only measurement (issue #813 follow-up).
  t.contains(strip, '>320k avg/320k min<', 'mobile keeps the CBR metric pair explicit');
  t.excludes(strip, '>320k</span>', 'mobile no longer collapses the CBR metric to one number');
  t.contains(strip, '>V0 245k<', 'an equal V0 pair still collapses (its prefix labels it)');
}

t.section('renderEvidenceStrip() shows the on-disk format on the HAVE side');
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
  t.contains(strip, '>MP3</span>', 'HAVE side leads with the on-disk format');
  t.contains(strip, '>min 256k</span>', 'HAVE bitrate stays min-labelled in its shared slot');
}

t.section('renderEvidenceStrip() renders a supplied pre-attempt HAVE snapshot');
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
  t.contains(strip, '>Opus</span>', 'pre-attempt format populates HAVE');
  t.contains(strip, '>min 93k</span>', 'pre-attempt minimum populates HAVE');
}

t.section('renderDownloadHistoryItem() does not infer output from legacy min fields');
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
  t.contains(html, 'class="p-hist-label">Output</span>',
    'fixed output row remains present');
  t.contains(html, '<span class="p-hist-value">—</span>',
    'legacy row without materialized evidence stays honest');
}

t.section('renderDownloadHistoryItem() does not fabricate output when format is unknown');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'testuser',
    created_at: '2026-07-10T10:30:00+00:00',
    actual_min_bitrate: 320,
    existing_min_bitrate: 256,
  });
  t.contains(html, 'class="p-hist-label">Output</span>',
    'fixed output row remains present');
}

t.section('renderDownloadHistoryItem() calls only explicit quality labels contracts');
{
  const legacyMp3 = renderDownloadHistoryFixture({
    outcome: 'success',
    created_at: '2026-07-13T00:29:00+00:00',
    final_format: 'MP3',
  });
  t.contains(legacyMp3, 'Stored as', 'legacy stored format still renders');
  t.contains(legacyMp3, '>MP3<', 'bare MP3 remains a codec fact');
  t.excludes(legacyMp3, 'MP3 contract', 'bare MP3 is not a quality contract');

  const explicitOpus = renderDownloadHistoryFixture({
    outcome: 'success',
    created_at: '2026-07-13T01:06:00+00:00',
    final_format: 'opus 128',
  });
  t.contains(explicitOpus, 'OPUS 128 contract', 'numeric target is a contract');
}

t.section('renderEvidenceStrip() stops compact V0 probes after the minimum');
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
    t.contains(strip, 'V0 247k avg (min 224k)', `${kind} keeps avg and min`);
    t.excludes(strip, 'from lossy', `${kind} omits lossy provenance`);
    t.excludes(strip, 'on-disk re-encode', `${kind} omits re-encode provenance`);
    t.excludes(strip, 'future_probe_kind', `${kind} omits raw kind provenance`);
  }
}

t.section('renderEvidenceStrip() escapes injected values');
{
  const strip = renderEvidenceFixture({
    downloaded_label: '<img src=x>',
    actual_min_bitrate: 200,
  });
  t.excludes(strip, '<img src=x>', 'raw label not rendered');
  t.contains(strip, '&lt;img src=x&gt;', 'label escaped');
}

t.section('renderEvidenceStrip() renders the persisted comparison basis when present');
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
  t.contains(strip, '288k avg (min 194k)',
    'IN side shows the deciding average and actual minimum');
  t.excludes(strip, '>transparent</span>',
    'compact IN leaves decision ranks to the expanded detail');
  t.contains(strip, '196k avg (min 194k)',
    'HAVE side shows the deciding average and actual minimum');
  t.excludes(strip, '>good</span>',
    'compact HAVE leaves decision ranks to the expanded detail');
  t.contains(strip, 'genuine', 'spectral grade chip survives');
  t.contains(strip, '~160k genuine',
    'ordinary avg basis does not suppress a distinct spectral floor');
  t.excludes(strip, 'MP3 V2', 'min-derived label replaced by the basis');
  t.excludes(strip, 'actual MP3',
    'materialized output stays in expanded detail instead of crowding the compact source strip');

  const detail = renderDownloadHistoryFixture({
    outcome: 'success',
    created_at: '2026-07-15T00:00:00+00:00',
    materialized_format: 'MP3',
    materialized_min_bitrate: 195,
    materialized_avg_bitrate: 320,
    materialized_median_bitrate: 320,
  });
  t.contains(detail, 'MP3 avg 320kbps · min 195kbps',
    'expanded detail retains the materialized output lineage');
}

t.section('Gas: contract, V0 proof, and materialized Opus output stay distinct');
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
  t.contains(strip, '>FLAC - Opus</span>',
    'IN suffixes the source codec with the selected storage codec');
  t.contains(strip, '>132k avg (min 102k)</span>',
    'IN metric stays numeric so it aligns with HAVE');
  t.excludes(strip, 'FLAC →', 'collapsed source uses the compact suffix grammar');
  t.excludes(strip, 'OPUS 128 contract',
    'target contract remains in expanded details instead of the source strip');
  t.contains(strip, '>MP3</span>',
    'HAVE names the pre-import codec');
  t.contains(strip, '>128k avg (min 128k)</span>',
    'HAVE uses the pre-import measurement');
  const outputCount = strip.split('132k avg (min 102k)').length - 1;
  t.equal(outputCount, 1, 'materialized output belongs only to IN');
  t.excludes(strip, '>transparent</span>',
    'compact HAVE never substitutes a decision rank for measured spectral data');
  t.contains(strip, '~128k suspect',
    'HAVE keeps the existing spectral measurement after conversion');
  t.contains(strip, 'V0 224k avg', 'source V0 proof remains explicit');
  t.excludes(strip, 'OPUS 128 min 191k', 'V0 minimum never wears an Opus label');

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
  t.contains(detail, 'Output', 'detail grid names the materialized side');
  t.contains(detail, 'FLAC avg 811kbps · min 742kbps',
    'detail source uses downloaded source measurements');
  t.contains(detail, 'Target contract', 'detail names target policy separately');
  t.contains(detail, 'OPUS avg 132kbps · min 102kbps',
    'detail output is codec-aware');
  t.contains(detail, 'OPUS 128 contract', 'detail comparison is contract-aware');
  t.excludes(detail, '>Min bitrate<', 'ambiguous unqualified row is gone');
}

t.section('Amaterasu Shiroi: force import HAVE stays pre-import');
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
  t.contains(strip, '>FLAC - Opus</span>',
    'IN keeps the downloaded source and suffixes its storage codec');
  t.contains(strip, '>124k avg (min 118k)</span>',
    'IN metric contains only measured output bytes');
  t.contains(strip, '~96k likely transcode', 'IN keeps source spectral evidence');
  t.contains(strip, 'V0 258k avg (min 246k)', 'IN keeps its source V0 probe');
  t.contains(strip, '>OPUS</span>', 'HAVE names the pre-import copy');
  t.contains(strip, '>101k avg (min 90k)</span>',
    'HAVE is populated from pre-import bytes');
  t.excludes(strip, '>transparent</span>',
    'HAVE does not substitute the decision rank for spectral data');
  t.excludes(strip, '>excellent</span>', 'decision rank stays out of compact HAVE');
  // One cell renders the phrase twice (full + compact span); a leak into
  // HAVE would double that to 4.
  const spectralCount = strip.split('~96k likely transcode').length - 1;
  t.equal(spectralCount, 2, 'candidate spectral belongs only to IN');
  const v0Count = strip.split('V0 258k avg (min 246k)').length - 1;
  t.equal(v0Count, 1, 'candidate V0 belongs only to IN');
  t.contains(strip, '~80k suspect', 'HAVE keeps its own spectral snapshot');
  t.contains(strip, 'V0 220k avg (min 201k)', 'HAVE keeps its own V0 snapshot');
}

t.section('Absentee Schmotime: an upgrade keeps the pre-import copy in HAVE');
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
  t.contains(strip, '>FLAC - Opus</span>', 'IN names the converted source');
  t.contains(strip, '>136k avg (min 127k)</span>', 'IN uses measured Opus output');
  t.contains(strip, '>MP3</span>', 'HAVE keeps the pre-import codec');
  t.contains(strip, '>320k avg (min 320k)</span>',
    'HAVE keeps the pre-import bitrate snapshot');
  t.excludes(strip, '>transparent</span>',
    'compact upgrade leaves the internal rank in expanded detail');
  const incomingMetricCount = strip.split('136k avg (min 127k)').length - 1;
  t.equal(incomingMetricCount, 1, 'incoming output must appear once');
}

t.section('every attempted import keeps materialized IN separate from historical HAVE');
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
        t.contains(strip, `>${existing}</span>`,
          'generated upgrade HAVE keeps its pre-import codec');
        t.contains(strip, `>${existingAvg}k avg (min ${existingMin}k)</span>`,
          'generated upgrade HAVE keeps its pre-import measurements');
        const incomingCount = strip.split(`${incomingAvg}k avg (min ${incomingMin}k)`).length - 1;
        t.equal(incomingCount, 1,
          `attempted output bled into HAVE (${outcome}/${storage}/${existing}/${offset})`);
        }
      }
    }
  }
}

t.section('Forty Days: provisional HAVE stays the comparable on-disk copy');
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
  t.contains(strip, '>FLAC - Opus</span>',
    'IN names the provisional source and selected storage codec');
  t.contains(strip, '>122k avg (min 106k)</span>',
    'IN metric contains only the measured provisional result');
  t.contains(strip, 'V0 223k avg (min 200k)', 'IN keeps source V0 evidence');
  t.contains(strip, '>Opus</span>', 'HAVE names the comparable library copy');
  t.contains(strip, '>113k avg (min 103k)</span>',
    'HAVE reports average and minimum for the pre-attempt copy');
  t.contains(strip, '~96k likely transcode', 'HAVE keeps spectral evidence');
  t.contains(strip, 'V0 207k avg (min 173k)', 'HAVE keeps its V0 probe');
  t.excludes(strip, 'avg 122k (min 106k)',
    'candidate output does not replace provisional comparison evidence');
}

t.section('Actual Life 3: current canonical evidence fully populates triage HAVE');
{
  const strip = renderEvidenceFixture({
    outcome: 'rejected',
    badge: 'Triaged · download deleted',
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
  t.contains(strip, '>FLAC</span>', 'retained lossless keeps the bare source label');
  t.contains(strip, '>725k avg (min 455k)</span>',
    'retained lossless metric reports average plus minimum');
  t.contains(strip, '>Opus</span>', 'triage HAVE names the current copy');
  t.contains(strip, '>129k avg (min 93k)</span>',
    'triage HAVE reports average plus minimum');
  t.contains(strip, '~96k suspect', 'triage HAVE keeps current spectral evidence');
  t.contains(strip, 'V0 256k avg (min 193k)',
    'triage HAVE keeps the canonical current V0 probe');
}

t.section('corrupt candidates suppress invalid IN quality claims while keeping HAVE');
{
  const strip = renderEvidenceFixture({
    outcome: 'rejected',
    badge: 'Rejected',
    source_format: 'FLAC',
    source_min_bitrate: null,
    source_avg_bitrate: null,
    source_median_bitrate: null,
    spectral_grade: null,
    actual_min_bitrate: null,
    existing_format: 'MP3',
    existing_min_bitrate: 192,
    existing_avg_bitrate: 224,
  });
  t.contains(strip, '>FLAC</span>', 'corrupt source codec remains inspectable');
  t.contains(strip, '>224k avg (min 192k)</span>', 'HAVE remains point-in-time evidence');
  t.excludes(strip, '0k', 'corrupt source never presents zero bitrate as quality evidence');
  t.excludes(strip, 'genuine', 'corrupt source never presents a positive spectral grade');

  const detail = renderDownloadHistoryFixture({
    outcome: 'rejected', badge: 'Rejected',
    created_at: '2026-07-25T12:00:00+00:00',
    verdict: 'Corrupt audio files detected',
    source_format: 'FLAC', downloaded_label: 'FLAC',
    actual_min_bitrate: null,
    source_min_bitrate: null, source_avg_bitrate: null,
    source_median_bitrate: null, spectral_grade: null,
    v0_probe_kind: null, v0_probe_min_bitrate: null,
    v0_probe_avg_bitrate: null, v0_probe_median_bitrate: null,
    target_contract_format: null,
    materialized_format: null, materialized_min_bitrate: null,
    materialized_avg_bitrate: null, materialized_median_bitrate: null,
    comparison_basis: null,
    existing_format: 'MP3', existing_min_bitrate: 192,
    existing_avg_bitrate: 224,
  });
  t.contains(detail, 'FLAC', 'expanded Source retains the honest codec');
  t.excludes(detail, '171kbps', 'expanded V0 cannot revive a corrupt candidate');
  t.excludes(detail, 'OPUS avg', 'expanded Output cannot revive converted candidate bytes');
  t.excludes(detail, 'Target contract', 'expanded conversion policy is hidden for corrupt input');

  const legacy = renderEvidenceFixture({
    outcome: 'rejected', source_format: null, slskd_filetype: 'FLAC',
    original_filetype: 'FLAC', filetype: 'MP3', actual_filetype: 'Opus',
    downloaded_label: 'FLAC', actual_min_bitrate: null,
    existing_format: 'MP3', existing_min_bitrate: 192, existing_avg_bitrate: 224,
  });
  const sourceCodec = legacy.indexOf('>FLAC</span>');
  const haveRow = legacy.indexOf('r-ev-row r-ev-have');
  t.ok(sourceCodec !== -1 && sourceCodec < haveRow,
    'legacy corrupt source trusts captured slskd codec before filetype fallbacks');
}

t.section('lossless storage labels distinguish V0 from retained FLAC');
{
  const v0 = renderEvidenceFixture({
    outcome: 'success', badge: 'Imported', was_converted: true,
    source_format: 'FLAC', source_min_bitrate: 600, source_avg_bitrate: 800,
    target_contract_format: 'mp3 v0',
    materialized_format: 'MP3', materialized_min_bitrate: 220,
    materialized_avg_bitrate: 245,
  });
  t.contains(v0, '>FLAC - V0</span>',
    'V0 target is suffixed to the lossless source label');
  t.contains(v0, '>245k avg (min 220k)</span>',
    'V0 target leaves the metric column numeric');
  const newImportMetricCount = v0.split('245k avg (min 220k)').length - 1;
  t.equal(newImportMetricCount, 1, 'first import must leave HAVE empty');
  t.contains(v0, '>—</span>',
    'first import makes the absent pre-import HAVE explicit');

  const flac = renderEvidenceFixture({
    outcome: 'success', badge: 'New', was_converted: false,
    source_format: 'FLAC', source_min_bitrate: 455, source_avg_bitrate: 725,
  });
  t.contains(flac, '>FLAC</span>', 'retained FLAC target keeps the bare codec label');
  t.contains(flac, '>725k avg (min 455k)</span>',
    'retained FLAC metric contains only its actual bytes');
}

t.section('Iron & Wine: the temporary V0 minimum never wears the FLAC label');
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
  t.contains(strip, '>FLAC</span>', 'source remains labelled FLAC');
  t.excludes(strip, '>min 165k</span>', 'V0 minimum is not a FLAC measurement');
  t.contains(strip, 'V0 171k avg (min 165k)', 'candidate V0 owns its minimum');
  t.contains(strip, '>Opus</span>', 'pre-attempt existing Opus keeps its codec slot');
  t.contains(strip, '>min 114k</span>', 'pre-attempt existing Opus keeps its real floor');
  t.contains(strip, 'V0 232k avg (min 223k)', 'existing source V0 owns its minimum');

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
    verdict: 'Unproven lossless source not better than on-disk copy; searching continues',
  });
  t.contains(detail, '171kbps avg · min 165kbps',
    'detail candidate V0 owns its minimum');
  t.contains(detail, '232kbps avg · min 223kbps',
    'detail existing V0 owns its minimum');
}

t.section('evidence strip CSS keeps desktop alignment and gives mobile readable aligned one-line rows');
{
  const css = readFileSync(new URL('../web/index.html', import.meta.url), 'utf8');
  t.contains(css, '.r-ev-row { display: contents; }',
    'desktop row wrappers participate in the parent grid instead of defining independent columns');
  t.contains(css, 'grid-template-columns: 3.6em minmax(4.5em, 0.8fr) minmax(12em, 1.7fr) minmax(7.5em, 1fr) minmax(9em, 1.35fr)',
    'desktop reserves aligned tag/source/metric/spectral/V0 columns');
  t.contains(css, '@media (max-width: 720px)', 'shared grid has a narrow-screen layout');
  t.contains(css, '.r-evidence { grid-template-columns: 2.9em 3.2em minmax(8.5em, max-content) minmax(3em, 1fr) max-content; column-gap: 0.45em; font-size: 12px;',
    'mobile fixes tag+source and floors the bitrate column at a labelled-pair width, so every metric cell (CBR pairs included) keeps aligned column edges');
  t.contains(css, 'font-family: system-ui,',
    'mobile uses the narrow system font so full lines fit without squeezing');
  t.contains(css, '.r-ev-cell { overflow: hidden; text-overflow: ellipsis; }',
    'squeezed cells drop end characters instead of wrapping');
  t.contains(css, '.r-ev-full { display: none; } .r-ev-compact { display: inline; }',
    'mobile swaps in the spelled-out compact wording');
  t.contains(css, '.r-ev-compact { display: none; }',
    'desktop keeps the full evidence wording');
  t.excludes(css, 'clamp(9px', 'mobile never shrinks evidence below readable size');
  t.excludes(css, 'grid-template-columns: max-content max-content max-content max-content max-content max-content;',
    'the six-column mobile crush cannot return');
  t.excludes(css, 'column-gap: 1px', 'the 1px column crush cannot return');
  t.excludes(css, 'grid-template-rows: auto auto auto;',
    'mobile does not spend three physical rows on each evidence side');
  t.contains(css, '.r-evidence .r-ev-tag { color: #d3deea; font-weight: 900; font-size: 1.08em;',
    'IN/HAVE labels are visibly prominent');
  t.contains(css, '@media (min-width: 721px) { .r-ev-v0 { padding-left: 1em; } }',
    'desktop V0 keeps a visible gutter after long spectral labels');
  t.contains(css, '.recents-triage-label { color: #d66; font-weight: 600; }',
    'secondary triage annotations stay rejection-coloured');
  t.excludes(css, '.r-ev-cell { min-width: 0; overflow-wrap: anywhere;',
    'evidence tokens never use arbitrary mid-word wrapping');
  t.excludes(css, 'repeat(5, minmax(0, 1fr))',
    'mobile evidence never collapses every field into equal tiny columns');
  t.excludes(css, 'grid-template-columns: 2.8em minmax(3.4em',
    'the overlapping five-column mobile layout cannot return');
  t.excludes(css, 'minmax(4.5em, 0.75fr)',
    'desktop does not reserve a track for the permanently empty rank slot');
  t.excludes(css, 'minmax(8.5em, max-content) 0 minmax(3em, 1fr)',
    'mobile does not retain a zero-width track for the permanently empty rank slot');
}

t.section('renderEvidenceStrip() marks spectral-clamped rank values with ~');
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
  t.contains(strip, '~250k', 'clamped value gets the ~ prefix, no metric label');
  t.excludes(strip, 'avg 250k', 'clamped value must not claim a metric');
}

t.section('renderEvidenceStrip() marks spectral_tiebreak values with ~ too (issue #813)');
{
  // Same clamped-value display rule as the rank branch — the coarse
  // rank band bucketed differing spectral estimates together, so the
  // same-rank tiebreak decided directly on the clamped values.
  const strip = renderEvidenceFixture({
    actual_min_bitrate: 1000,
    comparison_basis: {
      verdict: 'worse', branch: 'spectral_tiebreak',
      new_rank: 'good', existing_rank: 'good',
      new_metric: 'avg', existing_metric: 'avg',
      new_value_kbps: 200, existing_value_kbps: 230,
      new_format: 'MP3', existing_format: 'MP3',
      spectral_clamped: true, tolerance_kbps: null,
      verified_lossless_bypass: false,
    },
  });
  t.contains(strip, '~200k', 'clamped value gets the ~ prefix, no metric label');
  t.excludes(strip, 'avg 200k', 'clamped value must not claim a metric');
}

t.section('renderEvidenceStrip() clamps ONLY the candidate on spectral_candidate_bound');
{
  // Issue #911's bound is asymmetric: the candidate is bounded by its own
  // spectral class while the HAVE keeps its real raw metric. A single
  // clamped flag would print the HAVE's honest average as an unlabelled
  // ~, or the candidate's class under a metric name it never had.
  const strip = renderEvidenceFixture({
    actual_min_bitrate: 320,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 160,
    comparison_basis: {
      verdict: 'equivalent', branch: 'spectral_candidate_bound',
      new_rank: 'acceptable', existing_rank: 'acceptable',
      new_metric: 'avg', existing_metric: 'avg',
      new_value_kbps: 160, existing_value_kbps: 160,
      new_format: 'MP3', existing_format: 'MP3',
      spectral_clamped: true, tolerance_kbps: null,
      verified_lossless_bypass: false,
    },
  });
  t.contains(strip, '~160k', 'the bounded candidate value gets the ~ prefix');
  t.excludes(strip, 'avg 160k',
    'the candidate is CBR 320 — printing its class as an average is the display lie');
  // ...and the grade chip must not re-print the same floor beside it.
  t.excludes(strip, '~160k likely transcode',
    'the basis already carries the floor; the chip must not double it up');
}

t.section('renderEvidenceStrip() escapes basis strings');
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
  t.excludes(strip, '<img src=x>', 'raw basis format not rendered');
  t.excludes(strip, '<b>x</b>', 'raw basis rank not rendered');
}

t.section('renderDownloadHistoryItem() renders a Compared row from the basis');
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
  t.contains(html, 'Compared', 'Compared label rendered');
  t.contains(html, 'MP3 avg 288k · transparent', 'new side with rank');
  t.contains(html, 'MP3 avg 196k · good', 'existing side with rank');
}

t.section('renderDownloadHistoryItem() Compared row notes the verified-lossless bypass');
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
  t.contains(html, 'verified lossless bypass', 'bypass annotated');
}

t.section('renderDownloadHistoryItem() omits the Compared row without a basis');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'dbqs',
    created_at: '2026-07-10T14:46:05+00:00',
    actual_min_bitrate: 194,
  });
  t.excludes(html, 'Compared', 'no Compared row on legacy rows');
}

t.section('renderDownloadHistoryItem() leads with the verdict, red on rejections');
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

  t.contains(html, 'p-hist-verdict-reject', 'rejected verdict gets the reject class');
  const verdictPos = html.indexOf('mbid_missing');
  const gridPos = html.indexOf('p-hist-grid');
  t.ok(verdictPos !== -1 && gridPos !== -1 && verdictPos < gridPos,
    'rejection verdict should render before the evidence grid');
}

t.section('renderDownloadHistoryItem() colors the verdict red across the failure family');
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
    t.contains(html, 'p-hist-verdict-reject', `${outcome} verdict gets the reject class`);
  }
}

t.section('renderDownloadHistoryItem() keeps success verdicts unstyled and above the grid');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'dbqs',
    created_at: '2026-07-10T14:46:05+00:00',
    actual_min_bitrate: 194,
    verdict: 'Upgrade: MP3 V2 to MP3 320',
  });
  t.contains(html, 'p-hist-verdict', 'verdict line present on success rows');
  t.excludes(html, 'p-hist-verdict-reject', 'success verdict keeps the default colour');
  const verdictPos = html.indexOf('Upgrade: MP3 V2 to MP3 320');
  const gridPos = html.indexOf('p-hist-grid');
  t.ok(verdictPos !== -1 && gridPos !== -1 && verdictPos < gridPos,
    'success verdict should also render before the grid');
}

t.section('renderDownloadHistoryItem() surfaces beets_detail behind the forensics toggle');
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
  t.contains(html, '<details class="p-hist-forensics">',
    'forensics toggle present when beets_detail exists');
  t.contains(html, 'Target MBID 3de1b986-1b7d-4769-ba9a-5d2b398d0331 not in candidates',
    'beets_detail reachable in forensics');
  const detailsStart = html.indexOf('<details');
  const detailPos = html.indexOf('Target MBID');
  t.ok(detailsStart !== -1 && detailPos > detailsStart,
    'beets_detail should live inside the forensics toggle');
}

t.section('renderDownloadHistoryItem() omits the forensics Detail row when beets_detail repeats the verdict');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-07-10T22:28:12+00:00',
    verdict: 'audio_corrupt',
    beets_detail: 'audio_corrupt',
  });
  t.excludes(html, '<details class="p-hist-forensics">',
    'no forensics toggle for a redundant beets_detail');
}

t.section('renderDownloadHistoryItem() shows the raw peer message behind a humanized verdict');
{
  // Issue #868: the verdict interprets ("Peer X rejected all 29 files"),
  // so the peer's own words need their own row — transfer_detail is
  // log-only, and this bounded projection is the only place they appear.
  const html = renderDownloadHistoryFixture({
    outcome: 'timeout',
    badge: 'Failed',
    badge_class: 'badge-failed',
    soulseek_username: 'Tymemage',
    created_at: '2026-07-25T02:10:00+00:00',
    verdict: 'Peer Tymemage rejected all 29 files before transfer \u2014 "Verification required"',
    transfer_message: '29\u00d7 "Verification required"',
    transfer_message_label: 'Peer message',
  });

  t.contains(html, 'class="p-hist-label">Peer message</span>',
    'server-owned evidence label renders as its own row');
  t.contains(html, '29\u00d7 &quot;Verification required&quot;',
    'raw peer text is escaped and visible');
  t.contains(html, 'color:#888;', 'raw peer text renders dim');
}

t.section('renderDownloadHistoryItem() labels a local storage failure as storage, not a peer message');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'timeout',
    badge: 'Failed',
    badge_class: 'badge-failed',
    soulseek_username: 'Tymemage',
    created_at: '2026-07-25T02:10:00+00:00',
    verdict: 'Local storage error writing 3 files \u2014 "Failed to create file 01.flac: Stale file handle"',
    transfer_message: '3\u00d7 "Failed to create file 01.flac: Stale file handle"',
    transfer_message_label: 'Storage error',
  });

  t.contains(html, 'class="p-hist-label">Storage error</span>',
    'storage failures keep their own label');
  t.excludes(html, 'class="p-hist-label">Peer message</span>',
    'our own storage fault is never captioned as something a peer said');
}

t.section('renderDownloadHistoryItem() omits the evidence row when no transfer message exists');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'failed',
    badge: 'Failed',
    badge_class: 'badge-failed',
    soulseek_username: 'testuser',
    created_at: '2026-07-25T02:10:00+00:00',
    verdict: 'Download could not be staged for import in time; returned to the queue',
  });
  t.excludes(html, 'Peer message', 'no evidence row without evidence');
}

t.section('renderDownloadHistoryItem() escapes a hostile transfer message and label');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'timeout',
    soulseek_username: 'x',
    created_at: '2026-07-25T02:10:00+00:00',
    transfer_message: '1\u00d7 "<img src=x onerror=alert(1)>"',
    transfer_message_label: '<script>bad</script>',
  });
  t.excludes(html, '<img src=x', 'peer text is escaped');
  t.excludes(html, '<script>bad', 'label is escaped');
}

t.section('renderDownloadHistoryItem() labels a machine reason code as such in forensics');
{
  // Issue #868 review A4: PR1 persists the materialize reason in
  // beets_detail, which this card renders as a forensics row. The server
  // owns the label so a machine token is not captioned as beets prose.
  const html = renderDownloadHistoryFixture({
    outcome: 'failed',
    badge: 'Failed',
    badge_class: 'badge-failed',
    soulseek_username: 'testuser',
    created_at: '2026-07-25T02:10:00+00:00',
    verdict: 'Could not read a downloaded file from the slskd share (ESTALE); requeued',
    beets_detail: 'source_open_failed_ESTALE',
    beets_detail_label: 'Reason code',
  });

  t.contains(html, 'class="p-hist-label">Reason code</span>',
    'server-owned forensics label renders');
  t.contains(html, 'source_open_failed_ESTALE', 'the raw token stays visible');
  t.excludes(html, 'class="p-hist-label">Detail</span>',
    'a reason code is not captioned as beets detail');
}

t.section('renderDownloadHistoryItem() keeps the Detail label for beets prose');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-07-25T02:10:00+00:00',
    verdict: 'Wrong match (dist 0.190)',
    beets_detail: 'Target MBID not in candidates',
  });
  t.contains(html, 'class="p-hist-label">Detail</span>',
    'unlabelled details keep the historical caption');
}

t.section('spectralGradeIsAdmissible() withholds only the accusation');
{
  // Issue #829 PR4: undefined (no evidence join) keeps the historical
  // rendering; false neutralizes ONLY the two accusing grades.
  const cases = [
    ['likely_transcode', undefined, true],
    ['likely_transcode', true, true],
    ['likely_transcode', false, false],
    ['suspect', false, false],
    ['genuine', false, true],
    ['marginal', false, true],
  ];
  for (const [grade, admissible, expected] of cases) {
    t.equal(spectralGradeIsAdmissible(grade, admissible), expected,
      `${grade}/${admissible} should be ${expected}`);
  }
}

t.section('spectralChip() keeps the grade but drops the accusing colour');
{
  const accused = spectralChip('likely_transcode', 128, true);
  t.contains(accused, 'quality-tone-poor',
    'an admissible transcode grade keeps its red tone');
  t.excludes(accused, 'audit-only',
    'an admissible grade carries no audit-only suffix');

  const audited = spectralChip('likely_transcode', 128, false);
  t.contains(audited, 'likely transcode',
    'the measured grade stays visible as the audit fact');
  t.contains(audited, 'audit-only',
    'an inadmissible grade is labelled audit-only');
  t.contains(audited, 'quality-tone-unknown',
    'an inadmissible grade loses the accusing tone');
  t.excludes(audited, 'quality-tone-poor',
    'an inadmissible grade is never painted as a transcode');
}

t.section('renderDownloadHistoryItem() states the proof gate exactly once');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-08-01T02:10:00+00:00',
    verdict: 'Rejected',
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 128,
    spectral_accusation_admissible: true,
    verdict_tier: 1,
    verdict_tier_statement: 'Transcode detected: in-window spectral cliff',
    verdict_fired_legs: ['in_window_cliff'],
  });
  t.contains(html, 'class="p-hist-label">Proof gate</span>',
    'the proof-gate row renders');
  t.contains(html, 'Transcode detected: in-window spectral cliff',
    'the tier statement renders verbatim');
  const statements = html.split('Transcode detected').length - 1;
  t.equal(statements, 1, 'exactly one transcode statement');
}

t.section('renderDownloadHistoryItem() names the proof generation');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'testuser',
    created_at: '2026-08-01T02:10:00+00:00',
    verdict: 'Imported',
    verified_lossless_generation: 'cliff/grade + ultrasonic legs',
  });
  t.contains(html, 'class="p-hist-label">Verified lossless</span>',
    'the proof-generation row renders');
  t.contains(html, 'proved by cliff/grade + ultrasonic legs',
    'the generation label renders verbatim');
}

t.section('renderDownloadHistoryItem() explains a skipped spectral pass with CD proof');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'success',
    soulseek_username: 'testuser',
    created_at: '2026-08-01T02:10:00+00:00',
    verdict: 'Imported',
    spectral_attempted: false,
    existing_spectral_grade: 'genuine',
    existing_spectral_bitrate: 210,
    verified_lossless_generation: 'exact CD rip bit match',
    cd_rip_verification: validDualProviderProof(),
  });
  t.contains(
    html,
    'CD bit-verified · CTDB confidence 11 + AccurateRip min confidence 3',
    'both provider-attributable positive confidences are visible',
  );
  t.contains(html, '<span class="r-ev-tag">IN</span> not needed — CD bit match',
    'the candidate spectral side explains why measurement did not run');
  t.contains(html, '<span class="r-ev-tag">HAVE</span>',
    'the existing side remains present');
  t.contains(html, 'genuine',
    'the existing spectral measurement is unchanged');
  t.excludes(html, '<span class="r-ev-tag">IN</span> unmeasured',
    'the CD-proved candidate is not called unmeasured');
}

t.section('renderDownloadHistoryItem() surfaces the stage-2 counterfactual');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-08-01T02:10:00+00:00',
    verdict: 'Rejected',
    stage2_if_stage1_deferred: 'downgrade',
    stage2_if_stage1_deferred_verdict: 'worse',
  });
  t.contains(html, 'If stage 1 had deferred',
    'the counterfactual row renders in forensics');
  t.contains(html, 'stage2=downgrade, scoring the candidate worse',
    'the counterfactual reads the same way pipeline-cli quality prints it');
}

t.section('renderDownloadHistoryItem() reports an unevaluable counterfactual');
{
  const html = renderDownloadHistoryFixture({
    outcome: 'rejected',
    soulseek_username: 'testuser',
    created_at: '2026-08-01T02:10:00+00:00',
    verdict: 'Rejected',
    stage2_if_stage1_deferred: 'unavailable',
  });
  t.contains(html, 'stage 2 could not be evaluated',
    '"could not run" is distinct from "had nothing to say"');
}

t.section('spectralStripCell() states the fact instead of the accusation');
{
  const audited = spectralStripCell('likely_transcode', '~128k ', false);
  t.contains(audited, 'codec rolloff',
    'the strip states native rolloff for an audit-only codec');
  t.excludes(audited, '>~128k likely transcode<',
    'the strip does not stamp the grade');
  t.contains(audited, 'Measured grade: likely transcode',
    'the measured grade stays reachable in the hover title');
  t.contains(audited, 'quality-tone-unknown',
    'the strip cell loses the accusing tone');
  const accused = spectralStripCell('likely_transcode', '~128k ', true);
  t.contains(accused, 'quality-tone-poor',
    'an admissible grade keeps its tone in the strip');
  t.contains(accused, 'likely transcode',
    'an admissible grade keeps its wording in the strip');
}

t.section('an unresolved codec never claims native encoder rolloff');
{
  const strip = spectralStripCell(
    'likely_transcode', '~128k ', false, 'codec_unresolved');
  t.contains(strip, 'codec unresolved',
    'the strip says the codec is unknown');
  t.excludes(strip, 'rolloff',
    'the strip never describes an encoder it could not identify');
  const chip = spectralChip('likely_transcode', 128, false, 'codec_unresolved');
  t.contains(chip, 'codec unresolved',
    'the card says the codec is unknown');
  t.excludes(chip, 'native encoder behaviour',
    'the card never describes an encoder it could not identify');
  const auditOnly = spectralChip(
    'likely_transcode', 128, false, 'audit_only_codec');
  t.contains(auditOnly, 'native encoder behaviour',
    'a resolved audit-only codec keeps the measured explanation');
}

t.section('renderEvidenceStrip() neutralizes an audit-only grade chip');
{
  const audited = renderEvidenceFixture({
    outcome: 'rejected',
    actual_min_bitrate: 256,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 128,
    spectral_accusation_admissible: false,
  });
  t.contains(audited, 'codec rolloff',
    'the strip states native rolloff for an audit-only IN codec');
  t.excludes(audited, 'quality-tone-poor',
    'the strip never paints an audit-only codec as a transcode');

  const accused = renderEvidenceFixture({
    outcome: 'rejected',
    actual_min_bitrate: 256,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 128,
    spectral_accusation_admissible: true,
  });
  t.contains(accused, 'quality-tone-poor',
    'an admissible transcode grade still reads as one');

  const auditedHave = renderEvidenceFixture({
    outcome: 'rejected',
    actual_min_bitrate: 256,
    existing_min_bitrate: 256,
    existing_spectral_grade: 'likely_transcode',
    existing_spectral_bitrate: 128,
    existing_spectral_accusation_admissible: false,
  });
  t.contains(auditedHave, 'codec rolloff',
    'the HAVE side is neutralized too (request 6387: the AAC is installed)');
  t.excludes(auditedHave, 'quality-tone-poor',
    'the HAVE side never paints an audit-only codec as a transcode');
}

t.done();
