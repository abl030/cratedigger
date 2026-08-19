/**
 * Unit tests for web/js/recents.js activity rendering helpers.
 * Run with: node tests/test_js_recents.mjs
 */

import { __test__ } from '../web/js/recents.js';
import { state } from '../web/js/state.js';
import { esc } from '../web/js/util.js';
import { validDualProviderProof } from './fixtures/cd_rip_proof.mjs';

const { renderRecentsItems: renderRecentsFixture } = __test__;

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

function assertEqual(actual, expected, msg) {
  if (actual === expected) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} - expected '${expected}', got '${actual}'`);
  }
}

console.log('renderImportItems() consumes the server-classified display contract');
{
  const html = __test__.renderImportItems([{
    id: 77,
    job_type: 'force_import',
    status: 'queued',
    preview_status: 'evidence_ready',
    badge: 'Next check',
    badge_class: 'badge-new',
    border_color: '#1a4a2a',
    summary: 'Evidence ready for final check: import',
    artist_name: 'Broadcast',
    album_title: 'Tender Buttons',
    preview_message: 'Evidence ready for final check: import',
    preview_result: { stage_chain: ['stage2_import:import'] },
  }]);
  assertContains(html, 'Tender Buttons', 'album title rendered');
  assertContains(html, 'Broadcast', 'artist name rendered');
  assertContains(html, 'Next check', 'server badge is rendered verbatim');
  assertContains(html, 'preview: evidence_ready', 'preview state rendered');
  assertContains(html, 'stage2_import:import', 'stage chain rendered');
}

console.log('renderRecentsSubnav() refreshes the active recents subtab');
{
  state.recentsSub = 'acquisition';
  const html = __test__.renderRecentsSubnav();
  assertContains(html, 'window.setRecentsSub(\'history\')', 'history tab rendered');
  assertContains(html, 'window.setRecentsSub(\'acquisition\')', 'acquisition tab rendered');
  assertContains(html, '>Acquisition<', 'request lifecycle subtab has ownership-neutral name');
  assertExcludes(html, '>Downloading<', 'old transfer-only label is gone');
  assertContains(html, 'window.setRecentsSub(\'imports\')', 'imports tab rendered');
  assertContains(html, '>Imports<', 'ambiguous Queue label is gone');
  assertContains(html, 'window.loadRecents()', 'refresh reloads current recents subtab');
  assertContains(html, 'subtab-refresh', 'refresh uses shared subtab layout');
}

console.log('renderRecentsCounts() stays focused on history filters');
{
  state.recentsFilter = 'all';
  state.recentsCounts = {
    all: 10,
    imported: 3,
    rejected: 7,
    matches_24h: 24,
    matches_6h: 12,
    matches_per_hour_24h: 1,
    matches_per_hour_6h: 2,
  };
  const html = __test__.renderRecentsCounts();
  assertContains(html, '<div class="count-num">10</div><div class="count-label">all</div>',
    'all count rendered');
  assertContains(html, '<div class="count-num">3</div><div class="count-label">imported</div>',
    'imported count rendered');
  assertContains(html, '<div class="count-num">7</div><div class="count-label">rejected</div>',
    'rejected count rendered');
  assertExcludes(html, 'match/hr', 'match rates are not rendered in count cards');
}

console.log('renderRecentsItems() carries the detailed convergence action once');
{
  const html = renderRecentsFixture([{
    id: 10,
    request_id: 41,
    request_status: 'wanted',
    created_at: '2026-08-03T12:00:00+00:00',
    album_title: 'Provisional Album',
    artist_name: 'Artist',
    badge: 'Rejected',
    badge_class: 'badge-warn',
    border_color: '#805f20',
    summary: 'FLAC · peer',
    convergence: {
      request_id: 41,
      observation_count: 7,
      distinct_peer_count: 6,
      distinct_candidate_snapshot_count: 5,
      distinct_codec_count: 2,
      cliff_hz: 15000,
      raw_cliff_min_hz: 14900,
      raw_cliff_max_hz: 15100,
      cliff_spread_hz: 200,
      latest_qualifying_log_id: 99,
      signal_token: 'a'.repeat(64),
    },
  }]);
  assertEqual((html.match(/class="convergence-prompt"/g) || []).length, 1,
    'Recents renders one detailed convergence prompt');
  assertContains(html, '&quot;recents&quot;',
    'Recents action carries its refresh origin');
}

console.log('renderRecentsItems() shows attributable positive CD proof');
{
  const html = renderRecentsFixture([{
    id: 39293,
    request_id: 421,
    created_at: '2026-08-03T12:00:00+00:00',
    album_title: 'Jamaica Soul Shake Vol.1',
    artist_name: 'Sound Dimension',
    badge: 'Imported',
    badge_class: 'badge-new',
    border_color: '#1a4a2a',
    summary: 'FLAC · exact CD rip bit match',
    cd_rip_verification: validDualProviderProof(),
  }]);
  assertContains(
    html,
    'CD bit-verified · CTDB confidence 11 + AccurateRip min confidence 3',
    'the collapsed Recents row retains both positive provider confidences',
  );

  const absent = renderRecentsFixture([{
    id: 39294,
    request_id: 422,
    created_at: '2026-08-03T12:01:00+00:00',
    album_title: 'Ordinary row',
    artist_name: 'Artist',
    badge: 'Imported',
    badge_class: 'badge-new',
    border_color: '#1a4a2a',
    summary: 'MP3 320',
  }]);
  assertExcludes(absent, 'CD bit-verified',
    'absence creates no failed or negative verification label');
}

console.log('recentsLogUrl() requests enough history for triage labels');
{
  state.recentsFilter = 'all';
  assertContains(__test__.recentsLogUrl(), '/api/pipeline/log?limit=500',
    'all recents requests the expanded bounded history window');
  state.recentsFilter = 'rejected';
  assertContains(__test__.recentsLogUrl(), '/api/pipeline/log?outcome=rejected&limit=500',
    'filtered recents keeps outcome filter and expanded limit');
}

console.log('triageLabelText() restores the old recents label wording');
{
  assertContains(__test__.triageLabelText('kept: would import'), 'triage - kept would import',
    'kept would import label uses old wording');
  assertContains(__test__.triageLabelText('deleted: spectral reject'), 'triage - deleted spectral reject',
    'deleted spectral reject label uses old wording');
}

console.log('renderRecentsItems() shows match rates beside the first date header');
{
  const html = renderRecentsFixture([
    {
      id: 10,
      request_id: 20,
      created_at: '2026-05-05T12:00:00+00:00',
      album_title: 'Match Rate Album',
      artist_name: 'Artist',
      badge: 'Imported',
      badge_class: 'badge-new',
      border_color: '#1a4a2a',
      summary: 'MP3 320 · user',
    },
  ], {
    matches_per_hour_6h: 4.5,
    matches_per_hour_24h: 5.3333333333,
  });
  assertContains(html, 'recents-date-header', 'first date uses date metric row');
  assertContains(html, '6h 4.50 match/hr', '6h match rate rendered');
  assertContains(html, '24h 5.33 match/hr', '24h match rate rendered');
}

console.log('matchRatesFromDashboardWindows() derives found enqueue rates from old dashboard payloads');
{
  const rates = __test__.matchRatesFromDashboardWindows([
    {label: '24h', hours: 24, outcomes: {found: 132}},
    {label: '6h', hours: 6, outcomes: {found: 27}},
  ]);
  if (rates.matches_24h === 132
      && rates.matches_6h === 27
      && rates.matches_per_hour_24h === 5.5
      && rates.matches_per_hour_6h === 4.5) {
    passed++;
  } else {
    failed++;
    console.error('  FAIL: dashboard windows did not derive expected match rates');
  }
}

console.log('renderImportItems() shows server-classified uncertain preview failures');
{
  const html = __test__.renderImportItems([{
    id: 78,
    job_type: 'force_import',
    status: 'failed',
    preview_status: 'uncertain',
    badge: 'Uncertain',
    badge_class: 'badge-warn',
    border_color: '#a93',
    summary: 'Preview failed: path_missing',
    artist_name: 'Low',
    album_title: 'Things We Lost in the Fire',
    preview_message: 'Preview failed: path_missing',
  }]);
  assertContains(html, 'uncertain', 'uncertain badge rendered');
  assertContains(html, 'Preview failed: path_missing', 'failure message rendered');
  assertExcludes(html, 'next check', 'uncertain rows are not marked next');
}

console.log('renderImportItems() renders server-classified measurement failure');
{
  // Post-U5: preview emits preview_status='measurement_failed' instead of
  // 'uncertain'. The badge must be present (no blank pill) and the border
  // must be the same red as 'confident_reject' so operators see the failure
  // at a glance.
  const html = __test__.renderImportItems([{
    id: 79,
    job_type: 'force_import',
    status: 'failed',
    preview_status: 'measurement_failed',
    badge: 'Measurement failed',
    badge_class: 'badge-failed',
    border_color: '#a33',
    summary: 'Preview measurement failed: snapshot_stale',
    artist_name: 'Slowdive',
    album_title: 'Souvlaki',
    preview_message: 'Preview measurement failed: snapshot_stale',
  }]);
  assertContains(html, 'measurement failed', 'measurement_failed badge rendered');
  assertContains(html, '#a33', 'measurement_failed uses confident_reject red border');
  assertContains(html, 'Preview measurement failed: snapshot_stale',
    'measurement failure message rendered');
  assertExcludes(html, 'next check', 'measurement_failed rows are not marked next');
}

console.log('renderImportItems() trusts the server summary over stale raw messages');
{
  const html = __test__.renderImportItems([{
    id: 731,
    job_type: 'automation_import',
    status: 'failed',
    preview_status: 'would_import',
    badge: 'Importing',
    badge_class: 'badge-force',
    border_color: '#36c',
    summary: 'Rejected: high_distance - distance=0.1611',
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

console.log('renderImportItems() surfaces failed force-import source cleanup');
{
  const html = __test__.renderImportItems([{
    id: 40636,
    job_type: 'force_import',
    status: 'failed',
    preview_status: 'evidence_ready',
    badge: 'Next check',
    badge_class: 'badge-new',
    border_color: '#1a4a2a',
    summary: '',
    artist_name: 'Parts & Labor',
    album_title: 'Escapers Two',
    message: 'Rejected by persisted quality evidence: downgrade',
    result: {
      cleanup: {
        success: true,
        outcome: 'deleted',
        deleted_path: '/mnt/virtio/music/slskd/failed_imports/Parts & Labor - Escapers Two (2007)',
      },
    },
  }]);
  assertContains(html, 'source deleted',
    'cleanup-success chip rendered on failed force-import row');
  assertContains(html, 'Parts &amp; Labor - Escapers Two',
    'cleanup path is escaped in chip hover text');
}

console.log('renderAcquisitionItems() shows current transfer progress and user');
{
  const html = __test__.renderAcquisitionItems([{
    id: 81,
    status: 'downloading',
    processing_owner: null,
    created_at: '2026-05-05T10:00:00+00:00',
    updated_at: '2026-05-05T12:30:00+00:00',
    album_title: 'Ocean Songs',
    artist_name: 'Dirty Three',
    last_outcome: 'timeout',
    active_download_state: {
      filetype: 'mp3 320',
      enqueued_at: '2026-05-05T12:20:00+00:00',
      last_progress_at: '2026-05-05T12:25:00+00:00',
      files: [
        {
          username: 'peer-a',
          size: 100,
          bytes_transferred: 100,
          last_state: 'Completed, Succeeded',
        },
        {
          username: 'peer-a',
          size: 200,
          bytes_transferred: 0,
          last_state: 'Queued, Remotely',
        },
      ],
    },
  }]);
  assertContains(html, 'Ocean Songs', 'album title rendered');
  assertContains(html, 'Dirty Three', 'artist name rendered');
  assertContains(html, 'downloading', 'downloading badge rendered');
  assertContains(html, 'mp3 320 · 1/2 files · peer-a · 1 queued',
    'download progress summary rendered');
  assertContains(html, 'last: timeout', 'last outcome rendered');
}

console.log('renderAcquisitionItems() escapes current download fields');
{
  const html = __test__.renderAcquisitionItems([{
    id: 82,
    status: 'downloading',
    processing_owner: null,
    created_at: '2026-05-05T10:00:00+00:00',
    album_title: '<album>',
    artist_name: '<artist>',
    active_download_state: {
      filetype: '<lossless>',
      files: [{ username: '<peer>' }],
    },
  }]);
  assertContains(html, '&lt;album&gt;', 'album is escaped');
  assertContains(html, '&lt;artist&gt;', 'artist is escaped');
  assertContains(html, '&lt;lossless&gt;', 'filetype is escaped');
  assertContains(html, '&lt;peer&gt;', 'peer username is escaped');
  assertExcludes(html, '<album>', 'raw album is not rendered');
}

console.log('renderAcquisitionItems() shows active YouTube ingest rows without processor ownership');
{
  const row = __test__.normalizeYoutubeIngestItem({
    download_log_id: 301,
    request_id: 202,
    created_at: '2026-05-28T01:00:00+00:00',
    album_title: 'YT Album',
    artist_name: 'YT Artist',
    youtube_metadata: {
      browse_id: 'MPREb_yt',
      expected_track_count: 2,
    },
  });
  const html = __test__.renderAcquisitionItems([row]);
  assertEqual(row.processing_owner, null, 'YouTube normalization cannot grant processing ownership');
  assertContains(html, 'YT Album', 'YT album title rendered');
  assertContains(html, 'YT Artist', 'YT artist rendered');
  assertContains(html, 'youtube ingest', 'YouTube ingest badge rendered');
  assertContains(html, 'YouTube · 2 tracks · browse MPREb_yt',
    'YouTube ingest summary rendered');
  assertContains(html, '#202 · YT #301', 'request and download log ids rendered');
}

console.log('renderAcquisitionItems() consumes only the exact processing owner');
{
  const html = __test__.renderAcquisitionItems([{
    id: 203,
    status: 'processing',
    created_at: '2026-07-29T01:00:00+00:00',
    updated_at: '2026-07-29T01:05:00+00:00',
    album_title: 'Exact Owner',
    artist_name: 'The Apartments',
    active_download_state: {
      processing_started_at: '2026-07-29T01:01:00+00:00',
      files: [{ username: 'stale-peer', last_state: 'Completed, Succeeded' }],
    },
    active_import_job: {
      id: 9999,
      status: 'running',
      preview_status: 'evidence_ready',
    },
    processing_owner: {
      job_id: 304,
      status: 'queued',
      preview_status: 'running',
    },
  }]);
  assertContains(html, 'previewing', 'badge comes from durable owner state');
  assertContains(html, 'job #304', 'summary names exact owner');
  assertContains(html, '/api/import-jobs/304/recovery', 'row links exact owner recovery detail');
  assertExcludes(html, '#9999', 'latest-job fallback is ignored');
  assertExcludes(html, 'waiting for import', 'processing_started_at path inference is ignored');
}

console.log('loadRecents() consumes the combined Acquisition route without changing context');
{
  const oldDocument = globalThis.document;
  const oldFetch = globalThis.fetch;
  const content = { innerHTML: '' };
  const calls = [];
  globalThis.document = {
    getElementById(id) {
      return id === 'recents-content' ? content : null;
    },
  };
  globalThis.fetch = async (url) => {
    calls.push(String(url));
    return {
      ok: true,
      status: 200,
      async json() {
        return {
          acquisition: [{
            id: 205,
            status: 'processing',
            album_title: 'Acquiring',
            artist_name: 'Owner',
            created_at: '2026-07-29T02:00:00+00:00',
            processing_owner: {
              job_id: 306,
              status: 'queued',
              preview_status: 'evidence_ready',
            },
          }],
          youtube_ingest: [],
        };
      },
    };
  };
  state.recentsSub = 'acquisition';
  await __test__.loadRecents();
  assertEqual(calls.join(','), '/api/pipeline/acquisition', 'only combined route is fetched');
  assertContains(content.innerHTML, 'waiting to import', 'processing acquisition is rendered');
  assertEqual(state.recentsSub, 'acquisition', 'active subview is preserved');
  globalThis.document = oldDocument;
  globalThis.fetch = oldFetch;
}

console.log('renderRecentsItems() shows bad-extension postflight warning chip');
{
  const html = renderRecentsFixture([{
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

console.log('renderRecentsItems() shows the track-length warning chip (issue #1178)');
{
  const warning = "Track length contradicts the matched release: "
    + "'00 - Hidden Track.flac' is 237.6s where the release declares "
    + "15.0s for 'Lost Weekend'";
  const html = renderRecentsFixture([{
    id: 40061,
    request_id: 8954,
    created_at: '2026-08-15T10:25:15+00:00',
    album_title: 'Lost Weekend',
    artist_name: 'Phoebe Bridgers',
    badge: 'Imported',
    badge_class: 'badge-new',
    border_color: '#1a4a2a',
    summary: 'FLAC · lwl',
    track_length_warning: warning,
  }]);
  assertContains(html, 'track length', 'track-length warning chip rendered');
  assertContains(html, esc(warning),
    'the full derived sentence appears in the chip hover detail');
  // The chip is a WARNING, not a proof badge — badge-verified is the
  // green "positive proof" class this same module uses for CD-rip proof;
  // an accusing-looking amber chip must never render with the green
  // class, and this asserts the exact class attribute, not merely that
  // 'badge-warn' appears SOMEWHERE (disambig/bad-ext chips share it too).
  assertContains(
    html,
    `class="badge badge-warn" title="${esc(warning)}">track length<`,
    'the chip renders with the badge-warn class, not badge-verified',
  );
}

console.log('renderRecentsItems() omits the track-length warning chip when the field is null');
{
  const html = renderRecentsFixture([{
    id: 40062,
    request_id: 8955,
    created_at: '2026-08-15T10:25:15+00:00',
    album_title: 'Some Other Album',
    artist_name: 'Some Artist',
    badge: 'Imported',
    badge_class: 'badge-new',
    border_color: '#1a4a2a',
    summary: 'FLAC · lwl',
    track_length_warning: null,
  }]);
  assertExcludes(html, 'track length',
    'no track-length chip rendered when the field is null');
}

console.log('renderRecentsItems() uses the main badge and server-composed summary for deleted triage');
{
  const html = renderRecentsFixture([{
    id: 725,
    request_id: 801,
    created_at: '2026-04-25T23:25:00+00:00',
    album_title: 'For Screening Purposes Only',
    artist_name: 'Test Icicles',
    badge: 'Triaged · download deleted',
    badge_class: 'badge-rejected',
    border_color: '#a33',
    summary: 'Wrong match (dist 0.190) · download deleted: spectral reject · moundsofass',
    wrong_match_triage_summary: 'deleted: spectral reject',
    wrong_match_triage_detail: 'action: deleted reject · stages: mp3_spectral:reject',
  }]);
  assertContains(html, 'Wrong match (dist 0.190) · download deleted: spectral reject · moundsofass',
    'one server-composed summary keeps match verdict, cleanup disposition, and uploader');
  assertContains(html, 'Triaged · download deleted',
    'deleted triage is the primary row badge');
  assertContains(html, 'badge badge-rejected',
    'deleted triage remains visually rejected');
  assertExcludes(html, 'recents-triage-label',
    'deleted triage does not render a second competing status label');
  assertContains(html, 'mp3_spectral:reject',
    'triage detail appears in hover text');
}

console.log('renderRecentsItems() uses an amber main badge for kept triage');
{
  const html = renderRecentsFixture([{
    id: 726,
    request_id: 802,
    created_at: '2026-07-14T18:36:33+00:00',
    album_title: 'Amaterasu Shiroi',
    artist_name: 'Eldar',
    badge: 'Triaged · download kept',
    badge_class: 'badge-warn',
    border_color: '#a33',
    summary: 'Wrong match (dist 0.233) · download kept: would import · R@v@scholl',
    wrong_match_triage_summary: 'kept: would import',
    wrong_match_triage_detail: 'action: kept would import',
  }]);
  assertContains(html, 'Triaged · download kept', 'kept triage is the primary row badge');
  assertContains(html, 'badge badge-warn', 'kept triage uses the amber badge class');
  assertExcludes(html, 'recents-triage-label',
    'kept triage does not render a second competing status label');
}

console.log('renderRecentsItems() escapes wrong-match triage chip fields');
{
  const html = renderRecentsFixture([{
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

console.log('renderRecentsItems() does not mark rejected history as cleared wrong-matches');
{
  const html = renderRecentsFixture([{
    id: 15838,
    request_id: 2762,
    outcome: 'rejected',
    created_at: '2026-05-16T16:19:59+00:00',
    album_title: 'Escapers Two',
    artist_name: 'Parts & Labor',
    badge: 'Rejected',
    badge_class: 'badge-rejected',
    border_color: '#a33',
    summary: 'downgrade · AliceLo',
    validation_result: null,
  }]);
  assertExcludes(html, 'not in Wrong Matches',
    'ordinary rejected history row is not labelled as a wrong-match cleanup result');
}

console.log('renderRecentsItems() does not mark visible wrong-match rows as cleared');
{
  const html = renderRecentsFixture([{
    id: 14534,
    request_id: 2762,
    outcome: 'rejected',
    created_at: '2026-05-15T08:02:42+00:00',
    album_title: 'Escapers Two',
    artist_name: 'Parts & Labor',
    badge: 'Rejected',
    badge_class: 'badge-rejected',
    border_color: '#a33',
    summary: 'Wrong match (dist 0.167) · AliceLo',
    validation_result: {
      failed_path: '/mnt/virtio/music/slskd/failed_imports/Parts & Labor - Escapers Two (2007)',
    },
  }]);
  assertExcludes(html, 'not in Wrong Matches',
    'actionable row with failed_path does not get cleared chip');
}

console.log('renderRecentsItems() shows the compact IN/HAVE evidence strip on quality rows');
{
  const html = renderRecentsFixture([{
    id: 900,
    request_id: 901,
    created_at: '2026-07-10T07:18:00+00:00',
    album_title: 'The Warmest Place',
    artist_name: 'Catcall',
    badge: 'Rejected',
    badge_class: 'badge-rejected',
    border_color: '#a33',
    summary: '245kbps avg (spectral likely_transcode ~160kbps) is not better than existing 320kbps avg · davesv',
    downloaded_label: 'MP3 V0',
    actual_min_bitrate: 245,
    spectral_grade: 'likely_transcode',
    spectral_bitrate: 160,
    existing_min_bitrate: 320,
  }]);
  assertContains(html, 'r-evidence', 'evidence strip rendered on quality rows');
  assertContains(html, 'min 245k', 'incoming bitrate in strip, min-labelled');
  assertContains(html, 'min 320k', 'on-disk bitrate in strip');
}

console.log('renderRecentsItems() omits the evidence strip when a row has no measurements');
{
  const html = renderRecentsFixture([{
    id: 902,
    request_id: 903,
    created_at: '2026-07-10T06:37:00+00:00',
    album_title: 'Paper Crush',
    artist_name: 'Letting Up Despite Great Faults',
    badge: 'Failed',
    badge_class: 'badge-failed',
    border_color: '#a33',
    summary: "Download failed: all 5 files errored — 5× 'Inactivity timeout'",
  }]);
  assertExcludes(html, 'r-evidence', 'no strip without measurements');
}

console.log('renderRecentsItems() surfaces retryable HAVE analysis failures');
{
  const html = renderRecentsFixture([{
    id: 904,
    request_id: 905,
    outcome: 'have_analysis_error',
    created_at: '2026-07-16T10:00:00+00:00',
    album_title: 'Things We Lost in the Fire',
    artist_name: 'Low',
    badge: 'Environment failure',
    badge_class: 'badge-warn',
    border_color: '#a86f20',
    summary: 'Installed HAVE analysis failed. Request remains wanted; a future download will retry normally.',
    failure_category: 'permission_denied',
    analysis_error: 'PermissionError: <denied>',
    installed_path: '/mnt/Music/Beets/Low/<current>',
    candidate_reference: '/mnt/Music/Incoming/candidate&next',
  }]);
  assertContains(html, 'border-left-color:#a86f20', 'environment border is distinct');
  assertContains(html, 'Environment failure', 'environment badge rendered');
  assertContains(html, 'permission denied', 'failure category is visible');
  assertContains(html, 'HAVE /mnt/Music/Beets/Low/&lt;current&gt;', 'installed path is visible and escaped');
  assertContains(html, 'candidate /mnt/Music/Incoming/candidate&amp;next', 'candidate reference is visible and escaped');
  assertContains(html, 'PermissionError: &lt;denied&gt;', 'raw analysis error is visible and escaped');
  assertContains(html, 'remains wanted', 'retryable state copy rendered');
  assertExcludes(html, 'PermissionError: <denied>', 'raw error HTML is never rendered');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
