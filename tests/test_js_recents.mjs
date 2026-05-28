/**
 * Unit tests for web/js/recents.js queue rendering helpers.
 * Run with: node tests/test_js_recents.mjs
 */

import { __test__ } from '../web/js/recents.js';
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

console.log('renderImportQueueItems() shows ready next row and preview detail');
{
  const html = __test__.renderImportQueueItems([{
    id: 77,
    job_type: 'force_import',
    status: 'queued',
    preview_status: 'evidence_ready',
    artist_name: 'Broadcast',
    album_title: 'Tender Buttons',
    preview_message: 'Evidence ready for final check: import',
    preview_result: { stage_chain: ['stage2_import:import'] },
  }]);
  assertContains(html, 'Tender Buttons', 'album title rendered');
  assertContains(html, 'Broadcast', 'artist name rendered');
  assertContains(html, 'next check', 'first ready row is marked next check');
  assertContains(html, 'preview: evidence_ready', 'preview state rendered');
  assertContains(html, 'stage2_import:import', 'stage chain rendered');
}

console.log('renderRecentsSubnav() refreshes the active recents subtab');
{
  state.recentsSub = 'downloading';
  const html = __test__.renderRecentsSubnav();
  assertContains(html, 'window.setRecentsSub(\'history\')', 'history tab rendered');
  assertContains(html, 'window.setRecentsSub(\'downloading\')', 'downloading tab rendered');
  assertContains(html, 'window.setRecentsSub(\'queue\')', 'queue tab rendered');
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
  const html = __test__.renderRecentsItems([
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
  assertExcludes(html, 'next check', 'uncertain rows are not marked next');
}

console.log('renderImportQueueItems() renders measurement_failed badge and red border');
{
  // Post-U5: preview emits preview_status='measurement_failed' instead of
  // 'uncertain'. The badge must be present (no blank pill) and the border
  // must be the same red as 'confident_reject' so operators see the failure
  // at a glance.
  const html = __test__.renderImportQueueItems([{
    id: 79,
    job_type: 'force_import',
    status: 'failed',
    preview_status: 'measurement_failed',
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

console.log('renderImportQueueItems() surfaces failed force-import source cleanup');
{
  const html = __test__.renderImportQueueItems([{
    id: 40636,
    job_type: 'force_import',
    status: 'failed',
    preview_status: 'evidence_ready',
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

console.log('renderDownloadingItems() shows current file progress and user');
{
  const html = __test__.renderDownloadingItems([{
    id: 81,
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

console.log('renderDownloadingItems() escapes current download fields');
{
  const html = __test__.renderDownloadingItems([{
    id: 82,
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

console.log('renderDownloadingItems() shows active YouTube ingest rows');
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
  const html = __test__.renderDownloadingItems([row]);
  assertContains(html, 'YT Album', 'YT album title rendered');
  assertContains(html, 'YT Artist', 'YT artist rendered');
  assertContains(html, 'youtube ingest', 'YouTube ingest badge rendered');
  assertContains(html, 'YouTube · 2 tracks · browse MPREb_yt',
    'YouTube ingest summary rendered');
  assertContains(html, '#202 · YT #301', 'request and download log ids rendered');
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
  assertContains(html, 'triage - deleted spectral reject',
    'triage label rendered in recents metadata');
  assertContains(html, 'recents-triage-label',
    'triage label uses the visible yellow recents style');
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

console.log('renderRecentsItems() does not mark rejected history as cleared wrong-matches');
{
  const html = __test__.renderRecentsItems([{
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
  const html = __test__.renderRecentsItems([{
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

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
