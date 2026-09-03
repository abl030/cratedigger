/**
 * Unit tests for web/js/recents.js activity rendering helpers.
 *
 * `loadRecents` is the composed entry and this suite drives it, but the
 * three item renderers keep their direct calls on purpose (issue #1346).
 * `renderRecentsItems` is imported as `renderRecentsFixture` because
 * `tests/test_js_payload_contract_audit.py` reads the LITERAL argument at
 * each of those call sites and checks every key against the server's
 * payload contract; routing them through `loadRecents` would hand the
 * audit a `fetch` stub to parse instead of a payload, and it would go
 * quietly blind. `renderAcquisitionItems` and `renderImportItems` sit
 * behind mutually exclusive `state.recentsSub` branches, one fetch each.
 *
 * Run with: node tests/test_js_recents.mjs
 */

import {
  loadRecents,
  matchRatesFromDashboardWindows,
  normalizeYoutubeIngestItem,
  recentsLogUrl,
  renderAcquisitionItems,
  renderImportItems,
  renderRecentsCounts,
  renderRecentsItems as renderRecentsFixture,
  renderRecentsSubnav,
  triageLabelText,
} from '../web/js/recents.js';
import { closeSearchPlanDetail } from '../web/js/search_plan.js';
import { state } from '../web/js/state.js';
import { esc } from '../web/js/util.js';
import { validDualProviderProof } from './fixtures/cd_rip_proof.mjs';

import { domStub, stubGlobals, suite } from './js_harness.mjs';


const t = suite(import.meta.url);

t.section('renderImportItems() consumes the server-classified display contract');
{
  const html = renderImportItems([{
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
  t.contains(html, 'Tender Buttons', 'album title rendered');
  t.contains(html, 'Broadcast', 'artist name rendered');
  t.contains(html, 'Next check', 'server badge is rendered verbatim');
  t.contains(html, 'preview: evidence_ready', 'preview state rendered');
  t.contains(html, 'stage2_import:import', 'stage chain rendered');
}

t.section('renderRecentsSubnav() refreshes the active recents subtab');
{
  state.recentsSub = 'acquisition';
  const html = renderRecentsSubnav();
  t.contains(html, 'window.setRecentsSub(\'history\')', 'history tab rendered');
  t.contains(html, 'window.setRecentsSub(\'acquisition\')', 'acquisition tab rendered');
  t.contains(html, '>Acquisition<', 'request lifecycle subtab has ownership-neutral name');
  t.excludes(html, '>Downloading<', 'old transfer-only label is gone');
  t.contains(html, 'window.setRecentsSub(\'imports\')', 'imports tab rendered');
  t.contains(html, '>Imports<', 'ambiguous Queue label is gone');
  t.contains(html, 'window.loadRecents()', 'refresh reloads current recents subtab');
  t.contains(html, 'subtab-refresh', 'refresh uses shared subtab layout');
}

t.section('renderRecentsCounts() stays focused on history filters');
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
  const html = renderRecentsCounts();
  t.contains(html, '<div class="count-num">10</div><div class="count-label">all</div>',
    'all count rendered');
  t.contains(html, '<div class="count-num">3</div><div class="count-label">imported</div>',
    'imported count rendered');
  t.contains(html, '<div class="count-num">7</div><div class="count-label">rejected</div>',
    'rejected count rendered');
  t.excludes(html, 'match/hr', 'match rates are not rendered in count cards');
}

t.section('renderRecentsItems() carries the detailed convergence action once');
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
  t.equal((html.match(/class="convergence-prompt"/g) || []).length, 1,
    'Recents renders one detailed convergence prompt');
  t.contains(html, '&quot;recents&quot;',
    'Recents action carries its refresh origin');
}

t.section('renderRecentsItems() shows attributable positive CD proof');
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
  t.contains(
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
  t.excludes(absent, 'CD bit-verified',
    'absence creates no failed or negative verification label');
}

t.section('recentsLogUrl() requests enough history for triage labels');
{
  state.recentsFilter = 'all';
  t.contains(recentsLogUrl(), '/api/pipeline/log?limit=500',
    'all recents requests the expanded bounded history window');
  state.recentsFilter = 'rejected';
  t.contains(recentsLogUrl(), '/api/pipeline/log?outcome=rejected&limit=500',
    'filtered recents keeps outcome filter and expanded limit');
}

t.section('triageLabelText() restores the old recents label wording');
{
  t.contains(triageLabelText('kept: would import'), 'triage - kept would import',
    'kept would import label uses old wording');
  t.contains(triageLabelText('deleted: spectral reject'), 'triage - deleted spectral reject',
    'deleted spectral reject label uses old wording');
}

t.section('renderRecentsItems() shows match rates beside the first date header');
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
  t.contains(html, 'recents-date-header', 'first date uses date metric row');
  t.contains(html, '6h 4.50 match/hr', '6h match rate rendered');
  t.contains(html, '24h 5.33 match/hr', '24h match rate rendered');
}

t.section('matchRatesFromDashboardWindows() derives found enqueue rates from old dashboard payloads');
{
  const rates = matchRatesFromDashboardWindows([
    {label: '24h', hours: 24, outcomes: {found: 132}},
    {label: '6h', hours: 6, outcomes: {found: 27}},
  ]);
  t.deepEqual(
    {
      matches_24h: rates.matches_24h,
      matches_6h: rates.matches_6h,
      matches_per_hour_24h: rates.matches_per_hour_24h,
      matches_per_hour_6h: rates.matches_per_hour_6h,
    },
    {
      matches_24h: 132,
      matches_6h: 27,
      matches_per_hour_24h: 5.5,
      matches_per_hour_6h: 4.5,
    },
    'dashboard windows derive a per-hour match rate for each window',
  );
}

t.section('renderImportItems() shows server-classified uncertain preview failures');
{
  const html = renderImportItems([{
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
  t.contains(html, 'uncertain', 'uncertain badge rendered');
  t.contains(html, 'Preview failed: path_missing', 'failure message rendered');
  t.excludes(html, 'next check', 'uncertain rows are not marked next');
}

t.section('renderImportItems() renders server-classified measurement failure');
{
  // Post-U5: preview emits preview_status='measurement_failed' instead of
  // 'uncertain'. The badge must be present (no blank pill) and the border
  // must be the same red as 'confident_reject' so operators see the failure
  // at a glance.
  const html = renderImportItems([{
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
  t.contains(html, 'measurement failed', 'measurement_failed badge rendered');
  t.contains(html, '#a33', 'measurement_failed uses confident_reject red border');
  t.contains(html, 'Preview measurement failed: snapshot_stale',
    'measurement failure message rendered');
  t.excludes(html, 'next check', 'measurement_failed rows are not marked next');
}

t.section('renderImportItems() trusts the server summary over stale raw messages');
{
  const html = renderImportItems([{
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
  t.contains(html, 'Rejected: high_distance - distance=0.1611',
    'terminal failure message rendered');
  t.excludes(html, 'Preview gate disabled',
    'stale preview message hidden for terminal rows');
}

t.section('renderImportItems() surfaces failed force-import source cleanup');
{
  const html = renderImportItems([{
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
  t.contains(html, 'source deleted',
    'cleanup-success chip rendered on failed force-import row');
  t.contains(html, 'Parts &amp; Labor - Escapers Two',
    'cleanup path is escaped in chip hover text');
}

t.section('renderAcquisitionItems() shows current transfer progress and user');
{
  const html = renderAcquisitionItems([{
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
  t.contains(html, 'Ocean Songs', 'album title rendered');
  t.contains(html, 'Dirty Three', 'artist name rendered');
  t.contains(html, 'downloading', 'downloading badge rendered');
  t.contains(html, 'mp3 320 · 1/2 files · peer-a · 1 queued',
    'download progress summary rendered');
  t.contains(html, 'last: timeout', 'last outcome rendered');
}

t.section('renderAcquisitionItems() escapes current download fields');
{
  const html = renderAcquisitionItems([{
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
  t.contains(html, '&lt;album&gt;', 'album is escaped');
  t.contains(html, '&lt;artist&gt;', 'artist is escaped');
  t.contains(html, '&lt;lossless&gt;', 'filetype is escaped');
  t.contains(html, '&lt;peer&gt;', 'peer username is escaped');
  t.excludes(html, '<album>', 'raw album is not rendered');
}

t.section('renderAcquisitionItems() shows active YouTube ingest rows without processor ownership');
{
  const row = normalizeYoutubeIngestItem({
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
  const html = renderAcquisitionItems([row]);
  t.equal(row.processing_owner, null, 'YouTube normalization cannot grant processing ownership');
  t.contains(html, 'YT Album', 'YT album title rendered');
  t.contains(html, 'YT Artist', 'YT artist rendered');
  t.contains(html, 'youtube ingest', 'YouTube ingest badge rendered');
  t.contains(html, 'YouTube · 2 tracks · browse MPREb_yt',
    'YouTube ingest summary rendered');
  t.contains(html, '#202 · YT #301', 'request and download log ids rendered');
}

t.section('renderAcquisitionItems() consumes only the exact processing owner');
{
  const html = renderAcquisitionItems([{
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
  t.contains(html, 'previewing', 'badge comes from durable owner state');
  t.contains(html, 'job #304', 'summary names exact owner');
  t.contains(html, '/api/import-jobs/304/recovery', 'row links exact owner recovery detail');
  t.excludes(html, '#9999', 'latest-job fallback is ignored');
  t.excludes(html, 'waiting for import', 'processing_started_at path inference is ignored');
}

t.section('loadRecents() consumes the combined Acquisition route without changing context');
{
  const content = { innerHTML: '' };
  const calls = [];
  const globals = stubGlobals({
    document: domStub({ 'recents-content': content }),
    fetch: async (url) => {
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
    },
  });
  state.recentsSub = 'acquisition';
  await loadRecents();
  t.equal(calls.join(','), '/api/pipeline/acquisition', 'only combined route is fetched');
  t.contains(content.innerHTML, 'waiting to import', 'processing acquisition is rendered');
  t.equal(state.recentsSub, 'acquisition', 'active subview is preserved');
  globals.restore();
}

t.section('renderRecentsItems() shows bad-extension postflight warning chip');
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
  t.contains(html, 'bad ext: 1', 'bad extension chip rendered');
  t.contains(html, '01 One Too Many Itches.bak',
    'bad extension filename appears in hover detail');
}

t.section('renderRecentsItems() shows the track-length warning chip (issue #1178)');
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
  t.contains(html, 'track length', 'track-length warning chip rendered');
  t.contains(html, esc(warning),
    'the full derived sentence appears in the chip hover detail');
  // The chip is a WARNING, not a proof badge — badge-verified is the
  // green "positive proof" class this same module uses for CD-rip proof;
  // an accusing-looking amber chip must never render with the green
  // class, and this asserts the exact class attribute, not merely that
  // 'badge-warn' appears SOMEWHERE (disambig/bad-ext chips share it too).
  t.contains(
    html,
    `class="badge badge-warn" title="${esc(warning)}">track length<`,
    'the chip renders with the badge-warn class, not badge-verified',
  );
}

t.section('renderRecentsItems() omits the track-length warning chip when the field is null');
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
  t.excludes(html, 'track length',
    'no track-length chip rendered when the field is null');
}

t.section('renderRecentsItems() uses the main badge and server-composed summary for deleted triage');
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
  t.contains(html, 'Wrong match (dist 0.190) · download deleted: spectral reject · moundsofass',
    'one server-composed summary keeps match verdict, cleanup disposition, and uploader');
  t.contains(html, 'Triaged · download deleted',
    'deleted triage is the primary row badge');
  t.contains(html, 'badge badge-rejected',
    'deleted triage remains visually rejected');
  t.excludes(html, 'recents-triage-label',
    'deleted triage does not render a second competing status label');
  t.contains(html, 'mp3_spectral:reject',
    'triage detail appears in hover text');
}

t.section('renderRecentsItems() uses an amber main badge for kept triage');
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
  t.contains(html, 'Triaged · download kept', 'kept triage is the primary row badge');
  t.contains(html, 'badge badge-warn', 'kept triage uses the amber badge class');
  t.excludes(html, 'recents-triage-label',
    'kept triage does not render a second competing status label');
}

t.section('renderRecentsItems() escapes wrong-match triage chip fields');
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
  t.contains(html, '&lt;img src=x&gt;',
    'triage summary is escaped');
  t.contains(html, 'stage:&lt;script&gt;',
    'triage detail is escaped');
  t.excludes(html, '<img src=x>',
    'raw triage summary is not rendered');
}

t.section('renderRecentsItems() does not mark rejected history as cleared wrong-matches');
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
  t.excludes(html, 'not in Wrong Matches',
    'ordinary rejected history row is not labelled as a wrong-match cleanup result');
}

t.section('renderRecentsItems() does not mark visible wrong-match rows as cleared');
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
  t.excludes(html, 'not in Wrong Matches',
    'actionable row with failed_path does not get cleared chip');
}

t.section('renderRecentsItems() shows the compact IN/HAVE evidence strip on quality rows');
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
  t.contains(html, 'r-evidence', 'evidence strip rendered on quality rows');
  t.contains(html, 'min 245k', 'incoming bitrate in strip, min-labelled');
  t.contains(html, 'min 320k', 'on-disk bitrate in strip');
}

t.section('renderRecentsItems() omits the evidence strip when a row has no measurements');
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
  t.excludes(html, 'r-evidence', 'no strip without measurements');
}

t.section('renderRecentsItems() surfaces retryable HAVE analysis failures');
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
  t.contains(html, 'border-left-color:#a86f20', 'environment border is distinct');
  t.contains(html, 'Environment failure', 'environment badge rendered');
  t.contains(html, 'permission denied', 'failure category is visible');
  t.contains(html, 'HAVE /mnt/Music/Beets/Low/&lt;current&gt;', 'installed path is visible and escaped');
  t.contains(html, 'candidate /mnt/Music/Incoming/candidate&amp;next', 'candidate reference is visible and escaped');
  t.contains(html, 'PermissionError: &lt;denied&gt;', 'raw analysis error is visible and escaped');
  t.contains(html, 'remains wanted', 'retryable state copy rendered');
  t.excludes(html, 'PermissionError: <denied>', 'raw error HTML is never rendered');
}

t.section('Recents row wires window.toggleDetail with (dl-<log id>, request_id)');
{
  const html = renderRecentsFixture([{
    id: 10,
    request_id: 41,
    request_status: 'wanted',
    created_at: '2026-08-03T12:00:00+00:00',
    album_title: 'Wired Album',
    artist_name: 'Wired Artist',
    badge: 'Imported',
    badge_class: 'badge-ok',
    border_color: '#2a2',
    summary: 'FLAC',
  }]);
  // Exact handler + argument order (#1110/#1241 argument-inversion class):
  // first the detail element key from the download_log id, then request_id.
  t.contains(html, "window.toggleDetail('dl-10', 41)",
    'recents row onclick carries (dl-<download_log id>, request_id) in order');
  t.contains(html, 'id="dl-10"',
    'detail placeholder id matches the toggle target');
}

t.section('Acquisition row wires window.toggleDetail with (acquisition-<id>, id)');
{
  const html = renderAcquisitionItems([{
    id: 55,
    status: 'wanted',
    album_title: 'Acq Album',
    artist_name: 'Acq Artist',
    created_at: '2026-08-03T12:00:00+00:00',
  }]);
  t.contains(html, "window.toggleDetail('acquisition-55', 55)",
    'acquisition row onclick carries (acquisition-<request id>, request id)');
  t.contains(html, 'id="acquisition-55"',
    'detail placeholder id matches the toggle target');

  // The YouTube arm is the case where the two arguments DIVERGE (detail
  // key from the yt download_log id, navigation by request id) — the
  // non-YouTube row above cannot distinguish a detailKey/item.id mix-up.
  const yt = renderAcquisitionItems([{
    id: 202,
    download_kind: 'youtube_ingest',
    download_log_id: 301,
    status: 'processing',
    album_title: 'YT Album',
    artist_name: 'YT Artist',
    created_at: '2026-08-03T12:00:00+00:00',
  }]);
  t.contains(yt, "window.toggleDetail('acquisition-youtube-301', 202)",
    'YouTube acquisition row keys the detail on the yt log id but navigates by request id');
  t.contains(yt, 'id="acquisition-youtube-301"',
    'YouTube detail placeholder id matches the toggle target');
}

// --- WE-3: loadRecents() is the scroll-restore completion boundary ---
//
// Mirrors the pipeline.js pin: search_plan.js::closeSearchPlanDetail
// stashes the origin scroll position for a recents-tab origin and
// leaves it un-consumed; loadRecents() is the real destination and
// consumes it only once its own fetch-driven render is done.

t.section('loadRecents() consumes a pending scroll restore only after its own render completes');
{
  const content = { innerHTML: '' };
  const scrollCalls = [];
  // Captured at the exact moment scrollTo fires -- proves the restore
  // lands after the FINAL DOM write, not merely after the fetch settles
  // (a mutant that moved the consume call to just before the last
  // `el.innerHTML =` would still pass a fetch-timing-only assertion).
  const htmlAtScrollTime = [];
  let resolveFetch;
  const fetchGate = new Promise((resolve) => { resolveFetch = resolve; });
  const globals = stubGlobals({
    document: domStub({ 'recents-content': content }),
    window: /** @type {any} */ ({
      scrollTo(_x, y) { scrollCalls.push(y); htmlAtScrollTime.push(content.innerHTML); },
      showTab() {},
    }),
    fetch: async () => {
      await fetchGate;
      return { ok: true, status: 200, async json() { return { log: [] }; } };
    },
  });
  state.recentsFilter = 'imported'; // avoid the 'all' fallback fetch branch
  state.searchPlanDetailContext = {
    requestId: 99, originTab: 'recents', originScrollY: 321, originSubView: 'history',
  };
  state.recentsSub = 'acquisition';
  closeSearchPlanDetail();
  t.equal(state.recentsSub, 'history', 'origin subView restored to history before loadRecents runs');
  t.equal(scrollCalls.length, 0,
    'closeSearchPlanDetail does not restore scroll itself for a recents origin');

  const rendered = loadRecents();
  await Promise.resolve();
  await Promise.resolve();
  t.equal(scrollCalls.length, 0,
    'scrollTo is not called while the recents log fetch is still pending');

  resolveFetch();
  await rendered;
  t.equal(scrollCalls.length, 1,
    'scrollTo is called exactly once after loadRecents finishes rendering');
  t.equal(scrollCalls[0], 321, 'restores the exact stashed scroll position');
  t.excludes(htmlAtScrollTime[0], 'Loading...',
    'scrollTo never fires while the loading placeholder is still showing');
  t.equal(htmlAtScrollTime[0], content.innerHTML,
    'the recents DOM was already in its final rendered state at the moment scrollTo fired');
  globals.restore();
}

t.section('loadRecents() consumes a pending scroll restore on its imports/acquisition early-return branches too');
// Reader re-read finding: the first version of this test only drove the
// `imports` subview -- `acquisition` shares the exact same early-return
// shape (closeSearchPlanDetail can restore `recentsSub='acquisition'`
// too, see the recents/acquisition origin block in
// tests/test_js_search_plan.mjs) and was still unpinned, so the
// symmetric mutant on that branch would have survived. Drive both.
for (const subView of ['imports', 'acquisition']) {
  // Mutant-runner finding (WE-3 review): the `finally` wraps the whole
  // function, but the log-fetch pin above only proves that ONE branch
  // converges on it -- hoisting either early-return branch out of the
  // `try` left every existing test in this suite green.
  // closeSearchPlanDetail restores `recentsSub` to exactly this origin
  // subView, which is the real trigger for this branch, so drive it
  // that way rather than setting recentsSub by hand.
  const content = { innerHTML: '' };
  const scrollCalls = [];
  const globals = stubGlobals({
    document: domStub({ 'recents-content': content }),
    window: /** @type {any} */ ({
      scrollTo(_x, y) { scrollCalls.push(y); },
      showTab() {},
    }),
    fetch: async () => (subView === 'imports'
      ? { ok: true, status: 200, async json() { return { jobs: [], counts: {} }; } }
      : { ok: true, status: 200, async json() { return { acquisition: [], youtube_ingest: [] }; } }),
  });
  state.searchPlanDetailContext = {
    requestId: 101, originTab: 'recents', originScrollY: 8765, originSubView: subView,
  };
  state.recentsSub = 'history';
  closeSearchPlanDetail();
  t.equal(state.recentsSub, subView,
    `origin subView restored to ${subView} before loadRecents runs`);
  await loadRecents();
  t.equal(scrollCalls.length, 1,
    `the ${subView} early-return branch also consumes the pending restore`);
  t.equal(scrollCalls[0], 8765,
    `restores the exact stashed scroll position on the ${subView} branch`);
  globals.restore();
}

t.done();
