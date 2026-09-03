/**
 * Unit tests for web/js/library.js pure helpers.
 * Run with: node tests/test_js_library.mjs
 */

import {
  banSourceConfirmationMessage,
  buildDeleteConfirmHtml,
  describeBanSourceSuccess,
  describeBeetsDeletion,
  executeBeetsDeletion,
  libraryAlbumBadgeItem,
  renderLibraryAlbumRow,
  renderLibraryDetailBody,
  setLibQuality,
} from '../web/js/library.js';
import { esc } from '../web/js/util.js';
import { pipelineStore, state, updatePipelineStatus } from '../web/js/state.js';

import {
  suite, stubGlobals, element, domStub,
} from './js_harness.mjs';

const t = suite(import.meta.url);

function metadataHtmlIsEscaped(html, value) {
  return !html.includes(value) && html.includes(esc(value));
}

t.section('libraryAlbumBadgeItem() maps the production row contract exactly');
{
  const processingOwner = {
    job_id: 812,
    status: 'queued',
    preview_status: 'evidence_ready',
  };
  const item = libraryAlbumBadgeItem({
    mb_albumid: '0012856590',
    in_library: true,
    has_captured_history: true,
    formats: 'Opus',
    min_bitrate: 101600,
    avg_bitrate: 131999,
    library_rank: 'transparent',
    pipeline_status: 'processing',
    pipeline_id: null,
    processing_owner: processingOwner,
    pipeline_verified_lossless: true,
    pipeline_provisional: false,
  });
  t.equal(JSON.stringify(item), JSON.stringify({
    id: '12856590',
    in_library: true,
    has_captured_history: true,
    library_format: 'Opus',
    library_min_bitrate: 102,
    library_avg_bitrate: 131,
    library_rank: 'transparent',
    pipeline_status: 'processing',
    pipeline_id: null,
    processing_owner: processingOwner,
    pipeline_verified_lossless: true,
    pipeline_provisional: false,
  }), 'helper exposes the exact BadgeItem consumed by production');
}

t.section('buildDeleteConfirmHtml() escapes user-visible text and JS args');
{
  const html = buildDeleteConfirmHtml(
    42,
    'Mum & Dad',
    "Kid A's <special>",
    10,
    1712,
    "rel-10'oops",
  );
  t.contains(html, 'Mum &amp; Dad - Kid A&#39;s &lt;special&gt;', 'artist/album escaped in overlay body');
  t.contains(html, 'window.executeBeetsDeletion(42, this, 1712, &quot;rel-10&#39;oops&quot;)', 'release id encoded as JS string arg');
  t.contains(html, 'matching pipeline request/history', 'pipeline note rendered when release id provided');
}

t.section('buildDeleteConfirmHtml() omits pipeline note without release id');
{
  const html = buildDeleteConfirmHtml(7, 'Bodyjar', 'Plastic Skies', 12, null, '');
  t.contains(html, 'window.executeBeetsDeletion(7, this, null, &quot;&quot;)', 'empty release id still encoded safely');
  t.excludes(html, 'matching pipeline request/history', 'no pipeline note without release id');
}

t.section('delete result UI never presents incomplete cleanup as success');
{
  const incomplete = describeBeetsDeletion({
    error: 'delete_incomplete',
    detail: 'cover.jpg survived',
  });
  t.equal(incomplete.completed, false, 'incomplete result does not refresh away evidence');
  t.equal(incomplete.error, true, 'incomplete result is an error toast');
  t.contains(incomplete.message, 'cover.jpg survived', 'incomplete detail is visible');

  const lostAck = describeBeetsDeletion({
    error: 'delete_incomplete',
    acknowledgement_lost: true,
    album: 'Album', artist: 'Artist',
    former_album_path: '/music/Artist/Album',
    pipeline_id: 42, pipeline_status: 'imported',
    detail: 'Beets acknowledgement was lost; filesystem deletion is unconfirmed and Beets metadata may be gone. Do not assume files were deleted. Pipeline request #42 (imported) was preserved. Inspect the exact former album path "/music/Artist/Album" before explicit recovery.',
  });
  t.equal(lostAck.completed, false, 'lost acknowledgement requires manual recovery');
  t.contains(lostAck.message, 'metadata may be gone', 'metadata ambiguity is explicit');
  t.contains(lostAck.message, 'Do not assume files were deleted', 'file deletion is not claimed');
  t.contains(lostAck.message, 'Pipeline request #42 (imported) was preserved', 'pipeline preservation is explicit');
  t.contains(lostAck.message, '/music/Artist/Album', 'exact recovery path is visible');

  const partial = describeBeetsDeletion({
    status: 'partial', album_deleted: true, pipeline_id: 42,
    preserved_paths: ['/music/A/B/booklet.pdf'],
    notifications: [{
      provider: 'jellyfin',
      status: 'warning',
      // The producible detail shape since #1221 item 1 (see
      // lib/library_delete_notifiers.py's found-item branch) — the old
      // 'remains observable after refresh submission' trigger has no
      // producer any more.
      detail: "exact album item jf-7 found at former path '/music/A/B' but NOT refreshed — a targeted refresh cannot reap a vanished item; Jellyfin's own next library validation reaps it",
    }],
  });
  t.equal(partial.completed, true, 'PG partial acknowledges album is already gone');
  t.equal(partial.error, true, 'PG partial is not a normal success toast');
  t.contains(partial.message, 'pipeline request #42 remains', 'PG residual is actionable');
  t.contains(partial.message, '1 unknown path preserved', 'PG partial keeps preserved-path warning visible');
  t.contains(partial.message, '1 media notification warning', 'PG partial keeps media warning count visible');
  t.contains(partial.message, 'jellyfin: exact album item jf-7 found at former path', 'PG partial keeps media warning detail visible');
}

t.section('delete result UI surfaces unknown content and notifier warnings');
{
  const warning = describeBeetsDeletion({
    status: 'ok', artist: 'A', album: 'B', deleted_files: 2,
    deleted_artifacts: 4, pipeline_deleted: true,
    preserved_paths: ['/music/A/B/booklet.pdf'],
    notifications: [{
      provider: 'jellyfin', status: 'warning',
      // Producible shape since #1221 item 1 (found-item branch of
      // lib/library_delete_notifiers.py).
      detail: "exact album item jf-7 found at former path '/music/A/B' but NOT refreshed — a targeted refresh cannot reap a vanished item; Jellyfin's own next library validation reaps it",
    }],
  });
  t.equal(warning.completed, true, 'verified delete still completes');
  t.equal(warning.error, true, 'warning result gets warning styling');
  t.contains(warning.message, '1 unknown path preserved', 'unknown content count visible');
  t.contains(warning.message, '1 media notification warning', 'notifier warning count visible');
  t.contains(warning.message, 'jellyfin: exact album item jf-7 found at former path', 'notifier warning detail visible');
}

t.section('Bad Rip cleanup partial is never described as success');
{
  const partial = describeBanSourceSuccess({
    status: 'partial',
    error: 'cleanup_incomplete',
    request_status: 'wanted',
    username: 'peer',
    beets_removed: false,
    hashes_recorded: 12,
  });
  t.contains(partial, 'still in beets', 'retained album is explicit');
  t.excludes(partial, 'not in beets', 'partial is not phrased as absence');
}

/** Independent expected encoder: JSON JS literal, then HTML attribute escaping. */
function expectedJsArg(value) {
  return JSON.stringify(String(value))
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/\\/g, '&#92;');
}

function libraryDetail(releaseId, pipelineStatus = 'wanted', overrides = {}) {
  return renderLibraryDetailBody({
    mb_albumid: releaseId,
    pipeline_id: 1712,
    pipeline_status: pipelineStatus,
    pipeline_source: 'request',
    artist: 'Artist',
    album: 'Album',
    tracks: [],
    ...overrides,
  }, 42);
}

t.section('Bad Rip copy distinguishes requeue from preserved search stop');
{
  const confirmation = banSourceConfirmationMessage();
  t.contains(confirmation, 'remain unsearchable', 'confirmation explains preserved stop');
  t.contains(confirmation, 'reset to wanted', 'confirmation explains ordinary requeue');
  t.contains(
    describeBanSourceSuccess({
      request_status: 'unsearchable', username: 'bad-peer',
      beets_removed: true, hashes_recorded: 2,
    }),
    'remains unsearchable',
    'success copy reports the preserved search stop',
  );
  t.contains(
    describeBanSourceSuccess({
      request_status: 'wanted', username: null,
      beets_removed: false, hashes_recorded: 0,
    }),
    'requeued as wanted',
    'success copy reports the ordinary requeue',
  );
}

t.section('Library quality controls — adversarial deterministic release-id pin');
{
  const id = "release'\"\\</button><script>alert(1)</script>";
  const html = libraryDetail(id);
  const arg = expectedJsArg(id);
  t.contains(html, `window.setLibQuality(${arg}, 'wanted', null)`, 'wanted control encodes release id');
  t.contains(html, `window.setLibQuality(${arg}, 'unsearchable', null)`, 'unsearchable control encodes release id');
  t.contains(html, `window.setLibQuality(${arg}, null, parseInt(v))`, 'min-bitrate control encodes release id');
  t.excludes(html, `window.setLibQuality('${id}'`, 'known-bad raw single-quoted interpolation is absent');
}

t.section('renderLibraryAlbumRow() preserves ordinary metadata presentation');
{
  const html = renderLibraryAlbumRow({
    id: 42,
    album: 'Let Love Rule',
    year: 1989,
    country: 'US',
    type: 'Album',
    track_count: 13,
    in_library: false,
    pipeline_id: 17,
  });
  t.contains(html, '<span>1989</span>', 'ordinary year remains visible');
  t.contains(html, '<span>US</span>', 'ordinary country remains visible');
  t.contains(html, '<span>Album</span>', 'ordinary release type remains visible');
}

t.section('Converged Library release has one detailed stop prompt');
{
  const convergence = {
    request_id: 1712,
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
  };
  const row = renderLibraryAlbumRow({
    id: 42,
    album: 'Provisional Album',
    track_count: 10,
    in_library: true,
    beets_album_id: 42,
    pipeline_id: 1712,
    pipeline_status: 'wanted',
    convergence,
  });
  t.contains(row, 'search converged', 'compact Library row keeps signal badge');
  t.excludes(row, 'convergence-prompt', 'compact Library row has no duplicate prompt');
  t.excludes(row, 'Stop searching', 'compact Library row has no stop action');

  const detail = libraryDetail('release-id', 'wanted', { convergence });
  t.ok((detail.match(/class="convergence-prompt"/g) || []).length === 1,
    'expanded Library detail has exactly one convergence prompt');
  t.ok((detail.match(/>Stop searching<\/button>/g) || []).length === 1,
    'expanded Library detail has exactly one stop action');
  t.excludes(detail, '>Accept</button>',
    'convergence stop is not presented beside irreversible-looking Accept');
  t.excludes(detail, '>Status:</span>',
    'generic lifecycle status controls are suppressed beside convergence stop');
  t.excludes(detail, '>Min bitrate:</span>',
    'quality override controls are suppressed beside convergence stop');
  t.excludes(detail, '>Intent:</span>',
    'intent controls are suppressed beside convergence stop');
}

t.section('renderLibraryAlbumRow() escapes controlled metadata at the live HTML sink');
{
  const knownBad = '<span><script>alert(1)</script></span>';
  t.ok(!metadataHtmlIsEscaped(knownBad, '<script>alert(1)</script>'),
    'metadata escape checker rejects known-bad raw HTML');

  const atoms = ['<', '>', '&', '"', "'", '\\'];
  for (const left of atoms) {
    for (const right of atoms) {
      const year = `year${left}${right}tail`;
      const country = `country${left}${right}tail`;
      const type = `type${left}${right}tail`;
      const html = renderLibraryAlbumRow({
        id: 42,
        album: 'Album',
        year,
        country,
        type,
        track_count: 1,
        in_library: false,
        pipeline_id: 17,
      });
      t.ok(metadataHtmlIsEscaped(html, year), `year escaped: ${JSON.stringify(year)}`);
      t.ok(metadataHtmlIsEscaped(html, country), `country escaped: ${JSON.stringify(country)}`);
      t.ok(metadataHtmlIsEscaped(html, type), `type escaped: ${JSON.stringify(type)}`);
    }
  }
}

t.section('renderLibraryAlbumRow() escapes format metadata passed to status badges');
{
  const formats = '</span><img src=x onerror=alert(1)>';
  const html = renderLibraryAlbumRow({
    id: 42,
    album: 'Album',
    formats,
    track_count: 1,
    in_library: true,
    beets_album_id: 42,
  });
  t.contains(html, '>in library</span>',
    'library presence stays separate from quality');
  t.contains(html, '>&lt;/SPAN&gt;&lt;IMG SRC=X ONERROR=ALERT(1)&gt;</span>',
    'format-derived badge label is escaped in the real library row');
  t.excludes(html, formats.toUpperCase(),
    'format metadata cannot inject markup through the library row');
}

t.section('renderLibraryAlbumRow() uses the shared independent fact vocabulary');
{
  const missing = renderLibraryAlbumRow({
    id: 17,
    mb_albumid: 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    album: 'Captured pressing',
    track_count: 10,
    in_library: false,
    has_captured_history: true,
    pipeline_id: 17,
    pipeline_status: 'wanted',
    pipeline_verified_lossless: true,
    pipeline_provisional: false,
  });
  t.contains(missing, '>captured<', 'Library row renders acquisition history');
  t.contains(missing, '>missing<', 'Library row renders proven current absence');
  t.contains(missing, '>verified<', 'Library row carries proof independently');
  t.contains(missing, '>wanted<', 'Library row retains current lifecycle');

  const untracked = renderLibraryAlbumRow({
    id: 42,
    mb_albumid: 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
    album: 'Operator retag',
    formats: 'FLAC',
    avg_bitrate: 900000,
    library_rank: 'lossless',
    track_count: 10,
    in_library: true,
    has_captured_history: false,
    beets_album_id: 42,
    pipeline_id: null,
    pipeline_status: null,
    pipeline_verified_lossless: false,
    pipeline_provisional: false,
  });
  t.contains(untracked, '>in library</span>', 'Library row renders current holding');
  t.contains(untracked, '>F</span>', 'Library row renders current quality separately');
  t.contains(untracked, '>untracked<', 'Library row renders missing exact tracking');
  t.excludes(untracked, 'identity drift', 'Library row does not infer a sibling relationship');
}

t.section('renderLibraryAlbumRow() acknowledges one complete lifecycle before actions and badges');
{
  pipelineStore.clear();
  const releaseId = 'cccccccc-cccc-cccc-cccc-cccccccccccc';
  const owner = {
    job_id: 303,
    status: 'recovery_required',
    preview_status: 'evidence_ready',
  };
  updatePipelineStatus(releaseId, 'processing', 53, owner);
  const html = renderLibraryAlbumRow({
    id: 53,
    mb_albumid: releaseId,
    album: 'Recovering pressing',
    track_count: 10,
    in_library: false,
    has_captured_history: true,
    pipeline_id: 53,
    pipeline_status: 'processing',
    processing_owner: owner,
    pipeline_verified_lossless: true,
    pipeline_provisional: false,
  });
  t.contains(html, 'data-processing-locked="true"',
    'the toolbar consumes the acknowledged exact owner before choosing actions');
  t.contains(html, '>needs recovery<',
    'the toolbar and badge agree on the exact owner presentation');
  t.contains(html, '>captured<',
    'the same acknowledgement restores authoritative row facts');
  t.contains(html, '>verified<',
    'proof and lifecycle cross the acknowledgement boundary together');
  t.ok(!pipelineStore.has(releaseId),
    'the complete authoritative lifecycle expires the local overlay once');
  pipelineStore.clear();
}

t.section('renderLibraryDetailBody() preserves ordinary track and pipeline metadata');
{
  const html = libraryDetail('release-id', 'wanted', {
    pipeline_source: 'request',
    tracks: [{ track: 1, title: 'Track', format: 'FLAC', bitrate: 320000 }],
  });
  t.contains(html, 'FLAC 320kbps',
    'ordinary per-track format remains visible through the Library detail path');
  t.contains(html, '<span class="p-detail-value">wanted (request)</span>',
    'ordinary empty-history pipeline status and source remain visible');
}

t.section('renderLibraryDetailBody() escapes track format and empty-history pipeline metadata');
{
  const hostile = '</span><img src=x onerror=alert(1)>';
  const html = libraryDetail('release-id', hostile, {
    pipeline_source: hostile,
    tracks: [{ track: 1, title: 'Track', format: hostile }],
  });
  const escaped = esc(hostile);
  t.excludes(html, hostile,
    'Library detail cannot emit raw track format, pipeline status, or pipeline source markup');
  t.contains(html, `${escaped} (${escaped})`,
    'empty-history pipeline status and source are escaped at their HTML boundary');
  t.contains(html, `<span class="lib-track-meta">${escaped}</span>`,
    'track format is escaped at the shared row boundary through Library detail');
}

t.section('Library quality controls — generated critical-character property sweep');
{
  const atoms = ['a', "'", '"', '\\', '<', '>', '&', '\n', '\u2028'];
  const ids = ['plain-id'];
  for (const left of atoms) {
    for (const right of atoms) ids.push(`id${left}${right}tail`);
  }
  for (const id of ids) {
    const html = libraryDetail(id);
    const arg = expectedJsArg(id);
    const encodedCalls = html.split(`window.setLibQuality(${arg},`).length - 1;
    t.contains(html, `window.setLibQuality(${arg},`, `library ID encoded: ${JSON.stringify(id)}`);
    t.equal(encodedCalls, 5, `all five quality controls encode ${JSON.stringify(id)}`);
  }

  const badId = "break'out";
  const oldHandler = `window.setLibQuality('${badId}', 'wanted', null)`;
  let oldCompiles = true;
  try { new Function('window', oldHandler); } catch (_) { oldCompiles = false; }
  t.notOk(oldCompiles, 'known-bad raw library interpolation unexpectedly compiles');
}

t.section('Library status controls disable invalid unsearchable transitions');
{
  const imported = libraryDetail('release-id', 'imported');
  t.contains(imported, "class=\"p-btn active-status\" data-pipeline-request-id=\"1712\" onclick=\"event.stopPropagation(); window.setLibQuality(&quot;release-id&quot;, 'imported', null)\">imported</button>", 'imported remains visibly current and conflict-addressable');
  t.excludes(imported, "window.setLibQuality(&quot;release-id&quot;, 'unsearchable'", 'imported cannot invoke unsearchable');
  t.contains(imported, 'disabled aria-disabled="true">unsearchable</button>', 'invalid imported stop is disabled');

  const downloading = libraryDetail('release-id', 'downloading');
  t.contains(downloading, 'disabled aria-disabled="true">downloading</button>', 'downloading remains visibly current');
  t.excludes(downloading, "window.setLibQuality(&quot;release-id&quot;, 'unsearchable'", 'downloading cannot invoke unsearchable');
  t.contains(downloading, 'disabled aria-disabled="true">unsearchable</button>', 'invalid downloading stop is disabled');

  const processing = libraryDetail('release-id', 'processing', {
    processing_owner: {
      job_id: 1713,
      status: 'queued',
      preview_status: 'evidence_ready',
    },
  });
  t.contains(processing, 'aria-disabled="true"', 'processing controls expose disabled semantics');
  t.contains(processing, 'aria-describedby=', 'processing controls name visible explanation');
  t.contains(processing, 'job #1713 is waiting to import', 'processing explanation names exact owner');
  t.contains(processing, '/api/import-jobs/1713/recovery', 'processing links exact recovery detail');
  t.excludes(processing, ' disabled', 'processing controls remain focusable');
  t.excludes(processing, 'window.setLibQuality', 'processing controls cannot mutate lifecycle');
  t.excludes(processing, 'window.setIntent', 'processing intent remains locked');
  t.excludes(processing, 'window.confirmDeleteBeets', 'processing beets deletion remains locked');
  t.excludes(processing, 'window.banSource', 'processing source ban remains locked');

  const wanted = libraryDetail('release-id', 'wanted');
  t.contains(wanted, "window.setLibQuality(&quot;release-id&quot;, 'unsearchable', null)", 'wanted may become unsearchable');

  const stopped = libraryDetail('release-id', 'unsearchable');
  t.contains(stopped, "window.setLibQuality(&quot;release-id&quot;, 'unsearchable', null)", 'current unsearchable state remains an active control');
  t.contains(stopped, 'class="p-btn active-status"', 'unsearchable remains visibly current');
}

t.section('renderLibraryAlbumRow() wires pipeline-only rows through window.toggleDetail');
{
  const html = renderLibraryAlbumRow({
    id: 42,
    album: 'Pipeline Only',
    track_count: 3,
    in_library: false,
    pipeline_id: 17,
  });
  // Exact handler + argument order (#1110/#1241 argument-inversion class).
  t.contains(html, "window.toggleDetail('lib-pipeline-17', 17)",
    'pipeline-only row onclick carries (lib-pipeline-<id>, pipeline id) in order');
  t.contains(html, 'id="lib-pipeline-17"',
    'detail placeholder id matches the toggle target');

  const fallback = renderLibraryAlbumRow({
    id: 42,
    album: 'No Pipeline Id',
    track_count: 3,
    in_library: false,
  });
  t.contains(fallback, "window.toggleDetail('lib-pipeline-42', 42)",
    'row without a pipeline_id falls back to the library row id for both arguments');
}

t.section('renderLibraryAlbumRow() wires in-library rows through window.toggleLibDetail');
{
  const html = renderLibraryAlbumRow({
    id: 42,
    album: 'In Library',
    track_count: 3,
    in_library: true,
    beets_album_id: 99,
  });
  t.contains(html, 'window.toggleLibDetail(99)',
    'in-library row onclick keys on the beets album id');
  t.contains(html, 'id="lib-99"',
    'detail placeholder id matches the toggle target');
}

t.section('executeBeetsDeletion() refreshes by the active tab\'s data-tab-name, not its visible label');

/**
 * @param {any|null} activeTab
 */
function docWithActiveTab(activeTab) {
  return domStub({}, {
    querySelector(sel) {
      if (sel === '.tab.active') return activeTab;
      if (sel === '.confirm-overlay') return null;
      return null;
    },
  });
}

{
  // Recents is the active tab (by data-tab-name, not by rendered label
  // text — issue #1355 WE4 removed the '.tab.active'.textContent.trim()
  // === 'Recents' comparison).
  const active = element({ className: 'tab active' });
  active.setAttribute('data-tab-name', 'recents');
  let recentsCalls = 0;
  let pipelineCalls = 0;
  const globals = stubGlobals({
    document: docWithActiveTab(active),
    fetch: () => Promise.resolve({
      status: 200,
      json: async () => ({
        status: 'ok', artist: 'A', album: 'B', deleted_files: 1, deleted_artifacts: 0,
      }),
    }),
    window: { loadRecents: () => { recentsCalls += 1; }, loadPipeline: () => { pipelineCalls += 1; } },
  });
  try {
    await executeBeetsDeletion(1, element());
    t.equal(recentsCalls, 1, "executeBeetsDeletion refreshes via window.loadRecents when the active tab's data-tab-name is 'recents'");
    t.equal(pipelineCalls, 0, 'and does not also call window.loadPipeline');
  } finally {
    globals.restore();
  }
}

{
  // Pipeline is the active tab.
  const active = element({ className: 'tab active' });
  active.setAttribute('data-tab-name', 'pipeline');
  let recentsCalls = 0;
  let pipelineCalls = 0;
  const globals = stubGlobals({
    document: docWithActiveTab(active),
    fetch: () => Promise.resolve({
      status: 200,
      json: async () => ({
        status: 'ok', artist: 'A', album: 'B', deleted_files: 1, deleted_artifacts: 0,
      }),
    }),
    window: { loadRecents: () => { recentsCalls += 1; }, loadPipeline: () => { pipelineCalls += 1; } },
  });
  try {
    await executeBeetsDeletion(1, element());
    t.equal(pipelineCalls, 1, "executeBeetsDeletion refreshes via window.loadPipeline when the active tab's data-tab-name is 'pipeline'");
    t.equal(recentsCalls, 0, 'and does not also call window.loadRecents');
  } finally {
    globals.restore();
  }
}

{
  // Wrong Matches ('manual') is active: neither the Recents nor the
  // Pipeline refresh fires -- refreshAfterBeetsDeletion only special-cases
  // those two, same as before this item's attribute-based rewrite.
  const active = element({ className: 'tab active' });
  active.setAttribute('data-tab-name', 'manual');
  let recentsCalls = 0;
  let pipelineCalls = 0;
  const globals = stubGlobals({
    document: docWithActiveTab(active),
    fetch: () => Promise.resolve({
      status: 200,
      json: async () => ({
        status: 'ok', artist: 'A', album: 'B', deleted_files: 1, deleted_artifacts: 0,
      }),
    }),
    window: { loadRecents: () => { recentsCalls += 1; }, loadPipeline: () => { pipelineCalls += 1; } },
  });
  try {
    await executeBeetsDeletion(1, element());
    t.equal(recentsCalls, 0, "executeBeetsDeletion does not call window.loadRecents when the active tab is 'manual'");
    t.equal(pipelineCalls, 0, 'nor window.loadPipeline');
  } finally {
    globals.restore();
  }
}

t.section('setLibQuality() refreshes Recents by data-tab-name, not visible label text');

{
  const active = element({ className: 'tab active' });
  active.setAttribute('data-tab-name', 'recents');
  let recentsCalls = 0;
  const globals = stubGlobals({
    document: docWithActiveTab(active),
    fetch: () => Promise.resolve({ status: 200, json: async () => ({ status: 'ok' }) }),
    window: { loadRecents: () => { recentsCalls += 1; } },
  });
  const prevBrowseArtist = state.browseArtist;
  try {
    state.browseArtist = null;
    await setLibQuality('release-id', 'wanted', null);
    t.equal(recentsCalls, 1, "setLibQuality refreshes via window.loadRecents when the active tab's data-tab-name is 'recents'");
  } finally {
    state.browseArtist = prevBrowseArtist;
    globals.restore();
  }
}

{
  const active = element({ className: 'tab active' });
  active.setAttribute('data-tab-name', 'pipeline');
  let recentsCalls = 0;
  const globals = stubGlobals({
    document: docWithActiveTab(active),
    fetch: () => Promise.resolve({ status: 200, json: async () => ({ status: 'ok' }) }),
    window: { loadRecents: () => { recentsCalls += 1; } },
  });
  const prevBrowseArtist = state.browseArtist;
  try {
    state.browseArtist = null;
    await setLibQuality('release-id', 'wanted', null);
    t.equal(recentsCalls, 0, "setLibQuality does not call window.loadRecents when the active tab is 'pipeline'");
  } finally {
    state.browseArtist = prevBrowseArtist;
    globals.restore();
  }
}

t.done();
