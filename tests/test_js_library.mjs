/**
 * Unit tests for web/js/library.js pure helpers.
 * Run with: node tests/test_js_library.mjs
 */

import {
  banSourceConfirmationMessage,
  buildDeleteConfirmHtml,
  describeBanSourceSuccess,
  describeBeetsDeletion,
  renderLibraryAlbumRow,
  renderLibraryDetailBody,
} from '../web/js/library.js';
import { esc } from '../web/js/util.js';

let passed = 0;
let failed = 0;

function assert(condition, msg) {
  if (condition) passed++;
  else {
    failed++;
    console.error(`  FAIL: ${msg}`);
  }
}

function assertContains(haystack, needle, msg) {
  if (haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — '${needle}' not in output`);
  }
}

function assertExcludes(haystack, needle, msg) {
  if (!haystack.includes(needle)) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — unexpectedly found '${needle}'`);
  }
}

function assertEqual(actual, expected, msg) {
  if (actual === expected) passed++;
  else {
    failed++;
    console.error(`  FAIL: ${msg} — expected '${expected}', got '${actual}'`);
  }
}

function metadataHtmlIsEscaped(html, value) {
  return !html.includes(value) && html.includes(esc(value));
}

console.log('buildDeleteConfirmHtml() escapes user-visible text and JS args');
{
  const html = buildDeleteConfirmHtml(
    42,
    'Mum & Dad',
    "Kid A's <special>",
    10,
    1712,
    "rel-10'oops",
  );
  assertContains(html, 'Mum &amp; Dad - Kid A&#39;s &lt;special&gt;', 'artist/album escaped in overlay body');
  assertContains(html, 'window.executeBeetsDeletion(42, this, 1712, &quot;rel-10&#39;oops&quot;)', 'release id encoded as JS string arg');
  assertContains(html, 'matching pipeline request/history', 'pipeline note rendered when release id provided');
}

console.log('buildDeleteConfirmHtml() omits pipeline note without release id');
{
  const html = buildDeleteConfirmHtml(7, 'Bodyjar', 'Plastic Skies', 12, null, '');
  assertContains(html, 'window.executeBeetsDeletion(7, this, null, &quot;&quot;)', 'empty release id still encoded safely');
  assertExcludes(html, 'matching pipeline request/history', 'no pipeline note without release id');
}

console.log('delete result UI never presents incomplete cleanup as success');
{
  const incomplete = describeBeetsDeletion({
    error: 'delete_incomplete',
    detail: 'cover.jpg survived',
  });
  assertEqual(incomplete.completed, false, 'incomplete result does not refresh away evidence');
  assertEqual(incomplete.error, true, 'incomplete result is an error toast');
  assertContains(incomplete.message, 'cover.jpg survived', 'incomplete detail is visible');

  const lostAck = describeBeetsDeletion({
    error: 'delete_incomplete',
    acknowledgement_lost: true,
    album: 'Album', artist: 'Artist',
    former_album_path: '/music/Artist/Album',
    pipeline_id: 42, pipeline_status: 'imported',
    detail: 'Beets acknowledgement was lost; filesystem deletion is unconfirmed and Beets metadata may be gone. Do not assume files were deleted. Pipeline request #42 (imported) was preserved. Inspect the exact former album path "/music/Artist/Album" before explicit recovery.',
  });
  assertEqual(lostAck.completed, false, 'lost acknowledgement requires manual recovery');
  assertContains(lostAck.message, 'metadata may be gone', 'metadata ambiguity is explicit');
  assertContains(lostAck.message, 'Do not assume files were deleted', 'file deletion is not claimed');
  assertContains(lostAck.message, 'Pipeline request #42 (imported) was preserved', 'pipeline preservation is explicit');
  assertContains(lostAck.message, '/music/Artist/Album', 'exact recovery path is visible');

  const partial = describeBeetsDeletion({
    status: 'partial', album_deleted: true, pipeline_id: 42,
    preserved_paths: ['/music/A/B/booklet.pdf'],
    notifications: [{
      provider: 'jellyfin',
      status: 'warning',
      detail: 'exact album item jf-7 remains observable after refresh submission',
    }],
  });
  assertEqual(partial.completed, true, 'PG partial acknowledges album is already gone');
  assertEqual(partial.error, true, 'PG partial is not a normal success toast');
  assertContains(partial.message, 'pipeline request #42 remains', 'PG residual is actionable');
  assertContains(partial.message, '1 unknown path preserved', 'PG partial keeps preserved-path warning visible');
  assertContains(partial.message, '1 media notification warning', 'PG partial keeps media warning count visible');
  assertContains(partial.message, 'jellyfin: exact album item jf-7 remains observable', 'PG partial keeps media warning detail visible');
}

console.log('delete result UI surfaces unknown content and notifier warnings');
{
  const warning = describeBeetsDeletion({
    status: 'ok', artist: 'A', album: 'B', deleted_files: 2,
    deleted_artifacts: 4, pipeline_deleted: true,
    preserved_paths: ['/music/A/B/booklet.pdf'],
    notifications: [{
      provider: 'jellyfin', status: 'warning',
      detail: 'exact album item jf-7 remains observable',
    }],
  });
  assertEqual(warning.completed, true, 'verified delete still completes');
  assertEqual(warning.error, true, 'warning result gets warning styling');
  assertContains(warning.message, '1 unknown path preserved', 'unknown content count visible');
  assertContains(warning.message, '1 media notification warning', 'notifier warning count visible');
  assertContains(warning.message, 'jellyfin: exact album item jf-7 remains observable', 'notifier warning detail visible');
}

console.log('Bad Rip cleanup partial is never described as success');
{
  const partial = describeBanSourceSuccess({
    status: 'partial',
    error: 'cleanup_incomplete',
    request_status: 'wanted',
    username: 'peer',
    beets_removed: false,
    hashes_recorded: 12,
  });
  assertContains(partial, 'still in beets', 'retained album is explicit');
  assertExcludes(partial, 'not in beets', 'partial is not phrased as absence');
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

console.log('Bad Rip copy distinguishes requeue from preserved search stop');
{
  const confirmation = banSourceConfirmationMessage();
  assertContains(confirmation, 'remain unsearchable', 'confirmation explains preserved stop');
  assertContains(confirmation, 'reset to wanted', 'confirmation explains ordinary requeue');
  assertContains(
    describeBanSourceSuccess({
      request_status: 'unsearchable', username: 'bad-peer',
      beets_removed: true, hashes_recorded: 2,
    }),
    'remains unsearchable',
    'success copy reports the preserved search stop',
  );
  assertContains(
    describeBanSourceSuccess({
      request_status: 'wanted', username: null,
      beets_removed: false, hashes_recorded: 0,
    }),
    'requeued as wanted',
    'success copy reports the ordinary requeue',
  );
}

console.log('Library quality controls — adversarial deterministic release-id pin');
{
  const id = "release'\"\\</button><script>alert(1)</script>";
  const html = libraryDetail(id);
  const arg = expectedJsArg(id);
  assertContains(html, `window.setLibQuality(${arg}, 'wanted', null)`, 'wanted control encodes release id');
  assertContains(html, `window.setLibQuality(${arg}, 'unsearchable', null)`, 'unsearchable control encodes release id');
  assertContains(html, `window.setLibQuality(${arg}, null, parseInt(v))`, 'min-bitrate control encodes release id');
  assertExcludes(html, `window.setLibQuality('${id}'`, 'known-bad raw single-quoted interpolation is absent');
}

console.log('renderLibraryAlbumRow() preserves ordinary metadata presentation');
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
  assertContains(html, '<span>1989</span>', 'ordinary year remains visible');
  assertContains(html, '<span>US</span>', 'ordinary country remains visible');
  assertContains(html, '<span>Album</span>', 'ordinary release type remains visible');
}

console.log('renderLibraryAlbumRow() escapes controlled metadata at the live HTML sink');
{
  const knownBad = '<span><script>alert(1)</script></span>';
  assert(!metadataHtmlIsEscaped(knownBad, '<script>alert(1)</script>'),
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
      assert(metadataHtmlIsEscaped(html, year), `year escaped: ${JSON.stringify(year)}`);
      assert(metadataHtmlIsEscaped(html, country), `country escaped: ${JSON.stringify(country)}`);
      assert(metadataHtmlIsEscaped(html, type), `type escaped: ${JSON.stringify(type)}`);
    }
  }
}

console.log('renderLibraryAlbumRow() escapes format metadata passed to status badges');
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
  assertContains(html, 'in library · &lt;/SPAN&gt;&lt;IMG SRC=X ONERROR=ALERT(1)&gt;',
    'format-derived badge label is escaped in the real library row');
  assertExcludes(html, formats.toUpperCase(),
    'format metadata cannot inject markup through the library row');
}

console.log('renderLibraryDetailBody() preserves ordinary track and pipeline metadata');
{
  const html = libraryDetail('release-id', 'wanted', {
    pipeline_source: 'request',
    tracks: [{ track: 1, title: 'Track', format: 'FLAC', bitrate: 320000 }],
  });
  assertContains(html, 'FLAC 320kbps',
    'ordinary per-track format remains visible through the Library detail path');
  assertContains(html, '<span class="p-detail-value">wanted (request)</span>',
    'ordinary empty-history pipeline status and source remain visible');
}

console.log('renderLibraryDetailBody() escapes track format and empty-history pipeline metadata');
{
  const hostile = '</span><img src=x onerror=alert(1)>';
  const html = libraryDetail('release-id', hostile, {
    pipeline_source: hostile,
    tracks: [{ track: 1, title: 'Track', format: hostile }],
  });
  const escaped = esc(hostile);
  assertExcludes(html, hostile,
    'Library detail cannot emit raw track format, pipeline status, or pipeline source markup');
  assertContains(html, `${escaped} (${escaped})`,
    'empty-history pipeline status and source are escaped at their HTML boundary');
  assertContains(html, `<span class="lib-track-meta">${escaped}</span>`,
    'track format is escaped at the shared row boundary through Library detail');
}

console.log('Library quality controls — generated critical-character property sweep');
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
    assertContains(html, `window.setLibQuality(${arg},`, `library ID encoded: ${JSON.stringify(id)}`);
    if (encodedCalls !== 5) {
      failed++;
      console.error(`  FAIL: all five quality controls encode ${JSON.stringify(id)} — got ${encodedCalls}`);
    } else {
      passed++;
    }
  }

  const badId = "break'out";
  const oldHandler = `window.setLibQuality('${badId}', 'wanted', null)`;
  let oldCompiles = true;
  try { new Function('window', oldHandler); } catch (_) { oldCompiles = false; }
  if (!oldCompiles) passed++;
  else { failed++; console.error('  FAIL: known-bad raw library interpolation unexpectedly compiles'); }
}

console.log('Library status controls disable invalid unsearchable transitions');
{
  const imported = libraryDetail('release-id', 'imported');
  assertContains(imported, "class=\"p-btn active-status\" onclick=\"event.stopPropagation(); window.setLibQuality(&quot;release-id&quot;, 'imported', null)\">imported</button>", 'imported remains visibly current');
  assertExcludes(imported, "window.setLibQuality(&quot;release-id&quot;, 'unsearchable'", 'imported cannot invoke unsearchable');
  assertContains(imported, 'disabled aria-disabled="true">unsearchable</button>', 'invalid imported stop is disabled');

  const downloading = libraryDetail('release-id', 'downloading');
  assertContains(downloading, 'disabled aria-disabled="true">downloading</button>', 'downloading remains visibly current');
  assertExcludes(downloading, "window.setLibQuality(&quot;release-id&quot;, 'unsearchable'", 'downloading cannot invoke unsearchable');
  assertContains(downloading, 'disabled aria-disabled="true">unsearchable</button>', 'invalid downloading stop is disabled');

  const wanted = libraryDetail('release-id', 'wanted');
  assertContains(wanted, "window.setLibQuality(&quot;release-id&quot;, 'unsearchable', null)", 'wanted may become unsearchable');

  const stopped = libraryDetail('release-id', 'unsearchable');
  assertContains(stopped, "window.setLibQuality(&quot;release-id&quot;, 'unsearchable', null)", 'current unsearchable state remains an active control');
  assertContains(stopped, 'class="p-btn active-status"', 'unsearchable remains visibly current');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
