/**
 * Unit tests for web/js/library.js pure helpers.
 * Run with: node tests/test_js_library.mjs
 */

import {
  buildDeleteConfirmHtml,
  describeBeetsDeletion,
  renderLibraryDetailBody,
} from '../web/js/library.js';

let passed = 0;
let failed = 0;

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

function libraryDetail(releaseId) {
  return renderLibraryDetailBody({
    mb_albumid: releaseId,
    pipeline_id: 1712,
    pipeline_status: 'wanted',
    pipeline_source: 'request',
    artist: 'Artist',
    album: 'Album',
    tracks: [],
  }, 42);
}

console.log('Library quality controls — adversarial deterministic release-id pin');
{
  const id = "release'\"\\</button><script>alert(1)</script>";
  const html = libraryDetail(id);
  const arg = expectedJsArg(id);
  assertContains(html, `window.setLibQuality(${arg}, 'wanted', null)`, 'wanted control encodes release id');
  assertContains(html, `window.setLibQuality(${arg}, 'manual', null)`, 'manual control encodes release id');
  assertContains(html, `window.setLibQuality(${arg}, null, parseInt(v))`, 'min-bitrate control encodes release id');
  assertExcludes(html, `window.setLibQuality('${id}'`, 'known-bad raw single-quoted interpolation is absent');
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

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
