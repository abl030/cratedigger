/**
 * Unit tests for web/js/pipeline.js navigation/detail helpers.
 * Run with: node tests/test_js_pipeline.mjs
 */

import { __test__, mergeRekeyRequest } from '../web/js/pipeline.js';
import { state } from '../web/js/state.js';

let passed = 0;
let failed = 0;

function assert(condition, msg) {
  if (condition) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg}`);
  }
}

function assertEqual(actual, expected, msg) {
  if (actual === expected) {
    passed++;
  } else {
    failed++;
    console.error(`  FAIL: ${msg} — expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

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

/**
 * Drain pending microtasks — `mergeRekeyRequest`'s success path kicks off
 * `loadPipelineDashboard()` fire-and-forget (`void loadPipelineDashboard()`,
 * never awaited), so a test asserting the dashboard reload happened must
 * let that background chain settle first. Mirrors
 * `tests/test_js_wrong_matches.mjs::flushMicrotasks`.
 * @param {number} [times]
 */
async function flushMicrotasks(times = 30) {
  for (let i = 0; i < times; i += 1) {
    await Promise.resolve();
  }
}

/**
 * DOM stand-in for the merge-rekey drift row (#1089): `pipeline-content`
 * (read by `loadPipelineDashboard`'s loading/failure states), `toast`, and
 * one `drift-note-<id>` element — the exact three ids
 * `mergeRekeyRequest`/`loadPipelineDashboard` look up by id.
 */
function installDriftDom(requestId) {
  const pipelineContent = { innerHTML: '' };
  const toast = { textContent: '', className: '', style: { display: 'none' } };
  const note = { textContent: '', className: '' };
  const elements = new Map([
    ['pipeline-content', pipelineContent],
    ['toast', toast],
    [`drift-note-${requestId}`, note],
  ]);
  globalThis.document = {
    getElementById(id) {
      return elements.has(id) ? elements.get(id) : null;
    },
  };
  globalThis.setTimeout = (fn) => {
    fn();
    return 0;
  };
  return { pipelineContent, toast, note };
}

console.log('renderPipelineNav() has operational views only');
{
  state.pipelineView = 'dashboard';
  const html = __test__.renderPipelineNav();
  assertExcludes(html, 'window.setPipelineView(\'queue\')', 'request queue tab removed');
  assertContains(html, 'window.setPipelineView(\'dashboard\')', 'dashboard tab rendered');
  assertContains(html, 'window.setPipelineView(\'long-tail\')', 'long-tail tab rendered');
  assertContains(html, 'window.loadPipelineDashboard()', 'dashboard refresh reloads metrics');
  assertContains(html, 'subtab-refresh', 'refresh uses shared subtab layout');
}
console.log('renderPipelineNav() refreshes the dashboard subtab');
{
  state.pipelineView = 'dashboard';
  const html = __test__.renderPipelineNav();
  assertContains(html, 'window.loadPipelineDashboard()', 'dashboard refresh reloads dashboard metrics');
  assertContains(html, 'subtab-refresh', 'refresh uses shared subtab layout');
}
console.log('pipeline status controls disable invalid unsearchable transitions');
{
  const imported = __test__.renderPipelineStatusButtons(42, 'imported');
  assertContains(imported, "class=\"p-btn active-status\" data-pipeline-request-id=\"42\" onclick=\"event.stopPropagation(); window.updateStatus(42, 'imported')\">imported</button>", 'imported remains visibly current and conflict-addressable');
  assertExcludes(imported, "window.updateStatus(42, 'unsearchable')", 'imported cannot invoke unsearchable');
  assertContains(imported, 'disabled aria-disabled="true">unsearchable</button>', 'invalid imported stop is disabled');

  const downloading = __test__.renderPipelineStatusButtons(42, 'downloading');
  assertContains(downloading, 'disabled aria-disabled="true">downloading</button>', 'downloading remains visibly current');
  assertExcludes(downloading, "window.updateStatus(42, 'unsearchable')", 'downloading cannot invoke unsearchable');
  assertContains(downloading, 'disabled aria-disabled="true">unsearchable</button>', 'invalid downloading stop is disabled');

  const processing = __test__.renderPipelineStatusButtons(42, 'processing', {
    job_id: 420,
    status: 'recovery_required',
    preview_status: 'running',
  });
  assertContains(processing, 'aria-disabled="true"', 'processing controls expose disabled semantics');
  assertContains(processing, 'aria-describedby=', 'processing controls name visible explanation');
  assertContains(processing, 'job #420 awaits automatic convergence', 'processing explanation names exact owner');
  assertContains(processing, '/api/import-jobs/420/recovery', 'processing links exact recovery detail');
  assertExcludes(processing, ' disabled', 'processing controls remain focusable');
  assertExcludes(processing, 'window.updateStatus', 'processing controls cannot mutate lifecycle');

  const wanted = __test__.renderPipelineStatusButtons(42, 'wanted');
  assertContains(wanted, "window.updateStatus(42, 'unsearchable')", 'wanted may become unsearchable');

  const stopped = __test__.renderPipelineStatusButtons(42, 'unsearchable');
  assertContains(stopped, "window.updateStatus(42, 'unsearchable')", 'current unsearchable state remains an active control');
  assertContains(stopped, 'class="p-btn active-status"', 'unsearchable remains visibly current');
}
console.log('request detail caps history and collapses tracks');
{
  const history = Array.from({ length: 12 }, (_, id) => ({
    id, created_at: '2026-07-13T00:00:00+00:00',
  }));
  const tracks = Array.from({ length: 18 }, (_, id) => ({ id, title: `Track ${id}` }));
  const html = __test__.renderRequestEvidenceSections(history, tracks, []);
  assertContains(html, 'Download History (12)', 'full history count remains visible');
  assertContains(html, 'Show 2 older attempts', 'only older attempts move behind disclosure');
  assertContains(html, '<details class="p-tracks"', 'library tracks are collapsed by default');
  assertContains(html, 'In Library (18 tracks)', 'track disclosure keeps its count');
}

console.log('request detail disclosure — generated count sweep');
for (let count = 0; count <= 30; count++) {
  const history = Array.from({ length: count }, (_, id) => ({
    id, created_at: '2026-07-13T00:00:00+00:00',
  }));
  const html = __test__.renderRequestEvidenceSections(history, [], []);
  const expectedOlder = Math.max(0, count - 10);
  if (expectedOlder === 0) {
    assertExcludes(html, 'older attempt', `${count} histories need no older disclosure`);
  } else {
    assertContains(html, `Show ${expectedOlder} older attempt`, `${count} histories expose exact remainder`);
  }
}

console.log('current library display uses typed authority states only');
{
  const unique = __test__.renderCurrentLibraryRow({
    state: 'unique', path: '/library/Moved/current',
  });
  assertContains(unique, 'Imported to', 'unique state labels the fresh path');
  assertContains(unique, '/library/Moved/current', 'unique state renders the resolver path');

  const missing = __test__.renderCurrentLibraryRow({state: 'missing'});
  assertContains(missing, 'Beets library', 'missing state namespaces the live authority');
  assertContains(missing, 'Not installed', 'missing state stays explicit');

  const ambiguous = __test__.renderCurrentLibraryRow({
    state: 'ambiguous', reason: 'multiple_matches', album_ids: [7, 8],
  });
  assertContains(ambiguous, 'Manual review', 'ambiguous state fails closed visibly');
  assertContains(ambiguous, 'Beets library', 'ambiguous state namespaces the live authority');
  assertContains(ambiguous, 'multiple_matches', 'ambiguity reason is visible');
  assertContains(ambiguous, 'album IDs 7, 8', 'ambiguous album ids are visible');

  const unavailable = __test__.renderCurrentLibraryRow({
    state: 'unavailable', reason: 'conflicting_request_identity',
  });
  assertContains(unavailable, 'Unavailable', 'unavailable state stays explicit');
  assertContains(unavailable, 'Beets library', 'unavailable state namespaces the live authority');
  assertContains(unavailable, 'conflicting_request_identity', 'unavailable reason is visible');
}

console.log('request 6039 current Quality uses average positive track bitrate');
{
  const html = __test__.renderCurrentQualityRow(
    {
      current_spectral_bitrate: null,
      last_download_spectral_bitrate: null,
      current_spectral_grade: null,
      last_download_spectral_grade: null,
      verified_lossless: false,
    },
    [
      ...Array.from({ length: 6 }, () => ({ format: 'MP3', bitrate: 320000 })),
      { format: 'MP3', bitrate: 196000 },
      { format: 'MP3', bitrate: 194000 },
    ],
  );
  assertContains(html, 'MP3 V0', 'avg 288 renders the current V0 label');
  assertExcludes(html, 'MP3 V2', 'min 194 never paints current quality');
}

console.log('current Quality uses the shared ordered spectral palette');
for (const [grade, tone] of [
  ['likely_transcode', 'poor'],
  ['suspect', 'acceptable'],
  ['marginal', 'good'],
  ['genuine', 'lossless'],
]) {
  const html = __test__.renderCurrentQualityRow(
    {
      current_spectral_bitrate: 128,
      last_download_spectral_bitrate: null,
      current_spectral_grade: grade,
      last_download_spectral_grade: null,
      verified_lossless: false,
    },
    [{ format: 'MP3', bitrate: 192000 }],
  );
  assertContains(html, `quality-tone-${tone}`, `${grade} uses shared ${tone} tone`);
  assertContains(html, grade.replaceAll('_', ' '), `${grade} is humanized`);
  assertExcludes(html, grade.includes('_') ? grade : '__never__',
    `${grade} never leaks a raw token`);
}

// --- issue #829 Phase 5 PR4/N3: the Quality header's audit-only flags ---
//
// The header's grade comes from a fallback chain over the installed copy
// AND the last download, so it must apply the pair belonging to whichever
// grade the chain selected. A missing pair keeps the accusing render.

const BEETS_MP3 = [{ format: 'MP3', bitrate: 256000 }];

console.log('current Quality withholds an audit-only HAVE accusation');
{
  const html = __test__.renderCurrentQualityRow(
    {
      current_spectral_bitrate: 128,
      current_spectral_grade: 'likely_transcode',
      current_spectral_accusation_admissible: false,
      current_spectral_accusation_withheld: 'audit_only_codec',
      last_download_spectral_grade: null,
      verified_lossless: false,
    },
    BEETS_MP3,
  );
  assertContains(html, 'likely transcode', 'the measured grade stays visible');
  assertContains(html, 'audit-only', 'the withheld suffix is stated');
  assertContains(html, 'native encoder behaviour', 'the hover explains why');
  assertContains(html, 'quality-tone-unknown', 'the neutral tone is used');
  assertExcludes(html, 'quality-tone-poor', 'the accusing red is withheld');
}

console.log('current Quality keeps the accusation for a real codec');
{
  const html = __test__.renderCurrentQualityRow(
    {
      current_spectral_bitrate: 128,
      current_spectral_grade: 'likely_transcode',
      current_spectral_accusation_admissible: true,
      current_spectral_accusation_withheld: null,
      last_download_spectral_grade: null,
      verified_lossless: false,
    },
    BEETS_MP3,
  );
  assertContains(html, 'quality-tone-poor', 'an admissible grade still accuses');
  assertExcludes(html, 'audit-only', 'no withheld suffix on a real finding');
}

console.log('current Quality falls back to accusing when the flags are absent');
{
  const html = __test__.renderCurrentQualityRow(
    {
      current_spectral_bitrate: 128,
      current_spectral_grade: 'likely_transcode',
      last_download_spectral_grade: null,
      verified_lossless: false,
    },
    BEETS_MP3,
  );
  assertContains(html, 'quality-tone-poor',
    'a row with no evidence join keeps the historical accusing render');
  assertExcludes(html, 'audit-only', 'nothing is withheld without a flag');
}

console.log('current Quality applies the pair belonging to the chosen grade');
{
  // The chain fell through to the last download, so the HAVE pair must
  // NOT be read — it describes a different album.
  const html = __test__.renderCurrentQualityRow(
    {
      current_spectral_grade: null,
      current_spectral_accusation_admissible: true,
      current_spectral_accusation_withheld: null,
      last_download_spectral_bitrate: 128,
      last_download_spectral_grade: 'likely_transcode',
      last_download_spectral_accusation_admissible: false,
      last_download_spectral_accusation_withheld: 'audit_only_codec',
      verified_lossless: false,
    },
    BEETS_MP3,
  );
  assertContains(html, 'audit-only',
    'the last-download pair is applied to the last-download grade');
  assertExcludes(html, 'quality-tone-poor',
    'the HAVE pair never overrides the grade the chain selected');

  // ...and the converse: a HAVE grade must not read the candidate pair.
  const haveHtml = __test__.renderCurrentQualityRow(
    {
      current_spectral_bitrate: 128,
      current_spectral_grade: 'likely_transcode',
      current_spectral_accusation_admissible: true,
      current_spectral_accusation_withheld: null,
      last_download_spectral_grade: 'likely_transcode',
      last_download_spectral_accusation_admissible: false,
      last_download_spectral_accusation_withheld: 'audit_only_codec',
      verified_lossless: false,
    },
    BEETS_MP3,
  );
  assertContains(haveHtml, 'quality-tone-poor',
    'the HAVE grade keeps its own admissible finding');
  assertExcludes(haveHtml, 'audit-only',
    'the candidate pair never neutralizes a HAVE accusation');
}

console.log('current Quality never claims encoder facts for an unresolved codec');
{
  const html = __test__.renderCurrentQualityRow(
    {
      current_spectral_bitrate: 192,
      current_spectral_grade: 'suspect',
      current_spectral_accusation_admissible: false,
      current_spectral_accusation_withheld: 'codec_unresolved',
      last_download_spectral_grade: null,
      verified_lossless: false,
    },
    BEETS_MP3,
  );
  assertContains(html, 'codec unresolved', 'the unresolved world is named');
  assertContains(html, 'could not be identified', 'the hover says why');
  assertExcludes(html, 'native encoder behaviour',
    'an unresolved codec is never described as native encoder rolloff');
  assertExcludes(html, 'audit-only',
    'the two withholding worlds are never conflated');
}

// --- issue #1089 MAJOR-4: mergeRekeyRequest() had zero behavioral
// coverage — a reviewer replaced the whole function body with a no-op and
// every JS test still passed. ---

console.log('mergeRekeyRequest() success path posts, toasts, and reloads the dashboard');
{
  const dom = installDriftDom(8792);
  const btn = { disabled: false, textContent: 'Follow MB merge' };
  const calls = [];
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url: String(url), options });
    if (String(url) === '/api/pipeline/8792/merge-rekey') {
      return {
        ok: true,
        json: async () => ({
          outcome: 'rekeyed', request_id: 8792,
          new_release_id: '9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4',
        }),
      };
    }
    if (String(url) === '/api/pipeline/dashboard') {
      return { ok: true, json: async () => ({ counts: {}, drift_rows: [] }) };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };

  await mergeRekeyRequest(8792, btn);
  await flushMicrotasks();

  assertEqual(calls[0].url, '/api/pipeline/8792/merge-rekey', 'posts to the exact request-scoped route');
  assertEqual(calls[0].options.method, 'POST', 'uses POST');
  assertEqual(calls[0].options.headers['Content-Type'], 'application/json', 'sends a JSON content type');
  assertEqual(calls[0].options.body, '{}', 'sends an empty JSON body — no request payload');
  assert(calls.some(c => c.url === '/api/pipeline/dashboard'),
    'success reloads the dashboard so the healed row disappears');
  assertEqual(dom.toast.textContent,
    'Request #8792 rekeyed to 9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4',
    'toasts the exact survivor id');
  assertEqual(dom.toast.className, 'toast', 'success toast is not an error');
  assertEqual(dom.note.textContent, '', 'success never writes the refusal note');
  assertEqual(btn.textContent, 'Rekeying...',
    'success leaves the disabled mid-flight label — the dashboard reload replaces the row entirely');
}

console.log('mergeRekeyRequest() refusal path re-arms the button and writes the inline note');
{
  const dom = installDriftDom(8792);
  const btn = { disabled: true, textContent: 'Rekeying...' };
  const calls = [];
  globalThis.fetch = async (url) => {
    calls.push(String(url));
    if (String(url) === '/api/pipeline/8792/merge-rekey') {
      return {
        ok: false,
        status: 422,
        json: async () => ({
          outcome: 'not_merged',
          error_message: 'MusicBrainz names no merge survivor for the '
            + 'stored id; this request has not been merged',
        }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };

  await mergeRekeyRequest(8792, btn);

  assert(!calls.includes('/api/pipeline/dashboard'), 'a refusal never reloads the dashboard');
  assertEqual(btn.disabled, false, 'the button re-arms for a retry');
  assertEqual(btn.textContent, 'Follow MB merge', 'the button label resets');
  assertEqual(dom.note.textContent,
    'not_merged: MusicBrainz names no merge survivor for the stored id; '
    + 'this request has not been merged',
    'the inline note names the exact outcome and message');
  assertEqual(dom.note.className, 'drift-row-note metric-bad', 'the inline note uses the bad tone');
  assertEqual(dom.toast.className, 'toast error', 'a refusal toast is an error');
}

console.log('mergeRekeyRequest() network-error path re-arms the button with a generic note');
{
  const dom = installDriftDom(8792);
  const btn = { disabled: true, textContent: 'Rekeying...' };
  globalThis.fetch = async () => {
    throw new TypeError('network down');
  };

  await mergeRekeyRequest(8792, btn);

  assertEqual(btn.disabled, false, 'the button re-arms after a network failure');
  assertEqual(btn.textContent, 'Follow MB merge', 'the button label resets');
  assertEqual(dom.note.textContent, 'Merge-rekey request failed',
    'the inline note falls back to a generic message with no response to read');
  assertEqual(dom.note.className, 'drift-row-note metric-bad', 'the inline note uses the bad tone');
  assertEqual(dom.toast.className, 'toast error', 'a network failure toast is an error');
}

console.log('mergeRekeyRequest() refusal note falls back to the raw error field when unmessaged');
{
  const dom = installDriftDom(42);
  const btn = { disabled: true, textContent: 'Rekeying...' };
  globalThis.fetch = async () => ({
    ok: false,
    status: 409,
    json: async () => ({ outcome: 'rekey_refused', error: 'route-level error text' }),
  });

  await mergeRekeyRequest(42, btn);

  assertEqual(dom.note.textContent, 'rekey_refused: route-level error text',
    'falls back to the route-level "error" field when error_message is absent');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
