/**
 * Unit tests for web/js/pipeline.js navigation/detail helpers.
 * Run with: node tests/test_js_pipeline.mjs
 */

import { __test__, mergeRekeyRequest, recheckRetagDivergenceAlbum, syncRetagDivergenceAlbum } from '../web/js/pipeline.js';
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
 * Node-REPLACEMENT DOM stand-in for the merge-rekey drift row (#1089,
 * re-modeled for #1266 item 2): the `drift-note-<id>` element LIVES
 * inside `#pipeline-content`, and `loadPipelineDashboard()` rewrites
 * that container's `innerHTML` — so a dashboard reload genuinely
 * REPLACES the note node. Assigning `pipelineContent.innerHTML` here
 * swaps which note object `getElementById` returns, exactly like
 * `installReplacingRetagDom` below, so a handler that reloads the
 * dashboard and THEN writes a note it captured earlier goes RED (the
 * #1264 M26 / #1266 M4 stale-node shape). The shipped handler is
 * correct by write ORDERING — its only reloading path returns without
 * touching the note — and that ordering is now what these tests pin.
 */
function installDriftDom(requestId) {
  const preNote = { textContent: '', className: '' };
  const postNote = { textContent: '', className: '' };
  const toast = { textContent: '', className: '', style: { display: 'none' } };
  let reloaded = false;
  const pipelineContent = {
    _html: '',
    get innerHTML() { return this._html; },
    set innerHTML(value) { this._html = value; reloaded = true; },
  };
  globalThis.document = {
    getElementById(id) {
      if (id === 'pipeline-content') return pipelineContent;
      if (id === `drift-note-${requestId}`) {
        return reloaded ? postNote : preNote;
      }
      if (id === 'toast') return toast;
      return null;
    },
  };
  globalThis.setTimeout = (fn) => {
    fn();
    return 0;
  };
  return {
    pipelineContent,
    preNote,
    postNote,
    toast,
    isReloaded: () => reloaded,
    visibleNote: () => (reloaded ? postNote : preNote),
  };
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
  assert(dom.isReloaded(),
    'the reload really rewrote #pipeline-content — the note nodes were replaced');
  assertEqual(dom.toast.textContent,
    'Request #8792 rekeyed to 9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4',
    'toasts the exact survivor id');
  assertEqual(dom.toast.className, 'toast', 'success toast is not an error');
  assertEqual(dom.preNote.textContent, '', 'success never writes the pre-reload note');
  assertEqual(dom.postNote.textContent, '', 'success never writes the post-reload note');
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
  assertEqual(dom.visibleNote().textContent,
    'not_merged: MusicBrainz names no merge survivor for the stored id; '
    + 'this request has not been merged',
    'the VISIBLE inline note names the exact outcome and message');
  assertEqual(dom.visibleNote().className, 'drift-row-note metric-bad', 'the visible note uses the bad tone');
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
  assertEqual(dom.visibleNote().textContent, 'Merge-rekey request failed',
    'the VISIBLE inline note falls back to a generic message with no response to read');
  assertEqual(dom.visibleNote().className, 'drift-row-note metric-bad', 'the visible note uses the bad tone');
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

  assertEqual(dom.visibleNote().textContent, 'rekey_refused: route-level error text',
    'falls back to the route-level "error" field when error_message is absent');
}

// --- issue #1142: per-album retag-divergence recheck ---

/**
 * Node-REPLACEMENT DOM stand-in for one retag-divergence album row
 * (#1266 item 2, generalising #1264's S1 helper): assigning
 * `container.innerHTML` swaps which note object `getElementById`
 * returns, so a handler that captures the note BEFORE re-rendering
 * writes to a detached node and the assertions catch it — the exact bug
 * shape a fixed-map DOM (one note object forever) can never see.
 * `visibleNote()` is "whatever note node the page shows NOW"; assert
 * refusal copy through it so a future re-render-then-stale-write
 * regression goes RED. Shared by the recheck and sync handler tests;
 * `mergeRekeyRequest`'s inline drift DOM above stays a fixed map on
 * purpose — that handler performs no in-place container re-render, so
 * there is no node replacement to model.
 * @param {number} albumId
 */
function installReplacingRetagDom(albumId) {
  const preNote = { textContent: '', className: '' };
  const postNote = { textContent: '', className: '' };
  const toast = { textContent: '', className: '', style: { display: 'none' } };
  let rerendered = false;
  const container = {
    _html: '',
    get innerHTML() { return this._html; },
    set innerHTML(value) { this._html = value; rerendered = true; },
  };
  globalThis.document = {
    getElementById(id) {
      if (id === `retag-album-${albumId}`) return container;
      if (id === `retag-album-note-${albumId}`) {
        return rerendered ? postNote : preNote;
      }
      if (id === 'toast') return toast;
      return null;
    },
  };
  globalThis.setTimeout = (fn) => {
    fn();
    return 0;
  };
  return {
    container,
    preNote,
    postNote,
    toast,
    isRerendered: () => rerendered,
    visibleNote: () => (rerendered ? postNote : preNote),
  };
}

console.log('recheckRetagDivergenceAlbum() success path GETs, patches the row in place, and toasts');
{
  const dom = installReplacingRetagDom(6612);
  const btn = { disabled: false, textContent: 'Recheck' };
  const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push({ url: String(url), options });
    return {
      ok: true,
      json: async () => ({
        album_id: 6612, db_mb_albumid: 'd990b8af-0000-0000-0000-000000000000',
        album_class: 'agrees', item_count: 8, items: [],
      }),
    };
  };

  await recheckRetagDivergenceAlbum(6612, btn);

  assertEqual(calls.length, 1, 'exactly one fetch issued');
  assertEqual(calls[0].url, '/api/audit/retag-divergence/album/6612',
    'GETs the exact per-album route');
  assert(calls[0].options === undefined || calls[0].options.method === undefined
    || calls[0].options.method === 'GET', 'uses GET, never POST — this is a read-only check');
  assertContains(dom.container.innerHTML, 'agrees', 'row patched with the fresh classification');
  assertContains(dom.container.innerHTML, 'window.recheckRetagDivergenceAlbum(6612, this)',
    'patched row keeps its own recheck button wired for a further recheck');
  assertEqual(dom.toast.textContent, 'Album #6612 rechecked: agrees', 'toasts the fresh result');
  assertEqual(dom.toast.className, 'toast', 'success toast is not an error');
  assertEqual(dom.preNote.textContent, '', 'success never writes the pre-render note');
  assertEqual(dom.postNote.textContent, '', 'success never writes the post-render note');
}

console.log('recheckRetagDivergenceAlbum() N2 (fresh review) — the patched row shows fresh non-agreeing item detail');
{
  const dom = installReplacingRetagDom(6612);
  const btn = { disabled: false, textContent: 'Recheck' };
  globalThis.fetch = async () => ({
    ok: true,
    json: async () => ({
      album_id: 6612, db_mb_albumid: 'd990b8af-0000-0000-0000-000000000000',
      album_class: 'diverges', item_count: 2,
      items: [
        {
          path: '/library/Slipknot/01.flac', item_class: 'diverges',
          file_mb_albumid: 'a6269e96-0000-0000-0000-000000000000', detail: null,
        },
        {
          path: '/library/Slipknot/02.flac', item_class: 'agrees',
          file_mb_albumid: 'd990b8af-0000-0000-0000-000000000000', detail: null,
        },
      ],
    }),
  });

  await recheckRetagDivergenceAlbum(6612, btn);

  assertContains(dom.container.innerHTML, 'diverges', 'patched row shows the fresh album class');
  assertContains(dom.container.innerHTML, 'a6269e96-0000-0000-0000-000000000000',
    'patched row shows the fresh diverging item\'s identity');
  // #1260 revised the #1142 N2 stance on operator request: the file NAME
  // is the readable row subject, the FULL path only a hover title.
  assertContains(dom.container.innerHTML, 'title="/library/Slipknot/01.flac"',
    'patched row keeps the full item path one hover away');
  assertExcludes(dom.container.innerHTML, '>diverges: /library/Slipknot/01.flac',
    'patched row never renders the full path as row text');
}

console.log('recheckRetagDivergenceAlbum() never reloads the whole dashboard on success');
{
  installReplacingRetagDom(6612);
  const btn = { disabled: false, textContent: 'Recheck' };
  const calls = [];
  globalThis.fetch = async (url) => {
    calls.push(String(url));
    return {
      ok: true,
      json: async () => ({
        album_id: 6612, db_mb_albumid: '', album_class: 'agrees',
        item_count: 0, items: [],
      }),
    };
  };

  await recheckRetagDivergenceAlbum(6612, btn);

  assert(!calls.includes('/api/pipeline/dashboard'),
    'a per-album recheck never triggers a full dashboard reload');
}

console.log('recheckRetagDivergenceAlbum() not-found path re-arms the button and writes the inline note');
{
  const dom = installReplacingRetagDom(999);
  const btn = { disabled: true, textContent: 'Rechecking...' };
  globalThis.fetch = async () => ({
    ok: false,
    status: 404,
    json: async () => ({ error: 'No Beets album with id 999' }),
  });

  await recheckRetagDivergenceAlbum(999, btn);

  assertEqual(btn.disabled, false, 'the button re-arms for a retry');
  assertEqual(btn.textContent, 'Recheck', 'the button label resets');
  assertEqual(dom.visibleNote().textContent, 'No Beets album with id 999',
    'the VISIBLE inline note names the exact error');
  assertEqual(dom.visibleNote().className, 'drift-row-note metric-bad', 'the visible note uses the bad tone');
  assertEqual(dom.postNote.textContent, '', 'nothing is written to the unrendered post-render note');
  assertEqual(dom.toast.className, 'toast error', 'a refusal toast is an error');
}

console.log('recheckRetagDivergenceAlbum() network-error path re-arms the button with a generic note');
{
  const dom = installReplacingRetagDom(6612);
  const btn = { disabled: true, textContent: 'Rechecking...' };
  globalThis.fetch = async () => {
    throw new TypeError('network down');
  };

  await recheckRetagDivergenceAlbum(6612, btn);

  assertEqual(btn.disabled, false, 'the button re-arms after a network failure');
  assertEqual(btn.textContent, 'Recheck', 'the button label resets');
  assertEqual(dom.visibleNote().textContent, 'Recheck request failed',
    'the VISIBLE inline note falls back to a generic message with no response to read');
  assertEqual(dom.toast.className, 'toast error', 'a network failure toast is an error');
}

const SYNC_DB_ID = '26693e58-02c0-4bb1-b66f-f0f44f8a234d';

console.log('syncRetagDivergenceAlbum() POSTs the compare-and-set body and patches the row on success');
{
  const dom = installReplacingRetagDom(16948);
  const btn = {
    disabled: false, textContent: 'Write tags',
    dataset: { expected: SYNC_DB_ID },
  };
  const calls = [];
  globalThis.fetch = async (url, options) => {
    calls.push({ url: String(url), options });
    return {
      ok: true,
      json: async () => ({
        outcome: 'synced', album_id: 16948, db_mb_albumid: SYNC_DB_ID,
        error_message: null,
        album: {
          album_id: 16948, db_mb_albumid: SYNC_DB_ID,
          albumartist: 'Terre Thaemlitz / DJ Sprinkles', album: 'RA.1000',
          album_class: 'agrees', item_count: 1, items: [],
        },
      }),
    };
  };

  await syncRetagDivergenceAlbum(16948, btn);

  assertEqual(calls.length, 1, 'exactly one fetch issued');
  assertEqual(calls[0].url, '/api/audit/retag-divergence/album/16948/sync-tags',
    'the sync POSTs the canonical route path');
  assertEqual(calls[0].options.method, 'POST', 'the sync uses POST');
  assertEqual(JSON.parse(calls[0].options.body).expected_mb_albumid, SYNC_DB_ID,
    'the body carries the compare-and-set identity from data-expected');
  assert(dom.isRerendered(), 'the row re-renders from the returned album');
  assertContains(dom.container.innerHTML, 'agrees',
    'the patched row shows the post-sync classification');
  assertEqual(btn.disabled, true,
    'the detached pre-render button is never resurrected on success');
  assertEqual(dom.toast.className, 'toast', 'a success toast is not an error');
}

console.log('syncRetagDivergenceAlbum() residual refusal re-renders AND writes the POST-re-render note');
{
  const dom = installReplacingRetagDom(16948);
  const btn = {
    disabled: false, textContent: 'Write tags',
    dataset: { expected: SYNC_DB_ID },
  };
  globalThis.fetch = async () => ({
    ok: false,
    status: 409,
    json: async () => ({
      outcome: 'residual_divergence', album_id: 16948,
      db_mb_albumid: SYNC_DB_ID,
      error_message: 'beet write exited 1, but the re-read file tags still disagree',
      album: {
        album_id: 16948, db_mb_albumid: SYNC_DB_ID,
        album_class: 'diverges', item_count: 1,
        items: [{
          item_class: 'diverges', path: '/library/x/01.opus',
          file_mb_albumid: 'fdc54a6a-27c7-4936-87d7-7ab146812d4e',
          detail: null,
        }],
      },
    }),
  });

  await syncRetagDivergenceAlbum(16948, btn);

  assert(dom.isRerendered(), 'a residual refusal still re-renders the fresh scan');
  assertEqual(btn.disabled, true,
    'the detached pre-render button is never resurrected on a re-rendered refusal (#1260 review F11)');
  assertContains(dom.postNote.textContent, 'residual_divergence',
    'the refusal note lands in the POST-re-render note node');
  assertContains(dom.postNote.textContent, 're-read file tags still disagree',
    'the refusal note carries the service detail');
  assertEqual(dom.preNote.textContent, '',
    'nothing is written to the destroyed pre-render note node');
  assertEqual(dom.toast.className, 'toast error', 'a refusal toast is an error');
}

console.log('syncRetagDivergenceAlbum() album-less refusal re-arms the still-attached button');
{
  const dom = installReplacingRetagDom(42);
  const btn = {
    disabled: false, textContent: 'Write tags',
    dataset: { expected: SYNC_DB_ID },
  };
  globalThis.fetch = async () => ({
    ok: false,
    status: 409,
    json: async () => ({
      outcome: 'identity_mismatch', album_id: 42,
      db_mb_albumid: 'fdc54a6a-27c7-4936-87d7-7ab146812d4e',
      error_message: 'Beets album 42 now names fdc54a6a…; recheck and retry',
      album: null,
    }),
  });

  await syncRetagDivergenceAlbum(42, btn);

  assert(!dom.isRerendered(), 'no album payload, no re-render');
  assertEqual(btn.disabled, false, 'the still-attached button re-arms');
  assertEqual(btn.textContent, 'Write tags', 'the button label resets');
  assertContains(dom.visibleNote().textContent, 'identity_mismatch',
    'the visible note names the refusal outcome');
  assertEqual(dom.postNote.textContent, '',
    'nothing is written to the unrendered post-render note');
  assertEqual(dom.toast.className, 'toast error', 'a refusal toast is an error');
}

console.log('syncRetagDivergenceAlbum() network-error path re-arms the button with a generic note');
{
  const dom = installReplacingRetagDom(16948);
  const btn = {
    disabled: true, textContent: 'Writing tags...',
    dataset: { expected: SYNC_DB_ID },
  };
  globalThis.fetch = async () => {
    throw new TypeError('network down');
  };

  await syncRetagDivergenceAlbum(16948, btn);

  assertEqual(btn.disabled, false, 'the button re-arms after a network failure');
  assertEqual(btn.textContent, 'Write tags', 'the button label resets');
  assertEqual(dom.preNote.textContent, 'Tag-sync request failed',
    'the note falls back to a generic message');
  assertEqual(dom.toast.className, 'toast error', 'a network failure toast is an error');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
