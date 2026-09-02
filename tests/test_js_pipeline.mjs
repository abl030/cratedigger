/**
 * Unit tests for web/js/pipeline.js navigation/detail helpers.
 * Run with: node tests/test_js_pipeline.mjs
 */

import {
  mergeRekeyRequest,
  recheckRetagDivergenceAlbum,
  renderCurrentLibraryRow,
  renderCurrentQualityRow,
  renderPipeline,
  renderPipelineNav,
  renderPipelineStatusButtons,
  renderRequestEvidenceSections,
  syncRetagDivergenceAlbum,
  toggleDetail,
} from '../web/js/pipeline.js';
import { state } from '../web/js/state.js';
import { jsArg } from '../web/js/util.js';

import { domStub, element, stubGlobals, suite } from './js_harness.mjs';

const t = suite(import.meta.url);

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
  stubGlobals({ document: {
    getElementById(id) {
      if (id === 'pipeline-content') return pipelineContent;
      if (id === `drift-note-${requestId}`) {
        return reloaded ? postNote : preNote;
      }
      if (id === 'toast') return toast;
      return null;
    },
  } });
  stubGlobals({ setTimeout: (fn) => {
    fn();
    return 0;
  } });
  return {
    pipelineContent,
    preNote,
    postNote,
    toast,
    isReloaded: () => reloaded,
    visibleNote: () => (reloaded ? postNote : preNote),
  };
}

t.section('renderPipelineNav() has operational views only');
{
  state.pipelineView = 'dashboard';
  const html = renderPipelineNav();
  t.excludes(html, 'window.setPipelineView(\'queue\')', 'request queue tab removed');
  t.contains(html, 'window.setPipelineView(\'dashboard\')', 'dashboard tab rendered');
  t.contains(html, 'window.setPipelineView(\'long-tail\')', 'long-tail tab rendered');
  t.contains(html, 'window.loadPipelineDashboard()', 'dashboard refresh reloads metrics');
  t.contains(html, 'subtab-refresh', 'refresh uses shared subtab layout');
}
t.section('renderPipelineNav() refreshes the dashboard subtab');
{
  state.pipelineView = 'dashboard';
  const html = renderPipelineNav();
  t.contains(html, 'window.loadPipelineDashboard()', 'dashboard refresh reloads dashboard metrics');
  t.contains(html, 'subtab-refresh', 'refresh uses shared subtab layout');
}
t.section('pipeline status controls disable invalid unsearchable transitions');
{
  const imported = renderPipelineStatusButtons(42, 'imported');
  t.contains(imported, "class=\"p-btn active-status\" data-pipeline-request-id=\"42\" onclick=\"event.stopPropagation(); window.updateStatus(42, 'imported')\">imported</button>", 'imported remains visibly current and conflict-addressable');
  t.excludes(imported, "window.updateStatus(42, 'unsearchable')", 'imported cannot invoke unsearchable');
  t.contains(imported, 'disabled aria-disabled="true">unsearchable</button>', 'invalid imported stop is disabled');

  const downloading = renderPipelineStatusButtons(42, 'downloading');
  t.contains(downloading, 'disabled aria-disabled="true">downloading</button>', 'downloading remains visibly current');
  t.excludes(downloading, "window.updateStatus(42, 'unsearchable')", 'downloading cannot invoke unsearchable');
  t.contains(downloading, 'disabled aria-disabled="true">unsearchable</button>', 'invalid downloading stop is disabled');

  const processing = renderPipelineStatusButtons(42, 'processing', {
    job_id: 420,
    status: 'recovery_required',
    preview_status: 'running',
  });
  t.contains(processing, 'aria-disabled="true"', 'processing controls expose disabled semantics');
  t.contains(processing, 'aria-describedby=', 'processing controls name visible explanation');
  t.contains(processing, 'job #420 awaits automatic convergence', 'processing explanation names exact owner');
  t.contains(processing, '/api/import-jobs/420/recovery', 'processing links exact recovery detail');
  t.excludes(processing, ' disabled', 'processing controls remain focusable');
  t.excludes(processing, 'window.updateStatus', 'processing controls cannot mutate lifecycle');

  const wanted = renderPipelineStatusButtons(42, 'wanted');
  t.contains(wanted, "window.updateStatus(42, 'unsearchable')", 'wanted may become unsearchable');

  const stopped = renderPipelineStatusButtons(42, 'unsearchable');
  t.contains(stopped, "window.updateStatus(42, 'unsearchable')", 'current unsearchable state remains an active control');
  t.contains(stopped, 'class="p-btn active-status"', 'unsearchable remains visibly current');
}
t.section('request detail caps history and collapses tracks');
{
  const history = Array.from({ length: 12 }, (_, id) => ({
    id, created_at: '2026-07-13T00:00:00+00:00',
  }));
  const tracks = Array.from({ length: 18 }, (_, id) => ({ id, title: `Track ${id}` }));
  const html = renderRequestEvidenceSections(history, tracks, []);
  t.contains(html, 'Download History (12)', 'full history count remains visible');
  t.contains(html, 'Show 2 older attempts', 'only older attempts move behind disclosure');
  t.contains(html, '<details class="p-tracks"', 'library tracks are collapsed by default');
  t.contains(html, 'In Library (18 tracks)', 'track disclosure keeps its count');
}

t.section('request detail disclosure — generated count sweep');
for (let count = 0; count <= 30; count++) {
  const history = Array.from({ length: count }, (_, id) => ({
    id, created_at: '2026-07-13T00:00:00+00:00',
  }));
  const html = renderRequestEvidenceSections(history, [], []);
  const expectedOlder = Math.max(0, count - 10);
  if (expectedOlder === 0) {
    t.excludes(html, 'older attempt', `${count} histories need no older disclosure`);
  } else {
    t.contains(html, `Show ${expectedOlder} older attempt`, `${count} histories expose exact remainder`);
  }
}

t.section('current library display uses typed authority states only');
{
  const unique = renderCurrentLibraryRow({
    state: 'unique', path: '/library/Moved/current',
  });
  t.contains(unique, 'Imported to', 'unique state labels the fresh path');
  t.contains(unique, '/library/Moved/current', 'unique state renders the resolver path');

  const missing = renderCurrentLibraryRow({state: 'missing'});
  t.contains(missing, 'Beets library', 'missing state namespaces the live authority');
  t.contains(missing, 'Not installed', 'missing state stays explicit');

  const ambiguous = renderCurrentLibraryRow({
    state: 'ambiguous', reason: 'multiple_matches', album_ids: [7, 8],
  });
  t.contains(ambiguous, 'Manual review', 'ambiguous state fails closed visibly');
  t.contains(ambiguous, 'Beets library', 'ambiguous state namespaces the live authority');
  t.contains(ambiguous, 'multiple_matches', 'ambiguity reason is visible');
  t.contains(ambiguous, 'album IDs 7, 8', 'ambiguous album ids are visible');

  const unavailable = renderCurrentLibraryRow({
    state: 'unavailable', reason: 'conflicting_request_identity',
  });
  t.contains(unavailable, 'Unavailable', 'unavailable state stays explicit');
  t.contains(unavailable, 'Beets library', 'unavailable state namespaces the live authority');
  t.contains(unavailable, 'conflicting_request_identity', 'unavailable reason is visible');
}

t.section('request 6039 current Quality uses average positive track bitrate');
{
  const html = renderCurrentQualityRow(
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
  t.contains(html, 'MP3 V0', 'avg 288 renders the current V0 label');
  t.excludes(html, 'MP3 V2', 'min 194 never paints current quality');
}

t.section('current Quality uses the shared ordered spectral palette');
for (const [grade, tone] of [
  ['likely_transcode', 'poor'],
  ['suspect', 'acceptable'],
  ['marginal', 'good'],
  ['genuine', 'lossless'],
]) {
  const html = renderCurrentQualityRow(
    {
      current_spectral_bitrate: 128,
      last_download_spectral_bitrate: null,
      current_spectral_grade: grade,
      last_download_spectral_grade: null,
      verified_lossless: false,
    },
    [{ format: 'MP3', bitrate: 192000 }],
  );
  t.contains(html, `quality-tone-${tone}`, `${grade} uses shared ${tone} tone`);
  t.contains(html, grade.replaceAll('_', ' '), `${grade} is humanized`);
  t.excludes(html, grade.includes('_') ? grade : '__never__',
    `${grade} never leaks a raw token`);
}

// --- issue #829 Phase 5 PR4/N3: the Quality header's audit-only flags ---
//
// The header's grade comes from a fallback chain over the installed copy
// AND the last download, so it must apply the pair belonging to whichever
// grade the chain selected. A missing pair keeps the accusing render.

const BEETS_MP3 = [{ format: 'MP3', bitrate: 256000 }];

t.section('current Quality withholds an audit-only HAVE accusation');
{
  const html = renderCurrentQualityRow(
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
  t.contains(html, 'likely transcode', 'the measured grade stays visible');
  t.contains(html, 'audit-only', 'the withheld suffix is stated');
  t.contains(html, 'native encoder behaviour', 'the hover explains why');
  t.contains(html, 'quality-tone-unknown', 'the neutral tone is used');
  t.excludes(html, 'quality-tone-poor', 'the accusing red is withheld');
}

t.section('current Quality keeps the accusation for a real codec');
{
  const html = renderCurrentQualityRow(
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
  t.contains(html, 'quality-tone-poor', 'an admissible grade still accuses');
  t.excludes(html, 'audit-only', 'no withheld suffix on a real finding');
}

t.section('current Quality falls back to accusing when the flags are absent');
{
  const html = renderCurrentQualityRow(
    {
      current_spectral_bitrate: 128,
      current_spectral_grade: 'likely_transcode',
      last_download_spectral_grade: null,
      verified_lossless: false,
    },
    BEETS_MP3,
  );
  t.contains(html, 'quality-tone-poor',
    'a row with no evidence join keeps the historical accusing render');
  t.excludes(html, 'audit-only', 'nothing is withheld without a flag');
}

t.section('current Quality applies the pair belonging to the chosen grade');
{
  // The chain fell through to the last download, so the HAVE pair must
  // NOT be read — it describes a different album.
  const html = renderCurrentQualityRow(
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
  t.contains(html, 'audit-only',
    'the last-download pair is applied to the last-download grade');
  t.excludes(html, 'quality-tone-poor',
    'the HAVE pair never overrides the grade the chain selected');

  // ...and the converse: a HAVE grade must not read the candidate pair.
  const haveHtml = renderCurrentQualityRow(
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
  t.contains(haveHtml, 'quality-tone-poor',
    'the HAVE grade keeps its own admissible finding');
  t.excludes(haveHtml, 'audit-only',
    'the candidate pair never neutralizes a HAVE accusation');
}

t.section('current Quality never claims encoder facts for an unresolved codec');
{
  const html = renderCurrentQualityRow(
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
  t.contains(html, 'codec unresolved', 'the unresolved world is named');
  t.contains(html, 'could not be identified', 'the hover says why');
  t.excludes(html, 'native encoder behaviour',
    'an unresolved codec is never described as native encoder rolloff');
  t.excludes(html, 'audit-only',
    'the two withholding worlds are never conflated');
}

// --- issue #1089 MAJOR-4: mergeRekeyRequest() had zero behavioral
// coverage — a reviewer replaced the whole function body with a no-op and
// every JS test still passed. ---

t.section('mergeRekeyRequest() success path posts, toasts, and reloads the dashboard');
{
  const dom = installDriftDom(8792);
  const btn = { disabled: false, textContent: 'Follow MB merge' };
  const calls = [];
  stubGlobals({ fetch: async (url, options = {}) => {
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
  } });

  await mergeRekeyRequest(8792, btn);
  await flushMicrotasks();

  t.equal(calls[0].url, '/api/pipeline/8792/merge-rekey', 'posts to the exact request-scoped route');
  t.equal(calls[0].options.method, 'POST', 'uses POST');
  t.equal(calls[0].options.headers['Content-Type'], 'application/json', 'sends a JSON content type');
  t.equal(calls[0].options.body, '{}', 'sends an empty JSON body — no request payload');
  t.ok(calls.some(c => c.url === '/api/pipeline/dashboard'),
    'success reloads the dashboard so the healed row disappears');
  t.ok(dom.isReloaded(),
    'the reload really rewrote #pipeline-content — the note nodes were replaced');
  t.equal(dom.toast.textContent,
    'Request #8792 rekeyed to 9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4',
    'toasts the exact survivor id');
  t.equal(dom.toast.className, 'toast', 'success toast is not an error');
  t.equal(dom.preNote.textContent, '', 'success never writes the pre-reload note');
  t.equal(dom.postNote.textContent, '', 'success never writes the post-reload note');
  t.equal(btn.textContent, 'Rekeying...',
    'success leaves the disabled mid-flight label — the dashboard reload replaces the row entirely');
}

t.section('mergeRekeyRequest() refusal path re-arms the button and writes the inline note');
{
  const dom = installDriftDom(8792);
  const btn = { disabled: true, textContent: 'Rekeying...' };
  const calls = [];
  stubGlobals({ fetch: async (url) => {
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
  } });

  await mergeRekeyRequest(8792, btn);

  t.ok(!calls.includes('/api/pipeline/dashboard'), 'a refusal never reloads the dashboard');
  t.equal(btn.disabled, false, 'the button re-arms for a retry');
  t.equal(btn.textContent, 'Follow MB merge', 'the button label resets');
  t.equal(dom.visibleNote().textContent,
    'not_merged: MusicBrainz names no merge survivor for the stored id; '
    + 'this request has not been merged',
    'the VISIBLE inline note names the exact outcome and message');
  t.equal(dom.visibleNote().className, 'drift-row-note metric-bad', 'the visible note uses the bad tone');
  t.equal(dom.toast.className, 'toast error', 'a refusal toast is an error');
}

t.section('mergeRekeyRequest() network-error path re-arms the button with a generic note');
{
  const dom = installDriftDom(8792);
  const btn = { disabled: true, textContent: 'Rekeying...' };
  stubGlobals({ fetch: async () => {
    throw new TypeError('network down');
  } });

  await mergeRekeyRequest(8792, btn);

  t.equal(btn.disabled, false, 'the button re-arms after a network failure');
  t.equal(btn.textContent, 'Follow MB merge', 'the button label resets');
  t.equal(dom.visibleNote().textContent, 'Merge-rekey request failed',
    'the VISIBLE inline note falls back to a generic message with no response to read');
  t.equal(dom.visibleNote().className, 'drift-row-note metric-bad', 'the visible note uses the bad tone');
  t.equal(dom.toast.className, 'toast error', 'a network failure toast is an error');
}

t.section('mergeRekeyRequest() refusal note falls back to the raw error field when unmessaged');
{
  const dom = installDriftDom(42);
  const btn = { disabled: true, textContent: 'Rekeying...' };
  stubGlobals({ fetch: async () => ({
    ok: false,
    status: 409,
    json: async () => ({ outcome: 'rekey_refused', error: 'route-level error text' }),
  }) });

  await mergeRekeyRequest(42, btn);

  t.equal(dom.visibleNote().textContent, 'rekey_refused: route-level error text',
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
  stubGlobals({ document: {
    getElementById(id) {
      if (id === `retag-album-${albumId}`) return container;
      if (id === `retag-album-note-${albumId}`) {
        return rerendered ? postNote : preNote;
      }
      if (id === 'toast') return toast;
      return null;
    },
  } });
  stubGlobals({ setTimeout: (fn) => {
    fn();
    return 0;
  } });
  return {
    container,
    preNote,
    postNote,
    toast,
    isRerendered: () => rerendered,
    visibleNote: () => (rerendered ? postNote : preNote),
  };
}

t.section('recheckRetagDivergenceAlbum() success path GETs, patches the row in place, and toasts');
{
  const dom = installReplacingRetagDom(6612);
  const btn = { disabled: false, textContent: 'Recheck' };
  const calls = [];
  stubGlobals({ fetch: async (url, options) => {
    calls.push({ url: String(url), options });
    return {
      ok: true,
      json: async () => ({
        album_id: 6612, db_mb_albumid: 'd990b8af-0000-0000-0000-000000000000',
        album_class: 'agrees', item_count: 8, items: [],
      }),
    };
  } });

  await recheckRetagDivergenceAlbum(6612, btn);

  t.equal(calls.length, 1, 'exactly one fetch issued');
  t.equal(calls[0].url, '/api/audit/retag-divergence/album/6612',
    'GETs the exact per-album route');
  t.ok(calls[0].options === undefined || calls[0].options.method === undefined
    || calls[0].options.method === 'GET', 'uses GET, never POST — this is a read-only check');
  t.contains(dom.container.innerHTML, 'agrees', 'row patched with the fresh classification');
  t.contains(dom.container.innerHTML, 'window.recheckRetagDivergenceAlbum(6612, this)',
    'patched row keeps its own recheck button wired for a further recheck');
  t.equal(dom.toast.textContent, 'Album #6612 rechecked: agrees', 'toasts the fresh result');
  t.equal(dom.toast.className, 'toast', 'success toast is not an error');
  t.equal(dom.preNote.textContent, '', 'success never writes the pre-render note');
  t.equal(dom.postNote.textContent, '', 'success never writes the post-render note');
}

t.section('recheckRetagDivergenceAlbum() N2 (fresh review) — the patched row shows fresh non-agreeing item detail');
{
  const dom = installReplacingRetagDom(6612);
  const btn = { disabled: false, textContent: 'Recheck' };
  stubGlobals({ fetch: async () => ({
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
  }) });

  await recheckRetagDivergenceAlbum(6612, btn);

  t.contains(dom.container.innerHTML, 'diverges', 'patched row shows the fresh album class');
  t.contains(dom.container.innerHTML, 'a6269e96-0000-0000-0000-000000000000',
    'patched row shows the fresh diverging item\'s identity');
  // #1260 revised the #1142 N2 stance on operator request: the file NAME
  // is the readable row subject, the FULL path only a hover title.
  t.contains(dom.container.innerHTML, 'title="/library/Slipknot/01.flac"',
    'patched row keeps the full item path one hover away');
  t.excludes(dom.container.innerHTML, '>diverges: /library/Slipknot/01.flac',
    'patched row never renders the full path as row text');
}

t.section('recheckRetagDivergenceAlbum() never reloads the whole dashboard on success');
{
  installReplacingRetagDom(6612);
  const btn = { disabled: false, textContent: 'Recheck' };
  const calls = [];
  stubGlobals({ fetch: async (url) => {
    calls.push(String(url));
    return {
      ok: true,
      json: async () => ({
        album_id: 6612, db_mb_albumid: '', album_class: 'agrees',
        item_count: 0, items: [],
      }),
    };
  } });

  await recheckRetagDivergenceAlbum(6612, btn);

  t.ok(!calls.includes('/api/pipeline/dashboard'),
    'a per-album recheck never triggers a full dashboard reload');
}

t.section('recheckRetagDivergenceAlbum() not-found path re-arms the button and writes the inline note');
{
  const dom = installReplacingRetagDom(999);
  const btn = { disabled: true, textContent: 'Rechecking...' };
  stubGlobals({ fetch: async () => ({
    ok: false,
    status: 404,
    json: async () => ({ error: 'No Beets album with id 999' }),
  }) });

  await recheckRetagDivergenceAlbum(999, btn);

  t.equal(btn.disabled, false, 'the button re-arms for a retry');
  t.equal(btn.textContent, 'Recheck', 'the button label resets');
  t.equal(dom.visibleNote().textContent, 'No Beets album with id 999',
    'the VISIBLE inline note names the exact error');
  t.equal(dom.visibleNote().className, 'drift-row-note metric-bad', 'the visible note uses the bad tone');
  t.equal(dom.postNote.textContent, '', 'nothing is written to the unrendered post-render note');
  t.equal(dom.toast.className, 'toast error', 'a refusal toast is an error');
}

t.section('recheckRetagDivergenceAlbum() network-error path re-arms the button with a generic note');
{
  const dom = installReplacingRetagDom(6612);
  const btn = { disabled: true, textContent: 'Rechecking...' };
  stubGlobals({ fetch: async () => {
    throw new TypeError('network down');
  } });

  await recheckRetagDivergenceAlbum(6612, btn);

  t.equal(btn.disabled, false, 'the button re-arms after a network failure');
  t.equal(btn.textContent, 'Recheck', 'the button label resets');
  t.equal(dom.visibleNote().textContent, 'Recheck request failed',
    'the VISIBLE inline note falls back to a generic message with no response to read');
  t.equal(dom.toast.className, 'toast error', 'a network failure toast is an error');
}

const SYNC_DB_ID = '26693e58-02c0-4bb1-b66f-f0f44f8a234d';

t.section('syncRetagDivergenceAlbum() POSTs the compare-and-set body and patches the row on success');
{
  const dom = installReplacingRetagDom(16948);
  const btn = {
    disabled: false, textContent: 'Write tags',
    dataset: { expected: SYNC_DB_ID },
  };
  const calls = [];
  stubGlobals({ fetch: async (url, options) => {
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
  } });

  await syncRetagDivergenceAlbum(16948, btn);

  t.equal(calls.length, 1, 'exactly one fetch issued');
  t.equal(calls[0].url, '/api/audit/retag-divergence/album/16948/sync-tags',
    'the sync POSTs the canonical route path');
  t.equal(calls[0].options.method, 'POST', 'the sync uses POST');
  t.equal(JSON.parse(calls[0].options.body).expected_mb_albumid, SYNC_DB_ID,
    'the body carries the compare-and-set identity from data-expected');
  t.ok(dom.isRerendered(), 'the row re-renders from the returned album');
  t.contains(dom.container.innerHTML, 'agrees',
    'the patched row shows the post-sync classification');
  t.equal(btn.disabled, true,
    'the detached pre-render button is never resurrected on success');
  t.equal(dom.toast.className, 'toast', 'a success toast is not an error');
}

t.section('syncRetagDivergenceAlbum() residual refusal re-renders AND writes the POST-re-render note');
{
  const dom = installReplacingRetagDom(16948);
  const btn = {
    disabled: false, textContent: 'Write tags',
    dataset: { expected: SYNC_DB_ID },
  };
  stubGlobals({ fetch: async () => ({
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
  }) });

  await syncRetagDivergenceAlbum(16948, btn);

  t.ok(dom.isRerendered(), 'a residual refusal still re-renders the fresh scan');
  t.equal(btn.disabled, true,
    'the detached pre-render button is never resurrected on a re-rendered refusal (#1260 review F11)');
  t.contains(dom.postNote.textContent, 'residual_divergence',
    'the refusal note lands in the POST-re-render note node');
  t.contains(dom.postNote.textContent, 're-read file tags still disagree',
    'the refusal note carries the service detail');
  t.equal(dom.preNote.textContent, '',
    'nothing is written to the destroyed pre-render note node');
  t.equal(dom.toast.className, 'toast error', 'a refusal toast is an error');
}

t.section('syncRetagDivergenceAlbum() album-less refusal re-arms the still-attached button');
{
  const dom = installReplacingRetagDom(42);
  const btn = {
    disabled: false, textContent: 'Write tags',
    dataset: { expected: SYNC_DB_ID },
  };
  stubGlobals({ fetch: async () => ({
    ok: false,
    status: 409,
    json: async () => ({
      outcome: 'identity_mismatch', album_id: 42,
      db_mb_albumid: 'fdc54a6a-27c7-4936-87d7-7ab146812d4e',
      error_message: 'Beets album 42 now names fdc54a6a…; recheck and retry',
      album: null,
    }),
  }) });

  await syncRetagDivergenceAlbum(42, btn);

  t.ok(!dom.isRerendered(), 'no album payload, no re-render');
  t.equal(btn.disabled, false, 'the still-attached button re-arms');
  t.equal(btn.textContent, 'Write tags', 'the button label resets');
  t.contains(dom.visibleNote().textContent, 'identity_mismatch',
    'the visible note names the refusal outcome');
  t.equal(dom.postNote.textContent, '',
    'nothing is written to the unrendered post-render note');
  t.equal(dom.toast.className, 'toast error', 'a refusal toast is an error');
}

t.section('syncRetagDivergenceAlbum() network-error path re-arms the button with a generic note');
{
  const dom = installReplacingRetagDom(16948);
  const btn = {
    disabled: true, textContent: 'Writing tags...',
    dataset: { expected: SYNC_DB_ID },
  };
  stubGlobals({ fetch: async () => {
    throw new TypeError('network down');
  } });

  await syncRetagDivergenceAlbum(16948, btn);

  t.equal(btn.disabled, false, 'the button re-arms after a network failure');
  t.equal(btn.textContent, 'Write tags', 'the button label resets');
  t.equal(dom.preNote.textContent, 'Tag-sync request failed',
    'the note falls back to a generic message');
  t.equal(dom.toast.className, 'toast error', 'a network failure toast is an error');
}

// ---------------------------------------------------------------------------
// The composed entry (issue #1346).
//
// Every render assertion above calls a row renderer directly, and that is
// the right level for the HTML each one produces. What no direct call can
// reach is the composition: `toggleDetail` is what production runs, and it
// decides the order of the fragments it concatenates, which payload field
// feeds each of them, and what it derives before handing anything over.
// Swap `data.current_library` for `data.request` in that function and every
// one of those 99 render assertions still passes — necessarily so, since a
// leaf call passes its own arguments and the composer's cannot reach it.
//
// So the entry gets its own section rather than replacing the leaf calls,
// on the model of `renderPipelineDashboard`'s composer test (PR #1296).
// The leaf calls stay because a needle asserted against the whole panel
// could match a different fragment; each sentinel below is unique to the
// field it proves.
// ---------------------------------------------------------------------------

/** A `/api/pipeline/<id>` envelope carrying one sentinel per composed row. */
function detailEnvelope(requestOverrides = {}) {
  return {
    request: {
      id: 4242,
      artist_name: 'ARTISTSENTINEL',
      album_title: 'ALBUMSENTINEL',
      status: 'wanted',
      mb_release_id: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
      mb_release_group_id: 'rgsentinel-0000-0000-0000-000000000000',
      verified_lossless: true,
      processing_owner: null,
      ...requestOverrides,
    },
    current_library: {
      state: 'unique',
      path: '/library/LIBRARYPATHSENTINEL',
      album_id: 909,
    },
    beets_tracks: [{ bitrate: 900000, format: 'FLAC', title: 'a track' }],
    tracks: [{ title: 'expected one' }, { title: 'expected two' }],
    // `created_at` is not decoration: `renderDownloadHistoryItem` formats it
    // and throws "Invalid time value" without one, which `toggleExpand`
    // turns into the error placeholder. A history row the server can
    // actually emit always carries it.
    history: [
      { outcome: 'rejected', soulseek_username: 'peer-a', created_at: '2026-04-25T23:25:00+00:00' },
      { outcome: 'rejected', soulseek_username: 'peer-b', created_at: '2026-04-24T23:25:00+00:00' },
    ],
    last_search: { variant: 'FORENSICVARIANTSENTINEL', outcome: 'no_match' },
  };
}

/**
 * Open the detail panel the way production does, and hand back its HTML.
 *
 * `toggleExpand` swallows a loader throw into "Failed to load details", so
 * every caller checks for that string: without it a fixture wrong enough to
 * crash the composer would render an error placeholder and pass anything
 * asserted with `excludes`.
 */
async function openDetailPanel(envelope) {
  const panel = element();
  stubGlobals({
    document: domStub({ 4242: panel }),
    fetch: async () => ({ ok: true, status: 200, json: async () => envelope }),
  });
  await toggleDetail(4242);
  return panel.innerHTML;
}

t.section('toggleDetail() composes the panel in order, each row from its own payload field');
{
  const html = await openDetailPanel(detailEnvelope());
  t.excludes(html, 'Failed to load details', 'the composer ran to completion');

  // Ordered needles in composition order. Nine needles over eight
  // fragments: `renderRequestEvidenceSections` contributes two, its
  // download-history block and its in-library block, and their relative
  // order inside that one fragment is worth pinning too.
  const rows = [
    ['aaaaaaaa', 'the external link row renders request.mb_release_id'],
    ['LIBRARYPATHSENTINEL', 'the library row renders current_library'],
    ['verified lossless', 'the quality row reads request.verified_lossless'],
    ['Download History (2)', 'the evidence sections render history'],
    ['In Library (1 tracks)', 'the evidence sections render beets_tracks'],
    ['FORENSICVARIANTSENTINEL', 'the forensic block renders last_search'],
    ["window.updateStatus(4242, 'wanted')", 'the status buttons render request.status'],
    ['window.banSource(4242,', 'the bad-rip button renders below the status buttons'],
    ['window.openReplacePicker({sourceRequestId: 4242', 'the Replace button renders last'],
  ];
  let previous = -1;
  let ordered = true;
  for (const [needle, message] of rows) {
    const at = html.indexOf(needle);
    t.ok(at !== -1, message);
    if (at <= previous) ordered = false;
    previous = at;
  }
  t.ok(ordered, 'the nine needles appear in the composed order');

  // The two values `toggleDetail` derives rather than forwards. Both
  // needles go through `jsArg`, the same encoder the button uses, so the
  // assertion cannot drift from the escaping production applies.
  t.contains(html, `sourceLabel: ${jsArg('ARTISTSENTINEL — ALBUMSENTINEL')}`,
    'the Replace label is built from the request artist and album');
  t.contains(html, `releaseGroupId: ${jsArg('rgsentinel-0000-0000-0000-000000000000')}`,
    'the Replace button carries request.mb_release_group_id');
}

t.section('toggleDetail() withholds Replace on a frozen audit row');
{
  // A replaced row is terminal audit state; re-replacing it is out of scope
  // (R30). The gate lives in the composer, so nothing but the composer can
  // prove it — and the bad-rip button next to it must still render, or the
  // assertion would pass on a panel that simply failed to build.
  const html = await openDetailPanel(detailEnvelope({ status: 'replaced' }));
  t.excludes(html, 'Failed to load details', 'the composer ran to completion');
  t.contains(html, 'window.banSource(4242,',
    'the bad-rip button still renders on a replaced row');
  t.excludes(html, 'window.openReplacePicker',
    'a replaced row offers no Replace button');
}

t.section('renderPipeline() paints the long-tail view as nav then worklist');
{
  // The suite's other entry, and the reason `renderPipelineNav` gets direct
  // calls above: those assert the nav's own HTML, while only this proves
  // the nav is what `renderPipeline` puts in front of the worklist. The
  // dashboard arm is deliberately not driven — it goes straight to
  // `loadPipelineDashboard()` and a fetch, and `test_js_pipeline_dashboard.mjs`
  // owns the composition behind it.
  const content = element();
  stubGlobals({ document: domStub({ 'pipeline-content': content }) });
  const previousView = state.pipelineView;
  const previousLongTail = state.longTail;
  state.pipelineView = 'long-tail';
  state.longTail = {
    rows: [{ id: 501, artist_name: 'LONGTAILARTIST', album_title: 'Cohort', band: 'missing' }],
    band: null,
    query: '',
  };

  renderPipeline();

  const navAt = content.innerHTML.indexOf('window.setPipelineView(');
  const bodyAt = content.innerHTML.indexOf('lt-worklist');
  t.ok(navAt !== -1, 'the long-tail view renders the pipeline nav');
  t.ok(bodyAt !== -1, 'the long-tail view renders the worklist body');
  t.ok(navAt < bodyAt, 'the nav strip is painted before the worklist');
  t.contains(content.innerHTML, 'LONGTAILARTIST',
    'the worklist is built from the cached cohort, not refetched');

  state.pipelineView = previousView;
  state.longTail = previousLongTail;
}

t.section('toggleDetail() hands the exact processing owner to the status buttons');
{
  // Found by PR #1352's mutant runner: forcing the third argument of
  // `renderPipelineStatusButtons` to a bare `null` survived every
  // assertion, because no fixture here had a truthy `processing_owner`.
  // The owner's own status decides the label, so a dropped owner shows
  // "ownership details are unavailable" where this shows job #71.
  const html = await openDetailPanel(detailEnvelope({
    status: 'processing',
    processing_owner: { job_id: 71, status: 'running', preview_status: 'evidence_ready' },
  }));
  t.excludes(html, 'Failed to load details', 'the composer ran to completion');
  t.contains(html, 'job #71 is importing',
    'the lock explanation is derived from the owner the request carried');
  t.contains(html, '/api/import-jobs/71/recovery',
    'the exact owner job is linked for recovery');
  t.excludes(html, 'ownership details are unavailable',
    'the owner reached the status buttons, rather than arriving as null');
  t.excludes(html, "window.updateStatus(4242, 'wanted')",
    'a processing row offers no free-form status change');
}

t.section('toggleDetail() surfaces a failed request as an error panel');
{
  const panel = element();
  stubGlobals({
    document: domStub({ 4242: panel }),
    fetch: async () => ({ ok: false, status: 503, json: async () => ({}) }),
  });
  await toggleDetail(4242);
  t.contains(panel.innerHTML, 'Failed to load details',
    'a non-OK response renders the error placeholder');
  t.equal(panel.classList.contains('open'), true,
    'the panel stays open so the operator sees the failure');
}

t.section('toggleDetail() on an open panel collapses it without refetching');
{
  const panel = element();
  let fetches = 0;
  stubGlobals({
    document: domStub({ 4242: panel }),
    fetch: async () => {
      fetches += 1;
      return { ok: true, status: 200, json: async () => detailEnvelope() };
    },
  });
  await toggleDetail(4242);
  t.equal(fetches, 1, 'opening the panel fetches the request detail');
  // This section does not go through `openDetailPanel`, so it owes the
  // error-placeholder check itself: without it, a composer that threw on
  // the first open would satisfy both counters below and the section would
  // pass while asserting nothing about a rendered panel.
  t.excludes(panel.innerHTML, 'Failed to load details',
    'the first open rendered a real panel, not the error placeholder');
  await toggleDetail(4242);
  t.equal(fetches, 1, 'closing it again does not refetch');
  t.equal(panel.classList.contains('open'), false, 'the panel is collapsed');
}

t.done();
