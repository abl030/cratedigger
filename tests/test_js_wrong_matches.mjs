/**
 * Unit tests for web/js/wrong-matches.js polling behavior.
 * Run with: node tests/test_js_wrong_matches.mjs
 */

import {
  __test__,
  forceImportWrongMatch,
} from '../web/js/wrong-matches.js';
import { esc } from '../web/js/util.js';

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

function assertDeepEqual(actual, expected, msg) {
  assertEqual(JSON.stringify(actual), JSON.stringify(expected), msg);
}

function countOccurrences(text, needle) {
  return (String(text).match(new RegExp(needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g')) || []).length;
}

function metadataHtmlIsEscaped(html, value) {
  return !html.includes(value) && html.includes(esc(value));
}

function installStorage() {
  const values = new Map();
  globalThis.localStorage = {
    getItem(key) {
      return values.has(key) ? values.get(key) : null;
    },
    setItem(key, value) {
      values.set(key, String(value));
    },
    removeItem(key) {
      values.delete(key);
    },
    clear() {
      values.clear();
    },
  };
  return values;
}

/**
 * A minimal stand-in DOM element: `textContent`/`disabled`/`style`, a
 * `remove()` that just flags itself, and whatever fields the caller wants
 * to seed. Shared by every test that needs `document.getElementById` to
 * resolve a specific button/badge id (issue #1086 review blocker 2).
 * @param {Object} [initial]
 * @returns {any}
 */
function fakeElement(initial = {}) {
  return {
    textContent: '',
    disabled: false,
    style: {},
    removed: false,
    remove() { this.removed = true; },
    ...initial,
  };
}

/**
 * `installDom()` always wires `wrong-matches-content` and `toast`; the
 * returned `elements` Map is an open registry a test can `.set(id, el)`
 * BEFORE exercising code that looks up an id `installDom` doesn't know
 * about by default (`wm-delete-group-btn-<id>`, `wm-entry-card-<id>`,
 * `wm-release-<id>`, …) — a shared extension point, not a one-off inline
 * `getElementById` override per test.
 */
function installDom() {
  const wrongMatches = { innerHTML: '' };
  const toast = {
    textContent: '',
    className: '',
    style: { display: 'none' },
  };
  // A plain object stand-in for the Stop button (issue #1083) — real
  // production code re-fetches it by id each time bulkTriageWrongMatches
  // runs, exactly like the browser's live DOM. Registered in the open
  // element map (issue #1086) so any id a test needs can be seeded the
  // same way rather than adding another special case here.
  const stopBtn = { id: 'wm-bulk-triage-stop-btn', disabled: true, textContent: 'Stop' };
  const elements = new Map([
    ['wrong-matches-content', wrongMatches],
    ['toast', toast],
    ['wm-bulk-triage-stop-btn', stopBtn],
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
  return { wrongMatches, toast, elements, stopBtn };
}

function wrongMatchesData() {
  return {
    groups: [{
      request_id: 42,
      artist: 'Scott Walker',
      album: 'Scott 3',
      mb_release_id: '1290612',
      in_library: false,
      pending_count: 3,
      quality_rank: null,
      status: 'wanted',
      entries: [
        { download_log_id: 100, soulseek_username: 'u1', distance: 0.167, scenario: 'high_distance', source_dirs: ['user1\\Scott Walker - Scott 3'], local_items: [{ path: '01.mp3', format: 'MP3' }] },
        { download_log_id: 101, soulseek_username: 'u2', distance: 0.180, scenario: 'high_distance', source_dirs: ['user2\\Scott Walker - Scott 3'], local_items: [{ path: '02.mp3', format: 'MP3' }] },
        { download_log_id: 102, soulseek_username: 'u3', distance: 0.226, scenario: 'high_distance', source_dirs: ['user3\\Scott Walker - Scott 3'], local_items: [{ path: '03.mp3', format: 'MP3' }] },
      ],
    }],
  };
}

async function runPoll(job, logId) {
  installStorage();
  const calls = [];
  const dom = installDom();
  __test__.renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  globalThis.fetch = async (url) => {
    calls.push(url);
    if (String(url).startsWith('/api/import-jobs/')) {
      return {
        ok: true,
        json: async () => ({ job }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  const btn = { textContent: '', style: {} };
  await __test__.pollImportJob(17, btn, logId);
  return { calls, dom, btn };
}

console.log('_pollImportJob() removes row in place after completed jobs — no full refresh');
{
  const { calls, dom, btn } = await runPoll({
    status: 'completed',
    message: 'Import completed',
  }, 100);
  assertEqual(btn.textContent, 'Imported', 'button shows imported');
  assert(!calls.includes('/api/wrong-matches'),
    'does NOT refetch the queue on completion (in-place removal)');
  assertEqual(dom.toast.className, 'toast', 'completion toast is not an error');
}

console.log('_pollImportJob() leaves row visible after failed jobs — no full refresh');
{
  const { calls, dom, btn } = await runPoll({
    status: 'failed',
    message: 'Pre-import gate rejected',
  }, 100);
  assertEqual(btn.textContent, 'Failed', 'button shows failed');
  assert(!calls.includes('/api/wrong-matches'),
    'does NOT refetch the queue on failure (ambiguous source state)');
  assertEqual(dom.toast.className, 'toast error', 'failure toast is an error');
}

console.log('_pollImportJob() surfaces historical recovery while convergence continues');
{
  const { calls, dom, btn } = await runPoll({
    status: 'recovery_required',
    message: 'Recovery required: Beets may have run',
  }, 100);
  assertEqual(btn.textContent, 'Recovery required', 'button shows historical recovery');
  assert(!calls.includes('/api/wrong-matches'),
    'does NOT refetch or imply the ambiguous operation completed');
  assertEqual(dom.toast.className, 'toast error', 'historical recovery is prominent');
}

console.log('forceImportWrongMatch() maps processing conflict to the shared locked row state');
{
  const oldConfirm = globalThis.confirm;
  const oldDocument = globalThis.document;
  const oldFetch = globalThis.fetch;
  const oldWindow = globalThis.window;
  const attributes = new Map();
  const inserted = [];
  const btn = {
    dataset: {},
    disabled: false,
    textContent: 'Force Import',
    style: {},
    isConnected: true,
    setAttribute(name, value) { attributes.set(name, value); },
    removeAttribute(name) { attributes.delete(name); },
    getAttribute(name) { return attributes.get(name) || null; },
    focus() {},
    insertAdjacentElement(_position, element) {
      element.isConnected = true;
      inserted.push(element);
    },
  };
  const live = { textContent: '', setAttribute() {} };
  globalThis.confirm = () => true;
  globalThis.document = {
    activeElement: btn,
    body: { appendChild() {} },
    createElement() {
      return {
        children: [],
        className: '',
        id: '',
        textContent: '',
        isConnected: false,
        setAttribute() {},
        appendChild(child) { this.children.push(child); },
        remove() { this.isConnected = false; },
      };
    },
    getElementById(id) {
      if (id === 'processing-lock-live-region') return live;
      return inserted.find(element => element.id === id && element.isConnected) || null;
    },
    querySelectorAll() { return [btn]; },
  };
  globalThis.window = { scrollX: 0, scrollY: 0, scrollTo() {} };
  const calls = [];
  globalThis.fetch = async (url) => {
    calls.push(String(url));
    if (url === '/api/pipeline/force-import') {
      return {
        status: 409,
        async json() {
          return {
            error: 'processing_locked',
            request_id: 42,
            processing_owner: {
              job_id: 71,
              status: 'queued',
              preview_status: 'running',
            },
          };
        },
      };
    }
    if (url === '/api/pipeline/42') {
      return {
        ok: true,
        async json() {
          return {
            request: {
              id: 42,
              status: 'processing',
              mb_release_id: 'wrong-match-owner',
              processing_owner: {
                job_id: 71,
                status: 'queued',
                preview_status: 'evidence_ready',
              },
            },
          };
        },
      };
    }
    throw new Error(`unexpected fetch ${url}`);
  };
  await forceImportWrongMatch(100, btn);
  assertEqual(calls.join(','), '/api/pipeline/force-import,/api/pipeline/42',
    'force import refetches only the owner request');
  assertEqual(attributes.get('aria-disabled'), 'true', 'force-import control locks');
  assertEqual(btn.textContent, 'waiting to import', 'fresh owner state is rendered');
  assert(live.textContent.includes('job #71'), 'exact owner is announced');
  globalThis.confirm = oldConfirm;
  globalThis.document = oldDocument;
  globalThis.fetch = oldFetch;
  globalThis.window = oldWindow;
}

console.log('converge helpers classify green candidates');
{
  installStorage();
  assertEqual(__test__.normalizeThreshold(undefined), 180, 'default threshold is 180');
  assertEqual(__test__.normalizeThreshold('9999'), 999, 'threshold is clamped high');
  assertEqual(__test__.normalizeThreshold('-5'), 0, 'threshold is clamped low');
  assert(__test__.isConvergeGreen({ distance: 0.167 }, 180), '0.167 is green at 180');
  assert(__test__.isConvergeGreen({ distance: 0.180 }, 180), '0.180 is green at 180');
  assert(!__test__.isConvergeGreen({ distance: 0.226 }, 180), '0.226 is not green at 180');
  assert(!__test__.isConvergeGreen({ distance: null }, 180), 'missing distance is not green');
  assertDeepEqual(
    __test__.convergeRequestBody('42', '180'),
    { request_id: 42, threshold_milli: 180, delete_unmatched: true },
    'converge always asks the API to delete non-green rows',
  );
}

console.log('renderWrongMatches() shows threshold controls and green state');
{
  installStorage();
  const dom = installDom();
  __test__.renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  assert(dom.wrongMatches.innerHTML.includes('Loosen'), 'renders threshold input');
  assert(dom.wrongMatches.innerHTML.includes('2 green'), 'renders default green count');
  assert(dom.wrongMatches.innerHTML.includes('Converge (2)'), 'converge button includes count');
  assert(!dom.wrongMatches.innerHTML.includes('remove all wrong matches when converging'), 'cleanup checkbox is gone');
  assert(dom.wrongMatches.innerHTML.includes('Cleanup Wrong Matches (3)'), 'renders full-queue cleanup action');
  assert(dom.wrongMatches.innerHTML.includes('Delete All (3)'), 'renders per-group delete-all action');
  assert(dom.wrongMatches.innerHTML.includes('deleteWrongMatch(100'), 'renders per-entry delete action');
  assert(
    dom.wrongMatches.innerHTML.includes('data-pipeline-request-id="42"')
      && dom.wrongMatches.innerHTML.includes('forceImportWrongMatch(100, this)'),
    'force-import controls carry exact request identity and initiating control',
  );

  __test__.setWrongMatchConvergeThreshold(42, 230);
  assert(dom.wrongMatches.innerHTML.includes('3 green'), 'threshold edit updates green count');
  assert(dom.wrongMatches.innerHTML.includes('Converge (3)'), 'threshold edit updates converge count');
}

console.log('renderWrongMatches() keeps converge usable with active import jobs');
{
  installStorage();
  const dom = installDom();
  const data = JSON.parse(JSON.stringify(wrongMatchesData()));
  data.groups[0].import_jobs = [{
    id: 9,
    status: 'queued',
    request_id: 42,
    job_type: 'force_import',
  }];
  __test__.renderWrongMatches(data, dom.wrongMatches);

  assert(!dom.wrongMatches.innerHTML.includes('Import Active'), 'does not replace converge with Import Active');
  assert(dom.wrongMatches.innerHTML.includes('Converge (2)'), 'keeps converge label with active jobs');
  assert(!/id="wm-converge-btn-42"[^>]*disabled/.test(dom.wrongMatches.innerHTML), 'active jobs do not disable converge');
}

console.log('setWrongMatchConvergeThreshold() updates expanded group in place');
{
  installStorage();
  const dom = installDom();
  __test__.renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  const originalHtml = dom.wrongMatches.innerHTML;
  const elements = new Map();
  const el = (initial = {}) => ({
    textContent: '',
    disabled: false,
    style: {},
    removed: false,
    remove() { this.removed = true; },
    ...initial,
  });
  elements.set('wm-green-count-42', el());
  elements.set('wm-converge-btn-42', el({ textContent: 'Converge (2)' }));
  for (const id of [100, 101, 102]) {
    elements.set(`wm-entry-card-${id}`, el());
    elements.set(`wm-entry-green-${id}`, el());
    elements.set(`wm-entry-dist-${id}`, el());
  }
  globalThis.document.getElementById = (id) => {
    if (id === 'wrong-matches-content') return dom.wrongMatches;
    if (id === 'toast') return dom.toast;
    return elements.get(id) || null;
  };

  __test__.setWrongMatchConvergeThreshold(42, 230);

  assertEqual(dom.wrongMatches.innerHTML, originalHtml, 'threshold edit does not rerender the pane');
  assertEqual(elements.get('wm-green-count-42').textContent, '3 green', 'updates green count badge');
  assertEqual(elements.get('wm-converge-btn-42').textContent, 'Converge (3)', 'updates converge button text');
  assert(!String(elements.get('wm-entry-green-102').style.cssText || '').includes('display:none'), 'newly green entry badge is shown');
}

console.log('convergeWrongMatches() posts selected threshold and removes row in place');
{
  installStorage();
  const dom = installDom();
  __test__.renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  __test__.setWrongMatchConvergeThreshold(42, 180);
  const calls = [];
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url, options });
    if (url === '/api/wrong-matches/converge') {
      return {
        ok: true,
        json: async () => ({
          status: 'ok',
          queued: 2,
          deleted: 1,
          skipped: [],
          group_empty: true,
        }),
      };
    }
    if (url === '/api/wrong-matches') {
      return {
        ok: true,
        json: async () => ({ groups: [] }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  const btn = { disabled: false, textContent: 'Converge', style: {} };
  await __test__.convergeWrongMatches(42, btn);
  assertEqual(calls[0].url, '/api/wrong-matches/converge', 'posts to converge endpoint');
  assertDeepEqual(
    JSON.parse(calls[0].options.body),
    { request_id: 42, threshold_milli: 180, delete_unmatched: true },
    'posts converge payload',
  );
  assert(!calls.some(call => call.url === '/api/wrong-matches'), 'does not refetch the whole wrong-matches pane');
  assert(dom.toast.textContent.includes('Queued 2 candidates'), 'toasts converge result');
  assert(dom.wrongMatches.innerHTML.includes('No wrong matches'), 'removes the emptied group locally');
}

console.log('deleteWrongMatch() posts one row and removes it in place — no full refresh');
{
  installStorage();
  const dom = installDom();
  __test__.renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  const calls = [];
  globalThis.confirm = () => true;
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url, options });
    if (url === '/api/wrong-matches/delete') {
      return {
        ok: true,
        json: async () => ({
          status: 'ok',
          success: true,
          deleted_path: '/fi/a',
          cleared_rows: 1,
        }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  const btn = { disabled: false, textContent: 'Delete', style: {} };
  await __test__.deleteWrongMatch(100, btn);
  assertEqual(calls[0].url, '/api/wrong-matches/delete', 'posts to row delete endpoint');
  assertDeepEqual(
    JSON.parse(calls[0].options.body),
    { download_log_id: 100 },
    'posts selected download log id',
  );
  assert(!calls.some(call => call.url === '/api/wrong-matches'),
    'does NOT refetch the queue after row delete (in-place removal)');
  assert(dom.toast.textContent.includes('Deleted wrong match'), 'toasts row delete result');
}

console.log('deleteWrongMatchGroup() posts request id and removes the group in place');
{
  installStorage();
  const dom = installDom();
  __test__.renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  const calls = [];
  globalThis.confirm = () => true;
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url, options });
    if (url === '/api/wrong-matches/delete-group') {
      return {
        ok: true,
        json: async () => ({
          status: 'ok',
          processed: 3,
          deleted: 3,
          skipped: 0,
          errors: 0,
          remaining: 0,
        }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  const btn = { disabled: false, textContent: 'Delete All (3)', style: {} };
  await __test__.deleteWrongMatchGroup(42, btn);
  assertEqual(calls[0].url, '/api/wrong-matches/delete-group', 'posts to group delete endpoint');
  assert(!calls.some(call => call.url === '/api/wrong-matches'),
    'does NOT refetch the queue after group delete (in-place removal)');
  assertDeepEqual(
    JSON.parse(calls[0].options.body),
    { request_id: 42 },
    'posts selected request id',
  );
  // "candidates" became "folders": a pointer-only clear over an
  // already-missing folder is counted separately and never headlined as a
  // deletion (issue #1063).
  assert(dom.toast.textContent.includes('Deleted 3 folders'), 'toasts group delete result');
}

console.log('delete controls handle cancel and failures');
{
  installStorage();
  const dom = installDom();
  __test__.renderWrongMatches(wrongMatchesData(), dom.wrongMatches);

  let calls = [];
  globalThis.confirm = () => false;
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url, options });
    throw new Error(`unexpected fetch: ${url}`);
  };
  const cancelBtn = { disabled: false, textContent: 'Delete', style: {} };
  await __test__.deleteWrongMatch(100, cancelBtn);
  assertEqual(calls.length, 0, 'row delete cancel does not fetch');
  assertEqual(cancelBtn.disabled, false, 'row delete cancel leaves button enabled');

  globalThis.confirm = () => true;
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url, options });
    if (url === '/api/wrong-matches/delete') {
      return {
        ok: false,
        json: async () => ({ error: 'active_import_job' }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  const failBtn = { disabled: false, textContent: 'Delete', style: {} };
  await __test__.deleteWrongMatch(100, failBtn);
  assertEqual(failBtn.disabled, false, 'row delete API failure restores button enabled');
  assertEqual(failBtn.textContent, 'Delete', 'row delete API failure restores button text');
  assertEqual(dom.toast.className, 'toast error', 'row delete API failure shows error toast');

  globalThis.fetch = async () => {
    throw new Error('network down');
  };
  const errorBtn = { disabled: false, textContent: 'Delete', style: {} };
  await __test__.deleteWrongMatch(100, errorBtn);
  assertEqual(errorBtn.disabled, false, 'row delete fetch exception restores button enabled');
  assertEqual(errorBtn.textContent, 'Delete', 'row delete fetch exception restores button text');

  calls = [];
  globalThis.confirm = () => false;
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url, options });
    throw new Error(`unexpected fetch: ${url}`);
  };
  const cancelGroupBtn = { disabled: false, textContent: 'Delete All (3)', style: {} };
  await __test__.deleteWrongMatchGroup(42, cancelGroupBtn);
  assertEqual(calls.length, 0, 'group delete cancel does not fetch');

  globalThis.confirm = () => true;
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url, options });
    if (url === '/api/wrong-matches/delete-group') {
      return {
        ok: false,
        json: async () => ({ error: 'cleanup_lock_unavailable' }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  const failGroupBtn = { disabled: false, textContent: 'Delete All (3)', style: {} };
  await __test__.deleteWrongMatchGroup(42, failGroupBtn);
  assertEqual(failGroupBtn.disabled, false, 'group delete API failure restores button enabled');
  assertEqual(failGroupBtn.textContent, 'Delete All (3)', 'group delete API failure restores button text');
  assertEqual(dom.toast.className, 'toast error', 'group delete API failure shows error toast');

  globalThis.fetch = async () => {
    throw new Error('network down');
  };
  const errorGroupBtn = { disabled: false, textContent: 'Delete All (3)', style: {} };
  await __test__.deleteWrongMatchGroup(42, errorGroupBtn);
  assertEqual(errorGroupBtn.disabled, false, 'group delete fetch exception restores button enabled');
  assertEqual(errorGroupBtn.textContent, 'Delete All (3)', 'group delete fetch exception restores button text');
}

console.log('bulkTriageWrongMatches() posts full-queue confirmation and refreshes');
{
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  __test__.renderWrongMatches(data, dom.wrongMatches);
  assert(dom.wrongMatches.innerHTML.includes('Cleanup Wrong Matches (3)'), 'renders full-queue cleanup button');
  const calls = [];
  globalThis.confirm = () => true;
  // The sweep runs server-side on a background thread; the client polls.
  // Collapse the poll delay so the test doesn't sleep for real.
  const realSetTimeout = globalThis.setTimeout;
  globalThis.setTimeout = (fn) => { fn(); return 0; };
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url, options });
    if (url === '/api/wrong-matches/triage') {
      return {
        ok: true,
        status: 202,
        json: async () => ({ status: 'started', state: 'running' }),
      };
    }
    if (url === '/api/wrong-matches/triage/status') {
      return {
        ok: true,
        status: 200,
        json: async () => ({
          state: 'completed',
          started_at: '2026-06-11T00:00:00+00:00',
          finished_at: '2026-06-11T00:01:00+00:00',
          error: null,
          summary: {
            processed: 3,
            deleted: 2,
            kept_would_import: 1,
            kept_uncertain: 0,
            skipped_candidate_evidence_missing: 0,
            skipped_candidate_evidence_stale: 0,
            skipped_current_evidence_missing: 0,
            skipped_current_evidence_stale: 0,
            skipped_active_job: 0,
            skipped_invalid_row: 0,
            skipped_missing_path: 0,
            skipped_operational: 0,
            delete_failed: 0,
            results: [],
          },
        }),
      };
    }
    if (url === '/api/wrong-matches') {
      return {
        ok: true,
        status: 200,
        json: async () => ({ groups: [] }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  const btn = { disabled: false, textContent: 'Cleanup Wrong Matches (3)', style: {} };
  // The Stop button is enabled the moment the sweep starts (before the
  // first status poll even fires) and disabled again once it's done.
  let stopBtnEnabledDuringSweep = null;
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url, options) => {
    if (stopBtnEnabledDuringSweep === null) {
      stopBtnEnabledDuringSweep = dom.stopBtn.disabled === false;
    }
    return realFetch(url, options);
  };
  await __test__.bulkTriageWrongMatches(btn);
  assert(stopBtnEnabledDuringSweep, 'Stop button is enabled while the sweep runs');
  assertEqual(dom.stopBtn.disabled, true, 'Stop button is disabled again once the sweep completes');
  assertEqual(dom.stopBtn.textContent, 'Stop', 'Stop button label is restored');
  assertEqual(calls[0].url, '/api/wrong-matches/triage', 'posts to cleanup endpoint');
  assertDeepEqual(
    JSON.parse(calls[0].options.body),
    { confirm_all_wrong_matches: true },
    'posts explicit full-queue confirmation',
  );
  assert(calls.some(call => call.url === '/api/wrong-matches/triage/status'),
    'polls the background sweep status');
  assert(calls.some(call => call.url === '/api/wrong-matches'), 'refetches the full pane after cleanup');
  assert(dom.toast.textContent.includes('Deleted 2 candidates'), 'toasts cleanup result');
  assert(dom.wrongMatches.innerHTML.includes('No wrong matches'), 'renders refreshed empty state');
  globalThis.setTimeout = realSetTimeout;
}

console.log('bulkTriageWrongMatches() handles a restart-lost sweep as partial, not failed');
{
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  __test__.renderWrongMatches(data, dom.wrongMatches);
  globalThis.confirm = () => true;
  const realSetTimeout = globalThis.setTimeout;
  globalThis.setTimeout = (fn) => { fn(); return 0; };
  globalThis.fetch = async (url, _options = {}) => {
    if (url === '/api/wrong-matches/triage') {
      return {
        ok: true,
        status: 202,
        json: async () => ({ status: 'started', state: 'running' }),
      };
    }
    if (url === '/api/wrong-matches/triage/status') {
      // Web service restarted mid-sweep: fresh runner reports idle.
      return {
        ok: true,
        status: 200,
        json: async () => ({
          state: 'idle',
          started_at: null,
          finished_at: null,
          error: null,
          summary: null,
        }),
      };
    }
    if (url === '/api/wrong-matches') {
      return {
        ok: true,
        status: 200,
        json: async () => ({ groups: [] }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  const btn = { disabled: true, textContent: 'Cleaning...', style: {} };
  await __test__.bulkTriageWrongMatches(btn);
  assertEqual(btn.disabled, false, 'restart-lost sweep restores button enabled');
  assert(dom.toast.textContent.includes('status lost'), 'restart-lost sweep explains the lost status');
  assert(!dom.toast.textContent.includes('failed'), 'restart-lost sweep is not reported as failed');
  assert(dom.wrongMatches.innerHTML.includes('No wrong matches'), 'restart-lost sweep still refreshes the pane');
  globalThis.setTimeout = realSetTimeout;
}

console.log('bulkTriageWrongMatches() reports a cancelled sweep distinctly from completion (issue #1083)');
{
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  __test__.renderWrongMatches(data, dom.wrongMatches);
  globalThis.confirm = () => true;
  const realSetTimeout = globalThis.setTimeout;
  globalThis.setTimeout = (fn) => { fn(); return 0; };
  globalThis.fetch = async (url, _options = {}) => {
    if (url === '/api/wrong-matches/triage') {
      return {
        ok: true,
        status: 202,
        json: async () => ({ status: 'started', state: 'running' }),
      };
    }
    if (url === '/api/wrong-matches/triage/status') {
      return {
        ok: true,
        status: 200,
        json: async () => ({
          state: 'cancelled',
          started_at: '2026-06-11T00:00:00+00:00',
          finished_at: '2026-06-11T00:01:00+00:00',
          error: null,
          summary: {
            processed: 1,
            deleted: 1,
            kept_would_import: 0,
            kept_uncertain: 0,
            skipped_candidate_evidence_missing: 0,
            skipped_candidate_evidence_stale: 0,
            skipped_current_evidence_missing: 0,
            skipped_current_evidence_stale: 0,
            skipped_active_job: 0,
            skipped_invalid_row: 0,
            skipped_missing_path: 0,
            skipped_operational: 0,
            delete_failed: 0,
            results: [],
            cancelled: true,
          },
        }),
      };
    }
    if (url === '/api/wrong-matches') {
      return {
        ok: true,
        status: 200,
        json: async () => ({ groups: [] }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  const btn = { disabled: true, textContent: 'Cleaning...', style: {} };
  await __test__.bulkTriageWrongMatches(btn);
  assertEqual(btn.disabled, false, 'cancelled sweep restores button enabled');
  assertEqual(dom.stopBtn.disabled, true, 'Stop button is disabled once the sweep reaches a terminal state');
  assert(dom.toast.textContent.includes('stopped'), 'cancelled sweep says "stopped", not "completed"');
  assert(dom.toast.textContent.includes('Deleted 1 candidate'), 'cancelled sweep still reports what ran');
  assertEqual(dom.toast.className, 'toast', 'cancelled sweep is not toasted as an error');
  assert(dom.wrongMatches.innerHTML.includes('No wrong matches'), 'cancelled sweep still refreshes the pane');
  globalThis.setTimeout = realSetTimeout;
}

console.log('stopWrongMatchTriage() posts to the cancel endpoint and stays disabled on success');
{
  installStorage();
  installDom();
  const calls = [];
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url, options });
    return { ok: true, status: 200, json: async () => ({ state: 'running' }) };
  };
  const btn = { disabled: false, textContent: 'Stop' };
  await __test__.stopWrongMatchTriage(btn);
  assertEqual(calls.length, 1, 'posts exactly one cancel request');
  assertEqual(calls[0].url, '/api/wrong-matches/triage/cancel', 'posts to the canonical cancel route');
  assertEqual(calls[0].options.method, 'POST', 'cancel is a POST');
  assertEqual(btn.disabled, true, 'button stays disabled after a successful cancel request');
  assertEqual(btn.textContent, 'Stopping...', 'button shows the in-flight stopping state');
}

console.log('stopWrongMatchTriage() re-enables the button when the request itself fails');
{
  installStorage();
  const dom = installDom();
  globalThis.fetch = async () => {
    throw new Error('network down');
  };
  const btn = { disabled: false, textContent: 'Stop' };
  await __test__.stopWrongMatchTriage(btn);
  assertEqual(btn.disabled, false, 'a failed cancel request restores the button enabled');
  assertEqual(btn.textContent, 'Stop', 'a failed cancel request restores the button label');
  assert(dom.toast.textContent.includes('Stop request failed'), 'a failed cancel request is toasted');
}

console.log('bulkTriageWrongMatches() surfaces a failed sweep and restores the button');
{
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  __test__.renderWrongMatches(data, dom.wrongMatches);
  globalThis.confirm = () => true;
  const realSetTimeout = globalThis.setTimeout;
  globalThis.setTimeout = (fn) => { fn(); return 0; };
  globalThis.fetch = async (url, _options = {}) => {
    if (url === '/api/wrong-matches/triage') {
      return {
        ok: true,
        status: 202,
        json: async () => ({ status: 'started', state: 'running' }),
      };
    }
    if (url === '/api/wrong-matches/triage/status') {
      return {
        ok: true,
        status: 200,
        json: async () => ({
          state: 'failed',
          started_at: '2026-06-11T00:00:00+00:00',
          finished_at: '2026-06-11T00:01:00+00:00',
          error: 'RuntimeError: sweep blew up',
          summary: null,
        }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  const btn = { disabled: true, textContent: 'Cleaning...', style: {} };
  await __test__.bulkTriageWrongMatches(btn);
  assertEqual(btn.disabled, false, 'failed sweep restores button enabled');
  assertEqual(btn.textContent, 'Cleanup Wrong Matches (3)', 'failed sweep restores button text');
  assert(dom.toast.textContent.includes('sweep blew up'), 'failed sweep toasts the error');
  assertEqual(dom.toast.className, 'toast error', 'failed sweep shows error toast');
  globalThis.setTimeout = realSetTimeout;
}

console.log('formatEntryEvidence() formats spectral and lossless-source V0 cells');
{
  const request6039 = __test__.formatEntryEvidence({
    format: 'MP3',
    min_bitrate: 194,
    avg_bitrate: 288,
  });
  assertEqual(
    request6039.format,
    'MP3 avg 288k · min 194k',
    'current candidate summary labels average and retains the floor',
  );

  const gas = __test__.formatEntryEvidence({
    source_codec: 'flac',
    source_container: 'flac',
    target_format: 'opus 128',
    format: 'opus 128',
    min_bitrate: 191,
    avg_bitrate: 224,
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_avg_bitrate: 224,
  });
  assertEqual(
    gas.format,
    'FLAC → OPUS 128 contract',
    'Gas source and target render separately without relabelling the V0 proxy',
  );
  assertEqual(gas.v0, 'V0 ≈ 224 kbps', 'Gas V0 probe remains its own fact');
  assert(!gas.format.includes('191'), 'target contract does not claim the V0 min');
  assert(!gas.format.includes('224'), 'target contract does not claim the V0 average');

  // Happy path: AE1 — both pieces of evidence present.
  let cells = __test__.formatEntryEvidence({
    spectral_grade: 'genuine',
    spectral_bitrate: 950,
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_avg_bitrate: 265,
  });
  assert(cells.spectral.includes('genuine'), 'spectral cell shows the grade');
  assert(cells.spectral.includes('950'), 'spectral cell shows the bitrate floor');
  assert(cells.v0.includes('265'), 'V0 cell shows the lossless-source probe average');

  // AE2: missing evidence renders as a dash, not as a preview trigger.
  cells = __test__.formatEntryEvidence({
    spectral_grade: null,
    spectral_bitrate: null,
    v0_probe_kind: null,
    v0_probe_avg_bitrate: null,
  });
  assertEqual(cells.spectral, '—', 'absent spectral evidence renders as a dash');
  assertEqual(cells.v0, '—', 'absent V0 evidence renders as a dash');
  assert(!cells.spectral.toLowerCase().includes('preview'), 'no preview trigger in spectral cell');
  assert(!cells.v0.toLowerCase().includes('preview'), 'no preview trigger in V0 cell');

  // Wrong-match review surfaces V0 evidence regardless of source lineage —
  // operators want to compare every candidate's bitrate at a glance, not
  // just the lossless-source ones. Whichever probe ran, show the average.
  cells = __test__.formatEntryEvidence({
    spectral_grade: 'suspect',
    spectral_bitrate: 320,
    v0_probe_kind: 'native_lossy_research_v0',
    v0_probe_avg_bitrate: 240,
  });
  assert(cells.spectral.includes('suspect'), 'spectral cell still renders for suspect grade');
  assert(cells.v0.includes('240'),
    'V0 probe surfaces regardless of source lineage for wrong-match review');

  // Edge: spectral present, V0 absent (rejected pre-conversion).
  cells = __test__.formatEntryEvidence({
    spectral_grade: 'marginal',
    spectral_bitrate: 800,
    v0_probe_kind: null,
    v0_probe_avg_bitrate: null,
  });
  assert(cells.spectral.includes('marginal'), 'marginal grade renders');
  assertEqual(cells.v0, '—', 'absent V0 still renders as dash');

  // Edge: missing the four keys entirely (extra defensive — payload should
  // always include them, but the renderer must not crash if it doesn't).
  cells = __test__.formatEntryEvidence({});
  assertEqual(cells.spectral, '—', 'missing keys render as dash');
  assertEqual(cells.v0, '—', 'missing keys render as dash');
}

console.log('renderQualityBadges() labels current average and retained floor fallbacks');
{
  let html = __test__.renderQualityBadges({
    in_library: true,
    quality_label: null,
    format: null,
    avg_bitrate: 288,
    min_bitrate: 194,
  });
  assert(html.includes('avg 288k · min 194k'),
    'missing-format fallback leads with the current average and labels the floor');
  assert(!html.includes('>194k<'), 'minimum bitrate is never rendered as a bare current tier');

  html = __test__.renderQualityBadges({
    in_library: true,
    quality_label: null,
    format: null,
    avg_bitrate: null,
    min_bitrate: 194,
  });
  assert(html.includes('min 194k'), 'missing-average fallback labels minimum as floor data');
  assert(!html.includes('>194k<'), 'missing average never revives a bare min-derived tier');

  html = __test__.renderQualityBadges({
    in_library: true,
    quality_label: null,
    format: null,
    avg_bitrate: 288,
    min_bitrate: null,
  });
  assert(html.includes('avg 288k'), 'average-only fallback remains visible current data');

  html = __test__.renderQualityBadges({
    in_library: true,
    quality_label: 'MP3 V0',
    format: 'MP3',
    avg_bitrate: 288,
    min_bitrate: 194,
  });
  assert(html.includes('MP3 V0'), 'explicit backend quality label remains authoritative');
  assert(!html.includes('avg 288k'), 'fallback summary is omitted with an explicit label');

  assertEqual(
    __test__.renderQualityBadges({
      in_library: false,
      quality_label: null,
      format: null,
      avg_bitrate: 0,
      min_bitrate: 0,
    }),
    '<span class="badge" style="background:#3a2a2a;color:#f88;">nothing on disk</span>',
    'zero bitrate placeholders are treated as absent off disk',
  );
  assertEqual(
    __test__.renderQualityBadges({
      in_library: true,
      quality_label: null,
      format: null,
      avg_bitrate: null,
      min_bitrate: null,
    }),
    '',
    'partial in-library rows with null quality data remain defensively empty',
  );
}

console.log('wrong-match headers use the shared ordered spectral badge palette');
for (const [grade, tone] of [
  ['likely_transcode', 'poor'],
  ['suspect', 'acceptable'],
  ['marginal', 'good'],
]) {
  const html = __test__.renderQualityBadges({
    in_library: true,
    quality_label: 'MP3 V2',
    quality_rank: 'excellent',
    current_spectral_grade: grade,
    current_spectral_bitrate: 128,
  });
  assert(html.includes(`badge-rank-${tone}`), `${grade} uses shared ${tone} badge`);
  assert(html.includes(grade.replaceAll('_', ' ')), `${grade} is humanized`);
  if (grade.includes('_')) assert(!html.includes(grade), `${grade} raw token stays hidden`);
}

console.log('wrong-match bucket badges use the same canonical classes as every other view');
for (const rank of ['poor', 'acceptable', 'good', 'excellent', 'transparent', 'lossless']) {
  const html = __test__.renderQualityBadges({
    in_library: true,
    quality_label: rank,
    quality_rank: rank,
  });
  assert(html.includes(`badge-rank-${rank}`), `${rank} uses its canonical rank class`);
}

console.log('wrong-match verified-lossless identity reuses the lossless bucket colour');
{
  const html = __test__.renderQualityBadges({
    in_library: true,
    quality_label: 'FLAC',
    quality_rank: 'lossless',
    verified_lossless: true,
  });
  assert(html.includes('verified lossless'), 'verified identity remains explicit');
  assert(countOccurrences(html, 'badge-rank-lossless') === 3,
    'quality label, verified identity, and rank label share lossless colour');
}

console.log('renderEntry() embeds evidence cells without preview hooks');
{
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  data.groups[0].entries[0].spectral_grade = 'suspect';
  data.groups[0].entries[0].spectral_bitrate = 320;
  data.groups[0].entries[0].v0_probe_kind = 'lossless_source_v0';
  data.groups[0].entries[0].v0_probe_avg_bitrate = 265;
  __test__.renderWrongMatches(data, dom.wrongMatches);
  const html = dom.wrongMatches.innerHTML;
  assert(html.includes('suspect'), 'rendered HTML carries the spectral grade');
  assert(html.includes('quality-tone-acceptable'),
    'candidate spectral metadata uses the same orange suspect tone');
  assert(html.includes('265'), 'rendered HTML carries the lossless-source V0 average');
  assert(html.includes('Downloaded as'), 'rendered HTML surfaces preserved source folders');
  assert(html.includes('wm-explorer-100'), 'rendered HTML includes an explorer mount');
  // R3 / AE2: no preview button or preview action surfaces in this feature.
  assert(!/data-action=["']preview["']/.test(html), 'no data-action=preview attribute');
  assert(!/preview[-_]btn/.test(html), 'no preview button class');
  assert(!/onclick=["'][^"']*preview/i.test(html), 'no onclick handler invoking preview');
}

console.log('renderWrongMatches() preserves ordinary candidate metadata presentation');
{
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  data.groups[0].entries[0].candidate = {
    artist: 'Scott Walker',
    album: 'Scott 3',
    year: 1969,
    mapping: [{
      track: { title: 'It\'s Raining Today' },
      item: { title: 'It\'s Raining Today', format: 'MP3' },
    }],
  };
  __test__.renderWrongMatches(data, dom.wrongMatches);
  assert(dom.wrongMatches.innerHTML.includes('(1969)'), 'ordinary candidate year remains visible');
  assert(dom.wrongMatches.innerHTML.includes(' MP3'), 'ordinary local format remains visible');
}

console.log('renderWrongMatches() escapes candidate metadata at the live HTML sink');
{
  const knownBad = '<span><script>alert(1)</script></span>';
  assert(!metadataHtmlIsEscaped(knownBad, '<script>alert(1)</script>'),
    'metadata escape checker rejects known-bad raw HTML');

  const atoms = ['<', '>', '&', '"', "'", '\\'];
  for (const left of atoms) {
    for (const right of atoms) {
      const year = `year${left}${right}tail`;
      const format = `format${left}${right}tail`;
      const data = wrongMatchesData();
      data.groups[0].entries[0].candidate = {
        artist: 'Artist',
        album: 'Album',
        year,
        mapping: [{
          track: { title: 'Track' },
          item: { title: 'Track', format },
        }],
      };
      installStorage();
      const dom = installDom();
      __test__.renderWrongMatches(data, dom.wrongMatches);
      assert(metadataHtmlIsEscaped(dom.wrongMatches.innerHTML, year),
        `candidate year escaped: ${JSON.stringify(year)}`);
      assert(metadataHtmlIsEscaped(dom.wrongMatches.innerHTML, format),
        `local format escaped: ${JSON.stringify(format)}`);
    }
  }
}

console.log('renderWrongMatchExplorer() collapses shared album tags and hides replaygain noise');
{
  const html = __test__.renderWrongMatchExplorer({
    status: 'ok',
    ordered_by: 'matched',
    folder_name: 'The Castiles Live (Vol. 1)',
    source_dirs: ['user1\\The Castiles Live (Vol. 1)'],
    audio_file_count: 2,
    other_file_count: 1,
    files: [{
      relative_path: '01-Purple Haze.flac',
      filename: '01-Purple Haze.flac',
      format: 'FLAC',
      bitrate_kbps: 780,
      duration_seconds: 275,
      size_bytes: 26000000,
      playable: true,
      stream_url: '/api/wrong-matches/audio?download_log_id=1&path=01-Purple%20Haze.flac',
      tags: {
        title: ['Purple Haze'],
        tracknumber: ['7'],
        artist: ['The Castiles'],
        albumartist: ['The Castiles'],
        album: ['The Castiles Live (Vol. 1)'],
        date: ['1967'],
        genre: ['Americana'],
        musicbrainz_albumid: ['20f1e791-34cd-4b47-8783-51492b90218a'],
        musicbrainz_artistid: ['4f13e8cb-11aa-4b1a-8bb5-0ad1437dbdee'],
        replaygain_album_gain: ['-4.19 dB'],
        replaygain_track_gain: ['-4.86 dB'],
      },
    }, {
      relative_path: '02-Get Outta My Life.flac',
      filename: '02-Get Outta My Life.flac',
      format: 'FLAC',
      bitrate_kbps: 803,
      duration_seconds: 64,
      size_bytes: 6200000,
      playable: true,
      stream_url: '/api/wrong-matches/audio?download_log_id=1&path=02-Get%20Outta%20My%20Life.flac',
      tags: {
        title: ['Get Outta My Life'],
        tracknumber: ['8'],
        artist: ['The Castiles'],
        albumartist: ['The Castiles'],
        album: ['The Castiles Live (Vol. 1)'],
        date: ['1967'],
        genre: ['Americana'],
        musicbrainz_albumid: ['20f1e791-34cd-4b47-8783-51492b90218a'],
        musicbrainz_artistid: ['4f13e8cb-11aa-4b1a-8bb5-0ad1437dbdee'],
        replaygain_album_gain: ['-4.19 dB'],
        replaygain_track_gain: ['-5.04 dB'],
      },
    }],
  });

  assert(html.includes('Downloaded as'), 'keeps the original user folder in the summary');
  assert(html.includes('albumartist'), 'renders shared album-level tags');
  assert(html.includes('2 tracks in surviving folder in matched order'), 'surfaces matched-order explorer label');
  assertEqual(countOccurrences(html, 'The Castiles Live (Vol. 1)'), 2, 'album name appears in the preserved source folder and shared tag summary');
  assert(html.includes('Purple Haze'), 'renders the first track title inline');
  assert(html.includes('Get Outta My Life'), 'renders the second track title inline');
  assert(html.includes('https://musicbrainz.org/release/20f1e791-34cd-4b47-8783-51492b90218a'), 'links musicbrainz_albumid to the release page');
  assert(html.includes('https://musicbrainz.org/artist/4f13e8cb-11aa-4b1a-8bb5-0ad1437dbdee'), 'links musicbrainz_artistid to the artist page');
  assertEqual(countOccurrences(html, '<audio'), 2, 'renders one player per track');
  assert(!html.includes('replaygain_album_gain'), 'hides replaygain album tags');
  assert(!html.includes('replaygain_track_gain'), 'hides replaygain track tags');
}

console.log('renderWrongMatchExplorer() distinguishes a containment refusal from a world failure, visibly and without a futile Retry (issue #1086 review)');
{
  // A CONTAINMENT refusal (symlink/socket/FIFO/device node) alongside a
  // readable track: `status: "ok"`, `files` non-empty. Re-fetching can
  // never change a containment decision, so no Retry, and the lead
  // sentence must not say "could not be read" — that phrasing is
  // reserved for a world failure a retry might clear.
  const containmentHtml = __test__.renderWrongMatchExplorer({
    status: 'ok',
    download_log_id: 900,
    partial: true,
    unreadable_entry_count: 1,
    unreadable_reason: '02 - dirlink.flac: this is a symlink, refused '
      + 'rather than followed out of the quarantine root (containment, '
      + 'not a world failure)',
    unreadable_is_containment: true,
    audio_file_count: 1,
    files: [{
      relative_path: '01 - Readable.flac', filename: '01 - Readable.flac',
      format: 'FLAC', playable: true, duration_seconds: 210,
      bitrate_kbps: 989, size_bytes: 4300000, tags: {},
    }],
  });
  assert(
    containmentHtml.includes('1 entry was refused (not read) as a containment decision'),
    'containment refusal leads with the containment sentence',
  );
  assert(!containmentHtml.includes('could not be read'), 'containment refusal never says "could not be read"');
  assert(!containmentHtml.includes('Retry'), 'containment refusal offers no Retry — re-fetching cannot change it');

  // The world-failure control: same shape, EACCES instead of a symlink.
  const worldFailureHtml = __test__.renderWrongMatchExplorer({
    status: 'ok',
    download_log_id: 901,
    partial: true,
    unreadable_entry_count: 1,
    unreadable_reason: '02 - locked.flac: could not be read, may be '
      + 'transient (EACCES)',
    unreadable_is_containment: false,
    audio_file_count: 1,
    files: [{
      relative_path: '01 - Readable.flac', filename: '01 - Readable.flac',
      format: 'FLAC', playable: true, duration_seconds: 210,
      bitrate_kbps: 989, size_bytes: 4300000, tags: {},
    }],
  });
  assert(
    worldFailureHtml.includes('1 entry could not be read'),
    'world-failure refusal leads with the "could not be read" sentence',
  );
  assert(!worldFailureHtml.includes('refused (not read)'), 'world-failure refusal never uses the containment wording');
  assert(worldFailureHtml.includes('Retry'), 'world-failure refusal offers Retry — the world might have cleared');
  assert(
    worldFailureHtml.includes('window.reloadWrongMatchExplorer(901)'),
    'Retry targets the exact entry id',
  );
}

console.log('renderWrongMatchExplorer() empty-state (status:"unavailable") also honours the containment discriminator — the #1086 review blocker 1 shape');
{
  // The exact scenario the review named: a folder holding ONLY a
  // symlink is `status: "unavailable"` (nothing readable), so this hits
  // the EMPTY branch (`files.length === 0`), not the per-entry-notice
  // branch a partial listing uses above. Before the fix, this branch's
  // own wording ignored `unreadableIsContainment` entirely.
  const containmentEmpty = __test__.renderWrongMatchExplorer({
    status: 'unavailable',
    download_log_id: 902,
    partial: true,
    unreadable_entry_count: 1,
    unreadable_reason: '01 - dirlink.flac: this is a symlink, refused '
      + 'rather than followed out of the quarantine root (containment, '
      + 'not a world failure)',
    unreadable_is_containment: true,
    audio_file_count: 0,
    files: [],
  });
  assert(!containmentEmpty.includes('could not be read'), 'containment-refused empty state never says "could not be read"');
  assert(containmentEmpty.includes('refused (not read)'), 'containment-refused empty state uses the containment wording');
  assert(!containmentEmpty.includes('Retry'), 'containment-refused empty state offers no Retry');
  assert(
    containmentEmpty.includes('NOT evidence that the folder is empty'),
    'still denies the folder is confidently empty',
  );

  const worldFailureEmpty = __test__.renderWrongMatchExplorer({
    status: 'unavailable',
    download_log_id: 903,
    partial: true,
    unreadable_entry_count: 3,
    unreadable_reason: '01.flac: could not be read, may be transient (EACCES)',
    unreadable_is_containment: false,
    audio_file_count: 0,
    files: [],
  });
  assert(worldFailureEmpty.includes('could not be read'), 'world-failure empty state still says "could not be read"');
  assert(!worldFailureEmpty.includes('refused (not read)'), 'world-failure empty state never uses the containment wording');
  assert(worldFailureEmpty.includes('Retry'), 'world-failure empty state still offers Retry');
  assert(
    worldFailureEmpty.includes('NOT evidence that the folder is empty'),
    'still denies the folder is confidently empty',
  );
}

console.log('maybeLoadWrongMatchExplorer() lazy-loads explorer tags and audio on <details> toggle');
{
  installStorage();
  const dom = installDom();
  let open = false;
  const detail = {
    classList: {
      toggle() {
        open = !open;
        return open;
      },
      contains() {
        return open;
      },
    },
  };
  const mount = { innerHTML: '' };
  const elements = new Map([
    ['wm-entry-100', detail],
    ['wm-explorer-100', mount],
  ]);
  globalThis.document.getElementById = (id) => {
    if (id === 'wrong-matches-content') return dom.wrongMatches;
    if (id === 'toast') return dom.toast;
    return elements.get(id) || null;
  };
  const calls = [];
  globalThis.fetch = async (url) => {
    calls.push(String(url));
    return {
      ok: true,
      json: async () => ({
        status: 'ok',
        ordered_by: 'matched',
        failed_path: '/mnt/virtio/Music/Incoming/post-validation/Scott Walker - Scott 3',
        folder_name: 'Scott Walker - Scott 3',
        source_dirs: ['user1\\Scott Walker - Scott 3'],
        audio_file_count: 1,
        other_file_count: 0,
        files: [{
          relative_path: '01 - It\'s Raining Today.mp3',
          filename: '01 - It\'s Raining Today.mp3',
          format: 'mp3',
          bitrate_kbps: 320,
          duration_seconds: 181,
          size_bytes: 1234567,
          playable: true,
          stream_url: '/api/wrong-matches/audio?download_log_id=100&path=01%20-%20It%27s%20Raining%20Today.mp3',
          tags: {
            title: ['It\'s Raining Today'],
            artist: ['Scott Walker'],
            album: ['Scott 3'],
            musicbrainz_albumid: ['20f1e791-34cd-4b47-8783-51492b90218a'],
            musicbrainz_trackid: ['d5b1a858-84be-4005-a2a0-29dfcf005851'],
            replaygain_track_gain: ['-4.1 dB'],
          },
        }],
      }),
    };
  };

  // Entry expand alone is cheap — no fetch.
  await __test__.toggleWrongMatchEntry('wm-entry-100', 100);
  assertDeepEqual(calls, [], 'entry expand does not auto-load the file explorer');

  // Closed <details> toggle does nothing.
  const closedDetails = { open: false };
  await __test__.maybeLoadWrongMatchExplorer(100, closedDetails);
  assertDeepEqual(calls, [], 'closed details element does not trigger a load');

  // Opened <details> toggle lazy-loads exactly once.
  const openDetails = { open: true };
  await __test__.maybeLoadWrongMatchExplorer(100, openDetails);
  assertDeepEqual(
    calls,
    ['/api/wrong-matches/explorer?download_log_id=100'],
    'opening the file-explorer dropdown loads the explorer exactly once',
  );
  assert(mount.innerHTML.includes('Downloaded as'), 'explorer shows the original user folder');
  assert(mount.innerHTML.includes('Scott 3'), 'explorer shows shared album tags once loaded');
  assert(mount.innerHTML.includes('It&#39;s Raining Today'), 'explorer shows extracted tags');
  assert(mount.innerHTML.includes('https://musicbrainz.org/release/20f1e791-34cd-4b47-8783-51492b90218a'), 'lazy-loaded explorer links the album MBID');
  assert(mount.innerHTML.includes('https://musicbrainz.org/recording/d5b1a858-84be-4005-a2a0-29dfcf005851'), 'lazy-loaded explorer links the recording MBID');
  assert(mount.innerHTML.includes('<audio'), 'explorer renders a browser audio player');
  assert(!mount.innerHTML.includes('replaygain_track_gain'), 'explorer hides replaygain noise');

  await __test__.maybeLoadWrongMatchExplorer(100, openDetails);
  await __test__.maybeLoadWrongMatchExplorer(100, openDetails);
  assertEqual(calls.length, 1, 'reopening the dropdown reuses the loaded explorer state');
}

console.log('maybeLoadWrongMatchExplorer() renders the honest copy for a refused listing');
{
  // Issue #1063. The server answers 200 with ``status: "unavailable"``
  // when it recorded refusals and could read nothing. This consumer used
  // to reject anything that was not ``ok``, so the operator saw "Failed
  // to load file explorer" and the authored copy below was unreachable.
  // The composed producer->consumer property lives in
  // tests/test_protected_path_truth_generated.py; this is the fast pin on
  // the consumer branch itself.
  installStorage();
  const dom = installDom();
  const mount = { innerHTML: '' };
  const elements = new Map([['wm-explorer-200', mount]]);
  globalThis.document.getElementById = (id) => {
    if (id === 'wrong-matches-content') return dom.wrongMatches;
    if (id === 'toast') return dom.toast;
    return elements.get(id) || null;
  };
  const calls = [];
  globalThis.fetch = async (url) => ({
    ok: (calls.push(String(url)), true),
    status: 200,
    json: async () => ({
      status: 'unavailable',
      download_log_id: 200,
      failed_path: '/mnt/virtio/cratedigger/processing/albums/wrong_matches/Guapo - Five Suns',
      folder_name: 'Guapo - Five Suns',
      source_dirs: [],
      audio_file_count: 0,
      other_file_count: 0,
      partial: true,
      truncated_reason: null,
      unreadable_entry_count: 3,
      unreadable_reason: '01.flac: cannot open 01.flac: Permission denied',
      scanned_file_count: 0,
      scanned_bytes: 0,
      ordered_by: 'folder',
      files: [],
    }),
  });

  await __test__.maybeLoadWrongMatchExplorer(200, { open: true });

  assert(!mount.innerHTML.includes('Failed to load file explorer'),
    'a renderable unavailable payload is not treated as a load failure');
  assert(mount.innerHTML.includes('3 entries could not be read'),
    'the refusal count reaches the operator');
  assert(mount.innerHTML.includes('nothing here is confirmed missing'),
    'the listing is labelled incomplete');
  assert(mount.innerHTML.includes('NOT evidence that the folder is empty'),
    'an unreadable folder is never presented as an empty one');
  assert(mount.innerHTML.includes('Permission denied'),
    'the refusal reason is shown');
  // An unreadable folder is a world the operator can REPAIR, so the panel
  // owes a Retry and must not cache the answer — otherwise the only way
  // to see a fixed permission is a full page reload (issue #1063).
  assert(mount.innerHTML.includes('Retry'),
    'an unavailable listing offers a retry');
  assert(mount.innerHTML.includes('window.reloadWrongMatchExplorer(200)'),
    'the retry re-reads THIS entry');

  await __test__.maybeLoadWrongMatchExplorer(200, { open: true });
  assertEqual(calls.length, 2,
    'reopening after an unavailable listing re-fetches instead of caching');
}

console.log('maybeLoadWrongMatchExplorer() surfaces a 503 refusal reason instead of swallowing it');
{
  installStorage();
  const dom = installDom();
  const mount = { innerHTML: '' };
  const elements = new Map([['wm-explorer-201', mount]]);
  globalThis.document.getElementById = (id) => {
    if (id === 'wrong-matches-content') return dom.wrongMatches;
    if (id === 'toast') return dom.toast;
    return elements.get(id) || null;
  };
  globalThis.fetch = async () => ({
    ok: false,
    status: 503,
    json: async () => ({
      error: 'Wrong-match files could not be read: /x/wrong_matches/Album '
        + '(quarantine path is contained but unavailable: cannot open '
        + '/x/wrong_matches/Album: Permission denied)',
    }),
  });

  await __test__.maybeLoadWrongMatchExplorer(201, { open: true });

  // Issue #1099: a whole-root 503 now gets its own status-honest lead
  // sentence instead of the old one-size-fits-all "Failed to load file
  // explorer" — the operator still needs to know this IS a failure and
  // that it's the retryable-world-failure kind, not a containment refusal.
  assert(mount.innerHTML.includes('temporarily unavailable'),
    'a real transport/authority failure still reads as a retryable failure');
  assert(mount.innerHTML.includes('could not be read'),
    'the server’s own reason reaches the operator');
  assert(mount.innerHTML.includes('Retry'),
    'the retry affordance survives');
}

console.log('maybeLoadWrongMatchExplorer() surfaces a whole-root 422 refusal, never as "not found" (issue #1099)');
{
  installStorage();
  const dom = installDom();
  const mount = { innerHTML: '' };
  const elements = new Map([['wm-explorer-205', mount]]);
  globalThis.document.getElementById = (id) => {
    if (id === 'wrong-matches-content') return dom.wrongMatches;
    if (id === 'toast') return dom.toast;
    return elements.get(id) || null;
  };
  globalThis.fetch = async () => ({
    ok: false,
    status: 422,
    json: async () => ({
      error: 'Wrong-match files refused: /x/wrong_matches/Album '
        + '(quarantine path is contained but unavailable: unsafe symlink: '
        + '/x/wrong_matches/Album)',
    }),
  });

  await __test__.maybeLoadWrongMatchExplorer(205, { open: true });

  assert(mount.innerHTML.toLowerCase().includes('refused'),
    'a whole-root containment refusal names itself as a refusal');
  assert(!mount.innerHTML.toLowerCase().includes('not found'),
    'a containment refusal must never read as a definitive absence');
  assert(mount.innerHTML.includes('Retry'),
    'the retry affordance is still offered by this catch (unconditional per #1099)');
}

console.log('maybeLoadWrongMatchExplorer() treats a PARTIAL listing as repairable too');
{
  // Issue #1063 / N1. The server only says `unavailable` when NOTHING was
  // readable, so a partial listing — some files read, some refused —
  // answers `ok`. It was therefore cached as loaded and rendered no
  // Retry: the operator saw the amber notice, fixed the permission, and
  // still needed a full page reload. Exactly the complaint C2 raised,
  // unfixed for the case that actually has partial evidence.
  installStorage();
  const dom = installDom();
  const mount = { innerHTML: '' };
  const elements = new Map([['wm-explorer-202', mount]]);
  globalThis.document.getElementById = (id) => {
    if (id === 'wrong-matches-content') return dom.wrongMatches;
    if (id === 'toast') return dom.toast;
    return elements.get(id) || null;
  };
  const calls = [];
  let refused = true;
  globalThis.fetch = async (url) => ({
    ok: (calls.push(String(url)), true),
    status: 200,
    json: async () => (refused ? {
      status: 'ok',
      download_log_id: 202,
      folder_name: 'Album',
      source_dirs: [],
      audio_file_count: 1,
      other_file_count: 0,
      partial: true,
      truncated_reason: null,
      unreadable_entry_count: 1,
      unreadable_reason: '02.flac: cannot open 02.flac: Permission denied',
      ordered_by: 'folder',
      files: [{
        relative_path: '01.flac', filename: '01.flac', format: 'FLAC',
        playable: false, size_bytes: 10, tags: {},
      }],
    } : {
      status: 'ok',
      download_log_id: 202,
      folder_name: 'Album',
      source_dirs: [],
      audio_file_count: 2,
      other_file_count: 0,
      partial: false,
      truncated_reason: null,
      unreadable_entry_count: 0,
      unreadable_reason: null,
      ordered_by: 'folder',
      files: [{
        relative_path: '01.flac', filename: '01.flac', format: 'FLAC',
        playable: false, size_bytes: 10, tags: {},
      }, {
        relative_path: '02.flac', filename: '02.flac', format: 'FLAC',
        playable: false, size_bytes: 10, tags: {},
      }],
    }),
  });

  await __test__.maybeLoadWrongMatchExplorer(202, { open: true });
  assert(mount.innerHTML.includes('1 entry could not be read'),
    'the partial listing names its refusal');
  assert(mount.innerHTML.includes('1 track in surviving folder'),
    'the partial listing still lists what it could read');
  assert(mount.innerHTML.includes('Retry'),
    'a PARTIAL listing offers a retry, not only an empty one');
  assert(mount.innerHTML.includes('window.reloadWrongMatchExplorer(202)'),
    'the partial listing retry re-reads THIS entry');

  // The operator repairs the world; the retry must show the repair.
  refused = false;
  await __test__.reloadWrongMatchExplorer(202);
  assertEqual(calls.length, 2, 'the retry re-reads the folder');
  assert(!mount.innerHTML.includes('could not be read'),
    'the repaired listing drops the refusal notice');
  assert(mount.innerHTML.includes('2 tracks in surviving folder'),
    'the repaired listing shows the previously-refused track');
  assert(!mount.innerHTML.includes('Retry'),
    'a complete listing needs no retry');

  // …and a complete listing IS cached again.
  await __test__.maybeLoadWrongMatchExplorer(202, { open: true });
  assertEqual(calls.length, 2,
    'a complete listing is cached, so reopening does not re-fetch');
}

console.log('explorerListingIsRepairable() keys off refusals, not status');
{
  assert(__test__.explorerListingIsRepairable(
    { status: 'ok', unreadable_entry_count: 1 }),
    'a partial ok listing is repairable');
  assert(__test__.explorerListingIsRepairable(
    { status: 'unavailable', unreadable_entry_count: 0 }),
    'an unavailable listing is repairable');
  assert(!__test__.explorerListingIsRepairable(
    { status: 'ok', unreadable_entry_count: 0 }),
    'a complete listing is not repairable');
  assert(!__test__.explorerListingIsRepairable(
    { status: 'ok', unreadable_entry_count: 0, truncated_reason: 'file_limit' }),
    'a truncated listing is not repairable — retrying hits the same limit');
}

console.log('renderWrongMatchExplorer() must still work: a complete listing claims nothing');
{
  const html = __test__.renderWrongMatchExplorer({
    status: 'ok',
    audio_file_count: 0,
    other_file_count: 0,
    partial: false,
    truncated_reason: null,
    unreadable_entry_count: 0,
    unreadable_reason: null,
    files: [],
  });
  assert(html.includes('No audio files found in this folder.'),
    'a readable empty folder still reads as empty');
  assert(!html.includes('could not be read'),
    'a complete listing never claims a refusal');
  assert(!html.includes('NOT evidence'),
    'a complete listing never denies emptiness');
}

console.log('cleanupSummaryToast() reports kept, skipped, and delete failures');
{
  const body = __test__.cleanupSummaryToast({
    deleted: 2,
    kept_would_import: 1,
    kept_uncertain: 3,
    skipped_candidate_evidence_missing: 1,
    skipped_candidate_evidence_stale: 1,
    skipped_current_evidence_missing: 0,
    skipped_current_evidence_stale: 0,
    skipped_active_job: 1,
    skipped_invalid_row: 0,
    skipped_missing_path: 1,
    skipped_operational: 0,
    delete_failed: 1,
  });
  assertEqual(body, 'Deleted 2 candidates, kept 4, skipped 5', 'summarizes cleanup outcomes');
}

console.log('cleanupSummaryToast() includes verified-lossless deletes and current-evidence-failed skips');
{
  const body = __test__.cleanupSummaryToast({
    deleted: 1,
    deleted_verified_lossless_parent: 4,
    kept_would_import: 0,
    kept_uncertain: 0,
    skipped_current_evidence_failed: 2,
    skipped_active_job: 1,
    delete_failed: 0,
  });
  assertEqual(body, 'Deleted 5 candidates, kept 0, skipped 3', 'includes new outcome categories in totals');
}

console.log('renderLatestImport() distinguishes absent / in-library / verified-lossless / present states');
{
  // 1. No latest import, album not in library — neutral copy.
  let html = __test__.renderLatestImport(null, { in_library: false, verified_lossless: false });
  assert(html.includes('No previous import on disk.'), 'absent: renders neutral "no previous import" copy');
  assert(!html.includes('Album already in library'), 'absent: does not claim album in library');
  assert(!html.includes('Verified-lossless copy in library'), 'absent: no verified-lossless copy');
  assert(!html.includes('No successful import on disk'), 'absent: no longer uses old "No successful import on disk" copy');

  // 2. No latest import, album in library, not verified lossless — distinguishes
  //    "no cratedigger history" from "Beets already has this MBID".
  html = __test__.renderLatestImport(null, { in_library: true, verified_lossless: false });
  assert(html.includes('Album already in library'), 'in_library: surfaces the in-library copy');
  assert(html.includes('must beat current quality'), 'in_library: explains upgrade gate semantics');
  assert(!html.includes('No previous import'), 'in_library: does not claim no prior import');
  assert(!html.includes('No successful import'), 'in_library: no longer uses old "No successful import" copy');
  assert(!html.includes('Verified-lossless copy in library'), 'in_library: not the verified-lossless branch');

  // 3. No latest import, album in library AND verified lossless — strongest copy.
  html = __test__.renderLatestImport(null, { in_library: true, verified_lossless: true });
  assert(html.includes('Verified-lossless copy in library'), 'verified-lossless: surfaces the verified-lossless copy');
  assert(html.includes('cleared on the next cleanup sweep'), 'verified-lossless: explains the cleanup behavior');
  assert(!html.includes('Album already in library'), 'verified-lossless: does not fall back to plain in-library copy');
  assert(!html.includes('No previous import'), 'verified-lossless: does not fall back to absent copy');

  // 4. Latest import present — render existing summary regardless of in_library.
  html = __test__.renderLatestImport(
    {
      outcome: 'imported',
      created_at: '2026-05-17T00:00:00Z',
      actual_filetype: 'flac',
      actual_min_bitrate: 950,
    },
    { in_library: true, verified_lossless: false },
  );
  assert(html.includes('Last import: imported'), 'present: renders existing latest-import summary');
  assert(html.includes('FLAC 950k'), 'present: renders filetype and bitrate floor');
  assert(!html.includes('Album already in library'), 'present: in_library flag does not override the summary');
  assert(!html.includes('No previous import'), 'present: does not render absent copy');
}

// --- issue #829 Phase 5 PR4/N3: audit-only flags on both WM surfaces ---

console.log('renderQualityBadges() withholds an audit-only HAVE accusation');
{
  const group = {
    in_library: true,
    quality_label: 'AAC 256k',
    format: 'AAC',
    current_spectral_grade: 'likely_transcode',
    current_spectral_bitrate: 128,
  };
  let html = __test__.renderQualityBadges({
    ...group,
    current_spectral_accusation_admissible: false,
    current_spectral_accusation_withheld: 'audit_only_codec',
  });
  assert(html.includes('likely transcode'), 'the measured grade stays visible');
  assert(html.includes('audit-only'), 'the withheld suffix is stated');
  assert(html.includes('native encoder behaviour'), 'the hover explains why');
  assert(!html.includes('badge-rank-poor'),
    'the accusing red badge is withheld');

  html = __test__.renderQualityBadges({
    ...group,
    current_spectral_accusation_admissible: true,
    current_spectral_accusation_withheld: null,
  });
  assert(html.includes('badge-rank-poor'),
    'an admissible grade still gets the accusing badge');
  assert(!html.includes('audit-only'), 'nothing is withheld on a real finding');

  html = __test__.renderQualityBadges(group);
  assert(html.includes('badge-rank-poor'),
    'absent flags keep the historical accusing badge (fail-accusing)');

  html = __test__.renderQualityBadges({
    ...group,
    current_spectral_grade: 'suspect',
    current_spectral_accusation_admissible: false,
    current_spectral_accusation_withheld: 'codec_unresolved',
  });
  assert(html.includes('codec unresolved'), 'the unresolved world is named');
  assert(!html.includes('native encoder behaviour'),
    'an unresolved codec is never described as native encoder rolloff');
  assert(!html.includes('audit-only'),
    'the two withholding worlds are never conflated');
}

console.log('entrySpectralCell() withholds an audit-only candidate accusation');
{
  const entry = { spectral_grade: 'likely_transcode', spectral_bitrate: 128 };
  const text = __test__.formatEntryEvidence(entry).spectral;

  let html = __test__.entrySpectralCell({
    ...entry,
    spectral_accusation_admissible: false,
    spectral_accusation_withheld: 'audit_only_codec',
  }, text);
  assert(html.includes('likely transcode'), 'the measured grade stays visible');
  assert(html.includes('audit-only'), 'the withheld suffix is stated');
  assert(html.includes('quality-tone-unknown'), 'the neutral tone is used');
  assert(!html.includes('quality-tone-poor'), 'the accusing red is withheld');

  html = __test__.entrySpectralCell({
    ...entry,
    spectral_accusation_admissible: true,
    spectral_accusation_withheld: null,
  }, text);
  assert(html.includes('quality-tone-poor'),
    'an admissible candidate grade still accuses');
  assert(!html.includes('audit-only'), 'nothing is withheld on a real finding');

  html = __test__.entrySpectralCell(entry, text);
  assert(html.includes('quality-tone-poor'),
    'a pre-evidence candidate keeps the accusing chip (fail-accusing)');

  html = __test__.entrySpectralCell({
    spectral_grade: 'suspect',
    spectral_bitrate: 192,
    spectral_accusation_admissible: false,
    spectral_accusation_withheld: 'codec_unresolved',
  }, 'suspect · 192 kbps');
  assert(html.includes('codec unresolved'), 'the unresolved world is named');
  assert(!html.includes('native encoder behaviour'),
    'an unresolved codec is never described as native encoder rolloff');

  // A candidate with no grade at all keeps the pre-existing neutral cell.
  html = __test__.entrySpectralCell({}, '—');
  assert(html.includes('quality-tone-unknown'), 'a gradeless candidate is neutral');
  assert(!html.includes('audit-only'), 'a gradeless candidate withholds nothing');
}

console.log('an unobservable source is surfaced, never silently dropped');
{
  // Issue #1063: the server sends `path_unavailable` when its probe was
  // REFUSED. The row must stay visible, say so, disable both destructive
  // actions, and never be counted as converge-green.
  const unavailable = {
    download_log_id: 77,
    soulseek_username: 'peer',
    distance: 0.05,
    path_unavailable: true,
    path_unavailable_reason: 'path_unavailable[EACCES]: /x: Permission denied',
  };
  assert(__test__.entryPathUnavailable(unavailable),
    'the payload flag is the single source of the unavailable state');
  assert(!__test__.entryPathUnavailable({ download_log_id: 78, distance: 0.05 }),
    'an ordinary entry is not unavailable');
  assert(!__test__.isConvergeGreen(unavailable, 180),
    'an unobservable source is never converge-green, whatever its distance');
  assert(__test__.isConvergeGreen({ distance: 0.05 }, 180),
    'must still work: an observable close match is still green');

  const html = __test__.renderEntry(unavailable, 180, 42);
  assert(html.includes('source unavailable'), 'the card is badged unavailable');
  assert(html.includes('NOT been confirmed missing'),
    'the copy refuses to claim the folder is gone');
  assert(html.includes('EACCES'), 'the refusal reason reaches the operator');
  assertEqual(countOccurrences(html, 'disabled'), 2,
    'both Force Import and Delete are disabled');

  const ordinary = __test__.renderEntry(
    { download_log_id: 78, soulseek_username: 'peer', distance: 0.05 }, 180, 42);
  assert(!ordinary.includes('source unavailable'),
    'must still work: an ordinary entry carries no unavailable badge');
  assertEqual(countOccurrences(ordinary, 'disabled'), 0,
    'must still work: an ordinary entry keeps both actions enabled');
}

console.log('a partial group delete asks for attention and re-renders');
{
  // Found by the disposable Rule D fixture (issue #1063): one folder
  // deleted, one unavailable. The old code kept a green "all good" toast
  // and surgically removed the deleted row, leaving the group strip
  // advertising "Delete All (2)" and "1 green" over the ONE unavailable
  // candidate that survived.
  const dom = installDom();
  const calls = [];
  global.confirm = () => true;
  global.fetch = async (url, options) => {
    calls.push({ url, options });
    if (url === '/api/wrong-matches/delete-group') {
      return {
        ok: false,
        status: 503,
        json: async () => ({
          status: 'partial',
          processed: 2,
          deleted: 1,
          cleared_missing: 0,
          // Issue #1086 item 3: the unavailable candidate is its own
          // bucket now, not double-counted into skipped AND errors.
          unavailable: 1,
          skipped: 0,
          errors: 0,
          remaining: 1,
          results: [
            { download_log_id: 1, success: true },
            { download_log_id: 2, success: false,
              outcome: 'skipped_path_unavailable' },
          ],
        }),
      };
    }
    if (url === '/api/wrong-matches') {
      return { ok: true, json: async () => ({ groups: [] }) };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };
  const btn = { disabled: false, textContent: 'Delete All (2)', style: {} };
  await __test__.deleteWrongMatchGroup(42, btn);

  assert(dom.toast.textContent.includes('Deleted 1 folder'),
    'the toast still credits the folder that really went');
  assert(dom.toast.textContent.includes('unavailable 1'),
    'the toast surfaces the unavailable bucket by name');
  assert(!dom.toast.textContent.includes('skipped'),
    'an unavailable candidate is not ALSO reported as skipped');
  assert(!dom.toast.textContent.includes('errors'),
    'an unavailable candidate is not ALSO reported as an error — that was '
    + 'the double count issue #1086 item 3 fixes');
  assert(dom.toast.textContent.includes('1 left'),
    'the toast says work remains');
  assert(dom.toast.className.includes('error'),
    'an incomplete group delete asks for attention, not a green all-clear');
  assert(calls.some(call => call.url === '/api/wrong-matches'),
    'a partial outcome re-renders from the server instead of leaving a '
    + 'stale group strip');
}

console.log('Delete All reflects actionable candidates, never a dead end (issue #1086 item 2)');
{
  installStorage();
  const dom = installDom();

  // A fully available group keeps today's plain label and stays enabled —
  // the common case must not regress just because unavailability exists.
  __test__.renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  assert(dom.wrongMatches.innerHTML.includes('Delete All (3)'),
    'a fully available group keeps the plain label');
  assert(!/id="wm-delete-group-btn-42"[^>]*disabled/.test(dom.wrongMatches.innerHTML),
    'a fully available group stays enabled');

  // A partially unavailable group relabels with the actionable count and
  // stays enabled — a partial group is still the right action to take.
  const partial = JSON.parse(JSON.stringify(wrongMatchesData()));
  partial.groups[0].entries[0].path_unavailable = true;
  __test__.renderWrongMatches(partial, dom.wrongMatches);
  assert(dom.wrongMatches.innerHTML.includes('Delete All (2 of 3)'),
    'a partially unavailable group shows the actionable count');
  assert(!/id="wm-delete-group-btn-42"[^>]*disabled/.test(dom.wrongMatches.innerHTML),
    'a partially unavailable group stays enabled');

  // A group with ZERO actionable candidates is a dead end today: the
  // server truthfully refuses (503, nothing destroyed) and the operator
  // gets an error toast instead of a control that told them up front.
  const dead = JSON.parse(JSON.stringify(wrongMatchesData()));
  for (const entry of dead.groups[0].entries) entry.path_unavailable = true;
  __test__.renderWrongMatches(dead, dom.wrongMatches);
  assert(dom.wrongMatches.innerHTML.includes('Delete All (0 of 3)'),
    'a fully unavailable group names zero actionable candidates');
  assert(/id="wm-delete-group-btn-42"[^>]*disabled/.test(dom.wrongMatches.innerHTML),
    'a fully unavailable group disables Delete All instead of a dead-end 503');

  assertEqual(__test__.deleteAllButtonLabel(3, 3), 'Delete All (3)',
    'a fully actionable group keeps the plain label');
  assertEqual(__test__.deleteAllButtonLabel(2, 3), 'Delete All (2 of 3)',
    'a partially actionable group shows X of N');
  assertEqual(__test__.deleteAllButtonLabel(0, 2), 'Delete All (0 of 2)',
    'zero actionable candidates still names the total');
  assertEqual(
    __test__.actionableDeleteEntries({ entries: [
      { download_log_id: 1 },
      { download_log_id: 2, path_unavailable: true },
    ] }).length,
    1,
    'actionableDeleteEntries excludes unavailable candidates',
  );
}

console.log('deleteWrongMatchGroup() restores the actionable-aware label on every failure path (issue #1086 review blocker 2)');
{
  // renderConvergeControls computes the label from FRESH render data; the
  // three restore paths below instead fall back to a value captured once
  // at the top of deleteWrongMatchGroup, before the request. Each one
  // hard-coded a bare `Delete All (${count})` — reverting any of them
  // loses the actionable-of-total distinction the button exists to show
  // and silently re-enables the item-2 dead end.
  function partiallyUnavailableGroup() {
    const data = JSON.parse(JSON.stringify(wrongMatchesData()));
    data.groups[0].entries[0].path_unavailable = true; // 2 of 3 actionable
    return data;
  }

  installStorage();
  global.confirm = () => true;

  // Path A: a non-2xx but "summarised" response that is neither `status:
  // 'ok'` nor `remaining: 0` takes the partial-outcome restore branch.
  {
    const dom = installDom();
    __test__.renderWrongMatches(partiallyUnavailableGroup(), dom.wrongMatches);
    global.fetch = async (url) => {
      if (url === '/api/wrong-matches/delete-group') {
        return {
          ok: false, status: 503,
          json: async () => ({ status: 'partial', deleted: 1, remaining: 1 }),
        };
      }
      return { ok: true, json: async () => ({ groups: [] }) };
    };
    const btn = { disabled: false, textContent: 'Delete All (2 of 3)', style: {} };
    await __test__.deleteWrongMatchGroup(42, btn);
    assertEqual(btn.textContent, 'Delete All (2 of 3)',
      'the partial-outcome restore path keeps the actionable-aware label');
    assertEqual(btn.disabled, false,
      'the partial-outcome restore path leaves a partially actionable group enabled');
  }

  // Path B: a response with no numeric `deleted` (not "summarised") takes
  // the plain-error restore branch.
  {
    const dom = installDom();
    __test__.renderWrongMatches(partiallyUnavailableGroup(), dom.wrongMatches);
    global.fetch = async () => ({
      ok: false, json: async () => ({ error: 'cleanup_lock_unavailable' }),
    });
    const btn = { disabled: false, textContent: 'Delete All (2 of 3)', style: {} };
    await __test__.deleteWrongMatchGroup(42, btn);
    assertEqual(btn.textContent, 'Delete All (2 of 3)',
      'the unsummarised-error restore path keeps the actionable-aware label');
    assertEqual(btn.disabled, false,
      'the unsummarised-error restore path leaves a partially actionable group enabled');
  }

  // Path C: the fetch itself throws — the exception restore branch.
  {
    const dom = installDom();
    __test__.renderWrongMatches(partiallyUnavailableGroup(), dom.wrongMatches);
    global.fetch = async () => { throw new Error('network down'); };
    const btn = { disabled: false, textContent: 'Delete All (2 of 3)', style: {} };
    await __test__.deleteWrongMatchGroup(42, btn);
    assertEqual(btn.textContent, 'Delete All (2 of 3)',
      'the fetch-exception restore path keeps the actionable-aware label');
    assertEqual(btn.disabled, false,
      'the fetch-exception restore path leaves a partially actionable group enabled');
  }

  // Must still work: a group with ZERO actionable candidates stays
  // disabled after a failed request too, not just on the first render.
  {
    const dom = installDom();
    const dead = JSON.parse(JSON.stringify(wrongMatchesData()));
    for (const entry of dead.groups[0].entries) entry.path_unavailable = true;
    __test__.renderWrongMatches(dead, dom.wrongMatches);
    global.fetch = async () => { throw new Error('network down'); };
    const btn = { disabled: false, textContent: 'Delete All (0 of 3)', style: {} };
    await __test__.deleteWrongMatchGroup(42, btn);
    assertEqual(btn.disabled, true,
      'a fully unavailable group stays disabled after a failed request too');
  }
}

console.log('removeWrongMatchEntry() keeps the group Delete All button actionable-aware (issue #1086 review blocker 2)');
{
  // Unlike the restore paths above, this update runs on a SUCCESSFUL
  // single-candidate delete: the group button must still reflect the
  // remaining actionable count, not a bare `Delete All (${remaining})`.
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  data.groups[0].entries[1].path_unavailable = true; // logId 101 stays unavailable
  __test__.renderWrongMatches(data, dom.wrongMatches);

  const groupBtn = fakeElement({ textContent: 'Delete All (2 of 3)' });
  dom.elements.set('wm-delete-group-btn-42', groupBtn);
  dom.elements.set('wm-entry-card-100', fakeElement());

  // Remove the AVAILABLE candidate (id 100): 2 candidates remain, only one
  // (id 102) actionable.
  __test__.removeWrongMatchEntry(100);

  assertEqual(groupBtn.textContent, 'Delete All (1 of 2)',
    'removing an available candidate updates the group button to the new '
    + 'actionable-of-total count, not a bare N');
  assertEqual(groupBtn.disabled, false,
    'one actionable candidate remains, so the group button stays enabled');

  // Must still work: removing the LAST actionable candidate disables it.
  dom.elements.set('wm-entry-card-102', fakeElement());
  __test__.removeWrongMatchEntry(102);

  assertEqual(groupBtn.textContent, 'Delete All (0 of 1)',
    'removing the last actionable candidate updates the count to zero');
  assertEqual(groupBtn.disabled, true,
    'zero actionable candidates remain, so the group button disables — '
    + 'the item-2 dead end, reached through the per-entry delete path');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
