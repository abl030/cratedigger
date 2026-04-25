/**
 * Unit tests for web/js/wrong-matches.js polling behavior.
 * Run with: node tests/test_js_wrong_matches.mjs
 */

import { __test__ } from '../web/js/wrong-matches.js';

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

function installDom() {
  const wrongMatches = { innerHTML: '' };
  const toast = {
    textContent: '',
    className: '',
    style: { display: 'none' },
  };
  globalThis.document = {
    getElementById(id) {
      if (id === 'wrong-matches-content') return wrongMatches;
      if (id === 'toast') return toast;
      return null;
    },
  };
  globalThis.setTimeout = (fn) => {
    fn();
    return 0;
  };
  return { wrongMatches, toast };
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
      status: 'wanted',
      entries: [
        { download_log_id: 100, soulseek_username: 'u1', distance: 0.167, scenario: 'high_distance' },
        { download_log_id: 101, soulseek_username: 'u2', distance: 0.180, scenario: 'high_distance' },
        { download_log_id: 102, soulseek_username: 'u3', distance: 0.226, scenario: 'high_distance' },
      ],
    }],
  };
}

async function runPoll(job) {
  const calls = [];
  const dom = installDom();
  globalThis.fetch = async (url) => {
    calls.push(url);
    if (String(url).startsWith('/api/import-jobs/')) {
      return {
        ok: true,
        json: async () => ({ job }),
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
  const btn = { textContent: '', style: {} };
  await __test__.pollImportJob(17, btn);
  return { calls, dom, btn };
}

console.log('_pollImportJob() refreshes after completed jobs');
{
  const { calls, dom, btn } = await runPoll({
    status: 'completed',
    message: 'Import completed',
  });
  assertEqual(btn.textContent, 'Imported', 'button shows imported');
  assert(calls.includes('/api/wrong-matches'), 'refreshes wrong matches after completion');
  assert(dom.wrongMatches.innerHTML.includes('No wrong matches'), 'renders refreshed empty state');
  assertEqual(dom.toast.className, 'toast', 'completion toast is not an error');
}

console.log('_pollImportJob() refreshes after failed jobs');
{
  const { calls, dom, btn } = await runPoll({
    status: 'failed',
    message: 'Pre-import gate rejected',
  });
  assertEqual(btn.textContent, 'Failed', 'button shows failed');
  assert(calls.includes('/api/wrong-matches'), 'refreshes wrong matches after failure');
  assert(dom.wrongMatches.innerHTML.includes('No wrong matches'), 'renders refreshed empty state');
  assertEqual(dom.toast.className, 'toast error', 'failure toast is an error');
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
    __test__.convergeRequestBody('42', '180', false),
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
  assert(dom.wrongMatches.innerHTML.includes('Delete All (3)'), 'keeps delete-all action');

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
  __test__.setWrongMatchConvergeCleanup(false);
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

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
