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

function countOccurrences(text, needle) {
  return (String(text).match(new RegExp(needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g')) || []).length;
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
  assert(dom.toast.textContent.includes('Deleted 3 candidates'), 'toasts group delete result');
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
  await __test__.bulkTriageWrongMatches(btn);
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
  assert(html.includes('265'), 'rendered HTML carries the lossless-source V0 average');
  assert(html.includes('Downloaded as'), 'rendered HTML surfaces preserved source folders');
  assert(html.includes('wm-explorer-100'), 'rendered HTML includes an explorer mount');
  // R3 / AE2: no preview button or preview action surfaces in this feature.
  assert(!/data-action=["']preview["']/.test(html), 'no data-action=preview attribute');
  assert(!/preview[-_]btn/.test(html), 'no preview button class');
  assert(!/onclick=["'][^"']*preview/i.test(html), 'no onclick handler invoking preview');
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

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
