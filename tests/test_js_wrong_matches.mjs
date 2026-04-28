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
      quality_rank: null,
      status: 'wanted',
      entries: [
        { download_log_id: 100, soulseek_username: 'u1', distance: 0.167, scenario: 'high_distance', local_items: [{ path: '01.mp3', format: 'MP3' }] },
        { download_log_id: 101, soulseek_username: 'u2', distance: 0.180, scenario: 'high_distance', local_items: [{ path: '02.mp3', format: 'MP3' }] },
        { download_log_id: 102, soulseek_username: 'u3', distance: 0.226, scenario: 'high_distance', local_items: [{ path: '03.mp3', format: 'MP3' }] },
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

console.log('transparent non-FLAC bulk cleanup targets only transparent on-disk non-FLAC groups');
{
  const data = wrongMatchesData();
  data.groups[0].quality_rank = 'transparent';
  const flacGroup = JSON.parse(JSON.stringify(data.groups[0]));
  flacGroup.request_id = 43;
  flacGroup.entries[0].local_items = [{ path: '01.flac', format: 'FLAC' }];
  const poorGroup = JSON.parse(JSON.stringify(data.groups[0]));
  poorGroup.request_id = 44;
  poorGroup.quality_rank = 'poor';

  const targets = __test__.transparentNonFlacGroups([data.groups[0], flacGroup, poorGroup]);
  assertDeepEqual(targets.map(g => g.request_id), [42], 'only transparent groups with non-FLAC downloads are targeted');
}

console.log('lossless-Opus bulk cleanup targets only verified-lossless Opus groups');
{
  const data = wrongMatchesData();
  data.groups[0].verified_lossless = true;
  data.groups[0].format = 'Opus';
  const mp3Group = JSON.parse(JSON.stringify(data.groups[0]));
  mp3Group.request_id = 43;
  mp3Group.format = 'MP3';
  const unverifiedGroup = JSON.parse(JSON.stringify(data.groups[0]));
  unverifiedGroup.request_id = 44;
  unverifiedGroup.verified_lossless = false;

  const targets = __test__.losslessOpusGroups([data.groups[0], mp3Group, unverifiedGroup]);
  assertDeepEqual(targets.map(g => g.request_id), [42], 'only verified-lossless Opus groups are targeted');
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

console.log('deleteTransparentNonFlacWrongMatches() posts one bulk delete and removes rows in place');
{
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  data.groups[0].quality_rank = 'transparent';
  __test__.renderWrongMatches(data, dom.wrongMatches);
  assert(dom.wrongMatches.innerHTML.includes('Delete transparent non-FLAC (3)'), 'renders top-level cleanup button');
  const calls = [];
  globalThis.confirm = () => true;
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url, options });
    if (url === '/api/wrong-matches/delete-transparent-non-flac') {
      return {
        ok: true,
        json: async () => ({
          status: 'ok',
          deleted: 3,
          groups_deleted: 1,
          deleted_request_ids: [42],
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
  const btn = { disabled: false, textContent: 'Delete transparent non-FLAC (3)', style: {} };
  await __test__.deleteTransparentNonFlacWrongMatches(btn);
  assertEqual(calls[0].url, '/api/wrong-matches/delete-transparent-non-flac', 'posts to transparent cleanup endpoint');
  assert(!calls.some(call => call.url === '/api/wrong-matches'), 'does not refetch the whole pane after bulk cleanup');
  assert(dom.toast.textContent.includes('Deleted 3 candidates'), 'toasts bulk cleanup result');
  assert(dom.wrongMatches.innerHTML.includes('No wrong matches'), 'removes cleaned groups locally');
}

console.log('deleteLosslessOpusWrongMatches() posts one bulk delete and removes rows in place');
{
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  data.groups[0].verified_lossless = true;
  data.groups[0].format = 'Opus';
  __test__.renderWrongMatches(data, dom.wrongMatches);
  assert(dom.wrongMatches.innerHTML.includes('Delete lossless-Opus (3)'), 'renders top-level lossless-Opus cleanup button');
  const calls = [];
  globalThis.confirm = () => true;
  globalThis.fetch = async (url, options = {}) => {
    calls.push({ url, options });
    if (url === '/api/wrong-matches/delete-lossless-opus') {
      return {
        ok: true,
        json: async () => ({
          status: 'ok',
          deleted: 3,
          groups_deleted: 1,
          deleted_request_ids: [42],
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
  const btn = { disabled: false, textContent: 'Delete lossless-Opus (3)', style: {} };
  await __test__.deleteLosslessOpusWrongMatches(btn);
  assertEqual(calls[0].url, '/api/wrong-matches/delete-lossless-opus', 'posts to lossless-Opus cleanup endpoint');
  assert(!calls.some(call => call.url === '/api/wrong-matches'), 'does not refetch the whole pane after lossless-Opus cleanup');
  assert(dom.toast.textContent.includes('Deleted 3 candidates'), 'toasts lossless-Opus bulk cleanup result');
  assert(dom.wrongMatches.innerHTML.includes('No wrong matches'), 'removes cleaned lossless-Opus groups locally');
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

  // R2 — non-lossless-source V0 evidence is treated as missing for display.
  cells = __test__.formatEntryEvidence({
    spectral_grade: 'suspect',
    spectral_bitrate: 320,
    v0_probe_kind: 'native_lossy_research_v0',
    v0_probe_avg_bitrate: 240,
  });
  assert(cells.spectral.includes('suspect'), 'spectral cell still renders for suspect grade');
  assertEqual(cells.v0, '—',
    'non-lossless-source V0 probe is hidden — R2 scopes V0 display to lossless-source only');

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
  // R3 / AE2: no preview button or preview action surfaces in this feature.
  assert(!/data-action=["']preview["']/.test(html), 'no data-action=preview attribute');
  assert(!/preview[-_]btn/.test(html), 'no preview button class');
  assert(!/onclick=["'][^"']*preview/i.test(html), 'no onclick handler invoking preview');
}

console.log('formatLosslessOpusToastBody() reports spectral-suspect skips');
{
  // Covers R6 surfacing: server reports both deletes and the new
  // groups_skipped_spectral_suspect counter. The toast must break out
  // the spectral-suspect count separately from the existing summary.
  let body = __test__.formatLosslessOpusToastBody({
    deleted: 1,
    groups_deleted: 1,
    eligible_groups: 2,
    groups_skipped_spectral_suspect: 1,
    skipped: [
      { reason: 'spectral_suspect', request_id: 71 },
      { reason: 'spectral_suspect', request_id: 71 },
    ],
  });
  assert(body.toLowerCase().includes('1') && body.toLowerCase().includes('release'),
    'reports the deleted-release count');
  assert(body.toLowerCase().includes('skipped') && body.toLowerCase().includes('spectral'),
    'reports spectral-suspect skips by name');

  // All-blocked worst case: 0 deleted, every eligible group blocked.
  body = __test__.formatLosslessOpusToastBody({
    deleted: 0,
    groups_deleted: 0,
    eligible_groups: 3,
    groups_skipped_spectral_suspect: 3,
    skipped: [
      { reason: 'spectral_suspect', request_id: 1 },
      { reason: 'spectral_suspect', request_id: 2 },
      { reason: 'spectral_suspect', request_id: 3 },
      { reason: 'spectral_suspect', request_id: 3 },
    ],
  });
  assert(body.toLowerCase().includes('0'), 'reports the zero-delete case');
  assert(body.toLowerCase().includes('3') && body.toLowerCase().includes('skipped'),
    'reports the 3 skipped groups even when nothing was deleted');
  assert(body.length > 0, 'toast is never blank when all groups are blocked');

  // Forwards-compat fallback: server omits the new sibling field; the
  // count is derivable from skipped[] by deduplicating request_id.
  body = __test__.formatLosslessOpusToastBody({
    deleted: 0,
    groups_deleted: 0,
    eligible_groups: 1,
    skipped: [
      { reason: 'spectral_suspect', request_id: 5 },
      { reason: 'spectral_suspect', request_id: 5 },
    ],
  });
  assert(body.toLowerCase().includes('1') && body.toLowerCase().includes('skipped'),
    'fallback derives one skipped group from deduplicated request_id');

  // Regression: empty skipped[] + zero counter → today's behaviour, no
  // spectral mention.
  body = __test__.formatLosslessOpusToastBody({
    deleted: 3,
    groups_deleted: 1,
    eligible_groups: 1,
    groups_skipped_spectral_suspect: 0,
    skipped: [],
  });
  assert(!body.toLowerCase().includes('spectral'),
    'no spectral mention when nothing was skipped');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
