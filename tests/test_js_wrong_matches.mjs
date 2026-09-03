/**
 * Unit tests for web/js/wrong-matches.js polling behavior.
 * Run with: node tests/test_js_wrong_matches.mjs
 */

import {
  actionableDeleteEntries,
  bulkTriageWrongMatches,
  claimTriageFollow,
  cleanupSummaryToast,
  convergeRequestBody,
  convergeWrongMatches,
  deleteAllButtonLabel,
  deleteWrongMatch,
  deleteWrongMatchGroup,
  entryPathUnavailable,
  entrySpectralCell,
  explorerListingIsRepairable,
  forceImportWrongMatch,
  formatEntryEvidence,
  invalidateWrongMatches,
  isConvergeGreen,
  loadWrongMatches,
  maybeLoadWrongMatchExplorer,
  normalizeThreshold,
  pollImportJob,
  refreshWrongMatches,
  releaseTriageFollow,
  reloadWrongMatchExplorer,
  removeWrongMatchEntry,
  renderEntry,
  renderLatestImport,
  renderQualityBadges,
  renderWrongMatchExplorer,
  renderWrongMatches,
  retryTriageStatusOnce,
  setWrongMatchConvergeThreshold,
  stopWrongMatchTriage,
  toggleWrongMatchEntry,
  triageButtonPresentation,
} from '../web/js/wrong-matches.js';
import { closeSearchPlanDetail } from '../web/js/search_plan.js';
import { state } from '../web/js/state.js';
import { esc } from '../web/js/util.js';

import { domStub, element, stubGlobals, suite } from './js_harness.mjs';

const t = suite(import.meta.url);

function countOccurrences(text, needle) {
  return (String(text).match(new RegExp(needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g')) || []).length;
}

function metadataHtmlIsEscaped(html, value) {
  return !html.includes(value) && html.includes(esc(value));
}

function installStorage() {
  const values = new Map();
  stubGlobals({ localStorage: {
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
  } });
  return values;
}

/**
 * Drain pending microtasks. Used after a call that kicks off a
 * fire-and-forget background chain (the render-time triage attach —
 * issue #1106) so a test can let it settle to a terminal state before the
 * section ends.
 *
 * An un-drained chain used to keep polling into a LATER section's mock.
 * Since #1346 the harness hands `fetch` back at the section boundary, so it
 * polls node's real `fetch` instead — which throws `Failed to parse URL` on
 * these relative paths before any socket opens, and production's own
 * `catch` swallows it. Louder than answering the next test's mock, but
 * still silent, so draining is still the answer rather than a formality.
 * @param {number} [times]
 */
/**
 * Answer the render-time triage probe, and nothing else.
 *
 * `renderWrongMatches` starts a fire-and-forget triage attach (#1106), so a
 * section that renders is a section that fetches, whether it means to or
 * not. Sections asserting only rendered HTML used to leave `fetch` alone
 * and silently inherit the previous section's mock; since #1346 the harness
 * hands `fetch` back at the boundary, so they reached node's real one
 * instead. Neither is a stub anyone chose. This is: idle status, and a
 * throw for any other URL, so a section that starts fetching something
 * unexpected says so rather than drifting.
 */
function stubIdleTriageFetch() {
  return stubGlobals({ fetch: async (url) => {
    if (String(url) === '/api/wrong-matches/triage/status') {
      return {
        ok: true,
        status: 200,
        json: async () => ({
          state: 'idle', started_at: null, finished_at: null,
          error: null, summary: null,
        }),
      };
    }
    throw new Error(`unexpected fetch in a render-only section: ${url}`);
  } });
}

async function flushMicrotasks(times = 30) {
  for (let i = 0; i < times; i += 1) {
    await Promise.resolve();
  }
}

/**
 * `installDom()` always wires `wrong-matches-content` and `toast`; the
 * returned `elements` object is an open registry a test can write an id
 * into BEFORE exercising code that looks it up (`wm-delete-group-btn-<id>`,
 * `wm-entry-card-<id>`, `wm-release-<id>`, …) — a shared extension point,
 * not a one-off inline `getElementById` override per test. `domStub` reads
 * the same object on every lookup, so a later write is visible.
 */
function installDom() {
  const wrongMatches = element();
  const toast = element({ style: { display: 'none' } });
  // Plain object stand-ins for the triage toolbar buttons (issues #1083 /
  // #1106) — real production code re-fetches BOTH by id at every
  // mutation point, never a node captured once at click or render time,
  // because a mid-sweep re-render (Refresh, tab switch, a page reload
  // discovering an already-running sweep) replaces the pane's innerHTML
  // and detaches any previously-captured node. Registered in the open
  // element map (issue #1086) so a test can `.set()` a DIFFERENT object
  // under the same id to simulate exactly that detachment.
  const cleanupBtn = element({
    id: 'wm-bulk-triage-btn',
    textContent: 'Cleanup Wrong Matches (0)',
  });
  const stopBtn = element({
    id: 'wm-bulk-triage-stop-btn',
    disabled: true,
    textContent: 'Stop',
  });
  const elements = {
    'wrong-matches-content': wrongMatches,
    toast,
    'wm-bulk-triage-btn': cleanupBtn,
    'wm-bulk-triage-stop-btn': stopBtn,
  };
  stubGlobals({ document: domStub(elements) });
  stubGlobals({ setTimeout: (fn) => {
    fn();
    return 0;
  } });
  return { wrongMatches, toast, elements, stopBtn, cleanupBtn };
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
  renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  stubGlobals({ fetch: async (url) => {
    calls.push(url);
    if (String(url).startsWith('/api/import-jobs/')) {
      return {
        ok: true,
        json: async () => ({ job }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  } });
  const btn = { textContent: '', style: {} };
  await pollImportJob(17, btn, logId);
  return { calls, dom, btn };
}

t.section('pollImportJob() removes row in place after completed jobs — no full refresh');
{
  const { calls, dom, btn } = await runPoll({
    status: 'completed',
    message: 'Import completed',
  }, 100);
  t.equal(btn.textContent, 'Imported', 'button shows imported');
  t.ok(!calls.includes('/api/wrong-matches'),
    'does NOT refetch the queue on completion (in-place removal)');
  t.equal(dom.toast.className, 'toast', 'completion toast is not an error');
}

t.section('pollImportJob() leaves row visible after failed jobs — no full refresh');
{
  const { calls, dom, btn } = await runPoll({
    status: 'failed',
    message: 'Pre-import gate rejected',
  }, 100);
  t.equal(btn.textContent, 'Failed', 'button shows failed');
  t.ok(!calls.includes('/api/wrong-matches'),
    'does NOT refetch the queue on failure (ambiguous source state)');
  t.equal(dom.toast.className, 'toast error', 'failure toast is an error');
}

t.section('pollImportJob() surfaces historical recovery while convergence continues');
{
  const { calls, dom, btn } = await runPoll({
    status: 'recovery_required',
    message: 'Recovery required: Beets may have run',
  }, 100);
  t.equal(btn.textContent, 'Recovery required', 'button shows historical recovery');
  t.ok(!calls.includes('/api/wrong-matches'),
    'does NOT refetch or imply the ambiguous operation completed');
  t.equal(dom.toast.className, 'toast error', 'historical recovery is prominent');
}

t.section('forceImportWrongMatch() maps processing conflict to the shared locked row state');
{
  const inserted = [];
  const btn = element({
    textContent: 'Force Import',
    isConnected: true,
    inserted,
  });
  const live = element();
  const calls = [];
  const globals = stubGlobals({
    confirm: () => true,
    document: {
      activeElement: btn,
      body: element({ isConnected: true }),
      createElement() { return element(); },
      getElementById(id) {
        if (id === 'processing-lock-live-region') return live;
        return inserted.find(node => node.id === id && node.isConnected) || null;
      },
      querySelectorAll() { return [btn]; },
    },
    window: { scrollX: 0, scrollY: 0, scrollTo() {} },
    fetch: async (url) => {
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
    },
  });
  await forceImportWrongMatch(100, btn);
  t.equal(calls.join(','), '/api/pipeline/force-import,/api/pipeline/42',
    'force import refetches only the owner request');
  t.equal(btn.getAttribute('aria-disabled'), 'true', 'force-import control locks');
  t.equal(btn.textContent, 'waiting to import', 'fresh owner state is rendered');
  t.contains(live.textContent, 'job #71', 'exact owner is announced');
  globals.restore();
}

t.section('converge helpers classify green candidates');
{
  installStorage();
  t.equal(normalizeThreshold(undefined), 180, 'default threshold is 180');
  t.equal(normalizeThreshold('9999'), 999, 'threshold is clamped high');
  t.equal(normalizeThreshold('-5'), 0, 'threshold is clamped low');
  t.ok(isConvergeGreen({ distance: 0.167 }, 180), '0.167 is green at 180');
  t.ok(isConvergeGreen({ distance: 0.180 }, 180), '0.180 is green at 180');
  t.ok(!isConvergeGreen({ distance: 0.226 }, 180), '0.226 is not green at 180');
  t.ok(!isConvergeGreen({ distance: null }, 180), 'missing distance is not green');
  t.deepEqual(
    convergeRequestBody('42', '180'),
    { request_id: 42, threshold_milli: 180, delete_unmatched: true },
    'converge always asks the API to delete non-green rows',
  );
}

t.section('renderWrongMatches() shows threshold controls and green state');
{
  installStorage();
  stubIdleTriageFetch();
  const dom = installDom();
  renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  t.contains(dom.wrongMatches.innerHTML, 'Loosen', 'renders threshold input');
  t.contains(dom.wrongMatches.innerHTML, '2 green', 'renders default green count');
  t.contains(dom.wrongMatches.innerHTML, 'Converge (2)', 'converge button includes count');
  t.excludes(dom.wrongMatches.innerHTML, 'remove all wrong matches when converging', 'cleanup checkbox is gone');
  t.contains(dom.wrongMatches.innerHTML, 'Cleanup Wrong Matches (3)', 'renders full-queue cleanup action');
  t.contains(dom.wrongMatches.innerHTML, 'Delete All (3)', 'renders per-group delete-all action');
  t.contains(dom.wrongMatches.innerHTML, 'deleteWrongMatch(100', 'renders per-entry delete action');
  t.contains(
    dom.wrongMatches.innerHTML,
    'data-pipeline-request-id="42"',
    'force-import controls carry the exact request identity',
  );
  t.contains(
    dom.wrongMatches.innerHTML,
    'forceImportWrongMatch(100, this)',
    'force-import controls pass their own initiating control',
  );

  setWrongMatchConvergeThreshold(42, 230);
  t.contains(dom.wrongMatches.innerHTML, '3 green', 'threshold edit updates green count');
  t.contains(dom.wrongMatches.innerHTML, 'Converge (3)', 'threshold edit updates converge count');
  await flushMicrotasks();
}

t.section('renderWrongMatches() keeps converge usable with active import jobs');
{
  installStorage();
  stubIdleTriageFetch();
  const dom = installDom();
  const data = JSON.parse(JSON.stringify(wrongMatchesData()));
  data.groups[0].import_jobs = [{
    id: 9,
    status: 'queued',
    request_id: 42,
    job_type: 'force_import',
  }];
  renderWrongMatches(data, dom.wrongMatches);

  t.excludes(dom.wrongMatches.innerHTML, 'Import Active', 'does not replace converge with Import Active');
  t.contains(dom.wrongMatches.innerHTML, 'Converge (2)', 'keeps converge label with active jobs');
  t.notMatch(dom.wrongMatches.innerHTML, /id="wm-converge-btn-42"[^>]*disabled/,
    'active jobs do not disable converge');
}

t.section('setWrongMatchConvergeThreshold() updates expanded group in place');
{
  installStorage();
  stubIdleTriageFetch();
  const dom = installDom();
  renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  const originalHtml = dom.wrongMatches.innerHTML;
  const elements = new Map();
  elements.set('wm-green-count-42', element());
  elements.set('wm-converge-btn-42', element({ textContent: 'Converge (2)' }));
  for (const id of [100, 101, 102]) {
    elements.set(`wm-entry-card-${id}`, element());
    elements.set(`wm-entry-green-${id}`, element());
    elements.set(`wm-entry-dist-${id}`, element());
  }
  globalThis.document.getElementById = (id) => {
    if (id === 'wrong-matches-content') return dom.wrongMatches;
    if (id === 'toast') return dom.toast;
    return elements.get(id) || null;
  };

  setWrongMatchConvergeThreshold(42, 230);

  t.equal(dom.wrongMatches.innerHTML, originalHtml, 'threshold edit does not rerender the pane');
  t.equal(elements.get('wm-green-count-42').textContent, '3 green', 'updates green count badge');
  t.equal(elements.get('wm-converge-btn-42').textContent, 'Converge (3)', 'updates converge button text');
  t.excludes(String(elements.get('wm-entry-green-102').style.cssText || ''), 'display:none', 'newly green entry badge is shown');
  await flushMicrotasks();
}

t.section('convergeWrongMatches() posts selected threshold and removes row in place');
{
  installStorage();
  // Before the render, not after: the render's own triage attach fetches
  // immediately, so a stub installed below it arrives too late and the
  // probe reaches node's real `fetch`.
  stubIdleTriageFetch();
  const dom = installDom();
  renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  setWrongMatchConvergeThreshold(42, 180);
  const calls = [];
  stubGlobals({ fetch: async (url, options = {}) => {
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
  } });
  const btn = { disabled: false, textContent: 'Converge', style: {} };
  await convergeWrongMatches(42, btn);
  t.equal(calls[0].url, '/api/wrong-matches/converge', 'posts to converge endpoint');
  t.deepEqual(
    JSON.parse(calls[0].options.body),
    { request_id: 42, threshold_milli: 180, delete_unmatched: true },
    'posts converge payload',
  );
  t.ok(!calls.some(call => call.url === '/api/wrong-matches'), 'does not refetch the whole wrong-matches pane');
  t.contains(dom.toast.textContent, 'Queued 2 candidates', 'toasts converge result');
  t.contains(dom.wrongMatches.innerHTML, 'No wrong matches', 'removes the emptied group locally');
}

t.section('deleteWrongMatch() posts one row and removes it in place — no full refresh');
{
  installStorage();
  const dom = installDom();
  renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  const calls = [];
  stubGlobals({ confirm: () => true });
  stubGlobals({ fetch: async (url, options = {}) => {
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
  } });
  const btn = { disabled: false, textContent: 'Delete', style: {} };
  await deleteWrongMatch(100, btn);
  t.equal(calls[0].url, '/api/wrong-matches/delete', 'posts to row delete endpoint');
  t.deepEqual(
    JSON.parse(calls[0].options.body),
    { download_log_id: 100 },
    'posts selected download log id',
  );
  t.ok(!calls.some(call => call.url === '/api/wrong-matches'),
    'does NOT refetch the queue after row delete (in-place removal)');
  t.contains(dom.toast.textContent, 'Deleted wrong match', 'toasts row delete result');
}

t.section('deleteWrongMatchGroup() posts request id and removes the group in place');
{
  installStorage();
  const dom = installDom();
  renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  const calls = [];
  stubGlobals({ confirm: () => true });
  stubGlobals({ fetch: async (url, options = {}) => {
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
  } });
  const btn = { disabled: false, textContent: 'Delete All (3)', style: {} };
  await deleteWrongMatchGroup(42, btn);
  t.equal(calls[0].url, '/api/wrong-matches/delete-group', 'posts to group delete endpoint');
  t.ok(!calls.some(call => call.url === '/api/wrong-matches'),
    'does NOT refetch the queue after group delete (in-place removal)');
  t.deepEqual(
    JSON.parse(calls[0].options.body),
    { request_id: 42 },
    'posts selected request id',
  );
  // "candidates" became "folders": a pointer-only clear over an
  // already-missing folder is counted separately and never headlined as a
  // deletion (issue #1063).
  t.contains(dom.toast.textContent, 'Deleted 3 folders', 'toasts group delete result');
}

t.section('delete controls handle cancel and failures');
{
  installStorage();
  const dom = installDom();
  renderWrongMatches(wrongMatchesData(), dom.wrongMatches);

  let calls = [];
  stubGlobals({ confirm: () => false });
  stubGlobals({ fetch: async (url, options = {}) => {
    calls.push({ url, options });
    throw new Error(`unexpected fetch: ${url}`);
  } });
  const cancelBtn = { disabled: false, textContent: 'Delete', style: {} };
  await deleteWrongMatch(100, cancelBtn);
  t.equal(calls.length, 0, 'row delete cancel does not fetch');
  t.equal(cancelBtn.disabled, false, 'row delete cancel leaves button enabled');

  stubGlobals({ confirm: () => true });
  stubGlobals({ fetch: async (url, options = {}) => {
    calls.push({ url, options });
    if (url === '/api/wrong-matches/delete') {
      return {
        ok: false,
        json: async () => ({ error: 'active_import_job' }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  } });
  const failBtn = { disabled: false, textContent: 'Delete', style: {} };
  await deleteWrongMatch(100, failBtn);
  t.equal(failBtn.disabled, false, 'row delete API failure restores button enabled');
  t.equal(failBtn.textContent, 'Delete', 'row delete API failure restores button text');
  t.equal(dom.toast.className, 'toast error', 'row delete API failure shows error toast');

  stubGlobals({ fetch: async () => {
    throw new Error('network down');
  } });
  const errorBtn = { disabled: false, textContent: 'Delete', style: {} };
  await deleteWrongMatch(100, errorBtn);
  t.equal(errorBtn.disabled, false, 'row delete fetch exception restores button enabled');
  t.equal(errorBtn.textContent, 'Delete', 'row delete fetch exception restores button text');

  calls = [];
  stubGlobals({ confirm: () => false });
  stubGlobals({ fetch: async (url, options = {}) => {
    calls.push({ url, options });
    throw new Error(`unexpected fetch: ${url}`);
  } });
  const cancelGroupBtn = { disabled: false, textContent: 'Delete All (3)', style: {} };
  await deleteWrongMatchGroup(42, cancelGroupBtn);
  t.equal(calls.length, 0, 'group delete cancel does not fetch');

  stubGlobals({ confirm: () => true });
  stubGlobals({ fetch: async (url, options = {}) => {
    calls.push({ url, options });
    if (url === '/api/wrong-matches/delete-group') {
      return {
        ok: false,
        json: async () => ({ error: 'cleanup_lock_unavailable' }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  } });
  const failGroupBtn = { disabled: false, textContent: 'Delete All (3)', style: {} };
  await deleteWrongMatchGroup(42, failGroupBtn);
  t.equal(failGroupBtn.disabled, false, 'group delete API failure restores button enabled');
  t.equal(failGroupBtn.textContent, 'Delete All (3)', 'group delete API failure restores button text');
  t.equal(dom.toast.className, 'toast error', 'group delete API failure shows error toast');

  stubGlobals({ fetch: async () => {
    throw new Error('network down');
  } });
  const errorGroupBtn = { disabled: false, textContent: 'Delete All (3)', style: {} };
  await deleteWrongMatchGroup(42, errorGroupBtn);
  t.equal(errorGroupBtn.disabled, false, 'group delete fetch exception restores button enabled');
  t.equal(errorGroupBtn.textContent, 'Delete All (3)', 'group delete fetch exception restores button text');
}

t.section('bulkTriageWrongMatches() posts full-queue confirmation and refreshes');
{
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  renderWrongMatches(data, dom.wrongMatches);
  t.contains(dom.wrongMatches.innerHTML, 'Cleanup Wrong Matches (3)', 'renders full-queue cleanup button');
  const calls = [];
  stubGlobals({ confirm: () => true });
  // The sweep runs server-side on a background thread; the client polls.
  // Collapse the poll delay so the test doesn't sleep for real.
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });
  stubGlobals({ fetch: async (url, options = {}) => {
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
  } });
  // The Stop button is enabled the moment the sweep starts (before the
  // first status poll even fires) and disabled again once it's done.
  let stopBtnEnabledDuringSweep = null;
  // Not node's real fetch: the mock this section installed a few lines
  // above, which this second stub wraps to observe the button mid-sweep.
  const innerFetch = globalThis.fetch;
  stubGlobals({ fetch: async (url, options) => {
    if (stopBtnEnabledDuringSweep === null) {
      stopBtnEnabledDuringSweep = dom.stopBtn.disabled === false;
    }
    return innerFetch(url, options);
  } });
  // #1106: both toolbar buttons are looked up by id at mutation time,
  // never held as a captured node — bulkTriageWrongMatches() no longer
  // takes a button argument at all.
  await bulkTriageWrongMatches();
  t.ok(stopBtnEnabledDuringSweep, 'Stop button is enabled while the sweep runs');
  t.equal(dom.stopBtn.disabled, true, 'Stop button is disabled again once the sweep completes');
  t.equal(dom.stopBtn.textContent, 'Stop', 'Stop button label is restored');
  t.equal(dom.cleanupBtn.disabled, true, 'Cleanup button is disabled again once the queue is empty');
  t.equal(dom.cleanupBtn.textContent, 'Cleanup Wrong Matches (0)', 'Cleanup label reflects the post-refresh count');
  t.equal(calls[0].url, '/api/wrong-matches/triage', 'posts to cleanup endpoint');
  t.deepEqual(
    JSON.parse(calls[0].options.body),
    { confirm_all_wrong_matches: true },
    'posts explicit full-queue confirmation',
  );
  t.ok(calls.some(call => call.url === '/api/wrong-matches/triage/status'),
    'polls the background sweep status');
  t.ok(calls.some(call => call.url === '/api/wrong-matches'), 'refetches the full pane after cleanup');
  t.contains(dom.toast.textContent, 'Deleted 2 candidates', 'toasts cleanup result');
  t.contains(dom.wrongMatches.innerHTML, 'No wrong matches', 'renders refreshed empty state');
  globals.restore();
}

t.section('bulkTriageWrongMatches() handles a restart-lost sweep as partial, not failed');
{
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  renderWrongMatches(data, dom.wrongMatches);
  stubGlobals({ confirm: () => true });
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });
  stubGlobals({ fetch: async (url, _options = {}) => {
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
  } });
  await bulkTriageWrongMatches();
  t.equal(dom.cleanupBtn.disabled, true, 'restart-lost sweep leaves Cleanup disabled (queue is empty post-refresh)');
  t.contains(dom.toast.textContent, 'status lost', 'restart-lost sweep explains the lost status');
  t.excludes(dom.toast.textContent, 'failed', 'restart-lost sweep is not reported as failed');
  t.contains(dom.wrongMatches.innerHTML, 'No wrong matches', 'restart-lost sweep still refreshes the pane');
  globals.restore();
}

t.section('bulkTriageWrongMatches() reports a cancelled sweep distinctly from completion (issue #1083)');
{
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  renderWrongMatches(data, dom.wrongMatches);
  stubGlobals({ confirm: () => true });
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });
  stubGlobals({ fetch: async (url, _options = {}) => {
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
  } });
  await bulkTriageWrongMatches();
  t.equal(dom.cleanupBtn.disabled, true, 'cancelled sweep leaves Cleanup disabled (queue is empty post-refresh)');
  t.equal(dom.stopBtn.disabled, true, 'Stop button is disabled once the sweep reaches a terminal state');
  t.contains(dom.toast.textContent, 'stopped', 'cancelled sweep says "stopped", not "completed"');
  t.contains(dom.toast.textContent, 'Deleted 1 candidate', 'cancelled sweep still reports what ran');
  t.equal(dom.toast.className, 'toast', 'cancelled sweep is not toasted as an error');
  t.contains(dom.wrongMatches.innerHTML, 'No wrong matches', 'cancelled sweep still refreshes the pane');
  globals.restore();
}

t.section('stopWrongMatchTriage() posts to the cancel endpoint and stays disabled on success');
{
  installStorage();
  const dom = installDom();
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });
  const calls = [];
  stubGlobals({ fetch: async (url, options = {}) => {
    calls.push({ url, options });
    if (url === '/api/wrong-matches/triage/cancel') {
      return { ok: true, status: 200, json: async () => ({ state: 'running' }) };
    }
    // #1106 N7c: with nothing currently followed, a successful cancel
    // schedules one fresh derive -- answer it as already terminal so
    // the dangling follow attempt settles immediately.
    return {
      ok: true, status: 200,
      json: async () => ({
        state: 'cancelled', started_at: '2026-06-11T00:00:00+00:00',
        finished_at: '2026-06-11T00:01:00+00:00', error: null,
        summary: { processed: 1, deleted: 1, cancelled: true },
      }),
    };
  } });
  // #1106: no button argument — always the currently-registered node.
  await stopWrongMatchTriage();
  t.equal(calls[0].url, '/api/wrong-matches/triage/cancel', 'posts to the canonical cancel route');
  t.equal(calls[0].options.method, 'POST', 'cancel is a POST');
  t.equal(dom.stopBtn.disabled, true, 'button stays disabled after a successful cancel request');
  t.equal(dom.stopBtn.textContent, 'Stopping...', 'button shows the in-flight stopping state');
  t.ok(calls.some(call => call.url === '/api/wrong-matches/triage/status'),
    'N7c: with no follower attached, a successful cancel also schedules a fresh derive so the button is not stranded');
  await flushMicrotasks(50);
  globals.restore();
}

t.section('stopWrongMatchTriage() re-enables the button when the request itself fails');
{
  installStorage();
  const dom = installDom();
  stubGlobals({ fetch: async () => {
    throw new Error('network down');
  } });
  await stopWrongMatchTriage();
  t.equal(dom.stopBtn.disabled, false, 'a failed cancel request restores the button enabled');
  t.equal(dom.stopBtn.textContent, 'Stop', 'a failed cancel request restores the button label');
  t.contains(dom.toast.textContent, 'Stop request failed', 'a failed cancel request is toasted');
}

t.section('stopWrongMatchTriage() mutates the CURRENTLY registered Stop node, never a node captured earlier (#1106)');
{
  installStorage();
  const dom = installDom();
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });
  // Simulate a mid-sweep re-render replacing the pane's innerHTML: a
  // BRAND NEW Stop node takes over the same id, and the original
  // installDom() node is now detached — exactly what happened to
  // bulkTriageWrongMatches()'s old `restore()` closure before this fix.
  // Both nodes start identical (disabled: false) so a diverging value
  // after the call can only mean one of them was actually mutated.
  const staleStopBtn = dom.stopBtn;
  staleStopBtn.disabled = false;
  const freshStopBtn = element({
    id: 'wm-bulk-triage-stop-btn',
    textContent: 'Stop',
  });
  dom.elements['wm-bulk-triage-stop-btn'] = freshStopBtn;
  stubGlobals({ fetch: async (url) => {
    if (url === '/api/wrong-matches/triage/cancel') {
      return { ok: true, status: 200, json: async () => ({ state: 'running' }) };
    }
    // #1106 N7c: a successful cancel with nothing followed schedules a
    // fresh derive -- answer it so the dangling attempt settles.
    return {
      ok: true, status: 200,
      json: async () => ({
        state: 'cancelled', started_at: '2026-06-11T00:00:00+00:00',
        finished_at: '2026-06-11T00:01:00+00:00', error: null,
        summary: { processed: 1, deleted: 1, cancelled: true },
      }),
    };
  } });
  await stopWrongMatchTriage();
  t.equal(freshStopBtn.disabled, true, 'the currently-registered node is mutated');
  t.equal(freshStopBtn.textContent, 'Stopping...', 'the currently-registered node shows the in-flight label');
  t.equal(staleStopBtn.disabled, false, 'a node registered before the swap is left untouched');
  t.equal(staleStopBtn.textContent, 'Stop', 'a node registered before the swap is left untouched');
  await flushMicrotasks(50);
  globals.restore();
}

t.section('bulkTriageWrongMatches() surfaces a failed sweep and restores the button');
{
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  renderWrongMatches(data, dom.wrongMatches);
  stubGlobals({ confirm: () => true });
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });
  stubGlobals({ fetch: async (url, _options = {}) => {
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
  } });
  await bulkTriageWrongMatches();
  t.equal(dom.cleanupBtn.disabled, false, 'failed sweep restores button enabled');
  t.equal(dom.cleanupBtn.textContent, 'Cleanup Wrong Matches (3)', 'failed sweep restores button text off the still-current count');
  t.contains(dom.toast.textContent, 'sweep blew up', 'failed sweep toasts the error');
  t.equal(dom.toast.className, 'toast error', 'failed sweep shows error toast');
  globals.restore();
}

t.section('formatEntryEvidence() formats spectral and lossless-source V0 cells');
{
  const request6039 = formatEntryEvidence({
    format: 'MP3',
    min_bitrate: 194,
    avg_bitrate: 288,
  });
  t.equal(
    request6039.format,
    'MP3 avg 288k · min 194k',
    'current candidate summary labels average and retains the floor',
  );

  const gas = formatEntryEvidence({
    source_codec: 'flac',
    source_container: 'flac',
    target_format: 'opus 128',
    format: 'opus 128',
    min_bitrate: 191,
    avg_bitrate: 224,
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_avg_bitrate: 224,
  });
  t.equal(
    gas.format,
    'FLAC → OPUS 128 contract',
    'Gas source and target render separately without relabelling the V0 proxy',
  );
  t.equal(gas.v0, 'V0 ≈ 224 kbps', 'Gas V0 probe remains its own fact');
  t.excludes(gas.format, '191', 'target contract does not claim the V0 min');
  t.excludes(gas.format, '224', 'target contract does not claim the V0 average');

  // Happy path: AE1 — both pieces of evidence present.
  let cells = formatEntryEvidence({
    spectral_grade: 'genuine',
    spectral_bitrate: 950,
    v0_probe_kind: 'lossless_source_v0',
    v0_probe_avg_bitrate: 265,
  });
  t.contains(cells.spectral, 'genuine', 'spectral cell shows the grade');
  t.contains(cells.spectral, '950', 'spectral cell shows the bitrate floor');
  t.contains(cells.v0, '265', 'V0 cell shows the lossless-source probe average');

  // AE2: missing evidence renders as a dash, not as a preview trigger.
  cells = formatEntryEvidence({
    spectral_grade: null,
    spectral_bitrate: null,
    v0_probe_kind: null,
    v0_probe_avg_bitrate: null,
  });
  t.equal(cells.spectral, '—', 'absent spectral evidence renders as a dash');
  t.equal(cells.v0, '—', 'absent V0 evidence renders as a dash');
  t.excludes(cells.spectral.toLowerCase(), 'preview', 'no preview trigger in spectral cell');
  t.excludes(cells.v0.toLowerCase(), 'preview', 'no preview trigger in V0 cell');

  // Wrong-match review surfaces V0 evidence regardless of source lineage —
  // operators want to compare every candidate's bitrate at a glance, not
  // just the lossless-source ones. Whichever probe ran, show the average.
  cells = formatEntryEvidence({
    spectral_grade: 'suspect',
    spectral_bitrate: 320,
    v0_probe_kind: 'native_lossy_research_v0',
    v0_probe_avg_bitrate: 240,
  });
  t.contains(cells.spectral, 'suspect', 'spectral cell still renders for suspect grade');
  t.contains(cells.v0, '240',
    'V0 probe surfaces regardless of source lineage for wrong-match review');

  // Edge: spectral present, V0 absent (rejected pre-conversion).
  cells = formatEntryEvidence({
    spectral_grade: 'marginal',
    spectral_bitrate: 800,
    v0_probe_kind: null,
    v0_probe_avg_bitrate: null,
  });
  t.contains(cells.spectral, 'marginal', 'marginal grade renders');
  t.equal(cells.v0, '—', 'absent V0 still renders as dash');

  // Edge: missing the four keys entirely (extra defensive — payload should
  // always include them, but the renderer must not crash if it doesn't).
  cells = formatEntryEvidence({});
  t.equal(cells.spectral, '—', 'missing keys render as dash');
  t.equal(cells.v0, '—', 'missing keys render as dash');
}

t.section('renderQualityBadges() labels current average and retained floor fallbacks');
{
  let html = renderQualityBadges({
    in_library: true,
    quality_label: null,
    format: null,
    avg_bitrate: 288,
    min_bitrate: 194,
  });
  t.contains(html, 'avg 288k · min 194k',
    'missing-format fallback leads with the current average and labels the floor');
  t.excludes(html, '>194k<', 'minimum bitrate is never rendered as a bare current tier');

  html = renderQualityBadges({
    in_library: true,
    quality_label: null,
    format: null,
    avg_bitrate: null,
    min_bitrate: 194,
  });
  t.contains(html, 'min 194k', 'missing-average fallback labels minimum as floor data');
  t.excludes(html, '>194k<', 'missing average never revives a bare min-derived tier');

  html = renderQualityBadges({
    in_library: true,
    quality_label: null,
    format: null,
    avg_bitrate: 288,
    min_bitrate: null,
  });
  t.contains(html, 'avg 288k', 'average-only fallback remains visible current data');

  html = renderQualityBadges({
    in_library: true,
    quality_label: 'MP3 V0',
    format: 'MP3',
    avg_bitrate: 288,
    min_bitrate: 194,
  });
  t.contains(html, 'MP3 V0', 'explicit backend quality label remains authoritative');
  t.excludes(html, 'avg 288k', 'fallback summary is omitted with an explicit label');

  t.equal(
    renderQualityBadges({
      in_library: false,
      quality_label: null,
      format: null,
      avg_bitrate: 0,
      min_bitrate: 0,
    }),
    '<span class="badge" style="background:#3a2a2a;color:#f88;">nothing on disk</span>',
    'zero bitrate placeholders are treated as absent off disk',
  );
  t.equal(
    renderQualityBadges({
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

t.section('wrong-match headers use the shared ordered spectral badge palette');
for (const [grade, tone] of [
  ['likely_transcode', 'poor'],
  ['suspect', 'acceptable'],
  ['marginal', 'good'],
]) {
  const html = renderQualityBadges({
    in_library: true,
    quality_label: 'MP3 V2',
    quality_rank: 'excellent',
    current_spectral_grade: grade,
    current_spectral_bitrate: 128,
  });
  t.contains(html, `badge-rank-${tone}`, `${grade} uses shared ${tone} badge`);
  t.contains(html, grade.replaceAll('_', ' '), `${grade} is humanized`);
  if (grade.includes('_')) t.excludes(html, grade, `${grade} raw token stays hidden`);
}

t.section('wrong-match bucket badges use the same canonical classes as every other view');
for (const rank of ['poor', 'acceptable', 'good', 'excellent', 'transparent', 'lossless']) {
  const html = renderQualityBadges({
    in_library: true,
    quality_label: rank,
    quality_rank: rank,
  });
  t.contains(html, `badge-rank-${rank}`, `${rank} uses its canonical rank class`);
}

t.section('wrong-match verified-lossless identity reuses the lossless bucket colour');
{
  const html = renderQualityBadges({
    in_library: true,
    quality_label: 'FLAC',
    quality_rank: 'lossless',
    verified_lossless: true,
  });
  t.contains(html, 'verified lossless', 'verified identity remains explicit');
  t.ok(countOccurrences(html, 'badge-rank-lossless') === 3,
    'quality label, verified identity, and rank label share lossless colour');
}

t.section('renderEntry() embeds evidence cells without preview hooks');
{
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  data.groups[0].entries[0].spectral_grade = 'suspect';
  data.groups[0].entries[0].spectral_bitrate = 320;
  data.groups[0].entries[0].v0_probe_kind = 'lossless_source_v0';
  data.groups[0].entries[0].v0_probe_avg_bitrate = 265;
  renderWrongMatches(data, dom.wrongMatches);
  const html = dom.wrongMatches.innerHTML;
  t.contains(html, 'suspect', 'rendered HTML carries the spectral grade');
  t.contains(html, 'quality-tone-acceptable',
    'candidate spectral metadata uses the same orange suspect tone');
  t.contains(html, '265', 'rendered HTML carries the lossless-source V0 average');
  t.contains(html, 'Downloaded as', 'rendered HTML surfaces preserved source folders');
  t.contains(html, 'wm-explorer-100', 'rendered HTML includes an explorer mount');
  // R3 / AE2: no preview button or preview action surfaces in this feature.
  t.notMatch(html, /data-action=["']preview["']/, 'no data-action=preview attribute');
  t.notMatch(html, /preview[-_]btn/, 'no preview button class');
  t.notMatch(html, /onclick=["'][^"']*preview/i, 'no onclick handler invoking preview');
}

t.section('renderWrongMatches() preserves ordinary candidate metadata presentation');
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
  renderWrongMatches(data, dom.wrongMatches);
  t.contains(dom.wrongMatches.innerHTML, '(1969)', 'ordinary candidate year remains visible');
  t.contains(dom.wrongMatches.innerHTML, ' MP3', 'ordinary local format remains visible');
}

t.section('renderWrongMatches() escapes candidate metadata at the live HTML sink');
{
  const knownBad = '<span><script>alert(1)</script></span>';
  t.ok(!metadataHtmlIsEscaped(knownBad, '<script>alert(1)</script>'),
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
      renderWrongMatches(data, dom.wrongMatches);
      t.ok(metadataHtmlIsEscaped(dom.wrongMatches.innerHTML, year),
        `candidate year escaped: ${JSON.stringify(year)}`);
      t.ok(metadataHtmlIsEscaped(dom.wrongMatches.innerHTML, format),
        `local format escaped: ${JSON.stringify(format)}`);
    }
  }
}

t.section('renderWrongMatchExplorer() collapses shared album tags and hides replaygain noise');
{
  const html = renderWrongMatchExplorer({
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

  t.contains(html, 'Downloaded as', 'keeps the original user folder in the summary');
  t.contains(html, 'albumartist', 'renders shared album-level tags');
  t.contains(html, '2 tracks in surviving folder in matched order', 'surfaces matched-order explorer label');
  t.equal(countOccurrences(html, 'The Castiles Live (Vol. 1)'), 2, 'album name appears in the preserved source folder and shared tag summary');
  t.contains(html, 'Purple Haze', 'renders the first track title inline');
  t.contains(html, 'Get Outta My Life', 'renders the second track title inline');
  t.contains(html, 'https://musicbrainz.org/release/20f1e791-34cd-4b47-8783-51492b90218a', 'links musicbrainz_albumid to the release page');
  t.contains(html, 'https://musicbrainz.org/artist/4f13e8cb-11aa-4b1a-8bb5-0ad1437dbdee', 'links musicbrainz_artistid to the artist page');
  t.equal(countOccurrences(html, '<audio'), 2, 'renders one player per track');
  t.excludes(html, 'replaygain_album_gain', 'hides replaygain album tags');
  t.excludes(html, 'replaygain_track_gain', 'hides replaygain track tags');
}

t.section('renderWrongMatchExplorer() distinguishes a containment refusal from a world failure, visibly and without a futile Retry (issue #1086 review)');
{
  // A CONTAINMENT refusal (symlink/socket/FIFO/device node) alongside a
  // readable track: `status: "ok"`, `files` non-empty. Re-fetching can
  // never change a containment decision, so no Retry, and the lead
  // sentence must not say "could not be read" — that phrasing is
  // reserved for a world failure a retry might clear.
  const containmentHtml = renderWrongMatchExplorer({
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
  t.contains(
    containmentHtml,
    '1 entry was refused (not read) as a containment decision',
    'containment refusal leads with the containment sentence',
  );
  t.excludes(containmentHtml, 'could not be read', 'containment refusal never says "could not be read"');
  t.excludes(containmentHtml, 'Retry', 'containment refusal offers no Retry — re-fetching cannot change it');

  // The world-failure control: same shape, EACCES instead of a symlink.
  const worldFailureHtml = renderWrongMatchExplorer({
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
  t.contains(
    worldFailureHtml,
    '1 entry could not be read',
    'world-failure refusal leads with the "could not be read" sentence',
  );
  t.excludes(worldFailureHtml, 'refused (not read)', 'world-failure refusal never uses the containment wording');
  t.contains(worldFailureHtml, 'Retry', 'world-failure refusal offers Retry — the world might have cleared');
  t.contains(
    worldFailureHtml,
    'window.reloadWrongMatchExplorer(901)',
    'Retry targets the exact entry id',
  );
}

t.section('renderWrongMatchExplorer() empty-state (status:"unavailable") also honours the containment discriminator — the #1086 review blocker 1 shape');
{
  // The exact scenario the review named: a folder holding ONLY a
  // symlink is `status: "unavailable"` (nothing readable), so this hits
  // the EMPTY branch (`files.length === 0`), not the per-entry-notice
  // branch a partial listing uses above. Before the fix, this branch's
  // own wording ignored `unreadableIsContainment` entirely.
  const containmentEmpty = renderWrongMatchExplorer({
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
  t.excludes(containmentEmpty, 'could not be read', 'containment-refused empty state never says "could not be read"');
  t.contains(containmentEmpty, 'refused (not read)', 'containment-refused empty state uses the containment wording');
  t.excludes(containmentEmpty, 'Retry', 'containment-refused empty state offers no Retry');
  t.contains(
    containmentEmpty,
    'NOT evidence that the folder is empty',
    'still denies the folder is confidently empty',
  );

  const worldFailureEmpty = renderWrongMatchExplorer({
    status: 'unavailable',
    download_log_id: 903,
    partial: true,
    unreadable_entry_count: 3,
    unreadable_reason: '01.flac: could not be read, may be transient (EACCES)',
    unreadable_is_containment: false,
    audio_file_count: 0,
    files: [],
  });
  t.contains(worldFailureEmpty, 'could not be read', 'world-failure empty state still says "could not be read"');
  t.excludes(worldFailureEmpty, 'refused (not read)', 'world-failure empty state never uses the containment wording');
  t.contains(worldFailureEmpty, 'Retry', 'world-failure empty state still offers Retry');
  t.contains(
    worldFailureEmpty,
    'NOT evidence that the folder is empty',
    'still denies the folder is confidently empty',
  );
}

t.section('maybeLoadWrongMatchExplorer() lazy-loads explorer tags and audio on <details> toggle');
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
  stubGlobals({ fetch: async (url) => {
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
  } });

  // Entry expand alone is cheap — no fetch.
  await toggleWrongMatchEntry('wm-entry-100', 100);
  t.deepEqual(calls, [], 'entry expand does not auto-load the file explorer');

  // Closed <details> toggle does nothing.
  const closedDetails = { open: false };
  await maybeLoadWrongMatchExplorer(100, closedDetails);
  t.deepEqual(calls, [], 'closed details element does not trigger a load');

  // Opened <details> toggle lazy-loads exactly once.
  const openDetails = { open: true };
  await maybeLoadWrongMatchExplorer(100, openDetails);
  t.deepEqual(
    calls,
    ['/api/wrong-matches/explorer?download_log_id=100'],
    'opening the file-explorer dropdown loads the explorer exactly once',
  );
  t.contains(mount.innerHTML, 'Downloaded as', 'explorer shows the original user folder');
  t.contains(mount.innerHTML, 'Scott 3', 'explorer shows shared album tags once loaded');
  t.contains(mount.innerHTML, 'It&#39;s Raining Today', 'explorer shows extracted tags');
  t.contains(mount.innerHTML, 'https://musicbrainz.org/release/20f1e791-34cd-4b47-8783-51492b90218a', 'lazy-loaded explorer links the album MBID');
  t.contains(mount.innerHTML, 'https://musicbrainz.org/recording/d5b1a858-84be-4005-a2a0-29dfcf005851', 'lazy-loaded explorer links the recording MBID');
  t.contains(mount.innerHTML, '<audio', 'explorer renders a browser audio player');
  t.excludes(mount.innerHTML, 'replaygain_track_gain', 'explorer hides replaygain noise');

  await maybeLoadWrongMatchExplorer(100, openDetails);
  await maybeLoadWrongMatchExplorer(100, openDetails);
  t.equal(calls.length, 1, 'reopening the dropdown reuses the loaded explorer state');
}

t.section('maybeLoadWrongMatchExplorer() renders the honest copy for a refused listing');
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
  stubGlobals({ fetch: async (url) => ({
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
  }) });

  await maybeLoadWrongMatchExplorer(200, { open: true });

  t.excludes(mount.innerHTML, 'Failed to load file explorer',
    'a renderable unavailable payload is not treated as a load failure');
  t.contains(mount.innerHTML, '3 entries could not be read',
    'the refusal count reaches the operator');
  t.contains(mount.innerHTML, 'nothing here is confirmed missing',
    'the listing is labelled incomplete');
  t.contains(mount.innerHTML, 'NOT evidence that the folder is empty',
    'an unreadable folder is never presented as an empty one');
  t.contains(mount.innerHTML, 'Permission denied',
    'the refusal reason is shown');
  // An unreadable folder is a world the operator can REPAIR, so the panel
  // owes a Retry and must not cache the answer — otherwise the only way
  // to see a fixed permission is a full page reload (issue #1063).
  t.contains(mount.innerHTML, 'Retry',
    'an unavailable listing offers a retry');
  t.contains(mount.innerHTML, 'window.reloadWrongMatchExplorer(200)',
    'the retry re-reads THIS entry');

  await maybeLoadWrongMatchExplorer(200, { open: true });
  t.equal(calls.length, 2,
    'reopening after an unavailable listing re-fetches instead of caching');
}

t.section('maybeLoadWrongMatchExplorer() surfaces a 503 refusal reason instead of swallowing it');
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
  stubGlobals({ fetch: async () => ({
    ok: false,
    status: 503,
    json: async () => ({
      error: 'Wrong-match files could not be read: /x/wrong_matches/Album '
        + '(quarantine path is contained but unavailable: cannot open '
        + '/x/wrong_matches/Album: Permission denied)',
    }),
  }) });

  await maybeLoadWrongMatchExplorer(201, { open: true });

  // Issue #1099: a whole-root 503 now gets its own status-honest lead
  // sentence instead of the old one-size-fits-all "Failed to load file
  // explorer" — the operator still needs to know this IS a failure and
  // that it's the retryable-world-failure kind, not a containment refusal.
  // Review round 1: the wording must not PROMISE transience — the 503
  // bucket also carries the unclassified residual code, which is not a
  // disk hiccup a retry will clear.
  t.contains(mount.innerHTML, 'could not be read',
    'a real transport/authority failure still reads as a failure');
  t.contains(mount.innerHTML, 'may be temporary',
    '503 copy hedges rather than promising a retry will succeed');
  t.excludes(mount.innerHTML, 'a retry may succeed',
    '503 copy must not overclaim transience for the residual bucket');
  // "could not be read" alone is now ambiguous — the LEAD copy itself
  // contains that phrase — so assert something unique to the server's
  // OWN detail text to prove it still rides along, not just the lead.
  t.contains(mount.innerHTML, 'Permission denied',
    'the server’s own reason still reaches the operator as detail');
  t.contains(mount.innerHTML, 'Retry',
    'the retry affordance survives — a 503 can plausibly clear');
}

t.section('maybeLoadWrongMatchExplorer() surfaces a whole-root 422 refusal, never as "not found", with no Retry (issue #1099)');
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
  stubGlobals({ fetch: async () => ({
    ok: false,
    status: 422,
    json: async () => ({
      error: 'Wrong-match files refused: /x/wrong_matches/Album '
        + '(quarantine path is contained but unavailable: unsafe symlink: '
        + '/x/wrong_matches/Album)',
    }),
  }) });

  await maybeLoadWrongMatchExplorer(205, { open: true });

  t.contains(mount.innerHTML.toLowerCase(), 'refused',
    'a whole-root containment refusal names itself as a refusal');
  t.excludes(mount.innerHTML.toLowerCase(), 'not found',
    'a containment refusal must never read as a definitive absence');
  // Review round 1: the #1086 doctrine ("containment carries no Retry")
  // applies here too — re-fetching the same name answers the same
  // refusal every time, so offering Retry would be a dead end.
  t.excludes(mount.innerHTML, 'Retry',
    'a containment refusal offers no Retry — retrying can never help');
}

t.section('maybeLoadWrongMatchExplorer() treats a PARTIAL listing as repairable too');
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
  stubGlobals({ fetch: async (url) => ({
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
  }) });

  await maybeLoadWrongMatchExplorer(202, { open: true });
  t.contains(mount.innerHTML, '1 entry could not be read',
    'the partial listing names its refusal');
  t.contains(mount.innerHTML, '1 track in surviving folder',
    'the partial listing still lists what it could read');
  t.contains(mount.innerHTML, 'Retry',
    'a PARTIAL listing offers a retry, not only an empty one');
  t.contains(mount.innerHTML, 'window.reloadWrongMatchExplorer(202)',
    'the partial listing retry re-reads THIS entry');

  // The operator repairs the world; the retry must show the repair.
  refused = false;
  await reloadWrongMatchExplorer(202);
  t.equal(calls.length, 2, 'the retry re-reads the folder');
  t.excludes(mount.innerHTML, 'could not be read',
    'the repaired listing drops the refusal notice');
  t.contains(mount.innerHTML, '2 tracks in surviving folder',
    'the repaired listing shows the previously-refused track');
  t.excludes(mount.innerHTML, 'Retry',
    'a complete listing needs no retry');

  // …and a complete listing IS cached again.
  await maybeLoadWrongMatchExplorer(202, { open: true });
  t.equal(calls.length, 2,
    'a complete listing is cached, so reopening does not re-fetch');
}

t.section('explorerListingIsRepairable() keys off refusals, not status');
{
  t.ok(explorerListingIsRepairable(
    { status: 'ok', unreadable_entry_count: 1 }),
    'a partial ok listing is repairable');
  t.ok(explorerListingIsRepairable(
    { status: 'unavailable', unreadable_entry_count: 0 }),
    'an unavailable listing is repairable');
  t.ok(!explorerListingIsRepairable(
    { status: 'ok', unreadable_entry_count: 0 }),
    'a complete listing is not repairable');
  t.ok(!explorerListingIsRepairable(
    { status: 'ok', unreadable_entry_count: 0, truncated_reason: 'file_limit' }),
    'a truncated listing is not repairable — retrying hits the same limit');
}

t.section('renderWrongMatchExplorer() must still work: a complete listing claims nothing');
{
  const html = renderWrongMatchExplorer({
    status: 'ok',
    audio_file_count: 0,
    other_file_count: 0,
    partial: false,
    truncated_reason: null,
    unreadable_entry_count: 0,
    unreadable_reason: null,
    files: [],
  });
  t.contains(html, 'No audio files found in this folder.',
    'a readable empty folder still reads as empty');
  t.excludes(html, 'could not be read',
    'a complete listing never claims a refusal');
  t.excludes(html, 'NOT evidence',
    'a complete listing never denies emptiness');
}

t.section('cleanupSummaryToast() reports kept, skipped, and delete failures');
{
  const body = cleanupSummaryToast({
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
  t.equal(body, 'Deleted 2 candidates, kept 4, skipped 5', 'summarizes cleanup outcomes');
}

t.section('cleanupSummaryToast() includes verified-lossless deletes and current-evidence-failed skips');
{
  const body = cleanupSummaryToast({
    deleted: 1,
    deleted_verified_lossless_parent: 4,
    kept_would_import: 0,
    kept_uncertain: 0,
    skipped_current_evidence_failed: 2,
    skipped_active_job: 1,
    delete_failed: 0,
  });
  t.equal(body, 'Deleted 5 candidates, kept 0, skipped 3', 'includes new outcome categories in totals');
}

t.section('renderLatestImport() distinguishes absent / in-library / verified-lossless / present states');
{
  // 1. No latest import, album not in library — neutral copy.
  let html = renderLatestImport(null, { in_library: false, verified_lossless: false });
  t.contains(html, 'No previous import on disk.', 'absent: renders neutral "no previous import" copy');
  t.excludes(html, 'Album already in library', 'absent: does not claim album in library');
  t.excludes(html, 'Verified-lossless copy in library', 'absent: no verified-lossless copy');
  t.excludes(html, 'No successful import on disk', 'absent: no longer uses old "No successful import on disk" copy');

  // 2. No latest import, album in library, not verified lossless — distinguishes
  //    "no cratedigger history" from "Beets already has this MBID".
  html = renderLatestImport(null, { in_library: true, verified_lossless: false });
  t.contains(html, 'Album already in library', 'in_library: surfaces the in-library copy');
  t.contains(html, 'must beat current quality', 'in_library: explains upgrade gate semantics');
  t.excludes(html, 'No previous import', 'in_library: does not claim no prior import');
  t.excludes(html, 'No successful import', 'in_library: no longer uses old "No successful import" copy');
  t.excludes(html, 'Verified-lossless copy in library', 'in_library: not the verified-lossless branch');

  // 3. No latest import, album in library AND verified lossless — strongest copy.
  html = renderLatestImport(null, { in_library: true, verified_lossless: true });
  t.contains(html, 'Verified-lossless copy in library', 'verified-lossless: surfaces the verified-lossless copy');
  t.contains(html, 'cleared on the next cleanup sweep', 'verified-lossless: explains the cleanup behavior');
  t.excludes(html, 'Album already in library', 'verified-lossless: does not fall back to plain in-library copy');
  t.excludes(html, 'No previous import', 'verified-lossless: does not fall back to absent copy');

  // 4. Latest import present — render existing summary regardless of in_library.
  html = renderLatestImport(
    {
      outcome: 'imported',
      created_at: '2026-05-17T00:00:00Z',
      actual_filetype: 'flac',
      actual_min_bitrate: 950,
    },
    { in_library: true, verified_lossless: false },
  );
  t.contains(html, 'Last import: imported', 'present: renders existing latest-import summary');
  t.contains(html, 'FLAC 950k', 'present: renders filetype and bitrate floor');
  t.excludes(html, 'Album already in library', 'present: in_library flag does not override the summary');
  t.excludes(html, 'No previous import', 'present: does not render absent copy');
}

// --- issue #829 Phase 5 PR4/N3: audit-only flags on both WM surfaces ---

t.section('renderQualityBadges() withholds an audit-only HAVE accusation');
{
  const group = {
    in_library: true,
    quality_label: 'AAC 256k',
    format: 'AAC',
    current_spectral_grade: 'likely_transcode',
    current_spectral_bitrate: 128,
  };
  let html = renderQualityBadges({
    ...group,
    current_spectral_accusation_admissible: false,
    current_spectral_accusation_withheld: 'audit_only_codec',
  });
  t.contains(html, 'likely transcode', 'the measured grade stays visible');
  t.contains(html, 'audit-only', 'the withheld suffix is stated');
  t.contains(html, 'native encoder behaviour', 'the hover explains why');
  t.excludes(html, 'badge-rank-poor',
    'the accusing red badge is withheld');

  html = renderQualityBadges({
    ...group,
    current_spectral_accusation_admissible: true,
    current_spectral_accusation_withheld: null,
  });
  t.contains(html, 'badge-rank-poor',
    'an admissible grade still gets the accusing badge');
  t.excludes(html, 'audit-only', 'nothing is withheld on a real finding');

  html = renderQualityBadges(group);
  t.contains(html, 'badge-rank-poor',
    'absent flags keep the historical accusing badge (fail-accusing)');

  html = renderQualityBadges({
    ...group,
    current_spectral_grade: 'suspect',
    current_spectral_accusation_admissible: false,
    current_spectral_accusation_withheld: 'codec_unresolved',
  });
  t.contains(html, 'codec unresolved', 'the unresolved world is named');
  t.excludes(html, 'native encoder behaviour',
    'an unresolved codec is never described as native encoder rolloff');
  t.excludes(html, 'audit-only',
    'the two withholding worlds are never conflated');
}

t.section('entrySpectralCell() withholds an audit-only candidate accusation');
{
  const entry = { spectral_grade: 'likely_transcode', spectral_bitrate: 128 };
  const text = formatEntryEvidence(entry).spectral;

  let html = entrySpectralCell({
    ...entry,
    spectral_accusation_admissible: false,
    spectral_accusation_withheld: 'audit_only_codec',
  }, text);
  t.contains(html, 'likely transcode', 'the measured grade stays visible');
  t.contains(html, 'audit-only', 'the withheld suffix is stated');
  t.contains(html, 'quality-tone-unknown', 'the neutral tone is used');
  t.excludes(html, 'quality-tone-poor', 'the accusing red is withheld');

  html = entrySpectralCell({
    ...entry,
    spectral_accusation_admissible: true,
    spectral_accusation_withheld: null,
  }, text);
  t.contains(html, 'quality-tone-poor',
    'an admissible candidate grade still accuses');
  t.excludes(html, 'audit-only', 'nothing is withheld on a real finding');

  html = entrySpectralCell(entry, text);
  t.contains(html, 'quality-tone-poor',
    'a pre-evidence candidate keeps the accusing chip (fail-accusing)');

  html = entrySpectralCell({
    spectral_grade: 'suspect',
    spectral_bitrate: 192,
    spectral_accusation_admissible: false,
    spectral_accusation_withheld: 'codec_unresolved',
  }, 'suspect · 192 kbps');
  t.contains(html, 'codec unresolved', 'the unresolved world is named');
  t.excludes(html, 'native encoder behaviour',
    'an unresolved codec is never described as native encoder rolloff');

  // A candidate with no grade at all keeps the pre-existing neutral cell.
  html = entrySpectralCell({}, '—');
  t.contains(html, 'quality-tone-unknown', 'a gradeless candidate is neutral');
  t.excludes(html, 'audit-only', 'a gradeless candidate withholds nothing');
}

t.section('an unobservable source is surfaced, never silently dropped');
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
  t.ok(entryPathUnavailable(unavailable),
    'the payload flag is the single source of the unavailable state');
  t.ok(!entryPathUnavailable({ download_log_id: 78, distance: 0.05 }),
    'an ordinary entry is not unavailable');
  t.ok(!isConvergeGreen(unavailable, 180),
    'an unobservable source is never converge-green, whatever its distance');
  t.ok(isConvergeGreen({ distance: 0.05 }, 180),
    'must still work: an observable close match is still green');

  const html = renderEntry(unavailable, 180, 42);
  t.contains(html, 'source unavailable', 'the card is badged unavailable');
  t.contains(html, 'NOT been confirmed missing',
    'the copy refuses to claim the folder is gone');
  t.contains(html, 'EACCES', 'the refusal reason reaches the operator');
  t.equal(countOccurrences(html, 'disabled'), 2,
    'both Force Import and Delete are disabled');

  const ordinary = renderEntry(
    { download_log_id: 78, soulseek_username: 'peer', distance: 0.05 }, 180, 42);
  t.excludes(ordinary, 'source unavailable',
    'must still work: an ordinary entry carries no unavailable badge');
  t.equal(countOccurrences(ordinary, 'disabled'), 0,
    'must still work: an ordinary entry keeps both actions enabled');
}

t.section('the detail card renders operator-facing scenario detail copy (#1122 item 2)');
{
  // #1077's sweep-anomaly copy and #1099's refusal classifications both
  // land in `detail` (web/routes/imports.py's per-entry dict) with no
  // surface before this fix -- the JS never read `e.detail` at all.
  const withDetail = {
    download_log_id: 91,
    soulseek_username: 'peer',
    distance: 0.05,
    scenario: 'high_distance',
    detail: 'could not verify the curated move source was fully consumed',
  };
  const html = renderEntry(withDetail, 180, 42);
  t.contains(html, 'could not verify the curated move source was fully consumed',
    'the detail copy reaches the operator');
  t.contains(html, 'p-detail-label">Detail<',
    'the detail row carries its own labeled field, distinct from scenario');

  const escaped = renderEntry({
    download_log_id: 92,
    soulseek_username: 'peer',
    distance: 0.05,
    detail: '<script>alert(1)</script>',
  }, 180, 42);
  t.excludes(escaped, '<script>alert(1)</script>',
    'detail copy is escaped, not injected raw');
  t.contains(escaped, esc('<script>alert(1)</script>'),
    'the escaped form of the detail text is present');

  // Most rows have no detail at all -- the card must not grow an empty
  // block for them (most rejections never populate this field).
  const withoutDetail = renderEntry({
    download_log_id: 93,
    soulseek_username: 'peer',
    distance: 0.05,
  }, 180, 42);
  t.excludes(withoutDetail, 'p-detail-label">Detail<',
    'must still work: an entry with no detail renders no Detail row at all');

  const nullDetail = renderEntry({
    download_log_id: 94,
    soulseek_username: 'peer',
    distance: 0.05,
    detail: null,
  }, 180, 42);
  t.excludes(nullDetail, 'p-detail-label">Detail<',
    'must still work: an explicit null detail also renders no Detail row');
}

t.section('a partial group delete asks for attention and re-renders');
{
  // Found by the disposable Rule D fixture (issue #1063): one folder
  // deleted, one unavailable. The old code kept a green "all good" toast
  // and surgically removed the deleted row, leaving the group strip
  // advertising "Delete All (2)" and "1 green" over the ONE unavailable
  // candidate that survived.
  const dom = installDom();
  const calls = [];
  stubGlobals({ confirm: () => true });
  stubGlobals({ fetch: async (url, options) => {
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
  } });
  const btn = { disabled: false, textContent: 'Delete All (2)', style: {} };
  await deleteWrongMatchGroup(42, btn);

  t.contains(dom.toast.textContent, 'Deleted 1 folder',
    'the toast still credits the folder that really went');
  t.contains(dom.toast.textContent, 'unavailable 1',
    'the toast surfaces the unavailable bucket by name');
  t.excludes(dom.toast.textContent, 'skipped',
    'an unavailable candidate is not ALSO reported as skipped');
  t.excludes(dom.toast.textContent, 'errors',
    'an unavailable candidate is not ALSO reported as an error — that was '
    + 'the double count issue #1086 item 3 fixes');
  t.contains(dom.toast.textContent, '1 left',
    'the toast says work remains');
  t.contains(dom.toast.className, 'error',
    'an incomplete group delete asks for attention, not a green all-clear');
  t.ok(calls.some(call => call.url === '/api/wrong-matches'),
    'a partial outcome re-renders from the server instead of leaving a '
    + 'stale group strip');
}

t.section('Delete All reflects actionable candidates, never a dead end (issue #1086 item 2)');
{
  installStorage();
  const dom = installDom();

  // A fully available group keeps today's plain label and stays enabled —
  // the common case must not regress just because unavailability exists.
  renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  t.contains(dom.wrongMatches.innerHTML, 'Delete All (3)',
    'a fully available group keeps the plain label');
  t.notMatch(dom.wrongMatches.innerHTML, /id="wm-delete-group-btn-42"[^>]*disabled/,
    'a fully available group stays enabled');

  // A partially unavailable group relabels with the actionable count and
  // stays enabled — a partial group is still the right action to take.
  const partial = JSON.parse(JSON.stringify(wrongMatchesData()));
  partial.groups[0].entries[0].path_unavailable = true;
  renderWrongMatches(partial, dom.wrongMatches);
  t.contains(dom.wrongMatches.innerHTML, 'Delete All (2 of 3)',
    'a partially unavailable group shows the actionable count');
  t.notMatch(dom.wrongMatches.innerHTML, /id="wm-delete-group-btn-42"[^>]*disabled/,
    'a partially unavailable group stays enabled');

  // A group with ZERO actionable candidates is a dead end today: the
  // server truthfully refuses (503, nothing destroyed) and the operator
  // gets an error toast instead of a control that told them up front.
  const dead = JSON.parse(JSON.stringify(wrongMatchesData()));
  for (const entry of dead.groups[0].entries) entry.path_unavailable = true;
  renderWrongMatches(dead, dom.wrongMatches);
  t.contains(dom.wrongMatches.innerHTML, 'Delete All (0 of 3)',
    'a fully unavailable group names zero actionable candidates');
  t.match(dom.wrongMatches.innerHTML, /id="wm-delete-group-btn-42"[^>]*disabled/,
    'a fully unavailable group disables Delete All instead of a dead-end 503');

  t.equal(deleteAllButtonLabel(3, 3), 'Delete All (3)',
    'a fully actionable group keeps the plain label');
  t.equal(deleteAllButtonLabel(2, 3), 'Delete All (2 of 3)',
    'a partially actionable group shows X of N');
  t.equal(deleteAllButtonLabel(0, 2), 'Delete All (0 of 2)',
    'zero actionable candidates still names the total');
  t.equal(
    actionableDeleteEntries({ entries: [
      { download_log_id: 1 },
      { download_log_id: 2, path_unavailable: true },
    ] }).length,
    1,
    'actionableDeleteEntries excludes unavailable candidates',
  );
}

t.section('deleteWrongMatchGroup() restores the actionable-aware label on every failure path (issue #1086 review blocker 2)');
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
  stubGlobals({ confirm: () => true });

  // Path A: a non-2xx but "summarised" response that is neither `status:
  // 'ok'` nor `remaining: 0` takes the partial-outcome restore branch.
  {
    const dom = installDom();
    renderWrongMatches(partiallyUnavailableGroup(), dom.wrongMatches);
    stubGlobals({ fetch: async (url) => {
      if (url === '/api/wrong-matches/delete-group') {
        return {
          ok: false, status: 503,
          json: async () => ({ status: 'partial', deleted: 1, remaining: 1 }),
        };
      }
      return { ok: true, json: async () => ({ groups: [] }) };
    } });
    const btn = { disabled: false, textContent: 'Delete All (2 of 3)', style: {} };
    await deleteWrongMatchGroup(42, btn);
    t.equal(btn.textContent, 'Delete All (2 of 3)',
      'the partial-outcome restore path keeps the actionable-aware label');
    t.equal(btn.disabled, false,
      'the partial-outcome restore path leaves a partially actionable group enabled');
  }

  // Path B: a response with no numeric `deleted` (not "summarised") takes
  // the plain-error restore branch.
  {
    const dom = installDom();
    renderWrongMatches(partiallyUnavailableGroup(), dom.wrongMatches);
    stubGlobals({ fetch: async () => ({
      ok: false, json: async () => ({ error: 'cleanup_lock_unavailable' }),
    }) });
    const btn = { disabled: false, textContent: 'Delete All (2 of 3)', style: {} };
    await deleteWrongMatchGroup(42, btn);
    t.equal(btn.textContent, 'Delete All (2 of 3)',
      'the unsummarised-error restore path keeps the actionable-aware label');
    t.equal(btn.disabled, false,
      'the unsummarised-error restore path leaves a partially actionable group enabled');
  }

  // Path C: the fetch itself throws — the exception restore branch.
  {
    const dom = installDom();
    renderWrongMatches(partiallyUnavailableGroup(), dom.wrongMatches);
    stubGlobals({ fetch: async () => { throw new Error('network down'); } });
    const btn = { disabled: false, textContent: 'Delete All (2 of 3)', style: {} };
    await deleteWrongMatchGroup(42, btn);
    t.equal(btn.textContent, 'Delete All (2 of 3)',
      'the fetch-exception restore path keeps the actionable-aware label');
    t.equal(btn.disabled, false,
      'the fetch-exception restore path leaves a partially actionable group enabled');
  }

  // Must still work: a group with ZERO actionable candidates stays
  // disabled after a failed request too, not just on the first render.
  {
    const dom = installDom();
    const dead = JSON.parse(JSON.stringify(wrongMatchesData()));
    for (const entry of dead.groups[0].entries) entry.path_unavailable = true;
    renderWrongMatches(dead, dom.wrongMatches);
    stubGlobals({ fetch: async () => { throw new Error('network down'); } });
    const btn = { disabled: false, textContent: 'Delete All (0 of 3)', style: {} };
    await deleteWrongMatchGroup(42, btn);
    t.equal(btn.disabled, true,
      'a fully unavailable group stays disabled after a failed request too');
  }
}

t.section('removeWrongMatchEntry() keeps the group Delete All button actionable-aware (issue #1086 review blocker 2)');
{
  // Unlike the restore paths above, this update runs on a SUCCESSFUL
  // single-candidate delete: the group button must still reflect the
  // remaining actionable count, not a bare `Delete All (${remaining})`.
  installStorage();
  const dom = installDom();
  const data = wrongMatchesData();
  data.groups[0].entries[1].path_unavailable = true; // logId 101 stays unavailable
  renderWrongMatches(data, dom.wrongMatches);

  const groupBtn = element({ textContent: 'Delete All (2 of 3)' });
  dom.elements['wm-delete-group-btn-42'] = groupBtn;
  dom.elements['wm-entry-card-100'] = element();

  // Remove the AVAILABLE candidate (id 100): 2 candidates remain, only one
  // (id 102) actionable.
  removeWrongMatchEntry(100);

  t.equal(groupBtn.textContent, 'Delete All (1 of 2)',
    'removing an available candidate updates the group button to the new '
    + 'actionable-of-total count, not a bare N');
  t.equal(groupBtn.disabled, false,
    'one actionable candidate remains, so the group button stays enabled');

  // Must still work: removing the LAST actionable candidate disables it.
  dom.elements['wm-entry-card-102'] = element();
  removeWrongMatchEntry(102);

  t.equal(groupBtn.textContent, 'Delete All (0 of 1)',
    'removing the last actionable candidate updates the count to zero');
  t.equal(groupBtn.disabled, true,
    'zero actionable candidates remain, so the group button disables — '
    + 'the item-2 dead end, reached through the per-entry delete path');
}

t.section('triageButtonPresentation() derives the toolbar shape from state + count (issue #1106)');
{
  const CASES = [
    ['running, nonzero count', 'running', 3, { cleanupDisabled: true, cleanupLabel: 'Cleaning...', stopDisabled: false, stopLabel: 'Stop' }],
    ['running, zero count', 'running', 0, { cleanupDisabled: true, cleanupLabel: 'Cleaning...', stopDisabled: false, stopLabel: 'Stop' }],
    ['completed, nonzero count', 'completed', 5, { cleanupDisabled: false, cleanupLabel: 'Cleanup Wrong Matches (5)', stopDisabled: true, stopLabel: 'Stop' }],
    ['completed, zero count', 'completed', 0, { cleanupDisabled: true, cleanupLabel: 'Cleanup Wrong Matches (0)', stopDisabled: true, stopLabel: 'Stop' }],
    ['cancelled, nonzero count', 'cancelled', 2, { cleanupDisabled: false, cleanupLabel: 'Cleanup Wrong Matches (2)', stopDisabled: true, stopLabel: 'Stop' }],
    ['failed, nonzero count', 'failed', 4, { cleanupDisabled: false, cleanupLabel: 'Cleanup Wrong Matches (4)', stopDisabled: true, stopLabel: 'Stop' }],
    ['idle, nonzero count', 'idle', 1, { cleanupDisabled: false, cleanupLabel: 'Cleanup Wrong Matches (1)', stopDisabled: true, stopLabel: 'Stop' }],
    ['unknown (status fetch + retry both failed), nonzero count', 'unknown', 3, { cleanupDisabled: true, cleanupLabel: 'Cleanup Wrong Matches (status unknown)', stopDisabled: false, stopLabel: 'Stop' }],
    ['unknown, zero count', 'unknown', 0, { cleanupDisabled: true, cleanupLabel: 'Cleanup Wrong Matches (status unknown)', stopDisabled: false, stopLabel: 'Stop' }],
  ];
  for (const [desc, state, count, expected] of CASES) {
    t.deepEqual(triageButtonPresentation(state, count), expected, desc);
  }
}

t.section('refreshWrongMatches() discovers an already-running sweep and enables Stop without a confirm dialog (#1106)');
{
  installStorage();
  const dom = installDom();
  renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  let confirmCalls = 0;
  stubGlobals({ confirm: () => { confirmCalls += 1; return true; } });
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });
  let statusCalls = 0;
  stubGlobals({ fetch: async (url) => {
    if (url === '/api/wrong-matches') {
      return { ok: true, status: 200, json: async () => wrongMatchesData() };
    }
    if (url === '/api/wrong-matches/triage/status') {
      statusCalls += 1;
      if (statusCalls === 1) {
        // Discovered on the render-time derive: a sweep is already
        // running — started from the CLI, another tab, or a previous
        // page load this tab never saw.
        return {
          ok: true, status: 200,
          json: async () => ({
            state: 'running', started_at: '2026-08-12T00:00:00+00:00',
            finished_at: null, error: null, summary: null,
          }),
        };
      }
      // Terminate the attached poll quickly so it does not leak into a
      // later test's fetch mock.
      return {
        ok: true, status: 200,
        json: async () => ({
          state: 'failed', started_at: '2026-08-12T00:00:00+00:00',
          finished_at: '2026-08-12T00:01:00+00:00',
          error: 'generated test termination', summary: null,
        }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  } });
  const refreshBtn = { disabled: false, textContent: 'Refresh' };
  await refreshWrongMatches(refreshBtn);
  t.equal(dom.stopBtn.disabled, false, 'discovering a running sweep on refresh enables Stop');
  t.equal(dom.cleanupBtn.disabled, true, 'discovering a running sweep on refresh disables Cleanup');
  t.equal(confirmCalls, 0, 'no confirm dialog is shown for a sweep this tab did not start');
  await flushMicrotasks();
  globals.restore();
}

t.section('loadWrongMatches() discovers an already-running sweep on initial load/reload and enables Stop (#1106)');
{
  installStorage();
  const dom = installDom();
  // loadWrongMatches() short-circuits on its own module-scoped `_loaded`
  // cache, which an earlier test in this file may have already set.
  invalidateWrongMatches();
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });
  // Issue #1106 F6: loadWrongMatches() now derives TWICE on a fresh
  // load — once BEFORE the `_loaded` short-circuit (so a tab switch
  // re-derives without a full re-fetch) and once after rendering. Both
  // see the SAME `started_at`, so the follower each one tries to attach
  // dedupes to exactly one active poller (issue #1106 F5) regardless of
  // which wins the race. Stay 'running' for the ENTIRE loadWrongMatches()
  // call (a boolean flip, not a call-count threshold — the exact
  // interleaving of the two derive calls against the dangling follower's
  // own poll loop is not something a test should have to predict), then
  // terminate deliberately afterward so the winning follower settles
  // before this test block ends.
  let forceTerminal = false;
  stubGlobals({ fetch: async (url) => {
    if (url === '/api/wrong-matches') {
      return { ok: true, status: 200, json: async () => wrongMatchesData() };
    }
    if (url === '/api/wrong-matches/triage/status') {
      if (!forceTerminal) {
        return {
          ok: true, status: 200,
          json: async () => ({
            state: 'running', started_at: '2026-08-12T00:00:00+00:00',
            finished_at: null, error: null, summary: null,
          }),
        };
      }
      return {
        ok: true, status: 200,
        json: async () => ({
          state: 'failed', started_at: '2026-08-12T00:00:00+00:00',
          finished_at: '2026-08-12T00:01:00+00:00',
          error: 'generated test termination', summary: null,
        }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  } });
  // loadWrongMatches() caches on a module-scoped `_loaded` flag, so this
  // must stay the only call to it in this file — a second call would
  // silently no-op against the flag this one sets.
  await loadWrongMatches();
  t.equal(dom.stopBtn.disabled, false, 'a reload that discovers a running sweep enables Stop');
  t.equal(dom.cleanupBtn.disabled, true, 'a reload that discovers a running sweep disables Cleanup');
  forceTerminal = true;
  await flushMicrotasks(100);
  globals.restore();
}

t.section('a NEW sweep discovered while an OLDER one\'s terminal handling is still unwinding gets its own follower, not stranded (#1106 F5)');
{
  installStorage();
  const dom = installDom();
  renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });

  const STARTED_A = '2026-08-12T00:00:00+00:00';
  const STARTED_B = '2026-08-12T01:00:00+00:00';
  // A boolean state machine, not a call-count threshold: the exact
  // number of fetches A's poll loop makes before it is told to
  // terminate is not something a test should have to predict. Once
  // told, the FIRST subsequent status call answers A terminal; EVERY
  // call after that answers B running — covering both A's own poll
  // loop exiting AND the render-time derive (inside A's terminal
  // refresh) that discovers B, without needing to distinguish which
  // logical caller is asking.
  let tellATeminate = false;
  let aTerminalConsumed = false;
  let tellBTerminate = false;
  stubGlobals({ fetch: async (url) => {
    if (url === '/api/wrong-matches') {
      return { ok: true, status: 200, json: async () => wrongMatchesData() };
    }
    if (url !== '/api/wrong-matches/triage/status') {
      throw new Error(`unexpected fetch: ${url}`);
    }
    if (!tellATeminate) {
      return {
        ok: true, status: 200,
        json: async () => ({
          state: 'running', started_at: STARTED_A,
          finished_at: null, error: null, summary: null,
        }),
      };
    }
    if (!aTerminalConsumed) {
      aTerminalConsumed = true;
      return {
        ok: true, status: 200,
        json: async () => ({
          state: 'completed', started_at: STARTED_A,
          finished_at: '2026-08-12T00:05:00+00:00', error: null,
          summary: { processed: 3, deleted: 2 },
        }),
      };
    }
    if (!tellBTerminate) {
      return {
        ok: true, status: 200,
        json: async () => ({
          state: 'running', started_at: STARTED_B,
          finished_at: null, error: null, summary: null,
        }),
      };
    }
    return {
      ok: true, status: 200,
      json: async () => ({
        state: 'failed', started_at: STARTED_B,
        finished_at: '2026-08-12T01:05:00+00:00',
        error: 'sweep B blew up', summary: null,
      }),
    };
  } });

  await refreshWrongMatches();
  t.equal(dom.stopBtn.disabled, false, 'discovering sweep A running enables Stop');

  // Let A's follower reach its poll loop, then tell it to terminate —
  // its OWN terminal handling (toast, refresh, re-derive) is what
  // discovers sweep B, entirely inside the dangling follower chain the
  // test above never has to await directly.
  await flushMicrotasks(50);
  tellATeminate = true;
  await flushMicrotasks(50);
  t.equal(dom.stopBtn.disabled, false,
    'sweep B, discovered while A\'s terminal handling was still unwinding, still shows Stop enabled — not stranded');

  // Let B's follower reach its poll loop, then terminate it too.
  tellBTerminate = true;
  await flushMicrotasks(50);
  t.equal(dom.stopBtn.disabled, true, 'sweep B reaching a terminal state disables Stop again');
  t.contains(dom.toast.textContent, 'sweep B blew up',
    'sweep B\'s own terminal outcome reached the toast — proving it was actually followed to completion, not silently dropped');
  globals.restore();
}

t.section('a status fetch that fails once retries after ~3s and recovers (#1106 F4)');
{
  installStorage();
  const dom = installDom();
  renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });
  let statusCalls = 0;
  // Stays 'running' until explicitly told otherwise — a boolean flag,
  // not a call-count threshold: once the retry succeeds, the follower
  // it kicks off (fire-and-forget) keeps polling and would otherwise
  // race an exact-count assertion against however many extra loop
  // iterations flushMicrotasks happens to unwind.
  let stayRunning = true;
  stubGlobals({ fetch: async (url) => {
    if (url === '/api/wrong-matches') {
      return { ok: true, status: 200, json: async () => wrongMatchesData() };
    }
    if (url === '/api/wrong-matches/triage/status') {
      statusCalls += 1;
      if (statusCalls === 1) {
        throw new Error('network down');
      }
      if (stayRunning) {
        return {
          ok: true, status: 200,
          json: async () => ({
            state: 'running', started_at: '2026-08-12T00:00:00+00:00',
            finished_at: null, error: null, summary: null,
          }),
        };
      }
      return {
        ok: true, status: 200,
        json: async () => ({
          state: 'failed', started_at: '2026-08-12T00:00:00+00:00',
          finished_at: '2026-08-12T00:01:00+00:00',
          error: 'generated test termination', summary: null,
        }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  } });

  await refreshWrongMatches();
  t.equal(dom.stopBtn.disabled, true,
    'the failed first attempt leaves the safe default painted, not a stale enabled state');
  await flushMicrotasks(30);
  t.ok(statusCalls >= 2, 'the bounded retry fired at least once');
  t.equal(dom.stopBtn.disabled, false, 'the retry succeeded and discovered the running sweep');
  t.equal(dom.cleanupBtn.disabled, true, 'Cleanup reflects the recovered running state');
  stayRunning = false;
  await flushMicrotasks(50);
  globals.restore();
}

t.section('a status fetch that fails twice (initial + retry) paints the conservative unknown shape (#1106 F4)');
{
  installStorage();
  const dom = installDom();
  renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });
  stubGlobals({ fetch: async (url) => {
    if (url === '/api/wrong-matches') {
      return { ok: true, status: 200, json: async () => wrongMatchesData() };
    }
    if (url === '/api/wrong-matches/triage/status') {
      throw new Error('network down');
    }
    throw new Error(`unexpected fetch: ${url}`);
  } });

  await refreshWrongMatches();
  await flushMicrotasks(50);
  t.equal(dom.cleanupBtn.disabled, true,
    'Cleanup disables when the status genuinely cannot be determined — cannot verify it is safe to start another sweep');
  t.equal(dom.stopBtn.disabled, false,
    'Stop enables — harmless now that an unarmed cancel with nothing actually running is a pure no-op (#1106 F3)');
  globals.restore();
}

t.section('claimTriageFollow()/releaseTriageFollow() refuse a claim for an OLDER started_at, never steal the slot (#1106 N3)');
{
  const EARLIER = '2026-08-12T00:00:00+00:00';
  const LATER = '2026-08-12T02:00:00+00:00';
  const EVEN_LATER = '2026-08-12T03:00:00+00:00';

  t.equal(claimTriageFollow(LATER), true,
    'the first claim for a value succeeds (nothing held yet)');
  t.equal(claimTriageFollow(EARLIER), false,
    'a claim for a value OLDER than the held one is refused — it must not steal the slot');
  t.equal(claimTriageFollow(LATER), false,
    'a claim for the ALREADY-held value is refused (already claimed)');
  t.equal(claimTriageFollow(EVEN_LATER), true,
    'a claim for a genuinely NEWER value is allowed to take over');
  releaseTriageFollow(LATER);
  t.equal(claimTriageFollow(EVEN_LATER), false,
    'releasing a value that is NOT currently held is a no-op — the real holder keeps the slot');
  releaseTriageFollow(EVEN_LATER);
  t.equal(claimTriageFollow(EARLIER), true,
    'the slot is free again once the value actually held is released');
  // Leave the module-level slot clean for later tests in this file.
  releaseTriageFollow(EARLIER);
}

t.section('a follower skips terminal handling when its poll result names a DIFFERENT sweep than the one it claimed (#1106 N3)');
{
  installStorage();
  const dom = installDom();
  renderWrongMatches(wrongMatchesData(), dom.wrongMatches);
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });

  const CLAIMED = '2026-08-12T00:00:00+00:00';
  const DIFFERENT = '2026-08-12T05:00:00+00:00';
  let statusCalls = 0;
  stubGlobals({ fetch: async (url) => {
    if (url === '/api/wrong-matches') {
      return { ok: true, status: 200, json: async () => wrongMatchesData() };
    }
    if (url === '/api/wrong-matches/triage/status') {
      statusCalls += 1;
      if (statusCalls === 1) {
        return {
          ok: true, status: 200,
          json: async () => ({
            state: 'running', started_at: CLAIMED,
            finished_at: null, error: null, summary: null,
          }),
        };
      }
      // The poll's own next check reports a DIFFERENT sweep's terminal
      // state entirely -- the world moved on to a newer sweep while
      // this follower's poll was in flight. It must not misattribute
      // this result to the sweep it originally claimed.
      return {
        ok: true, status: 200,
        json: async () => ({
          state: 'failed', started_at: DIFFERENT,
          finished_at: '2026-08-12T05:05:00+00:00',
          error: 'the DIFFERENT sweep genuinely failed', summary: null,
        }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  } });

  await refreshWrongMatches();
  await flushMicrotasks(50);
  t.excludes(
    dom.toast.textContent,
    'the DIFFERENT sweep genuinely failed',
    'a poll result naming a different started_at is never toasted as this follower\'s own outcome',
  );
  globals.restore();
}

t.section('a literal-null status body degrades instead of aborting loadWrongMatches() before its own queue fetch (#1106 N5)');
{
  installStorage();
  const dom = installDom();
  // loadWrongMatches() short-circuits on its own module-scoped
  // `_loaded` cache, which an earlier test may have already set.
  invalidateWrongMatches();
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });
  let queueFetched = false;
  stubGlobals({ fetch: async (url) => {
    if (url === '/api/wrong-matches') {
      queueFetched = true;
      return { ok: true, status: 200, json: async () => wrongMatchesData() };
    }
    if (url === '/api/wrong-matches/triage/status') {
      // A malformed-but-successfully-parsed response: literal JSON
      // null, not a fetch/parse failure -- distinct from `undefined`.
      return { ok: true, status: 200, json: async () => null };
    }
    throw new Error(`unexpected fetch: ${url}`);
  } });

  await loadWrongMatches();

  t.ok(queueFetched,
    'a null status body does not stop loadWrongMatches() from reaching its own queue fetch (issue #1106 N5)');
  t.contains(dom.wrongMatches.innerHTML, 'Scott Walker',
    'the pane actually rendered the fetched queue data, proving the try block ran to completion');
  await flushMicrotasks(60);
  globals.restore();
}

t.section('retryTriageStatusOnce() is single-flight — a second concurrent call is a no-op (#1106 N7b)');
{
  installStorage();
  installDom();
  const globals = stubGlobals({ setTimeout: (fn) => { fn(); return 0; } });
  let statusCalls = 0;
  stubGlobals({ fetch: async (url) => {
    if (url === '/api/wrong-matches/triage/status') {
      statusCalls += 1;
      return {
        ok: true, status: 200,
        json: async () => ({
          state: 'idle', started_at: null, finished_at: null,
          error: null, summary: null,
        }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  } });

  // Two concurrent retry attempts -- without the single-flight guard,
  // both would independently sleep and fetch, doubling the request
  // count for no benefit.
  const first = retryTriageStatusOnce();
  const second = retryTriageStatusOnce();
  await Promise.all([first, second]);

  t.equal(statusCalls, 1,
    'only ONE retry actually performs its status fetch -- the concurrent second call is a no-op');
  globals.restore();
}

// --- WE-3: loadWrongMatches() is the scroll-restore completion boundary
//
// Mirrors the pipeline.js/recents.js pins: search_plan.js's
// closeSearchPlanDetail stashes the origin scroll position for a
// manual-tab origin and leaves it un-consumed; loadWrongMatches() is
// the real destination and consumes it only once its own fetch-driven
// render is done -- including when that render is the `_loaded` cache
// short-circuit, which still counts as "this tab's render is done".

t.section('loadWrongMatches() consumes a pending scroll restore only after its own render completes');
{
  invalidateWrongMatches();
  const content = element();
  const scrollCalls = [];
  let resolveFetch;
  const fetchGate = new Promise((resolve) => { resolveFetch = resolve; });
  const globals = stubGlobals({
    // No `wm-bulk-triage-btn` / `wm-bulk-triage-stop-btn` in this stub,
    // so `_deriveTriageButtonState` short-circuits with no fetch of its
    // own -- the ONLY fetch this render makes is the queue fetch below.
    document: domStub({ 'wrong-matches-content': content }),
    window: /** @type {any} */ ({
      scrollTo(_x, y) { scrollCalls.push(y); },
      showTab() {},
    }),
    localStorage: { getItem() { return null; } },
    fetch: async (url) => {
      if (url !== '/api/wrong-matches') throw new Error(`unexpected fetch: ${url}`);
      await fetchGate;
      return { ok: true, status: 200, json: async () => ({ groups: [] }) };
    },
  });
  state.searchPlanDetailContext = {
    requestId: 7, originTab: 'manual', originScrollY: 512, originSubView: null,
  };
  closeSearchPlanDetail();
  t.equal(scrollCalls.length, 0,
    'closeSearchPlanDetail does not restore scroll itself for a manual-tab origin');

  const rendered = loadWrongMatches();
  await Promise.resolve();
  await Promise.resolve();
  t.equal(scrollCalls.length, 0,
    'scrollTo is not called while the wrong-matches queue fetch is still pending');

  resolveFetch();
  await rendered;
  t.equal(scrollCalls.length, 1,
    'scrollTo is called exactly once after loadWrongMatches finishes rendering');
  t.equal(scrollCalls[0], 512, 'restores the exact stashed scroll position');
  globals.restore();
}

t.section('loadWrongMatches() consumes a pending scroll restore even on the cached _loaded short-circuit');
{
  // Prime the module-scoped `_loaded` cache with a real fetch first.
  const primeContent = element();
  const primeGlobals = stubGlobals({
    document: domStub({ 'wrong-matches-content': primeContent }),
    window: /** @type {any} */ ({ scrollTo() {}, showTab() {} }),
    localStorage: { getItem() { return null; } },
    fetch: async () => ({ ok: true, status: 200, json: async () => ({ groups: [] }) }),
  });
  await loadWrongMatches();
  primeGlobals.restore();

  // Now a second call short-circuits on `_loaded` -- no fetch at all --
  // and must still consume the stash.
  const scrollCalls = [];
  const globals = stubGlobals({
    document: domStub({ 'wrong-matches-content': primeContent }),
    window: /** @type {any} */ ({
      scrollTo(_x, y) { scrollCalls.push(y); },
      showTab() {},
    }),
    fetch: async (url) => { throw new Error(`unexpected fetch on cached load: ${url}`); },
  });
  state.searchPlanDetailContext = {
    requestId: 8, originTab: 'manual', originScrollY: 88, originSubView: null,
  };
  closeSearchPlanDetail();
  await loadWrongMatches();
  t.equal(scrollCalls.length, 1,
    'the cached _loaded short-circuit still consumes the pending restore');
  t.equal(scrollCalls[0], 88, 'restores the exact stashed scroll position on the cached path');
  globals.restore();
}

t.done();
