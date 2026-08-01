/**
 * Unit tests for web/js/long_tail_console.js's console-state consolidation
 * (#481 item 1) — the pure open/close/prune/canStart/settle transition
 * helpers over the single `Map<id, ConsoleState>` that replaced eight
 * parallel module-scoped structures (a token Map, five in-flight guard
 * Sets, a YouTube-result cache Map, and `state.longTail.open`).
 *
 * Split out of web/js/long_tail.js by #522 along with the console module
 * itself.
 *
 * Every helper takes the map explicitly, so these tests build their own
 * fresh `Map` per scenario rather than touching the module's real
 * `consoleStates` singleton (exercised separately, as a DOM-free no-op
 * check, in tests/test_js_util.mjs).
 *
 * Run with: node tests/test_js_long_tail_console.mjs
 */

import {
  __test__,
  longTailDeleteRequest,
  longTailSetImported,
  longTailSetIntent,
} from '../web/js/long_tail_console.js';
import { state } from '../web/js/state.js';

const {
  consoleOpen,
  consoleClose,
  consolePrune,
  consoleCanStart,
  consoleSettle,
  consoleClearGuards,
  consoleToken,
  consoleIsStale,
  consoleIsOpen,
  consoleYoutubeResult,
  consoleSetYoutubeResult,
  consoleOpenIds,
  renderSpectralFragment,
} = __test__;

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

console.log('long-tail current quality uses the shared ordered spectral palette');
for (const [grade, tone] of [
  ['likely_transcode', 'poor'],
  ['suspect', 'acceptable'],
  ['marginal', 'good'],
  ['genuine', 'lossless'],
]) {
  const html = renderSpectralFragment({
    current_spectral_grade: grade,
    current_spectral_bitrate: 128,
  });
  assert(html.includes(`quality-tone-${tone}`), `${grade} uses shared ${tone} tone`);
  assert(html.includes(grade.replaceAll('_', ' ')), `${grade} is humanized`);
  if (grade.includes('_')) assert(!html.includes(grade), `${grade} raw token stays hidden`);
}

// --- issue #829 Phase 5 PR4/N3: the worklist chip's audit-only flags ---
console.log('long-tail worklist chip never accuses an audit-only codec');
{
  const row = {
    current_spectral_grade: 'likely_transcode',
    current_spectral_bitrate: 128,
  };

  let html = renderSpectralFragment({
    ...row,
    current_spectral_accusation_admissible: false,
    current_spectral_accusation_withheld: 'audit_only_codec',
  });
  assert(html.includes('likely transcode'), 'the measured grade stays visible');
  assert(html.includes('audit-only'), 'the withheld suffix is stated');
  assert(html.includes('native encoder behaviour'), 'the hover explains why');
  assert(html.includes('quality-tone-unknown'), 'the neutral tone is used');
  assert(!html.includes('quality-tone-poor'), 'the accusing red is withheld');

  html = renderSpectralFragment({
    ...row,
    current_spectral_accusation_admissible: true,
    current_spectral_accusation_withheld: null,
  });
  assert(html.includes('quality-tone-poor'),
    'an admissible grade still accuses');
  assert(!html.includes('audit-only'), 'nothing is withheld on a real finding');

  html = renderSpectralFragment(row);
  assert(html.includes('quality-tone-poor'),
    'a row with no linked evidence keeps the accusing chip (fail-accusing)');

  html = renderSpectralFragment({
    current_spectral_grade: 'suspect',
    current_spectral_bitrate: 192,
    current_spectral_accusation_admissible: false,
    current_spectral_accusation_withheld: 'codec_unresolved',
  });
  assert(html.includes('codec unresolved'), 'the unresolved world is named');
  assert(!html.includes('native encoder behaviour'),
    'an unresolved codec is never described as native encoder rolloff');
  assert(!html.includes('audit-only'),
    'the two withholding worlds are never conflated');

  assertEqual(renderSpectralFragment({
    current_spectral_grade: null,
    current_spectral_accusation_admissible: false,
    current_spectral_accusation_withheld: 'audit_only_codec',
  }), '', 'no grade still renders nothing at all');
}

// --- consoleOpen / consoleClose / consoleIsOpen / consoleToken ---
console.log('consoleOpen / consoleClose / consoleIsOpen / consoleToken');
{
  const map = new Map();
  assertEqual(consoleIsOpen(map, 1), false, 'an untracked id is not open');
  assertEqual(consoleToken(map, 1), 0, 'an untracked id has token 0');

  const t1 = consoleOpen(map, 1);
  assertEqual(t1, 1, 'consoleOpen returns the new (bumped) token');
  assertEqual(consoleIsOpen(map, 1), true, 'consoleOpen marks the row open');
  assertEqual(consoleToken(map, 1), 1, 'consoleToken reflects the just-opened token');

  const t2 = consoleOpen(map, 1);
  assertEqual(t2, 2, 're-opening bumps the token again');
  assertEqual(consoleIsOpen(map, 1), true, 're-opening keeps the row open');

  const t3 = consoleClose(map, 1);
  assertEqual(t3, 3, 'consoleClose also bumps the token');
  assertEqual(consoleIsOpen(map, 1), false, 'consoleClose marks the row closed');

  // Closing a row that was never opened still creates a well-formed entry
  // (mirrors the old `consoleTokens.set(id, (consoleTokens.get(id)||0)+1)`
  // behaviour — collapsing a console the toggle handler never had to
  // pre-open must not throw).
  const freshClose = consoleClose(map, 99);
  assertEqual(freshClose, 1, 'closing a never-opened id still bumps from 0');
  assertEqual(consoleIsOpen(map, 99), false, 'a closed-only id reads as not open');

  // Two rows are independent.
  const map2 = new Map();
  consoleOpen(map2, 5);
  assertEqual(consoleToken(map2, 6), 0, 'a different id is unaffected by another id\'s open');
}

// --- consolePrune: the ONE prune function for BOTH call sites ---
console.log('consolePrune');
{
  // Site shape 1: loadLongTail's fresh-cohort intersect — keep = cohort ids.
  const map = new Map();
  consoleOpen(map, 1);
  consoleOpen(map, 2);
  consoleOpen(map, 3);
  consoleSetYoutubeResult(map, 2, { outcome: 'ok' });
  consoleCanStart(map, 3, 'resolve');

  const cohortIds = new Set([1, 3]);
  consolePrune(map, (id) => cohortIds.has(id));
  assertEqual(map.has(1), true, 'consolePrune keeps an id present in the cohort');
  assertEqual(map.has(2), false, 'consolePrune drops an id absent from the cohort');
  assertEqual(map.has(3), true, 'consolePrune keeps another cohort id');
  // Dropping id 2 removed its ENTIRE state atomically — token, open flag,
  // and (had it had one) its cached YouTube result — in one call. This is
  // the bug class #481 item 1 kills: no second structure to forget.
  assertEqual(consoleYoutubeResult(map, 2), null,
    'consolePrune drops the cached YouTube result along with everything else for a pruned id');
  // The kept id's OTHER state (in-flight guard) survived untouched.
  assertEqual(consoleCanStart(map, 3, 'resolve'), false,
    'consolePrune leaves a kept id\'s in-flight guard untouched');

  // Site shape 2: removeRowFromCohort's single-row drop — keep = "not this id".
  const map3 = new Map();
  consoleOpen(map3, 10);
  consoleOpen(map3, 11);
  consolePrune(map3, (id) => id !== 10);
  assertEqual(map3.has(10), false, 'consolePrune (single-row shape) drops exactly the removed id');
  assertEqual(map3.has(11), true, 'consolePrune (single-row shape) leaves every other id untouched');
}

// --- consoleCanStart / consoleSettle: the double-fire guard pair ---
console.log('consoleCanStart / consoleSettle');
{
  const map = new Map();
  assertEqual(consoleCanStart(map, 5, 'resolve'), true,
    'consoleCanStart: nothing outstanding → may start (and marks it started)');
  assertEqual(consoleCanStart(map, 5, 'resolve'), false,
    'consoleCanStart: an outstanding call for the same id+action → suppressed');
  // A DIFFERENT action on the same id is independent — this is the whole
  // point of naming actions instead of five separate Sets colliding on id.
  assertEqual(consoleCanStart(map, 5, 'submit'), true,
    'consoleCanStart: a different action on the same id is independent');
  // A different id is independent too.
  assertEqual(consoleCanStart(map, 6, 'resolve'), true,
    'consoleCanStart: a different id is independent');

  consoleSettle(map, 5, 'resolve');
  assertEqual(consoleCanStart(map, 5, 'resolve'), true,
    'consoleSettle clears the guard so a later call may start again');
  // Settling one action does not clear a sibling action's guard.
  assertEqual(consoleCanStart(map, 5, 'submit'), false,
    'consoleSettle only clears the named action, not every action for the id');

  // Settling an id with no tracked state at all is a safe no-op (the row
  // may have been pruned while its fetch was outstanding).
  consoleSettle(map, 12345, 'resolve');
  assertEqual(map.has(12345), false, 'consoleSettle does not fabricate state for an untracked id');
}

// --- consoleClearGuards: LT-R1, sweeps every in-flight flag for one id ---
console.log('consoleClearGuards');
{
  const map = new Map();
  consoleOpen(map, 7);
  consoleCanStart(map, 7, 'resolve');
  consoleCanStart(map, 7, 'submit');
  consoleCanStart(map, 7, 'intent');
  consoleSetYoutubeResult(map, 7, { outcome: 'ok' });

  consoleClearGuards(map, 7);
  assertEqual(consoleCanStart(map, 7, 'resolve'), true,
    'consoleClearGuards releases the resolve guard');
  assertEqual(consoleCanStart(map, 7, 'submit'), true,
    'consoleClearGuards releases the submit guard');
  assertEqual(consoleCanStart(map, 7, 'intent'), true,
    'consoleClearGuards releases the intent guard');
  // LT-R1 clears guards WITHOUT touching open/youtubeResult — only the
  // explicit toggle path calls this, never the #398 restore path.
  assertEqual(consoleIsOpen(map, 7), true,
    'consoleClearGuards does not touch the open flag');
  assertEqual(consoleYoutubeResult(map, 7).outcome, 'ok',
    'consoleClearGuards does not touch the cached YouTube result');

  // A never-tracked id is a safe no-op.
  consoleClearGuards(map, 999);
  assertEqual(map.has(999), false, 'consoleClearGuards does not fabricate state for an untracked id');
}

// --- consoleYoutubeResult / consoleSetYoutubeResult: the #398 cache ---
console.log('consoleYoutubeResult / consoleSetYoutubeResult');
{
  const map = new Map();
  assertEqual(consoleYoutubeResult(map, 1), null, 'an untracked id has no cached result');
  consoleSetYoutubeResult(map, 1, { outcome: 'ok', youtube_releases: [] });
  assertEqual(consoleYoutubeResult(map, 1).outcome, 'ok', 'caches the settled resolver result');
  // #398 fidelity: closing (collapsing) the console must NOT drop the cache
  // — a later reopen restores the matrix instead of resetting to never_run.
  consoleOpen(map, 1);
  consoleClose(map, 1);
  assertEqual(consoleYoutubeResult(map, 1).outcome, 'ok',
    'consoleClose preserves the cached YouTube result (#398)');
}

// --- consoleOpenIds: drives restoreLongTailConsoles ---
console.log('consoleOpenIds');
{
  const map = new Map();
  consoleOpen(map, 1);
  consoleOpen(map, 2);
  consoleOpen(map, 3);
  consoleClose(map, 2);
  assertEqual(JSON.stringify(consoleOpenIds(map)), JSON.stringify([1, 3]),
    'consoleOpenIds lists only ids currently marked open');
  assertEqual(consoleOpenIds(new Map()).length, 0, 'consoleOpenIds on an empty map is empty');
}

// --- The two deliberate token semantics, made explicit (#481 item 1) ---
console.log('consoleToken (resolver-settle) vs consoleIsStale (panel-paint)');
{
  const map = new Map();
  const capturedToken = consoleOpen(map, 1);  // e.g. a panel fetch fires here.
  assertEqual(consoleIsStale(map, 1, capturedToken), false,
    'consoleIsStale: not stale immediately after the captured token was issued');
  assertEqual(consoleToken(map, 1), capturedToken,
    'consoleToken: matches the captured token before anything else happens');

  // The operator collapses and reopens the console (or a #398 restore
  // re-creates it) while a fetch stamped with `capturedToken` is still
  // outstanding.
  consoleClose(map, 1);
  consoleOpen(map, 1);

  // Panel-paint semantic: a fetch stamped with the OLD captured token must
  // discard — the console it was fired against no longer exists.
  assertEqual(consoleIsStale(map, 1, capturedToken), true,
    'consoleIsStale: a fetch stamped with the captured token is stale after a reopen');

  // Resolver-settle semantic: the YouTube resolver instead re-reads the
  // CURRENT token at paint time (not the captured one) — because its panel
  // container is per-row, so whatever console exists now is the only one it
  // can paint into, including one re-created by a restore mid-flight.
  const liveToken = consoleToken(map, 1);
  assert(liveToken !== capturedToken,
    'consoleToken: the current token has moved on from what was captured at fire time');
  assertEqual(consoleIsStale(map, 1, liveToken), false,
    'consoleIsStale: re-reading the CURRENT token (the resolver-settle pattern) is never stale');
}

console.log('long-tail mutation adapters map typed processing conflicts to one row refresh');
for (const scenario of [
  {
    name: 'intent',
    requestId: 911,
    url: '/api/pipeline/set-intent',
    invoke: (id, control) => longTailSetIntent(id, control),
  },
  {
    name: 'imported status',
    requestId: 912,
    url: '/api/pipeline/update',
    invoke: (id, control) => longTailSetImported(id, control),
  },
  {
    name: 'delete',
    requestId: 913,
    url: '/api/pipeline/delete',
    invoke: (id, control) => longTailDeleteRequest(id, control),
  },
]) {
  const oldConfirm = globalThis.confirm;
  const oldDocument = globalThis.document;
  const oldFetch = globalThis.fetch;
  const oldWindow = globalThis.window;
  const attributes = new Map([['data-pipeline-request-id', String(scenario.requestId)]]);
  const inserted = [];
  const control = {
    dataset: {},
    disabled: false,
    textContent: scenario.name,
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
  state.longTail.rows = [{
    id: scenario.requestId,
    target_format: null,
    source: 'request',
  }];
  globalThis.confirm = () => true;
  globalThis.document = {
    activeElement: control,
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
    querySelectorAll() { return [control]; },
  };
  globalThis.window = { scrollX: 3, scrollY: 7, scrollTo() {} };
  const calls = [];
  globalThis.fetch = async (url) => {
    calls.push(String(url));
    if (url === scenario.url) {
      return {
        status: 409,
        async json() {
          return {
            error: 'processing_locked',
            request_id: scenario.requestId,
            processing_owner: {
              job_id: scenario.requestId + 1000,
              status: 'queued',
              preview_status: 'waiting',
            },
          };
        },
      };
    }
    if (url === `/api/pipeline/${scenario.requestId}`) {
      return {
        ok: true,
        async json() {
          return {
            request: {
              id: scenario.requestId,
              status: 'processing',
              mb_release_id: `long-tail-${scenario.requestId}`,
              processing_owner: {
                job_id: scenario.requestId + 1000,
                status: 'queued',
                preview_status: 'running',
              },
            },
          };
        },
      };
    }
    throw new Error(`unexpected fetch ${url}`);
  };
  await scenario.invoke(scenario.requestId, control);
  assertEqual(
    calls.join(','),
    `${scenario.url},/api/pipeline/${scenario.requestId}`,
    `${scenario.name} conflict refetches only its request`,
  );
  assertEqual(
    attributes.get('aria-disabled'),
    'true',
    `${scenario.name} control becomes aria-disabled`,
  );
  assertEqual(control.textContent, 'previewing', `${scenario.name} renders fresh owner status`);
  assertEqual(
    state.longTail.rows.some(row => row.id === scenario.requestId),
    false,
    `${scenario.name} removes non-wanted row from cached cohort`,
  );
  globalThis.confirm = oldConfirm;
  globalThis.document = oldDocument;
  globalThis.fetch = oldFetch;
  globalThis.window = oldWindow;
}

// --- checkYoutube: no residual ConsoleState for a row with no identifier ---
console.log('checkYoutube leaves no residual ConsoleState when mb_release_id is absent (#522)');
{
  const { state } = await import('../web/js/state.js');
  const { checkYoutube, consoleStates: liveConsoleStates } = await import('../web/js/long_tail_console.js');

  // A worklist row with no mb_release_id (e.g. an unresolved legacy row) —
  // checkYoutube must bail out before ever touching consoleStates, not
  // create-then-immediately-empty an entry that lingers until the next
  // consolePrune.
  state.longTail = { rows: [{ id: 424242, mb_release_id: '' }], band: null, query: '' };
  assertEqual(liveConsoleStates.has(424242), false,
    'sanity: no ConsoleState entry exists for this id before calling checkYoutube');

  await checkYoutube(424242);
  assertEqual(liveConsoleStates.has(424242), false,
    'checkYoutube must not create a ConsoleState entry for a row with no mb_release_id');

  // Same for an id with no cohort row at all (consoleRow returns null).
  await checkYoutube(999424242);
  assertEqual(liveConsoleStates.has(999424242), false,
    'checkYoutube must not create a ConsoleState entry for an id absent from the cohort');

  state.longTail = { rows: null, band: null, query: '' };
}

console.log('checkYoutube posts the complete resolver body');
{
  const { state } = await import('../web/js/state.js');
  const {
    checkYoutube,
    consoleStates: liveConsoleStates,
  } = await import('../web/js/long_tail_console.js');
  const requestId = 434343;
  const identifier = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';
  const fetchCalls = [];
  const originalFetch = globalThis.fetch;
  state.longTail = {
    rows: [{ id: requestId, mb_release_id: identifier }],
    band: null,
    query: '',
  };
  globalThis.fetch = async (url, options) => {
    fetchCalls.push({ url, options });
    return {
      status: 200,
      json: async () => ({
        outcome: 'ok',
        youtube_releases: [],
        from_cache: false,
        error_message: null,
      }),
    };
  };
  try {
    await checkYoutube(requestId);
  } finally {
    globalThis.fetch = originalFetch;
    liveConsoleStates.delete(requestId);
    state.longTail = { rows: null, band: null, query: '' };
  }

  assertEqual(fetchCalls.length, 1, 'resolver click sends exactly one request');
  assertEqual(fetchCalls[0].url, '/api/youtube-album',
    'resolver click targets the canonical route without query parameters');
  assertEqual(fetchCalls[0].options.method, 'POST',
    'resolver click uses POST so browser provenance is enforced');
  assertEqual(fetchCalls[0].options.headers['Content-Type'], 'application/json',
    'resolver click declares its JSON body');
  assertEqual(fetchCalls[0].options.body, JSON.stringify({
    identifier,
    refresh: false,
  }), 'resolver click sends identifier and refresh in the JSON body');
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
