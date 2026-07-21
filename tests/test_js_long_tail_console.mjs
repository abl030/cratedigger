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

import { __test__ } from '../web/js/long_tail_console.js';

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

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
