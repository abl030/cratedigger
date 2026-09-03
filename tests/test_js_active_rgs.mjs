/**
 * Unit tests for web/js/active_rgs.js — the active release-group cache
 * consulted by the Browse-search inverted Replace button (issue #1355
 * item 6). This module had no test file before this issue's item 8
 * residual comment flagged the gap; these are the first.
 *
 * `discography.js::loadReleaseGroup` is the composed entry that turns
 * this module's cache state into the operator-visible Replace button
 * explanation, so the composed-path sections below drive it directly
 * (mocking only `fetch`) rather than hand-building the `ctx` object
 * `renderPressingRow` receives. See the comment above those sections
 * for exactly which ones a field-mapping bug fails and why one of them
 * cannot.
 *
 * Run with: node tests/test_js_active_rgs.mjs
 */

import {
  loadActiveRgs,
  hasActiveRg,
  activeRgsUnavailable,
  invalidateActiveRgs,
} from '../web/js/active_rgs.js';
import { loadReleaseGroup } from '../web/js/discography.js';
import { pipelineStore } from '../web/js/state.js';

import { element, stubGlobals, suite } from './js_harness.mjs';

const t = suite(import.meta.url);

function okJsonResponse(body) {
  return { ok: true, status: 200, json: async () => body };
}

function httpErrorResponse(status) {
  return { ok: false, status, json: async () => ({}) };
}

t.section('loadActiveRgs() — successful fetch caches real membership');
{
  invalidateActiveRgs();
  stubGlobals({
    fetch: async () => okJsonResponse({ release_group_ids: ['rg-1', 'rg-2'] }),
  });
  await loadActiveRgs();
  t.ok(hasActiveRg('rg-1'), 'a release group present in the response reports true');
  t.ok(!hasActiveRg('rg-9'), 'a release group absent from the response reports false');
}

t.section('loadActiveRgs() — HTTP failure is disabled AND flagged unavailable, not confirmed absence');
{
  invalidateActiveRgs();
  stubGlobals({ fetch: async () => httpErrorResponse(503) });
  await loadActiveRgs();
  t.ok(!hasActiveRg('rg-1'), 'fail-closed: the button stays disabled on HTTP failure, same as genuine absence');
  t.ok(activeRgsUnavailable(), 'HTTP failure is now distinguishable from a confirmed empty collection');
}

t.section('loadActiveRgs() — network failure (fetch rejects) is also flagged unavailable');
{
  invalidateActiveRgs();
  stubGlobals({ fetch: async () => { throw new TypeError('network down'); } });
  await loadActiveRgs();
  t.ok(!hasActiveRg('rg-1'), 'fail-closed: the button stays disabled on a network failure');
  t.ok(activeRgsUnavailable(), 'network failure is flagged unavailable');
}

t.section('loadActiveRgs() — malformed release_group_ids is flagged unavailable, not cached as a confirmed empty answer');
{
  invalidateActiveRgs();
  stubGlobals({ fetch: async () => okJsonResponse({ release_group_ids: null }) });
  await loadActiveRgs();
  t.ok(!hasActiveRg('rg-1'), 'fail-closed: the button stays disabled on a malformed response');
  t.ok(activeRgsUnavailable(),
    'a malformed release_group_ids shape is flagged unavailable, where it used to be silently cached as a confirmed empty set');
}

t.section('loadActiveRgs() — retry: a failed attempt does not poison a later successful one');
{
  invalidateActiveRgs();
  let call = 0;
  stubGlobals({
    fetch: async () => {
      call += 1;
      if (call === 1) return httpErrorResponse(500);
      return okJsonResponse({ release_group_ids: ['rg-1'] });
    },
  });
  await loadActiveRgs();
  t.ok(activeRgsUnavailable(), 'first attempt failed and is flagged unavailable');
  // The failed attempt left the cache null, so the next loadActiveRgs()
  // call is a genuine retry — this is the "allowing later retry"
  // mechanism issue #1355 item 6 relies on. No separate retry control
  // is added: this is the same lazy re-fetch a browse-tab operator
  // triggers by collapsing and re-expanding the release group.
  await loadActiveRgs();
  t.ok(!activeRgsUnavailable(), 'a subsequent successful load clears the unavailable flag');
  t.ok(hasActiveRg('rg-1'), 'the retried load answers correctly');
}

t.section('invalidateActiveRgs() — clears the unavailable flag along with the cache');
{
  invalidateActiveRgs();
  stubGlobals({ fetch: async () => { throw new Error('down'); } });
  await loadActiveRgs();
  t.ok(activeRgsUnavailable(), 'set up: the last attempt failed');
  invalidateActiveRgs();
  t.ok(!activeRgsUnavailable(), 'invalidate resets the unavailable flag, not just the cache');
}

/** A controllable Promise: resolve()/reject() release it from outside. */
function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

t.section('loadActiveRgs() — a stale in-flight FAILURE cannot clobber a fresher invalidate + reload; it adopts the fresh answer');
{
  invalidateActiveRgs();
  const staleGate = deferred();
  let call = 0;
  stubGlobals({
    fetch: async () => {
      call += 1;
      if (call === 1) {
        // The first (stale) attempt hangs here until the test releases
        // it — simulating a slow response that outlives an
        // invalidateActiveRgs() call triggered by an unrelated operator
        // action (add/replace/remove) while this fetch is in flight.
        await staleGate.promise;
        return httpErrorResponse(500);
      }
      return okJsonResponse({ release_group_ids: ['rg-fresh'] });
    },
  });
  const staleLoad = loadActiveRgs();
  // Simulates the unrelated mutation that invalidates the cache while
  // the stale fetch above is still pending.
  invalidateActiveRgs();
  await loadActiveRgs();
  t.ok(hasActiveRg('rg-fresh'), 'the fresh reload result is in place');
  t.ok(!activeRgsUnavailable(), 'the fresh successful reload is not flagged unavailable');
  staleGate.resolve();
  const staleResult = await staleLoad;
  t.ok(hasActiveRg('rg-fresh'), 'a late-resolving stale FAILURE does not clobber the fresher cache');
  t.ok(!activeRgsUnavailable(),
    'a late-resolving stale FAILURE does not falsely flag a fresher successful load as unavailable');
  t.ok(staleResult.has('rg-fresh'),
    'the stale caller itself adopts the fresh answer (re-enters loadActiveRgs) rather than reporting a confirmed-empty set — deleting the catch-branch generation guard would make this the ONLY assertion in this file that observes the caller-facing return value, not just module state');
}

t.section('loadActiveRgs() — a stale in-flight SUCCESS cannot overwrite a fresher successful reload with older data');
{
  invalidateActiveRgs();
  const staleGate = deferred();
  let call = 0;
  stubGlobals({
    fetch: async () => {
      call += 1;
      if (call === 1) {
        // The stale attempt eventually SUCCEEDS too, just with data
        // that is no longer current by the time it lands — this is the
        // shape that specifically exercises the success-branch
        // generation guard, distinct from the failure-branch guard the
        // previous section exercises.
        await staleGate.promise;
        return okJsonResponse({ release_group_ids: ['rg-stale'] });
      }
      return okJsonResponse({ release_group_ids: ['rg-fresh'] });
    },
  });
  const staleLoad = loadActiveRgs();
  invalidateActiveRgs();
  await loadActiveRgs();
  t.ok(hasActiveRg('rg-fresh'), 'the fresh reload result is in place');
  staleGate.resolve();
  await staleLoad;
  t.ok(hasActiveRg('rg-fresh'), 'the fresh result survives a late-resolving stale SUCCESS');
  t.ok(!hasActiveRg('rg-stale'),
    'a late-resolving stale SUCCESS does not overwrite the cache with its own, no-longer-current data');
}

t.section('loadActiveRgs() — a caller arriving after a stale attempt settles still joins the current in-flight attempt, not a redundant fetch');
{
  invalidateActiveRgs();
  const staleGate = deferred();
  const freshGate = deferred();
  let call = 0;
  stubGlobals({
    fetch: async () => {
      call += 1;
      if (call === 1) {
        await staleGate.promise;
        return httpErrorResponse(500);
      }
      await freshGate.promise;
      return okJsonResponse({ release_group_ids: ['rg-fresh'] });
    },
  });
  const staleLoad = loadActiveRgs();
  invalidateActiveRgs();
  const freshLoad = loadActiveRgs(); // call #2, still pending (hangs on freshGate)
  staleGate.resolve();
  // The stale attempt's own generation guard re-enters loadActiveRgs
  // and adopts whatever is currently in flight (the fresh attempt,
  // still pending on freshGate) — so staleLoad itself will not settle
  // until freshGate does. Flush the microtask queue with a macrotask
  // boundary instead of awaiting staleLoad directly, so the stale
  // attempt's finally block runs (proving/disproving the finally guard)
  // without deadlocking on the fresh attempt this test deliberately
  // holds open a moment longer.
  await new Promise((resolve) => { setTimeout(resolve, 0); });
  // A third caller arrives AFTER the stale attempt has fully settled
  // (including its finally block) but BEFORE the fresh attempt has —
  // this is exactly the window where an unconditional `inflight = null`
  // in the stale attempt's finally would let this caller believe
  // nothing is in flight and start a THIRD fetch, instead of joining
  // the fresh attempt that is still genuinely pending.
  const thirdLoad = loadActiveRgs();
  freshGate.resolve();
  await Promise.all([staleLoad, freshLoad, thirdLoad]);
  t.equal(call, 2,
    'a caller arriving between a stale attempt\'s settlement and the current attempt\'s settlement joins the current in-flight fetch rather than starting a redundant third one');
  t.ok(hasActiveRg('rg-fresh'), 'the joined attempt still produces the correct result');
}

/*
 * Composed render path — loadReleaseGroup() is the entry discography.js
 * exposes that fetches a release group's pressings, awaits
 * loadActiveRgs() in parallel, and builds the ctx object renderPressingRow
 * receives. Driving it directly (rather than hand-building that ctx)
 * proves the real field mapping.
 *
 * A swap of canReplace/rgLookupUnavailable in discography.js fails BOTH
 * the "a failed active-RG lookup renders the unavailable explanation"
 * and "a real match still enables the button" sections below (each has
 * canReplace != rgLookupUnavailable, so a swap flips its outcome). A
 * stale activeRgsUnavailable() read taken before the fetch settles fails
 * only the "failed active-RG lookup" section — hoisting the read above
 * Promise.all would read the POST-invalidate value (false, since
 * expandReleaseGroup calls invalidateActiveRgs() before loadReleaseGroup
 * ever runs) on every section, and that happens to already equal the
 * correct value everywhere except the failed-lookup section, where the
 * correct value is genuinely true — so only there does hoisting produce
 * a visible difference.
 *
 * One section cannot catch the swap at all: "Confirmed absence renders
 * the confirmed-absence explanation" has hasActiveRg() and
 * activeRgsUnavailable() both false there by construction (a successful
 * load reporting a genuine absence), so swapping two equal values is
 * invisible; no fixture change closes this, since the section's own
 * definition forces both to false. It exists to pin its own tooltip
 * text, not to discriminate the mapping.
 */

/** One MB pressing row with no pipeline/library overlay — routes renderPressingRow into inverted-mode Replace, the only mode this cache affects. */
function unclaimedPressing(id, releaseGroupId) {
  return {
    id,
    release_group_id: releaseGroupId,
    title: 'Everything Is Alive',
    status: 'Official',
    country: 'AU',
    date: '2011-05-01',
    format: 'CD',
    track_count: 10,
  };
}

async function expandReleaseGroup({ rgId, sourceId, activeRgsBody, activeRgsFails }) {
  pipelineStore.clear();
  invalidateActiveRgs();
  const relEl = element();
  stubGlobals({
    fetch: async (url) => {
      if (String(url).includes('/api/pipeline/active-rgs')) {
        if (activeRgsFails) throw new TypeError('network down');
        return okJsonResponse(activeRgsBody);
      }
      return okJsonResponse({ releases: [unclaimedPressing(sourceId, rgId)] });
    },
  });
  await loadReleaseGroup(rgId, null, { targetEl: relEl, source: 'mb', identityKind: 'work' });
  return relEl.innerHTML;
}

t.section('loadReleaseGroup() composed path — confirmed absence renders the confirmed-absence explanation');
{
  const html = await expandReleaseGroup({
    rgId: 'rg-composed-1',
    sourceId: '129bebd8-a7b9-4099-b0bc-545b704e7a95',
    activeRgsBody: { release_group_ids: [] },
  });
  t.contains(html, 'disabled title="No existing request in this release group"',
    'a genuinely empty active-RG set renders the confirmed-absence explanation');
  t.excludes(html, 'Could not check',
    'the unavailable explanation is not shown when the lookup actually succeeded');
}

t.section('loadReleaseGroup() composed path — a failed active-RG lookup renders the unavailable explanation, button still disabled');
{
  // This section also carries the "a MusicBrainz row is unchanged" pin
  // for the issue #1355 residual sweep's Batch D gate correction: an MB
  // release-group id always had, and still has, its own parentRgId
  // fallback (untouched by the Discogs-side fix below), so this exact
  // assertion is the regression guard proving the gate rewrite left the
  // MB path alone.
  const html = await expandReleaseGroup({
    rgId: 'rg-composed-2',
    sourceId: '129bebd8-a7b9-4099-b0bc-545b704e7a95',
    activeRgsFails: true,
  });
  t.contains(html, 'disabled title="Could not check for an existing request in this release group. Collapse and re-expand to retry."',
    'a failed active-RG lookup renders the unavailable explanation, not the confirmed-absence one');
  t.excludes(html, 'title="No existing request in this release group"',
    'the confirmed-absence explanation is not shown for a lookup that never confirmed anything');
}

t.section('loadReleaseGroup() composed path — a real match still enables the button (must-still-work)');
{
  const html = await expandReleaseGroup({
    rgId: 'rg-composed-3',
    sourceId: '129bebd8-a7b9-4099-b0bc-545b704e7a95',
    activeRgsBody: { release_group_ids: ['rg-composed-3'] },
  });
  t.contains(html, 'window.openReplacePicker({targetMbid:',
    'the enabled button still wires the inverted-mode click handler');
  // renderReplaceButton's enabled branch template is exactly
  // `<button class="${className}"${style} onclick="...">` — no title
  // attribute anywhere in it. Assert that adjacency directly (the
  // button's own style attribute immediately followed by its onclick,
  // nothing interposed) rather than excluding "title=" from the whole
  // row, which also legitimately contains one on the search-plan
  // inspector button whenever a pipeline id is present. A mutant that
  // adds a false title string to the enabled branch's return value
  // breaks this exact adjacency (found in review of issue #1355 item 6:
  // such a mutant passed every other assertion in this file and the
  // whole JS unit suite).
  t.contains(html,
    'class="btn" style="padding:2px 8px;font-size:0.7em;white-space:nowrap;" onclick="event.stopPropagation(); window.openReplacePicker({targetMbid:',
    'the enabled Replace button carries no title attribute between its style and onclick attributes');
  // The adjacency check above only covers a title inserted BEFORE
  // onclick; a title appended immediately after the onclick attribute's
  // closing quote (the shape the earlier whole-row `excludes(html,
  // 'title=')` used to catch, before it was narrowed for being
  // whole-row-scoped) needs its own check.
  t.excludes(html, '})" title=',
    'no title attribute is appended immediately after the onclick attribute either');
}

/**
 * A Discogs release under a master carries no release_group_id field of
 * its own — web/discogs.py's get_master_releases never puts one on its
 * child rows, unlike the single-release endpoint synthesizeMasterlessRow
 * reads from. Its only lookup key is the master id itself, reached
 * through loadReleaseGroup's own parentRgId fallback — the same
 * mechanism an MB release-group id already uses. A masterless Discogs
 * release (fetched via /api/discogs/release/<id>, identityKind
 * 'release') is the one shape with no key at all: a genuinely masterless
 * release's master_id is null, so synthesizeMasterlessRow's own
 * release_group_id comes out null too.
 *
 * This corrects the #1361 premise (issue #1355 item 6): a Discogs row's
 * release_group_id was believed to be either a Discogs master id or
 * null, never a value the MB-only active-RG cache could answer for
 * either way — so the "could not check" explanation was gated on source
 * (!isDiscogs) rather than on whether the row actually has a lookup key.
 * Discogs requests persist their exact master in the same
 * mb_release_group_id column MB releases use (KTD-1,
 * lib/mbid_replace_service.py), and 215 live non-replaced requests carry
 * a Discogs-shaped (numeric) value there, so a Discogs master row's
 * lookup key CAN match the cache — a failed lookup on such a row owes
 * the same honest "could not check" explanation an MB row gets, not the
 * confirmed-absence claim a genuinely keyless (masterless) row keeps.
 */
async function expandDiscogsReleaseGroup({
  rgId, sourceId, identityKind = 'work', activeRgsBody, activeRgsFails,
}) {
  pipelineStore.clear();
  invalidateActiveRgs();
  const relEl = element();
  const masterless = identityKind === 'release';
  stubGlobals({
    fetch: async (url) => {
      if (String(url).includes('/api/pipeline/active-rgs')) {
        if (activeRgsFails) throw new TypeError('network down');
        return okJsonResponse(activeRgsBody);
      }
      if (masterless) {
        // /api/discogs/release/<id> payload for a genuinely masterless
        // release — no master, so no release_group_id.
        return okJsonResponse({
          id: sourceId,
          title: 'Everything Is Alive',
          date: '2011-05-01',
          country: 'AU',
          formats: [{ name: 'CD' }],
          tracks: new Array(10).fill({}),
          release_group_id: null,
        });
      }
      // /api/discogs/master/<id> payload — get_master_releases's child
      // rows carry no release_group_id field at all; the master id
      // reaches the row only through loadReleaseGroup's own parentRgId
      // fallback.
      return okJsonResponse({ releases: [unclaimedPressing(sourceId)] });
    },
  });
  await loadReleaseGroup(rgId, null, { targetEl: relEl, source: 'discogs', identityKind });
  return relEl.innerHTML;
}

t.section('loadReleaseGroup() composed path — a Discogs master row under a failed lookup renders the "could not check" tooltip (issue #1355 residual sweep, Batch D)');
{
  const html = await expandDiscogsReleaseGroup({
    rgId: 424242,
    sourceId: '999999',
    activeRgsFails: true,
  });
  t.contains(html, 'disabled title="Could not check for an existing request in this release group. Collapse and re-expand to retry."',
    'a Discogs row under a master gets the honest "could not check" explanation on a failed lookup, the same as an MB row');
  t.excludes(html, 'title="No existing request in this release group"',
    'the confirmed-absence explanation is not shown for a lookup that never confirmed anything');
}

t.section('loadReleaseGroup() composed path — a masterless Discogs release under the same failure keeps its current text');
{
  const html = await expandDiscogsReleaseGroup({
    rgId: '999999',
    sourceId: '999999',
    identityKind: 'release',
    activeRgsFails: true,
  });
  t.contains(html, 'disabled title="No existing request in this release group"',
    'a masterless Discogs release has no lookup key at all, so it keeps the confirmed-absence text even though the fetch failed');
  t.excludes(html, 'Could not check',
    'the unavailable explanation is never claimed for a row with no lookup key to check');
}

t.section('loadReleaseGroup() composed path — a Discogs master row with an active Discogs request for that master still enables Replace (must-still-work, proves the premise correction)');
{
  const html = await expandDiscogsReleaseGroup({
    rgId: 424242,
    sourceId: '999999',
    activeRgsBody: { release_group_ids: ['424242'] },
  });
  t.contains(html, 'window.openReplacePicker({targetMbid:',
    'a real match on the master id enables the inverted-mode Replace button for a Discogs row, proving the cache genuinely holds Discogs-shaped ids');
  t.excludes(html, 'Could not check',
    'an enabled button carries no "could not check" explanation');
}

t.done();
