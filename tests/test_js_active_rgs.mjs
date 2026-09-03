/**
 * Unit tests for web/js/active_rgs.js — the active release-group cache
 * consulted by the Browse-search inverted Replace button (issue #1355
 * item 6). This module had no test file before this issue's item 8
 * residual comment flagged the gap; these are the first.
 *
 * `discography.js::loadReleaseGroup` is the composed entry that turns
 * this module's cache state into the operator-visible Replace button
 * explanation, so the last three sections drive it directly (mocking
 * only `fetch`) rather than hand-building the `ctx` object
 * `renderPressingRow` receives — a swapped `canReplace`/
 * `rgLookupUnavailable` field, or a stale `activeRgsUnavailable()` read,
 * would fail those sections.
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
  t.ok(!activeRgsUnavailable(), 'a successful load is not flagged unavailable');
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

/*
 * Composed render path — loadReleaseGroup() is the entry discography.js
 * exposes that turns this cache's state into the button's visible
 * explanation. renderPressingRow / renderReplaceButton stay leaves of
 * that entry (per tests/test_js_discography.mjs's own header), so only
 * loadReleaseGroup can prove the composition: which cache state produces
 * which sentence.
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
  const html = await expandReleaseGroup({
    rgId: 'rg-composed-2',
    sourceId: '129bebd8-a7b9-4099-b0bc-545b704e7a95',
    activeRgsFails: true,
  });
  t.contains(html, 'disabled title="Could not check for an existing request in this release group. Try again."',
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
  t.excludes(html, 'disabled', 'a confirmed active request in the same release group enables the button');
  t.contains(html, 'window.openReplacePicker({targetMbid:',
    'the enabled button still wires the inverted-mode click handler');
}

t.done();
