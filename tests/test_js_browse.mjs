/**
 * Artist-page fast-pair failure tests (issue #603).
 *
 * Invariants:
 *  B1 Either non-OK fast response, or a network rejection, leaves the
 *     artist cache untouched and never renders a raw exception string.
 *  B2 The active failed load renders a clear Retry action wired through
 *     the existing window.reloadBrowseArtist binding.
 *  B3 A stale failed load cannot replace the active page's content.
 */

import assert from 'node:assert/strict';

import { loadArtistPage, reloadBrowseArtist } from '../web/js/browse.js';
import { state } from '../web/js/state.js';

const artistBody = {
  innerHTML: '',
  querySelector: () => null,
  insertAdjacentHTML: () => {},
};

globalThis.document = {
  getElementById(id) {
    if (id === 'browse-artist-body') return artistBody;
    return null;
  },
};

function response(status, data) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async json() { return data; },
  };
}

function resetWorld() {
  state.browseSource = 'mb';
  state.browseArtist = null;
  state.browseCache = {};
  state.searchTargetId = null;
  state.searchTargetExpandId = null;
  state.searchTargetSource = null;
  artistBody.innerHTML = '';
}

function assertSafeRetryFailure(aid, rawSecret) {
  assert.equal(state.browseCache[aid], undefined, 'failed fast pair must not populate cache');
  assert.match(artistBody.innerHTML, />Retry</, 'failure state must offer Retry');
  assert.match(
    artistBody.innerHTML,
    /onclick="window\.reloadBrowseArtist\(\)"/,
    'Retry must call the existing public reload binding',
  );
  assert.doesNotMatch(artistBody.innerHTML, new RegExp(rawSecret), 'raw failure detail must stay hidden');
}

// Known-bad qualification: the checker trips on both persistence and leakage.
resetWorld();
state.browseCache.bad = { fast: {} };
artistBody.innerHTML = '<div>raw-known-bad-secret</div>';
assert.throws(
  () => assertSafeRetryFailure('bad', 'raw-known-bad-secret'),
  /failed fast pair must not populate cache/,
);
resetWorld();
artistBody.innerHTML = '<button onclick="window.reloadBrowseArtist()">Retry</button> raw-known-bad-secret';
assert.throws(
  () => assertSafeRetryFailure('bad', 'raw-known-bad-secret'),
  /raw failure detail must stay hidden/,
);

// Deterministic pin: the motivating MusicBrainz 503 body never becomes data.
resetWorld();
{
  const aid = 'mb-503-pin';
  const rawSecret = 'SSL UNEXPECTED_EOF private upstream detail';
  globalThis.fetch = async (url) => url.includes('/api/library/artist')
    ? response(200, { albums: [] })
    : response(503, {
      error: 'MusicBrainz fallback unavailable, retry',
      retryable: true,
      raw: rawSecret,
    });
  await loadArtistPage(aid, 'Transport Failure');
  assertSafeRetryFailure(aid, rawSecret);
}

// Independent pin: the library half failing is just as cache-safe.
resetWorld();
{
  const aid = 'library-500-pin';
  const rawSecret = 'raw downstream database exception';
  globalThis.fetch = async (url) => url.includes('/api/library/artist')
    ? response(500, { error: rawSecret })
    : response(200, { release_groups: [] });
  await loadArtistPage(aid, 'Library Failure');
  assertSafeRetryFailure(aid, rawSecret);
}

// Independent pin: rejected fetch promises use the same stable Retry state.
resetWorld();
{
  const aid = 'network-pin';
  const rawSecret = 'socket exploded at 10.0.0.9';
  globalThis.fetch = async () => { throw new Error(rawSecret); };
  await loadArtistPage(aid, 'Network Failure');
  assertSafeRetryFailure(aid, rawSecret);
}

// Retry wiring: the public reload deletes any old artist cache and re-fetches.
resetWorld();
{
  const aid = 'retry-pin';
  let fetchCount = 0;
  state.browseArtist = { id: aid, name: 'Retry Artist' };
  state.browseCache[aid] = { stale: true };
  globalThis.fetch = async () => {
    fetchCount++;
    throw new Error('still unavailable');
  };
  reloadBrowseArtist();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(state.browseCache[aid], undefined);
  assert(fetchCount > 0, 'Retry binding must start a fresh fetch');
  assert.match(artistBody.innerHTML, />Retry</);
}

// Generated/property sweep: every non-OK status on either fast response keeps
// the same cache-safe, non-leaking Retry contract.
for (const status of [400, 401, 403, 404, 408, 409, 418, 429, 500, 502, 503, 504, 599]) {
  for (const failedPart of ['artist', 'library']) {
    resetWorld();
    const aid = `generated-${failedPart}-${status}`;
    const rawSecret = `raw-${failedPart}-secret-${status}`;
    globalThis.fetch = async (url) => {
      const isLibrary = url.includes('/api/library/artist');
      const shouldFail = failedPart === 'library' ? isLibrary : !isLibrary;
      if (shouldFail) return response(status, { error: rawSecret, retryable: status === 503 });
      return response(200, isLibrary ? { albums: [] } : { release_groups: [] });
    };
    await loadArtistPage(aid, `Generated ${status}`);
    assertSafeRetryFailure(aid, rawSecret);
  }
}

// Stale-token pin: after a newer load owns the page, the older transport
// failure may resolve but cannot overwrite the active Retry state.
resetWorld();
{
  const pending = [];
  globalThis.fetch = () => new Promise(resolve => pending.push(resolve));
  const oldLoad = loadArtistPage('old-artist', 'Old Artist');
  assert.equal(pending.length, 2, 'old fast pair started both requests');

  globalThis.fetch = async () => { throw new Error('new active failure'); };
  await loadArtistPage('new-artist', 'New Artist');
  const activeHtml = artistBody.innerHTML;

  for (const resolve of pending) {
    resolve(response(503, { error: 'raw stale failure', retryable: true }));
  }
  await oldLoad;
  assert.equal(artistBody.innerHTML, activeHtml, 'stale failure must not replace active content');
  assert.equal(state.browseCache['old-artist'], undefined);
}

console.log('JS browse fast-pair failure tests passed');
