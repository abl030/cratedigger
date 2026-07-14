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

import {
  loadArtistPage,
  reloadBrowseArtist,
  resolverTargetIdentityKind,
  setBrowseSource,
} from '../web/js/browse.js';
import { state } from '../web/js/state.js';

const artistBody = {
  innerHTML: '',
  querySelector: () => null,
  insertAdjacentHTML: () => {},
};

const elements = {
  'browse-artist-body': artistBody,
  'browse-artist-name': { textContent: '' },
  'browse-artist': { style: { display: 'block' } },
  results: { style: { display: 'none' } },
  q: { value: '' },
  'source-mb': { className: '' },
  'source-discogs': { className: '' },
  'source-hint': { innerHTML: '' },
};

globalThis.document = {
  getElementById(id) {
    return elements[id] || null;
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
  state.searchTargetIdentityKind = null;
  artistBody.innerHTML = '';
  elements['browse-artist-name'].textContent = '';
  elements['browse-artist'].style.display = 'block';
  elements.results.style.display = 'none';
  elements.q.value = '';
}

function deferred() {
  let resolve;
  const promise = new Promise(r => { resolve = r; });
  return { promise, resolve };
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

// Resolver identity is backend-authored. Equal master/release numbers are
// valid in separate Discogs namespaces and therefore prove nothing.
{
  const groupedEqualId = {
    source: 'discogs', kind: 'release', expand_id: '122', leaf_id: '122',
    target_identity_kind: 'work',
  };
  const masterlessEqualId = {
    source: 'discogs', kind: 'release', expand_id: '122', leaf_id: '122',
    target_identity_kind: 'release',
  };
  assert.equal(resolverTargetIdentityKind(groupedEqualId), 'work');
  assert.equal(resolverTargetIdentityKind(masterlessEqualId), 'release');

  const equalityMutant = data => (
    String(data.expand_id) === String(data.leaf_id) ? 'release' : 'work'
  );
  assert.equal(
    equalityMutant(groupedEqualId),
    'release',
    'known-bad equality heuristic misclassifies the grouped release',
  );
  assert.notEqual(
    equalityMutant(groupedEqualId),
    resolverTargetIdentityKind(groupedEqualId),
  );

  for (let id = 1; id <= 2000; id += 41) {
    for (const targetIdentityKind of ['work', 'release']) {
      const data = {
        source: 'discogs', kind: 'release',
        expand_id: String(id), leaf_id: String(id),
        target_identity_kind: targetIdentityKind,
      };
      assert.equal(
        resolverTargetIdentityKind(data),
        targetIdentityKind,
        `explicit target survives equal-ID world ${id}/${targetIdentityKind}`,
      );
    }
  }
  assert.throws(
    () => resolverTargetIdentityKind({ expand_id: '122', leaf_id: '122' }),
    /missing target_identity_kind/,
  );
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

// Source-switch race pin: invalidation happens before the cross-source artist
// lookup awaits. An old MB failure cannot paint Retry after Discogs becomes
// the active source, and the resulting current load/retry uses the Discogs id.
resetWorld();
{
  const oldArtist = deferred();
  const oldLibrary = deferred();
  const sourceLookup = deferred();
  const requests = [];
  state.browseSource = 'mb';
  state.browseArtist = { id: 'old-mb-id', name: 'Race Artist' };
  globalThis.fetch = (url) => {
    requests.push(url);
    if (url.includes('/api/artist/old-mb-id?')) return oldArtist.promise;
    if (url.includes('mbid=old-mb-id')) return oldLibrary.promise;
    if (url.includes('/api/discogs/search?')) return sourceLookup.promise;
    if (url.includes('/api/discogs/artist/new-discogs-id?')) {
      return Promise.resolve(response(503, { error: 'current failure', retryable: true }));
    }
    if (url.includes('/api/library/artist?name=Race%20Artist')) {
      return Promise.resolve(response(200, { albums: [] }));
    }
    throw new Error(`unexpected race request: ${url}`);
  };

  const oldLoad = loadArtistPage('old-mb-id', 'Race Artist');
  const sourceSwitch = setBrowseSource('discogs');
  oldArtist.resolve(response(503, { error: 'stale MB failure', retryable: true }));
  oldLibrary.resolve(response(200, { albums: [] }));
  await oldLoad;
  assert.doesNotMatch(
    artistBody.innerHTML,
    />Retry</,
    'old-source failure must be stale as soon as source switching starts',
  );

  sourceLookup.resolve(response(200, {
    artists: [{ id: 'new-discogs-id', name: 'Race Artist' }],
  }));
  await sourceSwitch;
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(state.browseArtist, { id: 'new-discogs-id', name: 'Race Artist' });
  assert(requests.some(url => url.includes('/api/discogs/artist/new-discogs-id?')));
  assert.match(artistBody.innerHTML, />Retry</, 'current-source failure owns Retry');

  requests.length = 0;
  globalThis.fetch = async (url) => {
    requests.push(url);
    throw new Error('retry remains unavailable');
  };
  reloadBrowseArtist();
  await new Promise(resolve => setImmediate(resolve));
  assert(requests.some(url => url.includes('/api/discogs/artist/new-discogs-id?')));
  assert(requests.some(url => url === '/api/library/artist?name=Race%20Artist'));
  assert(!requests.some(url => url.includes('old-mb-id')));
}

// Generated/property sweep around the race: either fast-pair half, several
// failure classes, and both source directions remain stale while lookup waits.
for (const [oldSource, newSource] of [['mb', 'discogs'], ['discogs', 'mb']]) {
  for (const failedPart of ['artist', 'library']) {
    for (const status of [404, 429, 503]) {
      resetWorld();
      const oldFastA = deferred();
      const oldFastB = deferred();
      const sourceLookup = deferred();
      let callIndex = 0;
      state.browseSource = oldSource;
      state.browseArtist = { id: `old-${oldSource}`, name: 'Generated Race' };
      globalThis.fetch = () => {
        const call = callIndex++;
        if (call === 0) return oldFastA.promise;
        if (call === 1) return oldFastB.promise;
        if (call === 2) return sourceLookup.promise;
        throw new Error(`unexpected generated race fetch ${call}`);
      };

      const oldLoad = loadArtistPage(`old-${oldSource}`, 'Generated Race');
      const sourceSwitch = setBrowseSource(newSource);
      oldFastA.resolve(failedPart === 'artist'
        ? response(status, { error: 'stale generated failure' })
        : response(200, { release_groups: [] }));
      oldFastB.resolve(failedPart === 'library'
        ? response(status, { error: 'stale generated failure' })
        : response(200, { albums: [] }));
      await oldLoad;
      assert.doesNotMatch(artistBody.innerHTML, />Retry</);

      sourceLookup.resolve(response(200, { artists: [] }));
      await sourceSwitch;
      assert.equal(state.browseArtist, null);
    }
  }
}

// Double-toggle ownership pin: lookups can resolve out of order. The newest
// MB toggle must keep its MB artist/id/endpoint even if the older Discogs
// lookup returns last with a valid but wrong-source match.
resetWorld();
{
  const discogsLookup = deferred();
  const mbLookup = deferred();
  const requests = [];
  state.browseSource = 'mb';
  state.browseArtist = { id: 'starting-mb-id', name: 'Toggle Artist' };
  globalThis.fetch = (url) => {
    requests.push(url);
    if (url.includes('/api/discogs/search?')) return discogsLookup.promise;
    if (url.includes('/api/search?')) return mbLookup.promise;
    if (url.includes('/api/artist/newest-mb-id?')) {
      return Promise.resolve(response(200, { release_groups: [] }));
    }
    if (url.includes('mbid=newest-mb-id')) {
      return Promise.resolve(response(200, { albums: [] }));
    }
    if (url.includes('/api/artist/compare?') || url.includes('/disambiguate')) {
      return Promise.resolve(response(503, { error: 'decoration unavailable' }));
    }
    if (url.includes('stale-discogs-id')) {
      return Promise.resolve(response(503, { error: 'stale lookup drove a load' }));
    }
    throw new Error(`unexpected double-toggle request: ${url}`);
  };

  const olderToggle = setBrowseSource('discogs');
  const newestToggle = setBrowseSource('mb');
  mbLookup.resolve(response(200, {
    artists: [{ id: 'newest-mb-id', name: 'Toggle Artist' }],
  }));
  await newestToggle;
  await new Promise(resolve => setImmediate(resolve));
  const newestHtml = artistBody.innerHTML;
  assert.equal(state.browseSource, 'mb');
  assert.deepEqual(state.browseArtist, { id: 'newest-mb-id', name: 'Toggle Artist' });
  assert(requests.some(url => url.includes('/api/artist/newest-mb-id?')));
  assert.doesNotMatch(newestHtml, />Retry</);

  discogsLookup.resolve(response(200, {
    artists: [{ id: 'stale-discogs-id', name: 'Toggle Artist' }],
  }));
  await olderToggle;
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(state.browseSource, 'mb');
  assert.deepEqual(
    state.browseArtist,
    { id: 'newest-mb-id', name: 'Toggle Artist' },
    'older lookup must not overwrite the newest source artist',
  );
  assert(!requests.some(url => url.includes('stale-discogs-id')));
  assert.equal(artistBody.innerHTML, newestHtml, 'older lookup must not repaint the newest page');
}

// Generated/property sweep for both directions. The newest lookup may own a
// Retry state, but resolving the older valid match cannot change its artist,
// endpoint family, or rendered content.
for (const newestSource of ['mb', 'discogs']) {
  resetWorld();
  const olderSource = newestSource === 'mb' ? 'discogs' : 'mb';
  const lookups = { mb: deferred(), discogs: deferred() };
  const newestId = `newest-${newestSource}-id`;
  const staleId = `stale-${olderSource}-id`;
  const requests = [];
  state.browseSource = newestSource;
  state.browseArtist = { id: `starting-${newestSource}-id`, name: 'Generated Toggle' };
  globalThis.fetch = (url) => {
    requests.push(url);
    if (url.includes('/api/discogs/search?')) return lookups.discogs.promise;
    if (url.includes('/api/search?')) return lookups.mb.promise;
    if (url.includes(newestId)) {
      return Promise.resolve(response(503, { error: 'newest source unavailable' }));
    }
    if (url.includes('/api/library/artist?')) {
      return Promise.resolve(response(200, { albums: [] }));
    }
    if (url.includes(staleId)) {
      return Promise.resolve(response(503, { error: 'stale source load' }));
    }
    throw new Error(`unexpected generated double-toggle request: ${url}`);
  };

  const olderToggle = setBrowseSource(olderSource);
  const newestToggle = setBrowseSource(newestSource);
  lookups[newestSource].resolve(response(200, {
    artists: [{ id: newestId, name: 'Generated Toggle' }],
  }));
  await newestToggle;
  await new Promise(resolve => setImmediate(resolve));
  const newestHtml = artistBody.innerHTML;
  assert.match(newestHtml, />Retry</);

  lookups[olderSource].resolve(response(200, {
    artists: [{ id: staleId, name: 'Generated Toggle' }],
  }));
  await olderToggle;
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(state.browseSource, newestSource);
  assert.deepEqual(state.browseArtist, { id: newestId, name: 'Generated Toggle' });
  const expectedEndpoint = newestSource === 'discogs'
    ? `/api/discogs/artist/${newestId}?`
    : `/api/artist/${newestId}?`;
  assert(requests.some(url => url.includes(expectedEndpoint)));
  assert(!requests.some(url => url.includes(staleId)));
  assert.equal(artistBody.innerHTML, newestHtml);
}

console.log('JS browse fast-pair failure tests passed');
