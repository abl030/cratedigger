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

import {
  closeBrowseArtist,
  loadArtistPage,
  pendingEarlyCompareHandoffsForTest,
  reloadBrowseArtist,
  resolverTargetIdentityKind,
  searchArtists,
  setBrowseSource,
} from '../web/js/browse.js';
import { state } from '../web/js/state.js';

import { stubGlobals, suite } from './js_harness.mjs';

const t = suite(import.meta.url);

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

stubGlobals({ document: {
  getElementById(id) {
    return elements[id] || null;
  },
} });

function response(status, data) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async json() { return data; },
  };
}

// Lifecycle pin: an early payload can arrive before useful content, then the
// view may close or be superseded.  Neither path may retain or later publish
// that stale payload into the next artist's cache.
t.section('close drops an early compare handoff');
resetWorld();
{
  const oldArtist = deferred();
  const oldLibrary = deferred();
  stubGlobals({ fetch: url => {
    if (url.includes('/api/artist/compare?')) {
      return Promise.resolve(response(200, {
        both: [], mb_unpaired: [{ id: 'stale-payload', title: 'Stale', source: 'mb', identity_kind: 'work', provenance: [] }],
        discogs_unpaired: [], discogs_ungrouped_releases: [],
      }));
    }
    if (url.includes('/api/library/artist')) return oldLibrary.promise;
    return oldArtist.promise;
  } });
  const old = loadArtistPage('closed-old', 'Closed Old');
  await new Promise(resolve => setImmediate(resolve));
  closeBrowseArtist();
  oldArtist.resolve(response(200, { release_groups: [] }));
  oldLibrary.resolve(response(200, { albums: [] }));
  await old;
  t.equal(state.browseCache['closed-old'], undefined, 'close drops early stale compare handoff');

  stubGlobals({ fetch: async url => {
    if (url.includes('/api/artist/compare?') || url.includes('/disambiguate')) return response(503, {});
    return response(200, url.includes('/api/library/artist') ? { albums: [] } : { release_groups: [] });
  } });
  await loadArtistPage('fresh-after-close', 'Fresh');
  t.equal(state.browseCache['fresh-after-close'].compare, null,
    'the next artist caches a null compare rather than the dropped payload');
  t.notMatch(artistBody.innerHTML, /Stale/,
    'the dropped payload never paints into the next artist page');
}

// A source switch can invalidate an artist page before its useful pair lands.
// If the new source has no name match, it does not start another artist load
// to incidentally clear the old handoff; the invalidation itself owns that
// cleanup. The old payload must never paint after this exact no-match path.
t.section('a no-match source switch drops the old handoff');
resetWorld();
{
  const oldArtist = deferred();
  const oldLibrary = deferred();
  state.browseArtist = { id: 'switch-old', name: 'No Match Artist' };
  stubGlobals({ fetch: url => {
    if (url.includes('/api/artist/compare?')) {
      return Promise.resolve(response(200, {
        both: [], mb_unpaired: [{ id: 'switch-stale', title: 'Stale', source: 'mb', identity_kind: 'work', provenance: [] }],
        discogs_unpaired: [], discogs_ungrouped_releases: [],
      }));
    }
    if (url.includes('/api/discogs/search?')) return Promise.resolve(response(200, { artists: [] }));
    if (url.includes('/api/library/artist')) return oldLibrary.promise;
    return oldArtist.promise;
  } });
  const old = loadArtistPage('switch-old', 'No Match Artist');
  await new Promise(resolve => setImmediate(resolve));
  await setBrowseSource('discogs');
  t.equal(state.browseArtist, null, 'no cross-source match closes the old artist context');
  t.equal(
    pendingEarlyCompareHandoffsForTest(), 0,
    'source invalidation releases the early compare immediately, before old useful requests settle',
  );
  oldArtist.resolve(response(200, { release_groups: [] }));
  oldLibrary.resolve(response(200, { albums: [] }));
  await old;
  t.equal(state.browseCache['switch-old'], undefined, 'source switch drops old early compare handoff');
  t.notMatch(artistBody.innerHTML, /Stale/,
    'the invalidated payload never paints after the source switch');
}

// Useful-pair failure revokes its token. A compare that resolves after Retry
// renders must not recreate the early handoff for that now-invalid page.
t.section('a late compare cannot revive a failed useful load');
resetWorld();
{
  const compare = deferred();
  stubGlobals({ fetch: url => {
    if (url.includes('/api/artist/compare?')) return compare.promise;
    if (url.includes('/api/library/artist')) return Promise.resolve(response(200, { albums: [] }));
    return Promise.resolve(response(503, {}));
  } });
  await loadArtistPage('failure-before-compare', 'Failure Before Compare');
  t.match(artistBody.innerHTML, />Retry</,
    'the failed useful pair renders Retry before the compare lands');
  compare.resolve(response(200, { both: [], mb_unpaired: [], discogs_unpaired: [], discogs_ungrouped_releases: [] }));
  await new Promise(resolve => setImmediate(resolve));
  t.equal(pendingEarlyCompareHandoffsForTest(), 0, 'late compare cannot revive failed useful load');
  t.equal(state.browseCache['failure-before-compare'], undefined,
    'the late compare leaves no cache entry behind');
}

// The inverse ordering also matters: a compare can win the race and occupy
// the handoff before the useful response fails, but failure still owns final
// cleanup and Retry leaves no retained payload.
t.section('useful failure clears a compare that won the race');
resetWorld();
{
  const artist = deferred();
  stubGlobals({ fetch: url => {
    if (url.includes('/api/artist/compare?')) {
      return Promise.resolve(response(200, { both: [], mb_unpaired: [], discogs_unpaired: [], discogs_ungrouped_releases: [] }));
    }
    if (url.includes('/api/library/artist')) return Promise.resolve(response(200, { albums: [] }));
    return artist.promise;
  } });
  const load = loadArtistPage('compare-before-failure', 'Compare Before Failure');
  await new Promise(resolve => setImmediate(resolve));
  t.equal(pendingEarlyCompareHandoffsForTest(), 1, 'compare handoff exists before useful failure');
  artist.resolve(response(503, {}));
  await load;
  t.equal(pendingEarlyCompareHandoffsForTest(), 0, 'useful failure clears an already-early compare handoff');
  t.match(artistBody.innerHTML, />Retry</,
    'the failure that cleared the handoff still renders Retry');
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

/**
 * Every way a fast-pair failure state can be unsafe, as a list.
 *
 * Accumulating rather than short-circuiting: every clause evaluates, so
 * the known-bad self-test below can prove each one trips instead of only
 * whichever comes first.
 */
function safeRetryFailureViolations(aid, rawSecret) {
  const violations = [];
  if (state.browseCache[aid] !== undefined) violations.push('failed fast pair must not populate cache');
  if (!/>Retry</.test(artistBody.innerHTML)) violations.push('failure state must offer Retry');
  if (!/onclick="window\.reloadBrowseArtist\(\)"/.test(artistBody.innerHTML)) violations.push('Retry must call the existing public reload binding');
  if (new RegExp(rawSecret).test(artistBody.innerHTML)) violations.push('raw failure detail must stay hidden');
  return violations;
}

// Warm compare cache renders exactly once. Large artists produce megabytes of
// collapsed Other-release markup, so repainting the fast source-only page
// immediately before the complete catalogue is a visible performance bug.
t.section('a warm compare cache paints exactly once');
resetWorld();
{
  const aid = 'warm-discogs-id';
  state.browseSource = 'discogs';
  state.browseCache[aid] = {
    fast: {
      rgRes: { release_groups: [{
        id: 'fast-only', title: 'Fast only', source: 'discogs',
        identity_kind: 'work', provenance: ['ordinary'],
      }] },
      libRes: { albums: [] },
    },
    compare: {
      both: [], mb_unpaired: [], discogs_unpaired: [{
        id: 'complete', title: 'Complete catalogue', source: 'discogs',
        identity_kind: 'work', provenance: ['ordinary'],
      }], discogs_ungrouped_releases: [],
    },
    disamb: null,
  };
  let writes = 0;
  let rendered = '';
  Object.defineProperty(artistBody, 'innerHTML', {
    configurable: true,
    get() { return rendered; },
    set(value) { writes++; rendered = value; },
  });
  await loadArtistPage(aid, 'Warm Artist');
  t.equal(writes, 1, 'warm compare cache must not paint the fast page first');
  t.match(rendered, /Complete catalogue/,
    'the single paint carries the complete catalogue');
  t.notMatch(rendered, /Fast only/,
    'the single paint never shows the fast source-only page');
  delete artistBody.innerHTML;
  artistBody.innerHTML = rendered;
}

// Resolver identity is backend-authored. Equal master/release numbers are
// valid in separate Discogs namespaces and therefore prove nothing.
t.section('resolver identity is backend-authored');
{
  const groupedEqualId = {
    source: 'discogs', kind: 'release', expand_id: '122', leaf_id: '122',
    target_identity_kind: 'work',
  };
  const masterlessEqualId = {
    source: 'discogs', kind: 'release', expand_id: '122', leaf_id: '122',
    target_identity_kind: 'release',
  };
  t.equal(resolverTargetIdentityKind(groupedEqualId), 'work',
    'an equal-ID grouped release keeps its backend-authored work identity');
  t.equal(resolverTargetIdentityKind(masterlessEqualId), 'release',
    'an equal-ID masterless release keeps its backend-authored release identity');

  const equalityMutant = data => (
    String(data.expand_id) === String(data.leaf_id) ? 'release' : 'work'
  );
  t.equal(
    equalityMutant(groupedEqualId),
    'release',
    'known-bad equality heuristic misclassifies the grouped release',
  );
  t.notEqual(
    equalityMutant(groupedEqualId),
    resolverTargetIdentityKind(groupedEqualId),
    'the known-bad heuristic and the real resolver disagree on that release',
  );

  for (let id = 1; id <= 2000; id += 41) {
    for (const targetIdentityKind of ['work', 'release']) {
      const data = {
        source: 'discogs', kind: 'release',
        expand_id: String(id), leaf_id: String(id),
        target_identity_kind: targetIdentityKind,
      };
      t.equal(
        resolverTargetIdentityKind(data),
        targetIdentityKind,
        `explicit target survives equal-ID world ${id}/${targetIdentityKind}`,
      );
    }
  }

  // The harness counts rather than throws, so a thrown MESSAGE is asserted
  // through an explicit catch instead of `t.throws`, which matches on class.
  let missingTarget = null;
  try {
    resolverTargetIdentityKind({ expand_id: '122', leaf_id: '122' });
  } catch (error) {
    missingTarget = error;
  }
  t.match(
    missingTarget && missingTarget.message,
    /missing target_identity_kind/,
    'an equal-ID payload with no explicit target is refused by name',
  );
}

// Known-bad qualification: every clause of the safe-retry checker is proven
// to trip, in a world built so that the exact violation list is assertable.
t.section('known-bad qualification of the safe-retry checker');

resetWorld();
artistBody.innerHTML = '<button onclick="window.reloadBrowseArtist()">Retry</button>';
t.deepEqual(
  safeRetryFailureViolations('clean', 'raw-known-bad-secret'), [],
  'a cache-free, correctly wired, secret-free Retry state has no violations',
);

resetWorld();
state.browseCache.bad = { fast: {} };
artistBody.innerHTML = '<button onclick="window.reloadBrowseArtist()">Retry</button>';
t.deepEqual(
  safeRetryFailureViolations('bad', 'raw-known-bad-secret'),
  ['failed fast pair must not populate cache'],
  'a populated cache trips only the cache clause',
);

resetWorld();
artistBody.innerHTML = '<button onclick="window.reloadBrowseArtist()">Try again</button>';
t.deepEqual(
  safeRetryFailureViolations('bad', 'raw-known-bad-secret'),
  ['failure state must offer Retry'],
  'a wired control that never says Retry trips only the Retry clause',
);

resetWorld();
artistBody.innerHTML = '<button onclick="window.openBrowseArtist()">Retry</button>';
t.deepEqual(
  safeRetryFailureViolations('bad', 'raw-known-bad-secret'),
  ['Retry must call the existing public reload binding'],
  'a Retry wired to another binding trips only the binding clause',
);

resetWorld();
artistBody.innerHTML = '<button onclick="window.reloadBrowseArtist()">Retry</button> raw-known-bad-secret';
t.deepEqual(
  safeRetryFailureViolations('bad', 'raw-known-bad-secret'),
  ['raw failure detail must stay hidden'],
  'a leaked raw detail trips only the leakage clause',
);

resetWorld();
state.browseCache.bad = { fast: {} };
artistBody.innerHTML = '<div>raw-known-bad-secret</div>';
t.deepEqual(
  safeRetryFailureViolations('bad', 'raw-known-bad-secret'),
  [
    'failed fast pair must not populate cache',
    'failure state must offer Retry',
    'Retry must call the existing public reload binding',
    'raw failure detail must stay hidden',
  ],
  'a wholly unsafe world accumulates every clause instead of short-circuiting',
);

// Deterministic pin: the motivating MusicBrainz 503 body never becomes data.
t.section('MusicBrainz 503 pin');
resetWorld();
{
  const aid = 'mb-503-pin';
  const rawSecret = 'SSL UNEXPECTED_EOF private upstream detail';
  stubGlobals({ fetch: async (url) => url.includes('/api/library/artist')
    ? response(200, { albums: [] })
    : response(503, {
      error: 'MusicBrainz fallback unavailable, retry',
      retryable: true,
      raw: rawSecret,
    }) });
  await loadArtistPage(aid, 'Transport Failure');
  t.deepEqual(safeRetryFailureViolations(aid, rawSecret), [],
    'a MusicBrainz 503 artist response leaves a safe Retry state');
}

// Independent pin: the library half failing is just as cache-safe.
t.section('library 500 pin');
resetWorld();
{
  const aid = 'library-500-pin';
  const rawSecret = 'raw downstream database exception';
  stubGlobals({ fetch: async (url) => url.includes('/api/library/artist')
    ? response(500, { error: rawSecret })
    : response(200, { release_groups: [] }) });
  await loadArtistPage(aid, 'Library Failure');
  t.deepEqual(safeRetryFailureViolations(aid, rawSecret), [],
    'a library 500 response leaves a safe Retry state');
}

// Independent pin: rejected fetch promises use the same stable Retry state.
t.section('rejected fetch pin');
resetWorld();
{
  const aid = 'network-pin';
  const rawSecret = 'socket exploded at 10.0.0.9';
  stubGlobals({ fetch: async () => { throw new Error(rawSecret); } });
  await loadArtistPage(aid, 'Network Failure');
  t.deepEqual(safeRetryFailureViolations(aid, rawSecret), [],
    'a rejected fetch promise leaves a safe Retry state');
}

// Retry wiring: the public reload deletes any old artist cache and re-fetches.
t.section('Retry wiring re-fetches and clears the old cache');
resetWorld();
{
  const aid = 'retry-pin';
  let fetchCount = 0;
  state.browseArtist = { id: aid, name: 'Retry Artist' };
  state.browseCache[aid] = { stale: true };
  stubGlobals({ fetch: async () => {
    fetchCount++;
    throw new Error('still unavailable');
  } });
  reloadBrowseArtist();
  await new Promise(resolve => setImmediate(resolve));
  t.equal(state.browseCache[aid], undefined, 'Retry deletes the stale artist cache entry');
  t.ok(fetchCount > 0, 'Retry binding must start a fresh fetch');
  t.match(artistBody.innerHTML, />Retry</, 'a still-failing Retry re-renders Retry');
}

// Generated/property sweep: every non-OK status on either fast response keeps
// the same cache-safe, non-leaking Retry contract.
t.section('generated sweep: every non-OK fast response');
for (const status of [400, 401, 403, 404, 408, 409, 418, 429, 500, 502, 503, 504, 599]) {
  for (const failedPart of ['artist', 'library']) {
    resetWorld();
    const aid = `generated-${failedPart}-${status}`;
    const rawSecret = `raw-${failedPart}-secret-${status}`;
    stubGlobals({ fetch: async (url) => {
      const isLibrary = url.includes('/api/library/artist');
      const shouldFail = failedPart === 'library' ? isLibrary : !isLibrary;
      if (shouldFail) return response(status, { error: rawSecret, retryable: status === 503 });
      return response(200, isLibrary ? { albums: [] } : { release_groups: [] });
    } });
    await loadArtistPage(aid, `Generated ${status}`);
    t.deepEqual(safeRetryFailureViolations(aid, rawSecret), [],
      `a failing ${failedPart} half at ${status} leaves a safe Retry state`);
  }
}

// Stale-token pin: after a newer load owns the page, the older transport
// failure may resolve but cannot overwrite the active Retry state.
t.section('stale-token pin');
resetWorld();
{
  const pending = [];
  stubGlobals({ fetch: () => new Promise(resolve => pending.push(resolve)) });
  const oldLoad = loadArtistPage('old-artist', 'Old Artist');
  t.equal(pending.length, 3, 'old load starts compare beside both fast requests');

  stubGlobals({ fetch: async () => { throw new Error('new active failure'); } });
  await loadArtistPage('new-artist', 'New Artist');
  const activeHtml = artistBody.innerHTML;

  for (const resolve of pending) {
    resolve(response(503, { error: 'raw stale failure', retryable: true }));
  }
  await oldLoad;
  t.equal(artistBody.innerHTML, activeHtml, 'stale failure must not replace active content');
  t.equal(state.browseCache['old-artist'], undefined,
    'the stale load leaves no cache entry for its own artist');
}

// Cold request-graph pin: compare starts immediately, but an early compare
// response remains an in-flight handoff until the useful pair succeeds.  A
// failed useful pair must therefore never leave a partial cache entry.
t.section('cold request-graph pin');
resetWorld();
{
  const aid = 'early-compare';
  const artist = deferred();
  const library = deferred();
  stubGlobals({ fetch: (url) => {
    if (url.includes('/api/artist/compare?')) {
      return Promise.resolve(response(200, {
        both: [], mb_unpaired: [{
          id: 'compare-only', title: 'Compare only', source: 'mb',
          identity_kind: 'work', provenance: ['ordinary'],
        }], discogs_unpaired: [], discogs_ungrouped_releases: [],
      }));
    }
    if (url.includes('/api/library/artist')) return library.promise;
    if (url.includes('/disambiguate')) return Promise.resolve(response(503, {}));
    return artist.promise;
  } });
  const load = loadArtistPage(aid, 'Early Compare');
  await new Promise(resolve => setImmediate(resolve));
  t.equal(state.browseCache[aid], undefined, 'early compare cannot create partial cache');
  artist.resolve(response(200, { release_groups: [] }));
  library.resolve(response(200, { albums: [] }));
  await load;
  t.equal(state.browseCache[aid].compare.mb_unpaired[0].id, 'compare-only',
    'the successful useful pair publishes the held compare payload');
  t.match(artistBody.innerHTML, /Compare only/,
    'the published compare payload paints into the artist page');
}

// Source-switch race pin: invalidation happens before the cross-source artist
// lookup awaits. An old MB failure cannot paint Retry after Discogs becomes
// the active source, and the resulting current load/retry uses the Discogs id.
t.section('source-switch race pin');
resetWorld();
{
  const oldArtist = deferred();
  const oldLibrary = deferred();
  const sourceLookup = deferred();
  const requests = [];
  state.browseSource = 'mb';
  state.browseArtist = { id: 'old-mb-id', name: 'Race Artist' };
  stubGlobals({ fetch: (url) => {
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
  } });

  const oldLoad = loadArtistPage('old-mb-id', 'Race Artist');
  const sourceSwitch = setBrowseSource('discogs');
  oldArtist.resolve(response(503, { error: 'stale MB failure', retryable: true }));
  oldLibrary.resolve(response(200, { albums: [] }));
  await oldLoad;
  t.notMatch(
    artistBody.innerHTML,
    />Retry</,
    'old-source failure must be stale as soon as source switching starts',
  );

  sourceLookup.resolve(response(200, {
    artists: [{ id: 'new-discogs-id', name: 'Race Artist' }],
  }));
  await sourceSwitch;
  await new Promise(resolve => setImmediate(resolve));
  t.deepEqual(state.browseArtist, { id: 'new-discogs-id', name: 'Race Artist' },
    'the cross-source match becomes the active artist');
  t.match(
    elements['source-hint'].innerHTML,
    /Discogs<\/b> identities selected for expansions and actions/,
    'source hint states the exact selected-identity contract',
  );
  t.notMatch(
    elements['source-hint'].innerHTML,
    /unpaired|ungrouped/i,
    'source hint does not expose comparison topology',
  );
  t.ok(requests.some(url => url.includes('/api/discogs/artist/new-discogs-id?')),
    'the switch loads the new Discogs artist endpoint');
  t.match(artistBody.innerHTML, />Retry</, 'current-source failure owns Retry');

  requests.length = 0;
  stubGlobals({ fetch: async (url) => {
    requests.push(url);
    throw new Error('retry remains unavailable');
  } });
  reloadBrowseArtist();
  await new Promise(resolve => setImmediate(resolve));
  t.ok(requests.some(url => url.includes('/api/discogs/artist/new-discogs-id?')),
    'Retry re-fetches the Discogs artist endpoint');
  t.ok(requests.some(url => url === '/api/library/artist?name=Race%20Artist'),
    'Retry re-fetches the library half by artist name');
  t.notOk(requests.some(url => url.includes('old-mb-id')),
    'Retry never re-fetches the superseded MB id');
}

// Generated/property sweep around the race: either fast-pair half, several
// failure classes, and both source directions remain stale while lookup waits.
t.section('generated sweep: source-switch race');
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
      stubGlobals({ fetch: (url) => {
        if (url.includes('/api/artist/compare?')) {
          return Promise.resolve(response(503, { error: 'decoration unavailable' }));
        }
        const call = callIndex++;
        if (call === 0) return oldFastA.promise;
        if (call === 1) return oldFastB.promise;
        if (call === 2) return sourceLookup.promise;
        throw new Error(`unexpected generated race fetch ${call}`);
      } });

      const oldLoad = loadArtistPage(`old-${oldSource}`, 'Generated Race');
      const sourceSwitch = setBrowseSource(newSource);
      oldFastA.resolve(failedPart === 'artist'
        ? response(status, { error: 'stale generated failure' })
        : response(200, { release_groups: [] }));
      oldFastB.resolve(failedPart === 'library'
        ? response(status, { error: 'stale generated failure' })
        : response(200, { albums: [] }));
      await oldLoad;
      t.notMatch(artistBody.innerHTML, />Retry</,
        `${oldSource}->${newSource} ${failedPart} ${status} keeps the old failure stale`);

      sourceLookup.resolve(response(200, { artists: [] }));
      await sourceSwitch;
      t.equal(state.browseArtist, null,
        `${oldSource}->${newSource} ${failedPart} ${status} closes the artist on no match`);
    }
  }
}

// Double-toggle ownership pin: lookups can resolve out of order. The newest
// MB toggle must keep its MB artist/id/endpoint even if the older Discogs
// lookup returns last with a valid but wrong-source match.
t.section('double-toggle ownership pin');
resetWorld();
{
  const discogsLookup = deferred();
  const mbLookup = deferred();
  const requests = [];
  state.browseSource = 'mb';
  state.browseArtist = { id: 'starting-mb-id', name: 'Toggle Artist' };
  stubGlobals({ fetch: (url) => {
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
  } });

  const olderToggle = setBrowseSource('discogs');
  const newestToggle = setBrowseSource('mb');
  mbLookup.resolve(response(200, {
    artists: [{ id: 'newest-mb-id', name: 'Toggle Artist' }],
  }));
  await newestToggle;
  await new Promise(resolve => setImmediate(resolve));
  const newestHtml = artistBody.innerHTML;
  t.equal(state.browseSource, 'mb', 'the newest toggle owns the active source');
  t.deepEqual(state.browseArtist, { id: 'newest-mb-id', name: 'Toggle Artist' },
    'the newest toggle owns the active artist');
  t.ok(requests.some(url => url.includes('/api/artist/newest-mb-id?')),
    'the newest toggle loads its own MB artist endpoint');
  t.notMatch(newestHtml, />Retry</, 'the newest toggle renders a successful page');

  discogsLookup.resolve(response(200, {
    artists: [{ id: 'stale-discogs-id', name: 'Toggle Artist' }],
  }));
  await olderToggle;
  await new Promise(resolve => setImmediate(resolve));
  t.equal(state.browseSource, 'mb', 'a late older lookup must not change the active source');
  t.deepEqual(
    state.browseArtist,
    { id: 'newest-mb-id', name: 'Toggle Artist' },
    'older lookup must not overwrite the newest source artist',
  );
  t.notOk(requests.some(url => url.includes('stale-discogs-id')),
    'the older lookup never drives a load of its own match');
  t.equal(artistBody.innerHTML, newestHtml, 'older lookup must not repaint the newest page');
}

// Generated/property sweep for both directions. The newest lookup may own a
// Retry state, but resolving the older valid match cannot change its artist,
// endpoint family, or rendered content.
t.section('generated sweep: double-toggle ownership');
for (const newestSource of ['mb', 'discogs']) {
  resetWorld();
  const olderSource = newestSource === 'mb' ? 'discogs' : 'mb';
  const lookups = { mb: deferred(), discogs: deferred() };
  const newestId = `newest-${newestSource}-id`;
  const staleId = `stale-${olderSource}-id`;
  const requests = [];
  state.browseSource = newestSource;
  state.browseArtist = { id: `starting-${newestSource}-id`, name: 'Generated Toggle' };
  stubGlobals({ fetch: (url) => {
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
  } });

  const olderToggle = setBrowseSource(olderSource);
  const newestToggle = setBrowseSource(newestSource);
  lookups[newestSource].resolve(response(200, {
    artists: [{ id: newestId, name: 'Generated Toggle' }],
  }));
  await newestToggle;
  await new Promise(resolve => setImmediate(resolve));
  const newestHtml = artistBody.innerHTML;
  t.match(newestHtml, />Retry</,
    `${newestSource} newest toggle owns a Retry state after its own failure`);

  lookups[olderSource].resolve(response(200, {
    artists: [{ id: staleId, name: 'Generated Toggle' }],
  }));
  await olderToggle;
  await new Promise(resolve => setImmediate(resolve));
  t.equal(state.browseSource, newestSource,
    `${newestSource} stays the active source after the older lookup resolves`);
  t.deepEqual(state.browseArtist, { id: newestId, name: 'Generated Toggle' },
    `${newestSource} keeps its own artist after the older lookup resolves`);
  const expectedEndpoint = newestSource === 'discogs'
    ? `/api/discogs/artist/${newestId}?`
    : `/api/artist/${newestId}?`;
  t.ok(requests.some(url => url.includes(expectedEndpoint)),
    `${newestSource} loaded its own endpoint family`);
  t.notOk(requests.some(url => url.includes(staleId)),
    `${newestSource} never loaded the older lookup's match`);
  t.equal(artistBody.innerHTML, newestHtml,
    `${newestSource} page content survives the older lookup`);
}

// Search result rows wire their onclick handlers with the exact argument
// order (#1110/#1241 argument-inversion class): window.openBrowseArtist on
// artist rows and both arms of the release-row ternary — openBrowseArtist
// for ordinary releases, loadReleaseGroup for VA/masterless ones.
t.section('search row onclick wiring');
resetWorld();
{
  state.browseSearchType = 'artist';
  stubGlobals({ fetch: async url => {
    t.contains(url, '/api/search?q=', 'artist search hits the MB search endpoint');
    return response(200, {
      artists: [{ id: 'a1', name: 'ArtName', disambiguation: '' }],
    });
  } });
  await searchArtists('artname');
  t.match(elements.results.innerHTML,
    /onclick="window\.openBrowseArtist\(&quot;a1&quot;, &quot;ArtName&quot;\)"/,
    'artist row onclick carries (artist id, artist name) in order');
}

resetWorld();
{
  state.browseSearchType = 'release';
  stubGlobals({ fetch: async () => response(200, {
    release_groups: [{
      id: 'rg1',
      artist_id: 'a2',
      artist_name: 'RelArtist',
      title: 'RelTitle',
      primary_type: 'Album',
    }],
  }) });
  await searchArtists('reltitle');
  t.match(elements.results.innerHTML,
    /onclick="window\.openBrowseArtist\(&quot;a2&quot;, &quot;RelArtist&quot;\)"/,
    'non-VA release row onclick routes to the artist page with (id, name) in order');
  state.browseSearchType = 'artist';
}

resetWorld();
{
  state.browseSearchType = 'release';
  state.browseSource = 'discogs';
  stubGlobals({ fetch: async () => response(200, {
    release_groups: [{
      id: 'm1',
      discogs_release_id: 'dr9',
      artist_id: 'a3',
      artist_name: 'NoMaster',
      title: 'Masterless',
      is_master: false,
    }],
  }) });
  await searchArtists('masterless');
  t.match(elements.results.innerHTML,
    /onclick="window\.loadReleaseGroup\(&quot;dr9&quot;, this, \{source:'discogs',identityKind:'release',masterless:true\}\)"/,
    'masterless Discogs release row onclick carries (discogs release id, this, load opts) in order');
  state.browseSearchType = 'artist';
  state.browseSource = 'mb';
}

resetWorld();
{
  // The VA disjunct of the same ternary: a Various Artists release group
  // must route to loadReleaseGroup too, never to a dead-end artist page.
  state.browseSearchType = 'release';
  stubGlobals({ fetch: async () => response(200, {
    release_groups: [{
      id: 'rgva',
      artist_id: '89ad4ac3-39f7-470e-963a-56509c546377',
      artist_name: 'Various Artists',
      title: 'VA Comp',
    }],
  }) });
  await searchArtists('va comp');
  t.match(elements.results.innerHTML,
    /onclick="window\.loadReleaseGroup\(&quot;rgva&quot;, this, \{source:'mb',identityKind:'work'\}\)"/,
    'VA release row onclick routes to loadReleaseGroup with (release group id, this, load opts)');
  state.browseSearchType = 'artist';
}

t.done();
