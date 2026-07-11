// @ts-check
import { state, API, toast } from './state.js';
import { esc, jsArg, parsePastedId } from './util.js';
import { loadReleaseGroup, renderReleaseDetail, applySearchTargetAfterDiscography } from './discography.js';
import { applyAnalysisChips, applyAnalysisToOpenExpansions } from './analysis.js';
import { classifyArtistRows, renderArtistSections, renderOtherSourceSection } from './artist_page.js';
import { searchLabels, renderLabelSearchResults, openLabelDetail, closeLabelDetail } from './labels.js';

/**
 * Look up an artist on the requested source by name. Returns the best match
 * (exact name preferred, else top-scored result), or null if no hits.
 * @param {string} name
 * @param {string} src - 'mb' or 'discogs'
 * @returns {Promise<{id:string, name:string}|null>}
 */
async function findArtistOnSource(name, src) {
  const url = src === 'discogs'
    ? `${API}/api/discogs/search?q=${encodeURIComponent(name)}&type=artist`
    : `${API}/api/search?q=${encodeURIComponent(name)}`;
  try {
    const r = await fetch(url);
    const data = await r.json();
    const artists = data.artists || [];
    if (!artists.length) return null;
    const lc = name.toLowerCase();
    const exact = artists.find(a => (a.name || '').toLowerCase() === lc);
    return exact || artists[0];
  } catch (_e) {
    return null;
  }
}

/**
 * Reflect the active metadata source in the toggle buttons + the hint line.
 * The selected source is the PRIMARY discography; the other source only
 * fills in releases the primary is missing (the appended "Only on <other>"
 * section). Called from every place that changes browseSource so the
 * primary/complement story never goes stale. Single source of truth for
 * the source-toggle chrome — the two call sites used to duplicate the
 * button-class flip and neither touched the hint.
 * @param {string} src - 'mb' | 'discogs'
 */
function applySourceUI(src) {
  const mbBtn = document.getElementById('source-mb');
  const dgBtn = document.getElementById('source-discogs');
  if (mbBtn) mbBtn.className = 'p-btn' + (src === 'mb' ? ' active-status' : '');
  if (dgBtn) dgBtn.className = 'p-btn' + (src === 'discogs' ? ' active-status' : '');
  const hint = document.getElementById('source-hint');
  if (hint) {
    hint.innerHTML = src === 'discogs'
      ? '<b class="src-primary">Discogs</b> is primary · MusicBrainz fills the rest (shown as "Only on MusicBrainz" below)'
      : '<b class="src-primary">MusicBrainz</b> is primary · Discogs fills the rest (shown as "Only on Discogs" below)';
  }
}

/**
 * Set the browse metadata source (mb or discogs). Preserves artist context
 * when possible: if an artist is currently selected, look up the equivalent
 * on the new source and re-render in place instead of dumping back to search.
 * @param {string} src - 'mb' or 'discogs'
 */
export async function setBrowseSource(src) {
  if (state.browseSource === src) return;
  searchArtistsRequestToken++;
  // Explicit user source-toggle clears any active search-by-ID ring.
  // The source-guard in applySearchTargetAfterDiscography would mask
  // the immediate symptom, but a paste→toggle-twice sequence (away
  // and back) would re-apply a stale ring after the user explicitly
  // changed contexts.
  clearSearchTarget();
  state.browseSource = src;
  applySourceUI(src);
  state.browseCache = {};

  // Sticky artist context across the toggle.
  if (state.browseArtist) {
    const prevName = state.browseArtist.name;
    const match = await findArtistOnSource(prevName, src);
    if (match) {
      state.browseArtist = { id: String(match.id), name: match.name };
      document.getElementById('browse-artist-name').textContent = match.name;
      loadArtistPage(String(match.id), match.name);
      return;
    }
    toast(`No ${src === 'discogs' ? 'Discogs' : 'MusicBrainz'} match for ${prevName}`, true);
    state.browseArtist = null;
    artistPageToken++;
    document.getElementById('browse-artist').style.display = 'none';
  }

  const q = /** @type {HTMLInputElement} */ (document.getElementById('q')).value.trim();
  if (q.length >= 2) searchArtists(q);
}

/**
 * Set the browse search type (artist, release, label, or id).
 * @param {string} type - 'artist' | 'release' | 'label' | 'id'
 */
export function setSearchType(type) {
  searchArtistsRequestToken++;
  state.browseSearchType = type;
  // Switching modes clears any active search-by-ID ring (R15).
  clearSearchTarget();
  const btnIds = {
    artist: 'search-type-artist',
    release: 'search-type-release',
    label: 'search-type-label',
    id: 'search-type-id',
  };
  for (const [t, id] of Object.entries(btnIds)) {
    const el = document.getElementById(id);
    if (el) el.className = 'p-btn' + (type === t ? ' active-status' : '');
  }
  const placeholder = type === 'artist'
    ? 'Search artists or albums...'
    : type === 'release'
      ? 'Search album titles...'
      : type === 'label'
        ? 'Search record labels...'
        : 'Paste MBID, Discogs release/master ID, or URL...';
  /** @type {HTMLInputElement} */ (document.getElementById('q')).placeholder = placeholder;
  // Hide label-detail view when switching modes (search results take focus).
  if (state.browseLabel) closeLabelDetail();
  // Re-trigger search if there's a query
  const q = /** @type {HTMLInputElement} */ (document.getElementById('q')).value.trim();
  if (q.length >= 2) searchArtists(q);
}

/**
 * Open the browse artist detail view.
 * @param {string} id - MusicBrainz artist ID
 * @param {string} name - Artist name
 */
const VA_MBID = '89ad4ac3-39f7-470e-963a-56509c546377';
let searchArtistsRequestToken = 0;

export function cancelBrowseSearch() {
  searchArtistsRequestToken++;
}

export function openBrowseArtist(id, name) {
  if (id === VA_MBID) {
    setSearchType('release');
    toast('Various Artists has too many releases — search by album title instead');
    return;
  }
  state.browseArtist = {id, name};
  document.getElementById('results').style.display = 'none';
  document.getElementById('browse-artist').style.display = 'block';
  document.getElementById('browse-artist-name').textContent = name;
  loadArtistPage(id, name);
}

/**
 * Close the browse artist detail view and show search results.
 */
export function closeBrowseArtist() {
  state.browseArtist = null;
  // Cancel in-flight artist-page loads + decoration fetches.
  artistPageToken++;
  // Closing the artist view clears any search-by-ID ring (R15).
  clearSearchTarget();
  document.getElementById('browse-artist').style.display = 'none';
  document.getElementById('results').style.display = 'block';
}

/**
 * Clear the search-by-ID target so subsequent renders don't apply the ring.
 * Used by close-artist-view, mode-switch, and as the first step of each
 * new paste resolution.
 */
export function clearSearchTarget() {
  state.searchTargetId = null;
  state.searchTargetExpandId = null;
  state.searchTargetSource = null;
}

/**
 * Resolve a pasted MBID / Discogs ID / URL via /api/browse/resolve and
 * drive the artist view (or VA fallback) accordingly. Called from
 * searchArtists when state.browseSearchType === 'id'.
 *
 * @param {string} q - Pasted text (raw, untrimmed; parsePastedId handles it)
 * @param {number} requestToken - Token from the calling searchArtists; if
 *   it doesn't match searchArtistsRequestToken when the resolver returns,
 *   the response is stale (a newer paste superseded this one) and discarded.
 */
export async function resolveAndNavigate(q, requestToken) {
  const el = document.getElementById('results');
  const parsed = parsePastedId(q);
  if (!parsed) {
    el.innerHTML = '<div class="loading">Not a recognised ID. Paste a MusicBrainz UUID, Discogs release/master ID, or URL.</div>';
    return;
  }
  // New paste — clear any previous ring before navigation begins.
  clearSearchTarget();
  el.innerHTML = '<div class="loading">Resolving...</div>';
  let data;
  try {
    const url = `${API}/api/browse/resolve?source=${parsed.family}&id=${encodeURIComponent(parsed.id)}&kind=${parsed.kind}`;
    const r = await fetch(url);
    if (requestToken !== searchArtistsRequestToken) return;
    if (!r.ok) {
      el.innerHTML = `<div class="loading">Resolve failed (HTTP ${r.status}). The ID may not exist on the ${parsed.family === 'mb' ? 'MusicBrainz' : 'Discogs'} mirror.</div>`;
      return;
    }
    data = await r.json();
  } catch (_e) {
    if (requestToken !== searchArtistsRequestToken) return;
    el.innerHTML = '<div class="loading">Resolve failed (network error).</div>';
    return;
  }
  if (requestToken !== searchArtistsRequestToken) return;

  // Switch source to match the resolved ID's family before opening the
  // artist view, otherwise the discography fetch would hit the wrong API.
  if (state.browseSource !== parsed.family) {
    state.browseSource = parsed.family;
    applySourceUI(parsed.family);
    state.browseCache = {};
  }

  if (data.is_va) {
    // VA fallback: bypass the artist view (the VA artist page is unworkably
    // large) and render a single-release detail card or a master/release-group
    // pressings list directly. Plan U5. The ring-target state fields are
    // unused on this path (openVaFallback consumes data.leaf_id / data.expand_id
    // directly), so we deliberately don't set them — that way state stays
    // dormant rather than load-bearing on every entry point remembering
    // to clear it.
    openVaFallback(data, parsed.id, requestToken);
    return;
  }

  // Non-VA: stash ring targets BEFORE openBrowseArtist triggers the discography
  // render — the render hook reads these to decide what to expand and ring.
  state.searchTargetId = data.leaf_id || null;
  state.searchTargetExpandId = data.expand_id || null;
  state.searchTargetSource = data.source;

  // Force a discography re-render even if the artist is already cached.
  // Without this, two consecutive pastes for different releases on the
  // same artist would leave the first paste's ring in place and skip
  // the post-render hook on the second paste.
  if (data.artist_id) {
    delete state.browseCache[data.artist_id];
  }

  // Hand off to the existing artist view. The render hooks in
  // discography.js pick up state.searchTargetExpandId / state.searchTargetId
  // and apply the ring after the discography + master expansion render.
  openBrowseArtist(data.artist_id, data.artist_name);
}

/**
 * Render the Various Artists fallback card. Branches on data.kind:
 *  - 'release'        → single-release detail via renderReleaseDetail.
 *  - 'master'         → Discogs master title + pressings list (loadReleaseGroup).
 *  - 'release-group'  → MB release-group title + pressings list (loadReleaseGroup).
 *
 * Shows the va-fallback container and hides the artist view + search results.
 *
 * In-flight token guard: each await is gated by `requestToken !==
 * searchArtistsRequestToken`. Unlike the artist-view path where a re-render
 * detaches prior #rel-X nodes (so a stale write goes to detached DOM and is
 * harmless), va-fallback-body is a stable, never-replaced node — a stale
 * fetch landing here would scribble onto a fresh card. The isStale callback
 * is also threaded into loadReleaseGroup so the nested write is protected.
 *
 * @param {Object} data - resolver response
 * @param {string} parsedId - the raw pasted id (used as the leaf for kind='release')
 * @param {number} requestToken - in-flight token from the calling searchArtists
 */
async function openVaFallback(data, parsedId, requestToken) {
  const wrap = document.getElementById('va-fallback');
  const titleEl = document.getElementById('va-fallback-title');
  const bodyEl = document.getElementById('va-fallback-body');
  if (!wrap || !titleEl || !bodyEl) return;
  const isStale = () => requestToken !== searchArtistsRequestToken;

  // Hide other Browse views. Also blank the artist-view discography so its
  // duplicate `id="reldet-${rel.id}"` nodes don't shadow the VA fallback's
  // own — getElementById returns the first match in document order, and
  // the hidden artist view DOM is preserved (display:none, not removed).
  document.getElementById('results').style.display = 'none';
  document.getElementById('browse-artist').style.display = 'none';
  artistPageToken++;
  const dgEl = document.getElementById('browse-artist-body');
  if (dgEl) dgEl.innerHTML = '';
  const browseLabel = document.getElementById('browse-label');
  if (browseLabel) browseLabel.style.display = 'none';
  wrap.style.display = 'block';
  bodyEl.innerHTML = '<div class="loading">Loading...</div>';
  titleEl.textContent = 'Loading…';

  try {
    if (data.kind === 'release') {
      const releaseId = data.leaf_id || parsedId;
      const url = data.source === 'discogs'
        ? `${API}/api/discogs/release/${encodeURIComponent(releaseId)}`
        : `${API}/api/release/${encodeURIComponent(releaseId)}`;
      const r = await fetch(url);
      if (isStale()) return;
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const releaseData = await r.json();
      if (isStale()) return;
      titleEl.textContent = releaseData.title || 'Various Artists';
      bodyEl.innerHTML = '';
      // Pass an explicit artist override so the action toolbar's artist
      // label doesn't fall through to whatever state.browseArtist points
      // at (which on the VA path is the previously-viewed non-VA artist).
      renderReleaseDetail(bodyEl, releaseId, releaseData, {
        artist: releaseData.artist_name || data.artist_name || 'Various Artists',
      });
      return;
    }
    // Master (Discogs) or release-group (MB) — render pressings via the
    // existing loadReleaseGroup, which gives each row an Add button and
    // the same toggleReleaseDetail expansion as the artist view.
    const groupId = data.expand_id;
    const groupUrl = data.source === 'discogs'
      ? `${API}/api/discogs/master/${encodeURIComponent(groupId)}`
      : `${API}/api/release-group/${encodeURIComponent(groupId)}`;
    const r = await fetch(groupUrl);
    if (isStale()) return;
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const groupData = await r.json();
    if (isStale()) return;
    titleEl.textContent = groupData.title || 'Various Artists';
    // Reuse loadReleaseGroup with an explicit target. It re-fetches the
    // same endpoint internally; that's a duplicate request but only ~80ms
    // for MB and ~20ms for Discogs, and it keeps the render path identical
    // to the artist view's expansion path. Pass source explicitly so the
    // helper hits the right API regardless of state.browseSource. isStale
    // is threaded so a stale fetch can't write into our live bodyEl.
    bodyEl.innerHTML = '';
    await loadReleaseGroup(groupId, bodyEl, {
      targetEl: bodyEl,
      source: data.source,
      isStale,
    });
    if (isStale()) {
      // loadReleaseGroup's own isStale checks should have prevented any
      // write, but blank the card defensively in case any partial render
      // landed before the guard fired.
      bodyEl.innerHTML = '';
    }
  } catch (_e) {
    if (isStale()) return;
    bodyEl.innerHTML = '<div class="loading">Failed to load Various Artists fallback.</div>';
    titleEl.textContent = 'Various Artists';
  }
}

/**
 * Close the VA fallback card and return to search results.
 */
export function closeVaFallback() {
  clearSearchTarget();
  const wrap = document.getElementById('va-fallback');
  if (wrap) wrap.style.display = 'none';
  document.getElementById('results').style.display = 'block';
}

/**
 * Clear cached data for the current browse artist so sub-views re-fetch.
 * Call after any mutation (add to pipeline, delete, ban, etc.).
 */
export function invalidateBrowseArtist() {
  if (state.browseArtist) {
    delete state.browseCache[state.browseArtist.id];
  }
}

/**
 * In-flight token for the unified artist page. Incremented on every
 * load and on close, so stale fast-pair renders and late decoration
 * fetches (compare / disambiguate) can never write over a newer page.
 */
let artistPageToken = 0;

/**
 * Reload the current browse artist's page from scratch (cache dropped).
 * Bound on window for post-mutation refreshes (e.g. beets deletion).
 */
export function reloadBrowseArtist() {
  if (!state.browseArtist) return;
  delete state.browseCache[state.browseArtist.id];
  loadArtistPage(state.browseArtist.id, state.browseArtist.name);
}

/**
 * Fetch one half of the artist page's fast pair as JSON.
 * A JSON error body is not artist data: reject it before the pair can be
 * cached or rendered. The caller deliberately renders a stable message
 * instead of exposing response or exception details.
 * @param {string} url
 * @returns {Promise<Object>}
 */
async function fetchArtistPageJson(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`artist page request failed (${response.status})`);
  return response.json();
}

/** Render the stable, retryable artist-page failure state. */
function renderArtistPageFailure(el) {
  el.innerHTML = `
    <div class="loading artist-load-error">
      <div>Artist page temporarily unavailable.</div>
      <button class="p-btn" style="margin-top:10px" onclick="window.reloadBrowseArtist()">Retry</button>
    </div>`;
}

/**
 * Load and render the unified artist page (issue #575 PR4).
 *
 * Fast pair first — the source discography (?name= so the backend
 * annotates in_library) and the library feed — rendered as sections.
 * Then two slow feeds decorate the rendered page in the background:
 * the MB↔Discogs compare complement and the unique-track analysis.
 * Both are token-guarded and cached per artist.
 *
 * @param {string} aid - MB artist UUID or numeric Discogs artist ID
 * @param {string} name - Artist name
 */
export async function loadArtistPage(aid, name) {
  const token = ++artistPageToken;
  const el = document.getElementById('browse-artist-body');
  if (!el) return;

  const cached = state.browseCache[aid];
  if (cached && cached.fast) {
    renderUnified(el, aid, name, cached.fast.rgRes, cached.fast.libRes);
    // Re-fire any decoration that never landed (navigated away before
    // the fetch returned, or a transient failure) — a null slot must
    // not become null-forever on cache hits.
    if (cached.disamb) {
      state.disambData = cached.disamb;
      applyAnalysisChips(el, cached.disamb);
    } else {
      fireAnalysis(el, aid, token);
    }
    if (cached.compare) {
      appendOtherSourceSection(el, name, cached.compare);
    } else {
      fireCompareComplement(el, aid, name, token);
    }
    return;
  }

  el.innerHTML = '<div class="loading">Loading discography...</div>';
  try {
    const isDiscogs = state.browseSource === 'discogs';
    // Pass ?name= to the discography endpoint so the backend can mark
    // each row with in_library (otherwise the row-level "in library"
    // badge stays off — the backend won't make the extra MB lookup).
    const nameParam = `?name=${encodeURIComponent(name)}`;
    const artistUrl = isDiscogs
      ? `${API}/api/discogs/artist/${aid}${nameParam}`
      : `${API}/api/artist/${aid}${nameParam}`;
    // Beets only stores MB UUIDs in mb_albumartistid; sending the numeric
    // Discogs ID would skip the UUID match and only return Discogs-tagged
    // albums, hiding the rest of the user's catalog. Pass empty mbid on the
    // Discogs path so the backend falls through to a pure name match.
    const libUrl = isDiscogs
      ? `${API}/api/library/artist?name=${encodeURIComponent(name)}`
      : `${API}/api/library/artist?name=${encodeURIComponent(name)}&mbid=${aid}`;
    const [rgRes, libRes] = await Promise.all([
      fetchArtistPageJson(artistUrl),
      fetchArtistPageJson(libUrl),
    ]);
    if (token !== artistPageToken) return;
    state.browseCache[aid] = { fast: { rgRes, libRes }, compare: null, disamb: null };
    renderUnified(el, aid, name, rgRes, libRes);
    // Fire-and-forget decorations; each guards on the token.
    fireCompareComplement(el, aid, name, token);
    fireAnalysis(el, aid, token);
  } catch (_e) {
    if (token !== artistPageToken) return;
    renderArtistPageFailure(el);
  }
}

/**
 * Section + render the fast pair, then apply the search-by-ID hook.
 * @param {HTMLElement} el
 * @param {string} aid
 * @param {string} name
 * @param {Object} rgRes - /api/[discogs/]artist response
 * @param {Object} libRes - /api/library/artist response
 */
function renderUnified(el, aid, name, rgRes, libRes) {
  const sections = classifyArtistRows({
    artistId: aid,
    artistName: name,
    releaseGroups: rgRes.release_groups || [],
    libraryAlbums: libRes.albums || [],
  });
  el.innerHTML = renderArtistSections(sections, { artistId: aid, artistName: name });
  applySearchTargetAfterDiscography(el);
}

/**
 * Background decoration 1: the MB↔Discogs compare complement. Appends
 * an "Only on <other source>" section with the deduped bucket from
 * /api/artist/compare. Silent on failure — a mirror-less host 503s and
 * the page simply stays single-source.
 * @param {HTMLElement} el
 * @param {string} aid
 * @param {string} name
 * @param {number} token
 */
async function fireCompareComplement(el, aid, name, token) {
  try {
    const isDiscogs = state.browseSource === 'discogs';
    const idParam = isDiscogs ? `discogs_id=${encodeURIComponent(aid)}` : `mbid=${encodeURIComponent(aid)}`;
    const r = await fetch(`${API}/api/artist/compare?name=${encodeURIComponent(name)}&${idParam}`);
    if (token !== artistPageToken || !r.ok) return;
    const data = await r.json();
    if (token !== artistPageToken) return;
    if (state.browseCache[aid]) state.browseCache[aid].compare = data;
    appendOtherSourceSection(el, name, data);
    // A search-by-ID target may live in the complement (e.g. a Discogs
    // release pasted while browsing MB) — re-run the ring hook now that
    // its row exists.
    applySearchTargetAfterDiscography(el);
  } catch (_e) { /* decoration only — the page is already rendered */ }
}

/**
 * Append the complement section (idempotent — skipped if present).
 * @param {HTMLElement} el
 * @param {string} name - Artist name
 * @param {Object} data - /api/artist/compare response
 */
function appendOtherSourceSection(el, name, data) {
  if (el.querySelector('#only-other-source')) return;
  const isDiscogs = state.browseSource === 'discogs';
  const rows = isDiscogs ? (data.mb_only || []) : (data.discogs_only || []);
  const html = renderOtherSourceSection(rows, {
    artistName: name,
    source: isDiscogs ? 'mb' : 'discogs',
  });
  if (html) el.insertAdjacentHTML('beforeend', html);
}

/**
 * Background decoration 2: unique-track analysis chips (MB artists
 * only — the disambiguate route needs MB recording IDs).
 * @param {HTMLElement} el
 * @param {string} aid
 * @param {number} token
 */
async function fireAnalysis(el, aid, token) {
  if (state.browseSource !== 'mb') return;
  try {
    const r = await fetch(`${API}/api/artist/${aid}/disambiguate`);
    if (token !== artistPageToken || !r.ok) return;
    const data = await r.json();
    if (token !== artistPageToken) return;
    if (state.browseCache[aid]) state.browseCache[aid].disamb = data;
    state.disambData = data;
    applyAnalysisChips(el, data);
    // Expansions that rendered before the payload arrived (search-by-ID
    // auto-expand) get their dots + recordings breakdown now.
    applyAnalysisToOpenExpansions(el, data);
  } catch (_e) { /* decoration only */ }
}


/**
 * Search for artists or releases and render results.
 * @param {string} q - Search query
 */
export async function searchArtists(q) {
  const requestToken = ++searchArtistsRequestToken;
  const searchType = state.browseSearchType;
  const browseSource = state.browseSource;
  const el = document.getElementById('results');
  el.style.display = 'block';
  document.getElementById('browse-artist').style.display = 'none';
  const browseLabel = document.getElementById('browse-label');
  if (browseLabel) browseLabel.style.display = 'none';
  const vaFallback = document.getElementById('va-fallback');
  if (vaFallback) vaFallback.style.display = 'none';
  el.innerHTML = '<div class="loading">Searching...</div>';
  // Search-by-ID short-circuits search entirely: parse, resolve, navigate.
  if (searchType === 'id') {
    return resolveAndNavigate(q, requestToken);
  }
  // Label search is Discogs-only in Phase A — independent of browseSource.
  if (searchType === 'label') {
    try {
      const hits = await searchLabels(q);
      if (requestToken !== searchArtistsRequestToken) return;
      renderLabelSearchResults(el, hits, openLabelDetail);
    } catch (_e) {
      if (requestToken !== searchArtistsRequestToken) return;
      el.innerHTML = '<div class="loading">Label search failed</div>';
    }
    return;
  }
  const isDiscogs = browseSource === 'discogs';
  const searchBase = isDiscogs ? `${API}/api/discogs/search` : `${API}/api/search`;
  try {
    if (searchType === 'release') {
      const r = await fetch(`${searchBase}?q=${encodeURIComponent(q)}&type=release`);
      const data = await r.json();
      if (requestToken !== searchArtistsRequestToken) return;
      const rgs = data.release_groups || [];
      if (!rgs.length) { el.innerHTML = '<div class="loading">No results</div>'; return; }
      el.innerHTML = rgs.map(rg => {
        const isVA = rg.artist_id === VA_MBID;
        // Discogs releases without a master: show pressings inline instead of dead-end artist page
        const isMasterless = isDiscogs && rg.is_master === false;
        const releaseId = isMasterless ? rg.discogs_release_id || rg.id : rg.id;
        const onclick = (isVA || isMasterless)
          ? `window.loadReleaseGroup(${jsArg(releaseId)}, this, ${isMasterless ? '{masterless:true}' : '{}'})`
          : `window.openBrowseArtist(${jsArg(rg.artist_id)}, ${jsArg(rg.artist_name)})`;
        return `
        <div class="artist" style="cursor:pointer;padding:6px 0;" onclick="${onclick}">
          <span class="artist-name">${esc(rg.artist_name)}</span>
          <span class="artist-dis"> — ${esc(rg.title)}</span>
          ${rg.primary_type ? `<span class="artist-dis" style="color:#888;"> (${esc(rg.primary_type)})</span>` : ''}
        </div>
        <div id="rel-${esc(String(rg.id))}"></div>`;
      }).join('');
    } else {
      const r = await fetch(`${searchBase}?q=${encodeURIComponent(q)}`);
      const data = await r.json();
      if (requestToken !== searchArtistsRequestToken) return;
      if (!data.artists || !data.artists.length) {
        el.innerHTML = '<div class="loading">No results</div>';
        return;
      }
      el.innerHTML = data.artists.map(a => `
        <div class="artist">
          <div class="artist-header" onclick="window.openBrowseArtist(${jsArg(a.id)}, ${jsArg(a.name)})">
            <span class="artist-name">${esc(a.name)}</span>
            ${a.disambiguation ? `<span class="artist-dis"> - ${esc(a.disambiguation)}</span>` : ''}
          </div>
        </div>
      `).join('');
    }
  } catch (e) {
    if (requestToken !== searchArtistsRequestToken) return;
    el.innerHTML = '<div class="loading">Search failed</div>';
  }
}
