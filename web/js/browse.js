// @ts-check
import { state, API, toast } from './state.js';
import { esc } from './util.js';
import { renderArtistDiscography } from './discography.js';
import { renderDisambiguateInto } from './analysis.js';
import { renderLibraryResultsInto } from './library.js';

/**
 * Set the browse metadata source (mb or discogs).
 * @param {string} src - 'mb' or 'discogs'
 */
export function setBrowseSource(src) {
  state.browseSource = src;
  const mbBtn = document.getElementById('source-mb');
  const dgBtn = document.getElementById('source-discogs');
  if (mbBtn) mbBtn.className = 'p-btn' + (src === 'mb' ? ' active-status' : '');
  if (dgBtn) dgBtn.className = 'p-btn' + (src === 'discogs' ? ' active-status' : '');
  // Clear and re-trigger search
  state.browseCache = {};
  const q = /** @type {HTMLInputElement} */ (document.getElementById('q')).value.trim();
  if (q.length >= 2) searchArtists(q);
}

/**
 * Set the browse search type (artist or release).
 * @param {string} type - 'artist' or 'release'
 */
export function setSearchType(type) {
  state.browseSearchType = type;
  document.getElementById('search-type-artist').className = 'p-btn' + (type === 'artist' ? ' active-status' : '');
  document.getElementById('search-type-release').className = 'p-btn' + (type === 'release' ? ' active-status' : '');
  document.getElementById('q').placeholder = type === 'artist' ? 'Search artists or albums...' : 'Search album titles...';
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

export function openBrowseArtist(id, name) {
  if (id === VA_MBID) {
    setSearchType('release');
    toast('Various Artists has too many releases — search by album title instead');
    return;
  }
  state.browseArtist = {id, name};
  state.browseSubView = 'discography';
  document.getElementById('results').style.display = 'none';
  document.getElementById('browse-artist').style.display = 'block';
  document.getElementById('browse-artist-name').textContent = name;
  // Reset sub-nav
  document.getElementById('subnav-discography').className = 'p-btn active-status';
  document.getElementById('subnav-analysis').className = 'p-btn';
  document.getElementById('subnav-library').className = 'p-btn';
  // Load discography (the default view)
  switchSubView('discography');
}

/**
 * Close the browse artist detail view and show search results.
 */
export function closeBrowseArtist() {
  state.browseArtist = null;
  document.getElementById('browse-artist').style.display = 'none';
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
 * Switch between sub-views (discography, analysis, library) in the browse artist view.
 * @param {string} view - 'discography', 'analysis', or 'library'
 */
export function switchSubView(view) {
  state.browseSubView = view;
  ['discography', 'analysis', 'library'].forEach(v => {
    document.getElementById('browse-' + v).style.display = v === view ? 'block' : 'none';
    document.getElementById('subnav-' + v).className = 'p-btn' + (v === view ? ' active-status' : '');
  });
  if (!state.browseArtist) return;
  /** @type {string} */
  const aid = state.browseArtist.id;
  const name = state.browseArtist.name;
  if (!state.browseCache[aid]) state.browseCache[aid] = {};
  if (view === 'discography' && !state.browseCache[aid].discography) {
    loadBrowseDiscography(aid, name);
  }
  if (view === 'analysis' && !state.browseCache[aid].analysis) {
    loadBrowseAnalysis(aid, name);
  }
  if (view === 'library' && !state.browseCache[aid].library) {
    loadBrowseLibrary(aid, name);
  }
}

/**
 * Load and render the discography for a browse artist.
 * @param {string} aid - MusicBrainz artist ID
 * @param {string} name - Artist name
 */
export async function loadBrowseDiscography(aid, name) {
  const el = document.getElementById('browse-discography');
  el.innerHTML = '<div class="loading">Loading discography...</div>';
  try {
    const isDiscogs = state.browseSource === 'discogs';
    const artistUrl = isDiscogs ? `${API}/api/discogs/artist/${aid}` : `${API}/api/artist/${aid}`;
    const [rgRes, libRes] = await Promise.all([
      fetch(artistUrl).then(r => r.json()),
      fetch(`${API}/api/library/artist?name=${encodeURIComponent(name)}&mbid=${aid}`).then(r => r.json()),
    ]);
    if (!state.browseCache[aid]) state.browseCache[aid] = {};
    state.browseCache[aid].discography = true;
    renderArtistDiscography(el, aid, name, rgRes, libRes);
  } catch (e) { el.innerHTML = '<div class="loading">Failed to load</div>'; }
}

/**
 * Load and render the disambiguate analysis for a browse artist.
 * @param {string} aid - MusicBrainz artist ID
 * @param {string} name - Artist name
 */
export async function loadBrowseAnalysis(aid, name) {
  const el = document.getElementById('browse-analysis');
  if (state.browseSource === 'discogs') {
    el.innerHTML = '<div class="loading" style="color:#888;">Analysis is not available for Discogs artists (requires MusicBrainz recording IDs).</div>';
    return;
  }
  el.innerHTML = '<div class="loading">Loading analysis (this may take a few seconds)...</div>';
  try {
    const r = await fetch(`${API}/api/artist/${aid}/disambiguate`);
    const data = await r.json();
    if (!state.browseCache[aid]) state.browseCache[aid] = {};
    state.browseCache[aid].analysis = true;
    state.disambData = data;
    renderDisambiguateInto(el);
  } catch (e) { el.innerHTML = '<div style="color:#f66;">Failed to load analysis</div>'; }
}

/**
 * Load and render library results for a browse artist.
 * @param {string} aid - MusicBrainz artist ID
 * @param {string} name - Artist name
 */
export async function loadBrowseLibrary(aid, name) {
  const el = document.getElementById('browse-library');
  el.innerHTML = '<div class="loading">Loading library...</div>';
  try {
    const r = await fetch(`${API}/api/library/artist?name=${encodeURIComponent(name)}&mbid=${aid}`);
    const data = await r.json();
    if (!state.browseCache[aid]) state.browseCache[aid] = {};
    state.browseCache[aid].library = true;
    renderLibraryResultsInto(el, data.albums || []);
  } catch (e) { el.innerHTML = '<div class="loading">Failed to load</div>'; }
}

/**
 * Search for artists or releases and render results.
 * @param {string} q - Search query
 */
export async function searchArtists(q) {
  const el = document.getElementById('results');
  el.style.display = 'block';
  document.getElementById('browse-artist').style.display = 'none';
  el.innerHTML = '<div class="loading">Searching...</div>';
  const isDiscogs = state.browseSource === 'discogs';
  const searchBase = isDiscogs ? `${API}/api/discogs/search` : `${API}/api/search`;
  try {
    if (state.browseSearchType === 'release') {
      const r = await fetch(`${searchBase}?q=${encodeURIComponent(q)}&type=release`);
      const data = await r.json();
      const rgs = data.release_groups || [];
      if (!rgs.length) { el.innerHTML = '<div class="loading">No results</div>'; return; }
      el.innerHTML = rgs.map(rg => {
        const isVA = rg.artist_id === VA_MBID;
        // Discogs releases without a master: show pressings inline instead of dead-end artist page
        const isMasterless = isDiscogs && rg.is_master === false;
        const onclick = (isVA || isMasterless)
          ? `window.loadReleaseGroup('${isMasterless ? rg.discogs_release_id || rg.id : rg.id}', this)`
          : `window.openBrowseArtist('${rg.artist_id}', '${esc(rg.artist_name)}')`;
        return `
        <div class="artist" style="cursor:pointer;padding:6px 0;" onclick="${onclick}">
          <span class="artist-name">${esc(rg.artist_name)}</span>
          <span class="artist-dis"> — ${esc(rg.title)}</span>
          ${rg.primary_type ? `<span class="artist-dis" style="color:#888;"> (${esc(rg.primary_type)})</span>` : ''}
        </div>
        <div id="rel-${rg.id}"></div>`;
      }).join('');
    } else {
      const r = await fetch(`${searchBase}?q=${encodeURIComponent(q)}`);
      const data = await r.json();
      if (!data.artists || !data.artists.length) {
        el.innerHTML = '<div class="loading">No results</div>';
        return;
      }
      el.innerHTML = data.artists.map(a => `
        <div class="artist">
          <div class="artist-header" onclick="window.openBrowseArtist('${a.id}', '${esc(a.name)}')">
            <span class="artist-name">${esc(a.name)}</span>
            ${a.disambiguation ? `<span class="artist-dis"> - ${esc(a.disambiguation)}</span>` : ''}
          </div>
        </div>
      `).join('');
    }
  } catch (e) { el.innerHTML = '<div class="loading">Search failed</div>'; }
}
