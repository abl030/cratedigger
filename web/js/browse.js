// @ts-check
import { state, API, toast } from './state.js';
import { esc } from './util.js';
import { renderArtistDiscography } from './discography.js';
import { renderDisambiguateInto } from './analysis.js';
import { renderLibraryResultsInto } from './library.js';

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
 * Set the browse metadata source (mb or discogs). Preserves artist context
 * when possible: if an artist is currently selected, look up the equivalent
 * on the new source and re-render in place instead of dumping back to search.
 * @param {string} src - 'mb' or 'discogs'
 */
export async function setBrowseSource(src) {
  if (state.browseSource === src) return;
  state.browseSource = src;
  const mbBtn = document.getElementById('source-mb');
  const dgBtn = document.getElementById('source-discogs');
  if (mbBtn) mbBtn.className = 'p-btn' + (src === 'mb' ? ' active-status' : '');
  if (dgBtn) dgBtn.className = 'p-btn' + (src === 'discogs' ? ' active-status' : '');
  state.browseCache = {};

  // Sticky artist context across the toggle.
  if (state.browseArtist) {
    const prevName = state.browseArtist.name;
    const match = await findArtistOnSource(prevName, src);
    if (match) {
      state.browseArtist = { id: String(match.id), name: match.name };
      document.getElementById('browse-artist-name').textContent = match.name;
      switchSubView(state.browseSubView || 'discography');
      return;
    }
    toast(`No ${src === 'discogs' ? 'Discogs' : 'MusicBrainz'} match for ${prevName}`, true);
    state.browseArtist = null;
    document.getElementById('browse-artist').style.display = 'none';
  }

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
  ['discography', 'analysis', 'library', 'compare'].forEach(v => {
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
  if (view === 'compare' && !state.browseCache[aid].compare) {
    loadBrowseCompare(aid, name);
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
    // Beets only stores MB UUIDs in mb_albumartistid; sending the numeric
    // Discogs ID would skip the UUID match and only return Discogs-tagged
    // albums, hiding the rest of the user's catalog. Pass empty mbid on the
    // Discogs path so the backend falls through to a pure name match.
    const libUrl = isDiscogs
      ? `${API}/api/library/artist?name=${encodeURIComponent(name)}`
      : `${API}/api/library/artist?name=${encodeURIComponent(name)}&mbid=${aid}`;
    const [rgRes, libRes] = await Promise.all([
      fetch(artistUrl).then(r => r.json()),
      fetch(libUrl).then(r => r.json()),
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
    // See loadBrowseDiscography: skip mbid on Discogs path (numeric ID isn't
    // a valid MB UUID, would suppress all non-Discogs-tagged albums).
    const isDiscogs = state.browseSource === 'discogs';
    const url = isDiscogs
      ? `${API}/api/library/artist?name=${encodeURIComponent(name)}`
      : `${API}/api/library/artist?name=${encodeURIComponent(name)}&mbid=${aid}`;
    const r = await fetch(url);
    const data = await r.json();
    if (!state.browseCache[aid]) state.browseCache[aid] = {};
    state.browseCache[aid].library = true;
    renderLibraryResultsInto(el, data.albums || []);
  } catch (e) { el.innerHTML = '<div class="loading">Failed to load</div>'; }
}

/**
 * Load the merged MB+Discogs comparison for a browse artist.
 * @param {string} aid - Artist ID (MB UUID or numeric Discogs ID)
 * @param {string} name - Artist name
 */
export async function loadBrowseCompare(aid, name) {
  const el = document.getElementById('browse-compare');
  el.innerHTML = '<div class="loading">Loading both sources (this may take ~5-15s)...</div>';
  try {
    const isDiscogs = state.browseSource === 'discogs';
    const idParam = isDiscogs ? `discogs_id=${encodeURIComponent(aid)}` : `mbid=${encodeURIComponent(aid)}`;
    const url = `${API}/api/artist/compare?name=${encodeURIComponent(name)}&${idParam}`;
    const r = await fetch(url);
    const data = await r.json();
    if (!state.browseCache[aid]) state.browseCache[aid] = {};
    state.browseCache[aid].compare = true;
    renderCompare(el, data);
  } catch (_e) { el.innerHTML = '<div class="loading">Failed to load comparison</div>'; }
}

/**
 * Render a row in the compare view. `mb` and `discogs` may be null when the
 * row only exists on one side.
 * @param {Object|null} mb
 * @param {Object|null} discogs
 */
function compareRow(mb, discogs) {
  const ref = mb || discogs;
  const title = ref.title || '?';
  const year = (ref.first_release_date || '').slice(0, 4) || '?';
  const type = ref.type || '';
  // Badges are clickable: jump into that source's discography for this artist
  // so the user can browse pressings / hit Add. v1 doesn't render inline
  // pressings here — that lives in the existing Discography sub-tab.
  const mbBadge = mb
    ? `<span class="library-src library-src-mb" style="cursor:pointer;" onclick="event.stopPropagation(); window.openBrowseArtistFromCompare('${mb.primary_artist_id}', '${esc(mb.artist_credit || '')}', 'mb')">MB</span>`
    : '<span class="library-src library-src-muted">MB —</span>';
  const dgBadge = discogs
    ? `<span class="library-src library-src-discogs" style="cursor:pointer;" onclick="event.stopPropagation(); window.openBrowseArtistFromCompare('${discogs.primary_artist_id}', '${esc(discogs.artist_credit || '')}', 'discogs')">Discogs</span>`
    : '<span class="library-src library-src-muted">Discogs —</span>';
  return `
    <div class="rg" style="display:flex;align-items:center;gap:8px;padding:4px 0;">
      <span class="rg-year">${year}</span>
      <span class="rg-title">${esc(title)}</span>
      ${type ? `<span class="rg-meta" style="color:#777;">(${esc(type)})</span>` : ''}
      <span style="margin-left:auto;display:flex;gap:4px;">${mbBadge}${dgBadge}</span>
    </div>`;
}

/**
 * @param {HTMLElement} el
 * @param {Object} data
 */
function renderCompare(el, data) {
  const both = data.both || [];
  const mbOnly = data.mb_only || [];
  const dgOnly = data.discogs_only || [];
  const mbName = data.mb_artist?.name || '—';
  const dgName = data.discogs_artist?.name || '—';

  const sortBy = (rows, getter) => {
    return [...rows].sort((a, b) => (getter(a) || '').localeCompare(getter(b) || ''));
  };
  const bothSorted = sortBy(both, p => (p.mb || p.discogs).first_release_date || '');
  const mbOnlySorted = sortBy(mbOnly, r => r.first_release_date || '');
  const dgOnlySorted = sortBy(dgOnly, r => r.first_release_date || '');

  el.innerHTML = `
    <div style="font-size:13px;color:#888;margin-bottom:10px;">
      MB artist: <b>${esc(mbName)}</b> · Discogs artist: <b>${esc(dgName)}</b>
    </div>
    <div class="type-section">
      <div class="type-header" onclick="event.stopPropagation(); this.nextElementSibling.classList.toggle('open')">
        On both sources <span class="type-count">${both.length}</span>
      </div>
      <div class="type-body open">${bothSorted.map(p => compareRow(p.mb, p.discogs)).join('') || '<div style="padding:6px;color:#777;">none</div>'}</div>
    </div>
    <div class="type-section">
      <div class="type-header" onclick="event.stopPropagation(); this.nextElementSibling.classList.toggle('open')" style="color:#9cf;">
        Only on MusicBrainz <span class="type-count">${mbOnly.length}</span>
      </div>
      <div class="type-body">${mbOnlySorted.map(r => compareRow(r, null)).join('') || '<div style="padding:6px;color:#777;">none</div>'}</div>
    </div>
    <div class="type-section">
      <div class="type-header" onclick="event.stopPropagation(); this.nextElementSibling.classList.toggle('open')" style="color:#fc9;">
        Only on Discogs <span class="type-count">${dgOnly.length}</span>
      </div>
      <div class="type-body">${dgOnlySorted.map(r => compareRow(null, r)).join('') || '<div style="padding:6px;color:#777;">none</div>'}</div>
    </div>`;
}

/**
 * Switch source then open an artist (used by Compare row badges to jump into
 * the matched-source's discography view).
 * @param {string} id
 * @param {string} name
 * @param {string} src
 */
export function openBrowseArtistFromCompare(id, name, src) {
  // Switch source synchronously without sticky-context lookup; we already
  // know exactly which artist to open on the new source.
  state.browseSource = src;
  const mbBtn = document.getElementById('source-mb');
  const dgBtn = document.getElementById('source-discogs');
  if (mbBtn) mbBtn.className = 'p-btn' + (src === 'mb' ? ' active-status' : '');
  if (dgBtn) dgBtn.className = 'p-btn' + (src === 'discogs' ? ' active-status' : '');
  state.browseCache = {};
  state.browseArtist = { id, name };
  document.getElementById('browse-artist-name').textContent = name;
  switchSubView('discography');
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
