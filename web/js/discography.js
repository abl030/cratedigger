// @ts-check
import { API, state, toast, updatePipelineStatus } from './state.js';
import { esc, externalReleaseUrl, sourceLabel, detectSource, normalizeReleaseId } from './util.js';
import { renderTypedSections } from './grouping.js';
import { buildReleaseActionState } from './release_action_state.js';
import { renderActionToolbar, renderAcquireActionButton, renderRemoveFromBeetsButton } from './release_actions.js';
import { renderStatusBadges } from './badges.js';
import { invalidateBrowseArtist } from './browse.js';
import { renderLabelLinks } from './labels.js';

/**
 * Render the artist discography into a target element.
 * @param {HTMLElement} rgEl - Container element
 * @param {string} id - MusicBrainz artist ID
 * @param {string} artistName - Artist name
 * @param {Object} data - API response with release_groups
 * @param {Object} libData - API response with library albums
 */
export function renderArtistDiscography(rgEl, id, artistName, data, libData) {
    const groups = data.release_groups || [];
    const libraryAlbums = libData.albums || [];

    // Split: own work vs appearances, filter bootleg-only release groups
    // Compare by artist ID (handles name changes like Kanye West → Ye)
    const nameLC = artistName.toLowerCase();
    const own = [], appearances = [], bootlegOnly = [];
    for (const rg of groups) {
      const credit = (rg.artist_credit || '').toLowerCase();
      const isOwn = rg.primary_artist_id === id
        || credit === nameLC || credit.startsWith(nameLC + ' /') || credit.startsWith(nameLC + ',') || !credit;

      if (!rg.has_official) {
        bootlegOnly.push(rg);
      } else if (isOwn) {
        own.push(rg);
      } else {
        appearances.push(rg);
      }
    }

    function renderRgRow(rg) {
      const year = rg.first_release_date ? rg.first_release_date.slice(0, 4) : '';
      const creditNote = rg.artist_credit && rg.artist_credit.toLowerCase() !== nameLC
        ? `<span class="rg-meta"> - ${esc(rg.artist_credit)}</span>` : '';
      const badges = renderStatusBadges(rg);
      return `
        <div class="rg">
          <div onclick="event.stopPropagation(); window.loadReleaseGroup('${rg.id}', this)">
            <span class="rg-year">${year}</span> <span class="rg-title">${esc(rg.title)}</span>${creditNote}${badges}
          </div>
          <div class="releases" id="rel-${rg.id}"></div>
        </div>
      `;
    }

    function renderSection(rgs, defaultOpen) {
      return renderTypedSections(rgs, renderRgRow,
        { defaultOpen: defaultOpen ? 'Albums' : null });
    }

    // Library section — what you already own
    let html = '';
    if (libraryAlbums.length > 0) {
      const discogs = libraryAlbums.filter(a => a.source === 'discogs');
      const mb = libraryAlbums.filter(a => a.source === 'musicbrainz');
      html += `<div class="library-section">
        <div class="library-header">In Library (${libraryAlbums.length})</div>
        ${mb.map(a => `
          <div class="library-album">
            <span class="library-album-title">${a.year || '?'} ${esc(a.album)} (${a.track_count}t)</span>
            <span class="library-src library-src-mb">MB</span>
          </div>
        `).join('')}
        ${discogs.map(a => `
          <div class="library-album">
            <span class="library-album-title">${a.year || '?'} ${esc(a.album)} (${a.track_count}t)</span>
            <span class="library-src library-src-discogs">Discogs</span>
          </div>
        `).join('')}
      </div>`;
    }

    html += renderSection(own, true);
    if (appearances.length > 0) {
      html += `
        <div class="type-section">
          <div class="type-header" onclick="event.stopPropagation(); this.nextElementSibling.classList.toggle('open')" style="color:#777;">
            Appearances <span class="type-count">${appearances.length}</span>
          </div>
          <div class="type-body">
            ${renderSection(appearances, false)}
          </div>
        </div>
      `;
    }
    if (bootlegOnly.length > 0) {
      html += `
        <div class="type-section">
          <div class="type-header" onclick="event.stopPropagation(); this.nextElementSibling.classList.toggle('open')" style="color:#555;">
            Bootleg-only releases <span class="type-count">${bootlegOnly.length}</span>
          </div>
          <div class="type-body">
            ${renderSection(bootlegOnly, false)}
          </div>
        </div>
      `;
    }
    rgEl.innerHTML = html;
}

/**
 * Load and display releases for a release group.
 *
 * @param {string} id - MusicBrainz release group ID or Discogs master ID
 * @param {HTMLElement} el - The clicked element (kept for signature compat)
 * @param {Object} [opts]
 * @param {HTMLElement} [opts.targetEl] - Where to render. Defaults to
 *   document.getElementById('rel-' + id) so existing call sites still work.
 *   Compare view passes its own div so its IDs don't collide with the
 *   Discography view's `rel-${id}` ones.
 * @param {string} [opts.source] - 'mb' or 'discogs'. Defaults to
 *   state.browseSource. Compare view passes the explicit source so MB and
 *   Discogs pressings can be loaded independently for the same row.
 */
export async function loadReleaseGroup(id, el, opts = {}) {
  const relEl = opts.targetEl || document.getElementById('rel-' + id);
  if (!relEl) return;
  if (relEl.innerHTML) { relEl.innerHTML = ''; return; }
  relEl.innerHTML = '<div class="loading">Loading releases...</div>';
  try {
    const source = opts.source || state.browseSource;
    const isDiscogs = source === 'discogs';
    const url = isDiscogs ? `${API}/api/discogs/master/${id}` : `${API}/api/release-group/${id}`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    if (data.error) throw new Error(data.error);
    const all = (data.releases || []).sort((a, b) => (a.date || '').localeCompare(b.date || ''));
    const official = all.filter(r => r.status === 'Official' || !r.status);
    const bootleg = all.filter(r => r.status && r.status !== 'Official');

    function renderRelease(rel) {
      const badges = renderStatusBadges(rel);
      const actionState = buildReleaseActionState({
        ...rel,
        artist: state.browseArtist?.name || '',
        album: rel.title,
      });
      const toolbar = renderActionToolbar(actionState, { size: 'small' });
      return `
        <div class="release" onclick="event.stopPropagation(); window.toggleReleaseDetail('${rel.id}')">
          <div class="release-info">
            <div class="release-title">${esc(rel.title)}${badges}</div>
            <div class="release-meta" style="color:#777;">${rel.country || '?'} ${rel.date || '?'} - ${rel.format} - ${rel.track_count}t - ${rel.status || '?'}</div>
          </div>
          ${toolbar}
        </div>
        <div class="release-detail" id="reldet-${rel.id}"></div>
      `;
    }

    let html = official.map(renderRelease).join('');
    if (bootleg.length > 0) {
      html += `
        <div class="type-header" onclick="event.stopPropagation(); this.nextElementSibling.classList.toggle('open')" style="color:#777;margin-top:6px;">
          Bootleg / Promo <span class="type-count">${bootleg.length}</span>
        </div>
        <div class="type-body">
          ${bootleg.map(renderRelease).join('')}
        </div>
      `;
    }
    relEl.innerHTML = html;
  } catch (e) { relEl.innerHTML = '<div class="loading">Failed to load</div>'; }
}

/**
 * Add a release to the pipeline.
 * @param {string} mbid - MusicBrainz release ID
 * @param {HTMLButtonElement} btn - The clicked button
 */
export async function addRelease(mbid, btn) {
  const releaseId = normalizeReleaseId(mbid);
  btn.disabled = true;
  btn.textContent = '...';
  try {
    const requestId = releaseId || mbid;
    const idField = detectSource(requestId) === 'discogs' ? 'discogs_release_id' : 'mb_release_id';
    const r = await fetch(`${API}/api/pipeline/add`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({[idField]: requestId}),
    });
    const data = await r.json();
    if (data.status === 'added') {
      btn.textContent = 'Added';
      invalidateBrowseArtist();
      updatePipelineStatus(requestId, 'wanted', data.id);
      toast(`Added: ${data.artist} - ${data.album} (${data.tracks} tracks)`);
    } else if (data.status === 'exists') {
      if (data.current_status === 'wanted' && data.id) {
        btn.textContent = 'Remove';
        btn.disabled = false;
        btn.style.background = '#5a2a2a';
        btn.style.color = '#f88';
        btn.onclick = (e) => { e.stopPropagation(); window.disambRemove(data.id, btn); };
      } else {
        btn.textContent = data.current_status;
      }
      toast(`Already in pipeline (${data.current_status})`);
    } else {
      btn.textContent = 'Error';
      toast(data.error || 'Unknown error', true);
    }
  } catch (e) {
    btn.textContent = 'Error';
    toast('Request failed', true);
  }
}

/**
 * Toggle release detail panel (tracks, links, actions).
 * @param {string} mbid - MusicBrainz release ID
 */
export async function toggleReleaseDetail(mbid) {
  const releaseId = normalizeReleaseId(mbid) || mbid;
  const el = document.getElementById('reldet-' + mbid);
  if (el.classList.contains('open')) { el.classList.remove('open'); return; }
  el.innerHTML = '<div class="loading" style="padding:8px;">Loading...</div>';
  el.classList.add('open');
  try {
    const isDiscogs = detectSource(releaseId) === 'discogs';
    const url = isDiscogs ? `${API}/api/discogs/release/${releaseId}` : `${API}/api/release/${releaseId}`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    let html = '';

    // Use beets tracks if owned (has bitrate info), otherwise MB tracks
    const hasBeets = data.beets_tracks && data.beets_tracks.length > 0;
    const tracks = hasBeets ? data.beets_tracks : (data.tracks || []);

    if (tracks.length > 0) {
      html += '<div style="margin-bottom:6px;color:#666;font-size:0.8em;">Tracks (' + tracks.length + ')' + (hasBeets ? ' — from library' : '') + '</div>';
      html += tracks.map(t => {
        if (hasBeets) {
          const dur = t.length ? `${Math.floor(t.length/60)}:${String(Math.round(t.length%60)).padStart(2,'0')}` : '';
          const br = t.bitrate ? `${Math.round(t.bitrate/1000)}kbps` : '';
          const depth = t.bitdepth && t.bitdepth > 16 ? `${t.bitdepth}bit` : '';
          const sr = t.samplerate && t.samplerate > 44100 ? `${(t.samplerate/1000).toFixed(1)}kHz` : '';
          const meta = [t.format, br, depth, sr].filter(Boolean).join(' ');
          return `<div class="lib-track">
            <span>${t.disc > 1 ? t.disc + '.' : ''}${t.track}. ${esc(t.title)} ${dur ? '<span style="color:#555;">' + dur + '</span>' : ''}</span>
            <span class="lib-track-meta">${meta}</span>
          </div>`;
        } else {
          const dur = t.length_seconds ? `${Math.floor(t.length_seconds/60)}:${String(Math.round(t.length_seconds%60)).padStart(2,'0')}` : '';
          return `<div class="lib-track">
            <span>${t.disc_number > 1 ? t.disc_number + '.' : ''}${t.track_number}. ${esc(t.title)} ${dur ? '<span style="color:#555;">' + dur + '</span>' : ''}</span>
          </div>`;
        }
      }).join('');
    }

    // Label links (U7) — Discogs releases carry `labels: [{id, name}]`;
    // MB releases don't surface labels through the route layer in v1.
    const labelLinksHtml = renderLabelLinks(data.labels);
    if (labelLinksHtml) {
      html += `<div class="release-labels" style="margin:4px 0;font-size:0.85em;color:#aaa;">`
        + `<span style="color:#666;margin-right:6px;">Label:</span>${labelLinksHtml}</div>`;
    }

    // Links and actions
    html += '<div class="release-links">';
    const externalUrl = externalReleaseUrl(releaseId);
    const label = sourceLabel(releaseId);
    if (externalUrl && label) {
      html += `<a href="${externalUrl}" target="_blank" rel="noopener" style="color:#6af;font-size:0.85em;" onclick="event.stopPropagation()">${label}</a>`;
    }
    const actionState = buildReleaseActionState({
      id: releaseId,
      in_library: data.in_library,
      beets_album_id: data.beets_album_id,
      pipeline_status: data.pipeline_status,
      pipeline_id: data.pipeline_id,
      artist: data.artist_name || state.browseArtist?.name || '',
      album: data.title || '',
      track_count: tracks.length,
    });
    html += renderAcquireActionButton(actionState, {
      addLabel: 'Add to pipeline',
      stopPropagation: true,
      hideDisabled: true,
    });
    html += renderRemoveFromBeetsButton(actionState, {
      stopPropagation: true,
      hideDisabled: true,
    });
    html += '</div>';

    el.innerHTML = html;
  } catch (e) { el.innerHTML = '<div class="loading" style="padding:8px;">Failed to load</div>'; }
}
