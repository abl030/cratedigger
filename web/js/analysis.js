// @ts-check
import { state, API, toast, updatePipelineStatus, pipelineStore } from './state.js';
import { esc } from './util.js';
import { renderTypedSections } from './grouping.js';
import { invalidateBrowseArtist } from './browse.js';

/** @type {string[]} */
export const _PRESSING_COLORS = ['#6af','#fa6','#6d6','#f6a','#af6','#6ff','#ff6','#a6f'];

/**
 * Render disambiguate/analysis results into a target element.
 * @param {HTMLElement} [targetEl]
 */
export function renderDisambiguateInto(targetEl) {
  if (!state.disambData) return;
  const el = targetEl || document.getElementById('disamb-content');
  if (!el) return;
  const d = state.disambData;
  const rgs = d.release_groups || [];

  const withUnique = rgs.filter(rg => rg.unique_track_count > 0).length;
  const covered = rgs.filter(rg => rg.covered_by).length;

  let html = `<div style="margin:12px 0;">
    <strong>${esc(d.artist_name)}</strong> — ${rgs.length} release groups (excl. live), ${withUnique} with unique tracks, ${covered} fully covered
  </div>`;

  // Same Albums/EPs/Singles sectioning the other browse sub-tabs use.
  // Analysis rows expose `primary_type` and `first_date` (not the
  // standard `first_release_date`), so override the date extractor.
  html += renderTypedSections(rgs, renderDisambRG, {
    dateOf: (r) => String(r.first_date || ''),
  });
  el.innerHTML = html;
}

/**
 * Render a single release group row.
 * @param {Object} rg - Release group data
 * @returns {string} HTML string
 */
export function renderDisambRG(rg) {
  let badges = '';
  if (rg.library_status) badges += '<span class="badge badge-library">in library</span>';
  if (rg.pipeline_status === 'wanted') badges += '<span class="badge badge-wanted">wanted</span>';
  if (rg.pipeline_status === 'downloading') badges += '<span class="badge badge-downloading">downloading</span>';
  if (rg.pipeline_status === 'imported') badges += '<span class="badge badge-imported">imported</span>';
  if (rg.pipeline_status === 'manual') badges += '<span class="badge badge-manual">manual</span>';

  let statusBadge;
  if (rg.covered_by) {
    statusBadge = `<span style="color:#777;font-size:0.85em;margin-left:6px;">covered by ${esc(rg.covered_by)}</span>`;
  } else if (rg.unique_track_count > 0) {
    statusBadge = `<span style="color:#6d6;font-weight:600;margin-left:6px;">${rg.unique_track_count} unique</span>`;
  } else {
    statusBadge = '<span style="color:#555;margin-left:6px;">0 unique</span>';
  }

  const opacity = rg.covered_by ? '0.5' : '1';

  return `
    <div class="release" onclick="event.stopPropagation(); window.toggleDisambRGTracks('${rg.release_group_id}')" style="cursor:pointer;opacity:${opacity};">
      <div class="release-info">
        <div class="release-title">${esc(rg.title)}${badges}${statusBadge}</div>
        <div class="release-meta" style="color:#777;">${rg.first_date || '?'} — ${esc(rg.primary_type)} — ${rg.track_count}t — ${rg.release_ids.length} pressing${rg.release_ids.length > 1 ? 's' : ''}</div>
      </div>
    </div>
    <div id="disamb-rg-${rg.release_group_id}" style="display:none;padding:4px 0 8px 16px;"></div>
  `;
}

/**
 * Toggle track listing for a release group in the disambiguate view.
 * @param {string} rgId - Release group ID
 */
export function toggleDisambRGTracks(rgId) {
  const el = document.getElementById('disamb-rg-' + rgId);
  if (el.style.display !== 'none') { el.style.display = 'none'; return; }

  const rg = state.disambData.release_groups.find(rg => rg.release_group_id === rgId);
  if (!rg) { el.style.display = 'none'; return; }

  const pressingRecSets = (rg.pressings || []).map(p => new Set(p.recording_ids || []));

  // For each recording, find which pressing(s) contain it
  const trackToPressings = {};
  for (const t of rg.tracks) {
    trackToPressings[t.recording_id] = [];
    for (let i = 0; i < pressingRecSets.length; i++) {
      if (pressingRecSets[i].has(t.recording_id)) {
        trackToPressings[t.recording_id].push(i);
      }
    }
  }

  // "Exclusive" = tracks on this pressing that NO other pressing has
  const pressingExclusiveCounts = pressingRecSets.map((recSet, i) => {
    let count = 0;
    for (const recId of recSet) {
      const onPressings = trackToPressings[recId];
      if (onPressings && onPressings.length === 1 && onPressings[0] === i) count++;
    }
    return count;
  });

  let html = '';

  // Show pressings with colour dots and unique counts
  if (rg.pressings && rg.pressings.length > 0) {
    html += '<div style="margin-bottom:8px;color:#888;font-size:0.85em;">Pressings:</div>';
    html += rg.pressings.map((p, i) => {
      const color = _PRESSING_COLORS[i % _PRESSING_COLORS.length];
      // Overlay local pipeline store (captures mutations since last API fetch)
      const stored = pipelineStore.get(p.release_id);
      const pStatus = stored ? stored.status : p.pipeline_status;
      const pId = stored ? stored.id : p.pipeline_id;
      let badges = '';
      if (p.in_library) badges += '<span class="badge badge-library">in library</span>';
      if (pStatus === 'wanted') badges += '<span class="badge badge-wanted">wanted</span>';
      if (pStatus === 'downloading') badges += '<span class="badge badge-downloading">downloading</span>';
      if (pStatus === 'imported') badges += '<span class="badge badge-imported">imported</span>';

      const canAdd = !p.in_library && !pStatus;
      const canRemove = pStatus === 'wanted' && pId;
      const canDeleteLib = p.in_library && p.beets_album_id;
      let btnHtml;
      if (canAdd) {
        btnHtml = `<button class="btn btn-add" style="font-size:0.8em;padding:2px 8px;" onclick="event.stopPropagation(); window.addRelease('${p.release_id}', this)">Add</button>`;
      } else if (canRemove) {
        btnHtml = `<button class="btn" style="background:#5a2a2a;color:#f88;font-size:0.8em;padding:2px 8px;" onclick="event.stopPropagation(); window.disambRemove(${pId}, this)">Remove</button>`;
      } else if (canDeleteLib) {
        btnHtml = `<button class="btn" style="background:#3a2a2a;color:#f88;font-size:0.8em;padding:2px 8px;" onclick="event.stopPropagation(); window.disambDeleteFromLibrary(${p.beets_album_id}, ${pId || 'null'}, this)">Delete from library</button>`;
      } else {
        btnHtml = `<button class="btn btn-add" disabled style="font-size:0.8em;padding:2px 8px;">${p.in_library ? 'Owned' : pStatus || '?'}</button>`;
      }

      const exCount = pressingExclusiveCounts[i];
      const uLabel = exCount > 0 ? `<span style="color:${color};font-weight:600;margin-left:6px;">${exCount} exclusive</span>` : '';

      return `<div style="display:flex;justify-content:space-between;align-items:center;padding:2px 0;">
        <span><span style="color:${color};font-weight:bold;">●</span> ${esc(p.title)}${badges} <span style="color:#777;">${p.country || '?'} ${p.date || '?'} — ${esc(p.format)} — ${p.track_count}t</span>${uLabel}</span>
        ${btnHtml}
      </div>`;
    }).join('');
  }

  // Show tracks with colour dots matching which pressing(s) contain them
  if (rg.tracks && rg.tracks.length > 0) {
    html += '<div style="margin:8px 0 4px;color:#888;font-size:0.85em;">Recordings:</div>';
    const totalPressings = pressingRecSets.length;
    html += rg.tracks.map(t => {
      if (!t.unique) {
        const alsoOn = t.also_on && t.also_on.length > 0
          ? `<span style="color:#777;font-size:0.85em;margin-left:8px;">also on: ${t.also_on.map(esc).join(', ')}</span>`
          : '';
        return `<div class="lib-track" style="opacity:0.5;">
          <span style="color:#555;">  </span><span>${esc(t.title)}${alsoOn}</span>
        </div>`;
      }
      const pIdxs = trackToPressings[t.recording_id] || [];
      // If on all pressings, it's a common track — no dots needed
      if (pIdxs.length === totalPressings) {
        return `<div class="lib-track">
          <span style="color:#6d6;font-weight:bold;">★ </span><span>${esc(t.title)}</span>
        </div>`;
      }
      // Colour dots for tracks only on some pressings
      const dots = pIdxs.map(i => `<span style="color:${_PRESSING_COLORS[i % _PRESSING_COLORS.length]};">●</span>`).join('');
      return `<div class="lib-track">
        <span style="margin-right:4px;">${dots || '★'}</span><span>${esc(t.title)}</span>
      </div>`;
    }).join('');
  }

  el.innerHTML = html;
  el.style.display = 'block';
}

/**
 * Remove a pipeline request from the disambiguate view.
 * @param {number} pipelineId
 * @param {HTMLButtonElement} btn
 */
export async function disambRemove(pipelineId, btn) {
  if (!confirm(`Remove pipeline request #${pipelineId}?`)) return;
  btn.disabled = true;
  btn.textContent = '...';
  try {
    const r = await fetch(`${API}/api/pipeline/delete`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id: pipelineId}),
    });
    const data = await r.json();
    if (data.status === 'ok') {
      btn.textContent = 'Removed';
      btn.style.background = '#333';
      btn.style.color = '#666';
      invalidateBrowseArtist();
      toast(`Removed #${pipelineId}`);
      // Find the mbid for this pipeline ID so we can update the central store
      if (state.disambData) {
        for (const rg of state.disambData.release_groups) {
          for (const p of (rg.pressings || [])) {
            if (p.pipeline_id === pipelineId) {
              updatePipelineStatus(p.release_id, null, null);
              break;
            }
          }
        }
      }
    } else {
      btn.textContent = 'Error';
      toast(data.error || 'Remove failed', true);
    }
  } catch (e) {
    btn.textContent = 'Error';
    toast('Remove failed', true);
  }
}

/**
 * Delete an album from the beets library and optionally the pipeline.
 * @param {number} beetsId
 * @param {number|null} pipelineId
 * @param {HTMLButtonElement} btn
 */
export async function disambDeleteFromLibrary(beetsId, pipelineId, btn) {
  if (!confirm(`Delete album #${beetsId} from beets library and pipeline? This removes files from disk.`)) return;
  btn.disabled = true;
  btn.textContent = '...';
  try {
    const r = await fetch(`${API}/api/beets/delete`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id: beetsId, confirm: 'DELETE'}),
    });
    const data = await r.json();
    if (data.status === 'ok') {
      // Also delete from pipeline if there's a pipeline entry
      if (pipelineId) {
        await fetch(`${API}/api/pipeline/delete`, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({id: pipelineId}),
        });
      }
      btn.textContent = 'Deleted';
      btn.style.background = '#333';
      btn.style.color = '#666';
      invalidateBrowseArtist();
      toast(`Deleted: ${data.artist} - ${data.album} (${data.deleted_files} files)`);
    } else {
      btn.textContent = 'Error';
      toast(data.error || 'Delete failed', true);
    }
  } catch (e) {
    btn.textContent = 'Error';
    toast('Delete failed', true);
  }
}
