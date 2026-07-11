// @ts-check
/**
 * Unique-track analysis overlay (issue #575 PR4).
 *
 * The old Analysis sub-view is gone; its intelligence now decorates the
 * unified artist page. The disambiguate payload (state.disambData, from
 * /api/artist/<id>/disambiguate) drives two decorations:
 *
 *  - applyAnalysisChips: "N unique" / "covered by X" chips on the
 *    release-group rows (matched by data-rg-id), applied when the
 *    payload lands — never a re-render, so expansion state survives.
 *  - applyAnalysisToExpansion: colour dots + exclusive counts on the
 *    pressing rows inside an expanded release group, plus the
 *    recordings breakdown block, applied by loadReleaseGroup's
 *    post-render hook.
 */
import { state, API, toast, updatePipelineStatus } from './state.js';
import { esc } from './util.js';
import { invalidateBrowseArtist } from './browse.js';
import { cssEscape } from './discography.js';

/** @type {string[]} */
export const _PRESSING_COLORS = ['#6af','#fa6','#6d6','#f6a','#af6','#6ff','#ff6','#a6f'];

/**
 * The per-release-group analysis chip: unique-track count or coverage.
 * @param {Object} rg - Disambiguate release-group row
 * @returns {string}
 */
export function analysisChipHtml(rg) {
  if (rg.covered_by) {
    return `<span style="color:#777;font-size:0.85em;margin-left:6px;white-space:nowrap;">covered by ${esc(rg.covered_by)}</span>`;
  }
  if (rg.unique_track_count > 0) {
    return `<span style="color:#6d6;font-weight:600;margin-left:6px;white-space:nowrap;">${rg.unique_track_count} unique</span>`;
  }
  return '<span style="color:#555;margin-left:6px;white-space:nowrap;">0 unique</span>';
}

/**
 * Decorate rendered release-group rows with analysis chips. Idempotent —
 * a row that already carries a chip is skipped, so late re-application
 * (cache-hit re-render) is safe.
 * @param {HTMLElement} containerEl - The artist-page container.
 * @param {Object} disambData - /api/artist/<id>/disambiguate payload.
 */
export function applyAnalysisChips(containerEl, disambData) {
  for (const rg of disambData.release_groups || []) {
    const row = containerEl.querySelector(`.rg[data-rg-id="${cssEscape(rg.release_group_id)}"]`);
    if (!row || row.querySelector('.disamb-chip')) continue;
    const title = row.querySelector('.rg-title');
    if (!title) continue;
    title.insertAdjacentHTML('afterend', `<span class="disamb-chip">${analysisChipHtml(rg)}</span>`);
  }
}

/**
 * Per-recording pressing membership + per-pressing exclusive counts.
 * Pure — Node-testable.
 * @param {Object} rg - Disambiguate release-group row (pressings + tracks)
 * @returns {{trackToPressings: Object<string, number[]>, pressingExclusiveCounts: number[], totalPressings: number}}
 */
export function computeRecordingDots(rg) {
  const pressingRecSets = (rg.pressings || []).map(p => new Set(p.recording_ids || []));
  /** @type {Object<string, number[]>} */
  const trackToPressings = {};
  for (const t of rg.tracks || []) {
    trackToPressings[t.recording_id] = [];
    for (let i = 0; i < pressingRecSets.length; i++) {
      if (pressingRecSets[i].has(t.recording_id)) {
        trackToPressings[t.recording_id].push(i);
      }
    }
  }
  const pressingExclusiveCounts = pressingRecSets.map((recSet, i) => {
    let count = 0;
    for (const recId of recSet) {
      const onPressings = trackToPressings[recId];
      if (onPressings && onPressings.length === 1 && onPressings[0] === i) count++;
    }
    return count;
  });
  return { trackToPressings, pressingExclusiveCounts, totalPressings: pressingRecSets.length };
}

/**
 * The recordings breakdown for an expanded release group: one row per
 * recording, colour dots matching the pressing rows above it. Pure —
 * Node-testable.
 * @param {Object} rg - Disambiguate release-group row
 * @returns {string}
 */
export function renderRecordingsBlock(rg) {
  if (!rg.tracks || rg.tracks.length === 0) return '';
  const { trackToPressings, totalPressings } = computeRecordingDots(rg);
  // Styled like the collapsible sub-section headers (Bootleg / Promo)
  // so the block reads as its own section of the expansion, not as a
  // tracklist of whichever pressing row happens to render above it.
  let html = '<div class="type-header" style="cursor:default;padding-left:0;" '
    + 'onclick="event.stopPropagation()">Recordings <span class="type-count">across pressings</span></div>';
  // One span per row — .lib-track is flex justify-between, so marker
  // and title must live together or they get pushed to opposite edges.
  html += rg.tracks.map(t => {
    if (!t.unique) {
      const alsoOn = t.also_on && t.also_on.length > 0
        ? `<span style="color:#777;font-size:0.85em;margin-left:8px;">also on: ${t.also_on.map(esc).join(', ')}</span>`
        : '';
      return `<div class="lib-track" style="opacity:0.5;">
        <span>${esc(t.title)}${alsoOn}</span>
      </div>`;
    }
    const pIdxs = trackToPressings[t.recording_id] || [];
    // If on all pressings, it's a common track — no dots needed
    if (pIdxs.length === totalPressings) {
      return `<div class="lib-track">
        <span><span style="color:#6d6;font-weight:bold;">★</span> ${esc(t.title)}</span>
      </div>`;
    }
    // Colour dots for tracks only on some pressings
    const dots = pIdxs.map(i => `<span style="color:${_PRESSING_COLORS[i % _PRESSING_COLORS.length]};">●</span>`).join('');
    return `<div class="lib-track">
      <span><span style="margin-right:4px;">${dots || '★'}</span>${esc(t.title)}</span>
    </div>`;
  }).join('');
  return html;
}

/**
 * Decorate an expanded release group's pressing rows with colour dots +
 * exclusive counts and append the recordings breakdown. Called by
 * loadReleaseGroup's post-render hook; no-op unless disambiguate data
 * for this release group is loaded. Idempotent via the marker class on
 * the appended block.
 * @param {HTMLElement} relEl - The .releases container that just rendered.
 * @param {string} rgId - Release-group id that was expanded.
 */
export function applyAnalysisToExpansion(relEl, rgId) {
  const rg = state.disambData?.release_groups?.find(
    (g) => g.release_group_id === rgId);
  if (!rg) return;
  if (relEl.querySelector('.disamb-recordings')) return;
  const { pressingExclusiveCounts } = computeRecordingDots(rg);
  (rg.pressings || []).forEach((p, i) => {
    const row = relEl.querySelector(`.release[data-release-id="${cssEscape(String(p.release_id))}"]`);
    if (!row) return;
    const title = row.querySelector('.release-title');
    if (!title) return;
    const color = _PRESSING_COLORS[i % _PRESSING_COLORS.length];
    title.insertAdjacentHTML('afterbegin', `<span style="color:${color};font-weight:bold;">● </span>`);
    const exCount = pressingExclusiveCounts[i];
    if (exCount > 0) {
      title.insertAdjacentHTML('beforeend',
        `<span style="color:${color};font-weight:600;margin-left:6px;white-space:nowrap;">${exCount} exclusive</span>`);
    }
  });
  const block = renderRecordingsBlock(rg);
  if (block) {
    // border-top separator: without it the block visually attaches to
    // the Bootleg / Promo sub-section rendered directly above it.
    relEl.insertAdjacentHTML('beforeend',
      `<div class="disamb-recordings" style="padding:4px 10px 8px;margin-top:10px;border-top:1px solid #2a2a2a;">${block}</div>`);
  }
}

/**
 * Decorate every ALREADY-EXPANDED release group once the disambiguate
 * payload lands. The manual-expand path decorates via loadReleaseGroup's
 * post-render hook, but an expansion that rendered BEFORE the payload
 * arrived (search-by-ID auto-expand, or a fast manual click) would
 * otherwise stay bare until collapsed and re-opened.
 * @param {HTMLElement} containerEl - The artist-page container.
 * @param {Object} disambData - /api/artist/<id>/disambiguate payload.
 */
export function applyAnalysisToOpenExpansions(containerEl, disambData) {
  for (const rg of disambData.release_groups || []) {
    const relEl = /** @type {HTMLElement|null} */ (
      containerEl.querySelector(`#rel-${cssEscape(rg.release_group_id)}`));
    if (relEl && relEl.innerHTML && !relEl.querySelector('.loading')) {
      applyAnalysisToExpansion(relEl, rg.release_group_id);
    }
  }
}

/**
 * Remove a pipeline request from an artist-page row.
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
