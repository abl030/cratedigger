// @ts-check

/**
 * Standardised 2-button action toolbar for any release/album row across
 * the browse sub-tabs (Discography, Library, Analysis, Compare).
 *
 * Buttons:
 *   [Acquire]           one context-aware button. The three actionable
 *                       states are mutually exclusive — at any moment a
 *                       row is either "not yet wanted" (Add request),
 *                       "owned and could be improved" (Upgrade), or
 *                       "queued" (Remove request to cancel). Whichever
 *                       fits the current state is the live label; the
 *                       other states' affordances are unreachable so
 *                       showing them as separate greyed buttons just
 *                       added noise.
 *   [Remove from beets] enabled when in library and beets album id known
 *
 * Acquire decision tree (highest priority first):
 *   1. pipeline_status === 'wanted'  → "Remove request" enabled
 *      (cancels both fresh add-requests and queued upgrades — the
 *      user's mental model: "wanted means it's in the pipeline,
 *      remove it")
 *   2. pipeline_status === 'downloading' → "Remove request" disabled
 *      (in flight; backend can't cancel mid-download cleanly)
 *   3. in_library OR pipeline_status === 'imported' → "Upgrade" enabled
 *      (own it / previously imported — re-queue for higher quality)
 *   4. !in_library AND no pipeline_status → "Add request" enabled
 *      (fresh request)
 *   5. else (manual review, etc) → "Add request" disabled
 *
 * Action handlers are window-bound globals (addRelease, upgradeAlbum,
 * disambRemove, confirmDeleteBeets) — defined elsewhere; this module
 * just decides who gets clicked.
 */

import { esc } from './util.js';
import { pipelineStore } from './state.js';

/**
 * @typedef {Object} ActionItem
 * @property {string} id - Release ID (MB UUID or numeric Discogs)
 * @property {boolean} [in_library]
 * @property {number|null} [beets_album_id]
 * @property {string|null} [pipeline_status]
 * @property {number|null} [pipeline_id]
 * @property {boolean} [upgrade_queued]
 * @property {string} [artist] - For delete-from-beets confirmation
 * @property {string} [album] - For delete-from-beets confirmation
 * @property {number} [track_count] - For delete-from-beets confirmation
 */

/**
 * Render the toolbar HTML for one row.
 *
 * @param {ActionItem} item
 * @param {Object} [opts]
 * @param {string} [opts.size] - 'normal' or 'small' for compact layouts
 * @returns {string}
 */
export function renderActionToolbar(item, opts = {}) {
  // pipelineStore overlays the latest local pipeline state on top of the
  // backend snapshot — same pattern the existing pressing renderer uses.
  const stored = pipelineStore.get(item.id);
  const pStatus = stored ? stored.status : (item.pipeline_status || null);
  const pId = stored ? stored.id : (item.pipeline_id || null);
  const inLibrary = !!item.in_library;
  const beetsId = item.beets_album_id || null;
  const canRemoveBeets = inLibrary && !!beetsId;

  const sizeStyle = opts.size === 'small'
    ? 'padding:2px 8px;font-size:0.7em;'
    : 'padding:4px 10px;font-size:0.78em;';
  const baseStyle = `${sizeStyle}white-space:nowrap;`;

  const escId = esc(item.id);
  const artist = esc(item.artist || '');
  const album = esc(item.album || '');
  const trackCount = item.track_count || 0;

  // Acquire — single context-aware button. See module header for the
  // full priority order. The key invariant: at most one of Add/Upgrade/
  // Remove request is meaningful at any time, so we collapse them.
  let acquireBtn;
  if (pStatus === 'wanted' && pId) {
    // Cancellable. Covers fresh add-requests AND queued upgrades —
    // the user's framing: "if album is wanted (either path), Remove
    // request".
    acquireBtn = `<button class="btn" style="${baseStyle}background:#5a2a2a;color:#f88;" onclick="event.stopPropagation(); window.disambRemove(${pId}, this)">Remove request</button>`;
  } else if (pStatus === 'downloading') {
    // In flight; can't cleanly cancel mid-download.
    acquireBtn = `<button class="btn" style="${baseStyle}" disabled>Remove request</button>`;
  } else if (inLibrary || pStatus === 'imported') {
    acquireBtn = `<button class="btn btn-add" style="${baseStyle}" onclick="event.stopPropagation(); window.upgradeAlbum('${escId}', this)">Upgrade</button>`;
  } else if (!inLibrary && !pStatus) {
    acquireBtn = `<button class="btn btn-add" style="${baseStyle}" onclick="event.stopPropagation(); window.addRelease('${escId}', this)">Add request</button>`;
  } else {
    // Manual review or other terminal/unknown state — no live action.
    acquireBtn = `<button class="btn btn-add" style="${baseStyle}" disabled>Add request</button>`;
  }

  // Remove from beets — greyed out when not in library
  const removeBeetsBtn = canRemoveBeets
    ? `<button class="btn" style="${baseStyle}background:#3a2a2a;color:#f88;" onclick="event.stopPropagation(); window.confirmDeleteBeets(${beetsId}, '${artist}', '${album}', ${trackCount})">Remove from beets</button>`
    : `<button class="btn" style="${baseStyle}" disabled>Remove from beets</button>`;

  return `<span class="action-toolbar" style="display:inline-flex;gap:4px;flex-wrap:wrap;">${acquireBtn}${removeBeetsBtn}</span>`;
}
