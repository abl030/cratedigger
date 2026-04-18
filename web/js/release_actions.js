// @ts-check

/**
 * Standardised 4-button action toolbar for any release/album row across
 * the browse sub-tabs (Discography, Library, Analysis, Compare). Every
 * button is always rendered; non-applicable ones are disabled (greyed)
 * so the row's available actions read at a glance.
 *
 * Buttons:
 *   [Add request]      enabled when not in library AND not in pipeline
 *   [Upgrade]          enabled when in library OR pipeline status is
 *                      'imported'; shows "Queued" when upgrade is
 *                      already queued
 *   [Remove request]   enabled when pipeline status is 'wanted'
 *                      (the only cancellable state)
 *   [Remove from beets] enabled when in library and beets album id known
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
  const upgradeQueued = !!item.upgrade_queued;

  const canAdd = !inLibrary && !pStatus;
  const canUpgrade = (inLibrary || pStatus === 'imported') && !upgradeQueued;
  const canRemoveReq = pStatus === 'wanted' && !!pId;
  const canRemoveBeets = inLibrary && !!beetsId;

  const sizeStyle = opts.size === 'small'
    ? 'padding:2px 8px;font-size:0.7em;'
    : 'padding:4px 10px;font-size:0.78em;';
  const baseStyle = `${sizeStyle}white-space:nowrap;`;

  const escId = esc(item.id);
  const artist = esc(item.artist || '');
  const album = esc(item.album || '');
  const trackCount = item.track_count || 0;

  // Add request
  const addBtn = canAdd
    ? `<button class="btn btn-add" style="${baseStyle}" onclick="event.stopPropagation(); window.addRelease('${escId}', this)">Add request</button>`
    : `<button class="btn btn-add" style="${baseStyle}" disabled>Add request</button>`;

  // Upgrade
  let upgradeBtn;
  if (upgradeQueued) {
    upgradeBtn = `<button class="btn" style="${baseStyle}border-color:#6a9;color:#6a9;" disabled>Queued</button>`;
  } else if (canUpgrade) {
    upgradeBtn = `<button class="btn" style="${baseStyle}" onclick="event.stopPropagation(); window.upgradeAlbum('${escId}', this)">Upgrade</button>`;
  } else {
    upgradeBtn = `<button class="btn" style="${baseStyle}" disabled>Upgrade</button>`;
  }

  // Remove request (cancel a wanted entry)
  const removeReqBtn = canRemoveReq
    ? `<button class="btn" style="${baseStyle}background:#5a2a2a;color:#f88;" onclick="event.stopPropagation(); window.disambRemove(${pId}, this)">Remove request</button>`
    : `<button class="btn" style="${baseStyle}" disabled>Remove request</button>`;

  // Remove from beets — greyed out when not in library
  const removeBeetsBtn = canRemoveBeets
    ? `<button class="btn" style="${baseStyle}background:#3a2a2a;color:#f88;" onclick="event.stopPropagation(); window.confirmDeleteBeets(${beetsId}, '${artist}', '${album}', ${trackCount})">Remove from beets</button>`
    : `<button class="btn" style="${baseStyle}" disabled>Remove from beets</button>`;

  return `<span class="action-toolbar" style="display:inline-flex;gap:4px;flex-wrap:wrap;">${addBtn}${upgradeBtn}${removeReqBtn}${removeBeetsBtn}</span>`;
}
