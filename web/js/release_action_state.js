// @ts-check

import { pipelineStore, pipelineStoreKey } from './state.js';

/**
 * @typedef {'add' | 'upgrade' | 'remove_request' | 'disabled'} AcquireActionKind
 */

/**
 * @typedef {Object} ReleaseActionInput
 * @property {string} id
 * @property {boolean} [in_library]
 * @property {number|null} [beets_album_id]
 * @property {string|null} [pipeline_status]
 * @property {number|null} [pipeline_id]
 * @property {string} [artist]
 * @property {string} [album]
 * @property {number} [track_count]
 */

/**
 * Shared action/view model for browse-tab release actions.
 *
 * @typedef {Object} ReleaseActionState
 * @property {string} releaseId
 * @property {boolean} inLibrary
 * @property {number|null} beetsAlbumId
 * @property {string|null} pipelineStatus
 * @property {number|null} pipelineId
 * @property {string} artist
 * @property {string} album
 * @property {number} trackCount
 * @property {AcquireActionKind} acquireKind
 * @property {boolean} canRemoveBeets
 */

/**
 * @param {unknown} value
 * @returns {number|null}
 */
function toPositiveNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

/**
 * @param {unknown} value
 * @returns {number}
 */
function toCount(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
}

/**
 * Build the shared browse action state from any row/detail payload.
 * The central pipeline store overlays recent mutations so all browse
 * surfaces render the same action semantics after local writes.
 *
 * @param {ReleaseActionInput} item
 * @returns {ReleaseActionState}
 */
export function buildReleaseActionState(item) {
  const releaseId = pipelineStoreKey(item.id);
  const stored = releaseId ? pipelineStore.get(releaseId) : null;
  const pipelineStatus = stored ? stored.status : (item.pipeline_status || null);
  const pipelineId = stored ? toPositiveNumber(stored.id) : toPositiveNumber(item.pipeline_id);
  const inLibrary = !!item.in_library;
  const beetsAlbumId = toPositiveNumber(item.beets_album_id);

  /** @type {AcquireActionKind} */
  let acquireKind = 'disabled';
  if ((pipelineStatus === 'wanted' || pipelineStatus === 'downloading') && pipelineId) {
    acquireKind = 'remove_request';
  } else if (releaseId && (inLibrary || pipelineStatus === 'imported')) {
    acquireKind = 'upgrade';
  } else if (releaseId && !inLibrary && !pipelineStatus) {
    acquireKind = 'add';
  }

  return {
    releaseId,
    inLibrary,
    beetsAlbumId,
    pipelineStatus,
    pipelineId,
    artist: item.artist || '',
    album: item.album || '',
    trackCount: toCount(item.track_count),
    acquireKind,
    canRemoveBeets: inLibrary && !!beetsAlbumId,
  };
}
