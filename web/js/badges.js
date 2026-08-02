// @ts-check

/**
 * Single source of truth for status badges shown on every release/album
 * row across the browse sub-tabs (Discography, Library, Analysis,
 * Compare) — and the per-pressing rows inside expanded views.
 *
 * Same code = same display. Presence, captured history, installed
 * quality, carried proof, lifecycle, and exact request tracking remain
 * independent facts. A compact quality label (for example M V2, F, or
 * O 128) is therefore its own badge beside the current-holding badge.
 */

import { pipelineStore, pipelineStoreKey } from './state.js';
import { esc, qualityLabelShort } from './util.js';
import { qualityRankBadgeClass } from './quality_palette.js';
import { processingOwnerPresentation } from './release_action_state.js';

/**
 * @typedef {Object} BadgeItem
 * @property {string} [id] - Used to look up live mutations in pipelineStore
 * @property {boolean} [in_library]
 * @property {boolean} [has_captured_history] - A durable successful
 *   acquisition witness, or the admitted current imported-status legacy
 *   fallback supplied by the backend projection.
 * @property {string|null|undefined} [library_format] - "MP3", "FLAC", etc.
 * @property {number|null|undefined} [library_min_bitrate] - kbps floor; not a rank signal
 * @property {number|null|undefined} [library_avg_bitrate] - positive-track mean kbps
 * @property {string|null|undefined} [library_rank] - lowercase QualityRank
 *   name from the codec-aware rank gate ('lossless' | 'transparent' |
 *   'excellent' | 'good' | 'acceptable' | 'poor' | 'unknown'). When
 *   present, drives the badge's colour class so the user sees at a
 *   glance whether their on-disk copy is high or low quality (codec
 *   matters: Opus 128 is transparent, MP3 128 is poor — same bitrate,
 *   different rank).
 * @property {string|null|undefined} [pipeline_status]
 *   'wanted' | 'downloading' | 'processing' | 'imported' | 'unsearchable' |
 *   'replaced' | null
 * @property {import('./release_action_state.js').ProcessingOwnerProjection|null} [processing_owner]
 * @property {boolean} [pipeline_verified_lossless] - The tracked install
 *   carries a verified-lossless proof (terminal quality identity).
 * @property {boolean} [pipeline_provisional] - The tracked install is an
 *   unverified lossless-source conversion (provisional import — the
 *   pipeline is still hunting a verified lossless copy).
 */

/**
 * Render the standardised badge HTML for one row or pressing.
 *
 * @param {BadgeItem} item
 * @returns {string}
 */
export function renderStatusBadges(item) {
  const key = item.id ? pipelineStoreKey(item.id) : '';
  const stored = key ? pipelineStore.get(key) : null;
  const pStatus = stored ? stored.status : (item.pipeline_status || null);
  const owner = stored
    ? stored.processing_owner
    : (item.processing_owner || null);
  const hasCapturedHistory = stored
    ? stored.has_captured_history
    : item.has_captured_history === true;
  const verifiedLossless = stored
    ? stored.pipeline_verified_lossless
    : item.pipeline_verified_lossless === true;
  const provisional = stored
    ? stored.pipeline_provisional
    : item.pipeline_provisional === true;
  const processing = processingOwnerPresentation(pStatus, owner);

  let html = '';
  if (item.in_library) {
    html += '<span class="badge badge-library" title="currently held in the library">in library</span>';
    const q = qualityLabelShort(
      item.library_format || '',
      item.library_avg_bitrate || 0,
    );
    if (q && q !== '?') {
      // The quality band is an observation about the current holding,
      // not the presence or acquisition-history badge itself.
      const rank = (item.library_rank || '').toLowerCase();
      const cls = rank ? qualityRankBadgeClass(rank) : 'badge-library';
      const label = esc(q);
      html += `<span class="badge badge-quality-outline ${cls}" title="current library quality: ${label}" aria-label="current library quality: ${label}">${label}</span>`;
    }
  }
  if (hasCapturedHistory) {
    html += '<span class="badge badge-captured" title="successfully acquired at least once">captured</span>';
    if (!item.in_library) {
      html += '<span class="badge badge-missing" title="captured previously; not currently held in the library">missing</span>';
    }
  }
  // Quality identity of the tracked install (issue #711 provisional
  // surfacing): verified is terminal; provisional means an unverified
  // lossless-source conversion the pipeline is still trying to verify.
  if (verifiedLossless) {
    html += '<span class="badge badge-verified badge-rank-lossless" title="tracked install carries verified lossless-source proof">verified</span>';
  } else if (provisional) {
    html += '<span class="badge badge-provisional" title="unverified lossless-source conversion — still hunting a verified lossless copy">provisional</span>';
  }
  if (pStatus === 'wanted') html += '<span class="badge badge-wanted">wanted</span>';
  if (pStatus === 'downloading') html += '<span class="badge badge-downloading">downloading</span>';
  if (processing) {
    html += `<span class="badge ${processing.badgeClass}" title="${esc(processing.lockReason)}">${esc(processing.label)}</span>`;
  }
  if (pStatus === 'unsearchable') html += '<span class="badge badge-unsearchable">unsearchable</span>';
  if (pStatus === 'replaced') {
    html += '<span class="badge badge-replaced" title="acquisition request superseded by another exact release">replaced</span>';
  }
  if (item.in_library && !pStatus) {
    html += '<span class="badge badge-untracked" title="currently held; no exact pipeline request tracks this release">untracked</span>';
  }
  return html;
}
