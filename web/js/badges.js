// @ts-check

/**
 * Single source of truth for status badges shown on every release/album
 * row across the browse sub-tabs (Discography, Library, Analysis,
 * Compare) — and the per-pressing rows inside expanded views.
 *
 * Same code = same display. If a row is in library, the badge always
 * looks the same and shows the same on-disk quality summary. If it's
 * queued, the wanted badge looks the same. Etc.
 *
 * The in-library badge is "expanded" with a compact on-disk quality
 * suffix (e.g. "in library · M V2", "in library · F", "in library ·
 * O 128") when the caller passes library_format / library_avg_bitrate.
 * Falls back to plain "in library" when those fields are absent.
 */

import { pipelineStore, pipelineStoreKey } from './state.js';
import { esc, qualityLabelShort } from './util.js';
import { qualityRankBadgeClass } from './quality_palette.js';

/**
 * @typedef {Object} BadgeItem
 * @property {string} [id] - Used to look up live mutations in pipelineStore
 * @property {boolean} [in_library]
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
 *   'wanted' | 'downloading' | 'imported' | 'unsearchable' | null
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

  let html = '';
  if (item.in_library) {
    const q = qualityLabelShort(
      item.library_format || '',
      item.library_avg_bitrate || 0,
    );
    const suffix = q && q !== '?' ? ` · ${esc(q)}` : '';
    // Rank colour overrides the default blue when the backend supplied
    // a codec-aware tier. Falls back to badge-library blue when not.
    const rank = (item.library_rank || '').toLowerCase();
    const cls = rank ? qualityRankBadgeClass(rank) : 'badge-library';
    html += `<span class="badge ${cls}">in library${suffix}</span>`;
  }
  // Quality identity of the tracked install (issue #711 provisional
  // surfacing): verified is terminal; provisional means an unverified
  // lossless-source conversion the pipeline is still trying to verify.
  if (item.pipeline_verified_lossless) {
    html += '<span class="badge badge-verified badge-rank-lossless" title="verified lossless source — search complete">verified</span>';
  } else if (item.pipeline_provisional) {
    html += '<span class="badge badge-provisional" title="unverified lossless-source conversion — still hunting a verified lossless copy">provisional</span>';
  }
  if (pStatus === 'wanted') html += '<span class="badge badge-wanted">wanted</span>';
  if (pStatus === 'downloading') html += '<span class="badge badge-downloading">downloading</span>';
  if (pStatus === 'imported') html += '<span class="badge badge-imported">imported</span>';
  if (pStatus === 'unsearchable') html += '<span class="badge badge-unsearchable">unsearchable</span>';
  return html;
}
