// @ts-check
import { awstDateTime, esc } from './util.js';

/**
 * Render a single V0 probe value with its provenance suffix.
 *
 * - ``lossless_source_v0`` is the gold standard — render bare ("260kbps avg"),
 *   the source proof itself is the message.
 * - ``native_lossy_research_v0`` means "we measured the candidate's avg
 *   bitrate, but it's not a comparable lossless-source probe" — render
 *   "(measurement)" so the reader doesn't misread it as an upgrade signal.
 * - Anything else falls back to showing the raw kind so debug rows are
 *   still legible.
 * @param {number|string} avg
 * @param {string|undefined} kind
 * @returns {string}
 */
function formatV0Probe(avg, kind) {
  const base = `${esc(avg)}kbps avg`;
  if (!kind || kind === 'lossless_source_v0') {
    return base;
  }
  if (kind === 'native_lossy_research_v0') {
    return `${base} (measurement)`;
  }
  return `${base} (${esc(kind)})`;
}

/**
 * Render a single download history item as a 2-column label/value grid.
 *
 * Every download (lossless source, MP3 V0, CBR 320, ...) produces the same
 * shape of row. Fields that have no value for a particular entry are simply
 * omitted from the grid; the renderer never invents placeholders, but the
 * V0 probe row is expected to be present on every row after migration 024
 * backfills legacy NULLs.
 * @param {Object} h - Download history entry from the API
 * @returns {string} HTML string
 */
export function renderDownloadHistoryItem(h) {
  const outcome = h.outcome || '?';
  const color = outcome === 'success' ? '#6d6' : outcome === 'rejected' ? '#d88'
    : outcome === 'force_import' ? '#6af' : '#aa8';
  const user = h.soulseek_username || '?';
  const date = awstDateTime(h.created_at || '');

  let html = `<div class="p-hist-header">
    <span style="color:${color};">${outcome === 'force_import' ? 'force imported' : outcome}</span>
    <span style="color:#888;">${esc(user)}</span>
    <span style="color:#555;">${date}</span>
  </div>`;

  const rows = [];

  if (h.downloaded_label) {
    rows.push(['Downloaded', h.downloaded_label]);
  }

  if (h.spectral_grade) {
    const sgColor = h.spectral_grade === 'genuine' ? '#6d6' : h.spectral_grade === 'suspect' ? '#d66' : '#aa8';
    let sgLabel = h.spectral_grade;
    // Show the spectral floor whenever it's present — even when the album's
    // rollup grade is `genuine`, a non-null spectral_bitrate means at least
    // one track triggered a cliff and the min-across-tracks is this value.
    // Hiding it makes "genuine + 96k floor" look indistinguishable from
    // "genuine + no cliff" (Eno case, download_log 3291) — the user reads
    // just "genuine" and doesn't see the partial-cliff signal that
    // compare_quality's shared-spectral clamp now acts on.
    if (h.spectral_bitrate) {
      sgLabel += ` (~${h.spectral_bitrate}kbps)`;
    }
    rows.push(['Spectral', `<span style="color:${sgColor};">${sgLabel}</span>`]);
  }

  if (h.v0_probe_avg_bitrate) {
    rows.push(['V0 probe', formatV0Probe(h.v0_probe_avg_bitrate, h.v0_probe_kind)]);
  }

  const existingBitrates = [];
  if (h.existing_min_bitrate) {
    existingBitrates.push(`${esc(h.existing_min_bitrate)}kbps`);
  }
  if (h.existing_spectral_bitrate) {
    existingBitrates.push(`~${esc(h.existing_spectral_bitrate)}kbps (spectral)`);
  }
  if (h.existing_v0_probe_avg_bitrate) {
    existingBitrates.push(`${esc(h.existing_v0_probe_avg_bitrate)}kbps source V0 avg`);
  }
  if (existingBitrates.length > 0) {
    rows.push(['On disk (before)', existingBitrates.join(' / ')]);
  }

  if (h.final_format) {
    rows.push(['Stored as', esc(h.final_format)]);
  }

  if (h.beets_distance != null) {
    rows.push(['Distance', parseFloat(h.beets_distance).toFixed(3)]);
  }

  const badExtensions = Array.isArray(h.bad_extensions) ? h.bad_extensions : [];
  if (badExtensions.length > 0) {
    rows.push([
      'Bad extension',
      `<span style="color:#ec6;">${esc(badExtensions.join(', '))}</span>`,
    ]);
  }

  if (h.wrong_match_triage_summary) {
    rows.push([
      'Triage',
      `<span style="color:#ec6;">${esc(h.wrong_match_triage_summary)}</span>`,
    ]);
  }

  const previewParts = [
    h.wrong_match_triage_preview_verdict,
    h.wrong_match_triage_preview_decision,
  ].filter(Boolean);
  if (previewParts.length > 0) {
    rows.push(['Preview', esc(previewParts.join(' / '))]);
  }

  if (
    h.wrong_match_triage_reason
    && !previewParts.includes(h.wrong_match_triage_reason)
  ) {
    rows.push(['Reason', esc(h.wrong_match_triage_reason)]);
  }

  const triageStages = Array.isArray(h.wrong_match_triage_stage_chain)
    ? h.wrong_match_triage_stage_chain
    : [];
  if (triageStages.length > 0) {
    rows.push(['Stages', esc(triageStages.join(' · '))]);
  }

  if (rows.length > 0) {
    html += '<div class="p-hist-grid">';
    for (const [label, value] of rows) {
      html += `<span class="p-hist-label">${label}</span><span class="p-hist-value">${value}</span>`;
    }
    html += '</div>';
  }

  const verdict = h.verdict || h.beets_scenario || '';
  if (verdict) {
    html += `<div class="p-hist-verdict">${esc(verdict)}</div>`;
  }

  return `<div class="p-hist-item">${html}</div>`;
}

export const __test__ = {
  formatV0Probe,
};
