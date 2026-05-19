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
 * Render a "Spectral" cell value with grade-aware coloring and floor bitrate.
 *
 * Shown on both candidate and existing sides — every "spectral_grade" /
 * "spectral_bitrate" pair routes through here so the rendering rules
 * (grade colors, ~ prefix for floor, etc.) live in one place.
 * @param {string} grade
 * @param {number|string|undefined} bitrate
 */
function formatSpectral(grade, bitrate) {
  const sgColor = grade === 'genuine' ? '#6d6' : grade === 'suspect' ? '#d66' : '#aa8';
  // Show the spectral floor whenever it's present — even when the album's
  // rollup grade is `genuine`, a non-null spectral_bitrate means at least
  // one track triggered a cliff and the min-across-tracks is this value
  // (Eno case, download_log 3291).
  const label = bitrate ? `${esc(grade)} (~${esc(bitrate)}kbps)` : esc(grade);
  return `<span style="color:${sgColor};">${label}</span>`;
}

/**
 * Render a single download history item.
 *
 * Two side-by-side sections compare apples to apples:
 *   - "Downloaded" — what the candidate looked like (format, spectral,
 *     V0 probe, final stored format)
 *   - "On disk (before)" — what the library album looked like before this
 *     candidate (bitrate, spectral, V0 probe)
 *
 * Common audit rows (distance, triage chain, etc.) render below in a
 * 4-cell grid. Either side is omitted when it has no rows — the
 * "On disk" section disappears entirely on a fresh new-album import.
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

  // Candidate side — facts about the download being evaluated.
  const downloadedRows = [];
  if (h.downloaded_label) {
    downloadedRows.push(['Source', h.downloaded_label]);
  }
  if (h.spectral_grade) {
    downloadedRows.push(['Spectral', formatSpectral(h.spectral_grade, h.spectral_bitrate)]);
  }
  if (h.v0_probe_avg_bitrate) {
    downloadedRows.push(['V0 probe', formatV0Probe(h.v0_probe_avg_bitrate, h.v0_probe_kind)]);
  }
  if (h.final_format) {
    downloadedRows.push(['Stored as', esc(h.final_format)]);
  }

  // Existing side — facts about the library album as it was before.
  const onDiskRows = [];
  if (h.existing_min_bitrate) {
    onDiskRows.push(['Bitrate', `${esc(h.existing_min_bitrate)}kbps`]);
  }
  if (h.existing_spectral_bitrate) {
    onDiskRows.push([
      'Spectral',
      `<span style="color:#aa8;">~${esc(h.existing_spectral_bitrate)}kbps</span>`,
    ]);
  }
  if (h.existing_v0_probe_avg_bitrate) {
    onDiskRows.push([
      'V0 probe',
      formatV0Probe(h.existing_v0_probe_avg_bitrate, h.existing_v0_probe_kind),
    ]);
  }

  if (downloadedRows.length > 0 || onDiskRows.length > 0) {
    html += '<div class="p-hist-sides">';
    if (downloadedRows.length > 0) {
      html += '<div class="p-hist-side"><div class="p-hist-side-header">Downloaded</div>';
      for (const [label, value] of downloadedRows) {
        html += `<span class="p-hist-label">${label}</span><span class="p-hist-value">${value}</span>`;
      }
      html += '</div>';
    }
    if (onDiskRows.length > 0) {
      html += '<div class="p-hist-side"><div class="p-hist-side-header">On disk (before)</div>';
      for (const [label, value] of onDiskRows) {
        html += `<span class="p-hist-label">${label}</span><span class="p-hist-value">${value}</span>`;
      }
      html += '</div>';
    }
    html += '</div>';
  }

  // Common rows — not specific to either side. Rendered in a separate
  // grid below the side-by-side comparison so the visual grouping
  // ("here's the comparison, here's the audit metadata") stays clean.
  const commonRows = [];
  if (h.beets_distance != null) {
    commonRows.push(['Distance', parseFloat(h.beets_distance).toFixed(3)]);
  }
  const badExtensions = Array.isArray(h.bad_extensions) ? h.bad_extensions : [];
  if (badExtensions.length > 0) {
    commonRows.push([
      'Bad extension',
      `<span style="color:#ec6;">${esc(badExtensions.join(', '))}</span>`,
    ]);
  }
  if (h.wrong_match_triage_summary) {
    commonRows.push([
      'Triage',
      `<span style="color:#ec6;">${esc(h.wrong_match_triage_summary)}</span>`,
    ]);
  }
  const previewParts = [
    h.wrong_match_triage_preview_verdict,
    h.wrong_match_triage_preview_decision,
  ].filter(Boolean);
  if (previewParts.length > 0) {
    commonRows.push(['Preview', esc(previewParts.join(' / '))]);
  }
  if (
    h.wrong_match_triage_reason
    && !previewParts.includes(h.wrong_match_triage_reason)
  ) {
    commonRows.push(['Reason', esc(h.wrong_match_triage_reason)]);
  }
  const triageStages = Array.isArray(h.wrong_match_triage_stage_chain)
    ? h.wrong_match_triage_stage_chain
    : [];
  if (triageStages.length > 0) {
    commonRows.push(['Stages', esc(triageStages.join(' · '))]);
  }

  if (commonRows.length > 0) {
    html += '<div class="p-hist-grid">';
    for (const [label, value] of commonRows) {
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
  formatSpectral,
};
