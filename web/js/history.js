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
 * Every "spectral_grade" / "spectral_bitrate" pair routes through here so
 * the rendering rules (grade colors, ~ prefix for floor) live in one place.
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
 * Append an inline "(was X)" suffix to a candidate value so the
 * existing-side comparison reads on the same row instead of in a
 * separate section. Returns the bare value when ``wasValue`` is null.
 * @param {string} value - already-escaped candidate value (HTML)
 * @param {string|null} wasValue - already-escaped existing value (HTML), or null
 */
function withWas(value, wasValue) {
  if (wasValue === null || wasValue === undefined) return value;
  return `${value} <span class="p-hist-was">(was ${wasValue})</span>`;
}

/**
 * Render a single download history item as one consistent label/value grid.
 *
 * Every entry uses the same row vocabulary regardless of source codec:
 *   Source / Spectral / V0 probe / Bitrate / Stored as
 * Existing-side data appears inline as "(was Xkbps)" inside the value
 * cell, so each metric is apples-to-apples on the same row. The grid is
 * a 4-cell row (label/value/label/value), which collapses to 2-cell on
 * narrow viewports. The V0 probe row only renders for true lossless-
 * source probes (kind=lossless_source_v0); for non-lossless candidates
 * the same data already shows in the Bitrate row, so a second "(measurement)"
 * row would be redundant.
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
    rows.push(['Source', h.downloaded_label]);
  }

  if (h.spectral_grade) {
    const candidate = formatSpectral(h.spectral_grade, h.spectral_bitrate);
    const was = h.existing_spectral_bitrate
      ? `<span style="color:#aa8;">~${esc(h.existing_spectral_bitrate)}kbps</span>`
      : null;
    rows.push(['Spectral', withWas(candidate, was)]);
  }

  // V0 probe row: only for true lossless-source probes (kind ==
  // 'lossless_source_v0'). Non-lossless candidates carry a v0_probe
  // populated from their avg bitrate measurement — useful for
  // backend policy decisions, but redundant with the Bitrate row in
  // the UI, where the same number already appears.
  if (
    h.v0_probe_avg_bitrate
    && h.v0_probe_kind === 'lossless_source_v0'
  ) {
    const candidate = formatV0Probe(h.v0_probe_avg_bitrate, h.v0_probe_kind);
    const was = h.existing_v0_probe_avg_bitrate
      ? `${esc(h.existing_v0_probe_avg_bitrate)}kbps avg`
      : null;
    rows.push(['V0 probe', withWas(candidate, was)]);
  }

  // Bitrate row — apples-to-apples between candidate and existing on
  // min bitrate (kbps). Always present when either side has data.
  const candidateMin = h.actual_min_bitrate;
  const existingMin = h.existing_min_bitrate;
  if (candidateMin || existingMin) {
    const candidate = candidateMin ? `${esc(candidateMin)}kbps` : '—';
    const was = existingMin ? `${esc(existingMin)}kbps` : null;
    rows.push(['Bitrate', withWas(candidate, was)]);
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
  formatSpectral,
  withWas,
};
