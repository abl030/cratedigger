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
 * Value phrase for one side of a persisted comparison basis: "avg 288k",
 * or "~250k" when the rank branch classified spectral-clamped values
 * (a clamped number is min(metric, spectral floor) — labelling it with
 * the metric would lie, which is what the basis exists to prevent).
 * @param {Object} basis - comparison_basis dict from the API
 * @param {'new'|'existing'} side
 * @returns {string} escaped HTML fragment
 */
function basisValuePhrase(basis, side) {
  const value = side === 'new' ? basis.new_value_kbps : basis.existing_value_kbps;
  const metric = side === 'new' ? basis.new_metric : basis.existing_metric;
  if (value === null || value === undefined) return 'unmeasured';
  if (basis.spectral_clamped && basis.branch === 'rank') return `~${esc(value)}k`;
  return `${esc(metric)} ${esc(value)}k`;
}

/**
 * One side of the basis as "MP3 avg 288k · transparent" for the strip.
 * @param {Object} basis
 * @param {'new'|'existing'} side
 * @returns {string} escaped HTML fragment
 */
function basisSidePhrase(basis, side) {
  const fmt = side === 'new' ? basis.new_format : basis.existing_format;
  const rank = side === 'new' ? basis.new_rank : basis.existing_rank;
  return `${esc((fmt || '?').toUpperCase())} ${basisValuePhrase(basis, side)}`
    + ` · ${esc(rank)}`;
}

/**
 * Compact one-line IN/HAVE evidence comparison for list rows.
 *
 * Same numbers as the detail grid (measured incoming vs on-disk at the
 * time of THIS download), compressed for glance-ability: the Recents
 * list renders it under the title so quality decisions read without
 * expanding the card. Returns '' when neither side has measurements
 * (download-phase failures have nothing to compare).
 * @param {Object} h - Download history entry / recents log item from the API
 * @returns {string} HTML string, or '' when there is no evidence
 */
export function renderEvidenceStrip(h) {
  // A strip is a comparison — it needs at least one NUMBER. A codec
  // label alone (failed downloads carry a filetype but no measurements)
  // would render a noisy "IN MP3 HAVE —" on every failure row.
  const hasMeasurement = Boolean(
    h.actual_min_bitrate || h.spectral_bitrate || h.spectral_grade
    || h.v0_probe_avg_bitrate || h.existing_min_bitrate
    || h.existing_spectral_bitrate || h.existing_v0_probe_avg_bitrate
    || h.comparison_basis,
  );
  if (!hasMeasurement) return '';

  const basis = h.comparison_basis || null;
  const inParts = [];
  if (basis) {
    // The persisted basis IS the comparison the decider performed —
    // render it instead of re-deriving labels from min bitrate (request
    // 6039: min-derived labels turned a real avg 196→288 rank upgrade
    // into "IN MP3 V2 · 194k HAVE MP3 194k").
    inParts.push(basisSidePhrase(basis, 'new'));
  } else {
    if (h.downloaded_label) inParts.push(esc(h.downloaded_label));
    if (h.actual_min_bitrate) inParts.push(`${esc(h.actual_min_bitrate)}k`);
  }
  if (h.spectral_grade) {
    // With a basis, the clamped rank value already carries the floor —
    // repeating "~250k" in the grade chip would double it up.
    const floor = (!basis && h.spectral_bitrate) ? `~${esc(h.spectral_bitrate)}k ` : '';
    const sgColor = h.spectral_grade === 'genuine' ? '#6d6'
      : h.spectral_grade === 'suspect' ? '#d66' : '#aa8';
    inParts.push(`<span style="color:${sgColor};">${floor}${esc(h.spectral_grade)}</span>`);
  }
  if (h.v0_probe_avg_bitrate && h.v0_probe_kind === 'lossless_source_v0') {
    inParts.push(`V0 ${esc(h.v0_probe_avg_bitrate)}k avg`);
  }

  const haveParts = [];
  // Lead with "MP3 256k" as one piece — the codec class is often the
  // deciding metric (a rank upgrade at equal bitrate is unreadable
  // without it).
  if (basis) {
    haveParts.push(basisSidePhrase(basis, 'existing'));
  } else if (h.existing_format && h.existing_min_bitrate) {
    haveParts.push(`${esc(h.existing_format)} ${esc(h.existing_min_bitrate)}k`);
  } else if (h.existing_format) {
    haveParts.push(esc(h.existing_format));
  } else if (h.existing_min_bitrate) {
    haveParts.push(`${esc(h.existing_min_bitrate)}k`);
  }
  if (h.existing_spectral_bitrate) haveParts.push(`~${esc(h.existing_spectral_bitrate)}k`);
  if (
    h.existing_v0_probe_avg_bitrate
    && h.existing_v0_probe_kind === 'lossless_source_v0'
  ) {
    haveParts.push(`V0 ${esc(h.existing_v0_probe_avg_bitrate)}k avg`);
  }

  if (inParts.length === 0 && haveParts.length === 0) return '';
  const inHtml = inParts.length ? inParts.join(' · ') : '—';
  const haveHtml = haveParts.length ? haveParts.join(' · ') : '—';
  return `<span class="r-evidence"><span class="r-ev-tag">IN</span> ${inHtml}`
    + ` <span class="r-ev-tag">HAVE</span> ${haveHtml}</span>`;
}

/**
 * Render a single download history item as one consistent label/value grid.
 *
 * The verdict (the entry's story — "mbid_missing", "Upgrade: …") renders
 * FIRST, under the header, red on failure-family rows: the evidence grid
 * is context, not the outcome, and a rejection whose quality evidence all
 * reads positive must not bury its reason below the grid.
 *
 * Fixed schema (issue #575): the core vocabulary — Source / Spectral /
 * Bitrate / Distance — renders on EVERY entry, with an em-dash when a
 * side has no data, so adjacent entries never jump shape. Existing-side
 * data appears inline as "(was Xkbps)" inside the value cell, so each
 * metric is apples-to-apples on the same row. Semantic extras (V0 probe
 * for lossless sources, Stored as, Bad extension, the Triage operator
 * audit) render only when present; internal debug rows (Detail / Preview /
 * Reason / Stages) live behind a collapsed forensics toggle.
 *
 * The header uses the server-classified badge — the SAME vocabulary as
 * the Recents list rows — so a row the list calls "Failed" is never
 * relabelled "timeout" in the detail panel.
 *
 * Force imports render "overridden" in the Distance row: beets records
 * distance 0.0 when the operator forces a match, and painting that as a
 * perfect 0.000 misled operators (issue #575).
 * @param {Object} h - Download history entry from the API
 * @returns {string} HTML string
 */
export function renderDownloadHistoryItem(h) {
  const outcome = h.outcome || '?';
  const user = h.soulseek_username || '?';
  const date = awstDateTime(h.created_at || '');

  let status;
  if (h.badge && h.badge_class) {
    status = `<span class="badge ${esc(h.badge_class)}">${esc(h.badge)}</span>`;
  } else {
    const color = outcome === 'success' ? '#6d6' : outcome === 'rejected' ? '#d88'
      : outcome === 'force_import' ? '#6af' : '#aa8';
    status = `<span style="color:${color};">${outcome === 'force_import' ? 'force imported' : esc(outcome)}</span>`;
  }

  let html = `<div class="p-hist-header">
    ${status}
    <span style="color:#888;">${esc(user)}</span>
    <span style="color:#555;">${date}</span>
  </div>`;

  // The verdict is the entry's story — it leads, before the evidence
  // grid. On failure-family rows it takes the reject colour: a rejected
  // download whose quality evidence all reads positive (transparent vs
  // transparent, verified lossless bypass) must not tell a quality
  // success story with the actual reason buried as a dim line below
  // the grid (request 8781 / download_log 36660: mbid_missing).
  const FAILURE_OUTCOMES = ['rejected', 'failed', 'timeout', 'user_offline', 'curator_ban'];
  const verdict = h.verdict || h.beets_scenario || '';
  if (verdict) {
    const rejectCls = FAILURE_OUTCOMES.includes(outcome) ? ' p-hist-verdict-reject' : '';
    html += `<div class="p-hist-verdict${rejectCls}">${esc(verdict)}</div>`;
  }

  const rows = [];

  rows.push(['Source', h.downloaded_label ? esc(h.downloaded_label) : '—']);

  if (h.spectral_grade) {
    const candidate = formatSpectral(h.spectral_grade, h.spectral_bitrate);
    const was = h.existing_spectral_bitrate
      ? `<span style="color:#aa8;">~${esc(h.existing_spectral_bitrate)}kbps</span>`
      : null;
    rows.push(['Spectral', withWas(candidate, was)]);
  } else {
    rows.push(['Spectral', '—']);
  }

  // V0 probe row: only for true lossless-source probes (kind ==
  // 'lossless_source_v0'). Non-lossless candidates carry a v0_probe
  // populated from their avg bitrate measurement — useful for
  // backend policy decisions, but redundant with the Bitrate row in
  // the UI, where the same number already appears.
  //
  // The "(was X)" suffix is V0-probe-avg vs V0-probe-avg only — a true
  // apples-to-apples comparison against the library album's recorded
  // lossless-source probe. We deliberately do NOT fall back to the
  // existing raw min bitrate: painting a V0-probe avg next to a
  // container min bitrate reads as a fake upgrade (e.g. "239kbps avg
  // (was 92kbps)" compares two different metrics). When there's no
  // comparable existing probe, the row shows the candidate alone; the
  // Bitrate row below already carries the min-vs-min comparison.
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

  // Compared row — the persisted comparison basis, rendered verbatim.
  // This is the decision's own story (metric, values, ranks); the Bitrate
  // row below stays as the raw min-vs-min detail.
  if (h.comparison_basis) {
    const b = h.comparison_basis;
    let compared = `${basisValuePhrase(b, 'new')} (${esc(b.new_rank)})`
      + ` vs ${basisValuePhrase(b, 'existing')} (${esc(b.existing_rank)})`;
    if (b.verified_lossless_bypass) {
      compared += ' <span style="color:#6af;">· verified lossless bypass</span>';
    }
    rows.push(['Compared', compared]);
  }

  // Bitrate row — apples-to-apples between candidate and existing on
  // min bitrate (kbps). The was-side names the on-disk codec when known:
  // "256kbps (was MP3 256kbps)" explains a rank upgrade that the bare
  // numbers would contradict.
  const candidateMin = h.actual_min_bitrate;
  const existingMin = h.existing_min_bitrate;
  if (candidateMin || existingMin) {
    const candidate = candidateMin ? `${esc(candidateMin)}kbps` : '—';
    const wasFmt = h.existing_format ? `${esc(h.existing_format)} ` : '';
    const was = existingMin ? `${wasFmt}${esc(existingMin)}kbps` : null;
    rows.push(['Bitrate', withWas(candidate, was)]);
  } else {
    rows.push(['Bitrate', '—']);
  }

  if (h.final_format) {
    rows.push(['Stored as', esc(h.final_format)]);
  }

  if (outcome === 'force_import') {
    rows.push(['Distance', '<span style="color:#6af;">overridden</span>']);
  } else if (h.beets_distance != null) {
    rows.push(['Distance', parseFloat(h.beets_distance).toFixed(3)]);
  } else {
    rows.push(['Distance', '—']);
  }

  const badExtensions = Array.isArray(h.bad_extensions) ? h.bad_extensions : [];
  if (badExtensions.length > 0) {
    rows.push([
      'Bad extension',
      `<span style="color:#ec6;">${esc(badExtensions.join(', '))}</span>`,
    ]);
  }

  // Triage is the operator-action audit — it stays visible. The
  // internal decision internals (Preview / Reason / Stages) go behind
  // the forensics toggle below.
  if (h.wrong_match_triage_summary) {
    rows.push([
      'Triage',
      `<span style="color:#ec6;">${esc(h.wrong_match_triage_summary)}</span>`,
    ]);
  }

  const forensicRows = [];
  // The raw beets/harness detail (e.g. "Target MBID … not in candidates")
  // explains WHY a match-failure verdict fired — reachable, but debug-tier.
  // Skipped when it just repeats the verdict.
  if (h.beets_detail && h.beets_detail !== verdict) {
    forensicRows.push(['Detail', esc(h.beets_detail)]);
  }
  const previewParts = [
    h.wrong_match_triage_preview_verdict,
    h.wrong_match_triage_preview_decision,
  ].filter(Boolean);
  if (previewParts.length > 0) {
    forensicRows.push(['Preview', esc(previewParts.join(' / '))]);
  }

  if (
    h.wrong_match_triage_reason
    && !previewParts.includes(h.wrong_match_triage_reason)
  ) {
    forensicRows.push(['Reason', esc(h.wrong_match_triage_reason)]);
  }

  const triageStages = Array.isArray(h.wrong_match_triage_stage_chain)
    ? h.wrong_match_triage_stage_chain
    : [];
  if (triageStages.length > 0) {
    forensicRows.push(['Stages', esc(triageStages.join(' · '))]);
  }

  html += '<div class="p-hist-grid">';
  for (const [label, value] of rows) {
    html += `<span class="p-hist-label">${label}</span><span class="p-hist-value">${value}</span>`;
  }
  html += '</div>';

  if (forensicRows.length > 0) {
    let fhtml = '<div class="p-hist-grid">';
    for (const [label, value] of forensicRows) {
      fhtml += `<span class="p-hist-label">${label}</span><span class="p-hist-value">${value}</span>`;
    }
    fhtml += '</div>';
    html += `<details class="p-hist-forensics"><summary>forensics</summary>${fhtml}</details>`;
  }

  return `<div class="p-hist-item">${html}</div>`;
}

export const __test__ = {
  formatV0Probe,
  formatSpectral,
  withWas,
};
