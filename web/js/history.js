// @ts-check
import { awstDateTime, esc } from './util.js';

/**
 * Render a single V0 probe value with its provenance suffix.
 *
 * V0 probes run on EVERY candidate and are load-bearing operator data
 * (Wrong Matches has surfaced them regardless of lineage all along):
 * - ``lossless_source_v0`` is the gold standard — render bare ("260kbps
 *   avg"), the source proof itself is the message.
 * - ``native_lossy_research_v0`` is a real ffmpeg V0-transcode probe of a
 *   lossy source — render "(from lossy)" so it never reads as
 *   lossless-source proof.
 * - Anything else falls back to showing the raw kind so debug rows are
 *   still legible.
 * @param {number|string} avg
 * @param {string|undefined} kind
 * @param {number|string|undefined} min
 * @returns {string}
 */
function formatV0Probe(avg, kind, min = undefined) {
  const floor = min !== null && min !== undefined ? ` · min ${esc(min)}kbps` : '';
  const base = `${esc(avg)}kbps avg${floor}`;
  if (!kind || kind === 'lossless_source_v0') {
    return base;
  }
  if (kind === 'native_lossy_research_v0') {
    return `${base} (from lossy)`;
  }
  if (kind === 'on_disk_research_v0') {
    return `${base} (on-disk re-encode)`;
  }
  return `${base} (${esc(kind)})`;
}

/**
 * Compact strip form of a V0 probe: "V0 255k avg (min 224k)". The compact
 * comparison stops at the minimum; probe-kind provenance remains available
 * in the expanded V0 probe row via formatV0Probe.
 * @param {number|string} avg
 * @param {number|string|undefined} min
 * @returns {string}
 */
function stripV0Phrase(avg, min = undefined) {
  const floor = min !== null && min !== undefined ? ` (min ${esc(min)}k)` : '';
  return `V0 ${esc(avg)}k avg${floor}`;
}

/**
 * Render a "Spectral" cell value with grade-aware coloring and floor bitrate.
 *
 * Every "spectral_grade" / "spectral_bitrate" pair routes through here so
 * the rendering rules (grade colors, ~ prefix for floor) live in one place.
 * @param {string} grade
 * @param {number|string|undefined} bitrate
 */
function spectralGradeLabel(grade) {
  return esc(String(grade || '').replace(/_/g, ' '));
}

function formatSpectral(grade, bitrate) {
  const sgColor = grade === 'genuine' ? '#6d6' : grade === 'suspect' ? '#d66' : '#aa8';
  // Show the spectral floor whenever it's present — even when the album's
  // rollup grade is `genuine`, a non-null spectral_bitrate means at least
  // one track triggered a cliff and the min-across-tracks is this value
  // (Eno case, download_log 3291).
  const label = bitrate
    ? `${spectralGradeLabel(grade)} (~${esc(bitrate)}kbps)`
    : spectralGradeLabel(grade);
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
  if (metric === 'contract') return 'contract';
  if (value === null || value === undefined) return 'unmeasured';
  if (basis.spectral_clamped && basis.branch === 'rank') return `~${esc(value)}k`;
  return `${esc(metric)} ${esc(value)}k`;
}

/**
 * Measured post-import bytes, kept disjoint from decision-time candidate/V0
 * evidence. Prefer the configured AVG statistic for VBR output and retain the
 * minimum as an explicitly-labelled floor.
 * @param {Object} h
 * @param {boolean} detail
 * @returns {string}
 */
function materializedOutputPhrase(h, detail = false) {
  const fmt = h.materialized_format || h.actual_filetype;
  const avg = h.materialized_avg_bitrate;
  const median = h.materialized_median_bitrate;
  const min = h.materialized_min_bitrate;
  if (!fmt || (avg == null && median == null && min == null)) return '';
  const primaryMetric = avg != null ? 'avg' : median != null ? 'median' : 'min';
  const primaryValue = avg != null ? avg : median != null ? median : min;
  const prefix = detail ? '' : 'actual ';
  let phrase = `${prefix}${esc(String(fmt).toUpperCase())} ${primaryMetric} ${esc(primaryValue)}`;
  phrase += detail ? 'kbps' : 'k';
  if (min != null && primaryMetric !== 'min') {
    phrase += detail ? ` · min ${esc(min)}kbps` : ` (min ${esc(min)}k)`;
  }
  return phrase;
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

function emptyEvidenceCells() {
  return { source: '', metric: '', spectral: '', v0: '' };
}

function avgMinPhrase(avg, median, min) {
  if (avg != null) {
    return `${esc(avg)}k avg${min != null ? ` (min ${esc(min)}k)` : ''}`;
  }
  if (median != null) {
    return `${esc(median)}k median${min != null ? ` (min ${esc(min)}k)` : ''}`;
  }
  return min != null ? `min ${esc(min)}k` : '';
}

function comparisonMetricPhrase(avg, median, min, basis, side) {
  if (avg != null || median != null) return avgMinPhrase(avg, median, min);
  if (!basis) return avgMinPhrase(null, null, min);

  const metric = side === 'new' ? basis.new_metric : basis.existing_metric;
  const value = side === 'new' ? basis.new_value_kbps : basis.existing_value_kbps;
  if (basis.spectral_clamped && basis.branch === 'rank') {
    return basisValuePhrase(basis, side);
  }
  if (metric === 'avg') return avgMinPhrase(value, null, min);
  if (metric === 'median') return avgMinPhrase(null, value, min);
  if (min != null) return avgMinPhrase(null, null, min);
  return basisValuePhrase(basis, side);
}

function isLosslessSource(format) {
  return ['FLAC', 'ALAC', 'WAV', 'AIFF'].includes(
    String(format || '').trim().toUpperCase(),
  );
}

function storageFormatLabel(h, fallback) {
  const contract = String(h.target_contract_format || '').toLowerCase();
  if (/\bv0\b/.test(contract)) return 'V0';
  if (contract.includes('opus')) return 'Opus';
  if (contract.includes('flac')) return 'FLAC';
  const format = h.materialized_format || fallback || '';
  const normalized = String(format).trim();
  /** @type {Record<string, string>} */
  const displayLabels = {
    OPUS: 'Opus',
    VORBIS: 'Vorbis',
    WMA: 'WMA',
  };
  const codec = normalized.toUpperCase();
  return displayLabels[codec] || codec;
}

function sourceStorageLabel(sourceFormat, storageFormat) {
  const source = String(sourceFormat || '').trim().toUpperCase();
  const storage = String(storageFormat || '').trim();
  if (!storage || source === storage.toUpperCase()) return source;
  return `${source} - ${storage}`;
}

/**
 * Normalize every Recents outcome into the same two-sided card model.
 *
 * IN always means the downloaded source; a lossless source suffixes the
 * selected storage codec in its source cell so its metric stays vertically
 * aligned with HAVE. HAVE always means the on-disk snapshot from before this
 * attempt. Materialized output is candidate evidence and therefore belongs
 * only to IN; a first import has an empty HAVE row.
 */
function buildEvidenceCardModel(h) {
  const basis = h.comparison_basis || null;
  const inCells = emptyEvidenceCells();
  const failedDownload = h.outcome === 'timeout';
  const sourceFormat = h.source_format || h.slskd_filetype || h.original_filetype;
  const losslessSource = isLosslessSource(sourceFormat);
  const hasMaterializedMeasurement = Boolean(
    h.materialized_format
    && (h.materialized_avg_bitrate != null
      || h.materialized_median_bitrate != null
      || h.materialized_min_bitrate != null),
  );
  if (sourceFormat) {
    inCells.source = esc(String(sourceFormat).toUpperCase());
  } else if (basis?.new_format) {
    inCells.source = esc(String(basis.new_format).toUpperCase());
  } else if (h.downloaded_label) {
    inCells.source = esc(h.downloaded_label);
  }
  if (losslessSource && hasMaterializedMeasurement) {
    const storage = storageFormatLabel(h, h.materialized_format);
    inCells.source = esc(sourceStorageLabel(sourceFormat, storage));
    const bytes = avgMinPhrase(
      h.materialized_avg_bitrate,
      h.materialized_median_bitrate,
      h.materialized_min_bitrate,
    );
    inCells.metric = bytes;
  } else {
    const projectedMinIsV0Probe = (
      h.source_min_bitrate == null
      && h.legacy_projection_version != null
      && (h.v0_probe_kind === 'lossless_source_v0'
        || h.v0_probe_kind === 'lossless_source')
      && h.v0_probe_min_bitrate != null
      && Number(h.actual_min_bitrate) === Number(h.v0_probe_min_bitrate)
    );
    const sourceMin = projectedMinIsV0Probe
      ? null : (h.source_min_bitrate ?? h.actual_min_bitrate);
    const bytes = comparisonMetricPhrase(
      h.source_avg_bitrate,
      h.source_median_bitrate,
      sourceMin,
      basis,
      'new',
    );
    if (losslessSource && bytes) {
      inCells.metric = bytes;
    } else if (bytes) {
      inCells.metric = bytes;
    }
  }
  if (h.spectral_grade) {
    // With a basis, the clamped rank value already carries the floor —
    // repeating "~250k" in the grade chip would double it up.
    const basisAlreadyHasFloor = Boolean(
      basis && basis.spectral_clamped && basis.branch === 'rank'
      && Number(basis.new_value_kbps) === Number(h.spectral_bitrate),
    );
    const floor = (h.spectral_bitrate && !basisAlreadyHasFloor)
      ? `~${esc(h.spectral_bitrate)}k ` : '';
    const sgColor = h.spectral_grade === 'genuine' ? '#6d6'
      : h.spectral_grade === 'suspect' ? '#d66' : '#aa8';
    inCells.spectral = `<span style="color:${sgColor};">${floor}${spectralGradeLabel(h.spectral_grade)}</span>`;
  } else if (h.spectral_attempted && h.spectral_error) {
    inCells.spectral = `<span style="color:#d66;" title="${esc(h.spectral_error)}">spectral failed</span>`;
  }
  // V0 on every candidate: keep the compact comparison numeric and bounded.
  // Probe-kind provenance remains in the expanded V0 probe row.
  if (h.v0_probe_avg_bitrate) {
    inCells.v0 = stripV0Phrase(h.v0_probe_avg_bitrate, h.v0_probe_min_bitrate);
  }
  if (failedDownload) {
    Object.assign(inCells, emptyEvidenceCells(), { source: '—' });
  }

  const haveCells = emptyEvidenceCells();
  if (basis) {
    haveCells.source = esc(String(basis.existing_format || '?').toUpperCase());
    haveCells.metric = comparisonMetricPhrase(
      h.existing_avg_bitrate,
      h.existing_median_bitrate,
      h.existing_min_bitrate,
      basis,
      'existing',
    );
  } else if (h.existing_format) {
    haveCells.source = esc(h.existing_format);
    haveCells.metric = avgMinPhrase(
      h.existing_avg_bitrate,
      h.existing_median_bitrate,
      h.existing_min_bitrate,
    );
  } else {
    haveCells.metric = avgMinPhrase(
      h.existing_avg_bitrate,
      h.existing_median_bitrate,
      h.existing_min_bitrate,
    );
  }
  if (h.existing_spectral_grade) {
    const floor = h.existing_spectral_bitrate ? `~${esc(h.existing_spectral_bitrate)}k ` : '';
    const color = h.existing_spectral_grade === 'genuine' ? '#6d6'
      : h.existing_spectral_grade === 'suspect' ? '#d66' : '#aa8';
    haveCells.spectral = `<span style="color:${color};">${floor}${spectralGradeLabel(h.existing_spectral_grade)}</span>`;
  } else if (h.existing_spectral_attempted && h.existing_spectral_error) {
    haveCells.spectral = `<span style="color:#d66;" title="${esc(h.existing_spectral_error)}">spectral failed</span>`;
  } else if (h.existing_spectral_bitrate) {
    // Historical attempts measured only the existing floor, not its grade.
    haveCells.spectral = `ungraded (~${esc(h.existing_spectral_bitrate)}k)`;
  }
  if (h.existing_v0_probe_avg_bitrate) {
    haveCells.v0 = stripV0Phrase(
      h.existing_v0_probe_avg_bitrate,
      h.existing_v0_probe_min_bitrate);
  }
  if (!Object.values(haveCells).some(Boolean)) {
    haveCells.source = '—';
  }

  return { inCells, haveCells };
}

/**
 * Mobile wording: each number carries its own label in place ("725k
 * avg/455k min") instead of a detached trailing legend. Equal pairs (CBR
 * albums) collapse to one number, the V0 cell stays the bare pair (the V0
 * prefix is its label), and a converted source reads "FLAC→Opus". Full
 * wording stays a tap away in the detail panel.
 */
function compactEvidenceValue(kind, value) {
  const labelled = (label) => (_, a, b) =>
    (a === b ? `${a}k` : `${a}k ${label}/${b}k min`);
  let compact = value
    .replace(/(\d+)k avg \(min (\d+)k\)/g, labelled('avg'))
    .replace(/(\d+)k median \(min (\d+)k\)/g, labelled('med'));
  if (kind === 'v0') {
    compact = compact.replace(/V0 (\d+)k \w+\/(\d+)k min/g, 'V0 $1/$2k');
  }
  if (kind === 'source') {
    compact = compact.replace(' - ', '→');
  }
  return compact;
}

function renderEvidenceCell(kind, value) {
  return `<span class="r-ev-cell r-ev-${kind}">`
    + `<span class="r-ev-full">${value}</span>`
    + `<span class="r-ev-compact">${compactEvidenceValue(kind, value)}</span>`
    + `</span>`;
}

function renderEvidenceRow(side, label, cells) {
  return `<span class="r-ev-row r-ev-${side}">`
    + `<strong class="r-ev-tag">${label}</strong>`
    + renderEvidenceCell('source', cells.source)
    + renderEvidenceCell('metric', cells.metric)
    + renderEvidenceCell('spectral', cells.spectral)
    + renderEvidenceCell('v0', cells.v0)
    + `</span>`;
}

/**
 * Compact one-line IN/HAVE evidence comparison for every list-row outcome.
 * @param {Object} h - Download history entry / recents log item from the API
 * @returns {string} HTML string, or '' when there is no evidence
 */
export function renderEvidenceStrip(h) {
  // A strip is a comparison — it needs at least one NUMBER. A codec
  // label alone (failed downloads carry a filetype but no measurements)
  // would render a noisy "IN MP3 HAVE —" on every failure row.
  const hasMeasurement = Boolean(
    h.actual_min_bitrate || h.spectral_bitrate || h.spectral_grade
    || h.spectral_error || h.existing_spectral_error
    || h.source_min_bitrate || h.source_avg_bitrate
    || h.source_median_bitrate
    || h.v0_probe_avg_bitrate || h.existing_min_bitrate
    || h.existing_spectral_bitrate || h.existing_v0_probe_avg_bitrate
    || h.materialized_min_bitrate || h.materialized_avg_bitrate
    || h.materialized_median_bitrate
    || h.comparison_basis,
  );
  if (!hasMeasurement) return '';

  const { inCells, haveCells } = buildEvidenceCardModel(h);

  const hasCells = (cells) => Object.values(cells).some(Boolean);
  if (!hasCells(inCells) && !hasCells(haveCells)) return '';
  return `<span class="r-evidence">`
    + renderEvidenceRow('in', 'IN', inCells)
    + renderEvidenceRow('have', 'HAVE', haveCells)
    + `</span>`;
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
 * Output / Distance — renders on EVERY entry, with an em-dash when a
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
  const FAILURE_OUTCOMES = [
    'rejected', 'failed', 'timeout', 'user_offline', 'curator_ban',
    'have_analysis_error',
  ];
  const verdict = h.verdict || h.beets_scenario || '';
  if (verdict) {
    const rejectCls = FAILURE_OUTCOMES.includes(outcome) ? ' p-hist-verdict-reject' : '';
    html += `<div class="p-hist-verdict${rejectCls}">${esc(verdict)}</div>`;
  }

  const rows = [];

  if (outcome === 'have_analysis_error') {
    const category = h.failure_category
      ? String(h.failure_category).replace(/_/g, ' ')
      : 'unknown analyser failure';
    rows.push(['Failure category', esc(category)]);
    rows.push(['Installed HAVE', h.installed_path ? esc(h.installed_path) : '—']);
    rows.push(['Candidate', h.candidate_reference ? esc(h.candidate_reference) : '—']);
    rows.push(['Analysis error', h.analysis_error ? esc(h.analysis_error) : '—']);
  }

  if (h.source_format) {
    const sourceMetric = h.source_avg_bitrate != null
      ? `avg ${esc(h.source_avg_bitrate)}kbps`
      : h.source_median_bitrate != null
        ? `median ${esc(h.source_median_bitrate)}kbps`
        : h.source_min_bitrate != null
          ? `min ${esc(h.source_min_bitrate)}kbps` : '';
    const sourceFloor = h.source_min_bitrate != null
      && h.source_avg_bitrate != null
      ? ` · min ${esc(h.source_min_bitrate)}kbps` : '';
    rows.push([
      'Source',
      `${esc(String(h.source_format).toUpperCase())}`
        + `${sourceMetric ? ` ${sourceMetric}` : ''}${sourceFloor}`,
    ]);
  } else {
    rows.push(['Source', h.downloaded_label ? esc(h.downloaded_label) : '—']);
  }

  if (h.spectral_grade || h.existing_spectral_grade || h.existing_spectral_bitrate
      || h.spectral_error || h.existing_spectral_error) {
    const candidate = h.spectral_error
      ? `<span style="color:#d66;" title="${esc(h.spectral_error)}">analysis failed</span>`
      : h.spectral_grade
      ? formatSpectral(h.spectral_grade, h.spectral_bitrate)
      : 'unmeasured';
    const existing = h.existing_spectral_error
      ? `<span style="color:#d66;" title="${esc(h.existing_spectral_error)}">analysis failed</span>`
      : h.existing_spectral_grade
      ? formatSpectral(h.existing_spectral_grade, h.existing_spectral_bitrate)
      : h.existing_spectral_bitrate
        ? `<span style="color:#aa8;">ungraded (~${esc(h.existing_spectral_bitrate)}kbps)</span>`
        : 'unmeasured';
    rows.push(['Spectral', `<span class="r-ev-tag">IN</span> ${candidate} `
      + `<span class="r-ev-tag">HAVE</span> ${existing}`]);
  } else {
    rows.push(['Spectral', '—']);
  }

  // V0 probe row: render whichever side exists and label both sides
  // explicitly. Probe-kind provenance belongs here (not in the compact
  // strip), and a HAVE-only historical row must remain visible. We never
  // substitute either side's raw container minimum for a missing probe.
  if (h.v0_probe_avg_bitrate || h.existing_v0_probe_avg_bitrate) {
    const candidate = h.v0_probe_avg_bitrate
      ? formatV0Probe(
        h.v0_probe_avg_bitrate, h.v0_probe_kind, h.v0_probe_min_bitrate)
      : '—';
    const existing = h.existing_v0_probe_avg_bitrate
      ? formatV0Probe(
        h.existing_v0_probe_avg_bitrate,
        h.existing_v0_probe_kind,
        h.existing_v0_probe_min_bitrate)
      : '—';
    rows.push(['V0 probe', `<span class="r-ev-tag">IN</span> ${candidate} `
      + `<span class="r-ev-tag">HAVE</span> ${existing}`]);
  }

  // Compared row — the persisted comparison basis, rendered verbatim.
  // This is the decision's own story (metric, values, ranks); the Bitrate
  // row below stays as the raw min-vs-min detail.
  if (h.comparison_basis) {
    const b = h.comparison_basis;
    let compared = `${basisSidePhrase(b, 'new')}`
      + ` vs ${basisSidePhrase(b, 'existing')}`;
    if (b.verified_lossless_bypass) {
      compared += ' <span style="color:#6af;">· verified lossless bypass</span>';
    }
    rows.push(['Compared', compared]);
  }

  // Output is measured only after target conversion/import. Do not fall back
  // to decision-time ``actual_min_bitrate`` here: historical evidence-action
  // rows used that column for a temporary V0 proxy (Gas / November 89), which
  // made 191k of MP3 proof appear as an Opus minimum.
  rows.push(['Output', materializedOutputPhrase(h, true) || '—']);

  const targetContract = h.target_contract_format || h.final_format;
  if (targetContract) {
    const finalFormat = String(targetContract);
    const explicitContract = h.comparison_basis?.new_metric === 'contract'
      || /(?:\bv\d+\b|\b\d+\b)/i.test(finalFormat);
    rows.push([
      h.target_contract_format ? 'Target contract' : 'Stored as',
      `${esc(finalFormat.toUpperCase())}${explicitContract ? ' contract' : ''}`,
    ]);
  }

  if (outcome === 'force_import') {
    const original = h.original_beets_distance != null
      ? ` <span class="p-hist-was">(was ${parseFloat(h.original_beets_distance).toFixed(3)})</span>`
      : '';
    rows.push(['Distance', `<span style="color:#6af;">overridden</span>${original}`]);
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
  if (h.spectral_error) {
    forensicRows.push(['Spectral IN error', esc(h.spectral_error)]);
  }
  if (h.existing_spectral_error) {
    forensicRows.push(['Spectral HAVE error', esc(h.existing_spectral_error)]);
  }
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
  storageFormatLabel,
  withWas,
};
