// @ts-check

/**
 * Shared visual ordering for codec-quality ranks and spectral grades.
 *
 * CSS owns the actual foreground/background values in `web/index.html`.
 * Renderers consume only the semantic classes returned here, so the same
 * rank or spectral grade cannot silently acquire a different colour in a
 * different view.
 */

export const QUALITY_RANK_ORDER = Object.freeze([
  'unknown',
  'poor',
  'acceptable',
  'good',
  'excellent',
  'transparent',
  'lossless',
]);

export const SPECTRAL_GRADE_ORDER = Object.freeze([
  'likely_transcode',
  'suspect',
  'marginal',
  'genuine',
]);

/**
 * Four spectral grades reuse four stops from the larger quality-rank scale:
 * red (worst), orange, yellow, and bright green (best).
 */
export const SPECTRAL_GRADE_TONES = Object.freeze({
  likely_transcode: 'poor',
  suspect: 'acceptable',
  marginal: 'good',
  genuine: 'lossless',
});

const QUALITY_RANKS = new Set(QUALITY_RANK_ORDER);

/** @param {unknown} value */
function token(value) {
  return String(value ?? '').trim().toLowerCase();
}

/** @param {unknown} rank */
export function qualityRankTone(rank) {
  const normalized = token(rank);
  return QUALITY_RANKS.has(normalized) ? normalized : 'unknown';
}

/** @param {unknown} rank */
export function qualityToneClass(rank) {
  return `quality-tone-${qualityRankTone(rank)}`;
}

/** @param {unknown} rank */
export function qualityRankBadgeClass(rank) {
  return `badge-rank-${qualityRankTone(rank)}`;
}

/** @param {unknown} grade */
export function spectralGradeTone(grade) {
  const normalized = token(grade);
  return Object.hasOwn(SPECTRAL_GRADE_TONES, normalized)
    ? SPECTRAL_GRADE_TONES[normalized]
    : 'unknown';
}

/** @param {unknown} grade */
export function spectralGradeClass(grade) {
  return `spectral-grade ${qualityToneClass(spectralGradeTone(grade))}`;
}

/** @param {unknown} grade */
export function spectralGradeBadgeClass(grade) {
  return `badge spectral-grade ${qualityRankBadgeClass(spectralGradeTone(grade))}`;
}

/** @param {unknown} grade */
export function spectralGradeLabel(grade) {
  return token(grade).replace(/_/g, ' ');
}

/**
 * The two reason tokens the server sends beside a withheld accusation
 * (`web/classify.py::ACCUSATION_WITHHELD_*`). They are DIFFERENT facts and
 * only one of them may be described: an audit-only family's cliff IS that
 * encoder's native rolloff, whereas an unresolved codec's cliff supports
 * no statement about any encoder, because none was identified.
 */
export const ACCUSATION_WITHHELD_AUDIT_ONLY_CODEC = 'audit_only_codec';
export const ACCUSATION_WITHHELD_CODEC_UNRESOLVED = 'codec_unresolved';

/**
 * Whether a measured spectral grade may be RENDERED as a transcode
 * accusation for the codec that produced it (issue #829 Phase 5 PR4).
 *
 * The server answers this in `spectral_accusation_admissible`, derived
 * from the same codec-aware interpretation the decider uses: it is false
 * for AAC, Opus, HE-AAC and unresolved families, whose natural rolloff the
 * codec-blind analyzer grades `suspect`/`likely_transcode` anyway (issue
 * #829's opening defect — download 37946, a 256 kbps CBR AAC graded
 * `likely_transcode` with a LAME-table 128 bucket). The grade stays
 * visible as the measured fact it is; only the accusing colour and
 * wording are withheld. `undefined`/`null` — a row with no evidence join —
 * keeps the historical accusing rendering on every surface.
 * @param {unknown} grade
 * @param {boolean|null|undefined} admissible
 * @returns {boolean}
 */
export function spectralGradeIsAdmissible(grade, admissible) {
  if (admissible !== false) return true;
  return grade !== 'suspect' && grade !== 'likely_transcode';
}

/**
 * The neutral presentation a withheld grade renders with, shared by every
 * surface that paints a spectral grade so they cannot state different
 * facts about the same measurement.
 *
 * `suffix` and `title` are static copy (no interpolation, so nothing here
 * needs escaping); callers still escape the grade label they compose it
 * with. The `codec_unresolved` branch deliberately claims NOTHING about
 * any encoder — asserting native rolloff over a codec nothing resolved
 * would fabricate the fact the flag exists to withhold.
 * @param {unknown} withheld - the server's reason token, or absent
 * @returns {{className: string, badgeClass: string, suffix: string, title: string}}
 */
export function spectralWithheldPresentation(withheld) {
  const className = qualityToneClass('unknown');
  const badgeClass = `badge spectral-grade ${qualityRankBadgeClass('unknown')}`;
  if (withheld === ACCUSATION_WITHHELD_CODEC_UNRESOLVED) {
    return {
      className,
      badgeClass,
      suffix: ' · codec unresolved',
      title: 'The codec that produced this measurement could not be '
        + 'identified, so its cliff supports no finding either way. The '
        + 'grade is kept as the measured fact.',
    };
  }
  return {
    className,
    badgeClass,
    suffix: ' · audit-only',
    title: "This codec's spectral rolloff is native encoder behaviour, not "
      + 'evidence of a transcode. The grade is kept as the measured fact.',
  };
}
