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
