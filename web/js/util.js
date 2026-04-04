// @ts-check

/**
 * Pure utility functions — no DOM, no fetch, no side effects.
 * Testable via Node: `node tests/test_js_util.mjs`
 */

/**
 * Format bitrate into a quality label like "MP3 V0" or "FLAC".
 * @param {string|null|undefined} formats - Comma-separated format string (e.g. "MP3" or "MP3,FLAC")
 * @param {number|null|undefined} kbps - Bitrate in kilobits per second
 * @returns {string}
 */
export function qualityLabel(formats, kbps) {
  if (!formats) return '?';
  const fmt = formats.split(',')[0].trim().toUpperCase();
  if (fmt === 'FLAC' || fmt === 'ALAC') return fmt;
  if (!kbps || kbps <= 0) return fmt;
  if (kbps >= 295) return fmt + ' 320';
  if (kbps >= 220) return fmt + ' V0';
  if (kbps >= 170) return fmt + ' V2';
  return fmt + ' ' + kbps + 'k';
}

/**
 * Convert a UTC ISO string to AWST (UTC+8) ISO-like string.
 * @param {string} isoStr - UTC ISO date string
 * @returns {string} AWST datetime as "YYYY-MM-DDTHH:MM:SS"
 */
export function toAWST(isoStr) {
  const d = new Date(isoStr);
  const awst = new Date(d.getTime() + 8 * 3600000);
  return awst.toISOString().slice(0, 19);
}

/** @param {string} isoStr @returns {string} */
export function awstDate(isoStr) { return toAWST(isoStr).slice(0, 10); }

/** @param {string} isoStr @returns {string} */
export function awstTime(isoStr) { return toAWST(isoStr).slice(11, 16); }

/** @param {string} isoStr @returns {string} */
export function awstDateTime(isoStr) { return toAWST(isoStr).slice(0, 16).replace('T', ' '); }

/**
 * Reverse-map quality_override DB string to QualityIntent enum value.
 * @param {string|null|undefined} override
 * @returns {string}
 */
export function overrideToIntent(override) {
  if (!override) return 'best_effort';
  if (override === 'flac') return 'flac_only';
  if (override === 'flac_preferred') return 'flac_preferred';
  return 'upgrade';  // CSV like "flac,mp3 v0,mp3 320"
}

/**
 * HTML-escape a string. Works in both browser and Node.
 * @param {string|null|undefined} s
 * @returns {string}
 */
export function esc(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/\\/g, '&#92;');
}
