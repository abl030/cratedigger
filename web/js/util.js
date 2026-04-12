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
 * Reverse-map target_format DB string to a friendly intent name.
 * @param {string|null|undefined} override
 * @returns {string}
 */
export function overrideToIntent(override) {
  if (!override) return 'default';
  if (override === 'lossless' || override === 'flac') return 'lossless';
  return 'default';
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

/**
 * Detect whether a release ID is MusicBrainz (UUID) or Discogs (numeric).
 * @param {string|null|undefined} id
 * @returns {'musicbrainz'|'discogs'|'unknown'}
 */
export function detectSource(id) {
  if (!id) return 'unknown';
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) return 'musicbrainz';
  if (/^\d+$/.test(id)) return 'discogs';
  return 'unknown';
}

/**
 * Build the external URL for a release based on its source.
 * @param {string} id
 * @returns {string}
 */
export function externalReleaseUrl(id) {
  return detectSource(id) === 'musicbrainz'
    ? `https://musicbrainz.org/release/${id}`
    : `https://www.discogs.com/release/${id}`;
}

/**
 * Short display label for an external source link.
 * @param {string} id
 * @returns {string}
 */
export function sourceLabel(id) {
  return detectSource(id) === 'musicbrainz' ? 'MusicBrainz' : 'Discogs';
}
