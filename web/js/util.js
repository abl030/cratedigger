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
 * Compact one-letter format code + bitrate tier, suitable for inline
 * badge use. Examples: "M V2", "M 320", "F", "O 128", "AL".
 *
 * Format code map:
 *   MP3 → M, FLAC → F, ALAC → AL, WAV → W, OPUS → O, AAC → A, OGG → OG
 *   Anything else → upper-cased name as-is.
 *
 * Bitrate tiers match qualityLabel(): >=295 → 320, >=220 → V0,
 * >=170 → V2, else raw kbps. Lossless formats (FLAC/ALAC/WAV) skip
 * the bitrate suffix.
 *
 * @param {string|null|undefined} formats
 * @param {number|null|undefined} kbps
 * @returns {string}
 */
export function qualityLabelShort(formats, kbps) {
  if (!formats) return '?';
  const raw = formats.split(',')[0].trim().toUpperCase();
  const codeMap = {
    MP3: 'M', FLAC: 'F', ALAC: 'AL', WAV: 'W',
    OPUS: 'O', AAC: 'A', OGG: 'OG',
  };
  const fmt = codeMap[raw] || raw;
  if (raw === 'FLAC' || raw === 'ALAC' || raw === 'WAV') return fmt;
  if (!kbps || kbps <= 0) return fmt;
  // V0/V2 are MP3-specific tier names; for AAC/Opus/OGG show raw kbps.
  if (raw === 'MP3') {
    if (kbps >= 295) return fmt + ' 320';
    if (kbps >= 220) return fmt + ' V0';
    if (kbps >= 170) return fmt + ' V2';
  }
  return fmt + ' ' + kbps;
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
 * Encode a JS string literal for embedding inside a double-quoted HTML attribute.
 * Returns HTML-escaped JSON, e.g. `&quot;Kid A&quot;`.
 * @param {string|null|undefined} value
 * @returns {string}
 */
export function jsArg(value) {
  return esc(JSON.stringify(String(value ?? '')));
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
