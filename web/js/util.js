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
 * Normalize the single release-id field the frontend keys on.
 * @param {string|null|undefined} id
 * @returns {string}
 */
export function normalizeReleaseId(id) {
  if (!id) return '';
  const value = String(id).trim();
  if (!value) return '';
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) {
    return value.toLowerCase();
  }
  if (/^\d+$/.test(value)) {
    const numeric = Number(value);
    return Number.isFinite(numeric) && numeric > 0 ? String(numeric) : '';
  }
  return value;
}

/**
 * Detect whether a release ID is MusicBrainz (UUID) or Discogs (numeric).
 * @param {string|null|undefined} id
 * @returns {'musicbrainz'|'discogs'|'unknown'}
 */
export function detectSource(id) {
  const normalized = normalizeReleaseId(id);
  if (!normalized) return 'unknown';
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(normalized)) return 'musicbrainz';
  if (/^\d+$/.test(normalized)) return 'discogs';
  return 'unknown';
}

/**
 * Build the external URL for a release based on its source.
 * @param {string} id
 * @returns {string}
 */
export function externalReleaseUrl(id) {
  const normalized = normalizeReleaseId(id);
  const source = detectSource(normalized);
  if (source === 'musicbrainz') return `https://musicbrainz.org/release/${normalized}`;
  if (source === 'discogs') return `https://www.discogs.com/release/${normalized}`;
  return '';
}

/**
 * Short display label for an external source link.
 * @param {string} id
 * @returns {string}
 */
export function sourceLabel(id) {
  const source = detectSource(id);
  if (source === 'musicbrainz') return 'MusicBrainz';
  if (source === 'discogs') return 'Discogs';
  return '';
}

/**
 * Map a `manual_reason` enum value to a short, human-friendly label for the
 * inline chip on the request-detail view. Returns the empty string for
 * NULL / unknown reasons so the caller can branch on truthiness.
 * @param {string|null|undefined} reason
 * @returns {string}
 */
export function manualReasonLabel(reason) {
  if (!reason) return '';
  if (reason === 'search_exhausted') return 'search exhausted';
  return reason;
}

/**
 * @typedef {Object} CandidateScore
 * @property {string} username
 * @property {string} dir
 * @property {string} filetype
 * @property {number} matched_tracks
 * @property {number} total_tracks
 * @property {number} avg_ratio
 * @property {string[]} missing_titles
 * @property {number} file_count
 */

/**
 * @typedef {Object} LastSearchPayload
 * @property {string|null} variant
 * @property {string|null} final_state
 * @property {string|null} outcome
 * @property {CandidateScore[]} top_candidates
 */

/**
 * Render the "search forensics" block for the request-detail view.
 *
 * UX: collapsed-by-default summary that shows the variant tag + final_state
 * tag and a top-3 candidates table (`username · dir · matched/total ·
 * avg_ratio · filetype`). When `last` is null (no search rows yet), returns
 * a small "no forensic data yet" line. Pure HTML producer — testable
 * without a DOM.
 *
 * @param {LastSearchPayload|null|undefined} last
 * @returns {string} HTML string
 */
export function renderForensicBlock(last) {
  if (!last) {
    return `<div class="p-forensic"><div class="p-forensic-summary">No search forensic data yet</div></div>`;
  }
  const variant = last.variant || '?';
  const finalState = last.final_state || '?';
  const outcome = last.outcome || '?';
  const cands = Array.isArray(last.top_candidates) ? last.top_candidates : [];
  const summary = `Last search: ${esc(variant)} → ${esc(outcome)} <span style="color:#666;">(${esc(finalState)}, top ${cands.length})</span>`;
  let body = `<div class="p-forensic-meta">variant: ${esc(variant)} · final_state: ${esc(finalState)} · outcome: ${esc(outcome)}</div>`;
  if (cands.length === 0) {
    body += `<div style="color:#666;">No candidates captured for this search.</div>`;
  } else {
    body += '<table class="p-forensic-table"><thead><tr>'
      + '<th>user</th><th>dir</th><th>match</th><th>avg</th><th>type</th>'
      + '</tr></thead><tbody>'
      + cands.map((c) => {
        const ratio = (typeof c.avg_ratio === 'number') ? c.avg_ratio.toFixed(2) : '?';
        return `<tr>
          <td>${esc(c.username || '?')}</td>
          <td style="color:#777;">${esc(c.dir || '?')}</td>
          <td>${c.matched_tracks ?? '?'}/${c.total_tracks ?? '?'}</td>
          <td>${ratio}</td>
          <td>${esc(c.filetype || '?')}</td>
        </tr>`;
      }).join('')
      + '</tbody></table>';
  }
  return `<div class="p-forensic" id="p-forensic-block">
    <div class="p-forensic-summary" onclick="event.stopPropagation(); this.parentElement.classList.toggle('open');">${summary}</div>
    <div class="p-forensic-body">${body}</div>
  </div>`;
}
