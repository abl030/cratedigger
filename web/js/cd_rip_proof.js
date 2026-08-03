// @ts-check

/**
 * @typedef {Object} CdRipProofPresentation
 * @property {'ctdb'|'accuraterip'} provider
 * @property {number} confidence
 * @property {string} text
 */

/** @param {unknown} value @returns {value is Record<string, unknown>} */
function isObject(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** @param {unknown} value @returns {value is number} */
function isInteger(value) {
  return Number.isSafeInteger(value);
}

/** @param {unknown} value @returns {boolean} */
function isUint32(value) {
  return isInteger(value) && value >= 0 && value <= 0xFFFFFFFF;
}

/** @param {unknown} value @returns {boolean} */
function isSha256(value) {
  return typeof value === 'string' && /^[0-9a-f]{64}$/.test(value);
}

/**
 * Validate the producer's exact TOC identity before attributing a provider.
 * @param {unknown} raw
 * @returns {number[]|null}
 */
function validTocOffsets(raw) {
  if (!isObject(raw) || !Array.isArray(raw.track_offsets_sectors)) return null;
  const offsets = raw.track_offsets_sectors;
  if (offsets.length === 0 || offsets.length > 99 || offsets[0] !== 0) return null;
  if (!offsets.every(isUint32)) return null;
  if (offsets.some((offset, index) => index > 0 && offset <= offsets[index - 1])) {
    return null;
  }
  if (!isUint32(raw.leadout_sector) || raw.leadout_sector <= offsets.at(-1)) {
    return null;
  }
  if (typeof raw.accuraterip_id !== 'string' || !raw.accuraterip_id) return null;
  if (typeof raw.musicbrainz_disc_id !== 'string' || !raw.musicbrainz_disc_id) {
    return null;
  }
  return offsets;
}

/** @param {unknown} raw @param {number[]} offsets @param {number} leadout */
function validCtdb(raw, offsets, leadout) {
  if (!isObject(raw) || raw.provider !== 'ctdb') return false;
  if (typeof raw.url !== 'string' || !raw.url.startsWith('https://')) return false;
  if (typeof raw.entry_id !== 'string' || !raw.entry_id) return false;
  if (!isInteger(raw.confidence) || raw.confidence <= 0) return false;
  if (!isUint32(raw.crc32) || raw.stride_samples !== 5880) return false;
  if (!Array.isArray(raw.response_toc_sectors)
      || raw.response_toc_sectors.length < 2
      || !raw.response_toc_sectors.every(isUint32)) return false;
  if (!isUint32(raw.response_toc_shift_sectors)) return false;
  if (!isSha256(raw.response_sha256)) return false;
  const normalized = raw.response_toc_sectors.map(
    (sector) => sector - raw.response_toc_shift_sectors,
  );
  const expected = [...offsets, leadout];
  return normalized.length === expected.length
    && normalized.every((sector, index) => sector === expected[index]);
}

/** @param {unknown} raw @param {number} trackCount */
function validAccurateRip(raw, trackCount) {
  if (!isObject(raw) || raw.provider !== 'accuraterip') return false;
  if (typeof raw.url !== 'string' || !raw.url.startsWith('https://')) return false;
  if (raw.checksum_version !== 'arv1' && raw.checksum_version !== 'arv2') {
    return false;
  }
  if (!isInteger(raw.read_offset_samples)
      || raw.read_offset_samples < -5000
      || raw.read_offset_samples > 5000) return false;
  if (!Array.isArray(raw.track_confidences)
      || raw.track_confidences.length !== trackCount
      || !raw.track_confidences.every(
        (confidence) => isInteger(confidence) && confidence > 0,
      )) return false;
  if (!Array.isArray(raw.track_checksums)
      || raw.track_checksums.length !== trackCount
      || !raw.track_checksums.every(isUint32)) return false;
  return isSha256(raw.response_sha256);
}

/**
 * Admit only complete positive evidence produced by CdRipBitVerification.
 * Missing, partial, and malformed data have no presentation: absence is not
 * a failed verification and must never grow a negative label.
 *
 * @param {unknown} raw
 * @returns {CdRipProofPresentation|null}
 */
export function cdRipProofPresentation(raw) {
  if (!isObject(raw) || raw.algorithm !== 'cd-rip-bit-verifier-v1') return null;
  if (raw.provenance !== 'measured' && raw.provenance !== 'carried') return null;
  if (raw.source_format !== 'flac' && raw.source_format !== 'alac') return null;
  const offsets = validTocOffsets(raw.toc);
  if (!offsets || !isObject(raw.toc) || !isInteger(raw.toc.leadout_sector)) {
    return null;
  }
  const hasCtdb = raw.ctdb !== null && raw.ctdb !== undefined;
  const hasAccurateRip = raw.accuraterip !== null && raw.accuraterip !== undefined;
  if (!hasCtdb && !hasAccurateRip) return null;
  if (hasCtdb && !validCtdb(raw.ctdb, offsets, raw.toc.leadout_sector)) return null;
  if (hasAccurateRip && !validAccurateRip(raw.accuraterip, offsets.length)) return null;

  if (hasCtdb && isObject(raw.ctdb) && isInteger(raw.ctdb.confidence)) {
    return {
      provider: 'ctdb',
      confidence: raw.ctdb.confidence,
      text: `CD bit-verified · CTDB confidence ${raw.ctdb.confidence}`,
    };
  }
  if (hasAccurateRip && isObject(raw.accuraterip)
      && Array.isArray(raw.accuraterip.track_confidences)) {
    const confidence = Math.min(...raw.accuraterip.track_confidences);
    return {
      provider: 'accuraterip',
      confidence,
      text: `CD bit-verified · AccurateRip min confidence ${confidence}`,
    };
  }
  return null;
}
