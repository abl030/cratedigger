/** Producer-shaped serializations of lib.quality CdRipBitVerification. */

function validToc(trackOffsets = [0]) {
  return {
    track_offsets_sectors: trackOffsets,
    leadout_sector: trackOffsets.at(-1) + 470,
    accuraterip_id: '000001d6-000003ac-02000601',
    musicbrainz_disc_id: 'exact-disc-id',
  };
}

export function validCtdbProof(confidence = 24) {
  return {
    algorithm: 'cd-rip-bit-verifier-v1',
    provenance: 'measured',
    source_format: 'flac',
    toc: validToc(),
    accuraterip: null,
    ctdb: {
      provider: 'ctdb',
      url: 'https://db.cue.tools/lookup2.php',
      entry_id: `ctdb-entry-${confidence}`,
      confidence,
      crc32: 0x12345678,
      stride_samples: 5880,
      response_toc_sectors: [0, 470],
      response_toc_shift_sectors: 0,
      response_sha256: 'a'.repeat(64),
    },
  };
}

export function validAccurateRipProof() {
  const offsets = Array.from({ length: 12 }, (_, index) => index * 200);
  return {
    algorithm: 'cd-rip-bit-verifier-v1',
    provenance: 'carried',
    source_format: 'alac',
    toc: validToc(offsets),
    accuraterip: {
      provider: 'accuraterip',
      url: 'https://www.accuraterip.com/example.bin',
      checksum_version: 'arv2',
      read_offset_samples: 664,
      track_confidences: [5, 4, 3, 2, 1, 6, 7, 8, 4, 3, 2, 5],
      track_checksums: offsets.map((_, index) => 0x11111111 + index),
      response_sha256: 'b'.repeat(64),
    },
    ctdb: null,
  };
}
