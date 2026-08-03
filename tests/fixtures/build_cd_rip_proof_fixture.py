"""Build the JavaScript CD-proof fixture from production wire structs."""

from __future__ import annotations

import json

import msgspec

from lib.quality import (
    AccurateRipBitMatch,
    CdRipBitVerification,
    CdTocIdentity,
    CtdbWholeDiscMatch,
)


def _ctdb_proof(confidence: int) -> CdRipBitVerification:
    return CdRipBitVerification(
        toc=CdTocIdentity(
            track_offsets_sectors=[0],
            leadout_sector=470,
            accuraterip_id="000001d6-000003ac-02000601",
            musicbrainz_disc_id="exact-disc-id",
        ),
        ctdb=CtdbWholeDiscMatch(
            provider="ctdb",
            url="https://db.cue.tools/lookup2.php",
            entry_id=f"ctdb-entry-{confidence}",
            confidence=confidence,
            crc32=0x12345678,
            stride_samples=5880,
            response_toc_sectors=[0, 470],
            response_toc_shift_sectors=0,
            response_sha256="a" * 64,
        ),
    )


def _accuraterip_proof() -> CdRipBitVerification:
    offsets = [index * 200 for index in range(12)]
    return CdRipBitVerification(
        provenance="carried",
        source_format="alac",
        toc=CdTocIdentity(
            track_offsets_sectors=offsets,
            leadout_sector=offsets[-1] + 470,
            accuraterip_id="000001d6-000003ac-0200060c",
            musicbrainz_disc_id="exact-disc-id-12",
        ),
        accuraterip=AccurateRipBitMatch(
            provider="accuraterip",
            url="https://www.accuraterip.com/example.bin",
            checksum_version="arv2",
            read_offset_samples=664,
            track_confidences=[5, 4, 3, 2, 1, 6, 7, 8, 4, 3, 2, 5],
            track_checksums=[0x11111111 + index for index in range(12)],
            response_sha256="b" * 64,
        ),
    )


def fixture_payload() -> dict[str, object]:
    proofs = {
        "ctdb_6": _ctdb_proof(6),
        "ctdb_24": _ctdb_proof(24),
        "accuraterip_min_1": _accuraterip_proof(),
    }
    for name, proof in proofs.items():
        if errors := proof.validation_errors():
            raise ValueError(f"invalid {name} fixture: {'; '.join(errors)}")
    return msgspec.to_builtins(proofs)


def fixture_text() -> str:
    return json.dumps(
        fixture_payload(),
        indent=2,
        sort_keys=True,
    ) + "\n"


if __name__ == "__main__":
    print(fixture_text(), end="")
