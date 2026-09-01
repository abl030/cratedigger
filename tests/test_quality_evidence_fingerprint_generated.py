"""Generated filesystem patrol for the album-path fingerprint consumers."""

from __future__ import annotations

import os
import tempfile
import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.beets_db import AlbumInfo
from lib.current_library_evidence import persist_exact_current_spectral_from_attempt
from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    SpectralAnalysisDetail,
    VerifiedLosslessProof,
)
from lib.quality_evidence import snapshot_audio_files
from lib.sidecar import SIDECAR_FILENAME
from lib.sidecar_service import write_sidecar_for_request
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row

_MBID = "rel-1140"
_REQUEST_ID = 1140


def _seed_current_evidence(
    db: FakePipelineDB,
    evidence: AlbumQualityEvidence,
) -> AlbumQualityEvidence:
    db.upsert_album_quality_evidence(evidence)
    stored = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert stored is not None and stored.id is not None
    db.set_request_current_evidence(_REQUEST_ID, stored.id)
    return stored


def _verified_lossless_evidence(
    files: list[AlbumQualityEvidenceFile],
) -> AlbumQualityEvidence:
    return make_album_quality_evidence(
        mb_release_id=_MBID,
        files=files,
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=900,
            avg_bitrate_kbps=1000,
            format="flac",
            spectral_grade="genuine",
            spectral_subject="source",
            spectral_provenance="measured",
            was_converted_from="flac",
        ),
        storage_format="FLAC",
        verified_lossless_proof=VerifiedLosslessProof(
            provenance="measured", source="flac", classifier="spectral",
        ),
    )


class TestFingerprintAlbumPathConsumersGenerated(unittest.TestCase):
    """Empty directories are never a current-evidence witness."""

    @example(consumer="sidecar", file_sizes=[])
    @example(consumer="preview", file_sizes=[])
    @given(
        consumer=st.sampled_from(("sidecar", "preview")),
        file_sizes=st.lists(st.integers(min_value=1, max_value=64), max_size=3),
    )
    def test_real_consumers_accept_only_nonempty_matching_snapshots(
        self,
        *,
        consumer: str,
        file_sizes: list[int],
    ) -> None:
        with tempfile.TemporaryDirectory() as album_path:
            for index, size in enumerate(file_sizes, start=1):
                with open(os.path.join(album_path, f"{index:02}.flac"), "wb") as fh:
                    fh.write(b"x" * size)
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=_REQUEST_ID,
                mb_release_id=_MBID,
                status="imported",
            ))
            files = snapshot_audio_files(album_path)

            if consumer == "sidecar":
                _seed_current_evidence(
                    db,
                    _verified_lossless_evidence(files),
                )
                beets = FakeBeetsDB()
                beets.set_album_info(_MBID, AlbumInfo(
                    album_id=1,
                    track_count=max(1, len(files)),
                    min_bitrate_kbps=900,
                    is_cbr=True,
                    album_path=album_path,
                ))

                result = write_sidecar_for_request(
                    db, beets, _REQUEST_ID, mb_release_id=_MBID,
                )

                self.assertEqual(
                    result.outcome,
                    "written" if files else "skipped_evidence_stale",
                )
                self.assertEqual(
                    os.path.exists(os.path.join(album_path, SIDECAR_FILENAME)),
                    bool(files),
                )
                return

            current = _seed_current_evidence(db, make_album_quality_evidence(
                mb_release_id=_MBID,
                source_path=album_path,
                files=files,
            ))
            result = persist_exact_current_spectral_from_attempt(
                db,
                request_id=_REQUEST_ID,
                current_evidence=current,
                measured_existing=SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                    bitrate_kbps=96,
                    spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
                ),
                measured_existing_path=album_path,
            )

            self.assertEqual(result.status, "ready" if files else "stale")


if __name__ == "__main__":
    unittest.main()
