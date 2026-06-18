"""Orchestration tests for ``write_sidecar_for_request`` (issue #184).

Drives the real service against ``FakePipelineDB`` + ``FakeBeetsDB`` + a temp
album directory that holds real audio files, asserting the on-disk sidecar
(domain outcome) and every skip branch. No mocks of our own logic — stateful
fakes only. The album folder carries real files so the disk-fidelity guard
(current evidence must match the bytes next to it) runs for real.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timezone

import msgspec

from lib.beets_db import AlbumInfo
from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    VerifiedLosslessProof,
)
from lib.quality_evidence import snapshot_audio_files
from lib.sidecar import SIDECAR_FILENAME, AlbumSidecar
from lib.sidecar_service import write_sidecar_for_request
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row

MBID = "rel-184"
REQUEST_ID = 184


def _verified_lossless_measurement() -> AudioQualityMeasurement:
    return AudioQualityMeasurement(
        min_bitrate_kbps=900,
        avg_bitrate_kbps=1000,
        format="flac",
        spectral_grade="genuine",
        verified_lossless=True,
        was_converted_from="flac",
    )


def _proof() -> VerifiedLosslessProof:
    return VerifiedLosslessProof(
        proof_origin="import", source="flac", classifier="spectral"
    )


class _SidecarServiceCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.album_path = os.path.join(self.tmp.name, "Artist", "Album")
        os.makedirs(self.album_path, exist_ok=True)
        # Real audio files so the disk-fidelity guard runs against true bytes.
        for name, size in (("01 - First.flac", 1000), ("02 - Second.flac", 2000)):
            with open(os.path.join(self.album_path, name), "wb") as fh:
                fh.write(b"\0" * size)

        self.db = FakePipelineDB()
        self.db.seed_request(
            make_request_row(id=REQUEST_ID, mb_release_id=MBID, status="imported")
        )
        self.beets = FakeBeetsDB()
        self.beets.set_album_info(
            MBID,
            AlbumInfo(
                album_id=1,
                track_count=2,
                min_bitrate_kbps=900,
                is_cbr=False,
                album_path=self.album_path,
            ),
        )

    def _verified_lossless_evidence(self) -> AlbumQualityEvidence:
        """Evidence whose fingerprint matches the on-disk album files."""
        return make_album_quality_evidence(
            mb_release_id=MBID,
            files=snapshot_audio_files(self.album_path),
            measurement=_verified_lossless_measurement(),
            verified_lossless_proof=_proof(),
        )

    def _seed_current_evidence(self, evidence: AlbumQualityEvidence) -> None:
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_request_current_evidence(REQUEST_ID, stored.id)

    def _read_sidecar(self) -> AlbumSidecar:
        path = os.path.join(self.album_path, SIDECAR_FILENAME)
        with open(path, "rb") as fh:
            return msgspec.json.decode(fh.read(), type=AlbumSidecar)


class TestWriteSidecarHappyPath(_SidecarServiceCase):
    def test_writes_sidecar_for_verified_lossless(self):
        self._seed_current_evidence(self._verified_lossless_evidence())
        self.db.log_download(
            request_id=REQUEST_ID,
            soulseek_username="archivist42",
            outcome="success",
        )
        result = write_sidecar_for_request(
            self.db,
            self.beets,
            REQUEST_ID,
            mb_release_id=MBID,
            generated_at=datetime(2026, 6, 18, tzinfo=timezone.utc),
        )
        self.assertEqual(result.outcome, "written")
        self.assertEqual(
            result.path, os.path.join(self.album_path, SIDECAR_FILENAME)
        )
        self.assertTrue(os.path.exists(result.path or ""))
        sidecar = self._read_sidecar()
        self.assertEqual(sidecar.mb_release_id, MBID)
        self.assertTrue(sidecar.verified_lossless)
        self.assertEqual(sidecar.source_username, "archivist42")
        self.assertEqual(len(sidecar.tracks), 2)

    def test_written_even_without_successful_uploader(self):
        self._seed_current_evidence(self._verified_lossless_evidence())
        result = write_sidecar_for_request(
            self.db, self.beets, REQUEST_ID, mb_release_id=MBID
        )
        self.assertEqual(result.outcome, "written")
        self.assertIsNone(self._read_sidecar().source_username)


class TestImportDispatchSidecarHook(_SidecarServiceCase):
    """The importer success hook writes the sidecar via the shared service."""

    def test_hook_writes_sidecar_via_beets_factory(self):
        from lib.import_dispatch import _write_album_sidecar_after_import

        self._seed_current_evidence(self._verified_lossless_evidence())
        self.db.log_download(
            request_id=REQUEST_ID,
            soulseek_username="peer",
            outcome="success",
        )

        def factory(library_root: str = "") -> FakeBeetsDB:
            return self.beets

        result = _write_album_sidecar_after_import(
            self.db,
            request_id=REQUEST_ID,
            mb_release_id=MBID,
            cfg=None,
            beets_factory=factory,
        )
        self.assertEqual(result.outcome, "written")
        sidecar = self._read_sidecar()
        self.assertTrue(sidecar.verified_lossless)
        self.assertEqual(sidecar.source_username, "peer")

    def test_hook_skips_non_verified_lossless(self):
        from lib.import_dispatch import _write_album_sidecar_after_import

        self._seed_current_evidence(
            make_album_quality_evidence(mb_release_id=MBID)
        )

        def factory(library_root: str = "") -> FakeBeetsDB:
            return self.beets

        result = _write_album_sidecar_after_import(
            self.db,
            request_id=REQUEST_ID,
            mb_release_id=MBID,
            cfg=None,
            beets_factory=factory,
        )
        self.assertEqual(result.outcome, "skipped_not_verified_lossless")
        self.assertFalse(
            os.path.exists(os.path.join(self.album_path, SIDECAR_FILENAME))
        )


class TestWriteSidecarSkips(_SidecarServiceCase):
    def test_skips_when_not_verified_lossless(self):
        # Default builder measurement is NOT verified lossless.
        self._seed_current_evidence(
            make_album_quality_evidence(mb_release_id=MBID)
        )
        result = write_sidecar_for_request(
            self.db, self.beets, REQUEST_ID, mb_release_id=MBID
        )
        self.assertEqual(result.outcome, "skipped_not_verified_lossless")
        self.assertFalse(
            os.path.exists(os.path.join(self.album_path, SIDECAR_FILENAME))
        )

    def test_skips_when_no_current_evidence(self):
        result = write_sidecar_for_request(
            self.db, self.beets, REQUEST_ID, mb_release_id=MBID
        )
        self.assertEqual(result.outcome, "skipped_no_evidence")

    def test_skips_when_no_album_path(self):
        self._seed_current_evidence(self._verified_lossless_evidence())
        # Beets cannot resolve the album on disk.
        self.beets = FakeBeetsDB()
        result = write_sidecar_for_request(
            self.db, self.beets, REQUEST_ID, mb_release_id=MBID
        )
        self.assertEqual(result.outcome, "skipped_no_album_path")

    def test_skips_when_album_path_dir_missing(self):
        self._seed_current_evidence(self._verified_lossless_evidence())
        self.beets.set_album_info(
            MBID,
            AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=900,
                is_cbr=False,
                album_path=os.path.join(self.tmp.name, "does", "not", "exist"),
            ),
        )
        result = write_sidecar_for_request(
            self.db, self.beets, REQUEST_ID, mb_release_id=MBID
        )
        self.assertEqual(result.outcome, "skipped_no_album_path")

    def test_skips_when_evidence_stale_vs_disk(self):
        # Verified-lossless evidence describing files that are NOT on disk —
        # the post-import refresh failed and current_evidence_id is stale.
        stale = make_album_quality_evidence(
            mb_release_id=MBID,
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="old-track.flac",
                    size_bytes=42,
                    mtime_ns=1,
                    extension="flac",
                    container="flac",
                    codec="flac",
                )
            ],
            measurement=_verified_lossless_measurement(),
            verified_lossless_proof=_proof(),
        )
        self._seed_current_evidence(stale)
        result = write_sidecar_for_request(
            self.db, self.beets, REQUEST_ID, mb_release_id=MBID
        )
        self.assertEqual(result.outcome, "skipped_evidence_stale")
        self.assertFalse(
            os.path.exists(os.path.join(self.album_path, SIDECAR_FILENAME))
        )


if __name__ == "__main__":
    unittest.main()
