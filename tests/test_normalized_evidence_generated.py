"""Generated trust-boundary contract for issue #853."""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from typing import TypeGuard
from unittest.mock import patch

import tests._hypothesis_profiles  # noqa: F401
from hypothesis import example, given
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib.import_preview import measure_and_persist_candidate_evidence
from lib.measurement import LocalFileInspection, PreimportMeasurement
from lib.quality import AlbumQualityEvidenceFile
from lib.quality_evidence import EvidenceBuildResult, snapshot_audio_files, snapshot_fingerprint
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row


def is_album_quality_evidence_file_list(
    value: object,
) -> TypeGuard[list[AlbumQualityEvidenceFile]]:
    """Narrow the deliberately broad persistence seam at the test boundary."""
    return isinstance(value, list) and all(
        isinstance(file, AlbumQualityEvidenceFile) for file in value
    )


def assert_normalized_evidence_contract(
    *,
    on_disk: bytes,
    persistence_bytes: bytes,
    evidence_files: list[AlbumQualityEvidenceFile],
    evidence_fingerprint: str,
    normalized: bytes,
) -> None:
    """Owned normalization must precede the evidence snapshot.

    The inventory fingerprint deliberately does not include content bytes, so
    a same-size mutation needs this direct byte assertion too.  Otherwise a
    pre-repair snapshot can look fresh even though it describes the wrong
    bytes.
    """
    if on_disk != normalized:
        raise AssertionError("owned processing bytes were not normalized")
    if persistence_bytes != normalized:
        raise AssertionError("persistence observed pre-normalization bytes")
    if evidence_files[0].size_bytes != len(normalized):
        raise AssertionError("evidence describes pre-normalization bytes")
    if evidence_fingerprint != snapshot_fingerprint(evidence_files):
        raise AssertionError("evidence fingerprint does not describe its snapshot")


class TestNormalizedEvidenceGenerated(unittest.TestCase):
    @given(raw=st.binary(min_size=1, max_size=64))
    @example(raw=b"Styrofoam / Dntel")
    def test_owned_processing_repair_and_evidence_always_converge(self, raw: bytes):
        root = tempfile.mkdtemp(prefix="cratedigger-853-gen-")
        source_root = os.path.join(root, "slskd")
        processing = os.path.join(root, "processing")
        albums = os.path.join(processing, "albums")
        preview = os.path.join(processing, "preview")
        os.mkdir(source_root)
        os.mkdir(processing, 0o700)
        os.mkdir(albums, 0o700)
        os.mkdir(preview, 0o700)
        album = tempfile.mkdtemp(prefix="album-", dir=albums)
        track = os.path.join(album, "01.mp3")
        # Preserve length to exercise the inventory fingerprint's blind spot:
        # it intentionally keys file identity by path/size/container, not a
        # content hash.  The test must therefore prove the bytes persisted
        # after repair are the bytes on disk, not merely matching-sized bytes.
        normalized = bytes(byte ^ 0xFF for byte in raw)
        persisted_files: list[list[AlbumQualityEvidenceFile]] = []
        persisted_fingerprints: list[str] = []
        persisted_bytes: list[bytes] = []
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=853, mb_release_id="issue-853"))
        cfg = CratediggerConfig(
            slskd_download_dir=source_root,
            processing_dir=processing,
            audio_check_mode="off",
        )
        try:
            with open(track, "wb") as handle:
                handle.write(raw)

            def repair(path: str) -> None:
                self.assertEqual(path, album)
                with open(track, "wb") as handle:
                    handle.write(normalized)

            def persist(*_args: object, **_kwargs: object) -> EvidenceBuildResult:
                files_value = _kwargs["files"]
                if not is_album_quality_evidence_file_list(files_value):
                    raise AssertionError("persistence did not receive an evidence inventory")
                files = files_value
                persisted_files.append(files)
                persisted_fingerprints.append(snapshot_fingerprint(files))
                with open(track, "rb") as handle:
                    persisted_bytes.append(handle.read())
                return EvidenceBuildResult(
                    make_album_quality_evidence(mb_release_id="issue-853"),
                    "ready",
                )

            with patch(
                "lib.import_preview.inspect_local_files",
                return_value=LocalFileInspection(filetype="mp3"),
            ), patch(
                "lib.import_preview.measure_preimport_state",
                return_value=PreimportMeasurement(
                    audio_corrupt=True,
                    folder_layout="flat",
                    audio_file_count=1,
                ),
            ):
                result = measure_and_persist_candidate_evidence(
                    db,
                    request_id=853,
                    path=album,
                    runtime_config=cfg,
                    current_evidence_loader=lambda *_args, **_kwargs: EvidenceBuildResult(
                        None, "empty_current",
                    ),
                    persist_measurement_fn=persist,
                    repair_fn=repair,
                )
            self.assertEqual(result.verdict, "evidence_ready")
            with open(track, "rb") as handle:
                on_disk = handle.read()
            assert_normalized_evidence_contract(
                on_disk=on_disk,
                persistence_bytes=persisted_bytes[0],
                evidence_files=persisted_files[0],
                evidence_fingerprint=persisted_fingerprints[0],
                normalized=normalized,
            )
            # Same-size fault injection: the canonical inventory fingerprint
            # cannot distinguish raw from repaired bytes.  The byte check
            # above is what makes ordering at the normalization boundary real.
            self.assertEqual(persisted_files[0], snapshot_audio_files(album))
            self.assertEqual(
                persisted_fingerprints[0],
                snapshot_fingerprint(snapshot_audio_files(album)),
            )
        finally:
            shutil.rmtree(root, ignore_errors=True)


class TestNormalizedEvidenceCheckerTripsOnViolation(unittest.TestCase):
    def test_rejects_pre_normalization_inventory_fault_injection(self):
        raw = b"raw"
        normalized = b"normalized"
        stale_pre_repair_files = [AlbumQualityEvidenceFile(
            relative_path="01.mp3",
            size_bytes=len(raw),
            mtime_ns=0,
            extension="mp3",
            container="mp3",
            codec="mp3",
        )]
        with self.assertRaises(AssertionError):
            assert_normalized_evidence_contract(
                on_disk=normalized,
                persistence_bytes=normalized,
                evidence_files=stale_pre_repair_files,
                evidence_fingerprint=snapshot_fingerprint(stale_pre_repair_files),
                normalized=normalized,
            )
