"""Pure tests for the verified-lossless album sidecar (issue #184).

The sidecar is derived state: a ``cratedigger.json`` written into a
verified-lossless album folder, built entirely from the content-addressed
``AlbumQualityEvidence`` we already persist. These tests pin the pure
build/gate logic and the wire-boundary contract (msgspec round-trip +
ValidationError on type drift).
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

import msgspec

from lib.quality import (
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    VerifiedLosslessProof,
)
from lib.sidecar import (
    SIDECAR_FILENAME,
    SIDECAR_GENERATOR,
    SIDECAR_SCHEMA_VERSION,
    AlbumSidecar,
    build_sidecar,
    should_write_sidecar,
)
from tests.helpers import make_album_quality_evidence


def _verified_lossless_evidence(**overrides):
    """Evidence with a genuine verified-lossless measurement + proof."""
    measurement = AudioQualityMeasurement(
        min_bitrate_kbps=900,
        avg_bitrate_kbps=1000,
        median_bitrate_kbps=950,
        format="flac",
        is_cbr=False,
        spectral_grade="genuine",
        spectral_bitrate_kbps=None,
        verified_lossless=True,
        was_converted_from="flac",
    )
    files = [
        AlbumQualityEvidenceFile(
            relative_path="01 - First.opus",
            size_bytes=5_000_000,
            mtime_ns=1_700_000_000_000_000_000,
            extension="opus",
            container="ogg",
            codec="opus",
        ),
        AlbumQualityEvidenceFile(
            relative_path="02 - Second.opus",
            size_bytes=6_000_000,
            mtime_ns=1_700_000_000_000_000_001,
            extension="opus",
            container="ogg",
            codec="opus",
        ),
    ]
    ev = make_album_quality_evidence(
        mb_release_id="rel-abc",
        files=files,
        measurement=measurement,
        codec="opus",
        container="ogg",
        storage_format="opus 128",
        target_format="opus",
        v0_metric=AlbumQualityV0Metric(
            min_bitrate_kbps=950,
            avg_bitrate_kbps=1000,
            median_bitrate_kbps=970,
            source_lineage="lossless_source",
        ),
        verified_lossless_proof=VerifiedLosslessProof(
            proof_origin="import",
            source="flac",
            classifier="spectral",
            detail="genuine cliff",
        ),
    )
    # ``make_album_quality_evidence`` defaults ``audio_file_count`` via the
    # struct (0); set it from the fileset so the sidecar reports it honestly.
    ev = msgspec.structs.replace(ev, audio_file_count=len(files))
    if overrides:
        ev = msgspec.structs.replace(ev, **overrides)
    return ev


class TestShouldWriteSidecar(unittest.TestCase):
    """The gate fires only for verified-lossless evidence."""

    def test_verified_lossless_evidence_qualifies(self):
        self.assertTrue(should_write_sidecar(_verified_lossless_evidence()))

    def test_non_verified_lossless_evidence_does_not(self):
        # Default builder measurement has verified_lossless=False.
        self.assertFalse(should_write_sidecar(make_album_quality_evidence()))


class TestBuildSidecar(unittest.TestCase):
    """``build_sidecar`` maps evidence → the on-disk payload faithfully."""

    def setUp(self):
        self.gen_at = datetime(2026, 6, 18, 12, 0, 0, tzinfo=timezone.utc)
        self.evidence = _verified_lossless_evidence()
        self.sidecar = build_sidecar(
            self.evidence,
            source_username="archivist42",
            generated_at=self.gen_at,
        )

    def test_envelope_fields(self):
        self.assertEqual(self.sidecar.schema_version, SIDECAR_SCHEMA_VERSION)
        self.assertEqual(self.sidecar.generator, SIDECAR_GENERATOR)
        self.assertEqual(self.sidecar.mb_release_id, "rel-abc")
        self.assertEqual(self.sidecar.generated_at, self.gen_at)
        self.assertTrue(self.sidecar.verified_lossless)
        self.assertEqual(self.sidecar.source_username, "archivist42")
        self.assertEqual(self.sidecar.audio_file_count, 2)

    def test_quality_block(self):
        q = self.sidecar.quality
        self.assertEqual(q.codec, "opus")
        self.assertEqual(q.container, "ogg")
        self.assertEqual(q.storage_format, "opus 128")
        self.assertEqual(q.target_format, "opus")
        self.assertEqual(q.spectral_grade, "genuine")
        self.assertEqual(q.min_bitrate_kbps, 900)
        self.assertEqual(q.avg_bitrate_kbps, 1000)
        self.assertEqual(q.median_bitrate_kbps, 950)
        self.assertFalse(q.is_cbr)
        self.assertEqual(q.was_converted_from, "flac")

    def test_proof_block(self):
        self.assertIsNotNone(self.sidecar.proof)
        assert self.sidecar.proof is not None
        self.assertEqual(self.sidecar.proof.proof_origin, "import")
        self.assertEqual(self.sidecar.proof.source, "flac")
        self.assertEqual(self.sidecar.proof.classifier, "spectral")
        self.assertEqual(self.sidecar.proof.detail, "genuine cliff")

    def test_v0_metric_block(self):
        self.assertIsNotNone(self.sidecar.v0_metric)
        assert self.sidecar.v0_metric is not None
        self.assertEqual(self.sidecar.v0_metric.avg_bitrate_kbps, 1000)
        self.assertEqual(self.sidecar.v0_metric.source_lineage, "lossless_source")

    def test_tracks_block(self):
        self.assertEqual(len(self.sidecar.tracks), 2)
        first = self.sidecar.tracks[0]
        self.assertEqual(first.relative_path, "01 - First.opus")
        self.assertEqual(first.extension, "opus")
        self.assertEqual(first.container, "ogg")
        self.assertEqual(first.codec, "opus")
        self.assertEqual(first.size_bytes, 5_000_000)

    def test_optional_blocks_absent_when_evidence_lacks_them(self):
        ev = _verified_lossless_evidence(
            v0_metric=None, verified_lossless_proof=None
        )
        sidecar = build_sidecar(
            ev, source_username=None, generated_at=self.gen_at
        )
        self.assertIsNone(sidecar.proof)
        self.assertIsNone(sidecar.v0_metric)
        self.assertIsNone(sidecar.source_username)


class TestSidecarWireBoundary(unittest.TestCase):
    """The sidecar crosses JSON to disk and to other Cratediggers."""

    def test_round_trip_through_json(self):
        sidecar = build_sidecar(
            _verified_lossless_evidence(),
            source_username="peer",
            generated_at=datetime(2026, 6, 18, tzinfo=timezone.utc),
        )
        encoded = msgspec.json.encode(sidecar)
        decoded = msgspec.json.decode(encoded, type=AlbumSidecar)
        self.assertEqual(decoded, sidecar)

    def test_filename_constant_is_visible_not_dotfile(self):
        # Must be browsable on slskd shares by other Cratediggers.
        self.assertEqual(SIDECAR_FILENAME, "cratedigger.json")
        self.assertFalse(SIDECAR_FILENAME.startswith("."))

    def test_type_drift_at_boundary_raises(self):
        # schema_version is an int; a string must be rejected, not coerced.
        bad = (
            b'{"schema_version": "one", "generator": "cratedigger", '
            b'"mb_release_id": "x", "generated_at": "2026-06-18T00:00:00Z", '
            b'"verified_lossless": true, "quality": {}, "tracks": [], '
            b'"audio_file_count": 0}'
        )
        with self.assertRaises(msgspec.ValidationError):
            msgspec.json.decode(bad, type=AlbumSidecar)


if __name__ == "__main__":
    unittest.main()
