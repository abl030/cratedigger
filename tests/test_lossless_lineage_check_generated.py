"""Generated PostgreSQL invariant for durable conversion lineage."""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from itertools import product

import msgspec
import psycopg2
import psycopg2.errors
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.quality import (
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    VerifiedLosslessProof,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row
from tests.test_pipeline_db import TEST_DSN, requires_postgres


@dataclass(frozen=True)
class EvidenceLineageWorld:
    lineage_version: int
    spectral_subject: str | None
    v0_subject: str | None
    verified_lossless: bool
    was_converted_from: str | None


@st.composite
def evidence_lineage_worlds(draw) -> EvidenceLineageWorld:
    converted = draw(st.sampled_from(
        (None, "flac", "FLAC", "alac", "wav", "m4a", "mp3")
    ))
    return EvidenceLineageWorld(
        lineage_version=draw(st.sampled_from((1, 3, 4))),
        spectral_subject=draw(st.sampled_from((None, "installed", "source"))),
        v0_subject=draw(st.sampled_from((None, "installed", "source"))),
        verified_lossless=draw(st.booleans()),
        was_converted_from=converted,
    )


def assert_database_matches_lineage_oracle(
    world: EvidenceLineageWorld,
    error: Exception | None,
) -> None:
    """Migration 073 permits independent output and spectral facts.

    R19's exact manifest predicate, rather than a database-only codec rule,
    decides whether a source observation may be reused.
    """
    if error is not None:
        raise AssertionError(
            f"durable-lineage world was rejected: {world!r}: {error!r}"
        )


def assert_lossless_merge_converged(
    *,
    existing_subject: str | None,
    incoming_preserves_source: bool,
    spectral_grade: str | None,
    spectral_subject: str | None,
) -> None:
    """Only an exact derivative can replace a same-address spectral tuple."""
    if incoming_preserves_source:
        if spectral_grade != "likely_transcode" or spectral_subject != "source":
            raise AssertionError("preserved-source derivative was not retained")
        return
    if existing_subject is None:
        if spectral_grade is not None or spectral_subject is not None:
            raise AssertionError("stale provenance invented spectral evidence")
        return
    if spectral_grade != "genuine" or spectral_subject != existing_subject:
        raise AssertionError("stale provenance erased existing spectral evidence")


def _run_fake_lossless_merge(
    *,
    existing_subject: str | None,
    anchor: str,
    converted_from: str,
) -> tuple[str | None, str | None]:
    measurement = AudioQualityMeasurement(
        min_bitrate_kbps=128,
        avg_bitrate_kbps=128,
        median_bitrate_kbps=128,
        format="Opus",
        spectral_grade=("genuine" if existing_subject is not None else None),
        spectral_subject=existing_subject,  # type: ignore[arg-type]
        spectral_provenance=(
            "measured" if existing_subject is not None else None
        ),
    )
    existing = make_album_quality_evidence(
        mb_release_id="generated-merge-lineage",
        measurement=measurement,
        codec="opus",
        container="opus",
        storage_format="Opus",
    )
    incoming = msgspec.structs.replace(
        existing,
        measurement=msgspec.structs.replace(
            existing.measurement,
            spectral_grade=(
                "likely_transcode" if anchor == "conversion" else None
            ),
            spectral_bitrate_kbps=None,
            spectral_subject=("source" if anchor == "conversion" else None),
            spectral_provenance=(
                "carried" if anchor == "conversion" else None
            ),
            spectral_measurement_version=None,
            was_converted_from=(converted_from if anchor == "conversion" else None),
        ),
        v0_metric=(
            AlbumQualityV0Metric(
                avg_bitrate_kbps=225,
                subject="source",
                provenance="carried",
            )
            if anchor == "source_v0"
            else None
        ),
        verified_lossless_proof=(
            VerifiedLosslessProof(
                provenance="carried",
                source="flac",
                classifier="spectral_verified_lossless",
            )
            if anchor == "proof"
            else None
        ),
    )
    db = FakePipelineDB()
    db.upsert_album_quality_evidence(existing)
    db.upsert_album_quality_evidence(incoming)
    loaded = db.find_album_quality_evidence(
        mb_release_id=existing.mb_release_id,
        snapshot_fingerprint=existing.snapshot_fingerprint,
    )
    assert loaded is not None
    return (
        loaded.measurement.spectral_grade,
        loaded.measurement.spectral_subject,
    )


@requires_postgres
class TestGeneratedLosslessLineageCheck(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = psycopg2.connect(TEST_DSN)

    def tearDown(self) -> None:
        self.conn.close()

    def _insert(self, world: EvidenceLineageWorld) -> Exception | None:
        error: Exception | None = None
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO album_quality_evidence (
                        mb_release_id, snapshot_fingerprint, source_path,
                        measured_at, lineage_version,
                        spectral_grade, spectral_subject,
                        spectral_provenance,
                        v0_avg_bitrate_kbps, v0_subject, v0_provenance,
                        verified_lossless, verified_lossless_provenance,
                        verified_lossless_source,
                        verified_lossless_classifier,
                        was_converted_from, audio_validation
                    ) VALUES (
                        'generated-lineage', 'generated-lineage-fingerprint',
                        '/generated', NOW(), %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        '{"policy_id":"pre-audio-integrity-v2",'
                        '"tool":"legacy","tool_version":"",'
                        '"outcome":"legacy_unrecorded","files_checked":0,'
                        '"files_failed":0,"diagnostics":[],'
                        '"omitted_diagnostics":0}'::jsonb
                    )
                    """,
                    (
                        world.lineage_version,
                        "genuine" if world.spectral_subject is not None else None,
                        world.spectral_subject,
                        (
                            "measured"
                            if world.spectral_subject is not None
                            else None
                        ),
                        220 if world.v0_subject is not None else None,
                        world.v0_subject,
                        "measured" if world.v0_subject is not None else None,
                        world.verified_lossless,
                        "measured" if world.verified_lossless else None,
                        "flac" if world.verified_lossless else None,
                        (
                            "spectral_verified_lossless"
                            if world.verified_lossless
                            else None
                        ),
                        world.was_converted_from,
                    ),
                )
        except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            error = exc
        finally:
            self.conn.rollback()
        return error

    @given(world=evidence_lineage_worlds())
    @example(EvidenceLineageWorld(4, "installed", "source", False, None))
    @example(EvidenceLineageWorld(4, "installed", None, True, None))
    @example(EvidenceLineageWorld(4, "installed", None, False, "flac"))
    @example(EvidenceLineageWorld(4, "installed", None, False, "m4a"))
    @example(EvidenceLineageWorld(4, "installed", None, False, None))
    @example(EvidenceLineageWorld(4, "source", "source", True, "flac"))
    @example(EvidenceLineageWorld(3, "installed", "source", True, "flac"))
    def test_database_matches_full_r19_lineage_oracle(
        self,
        world: EvidenceLineageWorld,
    ) -> None:
        assert_database_matches_lineage_oracle(world, self._insert(world))


class TestGeneratedLosslessLineageMerge(unittest.TestCase):
    def test_same_address_merge_converges_to_r19(self) -> None:
        cases = product(
            (None, "installed", "source"),
            ("source_v0", "proof", "conversion"),
            ("flac", "FLAC", "alac", "wav"),
        )
        for existing_subject, anchor, converted_from in cases:
            with self.subTest(
                existing_subject=existing_subject,
                anchor=anchor,
                converted_from=converted_from,
            ):
                self._assert_same_address_merge(
                    existing_subject=existing_subject,
                    anchor=anchor,
                    converted_from=converted_from,
                )

    def _assert_same_address_merge(
        self,
        *,
        existing_subject: str | None,
        anchor: str,
        converted_from: str,
    ) -> None:
        grade, subject = _run_fake_lossless_merge(
            existing_subject=existing_subject,
            anchor=anchor,
            converted_from=converted_from,
        )
        assert_lossless_merge_converged(
            existing_subject=existing_subject,
            incoming_preserves_source=anchor == "conversion",
            spectral_grade=grade,
            spectral_subject=subject,
        )

    @given(
        writer_order=st.sampled_from((("fresh", "stale"), ("stale", "fresh"))),
        converted_from=st.sampled_from(("flac", "FLAC", "alac", "wav")),
    )
    @example(writer_order=("fresh", "stale"), converted_from="flac")
    @example(writer_order=("stale", "fresh"), converted_from="alac")
    def test_stale_writer_never_erases_durable_conversion_lineage(
        self,
        writer_order: tuple[str, str],
        converted_from: str,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, mb_release_id="durable-lineage"))
        evidence = make_album_quality_evidence(
            mb_release_id="durable-lineage",
            files=[AlbumQualityEvidenceFile(
                relative_path="01.m4a",
                size_bytes=1,
                mtime_ns=1,
                extension="m4a",
                container="m4a",
                codec="m4a",
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=920,
                median_bitrate_kbps=910,
                format="ALAC",
                was_converted_from=converted_from,
            ),
            codec="m4a",
            container="m4a",
            storage_format="ALAC",
        )
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        assert db.set_request_current_evidence(42, stored.id)
        stale = msgspec.structs.replace(
            evidence,
            measurement=msgspec.structs.replace(
                evidence.measurement,
                was_converted_from=None,
            ),
        )
        for writer in writer_order:
            if writer == "fresh":
                self.assertTrue(db.persist_current_spectral_measurement(
                    request_id=42,
                    expected_evidence_id=stored.id,
                    expected_snapshot_fingerprint=evidence.snapshot_fingerprint,
                    grade="genuine",
                    bitrate_kbps=900,
                ))
            else:
                db.upsert_album_quality_evidence(stale)

        loaded = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None
        self.assertEqual(loaded.measurement.was_converted_from, converted_from)
        self.assertEqual(loaded.measurement.spectral_subject, "installed")
        self.assertEqual(loaded.measurement.spectral_grade, "genuine")


class TestLosslessLineageCheckCheckerTripsOnViolation(unittest.TestCase):
    def test_checker_rejects_any_constraint_error(self) -> None:
        world = EvidenceLineageWorld(4, "installed", None, False, "flac")
        error = psycopg2.errors.CheckViolation("known-bad rejection")
        with self.assertRaises(AssertionError):
            assert_database_matches_lineage_oracle(world, error)

    def test_merge_checker_rejects_erased_installed_spectral(self) -> None:
        with self.assertRaises(AssertionError):
            assert_lossless_merge_converged(
                existing_subject="installed",
                incoming_preserves_source=False,
                spectral_grade=None,
                spectral_subject=None,
            )


if __name__ == "__main__":
    unittest.main()
