#!/usr/bin/env python3
"""Generated PostgreSQL invariant for lossless-lineage spectral subjects."""

from __future__ import annotations

from dataclasses import dataclass
import unittest

from hypothesis import example, given, strategies as st
import msgspec
import psycopg2
import psycopg2.errors

import tests._hypothesis_profiles  # noqa: F401
from lib.quality import (
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    VerifiedLosslessProof,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence
from tests.test_pipeline_db import TEST_DSN, requires_postgres


CONSTRAINT = "album_quality_evidence_lossless_lineage_spectral_subject"
LOSSLESS_CONVERSION_SOURCES = frozenset({"flac", "alac", "wav"})


@dataclass(frozen=True)
class EvidenceLineageWorld:
    lineage_version: int
    spectral_subject: str | None
    v0_subject: str | None
    verified_lossless: bool
    was_converted_from: str | None

    @property
    def has_lossless_lineage(self) -> bool:
        converted = (self.was_converted_from or "").lower()
        return (
            self.v0_subject == "source"
            or self.verified_lossless
            or converted in LOSSLESS_CONVERSION_SOURCES
        )

    @property
    def must_be_rejected(self) -> bool:
        return (
            self.lineage_version >= 4
            and self.spectral_subject == "installed"
            and self.has_lossless_lineage
        )


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
    """The DB rejects exactly installed spectral on v4 lossless lineage."""
    if world.must_be_rejected:
        if not isinstance(error, psycopg2.errors.CheckViolation):
            raise AssertionError(
                f"invalid lossless-lineage world was accepted: {world!r}"
            )
        if error.diag.constraint_name != CONSTRAINT:
            raise AssertionError(
                f"wrong constraint rejected {world!r}: "
                f"{error.diag.constraint_name!r}"
            )
        return
    if error is not None:
        raise AssertionError(
            f"valid evidence world was rejected: {world!r}: {error!r}"
        )


def assert_lossless_merge_converged(
    *,
    existing_subject: str | None,
    spectral_grade: str | None,
    spectral_subject: str | None,
) -> None:
    """New lineage clears installed spectral but preserves source facts."""
    if existing_subject == "source":
        if spectral_grade != "genuine" or spectral_subject != "source":
            raise AssertionError("source-subject spectral fact was not preserved")
        return
    if spectral_grade is not None or spectral_subject is not None:
        raise AssertionError("installed spectral survived new lossless lineage")


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
            spectral_grade=None,
            spectral_bitrate_kbps=None,
            spectral_subject=None,
            spectral_provenance=None,
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
        except Exception as exc:
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
    @given(
        existing_subject=st.sampled_from((None, "installed", "source")),
        anchor=st.sampled_from(("source_v0", "proof", "conversion")),
        converted_from=st.sampled_from(("flac", "FLAC", "alac", "wav")),
    )
    @example(existing_subject="installed", anchor="source_v0", converted_from="flac")
    @example(existing_subject="installed", anchor="proof", converted_from="flac")
    @example(existing_subject="installed", anchor="conversion", converted_from="FLAC")
    @example(existing_subject="source", anchor="source_v0", converted_from="flac")
    def test_same_address_merge_converges_to_r19(
        self,
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
            spectral_grade=grade,
            spectral_subject=subject,
        )


class TestLosslessLineageCheckCheckerTripsOnViolation(unittest.TestCase):
    def test_checker_rejects_accepted_installed_source_anchor(self) -> None:
        world = EvidenceLineageWorld(4, "installed", "source", False, None)
        with self.assertRaises(AssertionError):
            assert_database_matches_lineage_oracle(world, None)

    def test_checker_rejects_denied_native_installed_fact(self) -> None:
        world = EvidenceLineageWorld(4, "installed", None, False, "m4a")
        error = psycopg2.errors.CheckViolation("known-bad rejection")
        with self.assertRaises(AssertionError):
            assert_database_matches_lineage_oracle(world, error)

    def test_merge_checker_rejects_preserved_installed_spectral(self) -> None:
        with self.assertRaises(AssertionError):
            assert_lossless_merge_converged(
                existing_subject="installed",
                spectral_grade="genuine",
                spectral_subject="installed",
            )


if __name__ == "__main__":
    unittest.main()
