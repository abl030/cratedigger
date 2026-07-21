#!/usr/bin/env python3
"""Current-Beets authority laws for evidence consumers (issue #762)."""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from pathlib import Path

from hypothesis import HealthCheck, example, given, settings, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_db import BeetsDB
from lib.import_evidence import (
    ActionEvidenceProvenance,
    CURRENT_STATUS_FAILED,
    CURRENT_STATUS_LOADED,
    CurrentEvidenceActionResult,
    load_current_evidence_for_action,
)
from lib.quality import AudioQualityMeasurement, VerifiedLosslessProof
from lib.quality_evidence import snapshot_audio_files
from lib.sidecar import SIDECAR_FILENAME
from lib.sidecar_service import write_sidecar_for_request
from tests.beets_world import BeetsWorld, BeetsWorldRelease
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row


REPO = Path(__file__).resolve().parent.parent
MB_RELEASE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
DISCOGS_RELEASE_ID = "12856590"


@dataclass(frozen=True)
class EvidenceAuthorityExpectation:
    exact_album_count: int
    current_path: str | None
    historical_source_path: str | None
    current_fingerprint_matches: bool


def assert_evidence_authority(
    result: CurrentEvidenceActionResult | None,
    expected: EvidenceAuthorityExpectation,
) -> None:
    """Executable law for exact current membership and evidence reuse."""

    if expected.exact_album_count == 0:
        if result is not None:
            raise AssertionError("missing exact membership was not honest absence")
        return
    if expected.exact_album_count > 1:
        if result is None:
            raise AssertionError("ambiguous exact membership collapsed into absence")
        if (
            result.available
            or result.provenance.current_status != CURRENT_STATUS_FAILED
            or "ambiguous" not in (result.provenance.fallback_reason or "").casefold()
        ):
            raise AssertionError("ambiguous exact membership did not fail closed")
        return

    if result is None:
        raise AssertionError("unique exact membership collapsed into absence")
    if expected.current_fingerprint_matches:
        if not result.available:
            raise AssertionError("matching current fingerprint was not reusable")
        if result.provenance.current_status != CURRENT_STATUS_LOADED:
            raise AssertionError("matching linked evidence was unnecessarily rebuilt")
        if result.provenance.installed_path != expected.current_path:
            raise AssertionError("current Beets path was not exposed separately")
        if result.evidence is None:
            raise AssertionError("matching linked evidence disappeared")
        if result.evidence.source_path != expected.historical_source_path:
            raise AssertionError("historical evidence source_path was rewritten")
        return

    if result.available and result.provenance.current_status == CURRENT_STATUS_LOADED:
        raise AssertionError("stale fingerprint authorized linked evidence reuse")


def _release(source: str, suffix: str = "") -> BeetsWorldRelease:
    release_id = MB_RELEASE_ID if source == "mb" else DISCOGS_RELEASE_ID
    return BeetsWorldRelease(
        release_id=release_id,
        artist=f"Authority Artist {suffix}".strip(),
        album=f"Exact Pressing {suffix}".strip(),
        year=2007,
        track_count=1,
    )


def _persist_linked_evidence(
    db: FakePipelineDB,
    *,
    release_id: str,
    album_path: str,
    source_path: str,
) -> None:
    evidence = make_album_quality_evidence(
        mb_release_id=release_id,
        source_path=source_path,
        files=snapshot_audio_files(album_path),
    )
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    db.set_request_current_evidence(42, persisted.id)


def _persist_verified_linked_evidence(
    db: FakePipelineDB,
    *,
    release_id: str,
    album_path: str,
) -> None:
    evidence = make_album_quality_evidence(
        mb_release_id=release_id,
        source_path="/historical/candidate-capture",
        files=snapshot_audio_files(album_path),
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=900,
            avg_bitrate_kbps=1000,
            format="FLAC",
            spectral_grade="genuine",
            spectral_subject="source",
            spectral_provenance="measured",
            was_converted_from="flac",
        ),
        storage_format="FLAC",
        verified_lossless_proof=VerifiedLosslessProof(
            provenance="measured",
            source="flac",
            classifier="spectral",
        ),
    )
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    db.set_request_current_evidence(42, persisted.id)


class TestCurrentEvidenceAuthorityPins(unittest.TestCase):
    def test_moved_unique_reuses_fingerprint_without_rewriting_capture_path(self) -> None:
        with BeetsWorld(REPO) as world:
            snapshot = world.import_release(_release("mb"))
            historical = str(world.root / "already-removed-staging")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, mb_release_id=MB_RELEASE_ID))
            _persist_linked_evidence(
                db,
                release_id=MB_RELEASE_ID,
                album_path=snapshot.album_path,
                source_path=historical,
            )
            moved = world.relocate_release_out_of_band(
                MB_RELEASE_ID,
                world.library_root / "Moved" / "Unicode 曖昧",
                store_relative_paths=True,
            )

            result = load_current_evidence_for_action(
                db,
                request_id=42,
                mb_release_id=MB_RELEASE_ID,
                beets_library_db_path=str(world.library_db),
                beets_library_root=str(world.library_root),
            )

            assert_evidence_authority(result, EvidenceAuthorityExpectation(
                exact_album_count=1,
                current_path=moved.album_path,
                historical_source_path=historical,
                current_fingerprint_matches=True,
            ))
            linked = db.load_album_quality_evidence_by_id(
                db.get_request_current_evidence_id(42),
            )
            assert linked is not None
            self.assertEqual(linked.source_path, historical)

    def test_real_duplicate_exact_identity_is_failed_not_missing(self) -> None:
        with BeetsWorld(REPO) as world:
            world.import_release(_release("mb", "one"))
            world.import_duplicate_release(_release("mb", "two"))
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, mb_release_id=MB_RELEASE_ID))

            result = load_current_evidence_for_action(
                db,
                request_id=42,
                mb_release_id=MB_RELEASE_ID,
                beets_library_db_path=str(world.library_db),
                beets_library_root=str(world.library_root),
            )

            assert_evidence_authority(result, EvidenceAuthorityExpectation(
                exact_album_count=2,
                current_path=None,
                historical_source_path=None,
                current_fingerprint_matches=False,
            ))

    def test_sidecar_resolves_modern_and_legacy_discogs_columns_exactly(self) -> None:
        for legacy in (False, True):
            with self.subTest(legacy=legacy), BeetsWorld(REPO) as world:
                snapshot = world.import_release(_release("discogs"))
                world.set_discogs_identity_layout(
                    DISCOGS_RELEASE_ID,
                    legacy=legacy,
                )
                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=42,
                    mb_release_id=DISCOGS_RELEASE_ID,
                    status="imported",
                ))
                _persist_verified_linked_evidence(
                    db,
                    release_id=DISCOGS_RELEASE_ID,
                    album_path=snapshot.album_path,
                )
                with BeetsDB(
                    str(world.library_db),
                    library_root=str(world.library_root),
                ) as beets:
                    result = write_sidecar_for_request(
                        db,
                        beets,
                        42,
                        mb_release_id=DISCOGS_RELEASE_ID,
                    )

                self.assertEqual(result.outcome, "written")
                self.assertEqual(
                    result.path,
                    str(Path(snapshot.album_path) / SIDECAR_FILENAME),
                )


class TestCurrentEvidenceAuthorityGenerated(unittest.TestCase):
    @settings(
        max_examples=9,
        deadline=None,
        suppress_health_check=(HealthCheck.too_slow,),
    )
    @example(source="discogs_legacy", cardinality=2, move=True, mutate=False)
    @example(source="mb", cardinality=1, move=True, mutate=False)
    @example(source="discogs_modern", cardinality=1, move=False, mutate=True)
    @given(
        source=st.sampled_from(("mb", "discogs_modern", "discogs_legacy")),
        cardinality=st.integers(min_value=0, max_value=2),
        move=st.booleans(),
        mutate=st.booleans(),
    )
    def test_action_loader_obeys_cardinality_path_and_fingerprint_law(
        self,
        source: str,
        cardinality: int,
        move: bool,
        mutate: bool,
    ) -> None:
        identity_source = "mb" if source == "mb" else "discogs"
        release_id = (
            MB_RELEASE_ID if identity_source == "mb" else DISCOGS_RELEASE_ID
        )
        with BeetsWorld(REPO) as world:
            snapshot = None
            if cardinality:
                snapshot = world.import_release(_release(identity_source, "one"))
                if source == "discogs_legacy":
                    world.set_discogs_identity_layout(release_id, legacy=True)
                if cardinality == 2:
                    world.import_duplicate_release(
                        _release(identity_source, "two"),
                    )

            historical = str(world.root / "historical-staging")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42, mb_release_id=release_id))
            if snapshot is not None:
                _persist_linked_evidence(
                    db,
                    release_id=release_id,
                    album_path=snapshot.album_path,
                    source_path=historical,
                )

            current_path = snapshot.album_path if snapshot is not None else None
            if cardinality == 1 and move:
                moved = world.relocate_release_out_of_band(
                    release_id,
                    world.library_root / "Moved" / source,
                    store_relative_paths=True,
                )
                current_path = moved.album_path
            if cardinality == 1 and mutate:
                assert current_path is not None
                audio_path = next(
                    path for path in Path(current_path).iterdir() if path.is_file()
                )
                with audio_path.open("ab") as handle:
                    handle.write(b"changed")

            result = load_current_evidence_for_action(
                db,
                request_id=42,
                mb_release_id=release_id,
                beets_library_db_path=str(world.library_db),
                beets_library_root=str(world.library_root),
            )

            assert_evidence_authority(result, EvidenceAuthorityExpectation(
                exact_album_count=cardinality,
                current_path=current_path,
                historical_source_path=historical if cardinality else None,
                current_fingerprint_matches=not mutate,
            ))

    def test_checker_rejects_collapsed_ambiguous_stale_and_rewritten_worlds(self) -> None:
        with self.assertRaisesRegex(AssertionError, "ambiguous.*collapsed"):
            assert_evidence_authority(None, EvidenceAuthorityExpectation(
                exact_album_count=2,
                current_path=None,
                historical_source_path=None,
                current_fingerprint_matches=False,
            ))

        bad_loaded = CurrentEvidenceActionResult(
            evidence=make_album_quality_evidence(source_path="/history"),
            provenance=ActionEvidenceProvenance(
                current_status=CURRENT_STATUS_LOADED,
            ),
        )
        with self.assertRaisesRegex(AssertionError, "stale fingerprint"):
            assert_evidence_authority(bad_loaded, EvidenceAuthorityExpectation(
                exact_album_count=1,
                current_path="/current",
                historical_source_path="/history",
                current_fingerprint_matches=False,
            ))

        rewritten = CurrentEvidenceActionResult(
            evidence=make_album_quality_evidence(source_path="/current"),
            provenance=ActionEvidenceProvenance(
                current_status=CURRENT_STATUS_LOADED,
                installed_path="/current",
            ),
        )
        with self.assertRaisesRegex(AssertionError, "source_path was rewritten"):
            assert_evidence_authority(rewritten, EvidenceAuthorityExpectation(
                exact_album_count=1,
                current_path="/current",
                historical_source_path="/history",
                current_fingerprint_matches=True,
            ))


if __name__ == "__main__":
    unittest.main()
