"""Generated outer-adapter contracts for request-keyed Beets consumers."""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import (
    make_album_quality_evidence,
    make_audio_corrupt_validation_report,
    make_request_row,
)
from tests.web._harness import _FakeDbWebServerCase


def assert_survivor_requeue_floor(
    *,
    persisted_floor: int | None,
    persisted_override: object,
    expected_floor: int | None,
    expected_override: object,
    rejected_average: int | None = None,
) -> None:
    """The HTTP mutation must carry the survivor minimum, never its average."""
    if (
        persisted_floor != expected_floor
        or persisted_override != expected_override
        or (
            rejected_average is not None
            and persisted_floor == rejected_average
        )
    ):
        raise AssertionError(
            "request-keyed current Beets state was not persisted on requeue"
        )


def assert_request_keyed_have_path(
    *, observed_path: str | None, expected_path: str,
) -> None:
    """The measurement adapter must inspect the survivor-held HAVE path."""
    if observed_path != expected_path:
        raise AssertionError("request-keyed HAVE path was not inspected")


class _OmittingCurrentBeets(FakeBeetsDB):
    """Production-shaped resolver fault: requested union side is omitted."""

    def resolve_current_releases(self, identities):
        del identities
        return {}


class TestRequestKeyedHaveGenerated(unittest.TestCase):
    """Drive the real measurement adapter that both preview paths delegate to."""

    @given(floor=st.integers(min_value=96, max_value=1411))
    @example(floor=128)
    @example(floor=1411)
    def test_canonical_held_have_is_measured_for_the_request(
        self, floor: int,
    ) -> None:
        from lib.beets_db import AlbumInfo
        from lib.config import CratediggerConfig
        from lib.measurement import (
            SpectralAnalysisDetail,
            existing_spectral_resolver_for_config,
            measure_preimport_state,
        )

        acquisition = f"a6cd62c4-da2a-4a89-a219-adba66{floor:06x}"
        survivor = f"b6cd62c4-da2a-4a89-a219-adba66{floor:06x}"
        with tempfile.TemporaryDirectory() as root:
            candidate = os.path.join(root, "candidate")
            existing = os.path.join(root, "existing")
            os.mkdir(candidate)
            os.mkdir(existing)
            with open(os.path.join(candidate, "01.flac"), "wb") as handle:
                handle.write(b"candidate")
            with open(os.path.join(existing, "01.mp3"), "wb") as handle:
                handle.write(b"existing")
            beets = FakeBeetsDB()
            beets.set_album_info(survivor, AlbumInfo(
                album_id=7,
                track_count=1,
                min_bitrate_kbps=floor,
                avg_bitrate_kbps=floor,
                median_bitrate_kbps=floor,
                is_cbr=True,
                album_path=existing,
                format="MP3",
            ))
            request = make_request_row(
                id=7,
                mb_release_id=acquisition,
                canonical_release_id=survivor,
            )
            seen: list[str] = []

            def analyze(path: str) -> SpectralAnalysisDetail:
                seen.append(path)
                return SpectralAnalysisDetail(
                    attempted=True,
                    grade="genuine",
                    bitrate_kbps=floor,
                )

            with patch("lib.beets_db.BeetsDB", lambda **_kwargs: beets):
                measurement = measure_preimport_state(
                    path=candidate,
                    mb_release_id=acquisition,
                    label="request keyed HAVE",
                    download_filetype="flac",
                    download_min_bitrate_bps=900_000,
                    download_is_vbr=False,
                    cfg=CratediggerConfig(audio_check_mode="off"),
                    spectral_detail_analyzer=analyze,
                    existing_spectral_resolver=(
                        existing_spectral_resolver_for_config(
                            CratediggerConfig(audio_check_mode="off"),
                            request=request,
                        )
                    ),
                )

        assert_request_keyed_have_path(
            observed_path=measurement.existing_spectral_path,
            expected_path=existing,
        )
        self.assertIn(existing, seen)

    def test_checker_rejects_acquisition_only_have_mutant(self) -> None:
        with self.assertRaisesRegex(AssertionError, "not inspected"):
            assert_request_keyed_have_path(
                observed_path=None,
                expected_path="/canonical-held-have",
            )

    @given(candidate_kbps=st.sampled_from((160, 192, 224)))
    @example(candidate_kbps=192)
    def test_preview_adapters_forward_request_aware_have_resolver(
        self, candidate_kbps: int,
    ) -> None:
        """Both outer preview adapters measure the survivor-held HAVE path."""
        from lib.beets_db import AlbumInfo
        from lib.config import CratediggerConfig
        from lib.import_preview import (
            measure_and_persist_candidate_evidence,
            preview_import_from_path,
        )
        from lib.measurement import LocalFileInspection, PreimportMeasurement
        from lib.quality import (
            AudioQualityMeasurement,
            SpectralAnalysisDetail,
            SpectralDetail,
            full_pipeline_decision_from_evidence,
        )
        from lib.quality_evidence import evidence_from_measurement
        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION

        acquisition = "a7cd62c4-da2a-4a89-a219-adba66d6c704"
        survivor = "b7cd62c4-da2a-4a89-a219-bdba66d6c704"
        with tempfile.TemporaryDirectory() as root:
            downloads = os.path.join(root, "downloads")
            processing = os.path.join(root, "processing")
            os.mkdir(downloads)
            os.mkdir(processing, 0o700)
            os.mkdir(os.path.join(processing, "preview"), 0o700)
            source = os.path.join(downloads, "candidate")
            existing = os.path.join(root, "existing")
            os.mkdir(source)
            os.mkdir(existing)
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"candidate")
            with open(os.path.join(existing, "01.mp3"), "wb") as handle:
                handle.write(b"existing")

            cfg = CratediggerConfig(
                slskd_download_dir=downloads,
                processing_dir=processing,
                audio_check_mode="off",
            )
            beets = FakeBeetsDB()
            beets.set_album_info(survivor, AlbumInfo(
                album_id=7,
                track_count=1,
                min_bitrate_kbps=287,
                avg_bitrate_kbps=287,
                median_bitrate_kbps=287,
                is_cbr=True,
                album_path=existing,
                format="MP3",
            ))
            observed: list[str | None] = []
            def measure(**kwargs):
                lookup = kwargs["existing_spectral_resolver"](
                    kwargs["mb_release_id"],
                )
                observed.append(lookup.path)
                return PreimportMeasurement(
                    audio_corrupt=True,
                    corrupt_files=["01.mp3"],
                    audio_validation=make_audio_corrupt_validation_report(
                        "01.mp3",
                    ),
                    folder_layout="flat",
                    audio_file_count=1,
                    existing_spectral_path=lookup.path,
                    spectral_audit=SpectralDetail(
                        existing=SpectralAnalysisDetail(
                            attempted=True,
                            grade="likely_transcode",
                            bitrate_kbps=96,
                            spectral_measurement_version=(
                                SPECTRAL_MEASUREMENT_VERSION
                            ),
                        ),
                    ),
                )

            def persist_measurement(*_args, **kwargs):
                result = evidence_from_measurement(
                    mb_release_id=kwargs["mb_release_id"],
                    source_path=kwargs["source_path"],
                    measurement=kwargs["measurement"],
                    files=kwargs["files"],
                )
                return result

            candidate = make_album_quality_evidence(
                mb_release_id=acquisition,
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=candidate_kbps,
                    avg_bitrate_kbps=candidate_kbps,
                    median_bitrate_kbps=candidate_kbps,
                    format="MP3",
                    is_cbr=True,
                    spectral_grade="genuine",
                    spectral_bitrate_kbps=candidate_kbps,
                ),
            )
            blind_current = make_album_quality_evidence(
                mb_release_id=survivor,
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=320,
                    avg_bitrate_kbps=320,
                    median_bitrate_kbps=320,
                    format="MP3",
                    is_cbr=True,
                ),
            )
            blind_decision = full_pipeline_decision_from_evidence(
                candidate, blind_current,
            )

            def adapter_decision(adapter: str):
                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=7,
                    mb_release_id=acquisition,
                    canonical_release_id=survivor,
                ))
                with patch("lib.beets_db.BeetsDB", lambda **_kwargs: beets), patch(
                    "lib.import_preview.inspect_local_files",
                    return_value=LocalFileInspection(filetype="mp3"),
                ), patch("lib.import_preview.measure_preimport_state", side_effect=measure):
                    if adapter == "persist":
                        result = measure_and_persist_candidate_evidence(
                            db,
                            request_id=7,
                            path=source,
                            runtime_config=cfg,
                            persist_measurement_fn=persist_measurement,
                        )
                        self.assertEqual(result.verdict, "evidence_ready", result)
                    else:
                        with patch(
                            "lib.config.read_runtime_config", return_value=cfg,
                        ):
                            result = preview_import_from_path(
                                db,
                                request_id=7,
                                path=source,
                            )
                        self.assertEqual(result.verdict, "confident_reject")
                current_id = db.get_request_current_evidence_id(7)
                refreshed_current = db.load_album_quality_evidence_by_id(current_id)
                assert refreshed_current is not None
                self.assertEqual(
                    refreshed_current.measurement.spectral_grade,
                    "likely_transcode",
                )
                self.assertEqual(
                    refreshed_current.measurement.spectral_bitrate_kbps,
                    96,
                )
                return full_pipeline_decision_from_evidence(
                    candidate, refreshed_current,
                )

            refreshed_decisions = [
                adapter_decision("persist"),
                adapter_decision("preview"),
            ]

        self.assertEqual(observed, [existing, existing])
        self.assertFalse(blind_decision["imported"])
        self.assertEqual(blind_decision["stage2_import"], "downgrade")
        for refreshed_decision in refreshed_decisions:
            self.assertTrue(refreshed_decision["imported"])
            self.assertEqual(
                refreshed_decision["stage3_quality_gate"], "requeue_upgrade",
            )


class TestRequestKeyedRequeueGenerated(_FakeDbWebServerCase):
    """Exercise the HTTP adapter, not the resolver or route helper alone."""

    @given(
        floor=st.integers(min_value=64, max_value=1411),
        topology=st.sampled_from(("unique", "missing", "ambiguous", "omitted")),
    )
    @example(floor=128, topology="unique")
    @example(floor=128, topology="missing")
    @example(floor=128, topology="ambiguous")
    @example(floor=128, topology="omitted")
    def test_requeue_retention_requires_unique_request_union(
        self, floor: int, topology: str,
    ) -> None:
        import web.server as srv

        acquisition = f"c6cd62c4-da2a-4a89-a219-adba66{floor:06x}"
        survivor = f"d6cd62c4-da2a-4a89-a219-adba66{floor:06x}"
        request_id = 30_000 + floor
        beets = _OmittingCurrentBeets() if topology == "omitted" else FakeBeetsDB()
        if topology == "unique":
            beets.set_tracks_for_release(survivor, [
                {"bitrate": floor * 1_000},
                {"bitrate": (floor + 30) * 1_000},
                {"bitrate": (floor + 60) * 1_000},
            ])
        elif topology == "ambiguous":
            beets.set_min_bitrate(acquisition, floor)
            beets.set_min_bitrate(survivor, floor + 1)
        previous = srv._beets
        srv._beets = beets
        try:
            self.db.seed_request(make_request_row(
                id=request_id,
                status="imported",
                mb_release_id=acquisition,
                canonical_release_id=survivor,
                min_bitrate=None,
                search_filetype_override="lossless",
            ))
            status, _payload = self._post("/api/pipeline/update", {
                "id": request_id, "status": "wanted",
            })
        finally:
            srv._beets = previous

        self.assertEqual(status, 200)
        assert_survivor_requeue_floor(
            persisted_floor=self.db.request(request_id)["min_bitrate"],
            persisted_override=(
                self.db.request(request_id)["search_filetype_override"]
            ),
            expected_floor=floor if topology == "unique" else None,
            expected_override="lossless" if topology == "unique" else None,
            rejected_average=floor + 30 if topology == "unique" else None,
        )

    def test_checker_rejects_acquisition_only_mutant(self) -> None:
        """Known-bad old behaviour (no survivor lookup) cannot satisfy it."""
        with self.assertRaisesRegex(AssertionError, "not persisted"):
            assert_survivor_requeue_floor(
                persisted_floor=None,
                persisted_override=None,
                expected_floor=287,
                expected_override="lossless",
            )

    def test_checker_rejects_average_for_minimum_mutant(self) -> None:
        """A survivor's average bitrate cannot stand in for its floor."""
        with self.assertRaisesRegex(AssertionError, "not persisted"):
            assert_survivor_requeue_floor(
                persisted_floor=320,
                persisted_override="lossless",
                expected_floor=287,
                expected_override="lossless",
                rejected_average=320,
            )


class TestRequestKeyedUpgradeGenerated(_FakeDbWebServerCase):
    """Upgrade is an existing-row quality mutation, not an exact lookup."""

    @given(
        floor=st.integers(min_value=64, max_value=1411),
        topology=st.sampled_from(("unique", "missing", "ambiguous", "omitted")),
        lifecycle=st.sampled_from(("imported", "initializing")),
    )
    @example(floor=287, topology="unique", lifecycle="imported")
    @example(floor=287, topology="unique", lifecycle="initializing")
    @example(floor=287, topology="missing", lifecycle="initializing")
    @example(floor=287, topology="ambiguous", lifecycle="initializing")
    @example(floor=287, topology="omitted", lifecycle="initializing")
    def test_existing_upgrade_uses_only_the_request_union_minimum(
        self, floor: int, topology: str, lifecycle: str,
    ) -> None:
        import web.server as srv

        acquisition = f"16cd62c4-da2a-4a89-a219-adba66{floor:06x}"
        survivor = f"26cd62c4-da2a-4a89-a219-adba66{floor:06x}"
        request_id = 50_000 + floor
        beets = _OmittingCurrentBeets() if topology == "omitted" else FakeBeetsDB()
        if topology == "unique":
            beets.set_tracks_for_release(survivor, [
                {"bitrate": floor * 1_000},
                {"bitrate": (floor + 20) * 1_000},
                {"bitrate": (floor + 70) * 1_000},
            ])
        elif topology == "ambiguous":
            beets.set_min_bitrate(acquisition, floor)
            beets.set_min_bitrate(survivor, floor + 1)
        previous = srv._beets
        srv._beets = beets
        try:
            self.db.seed_request(make_request_row(
                id=request_id,
                status=lifecycle,
                mb_release_id=acquisition,
                canonical_release_id=survivor,
                min_bitrate=floor + 200,
                search_filetype_override="lossless",
            ))
            with (
                patch(
                    "web.routes.pipeline_mutations.mb_api.get_release",
                    return_value={
                        "artist_name": "Resume", "title": "Upgrade", "tracks": [],
                    },
                ),
                patch(
                    "web.routes.pipeline_mutations.mb_api.get_release_raw",
                    return_value={},
                ),
            ):
                status, payload = self._post("/api/pipeline/upgrade", {
                    "mb_release_id": acquisition,
                })
        finally:
            srv._beets = previous

        self.assertEqual(status, 200)
        self.assertEqual(payload["min_bitrate"], floor if topology == "unique" else None)
        self.assertEqual(
            self.db.request(request_id)["min_bitrate"],
            floor if topology == "unique" else None,
        )
        self.assertEqual(self.db.request(request_id)["status"], "wanted")
        self.assertEqual(
            beets.get_min_bitrate_calls,
            [],
            "an existing row must never fall back to an exact-ID minimum",
        )


class TestRequestKeyedQualityRouteGenerated(_FakeDbWebServerCase):
    """Manual imported state admits only an unambiguous request union."""

    @given(
        floor=st.integers(min_value=96, max_value=1411),
        topology=st.sampled_from(("unique", "missing", "ambiguous", "omitted")),
    )
    @example(floor=287, topology="unique")
    @example(floor=287, topology="missing")
    @example(floor=287, topology="ambiguous")
    @example(floor=287, topology="omitted")
    def test_imported_quality_floor_requires_unique_request_current(
        self, floor: int, topology: str,
    ) -> None:
        import web.server as srv

        acquisition = f"e6cd62c4-da2a-4a89-a219-adba66{floor:06x}"
        survivor = f"f6cd62c4-da2a-4a89-a219-adba66{floor:06x}"
        beets = _OmittingCurrentBeets() if topology == "omitted" else FakeBeetsDB()
        if topology == "unique":
            beets.set_tracks_for_release(survivor, [
                {"bitrate": (floor - 1) * 1_000},
                {"bitrate": floor * 1_000},
                {"bitrate": (floor + 1) * 1_000},
            ])
        elif topology == "ambiguous":
            beets.set_min_bitrate(acquisition, floor)
            beets.set_min_bitrate(survivor, floor + 1)
        request_id = 40_000 + floor
        previous = srv._beets
        srv._beets = beets
        try:
            self.db.seed_request(make_request_row(
                id=request_id,
                status="wanted",
                mb_release_id=acquisition,
                canonical_release_id=survivor,
                min_bitrate=None,
            ))
            status, _payload = self._post("/api/pipeline/set-quality", {
                "mb_release_id": acquisition, "status": "imported",
            })
        finally:
            srv._beets = previous

        self.assertEqual(status, 200)
        self.assertEqual(
            self.db.request(request_id)["min_bitrate"],
            floor if topology == "unique" else None,
        )
