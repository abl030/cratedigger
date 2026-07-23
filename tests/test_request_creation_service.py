"""Pins for issue #791's create-or-resume publication boundary."""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.config import CratediggerConfig
from lib.request_creation_service import (
    PlanServiceFactory,
    RequestCreationDB,
    RequestCreationInput,
    RequestCreationService,
)
from lib.search_plan_service import SearchPlanService, ServiceResult
from tests.fakes import FakePipelineDB


class _UnpersistedPlan:
    def __init__(self, _db: object, _cfg: object) -> None:
        pass

    def generate_for_new_request(
        self, request_id: int, *, artist_name: str, album_title: str,
        year: object, tracks: list[dict[str, object]], source: str = "request",
        prepend_artist: bool | None = None, release_group_year: object = None,
        is_va_compilation: bool = False, catalog_number: object = None,
    ) -> ServiceResult:
        return ServiceResult(outcome="failed_transient", plan_id=None)


class _CapturePlan:
    release_group_years: list[object] = []

    def __init__(self, db: RequestCreationDB, cfg: CratediggerConfig) -> None:
        self.delegate = SearchPlanService(db, cfg)

    def generate_for_new_request(
        self, request_id: int, *, artist_name: str, album_title: str,
        year: object, tracks: list[dict[str, object]], source: str = "request",
        prepend_artist: bool | None = None, release_group_year: object = None,
        is_va_compilation: bool = False, catalog_number: object = None,
    ) -> ServiceResult:
        self.release_group_years.append(release_group_year)
        return self.delegate.generate_for_new_request(
            request_id, artist_name=artist_name, album_title=album_title,
            year=year, tracks=tracks, source=source,
            prepend_artist=prepend_artist, release_group_year=release_group_year,
            is_va_compilation=is_va_compilation, catalog_number=catalog_number,
        )


class _FailTracksOnce(FakePipelineDB):
    def __init__(self) -> None:
        super().__init__()
        self.fail_once = True

    def set_tracks(self, request_id: int, tracks: list[dict[str, object]]) -> None:
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("injected tracks write failure")
        super().set_tracks(request_id, tracks)


class _FailPublishOnce(FakePipelineDB):
    def __init__(self) -> None:
        super().__init__()
        self.fail_publish_once = True

    def update_status(
        self, request_id: int, status: str, *, expected_status: str | None = None,
        **extra: object,
    ) -> bool:
        if status == "wanted" and expected_status == "initializing" and self.fail_publish_once:
            self.fail_publish_once = False
            return False
        return super().update_status(
            request_id, status, expected_status=expected_status, **extra,
        )


def _input(*, discogs: bool = False, release_group_year: int | None = None,
           release_id: str | None = None) -> RequestCreationInput:
    release_id = release_id or ("79101" if discogs else "791-mbid")
    return RequestCreationInput(
        release_id=release_id,
        mb_release_id=release_id,
        discogs_release_id=release_id if discogs else None,
        artist_name="Archivist",
        album_title="Initialization",
        release_group_year=release_group_year,
        source="request",
        tracks=[{"disc_number": 1, "track_number": 1, "title": "One"}],
        discogs_release_payload={"artist_id": "1", "artists": [], "tracklist": []}
        if discogs else None,
        mb_release_payload={"artist-credit": [], "media": []} if not discogs else None,
    )


class TestRequestCreationService(unittest.TestCase):
    def _service(self, db: FakePipelineDB,
                 plan: PlanServiceFactory = SearchPlanService) -> RequestCreationService:
        return RequestCreationService(
            db, CratediggerConfig(), plan_service_factory=plan,
        )

    def test_created_request_publishes_only_after_owned_writes(self) -> None:
        db = FakePipelineDB()
        result = self._service(db).create_or_resume(_input())
        self.assertEqual(result.outcome, "created")
        assert result.request_id is not None
        self.assertEqual(db.request(result.request_id)["status"], "wanted")
        self.assertEqual(len(db.get_tracks(result.request_id)), 1)
        self.assertIsNotNone(db.get_active_search_plan(result.request_id))
        self.assertEqual(
            {field for (rid, field) in db.field_resolutions if rid == result.request_id},
            {"release_group_year", "release_group_id", "catalog_number", "track_artist"},
        )

    def test_failure_stays_initializing_then_same_add_resumes(self) -> None:
        db = _FailTracksOnce()
        first = self._service(db).create_or_resume(_input(discogs=True))
        self.assertEqual(first.outcome, "initialization_failed")
        assert first.request_id is not None
        self.assertEqual(db.request(first.request_id)["status"], "initializing")
        second = self._service(db).create_or_resume(_input(discogs=True))
        self.assertEqual(second.outcome, "resumed")
        self.assertEqual(second.request_id, first.request_id)
        self.assertEqual(db.request(first.request_id)["status"], "wanted")

    def test_unpersisted_plan_never_publishes(self) -> None:
        db = FakePipelineDB()
        result = self._service(db, _UnpersistedPlan).create_or_resume(
            _input(release_id="791-existing-plan"),
        )
        self.assertEqual(result.outcome, "initialization_failed")
        assert result.request_id is not None
        self.assertEqual(db.request(result.request_id)["status"], "initializing")

    def test_no_plan_id_never_publishes_even_if_an_active_plan_exists(self) -> None:
        db = FakePipelineDB()
        request_id = db.add_request(
            artist_name="Archivist", album_title="Initialization", source="request",
            mb_release_id="791-existing-plan", status="initializing",
        )
        tracks = [{"disc_number": 1, "track_number": 1, "title": "One"}]
        db.set_tracks(request_id, tracks)
        persisted = SearchPlanService(db, CratediggerConfig()).generate_for_new_request(
            request_id, artist_name="Archivist", album_title="Initialization",
            year=None, tracks=tracks,
        )
        self.assertIsNotNone(persisted.plan_id)

        result = self._service(db, _UnpersistedPlan).create_or_resume(_input())

        self.assertEqual(result.outcome, "initialization_failed")
        self.assertEqual(db.request(request_id)["status"], "initializing")

    def test_resume_accepts_plan_persisted_before_a_lost_publish_cas(self) -> None:
        db = _FailPublishOnce()
        service = RequestCreationService(db, CratediggerConfig())
        first = service.create_or_resume(_input())
        self.assertEqual(first.outcome, "initialization_failed")
        assert first.request_id is not None
        self.assertIsNotNone(db.get_active_search_plan(first.request_id))
        second = service.create_or_resume(_input())
        self.assertEqual(second.outcome, "resumed")
        self.assertEqual(db.request(first.request_id)["status"], "wanted")

    def test_discogs_creation_does_not_fetch_musicbrainz(self) -> None:
        db = FakePipelineDB()
        with (
            patch("web.mb.get_release", side_effect=AssertionError("MusicBrainz fetch")),
            patch("web.mb.get_release_group_year", side_effect=AssertionError("MusicBrainz fetch")),
        ):
            result = self._service(db).create_or_resume(_input(discogs=True))

        self.assertEqual(result.outcome, "created")

    def test_new_row_upgrade_keeps_known_release_group_year_for_plan(self) -> None:
        db = FakePipelineDB()
        _CapturePlan.release_group_years = []
        result = self._service(db, _CapturePlan).create_or_resume(
            _input(release_group_year=1991),
        )

        self.assertEqual(result.outcome, "created")
        self.assertEqual(_CapturePlan.release_group_years, [1991])

    def test_release_lock_contention_is_retryable_and_writes_nothing(self) -> None:
        db = FakePipelineDB()
        db.set_advisory_lock_result(False)
        result = self._service(db).create_or_resume(_input())
        self.assertEqual(result.outcome, "busy")
        self.assertEqual(db.count_by_status(), {})
