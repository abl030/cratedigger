"""Generated patrol for issue #791 request publication."""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401
from hypothesis import example, given, strategies as st

from lib.config import CratediggerConfig
from lib.request_creation_service import RequestCreationInput, RequestCreationService
from lib.search_plan_service import ServiceResult
from tests.fakes import FakePipelineDB


def assert_published_request_complete(row: dict[str, object], tracks: list[dict[str, object]]) -> None:
    """The issue invariant, kept callable so its negative control is real."""
    if row["status"] == "wanted" and not tracks:
        raise AssertionError("wanted request was published before tracks")


class _Plan:
    def __init__(self, _db: object, _cfg: object) -> None:
        pass

    def generate_for_new_request(
        self, request_id: int, *, artist_name: str, album_title: str,
        year: object, tracks: list[dict[str, object]], source: str = "request",
        prepend_artist: bool | None = None, release_group_year: object = None,
        is_va_compilation: bool = False, catalog_number: object = None,
    ) -> ServiceResult:
        return ServiceResult(outcome="success", plan_id=1)


class _FailSetTracks(FakePipelineDB):
    def __init__(self, fail: bool) -> None:
        super().__init__()
        self.fail = fail

    def set_tracks(self, request_id: int, tracks: list[dict[str, object]]) -> None:
        if self.fail:
            raise RuntimeError("fault injection")
        super().set_tracks(request_id, tracks)


def _creation(release_id: str, *, discogs: bool, tracks: list[dict[str, object]]) -> RequestCreationInput:
    return RequestCreationInput(
        release_id=release_id,
        mb_release_id=release_id,
        discogs_release_id=release_id if discogs else None,
        artist_name="A", album_title="B", source="request", tracks=tracks,
        discogs_release_payload={"artist_id": "1", "artists": [], "tracklist": []}
        if discogs else None,
        mb_release_payload={"artist-credit": [], "media": []} if not discogs else None,
    )


class TestRequestCreationGenerated(unittest.TestCase):
    @given(
        discogs=st.booleans(),
        fail_tracks=st.booleans(),
        track_count=st.integers(min_value=1, max_value=3),
    )
    @example(discogs=True, fail_tracks=True, track_count=1)
    def test_create_or_resume_never_publishes_without_tracks(
        self, discogs: bool, fail_tracks: bool, track_count: int,
    ) -> None:
        db = _FailSetTracks(fail_tracks)
        tracks = [
            {"disc_number": 1, "track_number": i + 1, "title": f"T{i}"}
            for i in range(track_count)
        ]
        creation = _creation(f"791-{discogs}-{track_count}", discogs=discogs, tracks=tracks)
        service = RequestCreationService(db, CratediggerConfig(), plan_service_factory=_Plan)
        first = service.create_or_resume(creation)
        assert first.request_id is not None
        row = db.request(first.request_id)
        assert_published_request_complete(row, db.get_tracks(first.request_id))
        if fail_tracks:
            db.fail = False
            second = service.create_or_resume(creation)
            self.assertEqual(second.outcome, "resumed")
            assert_published_request_complete(row, db.get_tracks(first.request_id))

    def test_invariant_checker_rejects_known_bad_old_add_shape(self) -> None:
        with self.assertRaisesRegex(AssertionError, "published before tracks"):
            assert_published_request_complete({"status": "wanted"}, [])
