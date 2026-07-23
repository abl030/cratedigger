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
from tests.fakes import FakePipelineDB


def assert_published_request_complete(db: FakePipelineDB, request_id: int) -> None:
    """The publication invariant, callable so known-bad controls prove it."""
    row = db.request(request_id)
    if row["status"] != "wanted":
        return
    if not db.get_tracks(request_id):
        raise AssertionError("wanted request was published before tracks")
    fields = {name for (rid, name) in db.field_resolutions if rid == request_id}
    required = {"release_group_year", "release_group_id", "catalog_number", "track_artist"}
    if fields != required:
        raise AssertionError("wanted request was published before field resolution")
    if db.get_active_search_plan(request_id) is None:
        raise AssertionError("wanted request was published before a durable plan")
    if row.get("search_filetype_override") != "upgrade" or row.get("min_bitrate") != 320:
        raise AssertionError("wanted upgrade request lost its publication policy")


class _FaultDB(FakePipelineDB):
    def __init__(self, phase: str | None) -> None:
        super().__init__()
        self.phase = phase

    def set_tracks(self, request_id: int, tracks: list[dict[str, object]]) -> None:
        if self.phase == "tracks":
            raise RuntimeError("injected tracks failure")
        super().set_tracks(request_id, tracks)

    def record_field_resolution(self, request_id: int, field_name: str, status: str,
                                reason_code: str | None) -> bool:
        if self.phase == "resolver":
            raise RuntimeError("injected resolver persistence failure")
        return super().record_field_resolution(request_id, field_name, status, reason_code)

    def create_successful_search_plan(self, **kwargs: object) -> int:
        if self.phase == "plan":
            raise RuntimeError("injected plan persistence failure")
        return super().create_successful_search_plan(**kwargs)  # type: ignore[arg-type]

    def create_failed_search_plan(self, **kwargs: object) -> int:
        if self.phase == "plan":
            raise RuntimeError("injected failed-plan persistence failure")
        return super().create_failed_search_plan(**kwargs)  # type: ignore[arg-type]

    def update_status(self, request_id: int, status: str, *, expected_status: str | None = None,
                      **extra: object) -> bool:
        if self.phase == "publish" and status == "wanted" and expected_status == "initializing":
            return False
        return super().update_status(request_id, status, expected_status=expected_status, **extra)


def _creation(release_id: str, *, discogs: bool, tracks: list[dict[str, object]]) -> RequestCreationInput:
    return RequestCreationInput(
        release_id=release_id, mb_release_id=release_id,
        discogs_release_id=release_id if discogs else None,
        artist_name="A", album_title="B", source="request", tracks=tracks,
        discogs_release_payload={"artist_id": "1", "artists": [], "tracklist": []}
        if discogs else None,
        mb_release_payload={"artist-credit": [], "media": []} if not discogs else None,
        final_fields={"search_filetype_override": "upgrade", "min_bitrate": 320},
    )


class TestRequestCreationGenerated(unittest.TestCase):
    @given(
        discogs=st.booleans(),
        phase=st.sampled_from([None, "tracks", "resolver", "plan", "publish"]),
        track_count=st.integers(min_value=1, max_value=3),
    )
    @example(discogs=True, phase="resolver", track_count=1)
    @example(discogs=False, phase="plan", track_count=1)
    def test_create_or_resume_only_publishes_complete_request(
        self, discogs: bool, phase: str | None, track_count: int,
    ) -> None:
        db = _FaultDB(phase)
        tracks = [{"disc_number": 1, "track_number": i + 1, "title": f"T{i}"}
                  for i in range(track_count)]
        creation = _creation(f"791-{discogs}-{phase}-{track_count}", discogs=discogs, tracks=tracks)
        service = RequestCreationService(db, CratediggerConfig())

        first = service.create_or_resume(creation)
        assert first.request_id is not None
        assert_published_request_complete(db, first.request_id)
        if phase is not None:
            self.assertEqual(db.request(first.request_id)["status"], "initializing")
            db.phase = None
            second = service.create_or_resume(creation)
            self.assertEqual(second.outcome, "resumed")
            # Re-read after retry: the stale first row must not prove recovery.
            assert_published_request_complete(db, second.request_id or first.request_id)

    def test_invariant_checker_rejects_known_bad_old_add_shape(self) -> None:
        db = FakePipelineDB()
        request_id = db.add_request(artist_name="A", album_title="B", source="request")
        with self.assertRaisesRegex(AssertionError, "published before tracks"):
            assert_published_request_complete(db, request_id)
