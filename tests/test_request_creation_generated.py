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


def assert_published_request_complete(
    db: FakePipelineDB, request_id: int, *, upgrade: bool,
) -> None:
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
    inspection = db.get_search_plan_inspection(request_id)
    if (
        inspection.active is None
        and inspection.latest_failed_deterministic is None
        and inspection.latest_failed_transient is None
    ):
        raise AssertionError("wanted request was published before a durable plan")
    if upgrade and (
        row.get("search_filetype_override") != "upgrade" or row.get("min_bitrate") != 320
    ):
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
        if self.phase == "audit_scalar" and field_name == "catalog_number":
            raise RuntimeError("injected resolver persistence failure")
        if self.phase == "audit_track_artist" and field_name == "track_artist":
            raise RuntimeError("injected resolver persistence failure")
        return super().record_field_resolution(request_id, field_name, status, reason_code)

    def update_request_fields(self, request_id: int, *, expected_status: str | None = None,
                              **fields: object) -> bool:
        if self.phase == "scalar" and expected_status == "initializing":
            return False
        return super().update_request_fields(
            request_id, expected_status=expected_status, **fields,
        )

    def update_track_artists(self, request_id: int, track_artists: list[str | None], *,
                             expected_status: str | None = None) -> bool:
        if self.phase == "track_artist":
            return False
        return super().update_track_artists(
            request_id, track_artists, expected_status=expected_status,
        )

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


def _creation(release_id: str, *, discogs: bool, tracks: list[dict[str, object]],
              upgrade: bool, resolved: bool) -> RequestCreationInput:
    mb_raw: dict[str, object] = {"artist-credit": [], "media": []}
    discogs_raw: dict[str, object] = {"artist_id": "1", "artists": [], "tracklist": []}
    if resolved:
        mb_raw = {
            "release-group": {"id": "rg-791", "first-release-date": "1991-01-01"},
            "label-info": [{"catalog-number": "CAT-791"}],
            "media": [{"tracks": [{"artist-credit": [{"name": "Artist"}]}]}],
        }
        discogs_raw = {
            "artist_id": "1", "artists": [{"name": "Artist"}],
            "tracklist": [{"title": "T0", "artists": [{"name": "Artist"}]}],
        }
    return RequestCreationInput(
        release_id=release_id, mb_release_id=release_id,
        discogs_release_id=release_id if discogs else None,
        artist_name="A", album_title="B", source="request", tracks=tracks,
        discogs_release_payload=discogs_raw if discogs else None,
        mb_release_payload=mb_raw if not discogs else None,
        final_fields=(
            {"search_filetype_override": "upgrade", "min_bitrate": 320}
            if upgrade else {}
        ),
    )


class TestRequestCreationGenerated(unittest.TestCase):
    @given(
        discogs=st.booleans(),
        upgrade=st.booleans(),
        resolved=st.booleans(),
        phase=st.sampled_from([
            None, "tracks", "audit_scalar", "audit_track_artist", "scalar",
            "track_artist", "plan", "publish",
        ]),
        track_count=st.integers(min_value=1, max_value=3),
    )
    @example(discogs=True, upgrade=True, resolved=True, phase="audit_track_artist", track_count=1)
    @example(discogs=False, upgrade=False, resolved=False, phase="plan", track_count=1)
    def test_create_or_resume_only_publishes_complete_request(
        self, discogs: bool, upgrade: bool, resolved: bool,
        phase: str | None, track_count: int,
    ) -> None:
        db = _FaultDB(phase)
        tracks = [{"disc_number": 1, "track_number": i + 1, "title": f"T{i}"}
                  for i in range(track_count)]
        creation = _creation(
            f"791-{discogs}-{upgrade}-{resolved}-{phase}-{track_count}",
            discogs=discogs, tracks=tracks, upgrade=upgrade, resolved=resolved,
        )
        service = RequestCreationService(db, CratediggerConfig())

        first = service.create_or_resume(creation)
        assert first.request_id is not None
        assert_published_request_complete(db, first.request_id, upgrade=upgrade)
        if phase is not None:
            self.assertEqual(db.request(first.request_id)["status"], "initializing")
            db.phase = None
            second = service.create_or_resume(creation)
            self.assertEqual(second.outcome, "resumed")
            # Re-read after retry: the stale first row must not prove recovery.
            assert_published_request_complete(
                db, second.request_id or first.request_id, upgrade=upgrade,
            )

    def test_invariant_checker_rejects_each_known_bad_completion_signal(self) -> None:
        for signal in ("tracks", "resolution", "plan", "policy"):
            with self.subTest(signal=signal):
                db = FakePipelineDB()
                result = RequestCreationService(db, CratediggerConfig()).create_or_resume(
                    _creation("known-bad", discogs=False,
                              tracks=[{"disc_number": 1, "track_number": 1, "title": "T"}],
                              upgrade=True, resolved=True),
                )
                assert result.request_id is not None
                request_id = result.request_id
                if signal == "tracks":
                    db._tracks[request_id] = []
                elif signal == "resolution":
                    del db.field_resolutions[(request_id, "track_artist")]
                elif signal == "plan":
                    db.search_plans.clear()
                else:
                    db.request(request_id)["min_bitrate"] = None
                with self.assertRaises(AssertionError):
                    assert_published_request_complete(db, request_id, upgrade=True)

    def test_completion_accepts_a_durable_failed_plan_outcome(self) -> None:
        """A persisted failure is audit-complete even though it is not active."""
        db = FakePipelineDB()
        result = RequestCreationService(db, CratediggerConfig()).create_or_resume(
            _creation("failed-plan", discogs=False,
                      tracks=[{"disc_number": 1, "track_number": 1, "title": "T"}],
                      upgrade=False, resolved=False),
        )
        assert result.request_id is not None
        request_id = result.request_id
        db.search_plans.clear()
        db.create_failed_search_plan(
            request_id=request_id, generator_id="test", failure_class="transient",
            transient=True,
        )

        assert_published_request_complete(db, request_id, upgrade=False)

    def test_known_bad_publication_fault_remains_provisional(self) -> None:
        db = _FaultDB("publish")
        result = RequestCreationService(db, CratediggerConfig()).create_or_resume(
            _creation("publish-fault", discogs=False,
                      tracks=[{"disc_number": 1, "track_number": 1, "title": "T"}],
                      upgrade=True, resolved=True),
        )

        assert result.request_id is not None
        self.assertEqual(db.request(result.request_id)["status"], "initializing")
