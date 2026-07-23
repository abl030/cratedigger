"""Generated patrol for issue #791 request publication."""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401
from hypothesis import example, given, strategies as st

from lib.config import CratediggerConfig
from lib.request_creation_service import RequestCreationInput, RequestCreationService
from tests.fakes import FakePipelineDB


def assert_published_request_complete(
    db: FakePipelineDB, request_id: int, *, upgrade: bool,
    discogs: bool = False, resolved: bool = False,
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
    if resolved:
        persisted_tracks = db.get_tracks(request_id)
        if discogs:
            # Direct Add does not pre-seed a Discogs master in the request;
            # the flattened browse payload can still resolve the release
            # group/master id and catalog, but deliberately has no year or
            # per-track artist evidence.
            if (
                row.get("mb_release_group_id") != "791"
                or row.get("catalog_number") != "DISC-791"
                or row.get("release_group_year") is not None
                or [track.get("track_artist") for track in persisted_tracks]
                != [None] * len(persisted_tracks)
            ):
                raise AssertionError("wanted Discogs request lost its resolved metadata")
            expected_statuses = {
                "release_group_year": "unresolved_malformed",
                "release_group_id": "resolved",
                "catalog_number": "resolved",
                "track_artist": "unresolved_field_missing_upstream",
            }
        else:
            if (
                row.get("mb_release_group_id") != "rg-791"
                or row.get("release_group_year") != 1991
                or row.get("catalog_number") != "CAT-791"
                or [track.get("track_artist") for track in persisted_tracks]
                != ["MB Artist"] * len(persisted_tracks)
            ):
                raise AssertionError("wanted request lost resolved MB metadata")
            expected_statuses = {field: "resolved" for field in required}
        if any(
            db.field_resolutions[(request_id, field)].status != expected_statuses[field]
            for field in required
        ):
            raise AssertionError("wanted request lost resolved field evidence")


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
        if self.phase in {"plan", "plan_transient"}:
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
              upgrade: bool, resolved: bool, artist_name: str = "A",
              album_title: str = "B") -> RequestCreationInput:
    mb_raw: dict[str, object] = {"artist-credit": [], "media": []}
    discogs_raw: dict[str, object] = {"artist_id": "1", "artists": [], "tracklist": []}
    payload_tracks = [
        {"title": str(track.get("title", ""))}
        for track in tracks
    ]
    if resolved:
        mb_raw = {
            # This is the direct MB release shape. The creation input below
            # derives its known rg id from this same nested release-group.
            "release-group": {"id": "rg-791", "first-release-date": "1991-01-01"},
            "release_group_id": "rg-791",
            "label-info": [{"catalog-number": "CAT-791"}],
            "media": [{"tracks": [
                {**track, "artist-credit": [{"name": "MB Artist"}]}
                for track in payload_tracks
            ]}],
        }
        discogs_raw = {
            # Flattened Discogs browse payload: it carries a master and
            # catalog but has no per-track artist credits.
            "artist_id": "1", "release_group_id": "791",
            "labels": [{"catno": "DISC-791"}],
            "tracks": payload_tracks,
        }
    # MB Add pre-seeds its known group id from the source payload. Discogs
    # deliberately leaves it to the resolver, matching the real adapter.
    mb_group = mb_raw.get("release-group")
    mb_group_id = (
        mb_group.get("id") if isinstance(mb_group, dict) else None
    )
    return RequestCreationInput(
        release_id=release_id, mb_release_id=release_id,
        discogs_release_id=release_id if discogs else None,
        mb_release_group_id=(
            str(mb_group_id) if not discogs and mb_group_id is not None
            else None
        ),
        artist_name=artist_name, album_title=album_title, source="request", tracks=tracks,
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
    @example(discogs=False, upgrade=True, resolved=True, phase=None, track_count=3)
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
        with (
            patch("web.mb.get_release_group_year", return_value=1991),
            patch("web.discogs.get_master_releases", return_value={"first_release_date": "1991"}),
        ):
            first = service.create_or_resume(creation)
        assert first.request_id is not None
        assert_published_request_complete(
            db, first.request_id, upgrade=upgrade, discogs=discogs, resolved=resolved,
        )
        if phase is not None:
            self.assertEqual(db.request(first.request_id)["status"], "initializing")
            db.phase = None
            with (
                patch("web.mb.get_release_group_year", return_value=1991),
                patch("web.discogs.get_master_releases", return_value={"first_release_date": "1991"}),
            ):
                second = service.create_or_resume(creation)
            self.assertEqual(second.outcome, "resumed")
            # Re-read after retry: the stale first row must not prove recovery.
            assert_published_request_complete(
                db, second.request_id or first.request_id, upgrade=upgrade,
                discogs=discogs, resolved=resolved,
            )

    def test_invariant_checker_rejects_each_known_bad_completion_signal(self) -> None:
        for signal in (
            "tracks", "resolution", "plan", "policy",
            "resolved_scalar", "resolved_release_group_id", "resolved_track_artist",
        ):
            with self.subTest(signal=signal):
                db = FakePipelineDB()
                with patch("web.mb.get_release_group_year", return_value=1991):
                    result = RequestCreationService(db, CratediggerConfig()).create_or_resume(
                        _creation("known-bad", discogs=False,
                                  tracks=[
                                      {"disc_number": 1, "track_number": 1, "title": "T1"},
                                      {"disc_number": 1, "track_number": 2, "title": "T2"},
                                  ],
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
                elif signal == "resolved_scalar":
                    # Audit still says resolved: only the persisted scalar is corrupt.
                    db.request(request_id)["catalog_number"] = None
                elif signal == "resolved_release_group_id":
                    # The scalar audit row remains resolved while its value is lost.
                    db.request(request_id)["mb_release_group_id"] = None
                elif signal == "resolved_track_artist":
                    # Keep resolved audit evidence, but corrupt the second
                    # generated artist so index-zero-only checks cannot pass.
                    self.assertEqual(len(db._tracks[request_id]), 2)
                    db._tracks[request_id][1]["track_artist"] = None
                else:
                    db.request(request_id)["min_bitrate"] = None
                with self.assertRaises(AssertionError):
                    assert_published_request_complete(
                        db, request_id, upgrade=True, resolved=True,
                    )

    def test_known_bad_publication_fault_remains_provisional(self) -> None:
        db = _FaultDB("publish")
        result = RequestCreationService(db, CratediggerConfig()).create_or_resume(
            _creation("publish-fault", discogs=False,
                      tracks=[{"disc_number": 1, "track_number": 1, "title": "T"}],
                      upgrade=True, resolved=True),
        )

        assert result.request_id is not None
        self.assertEqual(db.request(result.request_id)["status"], "initializing")

    def test_service_publishes_every_persisted_plan_outcome_only(self) -> None:
        """The service gates on plan_id, not on active-plan status."""
        normal = _creation(
            "plan-success", discogs=False,
            tracks=[{"disc_number": 1, "track_number": 1, "title": "T"}],
            upgrade=False, resolved=False,
        )
        deterministic = _creation(
            "plan-deterministic", discogs=False,
            tracks=[{"disc_number": 1, "track_number": 1, "title": ""}],
            upgrade=False, resolved=False, artist_name="", album_title="",
        )
        for label, db, creation, expected_status, expected_plan_status in (
            ("success", FakePipelineDB(), normal, "wanted", "active"),
            ("deterministic", FakePipelineDB(), deterministic, "wanted", "failed_deterministic"),
            ("transient", _FaultDB("plan_transient"), normal, "wanted", "failed_transient"),
            ("no_plan_id", _FaultDB("plan"), normal, "initializing", None),
        ):
            with self.subTest(outcome=label):
                result = RequestCreationService(db, CratediggerConfig()).create_or_resume(creation)
                assert result.request_id is not None
                request_id = result.request_id
                self.assertEqual(db.request(request_id)["status"], expected_status)
                persisted_statuses = [
                    row.status for row in db.search_plans.values()
                    if row.request_id == request_id
                ]
                if expected_plan_status is None:
                    self.assertEqual(persisted_statuses, [])
                else:
                    self.assertIn(expected_plan_status, persisted_statuses)
                    assert_published_request_complete(db, request_id, upgrade=False)
