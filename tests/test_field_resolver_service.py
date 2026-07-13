"""Unit tests for lib/field_resolver_service.py.

Drives the resolver functions with ``FakePipelineDB`` and injectable
collaborators (no real HTTP). The integration slice in
``tests/test_integration_slices.py::TestFieldResolverSlice`` exercises
the resolvers through real-mirror response shapes.

Per ``.claude/rules/code-quality.md`` § "MOCKS: LEAF-SEAM ONLY",
``FakePipelineDB`` is used for the recorder; collaborator callables are
passed as kwargs (the DI seam pattern).
"""

from __future__ import annotations

import copy
import socket
import threading
import unittest
import urllib.error
from typing import Any
from urllib.error import URLError

from lib.field_resolver_service import (
    DISCOGS_VA_ARTIST_ID,
    FIELD_CATALOG_NUMBER,
    FIELD_RELEASE_GROUP_ID,
    FIELD_RELEASE_GROUP_YEAR,
    FIELD_TRACK_ARTIST,
    MB_VA_ARTIST_MBID,
    ResolveAllResult,
    ResolverResult,
    _looks_numeric,
    apply_resolve_all_result,
    detect_va_compilation,
    resolve_catalog_number,
    resolve_release_group_id,
    resolve_release_group_year,
    resolve_track_artists,
)
from tests.fakes import FakePipelineDB as _GuardedFakePipelineDB
from tests.helpers import make_request_row


class FakePipelineDB(_GuardedFakePipelineDB):
    """Recorder harness with the parent row resolvers receive in production."""

    def record_field_resolution(
        self,
        request_id: int,
        field_name: str,
        status: str,
        reason_code: str | None,
    ) -> bool:
        if self.get_request(request_id) is None:
            self.seed_request(make_request_row(
                id=request_id,
                status="wanted",
                mb_release_id=f"resolver-parent-{request_id}",
            ))
        return super().record_field_resolution(
            request_id,
            field_name,
            status,
            reason_code,
        )


def _request(**overrides: Any) -> dict[str, Any]:
    """Tiny request-row builder. Only the fields the resolvers read."""
    row: dict[str, Any] = {
        "id": 1,
        "mb_release_id": "release-mbid-0001",
        "mb_release_group_id": "rg-mbid-0001",
        "mb_artist_id": None,
        "discogs_release_id": None,
        "artist_name": "Test Artist",
        "album_title": "Test Album",
    }
    row.update(overrides)
    return row


# --------------------------------------------------------------------- #
# release_group_year
# --------------------------------------------------------------------- #


class TestResolveReleaseGroupYear(unittest.TestCase):
    def test_mb_uuid_happy_path_returns_resolved(self):
        db = FakePipelineDB()
        req = _request(id=42, mb_release_group_id="abc-uuid")

        result = resolve_release_group_year(
            req, db,
            mb_get_release_group_year=lambda rg_id: 1997,
        )

        self.assertEqual(result.status, "resolved")
        self.assertEqual(result.value, 1997)
        self.assertEqual(result.field_name, FIELD_RELEASE_GROUP_YEAR)
        row = db.get_field_resolution(42, FIELD_RELEASE_GROUP_YEAR)
        assert row is not None
        self.assertEqual(row["status"], "resolved")
        self.assertEqual(row["attempts"], 1)

    def test_discogs_numeric_happy_path_dispatches_to_discogs(self):
        db = FakePipelineDB()
        # rg_id stored as numeric Discogs master id (legacy row shape).
        req = _request(id=43, mb_release_group_id="12345",
                       discogs_release_id="555", mb_release_id=None)

        result = resolve_release_group_year(
            req, db,
            mb_get_release_group_year=lambda rg_id: 9999,  # must NOT be called
            discogs_get_master_year=lambda mid: 2003,
        )

        self.assertEqual(result.status, "resolved")
        self.assertEqual(result.value, 2003)

    def test_mb_404_records_unresolved_404(self):
        db = FakePipelineDB()
        req = _request(id=44, mb_release_group_id="abc-uuid")

        def _raise(_: str) -> int | None:
            raise urllib.error.HTTPError(
                url="x", code=404, msg="Not Found", hdrs=None,  # type: ignore[arg-type]
                fp=None,
            )

        result = resolve_release_group_year(
            req, db, mb_get_release_group_year=_raise,
        )
        self.assertEqual(result.status, "unresolved_404")
        self.assertIsNone(result.value)
        row = db.get_field_resolution(44, FIELD_RELEASE_GROUP_YEAR)
        assert row is not None
        self.assertEqual(row["status"], "unresolved_404")

    def test_mb_4xx_other_than_404_records_sticky_4xx_client(self):
        """Fix #3: HTTP 4xx responses from the mirror (other than 404
        and 408) are permanent client-error semantics — retrying the
        same request gives the same answer. Classify as sticky
        ``unresolved_4xx_client`` instead of the 1d-retry
        ``unresolved_mirror_unavailable``.

        Live evidence from the 2026-05-25 deploy backfill: 74 wanted
        rows hit MB ``HTTP 400 Bad Request`` (likely deprecated /
        malformed-stored MBIDs) and got marked transient — they'd
        retry forever instead of being surfaced for operator
        attention.
        """
        # Subtests for the codes seen in the deploy backfill + a few
        # canonical 4xx (410 Gone, 422 Unprocessable Entity).
        for code, expected_reason in [
            (400, "http_400"),
            (410, "http_410"),
            (422, "http_422"),
        ]:
            with self.subTest(http_code=code):
                db = FakePipelineDB()
                req = _request(id=44, mb_release_group_id="abc-uuid")

                def _raise(_: str, c: int = code) -> int | None:
                    raise urllib.error.HTTPError(
                        url="x", code=c, msg="Client Error",
                        hdrs=None,  # type: ignore[arg-type]
                        fp=None,
                    )

                result = resolve_release_group_year(
                    req, db, mb_get_release_group_year=_raise,
                )
                self.assertEqual(result.status, "unresolved_4xx_client")
                self.assertEqual(result.reason_code, expected_reason)
                self.assertIsNone(result.value)
                row = db.get_field_resolution(44, FIELD_RELEASE_GROUP_YEAR)
                assert row is not None
                self.assertEqual(row["status"], "unresolved_4xx_client")

    def test_mb_5xx_stays_mirror_unavailable(self):
        """5xx is server-side — retry is legitimate. Stays in the 1d
        retry bucket (``unresolved_mirror_unavailable``)."""
        for code in [500, 502, 503]:
            with self.subTest(http_code=code):
                db = FakePipelineDB()
                req = _request(id=44, mb_release_group_id="abc-uuid")

                def _raise(_: str, c: int = code) -> int | None:
                    raise urllib.error.HTTPError(
                        url="x", code=c, msg="Server Error",
                        hdrs=None,  # type: ignore[arg-type]
                        fp=None,
                    )

                result = resolve_release_group_year(
                    req, db, mb_get_release_group_year=_raise,
                )
                self.assertEqual(
                    result.status, "unresolved_mirror_unavailable")

    def test_mirror_network_error_records_unresolved_mirror_unavailable(self):
        db = FakePipelineDB()
        req = _request(id=45, mb_release_group_id="abc-uuid")

        def _raise(_: str) -> int | None:
            raise URLError("connection refused")

        result = resolve_release_group_year(
            req, db, mb_get_release_group_year=_raise,
        )
        self.assertEqual(result.status, "unresolved_mirror_unavailable")
        self.assertIsNone(result.value)

    def test_timeout_records_unresolved_timeout(self):
        db = FakePipelineDB()
        req = _request(id=46, mb_release_group_id="abc-uuid")

        def _raise(_: str) -> int | None:
            raise socket.timeout("read timed out")

        result = resolve_release_group_year(
            req, db, mb_get_release_group_year=_raise,
        )
        self.assertEqual(result.status, "unresolved_timeout")

    def test_url_error_wrapping_timeout_classified_as_timeout(self):
        """URLError(reason=socket.timeout) is a timeout, not a generic error."""
        db = FakePipelineDB()
        req = _request(id=47, mb_release_group_id="abc-uuid")

        def _raise(_: str) -> int | None:
            raise URLError(socket.timeout("read timed out"))

        result = resolve_release_group_year(
            req, db, mb_get_release_group_year=_raise,
        )
        self.assertEqual(result.status, "unresolved_timeout")

    def test_discogs_master_with_no_year_records_field_missing_upstream(self):
        db = FakePipelineDB()
        req = _request(id=48, mb_release_group_id="12345",
                       discogs_release_id="555", mb_release_id=None)

        result = resolve_release_group_year(
            req, db, discogs_get_master_year=lambda _: None,
        )
        self.assertEqual(result.status, "unresolved_field_missing_upstream")
        self.assertIsNone(result.value)

    def test_mb_release_group_with_no_parseable_year_is_field_missing(self):
        """Code-review #17: ``web.mb.get_release_group_year`` now
        propagates ``HTTPError(404)`` so the resolver can disambiguate
        "MBID does not exist" (→ ``unresolved_404``) from "exists but
        no parseable year" (→ ``unresolved_field_missing_upstream``).
        This test pins the non-404 None branch."""
        db = FakePipelineDB()
        req = _request(id=51, mb_release_group_id="abc-uuid")

        result = resolve_release_group_year(
            req, db, mb_get_release_group_year=lambda _: None,
        )
        self.assertEqual(
            result.status, "unresolved_field_missing_upstream",
        )
        self.assertEqual(result.reason_code, "mb_release_group_no_year")
        row = db.get_field_resolution(51, FIELD_RELEASE_GROUP_YEAR)
        assert row is not None
        self.assertEqual(
            row["status"], "unresolved_field_missing_upstream",
        )
        self.assertEqual(row["reason_code"], "mb_release_group_no_year")

    def test_empty_rg_id_records_unresolved_malformed_and_no_mirror_call(self):
        db = FakePipelineDB()
        req = _request(id=49, mb_release_group_id=None,
                       discogs_release_id=None)

        calls: list[str] = []

        def _track(rg_id: str) -> int | None:
            calls.append(rg_id)
            return 2000

        result = resolve_release_group_year(
            req, db, mb_get_release_group_year=_track,
        )
        self.assertEqual(result.status, "unresolved_malformed")
        self.assertEqual(calls, [], "no mirror call should be attempted")

    def test_re_resolution_increments_attempts_and_updates_status(self):
        """Calling twice for the same (request, field) does not create a duplicate."""
        db = FakePipelineDB()
        req = _request(id=50, mb_release_group_id="abc-uuid")

        # First call: fail.
        def _fail(_: str) -> int | None:
            raise URLError("connection refused")
        resolve_release_group_year(req, db, mb_get_release_group_year=_fail)

        # Second call: succeed.
        resolve_release_group_year(
            req, db, mb_get_release_group_year=lambda _: 2010,
        )

        row = db.get_field_resolution(50, FIELD_RELEASE_GROUP_YEAR)
        assert row is not None
        self.assertEqual(row["status"], "resolved")
        self.assertEqual(row["attempts"], 2)
        # Only one row -- not duplicated.
        self.assertEqual(len(db.field_resolutions), 1)


# --------------------------------------------------------------------- #
# release_group_id
# --------------------------------------------------------------------- #


class TestResolveReleaseGroupId(unittest.TestCase):
    def test_mb_happy_path(self):
        db = FakePipelineDB()
        req = _request(id=101, mb_release_id="release-uuid")

        result = resolve_release_group_id(
            req, db,
            mb_get_release=lambda mbid, fresh=False: {
                "release_group_id": "rg-uuid-xyz",
            },
        )
        self.assertEqual(result.status, "resolved")
        self.assertEqual(result.value, "rg-uuid-xyz")
        row = db.get_field_resolution(101, FIELD_RELEASE_GROUP_ID)
        assert row is not None
        self.assertEqual(row["status"], "resolved")

    def test_discogs_happy_path(self):
        db = FakePipelineDB()
        req = _request(id=102, mb_release_id=None, discogs_release_id="555")

        result = resolve_release_group_id(
            req, db,
            discogs_get_release=lambda rid, fresh=False: {
                "release_group_id": "777",
            },
        )
        self.assertEqual(result.status, "resolved")
        self.assertEqual(result.value, "777")

    def test_release_with_no_release_group_records_field_missing_upstream(self):
        db = FakePipelineDB()
        req = _request(id=103, mb_release_id="release-uuid")

        result = resolve_release_group_id(
            req, db,
            mb_get_release=lambda mbid, fresh=False: {
                "release_group_id": None,
            },
        )
        self.assertEqual(result.status, "unresolved_field_missing_upstream")
        self.assertIsNone(result.value)

    def test_missing_release_id_records_malformed(self):
        db = FakePipelineDB()
        req = _request(id=104, mb_release_id=None, discogs_release_id=None)

        result = resolve_release_group_id(req, db)
        self.assertEqual(result.status, "unresolved_malformed")


# --------------------------------------------------------------------- #
# track_artists
# --------------------------------------------------------------------- #


class TestResolveTrackArtists(unittest.TestCase):
    def test_mb_per_track_artist_credits(self):
        db = FakePipelineDB()
        req = _request(id=201, mb_release_id="release-uuid")

        mb_payload = {
            "media": [
                {
                    "tracks": [
                        {
                            "title": "Track A",
                            "artist-credit": [
                                {"name": "Artist X", "joinphrase": ""},
                            ],
                        },
                        {
                            "title": "Track B",
                            "artist-credit": [
                                {"name": "Artist Y", "joinphrase": " & "},
                                {"name": "Artist Z", "joinphrase": ""},
                            ],
                        },
                    ],
                },
            ],
        }

        results = resolve_track_artists(
            req, db,
            mb_get_release=lambda mbid, fresh=False: mb_payload,
        )

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].status, "resolved")
        self.assertEqual(results[0].value, "Artist X")
        self.assertEqual(results[1].status, "resolved")
        self.assertEqual(results[1].value, "Artist Y & Artist Z")
        # Summary row records "resolved" in the side table.
        row = db.get_field_resolution(201, FIELD_TRACK_ARTIST)
        assert row is not None
        self.assertEqual(row["status"], "resolved")

    def test_discogs_per_track_artists(self):
        db = FakePipelineDB()
        req = _request(id=202, mb_release_id=None,
                       discogs_release_id="555")
        discogs_payload = {
            "tracks": [
                {"title": "Track A",
                 "artists": [{"id": 100, "name": "Artist X"}]},
                {"title": "Track B",
                 "artists": [
                     {"id": 101, "name": "Artist Y", "join": "&"},
                     {"id": 102, "name": "Artist Z"},
                 ]},
            ],
        }

        results = resolve_track_artists(
            req, db,
            discogs_get_release=lambda rid, fresh=False: discogs_payload,
        )

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].status, "resolved")
        self.assertEqual(results[0].value, "Artist X")
        self.assertEqual(results[1].status, "resolved")
        self.assertIn("Artist Y", str(results[1].value))
        self.assertIn("Artist Z", str(results[1].value))

    def test_partial_resolution_summary_records_resolved(self):
        """If at least one track resolves, the summary is "resolved"."""
        db = FakePipelineDB()
        req = _request(id=203, mb_release_id="release-uuid")
        mb_payload = {
            "media": [
                {
                    "tracks": [
                        {"title": "Track A",
                         "artist-credit": [{"name": "Artist X"}]},
                        # No artist-credit on track 2 -- partial.
                        {"title": "Track B", "artist-credit": []},
                    ],
                },
            ],
        }
        results = resolve_track_artists(
            req, db, mb_get_release=lambda mbid, fresh=False: mb_payload,
        )
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].status, "resolved")
        self.assertEqual(results[1].status,
                         "unresolved_field_missing_upstream")
        row = db.get_field_resolution(203, FIELD_TRACK_ARTIST)
        assert row is not None
        self.assertEqual(row["status"], "resolved")

    def test_mirror_404_propagates_to_summary(self):
        db = FakePipelineDB()
        req = _request(id=204, mb_release_id="release-uuid")

        def _raise(mbid: str, fresh: bool = False) -> dict[str, Any]:
            raise urllib.error.HTTPError(
                url="x", code=404, msg="Not Found",
                hdrs=None, fp=None,  # type: ignore[arg-type]
            )

        results = resolve_track_artists(req, db, mb_get_release=_raise)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, "unresolved_404")
        row = db.get_field_resolution(204, FIELD_TRACK_ARTIST)
        assert row is not None
        self.assertEqual(row["status"], "unresolved_404")

    def test_malformed_request_records_malformed(self):
        db = FakePipelineDB()
        req = _request(id=205, mb_release_id=None, discogs_release_id=None)
        results = resolve_track_artists(req, db)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, "unresolved_malformed")


# --------------------------------------------------------------------- #
# catalog_number
# --------------------------------------------------------------------- #


class TestResolveCatalogNumber(unittest.TestCase):
    def test_mb_first_label_catalog_number(self):
        db = FakePipelineDB()
        req = _request(id=301, mb_release_id="release-uuid")

        mb_payload = {
            "label-info": [
                {"catalog-number": "ABC-001", "label": {"name": "Label 1"}},
                {"catalog-number": "ABC-002", "label": {"name": "Label 2"}},
            ],
        }
        result = resolve_catalog_number(
            req, db, mb_get_release=lambda mbid, fresh=False: mb_payload,
        )
        self.assertEqual(result.status, "resolved")
        self.assertEqual(result.value, "ABC-001")

    def test_discogs_first_catno(self):
        db = FakePipelineDB()
        req = _request(id=302, mb_release_id=None,
                       discogs_release_id="555")
        discogs_payload = {
            "labels": [{"catno": "XYZ-001", "name": "Label"}],
        }
        result = resolve_catalog_number(
            req, db,
            discogs_get_release=lambda rid, fresh=False: discogs_payload,
        )
        self.assertEqual(result.status, "resolved")
        self.assertEqual(result.value, "XYZ-001")

    def test_no_labels_records_field_missing_upstream(self):
        db = FakePipelineDB()
        req = _request(id=303, mb_release_id="release-uuid")
        result = resolve_catalog_number(
            req, db, mb_get_release=lambda mbid, fresh=False: {},
        )
        self.assertEqual(result.status, "unresolved_field_missing_upstream")

    def test_mirror_timeout_records_timeout(self):
        db = FakePipelineDB()
        req = _request(id=304, mb_release_id="release-uuid")

        def _raise(mbid: str, fresh: bool = False) -> dict[str, Any]:
            raise TimeoutError("read timeout")

        result = resolve_catalog_number(req, db, mb_get_release=_raise)
        self.assertEqual(result.status, "unresolved_timeout")


# --------------------------------------------------------------------- #
# VA detection
# --------------------------------------------------------------------- #


class TestDetectVaCompilation(unittest.TestCase):
    def test_rule1_canonical_mb_va_mbid_match(self):
        req = _request(mb_artist_id=MB_VA_ARTIST_MBID)
        self.assertTrue(detect_va_compilation(req))

    def test_rule1_canonical_discogs_va_id_match(self):
        req = _request(mb_release_id=None,
                       discogs_release_id="555",
                       mb_artist_id=None)
        discogs_payload = {
            "artists": [{"id": int(DISCOGS_VA_ARTIST_ID), "name": "Various"}],
        }
        self.assertTrue(detect_va_compilation(
            req, discogs_release_payload=discogs_payload,
        ))

    def test_rule1_canonical_discogs_va_id_match_real_payload_shape(self):
        """Real ``web/discogs.py::get_release`` shape carries ``artist_id``
        at the top level, not nested under ``artists``. The detector
        must read both — Rule 1 was broken for the live web/CLI add
        path before this guard (code-review finding #5)."""
        req = _request(mb_release_id=None,
                       discogs_release_id="555",
                       mb_artist_id=None)
        # Shape produced by web/discogs.py::get_release (see line 349-362).
        discogs_payload = {
            "id": "555",
            "title": "Some Compilation",
            "artist_name": "Various",
            "artist_id": DISCOGS_VA_ARTIST_ID,
            "release_group_id": None,
            "tracks": [],
            "labels": [],
        }
        self.assertTrue(detect_va_compilation(
            req, discogs_release_payload=discogs_payload,
        ))

    def test_rule1_negative_real_discogs_shape_non_va_artist(self):
        """Real Discogs shape with a non-VA ``artist_id`` is not VA."""
        req = _request(mb_release_id=None,
                       discogs_release_id="555",
                       mb_artist_id=None)
        discogs_payload = {
            "id": "555",
            "title": "A Regular Album",
            "artist_name": "Some Artist",
            "artist_id": "12345",
            "release_group_id": None,
            "tracks": [],
            "labels": [],
        }
        self.assertFalse(detect_va_compilation(
            req, discogs_release_payload=discogs_payload,
        ))

    def test_rule2_release_group_primary_type_compilation_with_divergent_tracks(self):
        """Rule 2 (TIGHTENED #373, 2026-05-25): Compilation rg + divergent
        track artist-credits → True. The divergence requirement is what
        distinguishes real VA from greatest-hits / single-artist comps.
        """
        req = _request(mb_artist_id="some-other-artist-id")
        rg_payload = {"primary-type": "Compilation", "secondary-types": []}
        release_payload = {
            "artist-credit": [{"name": "Various", "joinphrase": ""}],
            "media": [
                {
                    "tracks": [
                        {"title": "Track 1",
                         "artist-credit": [{"name": "Dick Dale"}]},
                        {"title": "Track 2",
                         "artist-credit": [{"name": "Surfaris"}]},
                    ],
                },
            ],
        }
        self.assertTrue(detect_va_compilation(
            req,
            mb_release_payload=release_payload,
            mb_release_group_payload=rg_payload,
        ))

    def test_rule2_release_group_secondary_type_compilation_with_divergent_tracks(self):
        """Secondary-type Compilation + divergent track credits → True."""
        req = _request(mb_artist_id="some-other-artist-id")
        release_payload = {
            "artist-credit": [{"name": "Various", "joinphrase": ""}],
            "release-group": {
                "primary-type": "Album",
                "secondary-types": ["Compilation"],
            },
            "media": [
                {
                    "tracks": [
                        {"title": "Track 1",
                         "artist-credit": [{"name": "Artist X"}]},
                        {"title": "Track 2",
                         "artist-credit": [{"name": "Artist Y"}]},
                    ],
                },
            ],
        }
        self.assertTrue(detect_va_compilation(
            req, mb_release_payload=release_payload,
        ))

    def test_rule2_greatest_hits_single_artist_comp_is_not_va(self):
        """Regression guard for #373: greatest-hits releases are MB-tagged
        as Compilation primary-type, but every track shares the album
        artist. Without the per-track-divergence requirement, Rule 2
        false-flagged these as VA — the VA strategy mix is strictly
        worse for single-artist comps (drops default/literal, the
        natural query shape when every track is by the same artist).
        """
        req = _request(mb_artist_id="some-other-artist-id")
        rg_payload = {"primary-type": "Compilation", "secondary-types": []}
        release_payload = {
            "artist-credit": [{"name": "The Beatles", "joinphrase": ""}],
            "media": [
                {
                    "tracks": [
                        {"title": "Track 1",
                         "artist-credit": [{"name": "The Beatles"}]},
                        {"title": "Track 2",
                         "artist-credit": [{"name": "The Beatles"}]},
                        {"title": "Track 3",
                         "artist-credit": [{"name": "The Beatles"}]},
                    ],
                },
            ],
        }
        self.assertFalse(detect_va_compilation(
            req,
            mb_release_payload=release_payload,
            mb_release_group_payload=rg_payload,
        ))

    def test_rule2_compilation_rg_without_track_payload_does_not_fire(self):
        """Compilation rg alone (no mb_release_payload tracks) → False.

        Without per-track payload data we can't measure divergence, so
        the tightened Rule 2 cannot fire. Rule 1 (canonical VA artist
        id) still works without payload — and is the path the production
        backfill takes for the 18 known Discogs VA rows.
        """
        req = _request(mb_artist_id="some-other-artist-id")
        rg_payload = {"primary-type": "Compilation", "secondary-types": []}
        self.assertFalse(detect_va_compilation(
            req, mb_release_group_payload=rg_payload,
        ))

    def test_rule3_split_artist_joinphrase_with_divergent_track_credits(self):
        req = _request(mb_artist_id="some-other-artist-id")
        release_payload = {
            "artist-credit": [
                {"name": "Artist A", "joinphrase": " / "},
                {"name": "Artist B", "joinphrase": ""},
            ],
            "media": [
                {
                    "tracks": [
                        {"title": "Track 1",
                         "artist-credit": [{"name": "Artist A"}]},
                        {"title": "Track 2",
                         "artist-credit": [{"name": "Artist B"}]},
                    ],
                },
            ],
        }
        self.assertTrue(detect_va_compilation(
            req, mb_release_payload=release_payload,
        ))

    def test_rule1_discogs_va_when_mb_release_id_is_numeric(self):
        """Live-bug repro from the 2026-05-25 deploy backfill: 18 wanted
        rows had ``mb_release_id`` holding a numeric Discogs id (the
        web/CLI Discogs add stuffs the Discogs id into both
        ``mb_release_id`` and ``discogs_release_id`` for pipeline
        compat). With ``mb_release_id`` non-null, the detector treated
        them as MB-sourced, then compared the canonical Discogs VA id
        ``"194"`` against the MB UUID ``"89ad4ac3-..."`` — never
        matched.

        Fix: a numeric ``mb_release_id`` is a Discogs-source signal
        (UUIDs always contain ``-``); switch the detector's branch
        accordingly so Rule 1 compares against ``DISCOGS_VA_ARTIST_ID``.
        """
        req = _request(
            id=2522,
            mb_release_id="32457180",          # numeric → Discogs id
            discogs_release_id="32457180",
            mb_release_group_id=None,
            mb_artist_id="194",                 # canonical Discogs VA id
            artist_name="Various",
        )
        self.assertTrue(detect_va_compilation(req))

    def test_negative_artist_named_various_without_canonical_mbid(self):
        """Regression guard: name == "Various" with non-canonical MBID != VA."""
        # The artist_name field on the request says "Various" but the
        # MBID is not the canonical 89ad4ac3-... MBID. Detection must
        # compare IDs, not name strings.
        req = _request(
            mb_artist_id="not-the-canonical-mbid",
            artist_name="Various",
        )
        self.assertFalse(detect_va_compilation(req))

    def test_negative_artist_name_various_artists_without_canonical_mbid(self):
        """Mixed-case 'Various Artists' name without canonical MBID is False."""
        req = _request(
            mb_artist_id="not-the-canonical-mbid",
            artist_name="Various Artists",
        )
        self.assertFalse(detect_va_compilation(req))

    # --- #373 tighten regression guards ------------------------------- #

    def test_373_greatest_hits_compilation_is_not_va(self):
        """#373 regression guard: a greatest-hits release tagged as MB
        Compilation primary-type, with every track credited to the
        album artist, must NOT be flagged as VA.

        Pre-tightening, Rule 2 fired on Compilation rg alone and the
        Beatles' "1962-1966" / "1967-1970" greatest-hits comps got
        the VA strategy mix — dropping ``default``/``literal`` (the
        natural query shapes for "Beatles 1962-1966") and substituting
        ``va_track_artist_*`` queries that all use the same artist
        (because every track is "The Beatles"). Strictly worse search
        outcomes for that cohort.
        """
        req = _request(mb_artist_id="some-other-artist-id")
        rg_payload = {"primary-type": "Compilation", "secondary-types": []}
        release_payload = {
            "artist-credit": [{"name": "The Beatles", "joinphrase": ""}],
            "media": [
                {
                    "tracks": [
                        {"title": "Love Me Do",
                         "artist-credit": [{"name": "The Beatles"}]},
                        {"title": "Please Please Me",
                         "artist-credit": [{"name": "The Beatles"}]},
                        {"title": "From Me to You",
                         "artist-credit": [{"name": "The Beatles"}]},
                    ],
                },
            ],
        }
        self.assertFalse(detect_va_compilation(
            req,
            mb_release_payload=release_payload,
            mb_release_group_payload=rg_payload,
        ))

    def test_373_real_va_with_non_canonical_artist_is_va(self):
        """#373 positive: a real VA compilation whose album artist
        credit doesn't carry the canonical VA MBID (e.g. an album
        credited to "Various" without the canonical id, or a niche
        label sampler) but whose per-track artists genuinely diverge
        must still be flagged as VA via the tightened Rule 2.

        This is the inverse of the greatest-hits regression — Rule 2
        SHOULD fire when divergence is real.
        """
        req = _request(mb_artist_id="some-other-artist-id")
        rg_payload = {"primary-type": "Compilation", "secondary-types": []}
        release_payload = {
            "artist-credit": [{"name": "Various", "joinphrase": ""}],
            "media": [
                {
                    "tracks": [
                        {"title": "Misirlou",
                         "artist-credit": [{"name": "Dick Dale"}]},
                        {"title": "Wipe Out",
                         "artist-credit": [{"name": "Surfaris"}]},
                        {"title": "Pipeline",
                         "artist-credit": [{"name": "Chantays"}]},
                    ],
                },
            ],
        }
        self.assertTrue(detect_va_compilation(
            req,
            mb_release_payload=release_payload,
            mb_release_group_payload=rg_payload,
        ))


# --------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------- #


class TestLooksNumeric(unittest.TestCase):
    CASES = [
        ("12345", True),
        ("  555  ", True),
        ("abc-uuid", False),
        ("123abc", False),
        ("", False),
        (None, False),
        (0, True),
        (12345, True),
    ]

    def test_looks_numeric_cases(self):
        for value, expected in self.CASES:
            with self.subTest(value=value):
                self.assertEqual(_looks_numeric(value), expected)


class TestRecorderFailureIsSwallowed(unittest.TestCase):
    """If recording fails, the resolver still returns the resolved value.

    Recording is best-effort observability; an upsert failure must not
    block the caller from using the value the mirror just returned.
    """

    def test_recorder_exception_does_not_propagate(self):
        class BadRecorder:
            def record_field_resolution(
                self, *args: Any, **kwargs: Any,
            ) -> None:
                raise RuntimeError("disk full")

        req = _request(id=999, mb_release_group_id="abc-uuid")
        # Should NOT raise.
        result = resolve_release_group_year(
            req, BadRecorder(),  # type: ignore[arg-type]
            mb_get_release_group_year=lambda _: 1999,
        )
        self.assertEqual(result.status, "resolved")
        self.assertEqual(result.value, 1999)


# --------------------------------------------------------------------- #
# ResolverResult wire-boundary type
# --------------------------------------------------------------------- #


class TestResolverResultRoundTrip(unittest.TestCase):
    """Wire-boundary Struct: msgspec validates type drift at decode."""

    def test_round_trip_through_json(self):
        import msgspec

        original = ResolverResult(
            field_name=FIELD_RELEASE_GROUP_YEAR,
            value=1997,
            status="resolved",
            reason_code=None,
        )
        encoded = msgspec.json.encode(original)
        decoded = msgspec.json.decode(encoded, type=ResolverResult)
        self.assertEqual(decoded.field_name, FIELD_RELEASE_GROUP_YEAR)
        self.assertEqual(decoded.value, 1997)
        self.assertEqual(decoded.status, "resolved")

    def test_invalid_status_raises_validation_error(self):
        import msgspec

        raw = (
            b'{"field_name":"release_group_year","value":null,'
            b'"status":"made_up_status","reason_code":null}'
        )
        with self.assertRaises(msgspec.ValidationError):
            msgspec.json.decode(raw, type=ResolverResult)


# --------------------------------------------------------------------- #
# resolve_all — inline-at-enqueue orchestrator (U4)
# --------------------------------------------------------------------- #


class TestResolveAll(unittest.TestCase):
    """Coverage for ``resolve_all`` — parallel resolver fanout with budget."""

    def test_mb_happy_path_returns_populated_result(self):
        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(id=10, mb_release_group_id="rg-uuid")

        # Each collaborator is a fast in-process callable; total elapsed
        # well under the budget.
        result = resolve_all(
            req, db,
            mb_get_release_group_year=lambda _id: 1997,
            mb_get_release=lambda mbid, *, fresh=True: {
                "release_group_id": "rg-uuid",
                "media": [{"position": 1, "tracks": [
                    {"position": 1, "title": "T1",
                     "artist-credit": [{"name": "A1", "joinphrase": ""}]},
                ]}],
                "label-info": [{"catalog-number": "CAT-1234"}],
            },
        )
        self.assertEqual(result.release_group_year, 1997)
        self.assertEqual(result.release_group_id, "rg-uuid")
        self.assertEqual(result.catalog_number, "CAT-1234")
        self.assertEqual(result.track_artists, ["A1"])
        self.assertFalse(result.is_va_compilation)
        self.assertEqual(result.timed_out_fields, [])
        # Side-table rows recorded for every resolver.
        self.assertIsNotNone(
            db.get_field_resolution(10, FIELD_RELEASE_GROUP_YEAR))
        self.assertIsNotNone(
            db.get_field_resolution(10, FIELD_RELEASE_GROUP_ID))
        self.assertIsNotNone(
            db.get_field_resolution(10, FIELD_CATALOG_NUMBER))
        self.assertIsNotNone(
            db.get_field_resolution(10, FIELD_TRACK_ARTIST))

    def test_mb_release_fetched_at_most_once_per_invocation(self):
        """Code-review #1: when ``resolve_all`` is given an
        ``mb_release_payload`` kwarg, the per-field resolvers must
        reuse it instead of each making their own
        ``mb_get_release`` call.

        Pre-fix: ``resolve_release_group_id`` + ``resolve_track_artists``
        + ``resolve_catalog_number`` each called ``mb_get_release(mbid)``
        independently, producing 3 mirror round-trips per add for the
        MB happy path (4 with the ``rg_year`` site, which already runs
        through ``mb_get_release_group_year``). The fix threads the
        already-fetched payload through.
        """
        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(id=801, mb_release_group_id="rg-uuid")

        mb_release_payload = {
            "release_group_id": "rg-uuid",
            "media": [{"position": 1, "tracks": [
                {"position": 1, "title": "T1",
                 "artist-credit": [{"name": "A1", "joinphrase": ""}]},
            ]}],
            "label-info": [{"catalog-number": "CAT-1234"}],
        }

        call_count = {"mb_get_release": 0}

        def _count_mb_get_release(mbid, *, fresh=True):
            call_count["mb_get_release"] += 1
            return mb_release_payload

        result = resolve_all(
            req, db,
            mb_release_payload=mb_release_payload,
            mb_get_release_group_year=lambda _id: 1997,
            mb_get_release=_count_mb_get_release,
        )

        self.assertLessEqual(
            call_count["mb_get_release"], 1,
            f"resolve_all fetched mb_get_release {call_count['mb_get_release']} "
            "times in a single invocation — should be 0 (payload threaded "
            "in via mb_release_payload)",
        )
        # And the resolved fields still come out right — payload-driven
        # resolution is equivalent to fetch-driven resolution.
        self.assertEqual(result.release_group_id, "rg-uuid")
        self.assertEqual(result.catalog_number, "CAT-1234")
        self.assertEqual(result.track_artists, ["A1"])

    def test_discogs_release_fetched_at_most_once_per_invocation(self):
        """Mirror of the MB single-fetch guard, on the Discogs branch."""
        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(
            id=802,
            mb_release_id=None,
            mb_release_group_id=None,
            discogs_release_id="555",
        )

        discogs_payload = {
            "id": "555",
            "title": "Album",
            "artist_id": "12345",
            "release_group_id": "rg-master-id",
            "tracks": [{"disc_number": 1, "track_number": 1, "title": "T1",
                        "artists": [{"name": "Artist X"}]}],
            "labels": [{"catno": "CAT-7"}],
        }

        call_count = {"discogs_get_release": 0}

        def _count_discogs(_rid, *, fresh=True):
            call_count["discogs_get_release"] += 1
            return discogs_payload

        result = resolve_all(
            req, db,
            discogs_release_payload=discogs_payload,
            discogs_get_master_year=lambda _id: 2010,
            discogs_get_release=_count_discogs,
        )

        self.assertLessEqual(
            call_count["discogs_get_release"], 1,
            f"resolve_all fetched discogs_get_release "
            f"{call_count['discogs_get_release']} times — should be 0 (payload "
            "threaded in via discogs_release_payload)",
        )
        self.assertEqual(result.release_group_id, "rg-master-id")
        self.assertEqual(result.catalog_number, "CAT-7")

    def test_discogs_master_persisted_into_mb_release_group_id_via_apply(self):
        """KTD-1 pin (docs/plans/2026-07-04-001-feat-discogs-pathway-replace-plan.md
        U2): new Discogs adds already persist the Discogs master id into
        ``mb_release_group_id`` through ``resolve_all`` +
        ``apply_resolve_all_result`` — the mechanism the Discogs-pathway
        Replace picker anchors on. Chains both halves together; the two
        halves were previously only pinned in isolation
        (``test_discogs_release_fetched_at_most_once_per_invocation`` for
        the resolver, ``test_writes_release_group_id_when_existing_is_none``
        for the generic write helper)."""
        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(
            id=803,
            mb_release_id=None,
            mb_release_group_id=None,
            discogs_release_id="900555",
        )

        discogs_payload = {
            "id": "900555",
            "title": "Album",
            "artist_id": "12345",
            "release_group_id": "98765",
            "tracks": [],
            "labels": [],
        }

        result = resolve_all(
            req, db,
            discogs_release_payload=discogs_payload,
            discogs_get_master_year=lambda _id: 2010,
            discogs_get_release=lambda _rid, *, fresh=True: discogs_payload,
        )
        self.assertEqual(result.release_group_id, "98765")

        apply_resolve_all_result(
            db, 803, result, expected_status="wanted",
            existing_mb_release_group_id=None,
        )
        _req_id, fields = db.update_request_fields_calls[-1]
        self.assertEqual(fields["mb_release_group_id"], "98765")

    def test_discogs_masterless_release_leaves_mb_release_group_id_unwritten(self):
        """AE1/R2 companion: a masterless Discogs release resolves
        ``release_group_id=None`` and the write helper must neither write
        the column nor raise — matching the YouTube resolver's orphan-shape
        handling for the same case."""
        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(
            id=804,
            mb_release_id=None,
            mb_release_group_id=None,
            discogs_release_id="900777",
        )

        discogs_payload = {
            "id": "900777",
            "title": "Album",
            "artist_id": "12345",
            "release_group_id": None,
            "tracks": [],
            "labels": [],
        }

        result = resolve_all(
            req, db,
            discogs_release_payload=discogs_payload,
            discogs_get_master_year=lambda _id: None,
            discogs_get_release=lambda _rid, *, fresh=True: discogs_payload,
        )
        self.assertIsNone(result.release_group_id)

        apply_resolve_all_result(
            db, 804, result, expected_status="wanted",
            existing_mb_release_group_id=None,
        )
        _req_id, fields = db.update_request_fields_calls[-1]
        self.assertNotIn("mb_release_group_id", fields)

    def test_va_detection_via_canonical_mbid(self):
        """Rule 1: primary-artist MBID matches the canonical VA id."""
        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(id=11, mb_artist_id=MB_VA_ARTIST_MBID)

        result = resolve_all(
            req, db,
            mb_get_release_group_year=lambda _id: None,
            mb_get_release=lambda mbid, *, fresh=True: {
                "release_group_id": "rg-uuid",
                "media": [], "label-info": [],
            },
        )
        self.assertTrue(result.is_va_compilation)

    def test_va_detection_via_release_group_compilation_type(self):
        """Rule 2 (TIGHTENED #373): MB release-group Compilation +
        divergent per-track artist credits fires.
        """
        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(id=12, mb_artist_id="non-va-artist")
        mb_release = {
            "release_group_id": "rg-uuid",
            "artist-credit": [{"name": "Various", "joinphrase": ""}],
            "release-group": {"primary-type": "Compilation"},
            "media": [
                {
                    "tracks": [
                        {"title": "Track 1",
                         "artist-credit": [{"name": "Dick Dale"}]},
                        {"title": "Track 2",
                         "artist-credit": [{"name": "Surfaris"}]},
                    ],
                },
            ],
            "label-info": [],
        }

        result = resolve_all(
            req, db,
            mb_release_payload=mb_release,
            mb_get_release_group_year=lambda _id: 2010,
            mb_get_release=lambda mbid, *, fresh=True: mb_release,
        )
        self.assertTrue(result.is_va_compilation)

    def test_va_negative_for_artist_named_various_without_canonical_id(self):
        """Regression guard: name string 'Various' alone does NOT trigger VA."""
        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(
            id=13,
            artist_name="Various",
            mb_artist_id="something-else-not-canonical",
        )

        result = resolve_all(
            req, db,
            mb_get_release_group_year=lambda _id: 2010,
            mb_get_release=lambda mbid, *, fresh=True: {
                "release_group_id": "rg-uuid",
                "media": [], "label-info": [],
            },
        )
        self.assertFalse(result.is_va_compilation)

    def test_va_negative_for_discogs_release_with_non_va_artist_id(self):
        """Regression guard parallel to the MB-side negative test: a
        Discogs release whose primary artist id is NOT the canonical
        194 must NOT flip is_va_compilation, even if the artist name
        happens to contain 'Various' or similar. Rule 1 is identity,
        not string."""
        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(
            id=141,
            mb_release_id=None,
            mb_release_group_id=None,
            discogs_release_id="999999",
        )

        # Construct a Discogs payload with a non-VA artist id. Rule 1
        # MUST consult artist_id identity (not artist name).
        discogs_payload = {
            "id": "999999",
            "artists": [{"id": 12345, "name": "Various Pulp"}],
            "tracks": [],
            "labels": [],
        }

        result = resolve_all(
            req, db,
            discogs_get_release=lambda _rid, *, fresh=True: discogs_payload,
        )
        self.assertFalse(result.is_va_compilation,
                         "is_va_compilation must rely on identity, not name string")

    def test_discogs_va_detection_via_payload(self):
        """Rule 1 for Discogs: primary artist id 194 fires via payload."""
        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(
            id=14,
            mb_release_id=None,
            mb_release_group_id=None,
            discogs_release_id="83182",
        )
        discogs_payload = {
            "id": "83182",
            "artists": [{"id": DISCOGS_VA_ARTIST_ID, "name": "Various"}],
            "tracks": [], "labels": [],
        }

        result = resolve_all(
            req, db,
            discogs_release_payload=discogs_payload,
            discogs_get_master_year=lambda _id: 2000,
            discogs_get_release=lambda rid, *, fresh=True: discogs_payload,
        )
        self.assertTrue(result.is_va_compilation)

    def test_budget_enforcement_marks_slow_resolver_as_timeout(self):
        """A resolver that exceeds the wall-clock budget lands NULL +
        ``unresolved_timeout`` in the side table. Others that completed
        fast keep their values."""
        import threading
        import time

        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(id=15, mb_release_group_id="rg-uuid")

        slow_called = threading.Event()

        def _slow_get_release(mbid, *, fresh=True):
            slow_called.set()
            # Hold past the 0.3s budget; the orchestrator returns
            # while we're still sleeping, marks our slot as timeout,
            # and we get GC'd later. The future is left to run.
            time.sleep(1.5)
            return {
                "release_group_id": "rg-uuid",
                "media": [], "label-info": [],
            }

        start = time.monotonic()
        result = resolve_all(
            req, db,
            budget_seconds=0.3,
            mb_get_release_group_year=lambda _id: 1997,
            mb_get_release=_slow_get_release,
        )
        elapsed = time.monotonic() - start

        # Budget enforcement: orchestrator returned in under ~0.7s
        # (some slack for thread scheduling). MUST be well below the
        # 1.5s sleep duration.
        self.assertLess(elapsed, 1.2,
                        f"budget not enforced: elapsed={elapsed:.3f}s")
        # The fast resolver landed cleanly.
        self.assertEqual(result.release_group_year, 1997)
        # The slow resolvers timed out → NULL values.
        self.assertIsNone(result.release_group_id)
        self.assertIsNone(result.catalog_number)
        # Side-table records timed-out fields.
        self.assertGreater(len(result.timed_out_fields), 0)
        for field in result.timed_out_fields:
            row = db.get_field_resolution(15, field)
            assert row is not None
            self.assertEqual(row["status"], "unresolved_timeout")
            self.assertEqual(row["reason_code"], "budget_exhausted")
        # Sanity: the slow resolver was actually entered (not skipped).
        self.assertTrue(slow_called.is_set())

    def test_completed_future_harvested_after_budget_expires(self):
        """Regression guard for code-review finding #2: when the iteration
        gets to a future's slot AFTER the budget has already expired but the
        future already completed concurrently, the orchestrator MUST harvest
        the value instead of dropping it as NULL.

        Shape: order the jobs so ``mb_get_release_group_year`` is FIRST and
        slow (forces budget expiry), and the other resolvers' shared
        ``mb_get_release`` is FAST (already done by the time iteration
        reaches them). Pre-fix, those completed futures landed NULL because
        the ``remaining <= 0`` branch unconditionally dropped them. Post-fix
        they're harvested via ``fut.done()`` check + ``fut.result(timeout=0)``.
        """
        import threading
        import time as _t

        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(id=99, mb_release_group_id="rg-completed")

        # Slow rg_year resolver — exhausts the budget.
        slow_called = threading.Event()
        def _slow_year(_rg_id):
            slow_called.set()
            _t.sleep(1.5)
            return 2007

        # FAST get_release — completes well before the budget expires,
        # so by the time the iteration loop reaches the rg_id / catno /
        # track-artist slots, ``remaining <= 0`` AND ``fut.done() is True``.
        def _fast_get_release(mbid, *, fresh=True):
            return {
                "release_group_id": "rg-completed",
                "media": [
                    {"tracks": [
                        {"position": 1, "title": "Track 1",
                         "artist-credit": [{"name": "Some Artist"}]}
                    ]}
                ],
                "label-info": [{"catalog-number": "ABC-001"}],
            }

        start = _t.monotonic()
        result = resolve_all(
            req, db,
            budget_seconds=0.3,
            mb_get_release_group_year=_slow_year,
            mb_get_release=_fast_get_release,
        )
        elapsed = _t.monotonic() - start

        # Budget was enforced (returned well before the slow resolver's 1.5s).
        self.assertLess(elapsed, 1.2,
                        f"budget not enforced: elapsed={elapsed:.3f}s")
        # rg_year timed out as expected.
        self.assertIsNone(result.release_group_year)
        # The KEY assertion — fast-completing futures were harvested even
        # though the budget had already expired by the time the iteration
        # got to their slots.
        self.assertEqual(result.release_group_id, "rg-completed",
                         "completed rg_id future not harvested post-budget")
        self.assertEqual(result.catalog_number, "ABC-001",
                         "completed catalog_number future not harvested post-budget")
        self.assertTrue(slow_called.is_set())

    def test_mirror_unavailable_does_not_block_add(self):
        """Network error on a resolver → that field lands NULL with
        ``unresolved_mirror_unavailable`` in the side table; ``resolve_all``
        returns normally so the add flow can proceed."""
        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(id=16, mb_release_group_id="rg-uuid")

        def _boom(_rg_id):
            raise URLError("connection refused")

        result = resolve_all(
            req, db,
            mb_get_release_group_year=_boom,
            mb_get_release=lambda mbid, *, fresh=True: {
                "release_group_id": "rg-uuid",
                "media": [], "label-info": [],
            },
        )
        self.assertIsNone(result.release_group_year)
        rg_year_row = db.get_field_resolution(
            16, FIELD_RELEASE_GROUP_YEAR)
        assert rg_year_row is not None
        self.assertEqual(
            rg_year_row["status"], "unresolved_mirror_unavailable")
        # The orchestrator does NOT raise.
        self.assertFalse(result.is_va_compilation)

    def test_internal_error_in_resolver_lands_unresolved_internal_error(self):
        """Code-review #18: a programmer-error escape from a per-field
        resolver (something the resolver's own classifier can't classify)
        lands as ``unresolved_internal_error`` with
        ``reason_code='bug_<ExcName>'`` — NOT as
        ``unresolved_mirror_unavailable``. The two used to be conflated;
        the conflation made real bugs look like transient mirror outages
        and they got retried forever instead of being surfaced for fix.

        Mechanism: a collaborator that raises a non-transient exception
        (``KeyError``) propagates through the per-field resolver's
        ``_classify_lookup_exception`` (which re-raises programmer
        errors) and into ``resolve_all``'s ``except Exception`` clause.
        That clause now classifies the escape and tags it as
        ``unresolved_internal_error``.
        """
        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(id=181, mb_release_group_id="rg-uuid")

        def _bug(_rg_id: str) -> int | None:
            raise KeyError("simulated bug")

        result = resolve_all(
            req, db,
            mb_get_release_group_year=_bug,
            mb_get_release=lambda mbid, *, fresh=True: {
                "release_group_id": "rg-uuid",
                "media": [], "label-info": [],
            },
        )

        self.assertIsNone(result.release_group_year)
        row = db.get_field_resolution(181, FIELD_RELEASE_GROUP_YEAR)
        assert row is not None
        self.assertEqual(row["status"], "unresolved_internal_error")
        self.assertEqual(row["reason_code"], "bug_KeyError")

    def test_malformed_request_lands_with_nulls(self):
        """Missing release ids → resolvers return ``unresolved_malformed``;
        the orchestrator collects them, the request is created with
        NULL fields, no mirror calls attempted."""
        from lib.field_resolver_service import resolve_all

        db = FakePipelineDB()
        req = _request(
            id=17,
            mb_release_id=None,
            mb_release_group_id=None,
            discogs_release_id=None,
        )

        called = {"mb_rgy": False, "mb_release": False}

        def _mb_rgy(_rg_id):
            called["mb_rgy"] = True
            return 2000

        def _mb_release(_mbid, *, fresh=True):
            called["mb_release"] = True
            return {}

        result = resolve_all(
            req, db,
            mb_get_release_group_year=_mb_rgy,
            mb_get_release=_mb_release,
        )
        self.assertIsNone(result.release_group_year)
        self.assertIsNone(result.release_group_id)
        self.assertIsNone(result.catalog_number)
        # Resolvers short-circuit on missing ids — no mirror touched.
        self.assertFalse(called["mb_rgy"])
        self.assertFalse(called["mb_release"])
        # All four side-table rows recorded as unresolved_malformed.
        for field in (FIELD_RELEASE_GROUP_YEAR, FIELD_RELEASE_GROUP_ID,
                      FIELD_CATALOG_NUMBER, FIELD_TRACK_ARTIST):
            row = db.get_field_resolution(17, field)
            assert row is not None, f"{field} side-table row missing"
            self.assertEqual(
                row["status"], "unresolved_malformed",
                f"{field} status: {row['status']}",
            )


# --------------------------------------------------------------------- #
# apply_resolve_all_result — DB write helper
# --------------------------------------------------------------------- #


class TestApplyResolveAllResult(unittest.TestCase):
    """Helper that turns a ``ResolveAllResult`` into the right
    ``update_request_fields`` call. Extracted from web + CLI
    ``_resolve_and_update_after_add`` (code-review finding #8)."""

    def test_writes_all_resolved_fields(self):
        db = FakePipelineDB()
        result = ResolveAllResult(
            release_group_year=1997,
            release_group_id="rg-uuid-aaa",
            catalog_number="CAT-001",
            track_artists=["Artist A"],
            is_va_compilation=False,
        )

        apply_resolve_all_result(
            db, 42, result, expected_status="wanted",
            existing_mb_release_group_id=None,
        )

        self.assertEqual(len(db.update_request_fields_calls), 1)
        req_id, fields = db.update_request_fields_calls[0]
        self.assertEqual(req_id, 42)
        self.assertEqual(fields["release_group_year"], 1997)
        self.assertEqual(fields["mb_release_group_id"], "rg-uuid-aaa")
        self.assertEqual(fields["catalog_number"], "CAT-001")
        self.assertFalse(fields["is_va_compilation"])

    def test_skips_fields_that_resolver_could_not_populate(self):
        db = FakePipelineDB()
        result = ResolveAllResult(
            release_group_year=None,
            release_group_id=None,
            catalog_number=None,
            is_va_compilation=True,
        )

        apply_resolve_all_result(db, 7, result, expected_status="wanted")

        self.assertEqual(len(db.update_request_fields_calls), 1)
        req_id, fields = db.update_request_fields_calls[0]
        self.assertEqual(req_id, 7)
        # ``is_va_compilation`` is always written (immutability invariant
        # at enqueue) — the schema default is False, so writing the
        # detector's verdict matters here.
        self.assertTrue(fields["is_va_compilation"])
        # Optional fields are absent when resolution returned None.
        self.assertNotIn("release_group_year", fields)
        self.assertNotIn("mb_release_group_id", fields)
        self.assertNotIn("catalog_number", fields)

    def test_does_not_clobber_existing_release_group_id(self):
        """If the row already had an MB release-group id from the
        upstream release fetch, the resolver-derived value must not
        overwrite it."""
        db = FakePipelineDB()
        result = ResolveAllResult(
            release_group_id="resolver-derived-rg",
            is_va_compilation=False,
        )

        apply_resolve_all_result(
            db, 9, result,
            expected_status="wanted",
            existing_mb_release_group_id="upstream-known-rg",
        )

        req_id, fields = db.update_request_fields_calls[0]
        self.assertEqual(req_id, 9)
        self.assertNotIn("mb_release_group_id", fields)

    def test_writes_release_group_id_when_existing_is_none(self):
        db = FakePipelineDB()
        result = ResolveAllResult(
            release_group_id="resolver-derived-rg",
            is_va_compilation=False,
        )

        apply_resolve_all_result(
            db, 11, result,
            expected_status="wanted",
            existing_mb_release_group_id=None,
        )

        req_id, fields = db.update_request_fields_calls[0]
        self.assertEqual(req_id, 11)
        self.assertEqual(fields["mb_release_group_id"], "resolver-derived-rg")

    def test_db_failure_does_not_raise(self):
        """``update_request_fields`` exceptions are reported via the
        caller's exception channel; the helper itself does not raise.
        Web + CLI wrappers own their own logging style — the helper
        raises so the wrapper can decide how to surface it."""
        from typing import Any

        class FailingDB:
            update_request_fields_calls: list[tuple[int, dict[str, Any]]] = []

            def update_request_fields(self, request_id: int, **fields: Any) -> bool:
                raise RuntimeError("db boom")

            def update_track_artists(
                self,
                request_id: int,
                track_artists: list[str | None],
                *,
                expected_status: str | None = None,
            ) -> bool:
                # Never reached: update_request_fields raises first.
                raise AssertionError("unexpected call")

        # Helper re-raises; wrapper catches and reports in its own style.
        with self.assertRaises(RuntimeError):
            apply_resolve_all_result(
                FailingDB(), 99,
                ResolveAllResult(is_va_compilation=False),
                expected_status="wanted",
            )

    def test_scalar_conflict_aborts_track_artist_write(self):
        class ConflictDB:
            track_calls = 0

            def update_request_fields(
                self, request_id: int, **fields: Any,
            ) -> bool:
                return False

            def update_track_artists(
                self,
                request_id: int,
                track_artists: list[str | None],
                *,
                expected_status: str | None = None,
            ) -> bool:
                self.track_calls += 1
                return True

        db = ConflictDB()
        applied = apply_resolve_all_result(
            db,
            42,
            ResolveAllResult(
                is_va_compilation=False,
                track_artists=["Late Artist"],
            ),
            expected_status="wanted",
        )

        self.assertFalse(applied)
        self.assertEqual(db.track_calls, 0)

    def test_stale_wanted_snapshot_cannot_write_manual_parent_or_tracks(self):
        db = FakePipelineDB()
        request_id = db.add_request(
            "Artist", "Album", "request", mb_release_id="resolver-stale",
        )
        db.set_tracks(request_id, [{
            "disc_number": 1,
            "track_number": 1,
            "title": "Track",
            "track_artist": None,
        }])
        self.assertTrue(db.update_status(
            request_id, "manual", expected_status="wanted",
        ))
        before_row = copy.deepcopy(db.get_request(request_id))
        before_tracks = db.get_tracks(request_id)

        applied = apply_resolve_all_result(
            db,
            request_id,
            ResolveAllResult(
                release_group_year=1999,
                is_va_compilation=True,
                track_artists=["Late Artist"],
            ),
            expected_status="wanted",
        )

        self.assertFalse(applied)
        self.assertEqual(db.get_request(request_id), before_row)
        self.assertEqual(db.get_tracks(request_id), before_tracks)

    def test_in_flight_resolver_cannot_rewrite_replaced_tracks(self):
        db = FakePipelineDB()
        request_id = db.add_request(
            "Artist", "Album", "request", mb_release_id="resolver-old",
        )
        db.set_tracks(request_id, [{
            "disc_number": 1,
            "track_number": 1,
            "title": "Track",
            "track_artist": None,
        }])
        entered = threading.Event()
        release = threading.Event()
        outcomes: list[bool] = []

        def late_apply() -> None:
            entered.set()
            self.assertTrue(release.wait(timeout=10))
            outcomes.append(apply_resolve_all_result(
                db,
                request_id,
                ResolveAllResult(
                    is_va_compilation=False,
                    track_artists=["Late Artist"],
                ),
                expected_status="wanted",
            ))

        worker = threading.Thread(target=late_apply)
        worker.start()
        self.assertTrue(entered.wait(timeout=10))
        db.supersede_request_mbid(
            request_id,
            new_mb_release_id="resolver-new",
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="Artist",
            new_album_title="Album (correct pressing)",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )
        frozen = db.get_tracks(request_id)
        release.set()
        worker.join(timeout=10)

        self.assertFalse(worker.is_alive())
        self.assertEqual(outcomes, [False])
        self.assertEqual(db.get_tracks(request_id), frozen)
        self.assertIsNone(db.get_tracks(request_id)[0]["track_artist"])


if __name__ == "__main__":
    unittest.main()
