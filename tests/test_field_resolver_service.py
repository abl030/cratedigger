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

import socket
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
    ResolverResult,
    _looks_numeric,
    detect_va_compilation,
    resolve_catalog_number,
    resolve_release_group_id,
    resolve_release_group_year,
    resolve_track_artists,
)
from tests.fakes import FakePipelineDB


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

    def test_rule2_release_group_primary_type_compilation(self):
        req = _request(mb_artist_id="some-other-artist-id")
        rg_payload = {"primary-type": "Compilation", "secondary-types": []}
        self.assertTrue(detect_va_compilation(
            req, mb_release_group_payload=rg_payload,
        ))

    def test_rule2_release_group_secondary_type_compilation(self):
        req = _request(mb_artist_id="some-other-artist-id")
        release_payload = {
            "release-group": {
                "primary-type": "Album",
                "secondary-types": ["Compilation"],
            },
        }
        self.assertTrue(detect_va_compilation(
            req, mb_release_payload=release_payload,
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


if __name__ == "__main__":
    unittest.main()
