"""Contract tests for web/routes/long_tail.py.

Split from tests/web/test_routes_pipeline.py (#481 item 3), which itself
split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""
import os
import sqlite3
import sys
import unittest
from typing import ClassVar
from unittest.mock import patch

import msgspec

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.fakes import FakeBeetsDB
from tests.helpers import make_request_row
from tests.web._harness import _assert_required_fields, _FakeDbWebServerCase

MB_RELEASE_1 = "00000000-0000-0000-0000-000000000001"
MB_RELEASE_2 = "00000000-0000-0000-0000-000000000002"
DISCOGS_RELEASE = "12856590"
UNAVAILABLE_ERROR = {
    "category": "unavailable",
    "error": "long_tail_authority_unavailable",
    "message": "Current Beets authority is unavailable; retry later.",
}
CONFLICT_ERROR = {
    "category": "conflict",
    "error": "long_tail_authority_conflict",
    "message": "Long-tail exact release authority is ambiguous or invalid.",
}


class TestLongTailRouteContracts(_FakeDbWebServerCase):
    """U1 contract for ``GET /api/pipeline/long-tail``.

    Wraps ``lib.long_tail_service.list_long_tail`` — the same service
    ``pipeline-cli long-tail`` wraps (CLI ⇄ API symmetry). Drives the
    real service + DB cohort query against a fresh :class:`FakePipelineDB`
    (no service mocking, per MOCKS: LEAF-SEAM ONLY). Banding's beets
    collaborator (``_beets_db``) is the leaf seam — patched at
    ``web.server`` only when a test exercises a particular resolved world.
    """

    # The frontend long-tail list renders these fields per row out of the
    # serialized ``LongTailRow``. Pin every one so a rename can't silently
    # break the JS.
    ROW_REQUIRED_FIELDS: ClassVar = {
        "id", "artist_name", "album_title", "year", "status", "source",
        "mb_release_id", "discogs_release_id", "target_format",
        "min_bitrate", "search_filetype_override", "unfindable_category",
        "band", "in_flight_rescue",
        # Card meta (year · MB/Discogs · N tracks) + on-disk spectral strip.
        "track_count", "current_spectral_grade", "current_spectral_bitrate",
        # issue #829 Phase 5 PR4 — the worklist chip's audit-only pair,
        # derived from the request's linked current evidence so the
        # console stops painting an audit-only codec as a transcode.
        "current_spectral_accusation_admissible",
        "current_spectral_accusation_withheld",
        # The accept-sibling control + siblings panel read the rg straight
        # off the row — the single-row refetch must not drop it (#398).
        "mb_release_group_id",
    }
    ENVELOPE_REQUIRED_FIELDS: ClassVar = {"results", "band", "count"}

    def test_missing_row_bands_missing_and_imported_absent(self):
        """AE1 at the HTTP boundary: a wanted row with no beets album
        bands ``missing``; an imported request is absent from the
        result. The available fake Beets authority explicitly observes no
        matching album."""
        from lib.long_tail_service import LongTailRow
        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id=MB_RELEASE_1,
            artist_name="Vanishing", album_title="Lost"))
        self.db.seed_request(make_request_row(
            id=2, status="imported", mb_release_id=MB_RELEASE_2))

        status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.ENVELOPE_REQUIRED_FIELDS,
                                "long-tail envelope")
        self.assertEqual(data["count"], 1)
        self.assertIsNone(data["band"])
        row = data["results"][0]
        _assert_required_fields(self, row, self.ROW_REQUIRED_FIELDS,
                                "long-tail row")
        self.assertEqual(row["id"], 1)
        self.assertEqual(row["band"], "missing")
        self.assertFalse(row["in_flight_rescue"])
        # Wire shape IS the Struct shape — round-trips cleanly.
        back = msgspec.convert(row, type=LongTailRow)
        self.assertEqual(back.id, 1)

    def _link_installed_evidence(self, request_id: int, measurement,
                                 **evidence_kwargs) -> None:
        """Link a production-shaped installed evidence row to a request."""
        from tests.helpers import make_album_quality_evidence

        installed = make_album_quality_evidence(
            mb_release_id=f"installed-{request_id}",
            source_path="/mnt/virtio/Music/Beets/installed",
            measurement=measurement,
            **evidence_kwargs,
        )
        self.db.upsert_album_quality_evidence(installed)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=installed.mb_release_id,
            snapshot_fingerprint=installed.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(
            self.db.set_request_current_evidence(request_id, stored.id))

    def test_worklist_chip_withholds_an_audit_only_accusation(self):
        """Issue #829 PR4/N3: an installed AAC graded ``likely_transcode``
        by the codec-blind analyzer must not reach the console's red chip."""
        from lib.quality import AudioQualityMeasurement

        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id=MB_RELEASE_1,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=128))
        self._link_installed_evidence(
            1,
            AudioQualityMeasurement(
                min_bitrate_kbps=256, avg_bitrate_kbps=256, is_cbr=True,
                format="AAC", spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128, spectral_subject="installed",
                spectral_provenance="measured", cliff_hz=15000,
                codec_family="aac", spectral_measurement_version=2,
            ),
            codec="aac", container="m4a", storage_format="AAC",
        )

        status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 200)
        row = data["results"][0]
        self.assertEqual(row["current_spectral_grade"], "likely_transcode")
        self.assertIs(row["current_spectral_accusation_admissible"], False)
        self.assertEqual(
            row["current_spectral_accusation_withheld"], "audit_only_codec")

    def test_worklist_chip_keeps_a_real_transcode_accusation(self):
        """The must-still-work half: an MP3 cliff still accuses."""
        from lib.quality import AudioQualityMeasurement

        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id=MB_RELEASE_1,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=128))
        self._link_installed_evidence(
            1,
            AudioQualityMeasurement(
                min_bitrate_kbps=320, avg_bitrate_kbps=320, is_cbr=True,
                format="MP3", spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128, spectral_subject="installed",
                spectral_provenance="measured", cliff_hz=16000,
                codec_family="mp3", spectral_measurement_version=2,
            ),
            codec="mp3", container="mp3", storage_format="MP3",
        )

        status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 200)
        row = data["results"][0]
        self.assertIs(row["current_spectral_accusation_admissible"], True)
        self.assertIsNone(row["current_spectral_accusation_withheld"])

    def test_worklist_chip_has_no_flags_without_linked_evidence(self):
        """Fail-accusing: no linked evidence, no flags, accusing render.

        Also covers the single-row refetch (KTD8), which must project the
        same pair as the cohort read or a post-action patch would flip the
        chip's colour on its own.
        """
        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id=MB_RELEASE_1,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=128))

        status, data = self._get("/api/pipeline/long-tail")
        self.assertEqual(status, 200)
        row = data["results"][0]
        self.assertIsNone(row["current_spectral_accusation_admissible"])
        self.assertIsNone(row["current_spectral_accusation_withheld"])

        one_status, one = self._get("/api/pipeline/long-tail?id=1")
        self.assertEqual(one_status, 200)
        _assert_required_fields(
            self, one["result"], self.ROW_REQUIRED_FIELDS,
            "long-tail single row")
        self.assertIsNone(
            one["result"]["current_spectral_accusation_admissible"])

    def test_transparent_band_via_beets_seam(self):
        """AE2 at the HTTP boundary: a wanted row whose beets copy
        classifies Transparent bands ``transparent`` from its exact resolved
        item snapshot."""
        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id=MB_RELEASE_1))

        beets_db = FakeBeetsDB()
        # MP3 @ 320 kbps classifies TRANSPARENT in the default rank model.
        # 320, not the pre-#1145 256: one MP3 ladder puts the transparent
        # floor at 320 (see docs/quality-ranks.md). The band under test is
        # still ``transparent`` — only the bitrate that earns it moved.
        beets_db.set_mbid_detail(
            MB_RELEASE_1, {"beets_format": "MP3", "beets_bitrate": 194,
                           "beets_avg_bitrate": 320})
        with patch("web.server._beets_db", return_value=beets_db):
            status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 200)
        self.assertEqual(data["results"][0]["band"], "transparent")

    def test_unknown_band_when_in_library_but_unrankable(self):
        """In-library but no detail / unrankable → ``unknown``, not
        ``missing``."""
        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id=MB_RELEASE_1))

        beets_db = FakeBeetsDB()
        beets_db.set_album_exists(MB_RELEASE_1, True)
        with patch("web.server._beets_db", return_value=beets_db):
            status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 200)
        self.assertEqual(data["results"][0]["band"], "unknown")

    def test_in_flight_rescue_stamped(self):
        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id=MB_RELEASE_1))
        self.db.insert_youtube_running(
            request_id=1, browse_id="MPREb_z", audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=z",
            expected_track_count=10,
        )
        status, data = self._get("/api/pipeline/long-tail")
        self.assertEqual(status, 200)
        self.assertTrue(data["results"][0]["in_flight_rescue"])

    def test_band_filter_narrows_result(self):
        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id=MB_RELEASE_1))
        self.db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id=MB_RELEASE_2))
        # The available fake Beets authority explicitly observes both absent.
        status, data = self._get("/api/pipeline/long-tail?band=missing")
        self.assertEqual(status, 200)
        self.assertEqual(data["band"], "missing")
        self.assertEqual({r["id"] for r in data["results"]}, {1, 2})
        # A band with no members returns an empty cohort, still 200.
        status, data = self._get("/api/pipeline/long-tail?band=transparent")
        self.assertEqual(status, 200)
        self.assertEqual(data["count"], 0)

    def test_beets_open_failure_for_discogs_row_never_emits_missing(self):
        """A real missing-file shape returns an error, not Discogs Missing."""
        self.db.seed_request(make_request_row(
            id=1,
            status="wanted",
            mb_release_id=None,
            discogs_release_id=DISCOGS_RELEASE,
            artist_name="Not Known Missing",
            album_title="Discogs Authority Failed",
        ))

        with patch(
            "web.server._beets_db",
            side_effect=FileNotFoundError("Beets DB not found"),
        ):
            status, data = self._get(
                "/api/pipeline/long-tail?band=missing")

        self.assertEqual(status, 503)
        self.assertEqual(data, UNAVAILABLE_ERROR)
        self.assertNotIn("results", data)

    def test_beets_query_failure_for_discogs_row_never_emits_missing(self):
        """A real SQLite query failure remains an authority failure."""
        self.db.seed_request(make_request_row(
            id=1,
            status="wanted",
            mb_release_id=None,
            discogs_release_id=DISCOGS_RELEASE,
        ))
        failure = sqlite3.OperationalError("database is locked")
        failure.sqlite_errorcode = sqlite3.SQLITE_BUSY

        class LockedBeetsDB(FakeBeetsDB):
            def resolve_current_releases(self, identities):
                del identities
                raise failure

        with patch("web.server._beets_db", return_value=LockedBeetsDB()):
            status, data = self._get(
                "/api/pipeline/long-tail?band=missing")

        self.assertEqual(status, 503)
        self.assertEqual(data, UNAVAILABLE_ERROR)
        self.assertNotIn("results", data)

    def test_ambiguous_beets_resolution_returns_conflict_payload(self):
        self.db.seed_request(make_request_row(
            id=1,
            status="wanted",
            mb_release_id=MB_RELEASE_1,
        ))
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(MB_RELEASE_1, [10, 11])

        with patch("web.server._beets_db", return_value=beets):
            status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 409)
        self.assertEqual(data, CONFLICT_ERROR)
        self.assertNotIn("results", data)

    def test_invalid_request_identity_is_a_stable_conflict(self):
        self.db.seed_request(make_request_row(
            id=1,
            status="wanted",
            mb_release_id="not-a-release-id",
            discogs_release_id=None,
        ))

        status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 409)
        self.assertEqual(data, CONFLICT_ERROR)
        self.assertNotIn("results", data)

    def test_omitted_authority_result_is_a_stable_unavailable_error(self):
        self.db.seed_request(make_request_row(
            id=1,
            status="wanted",
            mb_release_id=MB_RELEASE_1,
        ))

        class OmittedAuthorityBeetsDB(FakeBeetsDB):
            def resolve_current_releases(self, identities):
                del identities
                return {}

        with patch(
            "web.server._beets_db",
            return_value=OmittedAuthorityBeetsDB(),
        ):
            status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 503)
        self.assertEqual(data, UNAVAILABLE_ERROR)
        self.assertNotIn("results", data)

    def test_unexpected_sqlite_schema_failure_remains_generic_500(self):
        self.db.seed_request(make_request_row(
            id=1,
            status="wanted",
            mb_release_id=MB_RELEASE_1,
        ))
        failure = sqlite3.OperationalError("no such table: albums")
        failure.sqlite_errorcode = sqlite3.SQLITE_ERROR

        class BrokenSchemaBeetsDB(FakeBeetsDB):
            def resolve_current_releases(self, identities):
                del identities
                raise failure

        with patch("web.server._beets_db", return_value=BrokenSchemaBeetsDB()):
            status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 500)
        self.assertEqual(data, {"error": "Internal server error"})
        self.assertNotIn("results", data)

    def test_empty_cohort_returns_200(self):
        status, data = self._get("/api/pipeline/long-tail")
        self.assertEqual(status, 200)
        self.assertEqual(data["count"], 0)
        self.assertEqual(data["results"], [])

    def test_single_id_returns_one_banded_row(self):
        """KTD8: ``?id=`` returns just that request's authoritative band."""
        from lib.long_tail_service import LongTailRow
        self.db.seed_request(make_request_row(
            id=42, status="wanted", mb_release_id=MB_RELEASE_1,
            artist_name="One", album_title="Row"))
        status, data = self._get("/api/pipeline/long-tail?id=42")
        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"result", "id"},
                                "long-tail single-id envelope")
        self.assertEqual(data["id"], 42)
        row = msgspec.convert(data["result"], type=LongTailRow)
        self.assertEqual(row.id, 42)
        self.assertEqual(row.band, "missing")

    def test_single_id_404_when_not_wanted(self):
        self.db.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=MB_RELEASE_1))
        status, data = self._get("/api/pipeline/long-tail?id=42")
        self.assertEqual(status, 404)
        self.assertEqual(data["id"], 42)

    def test_single_id_400_on_non_int(self):
        status, data = self._get("/api/pipeline/long-tail?id=not-an-int")
        self.assertEqual(status, 400)
        self.assertIn("error", data)


if __name__ == "__main__":
    unittest.main()
