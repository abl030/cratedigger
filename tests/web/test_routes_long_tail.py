#!/usr/bin/env python3
"""Contract tests for web/routes/long_tail.py.

Split from tests/web/test_routes_pipeline.py (#481 item 3), which itself
split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import os
import sys
import unittest
from unittest.mock import patch

import msgspec

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _assert_required_fields, _FakeDbWebServerCase

from tests.fakes import FakeBeetsDB
from tests.helpers import make_request_row


class TestLongTailRouteContracts(_FakeDbWebServerCase):
    """U1 contract for ``GET /api/pipeline/long-tail``.

    Wraps ``lib.long_tail_service.list_long_tail`` — the same service
    ``pipeline-cli long-tail`` wraps (CLI ⇄ API symmetry). Drives the
    real service + DB cohort query against a fresh :class:`FakePipelineDB`
    (no service mocking, per MOCKS: LEAF-SEAM ONLY). Banding's beets
    collaborators (``check_beets_library`` / ``_beets_db`` /
    ``compute_library_rank``) are the leaf seam — patched at
    ``web.server`` only when a test exercises an in-library band.
    """

    # The frontend long-tail list renders these fields per row out of the
    # serialized ``LongTailRow``. Pin every one so a rename can't silently
    # break the JS.
    ROW_REQUIRED_FIELDS = {
        "id", "artist_name", "album_title", "year", "status", "source",
        "mb_release_id", "discogs_release_id", "target_format",
        "min_bitrate", "search_filetype_override", "unfindable_category",
        "band", "in_flight_rescue",
        # Card meta (year · MB/Discogs · N tracks) + on-disk spectral strip.
        "track_count", "current_spectral_grade", "current_spectral_bitrate",
        # The accept-sibling control + siblings panel read the rg straight
        # off the row — the single-row refetch must not drop it (#398).
        "mb_release_group_id",
    }
    ENVELOPE_REQUIRED_FIELDS = {"results", "band", "count"}

    def test_missing_row_bands_missing_and_imported_absent(self):
        """AE1 at the HTTP boundary: a wanted row with no beets album
        bands ``missing``; an imported request is absent from the
        result. (No beets configured → everything Missing.)"""
        from lib.long_tail_service import LongTailRow
        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1",
            artist_name="Vanishing", album_title="Lost"))
        self.db.seed_request(make_request_row(
            id=2, status="imported", mb_release_id="rel-2"))

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

    def test_transparent_band_via_beets_seam(self):
        """AE2 at the HTTP boundary: a wanted row whose beets copy
        classifies Transparent bands ``transparent``. The beets leaf
        seam is patched to report the release in-library with a
        lossless detail row."""
        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1"))

        beets_db = FakeBeetsDB()
        # MP3 @ 256 kbps classifies TRANSPARENT in the default rank model
        # (Opus 128 / MP3 V0 are transparent; see docs/quality-ranks.md).
        beets_db.set_mbid_detail(
            "rel-1", {"beets_format": "MP3", "beets_bitrate": 256})
        with patch("web.server.check_beets_library",
                   return_value={"rel-1"}), \
                patch("web.server._beets_db", return_value=beets_db):
            status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 200)
        self.assertEqual(data["results"][0]["band"], "transparent")

    def test_unknown_band_when_in_library_but_unrankable(self):
        """In-library but no detail / unrankable → ``unknown``, not
        ``missing``."""
        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1"))

        beets_db = FakeBeetsDB()  # no detail row seeded
        with patch("web.server.check_beets_library",
                   return_value={"rel-1"}), \
                patch("web.server._beets_db", return_value=beets_db):
            status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 200)
        self.assertEqual(data["results"][0]["band"], "unknown")

    def test_in_flight_rescue_stamped(self):
        self.db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1"))
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
            id=1, status="wanted", mb_release_id="rel-1"))
        self.db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id="rel-2"))
        # No beets → both Missing.
        status, data = self._get("/api/pipeline/long-tail?band=missing")
        self.assertEqual(status, 200)
        self.assertEqual(data["band"], "missing")
        self.assertEqual({r["id"] for r in data["results"]}, {1, 2})
        # A band with no members returns an empty cohort, still 200.
        status, data = self._get("/api/pipeline/long-tail?band=transparent")
        self.assertEqual(status, 200)
        self.assertEqual(data["count"], 0)

    def test_empty_cohort_returns_200(self):
        status, data = self._get("/api/pipeline/long-tail")
        self.assertEqual(status, 200)
        self.assertEqual(data["count"], 0)
        self.assertEqual(data["results"], [])

    def test_single_id_returns_one_banded_row(self):
        """KTD8: ``?id=`` returns just that request's authoritative band."""
        from lib.long_tail_service import LongTailRow
        self.db.seed_request(make_request_row(
            id=42, status="wanted", mb_release_id="rel-42",
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
            id=42, status="imported", mb_release_id="rel-42"))
        status, data = self._get("/api/pipeline/long-tail?id=42")
        self.assertEqual(status, 404)
        self.assertEqual(data["id"], 42)

    def test_single_id_400_on_non_int(self):
        status, data = self._get("/api/pipeline/long-tail?id=not-an-int")
        self.assertEqual(status, 400)
        self.assertIn("error", data)


if __name__ == "__main__":
    unittest.main()
