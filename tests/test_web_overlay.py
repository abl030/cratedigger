"""Unit tests for web/overlay.py — overlay logic with explicit deps (#432).

The functions here used to live in web/server.py behind module globals;
these tests exercise them directly with fakes, no server wiring. The
server-side bindings (thread-local handle injection) stay covered by
tests/web/test_server_threading.py::TestProductionWiringOverlays.
"""
import datetime
import unittest

from web import overlay

from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row


class TestSerializeRow(unittest.TestCase):
    def test_datetimes_become_iso_strings(self):
        ts = datetime.datetime(2026, 6, 12, 1, 2, 3,
                               tzinfo=datetime.timezone.utc)
        out = overlay.serialize_row({"id": 1, "created_at": ts, "x": None})
        self.assertEqual(out["created_at"], ts.isoformat())
        self.assertEqual(out["id"], 1)
        self.assertIsNone(out["x"])


class TestCheckPipeline(unittest.TestCase):
    def test_none_handle_degrades_to_empty(self):
        self.assertEqual(overlay.check_pipeline(None, ["mbid-1"]), {})

    def test_empty_mbids_short_circuits(self):
        db = FakePipelineDB()
        self.assertEqual(overlay.check_pipeline(db, []), {})

    def test_returns_info_keyed_by_mbid(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=7, mb_release_id="mbid-1", status="wanted",
            search_filetype_override="lossless", min_bitrate=900))
        info = overlay.check_pipeline(db, ["mbid-1", "mbid-2"])
        self.assertEqual(set(info), {"mbid-1"})
        self.assertEqual(info["mbid-1"]["id"], 7)
        self.assertEqual(info["mbid-1"]["status"], "wanted")
        self.assertEqual(
            info["mbid-1"]["search_filetype_override"], "lossless")


class TestBeetsHelpers(unittest.TestCase):
    def test_none_beets_degrades(self):
        self.assertEqual(overlay.check_beets_library(None, ["m"]), set())
        self.assertEqual(overlay.check_beets_library_detail(None, ["m"]), {})
        self.assertEqual(overlay.get_library_artist(None, "X"), [])

    def test_check_beets_library_coerces_ids_to_str(self):
        beets = FakeBeetsDB()
        beets.set_album_exists("123", True)
        # check_mbids on the fake: assert the call records str ids.
        overlay.check_beets_library(beets, [123])
        self.assertEqual(beets.check_mbids_calls, [["123"]])


class TestComputeLibraryRank(unittest.TestCase):
    def test_returns_lowercase_rank_label(self):
        rank = overlay.compute_library_rank("FLAC", 1000)
        self.assertEqual(rank, rank.lower())
        self.assertIsInstance(rank, str)


if __name__ == "__main__":
    unittest.main()
