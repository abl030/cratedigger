"""Direct tests for shared release-row overlay helpers."""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from web.routes._overlay import band_release_ids, overlay_release_rows_in_place


class TestOverlayReleaseRowsInPlace(unittest.TestCase):
    def test_populates_library_and_pipeline_state(self):
        rows = [
            {"id": "held", "title": "Held Release"},
            {"id": "queued", "title": "Queued Release"},
            {"id": "both", "title": "Both Release"},
            {"id": "neither", "title": "Neither Release"},
            {"id": "bad-quality", "title": "Bad Quality Release"},
        ]
        mock_beets = MagicMock()
        mock_beets.get_album_ids_by_mbids.return_value = {
            "held": 10,
            "both": 11,
            "bad-quality": 12,
        }
        mock_beets.check_mbids_detail.return_value = {
            "held": {"beets_format": "FLAC", "beets_bitrate": 900,
                     "beets_avg_bitrate": 1100},
            "both": {"beets_format": "MP3", "beets_bitrate": 194,
                     "beets_avg_bitrate": 288},
            "bad-quality": {"beets_format": None, "beets_bitrate": None,
                            "beets_avg_bitrate": None},
        }

        with patch("web.server.check_beets_library",
                   return_value={"held", "both", "bad-quality"}), \
                patch("web.server.check_pipeline",
                      return_value={
                          "queued": {"id": 21, "status": "wanted",
                                     "verified_lossless": False,
                                     "provisional_lossless": True},
                          "both": {"id": 22, "status": "queued",
                                   "verified_lossless": True,
                                   "provisional_lossless": False},
                      }), \
                patch("web.server._beets_db", return_value=mock_beets):
            overlay_release_rows_in_place(rows, [r["id"] for r in rows])

        by_id = {row["id"]: row for row in rows}

        self.assertTrue(by_id["held"]["in_library"])
        self.assertEqual(by_id["held"]["beets_album_id"], 10)
        self.assertEqual(by_id["held"]["library_format"], "FLAC")
        self.assertEqual(by_id["held"]["library_min_bitrate"], 900)
        self.assertEqual(by_id["held"]["library_avg_bitrate"], 1100)
        # Real compute_library_rank — 1100kbps FLAC is lossless.
        self.assertEqual(by_id["held"]["library_rank"], "lossless")
        self.assertIsNone(by_id["held"]["pipeline_status"])

        self.assertFalse(by_id["queued"]["in_library"])
        self.assertIsNone(by_id["queued"]["beets_album_id"])
        self.assertEqual(by_id["queued"]["pipeline_status"], "wanted")
        self.assertEqual(by_id["queued"]["pipeline_id"], 21)
        self.assertFalse(by_id["queued"]["pipeline_verified_lossless"])
        self.assertTrue(by_id["queued"]["pipeline_provisional"])
        self.assertFalse(by_id["held"]["pipeline_verified_lossless"])
        self.assertFalse(by_id["held"]["pipeline_provisional"])

        self.assertTrue(by_id["both"]["in_library"])
        self.assertEqual(by_id["both"]["beets_album_id"], 11)
        self.assertEqual(by_id["both"]["library_min_bitrate"], 194)
        self.assertEqual(by_id["both"]["library_avg_bitrate"], 288)
        self.assertEqual(by_id["both"]["library_rank"], "transparent")
        self.assertEqual(by_id["both"]["pipeline_status"], "queued")
        self.assertEqual(by_id["both"]["pipeline_id"], 22)
        self.assertTrue(by_id["both"]["pipeline_verified_lossless"])
        self.assertFalse(by_id["both"]["pipeline_provisional"])

        self.assertFalse(by_id["neither"]["in_library"])
        self.assertIsNone(by_id["neither"]["beets_album_id"])
        self.assertIsNone(by_id["neither"]["pipeline_status"])
        self.assertIsNone(by_id["neither"]["pipeline_id"])

        self.assertEqual(by_id["bad-quality"]["library_format"], "")
        self.assertEqual(by_id["bad-quality"]["library_min_bitrate"], 0)
        # Real compute_library_rank — empty format/bitrate is unknown.
        self.assertEqual(by_id["bad-quality"]["library_rank"], "unknown")

    def test_empty_inputs_do_not_touch_backends(self):
        with patch("web.server.check_beets_library") as check_lib, \
                patch("web.server.check_pipeline") as check_pipeline, \
                patch("web.server._beets_db", return_value=None):
            overlay_release_rows_in_place([], [])

        check_lib.assert_not_called()
        check_pipeline.assert_not_called()

    def test_missing_row_id_raises_key_error(self):
        with patch("web.server.check_beets_library", return_value=set()), \
                patch("web.server.check_pipeline", return_value={}), \
                patch("web.server._beets_db", return_value=None):
            with self.assertRaises(KeyError):
                overlay_release_rows_in_place([{"title": "No ID"}], [])


class TestBandReleaseIds(unittest.TestCase):
    def test_degrades_to_missing_on_beets_error(self):
        """Beets unavailable (locked / missing DB) → all-"missing" rather than
        propagating the exception (which would 500 the worklist). Matches the
        CLI's _cli_band_fn fallback (REL-002)."""
        with patch("web.server.check_beets_library",
                   side_effect=OSError("db locked")):
            out = band_release_ids(["rel-1", "rel-2"])
        self.assertEqual(out, {"rel-1": "missing", "rel-2": "missing"})

    def test_bands_three_way_from_membership_and_detail(self):
        """Direct coverage of the three-way (missing / unknown / band) the
        long-tail worklist depends on (previously only indirect via the route
        contract test)."""
        mock_beets = MagicMock()
        mock_beets.check_mbids_detail.return_value = {
            "on-disk": {"beets_format": "FLAC", "beets_bitrate": 900,
                        "beets_avg_bitrate": 1100},
            "no-detail": {},
        }
        with patch("web.server.check_beets_library",
                   return_value={"on-disk", "no-detail"}), \
                patch("web.server._beets_db", return_value=mock_beets):
            out = band_release_ids(["on-disk", "no-detail", "gone"])
        self.assertEqual(out["on-disk"], "lossless")   # FLAC 1100 → lossless
        self.assertEqual(out["no-detail"], "unknown")  # in library, no detail
        self.assertEqual(out["gone"], "missing")       # absent from membership


if __name__ == "__main__":
    unittest.main()
