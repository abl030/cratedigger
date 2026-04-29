"""Direct tests for shared release-row overlay helpers."""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from web.routes._overlay import overlay_release_rows_in_place


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
            "held": {"beets_format": "FLAC", "beets_bitrate": 1100},
            "both": {"beets_format": "MP3", "beets_bitrate": 320},
            "bad-quality": {"beets_format": None, "beets_bitrate": None},
        }

        def _rank(fmt, bitrate):
            return f"{fmt or 'unknown'}:{bitrate}"

        with patch("web.server.check_beets_library",
                   return_value={"held", "both", "bad-quality"}), \
                patch("web.server.check_pipeline",
                      return_value={
                          "queued": {"id": 21, "status": "wanted"},
                          "both": {"id": 22, "status": "queued"},
                      }), \
                patch("web.server._beets_db", return_value=mock_beets), \
                patch("web.server.compute_library_rank", side_effect=_rank):
            overlay_release_rows_in_place(rows, [r["id"] for r in rows])

        by_id = {row["id"]: row for row in rows}

        self.assertTrue(by_id["held"]["in_library"])
        self.assertEqual(by_id["held"]["beets_album_id"], 10)
        self.assertEqual(by_id["held"]["library_format"], "FLAC")
        self.assertEqual(by_id["held"]["library_min_bitrate"], 1100)
        self.assertEqual(by_id["held"]["library_rank"], "FLAC:1100")
        self.assertIsNone(by_id["held"]["pipeline_status"])

        self.assertFalse(by_id["queued"]["in_library"])
        self.assertIsNone(by_id["queued"]["beets_album_id"])
        self.assertEqual(by_id["queued"]["pipeline_status"], "wanted")
        self.assertEqual(by_id["queued"]["pipeline_id"], 21)

        self.assertTrue(by_id["both"]["in_library"])
        self.assertEqual(by_id["both"]["beets_album_id"], 11)
        self.assertEqual(by_id["both"]["pipeline_status"], "queued")
        self.assertEqual(by_id["both"]["pipeline_id"], 22)

        self.assertFalse(by_id["neither"]["in_library"])
        self.assertIsNone(by_id["neither"]["beets_album_id"])
        self.assertIsNone(by_id["neither"]["pipeline_status"])
        self.assertIsNone(by_id["neither"]["pipeline_id"])

        self.assertEqual(by_id["bad-quality"]["library_format"], "")
        self.assertEqual(by_id["bad-quality"]["library_min_bitrate"], 0)
        self.assertEqual(by_id["bad-quality"]["library_rank"], "unknown:0")

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


if __name__ == "__main__":
    unittest.main()
