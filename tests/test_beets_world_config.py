"""Pins for the shared scratch-Beets shipped-config extraction (#743)."""

from __future__ import annotations

import os
import unittest

from tests.beets_world import extract_shipped_beets_world_config


class TestShippedBeetsWorldConfig(unittest.TestCase):
    def test_extracts_load_bearing_shipped_import_contract(self) -> None:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        shipped = extract_shipped_beets_world_config(repo_root)

        self.assertIn("%aunique{albumartist album,path_disambig}", shipped.default_path_template)
        self.assertEqual(
            dict(shipped.album_fields),
            {
                "path_disambig": (
                    "albumdisambig or releasegroupdisambig or catalognum "
                    "or label or str(year)"
                ),
            },
        )
        self.assertEqual(
            set(shipped.duplicate_album_keys),
            {"mb_albumid", "discogs_albumid"},
        )


if __name__ == "__main__":
    unittest.main()
