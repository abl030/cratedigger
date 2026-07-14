#!/usr/bin/env python3
"""Deletion-specific Plex/Jellyfin notification contracts."""

from __future__ import annotations

import io
from email.message import Message
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import MagicMock, patch

from lib.library_delete_notifiers import (
    _nearest_existing_ancestor,
    notify_library_delete,
)
from lib.util import JellyfinAlbumRef, PlexAlbumRef, request_jellyfin_refresh


class TestDeleteNotifierTargeting(unittest.TestCase):
    def _cfg(self, root: str) -> MagicMock:
        cfg = MagicMock()
        cfg.beets_directory = root
        cfg.plex_url = "http://plex"
        cfg.plex_library_section_id = "3"
        cfg.plex_path_map = f"{root}:/prom_music"
        cfg.resolved_plex_token.return_value = "plex-token"
        cfg.jellyfin_url = "http://jellyfin"
        cfg.jellyfin_library_id = "stale-library-id"
        cfg.jellyfin_path_map = f"{root}:/jf_music"
        cfg.resolved_jellyfin_token.return_value = "jf-token"
        return cfg

    def test_plex_uses_nearest_existing_ancestor_not_deleted_album(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            artist = root / "Artist"
            artist.mkdir()
            former = artist / "Deleted Album"
            cfg = self._cfg(raw)
            submissions: list[str] = []

            def submit(_cfg, path: str):
                submissions.append(path)
                return 200, "/prom_music/Artist"

            outcomes = notify_library_delete(
                cfg,
                str(former),
                plex_find_fn=lambda _cfg, _path: PlexAlbumRef("77", 1),
                plex_scan_fn=submit,
                jellyfin_find_fn=lambda _cfg, _path: None,
                jellyfin_refresh_fn=lambda _cfg, item_id=None: (
                    204, "/Library/Refresh"),
            )

            self.assertEqual(submissions, [str(artist)])
            plex = next(item for item in outcomes if item.provider == "plex")
            self.assertEqual(plex.status, "submitted")
            self.assertIn("not scan proof", plex.detail)

    def test_jellyfin_refreshes_exact_album_item_found_by_former_path(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            former = Path(raw) / "Artist" / "Deleted Album"
            cfg = self._cfg(raw)
            refreshes: list[str | None] = []
            lookups = [
                JellyfinAlbumRef("exact-album", "date"),
                None,
            ]

            def refresh(_cfg, item_id=None):
                refreshes.append(item_id)
                return 204, "/Items/exact-album/Refresh"

            outcomes = notify_library_delete(
                cfg,
                str(former),
                plex_find_fn=lambda _cfg, _path: None,
                plex_scan_fn=lambda _cfg, _path: (200, "/prom_music"),
                jellyfin_find_fn=lambda _cfg, _path: lookups.pop(0),
                jellyfin_refresh_fn=refresh,
            )

            self.assertEqual(refreshes, ["exact-album"])
            jellyfin = next(item for item in outcomes if item.provider == "jellyfin")
            self.assertEqual(jellyfin.status, "submitted")
            self.assertIn("is now absent by former path", jellyfin.detail)

    def test_jellyfin_2xx_with_stale_item_is_a_visible_warning(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            former = Path(raw) / "Artist" / "Deleted Album"
            cfg = self._cfg(raw)
            stale = JellyfinAlbumRef("stale-album", "date")

            outcomes = notify_library_delete(
                cfg,
                str(former),
                plex_find_fn=lambda _cfg, _path: None,
                plex_scan_fn=lambda _cfg, _path: (200, "/prom_music"),
                jellyfin_find_fn=lambda _cfg, _path: stale,
                jellyfin_refresh_fn=lambda _cfg, _item_id: (
                    204, "/Items/stale-album/Refresh"),
            )

            jellyfin = next(item for item in outcomes if item.provider == "jellyfin")
            self.assertEqual(jellyfin.status, "warning")
            self.assertIn("remains observable", jellyfin.detail)

    def test_nearest_ancestor_rejects_out_of_root_path(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            self.assertIsNone(_nearest_existing_ancestor("/outside/album", raw))

    def test_identity_lookup_failure_is_visible_but_refresh_still_runs(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            former = Path(raw) / "Artist" / "Deleted Album"
            cfg = self._cfg(raw)
            plex_refreshes: list[str] = []
            jellyfin_refreshes: list[str | None] = []

            def failed_plex_find(_cfg, _path):
                raise RuntimeError("plex lookup broke")

            def failed_jellyfin_find(_cfg, _path):
                raise RuntimeError("jellyfin lookup broke")

            def plex_refresh(_cfg, path):
                plex_refreshes.append(path)
                return 200, "/prom_music"

            def jellyfin_refresh(_cfg, item_id=None):
                jellyfin_refreshes.append(item_id)
                return 204, "/Library/Refresh"

            outcomes = notify_library_delete(
                cfg,
                str(former),
                plex_find_fn=failed_plex_find,
                plex_scan_fn=plex_refresh,
                jellyfin_find_fn=failed_jellyfin_find,
                jellyfin_refresh_fn=jellyfin_refresh,
            )
            self.assertEqual(len(plex_refreshes), 1)
            self.assertEqual(jellyfin_refreshes, ["stale-library-id"])
            self.assertEqual(
                {item.provider: item.status for item in outcomes},
                {"plex": "warning", "jellyfin": "warning"},
            )
            self.assertTrue(all(
                "identity lookup failed" in item.detail for item in outcomes
            ))


class TestJellyfinRefreshFallback(unittest.TestCase):
    @patch("lib.util.urllib.request.urlopen")
    def test_target_404_falls_back_to_full_library_refresh(self, urlopen) -> None:
        cfg = MagicMock()
        cfg.jellyfin_url = "http://jellyfin"
        cfg.resolved_jellyfin_token.return_value = "token"
        response = MagicMock()
        response.status = 204
        response.read.return_value = b""
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        urlopen.side_effect = [
            urllib.error.HTTPError(
                "http://jellyfin/Items/stale/Refresh", 404, "Not Found", Message(), io.BytesIO(),
            ),
            response,
        ]

        status, target = request_jellyfin_refresh(cfg, item_id="stale") or (0, "")

        self.assertEqual((status, target), (204, "/Library/Refresh"))
        urls = [entry.args[0].full_url for entry in urlopen.call_args_list]
        self.assertEqual(urls, [
            "http://jellyfin/Items/stale/Refresh",
            "http://jellyfin/Library/Refresh",
        ])


if __name__ == "__main__":
    unittest.main()
