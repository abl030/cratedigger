"""Deletion-specific Plex/Jellyfin notification contracts."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from lib.library_delete_notifiers import (
    _nearest_existing_ancestor,
    notify_library_delete,
)
from lib.util import JellyfinAlbumRef, PlexAlbumRef


def _cfg(root: str) -> MagicMock:
    cfg = MagicMock()
    cfg.beets_directory = root
    cfg.plex_url = "http://plex"
    cfg.plex_library_section_id = "3"
    cfg.plex_path_map = f"{root}:/prom_music"
    cfg.resolved_plex_token.return_value = "plex-token"
    cfg.jellyfin_url = "http://jellyfin"
    cfg.jellyfin_path_map = f"{root}:/jf_music"
    cfg.resolved_jellyfin_token.return_value = "jf-token"
    return cfg


class TestDeleteNotifierTargeting(unittest.TestCase):
    def test_plex_uses_nearest_existing_ancestor_not_deleted_album(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            artist = root / "Artist"
            artist.mkdir()
            former = artist / "Deleted Album"
            cfg = _cfg(raw)
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
            )

            self.assertEqual(submissions, [str(artist)])
            plex = next(item for item in outcomes if item.provider == "plex")
            self.assertEqual(plex.status, "submitted")
            self.assertIn("not scan proof", plex.detail)

    def test_jellyfin_destructive_lane_reports_found_item_without_refresh(
        self,
    ) -> None:
        """Regression pin for issue #1221 item 1: the destructive-delete
        caller (``allow_escalation=True``, the default) reports a found
        Jellyfin item exactly like the post-import lane — it never
        refreshes it. The retired find → refresh → re-observe behavior can
        never produce this report shape: it refreshed and reported
        ``submitted``/``warning`` keyed on absence observation or the
        refresh outcome, so under it this pin goes RED (verified against
        the pre-change code: with no refresh fake supplied, the default
        refresh attempt fails and yields a warning with an empty target,
        so the exact-item target assertion fails first)."""
        with tempfile.TemporaryDirectory() as raw:
            former = Path(raw) / "Artist" / "Deleted Album"
            cfg = _cfg(raw)

            outcomes = notify_library_delete(
                cfg,
                str(former),
                plex_find_fn=lambda _cfg, _path: None,
                plex_scan_fn=lambda _cfg, path: (200, path),
                jellyfin_find_fn=(
                    lambda _cfg, _path: JellyfinAlbumRef("exact-album", "date")),
            )

            jellyfin = next(
                item for item in outcomes if item.provider == "jellyfin")
            self.assertEqual(jellyfin.status, "warning")
            self.assertEqual(jellyfin.target, "exact-album")
            self.assertIn("exact-album", jellyfin.detail)
            self.assertIn(str(former), jellyfin.detail)
            self.assertIn("NOT refreshed", jellyfin.detail)
            self.assertIn("next library validation", jellyfin.detail)

    def test_jellyfin_outcome_is_identical_across_both_lanes(self) -> None:
        """Lane parity (issue #1221 item 1): for the same world, the
        destructive caller and the post-import reconciler get the SAME
        Jellyfin ``DeleteNotification`` — ``allow_escalation`` governs only
        the Plex root-scan escalation."""
        with tempfile.TemporaryDirectory() as raw:
            former = Path(raw) / "Artist" / "Deleted Album"
            for found in (True, False):
                ref = JellyfinAlbumRef("exact-album", "date") if found else None
                per_lane = [
                    next(
                        item for item in notify_library_delete(
                            _cfg(raw),
                            str(former),
                            allow_escalation=allow_escalation,
                            plex_find_fn=lambda _cfg, _path: None,
                            plex_scan_fn=lambda _cfg, path: (200, path),
                            jellyfin_find_fn=lambda _cfg, _path, _ref=ref: _ref,
                        ) if item.provider == "jellyfin")
                    for allow_escalation in (True, False)
                ]
                with self.subTest(found=found):
                    self.assertEqual(per_lane[0], per_lane[1])

    def test_nearest_ancestor_rejects_out_of_root_path(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            self.assertIsNone(_nearest_existing_ancestor("/outside/album", raw))

    def test_identity_lookup_failure_is_visible(self) -> None:
        """A failed identity lookup is surfaced as a ``warning`` on both
        legs. Plex still submits its ancestor scan (the scan needs no
        identity); Jellyfin cannot claim "no item found" — it does not
        know — so its warning names the failure instead."""
        with tempfile.TemporaryDirectory() as raw:
            former = Path(raw) / "Artist" / "Deleted Album"
            cfg = _cfg(raw)
            plex_scans: list[str] = []

            def failed_find(_cfg, _path):
                raise RuntimeError("lookup broke")

            outcomes = notify_library_delete(
                cfg,
                str(former),
                plex_find_fn=failed_find,
                plex_scan_fn=lambda _cfg, path: (
                    plex_scans.append(path) or (200, path)),
                jellyfin_find_fn=failed_find,
            )
            self.assertEqual(len(plex_scans), 1)
            self.assertEqual(
                {item.provider: item.status for item in outcomes},
                {"plex": "warning", "jellyfin": "warning"},
            )
            self.assertTrue(all(
                "identity lookup failed" in item.detail for item in outcomes
            ))
            jellyfin = next(
                item for item in outcomes if item.provider == "jellyfin")
            self.assertNotIn("no Jellyfin item found", jellyfin.detail)


class TestDeleteNotifierEscalationRefusal(unittest.TestCase):
    """``allow_escalation=False`` (issue #1203 item 2): a routine post-import
    reconciliation caller must never fall back to a Plex library-root scan
    — only the operator-authorized destructive-delete caller (the default,
    ``allow_escalation=True``) may. The flag governs ONLY that Plex
    escalation: since issue #1221 item 1 the Jellyfin leg is
    detect-and-report for every caller and has no refresh machinery at all
    (see ``lib.library_delete_notifiers.notify_library_delete``'s own
    docstring for the source-level reason a targeted refresh cannot reap a
    vanished item and would instead delete its child rows)."""

    def test_plex_refuses_a_library_root_scan(self) -> None:
        """The sole-album-artist-rename world: neither the vanished album
        folder NOR its parent artist folder survives, so the only existing
        ancestor is the configured root itself — the forbidden escalation."""
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            former = root / "Artist" / "Deleted Album"
            cfg = _cfg(raw)
            submissions: list[str] = []

            outcomes = notify_library_delete(
                cfg,
                str(former),
                allow_escalation=False,
                plex_find_fn=lambda _cfg, _path: None,
                plex_scan_fn=lambda _cfg, path: (
                    submissions.append(path) or (200, path)),
                jellyfin_find_fn=lambda _cfg, _path: None,
            )

            self.assertEqual(submissions, [])
            plex = next(item for item in outcomes if item.provider == "plex")
            self.assertEqual(plex.status, "skipped")
            self.assertIn("library-root scan", plex.detail)

    def test_plex_root_scan_still_allowed_for_the_destructive_caller(
        self,
    ) -> None:
        """Must-still-work: the operator-authorized destructive caller
        (``allow_escalation=True``) may scan the configured root itself when
        no narrower ancestor survives."""
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            former = root / "Artist" / "Deleted Album"
            cfg = _cfg(raw)
            submissions: list[str] = []

            outcomes = notify_library_delete(
                cfg,
                str(former),
                allow_escalation=True,
                plex_find_fn=lambda _cfg, _path: None,
                plex_scan_fn=lambda _cfg, path: (
                    submissions.append(path) or (200, path)),
                jellyfin_find_fn=lambda _cfg, _path: None,
            )

            self.assertEqual(submissions, [str(root)])
            plex = next(item for item in outcomes if item.provider == "plex")
            self.assertEqual(plex.status, "submitted")

    def test_plex_still_scans_a_narrower_surviving_ancestor(self) -> None:
        """Must-still-work: forbidding escalation does not forbid the
        ordinary narrower-ancestor scan (e.g. the artist folder) that a
        genuine path-changing rename leaves intact."""
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            artist = root / "Artist"
            artist.mkdir()
            former = artist / "Deleted Album"
            cfg = _cfg(raw)
            submissions: list[str] = []

            outcomes = notify_library_delete(
                cfg,
                str(former),
                allow_escalation=False,
                plex_find_fn=lambda _cfg, _path: PlexAlbumRef("77", 1),
                plex_scan_fn=lambda _cfg, path: (
                    submissions.append(path) or (200, path)),
                jellyfin_find_fn=lambda _cfg, _path: None,
            )

            self.assertEqual(submissions, [str(artist)])
            plex = next(item for item in outcomes if item.provider == "plex")
            self.assertEqual(plex.status, "submitted")

    def test_jellyfin_reports_not_found_as_skipped(self) -> None:
        """No item found by former path: report it and stop — there is no
        refresh machinery to fall back to (issue #1221 item 1)."""
        with tempfile.TemporaryDirectory() as raw:
            former = Path(raw) / "Artist" / "Deleted Album"
            cfg = _cfg(raw)

            outcomes = notify_library_delete(
                cfg,
                str(former),
                allow_escalation=False,
                plex_find_fn=lambda _cfg, _path: None,
                plex_scan_fn=lambda _cfg, path: (200, path),
                jellyfin_find_fn=lambda _cfg, _path: None,
            )

            jellyfin = next(
                item for item in outcomes if item.provider == "jellyfin")
            self.assertEqual(jellyfin.status, "skipped")
            self.assertIn("no Jellyfin item found", jellyfin.detail)

    def test_jellyfin_found_item_is_reported_not_refreshed(self) -> None:
        """Issue #1203 item 2 review (source-level finding against Jellyfin
        10.11): a targeted refresh of a stale album can never reap it —
        deletion is computed by the PARENT folder's own child-set diff, not
        the item's own refresh — and the vanished directory makes that
        refresh instead empty the item's child rows. A FOUND item is
        therefore reported, never refreshed (the same contract the
        destructive caller now shares — issue #1221 item 1)."""
        with tempfile.TemporaryDirectory() as raw:
            former = Path(raw) / "Artist" / "Deleted Album"
            cfg = _cfg(raw)

            outcomes = notify_library_delete(
                cfg,
                str(former),
                allow_escalation=False,
                plex_find_fn=lambda _cfg, _path: None,
                plex_scan_fn=lambda _cfg, path: (200, path),
                jellyfin_find_fn=(
                    lambda _cfg, _path: JellyfinAlbumRef("exact-album", "date")),
            )

            jellyfin = next(
                item for item in outcomes if item.provider == "jellyfin")
            self.assertEqual(jellyfin.status, "warning")
            self.assertEqual(jellyfin.target, "exact-album")
            self.assertIn("exact-album", jellyfin.detail)
            self.assertIn(str(former), jellyfin.detail)
            self.assertIn("NOT refreshed", jellyfin.detail)


if __name__ == "__main__":
    unittest.main()
