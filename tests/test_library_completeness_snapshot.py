"""Atomic snapshot and prior-preservation contracts for #1149."""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch

from lib.library_completeness import (
    CompletenessCounts,
    CompletenessReport,
    LibraryAlbum,
)
from lib.library_completeness_snapshot import (
    LibraryCompletenessSnapshot,
    read_library_completeness_snapshot,
    write_library_completeness_snapshot,
)
from scripts import run_library_completeness_census as census
from scripts.run_library_completeness_census import publish_library_completeness_census


def _snapshot() -> LibraryCompletenessSnapshot:
    return LibraryCompletenessSnapshot(
        "2026-08-17T00:00:00+00:00", 1.0,
        CompletenessReport("complete", CompletenessCounts(0, 0, 0, 0, 0), ()),
    )


class TestLibraryCompletenessSnapshot(unittest.TestCase):
    def test_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "library-completeness.json")
            snapshot = _snapshot()
            write_library_completeness_snapshot(path, snapshot)
            self.assertEqual(read_library_completeness_snapshot(path), snapshot)

    def test_beets_unavailable_does_not_replace_prior_snapshot(self) -> None:
        class LockedBeets:
            def list_library_completeness_albums(self) -> list[LibraryAlbum]:
                exc = sqlite3.OperationalError("locked")
                exc.sqlite_errorcode = sqlite3.SQLITE_LOCKED
                raise exc
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "library-completeness.json")
            prior = _snapshot()
            write_library_completeness_snapshot(path, prior)
            result = publish_library_completeness_census(
                path, LockedBeets(), fetch_musicbrainz_raw=lambda _: {},
                fetch_discogs_raw=lambda _: {},
            )
            self.assertEqual(result.report.status, "beets_unavailable")
            self.assertEqual(read_library_completeness_snapshot(path), prior)

    def test_failed_atomic_replace_preserves_prior_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "library-completeness.json")
            prior = _snapshot()
            write_library_completeness_snapshot(path, prior)
            newer = LibraryCompletenessSnapshot(
                "2026-08-18T00:00:00+00:00", 2.0,
                CompletenessReport("unknown", CompletenessCounts(1, 0, 0, 0, 1), ()),
            )

            # A real directory permission failure exercises the
            # same-directory-tempfile path without patching our atomic-write
            # helper. The old snapshot must remain the only published file.
            os.chmod(tmpdir, 0o500)
            try:
                with self.assertRaises(PermissionError):
                    write_library_completeness_snapshot(path, newer)
            finally:
                os.chmod(tmpdir, 0o700)

            self.assertEqual(read_library_completeness_snapshot(path), prior)
            self.assertEqual(
                [name for name in os.listdir(tmpdir) if name != "library-completeness.json"],
                [],
            )

    def test_per_album_unknown_is_published_not_hidden(self) -> None:
        class OneAlbum:
            def list_library_completeness_albums(self) -> list[LibraryAlbum]:
                return [LibraryAlbum(1, "Artist", "Album", None, "", ())]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "library-completeness.json")
            result = publish_library_completeness_census(
                path, OneAlbum(), fetch_musicbrainz_raw=lambda _: {},
                fetch_discogs_raw=lambda _: {},
            )
            self.assertEqual(result.report.status, "unknown")
            self.assertEqual(read_library_completeness_snapshot(path), result)

    def test_main_wires_redirect_proof_from_admitted_runtime_config(self) -> None:
        cfg = SimpleNamespace(var_dir="/runtime")
        resolver = lambda _release_id: None
        with (
            patch.object(sys, "argv", ["census"]),
            patch.object(census, "enforce_beets_startup", return_value=cfg),
            patch.object(census, "configure_api_bases_from_runtime_config"),
            patch.object(census, "configure_canonical_release_lookup") as configure_canonical,
            patch.object(census, "production_tagged_canonical_release_fn", return_value=resolver),
            patch.object(census, "library_completeness_snapshot_path", return_value="/runtime/snapshot"),
            patch.object(census, "open_beets_db", return_value=nullcontext(object())),
            patch.object(census, "publish_library_completeness_census", return_value=_snapshot()) as publish,
        ):
            self.assertEqual(census.main(), 0)
        configure_canonical.assert_called_once_with(cfg)
        self.assertIs(
            publish.call_args.kwargs["resolve_musicbrainz_redirect"], resolver,
        )


if __name__ == "__main__":
    unittest.main()
