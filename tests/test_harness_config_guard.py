"""Startup config assertion for beets `import.duplicate_keys.album`.

Guards against the 2026-04-20 Palo Santo misconfig class: a top-level
`duplicate_keys =` in the user's beets config.yaml is silently ignored by
beets (it reads strictly from `config["import"]["duplicate_keys"]["album"]`),
falling back to the default `[albumartist, album]` — no `mb_albumid`.
`find_duplicates()` then matches cross-MBID sibling pressings on album title
alone, enabling the harness's duplicate-remove answer to destroy the sibling's
files via beets' `task.should_remove_duplicates = True` blast radius.

The assertion lives at harness startup so the misconfig surfaces immediately
rather than at the next import that happens to hit a sibling pressing.

The live 2026-04-27 guarded-replacement deploy showed the other half of the
same config boundary: keeping `albumartist`/`album` in the duplicate key is too
strict for upgrades with normalized metadata drift. Exact release ids only are
what make Beets ask the harness before replacement, but the harness must map
Beets provider metadata (`album_id`) into library metadata (`mb_albumid`) before
Beets builds that duplicate query.
"""

from __future__ import annotations

import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock


# beets isn't installed in the test nix-shell — mock it before import.
_beets_mocks = {
    "beets": MagicMock(),
    "beets.config": MagicMock(),
    "beets.library": MagicMock(),
    "beets.plugins": MagicMock(),
    "beets.importer": MagicMock(),
    "beets.importer.session": MagicMock(),
    "beets.importer.tasks": MagicMock(),
    "beets.ui": MagicMock(),
}
for name, mock in _beets_mocks.items():
    sys.modules.setdefault(name, mock)

# ImportSession needs to be a class so subclassing works.
setattr(sys.modules["beets.importer.session"], "ImportSession",
        type("ImportSession", (object,), {}))

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from harness import beets_harness  # noqa: E402


def _make_cfg(keys: list[str]):
    """Build a stand-in for the beets `config` object with
    `config["import"]["duplicate_keys"]["album"].as_str_seq() -> keys`."""
    album_view = SimpleNamespace(as_str_seq=lambda: list(keys))
    dup_keys = {"album": album_view}
    import_section = {"duplicate_keys": dup_keys}
    return {"import": import_section}


class TestDuplicateKeysGuard(unittest.TestCase):

    def test_accepts_correct_config(self):
        cfg = _make_cfg(["mb_albumid", "discogs_albumid"])
        # Does not raise.
        beets_harness._assert_duplicate_keys_include_mb_albumid(cfg)

    def test_rejects_default_fallback(self):
        # The silent-fallback shape that caused Palo Santo.
        cfg = _make_cfg(["albumartist", "album"])
        with self.assertRaises(SystemExit) as ctx:
            beets_harness._assert_duplicate_keys_include_mb_albumid(cfg)
        self.assertEqual(ctx.exception.code, 1)

    def test_rejects_empty_list(self):
        cfg = _make_cfg([])
        with self.assertRaises(SystemExit):
            beets_harness._assert_duplicate_keys_include_mb_albumid(cfg)

    def test_rejects_mb_albumid_misspelled(self):
        # Catches a typo that would otherwise be structurally accepted.
        cfg = _make_cfg(["albumartist", "album", "mb_album_id"])
        with self.assertRaises(SystemExit):
            beets_harness._assert_duplicate_keys_include_mb_albumid(cfg)

    def test_rejects_mutable_artist_title_keys(self):
        cfg = _make_cfg(["albumartist", "album", "mb_albumid"])
        with self.assertRaises(SystemExit):
            beets_harness._assert_duplicate_keys_include_mb_albumid(cfg)

    def test_rejects_missing_discogs_albumid(self):
        cfg = _make_cfg(["mb_albumid"])
        with self.assertRaises(SystemExit):
            beets_harness._assert_duplicate_keys_include_mb_albumid(cfg)

    def test_error_message_names_palo_santo(self):
        # Future-me debugging a rebuild that fails should see the reference
        # without having to grep commit history.
        from io import StringIO
        captured = StringIO()
        cfg = _make_cfg(["albumartist", "album"])
        old_stderr = sys.stderr
        sys.stderr = captured
        try:
            with self.assertRaises(SystemExit):
                beets_harness._assert_duplicate_keys_include_mb_albumid(cfg)
        finally:
            sys.stderr = old_stderr
        self.assertIn("Palo Santo", captured.getvalue())
        self.assertIn("duplicate_keys", captured.getvalue())
        self.assertIn("exactly", captured.getvalue())


class TestDuplicateLookupMetadata(unittest.TestCase):

    def test_uses_album_info_item_data_mapping(self):
        class FakeAlbumInfo:
            item_data = {
                "albumartist": "The National",
                "album": "High Violet",
                "mb_albumid": "mb-123",
                "discogs_albumid": 0,
            }

        task = SimpleNamespace(chosen_info=lambda: FakeAlbumInfo())

        data = beets_harness._duplicate_lookup_metadata(task)

        self.assertEqual(data["mb_albumid"], "mb-123")
        self.assertEqual(data["discogs_albumid"], 0)
        self.assertEqual(data["albumartist"], "The National")

    def test_maps_raw_album_id_to_mb_albumid(self):
        task = SimpleNamespace(chosen_info=lambda: {
            "artist": "The National",
            "album": "High Violet",
            "album_id": "mb-123",
        })

        data = beets_harness._duplicate_lookup_metadata(task)

        self.assertEqual(data["mb_albumid"], "mb-123")
        self.assertEqual(data["albumartist"], "The National")

    def test_find_duplicates_queries_mapped_release_fields(self):
        class FakeAlbumInfo:
            item_data = {
                "albumartist": "The National",
                "album": "High Violet",
                "mb_albumid": "mb-123",
                "discogs_albumid": 0,
            }

        class FakeAlbum:
            last = None

            def __init__(self, lib, **kwargs):
                self.kwargs = kwargs
                FakeAlbum.last = self

            def duplicates_query(self, keys):
                self.keys = list(keys)
                return ("query", tuple(keys), self.kwargs)

        duplicate = SimpleNamespace(
            items=lambda: [SimpleNamespace(path=b"/beets/old/01.opus")])
        lib = MagicMock()
        lib.albums.return_value = [duplicate]
        task = SimpleNamespace(
            chosen_info=lambda: FakeAlbumInfo(),
            items=[SimpleNamespace(path=b"/incoming/new/01.opus")],
        )

        old_config = beets_harness.config
        old_album = beets_harness.library.Album
        beets_harness.config = _make_cfg(["mb_albumid", "discogs_albumid"])
        beets_harness.library.Album = FakeAlbum
        try:
            duplicates = beets_harness._find_duplicates_with_mapped_release_ids(
                task, lib)
        finally:
            beets_harness.config = old_config
            beets_harness.library.Album = old_album

        self.assertEqual(duplicates, [duplicate])
        self.assertEqual(FakeAlbum.last.kwargs["mb_albumid"], "mb-123")
        self.assertEqual(FakeAlbum.last.kwargs["discogs_albumid"], 0)
        self.assertEqual(FakeAlbum.last.keys,
                         ["mb_albumid", "discogs_albumid"])


if __name__ == "__main__":
    unittest.main()
