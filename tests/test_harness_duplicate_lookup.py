"""Release-ID mapping for the Beets duplicate lookup patch."""

from __future__ import annotations

import os
import sys
import unittest
from collections.abc import Iterable
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import MagicMock

_beets_mocks = {
    "beets": MagicMock(),
    "beets.config": MagicMock(),
    "beets.library": MagicMock(),
    "beets.plugins": MagicMock(),
    "beets.importer": MagicMock(),
    "beets.importer.actions": MagicMock(),
    "beets.importer.session": MagicMock(),
    "beets.importer.tasks": MagicMock(),
    "beets.autotag": MagicMock(),
    "beets.dbcore": MagicMock(),
    "beets.util": MagicMock(),
}
for name, mock in _beets_mocks.items():
    sys.modules.setdefault(name, mock)

setattr(  # noqa: B010 - populate a synthetic runtime module
    sys.modules["beets.importer.session"],
    "ImportSession",
    type("ImportSession", (object,), {}),
)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from harness import beets_harness


def _make_cfg(keys: list[str]) -> dict[str, object]:
    album_view = SimpleNamespace(as_str_seq=lambda: list(keys))
    return {
        "import": {"duplicate_keys": {"album": album_view}},
    }


class TestDuplicateLookupMetadata(unittest.TestCase):
    def test_uses_album_info_item_data_mapping(self) -> None:
        class FakeAlbumInfo:
            item_data: ClassVar = {
                "albumartist": "The National",
                "album": "High Violet",
                "mb_albumid": "mb-123",
                "discogs_albumid": 0,
            }

        task = SimpleNamespace(chosen_info=lambda: FakeAlbumInfo())

        data = beets_harness._duplicate_lookup_metadata(
            task,  # pyright: ignore[reportArgumentType]
        )

        self.assertEqual(data["mb_albumid"], "mb-123")
        self.assertEqual(data["discogs_albumid"], 0)
        self.assertEqual(data["albumartist"], "The National")

    def test_maps_raw_album_id_to_mb_albumid(self) -> None:
        task = SimpleNamespace(chosen_info=lambda: {
            "artist": "The National",
            "album": "High Violet",
            "album_id": "mb-123",
        })

        data = beets_harness._duplicate_lookup_metadata(
            task,  # pyright: ignore[reportArgumentType]
        )

        self.assertEqual(data["mb_albumid"], "mb-123")
        self.assertEqual(data["albumartist"], "The National")

    def test_find_duplicates_queries_mapped_release_fields(self) -> None:
        class FakeAlbumInfo:
            item_data: ClassVar = {
                "albumartist": "The National",
                "album": "High Violet",
                "mb_albumid": "mb-123",
                "discogs_albumid": 0,
            }

        class FakeAlbum:
            last = None

            kwargs: dict[str, object]
            keys: list[str]

            def __init__(self, lib: object, **kwargs: object) -> None:
                del lib
                self.kwargs = kwargs
                FakeAlbum.last = self

            def duplicates_query(
                self,
                keys: Iterable[str],
            ) -> tuple[str, tuple[str, ...], dict[str, object]]:
                self.keys = list(keys)
                return ("query", tuple(self.keys), self.kwargs)

        duplicate = SimpleNamespace(
            items=lambda: [SimpleNamespace(path=b"/beets/old/01.opus")],
        )
        lib = MagicMock()
        lib.albums.return_value = [duplicate]
        task = SimpleNamespace(
            chosen_info=lambda: FakeAlbumInfo(),
            items=[SimpleNamespace(path=b"/incoming/new/01.opus")],
        )

        old_config = beets_harness.config
        old_album = beets_harness.library.Album
        beets_harness.config = _make_cfg(
            ["mb_albumid", "discogs_albumid"],
        )
        beets_harness.library.Album = FakeAlbum
        try:
            duplicates = beets_harness._find_duplicates_with_mapped_release_ids(
                task,  # pyright: ignore[reportArgumentType]
                lib,
            )
        finally:
            beets_harness.config = old_config
            beets_harness.library.Album = old_album

        self.assertEqual(duplicates, [duplicate])
        last = FakeAlbum.last
        assert last is not None
        self.assertEqual(last.kwargs["mb_albumid"], "mb-123")
        self.assertEqual(last.kwargs["discogs_albumid"], 0)
        self.assertEqual(last.keys, ["mb_albumid", "discogs_albumid"])


if __name__ == "__main__":
    unittest.main()
