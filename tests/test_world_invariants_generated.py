"""Generated properties for the cross-engine world invariant bank (#743)."""

from __future__ import annotations

import os
import tempfile
import unittest

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from lib.world_invariants import (
    LibraryAlbumSnapshot,
    RequestMembershipSnapshot,
    check_folder_exclusivity,
    check_library_filesystem,
    check_status_membership,
)


_SEGMENT = st.text(
    alphabet=st.characters(
        blacklist_categories=("Cs",),
        blacklist_characters=("/", "\x00"),
    ),
    min_size=1,
    max_size=20,
)


class TestWorldInvariantGenerated(unittest.TestCase):
    @given(release_ids=st.lists(_SEGMENT, min_size=1, max_size=8, unique=True))
    def test_unique_release_folders_are_coherent(self, release_ids: list[str]) -> None:
        albums: list[LibraryAlbumSnapshot] = []
        requests: list[RequestMembershipSnapshot] = []
        for index, release_id in enumerate(release_ids, start=1):
            folder = os.path.join("/library", f"album-{index}")
            albums.append(LibraryAlbumSnapshot(
                album_id=index,
                release_id=release_id,
                album_path=folder,
                item_paths=(os.path.join(folder, "01 Track.flac"),),
            ))
            requests.append(RequestMembershipSnapshot(
                request_id=index,
                release_id=release_id,
                status="imported",
                imported_path=folder,
            ))

        self.assertEqual(check_folder_exclusivity(tuple(albums)), ())
        self.assertEqual(
            check_status_membership(tuple(requests), tuple(albums)),
            (),
        )

    @given(
        release_a=_SEGMENT,
        release_b=_SEGMENT.filter(lambda value: bool(value)),
        folder=_SEGMENT,
    )
    def test_any_shared_folder_is_rejected(
        self,
        release_a: str,
        release_b: str,
        folder: str,
    ) -> None:
        shared = os.path.join("/library", folder)
        violations = check_folder_exclusivity((
            LibraryAlbumSnapshot(1, release_a, shared, (os.path.join(shared, "1.flac"),)),
            LibraryAlbumSnapshot(2, release_b, shared, (os.path.join(shared, "2.flac"),)),
        ))

        self.assertIn("folder_shared", {v.code for v in violations})

    @given(
        album_id=st.integers(min_value=1),
        release_id=_SEGMENT,
        folder=_SEGMENT,
    )
    def test_any_empty_album_is_rejected(
        self,
        album_id: int,
        release_id: str,
        folder: str,
    ) -> None:
        violations = check_folder_exclusivity((LibraryAlbumSnapshot(
            album_id,
            release_id,
            os.path.join("/library", folder),
            (),
        ),))

        self.assertIn("album_empty", {v.code for v in violations})

    @given(release_id=_SEGMENT, folder=_SEGMENT)
    def test_any_missing_physical_album_is_rejected(
        self,
        release_id: str,
        folder: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing = os.path.join(tmpdir, f"missing-{folder}")
            violations = check_library_filesystem((LibraryAlbumSnapshot(
                1,
                release_id,
                missing,
                (os.path.join(missing, "01 Track.flac"),),
            ),))

        self.assertIn("album_folder_missing", {v.code for v in violations})
        self.assertIn("album_item_missing", {v.code for v in violations})

    @given(
        release_id=_SEGMENT,
        imported_path=_SEGMENT,
    )
    def test_imported_without_exact_release_is_always_rejected(
        self,
        release_id: str,
        imported_path: str,
    ) -> None:
        violations = check_status_membership((
            RequestMembershipSnapshot(
                1,
                release_id,
                "imported",
                os.path.join("/library", imported_path),
            ),
        ), ())

        self.assertIn("imported_release_missing", {v.code for v in violations})


if __name__ == "__main__":
    unittest.main()
