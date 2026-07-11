"""Generated invariants for bad-rip path and sidecar cleanup."""

from __future__ import annotations

import os
import tempfile
import unittest

from beets import util as beets_util
from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_db import _resolve_library_path
from lib.sidecar import SIDECAR_FILENAME


_COMPONENT = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu", "Nd"),
        whitelist_characters=" -_'’",
    ),
    min_size=1,
    max_size=30,
).filter(lambda value: value not in {".", ".."})


def assert_resolved_path_invariant(
    *, stored_path: str, library_root: str, resolved_path: str,
) -> None:
    """A configured root anchors relative paths and preserves absolutes."""
    expected = (
        stored_path
        if os.path.isabs(stored_path)
        else os.path.join(library_root, stored_path)
    )
    if resolved_path != expected:
        raise AssertionError(
            f"resolved {resolved_path!r}, expected {expected!r}"
        )


def assert_clutter_cleanup_invariant(
    *, had_foreign_file: bool, album_dir_exists: bool,
    sidecar_exists: bool, foreign_file_exists: bool,
) -> None:
    """Only a sidecar-only directory may be pruned."""
    if had_foreign_file:
        if not (album_dir_exists and sidecar_exists and foreign_file_exists):
            raise AssertionError("foreign content was not preserved")
    elif album_dir_exists or sidecar_exists:
        raise AssertionError("sidecar-only directory survived cleanup")


class TestBadRipGenerated(unittest.TestCase):
    @given(
        components=st.lists(_COMPONENT, min_size=2, max_size=5),
        absolute=st.booleans(),
    )
    @example(
        components=[
            "The Rolling Stones",
            "1964 - England's Newest Hit Makers",
            "01 Not Fade Away.opus",
        ],
        absolute=False,
    )
    def test_beets_paths_resolve_for_filesystem_consumers(
        self, components: list[str], absolute: bool,
    ) -> None:
        root = "/mnt/virtio/Music/Beets"
        relative = os.path.join(*components)
        stored = os.path.join(os.sep, "other", relative) if absolute else relative
        resolved = _resolve_library_path(stored, root)
        assert_resolved_path_invariant(
            stored_path=stored,
            library_root=root,
            resolved_path=resolved,
        )

    @given(
        had_foreign_file=st.booleans(),
        foreign_name=_COMPONENT.filter(lambda value: value != SIDECAR_FILENAME),
    )
    @example(had_foreign_file=False, foreign_name="operator.keep")
    @example(had_foreign_file=True, foreign_name="operator.keep")
    def test_beets_clutter_prunes_only_derived_sidecar(
        self, had_foreign_file: bool, foreign_name: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            album_dir = os.path.join(root, "Artist", "Album")
            os.makedirs(album_dir)
            sidecar = os.path.join(album_dir, SIDECAR_FILENAME)
            with open(sidecar, "w", encoding="utf-8") as handle:
                handle.write("{}")
            foreign = os.path.join(album_dir, foreign_name)
            if had_foreign_file:
                with open(foreign, "w", encoding="utf-8") as handle:
                    handle.write("keep")

            beets_util.prune_dirs(
                album_dir,
                root,
                clutter=[SIDECAR_FILENAME],
            )

            assert_clutter_cleanup_invariant(
                had_foreign_file=had_foreign_file,
                album_dir_exists=os.path.isdir(album_dir),
                sidecar_exists=os.path.isfile(sidecar),
                foreign_file_exists=os.path.isfile(foreign),
            )


class TestInvariantCheckersTripOnKnownBad(unittest.TestCase):
    def test_relative_path_left_unanchored_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "expected"):
            assert_resolved_path_invariant(
                stored_path="Artist/Album/01.flac",
                library_root="/music",
                resolved_path="Artist/Album/01.flac",
            )

    def test_sidecar_only_directory_left_behind_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "survived"):
            assert_clutter_cleanup_invariant(
                had_foreign_file=False,
                album_dir_exists=True,
                sidecar_exists=True,
                foreign_file_exists=False,
            )

    def test_foreign_file_deleted_trips(self) -> None:
        with self.assertRaisesRegex(AssertionError, "preserved"):
            assert_clutter_cleanup_invariant(
                had_foreign_file=True,
                album_dir_exists=False,
                sidecar_exists=False,
                foreign_file_exists=False,
            )


if __name__ == "__main__":
    unittest.main()
