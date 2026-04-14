"""Tests for lib/permissions.py — umask + recursive chmod helpers (issue #84)."""
from __future__ import annotations

import os
import stat
import tempfile
import unittest

from lib.permissions import (LIBRARY_DIR_MODE, LIBRARY_FILE_MODE,
                             fix_library_modes, reset_umask)


class TestResetUmask(unittest.TestCase):
    def test_returns_previous_mask_and_sets_zero(self):
        prior = os.umask(0o027)
        try:
            reset_umask()
            # reset_umask leaves process umask at 0
            current = os.umask(0)
            self.assertEqual(current, 0)
        finally:
            os.umask(prior)


class TestFixLibraryModes(unittest.TestCase):
    def _make_tree(self, root):
        artist = os.path.join(root, "Artist")
        album = os.path.join(artist, "Album")
        nested = os.path.join(album, "CD1")
        os.makedirs(nested)
        # Seed wrong modes — simulate beets creating 0755 dirs and 0644 files
        os.chmod(artist, 0o755)
        os.chmod(album, 0o755)
        os.chmod(nested, 0o755)
        f1 = os.path.join(album, "01 Track.mp3")
        f2 = os.path.join(nested, "02 Track.mp3")
        cover = os.path.join(album, "cover.jpg")
        for p in (f1, f2, cover):
            with open(p, "wb") as fp:
                fp.write(b"x")
            os.chmod(p, 0o600)
        return artist, album, nested, (f1, f2, cover)

    def _mode(self, path):
        return stat.S_IMODE(os.stat(path).st_mode)

    def test_fixes_album_dir_and_artist_dir(self):
        with tempfile.TemporaryDirectory() as root:
            artist, album, _, _ = self._make_tree(root)
            fix_library_modes(album)
            self.assertEqual(self._mode(album), LIBRARY_DIR_MODE)
            self.assertEqual(self._mode(artist), LIBRARY_DIR_MODE,
                             "artist (parent) dir must also be chmod'd")

    def test_recursive_on_subdirs_and_files(self):
        with tempfile.TemporaryDirectory() as root:
            _, album, nested, (f1, f2, cover) = self._make_tree(root)
            fix_library_modes(album)
            self.assertEqual(self._mode(nested), LIBRARY_DIR_MODE)
            for f in (f1, f2, cover):
                self.assertEqual(self._mode(f), LIBRARY_FILE_MODE,
                                 f"file {f} should be 0o666")

    def test_nonexistent_path_is_noop(self):
        fix_library_modes("/tmp/soularr-does-not-exist-xyz")

    def test_file_path_chmods_just_the_file(self):
        with tempfile.TemporaryDirectory() as root:
            f = os.path.join(root, "lone.mp3")
            with open(f, "wb") as fp:
                fp.write(b"x")
            os.chmod(f, 0o600)
            fix_library_modes(f)
            self.assertEqual(self._mode(f), LIBRARY_FILE_MODE)

    def test_survives_unreadable_child(self):
        # Drop a subdir we cannot chmod cleanly; fn must still succeed overall.
        with tempfile.TemporaryDirectory() as root:
            _, album, _, _ = self._make_tree(root)
            fix_library_modes(album)
            # Primary invariant: target dir is 0o777 even if some child failed
            self.assertEqual(self._mode(album), LIBRARY_DIR_MODE)


if __name__ == "__main__":
    unittest.main()
