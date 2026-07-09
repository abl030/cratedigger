"""Tests for lib/permissions.py — umask + recursive chmod helpers (issue #84)."""
from __future__ import annotations

import os
import stat
import tempfile
import unittest

from lib.permissions import LIBRARY_DIR_MODE, fix_library_modes, reset_umask


class TestResetUmask(unittest.TestCase):
    def test_sets_umask_to_group_writable(self):
        prior = os.umask(0o027)
        try:
            reset_umask()
            current = os.umask(0)
            self.assertEqual(current, 0o002)
        finally:
            os.umask(prior)


class TestFixLibraryModes(unittest.TestCase):
    def _make_tree(self, root):
        artist = os.path.join(root, "Artist")
        album = os.path.join(artist, "Album")
        nested = os.path.join(album, "CD1")
        os.makedirs(nested)
        # Seed wrong modes — simulate beets creating 0755 dirs.
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

    def test_dir_mode_is_setgid_and_group_writable(self):
        """The invariant this whole helper exists to guarantee: setgid
        (0o2000) so child dirs inherit the library group, and 0o775 so
        gid-consumers (Jellyfin) can write NFO/artwork alongside media. A
        plain 0o775 with no setgid bit silently defeats the design."""
        with tempfile.TemporaryDirectory() as root:
            _, album, _, _ = self._make_tree(root)
            fix_library_modes(album)
            mode = self._mode(album)
            self.assertTrue(mode & 0o2000, "setgid bit must be set")
            self.assertEqual(mode & 0o775, 0o775, "must be group-writable rwxrwxr-x")
            self.assertEqual(LIBRARY_DIR_MODE, 0o2775)

    def test_recursive_on_subdirs(self):
        with tempfile.TemporaryDirectory() as root:
            _, album, nested, _ = self._make_tree(root)
            fix_library_modes(album)
            self.assertEqual(self._mode(nested), LIBRARY_DIR_MODE)

    def test_does_not_touch_files(self):
        """Issue #84 is about dir accessibility. Files keep their source mode
        (beets' shutil.copystat preserves it from staging). Broadening file
        modes is out of scope and could make private files world-writable."""
        with tempfile.TemporaryDirectory() as root:
            _, album, _, (f1, f2, cover) = self._make_tree(root)
            for p in (f1, f2, cover):
                self.assertEqual(self._mode(p), 0o600)  # seeded mode
            fix_library_modes(album)
            for p in (f1, f2, cover):
                self.assertEqual(self._mode(p), 0o600,
                                 f"{p}: file mode must not be modified")

    def test_nonexistent_path_is_noop(self):
        fix_library_modes("/tmp/cratedigger-does-not-exist-xyz")

    def test_file_path_is_noop(self):
        """Passing a file path should not chmod anything — the helper is
        strictly about library *directories*."""
        with tempfile.TemporaryDirectory() as root:
            f = os.path.join(root, "lone.mp3")
            with open(f, "wb") as fp:
                fp.write(b"x")
            os.chmod(f, 0o600)
            fix_library_modes(f)
            self.assertEqual(self._mode(f), 0o600)


if __name__ == "__main__":
    unittest.main()
