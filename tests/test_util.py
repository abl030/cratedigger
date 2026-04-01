"""Tests for lib/util.py — pure utility functions extracted from soularr.py."""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock


class TestSanitizeFolderName(unittest.TestCase):

    def test_strips_invalid_chars(self):
        from lib.util import sanitize_folder_name
        self.assertEqual(sanitize_folder_name('AC/DC - Back:In "Black"'),
                         'ACDC - BackIn Black')

    def test_preserves_valid_name(self):
        from lib.util import sanitize_folder_name
        self.assertEqual(sanitize_folder_name("Radiohead - OK Computer (1997)"),
                         "Radiohead - OK Computer (1997)")

    def test_strips_trailing_whitespace(self):
        from lib.util import sanitize_folder_name
        self.assertEqual(sanitize_folder_name("Album Name   "), "Album Name")


class TestMoveFailedImport(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.orig_cwd = os.getcwd()
        os.chdir(self.tmpdir)

    def tearDown(self):
        os.chdir(self.orig_cwd)
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_moves_to_failed_imports(self):
        from lib.util import move_failed_import
        src = os.path.join(self.tmpdir, "Artist - Album (2020)")
        os.makedirs(src)
        open(os.path.join(src, "track.mp3"), "w").close()
        result = move_failed_import(src)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertIn("failed_imports", result)
        self.assertTrue(os.path.isdir(result))
        self.assertFalse(os.path.exists(src))

    def test_dedup_suffix(self):
        from lib.util import move_failed_import
        folder_name = "Artist - Album (2020)"
        # Create existing failed_imports entry
        failed_dir = os.path.join(self.tmpdir, "failed_imports")
        os.makedirs(os.path.join(failed_dir, folder_name))
        # Create source
        src = os.path.join(self.tmpdir, folder_name)
        os.makedirs(src)
        open(os.path.join(src, "track.mp3"), "w").close()
        result = move_failed_import(src)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertTrue(result.endswith("_1"))

    def test_missing_source_returns_none(self):
        from lib.util import move_failed_import
        result = move_failed_import("/nonexistent/path/album")
        self.assertIsNone(result)


class TestStageToAi(unittest.TestCase):

    def test_moves_files_to_staging(self):
        from lib.util import stage_to_ai
        tmpdir = tempfile.mkdtemp()
        try:
            source = os.path.join(tmpdir, "source")
            staging = os.path.join(tmpdir, "staging")
            os.makedirs(source)
            os.makedirs(staging)
            open(os.path.join(source, "track1.mp3"), "w").close()
            open(os.path.join(source, "track2.mp3"), "w").close()

            album_data = MagicMock()
            album_data.artist = "Artist"
            album_data.title = "Album"

            dest = stage_to_ai(album_data, source, staging)
            self.assertTrue(os.path.isdir(dest))
            self.assertTrue(os.path.exists(os.path.join(dest, "track1.mp3")))
            self.assertTrue(os.path.exists(os.path.join(dest, "track2.mp3")))
            self.assertFalse(os.path.exists(source))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestRepairMp3Headers(unittest.TestCase):

    def test_calls_mp3val_on_mp3_files(self):
        from lib.util import repair_mp3_headers
        tmpdir = tempfile.mkdtemp()
        try:
            open(os.path.join(tmpdir, "track.mp3"), "w").close()
            open(os.path.join(tmpdir, "cover.jpg"), "w").close()
            with patch("lib.util.sp.run") as mock_run:
                mock_run.return_value = MagicMock(stdout="OK", returncode=0)
                repair_mp3_headers(tmpdir)
                # Should only be called for .mp3 files
                self.assertEqual(mock_run.call_count, 1)
                call_args = mock_run.call_args[0][0]
                self.assertEqual(call_args[0], "mp3val")
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_no_mp3val_graceful(self):
        from lib.util import repair_mp3_headers
        tmpdir = tempfile.mkdtemp()
        try:
            open(os.path.join(tmpdir, "track.mp3"), "w").close()
            with patch("lib.util.sp.run", side_effect=FileNotFoundError):
                # Should not raise
                repair_mp3_headers(tmpdir)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestDenylist(unittest.TestCase):

    def test_round_trip(self):
        from lib.util import (load_search_denylist, save_search_denylist,
                              update_search_denylist, is_search_denylisted)
        tmpfile = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        tmpfile.close()
        try:
            dl = load_search_denylist(tmpfile.name)
            self.assertEqual(dl, {})
            update_search_denylist(dl, 42, success=False)
            self.assertEqual(dl["42"]["failures"], 1)
            save_search_denylist(tmpfile.name, dl)
            dl2 = load_search_denylist(tmpfile.name)
            self.assertEqual(dl2["42"]["failures"], 1)
        finally:
            os.unlink(tmpfile.name)

    def test_threshold(self):
        from lib.util import is_search_denylisted, update_search_denylist
        dl = {}
        update_search_denylist(dl, 1, success=False)
        self.assertFalse(is_search_denylisted(dl, 1, max_failures=3))
        update_search_denylist(dl, 1, success=False)
        update_search_denylist(dl, 1, success=False)
        self.assertTrue(is_search_denylisted(dl, 1, max_failures=3))


class TestCleanupDisambiguationOrphans(unittest.TestCase):
    """Tests for cleanup_disambiguation_orphans().

    When beets disambiguates an album (e.g. renames '2009 - Blood Bank' to
    '2009 - Blood Bank [2009]'), it moves audio files but leaves non-audio
    clutter (cover.jpg) in the original directory. This function removes
    those orphaned sibling directories.
    """

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.artist_dir = os.path.join(self.tmpdir, "Bon Iver")
        os.makedirs(self.artist_dir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _make_dir(self, name: str, files: list[str]) -> str:
        d = os.path.join(self.artist_dir, name)
        os.makedirs(d, exist_ok=True)
        for f in files:
            with open(os.path.join(d, f), "w") as fh:
                fh.write("x")
        return d

    def test_removes_orphan_with_only_cover_art(self):
        from lib.util import cleanup_disambiguation_orphans
        imported = self._make_dir("2009 - Blood Bank [2009]",
                                  ["01 Blood Bank.mp3", "cover.jpg"])
        orphan = self._make_dir("2009 - Blood Bank",
                                ["cover.jpg"])
        removed = cleanup_disambiguation_orphans(imported)
        self.assertFalse(os.path.exists(orphan))
        self.assertEqual(removed, [orphan])

    def test_does_not_remove_dir_with_audio_files(self):
        from lib.util import cleanup_disambiguation_orphans
        imported = self._make_dir("2009 - Blood Bank [2009]",
                                  ["01 Blood Bank.mp3", "cover.jpg"])
        other = self._make_dir("2020 - Blood Bank",
                               ["01 Blood Bank.mp3", "cover.jpg"])
        removed = cleanup_disambiguation_orphans(imported)
        self.assertTrue(os.path.exists(other))
        self.assertEqual(removed, [])

    def test_does_not_remove_imported_dir_itself(self):
        from lib.util import cleanup_disambiguation_orphans
        imported = self._make_dir("2009 - Blood Bank [2009]",
                                  ["01 Blood Bank.mp3"])
        removed = cleanup_disambiguation_orphans(imported)
        self.assertTrue(os.path.exists(imported))
        self.assertEqual(removed, [])

    def test_removes_multiple_orphans(self):
        from lib.util import cleanup_disambiguation_orphans
        imported = self._make_dir("2009 - Blood Bank [2009]",
                                  ["01 Blood Bank.mp3"])
        orphan1 = self._make_dir("2009 - Blood Bank",
                                 ["cover.jpg"])
        orphan2 = self._make_dir("2020 - Blood Bank [2020]",
                                 ["Thumbs.DB"])
        removed = cleanup_disambiguation_orphans(imported)
        self.assertFalse(os.path.exists(orphan1))
        self.assertFalse(os.path.exists(orphan2))
        self.assertEqual(sorted(removed), sorted([orphan1, orphan2]))

    def test_empty_dir_is_removed(self):
        from lib.util import cleanup_disambiguation_orphans
        imported = self._make_dir("2009 - Blood Bank [2009]",
                                  ["01 Blood Bank.mp3"])
        orphan = os.path.join(self.artist_dir, "2009 - Blood Bank")
        os.makedirs(orphan)
        removed = cleanup_disambiguation_orphans(imported)
        self.assertFalse(os.path.exists(orphan))
        self.assertEqual(removed, [orphan])

    def test_preserves_dir_with_flac(self):
        from lib.util import cleanup_disambiguation_orphans
        imported = self._make_dir("2009 - Blood Bank [2009]",
                                  ["01 Blood Bank.mp3"])
        other = self._make_dir("2009 - Blood Bank",
                               ["01 Blood Bank.flac"])
        removed = cleanup_disambiguation_orphans(imported)
        self.assertTrue(os.path.exists(other))
        self.assertEqual(removed, [])

    def test_nonexistent_imported_path_returns_empty(self):
        from lib.util import cleanup_disambiguation_orphans
        removed = cleanup_disambiguation_orphans("/nonexistent/path/album")
        self.assertEqual(removed, [])

    def test_preserves_dir_with_mixed_audio_and_clutter(self):
        from lib.util import cleanup_disambiguation_orphans
        imported = self._make_dir("2009 - Blood Bank [2009]",
                                  ["01 Blood Bank.mp3"])
        other = self._make_dir("2009 - Blood Bank",
                               ["cover.jpg", "01 Track.m4a"])
        removed = cleanup_disambiguation_orphans(imported)
        self.assertTrue(os.path.exists(other))
        self.assertEqual(removed, [])

    def test_ignores_files_in_artist_dir(self):
        """Files directly in the artist dir should not cause errors."""
        from lib.util import cleanup_disambiguation_orphans
        imported = self._make_dir("2009 - Blood Bank [2009]",
                                  ["01 Blood Bank.mp3"])
        # Put a file directly in the artist dir
        with open(os.path.join(self.artist_dir, "artist.nfo"), "w") as f:
            f.write("x")
        orphan = self._make_dir("2009 - Blood Bank", ["cover.jpg"])
        removed = cleanup_disambiguation_orphans(imported)
        self.assertFalse(os.path.exists(orphan))
        self.assertEqual(removed, [orphan])


class TestMeeloJwtLogin(unittest.TestCase):
    """Tests for _meelo_jwt_login()."""

    @patch("lib.util.urllib.request.urlopen")
    def test_returns_jwt_on_success(self, mock_urlopen):
        from lib.util import _meelo_jwt_login
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"access_token": "tok123"}'
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp
        jwt = _meelo_jwt_login("http://meelo:5001", "user", "pass")
        self.assertEqual(jwt, "tok123")

    @patch("lib.util.urllib.request.urlopen")
    def test_posts_correct_credentials(self, mock_urlopen):
        from lib.util import _meelo_jwt_login
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"access_token": "x"}'
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp
        _meelo_jwt_login("http://meelo:5001", "myuser", "mypass")
        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data)
        self.assertEqual(body["username"], "myuser")
        self.assertEqual(body["password"], "mypass")


class TestTriggerMeeloScan(unittest.TestCase):
    """Tests for trigger_meelo_scan()."""

    def _make_cfg(self, url="http://meelo:5001", user="u", pw="p"):
        cfg = MagicMock()
        cfg.meelo_url = url
        cfg.meelo_username = user
        cfg.meelo_password = pw
        return cfg

    @patch("lib.util._meelo_jwt_login", return_value="tok")
    @patch("lib.util._meelo_scanner_post")
    def test_calls_scan_endpoint(self, mock_post, mock_login):
        from lib.util import trigger_meelo_scan
        trigger_meelo_scan(self._make_cfg())
        mock_post.assert_called_once_with(
            "http://meelo:5001", "tok", "/scanner/scan?library=beets")

    def test_noop_when_no_url(self):
        from lib.util import trigger_meelo_scan
        cfg = self._make_cfg(url=None)
        trigger_meelo_scan(cfg)  # should not raise


class TestTriggerMeeloClean(unittest.TestCase):
    """Tests for trigger_meelo_clean()."""

    def _make_cfg(self, url="http://meelo:5001", user="u", pw="p"):
        cfg = MagicMock()
        cfg.meelo_url = url
        cfg.meelo_username = user
        cfg.meelo_password = pw
        return cfg

    @patch("lib.util._meelo_jwt_login", return_value="tok")
    @patch("lib.util._meelo_scanner_post")
    def test_calls_clean_endpoint(self, mock_post, mock_login):
        from lib.util import trigger_meelo_clean
        trigger_meelo_clean(self._make_cfg())
        mock_post.assert_called_once_with(
            "http://meelo:5001", "tok", "/scanner/clean?library=beets")

    def test_noop_when_no_url(self):
        from lib.util import trigger_meelo_clean
        cfg = self._make_cfg(url=None)
        trigger_meelo_clean(cfg)  # should not raise

    @patch("lib.util._meelo_jwt_login", side_effect=Exception("auth failed"))
    def test_does_not_raise_on_failure(self, mock_login):
        from lib.util import trigger_meelo_clean
        trigger_meelo_clean(self._make_cfg())  # best-effort, no raise


if __name__ == "__main__":
    unittest.main()
