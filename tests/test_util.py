"""Tests for lib/util.py — pure utility functions extracted from cratedigger.py."""

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

    def test_works_when_cwd_differs_from_src_parent(self):
        """Bug regression: move_failed_import must work regardless of CWD."""
        from lib.util import move_failed_import
        # Create source in a subdirectory, NOT in CWD
        subdir = os.path.join(self.tmpdir, "staging", "incoming")
        os.makedirs(subdir)
        src = os.path.join(subdir, "Artist - Album (2020)")
        os.makedirs(src)
        open(os.path.join(src, "track.mp3"), "w").close()
        # CWD is self.tmpdir, NOT subdir — this previously broke
        result = move_failed_import(src)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertIn("failed_imports", result)
        self.assertTrue(os.path.isdir(result))
        self.assertFalse(os.path.exists(src))

    def test_bad_files_subdir_for_corrupt(self):
        """audio_corrupt and spectral_reject go to failed_imports/bad_files/."""
        from lib.util import move_failed_import
        src = os.path.join(self.tmpdir, "Artist - Album (2020)")
        os.makedirs(src)
        open(os.path.join(src, "track.mp3"), "w").close()
        result = move_failed_import(src, scenario="audio_corrupt")
        self.assertIsNotNone(result)
        assert result is not None
        self.assertIn(os.path.join("failed_imports", "bad_files"), result)
        self.assertTrue(os.path.isdir(result))

    def test_bad_files_subdir_for_spectral_reject(self):
        from lib.util import move_failed_import
        src = os.path.join(self.tmpdir, "Artist - Album (2020)")
        os.makedirs(src)
        open(os.path.join(src, "track.mp3"), "w").close()
        result = move_failed_import(src, scenario="spectral_reject")
        self.assertIsNotNone(result)
        assert result is not None
        self.assertIn(os.path.join("failed_imports", "bad_files"), result)

    def test_wrong_match_stays_in_failed_imports(self):
        """Non-bad-file scenarios stay in failed_imports/ root."""
        from lib.util import move_failed_import
        src = os.path.join(self.tmpdir, "Artist - Album (2020)")
        os.makedirs(src)
        open(os.path.join(src, "track.mp3"), "w").close()
        result = move_failed_import(src, scenario="high_distance")
        self.assertIsNotNone(result)
        assert result is not None
        self.assertIn("failed_imports", result)
        self.assertNotIn("bad_files", result)

    def test_no_scenario_stays_in_failed_imports(self):
        """No scenario (default) stays in failed_imports/ root."""
        from lib.util import move_failed_import
        src = os.path.join(self.tmpdir, "Artist - Album (2020)")
        os.makedirs(src)
        open(os.path.join(src, "track.mp3"), "w").close()
        result = move_failed_import(src)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertNotIn("bad_files", result)


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
                self.assertIn("-nb", call_args, "must pass -nb to suppress .bak files")
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


class TestValidateAudio(unittest.TestCase):

    def test_ffmpeg_uses_audio_only_map(self):
        """Ensure ffmpeg decodes only audio streams, ignoring embedded art."""
        from lib.util import validate_audio
        tmpdir = tempfile.mkdtemp()
        try:
            open(os.path.join(tmpdir, "track.flac"), "w").close()
            with patch("lib.util.sp.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stderr="")
                validate_audio(tmpdir)
                call_args = mock_run.call_args[0][0]
                # Must have -map 0:a to skip non-audio streams
                self.assertIn("-map", call_args)
                map_idx = call_args.index("-map")
                self.assertEqual(call_args[map_idx + 1], "0:a")
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_ffmpeg_retest_after_md5_fix_uses_audio_only(self):
        """The MD5-fix retest path should also use -map 0:a."""
        from lib.util import validate_audio
        tmpdir = tempfile.mkdtemp()
        try:
            open(os.path.join(tmpdir, "track.flac"), "w").close()
            first_call = MagicMock(returncode=1, stderr="cannot check MD5 signature")
            fix_call = MagicMock(returncode=0, stderr="")
            retest_call = MagicMock(returncode=0, stderr="")
            with patch("lib.util.sp.run", side_effect=[first_call, fix_call, retest_call]):
                validate_audio(tmpdir)
            # Third call is the retest — check it has -map 0:a
            with patch("lib.util.sp.run", side_effect=[first_call, fix_call, retest_call]) as mock_run:
                validate_audio(tmpdir)
                retest_args = mock_run.call_args_list[2][0][0]
                self.assertIn("-map", retest_args)
                map_idx = retest_args.index("-map")
                self.assertEqual(retest_args[map_idx + 1], "0:a")
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_single_bad_file_rejects_album(self):
        """Even 1 corrupt file out of many should reject the album."""
        from lib.util import validate_audio
        tmpdir = tempfile.mkdtemp()
        try:
            # 10 good files, 1 bad
            for i in range(10):
                open(os.path.join(tmpdir, f"good_{i:02d}.mp3"), "w").close()
            open(os.path.join(tmpdir, "bad.mp3"), "w").close()
            good = MagicMock(returncode=0, stderr="")
            bad = MagicMock(returncode=1, stderr="Header missing")

            def side_effect(cmd, **kw):
                filepath = cmd[4]  # ffmpeg -v error -i <filepath> ...
                if os.path.basename(filepath) == "bad.mp3":
                    return bad
                return good

            with patch("lib.util.sp.run", side_effect=side_effect):
                result = validate_audio(tmpdir)
            self.assertFalse(result.valid)
            self.assertEqual(len(result.failed_files), 1)
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

    def _make_cfg(self, url: str | None = "http://meelo:5001", user: str = "u", pw: str = "p"):
        cfg = MagicMock()
        cfg.meelo_url = url
        cfg.meelo_username = user
        cfg.meelo_password = pw
        cfg.resolved_meelo_username.return_value = user
        cfg.resolved_meelo_password.return_value = pw
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

    def _make_cfg(self, url: str | None = "http://meelo:5001", user: str = "u", pw: str = "p"):
        cfg = MagicMock()
        cfg.meelo_url = url
        cfg.meelo_username = user
        cfg.meelo_password = pw
        cfg.resolved_meelo_username.return_value = user
        cfg.resolved_meelo_password.return_value = pw
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


class TestTriggerPlexScan(unittest.TestCase):
    """Tests for trigger_plex_scan()."""

    def _make_cfg(self, url: str | None = "http://plex:32400",
                  token: str | None = "tok123", section: str | None = "3",
                  path_map: str | None = None):
        cfg = MagicMock()
        cfg.plex_url = url
        cfg.plex_token = token
        cfg.plex_library_section_id = section
        cfg.plex_path_map = path_map
        cfg.resolved_plex_token.return_value = token
        return cfg

    @patch("lib.util.urllib.request.urlopen")
    def test_calls_refresh_endpoint(self, mock_urlopen):
        from lib.util import trigger_plex_scan
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        trigger_plex_scan(self._make_cfg(), "/Beets/Artist/Album")
        req = mock_urlopen.call_args[0][0]
        self.assertIn("/library/sections/3/refresh", req.full_url)
        self.assertIn("path=%2FBeets%2FArtist%2FAlbum", req.full_url)
        self.assertIn("X-Plex-Token=tok123", req.full_url)

    @patch("lib.util.urllib.request.urlopen")
    def test_works_without_path(self, mock_urlopen):
        from lib.util import trigger_plex_scan
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        trigger_plex_scan(self._make_cfg())
        req = mock_urlopen.call_args[0][0]
        self.assertIn("/library/sections/3/refresh", req.full_url)
        self.assertNotIn("path=", req.full_url)

    def test_noop_when_no_url(self):
        from lib.util import trigger_plex_scan
        trigger_plex_scan(self._make_cfg(url=None))  # should not raise

    def test_noop_when_no_token(self):
        from lib.util import trigger_plex_scan
        trigger_plex_scan(self._make_cfg(token=None))  # should not raise

    @patch("lib.util.urllib.request.urlopen", side_effect=Exception("connection refused"))
    def test_does_not_raise_on_failure(self, mock_urlopen):
        from lib.util import trigger_plex_scan
        trigger_plex_scan(self._make_cfg(), "/Beets/Artist/Album")  # best-effort


class TestTriggerJellyfinScan(unittest.TestCase):
    """Tests for trigger_jellyfin_scan()."""

    def _make_cfg(self, url: str | None = "http://jelly:8096",
                  token: str | None = "api-key-123",
                  library_id: str | None = None):
        cfg = MagicMock()
        cfg.jellyfin_url = url
        cfg.jellyfin_token = token
        cfg.jellyfin_library_id = library_id
        cfg.resolved_jellyfin_token.return_value = token
        return cfg

    @patch("lib.util.urllib.request.urlopen")
    def test_calls_library_refresh_endpoint(self, mock_urlopen):
        from lib.util import trigger_jellyfin_scan
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        trigger_jellyfin_scan(self._make_cfg())
        req = mock_urlopen.call_args[0][0]
        self.assertIn("/Library/Refresh", req.full_url)
        self.assertEqual(req.get_header("X-emby-token"), "api-key-123")
        self.assertEqual(req.get_method(), "POST")

    @patch("lib.util.urllib.request.urlopen")
    def test_scoped_refresh_with_library_id(self, mock_urlopen):
        from lib.util import trigger_jellyfin_scan
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        trigger_jellyfin_scan(self._make_cfg(library_id="abc123"))
        req = mock_urlopen.call_args[0][0]
        self.assertIn("/Items/abc123/Refresh", req.full_url)
        self.assertEqual(req.get_header("X-emby-token"), "api-key-123")

    def test_noop_when_no_url(self):
        from lib.util import trigger_jellyfin_scan
        trigger_jellyfin_scan(self._make_cfg(url=None))  # should not raise

    def test_noop_when_no_token(self):
        from lib.util import trigger_jellyfin_scan
        trigger_jellyfin_scan(self._make_cfg(token=None))  # should not raise

    @patch("lib.util.urllib.request.urlopen", side_effect=Exception("connection refused"))
    def test_does_not_raise_on_failure(self, mock_urlopen):
        from lib.util import trigger_jellyfin_scan
        trigger_jellyfin_scan(self._make_cfg())  # best-effort, no raise


class TestNotifiersReadSecretsFromFiles(unittest.TestCase):
    """Issue #117: notifier functions must read secrets from *_file paths so
    the rendered config.ini never embeds plaintext credentials.

    These tests use a real CratediggerConfig (not MagicMock) so the resolver
    methods actually run and the file-reading path is exercised end to end.
    """

    def setUp(self):
        from lib.config import invalidate_secret_cache
        invalidate_secret_cache()
        self.addCleanup(invalidate_secret_cache)
        self._tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self._tmpdir, True)

    def _write(self, name: str, value: str) -> str:
        path = os.path.join(self._tmpdir, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(value)
        return path

    @patch("lib.util._meelo_scanner_post")
    @patch("lib.util._meelo_jwt_login", return_value="tok")
    def test_meelo_scan_reads_credentials_from_files(self, mock_login, mock_post):
        from lib.config import CratediggerConfig
        from lib.util import trigger_meelo_scan
        user_path = self._write("meelo-user", "live-user\n")
        pass_path = self._write("meelo-pass", "live-pass\n")
        cfg = CratediggerConfig(
            meelo_url="http://meelo:5001",
            meelo_username_file=user_path,
            meelo_password_file=pass_path,
        )
        trigger_meelo_scan(cfg)
        mock_login.assert_called_once_with("http://meelo:5001", "live-user", "live-pass")
        mock_post.assert_called_once()

    @patch("lib.util._meelo_scanner_post")
    @patch("lib.util._meelo_jwt_login", return_value="tok")
    def test_meelo_clean_reads_credentials_from_files(self, mock_login, mock_post):
        from lib.config import CratediggerConfig
        from lib.util import trigger_meelo_clean
        user_path = self._write("meelo-user", "live-user\n")
        pass_path = self._write("meelo-pass", "live-pass\n")
        cfg = CratediggerConfig(
            meelo_url="http://meelo:5001",
            meelo_username_file=user_path,
            meelo_password_file=pass_path,
        )
        trigger_meelo_clean(cfg)
        mock_login.assert_called_once_with("http://meelo:5001", "live-user", "live-pass")

    @patch("lib.util.urllib.request.urlopen")
    def test_plex_scan_reads_token_from_file(self, mock_urlopen):
        from lib.config import CratediggerConfig
        from lib.util import trigger_plex_scan
        token_path = self._write("plex-token", "plex-live-tok\n")
        cfg = CratediggerConfig(
            plex_url="http://plex:32400",
            plex_token_file=token_path,
            plex_library_section_id="3",
        )
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        trigger_plex_scan(cfg, "/Beets/Artist/Album")
        req = mock_urlopen.call_args[0][0]
        self.assertIn("X-Plex-Token=plex-live-tok", req.full_url)
        self.assertNotIn(token_path, req.full_url)

    @patch("lib.util.urllib.request.urlopen")
    def test_jellyfin_scan_reads_token_from_file(self, mock_urlopen):
        from lib.config import CratediggerConfig
        from lib.util import trigger_jellyfin_scan
        token_path = self._write("jf-token", "jellyfin-live-tok\n")
        cfg = CratediggerConfig(
            jellyfin_url="http://jellyfin:8096",
            jellyfin_token_file=token_path,
        )
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        trigger_jellyfin_scan(cfg)
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.get_header("X-emby-token"), "jellyfin-live-tok")

    def test_plex_scan_skipped_when_token_file_empty_and_no_direct_token(self):
        from lib.config import CratediggerConfig
        from lib.util import trigger_plex_scan
        cfg = CratediggerConfig(plex_url="http://plex:32400")
        # Should not raise, should just skip — no token available.
        trigger_plex_scan(cfg, "/path")

    def test_jellyfin_scan_skipped_when_token_file_empty(self):
        from lib.config import CratediggerConfig
        from lib.util import trigger_jellyfin_scan
        cfg = CratediggerConfig(jellyfin_url="http://jellyfin:8096")
        trigger_jellyfin_scan(cfg)


class TestBeetsSubprocessEnv(unittest.TestCase):
    """beets_subprocess_env() is the single source of truth for the env dict
    used by every subprocess that invokes beets (directly or via the harness
    / import_one.py). Beets reads `~/.config/beets/config.yaml`; when cratedigger
    runs as the systemd service (root, HOME=/root), the Nix Home Manager
    beets config isn't there and the Discogs plugin returns 0 candidates for
    every --search-id. See live failures in download_log for Blueline Medic.
    """

    def test_helper_exists_and_returns_dict(self) -> None:
        from lib.util import beets_subprocess_env
        env = beets_subprocess_env()
        self.assertIsInstance(env, dict)

    def test_home_overridden_to_home_manager_profile(self) -> None:
        """HOME must point to the user profile where beets config lives,
        regardless of what HOME is in the caller's environment."""
        from lib.util import beets_subprocess_env
        with patch.dict(os.environ, {"HOME": "/root"}, clear=False):
            env = beets_subprocess_env()
        self.assertEqual(env["HOME"], "/home/abl030")

    def test_inherits_other_env_vars(self) -> None:
        """Non-HOME vars pass through unchanged — PATH, PYTHONPATH etc. must
        still reach the subprocess."""
        from lib.util import beets_subprocess_env
        sentinel = "CRATEDIGGER_TEST_SENTINEL_VAR_XYZ"
        with patch.dict(os.environ, {sentinel: "present"}, clear=False):
            env = beets_subprocess_env()
        self.assertEqual(env.get(sentinel), "present")

    def test_picks_up_environ_at_call_time(self) -> None:
        """Not a frozen module-level snapshot — os.environ is read fresh
        each call so test-time patching works and any late-set var shows up."""
        from lib.util import beets_subprocess_env
        with patch.dict(os.environ, {"LATE_BOUND_VAR": "first"}, clear=False):
            env1 = beets_subprocess_env()
        with patch.dict(os.environ, {"LATE_BOUND_VAR": "second"}, clear=False):
            env2 = beets_subprocess_env()
        self.assertEqual(env1["LATE_BOUND_VAR"], "first")
        self.assertEqual(env2["LATE_BOUND_VAR"], "second")


if __name__ == "__main__":
    unittest.main()
