"""Tests for lib/util.py — pure utility functions extracted from cratedigger.py."""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock


class TestMoveFailedImport(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.orig_cwd = os.getcwd()
        os.chdir(self.tmpdir)

    def tearDown(self):
        os.chdir(self.orig_cwd)
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_abandoned_auto_import_uses_prefixed_folder(self):
        from lib.util import move_abandoned_auto_import
        src = os.path.join(self.tmpdir, "Album [request-42]")
        os.makedirs(src)
        open(os.path.join(src, "01.opus"), "w").close()

        result = move_abandoned_auto_import(src)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIn("failed_imports", result)
        self.assertTrue(os.path.basename(result).startswith("abandoned_auto_import"))
        self.assertIn("Album [request-42]", os.path.basename(result))
        self.assertTrue(os.path.exists(os.path.join(result, "01.opus")))
        self.assertFalse(os.path.exists(src))
        self.assertNotIn("bad_files", result)

    def test_abandoned_auto_import_dedup_suffix(self):
        from lib.util import move_abandoned_auto_import
        src = os.path.join(self.tmpdir, "Album [request-42]")
        os.makedirs(src)
        failed_dir = os.path.join(self.tmpdir, "failed_imports")
        os.makedirs(os.path.join(
            failed_dir,
            "abandoned_auto_import - Album [request-42]",
        ))

        result = move_abandoned_auto_import(src)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertTrue(result.endswith("_1"))

    def test_abandoned_auto_import_missing_source_returns_none(self):
        from lib.util import move_abandoned_auto_import
        result = move_abandoned_auto_import("/nonexistent/path/album")
        self.assertIsNone(result)


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

    def test_flac_md5_repair_still_runs_when_ffmpeg_exits_zero(self):
        """FLAC MD5 repair is driven by the stderr signature, not exit status."""
        from lib.util import validate_audio
        tmpdir = tempfile.mkdtemp()
        try:
            open(os.path.join(tmpdir, "track.flac"), "w").close()
            first_call = MagicMock(returncode=0, stderr="cannot check MD5 signature")
            fix_call = MagicMock(returncode=0, stderr="")
            retest_call = MagicMock(returncode=0, stderr="")
            with patch("lib.util.sp.run", side_effect=[first_call, fix_call, retest_call]) as mock_run:
                result = validate_audio(tmpdir)
            self.assertTrue(result.valid)
            self.assertEqual(mock_run.call_count, 3)
            self.assertEqual(mock_run.call_args_list[1][0][0][0], "flac")
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_attached_picture_mimetype_warning_does_not_reject_audio(self):
        """Malformed cover-art metadata should not be treated as audio corruption."""
        from lib.util import validate_audio
        tmpdir = tempfile.mkdtemp()
        try:
            open(os.path.join(tmpdir, "track.flac"), "w").close()
            with patch("lib.util.sp.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=0,
                    stderr=("[flac @ 0xdeadbeef] "
                            "Could not read mimetype from an attached picture.\n"),
                )
                result = validate_audio(tmpdir)
            self.assertTrue(result.valid)
            self.assertEqual(result.failed_files, [])
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


class TestValidateAudioStderrPolicy(unittest.TestCase):
    """#251 regressions: stderr is informational; only ffmpeg rc != 0 rejects.

    ffmpeg's exit code is the authoritative signal that the audio stream failed
    to decode. Demuxer/parser warnings about metadata, attached pictures, or
    recoverable frame-level glitches are emitted to stderr but ffmpeg still
    returns rc=0 — those albums decode fine and must not be flagged as
    ``audio_corrupt``. Each false-positive case below is sourced from issue
    #251's matrix; the real-corruption cases pair them with documented
    rc != 0 stderr we MUST still reject.
    """

    # (description, returncode, stderr) — rc=0 cases must produce corrupt_files=[]
    FALSE_POSITIVE_CASES = [
        ("empty_stderr_happy_path", 0, ""),
        (
            "mp3float_backstep_recovery",
            0,
            "[mp3float @ 0xdeadbeef] invalid new backstep -1",
        ),
        (
            "bom_lyrics_id3_skipped",
            0,
            "[id3v2 @ 0xdeadbeef] Incorrect BOM value\n"
            "Error reading lyrics, skipped",
        ),
        (
            "bom_comment_frame_id3_skipped",
            0,
            "[id3v2 @ 0xdeadbeef] Incorrect BOM value\n"
            "Error reading comment frame, skipped",
        ),
        (
            "mjpeg_app_fields_warning",
            0,
            "[mjpeg @ 0xdeadbeef] unable to decode APP fields: "
            "Invalid data found when processing input",
        ),
        (
            "attached_picture_mimetype_warning",
            0,
            "[flac @ 0xdeadbeef] Could not read mimetype from an attached "
            "picture.",
        ),
    ]

    # (description, returncode, stderr) — rc != 0 cases MUST still reject
    REAL_CORRUPTION_CASES = [
        (
            "invalid_sync_code_decode_failure",
            1,
            "[mp3 @ 0xdeadbeef] invalid sync code\n"
            "[mp3 @ 0xdeadbeef] invalid frame header\n"
            "decode_frame() failed",
        ),
        (
            "illegal_residual_coding_method",
            1,
            "[flac @ 0xdeadbeef] illegal residual coding method 2",
        ),
        (
            "invalid_residual",
            1,
            "[flac @ 0xdeadbeef] invalid residual",
        ),
    ]

    def _run_validate(self, returncode: int, stderr: str):
        from lib.util import validate_audio
        tmpdir = tempfile.mkdtemp()
        try:
            open(os.path.join(tmpdir, "track.flac"), "w").close()
            with patch("lib.util.sp.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=returncode, stderr=stderr,
                )
                return validate_audio(tmpdir)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_false_positive_stderr_does_not_reject(self):
        for desc, rc, stderr in self.FALSE_POSITIVE_CASES:
            with self.subTest(case=desc):
                result = self._run_validate(rc, stderr)
                self.assertTrue(
                    result.valid,
                    f"{desc}: rc={rc} stderr={stderr!r} should be accepted, "
                    f"got error={result.error!r}",
                )
                self.assertEqual(
                    result.failed_files, [],
                    f"{desc}: expected empty failed_files",
                )

    def test_real_corruption_still_rejects(self):
        for desc, rc, stderr in self.REAL_CORRUPTION_CASES:
            with self.subTest(case=desc):
                result = self._run_validate(rc, stderr)
                self.assertFalse(
                    result.valid,
                    f"{desc}: rc={rc} stderr={stderr!r} must reject",
                )
                self.assertEqual(
                    len(result.failed_files), 1,
                    f"{desc}: expected one failed file",
                )


class TestTriggerPlexScan(unittest.TestCase):
    """Tests for trigger_plex_scan()."""

    def _make_cfg(self, url: str | None = "http://plex:32400",
                  token: str | None = "tok123", section: str | None = "3",
                  path_map: str | None = None,
                  beets_directory: str = ""):
        cfg = MagicMock()
        cfg.plex_url = url
        cfg.plex_token = token
        cfg.plex_library_section_id = section
        cfg.plex_path_map = path_map
        cfg.beets_directory = beets_directory
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

    @patch("lib.util.urllib.request.urlopen")
    def test_path_map_anchors_relative_imported_path(self, mock_urlopen):
        """beets.get_album_info() returns paths relative to the beets library
        root. The path_map must re-anchor relative paths under the container
        prefix so Plex can resolve them to a library section location."""
        from lib.util import trigger_plex_scan
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        cfg = self._make_cfg(path_map="/mnt/virtio/Music/Beets:/prom_music")
        trigger_plex_scan(cfg, "Artist/Album")
        req = mock_urlopen.call_args[0][0]
        self.assertIn("path=%2Fprom_music%2FArtist%2FAlbum", req.full_url)

    @patch("lib.util.urllib.request.urlopen")
    def test_path_map_substitutes_absolute_imported_path(self, mock_urlopen):
        """Regression guard for the original April-2 fix: absolute paths under
        the local prefix must still be substituted to the container prefix."""
        from lib.util import trigger_plex_scan
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        cfg = self._make_cfg(path_map="/mnt/virtio/Music/Beets:/prom_music")
        trigger_plex_scan(cfg, "/mnt/virtio/Music/Beets/Artist/Album")
        req = mock_urlopen.call_args[0][0]
        self.assertIn("path=%2Fprom_music%2FArtist%2FAlbum", req.full_url)

    @patch("lib.util.urllib.request.urlopen")
    def test_log_shows_substituted_path_not_raw_input(self, mock_urlopen):
        """The trigger's success log should reflect what was actually sent to
        Plex (post path_map substitution), not the raw input. Otherwise a
        future regression that breaks substitution would still log a
        success-looking line, repeating the silent failure that motivated PR
        #236."""
        import logging
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        cfg = self._make_cfg(path_map="/mnt/virtio/Music/Beets:/prom_music")
        from lib.util import trigger_plex_scan
        with self.assertLogs("cratedigger", level=logging.INFO) as captured:
            trigger_plex_scan(cfg, "Artist/Album")
        joined = "\n".join(captured.output)
        self.assertIn("/prom_music/Artist/Album", joined)

    @patch("lib.util.urllib.request.urlopen")
    def test_dot_relative_path_anchored_under_container_prefix(self, mock_urlopen):
        """./Artist/Album is technically relative (os.path.isabs returns
        False), so the relative-anchor branch should still apply rather than
        falling through to the warning."""
        from lib.util import trigger_plex_scan
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        cfg = self._make_cfg(path_map="/mnt/virtio/Music/Beets:/prom_music")
        trigger_plex_scan(cfg, "./Artist/Album")
        req = mock_urlopen.call_args[0][0]
        # Plex normalizes /prom_music/./Artist internally; we just need to
        # confirm we sent something rooted under /prom_music, not the raw
        # ./ prefix without anchoring.
        self.assertIn("path=%2Fprom_music%2F", req.full_url)

    @patch("lib.util.urllib.request.urlopen")
    def test_beets_directory_absolutizes_relative_path_for_bare_metal_plex(self, mock_urlopen):
        """Bare-metal Plex (no Docker, no path_map) must still get an absolute
        path. With cfg.beets_directory set and no path_map, the relative path
        from beets gets joined with beets_directory before being sent."""
        from lib.util import trigger_plex_scan
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        cfg = self._make_cfg(path_map=None,
                             beets_directory="/srv/music")
        trigger_plex_scan(cfg, "Artist/Album")
        req = mock_urlopen.call_args[0][0]
        self.assertIn("path=%2Fsrv%2Fmusic%2FArtist%2FAlbum", req.full_url)

    @patch("lib.util.urllib.request.urlopen")
    def test_beets_directory_composes_with_path_map(self, mock_urlopen):
        """When BOTH beets_directory and path_map are set (typical Docker
        deployment): absolutize against beets_directory FIRST, then path_map
        translates host→container. Output should be the container path."""
        from lib.util import trigger_plex_scan
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        cfg = self._make_cfg(path_map="/srv/music:/container/music",
                             beets_directory="/srv/music")
        trigger_plex_scan(cfg, "Artist/Album")
        req = mock_urlopen.call_args[0][0]
        self.assertIn("path=%2Fcontainer%2Fmusic%2FArtist%2FAlbum", req.full_url)

    @patch("lib.util.urllib.request.urlopen")
    def test_warns_when_relative_and_no_absolutize_config(self, mock_urlopen):
        """Final defensive guard: relative path with NO path_map AND NO
        beets_directory should log a warning, since Plex can't resolve it."""
        from lib.util import trigger_plex_scan
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        cfg = self._make_cfg(path_map=None, beets_directory="")
        with self.assertLogs("cratedigger", level="WARNING") as captured:
            trigger_plex_scan(cfg, "Artist/Album")
        self.assertTrue(
            any("relative" in m.lower() for m in captured.output),
            f"Expected warning about relative path, got: {captured.output}",
        )

    @patch("lib.util.urllib.request.urlopen")
    def test_empty_imported_path_skips_path_arg(self, mock_urlopen):
        """An empty string is falsy and should result in a full library scan
        (no path arg), matching the imported_path=None case."""
        from lib.util import trigger_plex_scan
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        cfg = self._make_cfg(path_map="/mnt/virtio/Music/Beets:/prom_music")
        trigger_plex_scan(cfg, "")
        req = mock_urlopen.call_args[0][0]
        self.assertNotIn("path=", req.full_url)

    @patch("lib.util.urllib.request.urlopen")
    def test_path_map_warns_when_absolute_path_unmappable(self, mock_urlopen):
        """Defensive log: if path_map is configured but the path is absolute
        AND outside the local prefix, we can't translate it to a container
        path. Warn instead of silently sending an unresolvable path."""
        from lib.util import trigger_plex_scan
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        cfg = self._make_cfg(path_map="/mnt/virtio/Music/Beets:/prom_music")
        with self.assertLogs("cratedigger", level="WARNING") as captured:
            trigger_plex_scan(cfg, "/some/other/absolute/Album")
        self.assertTrue(
            any("PLEX" in m and "path_map" in m for m in captured.output),
            f"Expected PLEX path_map warning, got: {captured.output}",
        )


class TestTriggerJellyfinScan(unittest.TestCase):
    """Tests for trigger_jellyfin_scan()."""

    def _make_cfg(self, url: str | None = "http://jelly:8096",
                  token: str | None = "api-key-123"):
        cfg = MagicMock()
        cfg.jellyfin_url = url
        cfg.jellyfin_token = token
        cfg.beets_directory = "/mnt/virtio/Music/Beets"
        cfg.jellyfin_path_map = (
            "/mnt/virtio/Music/Beets:/mnt/fuse/Media/Music/Beets"
        )
        cfg.resolved_jellyfin_token.return_value = token
        return cfg

    @patch("lib.util.urllib.request.urlopen")
    def test_reports_only_the_changed_album_path(self, mock_urlopen):
        from lib.util import trigger_jellyfin_scan
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        trigger_jellyfin_scan(
            self._make_cfg(),
            "/mnt/virtio/Music/Beets/Artist/2026 - Album",
        )
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.full_url, "http://jelly:8096/Library/Media/Updated")
        self.assertEqual(req.get_header("X-emby-token"), "api-key-123")
        self.assertEqual(req.get_header("Content-type"), "application/json")
        self.assertEqual(req.get_method(), "POST")
        self.assertEqual(
            json.loads(req.data),
            {"Updates": [{
                "Path": "/mnt/fuse/Media/Music/Beets/Artist/2026 - Album",
                "UpdateType": "Modified",
            }]},
        )
        self.assertNotIn("/Items/", req.full_url)
        self.assertNotEqual(req.full_url, "http://jelly:8096/Library/Refresh")

    def test_noop_when_no_url(self):
        from lib.util import trigger_jellyfin_scan
        trigger_jellyfin_scan(self._make_cfg(url=None), "Artist/Album")

    def test_noop_when_no_token(self):
        from lib.util import trigger_jellyfin_scan
        trigger_jellyfin_scan(self._make_cfg(token=None), "Artist/Album")

    @patch("lib.util.urllib.request.urlopen")
    def test_noop_when_album_path_cannot_be_mapped(self, mock_urlopen):
        from lib.util import trigger_jellyfin_scan
        trigger_jellyfin_scan(self._make_cfg(), "/outside/library/Album")
        mock_urlopen.assert_not_called()

    @patch("lib.util.urllib.request.urlopen", side_effect=Exception("connection refused"))
    def test_does_not_raise_on_failure(self, mock_urlopen):
        from lib.util import trigger_jellyfin_scan
        trigger_jellyfin_scan(self._make_cfg(), "Artist/Album")


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
            beets_directory="/mnt/virtio/Music/Beets",
            jellyfin_url="http://jellyfin:8096",
            jellyfin_token_file=token_path,
            jellyfin_path_map=(
                "/mnt/virtio/Music/Beets:/mnt/fuse/Media/Music/Beets"
            ),
        )
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b""
        mock_urlopen.return_value = mock_resp
        trigger_jellyfin_scan(cfg, "Artist/Album")
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
        trigger_jellyfin_scan(cfg, "Artist/Album")


class TestPlexAddedAtPinClient(unittest.TestCase):
    """Read/edit half of the Plex 'Recently Added' pin (migration 040).
    Path translation + find-by-path matching are driven through injected
    fetch/put seams; the urllib leaf is asserted once via the real PUT path."""

    def _cfg(self, **kw):
        from lib.config import CratediggerConfig
        return CratediggerConfig(**kw)

    def test_container_path_absolutize_then_path_map(self):
        from lib.util import _plex_container_path
        cfg = self._cfg(
            beets_directory="/mnt/virtio/Music/Beets",
            plex_path_map="/mnt/virtio/Music/Beets:/prom_music")
        self.assertEqual(
            _plex_container_path(cfg, "Artist/Album"),
            "/prom_music/Artist/Album")
        self.assertEqual(
            _plex_container_path(cfg, "/mnt/virtio/Music/Beets/X/Y"),
            "/prom_music/X/Y")

    def test_container_path_relative_anchored_under_container_prefix(self):
        from lib.util import _plex_container_path
        cfg = self._cfg(plex_path_map="/mnt/virtio/Music/Beets:/prom_music")
        self.assertEqual(
            _plex_container_path(cfg, "Artist/Album"),
            "/prom_music/Artist/Album")

    def test_container_path_none_when_not_absolutizable(self):
        from lib.util import _plex_container_path
        cfg = self._cfg()  # no beets_directory, no path_map
        self.assertIsNone(_plex_container_path(cfg, "Artist/Album"))
        self.assertIsNone(_plex_container_path(cfg, ""))

    def _fetch(self, search_xml: str, children_xml: str):
        import xml.etree.ElementTree as ET

        def _fn(path, **params):
            if "/search" in path:
                return ET.fromstring(search_xml)
            if "/children" in path:
                return ET.fromstring(children_xml)
            return ET.fromstring("<MediaContainer/>")
        return _fn

    def test_find_album_by_path_matches_on_part_prefix(self):
        from lib.util import plex_find_album_by_path
        cfg = self._cfg(
            plex_url="http://plex:32400",
            beets_directory="/mnt/virtio/Music/Beets",
            plex_path_map="/mnt/virtio/Music/Beets:/prom_music",
            plex_library_section_id="3")
        search = (
            '<MediaContainer><Directory type="album" ratingKey="458495" '
            'title="The Wow! Signal" parentTitle="Muse" '
            'addedAt="1782611948"/></MediaContainer>')
        children = (
            '<MediaContainer><Track><Media><Part '
            'file="/prom_music/Muse/2026 - The Wow! Signal/01 a.opus"/>'
            '</Media></Track></MediaContainer>')
        ref = plex_find_album_by_path(
            cfg, "Muse/2026 - The Wow! Signal",
            fetch_xml=self._fetch(search, children))
        self.assertIsNotNone(ref)
        assert ref is not None
        self.assertEqual(ref.rating_key, "458495")
        self.assertEqual(ref.added_at, 1782611948)
        self.assertEqual(ref.title, "The Wow! Signal")
        self.assertEqual(ref.artist, "Muse")

    def test_find_album_by_path_none_when_parts_dont_match(self):
        from lib.util import plex_find_album_by_path
        cfg = self._cfg(
            plex_url="http://plex:32400",
            beets_directory="/mnt/virtio/Music/Beets",
            plex_path_map="/mnt/virtio/Music/Beets:/prom_music",
            plex_library_section_id="3")
        search = (
            '<MediaContainer><Directory type="album" ratingKey="1" '
            'title="The Wow! Signal" parentTitle="Muse" '
            'addedAt="111"/></MediaContainer>')
        # Parts live under a DIFFERENT folder — the path join must reject it.
        children = (
            '<MediaContainer><Track><Media><Part '
            'file="/prom_music/Other/Album/01 a.opus"/>'
            '</Media></Track></MediaContainer>')
        ref = plex_find_album_by_path(
            cfg, "Muse/2026 - The Wow! Signal",
            fetch_xml=self._fetch(search, children))
        self.assertIsNone(ref)

    def test_find_album_by_path_none_when_plex_unconfigured(self):
        from lib.util import plex_find_album_by_path
        cfg = self._cfg()  # no plex_url
        self.assertIsNone(plex_find_album_by_path(cfg, "Artist/Album"))

    def test_find_album_via_artist_fallback_with_production_absolute_path(self):
        # Production passes an ABSOLUTE path (ir.postflight.imported_path).
        # When the album-title search misses, the artist-search fallback must
        # fire with the real artist ("Muse"), not the FS-root segment ("mnt").
        import xml.etree.ElementTree as ET
        from lib.util import plex_find_album_by_path
        cfg = self._cfg(
            plex_url="http://plex:32400",
            beets_directory="/mnt/virtio/Music/Beets",
            plex_path_map="/mnt/virtio/Music/Beets:/prom_music",
            plex_library_section_id="3")

        def _fetch(path, **params):
            if "/search" in path and params.get("type") == "9":
                return ET.fromstring("<MediaContainer/>")  # title search misses
            if "/search" in path and params.get("type") == "8":
                # The fix (parts[-2]) must yield "Muse" here, not "mnt".
                self.assertEqual(params.get("query"), "Muse")
                return ET.fromstring(
                    '<MediaContainer><Directory type="artist" ratingKey="art1" '
                    'title="Muse"/></MediaContainer>')
            if path == "/library/metadata/art1/children":
                return ET.fromstring(
                    '<MediaContainer><Directory type="album" ratingKey="458495" '
                    'title="The Wow! Signal" parentTitle="Muse" '
                    'addedAt="1782611948"/></MediaContainer>')
            if path == "/library/metadata/458495/children":
                return ET.fromstring(
                    '<MediaContainer><Track><Media><Part '
                    'file="/prom_music/Muse/2026 - The Wow! Signal/01 a.opus"/>'
                    '</Media></Track></MediaContainer>')
            return ET.fromstring("<MediaContainer/>")

        ref = plex_find_album_by_path(
            cfg, "/mnt/virtio/Music/Beets/Muse/2026 - The Wow! Signal",
            fetch_xml=_fetch)
        self.assertIsNotNone(ref)
        assert ref is not None
        self.assertEqual(ref.rating_key, "458495")
        self.assertEqual(ref.added_at, 1782611948)

    def test_set_added_at_builds_locked_pin_params(self):
        from lib.util import plex_set_added_at
        cfg = self._cfg(plex_url="http://plex:32400",
                        plex_library_section_id="3")
        calls = []

        def _put(path, **params):
            calls.append((path, params))
            return 200
        ok = plex_set_added_at(cfg, "458495", 1782611948, put_fn=_put)
        self.assertTrue(ok)
        self.assertEqual(len(calls), 1)
        path, params = calls[0]
        self.assertEqual(path, "/library/sections/3/all")
        self.assertEqual(params["type"], "9")
        self.assertEqual(params["id"], "458495")
        self.assertEqual(params["addedAt.value"], "1782611948")
        self.assertEqual(params["addedAt.locked"], "1")

    def test_set_added_at_false_on_non_200(self):
        from lib.util import plex_set_added_at
        cfg = self._cfg(plex_url="http://plex:32400")
        self.assertFalse(
            plex_set_added_at(cfg, "1", 1, put_fn=lambda p, **kw: 404))

    @patch("lib.util.urllib.request.urlopen")
    def test_set_added_at_urllib_leaf_sends_put_with_token(self, mock_urlopen):
        from lib.util import plex_set_added_at
        cfg = self._cfg(plex_url="http://plex:32400", plex_token="tok",
                        plex_library_section_id="3")
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200
        mock_urlopen.return_value = mock_resp
        self.assertTrue(plex_set_added_at(cfg, "458495", 1782611948))
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.method, "PUT")
        self.assertIn("addedAt.value=1782611948", req.full_url)
        self.assertIn("addedAt.locked=1", req.full_url)
        self.assertIn("X-Plex-Token=tok", req.full_url)


class TestJellyfinDateCreatedClient(unittest.TestCase):
    """Read/edit half of the Jellyfin 'Recently Added' pin (migration 046).
    Path translation + find-by-path matching are driven through injected
    get/post seams; the urllib leaves are asserted once via the real paths."""

    ALBUM_PATH = "/mnt/fuse/Media/Music/Beets/Muse/2026 - The Wow! Signal"

    def _cfg(self, **kw):
        from lib.config import CratediggerConfig
        kw.setdefault("jellyfin_url", "http://jellyfin:8096")
        kw.setdefault("jellyfin_token", "tok")
        kw.setdefault("beets_directory", "/mnt/virtio/Music/Beets")
        kw.setdefault("jellyfin_path_map",
                      "/mnt/virtio/Music/Beets:/mnt/fuse/Media/Music/Beets")
        return CratediggerConfig(**kw)

    def test_container_path_absolutize_then_path_map(self):
        from lib.util import _jellyfin_container_path
        cfg = self._cfg()
        self.assertEqual(
            _jellyfin_container_path(cfg, "Artist/Album"),
            "/mnt/fuse/Media/Music/Beets/Artist/Album")
        self.assertEqual(
            _jellyfin_container_path(cfg, "/mnt/virtio/Music/Beets/X/Y"),
            "/mnt/fuse/Media/Music/Beets/X/Y")

    def test_container_path_none_when_not_absolutizable(self):
        from lib.util import _jellyfin_container_path
        from lib.config import CratediggerConfig
        cfg = CratediggerConfig(jellyfin_url="http://jf:8096")
        self.assertIsNone(_jellyfin_container_path(cfg, "Artist/Album"))
        self.assertIsNone(_jellyfin_container_path(cfg, ""))

    def _get(self, responses: dict[str, object]):
        """A get_json seam keyed by (path, discriminating param)."""
        def _fn(path, **params):
            if path == "/Items" and params.get("includeItemTypes") == "MusicAlbum":
                return responses.get("albums", {"Items": []})
            if path == "/Items" and params.get("includeItemTypes") == "MusicArtist":
                return responses.get("artists", {"Items": []})
            if path == "/Items" and "parentId" in params:
                return responses.get("children", {"Items": []})
            return {"Items": []}
        return _fn

    def test_find_album_by_path_matches_on_exact_album_path(self):
        from lib.util import jellyfin_find_album_by_path
        albums = {"Items": [
            {"Id": "other", "Name": "Wow", "AlbumArtist": "X",
             "DateCreated": "2020-01-01T00:00:00Z",
             "Path": "/mnt/fuse/Media/Music/Beets/Other/2020 - Wow"},
            {"Id": "alb-1", "Name": "The Wow! Signal", "AlbumArtist": "Muse",
             "DateCreated": "2026-04-26T18:31:04.4425337Z",
             "Path": self.ALBUM_PATH},
        ]}
        ref = jellyfin_find_album_by_path(
            self._cfg(), "Muse/2026 - The Wow! Signal",
            get_json=self._get({"albums": albums}))
        self.assertIsNotNone(ref)
        assert ref is not None
        self.assertEqual(ref.item_id, "alb-1")
        self.assertEqual(ref.date_created, "2026-04-26T18:31:04.4425337Z")
        self.assertEqual(ref.name, "The Wow! Signal")
        self.assertEqual(ref.artist, "Muse")

    def test_find_album_by_path_none_when_paths_dont_match(self):
        from lib.util import jellyfin_find_album_by_path
        albums = {"Items": [
            {"Id": "1", "Name": "The Wow! Signal",
             "DateCreated": "2026-01-01T00:00:00Z",
             "Path": "/mnt/fuse/Media/Music/Me/Muse/The Wow! Signal"},
        ]}
        ref = jellyfin_find_album_by_path(
            self._cfg(), "Muse/2026 - The Wow! Signal",
            get_json=self._get({"albums": albums}))
        self.assertIsNone(ref)

    def test_find_album_via_artist_fallback(self):
        # Album-title search misses (tag/path divergence); the artist search
        # → albumArtistIds sweep still finds the album by exact path.
        from lib.util import jellyfin_find_album_by_path
        calls = []

        def _fn(path, **params):
            calls.append(params)
            if params.get("includeItemTypes") == "MusicArtist":
                return {"Items": [{"Id": "artist-1", "Name": "Muse"}]}
            if params.get("albumArtistIds") == "artist-1":
                return {"Items": [
                    {"Id": "alb-1", "Name": "Different Tag Name",
                     "AlbumArtist": "Muse",
                     "DateCreated": "2026-04-26T18:31:04Z",
                     "Path": self.ALBUM_PATH}]}
            return {"Items": []}
        ref = jellyfin_find_album_by_path(
            self._cfg(), "/mnt/virtio/Music/Beets/Muse/2026 - The Wow! Signal",
            get_json=_fn)
        self.assertIsNotNone(ref)
        assert ref is not None
        self.assertEqual(ref.item_id, "alb-1")

    def test_find_album_none_when_jellyfin_unconfigured(self):
        from lib.util import jellyfin_find_album_by_path
        from lib.config import CratediggerConfig
        self.assertIsNone(jellyfin_find_album_by_path(
            CratediggerConfig(), "A/B",
            get_json=lambda path, **p: {"Items": []}))

    def test_get_album_children_returns_audio_refs(self):
        from lib.util import jellyfin_get_album_children
        children = {"Items": [
            {"Id": "tr-1", "DateCreated": "2026-07-09T00:39:26Z"},
            {"Id": "tr-2", "DateCreated": "2026-07-09T00:39:27Z"},
            {"Name": "no id — dropped"},
        ]}
        refs = jellyfin_get_album_children(
            self._cfg(), "alb-1", get_json=self._get({"children": children}))
        self.assertEqual([(r.item_id, r.date_created) for r in refs],
                         [("tr-1", "2026-07-09T00:39:26Z"),
                          ("tr-2", "2026-07-09T00:39:27Z")])

    def test_set_date_created_round_trips_full_dto(self):
        # The update endpoint REPLACES item metadata — the setter must post
        # back the FULL fetched dto with only DateCreated changed.
        from lib.util import jellyfin_set_date_created
        posted = []

        def _get(path, **params):
            if path == "/Users":
                return [{"Id": "user-1"}, {"Id": "user-2"}]
            if path == "/Items/alb-1":
                self.assertEqual(params.get("userId"), "user-1")
                return {"Id": "alb-1", "Name": "The Wow! Signal",
                        "DateCreated": "2026-07-09T00:39:26Z",
                        "Genres": ["Rock"], "ProviderIds": {"MusicBrainzAlbum": "mbid"}}
            self.fail(f"unexpected GET {path}")

        def _post(path, payload):
            posted.append((path, payload))
            return 204
        ok = jellyfin_set_date_created(
            self._cfg(), "alb-1", "2026-04-26T18:31:04Z",
            get_json=_get, post_json=_post)
        self.assertTrue(ok)
        path, payload = posted[0]
        self.assertEqual(path, "/Items/alb-1")
        self.assertEqual(payload["DateCreated"], "2026-04-26T18:31:04Z")
        # Full dto preserved — a partial body would wipe these in Jellyfin.
        self.assertEqual(payload["Genres"], ["Rock"])
        self.assertEqual(payload["ProviderIds"], {"MusicBrainzAlbum": "mbid"})

    def test_set_date_created_false_on_non_2xx(self):
        from lib.util import jellyfin_set_date_created
        self.assertFalse(jellyfin_set_date_created(
            self._cfg(), "alb-1", "2026-01-01T00:00:00Z",
            get_json=lambda path, **p: (
                [{"Id": "u"}] if path == "/Users" else {"Id": "alb-1"}),
            post_json=lambda path, payload: 500))

    def test_set_date_created_false_when_no_users(self):
        from lib.util import jellyfin_set_date_created
        self.assertFalse(jellyfin_set_date_created(
            self._cfg(), "alb-1", "2026-01-01T00:00:00Z",
            get_json=lambda path, **p: [],
            post_json=lambda path, payload: self.fail("must not POST")))

    def test_set_date_created_false_when_item_missing(self):
        from lib.util import jellyfin_set_date_created
        self.assertFalse(jellyfin_set_date_created(
            self._cfg(), "alb-1", "2026-01-01T00:00:00Z",
            get_json=lambda path, **p: (
                [{"Id": "u"}] if path == "/Users" else {}),
            post_json=lambda path, payload: self.fail("must not POST")))

    @patch("lib.util.urllib.request.urlopen")
    def test_get_json_urllib_leaf_sends_token_header(self, mock_urlopen):
        from lib.util import _jellyfin_get_json
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b'{"Items": []}'
        mock_urlopen.return_value = mock_resp
        out = _jellyfin_get_json(self._cfg(), "/Items", searchTerm="x y")
        self.assertEqual(out, {"Items": []})
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.get_header("X-emby-token"), "tok")
        self.assertIn("/Items?searchTerm=x+y", req.full_url)

    @patch("lib.util.urllib.request.urlopen")
    def test_post_json_urllib_leaf_sends_json_body(self, mock_urlopen):
        from lib.util import _jellyfin_post_json
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 204
        mock_urlopen.return_value = mock_resp
        status = _jellyfin_post_json(
            self._cfg(), "/Items/alb-1", {"Id": "alb-1"})
        self.assertEqual(status, 204)
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.method, "POST")
        self.assertEqual(req.get_header("X-emby-token"), "tok")
        self.assertEqual(req.get_header("Content-type"), "application/json")
        self.assertEqual(req.data, b'{"Id": "alb-1"}')


class TestBeetsSubprocessEnv(unittest.TestCase):
    """beets_subprocess_env() is the single source of truth for the env dict
    used by every subprocess that invokes beets (directly or via the harness
    / import_one.py). It resolves BEETSDIR (beets' config-dir override) from
    the runtime config's [Beets] config_dir — the module-rendered config —
    with a pre-set env BEETSDIR as the dev/test fallback. The Home-Manager
    HOME impersonation is gone (tier-2 plan R6): unset config dir raises an
    actionable error instead of silently reading ~/.config/beets.
    """

    def _with_runtime_config(self, ini_text: str):
        """Context: a temp runtime config.ini as the active config."""
        import contextlib
        import tempfile

        @contextlib.contextmanager
        def cm():
            with tempfile.TemporaryDirectory() as d:
                path = os.path.join(d, "config.ini")
                with open(path, "w", encoding="utf-8") as f:
                    f.write(ini_text)
                with patch.dict(os.environ,
                                {"CRATEDIGGER_RUNTIME_CONFIG": path},
                                clear=False):
                    yield
        return cm()

    def test_beetsdir_comes_from_runtime_config(self) -> None:
        from lib.util import beets_subprocess_env
        with self._with_runtime_config(
            "[Beets]\nconfig_dir = /var/lib/cratedigger/beets\n"
        ):
            env = beets_subprocess_env()
        self.assertEqual(env["BEETSDIR"], "/var/lib/cratedigger/beets")

    def test_config_wins_over_preset_env(self) -> None:
        from lib.util import beets_subprocess_env
        with self._with_runtime_config(
            "[Beets]\nconfig_dir = /from/config\n"
        ), patch.dict(os.environ, {"BEETSDIR": "/from/env"}, clear=False):
            env = beets_subprocess_env()
        self.assertEqual(env["BEETSDIR"], "/from/config")

    def test_env_beetsdir_is_the_dev_fallback(self) -> None:
        from lib.util import beets_subprocess_env
        with self._with_runtime_config("[Slskd]\nhost_url = http://x\n"), \
                patch.dict(os.environ, {"BEETSDIR": "/from/env"}, clear=False):
            env = beets_subprocess_env()
        self.assertEqual(env["BEETSDIR"], "/from/env")

    def test_unset_config_dir_raises_actionable_error(self) -> None:
        """No silent fallback to the invoking user's ~/.config/beets."""
        from lib.util import beets_subprocess_env
        with self._with_runtime_config("[Slskd]\nhost_url = http://x\n"):
            no_beetsdir = {k: v for k, v in os.environ.items()
                           if k != "BEETSDIR"}
            with patch.dict(os.environ, no_beetsdir, clear=True):
                with self.assertRaises(RuntimeError) as ctx:
                    beets_subprocess_env()
        self.assertIn("[Beets] config_dir", str(ctx.exception))

    def test_no_home_override_remains(self) -> None:
        """The HOME=/home/<user> impersonation is deleted, not supplemented."""
        from lib.util import beets_subprocess_env
        with self._with_runtime_config(
            "[Beets]\nconfig_dir = /cfg\n"
        ), patch.dict(os.environ, {"HOME": "/root"}, clear=False):
            env = beets_subprocess_env()
        self.assertEqual(env["HOME"], "/root")

    def test_beets_python_exported_from_config(self) -> None:
        from lib.util import beets_subprocess_env
        with self._with_runtime_config(
            "[Beets]\nconfig_dir = /cfg\npython = /nix/store/x/bin/python\n"
        ):
            env = beets_subprocess_env()
        self.assertEqual(env["CRATEDIGGER_BEETS_PYTHON"], "/nix/store/x/bin/python")

    def test_inherits_other_env_vars(self) -> None:
        from lib.util import beets_subprocess_env
        sentinel = "CRATEDIGGER_TEST_SENTINEL_VAR_XYZ"
        with self._with_runtime_config(
            "[Beets]\nconfig_dir = /cfg\n"
        ), patch.dict(os.environ, {sentinel: "present"}, clear=False):
            env = beets_subprocess_env()
        self.assertEqual(env.get(sentinel), "present")
if __name__ == "__main__":
    unittest.main()
