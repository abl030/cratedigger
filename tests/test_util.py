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

    def test_relative_path_warns_and_skips(self):
        """beets stores paths relative to its library root, so consumers that
        do filesystem ops on imported_path must reject relative paths instead
        of silently no-opping. PR #236 fixed the symmetric bug in
        trigger_plex_scan; this is the same defensive guard here."""
        from lib.util import cleanup_disambiguation_orphans
        with self.assertLogs("cratedigger", level="WARNING") as captured:
            removed = cleanup_disambiguation_orphans(
                "Artist/2009 - Blood Bank [2009]")
        self.assertEqual(removed, [])
        self.assertTrue(
            any("relative" in m.lower() for m in captured.output),
            f"Expected warning about relative path, got: {captured.output}",
        )

    def test_relative_path_absolutizes_with_beets_directory(self):
        """When beets_directory is provided, relative imported_path should be
        absolutized against it and the function should perform real cleanup
        (not just warn-and-skip)."""
        from lib.util import cleanup_disambiguation_orphans
        # Use the existing tmpdir as the synthetic beets root.
        # self.artist_dir = <tmpdir>/Bon Iver, set by setUp.
        imported_rel = os.path.relpath(self._make_dir(
            "2009 - Blood Bank [2009]",
            ["01 Blood Bank.mp3"]), self.tmpdir)
        orphan = self._make_dir("2009 - Blood Bank", ["cover.jpg"])
        removed = cleanup_disambiguation_orphans(
            imported_rel, beets_directory=self.tmpdir)
        self.assertFalse(os.path.exists(orphan))
        self.assertEqual(removed, [orphan])

    def test_relative_path_with_empty_beets_directory_still_warns(self):
        """Empty beets_directory falls back to warn-and-skip — explicit
        regression guard for the case where the config option is wired up
        but left empty."""
        from lib.util import cleanup_disambiguation_orphans
        with self.assertLogs("cratedigger", level="WARNING") as captured:
            removed = cleanup_disambiguation_orphans(
                "Artist/Album", beets_directory="")
        self.assertEqual(removed, [])
        self.assertTrue(
            any("relative" in m.lower() for m in captured.output),
            f"Expected warning, got: {captured.output}",
        )

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
