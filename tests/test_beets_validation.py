"""Tests for beets validation pipeline in soularr.

Since soularr.py has heavy external dependencies (slskd_api, music_tag),
we mock at the module level before importing, or test via subprocess simulation.
"""

import json
import os
import re
import shutil
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch


# Mock heavy dependencies before importing soularr
sys.modules["requests"] = MagicMock()
sys.modules["music_tag"] = MagicMock()
sys.modules["slskd_api"] = MagicMock()
sys.modules["slskd_api.apis"] = MagicMock()
sys.modules["slskd_api.apis.users"] = MagicMock()

# Now import soularr and lib modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import soularr
from lib.beets import beets_validate
from lib.grab_list import GrabListEntry
from lib.quality import ValidationResult
from lib.util import stage_to_ai, log_validation_result, sanitize_folder_name


def make_choose_match_msg(mb_release_id, distance, extra_candidates=None):
    """Build a choose_match JSON message with the given MBID and distance."""
    candidates = [{
        "index": 0,
        "distance": distance,
        "artist": "Test Artist",
        "album": "Test Album",
        "album_id": mb_release_id,
        "year": 2020,
        "country": "US",
        "track_count": 10,
        "albumstatus": "Official",
    }]
    if extra_candidates:
        candidates.extend(extra_candidates)
    return json.dumps({
        "type": "choose_match",
        "task_id": 0,
        "path": "/test/path",
        "cur_artist": "Test Artist",
        "cur_album": "Test Album",
        "item_count": 10,
        "candidates": candidates,
    })


def make_session_end():
    return json.dumps({"type": "session_end"})


def make_should_resume():
    return json.dumps({"type": "should_resume", "path": "/test/path"})


class TestBeetsValidate(unittest.TestCase):
    """Test beets_validate() function with mocked subprocess."""

    HARNESS = "/fake/harness.sh"

    @patch("lib.beets.sp.Popen")
    def test_good_match(self, mock_popen):
        """Distance 0.05 with threshold 0.15 → valid=True."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        proc = MagicMock()
        proc.stdout = iter([
            make_choose_match_msg(mbid, 0.05) + "\n",
            make_session_end() + "\n",
        ])
        proc.stdin = MagicMock()
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        result = beets_validate(self.HARNESS, "/test/album", mbid, 0.15)

        self.assertTrue(result.valid)
        self.assertTrue(result.mbid_found)
        self.assertAlmostEqual(result.distance, 0.05)
        self.assertIsNone(result.error)
        # Verify skip was sent (dry-run)
        proc.stdin.write.assert_called_with('{"action":"skip"}\n')

    @patch("lib.beets.sp.Popen")
    def test_high_distance(self, mock_popen):
        """Distance 0.30 with threshold 0.15 → valid=False."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        proc = MagicMock()
        proc.stdout = iter([
            make_choose_match_msg(mbid, 0.30) + "\n",
            make_session_end() + "\n",
        ])
        proc.stdin = MagicMock()
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        result = beets_validate(self.HARNESS,"/test/album", mbid, 0.15)

        self.assertFalse(result.valid)
        self.assertTrue(result.mbid_found)
        self.assertAlmostEqual(result.distance, 0.30)

    @patch("lib.beets.sp.Popen")
    def test_mbid_not_found(self, mock_popen):
        """Target MBID not in candidates → valid=False, mbid_found=False."""
        target_mbid = "aaaaaaaa-1111-2222-3333-444444444444"
        wrong_mbid = "bbbbbbbb-1111-2222-3333-444444444444"
        proc = MagicMock()
        proc.stdout = iter([
            make_choose_match_msg(wrong_mbid, 0.05) + "\n",
            make_session_end() + "\n",
        ])
        proc.stdin = MagicMock()
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        result = beets_validate(self.HARNESS,"/test/album", target_mbid, 0.15)

        self.assertFalse(result.valid)
        self.assertFalse(result.mbid_found)
        self.assertIsNone(result.distance)

    @patch("lib.beets.sp.Popen")
    def test_no_candidates(self, mock_popen):
        """Empty candidates list → valid=False."""
        proc = MagicMock()
        proc.stdout = iter([
            json.dumps({
                "type": "choose_match",
                "task_id": 0,
                "path": "/test",
                "candidates": [],
            }) + "\n",
            make_session_end() + "\n",
        ])
        proc.stdin = MagicMock()
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        result = beets_validate(self.HARNESS,"/test/album", "some-mbid", 0.15)

        self.assertFalse(result.valid)
        self.assertFalse(result.mbid_found)

    @patch("lib.beets.sp.Popen")
    def test_subprocess_start_failure(self, mock_popen):
        """Harness fails to start → valid=False, error set."""
        mock_popen.side_effect = FileNotFoundError("No such file")

        result = beets_validate(self.HARNESS,"/test/album", "some-mbid", 0.15)

        self.assertFalse(result.valid)
        self.assertIn("Failed to start harness", result.error)

    @patch("lib.beets.sp.Popen")
    def test_handles_should_resume_then_choose_match(self, mock_popen):
        """should_resume followed by choose_match → handles both correctly."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        proc = MagicMock()
        proc.stdout = iter([
            make_should_resume() + "\n",
            make_choose_match_msg(mbid, 0.03) + "\n",
            make_session_end() + "\n",
        ])
        proc.stdin = MagicMock()
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        result = beets_validate(self.HARNESS,"/test/album", mbid, 0.15)

        self.assertTrue(result.valid)
        # Two skip calls: one for should_resume, one for choose_match
        self.assertEqual(proc.stdin.write.call_count, 2)

    @patch("lib.beets.sp.Popen")
    def test_exact_threshold(self, mock_popen):
        """Distance exactly at threshold → valid=True."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        proc = MagicMock()
        proc.stdout = iter([
            make_choose_match_msg(mbid, 0.15) + "\n",
            make_session_end() + "\n",
        ])
        proc.stdin = MagicMock()
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        result = beets_validate(self.HARNESS,"/test/album", mbid, 0.15)

        self.assertTrue(result.valid)  # <= threshold

    @patch("lib.beets.sp.Popen")
    def test_just_above_threshold(self, mock_popen):
        """Distance 0.1501 is above 0.15 → valid=False."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        proc = MagicMock()
        proc.stdout = iter([
            make_choose_match_msg(mbid, 0.1501) + "\n",
            make_session_end() + "\n",
        ])
        proc.stdin = MagicMock()
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        result = beets_validate(self.HARNESS,"/test/album", mbid, 0.15)

        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "high_distance")

    @patch("lib.beets.sp.Popen")
    def test_above_hard_limit(self, mock_popen):
        """Distance above 0.30 hard limit → valid=False."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        proc = MagicMock()
        proc.stdout = iter([
            make_choose_match_msg(mbid, 0.35) + "\n",
            make_session_end() + "\n",
        ])
        proc.stdin = MagicMock()
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        result = beets_validate(self.HARNESS,"/test/album", mbid, 0.15)

        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "high_distance")


    @patch("lib.beets.sp.Popen")
    def test_extra_tracks_rejected(self, mock_popen):
        """MB has more tracks than local files → valid=False even at low distance."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        proc = MagicMock()
        candidates = [{
            "index": 0, "distance": 0.02, "artist": "Test Artist",
            "album": "Test Album", "album_id": mbid, "year": 2020,
            "country": "US", "track_count": 12,
            "extra_tracks": [{"title": "Bonus 1"}, {"title": "Bonus 2"}],
            "albumstatus": "Official",
        }]
        msg = json.dumps({
            "type": "choose_match", "task_id": 0, "path": "/test/path",
            "cur_artist": "Test Artist", "cur_album": "Test Album",
            "item_count": 10, "candidates": candidates,
        })
        proc.stdout = iter([msg + "\n", make_session_end() + "\n"])
        proc.stdin = MagicMock()
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        result = beets_validate(self.HARNESS,"/test/album", mbid, 0.15)

        self.assertFalse(result.valid)
        self.assertEqual(result.scenario, "extra_tracks")

    @patch("lib.beets.sp.Popen")
    def test_non_official_accepted_if_match(self, mock_popen):
        """Non-official release (bootleg/promo) with good match → valid=True."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        proc = MagicMock()
        candidates = [{
            "index": 0, "distance": 0.05, "artist": "Test Artist",
            "album": "Test Album", "album_id": mbid, "year": 2020,
            "country": "US", "track_count": 10, "albumstatus": "Bootleg",
        }]
        msg = json.dumps({
            "type": "choose_match", "task_id": 0, "path": "/test/path",
            "cur_artist": "Test Artist", "cur_album": "Test Album",
            "item_count": 10, "candidates": candidates,
        })
        proc.stdout = iter([msg + "\n", make_session_end() + "\n"])
        proc.stdin = MagicMock()
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        result = beets_validate(self.HARNESS,"/test/album", mbid, 0.15)

        self.assertTrue(result.valid)

    @patch("lib.beets.sp.Popen")
    def test_discogs_numeric_str_album_id_matches(self, mock_popen):
        """Harness-emitted numeric-str album_id matches str target_mbid.

        This is the happy path for Discogs: beets' plugin returns
        album_id as int, the harness normalises it to str via
        `_id_str`, and msgspec decodes it straight into .mbid. The
        strict-typed boundary does not need to coerce — the harness
        has already done its job.
        """
        target_mbid = "2085134"  # numeric Discogs ID stored as str in DB
        proc = MagicMock()
        candidates = [{
            "index": 0, "distance": 0.05, "artist": "Blueline Medic",
            "album": "The Apology Wars",
            "album_id": "2085134",  # harness-emitted str
            "year": 2001, "country": "AU", "track_count": 11,
            "albumstatus": "Official", "data_source": "Discogs",
        }]
        msg = json.dumps({
            "type": "choose_match", "task_id": 0, "path": "/test/path",
            "cur_artist": "Blueline Medic", "cur_album": "The Apology Wars",
            "item_count": 11, "candidates": candidates,
        })
        proc.stdout = iter([msg + "\n", make_session_end() + "\n"])
        proc.stdin = MagicMock()
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        result = beets_validate(self.HARNESS, "/test/album", target_mbid, 0.15)

        self.assertTrue(result.mbid_found)
        self.assertTrue(result.valid)
        self.assertAlmostEqual(result.distance, 0.05)
        self.assertEqual(result.scenario, "strong_match")
        self.assertTrue(result.candidates[0].is_target)
        self.assertEqual(result.candidates[0].mbid, "2085134")
        self.assertIsNone(result.error)

    @patch("lib.beets.sp.Popen")
    def test_int_album_id_trips_msgspec_boundary(self, mock_popen):
        """Regression guard for PR #98: an int album_id on the wire
        should never reach downstream consumers. msgspec.convert raises
        ValidationError, beets_validate surfaces it as result.error,
        and returns an empty (invalid) result — loud failure instead of
        the silent `mbid_not_found` miss that was the original bug.

        In production the harness-side `_id_str` normalises all IDs to
        str so this path is unreachable; if it ever trips, the harness
        has regressed and we want to know immediately.
        """
        target_mbid = "2085134"
        proc = MagicMock()
        # int album_id — the shape of the live bug
        candidates = [{
            "index": 0, "distance": 0.05, "artist": "X",
            "album": "Y", "album_id": 2085134,
            "track_count": 11, "albumstatus": "Official",
        }]
        msg = json.dumps({
            "type": "choose_match", "task_id": 0, "path": "/test/path",
            "cur_artist": "X", "cur_album": "Y",
            "item_count": 11, "candidates": candidates,
        })
        proc.stdout = iter([msg + "\n", make_session_end() + "\n"])
        proc.stdin = MagicMock()
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        result = beets_validate(self.HARNESS, "/test/album", target_mbid, 0.15)

        self.assertFalse(result.valid)
        self.assertFalse(result.mbid_found)
        self.assertIsNotNone(result.error)
        # Error message should name the offending field so operators
        # can see exactly what broke.
        assert result.error is not None  # for pyright
        self.assertIn("album_id", result.error)

    @patch("lib.beets.sp.Popen")
    def test_artist_collab_match(self, mock_popen):
        """Collab credit — MBID matches and distance is good → valid=True."""
        mbid = "12345678-1234-1234-1234-123456789abc"
        proc = MagicMock()
        candidates = [{
            "index": 0, "distance": 0.06, "artist": "Action Bronson & Party Supplies",
            "album": "Blue Chips", "album_id": mbid, "year": 2012,
            "country": "US", "track_count": 16, "albumstatus": "Official",
        }]
        msg = json.dumps({
            "type": "choose_match", "task_id": 0, "path": "/test/path",
            "cur_artist": "Action Bronson", "cur_album": "Blue Chips",
            "item_count": 16, "candidates": candidates,
        })
        proc.stdout = iter([msg + "\n", make_session_end() + "\n"])
        proc.stdin = MagicMock()
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        result = beets_validate(self.HARNESS,"/test/album", mbid, 0.15)

        self.assertTrue(result.valid)
        self.assertEqual(result.scenario, "strong_match")


def _make_album_data(**overrides):
    """Build a minimal GrabListEntry for tests that need album_data."""
    defaults = dict(
        album_id=0, files=[], filetype="mp3", title="Test Album",
        artist="Test Artist", year="2024", mb_release_id="",
    )
    defaults.update(overrides)
    return GrabListEntry(**defaults)  # type: ignore[arg-type]


class TestStageToAi(unittest.TestCase):
    """Test stage_to_ai() function."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_creates_correct_structure(self):
        """Files moved to staging_dir/Artist/Album/."""
        source = os.path.join(self.tmpdir, "source")
        staging = os.path.join(self.tmpdir, "staging")
        os.makedirs(source)
        os.makedirs(staging)

        for name in ["01 - Track.flac", "02 - Track.flac", "cover.jpg"]:
            open(os.path.join(source, name), "w").close()

        album_data = _make_album_data(artist="Test Artist", title="Test Album")
        dest = stage_to_ai(album_data, source, staging)

        self.assertEqual(dest, os.path.join(staging, "Test Artist", "Test Album"))
        self.assertTrue(os.path.exists(os.path.join(dest, "01 - Track.flac")))
        self.assertTrue(os.path.exists(os.path.join(dest, "02 - Track.flac")))
        self.assertTrue(os.path.exists(os.path.join(dest, "cover.jpg")))

    def test_cleans_source(self):
        """Source directory removed after staging."""
        source = os.path.join(self.tmpdir, "source")
        staging = os.path.join(self.tmpdir, "staging")
        os.makedirs(source)
        os.makedirs(staging)
        open(os.path.join(source, "track.flac"), "w").close()

        album_data = _make_album_data(artist="Artist", title="Album")
        stage_to_ai(album_data, source, staging)

        self.assertFalse(os.path.exists(source))

    def test_sanitizes_names(self):
        """Special characters in artist/album are sanitized."""
        source = os.path.join(self.tmpdir, "source")
        staging = os.path.join(self.tmpdir, "staging")
        os.makedirs(source)
        os.makedirs(staging)
        open(os.path.join(source, "track.flac"), "w").close()

        album_data = _make_album_data(artist='Test: "Artist"', title="Album/Title?")
        dest = stage_to_ai(album_data, source, staging)

        # sanitize_folder_name removes <>:"/\|?*
        self.assertNotIn(":", os.path.basename(os.path.dirname(dest)))
        self.assertNotIn("?", os.path.basename(dest))
        self.assertTrue(os.path.exists(dest))


class TestLogValidationResult(unittest.TestCase):
    """Test log_validation_result() JSONL output."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.tracking_file = os.path.join(self.tmpdir, "tracking.jsonl")
        self.mock_cfg = MagicMock()
        self.mock_cfg.beets_tracking_file = self.tracking_file

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_appends_staged_entry(self):
        """Staged result writes correct JSONL."""
        album_data = _make_album_data(
            artist="Test Artist", title="Test Album",
            mb_release_id="abc-123", album_id=42,
        )
        result = ValidationResult(valid=True, distance=0.05, mbid_found=True)

        log_validation_result(album_data, result, self.mock_cfg,
                              dest_path="/AI/Test Artist/Test Album")

        with open(self.tracking_file) as f:
            entry = json.loads(f.readline())

        self.assertEqual(entry["status"], "staged")
        self.assertEqual(entry["artist"], "Test Artist")
        self.assertEqual(entry["mb_release_id"], "abc-123")
        self.assertEqual(entry["distance"], 0.05)
        self.assertEqual(entry["dest_path"], "/AI/Test Artist/Test Album")

    def test_appends_rejected_entry(self):
        """Rejected result writes status=rejected."""
        album_data = _make_album_data(artist="A", title="B", album_id=1)
        result = ValidationResult(valid=False, distance=0.40, mbid_found=True)

        log_validation_result(album_data, result, self.mock_cfg)

        with open(self.tracking_file) as f:
            entry = json.loads(f.readline())

        self.assertEqual(entry["status"], "rejected")
        self.assertIsNone(entry["dest_path"])

    def test_appends_multiple_entries(self):
        """Multiple calls append to same file."""
        album_data = _make_album_data(artist="A", title="B", album_id=1)
        result = ValidationResult(valid=True, distance=0.01)

        log_validation_result(album_data, result, self.mock_cfg, dest_path="/dest1")
        log_validation_result(album_data, result, self.mock_cfg, dest_path="/dest2")

        with open(self.tracking_file) as f:
            lines = f.readlines()

        self.assertEqual(len(lines), 2)


class TestSanitizeFolderName(unittest.TestCase):
    """Verify sanitize_folder_name works correctly for stage_to_ai."""

    def test_removes_special_chars(self):
        self.assertEqual(sanitize_folder_name('Test: "Artist"'), 'Test Artist')
        self.assertEqual(sanitize_folder_name("Album/Title?"), "AlbumTitle")
        self.assertEqual(sanitize_folder_name("Normal Name"), "Normal Name")


if __name__ == "__main__":
    unittest.main()
