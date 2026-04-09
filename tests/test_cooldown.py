"""Tests for global user cooldown system (issue #39)."""

import logging
import unittest
from unittest.mock import MagicMock, patch

from lib.context import SoularrContext
from lib.quality import CooldownConfig, should_cooldown
from soularr import TrackRecord


class TestShouldCooldown(unittest.TestCase):
    """Pure function: should_cooldown(outcomes, config) -> bool."""

    def test_all_timeouts_triggers(self):
        outcomes = ["timeout"] * 5
        self.assertTrue(should_cooldown(outcomes))

    def test_mixed_outcomes_no_trigger(self):
        outcomes = ["timeout", "timeout", "success", "timeout", "timeout"]
        self.assertFalse(should_cooldown(outcomes))

    def test_fewer_than_threshold_no_trigger(self):
        outcomes = ["timeout", "timeout", "timeout"]
        self.assertFalse(should_cooldown(outcomes))

    def test_empty_outcomes(self):
        self.assertFalse(should_cooldown([]))

    def test_all_rejected_triggers(self):
        outcomes = ["rejected"] * 5
        self.assertTrue(should_cooldown(outcomes))

    def test_mixed_failure_types_triggers(self):
        outcomes = ["timeout", "failed", "timeout", "rejected", "failed"]
        self.assertTrue(should_cooldown(outcomes))

    def test_success_anywhere_blocks(self):
        outcomes = ["timeout", "timeout", "success", "timeout", "timeout"]
        self.assertFalse(should_cooldown(outcomes))

    def test_custom_threshold(self):
        config = CooldownConfig(failure_threshold=3, lookback_window=3)
        outcomes = ["timeout", "timeout", "timeout"]
        self.assertTrue(should_cooldown(outcomes, config))

    def test_only_lookback_window_matters(self):
        """Extra outcomes beyond lookback_window are ignored."""
        config = CooldownConfig(failure_threshold=3, lookback_window=3)
        # Last 3 are failures, older success doesn't matter
        outcomes = ["timeout", "timeout", "timeout", "success"]
        self.assertTrue(should_cooldown(outcomes, config))

    def test_default_config_values(self):
        cfg = CooldownConfig()
        self.assertEqual(cfg.failure_threshold, 5)
        self.assertEqual(cfg.cooldown_days, 3)
        self.assertEqual(cfg.lookback_window, 5)
        self.assertIn("timeout", cfg.failure_outcomes)
        self.assertIn("failed", cfg.failure_outcomes)
        self.assertIn("rejected", cfg.failure_outcomes)
        self.assertNotIn("success", cfg.failure_outcomes)


class TestEnqueueCooldownFiltering(unittest.TestCase):
    """Cooled-down users should be skipped during enqueue with distinct log messages."""

    def _make_ctx(self, cooled_down_users: set[str] | None = None,
                  denied_users: list[str] | None = None) -> SoularrContext:
        source = MagicMock()
        db = MagicMock()
        db.get_denylisted_users.return_value = [
            {"username": u} for u in (denied_users or [])
        ]
        source._get_db.return_value = db
        ctx = SoularrContext(
            cfg=MagicMock(),
            slskd=MagicMock(),
            pipeline_db_source=source,
            cooled_down_users=cooled_down_users or set(),
        )
        return ctx

    def test_cooled_user_skipped_in_try_enqueue(self):
        """A user on cooldown should be skipped even if they have matching results."""
        from lib.enqueue import try_enqueue
        ctx = self._make_ctx(cooled_down_users={"deaduser"})
        ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")
        ctx.user_upload_speed["deaduser"] = 100

        tracks: list[TrackRecord] = [
            {"albumId": 1, "title": "Track 1", "mediumNumber": 1},
        ]
        results = {"deaduser": {"flac": ["Music\\Album"]}}

        with patch("lib.enqueue.check_for_match") as mock_match:
            attempt = try_enqueue(tracks, results, "flac", ctx)

        # check_for_match should never have been called for the cooled-down user
        mock_match.assert_not_called()
        self.assertFalse(attempt.matched)

    def test_non_cooled_user_proceeds(self):
        """A user NOT on cooldown should proceed through normal matching."""
        from lib.enqueue import try_enqueue
        ctx = self._make_ctx(cooled_down_users={"otheruser"})
        ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")
        ctx.user_upload_speed["gooduser"] = 100

        tracks: list[TrackRecord] = [
            {"albumId": 1, "title": "Track 1", "mediumNumber": 1},
        ]
        results = {"gooduser": {"flac": ["Music\\Album"]}}

        with patch("lib.enqueue.check_for_match",
                   return_value=(False, None, None)):
            attempt = try_enqueue(tracks, results, "flac", ctx)

        # check_for_match WAS called for the non-cooled user
        self.assertFalse(attempt.matched)

    def test_cooldown_log_message_distinct_from_denylist(self):
        """Log message for cooled-down users should say 'on cooldown', not 'denylisted'."""
        from lib.enqueue import try_enqueue
        ctx = self._make_ctx(cooled_down_users={"deaduser"})
        ctx.current_album_cache[1] = MagicMock(title="Album", artist_name="Artist")
        ctx.user_upload_speed["deaduser"] = 100

        tracks: list[TrackRecord] = [
            {"albumId": 1, "title": "Track 1", "mediumNumber": 1},
        ]
        results = {"deaduser": {"flac": ["Music\\Album"]}}

        with self.assertLogs("soularr", level=logging.INFO) as cm:
            try_enqueue(tracks, results, "flac", ctx)

        cooldown_msgs = [m for m in cm.output if "cooldown" in m.lower()]
        denylist_msgs = [m for m in cm.output if "denylisted" in m.lower()]
        self.assertTrue(len(cooldown_msgs) > 0, "Expected a 'cooldown' log message")
        self.assertEqual(len(denylist_msgs), 0,
                         "Should not log 'denylisted' for cooled-down users")


class TestCooldownTriggerOnTimeout(unittest.TestCase):
    """_timeout_album() should call check_and_apply_cooldown after logging."""

    def test_timeout_triggers_cooldown_check(self):
        from lib.download import _timeout_album
        from lib.grab_list import GrabListEntry, DownloadFile

        entry = GrabListEntry(
            album_id=1,
            files=[DownloadFile(
                filename="01.flac", id="xfer-1", file_dir="Music\\Album",
                username="deaduser", size=50000000,
            )],
            filetype="flac",
            title="Album",
            artist="Artist",
            year="2020",
            mb_release_id="mb-uuid",
            db_request_id=42,
        )

        mock_db = MagicMock()
        mock_source = MagicMock()
        mock_source._get_db.return_value = mock_db
        ctx = SoularrContext(
            cfg=MagicMock(),
            slskd=MagicMock(),
            pipeline_db_source=mock_source,
        )

        with patch("lib.download.cancel_and_delete"), \
             patch("lib.download.apply_transition"):
            _timeout_album(entry, 42, "stalled", ctx)

        mock_db.log_download.assert_called_once()
        self.assertEqual(mock_db.log_download.call_args.kwargs["outcome"], "timeout")
        mock_db.check_and_apply_cooldown.assert_called_once_with("deaduser")

    def test_timeout_no_cooldown_when_no_username(self):
        """If username is empty/None, don't call cooldown check."""
        from lib.download import _timeout_album
        from lib.grab_list import GrabListEntry, DownloadFile

        entry = GrabListEntry(
            album_id=1,
            files=[DownloadFile(
                filename="01.flac", id="xfer-1", file_dir="Music\\Album",
                username="", size=50000000,
            )],
            filetype="flac",
            title="Album",
            artist="Artist",
            year="2020",
            mb_release_id="mb-uuid",
            db_request_id=42,
        )

        mock_db = MagicMock()
        mock_source = MagicMock()
        mock_source._get_db.return_value = mock_db
        ctx = SoularrContext(
            cfg=MagicMock(),
            slskd=MagicMock(),
            pipeline_db_source=mock_source,
        )

        with patch("lib.download.cancel_and_delete"), \
             patch("lib.download.apply_transition"):
            _timeout_album(entry, 42, "stalled", ctx)

        mock_db.check_and_apply_cooldown.assert_not_called()


class TestCooldownTriggerOnRejection(unittest.TestCase):
    """mark_failed() should call check_and_apply_cooldown after logging."""

    def test_rejection_triggers_cooldown_check(self):
        from album_source import DatabaseSource
        from lib.quality import DownloadInfo

        mock_db = MagicMock()
        source = DatabaseSource.__new__(DatabaseSource)
        source._db = mock_db

        album_record = MagicMock()
        album_record.db_request_id = 42

        bv_result = MagicMock()
        bv_result.distance = 0.5
        bv_result.scenario = "bad_match"
        bv_result.detail = "too different"
        bv_result.error = "distance too high"

        dl = DownloadInfo(username="baduser")

        with patch("lib.transitions.apply_transition"):
            source.mark_failed(album_record, bv_result,
                               usernames=["baduser"], download_info=dl)

        mock_db.log_download.assert_called_once()
        self.assertEqual(mock_db.log_download.call_args.kwargs["outcome"], "rejected")
        mock_db.check_and_apply_cooldown.assert_called_once_with("baduser")


if __name__ == "__main__":
    unittest.main()
