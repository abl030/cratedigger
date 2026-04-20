"""Tests for global user cooldown system (issue #39)."""

import logging
import unittest
from unittest.mock import MagicMock, patch

from lib.context import CratediggerContext
from lib.quality import CooldownConfig, should_cooldown
from cratedigger import TrackRecord
from tests.fakes import FakePipelineDB
from tests.helpers import (
    make_ctx_with_fake_db,
    make_download_file,
    make_grab_list_entry,
    make_request_row,
)


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
                  denied_users: list[str] | None = None) -> CratediggerContext:
        source = MagicMock()
        db = MagicMock()
        db.get_denylisted_users.return_value = [
            {"username": u} for u in (denied_users or [])
        ]
        source._get_db.return_value = db
        ctx = CratediggerContext(
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

        with self.assertLogs("cratedigger", level=logging.INFO) as cm:
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

        entry = make_grab_list_entry(
            files=[make_download_file(filename="01.flac", id="xfer-1",
                                      file_dir="Music\\Album",
                                      username="deaduser", size=50000000)],
            filetype="flac", title="Album", artist="Artist",
            mb_release_id="mb-uuid", db_request_id=42,
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        db.set_cooldown_result(True)
        ctx = make_ctx_with_fake_db(db)

        with patch("lib.download.cancel_and_delete"), \
             patch("lib.download.apply_transition"):
            _timeout_album(entry, 42, "stalled", ctx)

        self.assertEqual(len(db.download_logs), 1)
        db.assert_log(self, 0, outcome="timeout")
        self.assertIn("deaduser", db.cooldowns_applied)
        self.assertEqual(ctx.cooled_down_users, {"deaduser"})

    def test_timeout_checks_each_user_for_multi_user_download(self):
        """Multi-user downloads should evaluate cooldown per source user."""
        from lib.download import _timeout_album

        entry = make_grab_list_entry(
            files=[
                make_download_file(filename="01.flac", id="xfer-1",
                                   file_dir="Music\\Disc 1",
                                   username="disc1user", size=50000000),
                make_download_file(filename="02.flac", id="xfer-2",
                                   file_dir="Music\\Disc 2",
                                   username="disc2user", size=50000000),
            ],
            filetype="flac", title="Album", artist="Artist",
            mb_release_id="mb-uuid", db_request_id=42,
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        db.set_cooldown_result(lambda u: u == "disc1user")
        ctx = make_ctx_with_fake_db(db)

        with patch("lib.download.cancel_and_delete"), \
             patch("lib.download.apply_transition"):
            _timeout_album(entry, 42, "stalled", ctx)

        self.assertEqual(set(db.cooldowns_applied), {"disc1user", "disc2user"})
        self.assertEqual(ctx.cooled_down_users, {"disc1user"})

    def test_timeout_no_cooldown_when_no_username(self):
        """If username is empty/None, don't call cooldown check."""
        from lib.download import _timeout_album

        entry = make_grab_list_entry(
            files=[make_download_file(filename="01.flac", id="xfer-1",
                                      file_dir="Music\\Album",
                                      username="", size=50000000)],
            filetype="flac", title="Album", artist="Artist",
            mb_release_id="mb-uuid", db_request_id=42,
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        ctx = make_ctx_with_fake_db(db)

        with patch("lib.download.cancel_and_delete"), \
             patch("lib.download.apply_transition"):
            _timeout_album(entry, 42, "stalled", ctx)

        self.assertEqual(db.cooldowns_applied, [])


class TestCooldownTriggerOnRejection(unittest.TestCase):
    """reject_and_requeue() should call check_and_apply_cooldown after logging."""

    def test_rejection_triggers_cooldown_check(self):
        from album_source import DatabaseSource
        from lib.quality import DownloadInfo

        fake_db = FakePipelineDB()
        fake_db.seed_request(make_request_row(id=42, status="downloading"))
        fake_db.set_cooldown_result(True)

        source = DatabaseSource.__new__(DatabaseSource)
        source._db = fake_db  # type: ignore[assignment]

        album_record = MagicMock()
        album_record.db_request_id = 42

        bv_result = MagicMock()
        bv_result.distance = 0.5
        bv_result.scenario = "bad_match"
        bv_result.detail = "too different"
        bv_result.error = "distance too high"

        dl = DownloadInfo(username="baduser")
        cooled_down_users: set[str] = set()

        source.reject_and_requeue(album_record, bv_result,
                                  usernames=["baduser"], download_info=dl,
                                  cooled_down_users=cooled_down_users)

        # Domain state assertions
        self.assertEqual(fake_db.request(42)["status"], "wanted")
        self.assertEqual(len(fake_db.download_logs), 1)
        fake_db.assert_log(self, 0, outcome="rejected")
        self.assertIn("baduser", fake_db.cooldowns_applied)
        self.assertEqual(cooled_down_users, {"baduser"})
        self.assertEqual(len(fake_db.denylist), 1)
        self.assertEqual(fake_db.denylist[0].username, "baduser")


if __name__ == "__main__":
    unittest.main()
