"""Tests for global user cooldown system (issue #39)."""

import unittest

from lib.quality import CooldownConfig, should_cooldown


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


if __name__ == "__main__":
    unittest.main()
