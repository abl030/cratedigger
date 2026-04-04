"""Tests for lib/transitions.py — state transition validation and side effects."""

import unittest

from lib.transitions import (
    VALID_TRANSITIONS,
    TransitionSideEffects,
    validate_transition,
    transition_side_effects,
)


class TestValidateTransition(unittest.TestCase):
    """All valid transitions return True, invalid ones return False."""

    def test_wanted_to_downloading(self):
        self.assertTrue(validate_transition("wanted", "downloading"))

    def test_downloading_to_imported(self):
        self.assertTrue(validate_transition("downloading", "imported"))

    def test_downloading_to_wanted(self):
        self.assertTrue(validate_transition("downloading", "wanted"))

    def test_downloading_to_manual(self):
        self.assertTrue(validate_transition("downloading", "manual"))

    def test_wanted_to_manual(self):
        self.assertTrue(validate_transition("wanted", "manual"))

    def test_imported_to_wanted(self):
        self.assertTrue(validate_transition("imported", "wanted"))

    def test_imported_to_imported(self):
        self.assertTrue(validate_transition("imported", "imported"))

    def test_manual_to_wanted(self):
        self.assertTrue(validate_transition("manual", "wanted"))

    # Invalid transitions
    def test_imported_to_downloading_invalid(self):
        self.assertFalse(validate_transition("imported", "downloading"))

    def test_manual_to_downloading_invalid(self):
        self.assertFalse(validate_transition("manual", "downloading"))

    def test_wanted_to_imported(self):
        self.assertTrue(validate_transition("wanted", "imported"))

    def test_manual_to_imported(self):
        self.assertTrue(validate_transition("manual", "imported"))

    def test_downloading_to_downloading_invalid(self):
        self.assertFalse(validate_transition("downloading", "downloading"))

    def test_unknown_status_invalid(self):
        self.assertFalse(validate_transition("unknown", "wanted"))
        self.assertFalse(validate_transition("wanted", "unknown"))


class TestTransitionSideEffects(unittest.TestCase):
    """Each transition returns the correct side-effect flags."""

    def test_downloading_to_wanted_clears_and_records(self):
        fx = transition_side_effects("downloading", "wanted")
        self.assertTrue(fx.clear_download_state)
        self.assertTrue(fx.record_attempt)
        self.assertFalse(fx.clear_retry_counters)

    def test_downloading_to_imported_clears_state(self):
        fx = transition_side_effects("downloading", "imported")
        self.assertTrue(fx.clear_download_state)
        self.assertFalse(fx.record_attempt)
        self.assertFalse(fx.clear_retry_counters)

    def test_downloading_to_manual_clears_state(self):
        fx = transition_side_effects("downloading", "manual")
        self.assertTrue(fx.clear_download_state)
        self.assertFalse(fx.record_attempt)

    def test_wanted_to_downloading_no_clearing(self):
        fx = transition_side_effects("wanted", "downloading")
        self.assertFalse(fx.clear_download_state)
        self.assertFalse(fx.record_attempt)
        self.assertFalse(fx.clear_retry_counters)

    def test_imported_to_wanted_clears_retry_counters(self):
        fx = transition_side_effects("imported", "wanted")
        self.assertTrue(fx.clear_retry_counters)
        self.assertFalse(fx.record_attempt)
        self.assertFalse(fx.clear_download_state)

    def test_manual_to_wanted_clears_retry_counters(self):
        fx = transition_side_effects("manual", "wanted")
        self.assertTrue(fx.clear_retry_counters)

    def test_imported_to_imported_clears_state(self):
        """In-place update on imported clears download state."""
        fx = transition_side_effects("imported", "imported")
        self.assertTrue(fx.clear_download_state)
        self.assertFalse(fx.record_attempt)

    def test_wanted_to_manual_no_effects(self):
        fx = transition_side_effects("wanted", "manual")
        self.assertFalse(fx.clear_download_state)
        self.assertFalse(fx.record_attempt)
        self.assertFalse(fx.clear_retry_counters)

    def test_manual_to_imported_clears_state(self):
        """Force-import from manual status."""
        fx = transition_side_effects("manual", "imported")
        self.assertTrue(fx.clear_download_state)

    def test_wanted_to_imported_clears_state(self):
        """Admin accept from wanted status."""
        fx = transition_side_effects("wanted", "imported")
        self.assertTrue(fx.clear_download_state)

    def test_invalid_transition_raises(self):
        with self.assertRaises(ValueError):
            transition_side_effects("imported", "downloading")


class TestTransitionTable(unittest.TestCase):
    """Structural tests on the transition table itself."""

    def test_all_entries_are_typed(self):
        for (from_s, to_s), fx in VALID_TRANSITIONS.items():
            self.assertIsInstance(fx, TransitionSideEffects,
                                 f"({from_s}, {to_s}) is not TransitionSideEffects")

    def test_exactly_10_transitions(self):
        self.assertEqual(len(VALID_TRANSITIONS), 10)

    def test_all_statuses_reachable(self):
        """Every status appears as a target at least once."""
        targets = {to_s for _, to_s in VALID_TRANSITIONS}
        self.assertEqual(targets, {"wanted", "downloading", "imported", "manual"})


if __name__ == "__main__":
    unittest.main()
