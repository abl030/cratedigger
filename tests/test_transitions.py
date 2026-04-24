"""Tests for lib/transitions.py — state transition validation and side effects."""

import unittest
from typing import Any, cast
from unittest.mock import MagicMock, patch

from lib.transitions import (
    VALID_TRANSITIONS,
    RequestTransition,
    TransitionSideEffects,
    apply_transition,
    finalize_request,
    transition_side_effects,
    validate_transition,
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

    def test_exactly_11_transitions(self):
        self.assertEqual(len(VALID_TRANSITIONS), 11)

    def test_all_statuses_reachable(self):
        """Every status appears as a target at least once."""
        targets = {to_s for _, to_s in VALID_TRANSITIONS}
        self.assertEqual(targets, {"wanted", "downloading", "imported", "manual"})


class TestApplyTransition(unittest.TestCase):
    """Tests for the imperative apply_transition function."""

    def _make_db(self, current_status="wanted"):
        db = MagicMock()
        db.get_request.return_value = {"status": current_status}
        return db

    def test_downloading_to_imported_calls_update_status(self):
        db = self._make_db("downloading")
        apply_transition(db, 1, "imported", from_status="downloading")
        db.update_status.assert_called_once_with(1, "imported")

    def test_downloading_to_wanted_calls_reset(self):
        db = self._make_db("downloading")
        apply_transition(db, 1, "wanted", from_status="downloading",
                         search_filetype_override="flac", attempt_type="download")
        db.reset_to_wanted.assert_called_once_with(
            1, search_filetype_override="flac")
        db.record_attempt.assert_called_once_with(1, "download")

    def test_imported_to_wanted_calls_reset(self):
        db = self._make_db("imported")
        apply_transition(db, 1, "wanted", from_status="imported",
                         search_filetype_override="flac,mp3 v0,mp3 320",
                         min_bitrate=245)
        db.reset_to_wanted.assert_called_once_with(
            1, search_filetype_override="flac,mp3 v0,mp3 320", min_bitrate=245)

    def test_wanted_to_downloading_calls_set_downloading(self):
        db = self._make_db("wanted")
        apply_transition(db, 1, "downloading", from_status="wanted",
                         state_json='{"filetype":"flac"}')
        db.set_downloading.assert_called_once_with(1, '{"filetype":"flac"}')

    def test_auto_detects_from_status(self):
        db = self._make_db("downloading")
        apply_transition(db, 1, "imported")
        db.get_request.assert_called_once_with(1)
        db.update_status.assert_called_once_with(1, "imported")

    def test_extra_fields_passed_to_update_status(self):
        db = self._make_db("downloading")
        apply_transition(db, 1, "imported", from_status="downloading",
                         min_bitrate=245, last_download_spectral_grade="genuine")
        db.update_status.assert_called_once_with(
            1, "imported", min_bitrate=245,
            last_download_spectral_grade="genuine")

    def test_invalid_transition_logs_warning(self):
        """Invalid transitions still proceed (with warning) for backward compat."""
        db = self._make_db("manual")
        apply_transition(db, 1, "downloading", from_status="manual",
                         state_json='{}')
        # Should still call set_downloading despite invalid transition
        db.set_downloading.assert_called_once()

    def test_downloading_guard_logs_when_rejected(self):
        """When set_downloading returns False, transition logs a warning."""
        db = self._make_db("wanted")
        db.set_downloading.return_value = False
        with self.assertLogs("cratedigger", level="WARNING") as cm:
            apply_transition(db, 1, "downloading", from_status="wanted",
                             state_json='{"filetype":"flac"}')
        self.assertTrue(any("status guard" in msg for msg in cm.output))

    def test_downloading_requires_state_json(self):
        db = self._make_db("wanted")

        with self.assertRaisesRegex(ValueError, "state_json"):
            apply_transition(db, 1, "downloading", from_status="wanted")

        db.set_downloading.assert_not_called()
        db.update_status.assert_not_called()

    def test_request_not_found(self):
        db = MagicMock()
        db.get_request.return_value = None
        apply_transition(db, 999, "imported")
        db.update_status.assert_not_called()

    def test_wanted_to_manual(self):
        db = self._make_db("wanted")
        apply_transition(db, 1, "manual", from_status="wanted")
        db.update_status.assert_called_once_with(1, "manual")


class TestRequestTransition(unittest.TestCase):
    """Target-specific request-transition commands."""

    def test_wanted_transition_forwards_common_fields_and_attempt_type(self):
        transition = RequestTransition.to_wanted(
            from_status="downloading",
            attempt_type="download",
            search_filetype_override="flac,mp3 v0",
            min_bitrate=245,
            prev_min_bitrate=320,
        )

        self.assertEqual(transition.target_status, "wanted")
        self.assertEqual(transition.from_status, "downloading")
        self.assertEqual(transition.attempt_type, "download")
        self.assertEqual(
            transition.fields,
            {
                "search_filetype_override": "flac,mp3 v0",
                "min_bitrate": 245,
                "prev_min_bitrate": 320,
            },
        )

    def test_imported_transition_preserves_explicit_none_for_clears(self):
        transition = RequestTransition.to_imported(
            from_status="imported",
            search_filetype_override=None,
            min_bitrate=245,
        )

        self.assertEqual(
            transition.fields,
            {
                "search_filetype_override": None,
                "min_bitrate": 245,
            },
        )

    def test_wanted_transition_rejects_imported_only_fields(self):
        with self.assertRaises(TypeError):
            RequestTransition.to_wanted(imported_path="/Beets/Artist/Album")  # type: ignore[call-arg]

    def test_transition_fields_are_immutable(self):
        transition = RequestTransition.to_manual(from_status="wanted")

        with self.assertRaises(TypeError):
            cast(Any, transition.fields)["imported_path"] = "/Beets/Artist/Album"

    def test_status_only_rejects_downloading_without_state(self):
        with self.assertRaisesRegex(ValueError, "state_json"):
            RequestTransition.status_only("downloading", from_status="wanted")


class TestFinalizeRequest(unittest.TestCase):
    """Final request-state command execution lives in lib.transitions."""

    def test_forwards_transition_fields_and_attempt_type(self):
        db = MagicMock()
        transition = RequestTransition.to_wanted(
            from_status="downloading",
            attempt_type="download",
            search_filetype_override="flac,mp3 v0",
            min_bitrate=245,
            prev_min_bitrate=320,
        )

        with patch("lib.transitions.apply_transition") as mock_apply:
            finalize_request(db, 42, transition)

        mock_apply.assert_called_once_with(
            db,
            42,
            "wanted",
            from_status="downloading",
            attempt_type="download",
            search_filetype_override="flac,mp3 v0",
            min_bitrate=245,
            prev_min_bitrate=320,
        )

    def test_rejects_direct_constructor_wrong_fields_at_finalization(self):
        db = MagicMock()
        transition = RequestTransition(
            "manual",
            from_status="wanted",
            fields={"imported_path": "/Beets/Artist/Album"},
        )

        with patch("lib.transitions.apply_transition") as mock_apply:
            with self.assertRaisesRegex(ValueError, "manual transitions"):
                finalize_request(db, 42, transition)

        mock_apply.assert_not_called()

    def test_rejects_downloading_without_state_at_finalization(self):
        db = MagicMock()
        transition = RequestTransition("downloading", from_status="wanted")

        with patch("lib.transitions.apply_transition") as mock_apply:
            with self.assertRaisesRegex(ValueError, "state_json"):
                finalize_request(db, 42, transition)

        mock_apply.assert_not_called()

    def test_rejects_downloading_with_explicit_none_state_at_finalization(self):
        db = MagicMock()
        transition = RequestTransition(
            "downloading",
            from_status="wanted",
            fields={"state_json": None},
        )

        with patch("lib.transitions.apply_transition") as mock_apply:
            with self.assertRaisesRegex(ValueError, "state_json"):
                finalize_request(db, 42, transition)

        mock_apply.assert_not_called()


if __name__ == "__main__":
    unittest.main()
