"""Tests for lib/transitions.py — state transition validation and side effects."""

import unittest
from typing import Any, cast

from lib.transitions import (
    VALID_TRANSITIONS,
    RequestTransition,
    TransitionSideEffects,
    apply_transition,
    finalize_request,
    transition_side_effects,
    validate_transition,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


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
    """Tests for the imperative apply_transition function.

    All tests drive real ``apply_transition`` against a ``FakePipelineDB``
    seeded with the relevant starting state, then assert on the resulting
    row. The migration replaces ``MagicMock`` + ``mock.assert_called_with``
    introspection with observable DB-state assertions.
    """

    def _make_db(self, current_status: str = "wanted") -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status=current_status))
        return db

    def test_downloading_to_imported_sets_status(self):
        db = self._make_db("downloading")
        apply_transition(
            cast(Any, db), 1, "imported", from_status="downloading",
        )
        self.assertEqual(db.request(1)["status"], "imported")

    def test_downloading_to_wanted_clears_state_and_records_attempt(self):
        db = self._make_db("downloading")
        apply_transition(
            cast(Any, db), 1, "wanted", from_status="downloading",
            search_filetype_override="flac",
            attempt_type="download",
        )
        row = db.request(1)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "flac")
        self.assertEqual(row["download_attempts"], 1)
        # Active download state cleared
        self.assertIsNone(row["active_download_state"])

    def test_downloading_to_wanted_guard_failure_skips_attempt_record(self):
        """Guard refuses non-downloading rows. ``apply_transition`` returns
        False and ``record_attempt`` must not advance the counter."""
        # Seed the row as 'wanted' so reset_downloading_to_wanted's guard
        # refuses the change (status != 'downloading').
        db = self._make_db("wanted")
        result = apply_transition(
            cast(Any, db), 1, "wanted", from_status="downloading",
            attempt_type="download",
        )
        self.assertFalse(result)
        self.assertEqual(db.request(1)["download_attempts"], 0)

    def test_imported_to_wanted_resets_and_clears_retry_counters(self):
        db = self._make_db("imported")
        apply_transition(
            cast(Any, db), 1, "wanted", from_status="imported",
            search_filetype_override="flac,mp3 v0,mp3 320",
            min_bitrate=245,
        )
        row = db.request(1)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(
            row["search_filetype_override"], "flac,mp3 v0,mp3 320",
        )
        self.assertEqual(row["min_bitrate"], 245)

    def test_wanted_to_downloading_sets_state(self):
        db = self._make_db("wanted")
        apply_transition(
            cast(Any, db), 1, "downloading", from_status="wanted",
            state_json='{"filetype":"flac"}',
        )
        row = db.request(1)
        self.assertEqual(row["status"], "downloading")
        self.assertEqual(row["active_download_state"], '{"filetype":"flac"}')

    def test_auto_detects_from_status(self):
        """No ``from_status`` arg → the seam looks up the current status
        from the row. Implicit verification: the transition succeeds (the
        guard would refuse if from_status were wrong)."""
        db = self._make_db("downloading")
        apply_transition(cast(Any, db), 1, "imported")
        self.assertEqual(db.request(1)["status"], "imported")

    def test_extra_fields_persist_through_update_status(self):
        db = self._make_db("downloading")
        apply_transition(
            cast(Any, db), 1, "imported", from_status="downloading",
            min_bitrate=245, last_download_spectral_grade="genuine",
        )
        row = db.request(1)
        self.assertEqual(row["status"], "imported")
        self.assertEqual(row["min_bitrate"], 245)
        self.assertEqual(row["last_download_spectral_grade"], "genuine")

    def test_invalid_transition_logs_warning_but_proceeds_to_seam(self):
        """Invalid transitions still proceed to the seam (with warning)
        for backward compatibility — see lib.transitions.apply_transition
        line 451. Whether the seam itself accepts the change is a
        separate concern: set_downloading has a SQL guard
        ``WHERE status='wanted'``, so a manual → downloading attempt
        logs the validity warning AND then logs the guard rejection.
        The row stays at ``manual``."""
        db = self._make_db("manual")
        with self.assertLogs("cratedigger", level="WARNING") as cm:
            apply_transition(
                cast(Any, db), 1, "downloading", from_status="manual",
                state_json='{}',
            )
        self.assertTrue(any("invalid" in m.lower() for m in cm.output))
        # Row stays at manual — the seam's own guard refused the
        # change. This matches production behavior; the previous
        # MagicMock test only verified the seam was called and
        # missed that production wouldn't actually have moved the
        # row either.
        self.assertEqual(db.request(1)["status"], "manual")

    def test_downloading_guard_logs_when_set_downloading_refuses(self):
        """When ``set_downloading`` returns False (row no longer wanted),
        the transition logs a warning and the row's status stays."""
        # Seed as 'imported' so set_downloading's guard refuses the change.
        db = self._make_db("imported")
        with self.assertLogs("cratedigger", level="WARNING") as cm:
            apply_transition(
                cast(Any, db), 1, "downloading", from_status="wanted",
                state_json='{"filetype":"flac"}',
            )
        self.assertTrue(any("status guard" in msg for msg in cm.output))
        # Status unchanged.
        self.assertEqual(db.request(1)["status"], "imported")

    def test_downloading_requires_state_json(self):
        db = self._make_db("wanted")
        with self.assertRaisesRegex(ValueError, "state_json"):
            apply_transition(
                cast(Any, db), 1, "downloading", from_status="wanted",
            )
        # ValueError fires before any DB mutation — row unchanged.
        self.assertEqual(db.request(1)["status"], "wanted")
        self.assertIsNone(db.request(1)["active_download_state"])

    def test_request_not_found_returns_without_writing(self):
        """No row for the request → apply_transition returns without
        any update. The empty DB stays empty."""
        db = FakePipelineDB()  # no rows seeded
        # auto-detect from_status path queries the row first, finds None,
        # logs, returns False.
        result = apply_transition(cast(Any, db), 999, "imported")
        self.assertFalse(result)
        self.assertIsNone(db._requests.get(999))

    def test_wanted_to_manual_sets_status(self):
        db = self._make_db("wanted")
        apply_transition(cast(Any, db), 1, "manual", from_status="wanted")
        self.assertEqual(db.request(1)["status"], "manual")


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

    def test_wanted_fields_reject_imported_only_fields(self):
        with self.assertRaisesRegex(ValueError, "imported_path"):
            RequestTransition.to_wanted_fields(
                fields={"imported_path": "/Beets/Artist/Album"})

    def test_imported_fields_reject_downloading_only_fields(self):
        with self.assertRaisesRegex(ValueError, "state_json"):
            RequestTransition.to_imported_fields(fields={"state_json": "{}"})

    def test_transition_fields_are_immutable(self):
        transition = RequestTransition.to_manual(from_status="wanted")

        with self.assertRaises(TypeError):
            cast(Any, transition.fields)["imported_path"] = "/Beets/Artist/Album"

    def test_status_only_rejects_downloading_without_state(self):
        with self.assertRaisesRegex(ValueError, "state_json"):
            RequestTransition.status_only("downloading", from_status="wanted")


class TestFinalizeRequest(unittest.TestCase):
    """Final request-state command execution lives in lib.transitions.

    Tests drive real ``finalize_request`` against a ``FakePipelineDB``
    and assert on the resulting row. Validation-error tests verify the
    DB row stays unchanged when the transition raises before any
    mutation. (Migrated from MagicMock + ``mock.assert_called_with``
    introspection per issue #290.)
    """

    def test_forwards_transition_fields_and_attempt_type(self):
        db = FakePipelineDB()
        db.seed_request(
            make_request_row(
                id=42, status="downloading",
                search_filetype_override=None,
                min_bitrate=320,
                prev_min_bitrate=None,
            ),
        )
        transition = RequestTransition.to_wanted(
            from_status="downloading",
            attempt_type="download",
            search_filetype_override="flac,mp3 v0",
            min_bitrate=245,
        )
        finalize_request(cast(Any, db), 42, transition)

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "flac,mp3 v0")
        self.assertEqual(row["min_bitrate"], 245)
        self.assertEqual(row["prev_min_bitrate"], 320)
        self.assertEqual(row["download_attempts"], 1)

    def test_rejects_direct_constructor_wrong_fields_at_finalization(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        transition = RequestTransition(
            "manual",
            from_status="wanted",
            fields={"imported_path": "/Beets/Artist/Album"},
        )

        with self.assertRaisesRegex(ValueError, "manual transitions"):
            finalize_request(cast(Any, db), 42, transition)

        # ValueError fires upstream of any DB mutation — row unchanged.
        self.assertEqual(db.request(42)["status"], "wanted")

    def test_rejects_downloading_without_state_at_finalization(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        transition = RequestTransition("downloading", from_status="wanted")

        with self.assertRaisesRegex(ValueError, "state_json"):
            finalize_request(cast(Any, db), 42, transition)

        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertIsNone(db.request(42)["active_download_state"])

    def test_rejects_downloading_with_explicit_none_state_at_finalization(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        transition = RequestTransition(
            "downloading",
            from_status="wanted",
            fields={"state_json": None},
        )

        with self.assertRaisesRegex(ValueError, "state_json"):
            finalize_request(cast(Any, db), 42, transition)

        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertIsNone(db.request(42)["active_download_state"])


if __name__ == "__main__":
    unittest.main()
