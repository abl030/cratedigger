"""Tests for lib/transitions.py — state transition validation and side effects."""

import unittest
from typing import Any, TYPE_CHECKING, cast

from lib.transitions import (
    VALID_TRANSITIONS,
    RequestTransition,
    TransitionApplied,
    TransitionConflict,
    TransitionConflictKind,
    TransitionSideEffects,
    apply_transition,
    finalize_operator_request,
    finalize_request,
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

    def test_downloading_to_unsearchable_is_invalid(self):
        self.assertFalse(validate_transition("downloading", "unsearchable"))

    def test_wanted_to_unsearchable(self):
        self.assertTrue(validate_transition("wanted", "unsearchable"))

    def test_imported_to_wanted(self):
        self.assertTrue(validate_transition("imported", "wanted"))

    def test_imported_to_imported(self):
        self.assertTrue(validate_transition("imported", "imported"))

    def test_imported_to_unsearchable_is_invalid(self):
        self.assertFalse(validate_transition("imported", "unsearchable"))

    def test_unsearchable_to_wanted(self):
        self.assertTrue(validate_transition("unsearchable", "wanted"))

    def test_unsearchable_to_unsearchable(self):
        self.assertTrue(validate_transition("unsearchable", "unsearchable"))

    # Invalid transitions
    def test_imported_to_downloading_invalid(self):
        self.assertFalse(validate_transition("imported", "downloading"))

    def test_unsearchable_to_downloading_invalid(self):
        self.assertFalse(validate_transition("unsearchable", "downloading"))

    def test_wanted_to_imported(self):
        self.assertTrue(validate_transition("wanted", "imported"))

    def test_unsearchable_to_imported(self):
        self.assertTrue(validate_transition("unsearchable", "imported"))

    def test_downloading_to_downloading_invalid(self):
        self.assertFalse(validate_transition("downloading", "downloading"))

    def test_unknown_status_invalid(self):
        self.assertFalse(validate_transition("unknown", "wanted"))
        self.assertFalse(validate_transition("wanted", "unknown"))


class TestTransitionSideEffects(unittest.TestCase):
    """Each transition returns the correct side-effect flags."""

    def test_downloading_to_wanted_records_attempt(self):
        fx = VALID_TRANSITIONS[("downloading", "wanted")]
        self.assertTrue(fx.record_attempt)
        self.assertFalse(fx.clear_retry_counters)

    def test_downloading_to_imported_no_effects(self):
        fx = VALID_TRANSITIONS[("downloading", "imported")]
        self.assertFalse(fx.record_attempt)
        self.assertFalse(fx.clear_retry_counters)

    def test_wanted_to_downloading_no_effects(self):
        fx = VALID_TRANSITIONS[("wanted", "downloading")]
        self.assertFalse(fx.record_attempt)
        self.assertFalse(fx.clear_retry_counters)

    def test_imported_to_wanted_clears_retry_counters(self):
        fx = VALID_TRANSITIONS[("imported", "wanted")]
        self.assertTrue(fx.clear_retry_counters)
        self.assertFalse(fx.record_attempt)

    def test_unsearchable_to_wanted_clears_retry_counters(self):
        fx = VALID_TRANSITIONS[("unsearchable", "wanted")]
        self.assertTrue(fx.clear_retry_counters)

    def test_wanted_to_unsearchable_no_effects(self):
        fx = VALID_TRANSITIONS[("wanted", "unsearchable")]
        self.assertFalse(fx.record_attempt)
        self.assertFalse(fx.clear_retry_counters)

    def test_invalid_transition_returns_none(self):
        """Invalid transitions are absent from the table."""
        self.assertNotIn(("imported", "downloading"), VALID_TRANSITIONS)


class TestTransitionTable(unittest.TestCase):
    """Structural tests on the transition table itself."""

    def test_all_entries_are_typed(self):
        for (from_s, to_s), fx in VALID_TRANSITIONS.items():
            self.assertIsInstance(fx, TransitionSideEffects,
                                 f"({from_s}, {to_s}) is not TransitionSideEffects")

    def test_exactly_12_transitions(self):
        self.assertEqual(len(VALID_TRANSITIONS), 12)

    def test_all_statuses_reachable(self):
        """Every status appears as a target at least once."""
        targets = {to_s for _, to_s in VALID_TRANSITIONS}
        self.assertEqual(
            targets, {"wanted", "downloading", "imported", "unsearchable"})

    def test_initializing_can_only_publish_to_wanted(self):
        self.assertIn(("initializing", "wanted"), VALID_TRANSITIONS)
        self.assertNotIn(("initializing", "downloading"), VALID_TRANSITIONS)


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
        self.assertIsInstance(result, TransitionConflict)
        assert isinstance(result, TransitionConflict)
        self.assertEqual(result.kind, TransitionConflictKind.stale_source)
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

    def test_invalid_transition_fails_closed_before_any_mutation_seam(self):
        """An invalid edge is a typed conflict and never reaches a writer."""
        db = self._make_db("unsearchable")
        before_history = list(db.status_history)
        result = apply_transition(
            cast(Any, db), 1, "downloading", from_status="unsearchable",
            state_json='{}',
        )

        self.assertIsInstance(result, TransitionConflict)
        assert isinstance(result, TransitionConflict)
        self.assertEqual(result.kind, TransitionConflictKind.invalid_edge)
        self.assertEqual(db.status_history, before_history)
        self.assertEqual(db.request(1)["status"], "unsearchable")

    def test_downloading_guard_logs_when_set_downloading_refuses(self):
        """When ``set_downloading`` returns False (row no longer wanted),
        the transition logs a warning and the row's status stays."""
        # Seed as 'imported' so set_downloading's guard refuses the change.
        db = self._make_db("imported")
        result = apply_transition(
            cast(Any, db), 1, "downloading", from_status="wanted",
            state_json='{"filetype":"flac"}',
        )
        self.assertIsInstance(result, TransitionConflict)
        assert isinstance(result, TransitionConflict)
        self.assertEqual(result.kind, TransitionConflictKind.stale_source)
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
        # returns a typed not-found conflict.
        result = apply_transition(cast(Any, db), 999, "imported")
        self.assertIsInstance(result, TransitionConflict)
        assert isinstance(result, TransitionConflict)
        self.assertEqual(result.kind, TransitionConflictKind.not_found)
        self.assertIsNone(db._requests.get(999))

    def test_wanted_to_unsearchable_sets_status(self):
        db = self._make_db("wanted")
        result = apply_transition(
            cast(Any, db), 1, "unsearchable", from_status="wanted")
        self.assertIsInstance(result, TransitionApplied)
        self.assertEqual(db.request(1)["status"], "unsearchable")

    def test_imported_to_unsearchable_is_a_conflict(self):
        db = self._make_db("imported")
        result = apply_transition(
            cast(Any, db), 1, "unsearchable", from_status="imported")
        self.assertIsInstance(result, TransitionConflict)
        assert isinstance(result, TransitionConflict)
        self.assertEqual(result.kind, TransitionConflictKind.invalid_edge)
        self.assertEqual(db.request(1)["status"], "imported")

    def test_operator_same_status_is_byte_identical_success(self):
        for status in ("wanted", "imported", "unsearchable"):
            with self.subTest(status=status):
                db = self._make_db(status)
                before = db.request(1)
                result = apply_transition(
                    cast(Any, db), 1, status, from_status=status)
                self.assertIsInstance(result, TransitionApplied)
                self.assertEqual(db.request(1), before)

    def test_explicit_source_is_validated_against_the_actual_row(self):
        db = self._make_db("imported")
        before = db.request(1)

        result = apply_transition(
            cast(Any, db), 1, "unsearchable", from_status="wanted")

        self.assertIsInstance(result, TransitionConflict)
        assert isinstance(result, TransitionConflict)
        self.assertEqual(result.kind, TransitionConflictKind.stale_source)
        self.assertEqual(result.actual_status, "imported")
        self.assertEqual(db.request(1), before)

    def test_replaced_row_cannot_be_resurrected(self):
        db = self._make_db("replaced")
        before = db.request(1)

        for target in ("wanted", "unsearchable", "imported", "downloading"):
            kwargs = {"state_json": "{}"} if target == "downloading" else {}
            result = apply_transition(
                cast(Any, db), 1, target, from_status="replaced", **kwargs)
            self.assertIsInstance(result, TransitionConflict)
            assert isinstance(result, TransitionConflict)
            self.assertEqual(result.kind, TransitionConflictKind.invalid_edge)
            self.assertEqual(db.request(1), before)


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

    def test_transition_rejects_removed_imported_path_parameter(self):
        with self.assertRaises(TypeError):
            RequestTransition.to_wanted(imported_path="/Beets/Artist/Album")  # type: ignore[call-arg]

    def test_wanted_fields_reject_removed_imported_path(self):
        with self.assertRaisesRegex(ValueError, "imported_path"):
            RequestTransition.to_wanted_fields(
                fields={"imported_path": "/Beets/Artist/Album"})

    def test_imported_fields_reject_removed_imported_path(self):
        with self.assertRaisesRegex(ValueError, "imported_path"):
            RequestTransition.to_imported_fields(
                fields={"imported_path": "/Beets/Artist/Album"})

    def test_imported_fields_reject_downloading_only_fields(self):
        with self.assertRaisesRegex(ValueError, "state_json"):
            RequestTransition.to_imported_fields(fields={"state_json": "{}"})

    def test_transition_fields_are_immutable(self):
        transition = RequestTransition.to_unsearchable(from_status="wanted")

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

    def test_operator_stop_rebases_to_conflict_after_terminal_import_wins(self):
        class RacingFakePipelineDB(FakePipelineDB):
            terminal_won = False

            def update_status(
                self,
                request_id: int,
                status: str,
                *,
                expected_status: str | None = None,
                **extra: Any,
            ) -> bool:
                if not self.terminal_won:
                    self.terminal_won = True
                    self._requests[request_id]["status"] = "imported"
                    return False
                return super().update_status(
                    request_id,
                    status,
                    expected_status=expected_status,
                    **extra,
                )

        db = RacingFakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))

        result = finalize_operator_request(
            cast(Any, db),
            42,
            RequestTransition.to_unsearchable(from_status="wanted"),
        )

        self.assertIsInstance(result, TransitionConflict)
        assert isinstance(result, TransitionConflict)
        self.assertEqual(result.kind, TransitionConflictKind.invalid_edge)
        self.assertEqual(db.request(42)["status"], "imported")

    def test_explicit_previous_bitrate_survives_operator_requeue(self):
        """The typed wanted command's public fields reach the reset CAS."""
        db = FakePipelineDB()
        db.seed_request(
            make_request_row(
                id=42,
                status="unsearchable",
                min_bitrate=320,
                prev_min_bitrate=192,
            ),
        )

        result = finalize_request(
            cast(Any, db),
            42,
            RequestTransition.to_wanted(
                from_status="unsearchable",
                min_bitrate=245,
                prev_min_bitrate=256,
            ),
        )

        self.assertIsInstance(result, TransitionApplied)
        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["min_bitrate"], 245)
        self.assertEqual(row["prev_min_bitrate"], 256)

    def test_rejects_direct_constructor_wrong_fields_at_finalization(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        transition = RequestTransition(
            "unsearchable",
            from_status="wanted",
            fields={"imported_path": "/Beets/Artist/Album"},
        )

        with self.assertRaisesRegex(ValueError, "unsearchable transitions"):
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


if TYPE_CHECKING:
    from lib.pipeline_db import PipelineDB
    from lib.transitions import TransitionsDB as _TransitionsDB

    # Static parity proof (#409) — see the matching block in
    # tests/test_wrong_match_cleanup_service.py for the rationale.
    _pipeline_db_satisfies_transitions_protocol: _TransitionsDB = cast("PipelineDB", None)
    _fake_db_satisfies_transitions_protocol: _TransitionsDB = cast("FakePipelineDB", None)


class TestTransitionsDBProtocolParity(unittest.TestCase):
    """#409: PipelineDB and FakePipelineDB must satisfy TransitionsDB."""

    def test_pipeline_db_satisfies_protocol(self) -> None:
        from lib.pipeline_db import PipelineDB
        from lib.transitions import TransitionsDB

        self.assertTrue(issubclass(PipelineDB, TransitionsDB))

    def test_fake_pipeline_db_satisfies_protocol(self) -> None:
        from lib.transitions import TransitionsDB

        self.assertTrue(issubclass(FakePipelineDB, TransitionsDB))


if __name__ == "__main__":
    unittest.main()
