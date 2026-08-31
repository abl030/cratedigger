"""Tests for lib/transitions.py — state transition validation and side effects."""

import json
import subprocess
import unittest
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from lib.transitions import (
    _IMPORTED_FIELDS,
    _UNSEARCHABLE_FIELDS,
    _WANTED_FIELDS,
    VALID_TRANSITIONS,
    RequestTransition,
    TransitionApplied,
    TransitionConflict,
    TransitionConflictKind,
    TransitionSideEffects,
    apply_transition,
    finalize_operator_request,
    finalize_request,
    publish_initialized_request,
    request_fields_cas_conflict,
    transition_conflict_http_status,
    transition_conflict_payload,
    validate_transition,
)
from tests.dispatch_helpers import handoff_automation_owner
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

    def test_exactly_11_transitions(self):
        self.assertEqual(len(VALID_TRANSITIONS), 11)

    def test_all_statuses_reachable(self):
        """Every status appears as a target at least once."""
        targets = {to_s for _, to_s in VALID_TRANSITIONS}
        self.assertEqual(
            targets, {"wanted", "downloading", "imported", "unsearchable"})

    def test_initializing_has_no_ordinary_lifecycle_edge(self):
        self.assertNotIn(("initializing", "wanted"), VALID_TRANSITIONS)
        self.assertNotIn(("initializing", "downloading"), VALID_TRANSITIONS)


class TestPublishInitializedRequest(unittest.TestCase):
    def test_publishes_fields_in_initializing_to_wanted_cas(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="initializing"))

        result = publish_initialized_request(
            db,
            1,
            fields={
                "search_filetype_override": "upgrade",
                "min_bitrate": 320,
            },
        )

        self.assertEqual(
            result,
            TransitionApplied(
                request_id=1,
                from_status="initializing",
                target_status="wanted",
            ),
        )
        row = db.request(1)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_filetype_override"], "upgrade")
        self.assertEqual(row["min_bitrate"], 320)

    def test_rejects_non_initializing_request(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="imported"))

        result = publish_initialized_request(db, 1, fields={})

        self.assertIsInstance(result, TransitionConflict)
        assert isinstance(result, TransitionConflict)
        self.assertEqual(result.kind, TransitionConflictKind.stale_source)
        self.assertEqual(db.request(1)["status"], "imported")


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

    def test_processing_owner_returns_exact_typed_conflict(self):
        db = self._make_db("wanted")
        job = handoff_automation_owner(db, 1)
        before = db.get_request(1)

        result = apply_transition(
            db,
            1,
            "wanted",
            from_status="processing",
        )

        self.assertIsInstance(result, TransitionConflict)
        assert isinstance(result, TransitionConflict)
        self.assertEqual(
            result.kind,
            TransitionConflictKind.processing_locked,
        )
        self.assertIsNotNone(result.processing_owner)
        assert result.processing_owner is not None
        self.assertEqual(result.processing_owner.job_id, job.id)
        self.assertEqual(
            transition_conflict_payload(result),
            {
                "error": "transition_conflict",
                "reason": "processing_locked",
                "request_id": 1,
                "expected_status": "processing",
                "actual_status": "processing",
                "target_status": "wanted",
                "processing_owner": {
                    "job_id": job.id,
                    "status": job.status,
                    "preview_status": job.preview_status,
                },
            },
        )
        self.assertEqual(db.get_request(1), before)

    def test_production_conflict_payload_reaches_browser_detector(self):
        """The browser consumes the real serializer, not a copied fixture."""
        db = self._make_db("wanted")
        job = handoff_automation_owner(db, 1)
        result = apply_transition(
            db,
            1,
            "wanted",
            from_status="processing",
        )
        assert isinstance(result, TransitionConflict)
        payload = transition_conflict_payload(result)
        module_url = (
            Path(__file__).parents[1]
            / "web"
            / "js"
            / "release_action_state.js"
        ).as_uri()
        script = f"""
import {{ processingConflictFromResponse }} from {json.dumps(module_url)};
let serialized = '';
for await (const chunk of process.stdin) serialized += chunk;
const conflict = processingConflictFromResponse(
  409,
  JSON.parse(serialized),
);
process.stdout.write(JSON.stringify(conflict));
"""

        completed = subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertEqual(
            json.loads(completed.stdout),
            {
                "requestId": 1,
                "owner": {
                    "job_id": job.id,
                    "status": job.status,
                    "preview_status": job.preview_status,
                },
            },
        )

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

    def test_transition_field_allowlist_membership_is_pinned(self):
        """Changing an allowlist is a deliberate act that must touch this pin.

        The round-trip tests below derive their iteration domain FROM the
        allowlists, so shrinking an allowlist silently shrinks them with it
        (#1258 mutant-runner finding A2). This pin makes membership itself
        the contract.
        """
        self.assertEqual(
            _WANTED_FIELDS,
            {
                "min_bitrate",
                "prev_min_bitrate",
                "priority_started_at",
                "search_filetype_override",
            },
        )
        self.assertEqual(
            _IMPORTED_FIELDS,
            {
                "beets_distance",
                "beets_scenario",
                "current_spectral_bitrate",
                "current_spectral_grade",
                "current_lossless_source_v0_probe_avg_bitrate",
                "current_lossless_source_v0_probe_median_bitrate",
                "current_lossless_source_v0_probe_min_bitrate",
                "final_format",
                "last_download_spectral_bitrate",
                "last_download_spectral_grade",
                "marked_incomplete_at",
                "min_bitrate",
                "prev_min_bitrate",
                "search_filetype_override",
                "verified_lossless",
            },
        )
        self.assertIs(_UNSEARCHABLE_FIELDS, _WANTED_FIELDS)

    def test_every_allowlisted_field_round_trips_through_fields_constructors(self):
        """Issue #1258 item 2: no allowlisted-but-dropped field can exist.

        The old ``to_*_fields`` shape forwarded each field explicitly, so a
        field added to the allowlist but not to the forwarding was silently
        dropped (bit the #1241 series on ``marked_incomplete_at``). Every
        member of every allowlist must survive its dict constructor verbatim.
        """
        cases = [
            ("imported", _IMPORTED_FIELDS,
             lambda fields: RequestTransition.to_imported_fields(fields=fields)),
            ("wanted", _WANTED_FIELDS,
             lambda fields: RequestTransition.to_wanted_fields(fields=fields)),
            ("unsearchable", _UNSEARCHABLE_FIELDS,
             lambda fields: RequestTransition.to_unsearchable_fields(
                 fields=fields)),
        ]
        for target, allowed, construct in cases:
            self.assertTrue(allowed, f"{target} allowlist unexpectedly empty")
            sentinel_by_field = {name: object() for name in sorted(allowed)}
            for field_name, sentinel in sentinel_by_field.items():
                with self.subTest(target=target, field=field_name):
                    transition = construct({field_name: sentinel})
                    self.assertEqual(
                        dict(transition.fields), {field_name: sentinel})
            with self.subTest(target=target, field="<all-at-once>"):
                transition = construct(dict(sentinel_by_field))
                self.assertEqual(dict(transition.fields), sentinel_by_field)

    def test_every_allowlisted_field_round_trips_through_typed_constructors(self):
        """The kwarg veneers must accept and carry every allowlisted field.

        A frozenset member with no matching kwarg raises TypeError here; a
        kwarg accepted but not threaded into the fields dict fails the
        equality — either way the drop is loud, never silent.
        """
        cases = [
            ("imported", _IMPORTED_FIELDS,
             lambda kwargs: RequestTransition.to_imported(**kwargs)),
            ("wanted", _WANTED_FIELDS,
             lambda kwargs: RequestTransition.to_wanted(**kwargs)),
        ]
        for target, allowed, construct in cases:
            sentinel_by_field = {name: object() for name in sorted(allowed)}
            for field_name, sentinel in sentinel_by_field.items():
                with self.subTest(target=target, field=field_name):
                    transition = construct({field_name: sentinel})
                    self.assertEqual(
                        dict(transition.fields), {field_name: sentinel})
            with self.subTest(target=target, field="<all-at-once>"):
                transition = construct(dict(sentinel_by_field))
                self.assertEqual(dict(transition.fields), sentinel_by_field)

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


class TestTransitionConflictHttpStatus(unittest.TestCase):
    """Both operator surfaces classify a conflict through this one map."""

    def _conflict(self, kind: TransitionConflictKind) -> TransitionConflict:
        return TransitionConflict(
            request_id=1,
            target_status="wanted",
            kind=kind,
            expected_status=None,
            actual_status=None,
        )

    def test_kinds_map_to_the_convention(self) -> None:
        cases = [
            (TransitionConflictKind.not_found, 404),
            (TransitionConflictKind.invalid_edge, 409),
            (TransitionConflictKind.stale_source, 409),
            (TransitionConflictKind.processing_locked, 409),
        ]
        for kind, expected in cases:
            with self.subTest(kind=kind):
                self.assertEqual(
                    transition_conflict_http_status(self._conflict(kind)),
                    expected,
                )


class TestRequestFieldsCasConflict(unittest.TestCase):
    """One shared explanation for a metadata compare-and-set miss."""

    def test_vanished_row_is_not_found(self) -> None:
        conflict = request_fields_cas_conflict(
            FakePipelineDB(), 41, expected_status="wanted",
        )
        self.assertEqual(conflict.kind, TransitionConflictKind.not_found)
        self.assertEqual(conflict.expected_status, "wanted")
        self.assertIsNone(conflict.actual_status)

    def test_concurrent_lifecycle_change_is_stale_source(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="imported"))
        conflict = request_fields_cas_conflict(
            db, 42, expected_status="wanted",
        )
        self.assertEqual(conflict.kind, TransitionConflictKind.stale_source)
        self.assertEqual(conflict.actual_status, "imported")

    def test_processing_row_names_the_exact_owner(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=43, status="wanted"))
        owner = handoff_automation_owner(db, 43)
        conflict = request_fields_cas_conflict(
            db, 43, expected_status="wanted",
        )
        self.assertEqual(
            conflict.kind, TransitionConflictKind.processing_locked
        )
        assert conflict.processing_owner is not None
        self.assertEqual(conflict.processing_owner.job_id, owner.id)


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
