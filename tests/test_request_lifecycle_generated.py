"""Generated request-lifecycle tests — issue #548 follow-up.

A Hypothesis ``RuleBasedStateMachine`` drives random sequences of REAL
production transitions — ``lib/transitions.py::finalize_request`` (the
single status-mutation funnel) and ``PipelineDB.supersede_request_mbid``
(Replace) — against a ``FakePipelineDB``, then asserts the archivist
lifecycle invariants after every step:

* every row's status stays in the legal set (the CHECK constraint's
  vocabulary — the fake doesn't enforce it, so the machine must);
* ``replaced`` rows are terminal and FROZEN — nothing mutates them after
  the supersede, ever (the request is the source of truth; operator
  actions supersede rows, they don't rewrite them);
* request identity (mb_release_id / source / created_at) is immutable
  for every row from creation onward;
* every replaced row has a linked descendant pointing back via
  ``replaces_request_id`` (unique structurally — a replaced row cannot be
  superseded again) — Replace creates a new row, not a mutation;
* ``active_download_state`` exists only on ``downloading`` rows or frozen
  ``replaced`` rows that preserve their in-flight historical snapshot;
* the guarded transitions really guard: a download claim on a non-wanted
  row and a downloading→wanted requeue on a non-downloading row are
  no-ops that leave the row untouched.

Alongside scenario-shaped rules, ``attempt_any_transition`` deliberately drives
every target from every current status (including ``replaced``), with both
current and stale explicit source snapshots. The production transition DAG and
SQL compare-and-set boundary must reject every invalid/stale world without
caller-side eligibility filtering.

Profiles, promotion policy, fault-injection qualification:
docs/generated-testing.md.
"""

import copy
import os
import sys
import unittest
from collections.abc import Mapping
from itertools import product

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import example, given
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    invariant,
    precondition,
    rule,
)

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.config import CratediggerConfig
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    automation_import_dedupe_key,
    automation_import_payload,
)
from lib.pipeline_db import (
    ConsumedAttemptInput,
    NonConsumingAttemptInput,
    ReplacedRequestMutationError,
    SearchPlanItemInput,
)
from lib.pipeline_db._shared import REQUEST_METADATA_RESERVED_FIELDS
from lib.search_plan_service import (
    RESULT_REQUEST_REPLACED,
    SearchPlanService,
)
from lib.transitions import (
    VALID_TRANSITIONS,
    RequestTransition,
    TransitionApplied,
    TransitionConflict,
    TransitionResult,
    finalize_request,
)
from lib.world_invariants import assert_replaced_row_frozen
from tests.fakes import FakePipelineDB
from tests.helpers import make_active_download_state_json

LEGAL_STATUSES = frozenset({
    "initializing",
    "wanted",
    "downloading",
    "processing",
    "imported",
    "unsearchable",
    "replaced",
})

_IDENTITY_FIELDS = ("mb_release_id", "source", "created_at")

_RETRY_COUNTERS = ("search_attempts", "download_attempts", "validation_attempts")


def attach_fake_processing_owner(
    db: FakePipelineDB,
    request_id: int,
) -> int:
    """Create the production-representable owner before entering processing."""
    job = db.enqueue_import_job(
        IMPORT_JOB_AUTOMATION,
        request_id=request_id,
        dedupe_key=automation_import_dedupe_key(request_id),
        payload=automation_import_payload(),
    )
    row = db.request(request_id)
    row["status"] = "processing"
    row["active_automation_import_job_id"] = job.id
    return job.id


# ===========================================================================
# Invariant checkers — module functions so the known-bad self-tests below
# can prove each one trips (harness RED/GREEN).
# ===========================================================================

def assert_statuses_legal(rows: list[dict]) -> None:
    for row in rows:
        if row["status"] not in LEGAL_STATUSES:
            raise AssertionError(
                f"request {row['id']} has illegal status {row['status']!r}")


def assert_replaced_tracks_frozen(
    request_id: int,
    snapshot: list[dict],
    tracks: list[dict],
) -> None:
    """A replaced request's child track rows are historical audit data too."""
    if tracks != snapshot:
        raise AssertionError(
            f"replaced request {request_id} tracks mutated after supersede: "
            f"{snapshot!r} -> {tracks!r}"
        )


def assert_identity_immutable(identity: tuple, row: dict) -> None:
    current = tuple(row[field] for field in _IDENTITY_FIELDS)
    if current != identity:
        raise AssertionError(
            f"request {row['id']} identity drifted: {identity} -> {current}")


def assert_download_state_coherent(row: dict) -> None:
    if (
        row["status"] not in {"downloading", "processing", "replaced"}
        and row.get("active_download_state")
    ):
        raise AssertionError(
            f"request {row['id']} carries active_download_state while "
            f"{row['status']!r}")


def assert_processing_owner_equivalent(row: Mapping[str, object]) -> None:
    """``processing`` and its exact automation-owner pointer are inseparable."""
    is_processing = row.get("status") == "processing"
    has_owner = row.get("active_automation_import_job_id") is not None
    if is_processing != has_owner:
        raise AssertionError(
            f"request {row.get('id')} has status={row.get('status')!r} "
            f"with owner={row.get('active_automation_import_job_id')!r}"
        )


def assert_processing_owner_unchanged(
    before: Mapping[str, object],
    after: Mapping[str, object] | None,
) -> None:
    """An ordinary request writer cannot mutate or delete an owned row."""
    if after != before:
        raise AssertionError(f"processing owner mutated: {before} -> {after}")


def assert_replacement_linked(
    replaced_id: int, descendant: Mapping[str, object] | None,
) -> None:
    if descendant is None:
        raise AssertionError(
            f"replaced request {replaced_id} has no linked descendant row")
    if descendant.get("replaces_request_id") != replaced_id:
        raise AssertionError(
            f"descendant of {replaced_id} does not point back "
            f"(replaces_request_id={descendant.get('replaces_request_id')!r})")


def assert_transition_result_matches(
    before: dict,
    after: dict,
    target_status: str,
    result: TransitionResult,
) -> None:
    """A conflict is a byte-identical no-op; applied means target landed."""
    if isinstance(result, TransitionApplied):
        if after["status"] != target_status:
            raise AssertionError(
                f"applied transition reported {target_status!r} but row is "
                f"{after['status']!r}")
        return
    if after != before:
        raise AssertionError(
            f"conflicted transition mutated request {before['id']}: "
            f"{before} -> {after}")


def assert_read_only_cas_result(
    *,
    applied: bool,
    expected_applied: bool,
    before: Mapping[str, object] | None,
    after: Mapping[str, object] | None,
) -> None:
    """An empty metadata CAS tells the truth and never mutates the row."""
    if applied is not expected_applied:
        raise AssertionError(
            f"read-only CAS reported applied={applied}, "
            f"expected={expected_applied}"
        )
    if after != before:
        raise AssertionError(f"read-only CAS mutated row: {before} -> {after}")


def assert_wanted_quality_fields(
    row: dict,
    *,
    min_bitrate: int | None,
    prev_min_bitrate: int | None,
) -> None:
    """A typed wanted transition preserves both explicit quality fields."""
    if row["status"] != "wanted":
        raise AssertionError(f"wanted transition landed as {row['status']!r}")
    if row.get("min_bitrate") != min_bitrate:
        raise AssertionError(
            f"min_bitrate drifted: {row.get('min_bitrate')!r} != "
            f"{min_bitrate!r}"
        )
    if row.get("prev_min_bitrate") != prev_min_bitrate:
        raise AssertionError(
            f"prev_min_bitrate drifted: {row.get('prev_min_bitrate')!r} != "
            f"{prev_min_bitrate!r}"
        )


class TestReadOnlyMetadataCasGenerated(unittest.TestCase):
    def test_empty_and_control_only_updates_match_truth_table(self) -> None:
        statuses = sorted(LEGAL_STATUSES)
        cases = product((False, True), statuses, (False, True), statuses)
        for exists, status, include_expected_status, expected_status in cases:
            with self.subTest(
                exists=exists,
                status=status,
                include_expected_status=include_expected_status,
                expected_status=expected_status,
            ):
                self._assert_empty_and_control_only_update(
                    exists=exists,
                    status=status,
                    include_expected_status=include_expected_status,
                    expected_status=expected_status,
                )

    def _assert_empty_and_control_only_update(
        self,
        *,
        exists: bool,
        status: str,
        include_expected_status: bool,
        expected_status: str,
    ) -> None:
        db = FakePipelineDB()
        request_id = 1
        if exists:
            db.add_request(
                "Artist",
                "Album",
                "request",
                mb_release_id="read-only-cas",
                status=("wanted" if status == "processing" else status),
            )
            if status == "processing":
                attach_fake_processing_owner(db, request_id)
        before = copy.deepcopy(db.get_request(request_id))
        kwargs = (
            {"expected_status": expected_status}
            if include_expected_status else {}
        )

        applied = db.update_request_fields(request_id, **kwargs)
        expected_applied = (
            exists
            and status != "replaced"
            and status != "processing"
            and (
                not include_expected_status
                or status == expected_status
            )
        )
        assert_read_only_cas_result(
            applied=applied,
            expected_applied=expected_applied,
            before=before,
            after=copy.deepcopy(db.get_request(request_id)),
        )

    @given(
        field=st.sampled_from(sorted(REQUEST_METADATA_RESERVED_FIELDS)),
        value=st.one_of(st.none(), st.integers(), st.text(max_size=24)),
    )
    @example(field="status", value="replaced")
    @example(field="status", value="unsearchable")
    @example(field="mb_release_id", value="different-pressing")
    def test_reserved_lifecycle_and_identity_fields_cannot_be_smuggled(
        self,
        *,
        field: str,
        value: object,
    ) -> None:
        db = FakePipelineDB()
        request_id = db.add_request(
            "Artist",
            "Album",
            "request",
            mb_release_id="immutable-release",
        )
        before = copy.deepcopy(db.get_request(request_id))

        with self.assertRaises(ValueError):
            db.update_request_fields(request_id, **{field: value})

        self.assertEqual(db.get_request(request_id), before)


class TestWantedQualityFieldsGenerated(unittest.TestCase):
    @given(
        source_status=st.sampled_from(["downloading", "imported", "unsearchable"]),
        current_min_bitrate=st.one_of(st.none(), st.integers(1, 2000)),
        current_prev_min_bitrate=st.one_of(st.none(), st.integers(1, 2000)),
        next_min_bitrate=st.one_of(st.none(), st.integers(1, 2000)),
        explicit_prev_min_bitrate=st.one_of(st.none(), st.integers(1, 2000)),
    )
    @example(
        source_status="unsearchable",
        current_min_bitrate=320,
        current_prev_min_bitrate=192,
        next_min_bitrate=245,
        explicit_prev_min_bitrate=256,
    )
    @example(
        source_status="downloading",
        current_min_bitrate=245,
        current_prev_min_bitrate=128,
        next_min_bitrate=192,
        explicit_prev_min_bitrate=None,
    )
    def test_explicit_wanted_quality_fields_match_typed_contract(
        self,
        *,
        source_status: str,
        current_min_bitrate: int | None,
        current_prev_min_bitrate: int | None,
        next_min_bitrate: int | None,
        explicit_prev_min_bitrate: int | None,
    ) -> None:
        db = FakePipelineDB()
        request_id = db.add_request(
            "Artist",
            "Album",
            "request",
            mb_release_id="wanted-quality-contract",
            status=source_status,
        )
        row = db.request(request_id)
        row["min_bitrate"] = current_min_bitrate
        row["prev_min_bitrate"] = current_prev_min_bitrate

        result = finalize_request(
            db,
            request_id,
            RequestTransition.to_wanted(
                from_status=source_status,
                min_bitrate=next_min_bitrate,
                prev_min_bitrate=explicit_prev_min_bitrate,
            ),
        )

        self.assertIsInstance(result, TransitionApplied)
        assert_wanted_quality_fields(
            db.request(request_id),
            min_bitrate=next_min_bitrate,
            prev_min_bitrate=explicit_prev_min_bitrate,
        )


class TestProcessingOwnerGuardsGenerated(unittest.TestCase):
    """Patrol the private processing edge through ordinary production writers."""

    @staticmethod
    def _owned_request() -> tuple[FakePipelineDB, int]:
        db = FakePipelineDB()
        request_id = db.add_request(
            "Artist",
            "Album",
            "request",
            mb_release_id="processing-owner-guard",
        )
        attach_fake_processing_owner(db, request_id)
        row = db.request(request_id)
        row["active_download_state"] = {"current_path": "/processing/album"}
        return db, request_id

    def test_owned_processing_row_rejects_every_generic_writer(self) -> None:
        db, request_id = self._owned_request()
        before = copy.deepcopy(db.request(request_id))

        self.assertFalse(db.update_request_fields(request_id, reasoning="late"))
        self.assertFalse(
            db.update_status(
                request_id,
                "wanted",
                expected_status="processing",
            )
        )
        self.assertFalse(
            db.mark_imported_with_rescue(
                request_id,
                expected_status="processing",
                beets_distance=0.05,
            )
        )
        self.assertFalse(
            db.reset_to_wanted(
                request_id,
                expected_status="processing",
                min_bitrate=320,
            )
        )
        db.delete_request(request_id)
        result = finalize_request(
            db,
            request_id,
            RequestTransition.to_wanted(from_status="processing"),
        )

        self.assertIsInstance(result, TransitionConflict)
        assert before is not None
        assert_processing_owner_unchanged(
            before,
            copy.deepcopy(db.get_request(request_id)),
        )

    @given(
        writer=st.sampled_from(("rescue", "reset")),
        expected_is_current=st.booleans(),
        value=st.integers(min_value=1, max_value=2000),
    )
    @example(writer="rescue", expected_is_current=True, value=5)
    @example(writer="reset", expected_is_current=True, value=320)
    def test_owned_processing_row_rejects_specialized_status_writers(
        self,
        *,
        writer: str,
        expected_is_current: bool,
        value: int,
    ) -> None:
        db, request_id = self._owned_request()
        before = copy.deepcopy(db.request(request_id))
        expected_status = "processing" if expected_is_current else "wanted"

        if writer == "rescue":
            applied = db.mark_imported_with_rescue(
                request_id,
                expected_status=expected_status,
                beets_distance=value / 1000,
            )
        else:
            applied = db.reset_to_wanted(
                request_id,
                expected_status=expected_status,
                min_bitrate=value,
            )

        self.assertFalse(applied)
        assert_processing_owner_unchanged(
            before,
            copy.deepcopy(db.get_request(request_id)),
        )

    @given(
        target=st.sampled_from(
            ("wanted", "downloading", "imported", "unsearchable")
        ),
        expected_is_current=st.booleans(),
    )
    @example(target="wanted", expected_is_current=True)
    def test_ordinary_transition_never_leaves_processing(
        self,
        *,
        target: str,
        expected_is_current: bool,
    ) -> None:
        db, request_id = self._owned_request()
        before = copy.deepcopy(db.request(request_id))
        source = "processing" if expected_is_current else "wanted"
        if target == "wanted":
            command = RequestTransition.to_wanted(from_status=source)
        elif target == "downloading":
            command = RequestTransition.to_downloading(
                from_status=source,
                state_json=make_active_download_state_json([]),
            )
        elif target == "imported":
            command = RequestTransition.to_imported(from_status=source)
        else:
            command = RequestTransition.to_unsearchable(from_status=source)

        result = finalize_request(db, request_id, command)

        self.assertIsInstance(result, TransitionConflict)
        assert before is not None
        assert_processing_owner_unchanged(
            before,
            copy.deepcopy(db.get_request(request_id)),
        )

    @given(
        status=st.sampled_from(sorted(LEGAL_STATUSES)),
        owner=st.one_of(st.none(), st.integers(min_value=1, max_value=1000)),
    )
    @example(status="processing", owner=None)
    @example(status="wanted", owner=1)
    def test_processing_owner_equivalence_checker(
        self,
        *,
        status: str,
        owner: int | None,
    ) -> None:
        row = {
            "id": 1,
            "status": status,
            "active_automation_import_job_id": owner,
        }
        if (status == "processing") == (owner is not None):
            assert_processing_owner_equivalent(row)
        else:
            with self.assertRaises(AssertionError):
                assert_processing_owner_equivalent(row)


class TestResolverSourceStatusGenerated(unittest.TestCase):
    def test_stale_source_cannot_mutate_ancestor_or_children(self) -> None:
        statuses = ("wanted", "downloading", "imported", "unsearchable")
        for exists, status, expected_status in product(
            (False, True), statuses, statuses,
        ):
            with self.subTest(
                exists=exists,
                status=status,
                expected_status=expected_status,
            ):
                self._assert_resolver_source_status(
                    exists=exists,
                    status=status,
                    expected_status=expected_status,
                )

    def _assert_resolver_source_status(
        self,
        *,
        exists: bool,
        status: str,
        expected_status: str,
    ) -> None:
        from lib.field_resolver_service import (
            ResolveAllResult,
            apply_resolve_all_result,
        )

        db = FakePipelineDB()
        request_id = 1
        if exists:
            db.add_request(
                "Artist",
                "Album",
                "request",
                mb_release_id="resolver-source-cas",
                status=status,
            )
            db.set_tracks(request_id, [{
                "disc_number": 1,
                "track_number": 1,
                "title": "Track",
                "track_artist": None,
            }])
        before_row = copy.deepcopy(db.get_request(request_id))
        before_tracks = db.get_tracks(request_id)

        applied = apply_resolve_all_result(
            db,
            request_id,
            ResolveAllResult(
                release_group_year=1999,
                is_va_compilation=True,
                track_artists=["Late Artist"],
            ),
            expected_status=expected_status,
        )
        should_apply = exists and status == expected_status
        if applied is not should_apply:
            raise AssertionError(
                f"resolver apply reported {applied}, expected {should_apply}"
            )
        after_row = db.get_request(request_id)
        after_tracks = db.get_tracks(request_id)
        if should_apply:
            assert after_row is not None
            if after_row["release_group_year"] != 1999:
                raise AssertionError("matching resolver CAS lost parent metadata")
            if after_tracks[0]["track_artist"] != "Late Artist":
                raise AssertionError("matching resolver CAS lost child metadata")
        elif after_row != before_row or after_tracks != before_tracks:
            raise AssertionError(
                "stale resolver CAS mutated ancestor or child rows"
            )


class RequestLifecycleMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.db = FakePipelineDB()
        self.ids: list[int] = []
        self.identity: dict[int, tuple] = {}
        self.frozen: dict[int, dict] = {}
        self.frozen_tracks: dict[int, list[dict]] = {}
        self._mbid_counter = 0

    # -- helpers ------------------------------------------------------

    def _unique_mbid(self) -> str:
        self._mbid_counter += 1
        return f"lifecycle-mbid-{self._mbid_counter:04d}"

    def _row(self, request_id: int) -> dict:
        row = self.db.request(request_id)
        assert row is not None
        return row

    def _ids_with_status(self, *statuses: str) -> list[int]:
        return [
            rid for rid in self.ids
            if self._row(rid)["status"] in statuses
        ]

    def _track(self, request_id: int) -> None:
        self.ids.append(request_id)
        row = self._row(request_id)
        self.identity[request_id] = tuple(
            row[field] for field in _IDENTITY_FIELDS)

    # -- rules (production entry points only) --------------------------

    @rule()
    def create_request(self) -> None:
        rid = self.db.add_request(
            "Lifecycle Artist", f"Album {self._mbid_counter}", "request",
            mb_release_id=self._unique_mbid())
        self._track(rid)

    @precondition(lambda self: self.ids)
    @rule(data=st.data())
    def claim_download(self, data) -> None:
        """DB-guarded: succeeds only from 'wanted'; targets ANY row."""
        rid = data.draw(st.sampled_from(self.ids), label="claim target")
        before = copy.deepcopy(self._row(rid))
        ok = finalize_request(self.db, rid, RequestTransition.to_downloading(
            state_json=make_active_download_state_json([])))
        row = self._row(rid)
        if before["status"] == "wanted":
            if not isinstance(ok, TransitionApplied) or row["status"] != "downloading":
                raise AssertionError(
                    f"wanted->downloading claim failed: ok={ok}, "
                    f"status={row['status']!r}")
        else:
            if not isinstance(ok, TransitionConflict) or row != before:
                raise AssertionError(
                    f"claim on {before['status']!r} row must be a no-op "
                    f"(ok={ok})")

    @precondition(lambda self: self._ids_with_status("downloading"))
    @rule(data=st.data())
    def import_success(self, data) -> None:
        rid = data.draw(
            st.sampled_from(self._ids_with_status("downloading")),
            label="import target")
        ok = finalize_request(self.db, rid, RequestTransition.to_imported(
            from_status="downloading",
            min_bitrate=245,
        ))
        row = self._row(rid)
        if not isinstance(ok, TransitionApplied) or row["status"] != "imported":
            raise AssertionError(
                f"downloading->imported failed: ok={ok}, "
                f"status={row['status']!r}")

    @precondition(lambda self: self.ids)
    @rule(data=st.data())
    def download_fail_requeue(self, data) -> None:
        """DB-guarded: succeeds only from 'downloading'; targets ANY row.
        Preserves retry counters and records the failed attempt."""
        rid = data.draw(st.sampled_from(self.ids), label="requeue target")
        before = copy.deepcopy(self._row(rid))
        ok = finalize_request(self.db, rid, RequestTransition.to_wanted(
            from_status="downloading", attempt_type="download"))
        row = self._row(rid)
        if before["status"] == "downloading":
            if not isinstance(ok, TransitionApplied) or row["status"] != "wanted":
                raise AssertionError(
                    f"downloading->wanted requeue failed: ok={ok}, "
                    f"status={row['status']!r}")
            if row["download_attempts"] != (before["download_attempts"] or 0) + 1:
                raise AssertionError(
                    "failed download attempt was not recorded "
                    f"({before['download_attempts']} -> "
                    f"{row['download_attempts']})")
        else:
            if not isinstance(ok, TransitionConflict) or row != before:
                raise AssertionError(
                    f"downloading->wanted on {before['status']!r} row must "
                    f"be a no-op (ok={ok})")

    @precondition(lambda self: self._ids_with_status("wanted", "downloading"))
    @rule(data=st.data())
    def apply_operator_search_stop(self, data) -> None:
        rid = data.draw(
            st.sampled_from(self._ids_with_status("wanted", "downloading")),
            label="search-stop target")
        before = copy.deepcopy(self._row(rid))
        from_status = self._row(rid)["status"]
        ok = finalize_request(
            self.db, rid, RequestTransition.to_unsearchable(from_status=from_status))
        row = self._row(rid)
        if from_status == "wanted":
            if (
                not isinstance(ok, TransitionApplied)
                or row["status"] != "unsearchable"
            ):
                raise AssertionError(
                    f"wanted->unsearchable failed: ok={ok}, "
                    f"status={row['status']!r}")
        elif not isinstance(ok, TransitionConflict) or row != before:
            raise AssertionError(
                f"downloading->unsearchable must be a no-op conflict: {ok}")

    @precondition(lambda self: self._ids_with_status("imported", "unsearchable"))
    @rule(data=st.data())
    def requeue_from_terminal(self, data) -> None:
        """Operator requeue (upgrade / resume search): clears counters."""
        rid = data.draw(
            st.sampled_from(self._ids_with_status("imported", "unsearchable")),
            label="requeue-from-terminal target")
        from_status = self._row(rid)["status"]
        ok = finalize_request(
            self.db, rid, RequestTransition.to_wanted(from_status=from_status))
        row = self._row(rid)
        if not isinstance(ok, TransitionApplied) or row["status"] != "wanted":
            raise AssertionError(
                f"{from_status}->wanted requeue failed: ok={ok}, "
                f"status={row['status']!r}")
        for counter in _RETRY_COUNTERS:
            if row[counter] != 0:
                raise AssertionError(
                    f"requeue did not clear {counter} (={row[counter]})")

    @precondition(lambda self: self.ids)
    @rule(
        data=st.data(),
        target_status=st.sampled_from(
            ("wanted", "downloading", "imported", "unsearchable")),
        explicit_source=st.booleans(),
    )
    def attempt_any_transition(
        self,
        data,
        target_status: str,
        explicit_source: bool,
    ) -> None:
        """Drive every target from every current status, including replaced.

        This is deliberately NOT eligibility-filtered: invalid edges and stale
        explicit snapshots are production inputs whose no-op behavior is part
        of the lifecycle contract.
        """
        rid = data.draw(st.sampled_from(self.ids), label="any transition target")
        before = copy.deepcopy(self._row(rid))
        actual = str(before["status"])
        claimed_source = actual if explicit_source else None
        if explicit_source and data.draw(st.booleans(), label="stale snapshot"):
            claimed_source = "wanted" if actual != "wanted" else "unsearchable"

        if target_status == "wanted":
            command = RequestTransition.to_wanted(from_status=claimed_source)
        elif target_status == "downloading":
            command = RequestTransition.to_downloading(
                from_status=claimed_source,
                state_json=make_active_download_state_json([]),
            )
        elif target_status == "imported":
            command = RequestTransition.to_imported(from_status=claimed_source)
        else:
            command = RequestTransition.to_unsearchable(from_status=claimed_source)

        result = finalize_request(self.db, rid, command)
        after = self._row(rid)
        should_apply = (
            (claimed_source is None or claimed_source == actual)
            and (actual, target_status) in VALID_TRANSITIONS
        )
        if should_apply and not isinstance(result, TransitionApplied):
            raise AssertionError(
                f"valid {actual!r}->{target_status!r} conflicted: {result}")
        if not should_apply and not isinstance(result, TransitionConflict):
            raise AssertionError(
                f"invalid/stale {actual!r}->{target_status!r} applied: {result}")
        assert_transition_result_matches(before, after, target_status, result)

    @precondition(lambda self: self._ids_with_status(
        "wanted", "downloading", "imported", "unsearchable"))
    @rule(data=st.data())
    def replace(self, data) -> None:
        """Replace (issue-#282 shape): the old row flips to 'replaced' and
        freezes; a NEW linked row is created. Replace deliberately accepts
        downloading rows, so in-flight writers must lose their later CAS."""
        rid = data.draw(
            st.sampled_from(
                self._ids_with_status(
                    "wanted", "downloading", "imported", "unsearchable")),
            label="replace target")
        if self._row(rid).get("active_plan_id") is None:
            self.db.create_successful_search_plan(
                request_id=rid,
                generator_id="lifecycle-generator",
                items=[
                    SearchPlanItemInput(
                        ordinal=0,
                        strategy="album",
                        query="Lifecycle Artist Album",
                        canonical_query_key="lifecycle artist album",
                        repeat_group=None,
                    ),
                    SearchPlanItemInput(
                        ordinal=1,
                        strategy="track",
                        query="Lifecycle Artist Track",
                        canonical_query_key="lifecycle artist track",
                        repeat_group=None,
                    ),
                ],
            )
        new_id = self.db.supersede_request_mbid(
            rid,
            new_mb_release_id=self._unique_mbid(),
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="Lifecycle Artist",
            new_album_title=f"Album {rid} (correct pressing)",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )
        self._track(new_id)
        old_row = self._row(rid)
        if old_row["status"] != "replaced":
            raise AssertionError(
                f"supersede left old row status={old_row['status']!r}")
        # The frozen audit snapshot: byte-identical from here on.
        self.frozen[rid] = copy.deepcopy(old_row)
        self.frozen_tracks[rid] = self.db.get_tracks(rid)
        assert_replacement_linked(
            rid, self.db.get_request_by_replaces_request_id(rid))

    @precondition(lambda self: bool(self.frozen))
    @rule(data=st.data())
    def late_writers_cannot_mutate_replaced_ancestor(self, data) -> None:
        """Exercise real metadata/search-plan writers after supersede.

        ``set_tracks`` models a resolver result arriving after Replace won.
        """
        rid = data.draw(st.sampled_from(sorted(self.frozen)), label="frozen row")
        snapshot = copy.deepcopy(self._row(rid))

        if self.db.update_download_state(
            rid,
            make_active_download_state_json([]),
            expected_status="downloading",
        ):
            raise AssertionError("late download-state write thawed replaced row")
        if self.db.update_download_state_current_path(rid, "/late/path"):
            raise AssertionError("late path write thawed replaced row")
        if self.db.mark_import_subprocess_started(rid, "late"):
            raise AssertionError("late import stamp thawed replaced row")
        if self.db.set_request_current_evidence(
            rid,
            999,
            expected_status="downloading",
        ):
            raise AssertionError("late evidence link thawed replaced row")
        if self.db.record_attempt(
            rid,
            "download",
            expected_status="wanted",
        ):
            raise AssertionError("late retry write thawed replaced row")

        try:
            self.db.set_tracks(rid, [{
                "disc_number": 1,
                "track_number": 1,
                "title": "Late resolver result",
                "length_seconds": 180,
            }])
        except ReplacedRequestMutationError:
            pass
        else:
            raise AssertionError("late resolver result rewrote replaced tracks")
        assert_replaced_tracks_frozen(
            rid,
            self.frozen_tracks[rid],
            self.db.get_tracks(rid),
        )
        if self.db.update_track_artists(rid, ["Late Artist"]):
            raise AssertionError(
                "late track-artist resolver thawed replaced tracks"
            )
        assert_replaced_tracks_frozen(
            rid,
            self.frozen_tracks[rid],
            self.db.get_tracks(rid),
        )
        prior_resolution = self.db.get_field_resolution(rid, "track_artist")
        if self.db.record_field_resolution(
            rid, "track_artist", "resolved", None,
        ):
            raise AssertionError(
                "late field-resolution audit thawed replaced child state"
            )
        if self.db.get_field_resolution(rid, "track_artist") != prior_resolution:
            raise AssertionError(
                "late field-resolution audit mutated replaced child state"
            )

        try:
            self.db.create_failed_search_plan(
                request_id=rid,
                generator_id="late-generator",
                failure_class="dependency_failure",
                transient=True,
            )
        except ReplacedRequestMutationError:
            pass
        else:
            raise AssertionError("late plan generation accepted replaced row")

        active = self.db.get_active_search_plan(rid)
        assert active is not None
        first = active.items[0]
        consumed = self.db.record_consumed_search_attempt(
            ConsumedAttemptInput(
                request_id=rid,
                plan_id=active.plan.id,
                plan_item_id=first.id,
                plan_ordinal=first.ordinal,
                plan_strategy=first.strategy,
                plan_canonical_query_key=first.canonical_query_key,
                plan_repeat_group=first.repeat_group,
                plan_generator_id=active.plan.generator_id,
                query=first.query,
                outcome="no_results",
                plan_item_count=len(active.items),
                cycle_count_snapshot=active.cycle_count,
                apply_scheduler_attempt=True,
            )
        )
        if not consumed.is_stale:
            raise AssertionError("replaced-row consumed attempt was accepted")
        self.db.record_non_consuming_search_attempt(
            NonConsumingAttemptInput(
                request_id=rid,
                outcome="error",
                apply_scheduler_attempt=True,
            )
        )

        result = SearchPlanService(
            self.db,
            CratediggerConfig(),
        ).generate_for_request(rid, regenerate=True)
        if result.outcome != RESULT_REQUEST_REPLACED:
            raise AssertionError(
                f"service generated plan for replaced row: {result.outcome}"
            )
        assert_replaced_row_frozen(snapshot, self._row(rid))

    # -- invariants (checked after every rule) --------------------------

    @invariant()
    def statuses_stay_legal(self) -> None:
        assert_statuses_legal([self._row(rid) for rid in self.ids])

    @invariant()
    def replaced_rows_stay_frozen(self) -> None:
        for rid, snapshot in self.frozen.items():
            assert_replaced_row_frozen(snapshot, self._row(rid))

    @invariant()
    def replaced_tracks_stay_frozen(self) -> None:
        for rid, snapshot in self.frozen_tracks.items():
            assert_replaced_tracks_frozen(rid, snapshot, self.db.get_tracks(rid))

    @invariant()
    def identity_never_drifts(self) -> None:
        for rid, identity in self.identity.items():
            assert_identity_immutable(identity, self._row(rid))

    @invariant()
    def download_state_only_while_downloading(self) -> None:
        for rid in self.ids:
            assert_download_state_coherent(self._row(rid))

    @invariant()
    def processing_status_matches_exact_owner_pointer(self) -> None:
        for rid in self.ids:
            assert_processing_owner_equivalent(self._row(rid))

    @invariant()
    def every_replaced_row_has_a_descendant(self) -> None:
        for rid in self.frozen:
            assert_replacement_linked(
                rid, self.db.get_request_by_replaces_request_id(rid))


TestRequestLifecycleMachine = RequestLifecycleMachine.TestCase


class TestLifecycleCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests for every lifecycle invariant checker."""

    def test_trips_on_illegal_status(self):
        with self.assertRaises(AssertionError):
            assert_statuses_legal([{"id": 1, "status": "zombie"}])

    def test_trips_on_thawed_replaced_row(self):
        snapshot = {"id": 1, "status": "replaced", "min_bitrate": 245}
        thawed = {"id": 1, "status": "replaced", "min_bitrate": 320}
        with self.assertRaises(AssertionError):
            assert_replaced_row_frozen(snapshot, thawed)

    def test_trips_on_rewritten_replaced_tracks(self):
        with self.assertRaises(AssertionError):
            assert_replaced_tracks_frozen(
                1,
                [{"track_number": 1, "title": "Original"}],
                [{"track_number": 1, "title": "Late resolver result"}],
            )

    def test_trips_on_resurrected_replaced_row(self):
        snapshot = {"id": 1, "status": "replaced"}
        resurrected = {"id": 1, "status": "wanted"}
        with self.assertRaises(AssertionError):
            assert_replaced_row_frozen(snapshot, resurrected)

    def test_trips_on_identity_drift(self):
        row = {"id": 1, "mb_release_id": "mbid-B", "source": "request",
               "created_at": "t0"}
        with self.assertRaises(AssertionError):
            assert_identity_immutable(("mbid-A", "request", "t0"), row)

    def test_trips_on_stray_download_state(self):
        with self.assertRaises(AssertionError):
            assert_download_state_coherent(
                {"id": 1, "status": "imported",
                 "active_download_state": "{}"})

    def test_trips_on_processing_without_owner_and_owner_without_processing(self):
        with self.assertRaises(AssertionError):
            assert_processing_owner_equivalent({
                "id": 1,
                "status": "processing",
                "active_automation_import_job_id": None,
            })
        with self.assertRaises(AssertionError):
            assert_processing_owner_equivalent({
                "id": 1,
                "status": "wanted",
                "active_automation_import_job_id": 9,
            })

    def test_processing_owner_guard_checker_trips_on_mutation_and_delete(self):
        before = {
            "id": 1,
            "status": "processing",
            "active_automation_import_job_id": 9,
        }
        with self.assertRaises(AssertionError):
            assert_processing_owner_unchanged(
                before,
                {**before, "status": "wanted"},
            )
        with self.assertRaises(AssertionError):
            assert_processing_owner_unchanged(before, None)

    def test_trips_on_dropped_explicit_previous_bitrate(self):
        with self.assertRaises(AssertionError):
            assert_wanted_quality_fields(
                {
                    "status": "wanted",
                    "min_bitrate": 245,
                    "prev_min_bitrate": 320,
                },
                min_bitrate=245,
                prev_min_bitrate=256,
            )

    def test_trips_on_missing_descendant(self):
        with self.assertRaises(AssertionError):
            assert_replacement_linked(1, None)
        with self.assertRaises(AssertionError):
            assert_replacement_linked(1, {"replaces_request_id": 99})

    def test_trips_when_conflict_mutates_row(self):
        from lib.transitions import TransitionConflictKind

        before = {"id": 1, "status": "replaced"}
        after = {"id": 1, "status": "wanted"}
        conflict = TransitionConflict(
            1, "wanted", TransitionConflictKind.invalid_edge,
            "replaced", "replaced",
        )
        with self.assertRaises(AssertionError):
            assert_transition_result_matches(before, after, "wanted", conflict)

    def test_trips_when_applied_target_did_not_land(self):
        row = {"id": 1, "status": "wanted"}
        applied = TransitionApplied(1, "wanted", "unsearchable")
        with self.assertRaises(AssertionError):
            assert_transition_result_matches(row, row, "unsearchable", applied)

    def test_read_only_cas_checker_trips_on_false_success_and_mutation(self):
        with self.assertRaises(AssertionError):
            assert_read_only_cas_result(
                applied=True,
                expected_applied=False,
                before=None,
                after=None,
            )
        with self.assertRaises(AssertionError):
            assert_read_only_cas_result(
                applied=True,
                expected_applied=True,
                before={"id": 1, "updated_at": "before"},
                after={"id": 1, "updated_at": "after"},
            )


if __name__ == "__main__":
    unittest.main()
