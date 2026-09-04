"""Generated all-or-none invariant for terminal DB outcome bundles."""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from itertools import product

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers active profile
from lib import transitions
from lib.import_queue import IMPORT_JOB_FORCE
from lib.pipeline_db import (
    BACKOFF_BASE_MINUTES,
    BACKOFF_MAX_MINUTES,
    PipelineDB,
)
from lib.terminal_outcomes import (
    ImportJobTerminal,
    ImportTerminalOutcome,
    TerminalDenylist,
    TerminalDownloadAudit,
)
from tests.dispatch_helpers import claim_next_import_job, fail_import_job_via_sql
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_pipeline_db import TEST_DSN, requires_postgres
from tests.test_terminal_outcomes import (
    SEEDED_BACKOFF_MINUTES,
    FaultInjectingPipelineDB,
    InjectedTerminalWriteFailure,
    _prepare_automation_preview_terminal_command,
    _prepare_automation_terminal_command,
    _seed_running_automation_preview,
    _seed_running_import,
    _seed_terminal_retry_state,
    _snapshot,
    automation_requeue_command,
    read_terminal_retry_state,
    terminal_backoff_minutes,
)


@dataclass(frozen=True)
class TerminalSnapshot:
    request_terminal: bool
    audit_present: bool
    denylist_present: bool
    attempt_recorded: bool
    job_terminal: bool


@dataclass(frozen=True)
class AutomationTerminalSnapshot:
    request_released: bool
    audit_present: bool
    job_terminal: bool
    cleanup_consumed: bool


def assert_automation_terminal_snapshot_all_or_none(
    snapshot: AutomationTerminalSnapshot,
) -> None:
    """Reject every owner-terminal world except unchanged or complete."""
    values = (
        snapshot.request_released,
        snapshot.audit_present,
        snapshot.job_terminal,
        snapshot.cleanup_consumed,
    )
    if any(values) and not all(values):
        raise AssertionError(
            f"partial automation terminal outcome: {snapshot!r}"
        )


def assert_terminal_snapshot_all_or_none(
    before: TerminalSnapshot,
    after: TerminalSnapshot,
) -> None:
    """Accept an unchanged snapshot or one with every terminal fact present."""
    if after == before:
        return
    expected = TerminalSnapshot(
        request_terminal=True,
        audit_present=True,
        denylist_present=True,
        attempt_recorded=True,
        job_terminal=True,
    )
    if after != expected:
        raise AssertionError(f"partial terminal outcome: {after!r}")


@dataclass(frozen=True)
class TerminalRetryState:
    """The exact retry-accounting facts one terminal edge may rewrite."""

    search_attempts: int
    download_attempts: int
    validation_attempts: int
    backoff_minutes: int | None

    def counters(self) -> dict[str, int]:
        return {
            "search_attempts": self.search_attempts,
            "download_attempts": self.download_attempts,
            "validation_attempts": self.validation_attempts,
        }

    @classmethod
    def read(cls, db: PipelineDB, request_id: int) -> TerminalRetryState:
        state = read_terminal_retry_state(db, request_id)
        counters: list[int] = []
        for name in (
            "search_attempts",
            "download_attempts",
            "validation_attempts",
        ):
            value = state[name]
            assert isinstance(value, int)
            counters.append(value)
        return cls(
            *counters,
            backoff_minutes=terminal_backoff_minutes(state),
        )


def assert_terminal_edge_retains_policy_counters(
    *,
    edge: tuple[str, str],
    before: TerminalRetryState,
    after: TerminalRetryState,
    attempt_type: str | None,
) -> None:
    """Only an edge the canonical table clears may zero retry counters.

    ``edge`` is the ordinary ``VALID_TRANSITIONS`` edge that the private
    ``processing`` terminal edge stands in for. When that edge does not
    declare ``clear_retry_counters``, prior automatic-retry history must
    survive the terminal commit, and a recorded attempt's backoff must be the
    one grown from that prior count — never the base interval.
    """
    cleared = transitions.VALID_TRANSITIONS[edge].clear_retry_counters
    prior_counters = before.counters()
    for name, value in after.counters().items():
        recorded = int(
            attempt_type is not None and name == f"{attempt_type}_attempts"
        )
        want = recorded if cleared else prior_counters[name] + recorded
        if value != want:
            raise AssertionError(
                f"terminal edge {edge} left {name}={value}, want {want} "
                f"(prior={prior_counters[name]}, cleared={cleared})"
            )
    if attempt_type is None:
        want_backoff = None if cleared else before.backoff_minutes
    else:
        prior = (
            0 if cleared else prior_counters[f"{attempt_type}_attempts"]
        )
        want_backoff = min(
            BACKOFF_BASE_MINUTES * (2 ** prior),
            BACKOFF_MAX_MINUTES,
        )
    if after.backoff_minutes != want_backoff:
        raise AssertionError(
            f"terminal edge {edge} left backoff={after.backoff_minutes} "
            f"minutes, want {want_backoff} (cleared={cleared})"
        )


def assert_operator_stop_matches_terminal_acceptance(
    status: str,
    *,
    successful_terminal_acceptance: bool,
) -> None:
    """Only explicit successful acceptance may supersede the search stop."""
    expected = "imported" if successful_terminal_acceptance else "unsearchable"
    if status != expected:
        raise AssertionError(
            f"terminal_acceptance={successful_terminal_acceptance!r} left "
            f"operator-stop row {status!r}, want {expected!r}"
        )


def _terminal_command(request_id: int, job_id: int) -> ImportTerminalOutcome:
    return ImportTerminalOutcome(
        request_id=request_id,
        import_job_id=job_id,
        initial_transition=transitions.RequestTransition.to_wanted(
            attempt_type="validation"
        ),
        audit=TerminalDownloadAudit(
            outcome="rejected",
            validation_result='{"scenario":"generated_reject"}',
        ),
        denylists=(TerminalDenylist("generated-peer", "generated"),),
        job=ImportJobTerminal(
            status="failed",
            error="generated_reject",
            result={"success": False},
            message="generated reject",
        ),
    )


def _read_terminal_snapshot(
    db: PipelineDB,
    request_id: int,
    job_id: int,
) -> TerminalSnapshot:
    request = db.get_request(request_id)
    job = db.get_import_job(job_id)
    assert request is not None and job is not None
    return TerminalSnapshot(
        request_terminal=request["status"] == "wanted",
        audit_present=len(db.get_download_history(request_id)) == 1,
        denylist_present=len(db.get_denylisted_users(request_id)) == 1,
        attempt_recorded=request["validation_attempts"] == 1,
        job_terminal=job.status == "failed",
    )


def _persist_known_bad_split_outcome(
    db: PipelineDB,
    request_id: int,
    job_id: int,
    *,
    fail_after_write: int,
) -> None:
    """Model the old separate-autocommit path with a deterministic fault."""
    transitions.require_transition_applied(transitions.finalize_request(
        db,
        request_id,
        transitions.RequestTransition.to_wanted(
            from_status="downloading",
            attempt_type="validation",
        ),
    ))
    if fail_after_write == 1:
        raise InjectedTerminalWriteFailure("known_bad.request")
    db.log_download(
        request_id,
        outcome="rejected",
        validation_result='{"scenario":"known_bad"}',
    )
    if fail_after_write == 2:
        raise InjectedTerminalWriteFailure("known_bad.download_log")
    db.add_denylist(request_id, "generated-peer", "known bad")
    if fail_after_write == 3:
        raise InjectedTerminalWriteFailure("known_bad.denylist")
    # ``PipelineDB.mark_import_job_failed`` had zero production callers and
    # was deleted (issue #1355 item A3); this fault-injection scenario
    # models the OLD separate-autocommit path, so it reproduces the exact
    # SQL that writer used rather than a still-live production writer.
    fail_import_job_via_sql(
        db,
        job_id,
        error="known_bad",
        result={"success": False},
        message="known bad",
    )
    if fail_after_write == 4:
        raise InjectedTerminalWriteFailure("known_bad.import_job")


class TestTerminalOutcomeGenerated(unittest.TestCase):
    @given(
        successful_terminal_acceptance=st.booleans(),
        min_bitrate=st.one_of(
            st.none(),
            st.integers(min_value=0, max_value=1500),
        ),
    )
    def test_only_successful_terminal_acceptance_supersedes_operator_stop(
        self,
        successful_terminal_acceptance: bool,
        min_bitrate: int | None,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="unsearchable",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={"download_log_id": 1, "failed_path": "/tmp/generated"},
        )
        db.mark_import_job_preview_importable(job.id, preview_result={})
        claimed = claim_next_import_job(db, worker_id="generated-stop")
        assert claimed is not None
        db.persist_import_terminal_outcome(ImportTerminalOutcome(
            request_id=42,
            import_job_id=claimed.id,
            initial_transition=transitions.RequestTransition.to_imported(
                min_bitrate=min_bitrate,
            ),
            audit=TerminalDownloadAudit(
                outcome=(
                    "success"
                    if successful_terminal_acceptance
                    else "rejected"
                ),
            ),
            job=ImportJobTerminal(
                status=(
                    "completed"
                    if successful_terminal_acceptance
                    else "failed"
                ),
                error=(
                    None
                    if successful_terminal_acceptance
                    else "rejected"
                ),
                result={"success": successful_terminal_acceptance},
                message="generated terminal outcome",
            ),
            successful_terminal_acceptance=(
                successful_terminal_acceptance
            ),
        ))

        row = db.request(42)
        assert_operator_stop_matches_terminal_acceptance(
            row["status"],
            successful_terminal_acceptance=(
                successful_terminal_acceptance
            ),
        )
        self.assertEqual(row["min_bitrate"], min_bitrate)

    def test_terminal_acceptance_checker_trips_on_wrong_status(self) -> None:
        for successful_terminal_acceptance, status in (
            (False, "imported"),
            (True, "unsearchable"),
        ):
            with self.subTest(
                successful_terminal_acceptance=(
                    successful_terminal_acceptance
                ),
            ), self.assertRaises(AssertionError):
                assert_operator_stop_matches_terminal_acceptance(
                    status,
                    successful_terminal_acceptance=(
                        successful_terminal_acceptance
                    ),
                )

    @example(
        attempt_type="validation",
        existing_min_bitrate=320,
        min_bitrate_present=True,
        min_bitrate=245,
        explicit_previous=False,
        search_override="lossless",
    )
    @example(
        attempt_type=None,
        existing_min_bitrate=0,
        min_bitrate_present=True,
        min_bitrate=245,
        explicit_previous=False,
        search_override=None,
    )
    @example(
        attempt_type=None,
        existing_min_bitrate=None,
        min_bitrate_present=True,
        min_bitrate=None,
        explicit_previous=False,
        search_override=None,
    )
    @given(
        attempt_type=st.one_of(
            st.none(),
            st.sampled_from(("search", "download", "validation")),
        ),
        existing_min_bitrate=st.one_of(
            st.none(),
            st.integers(min_value=0, max_value=1500),
        ),
        min_bitrate_present=st.booleans(),
        min_bitrate=st.one_of(
            st.none(),
            st.integers(min_value=0, max_value=1500),
        ),
        explicit_previous=st.booleans(),
        search_override=st.one_of(
            st.none(),
            st.sampled_from(("lossless", "flac,mp3 v0")),
        ),
    )
    def test_operator_stop_retains_generated_wanted_policy_effects(
        self,
        attempt_type: str | None,
        existing_min_bitrate: int | None,
        min_bitrate_present: bool,
        min_bitrate: int | None,
        explicit_previous: bool,
        search_override: str | None,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="unsearchable",
            min_bitrate=existing_min_bitrate,
            prev_min_bitrate=192,
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={"download_log_id": 1, "failed_path": "/tmp/generated"},
        )
        db.mark_import_job_preview_importable(job.id, preview_result={})
        claimed = claim_next_import_job(db, worker_id="generated-stop")
        assert claimed is not None
        fields: dict[str, object] = {
            "search_filetype_override": search_override,
        }
        if min_bitrate_present:
            fields["min_bitrate"] = min_bitrate
        if explicit_previous:
            fields["prev_min_bitrate"] = 256

        db.persist_import_terminal_outcome(ImportTerminalOutcome(
            request_id=42,
            import_job_id=claimed.id,
            initial_transition=transitions.RequestTransition.to_wanted_fields(
                from_status="unsearchable",
                attempt_type=attempt_type,
                fields=fields,
            ),
            audit=TerminalDownloadAudit(outcome="rejected"),
            job=ImportJobTerminal(
                status="failed",
                error="rejected",
                result={"success": False},
                message="rejected",
            ),
        ))

        row = db.request(42)
        self.assertEqual(row["status"], "unsearchable")
        self.assertEqual(row["search_filetype_override"], search_override)
        self.assertEqual(
            row[f"{attempt_type}_attempts"] if attempt_type else 0,
            1 if attempt_type else 0,
        )
        if min_bitrate_present:
            self.assertEqual(row["min_bitrate"], min_bitrate)
        self.assertEqual(
            row["prev_min_bitrate"],
            256
            if explicit_previous
            else (
                existing_min_bitrate
                if min_bitrate_present and existing_min_bitrate is not None
                else 192
            ),
        )

    def test_fake_transaction_is_unchanged_or_complete(self) -> None:
        for fail_after in (None, 1, 2, 3, 4, 5):
            with self.subTest(fail_after=fail_after):
                class FaultDB(FakePipelineDB):
                    def _terminal_outcome_write_boundary(
                        self,
                        index: int,
                        label: str,
                        *,
                        fail_after: int | None = fail_after,
                    ) -> None:
                        del label
                        if index == fail_after:
                            raise RuntimeError(
                                "generated terminal write failure"
                            )

                db = FaultDB()
                db.seed_request(make_request_row(
                    id=42,
                    status="downloading",
                    active_download_state={"files": []},
                ))
                job = db.enqueue_import_job(
                    IMPORT_JOB_FORCE,
                    request_id=42,
                    payload={
                        "download_log_id": 1,
                        "failed_path": "/tmp/generated",
                    },
                )
                db.mark_import_job_preview_importable(
                    job.id,
                    preview_result={"ready": True},
                )
                claimed = claim_next_import_job(db, worker_id="generated")
                assert claimed is not None

                before = TerminalSnapshot(False, False, False, False, False)
                command = _terminal_command(42, claimed.id)
                if fail_after is None:
                    db.persist_import_terminal_outcome(command)
                else:
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "generated terminal",
                    ):
                        db.persist_import_terminal_outcome(command)

                request = db.request(42)
                persisted_job = db.get_import_job(claimed.id)
                assert persisted_job is not None
                after = TerminalSnapshot(
                    request_terminal=request["status"] == "wanted",
                    audit_present=len(db.download_logs) == 1,
                    denylist_present=len(db.denylist) == 1,
                    attempt_recorded=request.get("validation_attempts") == 1,
                    job_terminal=persisted_job.status == "failed",
                )
                assert_terminal_snapshot_all_or_none(before, after)

    def test_generated_checker_rejects_every_partial_world(self) -> None:
        before = TerminalSnapshot(False, False, False, False, False)
        for values in product((False, True), repeat=5):
            (
                request_terminal,
                audit_present,
                denylist_present,
                attempt_recorded,
                job_terminal,
            ) = values
            with self.subTest(
                request_terminal=request_terminal,
                audit_present=audit_present,
                denylist_present=denylist_present,
                attempt_recorded=attempt_recorded,
                job_terminal=job_terminal,
            ):
                after = TerminalSnapshot(
                    request_terminal,
                    audit_present,
                    denylist_present,
                    attempt_recorded,
                    job_terminal,
                )
                all_false = after == before
                all_true = all(values)
                if all_false or all_true:
                    assert_terminal_snapshot_all_or_none(before, after)
                else:
                    with self.assertRaises(AssertionError):
                        assert_terminal_snapshot_all_or_none(before, after)

    def test_retained_counter_checker_trips_on_planted_violations(
        self,
    ) -> None:
        retained = TerminalRetryState(5, 2, 1, SEEDED_BACKOFF_MINUTES)
        # The compliant world the checker must NOT reject: counters survive
        # the retaining edge and the attempt grows the window past the base.
        assert_terminal_edge_retains_policy_counters(
            edge=("downloading", "wanted"),
            before=retained,
            after=TerminalRetryState(5, 2, 2, BACKOFF_BASE_MINUTES * 2),
            attempt_type="validation",
        )
        cases: tuple[
            tuple[str, tuple[str, str], TerminalRetryState, str | None],
            ...,
        ] = (
            (
                "shipped reset of retained counters",
                ("downloading", "wanted"),
                TerminalRetryState(0, 0, 1, BACKOFF_BASE_MINUTES),
                "validation",
            ),
            (
                "retained counters but ungrown base backoff",
                ("downloading", "wanted"),
                TerminalRetryState(5, 2, 2, BACKOFF_BASE_MINUTES),
                "validation",
            ),
            (
                "clean-slate edge that kept its counters",
                ("imported", "wanted"),
                TerminalRetryState(5, 2, 2, BACKOFF_BASE_MINUTES),
                "validation",
            ),
            (
                "accepted edge that dropped its counters",
                ("downloading", "imported"),
                TerminalRetryState(0, 0, 0, None),
                None,
            ),
        )
        for label, edge, after, attempt_type in cases:
            with self.subTest(label=label), self.assertRaises(AssertionError):
                assert_terminal_edge_retains_policy_counters(
                    edge=edge,
                    before=retained,
                    after=after,
                    attempt_type=attempt_type,
                )

    def test_automation_checker_rejects_known_bad_owner_first_world(
        self,
    ) -> None:
        known_bad = AutomationTerminalSnapshot(
            request_released=True,
            audit_present=False,
            job_terminal=False,
            cleanup_consumed=False,
        )
        with self.assertRaisesRegex(
            AssertionError,
            "partial automation terminal outcome",
        ):
            assert_automation_terminal_snapshot_all_or_none(known_bad)



@requires_postgres
class TestProductionTerminalOutcomeGenerated(unittest.TestCase):
    @given(
        fail_after=st.one_of(
            st.none(),
            st.sampled_from((1, 2, 3, 4, 5, 6)),
        ),
    )
    def test_automation_terminal_transaction_is_unchanged_or_complete(
        self,
        fail_after: int | None,
    ) -> None:
        assert TEST_DSN is not None
        seed, request_id, job_id = _seed_running_import(
            automation_state=True,
            cooldown_username="cooldown-peer",
        )
        command = _prepare_automation_terminal_command(
            seed,
            request_id,
            job_id,
        )
        before = _snapshot(seed, request_id, job_id)
        seed.close()

        writer: PipelineDB
        if fail_after is None:
            writer = PipelineDB(TEST_DSN)
        else:
            writer = FaultInjectingPipelineDB(
                TEST_DSN,
                fail_after_write=fail_after,
            )
        try:
            if fail_after is None:
                writer.persist_import_terminal_outcome(command)
            else:
                with self.assertRaises(InjectedTerminalWriteFailure):
                    writer.persist_import_terminal_outcome(command)
        finally:
            writer.close()

        observer = PipelineDB(TEST_DSN)
        try:
            after = _snapshot(observer, request_id, job_id)
        finally:
            observer.close()
        if fail_after is not None:
            self.assertEqual(after, before)
            return
        request = after["request"]
        job = after["job"]
        counts = after["counts"]
        before_counts = before["counts"]
        assert (
            isinstance(request, dict)
            and isinstance(job, dict)
            and isinstance(counts, dict)
            and isinstance(before_counts, dict)
        )
        snapshot = AutomationTerminalSnapshot(
            request_released=(
                request["status"] == "imported"
                and request["active_automation_import_job_id"] is None
                and request["active_download_state"] is None
            ),
            audit_present=counts["logs"] == before_counts["logs"] + 1,
            job_terminal=job["status"] == "completed",
            cleanup_consumed=counts["cleanup_journals"] == 0,
        )
        assert_automation_terminal_snapshot_all_or_none(snapshot)
        self.assertEqual(
            snapshot,
            AutomationTerminalSnapshot(True, True, True, True),
        )

    @settings(max_examples=24, deadline=None)
    @example(
        search_attempts=5,
        download_attempts=2,
        validation_attempts=1,
        attempt_type="validation",
        requeue=True,
    )
    @example(
        search_attempts=3,
        download_attempts=4,
        validation_attempts=6,
        attempt_type=None,
        requeue=True,
    )
    @example(
        search_attempts=2,
        download_attempts=1,
        validation_attempts=3,
        attempt_type="download",
        requeue=False,
    )
    @given(
        search_attempts=st.integers(min_value=0, max_value=6),
        download_attempts=st.integers(min_value=0, max_value=6),
        validation_attempts=st.integers(min_value=0, max_value=6),
        attempt_type=st.one_of(
            st.none(),
            st.sampled_from(("search", "download", "validation")),
        ),
        requeue=st.booleans(),
    )
    def test_automation_terminal_edge_retains_policy_retry_counters(
        self,
        search_attempts: int,
        download_attempts: int,
        validation_attempts: int,
        attempt_type: str | None,
        requeue: bool,
    ) -> None:
        """The private owner edge never invents its own counter policy."""
        assert TEST_DSN is not None
        # ``RequestTransition.to_imported`` takes no attempt_type: an
        # accepted terminal import cannot record an automatic retry attempt.
        recorded_attempt = attempt_type if requeue else None
        db, request_id, job_id = _seed_running_import(automation_state=True)
        try:
            _seed_terminal_retry_state(
                db,
                request_id,
                search_attempts=search_attempts,
                download_attempts=download_attempts,
                validation_attempts=validation_attempts,
            )
            prepared = _prepare_automation_terminal_command(
                db,
                request_id,
                job_id,
            )
            command = (
                automation_requeue_command(
                    prepared,
                    attempt_type=recorded_attempt,
                )
                if requeue
                else prepared
            )
            before = TerminalRetryState.read(db, request_id)
            db.persist_import_terminal_outcome(command)
            after = TerminalRetryState.read(db, request_id)
            request = db.get_request(request_id)
        finally:
            db.close()

        assert request is not None
        target_status = "wanted" if requeue else "imported"
        self.assertEqual(request["status"], target_status)
        self.assertEqual(before.backoff_minutes, SEEDED_BACKOFF_MINUTES)
        assert_terminal_edge_retains_policy_counters(
            edge=("downloading", target_status),
            before=before,
            after=after,
            attempt_type=recorded_attempt,
        )

    @given(
        fail_after=st.one_of(
            st.none(),
            st.sampled_from((1, 2, 3, 4)),
        ),
    )
    def test_automation_preview_transaction_is_unchanged_or_complete(
        self,
        fail_after: int | None,
    ) -> None:
        assert TEST_DSN is not None
        seed, request_id, job_id = _seed_running_automation_preview()
        command = _prepare_automation_preview_terminal_command(
            seed,
            request_id,
            job_id,
        )
        before = _snapshot(seed, request_id, job_id)
        seed.close()
        writer: PipelineDB
        if fail_after is None:
            writer = PipelineDB(TEST_DSN)
        else:
            writer = FaultInjectingPipelineDB(
                TEST_DSN,
                fail_after_write=fail_after,
            )
        try:
            if fail_after is None:
                writer.persist_preview_terminal_outcome(command)
            else:
                with self.assertRaises(InjectedTerminalWriteFailure):
                    writer.persist_preview_terminal_outcome(command)
        finally:
            writer.close()
        observer = PipelineDB(TEST_DSN)
        try:
            after = _snapshot(observer, request_id, job_id)
        finally:
            observer.close()
        if fail_after is not None:
            self.assertEqual(after, before)
            return
        request = after["request"]
        job = after["job"]
        counts = after["counts"]
        before_counts = before["counts"]
        assert (
            isinstance(request, dict)
            and isinstance(job, dict)
            and isinstance(counts, dict)
            and isinstance(before_counts, dict)
        )
        snapshot = AutomationTerminalSnapshot(
            request_released=(
                request["status"] == "wanted"
                and request["active_automation_import_job_id"] is None
                and request["active_download_state"] is None
            ),
            audit_present=counts["logs"] == before_counts["logs"] + 1,
            job_terminal=job["status"] == "failed",
            cleanup_consumed=counts["cleanup_journals"] == 0,
        )
        assert_automation_terminal_snapshot_all_or_none(snapshot)
        self.assertEqual(
            snapshot,
            AutomationTerminalSnapshot(True, True, True, True),
        )

    def test_real_transaction_is_unchanged_or_complete(self) -> None:
        for fail_after in (None, 1, 2, 3, 4, 5):
            with self.subTest(fail_after=fail_after):
                assert TEST_DSN is not None
                seed_db, request_id, job_id = _seed_running_import()
                seed_db.close()
                before_observer = PipelineDB(TEST_DSN)
                before = _read_terminal_snapshot(
                    before_observer,
                    request_id,
                    job_id,
                )
                before_observer.close()

                writer: PipelineDB
                if fail_after is None:
                    writer = PipelineDB(TEST_DSN)
                else:
                    writer = FaultInjectingPipelineDB(
                        TEST_DSN,
                        fail_after_write=fail_after,
                    )
                try:
                    if fail_after is None:
                        writer.persist_import_terminal_outcome(
                            _terminal_command(request_id, job_id)
                        )
                    else:
                        with self.assertRaises(InjectedTerminalWriteFailure):
                            writer.persist_import_terminal_outcome(
                                _terminal_command(request_id, job_id)
                            )
                finally:
                    writer.close()

                observer = PipelineDB(TEST_DSN)
                try:
                    after = _read_terminal_snapshot(
                        observer,
                        request_id,
                        job_id,
                    )
                finally:
                    observer.close()
                assert_terminal_snapshot_all_or_none(before, after)

    def test_faulted_split_writer_trips_same_oracle(self) -> None:
        assert TEST_DSN is not None
        db, request_id, job_id = _seed_running_import()
        before = _read_terminal_snapshot(db, request_id, job_id)
        with self.assertRaises(InjectedTerminalWriteFailure):
            _persist_known_bad_split_outcome(
                db,
                request_id,
                job_id,
                fail_after_write=1,
            )
        db.close()

        observer = PipelineDB(TEST_DSN)
        try:
            after = _read_terminal_snapshot(observer, request_id, job_id)
        finally:
            observer.close()
        with self.assertRaisesRegex(AssertionError, "partial terminal outcome"):
            assert_terminal_snapshot_all_or_none(before, after)


if __name__ == "__main__":
    unittest.main()
