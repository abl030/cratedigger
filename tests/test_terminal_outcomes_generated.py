"""Generated all-or-none invariant for terminal DB outcome bundles."""

from __future__ import annotations

from dataclasses import dataclass
import unittest

from hypothesis import example, given, settings, strategies as st

from lib import transitions
from lib.import_queue import IMPORT_JOB_FORCE
from lib.pipeline_db import PipelineDB
from lib.terminal_outcomes import (
    ImportJobTerminal,
    ImportTerminalOutcome,
    TerminalDenylist,
    TerminalDownloadAudit,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_pipeline_db import TEST_DSN, requires_postgres
from tests.test_terminal_outcomes import (
    FaultInjectingPipelineDB,
    InjectedTerminalWriteFailure,
    _seed_running_import,
)


@dataclass(frozen=True)
class TerminalSnapshot:
    request_terminal: bool
    audit_present: bool
    denylist_present: bool
    attempt_recorded: bool
    job_terminal: bool


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
    db.mark_import_job_failed(
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
        claimed = db.claim_next_import_job(worker_id="generated-stop")
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
        claimed = db.claim_next_import_job(worker_id="generated-stop")
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

    @given(fail_after=st.one_of(st.none(), st.integers(min_value=1, max_value=5)))
    def test_fake_transaction_is_unchanged_or_complete(
        self,
        fail_after: int | None,
    ) -> None:
        class FaultDB(FakePipelineDB):
            def _terminal_outcome_write_boundary(
                self,
                index: int,
                label: str,
            ) -> None:
                del label
                if index == fail_after:
                    raise RuntimeError("generated terminal write failure")

        db = FaultDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": []},
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={"download_log_id": 1, "failed_path": "/tmp/generated"},
        )
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = db.claim_next_import_job(worker_id="generated")
        assert claimed is not None

        before = TerminalSnapshot(False, False, False, False, False)
        command = _terminal_command(42, claimed.id)
        if fail_after is None:
            db.persist_import_terminal_outcome(command)
        else:
            with self.assertRaisesRegex(RuntimeError, "generated terminal"):
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

    @given(
        request_terminal=st.booleans(),
        audit_present=st.booleans(),
        denylist_present=st.booleans(),
        attempt_recorded=st.booleans(),
        job_terminal=st.booleans(),
    )
    def test_generated_checker_rejects_every_partial_world(
        self,
        request_terminal: bool,
        audit_present: bool,
        denylist_present: bool,
        attempt_recorded: bool,
        job_terminal: bool,
    ) -> None:
        before = TerminalSnapshot(False, False, False, False, False)
        after = TerminalSnapshot(
            request_terminal,
            audit_present,
            denylist_present,
            attempt_recorded,
            job_terminal,
        )
        all_false = after == before
        all_true = all((
            request_terminal,
            audit_present,
            denylist_present,
            attempt_recorded,
            job_terminal,
        ))
        if all_false or all_true:
            assert_terminal_snapshot_all_or_none(before, after)
        else:
            with self.assertRaises(AssertionError):
                assert_terminal_snapshot_all_or_none(before, after)



@requires_postgres
class TestProductionTerminalOutcomeGenerated(unittest.TestCase):
    @settings(max_examples=12, deadline=None)
    @example(fail_after=None)
    @example(fail_after=1)
    @example(fail_after=2)
    @example(fail_after=3)
    @example(fail_after=4)
    @example(fail_after=5)
    @given(fail_after=st.one_of(st.none(), st.integers(min_value=1, max_value=5)))
    def test_real_transaction_is_unchanged_or_complete(
        self,
        fail_after: int | None,
    ) -> None:
        assert TEST_DSN is not None
        seed_db, request_id, job_id = _seed_running_import()
        seed_db.close()
        before_observer = PipelineDB(TEST_DSN)
        before = _read_terminal_snapshot(before_observer, request_id, job_id)
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
            after = _read_terminal_snapshot(observer, request_id, job_id)
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
