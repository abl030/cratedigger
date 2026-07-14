"""Generated all-or-none invariant for terminal DB outcome bundles."""

from __future__ import annotations

from dataclasses import dataclass
import unittest

from hypothesis import given, strategies as st

from lib import transitions
from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
from lib.terminal_outcomes import (
    ImportJobTerminal,
    ImportTerminalOutcome,
    TerminalDenylist,
    TerminalDownloadAudit,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


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


class TestTerminalOutcomeGenerated(unittest.TestCase):
    @given(fail_after=st.one_of(st.none(), st.integers(min_value=1, max_value=4)))
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
            IMPORT_JOB_MANUAL,
            request_id=42,
            payload=manual_import_payload(failed_path="/tmp/generated"),
        )
        db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
        claimed = db.claim_next_import_job(worker_id="generated")
        assert claimed is not None

        before = TerminalSnapshot(False, False, False, False, False)
        command = ImportTerminalOutcome(
            request_id=42,
            import_job_id=claimed.id,
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

    def test_known_bad_partial_commit_trips_checker(self):
        before = TerminalSnapshot(False, False, False, False, False)
        known_bad = TerminalSnapshot(True, False, False, False, True)

        with self.assertRaisesRegex(AssertionError, "partial terminal outcome"):
            assert_terminal_snapshot_all_or_none(before, known_bad)


if __name__ == "__main__":
    unittest.main()
