"""Pins for the scan/claim shape both import-queue workers share."""

from __future__ import annotations

import signal
import unittest

from lib.import_execution import ExecutionLeaseSnapshot, ProcessIdentity
from lib.import_queue import ImportJob
from lib.import_worker_loop import (
    CandidateScanCursor,
    ClaimState,
    GracefulShutdown,
    capture_worker_execution_lease,
    claim_one_candidate,
    execution_lease_from_job,
)


def _job(job_id: int, **overrides: object) -> ImportJob:
    row: dict[str, object] = {
        "id": job_id,
        "job_type": "youtube_import",
        "status": "queued",
        "request_id": None,
        "dedupe_key": None,
        "payload": {
            "staged_path": "/incoming/album",
            "request_id": 1,
            "browse_id": "MPREb_x",
            "download_log_id": 1,
        },
        "result": None,
        "message": None,
        "error": None,
        "attempts": 0,
        "worker_id": None,
        "created_at": None,
        "updated_at": None,
        "started_at": None,
        "heartbeat_at": None,
        "completed_at": None,
    }
    row.update(overrides)
    return ImportJob.from_row(row)


_FULL_LEASE = {
    "execution_invocation_id": "invocation-1",
    "execution_host_boot_id": "boot-1",
    "execution_systemd_unit": "cratedigger-importer.service",
    "execution_worker_pid": 11,
    "execution_worker_start_ticks": 1100,
}


class TestClaimState(unittest.TestCase):
    def test_a_fresh_state_has_not_claimed(self) -> None:
        self.assertFalse(ClaimState().claimed)

    def test_mark_records_the_claim(self) -> None:
        state = ClaimState()
        state.mark()
        self.assertTrue(state.claimed)


class TestCandidateScanCursor(unittest.TestCase):
    def test_a_fresh_cursor_starts_at_the_head(self) -> None:
        self.assertEqual(CandidateScanCursor().offset, 0)


class TestGracefulShutdown(unittest.TestCase):
    def test_a_fresh_flag_is_unrequested(self) -> None:
        self.assertFalse(GracefulShutdown().requested)

    def test_request_is_a_valid_signal_handler_and_sets_the_flag(self) -> None:
        flag = GracefulShutdown()
        previous = signal.signal(signal.SIGTERM, flag.request)
        try:
            flag.request(signal.SIGTERM, None)
        finally:
            signal.signal(signal.SIGTERM, previous)
        self.assertTrue(flag.requested)


class TestExecutionLeaseFromJob(unittest.TestCase):
    def test_no_job_carries_no_lease(self) -> None:
        self.assertIsNone(execution_lease_from_job(None))

    def test_a_complete_row_rebuilds_the_exact_lease(self) -> None:
        lease = execution_lease_from_job(_job(1, **_FULL_LEASE))
        self.assertEqual(
            lease,
            ExecutionLeaseSnapshot(
                host_boot_id="boot-1",
                invocation_id="invocation-1",
                systemd_unit="cratedigger-importer.service",
                worker=ProcessIdentity(11, 1100),
                beets=None,
            ),
        )

    def test_any_missing_worker_field_yields_no_lease(self) -> None:
        """Incomplete evidence is no evidence, field by field."""
        for field in _FULL_LEASE:
            with self.subTest(missing=field):
                row = dict(_FULL_LEASE)
                row[field] = None
                self.assertIsNone(execution_lease_from_job(_job(1, **row)))

    def test_a_complete_beets_pair_is_carried(self) -> None:
        lease = execution_lease_from_job(_job(
            1,
            **_FULL_LEASE,
            execution_beets_pid=22,
            execution_beets_start_ticks=2200,
        ))
        assert lease is not None
        self.assertEqual(lease.beets, ProcessIdentity(22, 2200))

    def test_a_half_beets_pair_is_no_child(self) -> None:
        for pid, ticks in ((22, None), (None, 2200)):
            with self.subTest(pid=pid, ticks=ticks):
                lease = execution_lease_from_job(_job(
                    1,
                    **_FULL_LEASE,
                    execution_beets_pid=pid,
                    execution_beets_start_ticks=ticks,
                ))
                assert lease is not None
                self.assertIsNone(lease.beets)


class TestCaptureWorkerExecutionLease(unittest.TestCase):
    def test_a_non_systemd_run_captures_no_lease(self) -> None:
        """The ``ValueError`` that keeps automation invisible off systemd."""
        def _refuse(**_kwargs: object) -> ExecutionLeaseSnapshot:
            raise ValueError("not under systemd")

        self.assertIsNone(
            capture_worker_execution_lease(
                systemd_unit="cratedigger-importer.service", factory=_refuse,
            ),
        )

    def test_the_unit_reaches_the_factory_and_its_lease_is_returned(
        self,
    ) -> None:
        seen: list[str] = []
        snapshot = ExecutionLeaseSnapshot(
            host_boot_id="boot-1",
            invocation_id="invocation-1",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(11, 1100),
        )

        def _capture(*, systemd_unit: str) -> ExecutionLeaseSnapshot:
            seen.append(systemd_unit)
            return snapshot

        captured = capture_worker_execution_lease(
            systemd_unit="cratedigger-import-preview-worker.service",
            factory=_capture,
        )
        self.assertIs(captured, snapshot)
        self.assertEqual(seen, ["cratedigger-import-preview-worker.service"])


class TestClaimOneCandidate(unittest.TestCase):
    """The bounded-scan cursor contract both workers' ``run_once`` runs on."""

    def _scan(
        self,
        pages: dict[int, list[ImportJob]],
        *,
        cursor: CandidateScanCursor,
        claims: frozenset[int] = frozenset(),
    ) -> tuple[ImportJob | None, list[int]]:
        peeked: list[int] = []

        def _peek(offset: int) -> list[ImportJob]:
            peeked.append(offset)
            return pages.get(offset, [])

        def _claim(
            candidate: ImportJob,
            claim_state: ClaimState,
        ) -> ImportJob | None:
            if candidate.id not in claims:
                return None
            claim_state.mark()
            return candidate

        return claim_one_candidate(
            scan_cursor=cursor, peek=_peek, claim=_claim,
        ), peeked

    def test_a_claim_returns_the_job_and_rewinds_the_cursor(self) -> None:
        cursor = CandidateScanCursor(offset=7)
        result, peeked = self._scan(
            {7: [_job(1), _job(2)]}, cursor=cursor, claims=frozenset({2}),
        )
        assert result is not None
        self.assertEqual(result.id, 2)
        self.assertEqual(cursor.offset, 0)
        self.assertEqual(peeked, [7])

    def test_an_unclaimable_page_advances_the_cursor_past_it(self) -> None:
        cursor = CandidateScanCursor(offset=4)
        result, peeked = self._scan(
            {4: [_job(1), _job(2), _job(3)]}, cursor=cursor,
        )
        self.assertIsNone(result)
        self.assertEqual(cursor.offset, 7)
        self.assertEqual(peeked, [4])

    def test_an_empty_page_past_the_head_wraps_and_scans_again(self) -> None:
        cursor = CandidateScanCursor(offset=9)
        result, peeked = self._scan(
            {0: [_job(5)]}, cursor=cursor, claims=frozenset({5}),
        )
        assert result is not None
        self.assertEqual(result.id, 5)
        self.assertEqual(peeked, [9, 0])

    def test_an_empty_page_at_the_head_does_not_rescan(self) -> None:
        """Wrapping from offset 0 would peek the same empty page twice."""
        cursor = CandidateScanCursor()
        result, peeked = self._scan({}, cursor=cursor)
        self.assertIsNone(result)
        self.assertEqual(peeked, [0])
        self.assertEqual(cursor.offset, 0)

    def test_a_claimed_candidate_that_produced_nothing_still_rewinds(
        self,
    ) -> None:
        """The claim marker, not the return value, decides the cursor.

        A route can take the row and still return ``None`` — that is a
        successful claim, and the scan must stop and rewind rather than
        walk on to the next candidate.
        """
        cursor = CandidateScanCursor(offset=3)
        visited: list[int] = []

        def _peek(offset: int) -> list[ImportJob]:
            del offset
            return [_job(1), _job(2)]

        def _claim(
            candidate: ImportJob,
            claim_state: ClaimState,
        ) -> ImportJob | None:
            visited.append(candidate.id)
            claim_state.mark()
            return None

        self.assertIsNone(
            claim_one_candidate(
                scan_cursor=cursor, peek=_peek, claim=_claim,
            ),
        )
        self.assertEqual(visited, [1])
        self.assertEqual(cursor.offset, 0)

    def test_each_candidate_gets_its_own_claim_state(self) -> None:
        """One candidate's mark must never carry into the next candidate."""
        cursor = CandidateScanCursor()
        states: list[ClaimState] = []

        def _peek(offset: int) -> list[ImportJob]:
            del offset
            return [_job(1), _job(2), _job(3)]

        def _claim(
            candidate: ImportJob,
            claim_state: ClaimState,
        ) -> ImportJob | None:
            del candidate
            states.append(claim_state)
            return None

        claim_one_candidate(scan_cursor=cursor, peek=_peek, claim=_claim)
        self.assertEqual(len(states), 3)
        self.assertEqual(len({id(state) for state in states}), 3)
        self.assertTrue(all(not state.claimed for state in states))


if __name__ == "__main__":
    unittest.main()
