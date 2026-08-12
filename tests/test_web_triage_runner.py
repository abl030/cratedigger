"""Tests for the background Wrong Matches bulk-triage runner."""

from __future__ import annotations

import threading
import time
import unittest

from lib.import_execution import CancellationToken
from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary
from web.triage_runner import (
    STATE_CANCELLED,
    STATE_COMPLETED,
    STATE_FAILED,
    STATE_IDLE,
    STATE_RUNNING,
    TriageRunner,
)


class _ClosableDB:
    """Minimal sweep-DB stand-in recording close() calls."""

    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


class TriageRunnerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = TriageRunner()
        self.db = _ClosableDB()

    def _factory(self):
        return self.db

    def test_initial_status_is_idle(self) -> None:
        status = self.runner.status()
        self.assertEqual(status["state"], STATE_IDLE)
        self.assertIsNone(status["started_at"])
        self.assertIsNone(status["finished_at"])
        self.assertIsNone(status["summary"])
        self.assertIsNone(status["error"])

    def test_start_runs_cleanup_to_completion(self) -> None:
        seen: dict[str, object] = {}

        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            seen["db"] = db
            seen["confirm"] = confirm_all_wrong_matches
            seen["token"] = cancellation_token
            return WrongMatchCleanupSummary(processed=3, deleted=2,
                                            kept_uncertain=1)

        started = self.runner.start(
            db_factory=self._factory, cleanup_fn=cleanup_fn,
        )
        self.assertTrue(started)
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_COMPLETED)
        self.assertIsNotNone(status["started_at"])
        self.assertIsNotNone(status["finished_at"])
        self.assertIsNone(status["error"])
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 3)
        self.assertEqual(summary["deleted"], 2)
        self.assertIs(seen["db"], self.db)
        self.assertTrue(seen["confirm"])
        self.assertIsInstance(seen["token"], CancellationToken)
        self.assertEqual(self.db.closed, 1)

    def test_second_start_rejected_while_running(self) -> None:
        release = threading.Event()
        entered = threading.Event()

        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            entered.set()
            release.wait(timeout=5)
            return WrongMatchCleanupSummary(processed=0)

        self.assertTrue(self.runner.start(
            db_factory=self._factory, cleanup_fn=cleanup_fn,
        ))
        self.assertTrue(entered.wait(timeout=5))
        self.assertEqual(self.runner.status()["state"], STATE_RUNNING)
        self.assertFalse(self.runner.start(
            db_factory=self._factory, cleanup_fn=cleanup_fn,
        ))

        release.set()
        self.runner.join(timeout=5)
        self.assertEqual(self.runner.status()["state"], STATE_COMPLETED)

        # A finished runner accepts the next sweep.
        self.assertTrue(self.runner.start(
            db_factory=self._factory, cleanup_fn=cleanup_fn,
        ))
        self.runner.join(timeout=5)

    def test_cleanup_failure_records_error_and_closes_db(self) -> None:
        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            raise RuntimeError("sweep blew up")

        self.assertTrue(self.runner.start(
            db_factory=self._factory, cleanup_fn=cleanup_fn,
        ))
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_FAILED)
        self.assertIn("RuntimeError", str(status["error"]))
        self.assertIn("sweep blew up", str(status["error"]))
        self.assertIsNone(status["summary"])
        self.assertEqual(self.db.closed, 1)

    def test_db_factory_failure_records_error(self) -> None:
        def bad_factory():
            raise RuntimeError("no dsn")

        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            raise AssertionError("must not run without a db")

        self.assertTrue(self.runner.start(
            db_factory=bad_factory, cleanup_fn=cleanup_fn,
        ))
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_FAILED)
        self.assertIn("no dsn", str(status["error"]))


class TriageRunnerCancellationTest(unittest.TestCase):
    """Issue #1083: TriageRunner.cancel() and the STATE_CANCELLED terminal
    state, distinct from STATE_COMPLETED and STATE_FAILED."""

    def setUp(self) -> None:
        self.runner = TriageRunner()
        self.db = _ClosableDB()

    def _factory(self):
        return self.db

    def test_cancel_with_no_sweep_running_is_not_an_error(self) -> None:
        """Cancel with nothing running just returns the idle snapshot."""
        result = self.runner.cancel("operator_stop")
        self.assertEqual(result["state"], STATE_IDLE)
        self.assertIsNone(result["error"])

    def test_cancel_sets_the_token_the_sweep_receives(self) -> None:
        entered = threading.Event()
        received: dict[str, object] = {}

        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            received["token"] = cancellation_token
            entered.set()
            assert cancellation_token is not None
            deadline = time.monotonic() + 5
            while (
                not cancellation_token.cancelled
                and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            return WrongMatchCleanupSummary(
                processed=1, deleted=1, cancelled=cancellation_token.cancelled,
            )

        self.assertTrue(self.runner.start(
            db_factory=self._factory, cleanup_fn=cleanup_fn,
        ))
        self.assertTrue(entered.wait(timeout=5))

        result = self.runner.cancel("operator_stop")
        self.assertEqual(result["state"], STATE_RUNNING)

        self.runner.join(timeout=5)
        token = received["token"]
        assert isinstance(token, CancellationToken)
        self.assertTrue(token.cancelled)
        self.assertEqual(token.reason, "operator_stop")

    def test_cancelled_sweep_reports_cancelled_state_not_failed(self) -> None:
        """A sweep that honors the token reports STATE_CANCELLED, and the
        partial summary is preserved exactly — not discarded."""
        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            assert cancellation_token is not None
            cancellation_token.cancel("operator_stop")
            return WrongMatchCleanupSummary(
                processed=1, deleted=1, cancelled=True,
            )

        self.assertTrue(self.runner.start(
            db_factory=self._factory, cleanup_fn=cleanup_fn,
        ))
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_CANCELLED)
        self.assertNotEqual(status["state"], STATE_FAILED)
        self.assertIsNone(status["error"])
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["deleted"], 1)
        self.assertTrue(summary["cancelled"])

    def test_cancel_racing_a_finishing_sweep_reports_completed_not_failed(
        self,
    ) -> None:
        """#1083 invariant: a cancel that races the tail of a sweep that
        already finished its own work is not an error. The sweep here
        never actually observes the cancellation (it returns before ever
        re-checking the token, exactly like the real per-row loop when
        cancel() lands after the last row) — the runner must still report
        the accurate COMPLETED summary, never failed, never a stale
        RUNNING state."""
        entered = threading.Event()
        proceed = threading.Event()

        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            entered.set()
            proceed.wait(timeout=5)
            return WrongMatchCleanupSummary(processed=2, deleted=2)

        self.assertTrue(self.runner.start(
            db_factory=self._factory, cleanup_fn=cleanup_fn,
        ))
        self.assertTrue(entered.wait(timeout=5))

        result = self.runner.cancel("operator_stop")
        self.assertEqual(result["state"], STATE_RUNNING)
        proceed.set()
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_COMPLETED)
        self.assertIsNone(status["error"])
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 2)
        self.assertEqual(summary["deleted"], 2)
        self.assertFalse(summary["cancelled"])

    def test_cancel_after_sweep_already_finished_is_not_an_error(self) -> None:
        """Cancel arriving after the runner already recorded a terminal
        state (completed) is a no-op, not a conflict."""
        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            return WrongMatchCleanupSummary(processed=1, deleted=1)

        self.assertTrue(self.runner.start(
            db_factory=self._factory, cleanup_fn=cleanup_fn,
        ))
        self.runner.join(timeout=5)
        self.assertEqual(self.runner.status()["state"], STATE_COMPLETED)

        result = self.runner.cancel("operator_stop")
        self.assertEqual(result["state"], STATE_COMPLETED)
        self.assertIsNone(result["error"])


if __name__ == "__main__":
    unittest.main()
