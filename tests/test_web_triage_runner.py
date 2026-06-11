"""Tests for the background Wrong Matches bulk-triage runner."""

from __future__ import annotations

import threading
import unittest

from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary
from web.triage_runner import (
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

        def cleanup_fn(db, *, confirm_all_wrong_matches):
            seen["db"] = db
            seen["confirm"] = confirm_all_wrong_matches
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
        self.assertEqual(self.db.closed, 1)

    def test_second_start_rejected_while_running(self) -> None:
        release = threading.Event()
        entered = threading.Event()

        def cleanup_fn(db, *, confirm_all_wrong_matches):
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
        def cleanup_fn(db, *, confirm_all_wrong_matches):
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

        def cleanup_fn(db, *, confirm_all_wrong_matches):
            raise AssertionError("must not run without a db")

        self.assertTrue(self.runner.start(
            db_factory=bad_factory, cleanup_fn=cleanup_fn,
        ))
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_FAILED)
        self.assertIn("no dsn", str(status["error"]))


if __name__ == "__main__":
    unittest.main()
