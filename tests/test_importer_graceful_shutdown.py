"""Best-effort graceful SIGTERM drain for the importer's poll loop (#1089)."""

from __future__ import annotations

import os
import signal
import sys
import unittest

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

from scripts import importer
from tests.fakes import FakePipelineDB


class TestGracefulShutdownFlag(unittest.TestCase):
    def test_starts_unrequested(self) -> None:
        flag = importer.GracefulShutdown()

        self.assertFalse(flag.requested)

    def test_request_is_a_valid_signal_handler_and_sets_the_flag(self) -> None:
        """The exact call shape ``signal.signal(SIGTERM, flag.request)``
        invokes: ``(signum, frame)``."""
        flag = importer.GracefulShutdown()

        flag.request(signal.SIGTERM, None)

        self.assertTrue(flag.requested)


class TestDrainImportQueue(unittest.TestCase):
    def _counting_candidate_scan(self, db: FakePipelineDB) -> list[int]:
        calls: list[int] = []
        original = db.peek_import_job_candidates

        def counting(*args: object, **kwargs: object):
            calls.append(1)
            return original(*args, **kwargs)

        db.peek_import_job_candidates = counting
        return calls

    def test_shutdown_already_requested_claims_nothing(self) -> None:
        """The core claim: once SIGTERM is proven delivered, the loop must
        not attempt even one more claim scan."""
        db = FakePipelineDB()
        calls = self._counting_candidate_scan(db)
        shutdown = importer.GracefulShutdown(requested=True)

        importer._drain_import_queue(
            db,  # pyright: ignore[reportArgumentType]
            worker_id="worker",
            poll_interval=0.0,
            once=False,
            shutdown=shutdown,
        )

        self.assertEqual(calls, [])

    def test_shutdown_requested_mid_loop_stops_before_the_next_claim(
        self,
    ) -> None:
        """A signal delivered between iterations (no in-flight job to
        finish, nothing claimable) still halts claiming promptly rather
        than looping until an external kill."""
        db = FakePipelineDB()
        shutdown = importer.GracefulShutdown()
        original_scan = db.peek_import_job_candidates
        calls: list[int] = []

        def counting_then_request_shutdown(*args: object, **kwargs: object):
            calls.append(1)
            if len(calls) >= 2:
                shutdown.requested = True
            return original_scan(*args, **kwargs)

        db.peek_import_job_candidates = counting_then_request_shutdown

        importer._drain_import_queue(
            db,  # pyright: ignore[reportArgumentType]
            worker_id="worker",
            poll_interval=0.0,
            once=False,
            shutdown=shutdown,
        )

        # Exactly two scans happened before the flag stopped a third.
        self.assertEqual(len(calls), 2)

    def test_once_mode_returns_after_a_single_pass_regardless_of_shutdown(
        self,
    ) -> None:
        """The pre-existing ``--once`` contract is untouched by this
        feature: it still returns after exactly one ``run_once`` call."""
        db = FakePipelineDB()
        calls = self._counting_candidate_scan(db)
        shutdown = importer.GracefulShutdown()

        importer._drain_import_queue(
            db,  # pyright: ignore[reportArgumentType]
            worker_id="worker",
            poll_interval=0.0,
            once=True,
            shutdown=shutdown,
        )

        self.assertEqual(len(calls), 1)
        self.assertFalse(shutdown.requested)


if __name__ == "__main__":
    unittest.main()
