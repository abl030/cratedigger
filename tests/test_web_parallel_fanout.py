"""Direct contract tests for the shared fan-out owner (issue #1355 WE5).

``web.mb``, ``web.discogs``, and ``web.routes.browse`` used to each carry
their own copy of this lifecycle. These tests pin the owner's full contract
so a future edit changes it in exactly one place: successful completion,
what happens when one job raises (every future is asked to cancel, the
executor is torn down without waiting on a still-running sibling), and the
ordering of cancel versus shutdown. Mocks stay at the real leaf seam —
``concurrent.futures.Future``/``ThreadPoolExecutor`` themselves, wrapped so
the real implementation still runs — never at this module's own logic.
"""

import concurrent.futures
import threading
import time
import unittest
from unittest import mock

from web.parallel_fanout import parallel_results


class _ProbeError(RuntimeError):
    """Raised by a synthetic job so a test can identify its origin."""


def _wrap_executor_lifecycle():
    """Wrap the real ``Future.cancel``/``ThreadPoolExecutor.shutdown`` to
    record every call while still invoking the real implementation — the
    leaf-seam technique every test below uses instead of mocking our own
    ``parallel_results`` logic."""
    calls: list[str] = []
    real_cancel = concurrent.futures.Future.cancel
    real_shutdown = concurrent.futures.ThreadPoolExecutor.shutdown

    def recording_cancel(future):
        calls.append("cancel")
        return real_cancel(future)

    def recording_shutdown(executor, wait=True, cancel_futures=False):
        calls.append(f"shutdown(wait={wait}, cancel_futures={cancel_futures})")
        return real_shutdown(executor, wait=wait, cancel_futures=cancel_futures)

    return calls, recording_cancel, recording_shutdown


class TestParallelResultsSuccess(unittest.TestCase):
    def test_returns_every_job_result_keyed_by_its_own_key(self):
        jobs = {"a": lambda: 1, "b": lambda: 2, "c": lambda: 3}
        results = parallel_results(jobs, max_workers=3)
        self.assertEqual(results, {"a": 1, "b": 2, "c": 3})

    def test_jobs_actually_run_concurrently_not_serially(self):
        """Three 0.3s jobs on three workers finish near 0.3s total, not 0.9s.

        A regression to serial execution (e.g. max_workers pinned to 1, or
        the executor replaced with a plain loop) would still pass the
        result-shape test above, so this pins the concurrency itself.
        """
        def slow(n):
            time.sleep(0.3)
            return n

        start = time.monotonic()
        results = parallel_results(
            {i: (lambda n=i: slow(n)) for i in range(3)}, max_workers=3,
        )
        elapsed = time.monotonic() - start

        self.assertEqual(results, {0: 0, 1: 1, 2: 2})
        self.assertLess(elapsed, 0.7, "jobs did not run concurrently")

    def test_max_workers_bounds_actual_concurrency(self):
        """Mutation-caught gap: a mutant that replaces ``max_workers=
        max_workers`` with ``max_workers=None`` (the executor's own
        much-larger default) still returns correct results and still
        overlaps jobs in time, so neither test above catches it. This
        counts the actual number of jobs running at once.
        """
        lock = threading.Lock()
        concurrent_count = 0
        max_seen = 0

        def job():
            nonlocal concurrent_count, max_seen
            with lock:
                concurrent_count += 1
                max_seen = max(max_seen, concurrent_count)
            time.sleep(0.05)
            with lock:
                concurrent_count -= 1

        parallel_results({i: job for i in range(4)}, max_workers=1)

        self.assertEqual(max_seen, 1, "max_workers=1 did not bound concurrency")

    def test_empty_jobs_returns_empty_dict_without_creating_an_executor(self):
        with mock.patch.object(
            concurrent.futures, "ThreadPoolExecutor",
        ) as mock_executor_cls:
            results = parallel_results({}, max_workers=4)
        self.assertEqual(results, {})
        mock_executor_cls.assert_not_called()

    def test_success_path_shuts_down_with_wait_true_and_no_cancel(self):
        """Replaces the three per-module success-shutdown pins that used
        to live in ``tests/test_parallel_executor_shutdown.py`` (one each
        for ``web.mb``, ``web.discogs``, ``web.routes.browse``'s own former
        private copy) — now that all three call this one owner, the fact
        belongs here once. A fake ``executor``/``Future`` proved the same
        fact there without ever touching a real thread; this drives the
        real ``ThreadPoolExecutor`` instead, wrapped only to record the
        call, per this module's own leaf-seam convention.
        """
        calls, recording_cancel, recording_shutdown = _wrap_executor_lifecycle()

        with mock.patch.object(concurrent.futures.Future, "cancel", recording_cancel), \
             mock.patch.object(
                 concurrent.futures.ThreadPoolExecutor, "shutdown", recording_shutdown,
             ):
            results = parallel_results({"one": lambda: 1}, max_workers=1)

        self.assertEqual(results, {"one": 1})
        self.assertEqual(calls, ["shutdown(wait=True, cancel_futures=False)"])


class TestParallelResultsFailureLifecycle(unittest.TestCase):
    """Every assertion here targets the wrapped stdlib call record, proven
    against ``PYTHONDONTWRITEBYTECODE=1`` real-thread execution — never our
    own logic. ``real_cancel``/``real_shutdown`` are still invoked, so the
    executor genuinely tears down; only the call is also recorded.
    """

    def test_one_job_exception_cancels_every_future_and_shuts_down_without_waiting(self):
        """``max_workers`` equals the job count (3-for-3), so every job gets
        its own worker with nothing left queued — there is no scheduling
        race over a leftover job. ``raiser`` additionally waits for proof
        that both siblings have actually started before it raises, so by
        construction both are provably RUNNING (never merely pending) at
        the moment the exception fires and the cancel loop runs. An
        earlier version of this test left a genuinely queued third job,
        whose fate depended on a real scheduling race between the freed
        worker and this test's own cancel loop (harmless in the case
        observed, but a latent flake in the untested case — a still
        -queued future cancelled by the executor's own
        ``cancel_futures=True`` drain, rather than by this loop, appends
        an extra recorded ``cancel`` after the ``shutdown`` entry).
        """
        calls, recording_cancel, recording_shutdown = _wrap_executor_lifecycle()
        release = threading.Event()
        sibling_a_started = threading.Event()
        sibling_b_started = threading.Event()

        def raiser():
            self.assertTrue(sibling_a_started.wait(timeout=5), "sibling_a never started")
            self.assertTrue(sibling_b_started.wait(timeout=5), "sibling_b never started")
            raise _ProbeError("boom")

        def sibling_a():
            sibling_a_started.set()
            release.wait(timeout=5)
            return "a"

        def sibling_b():
            sibling_b_started.set()
            release.wait(timeout=5)
            return "b"

        jobs = {"raiser": raiser, "sibling_a": sibling_a, "sibling_b": sibling_b}

        with mock.patch.object(concurrent.futures.Future, "cancel", recording_cancel), \
             mock.patch.object(
                 concurrent.futures.ThreadPoolExecutor, "shutdown", recording_shutdown,
             ):
            start = time.monotonic()
            with self.assertRaises(_ProbeError):
                parallel_results(jobs, max_workers=3)
            elapsed = time.monotonic() - start

        release.set()  # let the still-running siblings finish; no leaked thread

        self.assertLess(
            elapsed, 1.0,
            "shutdown(wait=False) must not block on a still-running sibling",
        )
        # One cancel() call per future in the dict, unconditionally — the
        # loop asks every future to stop, not only the one that raised.
        self.assertEqual(calls.count("cancel"), len(jobs), calls)
        self.assertTrue(
            all(call == "cancel" for call in calls[:-1]), calls,
        )
        self.assertEqual(calls[-1], "shutdown(wait=False, cancel_futures=True)", calls)

    def test_a_still_running_sibling_never_blocks_the_prompt_raise(self):
        """The ``for future in done: future.result()`` check exists so the
        final results comprehension is never reached while an
        earlier-inserted sibling is still running. Without it, the
        comprehension would call ``.result()`` on "slow" (still running)
        before it ever reaches "raiser", and block until "slow" completes.
        """
        slow_released = threading.Event()

        def slow():
            slow_released.wait(timeout=5)
            return "late"

        def raiser():
            raise _ProbeError("boom")

        # Insertion order matters: "slow" first, "raiser" second, both
        # start immediately under max_workers=2.
        jobs = {"slow": slow, "raiser": raiser}

        start = time.monotonic()
        try:
            with self.assertRaises(_ProbeError):
                parallel_results(jobs, max_workers=2)
        finally:
            elapsed = time.monotonic() - start
            slow_released.set()

        self.assertLess(
            elapsed, 1.0,
            "raising promptly must not wait on the still-running earlier sibling",
        )

    def test_the_propagated_exception_is_the_raising_jobs_own(self):
        def raiser():
            raise _ProbeError("distinct message")

        with self.assertRaises(_ProbeError) as caught:
            parallel_results({"only": raiser}, max_workers=1)
        self.assertEqual(str(caught.exception), "distinct message")


if __name__ == "__main__":
    unittest.main()
