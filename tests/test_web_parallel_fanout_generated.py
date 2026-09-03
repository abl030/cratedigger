"""Generated contract for web.parallel_fanout.parallel_results (#1355 WE5).

The deterministic pins in ``tests/test_web_parallel_fanout.py`` own the
cancellation/shutdown mechanics for named worlds (three jobs, two workers,
a specific insertion order). This property patrols the value/exception
contract across the job-count and raiser-position combinations those pins
do not enumerate one by one: with no failing job, every key maps back to
its own result; with exactly one failing job, that job's own exception is
what propagates, whatever its position or the worker count.

Every job returns or raises immediately (no sleeping), so this stays fast
across the full example budget — the timing-sensitive cancellation
mechanics belong to the deterministic pins, not here.
"""
from __future__ import annotations

import os
import sys
import unittest
from collections.abc import Callable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from web.parallel_fanout import parallel_results

_MAX_JOBS = 8


class _ProbeError(RuntimeError):
    """Raised by a synthetic job so a property can identify its origin."""


@st.composite
def _no_raiser_worlds(draw: st.DrawFn) -> tuple[list[str], int]:
    job_count = draw(st.integers(min_value=1, max_value=_MAX_JOBS))
    max_workers = draw(st.integers(min_value=1, max_value=job_count))
    return [f"job-{i}" for i in range(job_count)], max_workers


@st.composite
def _single_raiser_worlds(draw: st.DrawFn) -> tuple[list[str], int, int]:
    job_count = draw(st.integers(min_value=1, max_value=_MAX_JOBS))
    raiser_index = draw(st.integers(min_value=0, max_value=job_count - 1))
    max_workers = draw(st.integers(min_value=1, max_value=job_count))
    return [f"job-{i}" for i in range(job_count)], raiser_index, max_workers


class TestParallelResultsGenerated(unittest.TestCase):
    @given(world=_no_raiser_worlds())
    def test_every_key_maps_to_its_own_result_when_nothing_raises(self, world):
        keys, max_workers = world
        jobs = {key: (lambda key=key: key) for key in keys}

        results = parallel_results(jobs, max_workers=max_workers)

        self.assertEqual(results, {key: key for key in keys})

    @given(world=_single_raiser_worlds())
    def test_the_single_raisers_own_exception_propagates(self, world):
        keys, raiser_index, max_workers = world
        raiser_key = keys[raiser_index]

        def make_job(key: str) -> Callable[[], str]:
            if key == raiser_key:
                def job() -> str:
                    raise _ProbeError(key)
                return job
            return lambda key=key: key

        jobs = {key: make_job(key) for key in keys}

        with self.assertRaises(_ProbeError) as caught:
            parallel_results(jobs, max_workers=max_workers)

        self.assertEqual(str(caught.exception), raiser_key)


if __name__ == "__main__":
    unittest.main()
