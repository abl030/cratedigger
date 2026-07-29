"""Success-path ownership pins for mirror and route fan-out executors."""
from __future__ import annotations

import unittest
from collections.abc import Callable
from unittest.mock import patch

from web import discogs, mb
from web.routes import browse


class _Future[T]:
    def __init__(self, job: Callable[[], T]) -> None:
        self._result = job()

    def result(self) -> T:
        return self._result

    def cancel(self) -> None:
        pass


class _Executor:
    def __init__(self, _max_workers: int) -> None:
        self.shutdown_calls: list[tuple[bool, bool]] = []

    def submit[T](self, job: Callable[[], T]) -> _Future[T]:
        return _Future(job)

    def shutdown(self, *, wait: bool, cancel_futures: bool = False) -> None:
        self.shutdown_calls.append((wait, cancel_futures))


def _assert_success_shutdown(
    module_name: str, parallel: Callable[[dict[str, Callable[[], int]]], dict[str, int]],
) -> None:
    executor = _Executor(1)
    with patch(f"{module_name}.concurrent.futures.ThreadPoolExecutor", return_value=executor), patch(
        f"{module_name}.concurrent.futures.wait", side_effect=lambda futures, **_kwargs: (set(futures), set()),
    ):
        assert parallel({"one": lambda: 1}) == {"one": 1}
    if executor.shutdown_calls != [(True, False)]:
        raise AssertionError(f"success fan-out did not own shutdown: {executor.shutdown_calls!r}")


class TestParallelExecutorShutdown(unittest.TestCase):
    def test_mb_success_shutdown_is_deterministic(self) -> None:
        _assert_success_shutdown("web.mb", lambda jobs: mb._parallel_results(jobs, max_workers=1))

    def test_discogs_success_shutdown_is_deterministic(self) -> None:
        _assert_success_shutdown("web.discogs", lambda jobs: discogs._parallel_results(jobs, max_workers=1))

    def test_route_success_shutdown_is_deterministic(self) -> None:
        _assert_success_shutdown("web.routes.browse", lambda jobs: browse._parallel_results(jobs, max_workers=1))


if __name__ == "__main__":
    unittest.main()
