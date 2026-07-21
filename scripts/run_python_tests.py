#!/usr/bin/env python3
"""Run the complete Python test suite across isolated unittest workers."""

from __future__ import annotations

import argparse
import io
import multiprocessing
import os
import subprocess
import sys
import tempfile
import time
import unittest
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import msgspec


DEFAULT_MAX_WORKERS = 4
DEFAULT_DURATIONS = 15
_FAILURE_MARKER = "=" * 70
_SCHEMA_READY_ENV = "CRATEDIGGER_TEST_SCHEMA_READY"


@dataclass(frozen=True)
class TestModule:
    """One importable unittest module and its scheduling weight."""

    name: str
    path: Path
    weight: int


@dataclass(frozen=True)
class ModuleRunResult:
    """Complete result for one module executed inside a persistent worker."""

    module: TestModule
    worker_pid: int
    successful: bool
    tests_run: int
    elapsed_seconds: float
    output: str


@dataclass(frozen=True)
class ModuleInfrastructureFailure:
    """A module whose worker failed outside unittest's result boundary."""

    module: TestModule
    detail: str


class ChildModuleResult(msgspec.Struct, frozen=True):
    """Wire result written by one fresh module interpreter."""

    successful: bool
    tests_run: int
    output: str


def _line_weight(path: Path) -> int:
    data = path.read_bytes()
    return max(1, data.count(b"\n") + int(bool(data) and not data.endswith(b"\n")))


def discover_test_modules(
    start_directory: Path,
    top_level_directory: Path,
    pattern: str,
) -> tuple[TestModule, ...]:
    """Discover recursive test modules without importing the test package."""
    start = start_directory.resolve()
    top = top_level_directory.resolve()
    try:
        start.relative_to(top)
    except ValueError as exc:
        raise ValueError(f"start directory {start} is outside top level {top}") from exc
    if not start.is_dir():
        raise ValueError(f"test start directory does not exist: {start}")

    modules: list[TestModule] = []
    for path in sorted(start.rglob(pattern)):
        if not path.is_file() or "__pycache__" in path.parts:
            continue
        relative = path.relative_to(top).with_suffix("")
        if any(not part.isidentifier() for part in relative.parts):
            raise ValueError(f"test path is not importable as a module: {path}")
        modules.append(TestModule(".".join(relative.parts), path, _line_weight(path)))
    return tuple(modules)


def schedule_modules(modules: Sequence[TestModule]) -> tuple[TestModule, ...]:
    """Put generated and large modules early on the shared worker queue."""
    return tuple(
        sorted(
            modules,
            key=lambda module: (
                not module.name.endswith("_generated"),
                -module.weight,
                module.name,
            ),
        ),
    )


def assert_exact_schedule(
    modules: Sequence[TestModule],
    schedule: Sequence[TestModule],
) -> None:
    """Fail if a schedule drops, duplicates, or invents a test module."""
    expected: dict[str, TestModule] = {}
    for module in modules:
        if module.name in expected:
            raise ValueError(f"duplicate input test module: {module.name}")
        expected[module.name] = module

    seen: set[str] = set()
    for module in schedule:
        if module.name in seen:
            raise ValueError(f"duplicate scheduled test module: {module.name}")
        if expected.get(module.name) != module:
            raise ValueError(f"unexpected scheduled test module: {module.name}")
        seen.add(module.name)

    missing = sorted(set(expected) - seen)
    if missing:
        raise ValueError(f"missing scheduled test modules: {', '.join(missing)}")


def worker_environment(
    base: Mapping[str, str],
    *,
    worker_index: int,
) -> dict[str, str]:
    """Build an isolated worker environment with no shared test database."""
    env = dict(base)
    env.pop("TEST_DB_DSN", None)
    env.pop(_SCHEMA_READY_ENV, None)
    env["CRATEDIGGER_TEST_WORKER"] = str(worker_index)
    return env


def _initialize_worker(top_level_directory: str) -> None:
    """Prepare one persistent worker and its private PostgreSQL fixture."""
    top = Path(top_level_directory)
    os.chdir(top)
    isolated = worker_environment(os.environ, worker_index=os.getpid())
    python_paths = [str(top)]
    tests_directory = top / "tests"
    if tests_directory.is_dir():
        python_paths.append(str(tests_directory))
    inherited_python_path = isolated.get("PYTHONPATH")
    if inherited_python_path:
        python_paths.append(inherited_python_path)
    isolated["PYTHONPATH"] = os.pathsep.join(python_paths)
    os.environ.clear()
    os.environ.update(isolated)

    # The real suite's conftest starts and migrates one private PostgreSQL per
    # persistent worker. Fresh module subprocesses inherit that DSN and skip
    # only the redundant schema application.
    conftest_path = tests_directory / "conftest.py"
    if conftest_path.is_file():
        if str(tests_directory) not in sys.path:
            sys.path.insert(0, str(tests_directory))
        if str(top) not in sys.path:
            sys.path.insert(0, str(top))
        __import__("conftest")
        if not os.environ.get("TEST_DB_DSN"):
            raise RuntimeError("worker conftest did not provide TEST_DB_DSN")
        os.environ[_SCHEMA_READY_ENV] = "1"

    # Tests deliberately exercise noisy failure paths. Keep their raw logging
    # local to the worker; unittest assertion/error diagnostics are returned in
    # ModuleRunResult and printed together after every module has completed.
    sink_fd = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(sink_fd, 1)
        os.dup2(sink_fd, 2)
    finally:
        os.close(sink_fd)


def _run_test_module_child(
    module_name: str,
    durations: int,
    result_path: Path,
) -> int:
    """Run one module in a fresh interpreter and persist its complete result."""
    stream = io.StringIO()
    suite = unittest.defaultTestLoader.loadTestsFromName(module_name)
    result = unittest.TextTestRunner(
        stream=stream,
        verbosity=2,
        durations=durations,
    ).run(suite)
    result_path.write_bytes(
        msgspec.json.encode(
            ChildModuleResult(
                successful=result.wasSuccessful(),
                tests_run=result.testsRun,
                output=stream.getvalue(),
            )
        )
    )
    return 0


def _run_test_module(module: TestModule, durations: int) -> ModuleRunResult:
    """Run one isolated module without stopping later queue work on failure."""
    started_at = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="cratedigger_test_module_") as tempdir:
        result_path = Path(tempdir) / "result.json"
        raw_output_path = Path(tempdir) / "raw-output.log"
        with raw_output_path.open("wb") as raw_output:
            completed = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--_run-module",
                    module.name,
                    str(durations),
                    str(result_path),
                ],
                stdout=raw_output,
                stderr=subprocess.STDOUT,
                check=False,
            )
        if completed.returncode != 0 or not result_path.is_file():
            raw_tail = raw_output_path.read_text(
                encoding="utf-8",
                errors="replace",
            )[-20_000:]
            raise RuntimeError(
                f"module subprocess exited {completed.returncode}: {raw_tail}"
            )
        child = msgspec.json.decode(
            result_path.read_bytes(),
            type=ChildModuleResult,
        )

    return ModuleRunResult(
        module=module,
        worker_pid=os.getpid(),
        successful=child.successful,
        tests_run=child.tests_run,
        elapsed_seconds=time.monotonic() - started_at,
        output=child.output,
    )


def _run_modules(
    schedule: Sequence[TestModule],
    *,
    worker_count: int,
    top_level_directory: Path,
    durations: int,
) -> tuple[tuple[ModuleRunResult, ...], tuple[ModuleInfrastructureFailure, ...]]:
    """Drain the shared queue completely and collect every module outcome."""
    results: list[ModuleRunResult] = []
    infrastructure_failures: list[ModuleInfrastructureFailure] = []
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(
        max_workers=worker_count,
        mp_context=context,
        initializer=_initialize_worker,
        initargs=(str(top_level_directory),),
    ) as executor:
        futures = {
            executor.submit(_run_test_module, module, durations): module
            for module in schedule
        }
        # as_completed observes failures but never cancels the remaining work.
        # Every queued module therefore contributes an outcome to this batch.
        for future in as_completed(futures):
            module = futures[future]
            try:
                result = future.result()
            except Exception as exc:  # worker infrastructure boundary
                infrastructure_failures.append(
                    ModuleInfrastructureFailure(
                        module=module,
                        detail=f"{type(exc).__name__}: {exc}",
                    )
                )
                continue
            if result.module != module:
                infrastructure_failures.append(
                    ModuleInfrastructureFailure(
                        module=module,
                        detail=f"worker returned result for {result.module.name}",
                    )
                )
                continue
            results.append(result)
    return tuple(results), tuple(infrastructure_failures)


def _failure_diagnostics(output: str) -> str:
    marker_index = output.find(_FAILURE_MARKER)
    if marker_index >= 0:
        return output[marker_index:].rstrip()
    lines = output.rstrip().splitlines()
    return "\n".join(lines[-200:])


def _parse_positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def _parse_nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be at least 0")
    return parsed


def _default_worker_count() -> int:
    configured = os.environ.get("CRATEDIGGER_TEST_JOBS")
    if configured is not None:
        return _parse_positive_int(configured)
    return min(DEFAULT_MAX_WORKERS, os.cpu_count() or 1)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-directory", type=Path, default=Path("tests"))
    parser.add_argument("--top-level-directory", type=Path, default=Path("."))
    parser.add_argument("--pattern", default="test*.py")
    parser.add_argument(
        "--jobs", type=_parse_positive_int, default=_default_worker_count()
    )
    parser.add_argument(
        "--durations",
        type=_parse_nonnegative_int,
        default=DEFAULT_DURATIONS,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    top = args.top_level_directory.resolve()
    start = args.start_directory
    if not start.is_absolute():
        start = top / start

    modules = discover_test_modules(start, top, args.pattern)
    if not modules:
        print(f"No Python tests found under {start}", file=sys.stderr)
        return 2
    schedule = schedule_modules(modules)
    assert_exact_schedule(modules, schedule)
    worker_count = min(args.jobs, len(schedule))

    print(
        f"Python suite: {len(modules)} modules across {worker_count} workers "
        f"({os.cpu_count() or 1} host CPUs)"
    )
    started_at = time.monotonic()
    results, infrastructure_failures = _run_modules(
        schedule,
        worker_count=worker_count,
        top_level_directory=top,
        durations=args.durations,
    )
    wall_seconds = time.monotonic() - started_at

    failed_results = [result for result in results if not result.successful]
    for result in sorted(results, key=lambda item: item.elapsed_seconds, reverse=True)[
        :8
    ]:
        print(
            f"SLOW: {result.elapsed_seconds:.1f}s {result.module.name} "
            f"({result.tests_run} tests, worker {result.worker_pid})"
        )

    if failed_results or infrastructure_failures:
        for result in sorted(failed_results, key=lambda item: item.module.name):
            print(
                f"\n--- FAIL: worker {result.worker_pid}, "
                f"module {result.module.name} ---"
            )
            print(_failure_diagnostics(result.output))
        for failure in sorted(
            infrastructure_failures,
            key=lambda item: item.module.name,
        ):
            print(
                f"\n--- FAIL: worker infrastructure, module {failure.module.name} ---"
            )
            print(failure.detail)
        known_count = sum(result.tests_run for result in results)
        failed_modules = len(failed_results) + len(infrastructure_failures)
        print(
            f"\nFAILED: {failed_modules} of {len(schedule)} modules; "
            f"Ran {known_count} reported tests in {wall_seconds:.1f}s"
        )
        return 1

    completed_schedule = tuple(result.module for result in results)
    assert_exact_schedule(modules, completed_schedule)
    total_tests = sum(result.tests_run for result in results)
    actual_workers = len({result.worker_pid for result in results})
    print(
        f"\nRan {total_tests} tests across {actual_workers} workers "
        f"in {wall_seconds:.1f}s"
    )
    print("\nOK")
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "--_run-module":
        raise SystemExit(
            _run_test_module_child(
                sys.argv[2],
                int(sys.argv[3]),
                Path(sys.argv[4]),
            )
        )
    raise SystemExit(main())
