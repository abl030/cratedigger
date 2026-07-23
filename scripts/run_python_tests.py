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
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import msgspec


DEFAULT_MAX_WORKERS = 12
DEFAULT_DURATIONS = 15
_FAILURE_MARKER = "=" * 70
_SCHEMA_READY_ENV = "CRATEDIGGER_TEST_SCHEMA_READY"
WORLD_MODEL_MODULE = "tests.world_model.state_machine"
HOTSPOT_SHARD_POLICIES = {
    "tests.test_beets_destructive_configs_generated": "method_batch",
    "tests.test_pipeline_db": "class_batch",
}
HOTSPOT_CLASS_BATCHES = 8
HOTSPOT_METHOD_BATCHES = 12


@dataclass(frozen=True)
class TestModule:
    """One importable unittest module and its scheduling weight."""

    name: str
    path: Path
    weight: int
    environment: tuple[tuple[str, str], ...] = ()
    unset_environment: tuple[str, ...] = ()
    frontload: bool = False


@dataclass(frozen=True)
class TestTarget:
    """One independently runnable unittest name from a source module."""

    module: TestModule
    test_name: str
    expected_test_ids: tuple[str, ...] = ()
    load_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class TargetRunResult:
    """Complete result for one target executed inside a persistent worker."""

    target: TestTarget
    worker_pid: int
    successful: bool
    tests_run: int
    elapsed_seconds: float
    output: str


@dataclass(frozen=True)
class TargetInfrastructureFailure:
    """A target whose worker failed outside unittest's result boundary."""

    target: TestTarget
    detail: str


class ChildTargetResult(msgspec.Struct, frozen=True):
    """Wire result written by one fresh target interpreter."""

    successful: bool
    tests_run: int
    test_ids: tuple[str, ...]
    output: str


class ListedTestIds(msgspec.Struct, frozen=True):
    """Wire manifest returned by an isolated unittest discovery process."""

    test_ids: tuple[str, ...]


class RecordingTextTestResult(unittest.TextTestResult):
    """Text result that proves which exact unittest IDs executed."""

    test_ids: list[str] | None = None

    def startTest(self, test: unittest.TestCase) -> None:
        if self.test_ids is None:
            self.test_ids = []
        self.test_ids.append(test.id())
        super().startTest(test)


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


def complete_test_modules(
    discovered: Sequence[TestModule],
    top_level_directory: Path,
) -> tuple[TestModule, ...]:
    """Add deterministic suites whose filenames intentionally evade discovery."""
    modules = tuple(discovered)
    if any(module.name == WORLD_MODEL_MODULE for module in modules):
        raise ValueError(f"duplicate explicit test module: {WORLD_MODEL_MODULE}")
    world_path = top_level_directory / "tests" / "world_model" / "state_machine.py"
    if not world_path.is_file():
        return modules
    return modules + (
        TestModule(
            name=WORLD_MODEL_MODULE,
            path=world_path,
            weight=_line_weight(world_path),
            environment=(
                ("CRATEDIGGER_WORLD_RANDOMIZED", "0"),
                ("CRATEDIGGER_WORLD_EXAMPLES", "6"),
                ("CRATEDIGGER_WORLD_STEPS", "8"),
            ),
            unset_environment=("TEST_DB_DSN", _SCHEMA_READY_ENV),
            frontload=True,
        ),
    )


def schedule_modules(modules: Sequence[TestModule]) -> tuple[TestModule, ...]:
    """Put generated and large modules early on the shared worker queue."""
    return tuple(
        sorted(
            modules,
            key=lambda module: (
                not (module.frontload or module.name.endswith("_generated")),
                not module.frontload,
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


def shard_test_ids(
    module: TestModule,
    test_ids: Sequence[str],
    *,
    granularity: str,
) -> tuple[TestTarget, ...]:
    """Split one audited hotspot while preserving every discovered test ID."""
    if granularity not in {"class", "class_batch", "method", "method_batch"}:
        raise ValueError(f"unsupported test sharding granularity: {granularity}")
    if not test_ids:
        raise ValueError(f"hotspot module has no discovered tests: {module.name}")
    if len(set(test_ids)) != len(test_ids):
        raise ValueError(f"duplicate discovered test ID in {module.name}")

    prefix = f"{module.name}."
    grouped: dict[str, list[str]] = {}
    for test_id in test_ids:
        if not test_id.startswith(prefix):
            raise ValueError(
                f"test ID {test_id} does not belong to module {module.name}"
            )
        target_name = (
            test_id
            if granularity in {"method", "method_batch"}
            else test_id.rsplit(".", 1)[0]
        )
        grouped.setdefault(target_name, []).append(test_id)

    if granularity in {"class_batch", "method_batch"}:
        maximum_batches = (
            HOTSPOT_CLASS_BATCHES
            if granularity == "class_batch"
            else HOTSPOT_METHOD_BATCHES
        )
        batch_count = min(maximum_batches, len(grouped))
        batches: list[tuple[list[str], list[str]]] = [
            ([], []) for _ in range(batch_count)
        ]
        for class_name, expected_ids in sorted(
            grouped.items(),
            key=lambda item: (-len(item[1]), item[0]),
        ):
            batch_names, batch_ids = min(
                batches,
                key=lambda batch: (len(batch[1]), tuple(batch[0])),
            )
            batch_names.append(class_name)
            batch_ids.extend(expected_ids)
        ordered_batches = sorted(
            batches,
            key=lambda batch: (-len(batch[1]), tuple(batch[0])),
        )
        targets = tuple(
            TestTarget(
                module=module,
                test_name=(
                    f"{module.name}::{granularity.replace('_', '-')}-{index:02d}"
                ),
                expected_test_ids=tuple(expected_ids),
                load_names=tuple(class_names),
            )
            for index, (class_names, expected_ids) in enumerate(
                ordered_batches,
                start=1,
            )
        )
        assert_exact_target_coverage(module, test_ids, targets)
        return targets

    targets = tuple(
        TestTarget(module, target_name, tuple(expected_ids))
        for target_name, expected_ids in sorted(
            grouped.items(),
            key=lambda item: (-len(item[1]), item[0]),
        )
    )
    assert_exact_target_coverage(module, test_ids, targets)
    return targets


def assert_exact_target_coverage(
    module: TestModule,
    test_ids: Sequence[str],
    targets: Sequence[TestTarget],
) -> None:
    """Reject a hotspot schedule that drops, duplicates, or invents a test ID."""
    expected = set(test_ids)
    scheduled = [test_id for target in targets for test_id in target.expected_test_ids]
    duplicates = sorted(
        test_id for test_id, count in Counter(scheduled).items() if count > 1
    )
    if duplicates:
        raise ValueError(f"duplicate test target: {', '.join(duplicates)}")
    unexpected = sorted(set(scheduled) - expected)
    if unexpected:
        raise ValueError(f"unexpected test target: {', '.join(unexpected)}")
    missing = sorted(expected - set(scheduled))
    if missing:
        raise ValueError(f"missing test target: {', '.join(missing)}")
    if any(target.module != module for target in targets):
        raise ValueError(f"test target belongs to the wrong module: {module.name}")


def build_test_targets(
    schedule: Sequence[TestModule],
    listed_test_ids: Mapping[str, Sequence[str]],
) -> tuple[TestTarget, ...]:
    """Expand only audited hotspots, leaving every other module isolated."""
    targets: list[TestTarget] = []
    for module in schedule:
        granularity = HOTSPOT_SHARD_POLICIES.get(module.name)
        if granularity is None:
            targets.append(TestTarget(module, module.name))
            continue
        test_ids = listed_test_ids.get(module.name)
        if test_ids is None:
            raise ValueError(f"missing discovery manifest for hotspot {module.name}")
        targets.extend(shard_test_ids(module, test_ids, granularity=granularity))
    return tuple(targets)


def assert_exact_target_schedule(
    expected: Sequence[TestTarget],
    actual: Sequence[TestTarget],
) -> None:
    """Fail if execution drops, duplicates, or substitutes a queue target."""
    expected_by_name = {target.test_name: target for target in expected}
    if len(expected_by_name) != len(expected):
        raise ValueError("duplicate expected test target")
    actual_by_name = {target.test_name: target for target in actual}
    if len(actual_by_name) != len(actual):
        raise ValueError("duplicate completed test target")
    missing = sorted(set(expected_by_name) - set(actual_by_name))
    if missing:
        raise ValueError(f"missing completed test target: {', '.join(missing)}")
    unexpected = sorted(set(actual_by_name) - set(expected_by_name))
    if unexpected:
        raise ValueError(f"unexpected completed test target: {', '.join(unexpected)}")
    for name, target in actual_by_name.items():
        if expected_by_name[name] != target:
            raise ValueError(f"completed target changed identity: {name}")


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


def test_subprocess_environment(
    base: Mapping[str, str],
    module: TestModule,
) -> dict[str, str]:
    """Apply one module's explicit environment boundary to a child process."""
    env = dict(base)
    for name in module.unset_environment:
        env.pop(name, None)
    env.update(module.environment)
    return env


def _python_path_environment(
    base: Mapping[str, str],
    top_level_directory: Path,
) -> dict[str, str]:
    """Make the repository and top-level test helpers importable in a child."""
    env = worker_environment(base, worker_index=0)
    python_paths = [str(top_level_directory)]
    tests_directory = top_level_directory / "tests"
    if tests_directory.is_dir():
        python_paths.append(str(tests_directory))
    inherited_python_path = env.get("PYTHONPATH")
    if inherited_python_path:
        python_paths.append(inherited_python_path)
    env["PYTHONPATH"] = os.pathsep.join(python_paths)
    return env


def _initialize_worker(top_level_directory: str) -> None:
    """Prepare one persistent worker and its private PostgreSQL fixture."""
    top = Path(top_level_directory)
    os.chdir(top)
    isolated = _python_path_environment(os.environ, top)
    isolated["CRATEDIGGER_TEST_WORKER"] = str(os.getpid())
    tests_directory = top / "tests"
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
    # TargetRunResult and printed together after every target has completed.
    sink_fd = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(sink_fd, 1)
        os.dup2(sink_fd, 2)
    finally:
        os.close(sink_fd)


def _iter_test_cases(suite: unittest.TestSuite) -> Iterator[unittest.TestCase]:
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from _iter_test_cases(test)
        else:
            yield test


def _list_module_test_ids_child(module_name: str, result_path: Path) -> int:
    """Discover exact unittest IDs in a disposable interpreter."""
    suite = unittest.defaultTestLoader.loadTestsFromName(module_name)
    test_ids = tuple(test.id() for test in _iter_test_cases(suite))
    result_path.write_bytes(msgspec.json.encode(ListedTestIds(test_ids)))
    return 0


def list_module_test_ids(
    module_name: str,
    top_level_directory: Path,
) -> tuple[str, ...]:
    """List a hotspot's tests without importing it into the coordinator."""
    with tempfile.TemporaryDirectory(prefix="cratedigger_test_list_") as tempdir:
        result_path = Path(tempdir) / "result.json"
        raw_output_path = Path(tempdir) / "raw-output.log"
        with raw_output_path.open("wb") as raw_output:
            completed = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--_list-module",
                    module_name,
                    str(result_path),
                ],
                cwd=top_level_directory,
                env=_python_path_environment(os.environ, top_level_directory),
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
                f"test listing subprocess exited {completed.returncode}: {raw_tail}"
            )
        listed = msgspec.json.decode(
            result_path.read_bytes(),
            type=ListedTestIds,
        )
    return listed.test_ids


def _run_test_target_child(
    test_names: tuple[str, ...],
    durations: int,
    result_path: Path,
    selected_test_ids: tuple[str, ...] | None = None,
) -> int:
    """Run one target in a fresh interpreter and persist its complete result."""
    stream = io.StringIO()
    suite = unittest.defaultTestLoader.loadTestsFromNames(test_names)
    if selected_test_ids is not None:
        discovered_by_id: dict[str, unittest.TestCase] = {}
        for test in _iter_test_cases(suite):
            test_id = test.id()
            if test_id in discovered_by_id:
                raise ValueError(f"duplicate loaded test ID: {test_id}")
            discovered_by_id[test_id] = test
        missing = [
            test_id
            for test_id in selected_test_ids
            if test_id not in discovered_by_id
        ]
        if missing:
            raise ValueError(
                f"selected test IDs were not loaded: {', '.join(missing)}"
            )
        suite = unittest.TestSuite(
            discovered_by_id[test_id] for test_id in selected_test_ids
        )
    result = unittest.TextTestRunner(
        stream=stream,
        verbosity=2,
        durations=durations,
        resultclass=RecordingTextTestResult,  # pyright: ignore[reportArgumentType]
    ).run(suite)
    if not isinstance(result, RecordingTextTestResult):
        raise TypeError("unittest runner returned an unexpected result type")
    result_path.write_bytes(
        msgspec.json.encode(
            ChildTargetResult(
                successful=result.wasSuccessful(),
                tests_run=result.testsRun,
                test_ids=tuple(result.test_ids or ()),
                output=stream.getvalue(),
            )
        )
    )
    return 0


def _run_test_target(target: TestTarget, durations: int) -> TargetRunResult:
    """Run one isolated target without stopping later queue work on failure."""
    started_at = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="cratedigger_test_target_") as tempdir:
        result_path = Path(tempdir) / "result.json"
        raw_output_path = Path(tempdir) / "raw-output.log"
        with raw_output_path.open("wb") as raw_output:
            completed = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--_run-target",
                    msgspec.json.encode(
                        target.load_names or (target.test_name,)
                    ).decode(),
                    str(durations),
                    str(result_path),
                ],
                env=test_subprocess_environment(os.environ, target.module),
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
                f"target subprocess exited {completed.returncode}: {raw_tail}"
            )
        child = msgspec.json.decode(
            result_path.read_bytes(),
            type=ChildTargetResult,
        )
        if target.expected_test_ids and child.test_ids != target.expected_test_ids:
            raise RuntimeError(
                f"target {target.test_name} ran unexpected test IDs: "
                f"expected {target.expected_test_ids!r}, got {child.test_ids!r}"
            )

    return TargetRunResult(
        target=target,
        worker_pid=os.getpid(),
        successful=child.successful,
        tests_run=child.tests_run,
        elapsed_seconds=time.monotonic() - started_at,
        output=child.output,
    )


def _run_targets(
    schedule: Sequence[TestTarget],
    *,
    worker_count: int,
    top_level_directory: Path,
    durations: int,
) -> tuple[tuple[TargetRunResult, ...], tuple[TargetInfrastructureFailure, ...]]:
    """Drain the shared queue completely and collect every target outcome."""
    results: list[TargetRunResult] = []
    infrastructure_failures: list[TargetInfrastructureFailure] = []
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(
        max_workers=worker_count,
        mp_context=context,
        initializer=_initialize_worker,
        initargs=(str(top_level_directory),),
    ) as executor:
        futures = {
            executor.submit(_run_test_target, target, durations): target
            for target in schedule
        }
        # as_completed observes failures but never cancels the remaining work.
        # Every queued target therefore contributes an outcome to this batch.
        for future in as_completed(futures):
            target = futures[future]
            try:
                result = future.result()
            except Exception as exc:  # worker infrastructure boundary
                infrastructure_failures.append(
                    TargetInfrastructureFailure(
                        target=target,
                        detail=f"{type(exc).__name__}: {exc}",
                    )
                )
                continue
            if result.target != target:
                infrastructure_failures.append(
                    TargetInfrastructureFailure(
                        target=target,
                        detail=(
                            f"worker returned result for {result.target.test_name}"
                        ),
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


def recommended_worker_count(cpu_count: int) -> int:
    """Use half the host up to the measured point of diminishing returns."""
    if cpu_count < 1:
        raise ValueError("cpu_count must be at least 1")
    return min(DEFAULT_MAX_WORKERS, max(1, cpu_count // 2))


def _default_worker_count() -> int:
    configured = os.environ.get("CRATEDIGGER_TEST_JOBS")
    if configured is not None:
        return _parse_positive_int(configured)
    return recommended_worker_count(os.cpu_count() or 1)


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

    discovered = discover_test_modules(start, top, args.pattern)
    if not discovered:
        print(f"No Python tests found under {start}", file=sys.stderr)
        return 2
    modules = complete_test_modules(discovered, top)
    module_schedule = schedule_modules(modules)
    assert_exact_schedule(modules, module_schedule)
    hotspot_names = HOTSPOT_SHARD_POLICIES.keys() & {module.name for module in modules}
    listed_test_ids = {
        module_name: list_module_test_ids(module_name, top)
        for module_name in sorted(hotspot_names)
    }
    schedule = build_test_targets(module_schedule, listed_test_ids)
    worker_count = min(args.jobs, len(schedule))

    print(
        f"Python suite: {len(modules)} modules across {worker_count} workers "
        f"({os.cpu_count() or 1} host CPUs)"
    )
    sharded_target_count = sum(
        target.module.name in HOTSPOT_SHARD_POLICIES for target in schedule
    )
    print(
        f"Queue: {len(schedule)} targets "
        f"({sharded_target_count} audited hotspot targets)"
    )
    started_at = time.monotonic()
    results, infrastructure_failures = _run_targets(
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
            f"SLOW: {result.elapsed_seconds:.1f}s {result.target.test_name} "
            f"({result.tests_run} tests, worker {result.worker_pid})"
        )

    completed_targets = tuple(result.target for result in results) + tuple(
        failure.target for failure in infrastructure_failures
    )
    assert_exact_target_schedule(schedule, completed_targets)

    if failed_results or infrastructure_failures:
        for result in sorted(
            failed_results,
            key=lambda item: item.target.test_name,
        ):
            print(
                f"\n--- FAIL: worker {result.worker_pid}, "
                f"target {result.target.test_name} ---"
            )
            print(_failure_diagnostics(result.output))
        for failure in sorted(
            infrastructure_failures,
            key=lambda item: item.target.test_name,
        ):
            print(
                "\n--- FAIL: worker infrastructure, target "
                f"{failure.target.test_name} ---"
            )
            print(failure.detail)
        known_count = sum(result.tests_run for result in results)
        failed_targets = len(failed_results) + len(infrastructure_failures)
        print(
            f"\nFAILED: {failed_targets} of {len(schedule)} targets; "
            f"Ran {known_count} reported tests in {wall_seconds:.1f}s"
        )
        return 1

    total_tests = sum(result.tests_run for result in results)
    actual_workers = len({result.worker_pid for result in results})
    print(
        f"\nRan {total_tests} tests across {actual_workers} workers "
        f"in {wall_seconds:.1f}s"
    )
    print("\nOK")
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--_list-module":
        raise SystemExit(
            _list_module_test_ids_child(
                sys.argv[2],
                Path(sys.argv[3]),
            )
        )
    if len(sys.argv) in {5, 6} and sys.argv[1] == "--_run-target":
        raise SystemExit(
            _run_test_target_child(
                msgspec.json.decode(sys.argv[2], type=tuple[str, ...]),
                int(sys.argv[3]),
                Path(sys.argv[4]),
                (
                    msgspec.json.decode(sys.argv[5], type=tuple[str, ...])
                    if len(sys.argv) == 6
                    else None
                ),
            )
        )
    raise SystemExit(main())
