#!/usr/bin/env python3
"""Run generated Hypothesis tests on an exact, property-balanced queue."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import msgspec
from hypothesis import is_hypothesis_test, settings

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_python_tests import (  # noqa: E402
    ChildTargetResult,
    _iter_test_cases,
)


TARGET_RUNNER = REPO_ROOT / "scripts" / "run_python_tests.py"
DEFAULT_PROFILE = "fuzz"
DEFAULT_DURATIONS = 5


class FuzzPropertyManifest(msgspec.Struct, frozen=True):
    """One property and the effective budget selected during discovery."""

    test_id: str
    max_examples: int
    uses_default_settings: bool


class FuzzModuleManifest(msgspec.Struct, frozen=True):
    """Exact tests and Hypothesis properties discovered in one module."""

    module_name: str
    test_ids: tuple[str, ...]
    hypothesis_tests: tuple[FuzzPropertyManifest, ...]


@dataclass(frozen=True)
class FuzzTarget:
    """One independently runnable fuzz queue target."""

    label: str
    module_name: str
    load_names: tuple[str, ...]
    expected_test_ids: tuple[str, ...]
    shard_index: int = 0
    shard_count: int = 1
    profile_max_examples: int | None = None


@dataclass(frozen=True)
class FuzzRunResult:
    """One completed target and its tmpfs log."""

    target: FuzzTarget
    successful: bool
    tests_run: int
    elapsed_seconds: float
    log_path: Path


@dataclass(frozen=True)
class FuzzInfrastructureFailure:
    """A target that failed outside unittest's result boundary."""

    target: FuzzTarget
    detail: str
    log_path: Path


class PersistedFuzzTarget(msgspec.Struct, frozen=True):
    """Failure-artifact mapping from a log file to its exact target."""

    log_name: str
    label: str
    load_names: tuple[str, ...]
    expected_test_ids: tuple[str, ...]
    shard_index: int
    shard_count: int
    profile_max_examples: int | None


class PersistedFuzzManifest(msgspec.Struct, frozen=True):
    """Complete target map copied beside retained failure logs."""

    targets: tuple[PersistedFuzzTarget, ...]


def build_fuzz_targets(
    manifests: Sequence[FuzzModuleManifest],
    *,
    property_shards: int = 1,
) -> tuple[FuzzTarget, ...]:
    """Split default fuzz budgets while batching ordinary pins exactly once."""
    if property_shards < 1:
        raise ValueError("property_shards must be at least 1")
    targets: list[FuzzTarget] = []
    ordered_manifests = sorted(
        manifests,
        key=lambda manifest: (
            -len(manifest.hypothesis_tests),
            -len(manifest.test_ids),
            manifest.module_name,
        ),
    )
    for manifest in ordered_manifests:
        if not manifest.test_ids:
            continue
        hypothesis_ids = {
            item.test_id for item in manifest.hypothesis_tests
        }
        isolate_properties = (
            len(manifest.hypothesis_tests) > 1
            or any(
                item.uses_default_settings and property_shards > 1
                for item in manifest.hypothesis_tests
            )
        )
        if not isolate_properties:
            targets.append(
                FuzzTarget(
                    label=manifest.module_name,
                    module_name=manifest.module_name,
                    load_names=(manifest.module_name,),
                    expected_test_ids=manifest.test_ids,
                )
            )
            continue

        for item in manifest.hypothesis_tests:
            if item.max_examples < 1:
                raise ValueError(
                    f"invalid Hypothesis budget for {item.test_id}: "
                    f"{item.max_examples}"
                )
            shard_count = (
                min(property_shards, item.max_examples)
                if item.uses_default_settings
                else 1
            )
            quotient, remainder = divmod(item.max_examples, shard_count)
            budgets = tuple(
                quotient + (1 if index < remainder else 0)
                for index in range(shard_count)
            )
            for shard_index, budget in enumerate(budgets):
                label = item.test_id
                if shard_count > 1:
                    label = (
                        f"{label}::entropy-"
                        f"{shard_index + 1:02d}-of-{shard_count:02d}"
                    )
                targets.append(
                    FuzzTarget(
                        label=label,
                        module_name=manifest.module_name,
                        load_names=(manifest.module_name,),
                        expected_test_ids=(item.test_id,),
                        shard_index=shard_index,
                        shard_count=shard_count,
                        profile_max_examples=(
                            budget if shard_count > 1 else None
                        ),
                    )
                )
        pin_ids = tuple(
            test_id
            for test_id in manifest.test_ids
            if test_id not in hypothesis_ids
        )
        if pin_ids:
            targets.append(
                FuzzTarget(
                    label=f"{manifest.module_name}::pins",
                    module_name=manifest.module_name,
                    load_names=(manifest.module_name,),
                    expected_test_ids=pin_ids,
                )
            )

    built = tuple(targets)
    assert_exact_fuzz_coverage(manifests, built)
    return built


def assert_exact_fuzz_coverage(
    manifests: Sequence[FuzzModuleManifest],
    targets: Sequence[FuzzTarget],
) -> None:
    """Reject omitted tests, repeated pins, or changed property budgets."""
    expected = [
        test_id for manifest in manifests for test_id in manifest.test_ids
    ]
    expected_counts = Counter(expected)
    duplicate_discovery = sorted(
        test_id for test_id, count in expected_counts.items() if count > 1
    )
    if duplicate_discovery:
        raise ValueError(
            f"duplicate discovered fuzz test: {', '.join(duplicate_discovery)}"
        )

    property_by_id = {
        item.test_id: item
        for manifest in manifests
        for item in manifest.hypothesis_tests
    }
    scheduled_by_id: dict[str, list[FuzzTarget]] = {}
    for target in targets:
        if len(set(target.expected_test_ids)) != len(target.expected_test_ids):
            raise ValueError(f"duplicate ID within fuzz target: {target.label}")
        for test_id in target.expected_test_ids:
            scheduled_by_id.setdefault(test_id, []).append(target)

    unexpected = sorted(set(scheduled_by_id) - set(expected))
    if unexpected:
        raise ValueError(f"unexpected fuzz test: {', '.join(unexpected)}")
    missing = sorted(set(expected) - set(scheduled_by_id))
    if missing:
        raise ValueError(f"missing fuzz test: {', '.join(missing)}")

    manifest_by_module = {
        manifest.module_name: manifest for manifest in manifests
    }
    for target in targets:
        manifest = manifest_by_module.get(target.module_name)
        if manifest is None:
            raise ValueError(f"unknown fuzz target module: {target.module_name}")
        if not target.load_names:
            raise ValueError(f"empty fuzz target: {target.label}")

    for test_id in expected:
        scheduled = scheduled_by_id[test_id]
        item = property_by_id.get(test_id)
        if item is None:
            if len(scheduled) != 1:
                raise ValueError(f"duplicate fuzz test: {test_id}")
            continue

        shard_counts = {target.shard_count for target in scheduled}
        if len(shard_counts) != 1:
            raise ValueError(f"inconsistent fuzz shard count: {test_id}")
        shard_count = shard_counts.pop()
        if shard_count != len(scheduled):
            raise ValueError(f"missing fuzz property shard: {test_id}")
        if {target.shard_index for target in scheduled} != set(
            range(shard_count)
        ):
            raise ValueError(f"invalid fuzz property shard index: {test_id}")
        if shard_count == 1:
            if scheduled[0].profile_max_examples is not None:
                raise ValueError(f"unexpected fuzz property budget: {test_id}")
            continue
        if not item.uses_default_settings:
            raise ValueError(f"explicit fuzz budget was sharded: {test_id}")
        shard_budgets = [
            target.profile_max_examples for target in scheduled
        ]
        if any(budget is None or budget < 1 for budget in shard_budgets):
            raise ValueError(f"invalid fuzz property budget: {test_id}")
        if sum(budget or 0 for budget in shard_budgets) != item.max_examples:
            raise ValueError(f"changed fuzz property budget: {test_id}")


def _test_method(test: unittest.TestCase) -> Callable[..., object] | None:
    method_name = test._testMethodName
    for owner in type(test).__mro__:
        candidate = vars(owner).get(method_name)
        if callable(candidate):
            return candidate
    return None


def _settings_max_examples(configured: settings) -> int:
    raw_max_examples: object = getattr(configured, "max_examples", None)
    if isinstance(raw_max_examples, bool) or not isinstance(
        raw_max_examples,
        int,
    ):
        raise TypeError("Hypothesis max_examples is not an integer")
    return raw_max_examples


def _settings_deadline(configured: settings) -> object:
    """Return the effective deadline so fuzz work never depends on timing."""
    return getattr(configured, "deadline", None)


def _discover_module_child(module_name: str, result_path: Path) -> int:
    suite = unittest.defaultTestLoader.loadTestsFromName(module_name)
    cases = tuple(_iter_test_cases(suite))
    test_ids = tuple(test.id() for test in cases)
    hypothesis_tests: list[FuzzPropertyManifest] = []
    default_settings = settings.default
    if default_settings is None:
        raise RuntimeError("Hypothesis has no active default settings")
    default_max_examples = _settings_max_examples(default_settings)
    for test in cases:
        method = _test_method(test)
        if method is None or not is_hypothesis_test(method):
            continue
        raw_configured: object | None = getattr(
            method,
            "_hypothesis_internal_use_settings",
            None,
        )
        if raw_configured is not None and not isinstance(
            raw_configured,
            settings,
        ):
            raise TypeError(f"Hypothesis test has invalid settings: {test.id()}")
        configured: settings | None = raw_configured
        uses_default_settings = configured is default_settings
        if configured is None and hasattr(
            method,
            "_hypothesis_state_machine_class",
        ):
            raw_stateful_settings: object | None = getattr(
                type(test),
                "settings",
                None,
            )
            if raw_stateful_settings is not None and not isinstance(
                raw_stateful_settings,
                settings,
            ):
                raise TypeError(
                    f"State-machine test has invalid settings: {test.id()}"
                )
            configured = raw_stateful_settings
            uses_default_settings = (
                configured is not None
                and _settings_max_examples(configured) == default_max_examples
            )
        if configured is None:
            raise TypeError(f"Hypothesis test has no settings: {test.id()}")
        deadline = _settings_deadline(configured)
        if deadline is not None:
            raise RuntimeError(
                "Hypothesis test has non-None deadline: "
                f"{test.id()}: {deadline!r}"
            )
        configured_max_examples = _settings_max_examples(configured)
        hypothesis_tests.append(
            FuzzPropertyManifest(
                test_id=test.id(),
                max_examples=configured_max_examples,
                uses_default_settings=uses_default_settings,
            )
        )
    result_path.write_bytes(
        msgspec.json.encode(
            FuzzModuleManifest(
                module_name=module_name,
                test_ids=test_ids,
                hypothesis_tests=tuple(hypothesis_tests),
            )
        )
    )
    return 0


def _discover_one_manifest(
    index: int,
    module_name: str,
    *,
    environment: Mapping[str, str],
    work_directory: Path,
) -> FuzzModuleManifest:
    result_path = work_directory / f"discover-{index:04d}.json"
    log_path = work_directory / f"discover-{index:04d}.log"
    with log_path.open("wb") as raw_output:
        completed = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--_discover-module",
                module_name,
                str(result_path),
            ],
            cwd=REPO_ROOT,
            env=environment,
            stdout=raw_output,
            stderr=subprocess.STDOUT,
            check=False,
        )
    if completed.returncode != 0 or not result_path.is_file():
        tail = log_path.read_text(encoding="utf-8", errors="replace")[-20_000:]
        raise RuntimeError(
            f"fuzz discovery failed for {module_name}: "
            f"exit {completed.returncode}: {tail}"
        )
    return msgspec.json.decode(
        result_path.read_bytes(),
        type=FuzzModuleManifest,
    )


def discover_fuzz_manifests(
    module_names: Sequence[str],
    *,
    worker_count: int,
    environment: Mapping[str, str],
    work_directory: Path,
) -> tuple[FuzzModuleManifest, ...]:
    """Discover modules concurrently without importing tests into the runner."""
    manifests_by_index: dict[int, FuzzModuleManifest] = {}
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                _discover_one_manifest,
                index,
                module_name,
                environment=environment,
                work_directory=work_directory,
            ): index
            for index, module_name in enumerate(module_names)
        }
        for future in as_completed(futures):
            index = futures[future]
            manifests_by_index[index] = future.result()
    return tuple(
        manifests_by_index[index] for index in range(len(module_names))
    )


def _execute_fuzz_target(
    index: int,
    target: FuzzTarget,
    *,
    environment: Mapping[str, str],
    log_directory: Path,
) -> FuzzRunResult | FuzzInfrastructureFailure:
    started_at = time.monotonic()
    log_path = log_directory / f"{index:04d}.log"
    result_path = log_directory / f"{index:04d}.json"
    child_environment = dict(environment)
    if target.profile_max_examples is not None:
        child_environment["CRATEDIGGER_FUZZ_MAX_EXAMPLES"] = str(
            target.profile_max_examples
        )
    with log_path.open("wb") as raw_output:
        completed = subprocess.run(
            [
                sys.executable,
                str(TARGET_RUNNER),
                "--_run-target",
                msgspec.json.encode(target.load_names).decode(),
                str(DEFAULT_DURATIONS),
                str(result_path),
                msgspec.json.encode(target.expected_test_ids).decode(),
            ],
            cwd=REPO_ROOT,
            env=child_environment,
            stdout=raw_output,
            stderr=subprocess.STDOUT,
            check=False,
        )
    elapsed_seconds = time.monotonic() - started_at
    if completed.returncode != 0 or not result_path.is_file():
        tail = log_path.read_text(encoding="utf-8", errors="replace")[-20_000:]
        return FuzzInfrastructureFailure(
            target=target,
            detail=f"target subprocess exited {completed.returncode}: {tail}",
            log_path=log_path,
        )

    child = msgspec.json.decode(
        result_path.read_bytes(),
        type=ChildTargetResult,
    )
    with log_path.open("a", encoding="utf-8") as output:
        output.write(child.output)
    if child.test_ids != target.expected_test_ids:
        return FuzzInfrastructureFailure(
            target=target,
            detail=(
                "target ran unexpected test IDs: "
                f"expected {target.expected_test_ids!r}, got {child.test_ids!r}"
            ),
            log_path=log_path,
        )
    return FuzzRunResult(
        target=target,
        successful=child.successful,
        tests_run=child.tests_run,
        elapsed_seconds=elapsed_seconds,
        log_path=log_path,
    )


def run_fuzz_targets(
    targets: Sequence[FuzzTarget],
    *,
    worker_count: int,
    environment: Mapping[str, str],
    log_directory: Path,
) -> tuple[
    tuple[FuzzRunResult, ...],
    tuple[FuzzInfrastructureFailure, ...],
]:
    """Drain every exact fuzz target and aggregate all failures."""
    results: list[FuzzRunResult] = []
    infrastructure_failures: list[FuzzInfrastructureFailure] = []
    completed_count = 0
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                _execute_fuzz_target,
                index,
                target,
                environment=environment,
                log_directory=log_directory,
            ): (index, target)
            for index, target in enumerate(targets)
        }
        for future in as_completed(futures):
            index, target = futures[future]
            completed_count += 1
            try:
                outcome = future.result()
            except Exception as exc:
                outcome = FuzzInfrastructureFailure(
                    target=target,
                    detail=f"{type(exc).__name__}: {exc}",
                    log_path=log_directory / f"{index:04d}.log",
                )
            if isinstance(outcome, FuzzInfrastructureFailure):
                infrastructure_failures.append(outcome)
                print(f"FAIL {outcome.target.label}", flush=True)
                continue
            results.append(outcome)
            if (
                outcome.successful
                and (
                    completed_count % 25 == 0
                    or completed_count == len(targets)
                )
            ):
                print(
                    f"PROGRESS {completed_count}/{len(targets)} targets",
                    flush=True,
                )
            elif not outcome.successful:
                print(f"FAIL {outcome.target.label}", flush=True)
    return tuple(results), tuple(infrastructure_failures)


def _failure_tail(log_path: Path) -> str:
    lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-80:])


def _persistent_path(value: str | None, default: Path) -> Path:
    if value is None:
        return default
    path = Path(value)
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def _seed_active_database(persistent: Path, active: Path) -> None:
    active.mkdir(parents=True)
    if persistent.is_dir():
        shutil.copytree(persistent, active, dirs_exist_ok=True)


def _test_subprocess_environment(
    base: Mapping[str, str],
    *,
    profile: str,
    active_database: Path,
) -> dict[str, str]:
    environment = dict(base)
    python_paths = [str(REPO_ROOT), str(REPO_ROOT / "tests")]
    inherited_python_path = environment.get("PYTHONPATH")
    if inherited_python_path:
        python_paths.append(inherited_python_path)
    environment.update(
        {
            "CRATEDIGGER_HYPOTHESIS_PROFILE": profile,
            "HYPOTHESIS_STORAGE_DIRECTORY": str(active_database),
            "PYTHONPATH": os.pathsep.join(python_paths),
        }
    )
    return environment


def _persist_failure_database(active: Path, persistent: Path) -> None:
    persistent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(active, persistent, dirs_exist_ok=True)


def _persist_failure_logs(
    log_directory: Path,
    persistent_root: Path,
    targets: Sequence[FuzzTarget],
) -> Path:
    persistent_root.mkdir(parents=True, exist_ok=True)
    run_directory = Path(
        tempfile.mkdtemp(prefix="run.", dir=persistent_root)
    )
    persisted_logs = run_directory / "logs"
    shutil.copytree(log_directory, persisted_logs)
    manifest = PersistedFuzzManifest(
        targets=tuple(
            PersistedFuzzTarget(
                log_name=f"{index:04d}.log",
                label=target.label,
                load_names=target.load_names,
                expected_test_ids=target.expected_test_ids,
                shard_index=target.shard_index,
                shard_count=target.shard_count,
                profile_max_examples=target.profile_max_examples,
            )
            for index, target in enumerate(targets)
        )
    )
    mapped_log_names = {target.log_name for target in manifest.targets}
    unexpected_logs = sorted(
        path.name
        for path in persisted_logs.glob("*.log")
        if path.name not in mapped_log_names
    )
    if unexpected_logs:
        raise RuntimeError(
            f"retained fuzz logs lack target mappings: {', '.join(unexpected_logs)}"
        )
    (run_directory / "targets.json").write_bytes(msgspec.json.encode(manifest))
    return run_directory


def _parse_positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def _default_jobs() -> int:
    configured = os.environ.get("CRATEDIGGER_FUZZ_JOBS")
    if configured is not None:
        return _parse_positive_int(configured)
    return os.cpu_count() or 1


def recommended_property_shards(cpu_count: int) -> int:
    """Keep a deep fuzz tail wide without excessive process startup."""
    if cpu_count < 1:
        raise ValueError("cpu_count must be at least 1")
    return min(8, max(1, (cpu_count + 3) // 4))


def _configured_property_shards() -> int | None:
    configured = os.environ.get("CRATEDIGGER_FUZZ_PROPERTY_SHARDS")
    if configured is None:
        return None
    return _parse_positive_int(configured)


def _default_modules() -> tuple[str, ...]:
    return tuple(
        f"tests.{path.stem}"
        for path in sorted((REPO_ROOT / "tests").glob("test_*_generated.py"))
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("modules", nargs="*")
    parser.add_argument("--jobs", type=_parse_positive_int, default=_default_jobs())
    parser.add_argument(
        "--property-shards",
        type=_parse_positive_int,
        default=_configured_property_shards(),
    )
    parser.add_argument(
        "--profile",
        default=os.environ.get("FUZZ_PROFILE", DEFAULT_PROFILE),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    module_names = tuple(args.modules) or _default_modules()
    if not module_names:
        print("No generated fuzz modules found", file=sys.stderr)
        return 2
    worker_count = min(args.jobs, max(1, len(module_names)))
    persistent_database = _persistent_path(
        os.environ.get("HYPOTHESIS_STORAGE_DIRECTORY"),
        REPO_ROOT / ".hypothesis",
    )
    persistent_output_value = os.environ.get("CRATEDIGGER_FUZZ_OUTPUT_DIR")
    property_shards = args.property_shards
    if property_shards is None:
        property_shards = (
            recommended_property_shards(os.cpu_count() or 1)
            if args.profile == DEFAULT_PROFILE
            else 1
        )
    if args.profile != DEFAULT_PROFILE and property_shards != 1:
        print(
            "Property entropy sharding is supported only by the fuzz profile",
            file=sys.stderr,
        )
        return 2

    with tempfile.TemporaryDirectory(prefix="cratedigger_fuzz_") as tempdir:
        active_root = Path(tempdir)
        active_database = active_root / "hypothesis"
        discovery_directory = active_root / "discovery"
        log_directory = active_root / "logs"
        discovery_directory.mkdir()
        log_directory.mkdir()
        _seed_active_database(persistent_database, active_database)
        child_environment = _test_subprocess_environment(
            os.environ,
            profile=args.profile,
            active_database=active_database,
        )

        manifests = discover_fuzz_manifests(
            module_names,
            worker_count=worker_count,
            environment=child_environment,
            work_directory=discovery_directory,
        )
        targets = build_fuzz_targets(
            manifests,
            property_shards=property_shards,
        )
        if not targets:
            print("No generated fuzz tests found", file=sys.stderr)
            return 2
        worker_count = min(args.jobs, len(targets))
        property_ids = {
            item.test_id
            for manifest in manifests
            for item in manifest.hypothesis_tests
        }
        property_targets = sum(
            len(target.expected_test_ids) == 1
            and target.expected_test_ids[0] in property_ids
            for target in targets
        )
        print(
            f"fuzz burst: {len(module_names)} generated modules, "
            f"{len(targets)} targets ({property_targets} property targets, "
            f"up to {property_shards} entropy shards), "
            f"up to {worker_count} parallel "
            f"({os.cpu_count() or 1} host cores), profile={args.profile}",
            flush=True,
        )
        started_at = time.monotonic()
        results, infrastructure_failures = run_fuzz_targets(
            targets,
            worker_count=worker_count,
            environment=child_environment,
            log_directory=log_directory,
        )
        wall_seconds = time.monotonic() - started_at

        failed_results = [result for result in results if not result.successful]
        for result in sorted(
            results,
            key=lambda item: item.elapsed_seconds,
            reverse=True,
        )[:12]:
            print(
                f"SLOW {result.elapsed_seconds:.1f}s {result.target.label}"
            )
        if failed_results or infrastructure_failures:
            for result in sorted(
                failed_results,
                key=lambda item: item.target.label,
            ):
                print(f"\n--- FAIL {result.target.label} ---")
                print(_failure_tail(result.log_path))
            for failure in sorted(
                infrastructure_failures,
                key=lambda item: item.target.label,
            ):
                print(f"\n--- INFRASTRUCTURE FAIL {failure.target.label} ---")
                print(failure.detail)
            _persist_failure_database(active_database, persistent_database)
            if persistent_output_value is not None:
                retained = _persist_failure_logs(
                    log_directory,
                    _persistent_path(persistent_output_value, REPO_ROOT),
                    targets,
                )
                print(f"fuzz burst: complete module logs retained at {retained}")
            print(
                f"fuzz burst: FAILURES after {wall_seconds:.1f}s "
                f"({len(failed_results) + len(infrastructure_failures)} targets)"
            )
            return 1

        if Counter(result.target for result in results) != Counter(targets):
            raise RuntimeError("completed fuzz target coverage changed")
        assert_exact_fuzz_coverage(manifests, targets)
        expected_ids = tuple(
            test_id
            for manifest in manifests
            for test_id in manifest.test_ids
        )
        print(
            f"fuzz burst: ALL GREEN ({len(module_names)} modules, "
            f"{len(expected_ids)} tests, {wall_seconds:.1f}s)"
        )
        return 0


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--_discover-module":
        raise SystemExit(
            _discover_module_child(
                sys.argv[2],
                Path(sys.argv[3]),
            )
        )
    raise SystemExit(main())
