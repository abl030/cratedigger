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
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ThreadPoolExecutor,
    as_completed,
    wait,
)
from dataclasses import dataclass
from pathlib import Path

import msgspec
from hypothesis import settings

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_python_tests import (
    HOTSPOT_SHARD_POLICIES,
    STRATEGY_SPACE_EXHAUSTED,
    TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE,
    ChildTargetResult,
    HypothesisPropertyStats,
    TestModule,
    _iter_test_cases,
    _test_method,
    assert_hypothesis_deadlines_disabled,
    resolve_hypothesis_settings,
    settings_max_examples,
    shard_test_ids,
)
from scripts.run_test_suite import (
    TEST_RAM_ROOT_EXHAUSTED,
    RamRootExhaustedError,
    _check_suite_headroom,
    headroom_floor_bytes,
    private_runtime_dir,
)
from tests.finite_domain_metadata import (
    FINITE_DOMAIN_ATTRIBUTE,
    FiniteDomainSpec,
)

TARGET_RUNNER = REPO_ROOT / "scripts" / "run_python_tests.py"
DEFAULT_PROFILE = "fuzz"
DEFAULT_DURATIONS = 5
EPHEMERAL_POSTGRES_TARGET_LIMIT = 24
MIN_EXAMPLES_PER_ENTROPY_SHARD = 250
FUZZ_PROPERTY_SHARD_LIMITS = {
    "tests.test_beets_destructive_configs_generated": 1,
}
_SCHEMA_READY_ENV = "CRATEDIGGER_TEST_SCHEMA_READY"
_DISCOVERY_DSN = "postgresql:///cratedigger_fuzz_discovery_only"

#: Ranked depth-report lines printed per section before truncation.
DEPTH_REPORT_LIMIT = 20

#: Discard rate at which a property's ``assume()`` cost is worth naming. Not a
#: defect: ``assume`` marks a world invalid and Hypothesis refills the budget,
#: which is exactly why it is the correct way to drop an unanswerable world.
DISCARD_RATE_THRESHOLD = 0.10
_DERANDOMIZE_SETTING = "derandomize"


def _settings_derandomize(configured: settings) -> bool:
    value = getattr(configured, _DERANDOMIZE_SETTING)
    if not isinstance(value, bool):
        raise TypeError("Hypothesis derandomize setting must be boolean")
    return value


class FuzzPropertyManifest(msgspec.Struct, frozen=True):
    """One property and the effective budget selected during discovery."""

    test_id: str
    max_examples: int
    uses_default_settings: bool
    entropy_shardable: bool = True
    finite_domain_cardinality: int | None = None


class FuzzModuleManifest(msgspec.Struct, frozen=True):
    """Exact tests and Hypothesis properties discovered in one module."""

    module_name: str
    test_ids: tuple[str, ...]
    hypothesis_tests: tuple[FuzzPropertyManifest, ...]
    uses_ephemeral_postgres: bool = False


@dataclass(frozen=True)
class FuzzTarget:
    """One independently runnable fuzz queue target."""

    label: str
    module_name: str
    load_names: tuple[str, ...]
    expected_test_ids: tuple[str, ...]
    shard_index: int = 0
    shard_count: int = 1
    max_examples_override: int | None = None
    uses_ephemeral_postgres: bool = False


@dataclass(frozen=True)
class FuzzRunResult:
    """One completed target and its tmpfs log."""

    target: FuzzTarget
    successful: bool
    tests_run: int
    elapsed_seconds: float
    log_path: Path
    hypothesis_stats: tuple[HypothesisPropertyStats, ...] = ()


@dataclass(frozen=True)
class PropertyDepth:
    """One property's generated depth, aggregated across its entropy shards.

    ``distinct_world_bound`` is the largest world count any single shard
    reached. Shards re-explore the same strategy space with different entropy,
    so summing them would overcount distinct worlds; the maximum is the honest
    bound on how much of its space the property can reach.

    ``shard_budget_bound`` is the largest budget any single child ran under.
    That, not ``budget``, is the number Hypothesis compared the worlds
    against, and it is therefore the report's detection ceiling.
    """

    test_id: str
    budget: int
    shard_budget_bound: int
    shards: int
    exhausted_shards: int
    distinct_world_bound: int
    valid: int
    invalid: int
    overrun: int
    interesting: int


@dataclass(frozen=True)
class FuzzInfrastructureFailure:
    """A target that failed outside unittest's result boundary."""

    target: FuzzTarget
    detail: str
    log_path: Path


@dataclass(frozen=True)
class FuzzTargetBatch:
    """Completed target outcomes plus work withheld after infrastructure loss."""

    results: tuple[FuzzRunResult, ...]
    infrastructure_failures: tuple[FuzzInfrastructureFailure, ...]
    not_started: tuple[FuzzTarget, ...]
    #: Set when the coordinator's own mid-run headroom check (issue #1156
    #: item 3) tripped, rather than any individual target — a coordinator-
    #: level abort, not a per-target infrastructure failure, so it is kept
    #: out of ``infrastructure_failures``.
    headroom_exhausted: bool = False


class PersistedFuzzTarget(msgspec.Struct, frozen=True):
    """Failure-artifact mapping from a log file to its exact target."""

    log_name: str
    label: str
    load_names: tuple[str, ...]
    expected_test_ids: tuple[str, ...]
    shard_index: int
    shard_count: int
    max_examples_override: int | None
    uses_ephemeral_postgres: bool


class PersistedFuzzManifest(msgspec.Struct, frozen=True):
    """Complete target map copied beside retained failure logs."""

    targets: tuple[PersistedFuzzTarget, ...]


def _build_fuzz_pin_targets(
    manifest: FuzzModuleManifest,
    pin_ids: Sequence[str],
) -> tuple[FuzzTarget, ...]:
    """Reuse the deterministic runner's audited hotspot sharding for pins."""
    granularity = HOTSPOT_SHARD_POLICIES.get(manifest.module_name)
    if granularity is None:
        return (
            FuzzTarget(
                label=f"{manifest.module_name}::pins",
                module_name=manifest.module_name,
                load_names=(manifest.module_name,),
                expected_test_ids=tuple(pin_ids),
                uses_ephemeral_postgres=manifest.uses_ephemeral_postgres,
            ),
        )
    module = TestModule(
        name=manifest.module_name,
        path=Path(),
        weight=1,
    )
    return tuple(
        FuzzTarget(
            label=target.test_name,
            module_name=manifest.module_name,
            load_names=target.load_names,
            expected_test_ids=target.expected_test_ids,
            uses_ephemeral_postgres=manifest.uses_ephemeral_postgres,
        )
        for target in shard_test_ids(
            module,
            pin_ids,
            granularity=granularity,
        )
    )


def _property_shard_count(
    manifest: FuzzModuleManifest,
    item: FuzzPropertyManifest,
) -> int:
    if item.finite_domain_cardinality is not None or not item.entropy_shardable:
        return 1
    return min(
        item.max_examples,
        FUZZ_PROPERTY_SHARD_LIMITS.get(manifest.module_name, item.max_examples),
    )


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
            manifest.module_name not in HOTSPOT_SHARD_POLICIES,
            -len(manifest.hypothesis_tests),
            -len(manifest.test_ids),
            manifest.module_name,
        ),
    )
    for manifest in ordered_manifests:
        if not manifest.test_ids:
            continue
        for item in manifest.hypothesis_tests:
            finite_cardinality = item.finite_domain_cardinality
            if finite_cardinality is not None and (
                finite_cardinality < 1
                or item.max_examples != finite_cardinality
            ):
                raise ValueError(
                    f"finite domain cardinality for {item.test_id} is "
                    f"{finite_cardinality}, but its budget is "
                    f"{item.max_examples}"
                )
        hypothesis_ids = {
            item.test_id for item in manifest.hypothesis_tests
        }
        isolate_properties = (
            len(manifest.hypothesis_tests) > 1
            or any(
                min(
                    property_shards,
                    _property_shard_count(manifest, item),
                ) > 1
                for item in manifest.hypothesis_tests
            )
            or manifest.module_name in HOTSPOT_SHARD_POLICIES
        )
        if not isolate_properties:
            targets.append(
                FuzzTarget(
                    label=manifest.module_name,
                    module_name=manifest.module_name,
                    load_names=(manifest.module_name,),
                    expected_test_ids=manifest.test_ids,
                    uses_ephemeral_postgres=(
                        manifest.uses_ephemeral_postgres
                    ),
                )
            )
            continue

        for item in manifest.hypothesis_tests:
            if item.max_examples < 1:
                raise ValueError(
                    f"invalid Hypothesis budget for {item.test_id}: "
                    f"{item.max_examples}"
                )
            shard_count = min(
                property_shards,
                _property_shard_count(manifest, item),
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
                        max_examples_override=(
                            budget if shard_count > 1 else None
                        ),
                        uses_ephemeral_postgres=(
                            manifest.uses_ephemeral_postgres
                        ),
                    )
                )
        pin_ids = tuple(
            test_id
            for test_id in manifest.test_ids
            if test_id not in hypothesis_ids
        )
        if pin_ids:
            targets.extend(_build_fuzz_pin_targets(manifest, pin_ids))

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
        if item.finite_domain_cardinality is not None:
            if item.max_examples != item.finite_domain_cardinality:
                raise ValueError(
                    f"finite domain cardinality changed: {test_id}"
                )
            if shard_count != 1 or len(scheduled) != 1:
                raise ValueError(f"finite fuzz property was sharded: {test_id}")
            if scheduled[0].max_examples_override is not None:
                raise ValueError(
                    f"finite fuzz property received a budget override: {test_id}"
                )
            continue
        if shard_count != len(scheduled):
            raise ValueError(f"missing fuzz property shard: {test_id}")
        if {target.shard_index for target in scheduled} != set(
            range(shard_count)
        ):
            raise ValueError(f"invalid fuzz property shard index: {test_id}")
        if shard_count == 1:
            if scheduled[0].max_examples_override is not None:
                raise ValueError(f"unexpected fuzz property budget: {test_id}")
            continue
        resource_limit = FUZZ_PROPERTY_SHARD_LIMITS.get(
            scheduled[0].module_name,
        )
        if resource_limit is not None and shard_count > resource_limit:
            raise ValueError(
                f"fuzz property exceeds resource shard limit: {test_id}"
            )
        if not item.entropy_shardable:
            raise ValueError(f"non-randomized fuzz property was sharded: {test_id}")
        shard_budgets = [
            target.max_examples_override for target in scheduled
        ]
        if any(budget is None or budget < 1 for budget in shard_budgets):
            raise ValueError(f"invalid fuzz property budget: {test_id}")
        if sum(budget or 0 for budget in shard_budgets) != item.max_examples:
            raise ValueError(f"changed fuzz property budget: {test_id}")


def aggregate_property_depth(
    records: Sequence[HypothesisPropertyStats],
) -> tuple[PropertyDepth, ...]:
    """Fold every entropy shard's statistics into one row per property.

    A default-budget property runs as up to eight children of
    ``budget / shards`` examples each. Folding does NOT change which
    properties are flagged — a shard that stops at its own budget reports
    ``settings.max_examples=...``, never exhaustion, so an unfolded verdict
    picks out the same set (measured on a real 8-shard run: 185 shard records,
    24 properties, same three flagged either way). It changes the report:
    one row instead of eight identical ones eating the ranked list, the
    property's real total budget, and one distinct-world bound (the maximum
    shard, since shards re-explore the same space and summing would
    overcount).
    """
    grouped: dict[str, list[HypothesisPropertyStats]] = {}
    for record in records:
        grouped.setdefault(record.test_id, []).append(record)
    return tuple(
        sorted(
            (
                PropertyDepth(
                    test_id=test_id,
                    budget=sum(shard.max_examples for shard in shards),
                    shard_budget_bound=max(
                        shard.max_examples for shard in shards
                    ),
                    shards=len(shards),
                    exhausted_shards=sum(
                        shard.stopped_because == STRATEGY_SPACE_EXHAUSTED
                        for shard in shards
                    ),
                    distinct_world_bound=max(shard.valid for shard in shards),
                    valid=sum(shard.valid for shard in shards),
                    invalid=sum(shard.invalid for shard in shards),
                    overrun=sum(shard.overrun for shard in shards),
                    interesting=sum(shard.interesting for shard in shards),
                )
                for test_id, shards in grouped.items()
            ),
            key=lambda depth: depth.test_id,
        )
    )


def attempted_examples(depth: PropertyDepth) -> int:
    """Every example Hypothesis actually spent on this property."""
    return depth.valid + depth.invalid + depth.overrun


def discard_rate(depth: PropertyDepth) -> float:
    """Share of spent examples that ``assume()`` (or a filter) threw away."""
    attempted = attempted_examples(depth)
    if attempted == 0:
        return 0.0
    return depth.invalid / attempted


def is_structurally_shallow(depth: PropertyDepth) -> bool:
    """True when a property ran out of worlds long before it ran out of budget.

    The disclosure #888 item 1 exists for: ``@given(a=st.booleans(),
    b=st.booleans())`` is exhausted in four worlds regardless of whether the
    burst budget is the ordinary 500 examples or the overnight 20,000, so a
    mutant outside those four worlds survives however deep the burst runs.

    **Detection ceiling.** Only a space smaller than ``shard_budget_bound``
    can be observed at all — 150 at the deterministic tier, or ``budget /
    shards`` at the fuzz tier (62 or 63 on a 30-core host by default; 2,500
    in the overnight run). A property with more worlds than that never
    exhausts, so it is reported as deep whatever its real space is, and an
    empty SHALLOW section means "none found below the ceiling", never "no
    shallow properties".

    Two carve-outs keep the verdict honest:

    * a run that reached an ``interesting`` case stopped early BECAUSE it
      found what it was looking for. This is defensive: no repository
      property produces interesting cases today, because the known-bad
      self-tests run their planted property in an inner ``@given`` whose
      statistics ``HypothesisStatsRecorder`` drops. It exists so a future
      property that does report them is not mislabelled;
    * a property whose worlds reach its budget spent everything it was given,
      whatever the tier's budget happens to be.

    It reports; it never gates. A small strategy space can be exactly right
    (the 36-world force-import authority property is correct), and a
    ``return``-based discard is invisible here by construction — a bare
    ``return`` spends the example as a PASS, so it counts as a valid world.
    """
    if depth.interesting:
        return False
    if depth.exhausted_shards != depth.shards:
        return False
    return depth.distinct_world_bound < depth.budget


def _shard_count(shards: int) -> str:
    return f"{shards} shard" if shards == 1 else f"{shards} shards"


def format_depth_report(depths: Sequence[PropertyDepth]) -> tuple[str, ...]:
    """Render the ranked per-property depth disclosure for one burst."""
    if not depths:
        return ()
    shallow = sorted(
        (depth for depth in depths if is_structurally_shallow(depth)),
        key=lambda depth: (depth.distinct_world_bound, depth.test_id),
    )
    discarding = sorted(
        (
            depth
            for depth in depths
            if discard_rate(depth) >= DISCARD_RATE_THRESHOLD
        ),
        key=lambda depth: (-discard_rate(depth), depth.test_id),
    )
    lines = [
        (f"DEPTH {len(depths)} properties measured, "
        f"{len(shallow)} shallow (space exhausted below budget), "
        f"{len(discarding)} discarding at least "
        f"{DISCARD_RATE_THRESHOLD:.0%} of their examples")
    ]
    for depth in shallow[:DEPTH_REPORT_LIMIT]:
        lines.append(
            f"SHALLOW {depth.distinct_world_bound} worlds vs "
            f"{depth.shard_budget_bound} examples per shard "
            f"({_shard_count(depth.shards)}, {depth.budget} total) "
            f"{depth.test_id}"
        )
    if len(shallow) > DEPTH_REPORT_LIMIT:
        lines.append(
            f"SHALLOW ... {len(shallow) - DEPTH_REPORT_LIMIT} more shallow"
        )
    for depth in discarding[:DEPTH_REPORT_LIMIT]:
        lines.append(
            f"DISCARD {discard_rate(depth):.0%} of {attempted_examples(depth)} "
            f"examples ({depth.invalid} discarded, {depth.valid} worlds) "
            f"{depth.test_id}"
        )
    if len(discarding) > DEPTH_REPORT_LIMIT:
        lines.append(
            f"DISCARD ... {len(discarding) - DEPTH_REPORT_LIMIT} more discarding"
        )
    return tuple(lines)


def _discover_module_child(module_name: str, result_path: Path) -> int:
    suite = unittest.defaultTestLoader.loadTestsFromName(module_name)
    cases = tuple(_iter_test_cases(suite))
    test_ids = tuple(test.id() for test in cases)
    hypothesis_tests: list[FuzzPropertyManifest] = []
    default_settings = settings.default
    if default_settings is None:
        raise RuntimeError("Hypothesis has no active default settings")
    default_max_examples = settings_max_examples(default_settings)
    # One owner for the deadline contract, shared with the deterministic
    # suite runner so neither tier can drift from the other (#882 B1b).
    assert_hypothesis_deadlines_disabled(suite)
    for test in cases:
        resolved = resolve_hypothesis_settings(test)
        if resolved is None:
            continue
        configured = resolved.configured
        uses_default_settings = (
            settings_max_examples(configured) == default_max_examples
            if resolved.from_state_machine_class
            else configured is default_settings
        )
        configured_max_examples = settings_max_examples(configured)
        method = _test_method(test)
        if method is None:
            raise RuntimeError(f"could not resolve fuzz test method: {test.id()}")
        raw_finite_spec = getattr(method, FINITE_DOMAIN_ATTRIBUTE, None)
        if raw_finite_spec is not None and not isinstance(
            raw_finite_spec,
            FiniteDomainSpec,
        ):
            raise TypeError(f"invalid finite-domain metadata: {test.id()}")
        if raw_finite_spec is not None:
            raw_finite_spec.verify()
        derandomize = _settings_derandomize(configured)
        hypothesis_tests.append(
            FuzzPropertyManifest(
                test_id=test.id(),
                max_examples=configured_max_examples,
                uses_default_settings=uses_default_settings,
                entropy_shardable=not derandomize,
                finite_domain_cardinality=(
                    raw_finite_spec.cardinality
                    if raw_finite_spec is not None
                    else None
                ),
            )
        )
    result_path.write_bytes(
        msgspec.json.encode(
            FuzzModuleManifest(
                module_name=module_name,
                test_ids=test_ids,
                hypothesis_tests=tuple(hypothesis_tests),
                uses_ephemeral_postgres=(
                    "conftest" in sys.modules
                    or "tests.conftest" in sys.modules
                ),
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
    discovery_environment = dict(environment)
    discovery_environment.update(
        {
            "TEST_DB_DSN": _DISCOVERY_DSN,
            _SCHEMA_READY_ENV: "1",
        }
    )
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
            env=discovery_environment,
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
    command = [
        sys.executable,
        str(TARGET_RUNNER),
        "--_run-target",
        msgspec.json.encode(target.load_names).decode(),
        str(DEFAULT_DURATIONS),
        str(result_path),
        msgspec.json.encode(target.expected_test_ids).decode(),
    ]
    if target.max_examples_override is not None:
        command.append(str(target.max_examples_override))
    with log_path.open("wb") as raw_output:
        completed = subprocess.run(
            command,
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
    if child.infrastructure_errors:
        detail = "\n".join(
            f"{error.test_id}: {error.kind}: {error.detail}"
            for error in child.infrastructure_errors
        )
        return FuzzInfrastructureFailure(
            target=target,
            detail=detail,
            log_path=log_path,
        )
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
        hypothesis_stats=child.hypothesis_stats,
    )


def assert_fuzz_admission(
    pending: Sequence[FuzzTarget],
    active: Sequence[FuzzTarget],
    admitted_indexes: Sequence[int],
    *,
    worker_count: int,
    postgres_worker_count: int,
) -> None:
    """Prove one queue admission fills safe worker and PostgreSQL capacity."""
    if worker_count < 1 or postgres_worker_count < 1:
        raise ValueError("worker limits must be at least 1")
    if len(active) > worker_count:
        raise ValueError("active targets exceed worker capacity")
    active_postgres = sum(
        target.uses_ephemeral_postgres for target in active
    )
    if active_postgres > postgres_worker_count:
        raise ValueError("active targets exceed PostgreSQL capacity")
    if len(set(admitted_indexes)) != len(admitted_indexes):
        raise AssertionError("duplicate fuzz admission")
    if any(index < 0 or index >= len(pending) for index in admitted_indexes):
        raise AssertionError("fuzz admission index is out of range")

    admitted = tuple(pending[index] for index in admitted_indexes)
    if len(active) + len(admitted) > worker_count:
        raise AssertionError("fuzz admission exceeds worker capacity")
    admitted_postgres = sum(
        target.uses_ephemeral_postgres for target in admitted
    )
    if active_postgres + admitted_postgres > postgres_worker_count:
        raise AssertionError("fuzz admission exceeds PostgreSQL capacity")
    required_postgres = min(
        postgres_worker_count - active_postgres,
        worker_count - len(active),
        sum(target.uses_ephemeral_postgres for target in pending),
    )
    if admitted_postgres < required_postgres:
        raise AssertionError("fuzz admission left PostgreSQL lane idle")

    admitted_set = set(admitted_indexes)
    remaining = tuple(
        target
        for index, target in enumerate(pending)
        if index not in admitted_set
    )
    total_slots = worker_count - len(active) - len(admitted)
    postgres_slots = (
        postgres_worker_count - active_postgres - admitted_postgres
    )
    has_eligible_remaining = any(
        not target.uses_ephemeral_postgres or postgres_slots > 0
        for target in remaining
    )
    if total_slots > 0 and has_eligible_remaining:
        raise AssertionError("fuzz admission left eligible worker capacity idle")


def select_fuzz_admissions(
    pending: Sequence[FuzzTarget],
    active: Sequence[FuzzTarget],
    *,
    worker_count: int,
    postgres_worker_count: int,
) -> tuple[int, ...]:
    """Select a work-conserving queue prefix under a separate PG ceiling."""
    total_slots = worker_count - len(active)
    postgres_slots = postgres_worker_count - sum(
        target.uses_ephemeral_postgres for target in active
    )
    admitted: list[int] = []
    for index, target in enumerate(pending):
        if postgres_slots < 1 or len(admitted) >= total_slots:
            break
        if target.uses_ephemeral_postgres:
            admitted.append(index)
            postgres_slots -= 1
    admitted_set = set(admitted)
    for index, target in enumerate(pending):
        if len(admitted) >= total_slots:
            break
        if index in admitted_set:
            continue
        if target.uses_ephemeral_postgres:
            if postgres_slots < 1:
                continue
            postgres_slots -= 1
        admitted.append(index)
    selected = tuple(admitted)
    assert_fuzz_admission(
        pending,
        active,
        selected,
        worker_count=worker_count,
        postgres_worker_count=postgres_worker_count,
    )
    return selected


def run_fuzz_targets(
    targets: Sequence[FuzzTarget],
    *,
    worker_count: int,
    postgres_worker_count: int,
    environment: Mapping[str, str],
    log_directory: Path,
    check_headroom: Callable[[], None],
) -> FuzzTargetBatch:
    """Run a bounded queue and stop admission after infrastructure loss.

    ``check_headroom`` is called once per admission cycle, right before any
    new target would be admitted (issue #1156 item 3): the production
    caller (``main`` below) binds it to the same
    ``_check_suite_headroom``/``headroom_floor_bytes`` precondition the
    coordinator already ran once, before any work, so a run admitted at
    floor+ε does not silently ENOSPC deep into the burst — it aborts here,
    under the same named identity, instead of surfacing as an opaque
    per-target subprocess crash. A ``RamRootExhaustedError`` stops
    admission exactly like an infrastructure failure (already-running
    targets are drained, nothing new starts), but is reported through
    ``FuzzTargetBatch.headroom_exhausted`` rather than
    ``infrastructure_failures`` — it is a coordinator-level abort, not an
    individual target's failure. No default: the real closure needs a
    resolved runtime path only ``main`` owns; tests inject a controlled
    fake.
    """
    results: list[FuzzRunResult] = []
    infrastructure_failures: list[FuzzInfrastructureFailure] = []
    pending = list(enumerate(targets))
    infrastructure_aborted = False
    headroom_exhausted = False
    completed_count = 0
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures: dict[
            Future[FuzzRunResult | FuzzInfrastructureFailure],
            tuple[int, FuzzTarget],
        ] = {}
        while futures or (pending and not infrastructure_aborted):
            if not infrastructure_aborted:
                try:
                    check_headroom()
                except RamRootExhaustedError as exc:
                    infrastructure_aborted = True
                    headroom_exhausted = True
                    print(f"RAM ROOT EXHAUSTED mid-run: {exc}", flush=True)
                else:
                    active = tuple(target for _index, target in futures.values())
                    admitted_positions = select_fuzz_admissions(
                        tuple(target for _index, target in pending),
                        active,
                        worker_count=worker_count,
                        postgres_worker_count=postgres_worker_count,
                    )
                    admitted_position_set = set(admitted_positions)
                    admitted = tuple(
                        item
                        for position, item in enumerate(pending)
                        if position in admitted_position_set
                    )
                    pending = [
                        item
                        for position, item in enumerate(pending)
                        if position not in admitted_position_set
                    ]
                    for index, target in admitted:
                        future = executor.submit(
                            _execute_fuzz_target,
                            index,
                            target,
                            environment=environment,
                            log_directory=log_directory,
                        )
                        futures[future] = (index, target)
            if not futures:
                break

            done, _not_done = wait(
                tuple(futures),
                return_when=FIRST_COMPLETED,
            )
            completed = sorted(
                (
                    (futures[future][0], future)
                    for future in done
                ),
                key=lambda item: item[0],
            )
            for _completed_index, future in completed:
                index, target = futures.pop(future)
                completed_count += 1
                try:
                    outcome = future.result()
                except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                    outcome = FuzzInfrastructureFailure(
                        target=target,
                        detail=f"{type(exc).__name__}: {exc}",
                        log_path=log_directory / f"{index:04d}.log",
                    )
                if isinstance(outcome, FuzzInfrastructureFailure):
                    infrastructure_failures.append(outcome)
                    infrastructure_aborted = True
                    print(
                        f"INFRASTRUCTURE FAIL {outcome.target.label}",
                        flush=True,
                    )
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

    return FuzzTargetBatch(
        results=tuple(results),
        infrastructure_failures=tuple(infrastructure_failures),
        not_started=tuple(target for _index, target in pending),
        headroom_exhausted=headroom_exhausted,
    )


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
    environment.pop("TEST_DB_DSN", None)
    environment.pop(_SCHEMA_READY_ENV, None)
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
                max_examples_override=target.max_examples_override,
                uses_ephemeral_postgres=target.uses_ephemeral_postgres,
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


#: Hard ceiling stacked on top of the doubling formula below (issue #1156
#: item 1). Mixed subprocess/I/O fuzz targets tolerate CPU oversubscription
#: far better than the deterministic suite's in-process workers, so
#: ``recommended_worker_count``'s audited three-quarters-of-cores contract
#: does not apply here — but nothing bounded the DOUBLING itself, so an
#: arbitrarily large host could mint an arbitrarily large number of
#: concurrent fuzz-target subprocesses, each importing its own Python
#: interpreter and Hypothesis, and (for ephemeral-postgres targets,
#: separately capped at ``EPHEMERAL_POSTGRES_TARGET_LIMIT``) test
#: fixtures. Mirrors ``run_world_model_burst.py::IN_PROCESS_JOB_CAP``'s
#: same shape — a flat ceiling stacked on top of a per-core formula — for
#: the same reason: an unattended overnight/daily run has no operator
#: watching worker counts climb. Chosen comfortably above every host
#: measured in this repository's fleet to date (doc1 at 30 cores computes
#: 60, well under this ceiling, so it never engages on hardware seen so
#: far) — this exists for the next, larger host, not to shrink today's
#: number.
MAX_FUZZ_JOBS = 64


def recommended_fuzz_jobs(cpu_count: int) -> int:
    """Keep mixed subprocess/I/O targets runnable at twice host cores, bounded."""
    if cpu_count < 1:
        raise ValueError("cpu_count must be at least 1")
    return min(cpu_count * 2, MAX_FUZZ_JOBS)


def recommended_postgres_jobs(cpu_count: int, worker_count: int) -> int:
    """Keep a bounded four-fifths-host PG lane busy through target turnover."""
    if cpu_count < 1:
        raise ValueError("cpu_count must be at least 1")
    if worker_count < 1:
        raise ValueError("worker_count must be at least 1")
    return min(
        worker_count,
        EPHEMERAL_POSTGRES_TARGET_LIMIT,
        max(1, (cpu_count * 4 + 4) // 5),
    )


def _default_jobs() -> int:
    configured = os.environ.get("CRATEDIGGER_FUZZ_JOBS")
    if configured is not None:
        return _parse_positive_int(configured)
    return recommended_fuzz_jobs(os.cpu_count() or 1)


def _default_postgres_jobs() -> int:
    configured = os.environ.get("CRATEDIGGER_FUZZ_POSTGRES_JOBS")
    if configured is not None:
        return _parse_positive_int(configured)
    return recommended_postgres_jobs(
        os.cpu_count() or 1,
        _default_jobs(),
    )


def property_profile_max_examples(
    manifests: Sequence[FuzzModuleManifest],
) -> int | None:
    """Return the one effective default-property budget from discovery."""
    budgets = {
        item.max_examples
        for manifest in manifests
        for item in manifest.hypothesis_tests
        if (
            item.uses_default_settings
            and item.finite_domain_cardinality is None
        )
    }
    if len(budgets) > 1:
        raise ValueError(
            f"inconsistent default property budgets: {sorted(budgets)}"
        )
    return next(iter(budgets), None)


def recommended_property_shards(
    cpu_count: int,
    profile_max_examples: int,
) -> int:
    """Keep enough examples in each child to repay process startup."""
    if cpu_count < 1:
        raise ValueError("cpu_count must be at least 1")
    if profile_max_examples < 1:
        raise ValueError("profile_max_examples must be at least 1")
    host_limit = min(8, max(1, (cpu_count + 3) // 4))
    budget_limit = max(
        1,
        profile_max_examples // MIN_EXAMPLES_PER_ENTROPY_SHARD,
    )
    return min(host_limit, budget_limit)


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
        "--postgres-jobs",
        type=_parse_positive_int,
        default=_default_postgres_jobs(),
    )
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


def main(
    argv: Sequence[str] | None = None,
    *,
    check_headroom: Callable[[], None] | None = None,
) -> int:
    """Run the fuzz burst.

    ``check_headroom`` mirrors ``run_world_model_burst.py::main``'s own DI
    seam (issue #1156 item 3): ``None`` (the production default) builds the
    real ``_check_suite_headroom``/``headroom_floor_bytes`` closure below;
    tests inject a controlled fake so BOTH the preflight call and the
    SAME callable threaded into ``run_fuzz_targets`` for the mid-run
    admission-loop check can be driven deterministically end-to-end,
    including through this function's own post-run reporting.
    """
    args = _parser().parse_args(argv)
    module_names = tuple(args.modules) or _default_modules()
    if not module_names:
        print("No generated fuzz modules found", file=sys.stderr)
        return 2
    worker_count = min(args.jobs, max(1, len(module_names)))

    if check_headroom is None:
        runtime = private_runtime_dir()
        # worker_count=1 deliberately, not args.jobs: see
        # headroom_floor_bytes's docstring (issue #1156 item 3) for why
        # the fuzz burst does not extend the suite's per-worker multiplier
        # to itself.
        headroom_minimum = headroom_floor_bytes(1)
        check_headroom = lambda: _check_suite_headroom(
            runtime, minimum_bytes=headroom_minimum
        )
    try:
        check_headroom()
    except RamRootExhaustedError as exc:
        print(f"fuzz burst: {exc}", file=sys.stderr)
        return TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE
    except (OSError, RuntimeError, ValueError) as exc:
        print(
            f"fuzz burst: infrastructure precondition failed: {exc}",
            file=sys.stderr,
        )
        return 2

    persistent_database = _persistent_path(
        os.environ.get("HYPOTHESIS_STORAGE_DIRECTORY"),
        REPO_ROOT / ".hypothesis",
    )
    persistent_output_value = os.environ.get("CRATEDIGGER_FUZZ_OUTPUT_DIR")
    property_shards = args.property_shards
    if args.profile != DEFAULT_PROFILE:
        if property_shards is None:
            property_shards = 1
        elif property_shards != 1:
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
        if property_shards is None:
            profile_max_examples = property_profile_max_examples(manifests)
            property_shards = (
                recommended_property_shards(
                    os.cpu_count() or 1,
                    profile_max_examples,
                )
                if profile_max_examples is not None
                else 1
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
            f"up to {worker_count} parallel, "
            f"up to {min(args.postgres_jobs, worker_count)} "
            "PostgreSQL-backed "
            f"({os.cpu_count() or 1} host cores), profile={args.profile}",
            flush=True,
        )
        started_at = time.monotonic()
        batch = run_fuzz_targets(
            targets,
            worker_count=worker_count,
            postgres_worker_count=min(
                args.postgres_jobs,
                worker_count,
            ),
            environment=child_environment,
            log_directory=log_directory,
            check_headroom=check_headroom,
        )
        results = batch.results
        infrastructure_failures = batch.infrastructure_failures
        wall_seconds = time.monotonic() - started_at

        # NOTE(#1156 item 3): batch.headroom_exhausted and
        # infrastructure_failures are NOT mutually exclusive -- a target
        # already admitted before the mid-run headroom check tripped can
        # ALSO fail independently (e.g. its own worker crash) in the same
        # drain. Both are folded into the SAME reporting block below so
        # neither swallows the other's detail, mirroring
        # _collapse_disk_full_failures's own "two separate entries" rule.
        failed_results = [result for result in results if not result.successful]
        if not infrastructure_failures and not batch.headroom_exhausted:
            for result in sorted(
                results,
                key=lambda item: item.elapsed_seconds,
                reverse=True,
            )[:12]:
                print(
                    f"SLOW {result.elapsed_seconds:.1f}s {result.target.label}"
                )
            for line in format_depth_report(
                aggregate_property_depth(
                    tuple(
                        record
                        for result in results
                        for record in result.hypothesis_stats
                    )
                )
            ):
                print(line)
        if failed_results or infrastructure_failures or batch.headroom_exhausted:
            if infrastructure_failures or batch.headroom_exhausted:
                if failed_results:
                    print(
                        f"\n{len(failed_results)} unittest-failed "
                        "target withheld from property reporting because "
                        "the burst's infrastructure failed."
                    )
            else:
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
            if batch.headroom_exhausted:
                print(f"\n--- {TEST_RAM_ROOT_EXHAUSTED} (mid-run) ---")
                print(
                    "the coordinator's own headroom precondition tripped "
                    "during admission; already-running targets were "
                    "drained and no new target was started"
                )
            if not infrastructure_failures and not batch.headroom_exhausted:
                _persist_failure_database(
                    active_database,
                    persistent_database,
                )
            if persistent_output_value is not None:
                retained = _persist_failure_logs(
                    log_directory,
                    _persistent_path(persistent_output_value, REPO_ROOT),
                    targets,
                )
                print(f"fuzz burst: complete module logs retained at {retained}")
            completed_targets = len(results) + len(infrastructure_failures)
            if batch.headroom_exhausted:
                print(
                    f"fuzz burst: {TEST_RAM_ROOT_EXHAUSTED} mid-run after "
                    f"{wall_seconds:.1f}s ({completed_targets} completed of "
                    f"{len(targets)}; {len(batch.not_started)} not started; "
                    "property verdict invalid)"
                )
                return TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE
            if infrastructure_failures:
                print(
                    f"fuzz burst: INFRASTRUCTURE ABORT after "
                    f"{wall_seconds:.1f}s ({completed_targets} completed of "
                    f"{len(targets)}; {len(batch.not_started)} not started; "
                    "property verdict invalid)"
                )
            else:
                print(
                    f"fuzz burst: FAILURES after {wall_seconds:.1f}s "
                    f"({len(failed_results)} failed targets; "
                    f"{len(results)} completed)"
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
