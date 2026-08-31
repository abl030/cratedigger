#!/usr/bin/env python3
"""Run the complete Python test suite across isolated unittest workers."""

from __future__ import annotations

import argparse
import errno
import io
import math
import multiprocessing
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, replace
from pathlib import Path
from types import TracebackType
from typing import Literal, TypeIs

import msgspec
from hypothesis import is_hypothesis_test, settings
from hypothesis.statistics import collector
from psycopg2 import Error as PsycopgError
from psycopg2 import OperationalError
from psycopg2.errors import DiskFull

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lib.json_narrow import json_dict, json_list
from scripts.run_test_suite import (
    FAILURE_MARKER_PREFIX,
    METRICS_MARKER_PREFIX,
    CheckFailureMarker,
    CheckMetricsMarker,
)
from scripts.test_substrate import (
    TEST_RAM_ROOT_EXHAUSTED,
    default_min_headroom_bytes,
    private_runtime_dir,
    recommended_worker_count,
)

ExcInfo = (
    tuple[type[BaseException], BaseException, TracebackType]
    | tuple[None, None, None]
)

DEFAULT_DURATIONS = 15
_FAILURE_MARKER = "=" * 70
_SCHEMA_READY_ENV = "CRATEDIGGER_TEST_SCHEMA_READY"

#: Distinct from the ordinary "1" failure exit code (issue #1111 item 2):
#: every failure this run reported turned out to be the shared test RAM root
#: running out, not a real test defect. scripts/run_test_suite.py's generic
#: phase-state derivation treats any exit code outside a phase's declared
#: `failure_exit_codes` (just `(1,)` for the "python" phase) as an
#: infrastructure failure, so returning this code alone is enough to promote
#: the phase — and the whole suite — out of an ordinary "failed" state,
#: without touching that generic logic.
TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE = 4

#: Issue #1156 item 2: a worker killed by the OOM killer raises the SAME
#: BrokenProcessPool shape this module already catches for every
#: outstanding future in a broken ProcessPoolExecutor, but the disk-full
#: classifier above measures the wrong resource for it (tmpfs, not system
#: memory) — so it always fell through as disk_full=False and reported as
#: N ordinary worker failures, each named after a different innocent test
#: (the same N-disguises shape issue #1111 fixed for disk-full). This is a
#: DIFFERENT resource than the tmpfs RAM root, so it gets its OWN identity
#: and exit code rather than being folded into TEST_RAM_ROOT_EXHAUSTED —
#: conflating the two would undo the honesty this classification exists
#: for.
TEST_HOST_MEMORY_EXHAUSTED = "test host memory exhausted"
TEST_HOST_MEMORY_EXHAUSTED_EXIT_CODE = 5
WORLD_MODEL_MODULE = "tests.world_model.state_machine"
# Method profiling showed the dominant Nix evaluation was otherwise admitted
# late enough to become the deterministic suite's tail.
#
# Issue #1226: ``schedule_modules`` below orders everything that is NOT
# frontloaded by ``_line_weight`` — a module's LINE COUNT — which is only a
# proxy for cost, and an actively misleading one for a small file that runs
# real subprocesses. Once #1226's shared-``nixpkgs`` fix removed
# ``test_nix_module`` as the phase's pole, the phase stopped being
# pole-bound and became throughput-bound, at which point queue ORDER is
# what costs wall time: a long target admitted late cannot be packed
# against anything. Measured on doc1 (30 cores, quiet, 22 workers) from a
# full per-target duration map of one real phase run, replayed through this
# same scheduler: perfect packing 76.0s, this queue order 88.1s, and
# longest-processing-time-first 76.2s — i.e. ~12s of the phase was pure
# ordering loss. The four modules added below are the ones the map showed
# were both expensive AND admitted late (measured target seconds, and their
# position in a 464-target queue): test_world_model_coordinator 38.9s at
# 256, test_fuzz_burst 33.6s at 211, test_targeted_test_selection 32.0s at
# 311, test_aac_lattice 30.9s at 273. None of them ends in ``_generated``,
# so nothing else pulls them forward. Replaying the map with exactly these
# four frontloaded predicts 81.5s (-6.3s); adding further modules past
# these four plateaus at ~81.4s and buys nothing, so the list stops here
# rather than growing to track every heavy module. Measured rather than
# only predicted: the real phase went 89.6s -> 82.6s on the same host,
# within 1.1s of the replay's prediction.
#
# This list is a hand-audited scheduling HINT, never a correctness
# boundary: a stale entry (a module that got cheap, or a new heavy one that
# is missing) costs a little wall time and nothing else. Do NOT grow it
# into a maintained per-target cost table — the remaining ~5s between the
# prediction above and the LPT bound is not worth 464 committed numbers
# that drift on every test change.
AUDITED_FRONTLOAD_MODULES = frozenset({
    "tests.test_nix_module",
    "tests.test_world_model_coordinator",
    "tests.test_fuzz_burst",
    "tests.test_targeted_test_selection",
    "tests.test_aac_lattice",
})
#: tests.test_nix_module (issue #1131) is NOT method_batch here on purpose.
#: Its nix-eval tests are cost-grouped into three ``functools``-free,
#: exception-memoizing cached helpers in the test module
#: (``_shared_module_worlds_web_auth_matrix_part1`` /
#: ``_shared_module_worlds_web_auth_matrix_part2`` /
#: ``_shared_module_worlds_rest``) — a cache only pays off when every one of
#: a given helper's consumers runs in the same worker PROCESS. method_batch
#: splits methods across separate processes by TEST COUNT, blind to cost,
#: and a naive full unshard (issue #1131's own round 1) serializes every
#: merged world onto ONE target instead. Measured (round 2 review, quiet
#: 30-core host): main's own method_batch worst case is 61.6s; a full
#: unshard is 118.3s (+92%); the original HOTSPOT_ISOLATED_METHODS
#: carve-out (one ``webAuthMatrix`` singleton + one ``_rest`` bundle)
#: restored a 2-target floor level with main (~61-65s), not better. The CPU
#: merging saves is modest — on the order of a few seconds, bounded by the
#: sub-1s shared preamble each eliminated `nix eval` call no longer pays,
#: NOT the gap between each world's SOLO measured time and one combined
#: run.
#:
#: Issue #1156 item 1 split the single ``webAuthMatrix`` singleton itself
#: into ``_part1``/``_part2`` (``missing``...``rootAccessGroup`` /
#: ``wheelAccessGroup``...``nginxRestartDisabled``), raising this module's
#: own concurrently-schedulable heavy-nix-eval target count
#: from two to THREE — exactly the axis the original 2-target design
#: capped, and exactly what the worker-count sweep below warns is
#: sensitive to concurrency. Measured before shipping it (three interleaved
#: baseline/candidate pairs of full ``run_tests.sh`` runs, ambient sibling
#: contention acknowledged and visible in the spread): the worst single
#: target this module contributes to a run dropped from a 100.1s/114.3s/
#: 103.3s baseline (mean 105.9s) to a 95.0s/96.4s/85.7s candidate (mean
#: 92.4s, -13%, every pair individually improved) with no runaway blowup —
#: neither new half ever exceeded ~62s even competing against two heavy
#: siblings instead of one. The suite-level python-phase wall time moved
#: from a mean of 118.6s to 113.8s (-4%, noisier than the per-target
#: number — baseline's own three runs alone spanned 107.5-137.0s from
#: ambient contention before this change touches anything). A real, if
#: modest, net win, not the dramatic headline number the solo floor alone
#: (58.5s mean down to ~30s per half) would suggest. See
#: ``_shared_module_worlds_web_auth_matrix_part1``'s own docstring in
#: ``tests/test_nix_module.py`` for the full numbers.
#:
#: A worker-count sweep on PRE-#1156-item-1 main showed this module's pole
#: inflating hard with concurrency (88.0s at 8 workers, 122.7s at 12,
#: 147.7s at 16, 152.3s at 20) — bounding this module's own concurrent
#: heavy-eval count is what makes raising the suite's worker count
#: affordable in the first place; that headroom is why item 1's move from
#: two to three concurrent heavy targets was safe to measure rather than
#: assumed unsafe outright.
#:
#: Issue #1226 ended this module's reign as the pole, and NOT by splitting
#: it again: it made each eval ~2.5x cheaper by sharing one nixpkgs
#: instance across the worlds inside it (details and the byte-identical
#: proof are in ``_shared_module_worlds_web_auth_matrix_part1``'s
#: docstring). Measured on a quiet 30-core doc1 at the default 22 workers,
#: this module's three targets went from 95.0s/65.6s/57.0s to
#: 38.9s/31.1s/24.8s, and the python phase from 98.6s to 89.6s. Two things
#: follow for anyone tuning this file next. First, the sweep above is now
#: historical: it measured a pole that no longer exists at that size, so do
#: not cite it as evidence about today's concurrency headroom without
#: re-measuring. Second, the phase is no longer POLE-bound at all — its
#: largest target (47.9s) is well under the perfect-packing floor
#: (1673 measured core-seconds / 22 workers = 76.0s), so splitting any
#: target further cannot move the phase. Raising the worker count cannot
#: either, and was measured: 26 workers inflated total core-seconds by 11%
#: (1673 -> 1860) for a 2% wall gain, because the host is already CPU-
#: saturated. What is left is total CPU work, and queue ORDER — see
#: ``AUDITED_FRONTLOAD_MODULES`` above.
HOTSPOT_SHARD_POLICIES = {
    "tests.test_beets_destructive_configs_generated": "method_batch",
    "tests.test_deploy_pin_generated": "method_batch",
    "tests.test_deploy_pin_script": "method_batch",
    "tests.test_pipeline_db": "class_batch",
}
HOTSPOT_CLASS_BATCHES = 8
HOTSPOT_METHOD_BATCHES = 12
#: Issue #1131 review round 2 (extended by #1156 item 1): exact test IDs
#: that must run as their OWN singleton target, isolated from the rest of
#: their module. Unlike HOTSPOT_SHARD_POLICIES (a generic, cost-BLIND split
#: balanced by test count), this is a manually audited, cost-AWARE
#: carve-out — the two named tests below are this module's most expensive
#: nix-eval consumers (issue #1156 item 1 split the original single
#: consumer in two), and each one's own cached helper (see
#: ``_shared_module_worlds_web_auth_matrix_part1`` /
#: ``_shared_module_worlds_web_auth_matrix_part2`` in
#: ``tests/test_nix_module.py``) is scoped so its target pays for exactly
#: the nix eval it needs, never a bundled neighbour's. A module named here
#: with no ``HOTSPOT_SHARD_POLICIES`` entry bundles every OTHER discovered
#: test into one remainder target (see ``hotspot_targets``).
HOTSPOT_ISOLATED_METHODS: Mapping[str, frozenset[str]] = {
    "tests.test_nix_module": frozenset({
        (
            "tests.test_nix_module.TestWebAuthenticationModuleContract."
            "test_basic_and_insecure_mode_matrix_is_evaluated_part1"
        ),
        (
            "tests.test_nix_module.TestWebAuthenticationModuleContract."
            "test_basic_and_insecure_mode_matrix_is_evaluated_part2"
        ),
    }),
}

#: Hypothesis' ``stopped-because`` reason when a strategy space ran out of
#: distinct worlds before the example budget ran out
#: (``ExitReason.finished``). This is the signal that a property is
#: structurally incapable of spending its budget, however large that budget is.
STRATEGY_SPACE_EXHAUSTED = "nothing left to do"

#: The generate-phase case statuses Hypothesis reports, lower-cased. Pinned by
#: ``tests/test_parallel_test_runner.py`` against the live ``Status`` enum: a
#: new status would otherwise be silently uncounted in the depth report.
HYPOTHESIS_CASE_STATUSES = ("valid", "invalid", "overrun", "interesting")

# Once a disposable database fails below this floor, the surrounding test
# cannot produce a trustworthy property verdict even if ENOSPC was swallowed.
_MIN_VALID_TEMP_HEADROOM_BYTES = 64 * 1024 * 1024


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
    failed_test_ids: tuple[str, ...]
    infrastructure_errors: tuple[ChildInfrastructureError, ...] = ()


@dataclass(frozen=True)
class TargetInfrastructureFailure:
    """A target whose worker failed outside unittest's result boundary."""

    target: TestTarget
    detail: str
    disk_full: bool = False
    memory_exhausted: bool = False


class HypothesisPropertyStats(msgspec.Struct, frozen=True):
    """One Hypothesis property's generate-phase depth in one child process.

    ``max_examples`` is the budget that child actually ran under — the
    per-shard budget when ``scripts/run_fuzz_tests.py`` split a property
    across entropy shards, so the shards sum back to the property's total.
    """

    test_id: str
    max_examples: int
    valid: int
    invalid: int
    overrun: int
    interesting: int
    stopped_because: str


class ChildInfrastructureError(msgspec.Struct, frozen=True):
    """One infrastructure failure detected inside a unittest result."""

    test_id: str
    kind: Literal["disk_full", "database_unavailable"]
    detail: str


class ChildTargetResult(msgspec.Struct, frozen=True):
    """Wire result written by one fresh target interpreter."""

    successful: bool
    tests_run: int
    test_ids: tuple[str, ...]
    output: str
    failed_test_ids: tuple[str, ...]
    hypothesis_stats: tuple[HypothesisPropertyStats, ...] = ()
    infrastructure_errors: tuple[ChildInfrastructureError, ...] = ()


class ListedTestIds(msgspec.Struct, frozen=True):
    """Wire manifest returned by an isolated unittest discovery process."""

    test_ids: tuple[str, ...]


class RecordingTextTestResult(unittest.TextTestResult):
    """Text result that proves which exact unittest IDs executed."""

    test_ids: list[str] | None = None
    infrastructure_errors: list[ChildInfrastructureError] | None = None

    def startTest(self, test: unittest.TestCase) -> None:
        if self.test_ids is None:
            self.test_ids = []
        self.test_ids.append(test.id())
        super().startTest(test)

    def _record_infrastructure_error(
        self,
        test: unittest.TestCase,
        error: BaseException,
    ) -> None:
        infrastructure = _classify_test_infrastructure_error(error)
        if infrastructure is None:
            return
        kind, detail = infrastructure
        if self.infrastructure_errors is None:
            self.infrastructure_errors = []
        self.infrastructure_errors.append(
            ChildInfrastructureError(
                test_id=test.id(),
                kind=kind,
                detail=detail,
            )
        )

    def addError(
        self,
        test: unittest.TestCase,
        err: ExcInfo,
    ) -> None:
        if err[1] is not None:
            self._record_infrastructure_error(test, err[1])
        super().addError(test, err)

    def addFailure(
        self,
        test: unittest.TestCase,
        err: ExcInfo,
    ) -> None:
        if err[1] is not None:
            self._record_infrastructure_error(test, err[1])
        super().addFailure(test, err)

    def addSubTest(
        self,
        test: unittest.TestCase,
        subtest: unittest.TestCase,
        err: ExcInfo | None,
    ) -> None:
        if err is not None and err[1] is not None:
            self._record_infrastructure_error(subtest, err[1])
        super().addSubTest(test, subtest, err)


def _find_disk_full_exception(
    error: BaseException,
) -> BaseException | None:
    """Find ENOSPC through database and exception-wrapper boundaries."""
    for current in _walk_exception_chain(error):
        if isinstance(current, OSError) and current.errno == errno.ENOSPC:
            return current
        if isinstance(current, DiskFull):
            return current
        if isinstance(current, PsycopgError) and current.pgcode == "53100":
            return current
    return None


def _walk_exception_chain(error: BaseException) -> Iterator[BaseException]:
    """Yield one exception graph once across groups, causes, and contexts."""
    pending = [error]
    visited: set[int] = set()
    while pending:
        current = pending.pop()
        identity = id(current)
        if identity in visited:
            continue
        visited.add(identity)
        yield current
        if _is_exception_group(current):
            pending.extend(current.exceptions)
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)


def _is_exception_group(
    error: BaseException,
) -> TypeIs[ExceptionGroup[Exception]]:
    """Narrow stdlib's otherwise-unknown ExceptionGroup parameter."""
    return isinstance(error, ExceptionGroup)


def _measure_tempdir_available_bytes() -> int | None:
    """Free bytes on the shared test RAM root right now, or None if unreadable."""
    try:
        return shutil.disk_usage(tempfile.gettempdir()).free
    except OSError:
        return None


#: Deliberately conservative floor for the OOM classifier below (issue
#: #1156 item 2) — unlike the tmpfs floor, which run_suite's own
#: precondition already pins to a specific, worker-aware value, there is
#: no equivalent configured "expected system memory headroom" anywhere in
#: this repository; 256 MiB is chosen to sit comfortably below what any
#: live, unstressed host is expected to have free, minimizing the chance
#: an ordinary transient dip gets misclassified as exhaustion. See
#: ``_classify_target_infrastructure_failure``'s docstring for the
#: measurement-timing residual this floor cannot close.
_MIN_VALID_MEMORY_HEADROOM_BYTES = 256 * 1024 * 1024


def _measure_available_memory_bytes(
    meminfo_path: Path = Path("/proc/meminfo"),
) -> int | None:
    """Best-effort system memory availability right now, via /proc/meminfo.

    ``MemAvailable`` (not ``MemFree``) is the kernel's own estimate of what
    a new allocation could get without swapping — closer to what actually
    drives OOM-kill decisions than raw free pages, which undercounts
    reclaimable page cache. Linux-only, matching every other resource
    guard in this module; returns ``None`` when unreadable (missing file,
    unexpected format) so the caller degrades to "cannot classify" rather
    than guessing.

    ``meminfo_path`` defaults to the real file so every production caller
    is unaffected; it exists so a test can pin the ``MemAvailable`` line
    and its ``* 1024`` (kB -> bytes) conversion against a KNOWN fixture
    (issue #1156 review F4) — /proc/meminfo cannot otherwise be forced to
    a controlled value.
    """
    try:
        with meminfo_path.open(encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def _default_min_memory_headroom_bytes() -> int:
    """The OOM classifier's floor: env-overridable, mirroring
    ``CRATEDIGGER_TEST_RAM_MIN_BYTES``'s own override pattern for the
    tmpfs floor. This is also the seam that makes the classifier's
    positive case testable end-to-end: a test can set this to a value no
    real host's ``MemAvailable`` can exceed — the identical trick
    ``TestRunTargetsWorkerExceptionWiring``'s own docstring already
    documents for the disk-full floor.
    """
    raw = os.environ.get("CRATEDIGGER_TEST_MEMORY_MIN_BYTES")
    if raw is not None:
        try:
            value = int(raw)
        except ValueError:
            value = -1
        if value < 0:
            raise ValueError(
                "CRATEDIGGER_TEST_MEMORY_MIN_BYTES must be a non-negative integer"
            )
        return value
    return _MIN_VALID_MEMORY_HEADROOM_BYTES


def _classify_test_infrastructure_error(
    error: BaseException,
    *,
    available_temp_bytes: int | None = None,
) -> tuple[Literal["disk_full", "database_unavailable"], str] | None:
    """Classify capacity and disposable-database loss before JSON encoding."""
    disk_full = _find_disk_full_exception(error)
    if disk_full is not None:
        return "disk_full", f"{type(disk_full).__name__}: {disk_full}"
    available = (
        available_temp_bytes
        if available_temp_bytes is not None
        else _measure_tempdir_available_bytes()
    )
    if (
        available is not None
        and available < _MIN_VALID_TEMP_HEADROOM_BYTES
    ):
        detail = (
            f"temporary filesystem has {available} bytes free; "
            f"{type(error).__name__}: {error}"
        )
        return "disk_full", detail
    operational = next(
        (
            current
            for current in _walk_exception_chain(error)
            if isinstance(current, OperationalError)
        ),
        None,
    )
    if operational is None:
        return None
    return (
        "database_unavailable",
        f"{type(operational).__name__}: {operational}",
    )


class HypothesisStatsRecorder:
    """Attribute each Hypothesis statistics callback to its running test.

    Hypothesis reports one statistics mapping per property run through
    ``hypothesis.statistics.collector``, and that mapping names no test. The
    running test is therefore read from the one canonical record of what
    started — ``RecordingTextTestResult.startTest`` — which the child bridges
    into :meth:`start`.

    Only tests that ARE properties are recorded. A plain test whose body
    declares and calls its own ``@given`` function also emits statistics, and
    the running test is then the enclosing plain test — the repository's
    known-bad self-test shape. Absence from the budget map is exactly the
    "not a property test method" signal, so those runs are dropped: filing
    them under the enclosing test would inflate the property count and, when
    one body runs two inner properties, fold them into a single row whose
    shard count and world bound are both fiction.

    The recorder never raises and never fails a run: a burst depth report is a
    disclosure, not a gate. A callback with no running test, a test that is
    not a property, or an unreadable statistics shape degrades to a dropped
    record instead of breaking the suite that produced it.
    """

    def __init__(self, budgets: Mapping[str, int]) -> None:
        self._budgets = budgets
        self._current_test_id: str | None = None
        self._records: list[HypothesisPropertyStats] = []

    @property
    def records(self) -> tuple[HypothesisPropertyStats, ...]:
        """Every property's statistics, in the order Hypothesis reported them."""
        return tuple(self._records)

    def start(self, test_id: str) -> None:
        """Mark the test whose statistics arrive next."""
        self._current_test_id = test_id

    def note(self, statistics: Mapping[str, object]) -> None:
        """Record one property's generate-phase case statuses."""
        test_id = self._current_test_id
        if test_id is None or test_id not in self._budgets:
            return
        generate_phase = json_dict(statistics.get("generate-phase"))
        statuses = [
            json_dict(case).get("status")
            for case in json_list(generate_phase.get("test-cases"))
        ]
        counts = {
            status: statuses.count(status) for status in HYPOTHESIS_CASE_STATUSES
        }
        stopped_because = statistics.get("stopped-because")
        self._records.append(
            HypothesisPropertyStats(
                test_id=test_id,
                max_examples=self._budgets[test_id],
                valid=counts["valid"],
                invalid=counts["invalid"],
                overrun=counts["overrun"],
                interesting=counts["interesting"],
                stopped_because=(
                    stopped_because if isinstance(stopped_because, str) else ""
                ),
            )
        )


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
        module_name = ".".join(relative.parts)
        modules.append(
            TestModule(
                module_name,
                path,
                _line_weight(path),
                frontload=module_name in AUDITED_FRONTLOAD_MODULES,
            )
        )
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


def hotspot_targets(
    module: TestModule,
    test_ids: Sequence[str],
    *,
    granularity: str | None,
    isolated: frozenset[str] = frozenset(),
) -> tuple[TestTarget, ...]:
    """Carve ``isolated`` test IDs into singleton targets, then shard the rest.

    Isolation (``HOTSPOT_ISOLATED_METHODS``) and ``method_batch``/
    ``class_batch`` granularity (``HOTSPOT_SHARD_POLICIES``) are
    orthogonal: an isolated ID always gets its own target, applied before
    any batching of what remains. With no ``granularity``, the remainder
    becomes one bundled target rather than one per remaining test — the
    shape ``tests.test_nix_module`` uses (see its module docstring). An
    isolated ID absent from the discovered set means the module was
    refactored (a rename or removal) without updating this table, and
    fails closed rather than silently dropping coverage.
    """
    # Same module-membership check shard_test_ids performs. Without a
    # granularity, the remainder target is built directly (not handed to
    # shard_test_ids), so this function must enforce it itself — otherwise
    # a foreign test ID slipped into test_ids would be silently bundled
    # into the remainder target instead of rejected.
    prefix = f"{module.name}."
    foreign = [test_id for test_id in test_ids if not test_id.startswith(prefix)]
    if foreign:
        raise ValueError(
            f"test ID {foreign[0]} does not belong to module {module.name}"
        )
    unknown = isolated - set(test_ids)
    if unknown:
        raise ValueError(
            f"unknown isolated test id(s) for {module.name}: {sorted(unknown)}"
        )
    isolated_targets = tuple(
        TestTarget(module, test_id, (test_id,), load_names=(test_id,))
        for test_id in sorted(isolated)
    )
    remainder = tuple(test_id for test_id in test_ids if test_id not in isolated)
    if not remainder:
        targets = isolated_targets
    elif granularity is None:
        targets = isolated_targets + (
            TestTarget(
                module,
                f"{module.name}::remainder",
                remainder,
                load_names=remainder,
            ),
        )
    else:
        targets = isolated_targets + shard_test_ids(
            module, remainder, granularity=granularity
        )
    assert_exact_target_coverage(module, test_ids, targets)
    return targets


def build_test_targets(
    schedule: Sequence[TestModule],
    listed_test_ids: Mapping[str, Sequence[str]],
    *,
    isolated_methods: Mapping[str, frozenset[str]] = HOTSPOT_ISOLATED_METHODS,
) -> tuple[TestTarget, ...]:
    """Expand only audited hotspots, leaving every other module isolated."""
    targets: list[TestTarget] = []
    for module in schedule:
        granularity = HOTSPOT_SHARD_POLICIES.get(module.name)
        isolated = isolated_methods.get(module.name, frozenset())
        if granularity is None and not isolated:
            targets.append(TestTarget(module, module.name))
            continue
        test_ids = listed_test_ids.get(module.name)
        if test_ids is None:
            raise ValueError(f"missing discovery manifest for hotspot {module.name}")
        targets.extend(
            hotspot_targets(
                module, test_ids, granularity=granularity, isolated=isolated
            )
        )
    return tuple(targets)


def select_test_targets(
    modules: Sequence[TestModule],
    selectors: Sequence[str],
    *,
    listed_test_ids: Mapping[str, Sequence[str]] | None = None,
    hotspot_policies: Mapping[str, str] = HOTSPOT_SHARD_POLICIES,
    hotspot_isolated_methods: Mapping[
        str, frozenset[str]
    ] = HOTSPOT_ISOLATED_METHODS,
) -> tuple[TestTarget, ...]:
    """Resolve unittest selectors while retaining canonical module isolation."""
    plan = tuple(selectors)
    if not plan:
        raise ValueError("at least one test selector is required")
    module_by_name = {module.name: module for module in modules}
    if len(module_by_name) != len(modules):
        raise ValueError("duplicate discovered test module")

    resolved: dict[str, list[str]] = {}
    exact_modules: set[str] = set()
    for selector in plan:
        matches = tuple(
            name
            for name in module_by_name
            if selector == name or selector.startswith(f"{name}.")
        )
        if not matches:
            raise ValueError(f"unknown test selector: {selector}")
        module_name = max(matches, key=len)
        if selector == module_name:
            exact_modules.add(module_name)
        resolved.setdefault(module_name, []).append(selector)

    selected_modules = schedule_modules(
        tuple(module_by_name[name] for name in resolved)
    )
    manifests = listed_test_ids or {}
    targets: list[TestTarget] = []
    for module in selected_modules:
        if module.name in exact_modules:
            granularity = hotspot_policies.get(module.name)
            isolated = hotspot_isolated_methods.get(module.name, frozenset())
            if granularity is None and not isolated:
                targets.append(TestTarget(module, module.name))
                continue
            test_ids = manifests.get(module.name)
            if test_ids is None:
                raise ValueError(
                    f"missing discovery manifest for hotspot {module.name}"
                )
            targets.extend(
                hotspot_targets(
                    module, test_ids, granularity=granularity, isolated=isolated
                )
            )
            continue
        seen: set[str] = set()
        for selector in resolved[module.name]:
            if selector in seen:
                continue
            seen.add(selector)
            targets.append(
                TestTarget(
                    module=module,
                    test_name=selector,
                    load_names=(selector,),
                )
            )
    return tuple(targets)


#: Issue #1229: the LAST of the packing loss, and the only thing that can
#: reach it. `schedule_modules` orders by `_line_weight` (line count) with
#: `AUDITED_FRONTLOAD_MODULES` as a hand-audited override, and that override
#: is MEASURED SATURATED: replaying a real 464-target duration map through
#: the real scheduler, every membership variant tried landed between 81.0s
#: and 82.3s against a longest-processing-time bound of 76.2s. Membership
#: cannot close a gap that comes from ordering WITHIN the early group, so
#: the only way to reach the bound is an actual per-target cost.
#:
#: The honest source of that cost is the previous run's own measurement.
#: A committed table was considered and rejected in #1226: 464 numbers that
#: drift on every test change is a worse trade than the seconds it buys.
#: This cache has no such problem -- it is written by the run that measured
#: it, and a tree change simply leaves the changed targets unknown.
#:
#: This is a SCHEDULING HINT and never a correctness boundary. Every target
#: still runs; `assert_exact_target_schedule` compares by name as a SET, so
#: it is order-insensitive by construction. A missing, stale, or corrupt
#: cache costs some packing efficiency and nothing else, and the no-cache
#: path is exactly the pre-#1229 behaviour.
TARGET_DURATION_CACHE_NAME = "cratedigger-target-durations.json"


def load_target_durations(runtime_dir: Path) -> dict[str, float]:
    """Read the previous run's per-target seconds, or ``{}`` if unusable.

    Every failure mode degrades to ``{}`` (schedule as before) rather than
    raising: this file is an optimisation hint written by a previous
    process, so a truncated write, a foreign format, or no file at all must
    never be able to fail a test run.
    """
    path = runtime_dir / TARGET_DURATION_CACHE_NAME
    try:
        raw = path.read_bytes()
    except OSError:
        return {}
    try:
        decoded = msgspec.json.decode(raw, type=dict[str, float])
    except (msgspec.DecodeError, msgspec.ValidationError):
        return {}
    return {
        name: seconds
        for name, seconds in decoded.items()
        if seconds >= 0.0
    }


def store_target_durations(
    runtime_dir: Path,
    results: Sequence[TargetRunResult],
) -> None:
    """Merge this run's measured target seconds into the ordering cache.

    Merged, not replaced, so a `--test`-selected partial run refines the
    cache instead of discarding every target it did not run. Written to a
    temporary file and renamed, so a concurrent reader sees either the old
    map or the new one, never a half-written one. Best effort throughout.
    """
    if not results:
        return
    merged = load_target_durations(runtime_dir)
    for result in results:
        merged[result.target.test_name] = round(result.elapsed_seconds, 3)
    path = runtime_dir / TARGET_DURATION_CACHE_NAME
    try:
        with tempfile.NamedTemporaryFile(
            dir=runtime_dir,
            prefix=f"{TARGET_DURATION_CACHE_NAME}.",
            delete=False,
        ) as handle:
            handle.write(msgspec.json.encode(merged))
            temporary = Path(handle.name)
    except OSError:
        return
    try:
        temporary.replace(path)
    except OSError:
        temporary.unlink(missing_ok=True)


def order_targets_by_measured_cost(
    schedule: Sequence[TestTarget],
    durations: Mapping[str, float],
) -> tuple[TestTarget, ...]:
    """Longest-processing-time-first, using the previous run's measurements.

    An UNKNOWN target (new, renamed, or resharded since the cache was
    written) sorts as if it were the most expensive known one, so it is
    admitted early. That is the fail-safe direction: admitting an unknown
    target early costs almost nothing when it turns out to be cheap, while
    admitting a genuinely expensive one late is exactly the tail this
    ordering exists to prevent. Python's sort is stable, so targets of
    equal cost -- including every target when the cache is empty -- keep
    the incoming heuristic order untouched.
    """
    # Strictly greater than every known cost, not equal to the largest: a
    # stable sort keeps ties in their incoming order, so `max(...)` would
    # leave an unknown target sitting BEHIND the dearest known one — the
    # opposite of the fail-safe direction this is documented to take.
    # (Caught by this function's own test, not by review.)
    #
    # This also makes an empty cache need no special case: every target is
    # then unknown, every key is equal, and a stable sort returns the
    # incoming order untouched. An `if not durations: return` short-circuit
    # was written here first and removed — a planted mutant proved it had
    # no observable behaviour at all, only a redundant branch to maintain.
    unknown_cost = math.inf
    return tuple(
        sorted(
            schedule,
            key=lambda target: -durations.get(target.test_name, unknown_cost),
        )
    )


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


@dataclass(frozen=True)
class ResolvedHypothesisSettings:
    """The settings object one Hypothesis test will actually run under."""

    configured: settings
    from_state_machine_class: bool


def _test_method(test: unittest.TestCase) -> Callable[..., object] | None:
    method_name = test._testMethodName
    for owner in type(test).__mro__:
        candidate = vars(owner).get(method_name)
        if callable(candidate):
            return candidate
    return None


def resolve_hypothesis_settings(
    test: unittest.TestCase,
) -> ResolvedHypothesisSettings | None:
    """Resolve one test's effective Hypothesis settings, or ``None``.

    ``None`` means the test is not a Hypothesis test. A Hypothesis test whose
    settings cannot be resolved raises: an unclassifiable shape fails closed
    rather than escaping the deadline contract below.
    """
    method = _test_method(test)
    if method is None or not is_hypothesis_test(method):
        return None
    raw_configured: object | None = getattr(
        method,
        "_hypothesis_internal_use_settings",
        None,
    )
    if raw_configured is not None and not isinstance(raw_configured, settings):
        raise TypeError(f"Hypothesis test has invalid settings: {test.id()}")
    if raw_configured is not None:
        return ResolvedHypothesisSettings(raw_configured, False)
    if hasattr(method, "_hypothesis_state_machine_class"):
        raw_stateful: object | None = getattr(type(test), "settings", None)
        if raw_stateful is not None and not isinstance(raw_stateful, settings):
            raise TypeError(
                f"State-machine test has invalid settings: {test.id()}"
            )
        if raw_stateful is not None:
            return ResolvedHypothesisSettings(raw_stateful, True)
    raise TypeError(f"Hypothesis test has no settings: {test.id()}")


def settings_max_examples(configured: settings) -> int:
    """Read one resolved settings object's integer example budget."""
    raw_max_examples: object = getattr(configured, "max_examples", None)
    if isinstance(raw_max_examples, bool) or not isinstance(
        raw_max_examples,
        int,
    ):
        raise TypeError("Hypothesis max_examples is not an integer")
    return raw_max_examples


def hypothesis_example_budgets(suite: unittest.TestSuite) -> dict[str, int]:
    """Map each Hypothesis test in this suite to the budget it will run under."""
    budgets: dict[str, int] = {}
    for test in _iter_test_cases(suite):
        resolved = resolve_hypothesis_settings(test)
        if resolved is None:
            continue
        budgets[test.id()] = settings_max_examples(resolved.configured)
    return budgets


def override_hypothesis_max_examples(
    suite: unittest.TestSuite,
    max_examples: int,
) -> None:
    """Set one isolated target's budget without changing its other settings."""
    if max_examples < 1:
        raise ValueError("Hypothesis target max_examples must be at least 1")
    properties = tuple(
        (test, resolved)
        for test in _iter_test_cases(suite)
        if (resolved := resolve_hypothesis_settings(test)) is not None
    )
    if len(properties) != 1:
        raise ValueError(
            "Hypothesis target budget override requires exactly one property, "
            f"found {len(properties)}"
        )
    test, resolved = properties[0]
    overridden = settings(
        parent=resolved.configured,
        max_examples=max_examples,
    )
    if resolved.from_state_machine_class:
        setattr(  # noqa: B010 - Hypothesis stores state-machine settings here
            type(test),
            "settings",
            overridden,
        )
        return
    method = _test_method(test)
    if method is None:
        raise ValueError(f"could not resolve Hypothesis target: {test.id()}")
    setattr(  # noqa: B010 - Hypothesis stores effective settings on the method
        method,
        "_hypothesis_internal_use_settings",
        overridden,
    )


def assert_hypothesis_deadlines_disabled(suite: unittest.TestSuite) -> None:
    """Every Hypothesis test in this suite must resolve ``deadline=None``.

    This is the RUNTIME half of the profile-tier invariant (issue #882): a
    module that never imports ``tests._hypothesis_profiles`` — or imports it
    below the decorators that snapshot ``settings.default`` — inherits stock
    Hypothesis defaults, including a 200ms per-example deadline that flakes
    under load. ``scripts/run_fuzz_tests.py`` has always asserted this during
    fuzz discovery; asserting it here means the deterministic suite enforces
    the same fact, and neither spelling nor import position can evade it.

    Deliberately scoped to ``deadline`` alone. ``derandomize`` and ``database``
    are legitimately varied by ``tests/world_model/state_machine.py`` under
    ``CRATEDIGGER_WORLD_RANDOMIZED=1`` (which ``scripts/world_model_burst.sh``
    exports), so asserting those would fail correctly-pinned tests.
    """
    offenders: list[str] = []
    for test in _iter_test_cases(suite):
        resolved = resolve_hypothesis_settings(test)
        if resolved is None:
            continue
        deadline: object = getattr(resolved.configured, "deadline", None)
        if deadline is not None:
            offenders.append(f"{test.id()}: {deadline!r}")
    if offenders:
        raise RuntimeError(
            "Hypothesis test has non-None deadline (its module never ran "
            "`import tests._hypothesis_profiles` before the decorator): "
            + ", ".join(sorted(offenders))
        )


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
    max_examples_override: int | None = None,
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
    if max_examples_override is not None:
        override_hypothesis_max_examples(suite, max_examples_override)
    assert_hypothesis_deadlines_disabled(suite)
    recorder = HypothesisStatsRecorder(hypothesis_example_budgets(suite))

    class _StatsRecordingResult(RecordingTextTestResult):
        """Bridge the canonical started-test record to the stats recorder."""

        def startTest(self, test: unittest.TestCase) -> None:
            super().startTest(test)
            recorder.start(test.id())

    # Hypothesis types the collector as ``DynamicVariable[None]``, so pyright
    # reads any callback as a bad argument; the runtime contract is a callable
    # taking one statistics mapping (``hypothesis.statistics.note_statistics``).
    with collector.with_value(
        recorder.note,  # pyright: ignore[reportArgumentType]
    ):
        result = unittest.TextTestRunner(
            stream=stream,
            verbosity=2,
            durations=durations,
            resultclass=_StatsRecordingResult,  # pyright: ignore[reportArgumentType]
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
                failed_test_ids=tuple(
                    test.id()
                    for test, _detail in (*result.failures, *result.errors)
                )
                + tuple(test.id() for test in result.unexpectedSuccesses),
                hypothesis_stats=recorder.records,
                infrastructure_errors=tuple(
                    result.infrastructure_errors or ()
                ),
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
        failed_test_ids=child.failed_test_ids,
        infrastructure_errors=child.infrastructure_errors,
    )


def _classify_target_infrastructure_failure(
    target: TestTarget,
    exc: Exception,
    *,
    available_bytes: Callable[[], int | None] = _measure_tempdir_available_bytes,
    minimum_bytes: int | None = None,
    available_memory_bytes: Callable[[], int | None] = _measure_available_memory_bytes,
    minimum_memory_bytes: int | None = None,
) -> TargetInfrastructureFailure:
    """Classify a worker's own crash by measuring free bytes right now.

    Covers the shape a running test's own classifier cannot see: the worker
    subprocess never produced a result at all (the observed
    ``FileNotFoundError`` on ``raw-output.log``, or any other shape a
    starved tmpfs produces). Measured, not parsed — the same honest
    "check bytes at failure time" the running-test classifier already uses
    (`_classify_test_infrastructure_error`), never a scan of `exc`'s text.

    The floor is the SAME configured minimum ``run_suite``'s own startup
    precondition enforces (``CRATEDIGGER_TEST_RAM_MIN_BYTES``, 1 GiB
    default) — not the running-test classifier's much lower internal
    ``_MIN_VALID_TEMP_HEADROOM_BYTES`` (64 MiB), which is intentionally
    unchanged. Below the suite's own floor is by definition the unsupported
    regime, since ``run_suite`` refuses to even start there and the floor
    only shrinks as the run consumes tmpfs: a worker crash measured in the
    64 MiB-1 GiB band is real exhaustion this classifier would otherwise
    miss, wherever in the run it happens to be measured.

    Residual, stated honestly: this can still MISS a genuine exhaustion
    event whose measured moment happened to read comfortably above the
    floor (a sudden large write between this check and the crash) — that
    failure reads as an ordinary worker failure, the same as it always did.
    It can never fold a genuine code defect INTO this bucket and hide it:
    nothing here suppresses a failure, only relabels ones it can prove are
    environmental.

    Issue #1156 item 2: an OOM-killed worker raises ``BrokenProcessPool``,
    not an ENOSPC-shaped exception, and the disk-full check above measures
    the wrong resource for it — so it always fell through as an ordinary,
    unclassified worker failure, one ``CheckFailureMarker`` per target,
    each named after a different innocent test (the same N-disguises shape
    issue #1111 fixed for disk-full). Classified the same "measured state"
    way, narrowed by exception SHAPE rather than left as a bare threshold
    check: ``isinstance(exc, BrokenProcessPool)`` is the specific signal
    every outstanding future in a ``ProcessPoolExecutor`` receives when one
    of its workers vanishes unexpectedly, and
    ``available_memory_bytes() < minimum_memory_bytes`` is the
    corroborating measured state — never a scan of ``exc``'s text. Only
    evaluated when the disk check already came back negative — a DELIBERATE
    ORDERING CHOICE, not a claim that the two are physically distinct
    (independent review B2, issue #1156): on a host where the shared tmpfs
    RAM root is itself what "memory" mostly consists of (issue #1214's own
    measurement — cgroup v2 ``memory.peak`` counts tmpfs pages, 69% of the
    fuzz phase's peak was scratch tmpfs), genuine system-wide memory
    pressure usually manifests as tmpfs exhaustion FIRST, so the disk-full
    branch wins and this memory branch is largely inert under exactly the
    pressure it exists to name. ``scripts/run_python_tests.py``'s own
    `main` wiring below (issue #1156 review F10) already states the
    correct, weaker fact: nothing stops a disk-full marker and a
    memory-exhausted marker from BOTH occurring in the same run (on
    different targets); this docstring must not contradict it.

    Residual, stated honestly (worse than the tmpfs case): an OOM kill
    FREES the killed worker's memory immediately, so by the time this
    classifier runs — after ``ProcessPoolExecutor`` has already detected
    the broken pool and raised — available memory may have already
    recovered above the floor. That reads as an ordinary worker failure,
    the same miss the disk-full branch already accepts for its own
    measurement-timing window, just more likely to occur here.

    This one is NOT one-directional the way the disk-full branch's own
    claim is (independent review F5(e), issue #1156): a genuine code
    defect whose crash happens to raise a real ``BrokenProcessPool``
    (e.g. a segfault in a C extension the pool worker was running) WHILE
    measured memory genuinely reads below the floor at that exact moment
    WOULD be folded into "memory exhausted" and reported as
    environmental rather than a real bug. What is still true: a
    ``BrokenProcessPool`` with healthy measured memory stays an ordinary
    infrastructure failure, and any exception OTHER than
    ``BrokenProcessPool`` is never reclassified as memory exhaustion no
    matter how little memory is measured — the exception-shape gate
    narrows the false-positive window to "the pool broke AND memory is
    genuinely low right now," not "any bug at all."
    """
    available = available_bytes()
    floor = minimum_bytes if minimum_bytes is not None else default_min_headroom_bytes()
    disk_full = available is not None and available < floor
    memory_exhausted = False
    available_memory: int | None = None
    if not disk_full and isinstance(exc, BrokenProcessPool):
        memory_floor = (
            minimum_memory_bytes
            if minimum_memory_bytes is not None
            else _default_min_memory_headroom_bytes()
        )
        available_memory = available_memory_bytes()
        memory_exhausted = (
            available_memory is not None
            and available_memory < memory_floor
        )
    detail = f"{type(exc).__name__}: {exc}"
    if disk_full:
        detail = f"temporary filesystem has {available} bytes free; {detail}"
    elif memory_exhausted:
        detail = f"system memory has {available_memory} bytes available; {detail}"
    return TargetInfrastructureFailure(
        target=target,
        detail=detail,
        disk_full=disk_full,
        memory_exhausted=memory_exhausted,
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
            except Exception as exc:  # noqa: BLE001 - worker infrastructure boundary
                infrastructure_failures.append(
                    _classify_target_infrastructure_failure(target, exc)
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


def _collapse_disk_full_failures(
    failed_results: Sequence[TargetRunResult],
    infrastructure_failures: Sequence[TargetInfrastructureFailure],
) -> tuple[
    tuple[TargetRunResult, ...],
    tuple[TargetInfrastructureFailure, ...],
    CheckFailureMarker | None,
]:
    """Fold every disk-full-classified failure into one named marker.

    Issue #1111 item 2: a target whose worker never produced a result at all
    (`TargetInfrastructureFailure.disk_full`) is unambiguous — it contributes
    nothing else and is dropped from the per-target reporting entirely. A
    target that ran but had one or more individual tests trip the
    ENOSPC-shaped classifier mid-run (`ChildInfrastructureError(kind=
    "disk_full")`, which can also surface as a Hypothesis ``FlakyFailure``)
    keeps its OTHER, unrelated failures reported normally — only the
    disk-full test IDs move into the combined bucket. Returns the inputs
    unchanged with a ``None`` marker when nothing was disk-full, so the
    ordinary (and by far the common) case is a no-op.
    """
    combined_ids: list[str] = []
    combined_details: list[str] = []
    remaining_results: list[TargetRunResult] = []
    for result in failed_results:
        disk_full_ids = frozenset(
            error.test_id
            for error in result.infrastructure_errors
            if error.kind == "disk_full"
        )
        if not disk_full_ids:
            remaining_results.append(result)
            continue
        combined_ids.extend(sorted(disk_full_ids))
        combined_details.extend(
            error.detail
            for error in result.infrastructure_errors
            if error.kind == "disk_full"
        )
        remaining_ids = tuple(
            test_id
            for test_id in result.failed_test_ids
            if test_id not in disk_full_ids
        )
        if remaining_ids:
            remaining_results.append(replace(result, failed_test_ids=remaining_ids))

    remaining_infrastructure: list[TargetInfrastructureFailure] = []
    for failure in infrastructure_failures:
        if failure.disk_full:
            combined_ids.append(failure.target.test_name)
            combined_details.append(failure.detail)
        else:
            remaining_infrastructure.append(failure)

    if not combined_ids:
        return tuple(remaining_results), tuple(remaining_infrastructure), None

    marker = CheckFailureMarker(
        identity=TEST_RAM_ROOT_EXHAUSTED,
        owner="",
        detail=(
            f"{len(combined_ids)} target(s)/test(s) failed while the shared "
            "test RAM root was exhausted; sample: "
            f"{combined_details[0] if combined_details else 'no detail'}"
        ),
        test_ids=tuple(combined_ids),
    )
    return tuple(remaining_results), tuple(remaining_infrastructure), marker


def _collapse_memory_exhausted_failures(
    infrastructure_failures: Sequence[TargetInfrastructureFailure],
) -> tuple[tuple[TargetInfrastructureFailure, ...], CheckFailureMarker | None]:
    """Fold every OOM-classified worker death into one named marker.

    Mirrors ``_collapse_disk_full_failures`` for a DIFFERENT resource
    (issue #1156 item 2), not a copy of its decision: memory exhaustion is
    detectable only at the WORKER-death level
    (``TargetInfrastructureFailure.memory_exhausted`` —
    ``BrokenProcessPool`` means the whole pool broke, not that a
    still-alive worker's specific test raised), so unlike the disk-full
    fold there is no ``failed_results`` half to fold here; a target that
    finished, however it finished, never carries this kind of failure.
    """
    remaining: list[TargetInfrastructureFailure] = []
    combined_ids: list[str] = []
    combined_details: list[str] = []
    for failure in infrastructure_failures:
        if failure.memory_exhausted:
            combined_ids.append(failure.target.test_name)
            combined_details.append(failure.detail)
        else:
            remaining.append(failure)

    if not combined_ids:
        return tuple(remaining), None

    marker = CheckFailureMarker(
        identity=TEST_HOST_MEMORY_EXHAUSTED,
        owner="",
        detail=(
            f"{len(combined_ids)} target(s) failed while the host's "
            "available system memory was exhausted; sample: "
            f"{combined_details[0] if combined_details else 'no detail'}"
        ),
        test_ids=tuple(combined_ids),
    )
    return tuple(remaining), marker


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
    return recommended_worker_count(os.cpu_count() or 1)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-directory", type=Path, default=Path("tests"))
    parser.add_argument("--top-level-directory", type=Path, default=Path("."))
    parser.add_argument("--pattern", default="test*.py")
    parser.add_argument(
        "--test",
        action="append",
        default=[],
        metavar="UNITTEST_NAME",
        help="run one module, class, or method; may be repeated",
    )
    parser.add_argument(
        "--jobs", type=_parse_positive_int, default=_default_worker_count()
    )
    parser.add_argument(
        "--durations",
        type=_parse_nonnegative_int,
        default=DEFAULT_DURATIONS,
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    run_targets_fn: Callable[
        ...,
        tuple[tuple[TargetRunResult, ...], tuple[TargetInfrastructureFailure, ...]],
    ] = _run_targets,
) -> int:
    """Run the deterministic Python suite phase's CLI entry point.

    ``run_targets_fn`` mirrors the ``check_headroom`` DI seam on the fuzz
    and world-model bursts' own ``main`` functions (issue #1156 item 3):
    ``_run_targets`` (the production default) is the ONE real boundary that
    talks to a genuine ``ProcessPoolExecutor`` and its subprocess-level
    worker-death semantics -- a worker dying poisons the WHOLE pool for
    every still-tracked future, so two independently-classified real
    deaths (one disk_full, one memory_exhausted) cannot be produced
    deterministically in a single real run. Independent review B-3 (third
    round): the two-clause promotion guard below needs exactly that
    heterogeneous-marker scenario to prove its own `is None` clauses are
    load-bearing; the test seeds it by replacing this ONE real boundary
    with pre-built results, while every other line of `main` -- CLI
    parsing, discovery, scheduling, the collapse helpers, printing, exit
    code selection -- runs for real, unmocked.
    """
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
    hotspot_module_names = HOTSPOT_SHARD_POLICIES.keys() | HOTSPOT_ISOLATED_METHODS.keys()
    selected_hotspots = {
        selector for selector in args.test if selector in hotspot_module_names
    }
    hotspot_names = (
        selected_hotspots
        if args.test
        else hotspot_module_names & {module.name for module in modules}
    )
    listed_test_ids = {
        module_name: list_module_test_ids(module_name, top)
        for module_name in sorted(hotspot_names)
    }
    if args.test:
        schedule = select_test_targets(
            modules,
            args.test,
            listed_test_ids=listed_test_ids,
        )
    else:
        module_schedule = schedule_modules(modules)
        assert_exact_schedule(modules, module_schedule)
        schedule = build_test_targets(module_schedule, listed_test_ids)
    worker_count = min(args.jobs, len(schedule))

    # Issue #1229: reorder by the previous run's measured per-target cost.
    # Hint only — see TARGET_DURATION_CACHE_NAME. Announced rather than
    # silent, so a run whose wall time differs from a sibling's is
    # explainable instead of mysterious.
    # RuntimeError is the one `private_runtime_dir` actually raises for every
    # unusable-root case (missing, symlink, wrong owner, wrong mode, not
    # tmpfs); OSError covers the stat/resolve paths underneath it. Catching
    # BOTH matters because this module is also runnable directly, without
    # `run_suite`'s admission lock having already demanded that root — an
    # ordering hint must never be the thing that fails such a run.
    try:
        runtime_dir = private_runtime_dir()
    except (OSError, RuntimeError):
        runtime_dir = None
    measured_durations = (
        load_target_durations(runtime_dir) if runtime_dir is not None else {}
    )
    if measured_durations:
        schedule = order_targets_by_measured_cost(schedule, measured_durations)
        known = sum(
            1 for target in schedule if target.test_name in measured_durations
        )
        print(
            f"Order: longest-first from {len(measured_durations)} cached "
            f"target timings ({known}/{len(schedule)} known)"
        )

    print(
        f"Python suite: {len({target.module.name for target in schedule})} "
        f"modules across {worker_count} workers "
        f"({os.cpu_count() or 1} host CPUs)"
    )
    sharded_target_count = sum(
        target.module.name in hotspot_module_names for target in schedule
    )
    print(
        f"Queue: {len(schedule)} targets "
        f"({sharded_target_count} audited hotspot targets)"
    )
    started_at = time.monotonic()
    results, infrastructure_failures = run_targets_fn(
        schedule,
        worker_count=worker_count,
        top_level_directory=top,
        durations=args.durations,
    )
    wall_seconds = time.monotonic() - started_at

    if runtime_dir is not None:
        store_target_durations(runtime_dir, results)

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
        (
            remaining_failed_results,
            remaining_infrastructure_failures,
            ram_root_marker,
        ) = _collapse_disk_full_failures(failed_results, infrastructure_failures)
        (
            remaining_infrastructure_failures,
            memory_marker,
        ) = _collapse_memory_exhausted_failures(remaining_infrastructure_failures)
        for result in sorted(
            remaining_failed_results,
            key=lambda item: item.target.test_name,
        ):
            try:
                owner = str(result.target.module.path.relative_to(top))
            except ValueError:
                owner = str(result.target.module.path)
            print(
                FAILURE_MARKER_PREFIX
                + msgspec.json.encode(
                    CheckFailureMarker(
                        identity=result.target.test_name,
                        owner=owner,
                        detail=f"{len(result.failed_test_ids)} failed test IDs",
                        test_ids=result.failed_test_ids,
                    )
                ).decode()
            )
            print(
                f"\n--- FAIL: worker {result.worker_pid}, "
                f"target {result.target.test_name} ---"
            )
            print(_failure_diagnostics(result.output))
        for failure in sorted(
            remaining_infrastructure_failures,
            key=lambda item: item.target.test_name,
        ):
            try:
                owner = str(failure.target.module.path.relative_to(top))
            except ValueError:
                owner = str(failure.target.module.path)
            print(
                FAILURE_MARKER_PREFIX
                + msgspec.json.encode(
                    CheckFailureMarker(
                        identity=failure.target.test_name,
                        owner=owner,
                        detail=failure.detail,
                    )
                ).decode()
            )
            print(
                "\n--- FAIL: worker infrastructure, target "
                f"{failure.target.test_name} ---"
            )
            print(failure.detail)
        if ram_root_marker is not None:
            # Issue #1111 item 2: every disk-full-classified failure is ONE
            # named entry here, never N separately-indexed disguises.
            print(FAILURE_MARKER_PREFIX + msgspec.json.encode(ram_root_marker).decode())
            print(
                "\n--- FAIL: "
                f"{TEST_RAM_ROOT_EXHAUSTED} "
                f"({len(ram_root_marker.test_ids)} target(s)/test(s)) ---"
            )
            print(ram_root_marker.detail)
        if memory_marker is not None:
            # Issue #1156 item 2: every OOM-classified worker death is ONE
            # named entry here, never N separately-indexed disguises.
            print(FAILURE_MARKER_PREFIX + msgspec.json.encode(memory_marker).decode())
            print(
                "\n--- FAIL: "
                f"{TEST_HOST_MEMORY_EXHAUSTED} "
                f"({len(memory_marker.test_ids)} target(s)) ---"
            )
            print(memory_marker.detail)
        known_count = sum(result.tests_run for result in results)
        failed_targets = (
            len(remaining_failed_results)
            + len(remaining_infrastructure_failures)
            + (1 if ram_root_marker is not None else 0)
            + (1 if memory_marker is not None else 0)
        )
        print(
            METRICS_MARKER_PREFIX
            + msgspec.json.encode(
                CheckMetricsMarker(
                    tests_run=known_count,
                    targets_run=len(completed_targets),
                    scheduled_targets=len(schedule),
                )
            ).decode()
        )
        print(
            f"\nFAILED: {failed_targets} of {len(schedule)} targets; "
            f"Ran {known_count} reported tests in {wall_seconds:.1f}s"
        )
        # Promotion requires a HOMOGENEOUS failure set: exactly one
        # environmental cause and nothing else unexplained. If BOTH
        # ram_root_marker and memory_marker are present (two DIFFERENT
        # targets independently classified disk_full and memory_exhausted
        # in the same run -- a per-target classification is mutually
        # exclusive between the two, but nothing stops different targets
        # in the same run from landing on different sides), neither
        # condition below matches and this falls through to the ordinary
        # `return 1`. Both markers are still printed above with their
        # correct identities; only the infrastructure-failure PROMOTION is
        # skipped. Stated honestly (independent review F10, issue #1156):
        # this is a known, accepted residual, not a claim that the two
        # causes can never co-occur in one run.
        if (
            not remaining_failed_results
            and not remaining_infrastructure_failures
            and ram_root_marker is not None
            and memory_marker is None
        ):
            return TEST_RAM_ROOT_EXHAUSTED_EXIT_CODE
        if (
            not remaining_failed_results
            and not remaining_infrastructure_failures
            and memory_marker is not None
            and ram_root_marker is None
        ):
            return TEST_HOST_MEMORY_EXHAUSTED_EXIT_CODE
        return 1

    total_tests = sum(result.tests_run for result in results)
    actual_workers = len({result.worker_pid for result in results})
    print(
        METRICS_MARKER_PREFIX
        + msgspec.json.encode(
            CheckMetricsMarker(
                tests_run=total_tests,
                targets_run=len(completed_targets),
                scheduled_targets=len(schedule),
            )
        ).decode()
    )
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
    if len(sys.argv) in {5, 6, 7} and sys.argv[1] == "--_run-target":
        raise SystemExit(
            _run_test_target_child(
                msgspec.json.decode(sys.argv[2], type=tuple[str, ...]),
                int(sys.argv[3]),
                Path(sys.argv[4]),
                (
                    msgspec.json.decode(sys.argv[5], type=tuple[str, ...])
                    if len(sys.argv) >= 6
                    else None
                ),
                int(sys.argv[6]) if len(sys.argv) == 7 else None,
            )
        )
    raise SystemExit(main())
