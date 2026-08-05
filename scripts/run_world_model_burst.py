#!/usr/bin/env python3
"""Coordinate isolated, multicore shards of the heavyweight world model."""

from __future__ import annotations

import argparse
import fcntl
import functools
import hashlib
import importlib
import io
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import unittest
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Self
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import hypothesis.stateful as hypothesis_stateful
import msgspec
from hypothesis import is_hypothesis_test
from hypothesis.internal.reflection import function_digest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lib.ephemeral_postgres import EphemeralPostgres
from lib.migrator import apply_migrations
from scripts.run_python_tests import RecordingTextTestResult

DEFAULT_EXAMPLES = 25
DEFAULT_STEPS = 100
IN_PROCESS_JOB_CAP = 30
MIRROR_JOB_CAP = 2
_SCHEMA_READY_ENV = "CRATEDIGGER_TEST_SCHEMA_READY"


class WorldManifest(msgspec.Struct, frozen=True):
    """Exact unittest discovery result from a fresh interpreter."""

    test_ids: tuple[str, ...]
    generated_ids: tuple[str, ...]
    load_names: tuple[str, ...] = ()
    database_key_paths: tuple[tuple[str, tuple[str, ...]], ...] = ()


class ChildReceipt(msgspec.Struct, frozen=True):
    """Strict wire receipt emitted by one fresh target interpreter."""

    test_ids: tuple[str, ...]
    outcome: str
    tests_run: int
    seed: int
    output: str


class ChildInvocation(msgspec.Struct, frozen=True):
    test_ids: tuple[str, ...]
    load_names: tuple[str, ...]
    generated: bool
    seed: int


class ReplayTarget(msgspec.Struct, frozen=True):
    label: str
    logical_id: str
    test_ids: tuple[str, ...]
    shard_index: int
    shard_count: int
    examples: int
    steps: int | None
    seed: int
    outcome: str
    elapsed_seconds: float


class ReplayReceipt(msgspec.Struct, frozen=True):
    root_seed: int
    engine: str
    examples: int
    steps: int
    jobs: int
    elapsed_seconds: float
    admission_aborted: bool
    targets: tuple[ReplayTarget, ...]
    not_started: tuple[ReplayTarget, ...]
    coordinator_error: str | None = None


@dataclass(frozen=True)
class WorldTarget:
    label: str
    logical_id: str
    test_ids: tuple[str, ...]
    generated: bool
    shard_index: int
    shard_count: int
    examples: int
    steps: int | None
    seed: int
    load_names: tuple[str, ...] = ()
    database_key_paths: tuple[str, ...] = ()
    replay_corpus: bool = False


@dataclass(frozen=True)
class TargetOutcome:
    target: WorldTarget
    outcome: str
    receipt: ChildReceipt | None
    log_path: Path
    database_path: Path
    detail: str = ""
    elapsed_seconds: float = 0.0


def target_seed(
    root_seed: int,
    logical_id: str,
    shard_index: int,
    shard_count: int = 1,
) -> int:
    """Derive entropy solely from stable target identity, never PID or order."""
    payload = (
        f"world-model-v1\0{root_seed}\0{logical_id}\0"
        f"{shard_index}\0{shard_count}"
    ).encode()
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big")


def effective_jobs(
    requested: int | None,
    engine: str,
    *,
    host_cpus: int | None = None,
) -> int:
    if requested is not None and requested < 1:
        raise ValueError("jobs must be at least 1")
    host = host_cpus if host_cpus is not None else (os.cpu_count() or 1)
    cap = MIRROR_JOB_CAP if engine == "mirror-harness" else IN_PROCESS_JOB_CAP
    return min(requested or host, host, cap)


def build_targets(
    manifest: WorldManifest,
    *,
    examples: int,
    jobs: int,
    root_seed: int,
    steps: int | None = None,
    expected_generated: int = 5,
    expected_pins: int | None = None,
) -> tuple[WorldTarget, ...]:
    """Schedule pins once and split each generated budget without dilution."""
    if examples < 1 or jobs < 1:
        raise ValueError("examples and jobs must be at least 1")
    if len(manifest.generated_ids) != expected_generated:
        raise ValueError(
            f"expected exactly {expected_generated} generated tests; "
            f"discovered {len(manifest.generated_ids)}"
        )
    if len(set(manifest.test_ids)) != len(manifest.test_ids):
        raise ValueError("duplicate unittest ID discovered")
    generated_set = set(manifest.generated_ids)
    if not generated_set.issubset(manifest.test_ids):
        raise ValueError("generated unittest ID absent from exact discovery")
    load_names = manifest.load_names or manifest.test_ids
    if len(load_names) != len(manifest.test_ids):
        raise ValueError("discovery load-name mapping changed")
    load_name_by_id = dict(zip(manifest.test_ids, load_names, strict=True))
    database_key_paths = dict(manifest.database_key_paths)
    if set(database_key_paths) != generated_set:
        raise ValueError("generated Hypothesis database-key mapping changed")
    for test_id, paths in database_key_paths.items():
        if len(paths) != 3 or any(
            len(path) != 16 or any(character not in "0123456789abcdef" for character in path)
            for path in paths
        ):
            raise ValueError(f"invalid Hypothesis database-key path for {test_id}")
    pins = tuple(test_id for test_id in manifest.test_ids if test_id not in generated_set)
    if expected_pins is not None and len(pins) != expected_pins:
        raise ValueError(
            f"expected exactly {expected_pins} deterministic pins; "
            f"discovered {len(pins)}"
        )
    targets: list[WorldTarget] = []
    if pins:
        pin_module = pins[0].rsplit(".", 2)[0]
        logical_id = f"{pin_module}.world-model-pins"
        targets.append(WorldTarget(
            label="pins",
            logical_id=logical_id,
            test_ids=pins,
            generated=False,
            shard_index=0,
            shard_count=1,
            examples=1,
            steps=None,
            seed=target_seed(root_seed, logical_id, 0, 1),
            load_names=tuple(load_name_by_id[test_id] for test_id in pins),
        ))
    ordinary_shard_count = min(
        examples,
        max(1, jobs // len(manifest.generated_ids)),
    )
    property_targets: list[list[WorldTarget]] = []
    for test_id in manifest.generated_ids:
        shard_count = (
            min(examples, jobs)
            if test_id.startswith("hypothesis.stateful.")
            else ordinary_shard_count
        )
        quotient, remainder = divmod(examples, shard_count)
        shards: list[WorldTarget] = []
        for index in range(shard_count):
            budget = quotient + (1 if index < remainder else 0)
            shards.append(WorldTarget(
                label=f"{test_id}::shard-{index + 1:02d}-of-{shard_count:02d}",
                logical_id=test_id,
                test_ids=(test_id,),
                generated=True,
                shard_index=index,
                shard_count=shard_count,
                examples=budget,
                steps=steps,
                seed=target_seed(root_seed, test_id, index, shard_count),
                load_names=(load_name_by_id[test_id],),
                database_key_paths=database_key_paths[test_id],
                replay_corpus=index == 0,
                ))
        property_targets.append(shards)
    # Admit one shard from each logical property in turn. Short properties
    # cannot hide the long state-machine tail behind a block of earlier work.
    for shard_index in range(max(map(len, property_targets))):
        for shards in property_targets:
            if shard_index < len(shards):
                targets.append(shards[shard_index])
    built = tuple(targets)
    scheduled = Counter(test_id for target in built for test_id in target.test_ids)
    for test_id in pins:
        if scheduled[test_id] != 1:
            raise AssertionError(f"pin coverage changed: {test_id}")
    for test_id in manifest.generated_ids:
        scheduled_shards = tuple(
            target for target in built if target.logical_id == test_id
        )
        if sum(target.examples for target in scheduled_shards) != examples:
            raise AssertionError(f"generated budget changed: {test_id}")
        if steps is not None and any(
            target.steps != steps for target in scheduled_shards
        ):
            raise AssertionError(f"state-machine steps changed: {test_id}")
    return built


def child_environment(
    base: Mapping[str, str],
    *,
    target: WorldTarget,
    dsn: str,
    target_root: Path,
    engine: str,
    mirror_url: str,
) -> dict[str, str]:
    """Build one child's isolated storage and deterministic generation boundary."""
    database = target_root / "hypothesis"
    beets = target_root / "beets"
    database.mkdir(parents=True, exist_ok=True)
    beets.mkdir(parents=True, exist_ok=True)
    environment = dict(base)
    environment.update({
        "TEST_DB_DSN": dsn,
        _SCHEMA_READY_ENV: "1",
        "CRATEDIGGER_WORLD_RANDOMIZED": "1",
        "CRATEDIGGER_WORLD_EXAMPLES": str(target.examples),
        "CRATEDIGGER_WORLD_STEPS": str(target.steps or DEFAULT_STEPS),
        "CRATEDIGGER_WORLD_DATABASE": str(database),
        "CRATEDIGGER_WORLD_SEED": str(target.seed),
        "CRATEDIGGER_WORLD_ENGINE": engine,
        "CRATEDIGGER_WORLD_MIRROR_URL": mirror_url,
        "HYPOTHESIS_STORAGE_DIRECTORY": str(database),
        "BEETSDIR": str(beets),
        "PYTHONPATH": os.pathsep.join((str(REPO_ROOT), str(REPO_ROOT / "tests"))),
    })
    return environment


def classify_receipt(target: WorldTarget, wire: bytes) -> str:
    try:
        receipt = msgspec.json.decode(wire, type=ChildReceipt)
    except (msgspec.DecodeError, msgspec.ValidationError) as error:
        raise RuntimeError(f"malformed child receipt: {error}") from error
    if receipt.test_ids != target.test_ids:
        raise RuntimeError(
            f"target ran unexpected test IDs: expected {target.test_ids!r}, "
            f"got {receipt.test_ids!r}"
        )
    if receipt.seed != target.seed:
        raise RuntimeError(
            f"target seed changed: expected {target.seed}, got {receipt.seed}"
        )
    allowed = {"passed", "property_failure", "test_failure", "infrastructure_error"}
    if receipt.outcome not in allowed:
        raise RuntimeError(f"malformed child receipt outcome: {receipt.outcome!r}")
    if (
        receipt.outcome != "infrastructure_error"
        and receipt.tests_run != len(target.test_ids)
    ):
        raise RuntimeError(
            f"target test count changed: expected {len(target.test_ids)}, "
            f"got {receipt.tests_run}"
        )
    if target.generated and receipt.outcome == "test_failure":
        raise RuntimeError("generated target returned non-property failure")
    return receipt.outcome


def classify_unittest_outcome(
    *,
    generated: bool,
    successful: bool,
    infrastructure_errors: Sequence[object],
    skipped: Sequence[object] = (),
) -> str:
    """Keep unavailable infrastructure and skipped coverage out of verdicts."""
    if infrastructure_errors or skipped:
        return "infrastructure_error"
    if successful:
        return "passed"
    return "property_failure" if generated else "test_failure"


def _iter_cases(suite: unittest.TestSuite) -> Iterator[unittest.TestCase]:
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from _iter_cases(item)
        else:
            assert isinstance(item, unittest.TestCase)
            yield item


class _StateMachineTestFactory(Protocol):
    def __call__(
        self,
        _state_machine_factory: object,
        *,
        settings: object | None = None,
        **kwargs: object,
    ) -> object: ...


def _state_machine_test_factory() -> _StateMachineTestFactory:
    """Return Hypothesis' untyped state-machine factory without widening callers."""
    candidate = hypothesis_stateful.__dict__["get_state_machine_test"]
    if not callable(candidate):
        raise TypeError("Hypothesis state-machine test factory is not callable")
    return candidate


def _set_hypothesis_seed(test: object, seed: int) -> None:
    namespace = getattr(test, "__dict__", None)
    if not isinstance(namespace, dict):
        raise TypeError("Hypothesis test has no writable function namespace")
    namespace["_hypothesis_internal_use_seed"] = seed


def _hypothesis_database_key_paths(
    owner: type[unittest.TestCase],
    method: object,
) -> tuple[str, str, str]:
    state_machine_class = getattr(method, "_hypothesis_state_machine_class", None)
    if state_machine_class is not None:
        get_state_machine_test = _state_machine_test_factory()
        generated_test = get_state_machine_test(
            state_machine_class,
            settings=vars(owner).get("settings"),
        )
    else:
        generated_test = method
    primary_key = getattr(generated_test, "_hypothesis_internal_database_key", None)
    if primary_key is None:
        handle = getattr(generated_test, "hypothesis", None)
        inner_test = getattr(handle, "inner_test", None)
        if not callable(inner_test):
            raise RuntimeError("Hypothesis test has no callable inner test")
        primary_key = function_digest(inner_test)
    if not isinstance(primary_key, bytes):
        raise TypeError("Hypothesis database key is not bytes")
    keys = (
        primary_key,
        b".".join((primary_key, b"secondary")),
        b".".join((primary_key, b"pareto")),
    )
    return (
        hashlib.sha384(keys[0]).hexdigest()[:16],
        hashlib.sha384(keys[1]).hexdigest()[:16],
        hashlib.sha384(keys[2]).hexdigest()[:16],
    )


def _discover_child(module_name: str, receipt_path: Path) -> int:
    suite = unittest.defaultTestLoader.loadTestsFromName(module_name)
    cases = tuple(_iter_cases(suite))
    ids = tuple(case.id() for case in cases)
    generated: list[str] = []
    database_key_paths: list[tuple[str, tuple[str, ...]]] = []
    module = importlib.import_module(module_name)
    load_names: list[str] = []
    for case in cases:
        method = getattr(type(case), case._testMethodName)
        if is_hypothesis_test(method):
            generated.append(case.id())
            database_key_paths.append((
                case.id(),
                _hypothesis_database_key_paths(type(case), method),
            ))
        if case.id().startswith(f"{module_name}."):
            load_names.append(case.id())
            continue
        aliases = sorted(
            name for name, value in vars(module).items() if value is type(case)
        )
        if len(aliases) != 1:
            raise RuntimeError(
                f"cannot map discovered unittest ID {case.id()!r} to one module alias"
            )
        load_names.append(f"{module_name}.{aliases[0]}.{case._testMethodName}")
    receipt_path.write_bytes(msgspec.json.encode(WorldManifest(
        test_ids=ids,
        generated_ids=tuple(generated),
        load_names=tuple(load_names),
        database_key_paths=tuple(database_key_paths),
    )))
    return 0


def _seed_exact_test(test_id: str, seed: int) -> None:
    module_name, class_name, method_name = test_id.rsplit(".", 2)
    module = importlib.import_module(module_name)
    owner = getattr(module, class_name)
    method = getattr(owner, method_name)
    state_machine_class = getattr(
        method,
        "_hypothesis_state_machine_class",
        None,
    )
    if state_machine_class is None:
        # The public @seed decorator also forces database=None. Setting the
        # attribute it consumes preserves deterministic entropy *and* the
        # per-shard replay database.
        _set_hypothesis_seed(method, seed)
        return

    @functools.wraps(method)
    def seeded_state_machine_run(test_case: unittest.TestCase) -> None:
        original_get_test = _state_machine_test_factory()

        def get_seeded_test(*args: object, **kwargs: object) -> object:
            generated_test = original_get_test(*args, **kwargs)
            _set_hypothesis_seed(generated_test, seed)
            return generated_test

        hypothesis_stateful.get_state_machine_test = get_seeded_test
        try:
            method(test_case)
        finally:
            hypothesis_stateful.get_state_machine_test = original_get_test

    setattr(owner, method_name, seeded_state_machine_run)


def _run_child(target_wire: bytes, receipt_path: Path) -> int:
    target = msgspec.json.decode(target_wire, type=ChildInvocation)
    if target.generated:
        for load_name in target.load_names:
            _seed_exact_test(load_name, target.seed)
    suite = unittest.defaultTestLoader.loadTestsFromNames(target.load_names)
    actual_ids = tuple(case.id() for case in _iter_cases(suite))
    stream = io.StringIO()
    result = unittest.TextTestRunner(
        stream=stream,
        verbosity=2,
        resultclass=RecordingTextTestResult,  # pyright: ignore[reportArgumentType]
    ).run(suite)
    assert isinstance(result, RecordingTextTestResult)
    outcome = classify_unittest_outcome(
        generated=target.generated,
        successful=result.wasSuccessful(),
        infrastructure_errors=tuple(result.infrastructure_errors or ()),
        skipped=tuple(result.skipped),
    )
    receipt_path.write_bytes(msgspec.json.encode(ChildReceipt(
        test_ids=actual_ids,
        outcome=outcome,
        tests_run=result.testsRun,
        seed=target.seed,
        output=stream.getvalue(),
    )))
    return 0


class CoordinatorPostgres:
    """One coordinator-owned cluster with one migrated template and child clones."""

    def __init__(self) -> None:
        self.cluster = EphemeralPostgres()
        self._lock = threading.Lock()
        self._counter = 0

    def __enter__(self) -> Self:
        self.cluster.start()
        assert self.cluster.dsn is not None
        apply_migrations(self.cluster.dsn)
        return self

    @property
    def template_dsn(self) -> str:
        assert self.cluster.dsn is not None
        return self.cluster.dsn

    def _database_command(self, arguments: Sequence[str]) -> None:
        process = subprocess.run(
            arguments,
            capture_output=True,
            text=True,
            check=False,
        )
        if process.returncode != 0:
            raise RuntimeError(
                f"PostgreSQL database command failed ({process.returncode}): "
                f"{' '.join(arguments)}\n{process.stdout}{process.stderr}"
            )

    def create_clone(self) -> tuple[str, str]:
        with self._lock:
            self._counter += 1
            name = f"world_{os.getpid()}_{self._counter}"
            self._database_command((
                "createdb",
                f"--maintenance-db={_dsn_database(self.template_dsn, 'postgres')}",
                f"--template={_dsn_database_name(self.template_dsn)}",
                name,
            ))
        return name, _dsn_database(self.template_dsn, name)

    def drop_clone(self, name: str) -> None:
        with self._lock:
            for attempt in range(3):
                try:
                    self._database_command((
                        "dropdb",
                        f"--maintenance-db={_dsn_database(self.template_dsn, 'postgres')}",
                        "--force",
                        name,
                    ))
                    return
                except RuntimeError:
                    if attempt == 2:
                        raise
                    time.sleep(0.1 * (attempt + 1))

    def __exit__(self, *_args: object) -> None:
        self.cluster.stop()


def _dsn_database(dsn: str, database: str) -> str:
    parts = urlsplit(dsn)
    return urlunsplit((parts.scheme, parts.netloc, f"/{quote(database)}", parts.query, parts.fragment))


def _dsn_database_name(dsn: str) -> str:
    name = unquote(urlsplit(dsn).path.lstrip("/"))
    if not name:
        raise ValueError("PostgreSQL DSN has no database name")
    return name


class _CanonicalLock:
    def __init__(self, database: Path) -> None:
        self.lock_path = database.parent / f".{database.name}.lock"
        self.handle: io.BufferedRandom | None = None

    def __enter__(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+b")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        self.handle = handle

    def __exit__(self, *_args: object) -> None:
        handle = self.handle
        if handle is not None:
            handle.close()
        self.handle = None


def _canonical_lock(database: Path) -> _CanonicalLock:
    return _CanonicalLock(database)


_DATABASE_MARKER = ".cratedigger-world-model-database"
_DATABASE_MARKER_CONTENT = "cratedigger-world-model-v1\n"
_DEFAULT_DATABASE = (REPO_ROOT / ".hypothesis/world-model").resolve()


def prepare_canonical_database(canonical: Path) -> None:
    """Claim an empty corpus path without gaining authority over arbitrary data."""
    canonical = canonical.resolve()
    with _canonical_lock(canonical):
        if canonical.exists() and not canonical.is_dir():
            raise ValueError("world-model database path must be a directory")
        canonical.mkdir(parents=True, exist_ok=True)
        marker = canonical / _DATABASE_MARKER
        if marker.exists():
            if marker.read_text(encoding="utf-8") != _DATABASE_MARKER_CONTENT:
                raise ValueError("invalid world-model database ownership marker")
            return
        entries = tuple(canonical.iterdir())
        legacy_hypothesis_corpus = bool(entries) and all(
            entry.is_dir()
            and len(entry.name) == 16
            and all(character in "0123456789abcdef" for character in entry.name)
            for entry in entries
        )
        if (
            canonical != _DEFAULT_DATABASE
            and entries
            and not legacy_hypothesis_corpus
        ):
            raise ValueError(
                "non-empty world-model database requires an ownership marker"
            )
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{_DATABASE_MARKER}.", dir=canonical
        )
        temporary_marker = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(_DATABASE_MARKER_CONTENT)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_marker, marker)
        finally:
            temporary_marker.unlink(missing_ok=True)


_ReplacePath = Callable[[Path, Path], None]
_ReplaceCanonical = Callable[[Path, tuple[Path, ...], tuple[str, ...]], None]


def _validate_database_key_paths(key_paths: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(key_paths)
    if not normalized or any(
        len(path) != 16
        or any(character not in "0123456789abcdef" for character in path)
        for path in normalized
    ):
        raise ValueError("invalid owned Hypothesis database-key path")
    return normalized


def _seed_private_database(
    canonical: Path,
    private: Path,
    key_paths: Sequence[str],
) -> None:
    private.mkdir(parents=True, exist_ok=True)
    if not key_paths:
        return
    owned = _validate_database_key_paths(key_paths)
    with _canonical_lock(canonical):
        for key_path in owned:
            source = canonical / key_path
            if source.is_dir():
                shutil.copytree(source, private / key_path, dirs_exist_ok=True)


def replace_canonical_database(
    canonical: Path,
    private_databases: Sequence[Path],
    key_paths: Sequence[str],
    *,
    replace: _ReplacePath = os.replace,
) -> None:
    """Transactionally replace owned Hypothesis key directories with their union."""
    if not private_databases:
        raise ValueError("at least one private replay database is required")
    owned = _validate_database_key_paths(key_paths)
    prepare_canonical_database(canonical)
    with _canonical_lock(canonical):
        staging = Path(tempfile.mkdtemp(
            prefix=f".{canonical.name}.staging.",
            dir=canonical.parent,
        ))
        staging.rmdir()
        union = Path(tempfile.mkdtemp(
            prefix=f".{canonical.name}.union.",
            dir=canonical.parent,
        ))
        backup: Path | None = None
        try:
            shutil.copytree(canonical, staging)
            for private in private_databases:
                for key_path in owned:
                    source = private / key_path
                    if source.is_dir():
                        shutil.copytree(
                            source,
                            union / key_path,
                            dirs_exist_ok=True,
                        )
            for key_path in owned:
                destination = staging / key_path
                source = union / key_path
                if destination.exists():
                    shutil.rmtree(destination)
                if source.exists():
                    os.replace(source, destination)
            backup = Path(tempfile.mkdtemp(
                prefix=f".{canonical.name}.backup.",
                dir=canonical.parent,
            ))
            backup.rmdir()
            replace(canonical, backup)
            try:
                replace(staging, canonical)
            except Exception:
                replace(backup, canonical)
                backup = None
                raise
            # Cleanup is post-commit housekeeping. A cleanup-only filesystem
            # failure must not turn a complete visible generation into an
            # infrastructure abort that claims the canonical corpus was unchanged.
            shutil.rmtree(backup, ignore_errors=True)
            backup = None
        finally:
            shutil.rmtree(union, ignore_errors=True)
            shutil.rmtree(staging, ignore_errors=True)
            if backup is not None and backup.exists() and not canonical.exists():
                replace(backup, canonical)


def _write_replay_receipt(path: Path, replay: ReplayReceipt) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(msgspec.json.encode(replay))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _preserve_failure_database(
    private: Path,
    canonical: Path,
    artifact: Path,
    key_paths: Sequence[str],
) -> None:
    artifact.parent.mkdir(parents=True, exist_ok=True)
    if private.is_dir():
        shutil.copytree(private, artifact, dirs_exist_ok=True)
        replace_canonical_database(
            canonical,
            (canonical, private),
            key_paths,
        )


def _archive_private_database(private: Path, artifact: Path) -> None:
    artifact.parent.mkdir(parents=True, exist_ok=True)
    if private.is_dir():
        shutil.copytree(private, artifact, dirs_exist_ok=True)


def _target_wire(target: WorldTarget) -> bytes:
    return msgspec.json.encode(ChildInvocation(
        test_ids=target.test_ids,
        load_names=target.load_names or target.test_ids,
        generated=target.generated,
        seed=target.seed,
    ))


def _execute_target(
    index: int,
    target: WorldTarget,
    *,
    postgres: CoordinatorPostgres,
    base_environment: Mapping[str, str],
    active_root: Path,
    log_directory: Path,
    canonical_database: Path,
    failure_artifact_directory: Path,
    engine: str,
    mirror_url: str,
) -> TargetOutcome:
    started_at = time.monotonic()
    target_root = active_root / f"target-{index:04d}"
    target_root.mkdir()
    private_database = target_root / "hypothesis"
    _seed_private_database(
        canonical_database,
        private_database,
        target.database_key_paths if target.replay_corpus else (),
    )
    receipt_path = target_root / "receipt.json"
    log_path = log_directory / f"{index:04d}.log"
    database_name = ""
    result: TargetOutcome | None = None

    def infrastructure(
        detail: str,
        receipt: ChildReceipt | None = None,
    ) -> TargetOutcome:
        return TargetOutcome(
            target,
            "infrastructure_error",
            receipt,
            log_path,
            private_database,
            detail,
            elapsed_seconds=time.monotonic() - started_at,
        )

    try:
        database_name, dsn = postgres.create_clone()
        environment = child_environment(
            base_environment,
            target=target,
            dsn=dsn,
            target_root=target_root,
            engine=engine,
            mirror_url=mirror_url,
        )
        process = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--_run-child",
                _target_wire(target).decode(),
                str(receipt_path),
            ],
            cwd=REPO_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        log_path.write_text(process.stdout + process.stderr, encoding="utf-8")
        if process.returncode != 0 or not receipt_path.is_file():
            result = infrastructure(
                f"child exited {process.returncode} without a valid receipt"
            )
        else:
            wire = receipt_path.read_bytes()
            try:
                outcome = classify_receipt(target, wire)
                receipt = msgspec.json.decode(wire, type=ChildReceipt)
            except RuntimeError as error:
                result = infrastructure(str(error))
            else:
                with log_path.open("a", encoding="utf-8") as log:
                    log.write(receipt.output)
                result = TargetOutcome(
                    target,
                    outcome,
                    receipt,
                    log_path,
                    private_database,
                    elapsed_seconds=time.monotonic() - started_at,
                )
    except Exception as error:  # noqa: BLE001 - subprocess/DB boundary
        with log_path.open("a", encoding="utf-8") as log:
            log.write(f"{type(error).__name__}: {error}\n")
        result = infrastructure(f"{type(error).__name__}: {error}")

    assert result is not None
    if target.generated and result.outcome != "passed":
        try:
            if result.outcome == "property_failure":
                _preserve_failure_database(
                    private_database,
                    canonical_database,
                    failure_artifact_directory / "hypothesis",
                    target.database_key_paths,
                )
            else:
                _archive_private_database(
                    private_database,
                    failure_artifact_directory / "hypothesis",
                )
        except Exception as error:  # noqa: BLE001 - evidence boundary
            with log_path.open("a", encoding="utf-8") as log:
                log.write(
                    f"failure corpus preservation failed: "
                    f"{type(error).__name__}: {error}\n"
                )
            result = infrastructure(
                f"failure corpus preservation failed after {result.outcome}: "
                f"{type(error).__name__}: {error}",
                result.receipt,
            )

    if database_name:
        try:
            postgres.drop_clone(database_name)
        except Exception as error:  # noqa: BLE001 - teardown boundary
            with log_path.open("a", encoding="utf-8") as log:
                log.write(f"clone teardown failed: {type(error).__name__}: {error}\n")
            result = infrastructure(
                f"clone teardown failed after {result.outcome}: "
                f"{type(error).__name__}: {error}",
                result.receipt,
            )
    return result


def _discover(module_name: str, postgres: CoordinatorPostgres, temporary: Path) -> WorldManifest:
    receipt = temporary / "discovery.json"
    environment = dict(os.environ)
    environment.update({
        "TEST_DB_DSN": postgres.template_dsn,
        _SCHEMA_READY_ENV: "1",
        "CRATEDIGGER_WORLD_RANDOMIZED": "0",
        "PYTHONPATH": os.pathsep.join((str(REPO_ROOT), str(REPO_ROOT / "tests"))),
    })
    process = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--_discover", module_name, str(receipt)],
        cwd=REPO_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if process.returncode != 0 or not receipt.is_file():
        raise RuntimeError(
            f"world-model discovery failed ({process.returncode}): "
            f"{process.stdout}{process.stderr}"
        )
    try:
        manifest = msgspec.json.decode(receipt.read_bytes(), type=WorldManifest)
    except (msgspec.DecodeError, msgspec.ValidationError) as error:
        raise RuntimeError(f"malformed discovery receipt: {error}") from error
    if not manifest.test_ids or len(set(manifest.test_ids)) != len(manifest.test_ids):
        raise RuntimeError("world-model discovery returned empty or duplicate IDs")
    return manifest


def _parse_positive(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _parse_seed(value: str) -> int:
    parsed = int(value, 0)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the randomized real-storage lifecycle hammer in isolated multicore shards."
    )
    parser.add_argument("--examples", type=_parse_positive, default=_parse_positive(os.getenv("CRATEDIGGER_WORLD_EXAMPLES", str(DEFAULT_EXAMPLES))))
    parser.add_argument("--steps", type=_parse_positive, default=_parse_positive(os.getenv("CRATEDIGGER_WORLD_STEPS", str(DEFAULT_STEPS))))
    parser.add_argument("--database", default=os.getenv("CRATEDIGGER_WORLD_DATABASE", ".hypothesis/world-model"))
    parser.add_argument("--engine", choices=("in-process", "mirror-harness"), default=os.getenv("CRATEDIGGER_WORLD_ENGINE", "in-process"))
    parser.add_argument("--mirror-url", default=os.getenv("CRATEDIGGER_WORLD_MIRROR_URL", ""))
    parser.add_argument("--jobs", type=_parse_positive, default=None)
    parser.add_argument("--seed", type=_parse_seed)
    parser.add_argument("--output-dir", default=os.getenv("CRATEDIGGER_WORLD_OUTPUT_DIR", ".hypothesis/world-model-runs"))
    parser.add_argument("--print-config", action="store_true")
    return parser


def _resolved_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (REPO_ROOT / path).resolve()


def main(
    argv: Sequence[str] | None = None,
    *,
    replace_canonical: _ReplaceCanonical = replace_canonical_database,
) -> int:
    os.environ.pop("TEST_DB_DSN", None)
    os.environ.pop(_SCHEMA_READY_ENV, None)
    args = _parser().parse_args(argv)
    if not args.database:
        print("database path must be non-empty", file=sys.stderr)
        return 2
    if args.engine == "mirror-harness" and not args.mirror_url:
        print("--mirror-url is required for mirror-harness", file=sys.stderr)
        return 2
    jobs = effective_jobs(args.jobs, args.engine)
    root_seed = args.seed if args.seed is not None else secrets.randbits(64)
    module_name = (
        "tests.world_model.mirror_harness"
        if args.engine == "mirror-harness"
        else "tests.world_model.state_machine"
    )
    expected_generated = 1 if args.engine == "mirror-harness" else 5
    print(f"root_seed={root_seed}")
    if args.print_config:
        print(f"examples={args.examples}")
        print(f"steps={args.steps}")
        print("randomized=true")
        print("postgres=ephemeral-coordinator")
        print("beets=private-per-target")
        print(f"engine={args.engine}")
        if args.mirror_url:
            print(f"mirror_url={args.mirror_url}")
        print(f"database={args.database}")
        print(f"jobs={jobs}")
        print(f"output_dir={args.output_dir}")
        return 0

    canonical_database = _resolved_path(args.database)
    output_root = _resolved_path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    run_directory = Path(tempfile.mkdtemp(prefix="run.", dir=output_root))
    logs = run_directory / "logs"
    logs.mkdir()
    started = time.monotonic()
    outcomes: list[TargetOutcome] = []
    not_started: list[WorldTarget] = []
    planned_targets: tuple[WorldTarget, ...] = ()
    admission_aborted = False
    coordinator_error: str | None = None
    try:
        prepare_canonical_database(canonical_database)
        with tempfile.TemporaryDirectory(prefix="cratedigger_world_burst_") as temporary_name:
            temporary = Path(temporary_name)
            with CoordinatorPostgres() as postgres:
                manifest = _discover(module_name, postgres, temporary)
                targets = build_targets(
                    manifest,
                    examples=args.examples,
                    jobs=jobs,
                    root_seed=root_seed,
                    steps=args.steps,
                    expected_generated=expected_generated,
                    expected_pins=19 if args.engine == "in-process" else None,
                )
                planned_targets = targets
                print(
                    f"world-model burst: {len(manifest.test_ids)} tests, "
                    f"{len(targets)} targets, up to {jobs} parallel",
                    flush=True,
                )
                pending = list(enumerate(targets))
                with ThreadPoolExecutor(max_workers=jobs) as executor:
                    active: dict[Future[TargetOutcome], tuple[int, WorldTarget]] = {}
                    while active or (pending and not admission_aborted):
                        while pending and len(active) < jobs and not admission_aborted:
                            index, target = pending.pop(0)
                            future = executor.submit(
                                _execute_target,
                                index,
                                target,
                                postgres=postgres,
                                base_environment=os.environ,
                                active_root=temporary,
                                log_directory=logs,
                                canonical_database=canonical_database,
                                failure_artifact_directory=(
                                    run_directory / "failures" / f"{index:04d}"
                                ),
                                engine=args.engine,
                                mirror_url=args.mirror_url,
                            )
                            active[future] = (index, target)
                        if not active:
                            break
                        done, _ = wait(tuple(active), return_when=FIRST_COMPLETED)
                        for future in done:
                            _index, target = active.pop(future)
                            try:
                                outcome = future.result()
                            except Exception as error:  # noqa: BLE001 - future boundary
                                outcome = TargetOutcome(
                                    target, "infrastructure_error", None,
                                    logs / "coordinator-error.log", temporary / "missing",
                                    f"{type(error).__name__}: {error}",
                                )
                            outcomes.append(outcome)
                            print(
                                f"world-model progress: {len(outcomes)}/{len(targets)} "
                                f"{outcome.outcome} {target.label} "
                                f"in {outcome.elapsed_seconds:.1f}s",
                                flush=True,
                            )
                            if outcome.outcome != "passed":
                                admission_aborted = True
                            if outcome.outcome == "infrastructure_error":
                                print(f"INFRASTRUCTURE FAIL {target.label}", flush=True)
                    not_started = [target for _index, target in pending]
                if outcomes and not not_started and all(
                    outcome.outcome == "passed" for outcome in outcomes
                ):
                    owned_key_paths = tuple(sorted({
                        key_path
                        for target in targets
                        for key_path in target.database_key_paths
                    }))
                    replace_canonical(
                        canonical_database,
                        tuple(outcome.database_path for outcome in outcomes),
                        owned_key_paths,
                    )
    except Exception as error:  # noqa: BLE001 - coordinator fail-closed boundary
        coordinator_error = f"{type(error).__name__}: {error}"
        admission_aborted = True
        (logs / "coordinator-error.log").write_text(
            traceback.format_exc(), encoding="utf-8"
        )
        print(
            f"world-model burst: INFRASTRUCTURE ABORT: {coordinator_error}",
            file=sys.stderr,
        )

    if coordinator_error is not None:
        completed_targets = {outcome.target for outcome in outcomes}
        not_started = [
            target for target in planned_targets if target not in completed_targets
        ]

    target_order = {
        target: index for index, target in enumerate(planned_targets)
    }
    outcomes.sort(key=lambda outcome: target_order[outcome.target])
    failures = [outcome for outcome in outcomes if outcome.outcome != "passed"]
    for outcome in failures:
        print(f"{outcome.outcome.upper()} {outcome.target.label}: {outcome.detail}")
        if outcome.log_path.is_file():
            print(outcome.log_path.read_text(encoding="utf-8", errors="replace")[-12000:])
    elapsed = time.monotonic() - started
    replay = ReplayReceipt(
        root_seed=root_seed,
        engine=args.engine,
        examples=args.examples,
        steps=args.steps,
        jobs=jobs,
        elapsed_seconds=elapsed,
        admission_aborted=admission_aborted,
        targets=tuple(ReplayTarget(
            label=outcome.target.label,
            logical_id=outcome.target.logical_id,
            test_ids=outcome.target.test_ids,
            shard_index=outcome.target.shard_index,
            shard_count=outcome.target.shard_count,
            examples=outcome.target.examples,
            steps=outcome.target.steps,
            seed=outcome.target.seed,
            outcome=outcome.outcome,
            elapsed_seconds=outcome.elapsed_seconds,
        ) for outcome in outcomes),
        not_started=tuple(ReplayTarget(
            label=target.label,
            logical_id=target.logical_id,
            test_ids=target.test_ids,
            shard_index=target.shard_index,
            shard_count=target.shard_count,
            examples=target.examples,
            steps=target.steps,
            seed=target.seed,
            outcome="not_started",
            elapsed_seconds=0.0,
        ) for target in not_started),
        coordinator_error=coordinator_error,
    )
    _write_replay_receipt(run_directory / "replay.json", replay)
    if coordinator_error is not None:
        print(
            f"world-model burst: INFRASTRUCTURE ABORT after {elapsed:.1f}s "
            f"({len(outcomes)} completed; {len(not_started)} not started; "
            f"receipts={run_directory})"
        )
        return 1
    if failures:
        infra = any(outcome.outcome == "infrastructure_error" for outcome in failures)
        properties = any(
            outcome.outcome == "property_failure" for outcome in failures
        )
        label = (
            "INFRASTRUCTURE ABORT"
            if infra
            else "PROPERTY FAILURES"
            if properties
            else "TEST FAILURES"
        )
        print(
            f"world-model burst: {label} after {elapsed:.1f}s "
            f"({len(outcomes)} completed; {len(not_started)} not started; "
            f"receipts={run_directory})"
        )
        return 1
    print(
        f"world-model burst: ALL GREEN in {elapsed:.1f}s "
        f"({len(outcomes)} targets; receipts={run_directory})"
    )
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--_discover":
        raise SystemExit(_discover_child(sys.argv[2], Path(sys.argv[3])))
    if len(sys.argv) == 4 and sys.argv[1] == "--_run-child":
        raise SystemExit(_run_child(sys.argv[2].encode(), Path(sys.argv[3])))
    raise SystemExit(main())
