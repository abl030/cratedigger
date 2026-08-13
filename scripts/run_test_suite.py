#!/usr/bin/env python3
"""Run every deterministic validation phase and publish one failure bundle."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import os
import re
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TextIO

import msgspec

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_VERSION = 1
RUNNER_VERSION = "1"
CANONICAL_COMMAND = "bash scripts/run_tests.sh"
FAILURE_MARKER_PREFIX = "CRATEDIGGER_CHECK_FAILURE "
METRICS_MARKER_PREFIX = "CRATEDIGGER_CHECK_METRICS "

ParserKind = Literal[
    "generic",
    "js-syntax",
    "js-unit",
    "pyright",
    "ruff",
    "vulture",
    "python",
]
PhaseState = Literal[
    "not-run",
    "running",
    "passed",
    "failed",
    "infrastructure-failure",
    "interrupted",
]
SuiteState = Literal[
    "running",
    "passed",
    "failed",
    "infrastructure-failure",
    "interrupted",
]


@dataclass(frozen=True)
class PhaseSpec:
    """One independently reportable validation phase."""

    name: str
    command: tuple[str, ...]
    rerun_command: str
    parser: ParserKind
    failure_exit_codes: tuple[int, ...] = (1,)


@dataclass(frozen=True)
class PhaseExecution:
    """Raw process outcome returned by a phase executor."""

    exit_code: int
    elapsed_seconds: float
    infrastructure_error: str | None = None


class CheckFailureMarker(msgspec.Struct, frozen=True):
    """Structured failure marker emitted by the Python test scheduler."""

    identity: str
    owner: str
    detail: str
    test_ids: tuple[str, ...] = ()


class CheckMetricsMarker(msgspec.Struct, frozen=True):
    """Structured completion metrics emitted by a validation phase."""

    tests_run: int = 0
    targets_run: int = 0
    scheduled_targets: int = 0


class CheckFailure(msgspec.Struct, frozen=True):
    """One compact failure-index entry linking to complete evidence."""

    identity: str
    owner: str
    detail: str
    rerun_command: str
    log: str
    test_ids: tuple[str, ...] = ()


class CheckPhase(msgspec.Struct, frozen=True):
    """Persisted state and evidence for one validation phase."""

    name: str
    state: PhaseState
    command: tuple[str, ...]
    rerun_command: str
    log: str
    started_at: str | None = None
    finished_at: str | None = None
    elapsed_seconds: float = 0.0
    exit_code: int | None = None
    failures: tuple[CheckFailure, ...] = ()
    tests_run: int = 0
    targets_run: int = 0
    scheduled_targets: int = 0


class CheckSummary(msgspec.Struct, frozen=True):
    """Stable bundle schema for one canonical full-suite invocation."""

    schema_version: int
    runner_version: str
    state: SuiteState
    command: str
    repo_root: str
    head: str
    dirty: bool
    dirty_state_sha256: str
    bundle: str
    started_at: str
    finished_at: str | None
    elapsed_seconds: float
    phases: tuple[CheckPhase, ...]
    interruption_signal: int | None = None


@dataclass(frozen=True)
class SuiteRun:
    """In-process result used by the CLI and contract tests."""

    exit_code: int
    bundle: Path
    summary: CheckSummary


PhaseExecutor = Callable[[PhaseSpec, tuple[str, ...], Path], PhaseExecution]

_active_process: subprocess.Popen[bytes] | None = None


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _git_bytes(repo_root: Path, *args: str) -> bytes:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode(errors="replace").strip()
        raise RuntimeError(f"git {' '.join(args)} failed: {detail}")
    return completed.stdout


def dirty_state_fingerprint(repo_root: Path) -> tuple[bool, str]:
    """Hash tracked diffs and every untracked file into one tree fingerprint."""
    status = _git_bytes(
        repo_root,
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
    )
    digest = hashlib.sha256()
    digest.update(b"status\0")
    digest.update(status)
    digest.update(b"\0tracked-diff\0")
    digest.update(_git_bytes(repo_root, "diff", "--binary", "HEAD", "--"))
    untracked = _git_bytes(
        repo_root,
        "ls-files",
        "--others",
        "--exclude-standard",
        "-z",
    )
    for raw_relative in untracked.split(b"\0"):
        if not raw_relative:
            continue
        relative = os.fsdecode(raw_relative)
        path = repo_root / relative
        digest.update(b"\0untracked\0")
        digest.update(raw_relative)
        digest.update(b"\0")
        if path.is_symlink():
            digest.update(b"symlink\0")
            digest.update(os.fsencode(os.readlink(path)))
        elif path.is_file():
            digest.update(b"file\0")
            digest.update(path.read_bytes())
        else:
            digest.update(b"special")
    return bool(status), digest.hexdigest()


def private_runtime_dir(candidate: Path | None = None) -> Path:
    """Resolve and validate the caller-owned runtime tmpfs."""
    runtime = candidate or Path(
        os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
    )
    try:
        info = runtime.lstat()
    except OSError as exc:
        raise RuntimeError(f"private runtime directory is unavailable: {runtime}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise RuntimeError(f"private runtime directory is not a real directory: {runtime}")
    if info.st_uid != os.getuid():
        raise RuntimeError(f"private runtime directory is not owned by this user: {runtime}")
    if stat.S_IMODE(info.st_mode) != 0o700:
        raise RuntimeError(f"private runtime directory is not mode 0700: {runtime}")
    resolved = runtime.resolve(strict=True)
    completed = subprocess.run(
        ["findmnt", "-no", "FSTYPE", "-T", str(resolved)],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0 or completed.stdout.strip() != "tmpfs":
        raise RuntimeError(f"private runtime directory is not tmpfs: {runtime}")
    return resolved


#: Bound for a second concurrently-launched canonical suite waiting on the
#: exclusive test-RAM-root admission lock (issue #1111). Generous enough to
#: outlast one full canonical suite's own runtime; still bounded, so a wait
#: never becomes a silent hang.
DEFAULT_ADMISSION_TIMEOUT_SECONDS = 1200.0
DEFAULT_ADMISSION_POLL_SECONDS = 1.0
DEFAULT_ADMISSION_PROGRESS_INTERVAL_SECONDS = 30.0

#: Same env var and default as scripts/test_tmpfs.sh's own shell-entry
#: headroom guard (CRATEDIGGER_TEST_RAM_MIN_BYTES) — one threshold, not two
#: that can silently drift apart.
DEFAULT_MIN_HEADROOM_BYTES = 1_073_741_824

#: A completed check bundle survives at least this long before admission-time
#: reaping may remove it. This protects the bundle's DETAILED EVIDENCE (the
#: per-phase logs and summary.md a run_final_gate.sh receipt points at), not
#: receipt reuse itself: `run_final_gate.sh status` only confirms the
#: receipt's own `terminal` and `bundle` FILE exist (a path string) — a
#: `pass` receipt's verdict outlives its bundle regardless of this floor.
#: `status_receipt` separately stats the bundle path and fails visibly when
#: it is gone (issue #1111 review). The floor's real job is keeping that
#: detailed evidence around for one ordinary session (CLAUDE.md "Running
#: tests"), not proving the receipt itself is still trustworthy. Reaping
#: only ever runs from `run_suite` while holding the admission lock
#: exclusively, so any bundle another `run_suite` call created predates this
#: run regardless of age — see the narrower claim in
#: `reap_stale_check_bundles` below; the age gate exists purely to protect
#: near-term evidence, not to prove liveness.
DEFAULT_STALE_BUNDLE_MAX_AGE_SECONDS = 4 * 60 * 60

#: The one named identity every RAM-root-exhaustion signal uses, at suite
#: start (this module) and mid-run (scripts/run_python_tests.py) alike —
#: issue #1111 item 2's single failure-index entry instead of N disguised
#: test failures.
TEST_RAM_ROOT_EXHAUSTED = "test RAM root exhausted"


class SuiteAdmissionTimeout(RuntimeError):
    """The bounded wait for exclusive canonical-suite admission expired."""


class RamRootExhaustedError(RuntimeError):
    """The shared test RAM root lacks the headroom a suite run requires."""


def admission_lock_path(runtime: Path) -> Path:
    """Lockfile scoped to the shared test RAM root, not to any one suite run."""
    return runtime / ".cratedigger-test-admission.lock"


def _proc_start_ticks(pid: int) -> str | None:
    """Field 22 of /proc/<pid>/stat — mirrors run_final_gate.sh's proc_start_ticks."""
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    close = raw.rfind(")")
    if close == -1:
        return None
    fields = raw[close + 2 :].split()
    if len(fields) < 20:
        return None
    return fields[19]


def _write_lock_holder_identity(descriptor: int) -> None:
    """Best-effort holder identity a waiter's progress message can read.

    Mirrors run_final_gate.sh's own pid+start-ticks identity precedent
    (helper_pid/helper_start_ticks, gate_pid/gate_start_ticks): a bare pid is
    ambiguous across process reuse, start ticks disambiguate it. This write
    can itself be lost to the very exhaustion this PR exists for (the
    ``except OSError: pass`` below) — safe only because the identity is
    always cleared on release first (``_clear_lock_holder_identity``), so a
    lost write degrades to an empty file, never a stale one, and the reader
    additionally verifies liveness before trusting any content it finds.
    """
    ticks = _proc_start_ticks(os.getpid()) or "0"
    payload = f"{os.getpid()} {ticks}\n".encode()
    try:
        os.ftruncate(descriptor, 0)
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.write(descriptor, payload)
    except OSError:
        pass


def _clear_lock_holder_identity(descriptor: int) -> None:
    """Erase the holder identity before releasing the lock.

    Must run while still holding the exclusive flock (issue #1111 review
    MAJOR-2): without this, a released holder's identity lingers in the
    lockfile until the next acquirer overwrites it, so a waiter reading in
    that window would confidently name a pid that no longer holds — or no
    longer exists at all. Clearing before unlock also means a delayed clear
    can never race a new holder's own write and wipe it.
    """
    try:
        os.ftruncate(descriptor, 0)
    except OSError:
        pass


def _read_lock_holder_identity(lock_path: Path) -> str | None:
    """The lock's live holder identity, or None if stale/malformed/unreadable.

    A stored pid+ticks pair is trusted only after an explicit liveness
    check (``_proc_start_ticks(pid) == ticks``, the same ``same_process``
    precedent ``run_final_gate.sh`` uses) — never on readability alone.
    Without it, a lingering write from a process that has since released or
    died (a write the release path failed to clear, or a write the current
    holder's own ``except OSError: pass`` swallowed) would be reported as
    live. A mismatch or a dead pid falls back to ``None``, and the caller
    falls back to the plain lockfile-path message.
    """
    try:
        content = lock_path.read_text().strip()
    except OSError:
        return None
    parts = content.split()
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        return None
    pid_str, ticks = parts
    if _proc_start_ticks(int(pid_str)) != ticks:
        return None
    return f"pid {pid_str}, start ticks {ticks}"


@contextmanager
def acquire_suite_admission(
    runtime: Path,
    *,
    stream: TextIO,
    timeout_seconds: float = DEFAULT_ADMISSION_TIMEOUT_SECONDS,
    poll_seconds: float = DEFAULT_ADMISSION_POLL_SECONDS,
    progress_interval_seconds: float = DEFAULT_ADMISSION_PROGRESS_INTERVAL_SECONDS,
) -> Generator[None]:
    """Serialize every ``run_suite`` invocation sharing one fixed-size test RAM root.

    Scoped to the suite-runner level (this module's ``run_suite``) — never to
    every ``nix-shell`` entry, which would also serialize interactive dev
    shells that never call ``run_suite`` at all.

    Targeted runs DO take this same lock: ``scripts/test.sh`` ->
    ``scripts/run_targeted_tests.py`` -> this same ``run_suite``, with no
    ``runtime_dir`` override, so it resolves the identical shared root and
    contends for the identical lockfile as the canonical suite. This is
    deliberate, not an oversight — issue #1111's own incident record
    includes a ``scripts/test.sh`` run hitting ``BrokenProcessPool`` at host
    load ~46, a full canonical suite is short-lived relative to the default
    wait budget below (an operator can raise it via
    ``admission_timeout_seconds`` on any installation where that stops being
    true), and the reap-safety argument in ``reap_stale_check_bundles``
    below depends on every caller of this function serializing through it.
    A second concurrently-launched ``run_suite`` call — canonical or
    targeted — waits here, bounded, reporting what it is waiting for
    (including the current holder's identity when verified live), instead
    of colliding with the first one's roughly a dozen ephemeral PostgreSQL
    clusters.
    """
    lock_path = admission_lock_path(runtime)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        deadline = time.monotonic() + timeout_seconds
        reported = False
        next_progress = 0.0
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                now = time.monotonic()
                if now >= deadline:
                    raise SuiteAdmissionTimeout(
                        f"timed out after {timeout_seconds:.0f}s waiting for "
                        f"exclusive test admission: {lock_path} is held by "
                        "another canonical suite run"
                    ) from None
                if not reported or now >= next_progress:
                    holder = _read_lock_holder_identity(lock_path)
                    holder_note = f" ({holder})" if holder else ""
                    stream.write(
                        f"waiting for admission: another canonical suite{holder_note} "
                        f"holds the test RAM root ({lock_path}); "
                        f"{deadline - now:.0f}s left before timeout\n"
                    )
                    stream.flush()
                    reported = True
                    next_progress = now + progress_interval_seconds
                time.sleep(min(poll_seconds, max(0.0, deadline - now)))
        _write_lock_holder_identity(descriptor)
        if reported:
            stream.write(f"admission acquired: exclusive lock on {lock_path}\n")
            stream.flush()
        yield
    finally:
        try:
            _clear_lock_holder_identity(descriptor)
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)


def _bundle_last_activity(bundle: Path) -> float:
    """The bundle's most recent evidence write, or its own mtime as a fallback."""
    summary_path = bundle / "summary.json"
    try:
        return summary_path.stat().st_mtime
    except OSError:
        pass
    try:
        return bundle.stat().st_mtime
    except OSError:
        return 0.0


#: Directory-name prefixes the reaper protects the shared runtime tmpfs from
#: leaking forever: ``run_suite``'s own check bundles, plus this test-infra
#: change's own per-test scratch directories (``tests/test_suite_coordinator.py``)
#: — a killed test process (SIGKILL, OOM) leaks its ``tempfile.mkdtemp``
#: fixture before ``tearDown`` can remove it, in the exact directory this PR
#: exists to keep clean. Age-gated the same way as check bundles; a fresh
#: fixture belonging to a currently-running test is never touched.
_REAPABLE_PREFIXES = (
    "cratedigger-checks.",
    "cratedigger-suite-test-",
    "cratedigger-admission-test-",
    "cratedigger-reap-test-",
    "cratedigger-headroom-test-",
)


def _receipt_protected_bundles(runtime: Path) -> frozenset[Path]:
    """Bundle paths a run_final_gate.sh receipt still references.

    ``cratedigger-final-gate.*`` receipts are never themselves in
    ``_REAPABLE_PREFIXES`` (a different prefix, never reaped), so protecting
    the bundle path they name preserves both the pass/fail verdict AND its
    evidence for a long-running review (issue #1111 review m13) — without
    this, a review that outlives the age floor would lose the bundle out
    from under a still-valid receipt. If a protected bundle is somehow gone
    anyway, that is a genuinely dangling receipt; this function's exclusion
    does not paper over it — ``run_final_gate.sh status``'s own stat check
    (issue #1111 review m5) is what surfaces that honestly.
    """
    protected: set[Path] = set()
    for receipt in sorted(runtime.glob("cratedigger-final-gate.*")):
        try:
            content = (receipt / "bundle").read_text().strip()
        except OSError:
            continue
        if content:
            protected.add(Path(content))
    return frozenset(protected)


def reap_stale_check_bundles(
    runtime: Path,
    *,
    max_age_seconds: float = DEFAULT_STALE_BUNDLE_MAX_AGE_SECONDS,
    reference_time: float | None = None,
) -> tuple[Path, ...]:
    """Best-effort cleanup of scratch directories nothing can still be writing.

    Only ever called from ``run_suite``, before it creates its own bundle,
    while holding ``acquire_suite_admission`` exclusively (issue #1111 item
    1). That guarantees every ``cratedigger-checks.*`` directory belonging to
    ANOTHER ``run_suite`` invocation — including one a crashed prior holder
    left mid-run — predates this run: no other ``run_suite`` call can be
    concurrently writing a new one under the same lock. It does not cover
    every possible creator of a matched prefix: a test fixture can (and
    does — ``tests/test_final_gate_receipt.py``) construct a
    ``cratedigger-checks.*`` directory directly, entirely outside
    ``run_suite``. The age gate is what actually protects those, not lock
    exclusivity, which is also why it covers this PR's own test-scaffolding
    prefixes above — a killed test process can leak one with no
    ``run_suite`` involved at all. A bundle a live receipt still references
    (``_receipt_protected_bundles``) is never reaped regardless of age.
    """
    reference = reference_time if reference_time is not None else time.time()
    protected = _receipt_protected_bundles(runtime)
    reaped: list[Path] = []
    candidates = sorted(
        {
            candidate
            for prefix in _REAPABLE_PREFIXES
            for candidate in runtime.glob(f"{prefix}*")
        }
    )
    for candidate in candidates:
        if candidate in protected:
            continue
        try:
            info = candidate.lstat()
        except OSError:
            continue
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            continue
        if info.st_uid != os.getuid():
            continue
        age = reference - _bundle_last_activity(candidate)
        if age < max_age_seconds:
            continue
        try:
            shutil.rmtree(candidate)
        except OSError:
            continue
        reaped.append(candidate)
    return tuple(reaped)


def _default_min_headroom_bytes() -> int:
    """Read the same env var and default scripts/test_tmpfs.sh uses."""
    raw = os.environ.get("CRATEDIGGER_TEST_RAM_MIN_BYTES")
    if raw is None:
        return DEFAULT_MIN_HEADROOM_BYTES
    try:
        value = int(raw)
    except ValueError:
        value = -1
    if value < 0:
        raise ValueError(
            "CRATEDIGGER_TEST_RAM_MIN_BYTES must be a non-negative integer"
        )
    return value


def _check_suite_headroom(runtime: Path, *, minimum_bytes: int) -> None:
    """Fail the whole suite once, immediately — never mid-run (issue #1111).

    Measures the same root scripts/test_tmpfs.sh's shell-entry guard does:
    ``CRATEDIGGER_TEST_RAM_ROOT`` when an operator has overridden it, else
    the validated ``runtime`` this suite was actually admitted under.
    """
    target = Path(os.environ.get("CRATEDIGGER_TEST_RAM_ROOT") or runtime)
    available = shutil.disk_usage(target).free
    if available < minimum_bytes:
        raise RamRootExhaustedError(
            f"{TEST_RAM_ROOT_EXHAUSTED}: {target} has {available} bytes "
            f"free, needs {minimum_bytes}"
        )


def _create_bundle(runtime: Path) -> Path:
    bundle = Path(tempfile.mkdtemp(prefix="cratedigger-checks.", dir=runtime))
    bundle.chmod(0o700)
    return bundle


def _write_private(path: Path, data: bytes) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(data)
    temporary.chmod(0o600)
    os.replace(temporary, path)


def _escape_markdown(value: str) -> str:
    return value.replace("|", r"\|").replace("\n", " ")


def _summary_markdown(summary: CheckSummary) -> str:
    lines = [
        "# Cratedigger check bundle",
        "",
        f"- State: `{summary.state}`",
        f"- Command: `{summary.command}`",
        f"- Repository: `{summary.repo_root}`",
        f"- HEAD: `{summary.head}`",
        f"- Dirty: `{str(summary.dirty).lower()}`",
        f"- Dirty-state SHA-256: `{summary.dirty_state_sha256}`",
        f"- Started: `{summary.started_at}`",
        f"- Finished: `{summary.finished_at or 'not finished'}`",
        f"- Bundle: `{summary.bundle}`",
        "",
        "## Phases",
        "",
        "| Phase | State | Exit | Time | Failures | Tests | Targets | Log |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for phase in summary.phases:
        lines.append(
            f"| {_escape_markdown(phase.name)} | {phase.state} | "
            f"{phase.exit_code if phase.exit_code is not None else '-'} | "
            f"{phase.elapsed_seconds:.2f}s | {len(phase.failures)} | "
            f"{phase.tests_run} | {phase.targets_run}/{phase.scheduled_targets} | "
            f"`{phase.log}` |"
        )
    failures = [
        (phase, failure)
        for phase in summary.phases
        for failure in phase.failures
    ]
    lines.extend(["", "## Failure index", ""])
    if not failures:
        lines.append("No indexed failures.")
    for phase, failure in failures:
        lines.extend(
            [
                f"### {phase.name}: `{_escape_markdown(failure.identity)}`",
                "",
                f"- Owner: `{_escape_markdown(failure.owner or 'unknown')}`",
                f"- Detail: {_escape_markdown(failure.detail)}",
                f"- Rerun: `{_escape_markdown(failure.rerun_command)}`",
                f"- Complete log: `{_escape_markdown(failure.log)}`",
            ]
        )
        if failure.test_ids:
            lines.append(
                "- Test IDs: "
                + ", ".join(f"`{_escape_markdown(item)}`" for item in failure.test_ids)
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _publish_summary(bundle: Path, summary: CheckSummary) -> None:
    _write_private(bundle / "summary.json", msgspec.json.encode(summary))
    _write_private(
        bundle / "summary.md",
        _summary_markdown(summary).encode(),
    )


def _default_phases() -> tuple[PhaseSpec, ...]:
    return (
        PhaseSpec(
            "js-syntax",
            ("bash", "scripts/run_js_checks.sh", "syntax"),
            "bash scripts/run_js_checks.sh syntax",
            "js-syntax",
        ),
        PhaseSpec(
            "js-unit",
            ("bash", "scripts/run_js_checks.sh", "unit"),
            "bash scripts/run_js_checks.sh unit",
            "js-unit",
        ),
        PhaseSpec(
            "pyright",
            ("python3", "scripts/run_pyright_checks.py"),
            "python3 scripts/run_pyright_checks.py",
            "pyright",
        ),
        PhaseSpec(
            "ruff",
            ("bash", "scripts/run_ruff.sh"),
            "bash scripts/run_ruff.sh",
            "ruff",
        ),
        PhaseSpec(
            "vulture",
            ("bash", "scripts/find_dead_code.sh"),
            "bash scripts/find_dead_code.sh",
            "vulture",
            (3,),
        ),
        PhaseSpec(
            "python",
            ("python3", "scripts/run_python_tests.py"),
            "python3 scripts/run_python_tests.py",
            "python",
        ),
    )


def execute_phase(
    _phase: PhaseSpec,
    command: tuple[str, ...],
    log_path: Path,
) -> PhaseExecution:
    """Run one command into its complete private phase log."""
    global _active_process
    started = time.monotonic()
    try:
        with log_path.open("wb") as output:
            try:
                process = subprocess.Popen(
                    command,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            except OSError as exc:
                detail = f"{type(exc).__name__}: {exc}"
                output.write(f"infrastructure failure: {detail}\n".encode())
                return PhaseExecution(
                    exit_code=127,
                    elapsed_seconds=time.monotonic() - started,
                    infrastructure_error=detail,
                )
            _active_process = process
            exit_code = process.wait()
    finally:
        _active_process = None
        if log_path.exists():
            log_path.chmod(0o600)
    elapsed = time.monotonic() - started
    if exit_code < 0:
        signum = -exit_code
        return PhaseExecution(
            exit_code=128 + signum,
            elapsed_seconds=elapsed,
            infrastructure_error=f"phase terminated by signal {signum}",
        )
    return PhaseExecution(exit_code=exit_code, elapsed_seconds=elapsed)


_PYRIGHT = re.compile(
    r"^(?P<owner>.+?):(?P<line>\d+):(?P<column>\d+) - error: (?P<detail>.+)$"
)
_RUFF = re.compile(
    r"^(?P<owner>.+?):(?P<line>\d+):(?P<column>\d+): "
    r"(?P<code>[A-Z]+\d+) (?P<detail>.+)$"
)
_RUFF_FULL_HEADER = re.compile(r"^(?P<code>[A-Z]+\d+) (?P<detail>.+)$")
_RUFF_FULL_LOCATION = re.compile(
    r"^\s*-->\s*(?P<owner>.+?):(?P<line>\d+):(?P<column>\d+)$"
)
_VULTURE = re.compile(
    r"^(?P<owner>.+?):(?P<line>\d+): (?P<detail>.+? \(\d+% confidence\))$"
)
_VULTURE_FRESHNESS = re.compile(
    r"^\+(?P<identity>\S+)\s+# (?P<detail>unused .+?) "
    r"\((?P<owner>[^:]+):(?P<line>\d+)\)$"
)


def _indexed_failure(
    *,
    identity: str,
    owner: str,
    detail: str,
    rerun_command: str,
    log: str,
    test_ids: tuple[str, ...] = (),
) -> CheckFailure:
    return CheckFailure(
        identity=identity,
        owner=owner,
        detail=detail,
        rerun_command=rerun_command,
        log=log,
        test_ids=test_ids,
    )


def _parse_failures(
    phase: PhaseSpec,
    log_path: Path,
) -> tuple[tuple[CheckFailure, ...], CheckMetricsMarker]:
    output = log_path.read_text(encoding="utf-8", errors="replace")
    failures: list[CheckFailure] = []
    metrics = CheckMetricsMarker()
    pending_ruff: tuple[str, str] | None = None
    for line in output.splitlines():
        if phase.parser in {"js-syntax", "js-unit"} and line.startswith(
            "CRATEDIGGER_JS_FAILURE\t"
        ):
            fields = line.split("\t", 2)
            if len(fields) != 3:
                raise ValueError(f"malformed JavaScript failure marker: {line}")
            identity, detail = fields[1:]
            rerun = (
                "node --check --input-type=module "
                f"< {shlex.quote(identity)}"
                if phase.parser == "js-syntax"
                else f"node {shlex.quote(identity)}"
            )
            failures.append(
                _indexed_failure(
                    identity=identity,
                    owner=identity,
                    detail=detail,
                    rerun_command=rerun,
                    log=log_path.name,
                )
            )
            continue
        if phase.parser == "pyright" and (match := _PYRIGHT.match(line)):
            owner = match.group("owner").strip()
            failures.append(
                _indexed_failure(
                    identity=(
                        f"{owner}:{match.group('line')}:{match.group('column')}"
                    ),
                    owner=owner,
                    detail=match.group("detail"),
                    rerun_command=phase.rerun_command,
                    log=log_path.name,
                )
            )
            continue
        if phase.parser == "ruff" and (match := _RUFF.match(line)):
            owner = match.group("owner").strip()
            failures.append(
                _indexed_failure(
                    identity=(
                        f"{owner}:{match.group('line')}:{match.group('column')}"
                    ),
                    owner=owner,
                    detail=f"{match.group('code')} {match.group('detail')}",
                    rerun_command=f"bash scripts/run_ruff.sh {shlex.quote(owner)}",
                    log=log_path.name,
                )
            )
            continue
        if phase.parser == "ruff" and (match := _RUFF_FULL_HEADER.match(line)):
            pending_ruff = (match.group("code"), match.group("detail"))
            continue
        if (
            phase.parser == "ruff"
            and pending_ruff is not None
            and (match := _RUFF_FULL_LOCATION.match(line))
        ):
            owner = match.group("owner").strip()
            code, detail = pending_ruff
            failures.append(
                _indexed_failure(
                    identity=(
                        f"{owner}:{match.group('line')}:{match.group('column')}"
                    ),
                    owner=owner,
                    detail=f"{code} {detail}",
                    rerun_command=f"bash scripts/run_ruff.sh {shlex.quote(owner)}",
                    log=log_path.name,
                )
            )
            pending_ruff = None
            continue
        if phase.parser == "vulture" and (match := _VULTURE.match(line)):
            owner = match.group("owner").strip()
            failures.append(
                _indexed_failure(
                    identity=f"{owner}:{match.group('line')}",
                    owner=owner,
                    detail=match.group("detail"),
                    rerun_command=phase.rerun_command,
                    log=log_path.name,
                )
            )
            continue
        if phase.parser == "vulture" and (
            match := _VULTURE_FRESHNESS.match(line)
        ):
            owner = match.group("owner").strip()
            failures.append(
                _indexed_failure(
                    identity=f"{owner}:{match.group('line')}",
                    owner=owner,
                    detail=(
                        f"{match.group('identity')}: {match.group('detail')}"
                    ),
                    rerun_command=phase.rerun_command,
                    log=log_path.name,
                )
            )
            continue
        if phase.parser == "python" and line.startswith(FAILURE_MARKER_PREFIX):
            marker = msgspec.json.decode(
                line.removeprefix(FAILURE_MARKER_PREFIX).encode(),
                type=CheckFailureMarker,
            )
            rerun = (
                "python3 -m unittest " + shlex.join(marker.test_ids)
                if marker.test_ids
                else phase.rerun_command
            )
            failures.append(
                _indexed_failure(
                    identity=marker.identity,
                    owner=marker.owner,
                    detail=marker.detail,
                    rerun_command=rerun,
                    log=log_path.name,
                    test_ids=marker.test_ids,
                )
            )
            continue
        if phase.parser == "python" and line.startswith(METRICS_MARKER_PREFIX):
            metrics = msgspec.json.decode(
                line.removeprefix(METRICS_MARKER_PREFIX).encode(),
                type=CheckMetricsMarker,
            )
    return tuple(failures), metrics


def _replace_phase(
    summary: CheckSummary,
    index: int,
    phase: CheckPhase,
    *,
    state: SuiteState | None = None,
    finished_at: str | None = None,
    elapsed_seconds: float | None = None,
    interruption_signal: int | None = None,
) -> CheckSummary:
    phases = list(summary.phases)
    phases[index] = phase
    return msgspec.structs.replace(
        summary,
        phases=tuple(phases),
        state=state if state is not None else summary.state,
        finished_at=finished_at,
        elapsed_seconds=(
            elapsed_seconds
            if elapsed_seconds is not None
            else summary.elapsed_seconds
        ),
        interruption_signal=interruption_signal,
    )


def _terminal_summary(summary: CheckSummary, stream: TextIO) -> None:
    failed_phases = [
        phase
        for phase in summary.phases
        if phase.state in {"failed", "infrastructure-failure"}
    ]
    failures = [
        (phase, failure)
        for phase in failed_phases
        for failure in phase.failures
    ]
    stream.write("\n")
    if summary.state == "passed":
        stream.write(f"PASSED: {len(summary.phases)} phases\n")
    elif summary.state == "failed":
        stream.write(
            f"FAILED: {len(failed_phases)} phases, {len(failures)} failures\n"
        )
    elif summary.state == "infrastructure-failure":
        stream.write(
            "INFRASTRUCTURE FAILURE: "
            f"{len(failed_phases)} phases, {len(failures)} failures\n"
        )
    else:
        stream.write(f"INTERRUPTED: signal {summary.interruption_signal}\n")
    stream.write(f"bundle: {summary.bundle}\n")
    stream.writelines(
        (
            f"{phase.name}: {failure.identity} | "
            f"{failure.detail.replace(chr(10), ' ')} | "
            f"rerun: {failure.rerun_command} | "
            f"log: {Path(summary.bundle) / failure.log}\n"
        )
        for phase, failure in failures
    )
    stream.flush()


def _execute_suite(
    *,
    root: Path,
    plan: tuple[PhaseSpec, ...],
    runtime: Path,
    executor: PhaseExecutor,
    output: TextIO,
    command: str,
) -> SuiteRun:
    """Create one bundle and run every phase to a terminal, published result.

    Called only from inside ``acquire_suite_admission`` — the admission lock
    is held for this entire call, covering the whole run's tmpfs footprint,
    not just the preflight (issue #1111).
    """
    bundle = _create_bundle(runtime)
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    dirty, dirty_sha256 = dirty_state_fingerprint(root)
    head = _git_bytes(root, "rev-parse", "HEAD").decode().strip()
    summary = CheckSummary(
        schema_version=SCHEMA_VERSION,
        runner_version=RUNNER_VERSION,
        state="running",
        command=command,
        repo_root=str(root),
        head=head,
        dirty=dirty,
        dirty_state_sha256=dirty_sha256,
        bundle=str(bundle),
        started_at=started_at,
        finished_at=None,
        elapsed_seconds=0.0,
        phases=tuple(
            CheckPhase(
                name=phase.name,
                state="not-run",
                command=phase.command,
                rerun_command=phase.rerun_command,
                log=f"{phase.name}.log",
            )
            for phase in plan
        ),
    )
    _publish_summary(bundle, summary)

    interrupted_signal: int | None = None
    handled_signals = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    prior_handlers = {
        signum: signal.getsignal(signum) for signum in handled_signals
    }

    def interrupt(signum: int, _frame: object) -> None:
        nonlocal interrupted_signal
        interrupted_signal = signum
        process = _active_process
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                pass

    for signum in handled_signals:
        signal.signal(signum, interrupt)

    try:
        for index, spec in enumerate(plan):
            if interrupted_signal is not None:
                break
            phase_started = _utc_now()
            running_phase = msgspec.structs.replace(
                summary.phases[index],
                state="running",
                started_at=phase_started,
            )
            summary = _replace_phase(summary, index, running_phase)
            _publish_summary(bundle, summary)
            output.write(f"=== {spec.name} ===\n")
            output.flush()

            log_path = bundle / f"{spec.name}.log"
            try:
                execution = executor(spec, spec.command, log_path)
            except Exception as exc:  # noqa: BLE001 - phase infrastructure boundary
                detail = f"{type(exc).__name__}: {exc}"
                if not log_path.exists():
                    log_path.write_text(
                        f"infrastructure failure: {detail}\n",
                        encoding="utf-8",
                    )
                execution = PhaseExecution(
                    exit_code=127,
                    elapsed_seconds=0.0,
                    infrastructure_error=detail,
                )
            if log_path.exists():
                log_path.chmod(0o600)
            phase_finished = _utc_now()

            if interrupted_signal is not None:
                interrupted_failure = _indexed_failure(
                    identity=spec.name,
                    owner="",
                    detail=f"interrupted by signal {interrupted_signal}",
                    rerun_command=spec.rerun_command,
                    log=log_path.name,
                )
                completed_phase = CheckPhase(
                    name=spec.name,
                    state="interrupted",
                    command=spec.command,
                    rerun_command=spec.rerun_command,
                    log=log_path.name,
                    started_at=phase_started,
                    finished_at=phase_finished,
                    elapsed_seconds=execution.elapsed_seconds,
                    exit_code=128 + interrupted_signal,
                    failures=(interrupted_failure,),
                )
                summary = _replace_phase(summary, index, completed_phase)
                _publish_summary(bundle, summary)
                break

            parser_error: str | None = None
            try:
                failures, metrics = _parse_failures(spec, log_path)
            except (OSError, ValueError, msgspec.DecodeError, msgspec.ValidationError) as exc:
                parser_error = f"{type(exc).__name__}: {exc}"
                failures = ()
                metrics = CheckMetricsMarker()

            infrastructure_error = execution.infrastructure_error or parser_error
            if execution.exit_code == 0 and failures:
                infrastructure_error = (
                    "phase emitted failure markers but exited zero"
                )
            if execution.exit_code == 0 and infrastructure_error is None:
                phase_state: PhaseState = "passed"
            elif (
                execution.exit_code in spec.failure_exit_codes
                and infrastructure_error is None
            ):
                phase_state = "failed"
            else:
                phase_state = "infrastructure-failure"

            if phase_state != "passed" and not failures:
                detail = infrastructure_error or (
                    f"phase failed with exit {execution.exit_code}"
                )
                failures = (
                    _indexed_failure(
                        identity=spec.name,
                        owner="",
                        detail=detail,
                        rerun_command=spec.rerun_command,
                        log=log_path.name,
                    ),
                )
            completed_phase = CheckPhase(
                name=spec.name,
                state=phase_state,
                command=spec.command,
                rerun_command=spec.rerun_command,
                log=log_path.name,
                started_at=phase_started,
                finished_at=phase_finished,
                elapsed_seconds=execution.elapsed_seconds,
                exit_code=execution.exit_code,
                failures=failures,
                tests_run=metrics.tests_run,
                targets_run=metrics.targets_run,
                scheduled_targets=metrics.scheduled_targets,
            )
            summary = _replace_phase(summary, index, completed_phase)
            _publish_summary(bundle, summary)
            output.write(
                f"{phase_state.upper()}: {spec.name} "
                f"({execution.elapsed_seconds:.1f}s, {len(failures)} failures)\n"
            )
            output.flush()
    finally:
        for signum, prior in prior_handlers.items():
            signal.signal(signum, prior)

    finished_at = _utc_now()
    elapsed = time.monotonic() - started_monotonic
    if interrupted_signal is not None:
        final_state: SuiteState = "interrupted"
        exit_code = 128 + interrupted_signal
    elif any(
        phase.state == "infrastructure-failure" for phase in summary.phases
    ):
        final_state = "infrastructure-failure"
        exit_code = 2
    elif any(phase.state == "failed" for phase in summary.phases):
        final_state = "failed"
        exit_code = 1
    elif all(phase.state == "passed" for phase in summary.phases):
        final_state = "passed"
        exit_code = 0
    else:
        final_state = "infrastructure-failure"
        exit_code = 2
    summary = msgspec.structs.replace(
        summary,
        state=final_state,
        finished_at=finished_at,
        elapsed_seconds=elapsed,
        interruption_signal=interrupted_signal,
    )
    _publish_summary(bundle, summary)
    _terminal_summary(summary, output)
    return SuiteRun(exit_code=exit_code, bundle=bundle, summary=summary)


def run_suite(
    *,
    repo_root: Path = REPO_ROOT,
    phases: Sequence[PhaseSpec] | None = None,
    runtime_dir: Path | None = None,
    executor: PhaseExecutor = execute_phase,
    stream: TextIO | None = None,
    command: str = CANONICAL_COMMAND,
    admission_timeout_seconds: float = DEFAULT_ADMISSION_TIMEOUT_SECONDS,
    admission_poll_seconds: float = DEFAULT_ADMISSION_POLL_SECONDS,
    admission_progress_interval_seconds: float = (
        DEFAULT_ADMISSION_PROGRESS_INTERVAL_SECONDS
    ),
    min_headroom_bytes: int | None = None,
    reap_max_age_seconds: float = DEFAULT_STALE_BUNDLE_MAX_AGE_SECONDS,
) -> SuiteRun:
    """Admit one canonical suite at a time, then run every phase (issue #1111).

    Validates the runtime tmpfs, then acquires the exclusive admission lock
    (bounded wait, reported progress), reaps stale check bundles, and checks
    headroom — all BEFORE any phase runs, so an unready environment fails
    once, immediately, with its real reason instead of tripping deep into a
    run after earlier phases already passed.
    """
    output = stream if stream is not None else sys.stdout
    root = repo_root.resolve(strict=True)
    plan = tuple(phases if phases is not None else _default_phases())
    if not plan or len({phase.name for phase in plan}) != len(plan):
        raise ValueError("phase plan must be non-empty with unique names")
    runtime = private_runtime_dir(runtime_dir)
    headroom_minimum = (
        min_headroom_bytes
        if min_headroom_bytes is not None
        else _default_min_headroom_bytes()
    )
    with acquire_suite_admission(
        runtime,
        stream=output,
        timeout_seconds=admission_timeout_seconds,
        poll_seconds=admission_poll_seconds,
        progress_interval_seconds=admission_progress_interval_seconds,
    ):
        reaped = reap_stale_check_bundles(
            runtime,
            max_age_seconds=reap_max_age_seconds,
        )
        if reaped:
            output.write(
                f"reaped {len(reaped)} stale check bundle(s): "
                + ", ".join(str(path) for path in reaped)
                + "\n"
            )
            output.flush()
        _check_suite_headroom(runtime, minimum_bytes=headroom_minimum)
        return _execute_suite(
            root=root,
            plan=plan,
            runtime=runtime,
            executor=executor,
            output=output,
            command=command,
        )


def _parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(description=__doc__)


def main(argv: Sequence[str] | None = None) -> int:
    _parser().parse_args(argv)
    try:
        return run_suite().exit_code
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"full-suite infrastructure failure: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
