#!/usr/bin/env python3
"""Run every deterministic validation phase and publish one failure bundle."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import shlex
import signal
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
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
            "pyright-production",
            ("pyright", "-p", "pyrightconfig.production.json", "--threads", "4"),
            "pyright -p pyrightconfig.production.json --threads 4",
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


def run_suite(
    *,
    repo_root: Path = REPO_ROOT,
    phases: Sequence[PhaseSpec] | None = None,
    runtime_dir: Path | None = None,
    executor: PhaseExecutor = execute_phase,
    stream: TextIO | None = None,
) -> SuiteRun:
    """Run every phase, preserving complete evidence and one terminal result."""
    output = stream if stream is not None else sys.stdout
    root = repo_root.resolve(strict=True)
    plan = tuple(phases if phases is not None else _default_phases())
    if not plan or len({phase.name for phase in plan}) != len(plan):
        raise ValueError("phase plan must be non-empty with unique names")
    runtime = private_runtime_dir(runtime_dir)
    bundle = _create_bundle(runtime)
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    dirty, dirty_sha256 = dirty_state_fingerprint(root)
    head = _git_bytes(root, "rev-parse", "HEAD").decode().strip()
    summary = CheckSummary(
        schema_version=SCHEMA_VERSION,
        runner_version=RUNNER_VERSION,
        state="running",
        command=CANONICAL_COMMAND,
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
