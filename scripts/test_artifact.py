#!/usr/bin/env python3
"""Create, finalize, and verify collision-free full-suite artifacts."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from typing import Literal
import unittest

import msgspec


SUMMARY_NAME = "summary.json"
OUTPUT_NAME = "output.log"
SUMMARY_SCHEMA_VERSION = 2


class GitSnapshot(msgspec.Struct, frozen=True):
    """The Git identity of a worktree at one instant."""

    head: str
    dirty: bool


class PythonTestCounts(msgspec.Struct, frozen=True):
    """Counts derived from the exact unittest suite object that ran."""

    discovered_tests: int
    run_tests: int


class TestRunSummary(msgspec.Struct, frozen=True):
    """Typed provenance record for one full-suite invocation."""

    schema_version: int
    artifact_path: str
    output_path: str
    worktree_path: str
    started_at: str
    ended_at: str | None
    start_head: str
    end_head: str | None
    start_dirty: bool
    end_dirty: bool | None
    status: Literal["running", "passed", "failed"]
    exit_code: int | None
    gate_exit_code: int | None
    capture_exit_code: int | None
    discovered_tests: int
    run_tests: int
    output_bytes: int | None
    output_sha256: str | None


class ArtifactVerificationError(ValueError):
    """Raised when a suite artifact cannot prove an exact green target."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_git(worktree: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(worktree), *args],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ValueError(
            f"git {' '.join(args)} failed for {worktree}: {detail}"
        )
    return result.stdout.strip()


def canonical_worktree(worktree: Path) -> Path:
    """Return the resolved canonical root for a Git worktree."""
    root = _run_git(worktree, "rev-parse", "--show-toplevel")
    return Path(root).resolve()


def git_snapshot(worktree: Path) -> GitSnapshot:
    """Capture the exact commit and complete tracked/untracked dirty state."""
    root = canonical_worktree(worktree)
    head = _run_git(root, "rev-parse", "HEAD")
    status = _run_git(
        root, "status", "--porcelain=v1", "--untracked-files=all"
    )
    return GitSnapshot(head=head, dirty=bool(status))


def _write_struct(path: Path, value: object) -> None:
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    temporary.write_bytes(msgspec.json.encode(value))
    temporary.replace(path)


def create_artifact(
    worktree: Path, artifact_root: Path | None = None
) -> Path:
    """Allocate and initialize a unique artifact for one suite invocation."""
    root = canonical_worktree(worktree)
    snapshot = git_snapshot(root)
    base = (
        artifact_root
        if artifact_root is not None
        else Path(os.environ.get("CRATEDIGGER_TEST_ARTIFACT_ROOT", "/tmp"))
    )
    base.mkdir(parents=True, exist_ok=True)
    worktree_token = hashlib.sha256(str(root).encode()).hexdigest()[:10]
    worktree_name = "".join(
        character if character.isalnum() or character in "-_" else "-"
        for character in root.name
    )
    prefix = f"{worktree_name}-{worktree_token}-{snapshot.head[:12]}."
    artifact = Path(
        tempfile.mkdtemp(prefix=prefix, dir=base)
    ).resolve()
    output = artifact / OUTPUT_NAME
    output.touch()
    summary = TestRunSummary(
        schema_version=SUMMARY_SCHEMA_VERSION,
        artifact_path=str(artifact),
        output_path=str(output),
        worktree_path=str(root),
        started_at=_utc_now(),
        ended_at=None,
        start_head=snapshot.head,
        end_head=None,
        start_dirty=snapshot.dirty,
        end_dirty=None,
        status="running",
        exit_code=None,
        gate_exit_code=None,
        capture_exit_code=None,
        discovered_tests=0,
        run_tests=0,
        output_bytes=None,
        output_sha256=None,
    )
    _write_struct(artifact / SUMMARY_NAME, summary)
    return artifact


def read_summary(artifact: Path) -> TestRunSummary:
    """Decode a typed suite summary from an artifact directory."""
    summary_path = artifact.resolve() / SUMMARY_NAME
    try:
        return msgspec.json.decode(
            summary_path.read_bytes(), type=TestRunSummary
        )
    except (OSError, msgspec.DecodeError, msgspec.ValidationError) as exc:
        raise ArtifactVerificationError(
            f"invalid or missing suite summary {summary_path}: {exc}"
        ) from exc


def finalize_artifact(
    artifact: Path,
    worktree: Path,
    *,
    gate_exit_code: int,
    capture_exit_code: int,
    discovered_tests: int,
    run_tests: int,
) -> TestRunSummary:
    """Finalize only after the gate-output capture process has completed."""
    initial = read_summary(artifact)
    root = canonical_worktree(worktree)
    if str(root) != initial.worktree_path:
        raise ValueError(
            "artifact worktree changed: "
            f"started in {initial.worktree_path}, finalized in {root}"
        )
    end = git_snapshot(root)
    counts_coherent = discovered_tests > 0 and run_tests == discovered_tests
    output_path = artifact.resolve() / OUTPUT_NAME
    try:
        output = output_path.read_bytes()
    except OSError:
        output_bytes = None
        output_sha256 = None
    else:
        output_bytes = len(output)
        output_sha256 = hashlib.sha256(output).hexdigest()
    output_complete = output_bytes is not None and output_bytes > 0
    passed = (
        gate_exit_code == 0
        and capture_exit_code == 0
        and counts_coherent
        and output_complete
    )
    if capture_exit_code != 0:
        recorded_exit = capture_exit_code
    elif gate_exit_code != 0:
        recorded_exit = gate_exit_code
    else:
        recorded_exit = 0 if passed else 1
    summary = TestRunSummary(
        schema_version=initial.schema_version,
        artifact_path=initial.artifact_path,
        output_path=initial.output_path,
        worktree_path=initial.worktree_path,
        started_at=initial.started_at,
        ended_at=_utc_now(),
        start_head=initial.start_head,
        end_head=end.head,
        start_dirty=initial.start_dirty,
        end_dirty=end.dirty,
        status="passed" if passed else "failed",
        exit_code=recorded_exit,
        gate_exit_code=gate_exit_code,
        capture_exit_code=capture_exit_code,
        discovered_tests=discovered_tests,
        run_tests=run_tests,
        output_bytes=output_bytes,
        output_sha256=output_sha256,
    )
    _write_struct(artifact.resolve() / SUMMARY_NAME, summary)
    return summary


def _timestamp(value: str, field: str) -> tuple[datetime | None, str | None]:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None, f"{field} is not an ISO-8601 timestamp: {value!r}"
    if parsed.tzinfo is None:
        return None, f"{field} must include a timezone: {value!r}"
    return parsed, None


def summary_rejection_reasons(
    summary: TestRunSummary, expected_head: str
) -> tuple[str, ...]:
    """Return every reason a summary cannot prove an exact target."""
    reasons: list[str] = []
    if summary.schema_version != SUMMARY_SCHEMA_VERSION:
        reasons.append(
            "unsupported summary schema version "
            f"{summary.schema_version}; expected {SUMMARY_SCHEMA_VERSION}"
        )
    if summary.status != "passed" or summary.exit_code != 0:
        reasons.append(
            "artifact is not a completed green run: "
            f"status={summary.status!r}, exit_code={summary.exit_code!r}"
        )
    if summary.gate_exit_code != 0:
        reasons.append(
            f"gate process failed with exit code {summary.gate_exit_code!r}"
        )
    if summary.capture_exit_code != 0:
        reasons.append(
            "output capture failed with exit code "
            f"{summary.capture_exit_code!r}"
        )
    if summary.ended_at is None or summary.end_head is None:
        reasons.append("artifact has not completed with end provenance")
    if summary.end_dirty is None:
        reasons.append("artifact has not recorded end cleanliness")
    if summary.start_head != expected_head:
        reasons.append(
            f"start HEAD {summary.start_head} does not equal expected HEAD "
            f"{expected_head}"
        )
    if summary.end_head != expected_head:
        reasons.append(
            f"end HEAD {summary.end_head!r} does not equal expected HEAD "
            f"{expected_head}"
        )
    if summary.start_dirty:
        reasons.append("artifact started from a dirty worktree")
    if summary.end_dirty:
        reasons.append("artifact ended in a dirty worktree")
    if (
        summary.discovered_tests <= 0
        or summary.run_tests != summary.discovered_tests
    ):
        reasons.append(
            "Python test counts are incoherent: "
            f"discovered={summary.discovered_tests}, run={summary.run_tests}"
        )
    if summary.output_bytes is None or summary.output_bytes <= 0:
        reasons.append(
            f"recorded output byte count is invalid: {summary.output_bytes!r}"
        )
    if (
        summary.output_sha256 is None
        or len(summary.output_sha256) != 64
        or any(
            character not in "0123456789abcdef"
            for character in summary.output_sha256
        )
    ):
        reasons.append(
            f"recorded output SHA-256 is invalid: {summary.output_sha256!r}"
        )

    started, start_error = _timestamp(summary.started_at, "started_at")
    if start_error is not None:
        reasons.append(start_error)
    ended: datetime | None = None
    if summary.ended_at is not None:
        ended, end_error = _timestamp(summary.ended_at, "ended_at")
        if end_error is not None:
            reasons.append(end_error)
    if started is not None and ended is not None and ended < started:
        reasons.append("ended_at precedes started_at")
    return tuple(reasons)


def verify_artifact(
    artifact: Path, expected_head: str
) -> TestRunSummary:
    """Require a complete, clean, exact-HEAD, count-coherent green artifact."""
    artifact = artifact.resolve()
    summary = read_summary(artifact)
    reasons = list(summary_rejection_reasons(summary, expected_head))
    expected_output = artifact / OUTPUT_NAME
    if summary.artifact_path != str(artifact):
        reasons.append(
            f"summary attributes artifact to {summary.artifact_path}, not {artifact}"
        )
    if summary.output_path != str(expected_output):
        reasons.append(
            f"summary output path is {summary.output_path}, expected {expected_output}"
        )
    worktree = Path(summary.worktree_path)
    if not worktree.is_absolute() or worktree.resolve() != worktree:
        reasons.append(
            f"worktree path is not canonical and absolute: {summary.worktree_path}"
        )
    try:
        output = expected_output.read_bytes()
    except OSError as exc:
        reasons.append(f"full gate output is missing: {exc}")
    else:
        actual_size = len(output)
        actual_sha256 = hashlib.sha256(output).hexdigest()
        if actual_size != summary.output_bytes:
            reasons.append(
                "full gate output byte count changed after finalization: "
                f"recorded={summary.output_bytes!r}, actual={actual_size}"
            )
        if actual_sha256 != summary.output_sha256:
            reasons.append(
                "full gate output SHA-256 changed after finalization: "
                f"recorded={summary.output_sha256!r}, actual={actual_sha256}"
            )
    if reasons:
        raise ArtifactVerificationError(
            f"suite artifact {artifact} cannot prove expected HEAD "
            f"{expected_head}: " + "; ".join(reasons)
        )
    return summary


def run_python_suite(counts_file: Path) -> int:
    """Discover once, execute that same suite, and persist both counts."""
    suite = unittest.defaultTestLoader.discover(
        start_dir="tests", pattern="test*.py", top_level_dir="."
    )
    discovered = suite.countTestCases()
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    _write_struct(
        counts_file,
        PythonTestCounts(
            discovered_tests=discovered,
            run_tests=result.testsRun,
        ),
    )
    return 0 if result.wasSuccessful() else 1


def _read_counts(path: Path) -> PythonTestCounts:
    try:
        return msgspec.json.decode(path.read_bytes(), type=PythonTestCounts)
    except (OSError, msgspec.DecodeError, msgspec.ValidationError):
        return PythonTestCounts(discovered_tests=0, run_tests=0)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser("start")
    start.add_argument("--worktree", type=Path, required=True)
    start.add_argument("--artifact-root", type=Path)

    finalize = subparsers.add_parser("finalize")
    finalize.add_argument("--artifact", type=Path, required=True)
    finalize.add_argument("--worktree", type=Path, required=True)
    finalize.add_argument("--gate-exit-code", type=int, required=True)
    finalize.add_argument("--capture-exit-code", type=int, required=True)
    finalize.add_argument("--counts-file", type=Path, required=True)

    run_python = subparsers.add_parser("run-python")
    run_python.add_argument("--counts-file", type=Path, required=True)

    verify = subparsers.add_parser("verify")
    verify.add_argument("--artifact", type=Path, required=True)
    verify.add_argument("--expected-head", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "start":
            artifact = create_artifact(args.worktree, args.artifact_root)
            print(artifact)
            return 0
        if args.command == "run-python":
            return run_python_suite(args.counts_file)
        if args.command == "finalize":
            counts = _read_counts(args.counts_file)
            summary = finalize_artifact(
                args.artifact,
                args.worktree,
                gate_exit_code=args.gate_exit_code,
                capture_exit_code=args.capture_exit_code,
                discovered_tests=counts.discovered_tests,
                run_tests=counts.run_tests,
            )
            print(
                f"summary={args.artifact / SUMMARY_NAME} "
                f"status={summary.status} "
                f"gate_exit={summary.gate_exit_code} "
                f"capture_exit={summary.capture_exit_code} "
                f"discovered={summary.discovered_tests} "
                f"run={summary.run_tests} "
                f"output_bytes={summary.output_bytes} "
                f"output_sha256={summary.output_sha256}"
            )
            return (
                0
                if summary.status == "passed"
                or args.gate_exit_code != 0
                or args.capture_exit_code != 0
                else 1
            )
        if args.command == "verify":
            summary = verify_artifact(args.artifact, args.expected_head)
            print(
                f"Verified suite artifact {args.artifact.resolve()} for "
                f"HEAD {args.expected_head}: {summary.run_tests} tests"
            )
            return 0
    except (ArtifactVerificationError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
