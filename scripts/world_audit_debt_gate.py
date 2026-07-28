#!/usr/bin/env python3
"""Classify a strict world-audit report against monotone known debt."""

from __future__ import annotations

import argparse
import fcntl
import os
import sys
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, TextIO

import msgspec

from lib.world_audit_debt import (
    WorldAuditDebtError,
    assess_world_audit_debt,
    decode_world_audit_report,
    initialization_world_audit_debt_report,
    initialize_world_audit_debt_state,
    load_world_audit_debt_state,
    write_world_audit_debt_state,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare strict live-world audit JSON with individually tracked "
            "known debt."
        ),
    )
    parser.add_argument(
        "--state",
        required=True,
        type=Path,
        help="Root-owned digest-state path.",
    )
    parser.add_argument(
        "--initialize",
        action="store_true",
        help="Create the initial authority state; refuses an existing path.",
    )
    return parser


@contextmanager
def _state_lock(state_path: Path) -> Generator[None]:
    lock_path = state_path.with_name(f"{state_path.name}.lock")
    try:
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    except OSError as exc:
        raise WorldAuditDebtError(
            f"could not open debt-state lock: {exc}"
        ) from exc
    try:
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    except OSError as exc:
        raise WorldAuditDebtError(
            f"could not lock debt state: {exc}"
        ) from exc
    finally:
        os.close(descriptor)


def _emit(report: object, stdout: TextIO) -> None:
    stdout.write(msgspec.json.encode(report).decode())
    stdout.write("\n")


def run(
    argv: Sequence[str],
    *,
    stdin: BinaryIO,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    args = _parser().parse_args(argv)
    try:
        report = decode_world_audit_report(stdin.read())
        with _state_lock(args.state):
            if args.initialize:
                state = initialize_world_audit_debt_state(report)
                write_world_audit_debt_state(
                    args.state,
                    state,
                    exclusive=True,
                )
                _emit(
                    initialization_world_audit_debt_report(state, report),
                    stdout,
                )
                return 0
            state = load_world_audit_debt_state(args.state)
            evaluation = assess_world_audit_debt(state, report)
            if (
                evaluation.passed
                and evaluation.next_state is not None
                and evaluation.next_state != state
            ):
                write_world_audit_debt_state(
                    args.state,
                    evaluation.next_state,
                )
            _emit(evaluation.report, stdout)
            return 0 if evaluation.passed else 1
    except WorldAuditDebtError as exc:
        print(f"world-audit debt gate: {exc}", file=stderr)
        return 5


def main() -> None:
    raise SystemExit(run(
        sys.argv[1:],
        stdin=sys.stdin.buffer,
        stdout=sys.stdout,
        stderr=sys.stderr,
    ))


if __name__ == "__main__":
    main()
