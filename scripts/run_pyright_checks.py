#!/usr/bin/env python3
"""Run whole-tree and production-strict Pyright concurrently."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

REPO_ROOT = Path(__file__).resolve().parent.parent
MAX_TOTAL_THREADS = 12


@dataclass(frozen=True)
class PyrightCheck:
    """One independently configured Pyright contract."""

    name: str
    project: str
    threads: int


@dataclass(frozen=True)
class PyrightOutcome:
    """Complete output and process status for one Pyright contract."""

    check: PyrightCheck
    returncode: int
    output: str


CheckRunner = Callable[[PyrightCheck], PyrightOutcome]


def recommended_thread_counts(cpu_count: int) -> tuple[int, int]:
    """Split the measured useful CPU budget between complementary checks."""
    if cpu_count < 1:
        raise ValueError("cpu_count must be at least 1")
    if cpu_count == 1:
        return 1, 1
    total = min(cpu_count, MAX_TOTAL_THREADS)
    strict = max(1, (total + 1) // 3)
    whole = total - strict
    return whole, strict


def _run_check(check: PyrightCheck) -> PyrightOutcome:
    completed = subprocess.run(
        [
            "pyright",
            "-p",
            check.project,
            "--threads",
            str(check.threads),
        ],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return PyrightOutcome(
        check=check,
        returncode=completed.returncode,
        output=completed.stdout,
    )


def _run_check_fail_loud(
    check: PyrightCheck,
    run: CheckRunner,
) -> PyrightOutcome:
    try:
        return run(check)
    except Exception as exc:  # noqa: BLE001 - process infrastructure boundary
        return PyrightOutcome(
            check=check,
            returncode=2,
            output=f"{type(exc).__name__}: {exc}\n",
        )


def execute_checks(
    checks: Sequence[PyrightCheck],
    *,
    run: CheckRunner = _run_check,
) -> tuple[PyrightOutcome, ...]:
    """Execute every configured contract concurrently and preserve order."""
    plan = tuple(checks)
    if not plan:
        raise ValueError("at least one Pyright check is required")
    if len({check.name for check in plan}) != len(plan):
        raise ValueError("Pyright check names must be unique")
    if any(check.threads < 1 for check in plan):
        raise ValueError("Pyright thread counts must be positive")
    with ThreadPoolExecutor(max_workers=len(plan)) as executor:
        futures = tuple(
            executor.submit(_run_check_fail_loud, check, run)
            for check in plan
        )
        return tuple(future.result() for future in futures)


def combined_exit_code(outcomes: Sequence[PyrightOutcome]) -> int:
    """Combine all child statuses without losing infrastructure failures."""
    if any(
        line.startswith("Config contains unrecognized setting")
        for outcome in outcomes
        for line in outcome.output.splitlines()
    ):
        return 2
    statuses = tuple(outcome.returncode for outcome in outcomes)
    if statuses and all(status == 0 for status in statuses):
        return 0
    if statuses and all(status in {0, 1} for status in statuses):
        return 1
    return 2


def write_outcomes(
    outcomes: Sequence[PyrightOutcome],
    stream: TextIO,
) -> None:
    """Publish complete child output in stable contract order."""
    for outcome in outcomes:
        stream.write(
            f"=== {outcome.check.name} Pyright "
            f"({outcome.check.threads} threads) ===\n"
        )
        stream.write(outcome.output)
        if outcome.output and not outcome.output.endswith("\n"):
            stream.write("\n")
    stream.flush()


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def _parser(cpu_count: int) -> argparse.ArgumentParser:
    whole_default, strict_default = recommended_thread_counts(cpu_count)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--whole-threads",
        type=_positive_int,
        default=whole_default,
    )
    parser.add_argument(
        "--strict-threads",
        type=_positive_int,
        default=strict_default,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser(os.cpu_count() or 1).parse_args(argv)
    outcomes = execute_checks(
        (
            PyrightCheck("whole-tree", "pyrightconfig.json", args.whole_threads),
            PyrightCheck(
                "production-strict",
                "pyrightconfig.production.json",
                args.strict_threads,
            ),
        )
    )
    write_outcomes(outcomes, sys.stdout)
    return combined_exit_code(outcomes)


if __name__ == "__main__":
    raise SystemExit(main())
