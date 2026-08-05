#!/usr/bin/env python3
"""Run explicit and adjacent tests with every ambient quality gate."""

from __future__ import annotations

import argparse
import shlex
import sys
from collections.abc import Sequence
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_test_suite import PhaseSpec, _default_phases, run_suite
from scripts.targeted_test_selection import (
    changed_paths_from_git,
    expand_test_selection,
)


def targeted_phases(selectors: Sequence[str]) -> tuple[PhaseSpec, ...]:
    """Reuse canonical non-Python gates and replace only the Python plan."""
    selected = tuple(selectors)
    if not selected:
        raise ValueError("targeted suite requires at least one selected test")
    python_command = (
        "python3",
        "scripts/run_python_tests.py",
        *(argument for selector in selected for argument in ("--test", selector)),
    )
    return (
        *(phase for phase in _default_phases() if phase.name != "python"),
        PhaseSpec(
            "python",
            python_command,
            shlex.join(python_command),
            "python",
        ),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tests", nargs="*", metavar="UNITTEST_NAME")
    parser.add_argument("--base-ref", default="origin/main")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    effective_argv = tuple(sys.argv[1:] if argv is None else argv)
    args = _parser().parse_args(effective_argv)
    try:
        changed_paths = changed_paths_from_git(
            REPO_ROOT,
            base_ref=args.base_ref,
        )
        selectors = expand_test_selection(
            args.tests,
            changed_paths=changed_paths,
            repo_root=REPO_ROOT,
        )
        command = shlex.join(
            ("python3", "scripts/run_targeted_tests.py", *effective_argv)
        )
        print(
            f"Targeted suite: {len(selectors)} Python selectors from "
            f"{len(args.tests)} explicit and {len(changed_paths)} changed paths"
        )
        for selector in selectors:
            print(f"  {selector}")
        return run_suite(
            repo_root=REPO_ROOT,
            phases=targeted_phases(selectors),
            command=command,
        ).exit_code
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"targeted-suite infrastructure failure: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
