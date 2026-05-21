#!/usr/bin/env python3
"""Find code that tests exercise but production never executes.

This is the actionable category for "tests are holding us back": code paths
that are statically reachable, but in real production traffic over the
measured window, only the test suite has ever run them. They're prime
candidates for deletion (or, when intentional — operator escape hatches,
rare-edge handlers — promotion to a smoke test the operator runs manually,
freeing the unit-test schedule).

Inputs (read-only; produced by other scripts):
  - build/test-coverage/.coverage     ← run `bash scripts/run_tests.sh` after exporting
                                        COVERAGE_FILE=build/test-coverage/.coverage and
                                        wrapping the suite with `coverage run`. The wrapper
                                        is scripts/run_tests_with_coverage.sh (also new).
  - build/prod-coverage/.coverage     ← run `bash scripts/coverage_report.sh doc2`

Output:
  - stdout: per-file line-count summary, sorted by "test-only line count desc"
  - build/test-only-lines.txt: full line-level dump for triage

Usage:
  nix-shell --run "python3 scripts/coverage_diff.py"
  nix-shell --run "python3 scripts/coverage_diff.py --min-lines 5"
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import coverage


def load_executed_lines(data_file: Path) -> dict[str, set[int]]:
    """Return {abs_file_path: set(executed_line_nos)} from a coverage data file."""
    if not data_file.exists():
        raise SystemExit(f"missing coverage data: {data_file}")
    cov = coverage.Coverage(data_file=str(data_file))
    cov.load()
    data = cov.get_data()
    result: dict[str, set[int]] = {}
    for f in data.measured_files():
        lines = data.lines(f)
        if lines is None:
            continue
        # Coverage stores absolute paths. Normalise via realpath so /nix/store
        # symlinks vs ./repo paths reconcile through the [paths] section we
        # configured in .coveragerc — but we still want a stable key.
        result[os.path.realpath(f)] = set(lines)
    return result


def normalise_to_repo(path: str, repo_root: Path) -> str | None:
    """Reduce an absolute path to a repo-relative path, or None if outside."""
    real = os.path.realpath(path)
    try:
        rel = Path(real).resolve().relative_to(repo_root.resolve())
    except ValueError:
        return None
    return str(rel)


def diff_coverage(
    test_data: Path,
    prod_data: Path,
    repo_root: Path,
) -> dict[str, set[int]]:
    """Return {repo_rel_path: set(lines_only_in_tests)}."""
    test_lines = load_executed_lines(test_data)
    prod_lines = load_executed_lines(prod_data)

    # Group by repo-relative path so /nix/store/...-source/lib/foo.py and
    # /home/abl030/cratedigger/lib/foo.py merge to lib/foo.py.
    def regroup(by_abs: dict[str, set[int]]) -> dict[str, set[int]]:
        out: dict[str, set[int]] = {}
        for abs_path, lines in by_abs.items():
            rel = normalise_to_repo(abs_path, repo_root)
            if rel is None:
                continue
            out.setdefault(rel, set()).update(lines)
        return out

    t = regroup(test_lines)
    p = regroup(prod_lines)

    test_only: dict[str, set[int]] = {}
    for f, lines in t.items():
        diff = lines - p.get(f, set())
        if diff:
            test_only[f] = diff
    return test_only


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--test-data",
        default="build/test-coverage/.coverage",
        help="Path to test-suite coverage data file",
    )
    parser.add_argument(
        "--prod-data",
        default="build/prod-coverage/.coverage",
        help="Path to production coverage data file",
    )
    parser.add_argument(
        "--min-lines",
        type=int,
        default=1,
        help="Suppress files with fewer than this many test-only lines",
    )
    parser.add_argument(
        "--output",
        default="build/test-only-lines.txt",
        help="Write the full line-level dump to this file",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    test_only = diff_coverage(
        Path(args.test_data),
        Path(args.prod_data),
        repo_root,
    )

    if not test_only:
        print("No test-only lines found — production exercises everything tests do.")
        return 0

    # Per-file summary, biggest first.
    rows = sorted(
        ((f, sorted(lines)) for f, lines in test_only.items()),
        key=lambda x: len(x[1]),
        reverse=True,
    )

    total_lines = sum(len(lines) for _, lines in rows)
    visible = [(f, lines) for f, lines in rows if len(lines) >= args.min_lines]

    print(
        f"=== {total_lines} lines covered by tests but never executed in production ==="
    )
    print(f"=== Across {len(rows)} files (showing {len(visible)} with ≥{args.min_lines} lines) ===")
    print()
    print(f"{'lines':>6}  file")
    print(f"{'-----':>6}  ----")
    for f, lines in visible:
        print(f"{len(lines):>6}  {f}")

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as fh:
        for f, lines in rows:
            fh.write(f"# {f} — {len(lines)} test-only lines\n")
            for ln in lines:
                fh.write(f"{f}:{ln}\n")
            fh.write("\n")
    print()
    print(f"Full line-level dump written to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
