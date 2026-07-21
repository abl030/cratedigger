#!/usr/bin/env python3
"""Benchmark parallel Soulseek searches to find the optimal concurrency level.

Runs the same set of searches at different concurrency levels (1, 2, 4, 8, 12,
16, 20) and reports wall-clock time, per-search avg, speedup vs sequential,
and result count consistency.

Usage:
    # Against doc2's slskd (needs API key from .slskd-creds.json or args):
    nix-shell --run "python3 scripts/bench_parallel_search.py --host http://192.168.1.35:5030 --api-key <key>"

    # Auto-start ephemeral Docker container (uses tests/.slskd-creds.json):
    nix-shell --run "python3 scripts/bench_parallel_search.py"

    # Custom concurrency levels:
    nix-shell --run "python3 scripts/bench_parallel_search.py --levels 1,2,4,8"
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Protocol

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.slskd_client import SlskdClient  # noqa: E402  — after sys.path insert
from lib.search_exec import (  # noqa: E402  — after sys.path insert
    SearchSubmitError,
    execute_search,
)


class _EphemeralSlskdHandle(Protocol):
    """Narrow structural contract for ``tests/ephemeral_slskd.py::EphemeralSlskd``.

    ``tests/`` is excluded from strict pyright (see
    ``pyrightconfig.strict-production.json``), so the dynamically
    path-inserted import below is untyped at its source; this Protocol
    plus ``_build_ephemeral`` (the one place the untyped import happens)
    keeps that Unknown-ness from leaking into the rest of this module.
    """

    host_url: str
    api_key: str

    def start(self) -> None: ...
    def wait_for_soulseek(self, timeout: int = 60) -> bool: ...
    def stop(self) -> None: ...


def _build_ephemeral(creds_path: str) -> _EphemeralSlskdHandle:
    """Construct the ephemeral slskd test fixture (see class docstring)."""
    from ephemeral_slskd import EphemeralSlskd  # pyright: ignore[reportMissingImports]
    return EphemeralSlskd(creds_path)


# Diverse queries: mix of popular (guaranteed results) and less common artists.
# All use the wildcarded-first-char format that cratedigger uses in production.
QUERIES = [
    "*eatles abbey road",
    "*ink *loyd dark side moon",
    "*ed *eppelin houses holy",
    "*olling *tones exile main",
    "*adiohead bends",
    "*iles *avis kind blue",
    "*ountain *oats tallahassee",
    "*eutral *ilk *otel california",
    "*lack *lag damaged",
    "*ugazi repeater",
]


@dataclass
class SingleSearchResult:
    query: str
    result_count: int
    elapsed_s: float
    error: str | None = None


def run_single_search(client: SlskdClient, query: str,
                      search_timeout: int = 30000) -> SingleSearchResult:
    """Run one search and return timing + result count.

    Drives the same unified lifecycle production uses
    (``lib.search_exec.execute_search``, issue #466) so the benchmark
    measures the real submit → poll (with the #212 watchdog) → settle-harvest
    (#242) → delete path rather than a drifted hand-rolled copy.
    """
    t0 = time.time()
    try:
        # Deliberately unledgered (issue #576): this hand-run dev bench has
        # no pipeline-DB handle, delete=True covers the happy path, and per
        # I3 the sweep treats its searches as foreign and leaves them alone
        # — a killed bench run leaks its in-flight searches, accepted for a
        # manual tool.
        exec_result = execute_search(
            client,
            submit_kwargs={"searchText": query, "searchTimeout": search_timeout},
            delete=True,
        )
    except SearchSubmitError as e:
        return SingleSearchResult(query=query, result_count=0,
                                  elapsed_s=time.time() - t0, error=str(e))

    return SingleSearchResult(query=query,
                              result_count=len(exec_result.responses),
                              elapsed_s=exec_result.elapsed_s)


def run_batch(client: SlskdClient, queries: list[str],
              concurrency: int, search_timeout: int = 30000) -> list[SingleSearchResult]:
    """Run all queries at the given concurrency level."""
    if concurrency <= 1:
        # Sequential
        return [run_single_search(client, q, search_timeout) for q in queries]

    results: list[SingleSearchResult] = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            pool.submit(run_single_search, client, q, search_timeout): q
            for q in queries
        }
        for future in as_completed(futures):
            results.append(future.result())

    # Return in original query order for consistent display
    by_query = {r.query: r for r in results}
    return [by_query[q] for q in queries]


def print_run(results: list[SingleSearchResult]) -> tuple[int, int]:
    """Print per-search results for one concurrency level."""
    total_results = sum(r.result_count for r in results)
    errors = sum(1 for r in results if r.error)

    for r in results:
        status = f"{r.result_count:4d} results" if not r.error else f"ERROR: {r.error[:40]}"
        print(f"    {r.query:<40s}  {r.elapsed_s:6.1f}s  {status}")

    return total_results, errors


def main():
    parser = argparse.ArgumentParser(description="Benchmark parallel Soulseek searches")
    parser.add_argument("--host", help="slskd host URL (e.g. http://192.168.1.35:5030)")
    parser.add_argument("--api-key", help="slskd API key")
    parser.add_argument("--levels", default="1,2,4,8,12,16,20",
                        help="Comma-separated concurrency levels to test (default: 1,2,4,8,12,16,20)")
    parser.add_argument("--timeout", type=int, default=30000,
                        help="Search timeout in ms (default: 30000)")
    parser.add_argument("--queries", type=int, default=len(QUERIES),
                        help=f"Number of queries to use (max {len(QUERIES)}, default: all)")
    args = parser.parse_args()

    host = args.host
    api_key = args.api_key

    # Try to get creds from .slskd-creds.json if not provided
    if not host or not api_key:
        creds_file = os.path.join(os.path.dirname(__file__), "..", "tests", ".slskd-creds.json")
        if os.path.exists(creds_file):
            with open(creds_file) as f:
                creds = json.load(f)
            if not api_key:
                api_key = creds.get("api_key", "")

    # If still no host, try ephemeral container
    ephemeral: _EphemeralSlskdHandle | None
    if not host:
        print("No --host provided, starting ephemeral slskd container...")
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tests"))
        creds_path = os.path.join(os.path.dirname(__file__), "..", "tests", ".slskd-creds.json")
        ephemeral = _build_ephemeral(creds_path)
        ephemeral.start()
        host = ephemeral.host_url
        api_key = ephemeral.api_key
        print(f"Ephemeral slskd at {host}, waiting for Soulseek connection...")
        if not ephemeral.wait_for_soulseek(timeout=90):
            print("WARNING: slskd API up but not connected to Soulseek — results may be empty")
    else:
        ephemeral = None

    levels = [int(x) for x in args.levels.split(",")]
    queries = QUERIES[:args.queries]

    print(f"\nBenchmark: {len(queries)} queries, concurrency levels: {levels}")
    print(f"Host: {host}")
    print(f"Search timeout: {args.timeout}ms")
    print(f"Queries: {', '.join(q[:30] for q in queries)}")
    print()

    client = SlskdClient(host=host, api_key=api_key)

    # Verify connection
    try:
        ver = client.application.version()
        print(f"slskd version: {ver}")
    except Exception as e:
        print(f"ERROR: Cannot connect to slskd at {host}: {e}")
        sys.exit(1)

    # Check Soulseek connection
    import urllib.request
    try:
        req = urllib.request.Request(
            f"{host}/api/v0/server",
            headers={"X-API-Key": api_key},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            server = json.loads(resp.read())
        if not server.get("isConnected"):
            print("WARNING: slskd not connected to Soulseek network — searches will return 0 results")
        else:
            print(f"Soulseek: connected, logged in as '{server.get('username', '?')}'")
    except Exception:
        pass

    print()

    # Run benchmark at each concurrency level
    summary: list[tuple[int, float, int, int, list[SingleSearchResult]]] = []

    for level in levels:
        label = "sequential" if level == 1 else f"parallel x{level}"
        print(f"{'='*60}")
        print(f"  Concurrency: {level} ({label})")
        print(f"{'='*60}")

        wall_start = time.time()
        results = run_batch(client, queries, level, args.timeout)
        wall_elapsed = time.time() - wall_start

        total_results, errors = print_run(results)

        print(f"  ---")
        print(f"  Wall time: {wall_elapsed:.1f}s | "
              f"Total results: {total_results} | "
              f"Errors: {errors}")
        summary.append((level, wall_elapsed, total_results, errors, results))
        print()

        # Brief pause between levels to let slskd settle
        if level != levels[-1]:
            time.sleep(3)

    # Summary table
    baseline_wall = summary[0][1] if summary else 1.0
    baseline_results = summary[0][2] if summary else 0

    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")
    print(f"  {'Level':>5s}  {'Wall':>7s}  {'Speedup':>7s}  {'Results':>8s}  {'Delta':>7s}  {'Errors':>6s}")
    print(f"  {'-----':>5s}  {'-------':>7s}  {'-------':>7s}  {'--------':>8s}  {'-------':>7s}  {'------':>6s}")

    best_level = 1
    best_wall = baseline_wall

    for level, wall, total_results, errors, _ in summary:
        speedup = baseline_wall / wall if wall > 0 else 0
        result_delta = total_results - baseline_results
        delta_str = f"{result_delta:+d}" if result_delta != 0 else "0"
        marker = ""

        if wall < best_wall:
            best_wall = wall
            best_level = level

        print(f"  {level:>5d}  {wall:>6.1f}s  {speedup:>6.1f}x  {total_results:>8d}  {delta_str:>7s}  {errors:>6d}  {marker}")

    print(f"\n  Best: concurrency={best_level} ({baseline_wall/best_wall:.1f}x speedup)")

    # Detect degradation
    if len(summary) >= 2:
        last = summary[-1]
        if last[2] < baseline_results * 0.8:
            print(f"\n  WARNING: Result count dropped {baseline_results - last[2]} at "
                  f"concurrency {last[0]} — slskd or Soulseek may be throttling")
        if last[3] > 0:
            print(f"\n  WARNING: {last[3]} errors at concurrency {last[0]}")

    if ephemeral:
        ephemeral.stop()


if __name__ == "__main__":
    main()
