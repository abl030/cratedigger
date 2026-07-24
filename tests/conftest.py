"""Shared test fixtures — ephemeral PostgreSQL server.

Starts a throwaway PostgreSQL on a random port before any DB tests run,
tears it down after. Completely isolated from any system PostgreSQL.

Requires: nix-shell -p postgresql python3Packages.psycopg2
"""

import os
import shutil
import sys

# Put repo root on sys.path so `from lib.X import Y` and `from scripts.X import Y`
# resolve. Do NOT add lib/ or scripts/ directly — that would reintroduce the
# issue #95 dual-load footgun (module reachable as both `quality` and `lib.quality`).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# A test process must own a live disposable database. Falling through to an
# unset DSN makes PipelineDB silently target ambient localhost instead.
_pg = None
TEST_DSN = os.environ.get("TEST_DB_DSN")

if not TEST_DSN:
    if not shutil.which("initdb") or not shutil.which("pg_ctl"):
        raise RuntimeError("initdb/pg_ctl are required for the test database")
    from tests.ephemeral_pg import EphemeralPostgres
    _pg = EphemeralPostgres()
    _pg.start()
    TEST_DSN = _pg.dsn
    assert TEST_DSN is not None
    os.environ["TEST_DB_DSN"] = TEST_DSN

if TEST_DSN and os.environ.get("CRATEDIGGER_TEST_SCHEMA_READY") != "1":
    # Apply schema once at session start for either an externally supplied
    # TEST_DB_DSN or the ephemeral DB above. Parallel-suite module subprocesses
    # inherit an already-migrated worker-local DSN and explicitly skip this
    # redundant step. Test helpers TRUNCATE between tests.
    from lib.migrator import apply_migrations
    try:
        apply_migrations(TEST_DSN)
    except Exception:
        if _pg is not None:
            _pg.stop()
            _pg = None
        raise

# NOTE: the ephemeral-slskd docker bootstrap that used to live here was
# deleted 2026-07-02 — it exported SLSKD_TEST_HOST/_API_KEY/_DOWNLOAD_DIR
# that no test has read since the 2026-05-20 skip-audit purge removed the
# slskd-gated tests. tests/ephemeral_slskd.py survives solely as the
# optional no---host fallback for scripts/bench_parallel_search.py.
