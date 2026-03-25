"""Shared test fixtures — ephemeral PostgreSQL server.

Starts a throwaway PostgreSQL on a random port before any DB tests run,
tears it down after. Completely isolated from any system PostgreSQL.

Requires: nix-shell -p postgresql python3Packages.psycopg2
"""

import os
import shutil
import sys

# Make this available to all test modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

# Try to start ephemeral PostgreSQL if tools are available
_pg = None
TEST_DSN = os.environ.get("TEST_DB_DSN")

if not TEST_DSN and shutil.which("initdb") and shutil.which("pg_ctl"):
    try:
        from ephemeral_pg import EphemeralPostgres
        _pg = EphemeralPostgres()
        _pg.start()
        TEST_DSN = _pg.dsn
        os.environ["TEST_DB_DSN"] = TEST_DSN
    except Exception as e:
        print(f"[WARN] Could not start ephemeral PostgreSQL: {e}", file=sys.stderr)
        _pg = None
