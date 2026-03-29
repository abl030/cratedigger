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

# Try to start ephemeral slskd if docker + creds available
_slskd = None
CREDS_FILE = os.path.join(os.path.dirname(__file__), ".slskd-creds.json")

if not os.environ.get("SLSKD_TEST_HOST") and os.path.exists(CREDS_FILE) and shutil.which("docker"):
    try:
        from ephemeral_slskd import EphemeralSlskd
        _slskd = EphemeralSlskd(CREDS_FILE)
        _slskd.start()
        os.environ["SLSKD_TEST_HOST"] = _slskd.host_url
        os.environ["SLSKD_TEST_API_KEY"] = _slskd.api_key
        os.environ["SLSKD_TEST_DOWNLOAD_DIR"] = _slskd.download_dir
        # Wait for Soulseek connection (needed for search tests)
        if _slskd.wait_for_soulseek(timeout=60):
            print(f"[INFO] Ephemeral slskd connected to Soulseek on port {_slskd.port}", file=sys.stderr)
        else:
            print(f"[WARN] Ephemeral slskd API up but not connected to Soulseek", file=sys.stderr)
    except Exception as e:
        print(f"[WARN] Could not start ephemeral slskd: {e}", file=sys.stderr)
        _slskd = None
