"""Ephemeral PostgreSQL server for tests.

Spins up an isolated PostgreSQL instance on a random port with a temp data
directory. Completely independent of any system PostgreSQL. Requires initdb
and pg_ctl on PATH (provided by pkgs.postgresql in Nix).

Usage:
    from ephemeral_pg import EphemeralPostgres

    # Module-level (shared across all tests):
    pg = EphemeralPostgres()
    pg.start()
    DSN = pg.dsn

    # In teardown:
    pg.stop()

Or as a context manager:
    with EphemeralPostgres() as pg:
        db = PipelineDB(pg.dsn)
"""

import atexit
import os
import shutil
import socket
import subprocess
import tempfile
import time


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class EphemeralPostgres:
    def __init__(self):
        self.tmpdir = None
        self.port = None
        self.dsn = None
        self._started = False

    def start(self):
        if self._started:
            return

        # Check that initdb/pg_ctl are available
        if not shutil.which("initdb") or not shutil.which("pg_ctl"):
            raise RuntimeError(
                "initdb/pg_ctl not found. Run tests inside: "
                "nix-shell -p postgresql python3Packages.psycopg2"
            )

        self.tmpdir = tempfile.mkdtemp(prefix="soularr_test_pg_")
        self.port = _find_free_port()
        datadir = os.path.join(self.tmpdir, "data")
        logfile = os.path.join(self.tmpdir, "pg.log")
        sockdir = self.tmpdir

        # initdb
        subprocess.run(
            ["initdb", "-D", datadir, "--no-locale", "-E", "UTF8", "-A", "trust"],
            capture_output=True, check=True,
        )

        # Start postgres on random port, unix socket in tmpdir
        subprocess.run(
            ["pg_ctl", "-D", datadir, "-l", logfile, "-o",
             f"-p {self.port} -k {sockdir} -c listen_addresses=127.0.0.1",
             "start"],
            capture_output=True, check=True,
        )

        # Wait for it to be ready
        for _ in range(30):
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host="127.0.0.1", port=self.port, dbname="postgres", user=os.getenv("USER", "root")
                )
                conn.close()
                break
            except Exception:
                time.sleep(0.1)
        else:
            self.stop()
            raise RuntimeError(f"PostgreSQL failed to start. Log: {logfile}")

        # Create test database
        import psycopg2
        conn = psycopg2.connect(
            host="127.0.0.1", port=self.port, dbname="postgres", user=os.getenv("USER", "root")
        )
        conn.autocommit = True
        conn.cursor().execute("CREATE DATABASE soularr_test")
        conn.close()

        self.dsn = f"postgresql://{os.getenv('USER', 'root')}@127.0.0.1:{self.port}/soularr_test"
        self._started = True

        # Register cleanup in case stop() is never called
        atexit.register(self.stop)

    def stop(self):
        if not self._started:
            return
        self._started = False

        datadir = os.path.join(self.tmpdir, "data")
        subprocess.run(
            ["pg_ctl", "-D", datadir, "-m", "immediate", "stop"],
            capture_output=True,
        )

        shutil.rmtree(self.tmpdir, ignore_errors=True)
        self.tmpdir = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()
