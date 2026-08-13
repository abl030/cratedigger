"""Disposable PostgreSQL clusters isolated on private Unix sockets."""

from __future__ import annotations

import atexit
import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Self
from urllib.parse import quote

from lib.pipeline_db import PipelineDB


class EphemeralPostgresError(RuntimeError):
    """A disposable cluster could not start, with its useful diagnostics."""


class EphemeralPostgres:
    """Own one temporary PostgreSQL cluster for replay or test isolation."""

    def __init__(self, *, temp_directory: str | Path | None = None) -> None:
        self._temp_directory = (
            str(temp_directory) if temp_directory is not None else None
        )
        self.tmpdir: Path | None = None
        self._socket_tmpdir: Path | None = None
        self.dsn: str | None = None
        self._server_started = False
        self._started = False

    def seed_transition_request(
        self,
        *,
        artist_name: str,
        album_title: str,
        mb_release_id: str,
    ) -> int:
        """Seed request authority only in this disposable database."""
        if self.dsn is None:
            raise EphemeralPostgresError(
                "disposable PostgreSQL has not started"
            )
        db = PipelineDB(self.dsn)
        try:
            return db.add_request(
                artist_name=artist_name,
                album_title=album_title,
                source="request",
                mb_release_id=mb_release_id,
            )
        finally:
            db.close()

    @property
    def _datadir(self) -> Path:
        assert self.tmpdir is not None
        return self.tmpdir / "data"

    @property
    def _logfile(self) -> Path:
        assert self.tmpdir is not None
        return self.tmpdir / "pg.log"

    @property
    def _socket_dir(self) -> Path:
        assert self._socket_tmpdir is not None
        return self._socket_tmpdir

    @property
    def _server_options(self) -> tuple[str, ...]:
        # This cluster is disposable: its data is destroyed at teardown (or
        # on the next reboot if teardown never runs) and it is used by
        # exactly one test worker process. Every setting below trades a
        # production durability/scalability guarantee this world does not
        # need for less RAM and less tmpfs — do NOT copy this list into a
        # production or long-lived Postgres config.
        return (
            f"-k {self._socket_dir}",
            "-c listen_addresses=''",
            # PostgreSQL defers unlinking relation files replaced by TRUNCATE
            # until a checkpoint. Bound that disposable-test scratch lifetime
            # instead of relying on the five-minute production default.
            "-c checkpoint_timeout=30s",
            "-c checkpoint_completion_target=0.1",
            # 128MB (the stock default) is sized for a real workload's
            # buffer pool; a throwaway schema exercised by one worker at a
            # time never approaches it. This is the single biggest real-RAM
            # lever available (issue #1131) — it does not touch tmpfs bytes,
            # since shared_buffers is anonymous shared memory, not a file
            # under the data directory.
            "-c shared_buffers=16MB",
            # No crash recovery is ever performed on this cluster — it is
            # deleted outright on any failure (EphemeralPostgres._cleanup)
            # rather than restarted against its data directory. Durability
            # and torn-page protection exist to survive a crash; skipping
            # them here is safe by construction, not merely acceptable.
            "-c fsync=off",
            "-c full_page_writes=off",
            "-c synchronous_commit=off",
            # wal_level=minimal cannot start with wal_senders > 0 — nothing
            # here streams, replicates, or does PITR, so replica-level WAL
            # (the default) buys this cluster nothing. Beyond shrinking
            # what's retained, minimal also lets Postgres skip WAL entirely
            # for operations on a relation created or truncated in the SAME
            # transaction — tests TRUNCATE between runs, so that path is hit
            # constantly here.
            "-c wal_level=minimal",
            "-c max_wal_senders=0",
            # 2MB is the hard-enforced floor with 1MB WAL segments (initdb
            # --wal-segsize=1 below); measured empirically (issue #1131) —
            # postgres refuses to start below it. Pushed to the floor
            # deliberately: a wider ceiling measured zero wall-time benefit
            # on a 553-real-DB-test burst (both landed at ~14s) while
            # costing an extra ~8MB of retained WAL for every doubling, so
            # there is no knee to stop at short of the floor itself — the
            # checkpoint churn this forces is CPU work against RAM, not disk
            # I/O, and this cluster has nothing else to do with that CPU.
            "-c min_wal_size=2MB",
            "-c max_wal_size=8MB",
            # The much smaller max_wal_size above forces frequent
            # checkpoints; both of these silence the resulting log spam
            # ("checkpoint starting: wal" and "checkpoints are occurring too
            # frequently") — that log lives in this same tmpdir on the same
            # tmpfs, so left on it would claw back a meaningful slice of the
            # WAL saving over a real worker's lifetime (measured: ~50KB of
            # spam from ONE 553-test module before these were added, at
            # zero cost since nothing here reads this cluster's checkpoint
            # telemetry).
            "-c checkpoint_warning=0",
            "-c log_checkpoints=off",
            # Autovacuum exists to reclaim space and update planner stats
            # over a database's working lifetime. Nothing here has one:
            # tests TRUNCATE between runs (reclaiming space immediately,
            # unlike DELETE) and the whole cluster is destroyed in minutes.
            "-c autovacuum=off",
            # NOT one connection at a time: scripts/run_world_model_burst.py
            # runs ONE coordinator-owned EphemeralPostgres (this same class)
            # with up to IN_PROCESS_JOB_CAP=30 concurrent child processes,
            # each holding a connection to its own cloned database on this
            # cluster, plus transient createdb/dropdb maintenance
            # connections for every clone create/drop. 50 clears that cap
            # with real headroom instead of the canonical suite's own
            # handful-of-threads ceiling; the PGPROC/lock-table cost of 50
            # vs. the stock 100 is trivial next to the WAL/shared_buffers
            # savings above.
            "-c max_connections=50",
        )

    @property
    def _initdb_args(self) -> tuple[str, ...]:
        return (
            "initdb", "-D", str(self._datadir), "--no-locale", "-E", "UTF8",
            "-A", "trust",
            # Disposable cluster (see _server_options): skip initdb's own
            # fsync of the freshly written catalog files, and shrink the
            # WAL segment size to its 1MB floor so pg_wal starts (and
            # stays) far below the 16MB-per-segment default footprint.
            "--no-sync", "--wal-segsize=1",
        )

    def _failure_detail(self, error: subprocess.CalledProcessError) -> str:
        command = " ".join(str(part) for part in error.cmd)
        stdout = (error.stdout or b"").decode("utf-8", errors="replace")
        stderr = (error.stderr or b"").decode("utf-8", errors="replace")
        log = ""
        if self.tmpdir is not None and self._logfile.is_file():
            log = self._logfile.read_text(encoding="utf-8", errors="replace")
        return (
            f"PostgreSQL command failed ({error.returncode}): {command}\n"
            f"stdout:\n{stdout}\nstderr:\n{stderr}\npg.log:\n{log}"
        )

    def _cleanup(self) -> None:
        if self._server_started and self.tmpdir is not None:
            subprocess.run(
                ["pg_ctl", "-D", str(self._datadir), "-m", "immediate", "stop"],
                capture_output=True,
                check=False,
            )
        self._server_started = False
        self._started = False
        self.dsn = None
        if self.tmpdir is not None:
            shutil.rmtree(self.tmpdir, ignore_errors=True)
        self.tmpdir = None
        if self._socket_tmpdir is not None:
            shutil.rmtree(self._socket_tmpdir, ignore_errors=True)
        self._socket_tmpdir = None

    def start(self) -> None:
        if self._started:
            return
        if not shutil.which("initdb") or not shutil.which("pg_ctl"):
            raise EphemeralPostgresError(
                "initdb/pg_ctl not found; run inside nix-shell"
            )

        user = os.getenv("USER", "root")
        try:
            self.tmpdir = Path(tempfile.mkdtemp(
                prefix="cratedigger_ephemeral_pg_",
                dir=self._temp_directory,
            ))
            # PostgreSQL's sockaddr_un path is limited to 107 bytes on Linux.
            # Keep the data and logs under the caller's (possibly long) TMPDIR,
            # but allocate the tiny private socket directory beneath a stable,
            # short path. systemd's PrivateTmp still isolates /tmp for the
            # unattended gate.
            self._socket_tmpdir = Path(tempfile.mkdtemp(prefix="cdpg-", dir="/tmp"))
            subprocess.run(
                self._initdb_args,
                capture_output=True,
                check=True,
            )
            subprocess.run(
                [
                    "pg_ctl", "-D", str(self._datadir), "-l", str(self._logfile),
                    "-o", " ".join(self._server_options), "start",
                ],
                capture_output=True,
                check=True,
            )
            self._server_started = True

            import psycopg2

            for _ in range(30):
                try:
                    with psycopg2.connect(
                        host=str(self._socket_dir), dbname="postgres", user=user,
                    ):
                        break
                except psycopg2.OperationalError:
                    time.sleep(0.1)
            else:
                log = self._logfile.read_text(encoding="utf-8", errors="replace")
                raise EphemeralPostgresError(
                    f"PostgreSQL did not become ready. pg.log:\n{log}"
                )

            connection = psycopg2.connect(
                host=str(self._socket_dir), dbname="postgres", user=user,
            )
            try:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    cursor.execute("CREATE DATABASE cratedigger_test")
            finally:
                connection.close()
            self.dsn = (
                f"postgresql://{quote(user)}@/cratedigger_test?host="
                f"{quote(str(self._socket_dir), safe='')}"
            )
            self._started = True
            atexit.register(self.stop)
        except subprocess.CalledProcessError as error:
            detail = self._failure_detail(error)
            self._cleanup()
            raise EphemeralPostgresError(detail) from error
        except Exception:
            self._cleanup()
            raise

    def stop(self) -> None:
        self._cleanup()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.stop()
