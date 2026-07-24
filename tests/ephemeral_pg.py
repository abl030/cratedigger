"""Disposable PostgreSQL clusters for tests, isolated on private Unix sockets."""

import atexit
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from urllib.parse import quote


class EphemeralPostgresError(RuntimeError):
    """A disposable cluster could not start, with its useful diagnostics."""


class EphemeralPostgres:
    def __init__(self) -> None:
        self.tmpdir: Path | None = None
        self.dsn: str | None = None
        self._server_started = False
        self._started = False

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
        assert self.tmpdir is not None
        return self.tmpdir / "socket"

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

    def start(self) -> None:
        if self._started:
            return
        if not shutil.which("initdb") or not shutil.which("pg_ctl"):
            raise EphemeralPostgresError(
                "initdb/pg_ctl not found; run tests inside nix-shell"
            )

        self.tmpdir = Path(tempfile.mkdtemp(prefix="cratedigger_test_pg_"))
        self._socket_dir.mkdir()
        user = os.getenv("USER", "root")
        try:
            subprocess.run(
                [
                    "initdb", "-D", str(self._datadir), "--no-locale", "-E", "UTF8",
                    "-A", "trust",
                ],
                capture_output=True,
                check=True,
            )
            subprocess.run(
                [
                    "pg_ctl", "-D", str(self._datadir), "-l", str(self._logfile), "-o",
                    f"-k {self._socket_dir} -c listen_addresses=''", "start",
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

    def __enter__(self) -> "EphemeralPostgres":
        self.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.stop()
