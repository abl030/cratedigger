"""Deterministic contracts for disposable PostgreSQL test clusters."""

from __future__ import annotations

import subprocess
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import psycopg2

from lib.ephemeral_postgres import EphemeralPostgres, EphemeralPostgresError
from scripts.run_world_model_burst import IN_PROCESS_JOB_CAP


def _server_option_int(options: tuple[str, ...], name: str) -> int:
    """Read an integer ``-c name=value`` out of ``_server_options``."""
    prefix = f"-c {name}="
    for option in options:
        if option.startswith(prefix):
            return int(option.removeprefix(prefix))
    raise AssertionError(f"{name!r} not found in server options: {options!r}")


class TestEphemeralPostgresFailures(unittest.TestCase):
    def test_transition_seed_refuses_before_disposable_cluster_starts(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            EphemeralPostgresError,
            "has not started",
        ):
            EphemeralPostgres().seed_transition_request(
                artist_name="Artist",
                album_title="Album",
                mb_release_id="release-id",
            )

    def test_initdb_failure_is_diagnostic_and_cleans_its_temporary_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            failed_dir = base / "failed-cluster"
            socket_dir = base / "socket"

            def make_tempdir(*_args: object, **_kwargs: object) -> str:
                target = socket_dir if _kwargs.get("dir") == "/tmp" else failed_dir
                target.mkdir()
                return str(target)

            with (
                patch("lib.ephemeral_postgres.shutil.which", return_value="/bin/tool"),
                patch("tempfile.mkdtemp", make_tempdir),
                patch(
                    "lib.ephemeral_postgres.subprocess.run",
                    side_effect=subprocess.CalledProcessError(
                        1,
                        ["initdb"],
                        output=b"initdb stdout",
                        stderr=b"initdb stderr",
                    ),
                ),
                self.assertRaisesRegex(EphemeralPostgresError, "initdb stderr"),
            ):
                EphemeralPostgres().start()

            self.assertFalse(failed_dir.exists())
            self.assertFalse(socket_dir.exists())


class TestEphemeralPostgresIsolation(unittest.TestCase):
    def test_server_options_open_with_the_private_socket_and_local_listener(
        self,
    ) -> None:
        """Issue #1131 review round 2 (F7): split out from the checkpoint
        pin below — this test's name now matches exactly what it asserts."""
        pg = EphemeralPostgres()
        pg._socket_tmpdir = Path("/tmp/cdpg-socket-contract")

        self.assertEqual(
            pg._server_options[:2],
            (
                "-k /tmp/cdpg-socket-contract",
                "-c listen_addresses=''",
            ),
        )

    def test_server_options_pin_the_utc_clock_frame(self) -> None:
        """The disposable cluster's sessions must truncate dates in the
        same frame the fakes mirror (UTC), or the read-projection
        value-parity gate fails only inside two one-hour UTC wall-clock
        windows per day (issue #1312 gate incident, 2026-09-01 00:04
        UTC). ``tests/test_pipeline_db.py::TestEphemeralPostgresClockFrame``
        pins the live session setting; this pins the argv."""
        pg = EphemeralPostgres()
        pg._socket_tmpdir = Path("/tmp/cdpg-clock-contract")

        self.assertEqual(pg._server_options[2], "-c timezone=UTC")

    def test_server_options_bound_delayed_relation_unlink_lifetime(self) -> None:
        pg = EphemeralPostgres()
        pg._socket_tmpdir = Path("/tmp/cdpg-checkpoint-contract")

        self.assertEqual(
            pg._server_options[3:5],
            (
                "-c checkpoint_timeout=30s",
                "-c checkpoint_completion_target=0.1",
            ),
        )

    def test_server_options_include_the_disposable_diet_settings(self) -> None:
        """Issue #1131: argv-pins the RAM/tmpfs trims, split out from the
        socket/checkpoint pins above so each test's name matches what it
        actually asserts."""
        pg = EphemeralPostgres()
        pg._socket_tmpdir = Path("/tmp/cdpg-diet-contract")

        self.assertEqual(
            pg._server_options[5:],
            (
                "-c shared_buffers=16MB",
                "-c fsync=off",
                "-c full_page_writes=off",
                "-c synchronous_commit=off",
                "-c wal_level=minimal",
                "-c max_wal_senders=0",
                "-c min_wal_size=2MB",
                "-c max_wal_size=8MB",
                "-c log_checkpoints=off",
                "-c autovacuum=off",
                "-c max_connections=50",
            ),
        )

    def test_max_connections_exceeds_the_world_model_burst_job_cap(self) -> None:
        """Issue #1131 review round 2 (F2): the genuinely load-bearing
        invariant, pinned statically against the REAL imported
        `IN_PROCESS_JOB_CAP` — not a hand-copied number — so raising that
        cap flips this pin without a live cluster, threads, or timing.
        """
        pg = EphemeralPostgres()
        pg._socket_tmpdir = Path("/tmp/cdpg-max-connections-contract")

        max_connections = _server_option_int(pg._server_options, "max_connections")

        self.assertGreater(max_connections, IN_PROCESS_JOB_CAP)

    def test_initdb_args_skip_fsync_and_shrink_wal_segments(self) -> None:
        """Issue #1131: mirrors the `_server_options` seam pin above.

        Before this test, nothing asserted `initdb`'s argv at all — the only
        existing initdb test patches `subprocess.run` wholesale with a
        `CalledProcessError` side effect and never inspects the call args —
        so `--no-sync`/`--wal-segsize=1` could be deleted without failing
        anything.
        """
        pg = EphemeralPostgres()
        pg.tmpdir = Path("/tmp/cdpg-initdb-contract")

        self.assertEqual(
            pg._initdb_args,
            (
                "initdb", "-D", "/tmp/cdpg-initdb-contract/data", "--no-locale",
                "-E", "UTF8", "-A", "trust", "--no-sync", "--wal-segsize=1",
            ),
        )

    def test_live_cluster_bounds_delayed_relation_unlink_lifetime(self) -> None:
        with EphemeralPostgres() as pg:
            assert pg.dsn is not None
            with psycopg2.connect(pg.dsn) as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT current_setting('checkpoint_timeout'), "
                    "current_setting('checkpoint_completion_target')"
                )
                self.assertEqual(cursor.fetchone(), ("30s", "0.1"))

    def test_live_cluster_applies_the_disposable_diet_settings(self) -> None:
        """Issue #1131: every RAM/tmpfs trim actually takes effect live.

        `_server_options` is a seam-level pin on the argv shape; this proves
        PostgreSQL itself accepted and applied every setting rather than
        silently falling back to a stock default on a typo or a rejected
        value.
        """
        with EphemeralPostgres() as pg:
            assert pg.dsn is not None
            with psycopg2.connect(pg.dsn) as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT current_setting('shared_buffers'), "
                    "current_setting('fsync'), "
                    "current_setting('full_page_writes'), "
                    "current_setting('synchronous_commit'), "
                    "current_setting('wal_level'), "
                    "current_setting('max_wal_senders'), "
                    "current_setting('min_wal_size'), "
                    "current_setting('max_wal_size'), "
                    "current_setting('log_checkpoints'), "
                    "current_setting('autovacuum'), "
                    "current_setting('max_connections')"
                )
                self.assertEqual(
                    cursor.fetchone(),
                    (
                        "16MB", "off", "off", "off", "minimal", "0", "2MB", "8MB",
                        "off", "off", "50",
                    ),
                )

    def test_initdb_shrinks_the_wal_segment_size_to_its_floor(self) -> None:
        """--wal-segsize=1 is initdb-time, so pin it directly (issue #1131)."""
        with EphemeralPostgres() as pg:
            assert pg.dsn is not None
            with psycopg2.connect(pg.dsn) as connection, connection.cursor() as cursor:
                cursor.execute("SHOW wal_segment_size")
                self.assertEqual(cursor.fetchone(), ("1MB",))

    def test_live_cluster_keeps_checkpoint_warning_at_its_default(self) -> None:
        """Issue #1131 review round 2 (F5): checkpoint_warning is
        deliberately NOT silenced — it is the one in-band signal that would
        tell an operator this cluster's own min_wal_size/max_wal_size
        ceiling has been pushed too small for some future workload, and
        this proves it is genuinely live, not accidentally still 0."""
        with EphemeralPostgres() as pg:
            assert pg.dsn is not None
            with psycopg2.connect(pg.dsn) as connection, connection.cursor() as cursor:
                cursor.execute("SHOW checkpoint_warning")
                self.assertEqual(cursor.fetchone(), ("30s",))

    def test_long_tmpdir_keeps_socket_path_below_postgres_limit(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as root:
            long_tmpdir = Path(root) / ("deep-test-scratch-" * 5)
            long_tmpdir.mkdir()
            with EphemeralPostgres(temp_directory=long_tmpdir) as pg:
                assert pg.tmpdir is not None
                self.assertTrue(pg.tmpdir.is_relative_to(long_tmpdir))
                socket_path = pg._socket_dir / ".s.PGSQL.5432"
                self.assertLessEqual(len(str(socket_path).encode()), 107)
                data_dir = pg.tmpdir
                socket_dir = pg._socket_dir

            self.assertFalse(data_dir.exists())
            self.assertFalse(socket_dir.exists())

    def test_multiple_instances_have_isolated_live_unix_socket_clusters(self) -> None:
        def start_query_stop(_: int) -> str:
            with EphemeralPostgres() as pg:
                assert pg.dsn is not None
                self.assertIn("host=%2F", pg.dsn)
                with psycopg2.connect(pg.dsn) as connection, connection.cursor() as cursor:
                    cursor.execute("SELECT current_database()")
                    row = cursor.fetchone()
                    assert row is not None
                    return str(row[0])

        with ThreadPoolExecutor(max_workers=4) as executor:
            databases = tuple(executor.map(start_query_stop, range(4)))

        self.assertEqual(databases, ("cratedigger_test",) * 4)

    def test_admits_more_than_the_world_model_burst_cap_concurrently(self) -> None:
        """Issue #1131: end-to-end companion to the static
        `test_max_connections_exceeds_the_world_model_burst_job_cap` pin
        above — proves a real cluster actually admits that many real
        connections, not just that the arithmetic works out.

        scripts/run_world_model_burst.py runs ONE coordinator-owned cluster
        (this same `EphemeralPostgres`) with up to `IN_PROCESS_JOB_CAP`
        concurrent child processes, each holding a connection to its own
        cloned database on that cluster, plus transient createdb/dropdb
        maintenance connections for every clone create/drop. The count
        below is derived from the real imported constant (round 2 review
        F2: an earlier version hardcoded 32 behind a false "circular
        import" excuse — scripts/run_world_model_burst.py never imports
        this test module, so importing it here is safe and
        tests/test_world_model_coordinator.py already does).

        `barrier.wait` is a deadlock guard, not the assertion (round 2
        review F4): a genuine per-connection failure calls `barrier.abort()`
        so every OTHER waiting thread fails fast instead of silently
        timing out and reporting a `BrokenBarrierError` that looks like a
        scheduling delay. The timeout itself is generous — this repo's own
        shared test host is documented to run under real load — so a slow
        but eventually-successful connection is never mistaken for a
        rejected one.
        """
        concurrent_connections = IN_PROCESS_JOB_CAP + 2

        with EphemeralPostgres() as pg:
            assert pg.dsn is not None
            barrier = threading.Barrier(concurrent_connections)
            errors: list[BaseException] = []

            def connect_and_hold(_: int) -> None:
                try:
                    with psycopg2.connect(pg.dsn) as connection:
                        barrier.wait(timeout=60)
                        with connection.cursor() as cursor:
                            cursor.execute("SELECT 1")
                            cursor.fetchone()
                except BaseException as error:  # noqa: BLE001 - proving admission
                    errors.append(error)
                    barrier.abort()

            with ThreadPoolExecutor(max_workers=concurrent_connections) as executor:
                list(executor.map(connect_and_hold, range(concurrent_connections)))

        self.assertEqual(errors, [])
