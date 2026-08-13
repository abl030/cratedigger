"""Deterministic contracts for disposable PostgreSQL test clusters."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import psycopg2

from lib.ephemeral_postgres import EphemeralPostgres, EphemeralPostgresError


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
    def test_server_options_bound_delayed_relation_unlink_lifetime(self) -> None:
        pg = EphemeralPostgres()
        pg._socket_tmpdir = Path("/tmp/cdpg-checkpoint-contract")

        self.assertEqual(
            pg._server_options,
            (
                "-k /tmp/cdpg-checkpoint-contract",
                "-c listen_addresses=''",
                "-c checkpoint_timeout=30s",
                "-c checkpoint_completion_target=0.1",
                "-c shared_buffers=16MB",
                "-c fsync=off",
                "-c full_page_writes=off",
                "-c synchronous_commit=off",
                "-c min_wal_size=32MB",
                "-c max_wal_size=64MB",
                "-c autovacuum=off",
                "-c max_connections=20",
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
                    "current_setting('min_wal_size'), "
                    "current_setting('max_wal_size'), "
                    "current_setting('autovacuum'), "
                    "current_setting('max_connections')"
                )
                self.assertEqual(
                    cursor.fetchone(),
                    ("16MB", "off", "off", "off", "32MB", "64MB", "off", "20"),
                )

    def test_initdb_shrinks_the_wal_segment_size_to_its_floor(self) -> None:
        """--wal-segsize=1 is initdb-time, so pin it directly (issue #1131)."""
        with EphemeralPostgres() as pg:
            assert pg.dsn is not None
            with psycopg2.connect(pg.dsn) as connection, connection.cursor() as cursor:
                cursor.execute("SHOW wal_segment_size")
                self.assertEqual(cursor.fetchone(), ("1MB",))

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
