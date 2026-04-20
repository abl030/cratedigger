"""Tests for lib/migrator.py — minimal versioned SQL migrator.

Mix of pure file-discovery tests (no DB) and integration tests against
the ephemeral PostgreSQL fixture from ``conftest.py``.
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import conftest  # noqa: F401 — sets TEST_DB_DSN env var

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

import psycopg2  # noqa: E402

from lib.migrator import (  # noqa: E402
    DEFAULT_MIGRATIONS_DIR,
    Migration,
    apply_migrations,
    discover_migrations,
)

TEST_DSN: str = os.environ.get("TEST_DB_DSN") or ""


def requires_postgres(cls):
    if not TEST_DSN:
        return unittest.skip("TEST_DB_DSN not set — skipping PostgreSQL migrator tests")(cls)
    return cls


# ---------------------------------------------------------------------------
# Pure file-discovery tests (no DB)
# ---------------------------------------------------------------------------

class TestDiscoverMigrations(unittest.TestCase):
    """File parsing and ordering — pure logic, no DB needed."""

    def _write(self, dirpath: str, filename: str, body: str = "-- noop\n") -> None:
        with open(os.path.join(dirpath, filename), "w") as f:
            f.write(body)

    def test_discovers_and_orders_by_version(self):
        with tempfile.TemporaryDirectory() as d:
            self._write(d, "002_second.sql")
            self._write(d, "010_tenth.sql")
            self._write(d, "001_first.sql")

            migs = discover_migrations(d)

            self.assertEqual([m.version for m in migs], [1, 2, 10])
            self.assertEqual([m.name for m in migs], ["first", "second", "tenth"])
            self.assertEqual(migs[0].label, "001_first")
            self.assertEqual(migs[2].label, "010_tenth")

    def test_returns_migration_dataclass(self):
        with tempfile.TemporaryDirectory() as d:
            self._write(d, "001_initial.sql")
            migs = discover_migrations(d)
            self.assertIsInstance(migs[0], Migration)
            self.assertEqual(migs[0].path, os.path.join(d, "001_initial.sql"))

    def test_rejects_malformed_filename(self):
        for filename in ["no_number.sql", "001-bad-dashes.sql"]:
            with self.subTest(filename=filename):
                with tempfile.TemporaryDirectory() as d:
                    self._write(d, filename)
                    with self.assertRaises(ValueError):
                        discover_migrations(d)

    def test_rejects_short_prefix(self):
        """Migration filenames must use the documented three-digit prefix."""
        with tempfile.TemporaryDirectory() as d:
            self._write(d, "1_short_prefix.sql")
            with self.assertRaises(ValueError):
                discover_migrations(d)

    def test_rejects_duplicate_version(self):
        with tempfile.TemporaryDirectory() as d:
            self._write(d, "001_first.sql")
            self._write(d, "001_other.sql")
            with self.assertRaisesRegex(ValueError, "Duplicate migration version 1"):
                discover_migrations(d)

    def test_missing_directory_raises(self):
        with self.assertRaises(FileNotFoundError):
            discover_migrations("/tmp/this-path-does-not-exist-cratedigger")

    def test_default_migrations_dir_resolves(self):
        """Sanity: the package-level DEFAULT_MIGRATIONS_DIR points at migrations/."""
        self.assertTrue(os.path.isdir(DEFAULT_MIGRATIONS_DIR))
        # 001_initial.sql must exist as the baseline
        self.assertTrue(
            os.path.exists(os.path.join(DEFAULT_MIGRATIONS_DIR, "001_initial.sql"))
        )

    def test_baseline_is_discoverable(self):
        """The shipped 001_initial.sql is discovered and parsed correctly."""
        migs = discover_migrations(DEFAULT_MIGRATIONS_DIR)
        self.assertGreaterEqual(len(migs), 1)
        self.assertEqual(migs[0].version, 1)
        self.assertEqual(migs[0].name, "initial")


# ---------------------------------------------------------------------------
# DB integration tests (require ephemeral PG)
# ---------------------------------------------------------------------------

@requires_postgres
class TestApplyMigrations(unittest.TestCase):
    """End-to-end: apply real migration files against the ephemeral PG.

    Each test uses high, unique version numbers (9000+) to avoid colliding
    with the real shipped migrations that conftest.py already applied at
    session start. Tests clean up their own rows from schema_migrations and
    drop the test tables they created.
    """

    # Test-only tables we may create. Tracked so tearDown can drop them all.
    _TEST_TABLES = [
        "migrator_test_t1",
        "migrator_test_t2",
        "migrator_test_t3",
        "migrator_test_t4",
    ]
    _TEST_VERSION_FLOOR = 9000  # Real migrations live in [1, 8999].

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.migrations_dir = self._tmp.name

    def tearDown(self):
        # Drop any test tables and any test-version rows we may have left.
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        with conn.cursor() as cur:
            for table in self._TEST_TABLES:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(
                "DELETE FROM schema_migrations WHERE version >= %s",
                (self._TEST_VERSION_FLOOR,),
            )
        conn.close()

    def _write_migration(self, version: int, name: str, sql: str) -> None:
        path = os.path.join(self.migrations_dir, f"{version:03d}_{name}.sql")
        with open(path, "w") as f:
            f.write(sql)

    def _query(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def test_records_applied_version_in_tracking_table(self):
        self._write_migration(
            9001, "create_t1",
            "CREATE TABLE migrator_test_t1 (id INT PRIMARY KEY);",
        )
        applied = apply_migrations(TEST_DSN, self.migrations_dir)
        self.assertEqual([m.version for m in applied], [9001])
        rows = self._query(
            "SELECT version, name FROM schema_migrations WHERE version = %s",
            (9001,),
        )
        self.assertEqual(rows, [(9001, "create_t1")])

    def test_idempotent_second_run(self):
        self._write_migration(
            9002, "create_t2",
            "CREATE TABLE migrator_test_t2 (id INT PRIMARY KEY);",
        )
        first = apply_migrations(TEST_DSN, self.migrations_dir)
        second = apply_migrations(TEST_DSN, self.migrations_dir)
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [], "Second run must be a no-op")

    def test_applies_only_new_versions(self):
        self._write_migration(
            9003, "first",
            "CREATE TABLE migrator_test_t3 (id INT PRIMARY KEY);",
        )
        first = apply_migrations(TEST_DSN, self.migrations_dir)
        self.assertEqual([m.version for m in first], [9003])

        self._write_migration(
            9004, "second",
            "ALTER TABLE migrator_test_t3 ADD COLUMN name TEXT;",
        )
        second = apply_migrations(TEST_DSN, self.migrations_dir)
        # Only 9004 is newly applied; 9003 is skipped
        self.assertEqual([m.version for m in second], [9004])

        cols = self._query("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'migrator_test_t3'
        """)
        self.assertEqual({c[0] for c in cols}, {"id", "name"})

    def test_failed_migration_rolls_back(self):
        """A failed migration leaves the schema unchanged AND unrecorded."""
        self._write_migration(
            9005, "broken",
            "CREATE TABLE migrator_test_t4 (id INT PRIMARY KEY);\n"
            "INSERT INTO nonexistent_table VALUES (1);\n",
        )
        with self.assertRaises(psycopg2.Error):
            apply_migrations(TEST_DSN, self.migrations_dir)

        # The CREATE TABLE was rolled back with the failing INSERT
        tables = self._query("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'migrator_test_t4'
        """)
        self.assertEqual(tables, [])

        # And no version row was recorded
        rows = self._query(
            "SELECT version FROM schema_migrations WHERE version = %s",
            (9005,),
        )
        self.assertEqual(rows, [])

    def test_baseline_already_applied_against_existing_schema(self):
        """Re-applying 001_initial.sql against the already-migrated DB is a no-op."""
        # conftest applied the shipped baseline at session start.
        rows = self._query(
            "SELECT version FROM schema_migrations WHERE version = 1"
        )
        self.assertEqual(rows, [(1,)])

        applied = apply_migrations(TEST_DSN, DEFAULT_MIGRATIONS_DIR)
        self.assertEqual(applied, [])


if __name__ == "__main__":
    unittest.main()
