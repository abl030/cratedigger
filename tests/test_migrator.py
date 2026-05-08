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


# ---------------------------------------------------------------------------
# Migration 014 — persisted search plans (U1)
# ---------------------------------------------------------------------------

@requires_postgres
class TestPersistedSearchPlansSchema(unittest.TestCase):
    """Migration 014 lands the search_plans schema and adds nullable plan
    context to album_requests / search_log without breaking historical rows.
    """

    def _query(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def _exec(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
        finally:
            conn.close()

    def test_search_plans_table_exists_with_expected_columns(self):
        rows = self._query("""
            SELECT column_name, is_nullable, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'search_plans'
        """)
        cols = {r[0] for r in rows}
        self.assertIn("id", cols)
        self.assertIn("request_id", cols)
        self.assertIn("generator_id", cols)
        self.assertIn("status", cols)
        self.assertIn("failure_class", cols)
        self.assertIn("metadata_snapshot", cols)
        self.assertIn("provenance", cols)
        self.assertIn("error_message", cols)
        self.assertIn("superseded_at", cols)
        self.assertIn("superseded_by_plan_id", cols)
        self.assertIn("created_at", cols)

    def test_search_plan_items_table_exists_with_expected_columns(self):
        rows = self._query("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'search_plan_items'
        """)
        cols = {r[0] for r in rows}
        for col in (
            "id", "plan_id", "ordinal", "strategy",
            "query", "canonical_query_key", "repeat_group", "provenance",
        ):
            self.assertIn(col, cols)

    def test_album_requests_has_cursor_columns(self):
        rows = self._query("""
            SELECT column_name, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'album_requests'
              AND column_name IN (
                'active_plan_id', 'next_plan_ordinal', 'plan_cycle_count')
        """)
        by_col = {r[0]: (r[1], r[2]) for r in rows}
        self.assertIn("active_plan_id", by_col)
        # active_plan_id MUST be nullable -- requests can exist without a plan.
        self.assertEqual(by_col["active_plan_id"][0], "YES")
        self.assertIn("next_plan_ordinal", by_col)
        self.assertEqual(by_col["next_plan_ordinal"][0], "NO")
        self.assertIn("plan_cycle_count", by_col)
        self.assertEqual(by_col["plan_cycle_count"][0], "NO")

    def test_search_log_has_nullable_plan_context_columns(self):
        rows = self._query("""
            SELECT column_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'search_log'
              AND column_name IN (
                'plan_id', 'plan_item_id', 'plan_ordinal', 'plan_strategy',
                'plan_canonical_query_key', 'plan_repeat_group',
                'plan_generator_id', 'execution_stage', 'attempt_consumed',
                'cursor_update_status', 'stale_reason', 'plan_cycle_snapshot')
        """)
        # All 12 columns present and every one nullable.
        self.assertEqual(len(rows), 12)
        for col, is_null in rows:
            with self.subTest(col=col):
                self.assertEqual(is_null, "YES")

    def test_search_log_outcome_check_still_allows_exhausted(self):
        """Migration 014 must NOT tighten the outcome domain.

        Historical search_log rows with outcome='exhausted' must remain valid
        even though new code stops emitting it.
        """
        # Add a request to satisfy the FK.
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('exhausted-legacy-mbid', 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """)
        rid_rows = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("exhausted-legacy-mbid",),
        )
        rid = rid_rows[0][0]
        try:
            # If the outcome CHECK rejected 'exhausted', this would raise.
            self._exec(
                "INSERT INTO search_log (request_id, outcome) VALUES (%s, %s)",
                (rid, "exhausted"),
            )
            rows = self._query(
                "SELECT outcome FROM search_log WHERE request_id = %s", (rid,)
            )
            self.assertIn(("exhausted",), rows)
        finally:
            self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))

    def test_one_active_plan_per_request_partial_unique_index(self):
        """Inserting a second status='active' plan for the same request fails;
        failed/superseded plans coexist freely."""
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('active-uniq-mbid', 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """)
        rid = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("active-uniq-mbid",),
        )[0][0]
        try:
            self._exec(
                "INSERT INTO search_plans (request_id, generator_id, status) "
                "VALUES (%s, %s, %s)",
                (rid, "g1", "active"),
            )
            with self.assertRaises(psycopg2.errors.UniqueViolation):
                self._exec(
                    "INSERT INTO search_plans (request_id, generator_id, status) "
                    "VALUES (%s, %s, %s)",
                    (rid, "g1", "active"),
                )
            # Failed and superseded rows are NOT blocked.
            self._exec(
                "INSERT INTO search_plans (request_id, generator_id, status) "
                "VALUES (%s, %s, %s)",
                (rid, "g1", "failed_deterministic"),
            )
            self._exec(
                "INSERT INTO search_plans (request_id, generator_id, status) "
                "VALUES (%s, %s, %s)",
                (rid, "g1", "superseded"),
            )
            count = self._query(
                "SELECT COUNT(*) FROM search_plans WHERE request_id = %s", (rid,)
            )[0][0]
            self.assertEqual(count, 3)
        finally:
            self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))

    def test_active_plan_must_belong_to_request(self):
        """album_requests.active_plan_id is enforced to point at a plan
        whose request_id matches this row's id."""
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('owner-a-mbid', 'A', 'B', 'request'),
                   ('owner-b-mbid', 'C', 'D', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """)
        rid_a = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("owner-a-mbid",),
        )[0][0]
        rid_b = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("owner-b-mbid",),
        )[0][0]
        try:
            # Plan owned by request A.
            self._exec(
                "INSERT INTO search_plans (request_id, generator_id, status) "
                "VALUES (%s, %s, %s)",
                (rid_a, "g1", "active"),
            )
            plan_id = self._query(
                "SELECT id FROM search_plans WHERE request_id = %s", (rid_a,)
            )[0][0]

            # Setting request B's active_plan_id to a plan owned by A must fail.
            with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
                self._exec(
                    "UPDATE album_requests SET active_plan_id = %s WHERE id = %s",
                    (plan_id, rid_b),
                )

            # Setting request A's active_plan_id to its own plan succeeds.
            self._exec(
                "UPDATE album_requests SET active_plan_id = %s WHERE id = %s",
                (plan_id, rid_a),
            )
        finally:
            self._exec("DELETE FROM album_requests WHERE id IN (%s, %s)", (rid_a, rid_b))

    def test_plan_delete_nulls_active_plan_id_only(self):
        """Deleting an active plan nulls album_requests.active_plan_id but the
        request row's PK and other fields are unaffected (PG15+ SET NULL on
        a single column)."""
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('plan-null-mbid', 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """)
        rid = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("plan-null-mbid",),
        )[0][0]
        try:
            self._exec(
                "INSERT INTO search_plans (request_id, generator_id, status) "
                "VALUES (%s, %s, %s)",
                (rid, "g1", "active"),
            )
            plan_id = self._query(
                "SELECT id FROM search_plans WHERE request_id = %s", (rid,)
            )[0][0]
            self._exec(
                "UPDATE album_requests SET active_plan_id = %s WHERE id = %s",
                (plan_id, rid),
            )
            self._exec("DELETE FROM search_plans WHERE id = %s", (plan_id,))
            row = self._query(
                "SELECT id, active_plan_id FROM album_requests WHERE id = %s",
                (rid,),
            )
            self.assertEqual(row[0][0], rid)  # request still here
            self.assertIsNone(row[0][1])
        finally:
            self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))

    def test_request_delete_cascades_plans_and_items_but_logs_remain(self):
        """Deleting a request cascades to plans and plan items, but search_log
        rows survive (their plan FKs are nullified) so historical inspection
        keeps working."""
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('cascade-mbid', 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """)
        rid = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("cascade-mbid",),
        )[0][0]
        # NOTE: search_log has FK to album_requests with ON DELETE CASCADE
        # (from migration 001), so its rows DON'T survive request deletion.
        # Plans/items must cascade with the request via CASCADE FKs.
        self._exec(
            "INSERT INTO search_plans (request_id, generator_id, status) "
            "VALUES (%s, %s, %s)",
            (rid, "g1", "active"),
        )
        plan_id = self._query(
            "SELECT id FROM search_plans WHERE request_id = %s", (rid,)
        )[0][0]
        self._exec(
            "INSERT INTO search_plan_items (plan_id, ordinal, strategy, query) "
            "VALUES (%s, %s, %s, %s)",
            (plan_id, 0, "default", "artist title"),
        )

        self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))

        # Plans + items cascade away with the request.
        self.assertEqual(
            self._query(
                "SELECT COUNT(*) FROM search_plans WHERE id = %s", (plan_id,)
            )[0][0],
            0,
        )
        self.assertEqual(
            self._query(
                "SELECT COUNT(*) FROM search_plan_items WHERE plan_id = %s",
                (plan_id,),
            )[0][0],
            0,
        )

    def test_plan_item_query_must_be_non_empty(self):
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('empty-q-mbid', 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """)
        rid = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("empty-q-mbid",),
        )[0][0]
        try:
            self._exec(
                "INSERT INTO search_plans (request_id, generator_id, status) "
                "VALUES (%s, %s, %s)",
                (rid, "g1", "active"),
            )
            plan_id = self._query(
                "SELECT id FROM search_plans WHERE request_id = %s", (rid,)
            )[0][0]
            for bad_query in ("", "   ", "\t\n"):
                with self.subTest(q=repr(bad_query)):
                    with self.assertRaises(psycopg2.errors.CheckViolation):
                        self._exec(
                            "INSERT INTO search_plan_items "
                            "(plan_id, ordinal, strategy, query) "
                            "VALUES (%s, %s, %s, %s)",
                            (plan_id, 0, "default", bad_query),
                        )
        finally:
            self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))

    def test_plan_item_unique_plan_ordinal(self):
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('uniq-ord-mbid', 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """)
        rid = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("uniq-ord-mbid",),
        )[0][0]
        try:
            self._exec(
                "INSERT INTO search_plans (request_id, generator_id, status) "
                "VALUES (%s, %s, %s)",
                (rid, "g1", "active"),
            )
            plan_id = self._query(
                "SELECT id FROM search_plans WHERE request_id = %s", (rid,)
            )[0][0]
            self._exec(
                "INSERT INTO search_plan_items "
                "(plan_id, ordinal, strategy, query) "
                "VALUES (%s, %s, %s, %s)",
                (plan_id, 0, "default", "artist title"),
            )
            with self.assertRaises(psycopg2.errors.UniqueViolation):
                self._exec(
                    "INSERT INTO search_plan_items "
                    "(plan_id, ordinal, strategy, query) "
                    "VALUES (%s, %s, %s, %s)",
                    (plan_id, 0, "default", "different"),
                )
        finally:
            self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))

    def test_status_and_failure_class_check_constraints(self):
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('check-mbid', 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """)
        rid = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("check-mbid",),
        )[0][0]
        try:
            with self.assertRaises(psycopg2.errors.CheckViolation):
                self._exec(
                    "INSERT INTO search_plans (request_id, generator_id, status) "
                    "VALUES (%s, %s, %s)",
                    (rid, "g1", "totally-bogus"),
                )
            with self.assertRaises(psycopg2.errors.CheckViolation):
                self._exec(
                    "INSERT INTO search_plans "
                    "(request_id, generator_id, status, failure_class) "
                    "VALUES (%s, %s, %s, %s)",
                    (rid, "g1", "failed_deterministic", "totally-bogus"),
                )
        finally:
            self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))


if __name__ == "__main__":
    unittest.main()
