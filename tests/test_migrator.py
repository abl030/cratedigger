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

    def test_search_log_plan_item_must_belong_to_logged_plan(self):
        rid_a = self._query("""
            INSERT INTO album_requests
                (mb_release_id, artist_name, album_title, source)
            VALUES ('owner-a', 'A', 'A', 'request')
            RETURNING id
        """)[0][0]
        rid_b = self._query("""
            INSERT INTO album_requests
                (mb_release_id, artist_name, album_title, source)
            VALUES ('owner-b', 'B', 'B', 'request')
            RETURNING id
        """)[0][0]
        try:
            plan_a = self._query("""
                INSERT INTO search_plans (request_id, generator_id, status)
                VALUES (%s, 'g1', 'active')
                RETURNING id
            """, (rid_a,))[0][0]
            plan_b = self._query("""
                INSERT INTO search_plans (request_id, generator_id, status)
                VALUES (%s, 'g1', 'active')
                RETURNING id
            """, (rid_b,))[0][0]
            item_b = self._query("""
                INSERT INTO search_plan_items
                    (plan_id, ordinal, strategy, query)
                VALUES (%s, 0, 'default', 'q')
                RETURNING id
            """, (plan_b,))[0][0]

            with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
                self._exec("""
                    INSERT INTO search_log
                        (request_id, query, outcome, plan_id, plan_item_id)
                    VALUES (%s, 'q', 'no_match', %s, %s)
                """, (rid_a, plan_a, item_b))
        finally:
            self._exec(
                "DELETE FROM album_requests WHERE id IN (%s, %s)",
                (rid_a, rid_b),
            )

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


@requires_postgres
class TestAlbumQualityEvidenceSchema(unittest.TestCase):
    """Migration 017 stores active quality evidence relationally."""

    def _query(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            conn.close()

    def _exec(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
        finally:
            conn.close()

    def test_tables_and_owner_domain_exist(self):
        # NOTE: post-migration 021 the schema is keyed by
        # ``(mb_release_id, snapshot_fingerprint)`` rather than the legacy
        # ``(owner_type, owner_id)`` domain. The test name is kept for
        # historical continuity; assertion shape has been updated.
        tables = self._query("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN (
                'album_quality_evidence',
                'album_quality_evidence_files'
              )
        """)
        self.assertEqual(
            {row[0] for row in tables},
            {"album_quality_evidence", "album_quality_evidence_files"},
        )

        # mb_release_id is NOT NULL with a length>0 CHECK after 021. The
        # empty string therefore triggers a CHECK violation.
        with self.assertRaises(psycopg2.errors.CheckViolation):
            self._exec("""
                INSERT INTO album_quality_evidence (
                    mb_release_id, snapshot_fingerprint, source_path,
                    measured_at, format, verified_lossless
                )
                VALUES ('', 'fp', '', NOW(), 'mp3 v0', FALSE)
            """)

    def test_relational_file_rows_and_verified_proof_constraints(self):
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('aqe-schema-mbid', 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """)
        rid = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("aqe-schema-mbid",),
        )[0][0]
        try:
            # verified_lossless=TRUE without proof columns must still raise.
            with self.assertRaises(psycopg2.errors.CheckViolation):
                self._exec("""
                    INSERT INTO album_quality_evidence (
                        mb_release_id, snapshot_fingerprint, source_path,
                        measured_at, format, verified_lossless
                    )
                    VALUES ('aqe-schema-mbid', 'fp-1', '/p/1', NOW(), 'flac', TRUE)
                """)

            self._exec("""
                INSERT INTO album_quality_evidence (
                    mb_release_id, snapshot_fingerprint, source_path,
                    measured_at, format,
                    verified_lossless, verified_lossless_proof_origin,
                    verified_lossless_source, verified_lossless_classifier,
                    v0_avg_bitrate_kbps, v0_source_lineage
                )
                VALUES (
                    'aqe-schema-mbid', 'fp-2', '/p/2', NOW(), 'flac',
                    TRUE, 'import',
                    'lossless candidate', 'spectral+v0', 228,
                    'lossless_container_source'
                )
            """)
            evidence_id = self._query(
                "SELECT id FROM album_quality_evidence "
                "WHERE mb_release_id = %s AND snapshot_fingerprint = %s",
                ("aqe-schema-mbid", "fp-2"),
            )[0][0]
            self._exec("""
                INSERT INTO album_quality_evidence_files (
                    evidence_id, ordinal, relative_path, size_bytes, mtime_ns,
                    extension, container, codec
                )
                VALUES (%s, 0, '01.flac', 1000, 10, 'flac', 'flac', 'flac')
            """, (evidence_id,))
            rows = self._query(
                "SELECT relative_path, container FROM album_quality_evidence_files "
                "WHERE evidence_id = %s",
                (evidence_id,),
            )
            self.assertEqual(rows, [("01.flac", "flac")])
        finally:
            self._exec(
                "DELETE FROM album_quality_evidence "
                "WHERE mb_release_id = %s",
                ("aqe-schema-mbid",),
            )
            self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))


@requires_postgres
class TestPreviewEvidenceFactsSchema(unittest.TestCase):
    """Migration 019 adds preview-decision evidence facts + widens two CHECKs.

    Validates:
    - new columns on album_quality_evidence (audio_corrupt, folder_layout,
      audio_file_count, filetype_band, matched_bad_audio_hash_id +path) with
      conservative defaults
    - new per-file decode_ok column with TRUE default
    - download_log.outcome CHECK accepts 'measurement_failed'
    - import_jobs.preview_status CHECK accepts 'measurement_failed'
    - folder_layout CHECK rejects values not in ('flat', 'nested')
    - bad_audio_hash FK cascade-to-NULL on delete
    """

    def _query(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            conn.close()

    def _exec(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
        finally:
            conn.close()

    def test_schema_migrations_records_019(self):
        rows = self._query(
            "SELECT version FROM schema_migrations WHERE version = 19"
        )
        self.assertEqual(rows, [(19,)])

    def test_album_quality_evidence_has_new_preview_fact_columns(self):
        rows = self._query("""
            SELECT column_name, is_nullable, data_type, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'album_quality_evidence'
              AND column_name IN (
                'audio_corrupt', 'folder_layout', 'audio_file_count',
                'filetype_band', 'matched_bad_audio_hash_id',
                'matched_bad_audio_hash_path'
              )
        """)
        by_col = {r[0]: (r[1], r[2], r[3]) for r in rows}
        self.assertIn("audio_corrupt", by_col)
        self.assertEqual(by_col["audio_corrupt"][0], "NO")
        self.assertEqual(by_col["audio_corrupt"][1], "boolean")
        self.assertIn("false", (by_col["audio_corrupt"][2] or "").lower())
        self.assertIn("folder_layout", by_col)
        self.assertEqual(by_col["folder_layout"][0], "NO")
        self.assertEqual(by_col["folder_layout"][1], "text")
        self.assertIn("'flat'", by_col["folder_layout"][2] or "")
        self.assertIn("audio_file_count", by_col)
        self.assertEqual(by_col["audio_file_count"][0], "NO")
        self.assertEqual(by_col["audio_file_count"][1], "integer")
        self.assertIn("filetype_band", by_col)
        self.assertEqual(by_col["filetype_band"][0], "NO")
        self.assertEqual(by_col["filetype_band"][1], "text")
        # FK pair is nullable (optional reference + paired path)
        self.assertIn("matched_bad_audio_hash_id", by_col)
        self.assertEqual(by_col["matched_bad_audio_hash_id"][0], "YES")
        self.assertEqual(by_col["matched_bad_audio_hash_id"][1], "bigint")
        self.assertIn("matched_bad_audio_hash_path", by_col)
        self.assertEqual(by_col["matched_bad_audio_hash_path"][0], "YES")

    def test_album_quality_evidence_files_has_decode_ok_column(self):
        rows = self._query("""
            SELECT column_name, is_nullable, data_type, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'album_quality_evidence_files'
              AND column_name = 'decode_ok'
        """)
        self.assertEqual(len(rows), 1)
        _, is_nullable, data_type, default = rows[0]
        self.assertEqual(is_nullable, "NO")
        self.assertEqual(data_type, "boolean")
        self.assertIn("true", (default or "").lower())

    def test_folder_layout_check_rejects_unknown_values(self):
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('mig019-folder-mbid', 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """)
        rid = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("mig019-folder-mbid",),
        )[0][0]
        try:
            with self.assertRaises(psycopg2.errors.CheckViolation):
                self._exec("""
                    INSERT INTO album_quality_evidence (
                        mb_release_id, snapshot_fingerprint, source_path,
                        measured_at, format, verified_lossless, folder_layout
                    )
                    VALUES (
                        'mig019-folder-mbid', 'fp-folder', '', NOW(), 'flac',
                        FALSE, 'tree'
                    )
                """)
        finally:
            self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))

    def test_download_log_outcome_check_accepts_measurement_failed(self):
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('mig019-dl-mbid', 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """)
        rid = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("mig019-dl-mbid",),
        )[0][0]
        try:
            self._exec("""
                INSERT INTO download_log (request_id, outcome)
                VALUES (%s, 'measurement_failed')
            """, (rid,))
            rows = self._query(
                "SELECT outcome FROM download_log WHERE request_id = %s",
                (rid,),
            )
            self.assertIn(("measurement_failed",), rows)
            # Unknown outcomes still rejected.
            with self.assertRaises(psycopg2.errors.CheckViolation):
                self._exec("""
                    INSERT INTO download_log (request_id, outcome)
                    VALUES (%s, 'definitely_not_a_real_outcome')
                """, (rid,))
        finally:
            self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))

    def test_import_jobs_preview_status_check_accepts_measurement_failed(self):
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('mig019-job-mbid', 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """)
        rid = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("mig019-job-mbid",),
        )[0][0]
        try:
            self._exec("""
                INSERT INTO import_jobs (
                    job_type, status, request_id, payload, preview_status
                )
                VALUES (
                    'manual_import', 'queued', %s, '{}'::jsonb,
                    'measurement_failed'
                )
            """, (rid,))
            rows = self._query(
                "SELECT preview_status FROM import_jobs WHERE request_id = %s",
                (rid,),
            )
            self.assertEqual(rows, [("measurement_failed",)])
            # Unknown preview_status still rejected.
            with self.assertRaises(psycopg2.errors.CheckViolation):
                self._exec("""
                    INSERT INTO import_jobs (
                        job_type, status, request_id, payload, preview_status
                    )
                    VALUES (
                        'manual_import', 'queued', %s, '{}'::jsonb,
                        'not_a_real_preview_status'
                    )
                """, (rid,))
        finally:
            self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))

    def test_matched_bad_audio_hash_fk_cascade_to_null(self):
        # Insert a parent request + an album_quality_evidence row referencing
        # a bad_audio_hashes row. Delete the hash row; the FK should null out
        # the evidence column without error.
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('mig019-fk-mbid', 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """)
        rid = self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            ("mig019-fk-mbid",),
        )[0][0]
        try:
            self._exec("""
                INSERT INTO bad_audio_hashes (hash_value, audio_format, request_id)
                VALUES (decode('deadbeef', 'hex'), 'flac', %s)
                ON CONFLICT DO NOTHING
            """, (rid,))
            bad_id_rows = self._query("""
                SELECT id FROM bad_audio_hashes
                WHERE hash_value = decode('deadbeef', 'hex')
                  AND audio_format = 'flac'
            """)
            bad_id = bad_id_rows[0][0]
            self._exec("""
                INSERT INTO album_quality_evidence (
                    mb_release_id, snapshot_fingerprint, source_path,
                    measured_at, format,
                    verified_lossless, matched_bad_audio_hash_id,
                    matched_bad_audio_hash_path
                )
                VALUES (
                    'mig019-fk-mbid', 'fp-bad-hash', '', NOW(), 'flac',
                    FALSE, %s,
                    '01 - Track.flac'
                )
            """, (bad_id,))
            self._exec("DELETE FROM bad_audio_hashes WHERE id = %s", (bad_id,))
            rows = self._query("""
                SELECT matched_bad_audio_hash_id, matched_bad_audio_hash_path
                FROM album_quality_evidence
                WHERE mb_release_id = %s AND snapshot_fingerprint = %s
            """, ("mig019-fk-mbid", "fp-bad-hash"))
            self.assertEqual(len(rows), 1)
            self.assertIsNone(rows[0][0])
            # The paired path column is NOT touched by the FK cascade — it
            # remains populated so the audit trail survives the hash delete.
            # The Struct validation enforces "set together or both NULL" only
            # on writes, not on the cascade-NULL aftermath.
            self.assertEqual(rows[0][1], "01 - Track.flac")
        finally:
            self._exec("""
                DELETE FROM album_quality_evidence
                WHERE mb_release_id = %s
            """, ("mig019-fk-mbid",))
            self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))


# ---------------------------------------------------------------------------
# Migration 020 — recovery sweep for preview_status='uncertain'
# ---------------------------------------------------------------------------

@requires_postgres
class TestRecoverStuckPreviewUncertainJobsSchema(unittest.TestCase):
    """Migration 020 sweeps stuck ``preview_status='uncertain'`` rows back
    to ``'waiting'`` so the preview worker re-claims them under the new
    preview-never-decides contract.

    Validates:
    - schema_migrations records 020
    - the canonical UPDATE flips ``status='queued' AND preview_status='uncertain'``
      rows to ``preview_status='waiting'`` and clears the lifecycle columns
    - idempotent: re-running the UPDATE on a clean DB flips zero rows
    - rows in other preview_status values (``evidence_ready``, ``error``,
      ``measurement_failed``) are untouched
    """

    # The exact UPDATE statement shipped in
    # ``migrations/020_recover_stuck_preview_uncertain_jobs.sql``. Tests run
    # this statement directly against test-seeded rows because conftest.py
    # already applied migration 020 at session start (so there's no stuck
    # 'uncertain' row in the live test DB to observe).
    _RECOVERY_SWEEP_SQL = """
        UPDATE import_jobs
        SET preview_status = 'waiting',
            preview_result = NULL,
            preview_message = 'Recovered by preview-never-decides refactor (020)',
            preview_error = NULL,
            preview_worker_id = NULL,
            preview_started_at = NULL,
            preview_heartbeat_at = NULL,
            preview_completed_at = NULL,
            importable_at = NULL,
            updated_at = NOW()
        WHERE status = 'queued'
          AND preview_status = 'uncertain'
    """

    def _query(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            conn.close()

    def _exec(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
        finally:
            conn.close()

    def _exec_with_rowcount(self, sql: str, params: tuple = ()) -> int:
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.rowcount
        finally:
            conn.close()

    def _make_request(self, mbid: str) -> int:
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES (%s, 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """, (mbid,))
        return self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            (mbid,),
        )[0][0]

    def _cleanup_request(self, rid: int) -> None:
        # CASCADE on album_requests.id → import_jobs cleans up associated jobs.
        self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))

    def test_schema_migrations_records_020(self):
        rows = self._query(
            "SELECT version FROM schema_migrations WHERE version = 20"
        )
        self.assertEqual(rows, [(20,)])

    def test_recovery_sweep_flips_uncertain_to_waiting_and_clears_lifecycle(self):
        """A stuck uncertain row flips to waiting with the lifecycle cleared."""
        rid = self._make_request("mig020-flip-mbid")
        try:
            self._exec("""
                INSERT INTO import_jobs (
                    job_type, status, request_id, payload,
                    preview_status, preview_result, preview_message,
                    preview_error, preview_worker_id,
                    preview_started_at, preview_heartbeat_at,
                    preview_completed_at, importable_at
                )
                VALUES (
                    'automation_import', 'queued', %s, '{}'::jsonb,
                    'uncertain', '{"verdict": "uncertain"}'::jsonb,
                    'pre-U7 stuck reason',
                    'pre-U7 stuck error', 'pre-U7-worker',
                    NOW(), NOW(),
                    NOW(), NOW()
                )
            """, (rid,))
            affected = self._exec_with_rowcount(self._RECOVERY_SWEEP_SQL)
            self.assertEqual(affected, 1)
            rows = self._query("""
                SELECT preview_status, preview_result, preview_message,
                       preview_error, preview_worker_id,
                       preview_started_at, preview_heartbeat_at,
                       preview_completed_at, importable_at
                FROM import_jobs WHERE request_id = %s
            """, (rid,))
            self.assertEqual(len(rows), 1)
            (preview_status, preview_result, preview_message,
             preview_error, preview_worker_id, preview_started_at,
             preview_heartbeat_at, preview_completed_at,
             importable_at) = rows[0]
            self.assertEqual(preview_status, "waiting")
            self.assertIsNone(preview_result)
            self.assertEqual(
                preview_message,
                "Recovered by preview-never-decides refactor (020)",
            )
            self.assertIsNone(preview_error)
            self.assertIsNone(preview_worker_id)
            self.assertIsNone(preview_started_at)
            self.assertIsNone(preview_heartbeat_at)
            self.assertIsNone(preview_completed_at)
            self.assertIsNone(importable_at)
        finally:
            self._cleanup_request(rid)

    def test_recovery_sweep_is_idempotent(self):
        """Re-running the sweep on a row already flipped touches nothing."""
        rid = self._make_request("mig020-idem-mbid")
        try:
            self._exec("""
                INSERT INTO import_jobs (
                    job_type, status, request_id, payload, preview_status
                )
                VALUES (
                    'automation_import', 'queued', %s, '{}'::jsonb, 'uncertain'
                )
            """, (rid,))
            first = self._exec_with_rowcount(self._RECOVERY_SWEEP_SQL)
            self.assertEqual(first, 1)
            second = self._exec_with_rowcount(self._RECOVERY_SWEEP_SQL)
            self.assertEqual(
                second, 0,
                "Second run of the recovery sweep must be a no-op",
            )
        finally:
            self._cleanup_request(rid)

    def test_recovery_sweep_leaves_other_preview_statuses_untouched(self):
        """Rows in other preview_status values are untouched."""
        rid = self._make_request("mig020-untouched-mbid")
        try:
            # Seed three jobs in distinct non-uncertain preview_status values.
            # Use a heartbeat timestamp the sweep would have nulled, to prove
            # those fields are untouched.
            for preview_status in ("evidence_ready", "measurement_failed",
                                   "error"):
                self._exec("""
                    INSERT INTO import_jobs (
                        job_type, status, request_id, payload,
                        preview_status, preview_message,
                        preview_heartbeat_at
                    )
                    VALUES (
                        'manual_import', 'queued', %s, '{}'::jsonb,
                        %s, 'pre-existing message',
                        '2026-05-15 00:00:00+00'
                    )
                """, (rid, preview_status))

            affected = self._exec_with_rowcount(self._RECOVERY_SWEEP_SQL)
            self.assertEqual(affected, 0)

            rows = self._query("""
                SELECT preview_status, preview_message, preview_heartbeat_at
                FROM import_jobs WHERE request_id = %s
                ORDER BY id ASC
            """, (rid,))
            self.assertEqual(len(rows), 3)
            statuses = {r[0] for r in rows}
            self.assertEqual(
                statuses,
                {"evidence_ready", "measurement_failed", "error"},
            )
            for _, message, heartbeat in rows:
                self.assertEqual(message, "pre-existing message")
                self.assertIsNotNone(heartbeat)
        finally:
            self._cleanup_request(rid)

    def test_recovery_sweep_only_targets_queued_jobs(self):
        """A non-queued ``uncertain`` row is left alone — the WHERE guard
        gates on ``status='queued'`` so completed/failed legacy rows are
        not retro-resurrected."""
        rid = self._make_request("mig020-failed-mbid")
        try:
            self._exec("""
                INSERT INTO import_jobs (
                    job_type, status, request_id, payload,
                    preview_status, preview_message
                )
                VALUES (
                    'manual_import', 'failed', %s, '{}'::jsonb,
                    'uncertain', 'historical failed job'
                )
            """, (rid,))
            affected = self._exec_with_rowcount(self._RECOVERY_SWEEP_SQL)
            self.assertEqual(affected, 0)
            rows = self._query("""
                SELECT status, preview_status, preview_message
                FROM import_jobs WHERE request_id = %s
            """, (rid,))
            self.assertEqual(rows, [("failed", "uncertain",
                                     "historical failed job")])
        finally:
            self._cleanup_request(rid)


class TestDropLidarrColumnsSchema(unittest.TestCase):
    """Migration 022 drops vestigial lidarr_album_id / lidarr_artist_id
    columns from album_requests. They had no readers or writers in the
    codebase since the soularr-fork era.
    """

    def _query(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            conn.close()

    def test_schema_migrations_records_022(self):
        rows = self._query(
            "SELECT version FROM schema_migrations WHERE version = 22"
        )
        self.assertEqual(rows, [(22,)])

    def test_lidarr_columns_dropped_from_album_requests(self):
        rows = self._query("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'album_requests'
              AND column_name IN ('lidarr_album_id', 'lidarr_artist_id')
        """)
        self.assertEqual(rows, [])


class TestReplaceSupersedeSchema(unittest.TestCase):
    """Migration 023 adds:
    - ``album_requests.replaces_request_id`` (nullable self-FK, ON DELETE RESTRICT)
    - partial index on the lineage FK
    - ``'replaced'`` value in the ``album_requests_status_check`` CHECK constraint
    """

    def _query(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            conn.close()

    def _exec(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
        finally:
            conn.close()

    def _make_request(self, mbid: str, **extra) -> int:
        cols = ["mb_release_id", "artist_name", "album_title", "source"]
        vals = [mbid, "A", "B", "request"]
        for k, v in extra.items():
            cols.append(k)
            vals.append(v)
        placeholders = ",".join(["%s"] * len(vals))
        sql = (
            f"INSERT INTO album_requests ({','.join(cols)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT (mb_release_id) DO NOTHING"
        )
        self._exec(sql, tuple(vals))
        return self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            (mbid,),
        )[0][0]

    def _cleanup_request(self, rid: int) -> None:
        self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))

    def test_schema_migrations_records_023(self):
        rows = self._query(
            "SELECT version FROM schema_migrations WHERE version = 23"
        )
        self.assertEqual(rows, [(23,)])

    def test_replaces_request_id_column_present(self):
        rows = self._query("""
            SELECT column_name, is_nullable, data_type
            FROM information_schema.columns
            WHERE table_name = 'album_requests'
              AND column_name = 'replaces_request_id'
        """)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "replaces_request_id")
        self.assertEqual(rows[0][1], "YES")  # nullable
        self.assertEqual(rows[0][2], "integer")

    def test_partial_index_on_replaces_request_id(self):
        rows = self._query("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'album_requests'
              AND indexname = 'idx_album_requests_replaces_request_id'
        """)
        self.assertEqual(len(rows), 1)

    def test_status_check_includes_replaced(self):
        # Insert succeeds with the new status.
        rid = self._make_request("mig023-replaced-mbid", status="replaced")
        try:
            rows = self._query(
                "SELECT status FROM album_requests WHERE id = %s", (rid,)
            )
            self.assertEqual(rows, [("replaced",)])
        finally:
            self._cleanup_request(rid)

    def test_status_check_rejects_unknown(self):
        with self.assertRaises(psycopg2.errors.CheckViolation):
            self._exec("""
                INSERT INTO album_requests (mb_release_id, artist_name, album_title, source, status)
                VALUES ('mig023-bogus', 'A', 'B', 'request', 'bogus')
            """)

    def test_replaces_request_id_fk_violation(self):
        with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
            self._exec("""
                INSERT INTO album_requests (
                    mb_release_id, artist_name, album_title, source, replaces_request_id
                ) VALUES ('mig023-fk-violation', 'A', 'B', 'request', 99999999)
            """)

    def test_on_delete_restrict_prevents_parent_deletion(self):
        parent = self._make_request("mig023-fk-parent")
        child = self._make_request(
            "mig023-fk-child", replaces_request_id=parent
        )
        try:
            with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
                self._exec(
                    "DELETE FROM album_requests WHERE id = %s", (parent,)
                )
        finally:
            self._cleanup_request(child)
            self._cleanup_request(parent)

    def test_chain_delete_descendants_first_succeeds(self):
        # r1 ← r2 ← r3 lineage; deletes go r3, r2, r1.
        r1 = self._make_request("mig023-chain-r1")
        r2 = self._make_request("mig023-chain-r2", replaces_request_id=r1)
        r3 = self._make_request("mig023-chain-r3", replaces_request_id=r2)
        self._cleanup_request(r3)
        self._cleanup_request(r2)
        self._cleanup_request(r1)
        rows = self._query(
            "SELECT id FROM album_requests WHERE id IN (%s, %s, %s)",
            (r1, r2, r3),
        )
        self.assertEqual(rows, [])


class TestBackfillV0MetricFromMeasurementSchema(unittest.TestCase):
    """Migration 024 backfills v0_metric on album_quality_evidence and
    v0_probe_* columns on download_log from the row's own
    ``import_result.new_measurement`` JSONB. Without it, every non-lossless
    candidate (MP3 V0, Opus, CBR 320) had NULL V0 probe fields and the
    audit/UI surface showed a blank "V0 probe" row.

    Validates:
    - schema_migrations records 024
    - the backfill UPDATE fills NULL v0_* fields on evidence rows that have
      a linked download_log with ``import_result.new_measurement``
    - the backfill UPDATE fills NULL v0_probe_* columns on download_log
      rows whose own JSONB carries ``new_measurement``
    - rows that already have v0_metric / v0_probe_* set are left untouched
    """

    _BACKFILL_EVIDENCE_SQL = """
        WITH evidence_measurements AS (
            SELECT
                dl.candidate_evidence_id                                                AS evidence_id,
                ((dl.import_result -> 'new_measurement') ->> 'min_bitrate_kbps')::int   AS min_kbps,
                ((dl.import_result -> 'new_measurement') ->> 'avg_bitrate_kbps')::int   AS avg_kbps,
                ((dl.import_result -> 'new_measurement') ->> 'median_bitrate_kbps')::int AS median_kbps,
                ROW_NUMBER() OVER (
                    PARTITION BY dl.candidate_evidence_id
                    ORDER BY dl.id DESC
                ) AS rn
            FROM download_log dl
            WHERE dl.candidate_evidence_id IS NOT NULL
              AND dl.import_result IS NOT NULL
              AND dl.import_result -> 'new_measurement' IS NOT NULL
        )
        UPDATE album_quality_evidence e
        SET v0_min_bitrate_kbps    = m.min_kbps,
            v0_avg_bitrate_kbps    = m.avg_kbps,
            v0_median_bitrate_kbps = m.median_kbps,
            v0_source_lineage      = 'native_lossy_research',
            v0_source_provenance   = 'new_measurement_fallback',
            updated_at             = NOW()
        FROM evidence_measurements m
        WHERE e.id = m.evidence_id
          AND m.rn = 1
          AND e.v0_min_bitrate_kbps    IS NULL
          AND e.v0_avg_bitrate_kbps    IS NULL
          AND e.v0_median_bitrate_kbps IS NULL
          AND e.v0_source_lineage      IS NULL
          AND e.v0_source_provenance   IS NULL
          AND e.v0_proof_provenance    IS NULL
          AND (m.min_kbps IS NOT NULL OR m.avg_kbps IS NOT NULL OR m.median_kbps IS NOT NULL)
    """

    _BACKFILL_DOWNLOAD_LOG_SQL = """
        UPDATE download_log dl
        SET v0_probe_kind            = 'native_lossy_research_v0',
            v0_probe_min_bitrate     = ((dl.import_result -> 'new_measurement') ->> 'min_bitrate_kbps')::int,
            v0_probe_avg_bitrate     = ((dl.import_result -> 'new_measurement') ->> 'avg_bitrate_kbps')::int,
            v0_probe_median_bitrate  = ((dl.import_result -> 'new_measurement') ->> 'median_bitrate_kbps')::int
        WHERE dl.v0_probe_kind IS NULL
          AND dl.import_result IS NOT NULL
          AND dl.import_result -> 'new_measurement' IS NOT NULL
          AND (
                ((dl.import_result -> 'new_measurement') ->> 'min_bitrate_kbps')    IS NOT NULL
             OR ((dl.import_result -> 'new_measurement') ->> 'avg_bitrate_kbps')    IS NOT NULL
             OR ((dl.import_result -> 'new_measurement') ->> 'median_bitrate_kbps') IS NOT NULL
          )
    """

    def _query(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            conn.close()

    def _exec(self, sql: str, params: tuple = ()):
        conn = psycopg2.connect(TEST_DSN)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
        finally:
            conn.close()

    def _make_request(self, mbid: str) -> int:
        self._exec("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES (%s, 'A', 'B', 'request')
            ON CONFLICT (mb_release_id) DO NOTHING
        """, (mbid,))
        return self._query(
            "SELECT id FROM album_requests WHERE mb_release_id = %s",
            (mbid,),
        )[0][0]

    def _make_evidence(self, mbid: str, fingerprint: str) -> int:
        self._exec("""
            INSERT INTO album_quality_evidence (
                mb_release_id, snapshot_fingerprint, source_path,
                measured_at, format, verified_lossless
            ) VALUES (
                %s, %s, '/tmp/test', NOW(), 'mp3 v0', FALSE
            )
            ON CONFLICT (mb_release_id, snapshot_fingerprint) DO NOTHING
        """, (mbid, fingerprint))
        return self._query(
            """
            SELECT id FROM album_quality_evidence
            WHERE mb_release_id = %s AND snapshot_fingerprint = %s
            """,
            (mbid, fingerprint),
        )[0][0]

    def _make_download_log(
        self,
        request_id: int,
        evidence_id: int | None,
        import_result_json: str | None,
    ) -> int:
        self._exec(
            """
            INSERT INTO download_log (
                request_id, candidate_evidence_id, outcome,
                soulseek_username, import_result
            ) VALUES (%s, %s, 'rejected', 'tester', %s::jsonb)
            """,
            (request_id, evidence_id, import_result_json),
        )
        return self._query(
            "SELECT MAX(id) FROM download_log WHERE request_id = %s",
            (request_id,),
        )[0][0]

    def _cleanup_request(self, rid: int) -> None:
        self._exec(
            "DELETE FROM download_log WHERE request_id = %s",
            (rid,),
        )
        self._exec("DELETE FROM album_requests WHERE id = %s", (rid,))

    def _cleanup_evidence(self, eid: int) -> None:
        self._exec(
            "DELETE FROM album_quality_evidence WHERE id = %s", (eid,)
        )

    def test_schema_migrations_records_024(self):
        rows = self._query(
            "SELECT version FROM schema_migrations WHERE version = 24"
        )
        self.assertEqual(rows, [(24,)])

    def test_backfill_fills_null_evidence_v0_metric_from_jsonb_measurement(self):
        rid = self._make_request("mig024-evidence-mbid")
        eid = self._make_evidence("mig024-evidence-mbid", "fp-evidence")
        ir = (
            '{"decision":"transcode_downgrade",'
            '"new_measurement":{"min_bitrate_kbps":237,'
            '"avg_bitrate_kbps":247,"median_bitrate_kbps":246,"format":"mp3 v0"}}'
        )
        dlid = self._make_download_log(rid, eid, ir)
        try:
            self._exec(self._BACKFILL_EVIDENCE_SQL)
            rows = self._query(
                """
                SELECT v0_min_bitrate_kbps, v0_avg_bitrate_kbps,
                       v0_median_bitrate_kbps, v0_source_lineage,
                       v0_source_provenance
                FROM album_quality_evidence WHERE id = %s
                """,
                (eid,),
            )
            self.assertEqual(
                rows[0],
                (237, 247, 246, "native_lossy_research", "new_measurement_fallback"),
            )
        finally:
            self._exec("DELETE FROM download_log WHERE id = %s", (dlid,))
            self._cleanup_evidence(eid)
            self._cleanup_request(rid)

    def test_backfill_leaves_already_populated_evidence_untouched(self):
        rid = self._make_request("mig024-already-mbid")
        eid = self._make_evidence("mig024-already-mbid", "fp-already")
        # Seed an existing v0_metric on the evidence row (lossless source).
        self._exec(
            """
            UPDATE album_quality_evidence
            SET v0_min_bitrate_kbps = 250,
                v0_avg_bitrate_kbps = 260,
                v0_median_bitrate_kbps = 258,
                v0_source_lineage = 'lossless_source',
                v0_source_provenance = 'lossless_source_v0'
            WHERE id = %s
            """,
            (eid,),
        )
        ir = (
            '{"decision":"import",'
            '"new_measurement":{"min_bitrate_kbps":900,'
            '"avg_bitrate_kbps":900,"median_bitrate_kbps":900,"format":"mp3 v0"}}'
        )
        dlid = self._make_download_log(rid, eid, ir)
        try:
            self._exec(self._BACKFILL_EVIDENCE_SQL)
            rows = self._query(
                """
                SELECT v0_avg_bitrate_kbps, v0_source_lineage
                FROM album_quality_evidence WHERE id = %s
                """,
                (eid,),
            )
            # Backfill must NOT overwrite the lossless probe with the
            # synthetic 900-kbps measurement value.
            self.assertEqual(rows[0], (260, "lossless_source"))
        finally:
            self._exec("DELETE FROM download_log WHERE id = %s", (dlid,))
            self._cleanup_evidence(eid)
            self._cleanup_request(rid)

    def test_backfill_fills_null_download_log_v0_probe_from_own_jsonb(self):
        rid = self._make_request("mig024-dl-mbid")
        ir = (
            '{"decision":"transcode_downgrade",'
            '"new_measurement":{"min_bitrate_kbps":192,'
            '"avg_bitrate_kbps":215,"median_bitrate_kbps":213,"format":"mp3 v0"}}'
        )
        # No candidate_evidence_id — covers the pre-rekey audit-history case.
        dlid = self._make_download_log(rid, None, ir)
        try:
            self._exec(self._BACKFILL_DOWNLOAD_LOG_SQL)
            rows = self._query(
                """
                SELECT v0_probe_kind, v0_probe_min_bitrate,
                       v0_probe_avg_bitrate, v0_probe_median_bitrate
                FROM download_log WHERE id = %s
                """,
                (dlid,),
            )
            self.assertEqual(
                rows[0],
                ("native_lossy_research_v0", 192, 215, 213),
            )
        finally:
            self._exec("DELETE FROM download_log WHERE id = %s", (dlid,))
            self._cleanup_request(rid)

    def test_backfill_leaves_already_populated_download_log_untouched(self):
        rid = self._make_request("mig024-dl-already-mbid")
        ir = (
            '{"decision":"import",'
            '"new_measurement":{"min_bitrate_kbps":900,'
            '"avg_bitrate_kbps":900,"median_bitrate_kbps":900,"format":"mp3 v0"}}'
        )
        dlid = self._make_download_log(rid, None, ir)
        # Seed a real lossless probe on the row.
        self._exec(
            """
            UPDATE download_log
            SET v0_probe_kind = 'lossless_source_v0',
                v0_probe_min_bitrate = 250,
                v0_probe_avg_bitrate = 260,
                v0_probe_median_bitrate = 258
            WHERE id = %s
            """,
            (dlid,),
        )
        try:
            self._exec(self._BACKFILL_DOWNLOAD_LOG_SQL)
            rows = self._query(
                """
                SELECT v0_probe_kind, v0_probe_avg_bitrate
                FROM download_log WHERE id = %s
                """,
                (dlid,),
            )
            self.assertEqual(rows[0], ("lossless_source_v0", 260))
        finally:
            self._exec("DELETE FROM download_log WHERE id = %s", (dlid,))
            self._cleanup_request(rid)


class TestSearchLogPreFilterSkipCountSchema(unittest.TestCase):
    """Migration 025 adds ``search_log.pre_filter_skip_count`` (NOT NULL,
    default 0). U2 of search-plan-entropy. Pre-existing rows must default
    to 0 because the data was never captured at the time, and we
    intentionally do not backfill — forward-only by design.
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

    def test_column_exists_with_default_zero(self):
        rows = self._query("""
            SELECT column_name, is_nullable, column_default, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'search_log'
              AND column_name = 'pre_filter_skip_count'
        """)
        self.assertEqual(len(rows), 1)
        col, is_nullable, default, dtype = rows[0]
        self.assertEqual(col, "pre_filter_skip_count")
        self.assertEqual(is_nullable, "NO")
        # Default is the SQL literal ``0``.
        assert default is not None
        self.assertIn("0", str(default))
        self.assertEqual(dtype, "integer")

    def test_existing_row_defaults_to_zero_on_insert_without_value(self):
        rid = self._query("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('pre-filter-skip-test', 'A', 'B', 'request')
            RETURNING id
        """)[0][0]
        try:
            self._exec("""
                INSERT INTO search_log (request_id, query, outcome)
                VALUES (%s, 'q', 'no_match')
            """, (rid,))
            rows = self._query(
                "SELECT pre_filter_skip_count FROM search_log "
                "WHERE request_id = %s",
                (rid,),
            )
            self.assertEqual(rows[0][0], 0)
        finally:
            self._exec(
                "DELETE FROM album_requests WHERE id = %s", (rid,),
            )

    def test_records_applied_version_025(self):
        rows = self._query("""
            SELECT version FROM schema_migrations
            WHERE version = 25
        """)
        self.assertEqual(len(rows), 1)


@requires_postgres
class TestAlbumRequestsReleaseGroupYearSchema(unittest.TestCase):
    """Migration 026 adds ``album_requests.release_group_year``
    (INTEGER, NULL). U3 of search-plan-entropy / R9 (data layer). The
    column is nullable because many requests have no
    ``mb_release_group_id`` (legacy / Discogs / manual rows) — the
    backfill skips those, the generator handles NULL gracefully.
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

    def test_column_exists_nullable_with_no_default(self):
        rows = self._query("""
            SELECT column_name, is_nullable, column_default, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'album_requests'
              AND column_name = 'release_group_year'
        """)
        self.assertEqual(len(rows), 1)
        col, is_nullable, default, dtype = rows[0]
        self.assertEqual(col, "release_group_year")
        self.assertEqual(is_nullable, "YES")
        self.assertIsNone(default)
        self.assertEqual(dtype, "integer")

    def test_existing_row_defaults_to_null_on_insert_without_value(self):
        rid = self._query("""
            INSERT INTO album_requests (mb_release_id, artist_name, album_title, source)
            VALUES ('rg-year-default-test', 'A', 'B', 'request')
            RETURNING id
        """)[0][0]
        try:
            rows = self._query(
                "SELECT release_group_year FROM album_requests "
                "WHERE id = %s",
                (rid,),
            )
            self.assertIsNone(rows[0][0])
        finally:
            self._exec(
                "DELETE FROM album_requests WHERE id = %s", (rid,),
            )

    def test_writable_when_explicitly_set(self):
        rid = self._query("""
            INSERT INTO album_requests (
                mb_release_id, mb_release_group_id, artist_name,
                album_title, source, release_group_year
            )
            VALUES ('rg-year-set-test', 'rg-test-uuid', 'A', 'B', 'request', 2000)
            RETURNING id
        """)[0][0]
        try:
            rows = self._query(
                "SELECT release_group_year FROM album_requests "
                "WHERE id = %s",
                (rid,),
            )
            self.assertEqual(rows[0][0], 2000)
        finally:
            self._exec(
                "DELETE FROM album_requests WHERE id = %s", (rid,),
            )

    def test_records_applied_version_026(self):
        rows = self._query("""
            SELECT version FROM schema_migrations
            WHERE version = 26
        """)
        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
