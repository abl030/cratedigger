"""Tests for migration 021 — evidence canonical rekey.

These tests seed an old-shape DB (owner_type / owner_id keying), run
migration 021 against it, and assert the post-state matches the canonical
new shape: ``(mb_release_id, snapshot_fingerprint)`` key + addressing FK
columns on ``import_jobs`` / ``download_log`` / ``album_requests``.

The test creates an isolated database inside the already-running ephemeral
cluster (or whatever TEST_DB_DSN points at). Migrations 001..020 are
applied to that DB, then synthetic owner-keyed evidence is seeded directly
with raw SQL, then migration 021 is applied. Assertions read the post-
migration shape.

The fingerprint cross-check tests confirm that the in-SQL hash matches the
Python helper ``snapshot_fingerprint`` for at least three file-list shapes
(single file, multiple files, file with NULL codec) — guarding against
silent drift between the SQL backfill and the Python helper that all
future writers will use.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
import unittest
import uuid
from typing import Sequence

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401 — sets TEST_DB_DSN env var

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "lib"))

import psycopg2  # noqa: E402

from lib.migrator import (  # noqa: E402
    DEFAULT_MIGRATIONS_DIR,
    apply_migrations,
    discover_migrations,
)
from lib.quality import AlbumQualityEvidenceFile  # noqa: E402
from lib.quality_evidence import snapshot_fingerprint  # noqa: E402

TEST_DSN: str = os.environ.get("TEST_DB_DSN") or ""

EMPTY_FILESET_FINGERPRINT = hashlib.sha256("[]".encode("utf-8")).hexdigest()


def requires_postgres(cls):
    if not TEST_DSN:
        return unittest.skip(
            "TEST_DB_DSN not set — skipping PostgreSQL migration tests"
        )(cls)
    return cls


def _parse_dsn(dsn: str) -> tuple[str, str, int, str]:
    """Return (user, host, port, dbname) from a postgres DSN."""
    # postgresql://user@host:port/dbname
    body = dsn.split("://", 1)[1]
    userhost, dbname = body.rsplit("/", 1)
    user, hostport = userhost.split("@", 1)
    if ":" in hostport:
        host, port_str = hostport.split(":", 1)
        port = int(port_str)
    else:
        host = hostport
        port = 5432
    return user, host, port, dbname


def _build_isolated_dsn(template_dsn: str, db_name: str) -> str:
    user, host, port, _ = _parse_dsn(template_dsn)
    return f"postgresql://{user}@{host}:{port}/{db_name}"


def _maintenance_conn(template_dsn: str):
    user, host, port, _ = _parse_dsn(template_dsn)
    conn = psycopg2.connect(
        host=host, port=port, dbname="postgres", user=user
    )
    conn.autocommit = True
    return conn


def _make_isolated_db(template_dsn: str) -> tuple[str, str]:
    """Create a fresh isolated database; return (db_name, dsn)."""
    db_name = "cratedigger_mig021_" + uuid.uuid4().hex[:12]
    conn = _maintenance_conn(template_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {db_name}")
    finally:
        conn.close()
    return db_name, _build_isolated_dsn(template_dsn, db_name)


def _drop_isolated_db(template_dsn: str, db_name: str) -> None:
    conn = _maintenance_conn(template_dsn)
    try:
        with conn.cursor() as cur:
            # Force-disconnect any leftover sessions before drop.
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db_name,),
            )
            cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
    finally:
        conn.close()


def _stage_pre_021_migrations(target_dir: str) -> None:
    """Copy migrations 001..020 to ``target_dir`` (skipping 021+)."""
    for mig in discover_migrations(DEFAULT_MIGRATIONS_DIR):
        if mig.version >= 21:
            continue
        with open(mig.path) as src:
            body = src.read()
        with open(
            os.path.join(target_dir, os.path.basename(mig.path)), "w"
        ) as dst:
            dst.write(body)


@requires_postgres
class _Mig021CaseBase(unittest.TestCase):
    """Shared fixture: a fresh DB migrated up to 020, ready for seeding."""

    def setUp(self) -> None:
        self.db_name, self.dsn = _make_isolated_db(TEST_DSN)
        self.addCleanup(_drop_isolated_db, TEST_DSN, self.db_name)
        self._pre_dir = tempfile.mkdtemp(prefix="cratedigger_mig021_pre_")
        self.addCleanup(shutil.rmtree, self._pre_dir, ignore_errors=True)
        _stage_pre_021_migrations(self._pre_dir)
        apply_migrations(self.dsn, self._pre_dir)

    def _conn(self):
        conn = psycopg2.connect(self.dsn)
        conn.autocommit = True
        return conn

    def _exec(self, sql: str, params: tuple = ()) -> None:
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
        finally:
            conn.close()

    def _query(self, sql: str, params: tuple = ()):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            conn.close()

    def _query_one(self, sql: str, params: tuple = ()):
        rows = self._query(sql, params)
        return rows[0] if rows else None

    def _apply_021(self) -> None:
        """Apply only migration 021 by running its SQL directly.

        Uses ``apply_migrations`` against a temp dir that holds only the
        021 file, so ``schema_migrations`` records 021 just like prod.
        """
        with tempfile.TemporaryDirectory(prefix="mig021_only_") as tmp:
            src = os.path.join(DEFAULT_MIGRATIONS_DIR, "021_evidence_canonical_rekey.sql")
            with open(src) as fh:
                body = fh.read()
            with open(os.path.join(tmp, "021_evidence_canonical_rekey.sql"), "w") as fh:
                fh.write(body)
            apply_migrations(self.dsn, tmp)

    # ---- Seeding helpers (pre-021 shape) ----

    def _seed_request(self, mbid: str, imported_path: str = "") -> int:
        rid = self._query_one(
            """
            INSERT INTO album_requests
                (mb_release_id, artist_name, album_title, source, imported_path)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (mbid, "Artist", "Album", "request", imported_path or None),
        )
        assert rid is not None
        return int(rid[0])

    def _seed_import_job(
        self, request_id: int, failed_path: str = "/tmp/staged"
    ) -> int:
        rid = self._query_one(
            """
            INSERT INTO import_jobs (
                job_type, status, request_id, payload, preview_status
            )
            VALUES ('manual_import', 'queued', %s, %s::jsonb, 'waiting')
            RETURNING id
            """,
            (request_id, json.dumps({"failed_path": failed_path})),
        )
        assert rid is not None
        return int(rid[0])

    def _seed_download_log(
        self, request_id: int, staged_path: str = "/tmp/dl"
    ) -> int:
        rid = self._query_one(
            """
            INSERT INTO download_log (request_id, outcome, staged_path)
            VALUES (%s, 'rejected', %s)
            RETURNING id
            """,
            (request_id, staged_path),
        )
        assert rid is not None
        return int(rid[0])

    def _seed_evidence(
        self,
        *,
        owner_type: str,
        owner_id: int,
        files: Sequence[tuple[str, int, str, str, str | None]],
        measured_at: str = "2026-05-16 12:00:00+00",
    ) -> int:
        eid = self._query_one(
            """
            INSERT INTO album_quality_evidence (
                owner_type, owner_id, measured_at, format,
                min_bitrate_kbps, avg_bitrate_kbps, median_bitrate_kbps,
                verified_lossless
            )
            VALUES (%s, %s, %s::timestamptz, %s, %s, %s, %s, FALSE)
            RETURNING id
            """,
            (owner_type, owner_id, measured_at, "flac", 1000, 1000, 1000),
        )
        assert eid is not None
        evidence_id = int(eid[0])
        for ordinal, (rel, size, ext, container, codec) in enumerate(files):
            self._exec(
                """
                INSERT INTO album_quality_evidence_files (
                    evidence_id, ordinal, relative_path, size_bytes,
                    mtime_ns, extension, container, codec
                )
                VALUES (%s, %s, %s, %s, 0, %s, %s, %s)
                """,
                (evidence_id, ordinal, rel, size, ext, container, codec),
            )
        return evidence_id


@requires_postgres
class TestMigration021BasicShape(_Mig021CaseBase):
    """After migration, owner_type / owner_id are gone and the new key + FK
    columns exist."""

    def test_post_migration_columns_present_and_old_columns_gone(self):
        rid = self._seed_request("mb-shape-1")
        jid = self._seed_import_job(rid)
        self._seed_evidence(
            owner_type="import_job_candidate",
            owner_id=jid,
            files=[("01.flac", 12345, "flac", "flac", "flac")],
        )
        self._apply_021()

        cols = self._query(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'album_quality_evidence'
            """
        )
        col_names = {row[0] for row in cols}
        self.assertIn("mb_release_id", col_names)
        self.assertIn("snapshot_fingerprint", col_names)
        self.assertIn("source_path", col_names)
        self.assertNotIn("owner_type", col_names)
        self.assertNotIn("owner_id", col_names)

        for table, col in (
            ("import_jobs", "candidate_evidence_id"),
            ("download_log", "candidate_evidence_id"),
            ("album_requests", "current_evidence_id"),
        ):
            present = self._query(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s AND column_name=%s
                """,
                (table, col),
            )
            self.assertTrue(
                present,
                f"missing addressing FK column {table}.{col}",
            )


@requires_postgres
class TestMigration021Backfill(_Mig021CaseBase):
    """Backfill populates mb_release_id, snapshot_fingerprint, source_path
    and the addressing FKs."""

    def test_import_job_candidate_row_backfilled_and_fk_set(self):
        mbid = "mb-import-candidate-1"
        rid = self._seed_request(mbid)
        jid = self._seed_import_job(rid, failed_path="/staged/import/path")
        self._seed_evidence(
            owner_type="import_job_candidate",
            owner_id=jid,
            files=[("01.flac", 12345, "flac", "flac", "flac")],
        )

        self._apply_021()

        row = self._query_one(
            """
            SELECT mb_release_id, snapshot_fingerprint, source_path
            FROM album_quality_evidence
            """
        )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row[0], mbid)
        self.assertEqual(row[2], "/staged/import/path")

        py_fp = snapshot_fingerprint([
            AlbumQualityEvidenceFile(
                relative_path="01.flac",
                size_bytes=12345,
                mtime_ns=0,
                extension="flac",
                container="flac",
                codec="flac",
            )
        ])
        self.assertEqual(row[1], py_fp)

        fk = self._query_one(
            "SELECT candidate_evidence_id FROM import_jobs WHERE id = %s",
            (jid,),
        )
        self.assertIsNotNone(fk)
        assert fk is not None
        self.assertIsNotNone(fk[0])

    def test_request_current_row_backfilled(self):
        mbid = "mb-request-current-1"
        rid = self._seed_request(mbid, imported_path="/beets/Artist/Album")
        self._seed_evidence(
            owner_type="request_current",
            owner_id=rid,
            files=[("01.flac", 12345, "flac", "flac", "flac")],
        )

        self._apply_021()

        ev = self._query_one(
            """
            SELECT mb_release_id, source_path
            FROM album_quality_evidence
            """
        )
        assert ev is not None
        self.assertEqual(ev[0], mbid)
        self.assertEqual(ev[1], "/beets/Artist/Album")

        fk = self._query_one(
            "SELECT current_evidence_id FROM album_requests WHERE id = %s",
            (rid,),
        )
        assert fk is not None
        self.assertIsNotNone(fk[0])

    def test_download_log_candidate_row_backfilled(self):
        mbid = "mb-dl-cand-1"
        rid = self._seed_request(mbid)
        dlid = self._seed_download_log(rid, staged_path="/incoming/audit-stage")
        self._seed_evidence(
            owner_type="download_log_candidate",
            owner_id=dlid,
            files=[("01.flac", 12345, "flac", "flac", "flac")],
        )

        self._apply_021()

        ev = self._query_one(
            """
            SELECT mb_release_id, source_path
            FROM album_quality_evidence
            """
        )
        assert ev is not None
        self.assertEqual(ev[0], mbid)
        self.assertEqual(ev[1], "/incoming/audit-stage")

        fk = self._query_one(
            "SELECT candidate_evidence_id FROM download_log WHERE id = %s",
            (dlid,),
        )
        assert fk is not None
        self.assertIsNotNone(fk[0])


@requires_postgres
class TestMigration021Dedupe(_Mig021CaseBase):
    """PR #256 duplicate scenario: an import_job_candidate row AND a
    download_log_candidate row for the same audio collapse into one
    surviving row; both FKs point at the survivor."""

    def test_duplicate_owner_rows_collapse_to_one_row(self):
        mbid = "mb-dedupe-1"
        rid = self._seed_request(mbid)
        jid = self._seed_import_job(rid, failed_path="/staged/x")
        dlid = self._seed_download_log(rid, staged_path="/staged/x")
        files = [("01.flac", 12345, "flac", "flac", "flac")]
        # Older row owned by import_job_candidate
        e1 = self._seed_evidence(
            owner_type="import_job_candidate",
            owner_id=jid,
            files=files,
            measured_at="2026-05-15 10:00:00+00",
        )
        # Newer row owned by download_log_candidate — same audio, identical
        # fingerprint
        e2 = self._seed_evidence(
            owner_type="download_log_candidate",
            owner_id=dlid,
            files=files,
            measured_at="2026-05-16 10:00:00+00",
        )

        self._apply_021()

        rows = self._query("SELECT id FROM album_quality_evidence")
        self.assertEqual(len(rows), 1)
        survivor_id = rows[0][0]
        # The newer row's measured_at wins.
        self.assertEqual(survivor_id, e2)
        self.assertNotEqual(survivor_id, e1)

        jfk = self._query_one(
            "SELECT candidate_evidence_id FROM import_jobs WHERE id = %s",
            (jid,),
        )
        dfk = self._query_one(
            "SELECT candidate_evidence_id FROM download_log WHERE id = %s",
            (dlid,),
        )
        assert jfk is not None and dfk is not None
        self.assertEqual(jfk[0], survivor_id)
        self.assertEqual(dfk[0], survivor_id)


@requires_postgres
class TestMigration021CrossWalk(_Mig021CaseBase):
    """A download_log row with no direct evidence ownership gets its FK
    from a sibling import_job's candidate_evidence_id."""

    def test_download_log_fk_inherits_from_sibling_import_job(self):
        mbid = "mb-cross-walk-1"
        rid = self._seed_request(mbid)
        jid = self._seed_import_job(rid, failed_path="/staged/abc")
        dlid = self._seed_download_log(rid, staged_path="/staged/abc")
        self._seed_evidence(
            owner_type="import_job_candidate",
            owner_id=jid,
            files=[("01.flac", 12345, "flac", "flac", "flac")],
        )

        self._apply_021()

        ev_id = self._query_one("SELECT id FROM album_quality_evidence")
        assert ev_id is not None

        dfk = self._query_one(
            "SELECT candidate_evidence_id FROM download_log WHERE id = %s",
            (dlid,),
        )
        assert dfk is not None
        self.assertEqual(dfk[0], ev_id[0])


@requires_postgres
class TestMigration021Orphans(_Mig021CaseBase):
    """Evidence rows whose owner resolves to a NULL mb_release_id are
    deleted by the migration."""

    def test_orphan_evidence_row_deleted(self):
        # Insert an import_job with no request → request_id NULL → owner JOIN
        # yields NULL mb_release_id.
        jid = self._query_one(
            """
            INSERT INTO import_jobs (
                job_type, status, payload, preview_status, request_id
            )
            VALUES (
                'manual_import', 'queued', %s::jsonb, 'waiting', NULL
            )
            RETURNING id
            """,
            (json.dumps({"failed_path": "/orphan/staged"}),),
        )
        assert jid is not None
        self._seed_evidence(
            owner_type="import_job_candidate",
            owner_id=int(jid[0]),
            files=[("01.flac", 12345, "flac", "flac", "flac")],
        )

        # Also seed a healthy row that should survive.
        mbid = "mb-orphan-survivor-1"
        rid = self._seed_request(mbid)
        healthy_job = self._seed_import_job(rid)
        self._seed_evidence(
            owner_type="import_job_candidate",
            owner_id=healthy_job,
            files=[("02.flac", 22345, "flac", "flac", "flac")],
        )

        self._apply_021()

        rows = self._query("SELECT mb_release_id FROM album_quality_evidence")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], mbid)


@requires_postgres
class TestMigration021Idempotence(_Mig021CaseBase):
    """A second apply of migration 021 is a no-op (schema_migrations records
    block re-execution)."""

    def test_second_apply_is_noop(self):
        rid = self._seed_request("mb-idem-1")
        jid = self._seed_import_job(rid)
        self._seed_evidence(
            owner_type="import_job_candidate",
            owner_id=jid,
            files=[("01.flac", 12345, "flac", "flac", "flac")],
        )

        self._apply_021()
        first = self._query(
            "SELECT id, mb_release_id, snapshot_fingerprint, source_path "
            "FROM album_quality_evidence"
        )

        # Run apply_migrations again against the full dir — 021 must be
        # skipped (schema_migrations records it). Migrations newer than
        # 021 (022, 023, …) may legitimately apply because this test
        # base only seeds up to 020 + then jumps to 021.
        applied = apply_migrations(self.dsn, DEFAULT_MIGRATIONS_DIR)
        self.assertNotIn(
            21,
            [m.version for m in applied],
            "Second apply must not re-run migration 021",
        )

        second = self._query(
            "SELECT id, mb_release_id, snapshot_fingerprint, source_path "
            "FROM album_quality_evidence"
        )
        self.assertEqual(first, second)


@requires_postgres
class TestMigration021FingerprintCrossCheck(_Mig021CaseBase):
    """The in-SQL fingerprint must equal Python's ``snapshot_fingerprint`` for
    a representative set of file-list shapes. This is the regression guard
    against silent drift between the migration's SQL and the helper that
    every post-deploy writer will use.
    """

    def _fingerprint_after_migration(self, fixture_files):
        mbid = "mb-fp-" + uuid.uuid4().hex[:6]
        rid = self._seed_request(mbid)
        jid = self._seed_import_job(rid)
        self._seed_evidence(
            owner_type="import_job_candidate",
            owner_id=jid,
            files=fixture_files,
        )
        self._apply_021()
        row = self._query_one(
            "SELECT snapshot_fingerprint FROM album_quality_evidence "
            "WHERE mb_release_id = %s",
            (mbid,),
        )
        assert row is not None
        return row[0]

    def _py_fingerprint(self, fixture_files):
        files = [
            AlbumQualityEvidenceFile(
                relative_path=rel,
                size_bytes=size,
                mtime_ns=0,
                extension=ext,
                container=container,
                codec=codec,
            )
            for (rel, size, ext, container, codec) in fixture_files
        ]
        return snapshot_fingerprint(files)

    def test_single_file_fixture_matches_python(self):
        fixture = [("01.flac", 12345, "flac", "flac", "flac")]
        self.assertEqual(
            self._fingerprint_after_migration(fixture),
            self._py_fingerprint(fixture),
        )

    def test_multi_file_fixture_matches_python(self):
        fixture = [
            ("01 - Track A.mp3", 5_123_456, "mp3", "mp3", "mp3"),
            ("02 - Track B.mp3", 5_678_901, "mp3", "mp3", "mp3"),
            ("03 - Track C.mp3", 4_321_098, "mp3", "mp3", "mp3"),
        ]
        # Seed unsorted to verify SQL's ORDER BY relative_path also sorts.
        seeded = list(reversed(fixture))
        self.assertEqual(
            self._fingerprint_after_migration(seeded),
            self._py_fingerprint(fixture),
        )

    def test_null_codec_fixture_matches_python(self):
        fixture = [("01.unknown", 9999, "unknown", "unknown", None)]
        self.assertEqual(
            self._fingerprint_after_migration(fixture),
            self._py_fingerprint(fixture),
        )


if __name__ == "__main__":
    unittest.main()
