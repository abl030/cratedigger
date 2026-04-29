#!/usr/bin/env python3
"""Pipeline DB — PostgreSQL-based source of truth for the download pipeline.

Connects to PostgreSQL via a DSN (connection string). Both doc1 and doc2
connect over the network — no more SQLite file locking issues on virtiofs.

Usage:
    from lib.pipeline_db import PipelineDB
    db = PipelineDB("postgresql://cratedigger@192.168.1.35/cratedigger")
    db.add_request(mb_release_id="...", artist_name="...", album_title="...", source="redownload")
"""

import os
import json
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from lib.quality import CandidateScore

import psycopg2
import psycopg2.extras

from lib.import_queue import (
    ImportJob,
    IMPORT_JOB_PREVIEW_DISABLED_MESSAGE,
    IMPORT_JOB_PREVIEW_WAITING,
    IMPORT_JOB_PREVIEW_WOULD_IMPORT,
    import_preview_enabled_from_env,
    validate_preview_failure_status,
    validate_job_type,
    validate_payload,
    validate_status,
)
from lib.quality import (CooldownConfig, SpectralMeasurement, V0ProbeEvidence,
                         should_cooldown)
from lib.release_identity import ReleaseIdentity, normalize_release_id

DEFAULT_DSN = os.environ.get("PIPELINE_DB_DSN", "postgresql://cratedigger@localhost/cratedigger")

# Exponential backoff: base_minutes * 2^(attempts-1), capped at max
BACKOFF_BASE_MINUTES = 30
BACKOFF_MAX_MINUTES = 60 * 6  # 6 hours


def _escape_like_pattern(value: str) -> str:
    """Escape SQL LIKE wildcards for ``... LIKE %s ESCAPE '\'``."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

# Advisory-lock namespaces. Every lock in this codebase is
# session-scoped, non-blocking (``pg_try_advisory_lock``), and
# session-reentrant. See ``docs/advisory-locks.md`` for the canonical
# rules covering namespace values, key derivation, ordering, and call
# sites. Every acquire site links back there.

# Per-request lock — force/manual-import double-click protection
# (issue #92). Key = ``request_id``. ``0x46494D50`` = ASCII "FIMP",
# recognisable in ``pg_locks`` during debugging.
ADVISORY_LOCK_NAMESPACE_IMPORT = 0x46494D50

# Per-release lock — cross-process Palo Santo-class protection
# (issue #132 P1, issue #133). Not the 04-20 incident's root cause
# (that was YAML misconfig; see CLAUDE.md § Resolved canonical RCs)
# but an independent vector that could produce similar data loss if
# the lock were missing. Key = ``release_id_to_lock_key(mb_release_id)``.
# ``0x52454C45`` = ASCII "RELE", recognisable alongside FIMP in ``pg_locks``.
ADVISORY_LOCK_NAMESPACE_RELEASE = 0x52454C45

# Singleton importer-worker lock. The DB queue serializes claims, but beets
# mutation is intentionally a single lane, so the worker process itself also
# takes a process-wide lock before recovering or claiming jobs.
# ``0x51554555`` = ASCII "QUEU", recognisable in ``pg_locks``.
ADVISORY_LOCK_NAMESPACE_IMPORTER = 0x51554555


def release_id_to_lock_key(mb_release_id: str) -> int:
    """Map an ``mb_release_id`` string to a stable int32 advisory-lock key.

    PostgreSQL's two-arg ``pg_advisory_lock(int4, int4)`` takes signed
    int32 keys. ``mb_release_id`` is a str — either a MusicBrainz UUID
    (36 chars) or a Discogs numeric release id. ``zlib.crc32`` is
    stable across processes (Python's builtin ``hash`` is salted per
    interpreter — unusable for cross-process locking), fast, and its
    32-bit output fits once we mask to 31 bits to keep the value
    non-negative (simpler to display in ``pg_locks`` rows).

    Collision behaviour: 2^31 distinct keys. With N concurrent
    same-release contenders, collision probability is ~N²/2^31 — a
    false-collision would serialise two unrelated releases, delaying
    the second by at most one import cycle (~minutes). Acceptable:
    losing a cycle of parallelism is cheap, whereas a missed lock on
    the real cross-process race could produce Palo Santo-*class* data
    loss (an independent vector from the 04-20 incident's YAML-misconfig
    root cause — see ``CLAUDE.md`` § Resolved canonical RCs).

    Input is ``.strip()``ed before hashing so a legacy DB row with
    stray leading/trailing whitespace (``"12856590 "`` vs
    ``"12856590"``) still keys the lock at the same value across
    processes — otherwise a normalization mismatch would defeat the
    lock's purpose silently.
    """
    return zlib.crc32(mb_release_id.strip().encode("utf-8")) & 0x7FFFFFFF

# Schema is managed by lib/migrator.py via numbered files in migrations/.
# PipelineDB itself never runs DDL — see scripts/migrate_db.py and the
# cratedigger-db-migrate.service systemd unit (Nix module).


@dataclass(frozen=True)
class RequestSpectralStateUpdate:
    """Typed update for latest-download and on-disk spectral state."""
    last_download: SpectralMeasurement | None = None
    current: SpectralMeasurement | None = None

    def as_update_fields(self) -> dict[str, object]:
        """Expand the typed state into album_requests column updates."""
        fields: dict[str, object] = {}
        if self.last_download is not None:
            fields["last_download_spectral_grade"] = self.last_download.grade
            fields["last_download_spectral_bitrate"] = self.last_download.bitrate_kbps
        if self.current is not None:
            fields["current_spectral_grade"] = self.current.grade
            fields["current_spectral_bitrate"] = self.current.bitrate_kbps
        return fields


# BadAudioHashRow / BadAudioHashInput are @dataclass — not msgspec.Struct —
# because they never cross JSON. They round-trip between Python and PostgreSQL
# only (`bad_audio_hashes` table). Per `.claude/rules/code-quality.md`
# "Wire-boundary types", `@dataclass` is correct here.
@dataclass(frozen=True)
class BadAudioHashInput:
    """One row to insert into `bad_audio_hashes`."""
    hash_value: bytes  # raw 32-byte SHA-256
    audio_format: str  # 'flac' | 'mp3' | 'm4a' | 'ogg' | ...


@dataclass(frozen=True)
class BadAudioHashRow:
    """One row read back from `bad_audio_hashes`."""
    id: int
    hash_value: bytes
    audio_format: str
    request_id: int | None
    reported_username: str | None
    reason: str | None
    reported_at: datetime  # tz-aware


@dataclass(frozen=True)
class RequestV0ProbeStateUpdate:
    """Typed update for current comparable lossless-source V0 probe state."""

    current_lossless_source: V0ProbeEvidence | None = None
    clear_current_lossless_source: bool = False

    def as_update_fields(self) -> dict[str, object]:
        fields: dict[str, object] = {}
        if self.clear_current_lossless_source:
            fields["current_lossless_source_v0_probe_min_bitrate"] = None
            fields["current_lossless_source_v0_probe_avg_bitrate"] = None
            fields["current_lossless_source_v0_probe_median_bitrate"] = None
        elif self.current_lossless_source is not None:
            fields["current_lossless_source_v0_probe_min_bitrate"] = (
                self.current_lossless_source.min_bitrate_kbps
            )
            fields["current_lossless_source_v0_probe_avg_bitrate"] = (
                self.current_lossless_source.avg_bitrate_kbps
            )
            fields["current_lossless_source_v0_probe_median_bitrate"] = (
                self.current_lossless_source.median_bitrate_kbps
            )
        return fields


class PipelineDB:
    """PostgreSQL-backed pipeline database.

    Schema migrations are NOT this class's responsibility. They live in
    ``migrations/*.sql`` and are applied by ``lib.migrator.apply_migrations``,
    which the deploy systemd unit ``cratedigger-db-migrate.service`` runs on every
    ``nixos-rebuild switch``. Construct this class against an already-migrated
    database.
    """

    def __init__(self, dsn=None):
        self.dsn = dsn or DEFAULT_DSN
        self.conn = self._connect()

    def _connect(self):
        conn = psycopg2.connect(
            self.dsn,
            connect_timeout=10,
            options="-c statement_timeout=30000"
                    " -c tcp_keepalives_idle=60"
                    " -c tcp_keepalives_interval=10"
                    " -c tcp_keepalives_count=5",
        )
        conn.autocommit = True
        return conn

    def _ensure_conn(self):
        """Reconnect if the connection is dead."""
        if self.conn.closed:
            self.conn = self._connect()

    def close(self):
        self.conn.close()

    def _execute(self, sql, params=()):
        self._ensure_conn()
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return cur

    @contextmanager
    def advisory_lock(self, namespace: int, key: int) -> Iterator[bool]:
        """Try to acquire a session-level PostgreSQL advisory lock. Non-blocking.

        Yields ``True`` if acquired, ``False`` if another session already
        holds it. Always releases on ``__exit__`` when acquired.

        Used to serialise operations that must not run concurrently on the
        same ``(namespace, key)`` pair across different DB sessions — e.g.
        two ``pipeline-cli force-import`` invocations racing on the same
        ``request_id`` (issue #92). Advisory locks are reentrant within a
        single session, so this only protects against inter-session races;
        the web server (single-threaded ``HTTPServer``) already serialises
        within its own session.

        See ``docs/advisory-locks.md`` for namespaces, keys, ordering,
        and call-site index.
        """
        self._ensure_conn()
        with self.conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s, %s)", (namespace, key))
            row = cur.fetchone()
        acquired = bool(row and row[0])
        try:
            yield acquired
        finally:
            if acquired:
                with self.conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_advisory_unlock(%s, %s)", (namespace, key)
                    )
                    cur.fetchone()

    # --- import_jobs queue ---

    def enqueue_import_job(
        self,
        job_type: str,
        *,
        request_id: int | None = None,
        dedupe_key: str | None = None,
        payload: dict[str, Any] | None = None,
        message: str | None = None,
        preview_enabled: bool | None = None,
    ) -> ImportJob:
        """Create an import job or return the active job with the same key."""
        validate_job_type(job_type)
        payload = validate_payload(job_type, payload or {})
        preview_enabled = (
            import_preview_enabled_from_env()
            if preview_enabled is None
            else preview_enabled
        )
        preview_status = (
            IMPORT_JOB_PREVIEW_WAITING
            if preview_enabled
            else IMPORT_JOB_PREVIEW_WOULD_IMPORT
        )
        preview_message = None if preview_enabled else IMPORT_JOB_PREVIEW_DISABLED_MESSAGE
        preview_completed_at = None if preview_enabled else datetime.now(timezone.utc)
        importable_at = None if preview_enabled else preview_completed_at
        cur = self._execute("""
            WITH inserted AS (
                INSERT INTO import_jobs (
                    job_type, request_id, dedupe_key, payload, message,
                    preview_status, preview_message, preview_completed_at,
                    importable_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dedupe_key)
                    WHERE dedupe_key IS NOT NULL
                      AND status IN ('queued', 'running')
                DO NOTHING
                RETURNING *
            )
            SELECT inserted.*, false AS deduped
            FROM inserted
            UNION ALL
            SELECT import_jobs.*, true AS deduped
            FROM import_jobs
            WHERE %s IS NOT NULL
              AND dedupe_key = %s
              AND status IN ('queued', 'running')
              AND NOT EXISTS (SELECT 1 FROM inserted)
            ORDER BY deduped
            LIMIT 1
        """, (
            job_type,
            request_id,
            dedupe_key,
            psycopg2.extras.Json(payload),
            message,
            preview_status,
            preview_message,
            preview_completed_at,
            importable_at,
            dedupe_key,
            dedupe_key,
        ))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("import job enqueue returned no row")
        return ImportJob.from_row(dict(row), deduped=bool(row["deduped"]))

    def get_import_job(self, job_id: int) -> ImportJob | None:
        cur = self._execute(
            "SELECT * FROM import_jobs WHERE id = %s",
            (job_id,),
        )
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def get_import_job_by_dedupe_key(
        self,
        dedupe_key: str,
        *,
        active_only: bool = True,
    ) -> ImportJob | None:
        status_filter = (
            "AND status IN ('queued', 'running')"
            if active_only
            else ""
        )
        cur = self._execute(f"""
            SELECT *
            FROM import_jobs
            WHERE dedupe_key = %s
            {status_filter}
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
        """, (dedupe_key,))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def list_import_jobs(
        self,
        *,
        status: str | None = None,
        request_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]:
        params: list[Any] = []
        clauses: list[str] = []
        if status is not None:
            validate_status(status)
            clauses.append("status = %s")
            params.append(status)
        if request_id is not None:
            clauses.append("request_id = %s")
            params.append(request_id)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        params.append(limit)
        cur = self._execute(f"""
            SELECT *
            FROM import_jobs
            {where}
            ORDER BY updated_at DESC, id DESC
            LIMIT %s
        """, tuple(params))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def list_active_import_jobs(
        self,
        *,
        request_id: int | None = None,
        limit: int = 50,
    ) -> list[ImportJob]:
        params: list[Any] = []
        request_filter = ""
        if request_id is not None:
            request_filter = "AND request_id = %s"
            params.append(request_id)
        params.append(limit)
        cur = self._execute(f"""
            SELECT *
            FROM import_jobs
            WHERE status IN ('queued', 'running')
            {request_filter}
            ORDER BY created_at ASC, id ASC
            LIMIT %s
        """, tuple(params))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def count_import_jobs_by_status(self) -> dict[str, int]:
        cur = self._execute("""
            SELECT status, COUNT(*) AS count
            FROM import_jobs
            GROUP BY status
        """)
        return {str(row["status"]): int(row["count"]) for row in cur.fetchall()}

    def list_import_job_timeline(self, *, limit: int = 50) -> list[ImportJob]:
        cur = self._execute("""
            SELECT *
            FROM import_jobs
            WHERE status IN ('queued', 'running')
            ORDER BY
              CASE
                WHEN status = 'queued' AND preview_status = 'would_import' THEN 0
                WHEN status = 'running' THEN 1
                WHEN status = 'queued' AND preview_status = 'running' THEN 2
                WHEN status = 'queued' AND preview_status = 'waiting' THEN 3
                ELSE 4
              END,
              CASE
                WHEN status = 'queued' THEN importable_at
              END ASC NULLS LAST,
              created_at ASC,
              id ASC
            LIMIT %s
        """, (limit,))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def claim_next_import_job(
        self,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            WITH next_job AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'queued'
                  AND preview_status = 'would_import'
                ORDER BY importable_at ASC NULLS LAST, created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE import_jobs
            SET status = 'running',
                attempts = attempts + 1,
                worker_id = %s,
                started_at = COALESCE(started_at, NOW()),
                heartbeat_at = NOW(),
                updated_at = NOW()
            FROM next_job
            WHERE import_jobs.id = next_job.id
            RETURNING import_jobs.*
        """, (worker_id,))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def heartbeat_import_job(self, job_id: int) -> bool:
        cur = self._execute("""
            UPDATE import_jobs
            SET heartbeat_at = NOW(), updated_at = NOW()
            WHERE id = %s AND status = 'running'
            RETURNING id
        """, (job_id,))
        return cur.fetchone() is not None

    def mark_import_job_completed(
        self,
        job_id: int,
        *,
        result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            UPDATE import_jobs
            SET status = 'completed',
                result = %s,
                message = %s,
                error = NULL,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
              AND status IN ('queued', 'running')
            RETURNING *
        """, (psycopg2.extras.Json(result or {}), message, job_id))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def mark_import_job_failed(
        self,
        job_id: int,
        *,
        error: str,
        result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            UPDATE import_jobs
            SET status = 'failed',
                result = %s,
                message = %s,
                error = %s,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
              AND status IN ('queued', 'running')
            RETURNING *
        """, (psycopg2.extras.Json(result or {}), message, error, job_id))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def list_stale_running_import_jobs(
        self,
        *,
        older_than: timedelta,
        limit: int = 50,
    ) -> list[ImportJob]:
        cutoff = datetime.now(timezone.utc) - older_than
        cur = self._execute("""
            SELECT *
            FROM import_jobs
            WHERE status = 'running'
              AND COALESCE(heartbeat_at, started_at, updated_at) < %s
            ORDER BY updated_at ASC, id ASC
            LIMIT %s
        """, (cutoff, limit))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def fail_stale_running_import_jobs(
        self,
        *,
        older_than: timedelta,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        cutoff = datetime.now(timezone.utc) - older_than
        cur = self._execute("""
            WITH stale AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'running'
                  AND COALESCE(heartbeat_at, started_at, updated_at) < %s
                ORDER BY updated_at ASC, id ASC
                LIMIT %s
            )
            UPDATE import_jobs
            SET status = 'failed',
                error = %s,
                message = %s,
                completed_at = NOW(),
                updated_at = NOW()
            FROM stale
            WHERE import_jobs.id = stale.id
            RETURNING import_jobs.*
        """, (cutoff, limit, message, message))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def requeue_running_import_jobs(
        self,
        *,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        """Reset abandoned running jobs to queued for immediate retry."""
        cur = self._execute("""
            WITH running AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'running'
                ORDER BY updated_at ASC, id ASC
                LIMIT %s
            )
            UPDATE import_jobs
            SET status = 'queued',
                message = %s,
                error = NULL,
                worker_id = NULL,
                started_at = NULL,
                heartbeat_at = NULL,
                updated_at = NOW()
            FROM running
            WHERE import_jobs.id = running.id
            RETURNING import_jobs.*
        """, (limit, message))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def claim_next_import_preview_job(
        self,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            WITH next_job AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'queued'
                  AND preview_status = 'waiting'
                ORDER BY created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE import_jobs
            SET preview_status = 'running',
                preview_attempts = preview_attempts + 1,
                preview_worker_id = %s,
                preview_started_at = COALESCE(preview_started_at, NOW()),
                preview_heartbeat_at = NOW(),
                preview_message = NULL,
                preview_error = NULL,
                updated_at = NOW()
            FROM next_job
            WHERE import_jobs.id = next_job.id
            RETURNING import_jobs.*
        """, (worker_id,))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def heartbeat_import_job_preview(self, job_id: int) -> bool:
        cur = self._execute("""
            UPDATE import_jobs
            SET preview_heartbeat_at = NOW(), updated_at = NOW()
            WHERE id = %s
              AND status = 'queued'
              AND preview_status = 'running'
            RETURNING id
        """, (job_id,))
        return cur.fetchone() is not None

    def mark_import_job_preview_importable(
        self,
        job_id: int,
        *,
        preview_result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        cur = self._execute("""
            UPDATE import_jobs
            SET preview_status = 'would_import',
                preview_result = %s,
                preview_message = %s,
                preview_error = NULL,
                preview_completed_at = NOW(),
                importable_at = COALESCE(importable_at, NOW()),
                preview_worker_id = NULL,
                preview_heartbeat_at = NULL,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'queued'
              AND preview_status IN ('waiting', 'running')
            RETURNING *
        """, (
            psycopg2.extras.Json(preview_result or {}),
            message,
            job_id,
        ))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def mark_import_job_preview_failed(
        self,
        job_id: int,
        *,
        preview_status: str,
        error: str,
        preview_result: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> ImportJob | None:
        validate_preview_failure_status(preview_status)
        result = dict(preview_result or {})
        cur = self._execute("""
            UPDATE import_jobs
            SET status = 'failed',
                preview_status = %s,
                preview_result = %s,
                preview_message = %s,
                preview_error = %s,
                result = %s,
                message = %s,
                error = %s,
                preview_completed_at = NOW(),
                completed_at = NOW(),
                preview_worker_id = NULL,
                preview_heartbeat_at = NULL,
                updated_at = NOW()
            WHERE id = %s
              AND status = 'queued'
              AND preview_status IN ('waiting', 'running')
            RETURNING *
        """, (
            preview_status,
            psycopg2.extras.Json(result),
            message,
            error,
            psycopg2.extras.Json({"preview": result}),
            message,
            error,
            job_id,
        ))
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row else None

    def list_stale_import_preview_jobs(
        self,
        *,
        older_than: timedelta,
        limit: int = 50,
    ) -> list[ImportJob]:
        cutoff = datetime.now(timezone.utc) - older_than
        cur = self._execute("""
            SELECT *
            FROM import_jobs
            WHERE status = 'queued'
              AND preview_status = 'running'
              AND COALESCE(preview_heartbeat_at, preview_started_at, updated_at) < %s
            ORDER BY updated_at ASC, id ASC
            LIMIT %s
        """, (cutoff, limit))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    def requeue_stale_import_preview_jobs(
        self,
        *,
        older_than: timedelta,
        message: str,
        limit: int = 50,
    ) -> list[ImportJob]:
        cutoff = datetime.now(timezone.utc) - older_than
        cur = self._execute("""
            WITH stale AS (
                SELECT id
                FROM import_jobs
                WHERE status = 'queued'
                  AND preview_status = 'running'
                  AND COALESCE(preview_heartbeat_at, preview_started_at, updated_at) < %s
                ORDER BY updated_at ASC, id ASC
                LIMIT %s
            )
            UPDATE import_jobs
            SET preview_status = 'waiting',
                preview_message = %s,
                preview_error = NULL,
                preview_worker_id = NULL,
                preview_started_at = NULL,
                preview_heartbeat_at = NULL,
                updated_at = NOW()
            FROM stale
            WHERE import_jobs.id = stale.id
            RETURNING import_jobs.*
        """, (cutoff, limit, message))
        return [ImportJob.from_row(dict(row)) for row in cur.fetchall()]

    # --- album_requests CRUD ---

    def add_request(self, artist_name, album_title, source,
                    mb_release_id=None, mb_release_group_id=None,
                    mb_artist_id=None, discogs_release_id=None,
                    year=None, country=None, format=None,
                    source_path=None, reasoning=None,
                    status="wanted"):
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            INSERT INTO album_requests (
                mb_release_id, mb_release_group_id, mb_artist_id, discogs_release_id,
                artist_name, album_title, year, country, format,
                source, source_path, reasoning, status,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            mb_release_id, mb_release_group_id, mb_artist_id, discogs_release_id,
            artist_name, album_title, year, country, format,
            source, source_path, reasoning, status,
            now, now,
        ))
        row = cur.fetchone()
        self.conn.commit()
        assert row is not None, "INSERT RETURNING should always return a row"
        return row["id"]

    def get_request(self, request_id: int) -> dict[str, Any] | None:
        cur = self._execute(
            "SELECT * FROM album_requests WHERE id = %s", (request_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_request_by_mb_release_id(self, mb_release_id) -> dict[str, Any] | None:
        cur = self._execute(
            "SELECT * FROM album_requests WHERE mb_release_id = %s", (mb_release_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_request_by_discogs_release_id(self, discogs_release_id: str) -> dict[str, Any] | None:
        cur = self._execute(
            "SELECT * FROM album_requests WHERE discogs_release_id = %s", (discogs_release_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_request_by_release_id(self, release_id: object | None) -> dict[str, Any] | None:
        """Resolve a pipeline row through the shared exact-release seam.

        - MB UUIDs query ``mb_release_id``.
        - Discogs numerics prefer ``discogs_release_id`` and then fall back to
          ``mb_release_id`` for legacy rows that stored the numeric there.
        - Unknown non-empty strings fall back to ``mb_release_id`` so tests and
          synthetic/manual fixture IDs still round-trip without special casing.
        """
        normalized = normalize_release_id(release_id)
        if not normalized:
            return None

        identity = ReleaseIdentity.from_fields(normalized)
        if identity is None:
            return self.get_request_by_mb_release_id(normalized)

        if identity.source == "musicbrainz":
            return self.get_request_by_mb_release_id(identity.release_id)

        req = self.get_request_by_discogs_release_id(identity.release_id)
        if req:
            return req
        return self.get_request_by_mb_release_id(identity.release_id)

    def delete_request(self, request_id: int) -> None:
        self._execute("DELETE FROM album_requests WHERE id = %s", (request_id,))
        self.conn.commit()

    def update_request_fields(self, request_id: int, **extra: Any) -> None:
        """Update album_requests metadata without changing status."""
        if not extra:
            return
        now = datetime.now(timezone.utc)
        sets = ["updated_at = %s"]
        params: list[object] = [now]
        for key, val in extra.items():
            sets.append(f"{key} = %s")
            params.append(val)
        params.append(request_id)
        self._execute(
            f"UPDATE album_requests SET {', '.join(sets)} WHERE id = %s",
            params,
        )
        self.conn.commit()

    def update_status(self, request_id, status, **extra):
        now = datetime.now(timezone.utc)
        sets = ["status = %s", "active_download_state = NULL", "updated_at = %s"]
        params = [status, now]
        for key, val in extra.items():
            sets.append(f"{key} = %s")
            params.append(val)
        params.append(request_id)
        self._execute(
            f"UPDATE album_requests SET {', '.join(sets)} WHERE id = %s",
            params,
        )
        self.conn.commit()

    def set_manual(
        self,
        request_id: int,
        *,
        manual_reason: str | None = None,
    ) -> None:
        """Flip a request to ``status='manual'``, optionally writing a reason.

        - ``manual_reason`` is a system-driven cause string (e.g.
          ``'search_exhausted'``). When non-None, it is written to the
          new ``album_requests.manual_reason`` column.
        - When ``manual_reason`` is None (the default), the column is left
          untouched — never overwritten with NULL. This protects an
          existing reason when a generic flip path runs against a row
          that already has a populated reason.

        Re-queue is the only path that clears ``manual_reason``; that
        clearing happens in ``reset_to_wanted``.
        """
        now = datetime.now(timezone.utc)
        sets = [
            "status = 'manual'",
            "active_download_state = NULL",
            "updated_at = %s",
        ]
        params: list[object] = [now]
        if manual_reason is not None:
            sets.append("manual_reason = %s")
            params.append(manual_reason)
        params.append(request_id)
        self._execute(
            f"UPDATE album_requests SET {', '.join(sets)} WHERE id = %s",
            params,
        )
        self.conn.commit()

    def update_spectral_state(
        self,
        request_id: int,
        update: RequestSpectralStateUpdate,
    ) -> None:
        """Write spectral state pairs together, including explicit NULLs."""
        self.update_request_fields(request_id, **update.as_update_fields())

    def update_v0_probe_state(
        self,
        request_id: int,
        update: RequestV0ProbeStateUpdate,
    ) -> None:
        """Write current comparable source-probe state together."""
        self.update_request_fields(request_id, **update.as_update_fields())

    def clear_on_disk_quality_fields(self, request_id: int) -> None:
        """Zero fields that describe files currently on disk in beets.

        Call this whenever an album leaves the beets library — ban-source
        followed by ``beet remove -d``, a manual ``beet rm``, etc. The
        fields cleared describe on-disk state:

        - ``verified_lossless`` (set only after a genuine FLAC→V0 chain)
        - ``current_spectral_*`` (spectral grade of files currently in
          beets)
        - ``imported_path`` (beets filesystem path for the release, shown
          directly by the web UI — leaving it populated after a remove
          means the pipeline tab still claims the album is imported at a
          path that has just been deleted)

        ``min_bitrate`` and ``prev_min_bitrate`` are preserved deliberately
        — they still act as a conservative baseline for the next quality-
        gate comparison. ``last_download_spectral_*`` is also preserved:
        that's an audit field describing the most recent download attempt,
        independent of whether the result made it onto disk.
        """
        now = datetime.now(timezone.utc)
        self._execute(
            """UPDATE album_requests SET
                   verified_lossless = FALSE,
                   current_spectral_grade = NULL,
                   current_spectral_bitrate = NULL,
                   current_lossless_source_v0_probe_min_bitrate = NULL,
                   current_lossless_source_v0_probe_avg_bitrate = NULL,
                   current_lossless_source_v0_probe_median_bitrate = NULL,
                   imported_path = NULL,
                   updated_at = %s
               WHERE id = %s""",
            (now, request_id),
        )
        self.conn.commit()

    def reset_to_wanted(self, request_id: int, **fields: Any) -> None:
        """Reset to wanted, clearing retry counters.

        Only fields explicitly passed are updated — omitted fields are
        preserved.  Pass ``search_filetype_override=None`` to clear the column;
        omitting it leaves the existing value untouched.

        Always clears ``manual_reason`` — re-queueing past a manual flip
        means the operator wants a clean slate. Per U6: every re-queue path
        funnels through this method, so a single ``manual_reason = NULL``
        write here covers web UI, CLI, and importer requeue paths.
        """
        now = datetime.now(timezone.utc)
        sets = [
            "status = 'wanted'",
            "search_attempts = 0",
            "download_attempts = 0",
            "validation_attempts = 0",
            "next_retry_after = NULL",
            "last_attempt_at = NULL",
            "active_download_state = NULL",
            "manual_reason = NULL",
            "updated_at = %s",
        ]
        params: list[object] = [now]
        if "search_filetype_override" in fields:
            sets.append("search_filetype_override = %s")
            params.append(fields["search_filetype_override"])
        if "min_bitrate" in fields:
            sets.append("prev_min_bitrate = COALESCE(min_bitrate, prev_min_bitrate)")
            sets.append("min_bitrate = %s")
            params.append(fields["min_bitrate"])
        params.append(request_id)
        self._execute(
            f"UPDATE album_requests SET {', '.join(sets)} WHERE id = %s",
            params,
        )
        self.conn.commit()

    def update_imported_path_by_release_id(
        self,
        *,
        mb_albumid: str,
        discogs_albumid: str,
        new_path: str,
    ) -> int:
        """Update ``imported_path`` for any request whose release id matches.

        Issue #132 P2 / issue #133: when sibling canonicalization in
        the harness moves a sibling's files on disk (e.g. from
        ``/Beets/Shearwater/2006 - Palo Santo/`` to ``…/2006 - Palo
        Santo [2006]/`` after ``%aunique`` re-evaluates because a new
        same-name edition was just imported), the sibling might itself
        be a tracked pipeline request — in which case its
        ``album_requests.imported_path`` column is now stale. The UI
        ("Imported to" label, ban-source button) would point at a
        directory that no longer exists.

        This method finds the tracked request across both layout combos
        (MB-sourced: ``mb_release_id=<mbid>``; Discogs-sourced:
        ``discogs_release_id=<numeric>`` and/or ``mb_release_id=<numeric>``
        for legacy pre-plugin-patch imports) and updates its
        ``imported_path``. Callers pass the two beets-side columns as
        two arguments; either may be the empty string. No-op if neither
        is populated.

        Returns the number of rows updated (usually 0 or 1). A duplicate
        request for the same release in the pipeline DB would return
        more — that's the caller's signal that data is inconsistent
        (the ``UNIQUE`` constraint on ``mb_release_id`` makes duplicate
        MBIDs impossible in practice).
        """
        if not mb_albumid and not discogs_albumid:
            return 0
        now = datetime.now(timezone.utc)
        clauses: list[str] = []
        params: list[object] = [new_path, now]
        if mb_albumid:
            # Beets-side ``mb_albumid`` is either a MB UUID (stored
            # in pipeline's ``mb_release_id``) or a legacy numeric
            # (also stored in ``mb_release_id`` — the pre-plugin-patch
            # layout). Either way the single-column match covers it.
            clauses.append("mb_release_id = %s")
            params.append(mb_albumid)
        if discogs_albumid:
            # Beets-side ``discogs_albumid`` is always numeric. The
            # pipeline side could store the same numeric in EITHER
            # ``discogs_release_id`` (rows added through the web UI
            # after the discogs-plugin integration) OR
            # ``mb_release_id`` (legacy "pipeline compat" convention
            # documented in CLAUDE.md § "Discogs-sourced albums":
            # *Numeric IDs stored in ``mb_release_id`` for pipeline
            # compat*). Match both columns so a sibling whose beets
            # row carries only ``discogs_albumid`` still finds its
            # tracked request regardless of which pipeline layout
            # that request was created under. Codex R2 P2.
            clauses.append(
                "(mb_release_id = %s OR discogs_release_id = %s)")
            params.append(discogs_albumid)
            params.append(discogs_albumid)
        where = " OR ".join(clauses)
        cur = self._execute(
            f"UPDATE album_requests SET imported_path = %s, "
            f"updated_at = %s WHERE {where}",
            tuple(params),
        )
        self.conn.commit()
        return cur.rowcount

    # --- Downloading state ---

    def set_downloading(self, request_id: int, state_json: str) -> bool:
        """Set album to downloading and store the active download state.

        Only transitions from 'wanted' status. Returns True if the update
        matched (album was wanted), False if the status guard prevented it.
        """
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            UPDATE album_requests
            SET status = 'downloading',
                active_download_state = %s::jsonb,
                last_attempt_at = %s,
                updated_at = %s
            WHERE id = %s AND status = 'wanted'
        """, (state_json, now, now, request_id))
        self.conn.commit()
        return cur.rowcount > 0

    def update_download_state(self, request_id: int, state_json: str) -> None:
        """Rewrite active_download_state without changing status or attempt counters."""
        now = datetime.now(timezone.utc)
        self._execute("""
            UPDATE album_requests
            SET active_download_state = %s::jsonb,
                updated_at = %s
            WHERE id = %s
        """, (state_json, now, request_id))
        self.conn.commit()

    def update_download_state_current_path(
        self,
        request_id: int,
        current_path: str | None,
    ) -> None:
        """Rewrite only ``active_download_state.current_path`` on downloading rows."""
        now = datetime.now(timezone.utc)
        self._execute("""
            UPDATE album_requests
            SET active_download_state = jsonb_set(
                    COALESCE(active_download_state, '{}'::jsonb),
                    '{current_path}',
                    to_jsonb(%s::text),
                    true
                ),
                updated_at = %s
            WHERE id = %s
              AND status = 'downloading'
              AND active_download_state IS NOT NULL
        """, (current_path, now, request_id))
        self.conn.commit()

    def get_downloading(self) -> list[dict[str, Any]]:
        """Get all albums currently being downloaded."""
        cur = self._execute(
            "SELECT * FROM album_requests WHERE status = 'downloading' "
            "ORDER BY updated_at ASC"
        )
        return [dict(r) for r in cur.fetchall()]

    def clear_download_state(self, request_id: int) -> None:
        """Clear active_download_state when download completes/fails."""
        now = datetime.now(timezone.utc)
        self._execute("""
            UPDATE album_requests
            SET active_download_state = NULL,
                updated_at = %s
            WHERE id = %s
        """, (now, request_id))
        self.conn.commit()

    # --- Query methods ---

    def get_wanted(self, limit=None):
        now = datetime.now(timezone.utc)
        # New/re-queued albums (0 search attempts) go first, then random.
        # This ensures freshly added or upgrade-requeued albums get picked
        # up on the next cycle instead of waiting for random selection.
        sql = """
            SELECT * FROM album_requests
            WHERE status = 'wanted'
              AND (next_retry_after IS NULL OR next_retry_after <= %s)
            ORDER BY
              CASE WHEN search_attempts = 0 THEN 0 ELSE 1 END,
              RANDOM()
        """
        if limit:
            sql += f" LIMIT {int(limit)}"
        cur = self._execute(sql, (now,))
        return [dict(r) for r in cur.fetchall()]

    def get_log(self, limit: int = 50,
                outcome_filter: str | None = None) -> list[dict[str, object]]:
        """Get recent download_log entries joined with album_requests.

        Args:
            limit: max entries to return
            outcome_filter: "imported" (success + force_import),
                           "rejected" (rejected + failed + timeout),
                           or None for all
        """
        base = """
            SELECT dl.*,
                   ar.album_title, ar.artist_name, ar.mb_release_id,
                   ar.year, ar.country, ar.status AS request_status,
                   ar.min_bitrate AS request_min_bitrate,
                   ar.prev_min_bitrate, ar.search_filetype_override, ar.source
            FROM download_log dl
            JOIN album_requests ar ON dl.request_id = ar.id
        """
        if outcome_filter == "imported":
            base += " WHERE dl.outcome IN ('success', 'force_import')"
        elif outcome_filter == "rejected":
            base += " WHERE dl.outcome IN ('rejected', 'failed', 'timeout')"
        base += " ORDER BY dl.created_at DESC LIMIT %s"
        cur = self._execute(base, (limit,))
        return [dict(r) for r in cur.fetchall()]

    def get_by_status(self, status):
        cur = self._execute(
            "SELECT * FROM album_requests WHERE status = %s ORDER BY created_at ASC",
            (status,),
        )
        return [dict(r) for r in cur.fetchall()]

    def get_recent(self, limit=20):
        """Get recently downloaded/imported albums (must have download history)."""
        cur = self._execute(
            "SELECT ar.* FROM album_requests ar "
            "WHERE EXISTS (SELECT 1 FROM download_log dl WHERE dl.request_id = ar.id) "
            "ORDER BY ar.updated_at DESC LIMIT %s",
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]

    def count_by_status(self):
        cur = self._execute(
            "SELECT status, COUNT(*) as cnt FROM album_requests GROUP BY status"
        )
        return {r["status"]: r["cnt"] for r in cur.fetchall()}

    def list_requests_by_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[dict[str, Any]]:
        """List request rows for one artist, including legacy name fallbacks.

        ``/api/library/artist`` is the SSOT view for albums already in
        beets and albums still wanted in beets. Prefer exact
        ``mb_artist_id`` matches when available, but keep the legacy
        name fallback for older pipeline rows that predate artist-id
        population or store a non-MB value there.
        """
        # Pair with `ESCAPE '\'` below so literal `%` / `_` in artist names
        # do not expand into wildcard matches on PostgreSQL.
        name_pattern = f"%{_escape_like_pattern(artist_name.strip())}%"
        if mb_artist_id:
            cur = self._execute(
                """
                SELECT *
                FROM album_requests
                WHERE mb_artist_id = %s
                   OR (artist_name ILIKE %s ESCAPE '\\'
                       -- Hyphen-free ids (e.g. legacy numerics / Discogs ids)
                       -- deliberately fall back to the artist-name match.
                       AND (mb_artist_id IS NULL OR mb_artist_id = ''
                            OR mb_artist_id NOT LIKE '%%-%%'))
                ORDER BY year, album_title
                """,
                (mb_artist_id, name_pattern),
            )
        else:
            cur = self._execute(
                """
                SELECT *
                FROM album_requests
                WHERE artist_name ILIKE %s ESCAPE '\\'
                ORDER BY year, album_title
                """,
                (name_pattern,),
            )
        return [dict(r) for r in cur.fetchall()]

    # --- Track management ---

    def set_tracks(self, request_id, tracks):
        self._execute("DELETE FROM album_tracks WHERE request_id = %s", (request_id,))
        for t in tracks:
            self._execute("""
                INSERT INTO album_tracks (request_id, disc_number, track_number, title, length_seconds)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                request_id,
                t.get("disc_number", 1),
                t["track_number"],
                t["title"],
                t.get("length_seconds"),
            ))
        self.conn.commit()

    def get_tracks(self, request_id):
        cur = self._execute("""
            SELECT disc_number, track_number, title, length_seconds
            FROM album_tracks
            WHERE request_id = %s
            ORDER BY disc_number, track_number
        """, (request_id,))
        return [dict(r) for r in cur.fetchall()]

    # --- Download logging ---

    def log_download(self, request_id, soulseek_username=None, filetype=None,
                     download_path=None, beets_distance=None, beets_scenario=None,
                     beets_detail=None, valid=None, outcome=None,
                     staged_path=None, error_message=None,
                     bitrate=None, sample_rate=None, bit_depth=None,
                     is_vbr=None, was_converted=None, original_filetype=None,
                     # Spectral quality verification fields
                     slskd_filetype=None, slskd_bitrate=None,
                     actual_filetype=None, actual_min_bitrate=None,
                     spectral_grade=None, spectral_bitrate=None,
                     existing_min_bitrate=None, existing_spectral_bitrate=None,
                     # Full import_one.py result (JSON string)
                     import_result=None,
                     # Full validation result (JSON string)
                     validation_result=None,
                     # Final format on disk
                     final_format=None,
                     v0_probe_kind=None, v0_probe_min_bitrate=None,
                     v0_probe_avg_bitrate=None,
                     v0_probe_median_bitrate=None,
                     existing_v0_probe_kind=None,
                     existing_v0_probe_min_bitrate=None,
                     existing_v0_probe_avg_bitrate=None,
                     existing_v0_probe_median_bitrate=None):
        cur = self._execute("""
            INSERT INTO download_log (
                request_id, soulseek_username, filetype, download_path,
                beets_distance, beets_scenario, beets_detail, valid,
                outcome, staged_path, error_message,
                bitrate, sample_rate, bit_depth, is_vbr,
                was_converted, original_filetype,
                slskd_filetype, slskd_bitrate,
                actual_filetype, actual_min_bitrate,
                spectral_grade, spectral_bitrate,
                existing_min_bitrate, existing_spectral_bitrate,
                import_result, validation_result, final_format,
                v0_probe_kind, v0_probe_min_bitrate,
                v0_probe_avg_bitrate, v0_probe_median_bitrate,
                existing_v0_probe_kind, existing_v0_probe_min_bitrate,
                existing_v0_probe_avg_bitrate, existing_v0_probe_median_bitrate
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            request_id, soulseek_username, filetype, download_path,
            beets_distance, beets_scenario, beets_detail, valid,
            outcome, staged_path, error_message,
            bitrate, sample_rate, bit_depth, is_vbr,
            was_converted, original_filetype,
            slskd_filetype, slskd_bitrate,
            actual_filetype, actual_min_bitrate,
            spectral_grade, spectral_bitrate,
            existing_min_bitrate, existing_spectral_bitrate,
            import_result, validation_result, final_format,
            v0_probe_kind, v0_probe_min_bitrate,
            v0_probe_avg_bitrate, v0_probe_median_bitrate,
            existing_v0_probe_kind, existing_v0_probe_min_bitrate,
            existing_v0_probe_avg_bitrate, existing_v0_probe_median_bitrate,
        ))
        row = cur.fetchone()
        self.conn.commit()
        assert row is not None, "INSERT RETURNING should always return a row"
        return int(row["id"])

    def get_download_log_entry(self, log_id):
        """Get a single download_log entry by its ID."""
        cur = self._execute(
            "SELECT * FROM download_log WHERE id = %s", (log_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_download_history(self, request_id):
        cur = self._execute("""
            SELECT * FROM download_log
            WHERE request_id = %s
            ORDER BY id DESC
        """, (request_id,))
        return [dict(r) for r in cur.fetchall()]

    def get_download_history_batch(self, request_ids: list[int]) -> dict[int, list[dict]]:
        """Batch fetch download history for multiple request IDs.

        Returns dict of request_id → list of history rows (most recent first).
        """
        if not request_ids:
            return {}
        ph = ",".join(["%s"] * len(request_ids))
        cur = self._execute(
            f"SELECT * FROM download_log WHERE request_id IN ({ph}) ORDER BY id DESC",
            tuple(request_ids),
        )
        result: dict[int, list[dict]] = {}
        for row in cur.fetchall():
            r = dict(row)
            rid = r["request_id"]
            if rid not in result:
                result[rid] = []
            result[rid].append(r)
        return result

    # -- Wrong matches ---------------------------------------------------------

    def get_wrong_matches(self) -> list[dict[str, object]]:
        """Return every rejected wrong-match candidate still on disk.

        Issue #113: one row per actionable folder, not one per request.
        ``download_log`` accumulates multiple rejected rows for the same
        ``failed_path`` whenever a folder is retried (force/manual paths log
        the same ``failed_path`` on every retry), so we collapse to the newest
        row per ``(request_id, failed_path)`` pair — each surviving row
        represents a distinct on-disk directory the user can act on.

        Only wrong-match rejections survive — ``audio_corrupt`` /
        ``spectral_reject`` scenarios have their own handling and stay out of
        the manual-review queue.
        """
        cur = self._execute("""
            SELECT DISTINCT ON (dl.request_id, dl.validation_result->>'failed_path')
                dl.id AS download_log_id,
                dl.request_id,
                ar.artist_name,
                ar.album_title,
                ar.mb_release_id,
                dl.soulseek_username,
                dl.validation_result,
                dl.spectral_grade,
                dl.spectral_bitrate,
                dl.v0_probe_kind,
                dl.v0_probe_avg_bitrate,
                ar.status AS request_status,
                ar.min_bitrate AS request_min_bitrate,
                ar.verified_lossless AS request_verified_lossless,
                ar.current_spectral_grade AS request_current_spectral_grade,
                ar.current_spectral_bitrate AS request_current_spectral_bitrate,
                ar.imported_path AS request_imported_path
            FROM download_log dl
            JOIN album_requests ar ON dl.request_id = ar.id
            WHERE dl.outcome = 'rejected'
              AND dl.validation_result->>'failed_path' IS NOT NULL
              AND (dl.validation_result->>'scenario' IS NULL
                   OR dl.validation_result->>'scenario' NOT IN ('audio_corrupt', 'spectral_reject'))
            ORDER BY dl.request_id, dl.validation_result->>'failed_path', dl.id DESC
        """)
        rows = [dict(r) for r in cur.fetchall()]
        # DISTINCT ON sorts by path within a request; re-sort so the route
        # layer sees newest-first within each request, matching the frontend
        # expectation that the most-recent candidate appears first.
        rows.sort(key=lambda r: (r["request_id"], -int(r["download_log_id"])))
        return rows

    def clear_wrong_match_path(self, log_id: int) -> bool:
        """Null out failed_path in validation_result for a download_log entry.

        Returns True if the entry was found and updated.
        """
        cur = self._execute("""
            UPDATE download_log
            SET validation_result = validation_result - 'failed_path'
            WHERE id = %s AND validation_result->>'failed_path' IS NOT NULL
        """, (log_id,))
        return cur.rowcount > 0

    def clear_wrong_match_paths(
        self,
        request_id: int,
        failed_paths: list[str] | tuple[str, ...] | set[str],
    ) -> int:
        """Null out failed_path for rejected rows matching request/path pairs."""
        paths = [str(path) for path in dict.fromkeys(failed_paths) if path]
        if not paths:
            return 0
        placeholders = ", ".join(["%s"] * len(paths))
        cur = self._execute(f"""
            UPDATE download_log
            SET validation_result = validation_result - 'failed_path'
            WHERE request_id = %s
              AND outcome = 'rejected'
              AND validation_result->>'failed_path' IN ({placeholders})
        """, tuple([request_id, *paths]))
        self.conn.commit()
        return cur.rowcount

    def update_download_log_measurement(
        self,
        download_log_id: int,
        *,
        spectral_grade: str | None = None,
        spectral_bitrate: int | None = None,
        v0_probe_kind: str | None = None,
        v0_probe_avg_bitrate: int | None = None,
    ) -> bool:
        """Persist measurement evidence onto one download_log row.

        Partial / non-destructive: only columns whose source value is
        non-None are touched. Used by wrong-match triage to plumb the
        measurement from ``ImportPreviewResult.import_result`` onto the
        same row that ``get_wrong_matches`` reads, so the candidate-
        evidence cells from PR #181 populate without changing the read
        path. Returns True when at least one column was updated, False
        when the call was a no-op (all None) or the row didn't exist.
        """
        sets: list[str] = []
        params: list[object] = []
        if spectral_grade is not None:
            sets.append("spectral_grade = %s")
            params.append(spectral_grade)
        if spectral_bitrate is not None:
            sets.append("spectral_bitrate = %s")
            params.append(spectral_bitrate)
        if v0_probe_kind is not None:
            sets.append("v0_probe_kind = %s")
            params.append(v0_probe_kind)
        if v0_probe_avg_bitrate is not None:
            sets.append("v0_probe_avg_bitrate = %s")
            params.append(v0_probe_avg_bitrate)
        if not sets:
            return False
        params.append(download_log_id)
        cur = self._execute(
            f"UPDATE download_log SET {', '.join(sets)} WHERE id = %s",
            tuple(params),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def record_wrong_match_triage(
        self,
        log_id: int,
        triage_result: dict[str, object],
    ) -> bool:
        """Persist preview-driven triage audit details on a download_log row."""
        cur = self._execute("""
            UPDATE download_log
            SET validation_result = jsonb_set(
                CASE
                    WHEN jsonb_typeof(validation_result) = 'object'
                    THEN validation_result
                    ELSE '{}'::jsonb
                END,
                '{wrong_match_triage}',
                %s::jsonb,
                true
            )
            WHERE id = %s
        """, (json.dumps(triage_result), log_id))
        self.conn.commit()
        return cur.rowcount > 0

    # -- Search log -----------------------------------------------------------

    def log_search(self, request_id: int, query: str | None = None,
                   result_count: int | None = None,
                   elapsed_s: float | None = None,
                   outcome: str = "error",
                   candidates: "list[CandidateScore] | None" = None,
                   variant: str | None = None,
                   final_state: str | None = None) -> None:
        """Record one search attempt for an album request.

        ``candidates`` is the top-N forensic ``CandidateScore`` list (already
        truncated by the caller). It is encoded via ``msgspec.json.encode``
        and written to ``search_log.candidates`` JSONB. ``None`` writes SQL
        NULL — error / submission-failure rows have no scoring data to
        report. See ``.claude/rules/code-quality.md`` § Wire-boundary types
        for the symmetric encode/decode contract.
        """
        candidates_json: str | None = None
        if candidates is not None:
            import msgspec  # local import keeps top-of-module deps narrow
            candidates_json = msgspec.json.encode(candidates).decode()
        self._execute("""
            INSERT INTO search_log (
                request_id, query, result_count, elapsed_s, outcome,
                candidates, variant, final_state
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (request_id, query, result_count, elapsed_s, outcome,
              candidates_json, variant, final_state))
        self.conn.commit()

    def get_search_history(self, request_id: int) -> list[dict[str, object]]:
        """Return all search_log rows for a single request_id, newest first."""
        cur = self._execute("""
            SELECT * FROM search_log
            WHERE request_id = %s
            ORDER BY id DESC
        """, (request_id,))
        return [dict(r) for r in cur.fetchall()]

    def get_search_history_batch(self, request_ids: list[int]) -> dict[int, list[dict[str, object]]]:
        """Batch fetch search history for multiple request IDs.

        Returns dict of request_id → list of history rows (most recent first).
        """
        if not request_ids:
            return {}
        ph = ",".join(["%s"] * len(request_ids))
        cur = self._execute(
            f"SELECT * FROM search_log WHERE request_id IN ({ph}) ORDER BY id DESC",
            tuple(request_ids),
        )
        result: dict[int, list[dict[str, object]]] = {}
        for row in cur.fetchall():
            r = dict(row)
            rid = r["request_id"]
            assert isinstance(rid, int)
            if rid not in result:
                result[rid] = []
            result[rid].append(r)
        return result

    # -- Track counts --------------------------------------------------------

    def get_track_counts(self, request_ids: list[int]) -> dict[int, int]:
        """Batch fetch track counts for multiple request IDs.

        Returns dict of request_id → track count (only for IDs with tracks).
        """
        if not request_ids:
            return {}
        ph = ",".join(["%s"] * len(request_ids))
        cur = self._execute(
            f"SELECT request_id, COUNT(*) FROM album_tracks "
            f"WHERE request_id IN ({ph}) GROUP BY request_id",
            tuple(request_ids),
        )
        return {row["request_id"]: row["count"] for row in cur.fetchall()}

    # --- Denylist ---

    def add_denylist(self, request_id, username, reason=None):
        self._execute("""
            INSERT INTO source_denylist (request_id, username, reason)
            VALUES (%s, %s, %s)
            ON CONFLICT (request_id, username) DO NOTHING
        """, (request_id, username, reason))
        self.conn.commit()

    def get_denylisted_users(self, request_id):
        cur = self._execute("""
            SELECT username, reason, created_at
            FROM source_denylist
            WHERE request_id = %s
            ORDER BY created_at ASC
        """, (request_id,))
        return [dict(r) for r in cur.fetchall()]

    # --- User cooldowns (issue #39) ---

    def add_cooldown(self, username: str, cooldown_until: datetime,
                     reason: str | None = None) -> None:
        """Insert or update a user cooldown (upsert by username)."""
        self._execute("""
            INSERT INTO user_cooldowns (username, cooldown_until, reason)
            VALUES (%s, %s, %s)
            ON CONFLICT (username) DO UPDATE
                SET cooldown_until = EXCLUDED.cooldown_until,
                    reason = EXCLUDED.reason
        """, (username, cooldown_until, reason))
        self.conn.commit()

    def get_cooled_down_users(self) -> list[str]:
        """Return usernames with active (non-expired) cooldowns."""
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            SELECT username FROM user_cooldowns
            WHERE cooldown_until > %s
        """, (now,))
        return [r["username"] for r in cur.fetchall()]

    def get_user_cooldowns(self) -> list[dict[str, Any]]:
        """Return all cooldown rows (including expired) for CLI/web display."""
        cur = self._execute("""
            SELECT username, cooldown_until, reason, created_at
            FROM user_cooldowns
            ORDER BY cooldown_until DESC
        """)
        return [dict(r) for r in cur.fetchall()]

    def check_and_apply_cooldown(
        self,
        username: str,
        config: CooldownConfig | None = None,
    ) -> bool:
        """Check a user's recent outcomes and apply cooldown if warranted.

        Queries the last N download_log outcomes for this user globally
        (across all requests), then delegates to should_cooldown().
        Returns True if a cooldown was applied.
        """
        cfg = config or CooldownConfig()
        cur = self._execute("""
            SELECT outcome FROM download_log
            WHERE outcome IS NOT NULL
              AND %s = ANY(
                  regexp_split_to_array(
                      regexp_replace(COALESCE(soulseek_username, ''), '\\s*,\\s*', ',', 'g'),
                      ','
                  )
              )
            ORDER BY id DESC
            LIMIT %s
        """, (username, cfg.lookback_window))
        outcomes = [r["outcome"] for r in cur.fetchall()]
        if not should_cooldown(outcomes, cfg):
            return False
        cooldown_until = datetime.now(timezone.utc) + timedelta(days=cfg.cooldown_days)
        self.add_cooldown(
            username, cooldown_until,
            f"{cfg.failure_threshold} consecutive failures",
        )
        return True

    # --- Retry logic ---

    def record_attempt(self, request_id, attempt_type):
        col = f"{attempt_type}_attempts"
        now = datetime.now(timezone.utc)

        # Atomic increment + fetch in single statement (avoids TOCTOU race)
        cur = self._execute(f"""
            UPDATE album_requests
            SET {col} = COALESCE({col}, 0) + 1,
                last_attempt_at = %s,
                updated_at = %s
            WHERE id = %s
            RETURNING {col}
        """, (now, now, request_id))
        row = cur.fetchone()
        assert row is not None, f"Request {request_id} not found"
        new_count: int = int(row[col])

        # Exponential backoff: base * 2^(attempts-1), capped
        backoff_minutes = min(
            BACKOFF_BASE_MINUTES * (2 ** (new_count - 1)),
            BACKOFF_MAX_MINUTES,
        )
        next_retry = now + timedelta(minutes=backoff_minutes)

        self._execute("""
            UPDATE album_requests
            SET next_retry_after = %s
            WHERE id = %s
        """, (next_retry, request_id))

    # --- bad_audio_hashes (curator-reported bad-rip audio-content hashes) ---

    def add_bad_audio_hashes(
        self,
        request_id: int,
        reported_username: str | None,
        reason: str | None,
        hashes: list[BadAudioHashInput],
    ) -> int:
        """Insert curator-reported bad-rip hashes; return count of NEW rows.

        Single multi-row INSERT with ON CONFLICT (hash_value, audio_format)
        DO NOTHING — re-reporting the same content on a second click is a
        no-op (returns 0). Per Key Technical Decision in the plan,
        request_id is intentionally NOT part of the unique key.
        """
        if not hashes:
            return 0
        values_sql = ",".join(["(%s, %s, %s, %s, %s)"] * len(hashes))
        params: list[Any] = []
        for h in hashes:
            params.extend([
                psycopg2.Binary(h.hash_value),
                h.audio_format,
                request_id,
                reported_username,
                reason,
            ])
        cur = self._execute(f"""
            INSERT INTO bad_audio_hashes
                (hash_value, audio_format, request_id, reported_username, reason)
            VALUES {values_sql}
            ON CONFLICT (hash_value, audio_format) DO NOTHING
            RETURNING id
        """, tuple(params))
        inserted = cur.fetchall()
        return len(inserted)

    def lookup_bad_audio_hash(
        self,
        hash_value: bytes,
        audio_format: str,
    ) -> BadAudioHashRow | None:
        """Point-lookup by (hash_value, audio_format). Returns None on miss."""
        cur = self._execute("""
            SELECT id, hash_value, audio_format, request_id,
                   reported_username, reason, reported_at
            FROM bad_audio_hashes
            WHERE hash_value = %s AND audio_format = %s
            LIMIT 1
        """, (psycopg2.Binary(hash_value), audio_format))
        row = cur.fetchone()
        if row is None:
            return None
        # psycopg2 returns BYTEA as memoryview; coerce to bytes for the typed row.
        raw = row["hash_value"]
        if isinstance(raw, memoryview):
            raw = bytes(raw)
        return BadAudioHashRow(
            id=int(row["id"]),
            hash_value=raw,
            audio_format=str(row["audio_format"]),
            request_id=(int(row["request_id"])
                        if row["request_id"] is not None else None),
            reported_username=row["reported_username"],
            reason=row["reason"],
            reported_at=row["reported_at"],
        )

    def has_any_bad_audio_hashes(self) -> bool:
        """Empty-table fast-path probe; uncached at this layer."""
        cur = self._execute(
            "SELECT 1 FROM bad_audio_hashes LIMIT 1"
        )
        return cur.fetchone() is not None

    def get_recent_successful_uploader(
        self,
        request_id: int,
    ) -> str | None:
        """Return the most recent successful uploader for this request.

        Used by the ban-source route to resolve `reported_username`
        server-side. Considers both `success` and `force_import` outcomes.
        """
        cur = self._execute("""
            SELECT soulseek_username
            FROM download_log
            WHERE request_id = %s
              AND outcome IN ('success', 'force_import')
              AND soulseek_username IS NOT NULL
            ORDER BY id DESC
            LIMIT 1
        """, (request_id,))
        row = cur.fetchone()
        return row["soulseek_username"] if row else None

    def get_active_import_job_for_request(
        self,
        request_id: int,
    ) -> dict[str, Any] | None:
        """Return the most recent queued/running import job for this request.

        Used by the ban-source route's importer-race check (E1.3 in the
        plan). Returns the raw row dict (not an `ImportJob`) because the
        caller only inspects `status` for the 409 decision.
        """
        cur = self._execute("""
            SELECT *
            FROM import_jobs
            WHERE request_id = %s
              AND status IN ('queued', 'running')
            ORDER BY id DESC
            LIMIT 1
        """, (request_id,))
        row = cur.fetchone()
        return dict(row) if row else None
