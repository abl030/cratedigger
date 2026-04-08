#!/usr/bin/env python3
"""Pipeline DB — PostgreSQL-based source of truth for the download pipeline.

Connects to PostgreSQL via a DSN (connection string). Both doc1 and doc2
connect over the network — no more SQLite file locking issues on virtiofs.

Usage:
    from pipeline_db import PipelineDB
    db = PipelineDB("postgresql://soularr@192.168.1.35/soularr")
    db.add_request(mb_release_id="...", artist_name="...", album_title="...", source="redownload")
"""

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
import psycopg2.extras

from lib.quality import SpectralMeasurement

DEFAULT_DSN = os.environ.get("PIPELINE_DB_DSN", "postgresql://soularr@localhost/soularr")

# Exponential backoff: base_minutes * 2^(attempts-1), capped at max
BACKOFF_BASE_MINUTES = 30
BACKOFF_MAX_MINUTES = 60 * 24  # 24 hours

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS album_requests (
    id SERIAL PRIMARY KEY,

    -- Identity (at least one required)
    mb_release_id TEXT UNIQUE,
    mb_release_group_id TEXT,
    mb_artist_id TEXT,
    discogs_release_id TEXT,

    -- Metadata
    artist_name TEXT NOT NULL,
    album_title TEXT NOT NULL,
    year INTEGER,
    country TEXT,
    format TEXT,

    -- Source
    source TEXT NOT NULL CHECK(source IN ('redownload', 'request', 'manual')),
    source_path TEXT,
    reasoning TEXT,

    -- Status lifecycle
    status TEXT NOT NULL DEFAULT 'wanted'
        CHECK(status IN ('wanted', 'downloading', 'imported', 'manual')),

    -- Retry
    search_attempts INTEGER NOT NULL DEFAULT 0,
    download_attempts INTEGER NOT NULL DEFAULT 0,
    validation_attempts INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    next_retry_after TIMESTAMPTZ,

    -- Import result
    beets_distance REAL,
    beets_scenario TEXT,
    imported_path TEXT,

    -- Quality upgrade
    search_filetype_override TEXT,
    target_format TEXT,
    min_bitrate INTEGER,
    prev_min_bitrate INTEGER,

    -- Legacy Lidarr columns (unused, kept for schema compat)
    lidarr_album_id INTEGER,
    lidarr_artist_id INTEGER,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS album_tracks (
    id SERIAL PRIMARY KEY,
    request_id INTEGER NOT NULL REFERENCES album_requests(id) ON DELETE CASCADE,
    disc_number INTEGER NOT NULL DEFAULT 1,
    track_number INTEGER NOT NULL,
    title TEXT NOT NULL,
    length_seconds REAL
);

CREATE TABLE IF NOT EXISTS download_log (
    id SERIAL PRIMARY KEY,
    request_id INTEGER NOT NULL REFERENCES album_requests(id) ON DELETE CASCADE,
    soulseek_username TEXT,
    filetype TEXT,
    download_path TEXT,
    beets_distance REAL,
    beets_scenario TEXT,
    beets_detail TEXT,
    valid BOOLEAN,
    outcome TEXT CHECK(outcome IN ('success', 'rejected', 'failed', 'timeout', 'force_import')),
    staged_path TEXT,
    error_message TEXT,
    bitrate INTEGER,
    sample_rate INTEGER,
    bit_depth INTEGER,
    is_vbr BOOLEAN,
    was_converted BOOLEAN DEFAULT FALSE,
    original_filetype TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source_denylist (
    id SERIAL PRIMARY KEY,
    request_id INTEGER NOT NULL REFERENCES album_requests(id) ON DELETE CASCADE,
    username TEXT NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(request_id, username)
);

CREATE INDEX IF NOT EXISTS idx_requests_status ON album_requests(status);
CREATE INDEX IF NOT EXISTS idx_requests_mb_release ON album_requests(mb_release_id);
CREATE INDEX IF NOT EXISTS idx_requests_source ON album_requests(source);
CREATE INDEX IF NOT EXISTS idx_tracks_request ON album_tracks(request_id);
CREATE INDEX IF NOT EXISTS idx_download_log_request ON download_log(request_id);
CREATE INDEX IF NOT EXISTS idx_denylist_request ON source_denylist(request_id);
"""


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


class PipelineDB:
    """PostgreSQL-backed pipeline database."""

    def __init__(self, dsn=None, run_migrations=False):
        self.dsn = dsn or DEFAULT_DSN
        self.conn = self._connect()
        if run_migrations:
            self.init_schema()

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

    def init_schema(self):
        """Run DDL on a separate short-lived connection to avoid blocking."""
        mig_conn = psycopg2.connect(self.dsn, connect_timeout=10)
        mig_conn.autocommit = True
        with mig_conn.cursor() as cur:
            cur.execute("SET lock_timeout TO '5s'")
            # Safety net: kill any future idle-in-transaction after 60s
            try:
                cur.execute(
                    "ALTER ROLE soularr SET idle_in_transaction_session_timeout = '60s'"
                )
            except psycopg2.errors.UndefinedObject:
                pass  # Role doesn't exist (e.g. ephemeral test DB)
            cur.execute(SCHEMA_SQL)
            for col, coltype in [
                ("bitrate", "INTEGER"),
                ("sample_rate", "INTEGER"),
                ("bit_depth", "INTEGER"),
                ("is_vbr", "BOOLEAN"),
                ("was_converted", "BOOLEAN DEFAULT FALSE"),
                ("original_filetype", "TEXT"),
                # Spectral quality verification columns
                ("slskd_filetype", "TEXT"),
                ("slskd_bitrate", "INTEGER"),
                ("actual_filetype", "TEXT"),
                ("actual_min_bitrate", "INTEGER"),
                ("spectral_grade", "TEXT"),
                ("spectral_bitrate", "INTEGER"),
                ("existing_min_bitrate", "INTEGER"),
                ("existing_spectral_bitrate", "INTEGER"),
                # Full import_one.py result for audit trail
                ("import_result", "JSONB"),
                # Full validation result for audit trail
                ("validation_result", "JSONB"),
                # Final format on disk (e.g. "opus 128" when Opus conversion used)
                ("final_format", "TEXT"),
            ]:
                cur.execute(f"""
                    DO $$ BEGIN
                        ALTER TABLE download_log ADD COLUMN {col} {coltype};
                    EXCEPTION WHEN duplicate_column THEN NULL;
                    END $$;
                """)
            for old_col, new_col in [
                ("spectral_bitrate", "last_download_spectral_bitrate"),
                ("spectral_grade", "last_download_spectral_grade"),
                ("on_disk_spectral_grade", "current_spectral_grade"),
                ("on_disk_spectral_bitrate", "current_spectral_bitrate"),
            ]:
                cur.execute(f"""
                    DO $$ BEGIN
                        IF EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema = 'public'
                              AND table_name = 'album_requests'
                              AND column_name = '{old_col}'
                        ) AND NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema = 'public'
                              AND table_name = 'album_requests'
                              AND column_name = '{new_col}'
                        ) THEN
                            ALTER TABLE album_requests RENAME COLUMN {old_col} TO {new_col};
                        END IF;
                    END $$;
                """)
            for col, coltype in [
                ("search_filetype_override", "TEXT"),
                ("min_bitrate", "INTEGER"),
                ("prev_min_bitrate", "INTEGER"),
                ("verified_lossless", "BOOLEAN DEFAULT FALSE"),
                # Latest spectral analysis of the most recent download attempt
                ("last_download_spectral_bitrate", "INTEGER"),
                ("last_download_spectral_grade", "TEXT"),
                # Spectral data for the files currently in beets, regardless
                # of how they got there — updated on every spectral run
                ("current_spectral_grade", "TEXT"),
                ("current_spectral_bitrate", "INTEGER"),
                # Async downloads: per-album download state
                ("active_download_state", "JSONB"),
                # Final format on disk (e.g. "opus 128" when Opus conversion used)
                ("final_format", "TEXT"),
            ]:
                cur.execute(f"""
                    DO $$ BEGIN
                        ALTER TABLE album_requests ADD COLUMN {col} {coltype};
                    EXCEPTION WHEN duplicate_column THEN NULL;
                    END $$;
                """)
            # Migrate outcome CHECK constraint to include 'force_import'
            cur.execute("""
                DO $$ BEGIN
                    ALTER TABLE download_log DROP CONSTRAINT IF EXISTS download_log_outcome_check;
                    ALTER TABLE download_log ADD CONSTRAINT download_log_outcome_check
                        CHECK (outcome IN ('success', 'rejected', 'failed', 'timeout', 'force_import', 'manual_import'));
                END $$;
            """)
            # Migrate status CHECK to include 'downloading'
            cur.execute("""
                DO $$ BEGIN
                    ALTER TABLE album_requests DROP CONSTRAINT IF EXISTS album_requests_status_check;
                    ALTER TABLE album_requests ADD CONSTRAINT album_requests_status_check
                        CHECK(status IN ('wanted', 'downloading', 'imported', 'manual'));
                END $$;
            """)
            # Migrate symbolic intent names to concrete CSV values
            cur.execute("""
                UPDATE album_requests SET search_filetype_override = 'flac,mp3 v0,mp3 320'
                WHERE search_filetype_override IN ('flac_preferred', 'upgrade');
            """)
            cur.execute("""
                UPDATE album_requests SET search_filetype_override = NULL
                WHERE search_filetype_override = 'best_effort';
            """)
            # Rename quality_override → search_filetype_override
            cur.execute("""
                DO $$ BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'album_requests'
                          AND column_name = 'quality_override'
                    ) AND NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'album_requests'
                          AND column_name = 'search_filetype_override'
                    ) THEN
                        ALTER TABLE album_requests RENAME COLUMN quality_override TO search_filetype_override;
                    END IF;
                END $$;
            """)
            # Add target_format column
            cur.execute("""
                DO $$ BEGIN
                    ALTER TABLE album_requests ADD COLUMN target_format TEXT;
                EXCEPTION WHEN duplicate_column THEN NULL;
                END $$;
            """)
            # Rename "flac" tier to "lossless" (issue #35)
            cur.execute("""
                UPDATE album_requests SET search_filetype_override = 'lossless'
                WHERE search_filetype_override = 'flac';
            """)
            cur.execute("""
                UPDATE album_requests SET target_format = 'lossless'
                WHERE target_format = 'flac';
            """)
            # Clean up nonsensical target_format values from old upgrade intent
            cur.execute("""
                UPDATE album_requests SET target_format = NULL
                WHERE target_format LIKE '%,%';
            """)
        mig_conn.close()

    def close(self):
        self.conn.close()

    def _execute(self, sql, params=()):
        self._ensure_conn()
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return cur

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

    def get_request(self, request_id) -> dict[str, Any] | None:
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

    def delete_request(self, request_id):
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

    def update_spectral_state(
        self,
        request_id: int,
        update: RequestSpectralStateUpdate,
    ) -> None:
        """Write spectral state pairs together, including explicit NULLs."""
        self.update_request_fields(request_id, **update.as_update_fields())

    def reset_to_wanted(self, request_id: int, **fields: Any) -> None:
        """Reset to wanted, clearing retry counters.

        Only fields explicitly passed are updated — omitted fields are
        preserved.  Pass ``search_filetype_override=None`` to clear the column;
        omitting it leaves the existing value untouched.
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
                   ar.prev_min_bitrate, ar.search_filetype_override AS quality_override, ar.source
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
                     final_format=None):
        self._execute("""
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
                import_result, validation_result, final_format
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        ))
        self.conn.commit()

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
