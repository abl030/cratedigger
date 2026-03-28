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
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras

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
        CHECK(status IN ('wanted', 'imported', 'manual')),

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
    quality_override TEXT,
    min_bitrate INTEGER,
    prev_min_bitrate INTEGER,

    -- Lidarr bridge
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
    outcome TEXT CHECK(outcome IN ('success', 'rejected', 'failed', 'timeout')),
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
            cur.execute(
                "ALTER ROLE soularr SET idle_in_transaction_session_timeout = '60s'"
            )
            cur.execute(SCHEMA_SQL)
            for col, coltype in [
                ("bitrate", "INTEGER"),
                ("sample_rate", "INTEGER"),
                ("bit_depth", "INTEGER"),
                ("is_vbr", "BOOLEAN"),
                ("was_converted", "BOOLEAN DEFAULT FALSE"),
                ("original_filetype", "TEXT"),
            ]:
                cur.execute(f"""
                    DO $$ BEGIN
                        ALTER TABLE download_log ADD COLUMN {col} {coltype};
                    EXCEPTION WHEN duplicate_column THEN NULL;
                    END $$;
                """)
            for col, coltype in [
                ("quality_override", "TEXT"),
                ("min_bitrate", "INTEGER"),
                ("prev_min_bitrate", "INTEGER"),
            ]:
                cur.execute(f"""
                    DO $$ BEGIN
                        ALTER TABLE album_requests ADD COLUMN {col} {coltype};
                    EXCEPTION WHEN duplicate_column THEN NULL;
                    END $$;
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
                    lidarr_album_id=None, lidarr_artist_id=None,
                    status="wanted"):
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            INSERT INTO album_requests (
                mb_release_id, mb_release_group_id, mb_artist_id, discogs_release_id,
                artist_name, album_title, year, country, format,
                source, source_path, reasoning, status,
                lidarr_album_id, lidarr_artist_id,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            mb_release_id, mb_release_group_id, mb_artist_id, discogs_release_id,
            artist_name, album_title, year, country, format,
            source, source_path, reasoning, status,
            lidarr_album_id, lidarr_artist_id,
            now, now,
        ))
        row = cur.fetchone()
        self.conn.commit()
        return row["id"]

    def get_request(self, request_id):
        cur = self._execute(
            "SELECT * FROM album_requests WHERE id = %s", (request_id,)
        )
        return dict(cur.fetchone()) if cur.rowcount else None

    def get_request_by_mb_release_id(self, mb_release_id):
        cur = self._execute(
            "SELECT * FROM album_requests WHERE mb_release_id = %s", (mb_release_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def delete_request(self, request_id):
        self._execute("DELETE FROM album_requests WHERE id = %s", (request_id,))
        self.conn.commit()

    def update_status(self, request_id, status, **extra):
        now = datetime.now(timezone.utc)
        sets = ["status = %s", "updated_at = %s"]
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

    def reset_to_wanted(self, request_id, quality_override=None, min_bitrate=None):
        now = datetime.now(timezone.utc)
        if min_bitrate is not None:
            # Upgrading: save current as prev, set new
            self._execute("""
                UPDATE album_requests
                SET status = 'wanted',
                    search_attempts = 0,
                    download_attempts = 0,
                    validation_attempts = 0,
                    next_retry_after = NULL,
                    last_attempt_at = NULL,
                    quality_override = %s,
                    prev_min_bitrate = COALESCE(min_bitrate, prev_min_bitrate),
                    min_bitrate = %s,
                    updated_at = %s
                WHERE id = %s
            """, (quality_override, min_bitrate, now, request_id))
        else:
            # No new bitrate info — keep existing values
            self._execute("""
                UPDATE album_requests
                SET status = 'wanted',
                    search_attempts = 0,
                    download_attempts = 0,
                    validation_attempts = 0,
                    next_retry_after = NULL,
                    last_attempt_at = NULL,
                    quality_override = %s,
                    updated_at = %s
                WHERE id = %s
            """, (quality_override, now, request_id))
        self.conn.commit()

    # --- Query methods ---

    def get_wanted(self, limit=None):
        now = datetime.now(timezone.utc)
        sql = """
            SELECT * FROM album_requests
            WHERE status = 'wanted'
              AND (next_retry_after IS NULL OR next_retry_after <= %s)
            ORDER BY RANDOM()
        """
        if limit:
            sql += f" LIMIT {int(limit)}"
        cur = self._execute(sql, (now,))
        return [dict(r) for r in cur.fetchall()]

    def get_log(self, limit=50):
        """Get recent download_log entries joined with album_requests."""
        cur = self._execute("""
            SELECT dl.*,
                   ar.album_title, ar.artist_name, ar.mb_release_id,
                   ar.year, ar.country, ar.status AS request_status,
                   ar.min_bitrate AS request_min_bitrate,
                   ar.prev_min_bitrate, ar.quality_override, ar.source
            FROM download_log dl
            JOIN album_requests ar ON dl.request_id = ar.id
            ORDER BY dl.created_at DESC
            LIMIT %s
        """, (limit,))
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
                     is_vbr=None, was_converted=None, original_filetype=None):
        self._execute("""
            INSERT INTO download_log (
                request_id, soulseek_username, filetype, download_path,
                beets_distance, beets_scenario, beets_detail, valid,
                outcome, staged_path, error_message,
                bitrate, sample_rate, bit_depth, is_vbr,
                was_converted, original_filetype
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            request_id, soulseek_username, filetype, download_path,
            beets_distance, beets_scenario, beets_detail, valid,
            outcome, staged_path, error_message,
            bitrate, sample_rate, bit_depth, is_vbr,
            was_converted, original_filetype,
        ))
        self.conn.commit()

    def get_download_history(self, request_id):
        cur = self._execute("""
            SELECT * FROM download_log
            WHERE request_id = %s
            ORDER BY id DESC
        """, (request_id,))
        return [dict(r) for r in cur.fetchall()]

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

        # Get current attempt count
        req = self.get_request(request_id)
        current = req[col]
        new_count = current + 1

        # Exponential backoff: base * 2^(attempts-1), capped
        backoff_minutes = min(
            BACKOFF_BASE_MINUTES * (2 ** (new_count - 1)),
            BACKOFF_MAX_MINUTES,
        )
        next_retry = now + timedelta(minutes=backoff_minutes)

        self._execute(f"""
            UPDATE album_requests
            SET {col} = %s,
                last_attempt_at = %s,
                next_retry_after = %s,
                updated_at = %s
            WHERE id = %s
        """, (new_count, now, next_retry, now, request_id))
        self.conn.commit()
