#!/usr/bin/env python3
"""Pipeline DB — SQLite-based source of truth for the download pipeline.

Replaces the JSONL tracking files (pending-lidarr.jsonl, processed-lidarr.jsonl,
beets-validated.jsonl) with a proper database. Shared between doc1 (scripts) and
doc2 (Soularr) via virtiofs.

Usage:
    from pipeline_db import PipelineDB
    db = PipelineDB("/mnt/virtio/Music/pipeline.db")
    db.add_request(mb_release_id="...", artist_name="...", album_title="...", source="redownload")
"""

import json
import sqlite3
from datetime import datetime, timedelta, timezone

DEFAULT_DB_PATH = "/mnt/virtio/Music/pipeline.db"

# Exponential backoff: base_minutes * 2^(attempts-1), capped at max
BACKOFF_BASE_MINUTES = 30
BACKOFF_MAX_MINUTES = 60 * 24  # 24 hours

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS album_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

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
        CHECK(status IN (
            'wanted', 'searching', 'downloading', 'downloaded',
            'validating', 'staged', 'converting', 'importing', 'imported',
            'failed', 'review_needed', 'manual'
        )),

    -- Retry
    search_attempts INTEGER NOT NULL DEFAULT 0,
    download_attempts INTEGER NOT NULL DEFAULT 0,
    validation_attempts INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    next_retry_after TEXT,

    -- Import result
    beets_distance REAL,
    beets_scenario TEXT,
    imported_path TEXT,

    -- Lidarr bridge
    lidarr_album_id INTEGER,
    lidarr_artist_id INTEGER,

    -- Timestamps
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now'))
);

CREATE TABLE IF NOT EXISTS album_tracks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER NOT NULL REFERENCES album_requests(id) ON DELETE CASCADE,
    disc_number INTEGER NOT NULL DEFAULT 1,
    track_number INTEGER NOT NULL,
    title TEXT NOT NULL,
    length_seconds REAL
);

CREATE TABLE IF NOT EXISTS download_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER NOT NULL REFERENCES album_requests(id) ON DELETE CASCADE,
    soulseek_username TEXT,
    filetype TEXT,
    download_path TEXT,
    beets_distance REAL,
    beets_scenario TEXT,
    beets_detail TEXT,
    valid INTEGER,
    outcome TEXT CHECK(outcome IN ('staged', 'rejected', 'failed', 'timeout')),
    staged_path TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now'))
);

CREATE TABLE IF NOT EXISTS source_denylist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER NOT NULL REFERENCES album_requests(id) ON DELETE CASCADE,
    username TEXT NOT NULL,
    reason TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%S', 'now')),
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
    """SQLite-backed pipeline database."""

    def __init__(self, db_path=DEFAULT_DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        # Use delete journal mode (NOT WAL) — WAL uses mmap which
        # corrupts on virtiofs. Delete mode uses plain file I/O,
        # same as beets which runs fine on the same filesystem.
        self.conn.execute("PRAGMA journal_mode = DELETE")
        self.init_schema()

    def init_schema(self):
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    def close(self):
        self.conn.close()

    def _execute(self, sql, params=()):
        return self.conn.execute(sql, params)

    # --- album_requests CRUD ---

    def add_request(self, artist_name, album_title, source,
                    mb_release_id=None, mb_release_group_id=None,
                    mb_artist_id=None, discogs_release_id=None,
                    year=None, country=None, format=None,
                    source_path=None, reasoning=None,
                    lidarr_album_id=None, lidarr_artist_id=None,
                    status="wanted"):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        cur = self._execute("""
            INSERT INTO album_requests (
                mb_release_id, mb_release_group_id, mb_artist_id, discogs_release_id,
                artist_name, album_title, year, country, format,
                source, source_path, reasoning, status,
                lidarr_album_id, lidarr_artist_id,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            mb_release_id, mb_release_group_id, mb_artist_id, discogs_release_id,
            artist_name, album_title, year, country, format,
            source, source_path, reasoning, status,
            lidarr_album_id, lidarr_artist_id,
            now, now,
        ))
        self.conn.commit()
        return cur.lastrowid

    def get_request(self, request_id):
        row = self._execute(
            "SELECT * FROM album_requests WHERE id = ?", (request_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_request_by_mb_release_id(self, mb_release_id):
        row = self._execute(
            "SELECT * FROM album_requests WHERE mb_release_id = ?", (mb_release_id,)
        ).fetchone()
        return dict(row) if row else None

    def delete_request(self, request_id):
        self._execute("DELETE FROM album_requests WHERE id = ?", (request_id,))
        self.conn.commit()

    def update_status(self, request_id, status, **extra):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        sets = ["status = ?", "updated_at = ?"]
        params = [status, now]
        for key, val in extra.items():
            sets.append(f"{key} = ?")
            params.append(val)
        params.append(request_id)
        self._execute(
            f"UPDATE album_requests SET {', '.join(sets)} WHERE id = ?",
            params,
        )
        self.conn.commit()

    def reset_to_wanted(self, request_id):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        self._execute("""
            UPDATE album_requests
            SET status = 'wanted',
                search_attempts = 0,
                download_attempts = 0,
                validation_attempts = 0,
                next_retry_after = NULL,
                last_attempt_at = NULL,
                updated_at = ?
            WHERE id = ?
        """, (now, request_id))
        self.conn.commit()

    # --- Query methods ---

    def get_wanted(self, limit=None):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        sql = """
            SELECT * FROM album_requests
            WHERE status = 'wanted'
              AND (next_retry_after IS NULL OR next_retry_after <= ?)
            ORDER BY RANDOM()
        """
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = self._execute(sql, (now,)).fetchall()
        return [dict(r) for r in rows]

    def get_by_status(self, status):
        rows = self._execute(
            "SELECT * FROM album_requests WHERE status = ? ORDER BY created_at ASC",
            (status,),
        ).fetchall()
        return [dict(r) for r in rows]

    def count_by_status(self):
        rows = self._execute(
            "SELECT status, COUNT(*) as cnt FROM album_requests GROUP BY status"
        ).fetchall()
        return {r["status"]: r["cnt"] for r in rows}

    # --- Track management ---

    def set_tracks(self, request_id, tracks):
        self._execute("DELETE FROM album_tracks WHERE request_id = ?", (request_id,))
        for t in tracks:
            self._execute("""
                INSERT INTO album_tracks (request_id, disc_number, track_number, title, length_seconds)
                VALUES (?, ?, ?, ?, ?)
            """, (
                request_id,
                t.get("disc_number", 1),
                t["track_number"],
                t["title"],
                t.get("length_seconds"),
            ))
        self.conn.commit()

    def get_tracks(self, request_id):
        rows = self._execute("""
            SELECT disc_number, track_number, title, length_seconds
            FROM album_tracks
            WHERE request_id = ?
            ORDER BY disc_number, track_number
        """, (request_id,)).fetchall()
        return [dict(r) for r in rows]

    # --- Download logging ---

    def log_download(self, request_id, soulseek_username=None, filetype=None,
                     download_path=None, beets_distance=None, beets_scenario=None,
                     beets_detail=None, valid=None, outcome=None,
                     staged_path=None, error_message=None):
        self._execute("""
            INSERT INTO download_log (
                request_id, soulseek_username, filetype, download_path,
                beets_distance, beets_scenario, beets_detail, valid,
                outcome, staged_path, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            request_id, soulseek_username, filetype, download_path,
            beets_distance, beets_scenario, beets_detail, valid,
            outcome, staged_path, error_message,
        ))
        self.conn.commit()

    def get_download_history(self, request_id):
        rows = self._execute("""
            SELECT * FROM download_log
            WHERE request_id = ?
            ORDER BY id DESC
        """, (request_id,)).fetchall()
        return [dict(r) for r in rows]

    # --- Denylist ---

    def add_denylist(self, request_id, username, reason=None):
        self._execute("""
            INSERT OR IGNORE INTO source_denylist (request_id, username, reason)
            VALUES (?, ?, ?)
        """, (request_id, username, reason))
        self.conn.commit()

    def get_denylisted_users(self, request_id):
        rows = self._execute("""
            SELECT username, reason, created_at
            FROM source_denylist
            WHERE request_id = ?
            ORDER BY created_at ASC
        """, (request_id,)).fetchall()
        return [dict(r) for r in rows]

    # --- Retry logic ---

    def record_attempt(self, request_id, attempt_type):
        col = f"{attempt_type}_attempts"
        now = datetime.now(timezone.utc)
        now_str = now.strftime("%Y-%m-%dT%H:%M:%S")

        # Get current attempt count
        req = self.get_request(request_id)
        current = req[col]
        new_count = current + 1

        # Exponential backoff: base * 2^(attempts-1), capped
        backoff_minutes = min(
            BACKOFF_BASE_MINUTES * (2 ** (new_count - 1)),
            BACKOFF_MAX_MINUTES,
        )
        next_retry = (now + timedelta(minutes=backoff_minutes)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

        self._execute(f"""
            UPDATE album_requests
            SET {col} = ?,
                last_attempt_at = ?,
                next_retry_after = ?,
                updated_at = ?
            WHERE id = ?
        """, (new_count, now_str, next_retry, now_str, request_id))
        self.conn.commit()

    # --- JSONL migration ---

    def import_from_jsonl(self, jsonl_path, source="redownload", status="wanted"):
        count = 0
        with open(jsonl_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)

                mb_release_id = entry.get("mb_release_id")
                if not mb_release_id:
                    continue

                # Skip if already exists
                if self.get_request_by_mb_release_id(mb_release_id):
                    continue

                self.add_request(
                    mb_release_id=mb_release_id,
                    mb_release_group_id=entry.get("release_group_id"),
                    mb_artist_id=entry.get("artist_mb_id"),
                    artist_name=entry.get("artist", "Unknown"),
                    album_title=entry.get("album", "Unknown"),
                    source=source,
                    source_path=entry.get("source_path"),
                    reasoning=entry.get("reasoning"),
                    lidarr_album_id=entry.get("lidarr_album_id"),
                    lidarr_artist_id=entry.get("lidarr_artist_id"),
                    status=status,
                )
                count += 1
        return count
