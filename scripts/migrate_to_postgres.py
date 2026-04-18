#!/usr/bin/env python3
"""One-shot migration: SQLite pipeline.db → PostgreSQL.

Reads all data from the SQLite database and inserts it into PostgreSQL.
All non-imported/non-manual statuses are mapped to 'wanted'.

Usage:
    python3 scripts/migrate_to_postgres.py /mnt/virtio/Music/pipeline.db postgresql://soularr@localhost/soularr
    python3 scripts/migrate_to_postgres.py /mnt/virtio/Music/pipeline.db postgresql://soularr@localhost/soularr --dry-run
"""

import argparse
import sqlite3
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from lib.pipeline_db import PipelineDB


STATUS_MAP = {
    "wanted": "wanted",
    "imported": "imported",
    "manual": "manual",
    # Everything else becomes wanted
}


def migrate(sqlite_path, pg_dsn, dry_run=False):
    print(f"Source: {sqlite_path}")
    print(f"Target: {pg_dsn}")

    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row

    # Read all data
    requests = [dict(r) for r in conn.execute("SELECT * FROM album_requests ORDER BY id").fetchall()]
    tracks = [dict(r) for r in conn.execute("SELECT * FROM album_tracks ORDER BY id").fetchall()]
    downloads = [dict(r) for r in conn.execute("SELECT * FROM download_log ORDER BY id").fetchall()]
    denylists = [dict(r) for r in conn.execute("SELECT * FROM source_denylist ORDER BY id").fetchall()]
    conn.close()

    print(f"\nFound: {len(requests)} requests, {len(tracks)} tracks, "
          f"{len(downloads)} download logs, {len(denylists)} denylist entries")

    # Map statuses
    status_changes = {}
    for req in requests:
        old = req["status"]
        new = STATUS_MAP.get(old, "wanted")
        if old != new:
            status_changes.setdefault(f"{old} → {new}", 0)
            status_changes[f"{old} → {new}"] += 1
        req["status"] = new

    if status_changes:
        print("\nStatus mappings:")
        for change, count in sorted(status_changes.items()):
            print(f"  {change}: {count}")

    # Map download_log outcome 'staged' → 'success'
    for dl in downloads:
        if dl.get("outcome") == "staged":
            dl["outcome"] = "success"

    if dry_run:
        print("\n[DRY RUN] Would insert all data. Exiting.")
        return

    # Connect to PostgreSQL and insert
    db = PipelineDB(pg_dsn)

    # Insert requests
    pg_conn = db.conn
    cur = pg_conn.cursor()

    # Track valid request IDs for FK filtering
    valid_ids = {req["id"] for req in requests}

    for req in requests:
        cur.execute("""
            INSERT INTO album_requests (
                id, mb_release_id, mb_release_group_id, mb_artist_id, discogs_release_id,
                artist_name, album_title, year, country, format,
                source, source_path, reasoning, status,
                search_attempts, download_attempts, validation_attempts,
                last_attempt_at, next_retry_after,
                beets_distance, beets_scenario, imported_path,
                lidarr_album_id, lidarr_artist_id,
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
        """, (
            req["id"], req["mb_release_id"], req["mb_release_group_id"],
            req["mb_artist_id"], req["discogs_release_id"],
            req["artist_name"], req["album_title"], req["year"],
            req["country"], req["format"],
            req["source"], req["source_path"], req["reasoning"], req["status"],
            req["search_attempts"], req["download_attempts"], req["validation_attempts"],
            req["last_attempt_at"], req["next_retry_after"],
            req["beets_distance"], req["beets_scenario"], req["imported_path"],
            req["lidarr_album_id"], req["lidarr_artist_id"],
            req["created_at"], req["updated_at"],
        ))

    for t in tracks:
        if t["request_id"] not in valid_ids:
            continue
        cur.execute("""
            INSERT INTO album_tracks (id, request_id, disc_number, track_number, title, length_seconds)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (t["id"], t["request_id"], t["disc_number"], t["track_number"],
              t["title"], t["length_seconds"]))

    for dl in downloads:
        if dl["request_id"] not in valid_ids:
            continue
        cur.execute("""
            INSERT INTO download_log (
                id, request_id, soulseek_username, filetype, download_path,
                beets_distance, beets_scenario, beets_detail, valid,
                outcome, staged_path, error_message, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            dl["id"], dl["request_id"], dl["soulseek_username"], dl["filetype"],
            dl["download_path"], dl["beets_distance"], dl["beets_scenario"],
            dl["beets_detail"], dl["valid"], dl["outcome"],
            dl["staged_path"], dl["error_message"], dl["created_at"],
        ))

    for d in denylists:
        if d["request_id"] not in valid_ids:
            continue
        cur.execute("""
            INSERT INTO source_denylist (id, request_id, username, reason, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (d["id"], d["request_id"], d["username"], d["reason"], d["created_at"]))

    # Sync sequences
    for table, seq in [
        ("album_requests", "album_requests_id_seq"),
        ("album_tracks", "album_tracks_id_seq"),
        ("download_log", "download_log_id_seq"),
        ("source_denylist", "source_denylist_id_seq"),
    ]:
        cur.execute(f"SELECT setval('{seq}', COALESCE((SELECT MAX(id) FROM {table}), 1))")

    pg_conn.commit()
    cur.close()

    # Verify
    counts = db.count_by_status()
    total = sum(counts.values())
    print(f"\nMigration complete: {total} requests in PostgreSQL")
    for status, count in sorted(counts.items()):
        print(f"  {status}: {count}")

    db.close()


def main():
    parser = argparse.ArgumentParser(description="Migrate pipeline DB from SQLite to PostgreSQL")
    parser.add_argument("sqlite_path", help="Path to SQLite pipeline.db")
    parser.add_argument("pg_dsn", help="PostgreSQL connection string")
    parser.add_argument("--dry-run", action="store_true", help="Preview without changes")
    args = parser.parse_args()

    migrate(args.sqlite_path, args.pg_dsn, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
