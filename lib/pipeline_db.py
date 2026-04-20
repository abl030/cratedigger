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
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import psycopg2
import psycopg2.extras

from lib.quality import CooldownConfig, SpectralMeasurement, should_cooldown

DEFAULT_DSN = os.environ.get("PIPELINE_DB_DSN", "postgresql://cratedigger@localhost/cratedigger")

# Exponential backoff: base_minutes * 2^(attempts-1), capped at max
BACKOFF_BASE_MINUTES = 30
BACKOFF_MAX_MINUTES = 60 * 24  # 24 hours

# Advisory-lock namespace for force/manual-import concurrency protection
# (issue #92). The two-arg pg_advisory_lock takes (int4, int4); the first
# arg is a per-feature namespace constant, the second is the request_id.
# 0x46494D50 = ASCII "FIMP" — recognisable in pg_locks during debugging.
ADVISORY_LOCK_NAMESPACE_IMPORT = 0x46494D50

# Advisory-lock namespace for same-release concurrency protection
# (issue #132 P1, issue #133). ``ADVISORY_LOCK_NAMESPACE_IMPORT`` above
# serialises operations on the same *request_id* — it prevents a
# double-click on force-import from running the pipeline twice for the
# same album row. That is NOT enough to close the Palo Santo blast
# radius: the auto-import cycle and the web force-import path can each
# hold its own per-request lock while both targeting the same MBID
# (different request_id, same release). In that race,
# ``import_one.py``'s post-import ``max(post_import_ids)`` query can
# pick up the OTHER process's newly-inserted row as "the newest" and
# delete the row this process just imported. The fix is to serialise
# at the release level too: every ``dispatch_import_core`` invocation
# acquires this lock keyed on a stable hash of ``mb_release_id``.
#
# 0x52454C45 = ASCII "RELE" — recognisable alongside FIMP in pg_locks.
ADVISORY_LOCK_NAMESPACE_RELEASE = 0x52454C45


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
    the real race is how the Palo Santo 11-track edition lost every
    mp3 on disk.

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

    def get_request_by_discogs_release_id(self, discogs_release_id: str) -> dict[str, Any] | None:
        cur = self._execute(
            "SELECT * FROM album_requests WHERE discogs_release_id = %s", (discogs_release_id,)
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
            # Covers both ``mb_release_id = <UUID>`` (MB-sourced) AND
            # ``mb_release_id = <numeric>`` (legacy Discogs). ``mb_albumid``
            # on the beets side carries whichever value beets has for
            # the row; matching against ``mb_release_id`` here handles
            # both cases without needing to distinguish on the pipeline
            # side.
            clauses.append("mb_release_id = %s")
            params.append(mb_albumid)
        if discogs_albumid:
            clauses.append("discogs_release_id = %s")
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

    # -- Search log -----------------------------------------------------------

    def log_search(self, request_id: int, query: str | None = None,
                   result_count: int | None = None,
                   elapsed_s: float | None = None,
                   outcome: str = "error") -> None:
        """Record one search attempt for an album request."""
        self._execute("""
            INSERT INTO search_log (request_id, query, result_count, elapsed_s, outcome)
            VALUES (%s, %s, %s, %s, %s)
        """, (request_id, query, result_count, elapsed_s, outcome))
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
