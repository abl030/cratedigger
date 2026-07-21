"""YouTube rescue-ingest queue and album-mapping cache."""
from typing import Any, Optional
import msgspec
import psycopg2
import psycopg2.extras

from lib.import_queue import (
    IMPORT_JOB_PREVIEW_WAITING,
    IMPORT_JOB_YOUTUBE,
    ImportJob,
    validate_job_type,
    validate_payload,
)

from lib.pipeline_db._shared import (
    PersistedYoutubeRow,
    YoutubeInFlightError,
    pg_execute_values,
)

from lib.pipeline_db._core import _PipelineDBBase


# The two JSONB columns on ``youtube_album_mappings`` — every other
# ``PersistedYoutubeRow`` field is a scalar column, passed through via
# ``getattr``.
_YT_JSONB_COLUMNS = frozenset({"yt_tracks", "distances"})


class _YoutubeMixin(_PipelineDBBase):
    """YouTube rescue-ingest queue and album-mapping cache."""


    # --- YouTube rescue ingest (download_log doubles as queue + audit) ---
    #
    # The four methods below operate on ``download_log`` rows with
    # ``source='youtube'`` (migration 037). Together they implement the
    # queue contract the YT ingest worker drains:
    #
    #   * ``insert_youtube_running`` — submission-time INSERT. The partial
    #     unique index ``one_youtube_running_per_request`` (migration 037)
    #     enforces R4 idempotency at the DB layer; we catch the
    #     ``UniqueViolation`` and re-raise as ``YoutubeInFlightError`` so
    #     the service layer can map it to the ``in_flight`` outcome.
    #
    #   * ``update_youtube_terminal`` — worker-side terminal-state UPDATE.
    #     ``youtube_metadata`` uses the PG ``||`` JSONB merge operator so
    #     the worker can layer reason / stderr_excerpt / observed counts
    #     on top of the submission-time blob without re-reading it.
    #
    #   * ``claim_next_youtube_pending`` — worker-side claim transition. It
    #     adds ``worker_claimed_at`` / ``worker_id`` to ``youtube_metadata``
    #     before the worker starts yt-dlp, which lets startup recovery
    #     distinguish accepted-but-unclaimed rows from a prior worker's
    #     abandoned in-flight work.
    #
    #   * ``find_orphan_youtube_running`` — startup orphan sweep (R22).
    #     At worker startup only claimed rows are orphans (the previous
    #     worker process crashed mid-job); accepted-but-unclaimed rows
    #     remain queued. The worker transitions each orphan id via
    #     ``update_youtube_terminal(id, 'youtube_failed', {reason:
    #     'worker_interrupted', ...})``.

    _YOUTUBE_TERMINAL_OUTCOMES: frozenset[str] = frozenset({
        "youtube_success", "youtube_failed",
    })

    def insert_youtube_running(
        self,
        *,
        request_id: int,
        browse_id: str,
        audio_playlist_id: str | None,
        yt_url: str,
        expected_track_count: int,
        resolver_mapping_id: int | None = None,
        per_track_video_ids: list[str] | None = None,
    ) -> int:
        """Insert a ``download_log`` row for a YT rescue submission.

        Returns the new row's id. Raises ``YoutubeInFlightError`` if the
        partial unique index ``one_youtube_running_per_request`` (migration
        037) rejects the insert because a prior ``youtube_running`` row
        already exists for the same ``request_id`` — the caller maps this
        to the ``in_flight`` outcome.

        ``youtube_metadata`` is the submission-time blob: the worker layers
        terminal-state fields (reason, stderr_excerpt, observed counts) on
        top via ``update_youtube_terminal``.
        """
        metadata: dict[str, Any] = {
            "yt_url": yt_url,
            "browse_id": browse_id,
            "audio_playlist_id": audio_playlist_id,
            "expected_track_count": int(expected_track_count),
        }
        if resolver_mapping_id is not None:
            metadata["resolver_mapping_id"] = int(resolver_mapping_id)
        if per_track_video_ids is not None:
            metadata["per_track_video_ids"] = [
                str(video_id) for video_id in per_track_video_ids
            ]
        try:
            cur = self._execute(
                """
                INSERT INTO download_log (
                    request_id, source, outcome, youtube_metadata
                ) VALUES (%s, 'youtube', 'youtube_running', %s)
                RETURNING id
                """,
                (request_id, psycopg2.extras.Json(metadata)),
            )
        except psycopg2.errors.UniqueViolation as exc:
            # Look up the in-flight row so the caller can surface the
            # existing id in the outcome's ``detail`` field.
            existing_id: int | None = None
            try:
                lookup = self._execute(
                    """
                    SELECT id FROM download_log
                    WHERE request_id = %s
                      AND source = 'youtube'
                      AND outcome = 'youtube_running'
                    LIMIT 1
                    """,
                    (request_id,),
                )
                row = lookup.fetchone()
                if row is not None:
                    existing_id = int(row["id"])
            except Exception:
                pass
            raise YoutubeInFlightError(request_id, existing_id) from exc
        row = cur.fetchone()
        assert row is not None, "INSERT RETURNING should always return a row"
        return int(row["id"])


    def enqueue_youtube_import_and_mark_success(
        self,
        *,
        download_log_id: int,
        request_id: int,
        dedupe_key: str,
        payload: dict[str, Any],
        message: str,
        terminal_metadata: dict[str, Any],
    ) -> ImportJob:
        """Atomically hand staged YT audio to importer and mark audit success."""
        validate_job_type(IMPORT_JOB_YOUTUBE)
        payload = validate_payload(IMPORT_JOB_YOUTUBE, payload)
        with self._atomic():
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                cur.execute(
                    """
                    WITH inserted AS (
                        INSERT INTO import_jobs (
                            job_type, request_id, dedupe_key, payload, message,
                            preview_status, preview_message,
                            preview_completed_at, importable_at,
                            expected_request_status
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, NULL, NULL, NULL,
                            (SELECT status FROM album_requests WHERE id = %s)
                        )
                        ON CONFLICT (dedupe_key)
                            WHERE dedupe_key IS NOT NULL
                              AND status IN (
                                  'queued', 'running', 'recovery_required'
                              )
                        DO NOTHING
                        RETURNING *
                    )
                    SELECT inserted.*, false AS deduped
                    FROM inserted
                    UNION ALL
                    SELECT import_jobs.*, true AS deduped
                    FROM import_jobs
                    WHERE dedupe_key = %s
                      AND status IN (
                          'queued', 'running', 'recovery_required'
                      )
                      AND NOT EXISTS (SELECT 1 FROM inserted)
                    ORDER BY deduped
                    LIMIT 1
                    """,
                    (
                        IMPORT_JOB_YOUTUBE,
                        int(request_id),
                        dedupe_key,
                        psycopg2.extras.Json(payload),
                        message,
                        IMPORT_JOB_PREVIEW_WAITING,
                        int(request_id),
                        dedupe_key,
                    ),
                )
                job_row = cur.fetchone()
                if job_row is None:
                    raise RuntimeError("youtube import enqueue returned no row")
                cur.execute(
                    """
                    UPDATE download_log
                    SET outcome = 'youtube_success',
                        youtube_metadata =
                            COALESCE(youtube_metadata, '{}'::jsonb)
                            || %s::jsonb
                    WHERE id = %s
                    """,
                    (
                        psycopg2.extras.Json(terminal_metadata),
                        int(download_log_id),
                    ),
                )
            self.conn.commit()
            return ImportJob.from_row(
                dict(job_row),
                deduped=bool(job_row["deduped"]),
            )


    def update_youtube_terminal(
        self,
        download_log_id: int,
        outcome: str,
        metadata_dict: dict[str, Any],
    ) -> None:
        """Transition a ``youtube_running`` row to a terminal outcome.

        ``outcome`` MUST be ``'youtube_success'`` or ``'youtube_failed'``
        — anything else raises ``ValueError`` before touching the DB.
        ``metadata_dict`` is merged onto the existing ``youtube_metadata``
        blob via the PG ``||`` JSONB operator, so callers can add fields
        (reason, stderr_excerpt, observed_track_count, ...) without
        re-reading the submission-time payload.

        The UPDATE is intentionally not guarded by a ``WHERE
        outcome='youtube_running'`` filter — the partial unique index
        already permits at most one such row per request, and the
        operator-visible audit value of a possible double-write is the
        row's final ``updated_at`` not its source state. Keeping it
        unconditional means tests and ops queries can replay terminal-
        write paths without first re-priming the row to running.
        """
        if outcome not in self._YOUTUBE_TERMINAL_OUTCOMES:
            raise ValueError(
                f"update_youtube_terminal: outcome must be one of "
                f"{sorted(self._YOUTUBE_TERMINAL_OUTCOMES)!r}, got {outcome!r}"
            )
        self._execute(
            """
            UPDATE download_log
            SET outcome = %s,
                youtube_metadata = COALESCE(youtube_metadata, '{}'::jsonb)
                                   || %s::jsonb
            WHERE id = %s
            """,
            (outcome, psycopg2.extras.Json(metadata_dict), download_log_id),
        )


    def claim_next_youtube_pending(
        self,
        *,
        worker_id: str | None,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        """Claim the next unclaimed YT rows and return them for processing."""
        cur = self._execute(
            """
            WITH candidate AS (
                SELECT id
                FROM download_log
                WHERE source = 'youtube'
                  AND outcome = 'youtube_running'
                  AND NOT (COALESCE(youtube_metadata, '{}'::jsonb)
                           ? 'worker_claimed_at')
                ORDER BY created_at ASC, id ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE download_log dl
            SET youtube_metadata = COALESCE(dl.youtube_metadata, '{}'::jsonb)
                                   || jsonb_build_object(
                                       'worker_claimed_at', NOW(),
                                       'worker_id', %s
                                   )
            FROM candidate
            WHERE dl.id = candidate.id
            RETURNING dl.id, dl.request_id, dl.source, dl.outcome,
                      dl.youtube_metadata, dl.created_at
            """,
            (int(limit), worker_id),
        )
        return [dict(row) for row in cur.fetchall()]


    def find_orphan_youtube_running(self) -> list[int]:
        """Return ids of claimed ``youtube_running`` rows.

        Called by the worker's startup orphan sweep (R22). At startup
        time only rows with ``worker_claimed_at`` are orphans — accepted
        but unclaimed rows remain drainable across worker downtime. The
        caller iterates the returned ids and transitions each via
        ``update_youtube_terminal(id, 'youtube_failed', {reason:
        'worker_interrupted', ...})``.
        """
        cur = self._execute(
            """
            SELECT id
            FROM download_log
            WHERE source = 'youtube'
              AND outcome = 'youtube_running'
              AND COALESCE(youtube_metadata, '{}'::jsonb)
                  ? 'worker_claimed_at'
            ORDER BY created_at ASC, id ASC
            """,
        )
        return [int(row["id"]) for row in cur.fetchall()]


    def find_active_youtube_import_job(
        self,
        *,
        request_id: int,
        browse_id: str,
    ) -> ImportJob | None:
        """Return active ``youtube_import`` job for this request."""
        cur = self._execute(
            """
            SELECT *, false AS deduped
            FROM import_jobs
            WHERE job_type = %s
              AND request_id = %s
              AND status IN ('queued', 'running', 'recovery_required')
            ORDER BY id ASC
            LIMIT 1
            """,
            (IMPORT_JOB_YOUTUBE, int(request_id)),
        )
        row = cur.fetchone()
        return ImportJob.from_row(dict(row)) if row is not None else None


    def list_active_youtube_rescues(
        self,
        *,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return active YouTube rescue rows for API/operator visibility."""
        cur = self._execute(
            """
            SELECT
                dl.id AS download_log_id,
                dl.request_id,
                dl.source,
                dl.outcome,
                dl.youtube_metadata,
                dl.created_at,
                ar.artist_name,
                ar.album_title,
                ar.mb_release_id,
                ar.status AS request_status
            FROM download_log dl
            JOIN album_requests ar ON ar.id = dl.request_id
            WHERE dl.source = 'youtube'
              AND dl.outcome = 'youtube_running'
            ORDER BY dl.created_at ASC, dl.id ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        return [dict(row) for row in cur.fetchall()]


    # --- youtube_album_mappings (migration 034) ---
    #
    # The YouTube Music album resolver caches its scored matrix here:
    # one row per ``yt_browse_id`` per release-group / source pair. R14
    # (operator-triggered refresh) is satisfied by ``delete_…`` + ``upsert_…``;
    # the natural read path is "give me the full matrix" via ``get_…``.

    def get_youtube_album_mapping(
        self,
        release_group_identifier: str,
        source: str,
    ) -> Optional[list[dict[str, Any]]]:
        """Return all cached rows for the ``(release_group_identifier, source)`` pair.

        Reads from two tables: the main ``youtube_album_mappings``
        (row per YT sibling) and the ``youtube_album_empty_resolutions``
        marker (one row per ``(rg, source)`` pair whose YT search
        returned zero albums). JSONB columns are deserialised by
        psycopg2 into native Python ``list`` / ``dict``; outer rows
        are ordered by ``yt_browse_id`` ASC for deterministic output.

        Returns ``None`` when the pair has never been resolved, and an
        empty list when it has been resolved to an empty matrix (AE2 —
        an empty YT search result is persisted as the empty list).
        The distinction matters: ``[]`` means "we checked and found
        nothing" (cache HIT, the resolver short-circuits); ``None``
        means "we have no record" (cache MISS, the resolver re-polls
        YT). Previously the resolver couldn't tell the two cases apart
        and re-polled YT on every resolve for empty-search release
        groups, defeating R14.

        Implementation: if ``youtube_album_mappings`` has any rows, we
        return them. Otherwise we probe the marker table — a row there
        means "resolved-to-empty" (return ``[]``); absence means "never
        resolved" (return ``None``).
        """
        cur = self._execute(
            """
            SELECT id, release_group_identifier, source, yt_browse_id,
                   yt_audio_playlist_id, yt_url, yt_year, yt_track_count,
                   album_title, album_artist,
                   yt_tracks, distances, resolved_at
            FROM youtube_album_mappings
            WHERE release_group_identifier = %s AND source = %s
            ORDER BY yt_browse_id ASC
            """,
            (release_group_identifier, source),
        )
        rows = [dict(r) for r in cur.fetchall()]
        if rows:
            return rows
        # Check the empty-resolution marker (see
        # ``upsert_youtube_album_mapping`` — empty matrices are
        # persisted into a side table since the main table is
        # row-shaped and an empty matrix has no rows to insert).
        cur2 = self._execute(
            """
            SELECT 1 FROM youtube_album_empty_resolutions
            WHERE release_group_identifier = %s AND source = %s
            """,
            (release_group_identifier, source),
        )
        if cur2.fetchone() is not None:
            return []
        return None


    def find_youtube_album_mapping_for_release(
        self,
        *,
        source: str,
        release_id: str,
        browse_id: str,
    ) -> Optional[dict[str, Any]]:
        """Return cached YT mapping row whose distance targets one release.

        Discogs ingest uses this as the no-new-column bridge from a request's
        release id to the resolver's widened group key. The resolver stores
        Discogs matrices under the master id for normal releases and under the
        leaf id for orphan releases; the exact ``distances[].mbid`` entry is
        the stable link back to the request release.
        """
        cur = self._execute(
            """
            SELECT id, release_group_identifier, source, yt_browse_id,
                   yt_audio_playlist_id, yt_url, yt_year, yt_track_count,
                   album_title, album_artist,
                   yt_tracks, distances, resolved_at
            FROM youtube_album_mappings
            WHERE source = %s
              AND yt_browse_id = %s
              AND EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements(distances) AS dist
                  WHERE dist->>'mbid' = %s
              )
            ORDER BY resolved_at DESC, id ASC
            LIMIT 1
            """,
            (source, browse_id, str(release_id)),
        )
        row = cur.fetchone()
        return dict(row) if row is not None else None


    def upsert_youtube_album_mapping(
        self,
        release_group_identifier: str,
        source: str,
        rows: list[PersistedYoutubeRow],
    ) -> None:
        """Atomically replace the matrix for ``(release_group_identifier, source)``.

        Runs DELETE + INSERTs in a single transaction so a concurrent
        reader never observes a mid-replace partial state. Partial updates
        are not supported — refresh always replaces (R14).

        The per-row INSERT column list is DERIVED from
        ``msgspec.structs.fields(PersistedYoutubeRow)`` — a payload field
        can never silently drift from the SQL, the ``album_title`` class
        of bug migration 036 fixed (the hand-listed INSERT column list
        omitted a field ``psycopg2.extras.execute_values`` then silently
        dropped). Two outer columns (``release_group_identifier``,
        ``source``) are prepended; the two JSONB columns (``yt_tracks``,
        ``distances``) are wrapped in ``psycopg2.extras.Json`` via
        ``msgspec.to_builtins``, everything else passes through via
        ``getattr``.

        When ``rows`` is empty, also writes a marker row into
        ``youtube_album_empty_resolutions`` so the next
        ``get_youtube_album_mapping`` returns ``[]`` (cache HIT) instead
        of ``None`` (cache MISS) — the distinction that lets the
        resolver short-circuit empty-search release groups instead of
        re-polling YT every cycle. The marker is deleted when ``rows``
        is non-empty (a later resolve that found albums supersedes the
        empty flag).
        """
        field_names = [f.name for f in msgspec.structs.fields(PersistedYoutubeRow)]
        columns = ["release_group_identifier", "source", *field_names]
        col_sql = ", ".join(columns)
        with self._atomic():
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                cur.execute(
                    """
                    DELETE FROM youtube_album_mappings
                    WHERE release_group_identifier = %s AND source = %s
                    """,
                    (release_group_identifier, source),
                )
                if rows:
                    values: list[tuple[object, ...]] = []
                    for row in rows:
                        row_values: list[Any] = [
                            release_group_identifier, source,
                        ]
                        for name in field_names:
                            value = getattr(row, name)
                            if name in _YT_JSONB_COLUMNS:
                                value = psycopg2.extras.Json(
                                    msgspec.to_builtins(value))
                            row_values.append(value)
                        values.append(tuple(row_values))
                    pg_execute_values(
                        cur,
                        f"INSERT INTO youtube_album_mappings ({col_sql}) "
                        f"VALUES %s",
                        values,
                    )
                    # A non-empty resolve supersedes any prior empty marker.
                    cur.execute(
                        """
                        DELETE FROM youtube_album_empty_resolutions
                        WHERE release_group_identifier = %s AND source = %s
                        """,
                        (release_group_identifier, source),
                    )
                else:
                    # Empty resolve: stamp the marker so subsequent reads
                    # return ``[]`` (cache HIT, don't re-poll YT) instead of
                    # ``None`` (cache MISS, re-poll YT). ON CONFLICT to keep
                    # the upsert idempotent across refresh cycles.
                    cur.execute(
                        """
                        INSERT INTO youtube_album_empty_resolutions
                            (release_group_identifier, source)
                        VALUES (%s, %s)
                        ON CONFLICT (release_group_identifier, source)
                          DO UPDATE SET resolved_at = NOW()
                        """,
                        (release_group_identifier, source),
                    )
            self.conn.commit()
