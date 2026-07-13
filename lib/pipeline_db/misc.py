"""Tracks, denylist/cooldowns, bad-audio hashes, field-resolution, triage."""
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any
import psycopg2


if TYPE_CHECKING:
    from lib.triage_service import ParsedTriageFilter

from lib.quality import (
    CooldownConfig,
    should_cooldown,
)

from lib.pipeline_db._shared import (
    BadAudioHashInput,
    BadAudioHashRow,
    ReplacedRequestMutationError,
)

from lib.pipeline_db._core import _PipelineDBBase


class _MiscMixin(_PipelineDBBase):
    """Tracks, denylist/cooldowns, bad-audio hashes, field-resolution, triage."""


    # --- slskd events cursor (issue #146 phase 1) ---

    def get_slskd_event_cursor(self) -> dict[str, Any] | None:
        """Return the single cursor row for the slskd events poller, or None."""
        cur = self._execute("""
            SELECT last_event_id, last_event_timestamp, updated_at
            FROM slskd_event_cursor
            WHERE id = 1
        """)
        row = cur.fetchone()
        return dict(row) if row else None

    def upsert_slskd_event_cursor(
        self,
        last_event_id: str,
        last_event_timestamp: str,
    ) -> None:
        """Record the newest slskd event the poller has processed.

        ``last_event_timestamp`` is the raw ISO-8601 string slskd emits
        (7-digit fractional seconds) — stored verbatim, compared in Python.
        """
        now = datetime.now(timezone.utc)
        self._execute("""
            INSERT INTO slskd_event_cursor (id, last_event_id, last_event_timestamp, updated_at)
            VALUES (1, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                last_event_id = EXCLUDED.last_event_id,
                last_event_timestamp = EXCLUDED.last_event_timestamp,
                updated_at = EXCLUDED.updated_at
        """, (last_event_id, last_event_timestamp, now))
        self.conn.commit()


    # --- Track management ---

    def set_tracks(self, request_id: int, tracks: list[dict[str, Any]]) -> None:
        """Replace a live request's tracklist without thawing an ancestor.

        The request-row lock linearizes this multi-row replacement with
        ``supersede_request_mbid``. A resolver may have started before Replace;
        once Replace wins, its late result must not mutate the frozen request's
        child rows.
        """
        with self._atomic():
            cur = self._execute(
                "SELECT status FROM album_requests WHERE id = %s FOR UPDATE",
                (request_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"request {request_id} not found")
            if row["status"] == "replaced":
                raise ReplacedRequestMutationError(request_id)

            self._execute(
                "DELETE FROM album_tracks WHERE request_id = %s",
                (request_id,),
            )
            for track in tracks:
                self._execute("""
                    INSERT INTO album_tracks (
                        request_id, disc_number, track_number, title,
                        length_seconds, track_artist
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    request_id,
                    track.get("disc_number", 1),
                    track["track_number"],
                    track["title"],
                    track.get("length_seconds"),
                    track.get("track_artist"),
                ))
            self.conn.commit()


    def get_tracks(self, request_id: int) -> list[dict[str, Any]]:
        cur = self._execute("""
            SELECT disc_number, track_number, title, length_seconds, track_artist
            FROM album_tracks
            WHERE request_id = %s
            ORDER BY disc_number, track_number
        """, (request_id,))
        return [dict(r) for r in cur.fetchall()]


    def update_track_artists(
        self, request_id, track_artists,
    ):
        """Update ``album_tracks.track_artist`` for ``request_id`` row-by-row.

        ``track_artists`` aligns with ``get_tracks`` ordering
        (``disc_number, track_number ASC``). Pass the full list — entries
        can be ``None`` for tracks the resolver couldn't extract.
        Length mismatches are tolerated:

          * fewer entries than rows: trailing rows keep their existing
            ``track_artist`` value.
          * more entries than rows: extra entries are silently dropped.

        Called by ``lib/field_resolver_service.py::apply_resolve_all_result``
        after ``set_tracks`` (which inserts ``track_artist=NULL`` for
        every row when the upstream payload didn't carry per-track
        artists) and after ``resolve_all`` (which produces the per-track
        results). The ORDER BY here MUST match ``get_tracks`` so the
        resolver's per-track output — sorted by ``(disc_number,
        track_number)`` via ``_tracks_titles_and_artists`` — lines up.
        """
        if not track_artists:
            return
        cur = self._execute(
            "SELECT id FROM album_tracks WHERE request_id = %s "
            "ORDER BY disc_number, track_number",
            (request_id,),
        )
        row_ids = [r["id"] for r in cur.fetchall()]
        for row_id, artist in zip(row_ids, track_artists):
            self._execute(
                "UPDATE album_tracks SET track_artist = %s WHERE id = %s",
                (artist, row_id),
            )


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
              AND COALESCE(beets_scenario, '') <> 'abandoned_auto_import'
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


    # --- album_request_field_resolutions (migration 030) ---

    def record_field_resolution(
        self,
        request_id: int,
        field_name: str,
        status: str,
        reason_code: str | None,
    ) -> None:
        """Persist one field-resolution attempt for ``request_id``.

        UPSERT: a fresh row carries ``attempts=1``; re-resolving the
        same ``(request_id, field_name)`` increments ``attempts`` and
        updates the status / reason / timestamp atomically. ``resolved_at``
        is bumped to NOW() on conflict so retry-window queries in U3
        see the actual last-probe time.

        Service layer in ``lib/field_resolver_service.py`` is the single
        caller; this method just writes the row. The status enum is
        enforced at the service layer, not via DB CHECK (the migration
        comments are the canonical source -- new statuses will appear as
        the system grows and shipped migrations are frozen).
        """
        self._execute(
            """
            INSERT INTO album_request_field_resolutions
                (request_id, field_name, status, reason_code)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (request_id, field_name) DO UPDATE
            SET status = EXCLUDED.status,
                reason_code = EXCLUDED.reason_code,
                attempts = album_request_field_resolutions.attempts + 1,
                resolved_at = NOW()
            """,
            (request_id, field_name, status, reason_code),
        )


    def get_field_resolution(
        self,
        request_id: int,
        field_name: str,
    ) -> dict[str, Any] | None:
        """Return the side-table row for ``(request_id, field_name)`` or None.

        Used by tests and by the U3 backfill row-selection. The row's
        ``resolved_at`` timestamp gives the caller the retry-window
        anchor; ``attempts`` is the running count.
        """
        cur = self._execute(
            """
            SELECT id, request_id, field_name, resolved_at, status,
                   reason_code, attempts
            FROM album_request_field_resolutions
            WHERE request_id = %s AND field_name = %s
            """,
            (request_id, field_name),
        )
        row = cur.fetchone()
        return dict(row) if row is not None else None


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


    # --- Triage (U15) -----------------------------------------------------
    #
    # The cohort-listing service ``lib.triage_service.list_triage`` reads
    # one bulk page query + three ``WHERE ... = ANY(%s)`` bulk queries
    # (field resolutions, search summaries, recent search_log slice).
    # These four methods are the DB-layer half. Filter parsing lives in
    # ``lib.triage_service.parse_filter`` and the parsed filter is what
    # the service passes here; this method never inspects raw strings.

    def list_triage_page(
        self,
        *,
        filter_spec: "ParsedTriageFilter",
        page_size: int,
        after_request_id: int | None,
    ) -> list[dict[str, Any]]:
        """One cohort page of ``album_requests`` rows for the triage view.

        ``filter_spec`` is the ``ParsedTriageFilter`` produced by
        ``lib.triage_service.parse_filter``. We type via ``TYPE_CHECKING``
        so static analysis can catch a caller passing a wrong shape, but
        the import stays deferred at runtime — the service owns the
        parser, the DB owns the SQL.

        Ordered by ``id`` ASC; ``after_request_id`` is the keyset cursor
        from the previous page's last row. ``page_size`` is honoured
        verbatim (caller clamps).

        SELECT lists the columns the triage service composes into
        ``RequestMeta`` + ``UnfindableState``; the field-resolutions /
        search summaries / recent search_log entries come from the
        sibling bulk getters.

        **Replaced rows are intentionally included.** Rows with
        ``status='replaced'`` are frozen audit tombstones from the
        operator's Replace action (see ``CLAUDE.md`` invariant #6).
        Including them in cohort listings lets the operator spot
        patterns across replacement history — e.g. an MBID-shape that
        keeps tripping HTTP 4xx and keeps getting replaced. The
        lineage chain (``replaces_request_id``) is read via
        ``pipeline-cli show <id>`` for per-request audit detail.
        ``tests/test_triage_service.py::test_list_includes_replaced_rows``
        pins this contract.
        """
        select_cols = (
            "ar.id, ar.artist_name, ar.album_title, ar.year, ar.status, "
            "ar.source, ar.mb_release_id, ar.discogs_release_id, "
            "ar.release_group_year, ar.is_va_compilation, ar.catalog_number, "
            "ar.failure_class, ar.search_filetype_override, "
            "ar.unfindable_category, ar.unfindable_categorised_at, "
            "ar.last_artist_probe_at, ar.last_artist_probe_match_count, "
            "ar.rescued_at, ar.prior_unfindable_category"
        )

        # filter_spec's attributes are normalised + safe-by-parse; the
        # values flow into placeholders, never string-interpolation.
        kind = getattr(filter_spec, "kind", None)
        unfindable_category = getattr(filter_spec, "unfindable_category", None)
        field_name = getattr(filter_spec, "field_name", None)
        status_code = getattr(filter_spec, "status_code", None)
        reason_code = getattr(filter_spec, "reason_code", None)

        where_clauses: list[str] = []
        params: list[Any] = []
        joins: list[str] = []

        if kind == "unfindable":
            where_clauses.append("ar.unfindable_category IS NOT NULL")
            if unfindable_category is not None:
                where_clauses.append("ar.unfindable_category = %s")
                params.append(unfindable_category)
        elif kind == "data_quality":
            # EXISTS-join — any row in the side table whose status is in
            # the unresolved-* enum qualifies. Sub-filters narrow on
            # field name, status, or reason_code. Status values come from
            # ``lib.field_resolver_service.ResolverStatus`` to avoid the
            # ``LIKE 'unresolved_%%'`` underscore-wildcard ambiguity.
            from lib.field_resolver_service import ResolverStatus
            from typing import get_args as _get_args
            unresolved_statuses = [
                s for s in _get_args(ResolverStatus)
                if s.startswith("unresolved_")
            ]
            sub = (
                "SELECT 1 FROM album_request_field_resolutions fr "
                "WHERE fr.request_id = ar.id "
                "AND fr.status = ANY(%s)"
            )
            sub_params: list[Any] = [unresolved_statuses]
            if field_name is not None:
                sub += " AND fr.field_name = %s"
                sub_params.append(field_name)
            if status_code is not None:
                sub += " AND fr.status = %s"
                sub_params.append(status_code)
            if reason_code is not None:
                sub += " AND fr.reason_code = %s"
                sub_params.append(reason_code)
            where_clauses.append(f"EXISTS ({sub})")
            params.extend(sub_params)
        elif kind == "search_not_converting":
            joins.append(
                "JOIN request_search_summary rss ON rss.request_id = ar.id"
            )
            where_clauses.append("rss.total_searches > 0")
            where_clauses.append("rss.found_count = 0")
        elif kind == "all":
            pass  # No predicate.
        else:  # pragma: no cover — parser is the gate
            raise ValueError(f"unsupported triage filter kind: {kind!r}")

        if after_request_id is not None:
            where_clauses.append("ar.id > %s")
            params.append(int(after_request_id))

        sql = f"SELECT {select_cols} FROM album_requests ar"
        if joins:
            sql += " " + " ".join(joins)
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY ar.id ASC LIMIT %s"
        params.append(int(page_size))

        cur = self._execute(sql, tuple(params))
        return [dict(r) for r in cur.fetchall()]


    def get_field_resolutions_for_requests(
        self,
        request_ids: list[int],
    ) -> dict[int, list[dict[str, Any]]]:
        """Bulk-fetch ``album_request_field_resolutions`` for a request set.

        Returns ``{request_id: [row, ...]}``. Each row dict carries the
        same fields ``get_field_resolution`` returns (id, request_id,
        field_name, resolved_at, status, reason_code, attempts).
        Requests with no resolutions are omitted from the result; the
        triage composer treats absence as an empty list.

        Ordering: ``field_name`` ASC for stable per-request rendering.
        """
        if not request_ids:
            return {}
        cur = self._execute(
            """
            SELECT id, request_id, field_name, resolved_at, status,
                   reason_code, attempts
            FROM album_request_field_resolutions
            WHERE request_id = ANY(%s)
            ORDER BY request_id, field_name
            """,
            ([int(r) for r in request_ids],),
        )
        out: dict[int, list[dict[str, Any]]] = {}
        for row in cur.fetchall():
            rid = int(row["request_id"])
            out.setdefault(rid, []).append(dict(row))
        return out
