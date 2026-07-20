"""album_requests CRUD, status state machine, and Replace/rescue."""
import dataclasses
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
import msgspec
import psycopg2
import psycopg2.extras


if TYPE_CHECKING:
    from lib.unfindable_detection_service import UnfindableSearchLogSignal

from lib.release_identity import ReleaseIdentity, normalize_release_id

from lib.pipeline_db.rows import AlbumRequestRow, album_request_row
from lib.pipeline_db._shared import (
    AddRequestInput,
    BACKOFF_BASE_MINUTES,
    BACKOFF_MAX_MINUTES,
    MbidCollisionError,
    RequestSpectralStateUpdate,
    SupersedeRaceError,
    _escape_like_pattern,
    validate_request_metadata_fields,
)

from lib.pipeline_db._core import _PipelineDBBase


class _RequestsMixin(_PipelineDBBase):
    """album_requests CRUD, status state machine, and Replace/rescue."""


    # --- album_requests CRUD ---

    def add_request(self, artist_name, album_title, source,
                    mb_release_id=None, mb_release_group_id=None,
                    mb_artist_id=None, discogs_release_id=None,
                    year=None, country=None, format=None,
                    source_path=None, reasoning=None,
                    status="wanted",
                    release_group_year=None,
                    is_va_compilation=False):
        """Insert one ``album_requests`` row.

        The kwargs are funnelled through the typed ``AddRequestInput`` payload
        and the INSERT column list is DERIVED from that dataclass's fields
        (their names ARE ``album_requests`` columns) — so a column present in
        the payload can never be silently dropped from the SQL, the
        ``album_title`` class of bug #382 Layer 1 targets. The
        fields-are-a-subset-of-columns invariant is held by
        ``tests/test_pipeline_db_column_contract.py``. ``created_at`` /
        ``updated_at`` are stamped here; ``is_va_compilation`` (migration 028)
        defaults FALSE and is never re-resolved by automated paths.
        """
        request = AddRequestInput(
            artist_name=artist_name, album_title=album_title, source=source,
            mb_release_id=mb_release_id, mb_release_group_id=mb_release_group_id,
            mb_artist_id=mb_artist_id, discogs_release_id=discogs_release_id,
            year=year, release_group_year=release_group_year,
            country=country, format=format, source_path=source_path,
            reasoning=reasoning, status=status,
            is_va_compilation=bool(is_va_compilation),
        )
        now = datetime.now(timezone.utc)
        columns = [f.name for f in dataclasses.fields(request)]
        values = [getattr(request, name) for name in columns]
        col_sql = ", ".join(columns + ["created_at", "updated_at"])
        placeholders = ", ".join(["%s"] * (len(columns) + 2))
        cur = self._execute(
            f"INSERT INTO album_requests ({col_sql}) "
            f"VALUES ({placeholders}) RETURNING id",
            tuple(values + [now, now]),
        )
        row = cur.fetchone()
        self.conn.commit()
        assert row is not None, "INSERT RETURNING should always return a row"
        return row["id"]


    def get_request(self, request_id: int) -> AlbumRequestRow | None:
        cur = self._execute(
            "SELECT * FROM album_requests WHERE id = %s", (request_id,)
        )
        row = cur.fetchone()
        return album_request_row(row) if row else None


    def get_pipeline_overlay(
        self, mbids: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Map mb_release_id → badge-overlay info for the browse /
        library views (#445 item 2 — formerly inline SQL in
        ``web/overlay.py::check_pipeline``).

        ``verified_lossless`` / ``provisional_lossless`` derive from the
        linked current evidence row only — a request without current
        evidence makes no identity claim. Provisional = an unverified
        install holding a lossless-source V0 anchor (the quality identity
        the badge layer renders)."""
        if not mbids:
            return {}
        placeholders = ",".join(["%s"] * len(mbids))
        cur = self._execute(
            f"SELECT r.id, r.mb_release_id, r.status, "
            f"r.search_filetype_override, r.target_format, r.min_bitrate, "
            f"COALESCE(e.verified_lossless, FALSE) AS verified_lossless, "
            f"(COALESCE(e.v0_subject, '') = 'source' "
            f" AND NOT COALESCE(e.verified_lossless, FALSE)) "
            f"AS provisional_lossless "
            f"FROM album_requests r "
            f"LEFT JOIN album_quality_evidence e "
            f"ON e.id = r.current_evidence_id "
            f"WHERE r.mb_release_id IN ({placeholders})",
            tuple(mbids),
        )
        return {
            r["mb_release_id"]: {
                "id": r["id"],
                "status": r["status"],
                "search_filetype_override": r["search_filetype_override"],
                "target_format": r["target_format"],
                "min_bitrate": r["min_bitrate"],
                "verified_lossless": r["verified_lossless"],
                "provisional_lossless": r["provisional_lossless"],
            }
            for r in cur.fetchall()
        }

    def get_request_by_mb_release_id(self, mb_release_id: str) -> AlbumRequestRow | None:
        cur = self._execute(
            "SELECT * FROM album_requests WHERE mb_release_id = %s", (mb_release_id,)
        )
        row = cur.fetchone()
        return album_request_row(row) if row else None


    def get_request_by_discogs_release_id(self, discogs_release_id: str) -> AlbumRequestRow | None:
        cur = self._execute(
            "SELECT * FROM album_requests WHERE discogs_release_id = %s", (discogs_release_id,)
        )
        row = cur.fetchone()
        return album_request_row(row) if row else None


    def get_request_by_release_id(self, release_id: object | None) -> AlbumRequestRow | None:
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


    def get_request_by_replaces_request_id(
        self, replaced_id: int
    ) -> AlbumRequestRow | None:
        """Reverse lineage lookup: find the descendant row that points at
        ``replaced_id`` via ``replaces_request_id``.

        Returns None when no descendant exists (the chain was manually
        broken via SQL despite the ``ON DELETE RESTRICT`` FK — defensive).
        The partial index ``idx_album_requests_replaces_request_id``
        (migration 023) backs this lookup.
        """
        cur = self._execute(
            "SELECT * FROM album_requests "
            "WHERE replaces_request_id = %s LIMIT 1",
            (replaced_id,),
        )
        row = cur.fetchone()
        return album_request_row(row) if row else None


    def get_oldest_request_chain_created_at(
        self, request_id: int
    ) -> datetime | None:
        """The oldest ``created_at`` across the request's replace chain,
        walking ``replaces_request_id`` back through every superseded
        ancestor. This is the earliest moment the pipeline knew of the
        release — the Jellyfin pin capture's floor when no pre-upgrade
        Jellyfin item is findable (a path-changing upgrade whose old item
        is already gone). Returns None for an unknown request id.
        """
        cur = self._execute(
            """
            WITH RECURSIVE chain AS (
                SELECT id, replaces_request_id, created_at
                FROM album_requests WHERE id = %s
                UNION ALL
                SELECT ar.id, ar.replaces_request_id, ar.created_at
                FROM album_requests ar
                JOIN chain c ON ar.id = c.replaces_request_id
            )
            SELECT MIN(created_at) AS oldest FROM chain
            """,
            (int(request_id),),
        )
        row = cur.fetchone()
        return row["oldest"] if row else None


    def list_requests_in_release_group(
        self,
        rg_id: str,
        *,
        exclude_replaced: bool = True,
        exclude_request_id: int | None = None,
    ) -> list[AlbumRequestRow]:
        """List ``album_requests`` rows in the same MB release group.

        - ``exclude_replaced=True`` (default) skips rows with
          ``status='replaced'`` so the Browse-search inverted-click picker
          only sees active rows.
        - ``exclude_request_id`` skips a specific request id when set —
          used by the picker to avoid offering "replace this row with
          itself" choices.

        Ordered by ``id DESC`` (newest first).
        """
        conditions = ["mb_release_group_id = %s"]
        params: list[object] = [rg_id]
        if exclude_replaced:
            conditions.append("status != 'replaced'")
        if exclude_request_id is not None:
            conditions.append("id != %s")
            params.append(exclude_request_id)
        sql = (
            "SELECT * FROM album_requests "
            f"WHERE {' AND '.join(conditions)} "
            "ORDER BY id DESC"
        )
        cur = self._execute(sql, tuple(params))
        return [album_request_row(r) for r in cur.fetchall()]


    def list_active_release_group_ids(self) -> set[str]:
        """Return the distinct set of ``mb_release_group_id`` values held
        by any non-replaced ``album_requests`` row.

        Used by the Browse-search Replace button to compute its enable
        state per R7: the frontend builds a Set from this list and uses
        ``set.has(row.release_group_id)`` per render. NULL RG values are
        excluded.
        """
        cur = self._execute(
            "SELECT DISTINCT mb_release_group_id FROM album_requests "
            "WHERE status != 'replaced' "
            "AND mb_release_group_id IS NOT NULL"
        )
        return {row["mb_release_group_id"] for row in cur.fetchall()}


    def list_non_replaced_requests(self) -> list[AlbumRequestRow]:
        """Return active pipeline rows for disk-coverage reconciliation."""
        cur = self._execute(
            "SELECT * FROM album_requests "
            "WHERE status != 'replaced' "
            "ORDER BY id ASC"
        )
        return [album_request_row(r) for r in cur.fetchall()]


    @staticmethod
    def _mark_request_replaced(
        cur: Any,
        request_id: int,
        expected_status: str,
        now: datetime,
    ) -> bool:
        """Canonical locked-row status CAS used only by Replace."""
        cur.execute(
            "UPDATE album_requests "
            "SET status = 'replaced', imported_path = NULL, "
            "updated_at = %s "
            "WHERE id = %s AND status = %s "
            "RETURNING id",
            (now, request_id, expected_status),
        )
        return cur.fetchone() is not None


    def supersede_request_mbid(
        self,
        old_request_id: int,
        *,
        new_mb_release_id: str,
        new_mb_release_group_id: str | None,
        new_mb_artist_id: str | None,
        new_artist_name: str,
        new_album_title: str,
        new_year: int | None,
        new_country: str | None,
        new_tracks: list[dict[str, Any]],
        new_discogs_release_id: str | None = None,
    ) -> int:
        """Atomically supersede ``old_request_id`` with a new row.

        In one ``autocommit=False`` transaction:

        1. ``SELECT ... FOR UPDATE`` on the old row (acquire row lock).
        2. ``UPDATE`` old row's ``status`` to ``'replaced'``, clear
           ``imported_path`` (R14 carve-out — Phase 4 deletes the files
           at that path so the pointer would dangle). All other columns
           on the old row stay untouched as historical truth.
        3. ``INSERT`` a new ``album_requests`` row with the target MBID,
           ``status='wanted'``, ``replaces_request_id=old_request_id``,
           and the source inherited from the old row.
           ``new_discogs_release_id`` is dual-written for the Discogs-pathway
           Replace (the new row carries both the MB and Discogs identity, as
           the add flow writes them); MB callers pass ``None``.
        4. ``INSERT`` the new row's ``album_tracks`` rows.

        Returns the new request_id.

        Raises:
            ``SupersedeRaceError``: the old row was already in
                ``status='replaced'`` (rowcount=0 on the UPDATE).
            ``MbidCollisionError``: the target MBID already exists in
                ``album_requests`` (UNIQUE violation defensively caught).
            Any other exception triggers automatic rollback and re-raises.
        """
        with self._atomic():
            now = datetime.now(timezone.utc)
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                # 1. Row lock on the old row.
                cur.execute(
                    "SELECT id, source, status FROM album_requests "
                    "WHERE id = %s FOR UPDATE",
                    (old_request_id,),
                )
                old_row = cur.fetchone()
                if old_row is None:
                    raise SupersedeRaceError(
                        f"old request {old_request_id} disappeared "
                        "between Phase 0 read and Phase 3 lock"
                    )
                old_source = old_row["source"]
                if str(old_row["status"]) == "replaced":
                    raise SupersedeRaceError(
                        f"old request {old_request_id} was already replaced"
                    )

                # 2. Flip old row's status; clear imported_path (R14).
                if not self._mark_request_replaced(
                    cur,
                    old_request_id,
                    str(old_row["status"]),
                    now,
                ):
                    raise SupersedeRaceError(
                        f"old request {old_request_id} was already "
                        "replaced (rowcount=0 on UPDATE)"
                    )

                # 3. Insert new row.
                try:
                    cur.execute(
                        """
                        INSERT INTO album_requests (
                            mb_release_id, mb_release_group_id, mb_artist_id,
                            artist_name, album_title, year, country,
                            discogs_release_id,
                            source, status, replaces_request_id,
                            created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            'wanted', %s, %s, %s
                        )
                        RETURNING id
                        """,
                        (
                            new_mb_release_id,
                            new_mb_release_group_id,
                            new_mb_artist_id,
                            new_artist_name,
                            new_album_title,
                            new_year,
                            new_country,
                            new_discogs_release_id,
                            old_source,
                            old_request_id,
                            now,
                            now,
                        ),
                    )
                except psycopg2.errors.UniqueViolation as exc:
                    raise MbidCollisionError(
                        f"target MBID {new_mb_release_id} already exists"
                    ) from exc
                row = cur.fetchone()
                assert row is not None, (
                    "INSERT RETURNING should always return a row"
                )
                new_id = int(row["id"])

                # 4. Insert tracks for the new row.
                for t in new_tracks:
                    cur.execute(
                        """
                        INSERT INTO album_tracks (
                            request_id, disc_number, track_number,
                            title, length_seconds
                        ) VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            new_id,
                            t.get("disc_number", 1),
                            t["track_number"],
                            t["title"],
                            t.get("length_seconds"),
                        ),
                    )

            self.conn.commit()
            return new_id


    def delete_request(self, request_id: int) -> None:
        # Evidence rows are content-addressed after migration 021 — they are
        # NOT deleted when the request is deleted. Addressing FKs on
        # ``album_requests`` / ``import_jobs`` / ``download_log`` are
        # ``ON DELETE SET NULL`` so the evidence survives. The mantra:
        # "evidence is never deleted unless files change."
        self._execute("DELETE FROM album_requests WHERE id = %s", (request_id,))
        self.conn.commit()


    def _update_request_metadata_cas(
        self,
        request_id: int,
        fields: dict[str, Any],
        *,
        expected_status: str | None,
        now: datetime,
    ) -> bool:
        """Apply bounded dynamic metadata without owning lifecycle fields."""
        validate_request_metadata_fields(fields)
        if not fields:
            return True
        assignments = ", ".join(
            f"{key} = populated.{key}" for key in sorted(fields)
        )
        if expected_status is not None:
            cur = self._execute(
                f"UPDATE album_requests AS ar "
                f"SET updated_at = %s, {assignments} "
                "FROM jsonb_populate_record("
                "NULL::album_requests, %s::jsonb) AS populated "
                "WHERE ar.id = %s AND ar.status != 'replaced' "
                "AND ar.status = %s",
                (
                    now,
                    psycopg2.extras.Json(
                        fields,
                        dumps=lambda value: msgspec.json.encode(value).decode(),
                    ),
                    request_id,
                    expected_status,
                ),
            )
        else:
            cur = self._execute(
                f"UPDATE album_requests AS ar "
                f"SET updated_at = %s, {assignments} "
                "FROM jsonb_populate_record("
                "NULL::album_requests, %s::jsonb) AS populated "
                "WHERE ar.id = %s AND ar.status != 'replaced'",
                (
                    now,
                    psycopg2.extras.Json(
                        fields,
                        dumps=lambda value: msgspec.json.encode(value).decode(),
                    ),
                    request_id,
                ),
            )
        return cur.rowcount > 0


    def update_request_fields(
        self,
        request_id: int,
        **extra: Any,
    ) -> bool:
        """Compare-and-set metadata without mutating lifecycle or identity.

        ``expected_status`` lets read-then-write adapters reject a concurrent
        lifecycle change instead of reporting a metadata update that matched
        no row. Callers that do not hold a source snapshot still receive the
        terminal ``replaced`` guard. Lifecycle, immutable identity, and
        dedicated audit fields are reserved for their typed writer seams.
        """
        expected_status_raw = extra.pop("expected_status", None)
        if (
            expected_status_raw is not None
            and not isinstance(expected_status_raw, str)
        ):
            raise TypeError("expected_status must be a string or None")
        expected_status = expected_status_raw
        # Validate before the empty/control-only branch too: an attempted
        # reserved write must never be mistaken for an empty CAS.
        validate_request_metadata_fields(dict(extra))
        if not extra:
            # A control-only/empty update still has a meaningful CAS result.
            # Returning True without consulting the row lets a dependent
            # adapter report success for a deleted, replaced, or stale
            # request.  Keep this branch read-only (including ``updated_at``)
            # while applying the same existence/lifecycle predicate as the
            # UPDATE below.
            if expected_status is not None:
                cur = self._execute(
                    "SELECT 1 FROM album_requests "
                    "WHERE id = %s AND status != 'replaced' AND status = %s",
                    (request_id, expected_status),
                )
            else:
                cur = self._execute(
                    "SELECT 1 FROM album_requests "
                    "WHERE id = %s AND status != 'replaced'",
                    (request_id,),
                )
            return cur.fetchone() is not None
        applied = self._update_request_metadata_cas(
            request_id,
            dict(extra),
            expected_status=expected_status,
            now=datetime.now(timezone.utc),
        )
        self.conn.commit()
        return applied


    # ---------- Unfindable detection (U13) ----------
    #
    # Three thin writers used by ``lib.unfindable_detection_service`` and
    # nothing else. Each is a single statement; the autocommit-mode
    # default of ``PipelineDB`` is the right boundary — there is no
    # cross-statement invariant to protect (cursor / cycle state is
    # explicitly NOT touched, per R20).

    def list_unfindable_probe_candidates(
        self,
        *,
        limit: int,
        probe_interval_days: int,
    ) -> list[dict[str, Any]]:
        """Return wanted-cohort members eligible for a probe right now.

        A row is eligible when:

          * ``status = 'wanted'`` (only the unfindable cohort), AND
          * ``last_artist_probe_at IS NULL`` (never probed), OR
            ``last_artist_probe_at < now() - probe_interval_days``.

        Ordered oldest-probe-first so the daily run picks up the most
        overdue members first. ``NULL`` sorts before any timestamp via
        ``NULLS FIRST`` so a freshly-added request is preferred over a
        7d-old probed row.

        Returns the minimal column set the service needs (request id,
        artist_name, current_category, prior probe count) so the
        per-row processing in the service is one DB round-trip per
        candidate at most.
        """
        if limit <= 0:
            return []
        cur = self._execute(
            """
            SELECT id, artist_name, unfindable_category,
                   last_artist_probe_at, last_artist_probe_match_count
            FROM album_requests
            WHERE status = 'wanted'
              AND (last_artist_probe_at IS NULL
                   OR last_artist_probe_at < (NOW() - %s * INTERVAL '1 day'))
            ORDER BY last_artist_probe_at NULLS FIRST, id
            LIMIT %s
            """,
            (int(probe_interval_days), int(limit)),
        )
        return [dict(r) for r in cur.fetchall()]


    def record_artist_probe(
        self,
        request_id: int,
        *,
        match_count: int,
        observed_at: datetime,
    ) -> None:
        """Persist one artist-only probe observation.

        Two columns + ``updated_at``. Deliberately separate from
        ``set_unfindable_category`` so the probe-recorded-but-
        verdict-unchanged case stays explicit in the audit trail.

        Guarded by ``status='wanted'``: detection runs the probe
        against a wanted-cohort snapshot, then writes back. If the row
        transitions out from under us mid-probe (e.g. a concurrent
        rescue via ``mark_imported_with_rescue`` flips status to
        ``imported``), this late write is a silent no-op rather than
        clobbering the rescue's audit trail. The detection service is
        exclusively for the wanted cohort by design (R20 / U13 plan).
        """
        self._execute(
            """
            UPDATE album_requests
            SET last_artist_probe_at = %s,
                last_artist_probe_match_count = %s,
                updated_at = %s
            WHERE id = %s AND status = 'wanted'
            """,
            (observed_at, int(match_count), observed_at, request_id),
        )
        self.conn.commit()


    def set_unfindable_category(
        self,
        request_id: int,
        *,
        category: str | None,
        categorised_at: datetime,
    ) -> None:
        """Write ``unfindable_category`` + ``unfindable_categorised_at``.

        ``category=None`` clears the column (re-categorisation downgrade).
        Always stamps ``unfindable_categorised_at`` so operators can
        see how fresh the categorisation is — even a clear is an
        observation worth dating.

        The DB CHECK constraint enforces the 4-category vocabulary; an
        unknown string raises ``IntegrityError`` here rather than
        silently writing garbage.

        Guarded by ``status='wanted'``: same rationale as
        ``record_artist_probe``. The detection service reads the
        wanted-cohort, probes slskd (slow), then writes a verdict back.
        If a concurrent ``mark_imported_with_rescue`` flipped the row
        to ``imported`` mid-flight, this late write must be a silent
        no-op — otherwise it would re-stamp ``unfindable_category`` and
        ``unfindable_categorised_at`` on a row that's already been
        rescued, leaving an incoherent ``status='imported' AND
        unfindable_category='…'`` audit row. The guard makes the
        lost-update race a benign no-op rather than corruption.
        """
        self._execute(
            """
            UPDATE album_requests
            SET unfindable_category = %s,
                unfindable_categorised_at = %s,
                updated_at = %s
            WHERE id = %s AND status = 'wanted'
            """,
            (category, categorised_at, categorised_at, request_id),
        )
        self.conn.commit()


    def get_unfindable_search_log_signal(
        self,
        request_id: int,
        *,
        window_days: int,
        matcher_score_threshold: float,
    ) -> "UnfindableSearchLogSignal":
        """Aggregate the search-log signal for the unfindable classifier.

        Window-bounded so historical noise doesn't pin a verdict
        forever. Computes two scalars in one pass:

          * ``zero_find_cycles`` — of the distinct
            ``plan_cycle_snapshot`` values seen for this request in the
            window, how many cycles had zero rows with
            ``outcome='found'``. Drives the
            ``album_absent_artist_present`` rule.
          * ``wrong_pressing_hits`` — count of rows with
            ``rejection_reason='strict_count_mismatch'`` AND
            ``matcher_score_top1 >= matcher_score_threshold``. Drives
            the ``wrong_pressing_available`` rule.
        """
        # Import lazily to avoid a circular import via lib.quality.
        from lib.unfindable_detection_service import UnfindableSearchLogSignal

        cur = self._execute(
            """
            WITH window_rows AS (
                SELECT *
                FROM search_log
                WHERE request_id = %s
                  AND attempt_consumed = TRUE
                  AND created_at > (NOW() - %s * INTERVAL '1 day')
            ),
            per_cycle AS (
                SELECT plan_cycle_snapshot,
                       SUM(CASE WHEN outcome = 'found' THEN 1 ELSE 0 END)
                           AS found_count
                FROM window_rows
                WHERE plan_cycle_snapshot IS NOT NULL
                GROUP BY plan_cycle_snapshot
            )
            SELECT
                (SELECT COUNT(*) FROM per_cycle WHERE found_count = 0)::int
                    AS zero_find_cycles,
                (SELECT COUNT(*) FROM window_rows
                 WHERE rejection_reason = 'strict_count_mismatch'
                   AND matcher_score_top1 IS NOT NULL
                   AND matcher_score_top1 >= %s)::int
                    AS wrong_pressing_hits
            """,
            (
                int(request_id),
                int(window_days),
                float(matcher_score_threshold),
            ),
        )
        row = cur.fetchone()
        if row is None:
            return UnfindableSearchLogSignal(
                zero_find_cycles=0,
                wrong_pressing_hits=0,
            )
        return UnfindableSearchLogSignal(
            zero_find_cycles=int(row.get("zero_find_cycles") or 0),
            wrong_pressing_hits=int(row.get("wrong_pressing_hits") or 0),
        )


    def _status_for_cas(
        self,
        request_id: int,
        expected_status: str | None,
    ) -> str | None:
        """Resolve the exact source status for a compare-and-set writer."""
        if expected_status is not None:
            return expected_status
        cur = self._execute(
            "SELECT status FROM album_requests WHERE id = %s", (request_id,)
        )
        row = cur.fetchone()
        return str(row["status"]) if row is not None else None


    def compare_request_status(
        self,
        request_id: int,
        *,
        expected_status: str,
    ) -> bool:
        """Linearizing no-op CAS for an idempotent operator command."""
        if expected_status == "replaced":
            return False
        cur = self._execute(
            "UPDATE album_requests SET status = status "
            "WHERE id = %s AND status = %s AND status != 'replaced'",
            (request_id, expected_status),
        )
        self.conn.commit()
        return cur.rowcount > 0


    def update_status(
        self,
        request_id: int,
        status: str,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool:
        if status == "replaced":
            raise ValueError(
                "status='replaced' is owned by supersede_request_mbid")
        validate_request_metadata_fields(dict(extra))
        if expected_status is None:
            observed_status = self._status_for_cas(request_id, None)
            if observed_status is None:
                return False
            return self.update_status(
                request_id,
                status,
                expected_status=observed_status,
                **extra,
            )
        if expected_status == "replaced":
            return False
        with self._atomic():
            now = datetime.now(timezone.utc)
            cur = self._execute(
                "UPDATE album_requests "
                "SET status = %s, active_download_state = NULL, "
                "updated_at = %s "
                "WHERE id = %s AND status = %s "
                "AND status != 'replaced'",
                (status, now, request_id, expected_status),
            )
            if cur.rowcount <= 0:
                self.conn.commit()
                return False
            if extra and not self._update_request_metadata_cas(
                request_id,
                dict(extra),
                expected_status=status,
                now=now,
            ):
                raise RuntimeError(
                    "status transition metadata CAS lost its owned row"
                )
            self.conn.commit()
            return True


    def mark_imported_with_rescue(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        **extra: Any,
    ) -> bool:
        """Flip ``status`` to ``'imported'`` + capture long-tail-rescue audit
        atomically. U14 / R21.

        When a request transitions to ``imported`` and its
        ``unfindable_category`` was non-NULL, this is the
        long-tail-rescue moment (the archivist frame's entire payoff —
        an "unfindable" request finally landed because a fresh peer
        appeared). Four mutations commit together OR none of them
        apply:

          1. ``status`` → ``'imported'`` + ``active_download_state``
             cleared (same shape as ``update_status``).
          2. ``rescued_at`` → ``NOW()`` (only if the row was not
             already rescued — first rescue wins).
          3. ``prior_unfindable_category`` → the cleared category
             value (only if the row was not already rescued).
          4. ``unfindable_category`` → ``NULL`` (the rescue IS the
             resolution; the category no longer applies, regardless
             of one-shot-stamp semantics).

        **One-shot capture semantics:** once ``rescued_at`` is
        populated, it is immutable. A subsequent re-import (e.g. via
        Replace → re-categorise → re-import) does NOT bump the
        timestamp nor overwrite ``prior_unfindable_category``. The
        original rescue instant is the canonical audit record;
        downstream surfaces (web UI, reports) treat it as a "rescued
        at" lineage marker, not a "last-import-touched" timestamp.
        The current ``unfindable_category`` IS still cleared on every
        call, because the rescue still IS the resolution.

        **Atomicity contract:** the static lifecycle UPDATE captures and
        clears rescue state in one compare-and-set. Optional metadata is a
        separate, bounded UPDATE in the same explicit transaction, so a
        metadata error rolls the lifecycle write back too.

        ``**extra`` mirrors ``update_status`` — additional column
        writes that ride along with the status flip (e.g.
        ``beets_distance``, ``beets_scenario``, spectral fields).
        Reserved keys (``status``, ``active_download_state``,
        ``updated_at``, the four rescue columns) are not accepted —
        they're managed by this method.
        """
        rescue_owned = {
            "unfindable_category",
            "unfindable_categorised_at",
        }
        bad_rescue_fields = sorted(set(extra) & rescue_owned)
        if bad_rescue_fields:
            raise ValueError(
                "mark_imported_with_rescue cannot accept rescue-owned fields: "
                + ", ".join(bad_rescue_fields)
            )
        validate_request_metadata_fields(dict(extra))
        if expected_status is None:
            observed_status = self._status_for_cas(request_id, None)
            if observed_status is None:
                return False
            return self.mark_imported_with_rescue(
                request_id,
                expected_status=observed_status,
                **extra,
            )
        if expected_status == "replaced":
            return False

        with self._atomic():
            now = datetime.now(timezone.utc)
            cur = self._execute(
                "UPDATE album_requests AS ar "
                "SET status = 'imported', "
                "active_download_state = NULL, "
                "updated_at = %s, "
                "rescued_at = CASE "
                "  WHEN ar.unfindable_category IS NOT NULL "
                "   AND ar.rescued_at IS NULL THEN %s "
                "  ELSE ar.rescued_at END, "
                "prior_unfindable_category = CASE "
                "  WHEN ar.unfindable_category IS NOT NULL "
                "   AND ar.rescued_at IS NULL "
                "  THEN ar.unfindable_category "
                "  ELSE ar.prior_unfindable_category END, "
                "unfindable_categorised_at = CASE "
                "  WHEN ar.unfindable_category IS NOT NULL THEN %s "
                "  ELSE ar.unfindable_categorised_at END, "
                "unfindable_category = NULL "
                "WHERE ar.id = %s AND ar.status = %s "
                "AND ar.status != 'replaced'",
                (now, now, now, request_id, expected_status),
            )
            if cur.rowcount <= 0:
                self.conn.commit()
                return False
            if extra and not self._update_request_metadata_cas(
                request_id,
                dict(extra),
                expected_status="imported",
                now=now,
            ):
                raise RuntimeError(
                    "import rescue metadata CAS lost its owned row"
                )
            self.conn.commit()
            return True


    def update_spectral_state(
        self,
        request_id: int,
        update: RequestSpectralStateUpdate,
    ) -> bool:
        """Write spectral state pairs together, including explicit NULLs."""
        return self.update_request_fields(
            request_id, **update.as_update_fields(),
        )



    def clear_on_disk_quality_fields(self, request_id: int) -> None:
        """Zero fields that describe files currently on disk in beets.

        Call this whenever an album leaves the beets library — ban-source
        followed by ``beet remove -d``, a manual ``beet rm``, etc. The
        fields cleared describe on-disk state:

        - ``verified_lossless`` (set only after a genuine FLAC→V0 chain)
        - ``current_spectral_*`` (spectral grade of files currently in
          beets)
        - ``current_evidence_id`` (content-addressed snapshot of those files)
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
                   current_evidence_id = NULL,
                   imported_path = NULL,
                   updated_at = %s
               WHERE id = %s AND status != 'replaced'""",
            (now, request_id),
        )
        self.conn.commit()


    def reset_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        clear_retry_counters: bool = True,
        **fields: Any,
    ) -> bool:
        """Reset to wanted.

        Only fields explicitly passed are updated — omitted fields are
        preserved.  Pass ``search_filetype_override=None`` to clear the column;
        omitting it leaves the existing value untouched.

        ``clear_retry_counters`` is for operator requeues that should get
        a clean slate. Automatic downloading → wanted failure paths preserve the
        counters so backoff can keep growing. Scheduler priority is based only
        on immutable ``created_at`` and is unaffected by either reset mode.

        """
        unknown = sorted(
            set(fields) - {
                "search_filetype_override",
                "min_bitrate",
                "prev_min_bitrate",
            }
        )
        if unknown:
            raise ValueError(
                "reset_to_wanted does not accept fields: "
                + ", ".join(unknown)
            )
        if expected_status is None:
            observed_status = self._status_for_cas(request_id, None)
            if observed_status is None:
                return False
            return self.reset_to_wanted(
                request_id,
                expected_status=observed_status,
                clear_retry_counters=clear_retry_counters,
                **fields,
            )
        if expected_status == "replaced":
            return False
        now = datetime.now(timezone.utc)
        override_present = "search_filetype_override" in fields
        min_bitrate_present = "min_bitrate" in fields
        prev_min_bitrate_present = "prev_min_bitrate" in fields
        cur = self._execute(
            "UPDATE album_requests "
            "SET status = 'wanted', active_download_state = NULL, "
            "updated_at = %s, "
            "search_attempts = CASE WHEN %s THEN 0 ELSE search_attempts END, "
            "download_attempts = CASE WHEN %s THEN 0 ELSE download_attempts END, "
            "validation_attempts = CASE WHEN %s THEN 0 ELSE validation_attempts END, "
            "next_retry_after = CASE WHEN %s THEN NULL ELSE next_retry_after END, "
            "last_attempt_at = CASE WHEN %s THEN NULL ELSE last_attempt_at END, "
            "prev_min_bitrate = CASE WHEN %s THEN %s "
            "WHEN %s THEN COALESCE(min_bitrate, prev_min_bitrate) "
            "ELSE prev_min_bitrate END, "
            "min_bitrate = CASE WHEN %s THEN %s ELSE min_bitrate END, "
            "search_filetype_override = CASE WHEN %s THEN %s "
            "ELSE search_filetype_override END "
            "WHERE id = %s AND status = %s AND status != 'replaced'",
            (
                now,
                clear_retry_counters,
                clear_retry_counters,
                clear_retry_counters,
                clear_retry_counters,
                clear_retry_counters,
                prev_min_bitrate_present,
                fields.get("prev_min_bitrate"),
                min_bitrate_present,
                min_bitrate_present,
                fields.get("min_bitrate"),
                override_present,
                fields.get("search_filetype_override"),
                request_id,
                expected_status,
            ),
        )
        self.conn.commit()
        return cur.rowcount > 0


    def reset_downloading_to_wanted(
        self,
        request_id: int,
        *,
        expected_status: str = "downloading",
        **fields: Any,
    ) -> bool:
        """Reset a still-downloading request to wanted.

        This is the guarded automatic failure path: stale workers must not
        requeue rows that an operator or another phase already moved elsewhere.
        Retry counters are preserved so automatic backoff keeps growing.
        """
        unknown = sorted(
            set(fields) - {
                "search_filetype_override",
                "min_bitrate",
                "prev_min_bitrate",
            }
        )
        if unknown:
            raise ValueError(
                "reset_downloading_to_wanted does not accept fields: "
                + ", ".join(unknown)
            )
        if expected_status != "downloading":
            return False
        now = datetime.now(timezone.utc)
        override_present = "search_filetype_override" in fields
        min_bitrate_present = "min_bitrate" in fields
        prev_min_bitrate_present = "prev_min_bitrate" in fields
        cur = self._execute(
            "UPDATE album_requests "
            "SET status = 'wanted', active_download_state = NULL, "
            "updated_at = %s, "
            "prev_min_bitrate = CASE WHEN %s THEN %s "
            "WHEN %s THEN COALESCE(min_bitrate, prev_min_bitrate) "
            "ELSE prev_min_bitrate END, "
            "min_bitrate = CASE WHEN %s THEN %s ELSE min_bitrate END, "
            "search_filetype_override = CASE WHEN %s THEN %s "
            "ELSE search_filetype_override END "
            "WHERE id = %s AND status = %s AND status != 'replaced'",
            (
                now,
                prev_min_bitrate_present,
                fields.get("prev_min_bitrate"),
                min_bitrate_present,
                min_bitrate_present,
                fields.get("min_bitrate"),
                override_present,
                fields.get("search_filetype_override"),
                request_id,
                expected_status,
            ),
        )
        self.conn.commit()
        return cur.rowcount > 0


    # --- Downloading state ---

    def set_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_status: str = "wanted",
    ) -> bool:
        """Set album to downloading and store the active download state.

        Only transitions from 'wanted' status. Returns True if the update
        matched (album was wanted), False if the status guard prevented it.
        """
        if expected_status != "wanted":
            return False
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            UPDATE album_requests
            SET status = 'downloading',
                active_download_state = %s::jsonb,
                last_attempt_at = %s,
                updated_at = %s
            WHERE id = %s AND status = %s AND status != 'replaced'
        """, (state_json, now, now, request_id, expected_status))
        self.conn.commit()
        return cur.rowcount > 0


    def set_downloading_if_plan_current(
        self,
        request_id: int,
        state_json: str,
        *,
        plan_id: int,
        plan_ordinal: int,
        cycle_count_snapshot: int,
    ) -> bool:
        """Atomic plan-aware ``set_downloading`` for stale-completion guard.

        Equivalent to ``set_downloading`` but additionally requires the
        request's ``active_plan_id`` / ``next_plan_ordinal`` /
        ``plan_cycle_count`` to still match the snapshot the executor
        captured at search-submit time. The single UPDATE eliminates the
        TOCTOU window between a separate currentness check and the
        wanted->downloading flip.

        Returns True iff the UPDATE matched and downloading was claimed.
        Returns False on any of: status no longer 'wanted', plan
        regenerated (active_plan_id mismatch), cursor advanced (ordinal
        mismatch), cycle bumped (cycle_count mismatch).
        """
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            UPDATE album_requests
            SET status = 'downloading',
                active_download_state = %s::jsonb,
                last_attempt_at = %s,
                updated_at = %s
            WHERE id = %s
              AND status = 'wanted'
              AND active_plan_id = %s
              AND next_plan_ordinal = %s
              AND plan_cycle_count = %s
        """, (
            state_json, now, now, request_id,
            plan_id, plan_ordinal, cycle_count_snapshot,
        ))
        self.conn.commit()
        return cur.rowcount > 0


    def update_download_state(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_status: str = "downloading",
    ) -> bool:
        """CAS active download state while the worker still owns the row."""
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            UPDATE album_requests
            SET active_download_state = %s::jsonb,
                updated_at = %s
            WHERE id = %s
              AND status = %s
              AND status != 'replaced'
        """, (state_json, now, request_id, expected_status))
        self.conn.commit()
        return cur.rowcount > 0


    def update_download_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
    ) -> bool:
        """Rewrite active_download_state only while the request is downloading."""
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            UPDATE album_requests
            SET active_download_state = %s::jsonb,
                updated_at = %s
            WHERE id = %s
              AND status = 'downloading'
        """, (state_json, now, request_id))
        self.conn.commit()
        return cur.rowcount > 0


    def update_download_state_current_path(
        self,
        request_id: int,
        current_path: str | None,
    ) -> bool:
        """Rewrite only ``active_download_state.current_path`` on downloading rows."""
        now = datetime.now(timezone.utc)
        cur = self._execute("""
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
        return cur.rowcount > 0


    def mark_import_subprocess_started(
        self,
        request_id: int,
        timestamp: str,
    ) -> bool:
        """Stamp ``active_download_state.import_subprocess_started_at``.

        Called immediately before launching ``import_one.py`` on the
        auto-import path so the resume guard can later distinguish
        "subprocess never launched" (safe to retry) from "subprocess
        may have written to beets" (manual recovery required). No-op
        when ``active_download_state`` is NULL — force-import paths
        operate on a different ownership boundary
        (``failed_imports/...``) and don't carry this state.
        See ``docs/advisory-locks.md``.
        """
        now = datetime.now(timezone.utc)
        cur = self._execute("""
            UPDATE album_requests
            SET active_download_state = jsonb_set(
                    active_download_state,
                    '{import_subprocess_started_at}',
                    to_jsonb(%s::text),
                    true
                ),
                updated_at = %s
            WHERE id = %s
              AND status = 'downloading'
              AND active_download_state IS NOT NULL
        """, (timestamp, now, request_id))
        self.conn.commit()
        return cur.rowcount > 0


    def get_downloading(self) -> list[AlbumRequestRow]:
        """Get all albums currently being downloaded."""
        cur = self._execute(
            "SELECT * FROM album_requests WHERE status = 'downloading' "
            "ORDER BY updated_at ASC"
        )
        return [album_request_row(r) for r in cur.fetchall()]


    # --- Query methods ---

    def get_wanted(self, limit=None) -> list[AlbumRequestRow]:
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
        return [album_request_row(r) for r in cur.fetchall()]


    def get_by_status(self, status, *, limit=None, newest_first=False) -> list[AlbumRequestRow]:
        """Rows in one status. ``newest_first`` orders by ``updated_at``
        DESC (recency window for the imported list, #426); ``limit``
        caps the result. Defaults preserve the original full-list shape.
        """
        order = "updated_at DESC" if newest_first else "created_at ASC"
        sql = f"SELECT * FROM album_requests WHERE status = %s ORDER BY {order}"
        params: list[object] = [status]
        if limit is not None:
            sql += " LIMIT %s"
            params.append(int(limit))
        cur = self._execute(sql, tuple(params))
        return [album_request_row(r) for r in cur.fetchall()]


    def search_requests(
        self,
        query: str,
        *,
        limit: int = 200,
        status: str | None = None,
    ) -> list[AlbumRequestRow]:
        """Operator search over artist/album (#426).

        Case-insensitive substring match with LIKE wildcards escaped, so
        ``100%`` finds the artist named ``100% Wool`` rather than
        everything. Ordered like the queue view: artist, then year.
        ``status`` narrows in SQL — filtering after the LIMIT would
        silently under-report on queries matching more rows than the cap.
        """
        q = (query or "").strip()
        if not q:
            return []
        pattern = f"%{_escape_like_pattern(q)}%"
        status_clause = ""
        params: list[object] = [pattern, pattern]
        if status is not None:
            status_clause = " AND status = %s"
            params.append(status)
        params.append(max(1, min(int(limit), 500)))
        cur = self._execute(
            "SELECT * FROM album_requests"
            " WHERE (artist_name ILIKE %s ESCAPE '\\'"
            "    OR album_title ILIKE %s ESCAPE '\\')"
            f"{status_clause}"
            " ORDER BY artist_name, year NULLS LAST, id"
            " LIMIT %s",
            tuple(params),
        )
        return [album_request_row(r) for r in cur.fetchall()]


    def count_by_status(self):
        cur = self._execute(
            "SELECT status, COUNT(*) as cnt FROM album_requests GROUP BY status"
        )
        return {r["status"]: r["cnt"] for r in cur.fetchall()}


    # --- Long-tail worklist cohort (U1) ---------------------------------
    #
    # The Long-Tail Triage Console opens on the ``wanted`` set. Both methods
    # below return the row UNbanded — banding is the beets-only concern of the
    # web layer (``compute_library_rank`` keyed by ``mb_release_id``) and lives
    # in the service's injected ``band_fn``. The DB layer's only
    # banding-adjacent responsibility is stamping ``in_flight_rescue`` via the
    # ``youtube_running`` EXISTS predicate (KTD4) — backed by the partial unique
    # index ``one_youtube_running_per_request`` (migration 037), so it probes a
    # tiny index, not a seq scan — so the service doesn't issue an N-query loop.
    #
    # Operator-facing column projection shared by the cohort + single-id reads.
    # ``ar.*`` would carry the full row, but the worklist only renders this
    # subset plus ``in_flight_rescue``; pinning the list keeps the wire payload
    # narrow and the contract explicit.
    _LONG_TAIL_SELECT = """
        SELECT
            ar.id,
            ar.artist_name,
            ar.album_title,
            ar.year,
            ar.status,
            ar.source,
            ar.mb_release_id,
            ar.mb_release_group_id,
            ar.discogs_release_id,
            ar.target_format,
            ar.min_bitrate,
            ar.search_filetype_override,
            ar.unfindable_category,
            ar.current_spectral_grade,
            ar.current_spectral_bitrate,
            (
                SELECT COUNT(*) FROM album_tracks t
                WHERE t.request_id = ar.id
            )::int AS track_count,
            EXISTS (
                SELECT 1 FROM download_log dl
                WHERE dl.request_id = ar.id
                  AND dl.source = 'youtube'
                  AND dl.outcome = 'youtube_running'
            ) AS in_flight_rescue
        FROM album_requests ar
    """

    def get_long_tail_cohort(self) -> list[dict[str, Any]]:
        """Return the full ``wanted`` cohort, each row stamped with
        ``in_flight_rescue``.

        One Postgres query regardless of cohort size. Banding happens
        downstream in the service (beets-only, batched). Ordered by id ASC for
        stable rendering. ``replaced`` / ``imported`` / ``unsearchable`` /
        ``downloading`` rows are correctly excluded (R2 — worklist is the
        ``wanted`` set only).
        """
        sql = (self._LONG_TAIL_SELECT
               + " WHERE ar.status = 'wanted' ORDER BY ar.id ASC")
        cur = self._execute(sql)
        return [dict(r) for r in cur.fetchall()]

    def get_long_tail_request(
        self, request_id: int,
    ) -> dict[str, Any] | None:
        """Return a single ``wanted`` request stamped with ``in_flight_rescue``,
        or ``None``.

        Single-id variant of ``get_long_tail_cohort`` (KTD8 / R16 — backs the
        post-action single-row refetch). Returns ``None`` when the row doesn't
        exist OR is no longer ``wanted`` (an imported / replaced row is
        correctly absent from the worklist).
        """
        sql = self._LONG_TAIL_SELECT + " WHERE ar.id = %s AND ar.status = 'wanted'"
        cur = self._execute(sql, (int(request_id),))
        row = cur.fetchone()
        return dict(row) if row else None


    def list_requests_by_artist(
        self,
        artist_name: str,
        mb_artist_id: str = "",
    ) -> list[AlbumRequestRow]:
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
        return [album_request_row(r) for r in cur.fetchall()]


    # --- Retry logic ---

    def record_attempt(
        self,
        request_id: int,
        attempt_type: str,
        *,
        expected_status: str,
    ) -> bool:
        if attempt_type not in {"search", "download", "validation"}:
            raise ValueError(f"Unknown attempt type: {attempt_type!r}")
        col = f"{attempt_type}_attempts"
        now = datetime.now(timezone.utc)

        # Counter + backoff are one CAS. A Replace that wins before this
        # statement leaves the frozen ancestor byte-identical.
        cur = self._execute(f"""
            UPDATE album_requests
            SET {col} = COALESCE({col}, 0) + 1,
                last_attempt_at = %s,
                next_retry_after = %s + (
                    LEAST(
                        %s * POWER(2, COALESCE({col}, 0)),
                        %s
                    ) * INTERVAL '1 minute'
                ),
                updated_at = %s
            WHERE id = %s
              AND status = %s
              AND status != 'replaced'
            RETURNING {col}
        """, (
            now,
            now,
            BACKOFF_BASE_MINUTES,
            BACKOFF_MAX_MINUTES,
            now,
            request_id,
            expected_status,
        ))
        row = cur.fetchone()
        self.conn.commit()
        return row is not None
