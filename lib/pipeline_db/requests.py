"""album_requests CRUD, status state machine, and Replace/rescue."""
import dataclasses
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TypedDict

import msgspec
import psycopg2
import psycopg2.extras

if TYPE_CHECKING:
    from lib.unfindable_detection_service import UnfindableSearchLogSignal

from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_LOCAL,
    IMPORT_JOB_YOUTUBE,
)
from lib.json_narrow import is_object_list, is_str_object_dict
from lib.pipeline_db._core import _PipelineDBBase
from lib.pipeline_db._shared import (
    ACQUISITION_REQUEST_STATUSES,
    BACKOFF_BASE_MINUTES,
    BACKOFF_MAX_MINUTES,
    REQUEST_PRESENTATION_FROM,
    REQUEST_PRESENTATION_SELECT,
    REQUEST_STATUS_PROCESSING,
    AddRequestInput,
    MbidCollisionError,
    MergeRekeyCollision,
    SupersedeRaceError,
    _escape_like_pattern,
    _msgspec_json_dumps,
    processing_owner_payload,
    request_presentation_row,
    validate_request_metadata_fields,
)
from lib.pipeline_db.decisions import SEARCH_BACKOFF_MAX_EXPONENT
from lib.pipeline_db.rows import (
    AlbumRequestPresentationRow,
    AlbumRequestRow,
    ArtistRequestRow,
    album_request_row,
)
from lib.release_identity import (
    ReleaseIdentity,
    exact_request_evidence_identity_matches,
    frontend_release_id,
    normalize_release_id,
)

#: What ``has_captured_history`` accepts as proof the album was once
#: acquired: a ``download_log`` outcome, or a completed ``import_jobs`` row
#: of one of these types. Two independent vocabularies, deliberately
#: separate constants — migration 080 retired ``manual_import`` as a
#: job_type while leaving the download_log outcome of the same name
#: historically valid, so the two lists are NOT interchangeable. Both are
#: exported so the in-memory twin stops hand-copying them; the SQL below
#: keeps its literals, and ``TestSharedOutcomeVocabularies`` binds each
#: list to the real query by round-tripping every canonical member through
#: PostgreSQL.
CAPTURE_DOWNLOAD_OUTCOMES: tuple[str, ...] = (
    "success", "force_import", "manual_import", "local_import",
)
CAPTURE_IMPORT_JOB_TYPES: tuple[str, ...] = (
    IMPORT_JOB_AUTOMATION, IMPORT_JOB_FORCE, IMPORT_JOB_YOUTUBE,
    IMPORT_JOB_LOCAL,
)

_CAPTURE_AND_EVIDENCE_SELECT = """
    (
        request_row.status = 'imported'
        OR EXISTS (
            SELECT 1
            FROM download_log capture_download
            WHERE capture_download.request_id = request_row.id
              AND capture_download.outcome IN (
                  'success', 'force_import', 'manual_import', 'local_import'
              )
        )
        OR EXISTS (
            SELECT 1
            FROM import_jobs capture_job
            WHERE capture_job.request_id = request_row.id
              AND capture_job.status = 'completed'
              AND capture_job.job_type IN (
                  'automation_import', 'force_import', 'youtube_import',
                  'local_import'
              )
        )
    ) AS has_captured_history,
    COALESCE(current_evidence.verified_lossless, FALSE)
        AS _linked_verified_lossless,
    current_evidence.mb_release_id
        AS _linked_evidence_release_id,
    (
        COALESCE(current_evidence.v0_subject, '') = 'source'
        AND NOT COALESCE(current_evidence.verified_lossless, FALSE)
    ) AS provisional_lossless
"""


def _overlay_release_id_sets(
    release_ids: list[str],
) -> tuple[list[str], list[str]]:
    """Partition exact browse identities by their authoritative column."""
    musicbrainz_ids: set[str] = set()
    discogs_ids: set[str] = set()
    for raw_release_id in release_ids:
        normalized = normalize_release_id(raw_release_id)
        if not normalized:
            continue
        identity = ReleaseIdentity.from_id(normalized)
        if identity is not None and identity.source == "discogs":
            discogs_ids.add(identity.release_id)
            # Legacy Discogs requests predate the dedicated column and store
            # the numeric exact identity in mb_release_id. Query both, with
            # the row projector below preferring a dedicated-column match.
            musicbrainz_ids.add(identity.release_id)
        else:
            # Unknown synthetic IDs retain the historical MB-column fallback.
            musicbrainz_ids.add(normalized)
    return sorted(musicbrainz_ids), sorted(discogs_ids)


def _linked_current_evidence_facts(
    raw: Mapping[str, object],
) -> tuple[bool, bool]:
    """Gate every current-evidence fact on the request's exact pressing."""
    if not exact_request_evidence_identity_matches(
        raw.get("mb_release_id"),
        raw.get("discogs_release_id"),
        raw.get("_linked_evidence_release_id"),
    ):
        return False, False
    verified = raw["_linked_verified_lossless"]
    provisional = raw["provisional_lossless"]
    if not isinstance(verified, bool) or not isinstance(provisional, bool):
        raise TypeError("linked current-evidence scalar facts must be bool")
    return verified, provisional


def _overlay_row_release_id(row: Mapping[str, object]) -> str:
    """Return the exact identity key for one matched request row."""
    release_id = frontend_release_id(
        row.get("mb_release_id"),
        row.get("discogs_release_id"),
    )
    if release_id:
        return release_id
    # Existing tests and manually seeded rows use non-UUID MB identifiers.
    fallback = normalize_release_id(row.get("mb_release_id"))
    if fallback:
        return fallback
    raise ValueError("pipeline overlay row has no exact release identity")


def collect_pipeline_overlays(
    rows: Iterable[Mapping[str, object]],
) -> dict[str, dict[str, object]]:
    """Project matched request rows into the browse badge-overlay map.

    ``rows`` carries the ``_CAPTURE_AND_EVIDENCE_SELECT`` aliases plus the
    processing-owner join. One request identity may match several rows —
    a modern Discogs row and its legacy twin that stores the same numeric
    id in ``mb_release_id`` — so the dedicated-column match wins the key.

    ``tests/fakes/pipeline_db/requests.py`` builds the same raw rows and calls this,
    so the two surfaces cannot disagree on precedence (issue #1278 item 7).
    """
    overlays: dict[str, dict[str, object]] = {}
    for row in rows:
        release_id = _overlay_row_release_id(row)
        verified, provisional = _linked_current_evidence_facts(row)
        projected: dict[str, object] = {
            "id": row["id"],
            "status": row["status"],
            "search_filetype_override": row["search_filetype_override"],
            "target_format": row["target_format"],
            "min_bitrate": row["min_bitrate"],
            "has_captured_history": row["has_captured_history"],
            "verified_lossless": verified,
            "provisional_lossless": provisional,
            "processing_owner": processing_owner_payload(row),
        }
        existing = overlays.get(release_id)
        dedicated_discogs_match = normalize_release_id(
            row["discogs_release_id"]
        ) == release_id
        if existing is None or dedicated_discogs_match:
            overlays[release_id] = projected
    return overlays


class AcquisitionPayload(TypedDict):
    acquisition: list[AlbumRequestPresentationRow]
    youtube_ingest: list[dict[str, object]]


class _RequestsMixin(_PipelineDBBase):
    """album_requests CRUD, status state machine, and Replace/rescue."""


    # --- album_requests CRUD ---

    @staticmethod
    def _request_presentation_row(
        raw: Mapping[str, object],
    ) -> AlbumRequestPresentationRow:
        """Validate a request row and attach its exact owner projection.

        Thin delegate to the shared adapter (``lib.pipeline_db._shared
        .request_presentation_row``, issue #1355 item 3) so every reader of
        a ``REQUEST_PRESENTATION_SELECT`` row — this mixin's own queries and
        ``_TransactionalTransitionsDB.get_request`` — shares one
        implementation.
        """
        return request_presentation_row(raw)

    @classmethod
    def _artist_request_row(
        cls,
        raw: Mapping[str, object],
    ) -> ArtistRequestRow:
        """Validate one artist-view request and its specialized facts."""
        row = cls._request_presentation_row(raw)
        verified, provisional = _linked_current_evidence_facts(raw)
        return msgspec.convert(
            {
                **row,
                "has_captured_history": raw["has_captured_history"],
                "verified_lossless": verified,
                "provisional_lossless": provisional,
            },
            type=ArtistRequestRow,
        )

    def add_request(
        self,
        artist_name: str,
        album_title: str,
        source: str,
        mb_release_id: str | None = None,
        mb_release_group_id: str | None = None,
        mb_artist_id: str | None = None,
        discogs_release_id: str | None = None,
        year: int | None = None,
        country: str | None = None,
        format: str | None = None,
        source_path: str | None = None,
        reasoning: str | None = None,
        status: str = "wanted",
        release_group_year: int | None = None,
        is_va_compilation: bool = False,
    ) -> int:
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
        if status == REQUEST_STATUS_PROCESSING:
            raise ValueError(
                "processing requests require an exact automation owner")
        request = AddRequestInput(
            artist_name=artist_name, album_title=album_title, source=source,
            mb_release_id=mb_release_id, mb_release_group_id=mb_release_group_id,
            mb_artist_id=mb_artist_id, discogs_release_id=discogs_release_id,
            year=year, release_group_year=release_group_year,
            country=country, format=format, source_path=source_path,
            reasoning=reasoning, status=status,
            is_va_compilation=bool(is_va_compilation),
        )
        now = datetime.now(UTC)
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
        return int(row["id"])


    def get_request(self, request_id: int) -> AlbumRequestRow | None:
        cur = self._execute(
            f"""
            SELECT {REQUEST_PRESENTATION_SELECT}
            {REQUEST_PRESENTATION_FROM}
            WHERE request_row.id = %s
            """,
            (request_id,),
        )
        row = cur.fetchone()
        return self._request_presentation_row(row) if row else None


    def get_pipeline_overlay(
        self, mbids: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Map exact MB/Discogs release ID → browse badge-overlay info.

        This is the library/browse seam from #445 item 2 (formerly inline SQL in
        ``web/overlay.py::check_pipeline``).

        ``verified_lossless`` / ``provisional_lossless`` derive from the
        linked current evidence row only — a request without current
        evidence makes no identity claim. Provisional = an unverified
        install holding a lossless-source V0 anchor (the quality identity
        the badge layer renders)."""
        if not mbids:
            return {}
        musicbrainz_ids, discogs_ids = _overlay_release_id_sets(mbids)
        if not musicbrainz_ids and not discogs_ids:
            return {}
        cur = self._execute(
            f"""
            SELECT request_row.id, request_row.mb_release_id,
                   request_row.discogs_release_id,
                   request_row.status,
                   request_row.active_automation_import_job_id,
                   request_row.search_filetype_override,
                   request_row.target_format, request_row.min_bitrate,
                   {_CAPTURE_AND_EVIDENCE_SELECT},
                   processing_owner_job.id AS _processing_owner_job_id,
                   processing_owner_job.status AS _processing_owner_status,
                   processing_owner_job.preview_status
                       AS _processing_owner_preview_status
            FROM album_requests request_row
            LEFT JOIN album_quality_evidence current_evidence
              ON current_evidence.id = request_row.current_evidence_id
            LEFT JOIN import_jobs processing_owner_job
              ON processing_owner_job.id =
                 request_row.active_automation_import_job_id
            WHERE request_row.mb_release_id = ANY(%s)
               OR request_row.discogs_release_id = ANY(%s)
            """,
            (musicbrainz_ids, discogs_ids),
        )
        return collect_pipeline_overlays(cur.fetchall())

    def list_library_request_candidates(
        self,
        release_ids: list[str],
    ) -> list[ArtistRequestRow]:
        """Return every strict request candidate for exact library IDs.

        This read intentionally preserves cardinality.  A library album may
        match multiple historical request rows (including duplicate modern
        Discogs rows or modern plus legacy Discogs storage), and its caller
        must fail closed instead of accepting a ``fetchone``/dict winner.
        """
        requested_identities = {
            identity.key
            for release_id in release_ids
            if (identity := ReleaseIdentity.from_id(release_id)) is not None
        }
        if not requested_identities:
            return []
        musicbrainz_ids, discogs_ids = _overlay_release_id_sets(release_ids)
        cur = self._execute(
            f"""
            SELECT {REQUEST_PRESENTATION_SELECT},
                   {_CAPTURE_AND_EVIDENCE_SELECT}
            {REQUEST_PRESENTATION_FROM}
            LEFT JOIN album_quality_evidence current_evidence
              ON current_evidence.id = request_row.current_evidence_id
            WHERE request_row.mb_release_id = ANY(%s)
               OR request_row.discogs_release_id = ANY(%s)
            ORDER BY request_row.id
            """,
            (musicbrainz_ids, discogs_ids),
        )
        candidates: list[ArtistRequestRow] = []
        for raw in cur.fetchall():
            identity = ReleaseIdentity.from_strict_fields(
                raw.get("mb_release_id"),
                raw.get("discogs_release_id"),
            )
            if identity is not None and identity.key in requested_identities:
                candidates.append(self._artist_request_row(raw))
        return candidates

    def get_request_by_mb_release_id(self, mb_release_id: str) -> AlbumRequestRow | None:
        cur = self._execute(
            f"""
            SELECT {REQUEST_PRESENTATION_SELECT}
            {REQUEST_PRESENTATION_FROM}
            WHERE request_row.mb_release_id = %s
            """,
            (mb_release_id,),
        )
        row = cur.fetchone()
        return self._request_presentation_row(row) if row else None


    def get_request_by_discogs_release_id(self, discogs_release_id: str) -> AlbumRequestRow | None:
        cur = self._execute(
            f"""
            SELECT {REQUEST_PRESENTATION_SELECT}
            {REQUEST_PRESENTATION_FROM}
            WHERE request_row.discogs_release_id = %s
            """,
            (discogs_release_id,),
        )
        row = cur.fetchone()
        return self._request_presentation_row(row) if row else None


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
            f"""
            SELECT {REQUEST_PRESENTATION_SELECT}
            {REQUEST_PRESENTATION_FROM}
            WHERE request_row.replaces_request_id = %s
            LIMIT 1
            """,
            (replaced_id,),
        )
        row = cur.fetchone()
        return self._request_presentation_row(row) if row else None


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
        conditions = ["request_row.mb_release_group_id = %s"]
        params: list[object] = [rg_id]
        if exclude_replaced:
            conditions.append("request_row.status != 'replaced'")
        if exclude_request_id is not None:
            conditions.append("request_row.id != %s")
            params.append(exclude_request_id)
        sql = f"""
            SELECT {REQUEST_PRESENTATION_SELECT}
            {REQUEST_PRESENTATION_FROM}
            WHERE {" AND ".join(conditions)}
            ORDER BY request_row.id DESC
        """
        cur = self._execute(sql, tuple(params))
        return [self._request_presentation_row(r) for r in cur.fetchall()]


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
        cur = self._execute(f"""
            SELECT {REQUEST_PRESENTATION_SELECT}
            {REQUEST_PRESENTATION_FROM}
            WHERE request_row.status != 'replaced'
            ORDER BY request_row.id ASC
        """)
        return [self._request_presentation_row(r) for r in cur.fetchall()]


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
            "SET status = 'replaced', updated_at = %s "
            "WHERE id = %s AND status = %s "
            "AND active_automation_import_job_id IS NULL "
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
        2. ``UPDATE`` old row's ``status`` to ``'replaced'``. All other
           columns on the old row stay untouched as historical truth.
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
            now = datetime.now(UTC)
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                # 1. Row lock on the old row.
                cur.execute(
                    "SELECT id, source, status, "
                    "active_automation_import_job_id "
                    "FROM album_requests "
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
                if old_row["active_automation_import_job_id"] is not None:
                    raise SupersedeRaceError(
                        f"old request {old_request_id} gained a processing "
                        "owner before supersede"
                    )

                # 2. Flip the old row's status.
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


    def delete_request(self, request_id: int) -> bool:
        # Evidence rows are content-addressed after migration 021 — they are
        # NOT deleted when the request is deleted. Addressing FKs on
        # ``album_requests`` / ``import_jobs`` / ``download_log`` are
        # ``ON DELETE SET NULL`` so the evidence survives. The mantra:
        # "evidence is never deleted unless files change."
        cur = self._execute(
            "DELETE FROM album_requests "
            "WHERE id = %s AND active_automation_import_job_id IS NULL",
            (request_id,),
        )
        self.conn.commit()
        return cur.rowcount > 0


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
                "AND ar.status = %s "
                "AND ar.active_automation_import_job_id IS NULL",
                (
                    now,
                    psycopg2.extras.Json(
                        fields,
                        dumps=_msgspec_json_dumps,
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
                "WHERE ar.id = %s AND ar.status != 'replaced' "
                "AND ar.active_automation_import_job_id IS NULL",
                (
                    now,
                    psycopg2.extras.Json(
                        fields,
                        dumps=_msgspec_json_dumps,
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
                    "WHERE id = %s AND status != 'replaced' AND status = %s "
                    "AND active_automation_import_job_id IS NULL",
                    (request_id, expected_status),
                )
            else:
                cur = self._execute(
                    "SELECT 1 FROM album_requests "
                    "WHERE id = %s AND status != 'replaced' "
                    "AND active_automation_import_job_id IS NULL",
                    (request_id,),
                )
            return cur.fetchone() is not None
        applied = self._update_request_metadata_cas(
            request_id,
            dict(extra),
            expected_status=expected_status,
            now=datetime.now(UTC),
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
            "WHERE id = %s AND status = %s AND status != 'replaced' "
            "AND active_automation_import_job_id IS NULL",
            (request_id, expected_status),
        )
        self.conn.commit()
        return cur.rowcount > 0


    def request_marked_incomplete(self, request_id: int) -> bool:
        """Whether the operator's incomplete mark is set (issue #1241).

        A deliberately narrow scalar read for the dispatch decision path —
        never the presentation projection, which joins the processing owner
        and refuses inconsistent mid-transition worlds the importer's own
        fences handle separately. A missing row reads as unmarked.
        """
        cur = self._execute(
            "SELECT marked_incomplete_at FROM album_requests WHERE id = %s",
            (int(request_id),),
        )
        row = cur.fetchone()
        return bool(
            row is not None and row["marked_incomplete_at"] is not None
        )

    def set_marked_incomplete(self, request_id: int, *, marked: bool) -> str:
        """Atomically set/clear the operator's incomplete mark (issue #1241).

        ``album_requests.marked_incomplete_at`` is operator-owned: NULL means
        unmarked; a timestamp records when the operator asserted the
        installed copy is missing declared program. Returns one of
        ``marked`` / ``cleared`` / ``already_marked`` / ``already_clear`` /
        ``not_found`` / ``replaced`` — the idempotent no-ops are distinct
        outcomes so both operator surfaces can echo them honestly.
        ``replaced`` rows are frozen audit and refuse the write.
        """
        with self._atomic():
            cur = self._execute(
                "SELECT status, marked_incomplete_at FROM album_requests "
                "WHERE id = %s FOR UPDATE",
                (int(request_id),),
            )
            row = cur.fetchone()
            if row is None:
                self.conn.commit()
                return "not_found"
            if row["status"] == "replaced":
                self.conn.commit()
                return "replaced"
            current = row["marked_incomplete_at"]
            if marked and current is not None:
                self.conn.commit()
                return "already_marked"
            if not marked and current is None:
                self.conn.commit()
                return "already_clear"
            now = datetime.now(UTC)
            # The locked SELECT above already proved the row is live; the
            # WHERE guard restates it so the write itself carries the
            # not-replaced proof (tests/test_replaced_write_audit.py).
            self._execute(
                "UPDATE album_requests "
                "SET marked_incomplete_at = %s, updated_at = %s "
                "WHERE id = %s AND status != 'replaced'",
                (now if marked else None, now, int(request_id)),
            )
            self.conn.commit()
            return "marked" if marked else "cleared"

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
        if status == REQUEST_STATUS_PROCESSING:
            raise ValueError(
                "status='processing' is owned by automation handoff")
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
            now = datetime.now(UTC)
            cur = self._execute(
                "UPDATE album_requests "
                "SET status = %s, active_download_state = NULL, "
                "updated_at = %s "
                "WHERE id = %s AND status = %s "
                "AND status != 'replaced' "
                "AND active_automation_import_job_id IS NULL",
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
            now = datetime.now(UTC)
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
                "AND ar.status != 'replaced' "
                "AND ar.active_automation_import_job_id IS NULL",
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


    def clear_on_disk_quality_fields(self, request_id: int) -> None:
        """Zero fields that describe files currently on disk in beets.

        Call this whenever an album leaves the beets library — ban-source
        followed by ``beet remove -d``, a manual ``beet rm``, etc. The
        fields cleared describe on-disk state:

        - ``verified_lossless`` (set only after a genuine FLAC→V0 chain)
        - ``current_spectral_*`` (spectral grade of files currently in
          beets)
        - ``current_evidence_id`` (content-addressed snapshot of those files)
        ``min_bitrate`` and ``prev_min_bitrate`` are preserved deliberately
        — they still act as a conservative baseline for the next quality-
        gate comparison. ``last_download_spectral_*`` is also preserved:
        that's an audit field describing the most recent download attempt,
        independent of whether the result made it onto disk.
        """
        now = datetime.now(UTC)
        self._execute(
            """UPDATE album_requests SET
                   verified_lossless = FALSE,
                   current_spectral_grade = NULL,
                   current_spectral_bitrate = NULL,
                   current_lossless_source_v0_probe_min_bitrate = NULL,
                   current_lossless_source_v0_probe_avg_bitrate = NULL,
                   current_lossless_source_v0_probe_median_bitrate = NULL,
                   current_evidence_id = NULL,
                   updated_at = %s
               WHERE id = %s
                 AND status != 'replaced'
                 AND active_automation_import_job_id IS NULL""",
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
        counters so backoff can keep growing. Ordinary resets preserve both
        immutable ``created_at`` and nullable ``priority_started_at``; the Bad
        Rip transition explicitly stamps the latter.

        """
        unknown = sorted(
            set(fields) - {
                "search_filetype_override",
                "min_bitrate",
                "prev_min_bitrate",
                "priority_started_at",
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
        now = datetime.now(UTC)
        override_present = "search_filetype_override" in fields
        min_bitrate_present = "min_bitrate" in fields
        prev_min_bitrate_present = "prev_min_bitrate" in fields
        priority_started_at_present = "priority_started_at" in fields
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
            "ELSE search_filetype_override END, "
            "priority_started_at = CASE WHEN %s THEN %s "
            "ELSE priority_started_at END "
            "WHERE id = %s AND status = %s AND status != 'replaced' "
            "AND active_automation_import_job_id IS NULL",
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
                priority_started_at_present,
                fields.get("priority_started_at"),
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
        now = datetime.now(UTC)
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
        now = datetime.now(UTC)
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
        now = datetime.now(UTC)
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


    def update_download_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_enqueued_at: str,
    ) -> bool:
        """CAS whole download state against its exact attempt witness."""
        now = datetime.now(UTC)
        cur = self._execute("""
            WITH outgoing AS (
                SELECT %s::jsonb AS state
            )
            UPDATE album_requests AS request
            SET active_download_state = outgoing.state,
                updated_at = %s
            FROM outgoing
            WHERE request.id = %s
              AND request.status = 'downloading'
              AND request.active_download_state -> 'enqueued_at'
                  = to_jsonb(%s::text)
              AND outgoing.state -> 'enqueued_at'
                  = to_jsonb(%s::text)
        """, (
            state_json,
            now,
            request_id,
            expected_enqueued_at,
            expected_enqueued_at,
        ))
        self.conn.commit()
        return cur.rowcount > 0


    def get_downloading(self) -> list[AlbumRequestRow]:
        """Get all albums currently being downloaded."""
        cur = self._execute(
            "SELECT * FROM album_requests WHERE status = 'downloading' "
            "ORDER BY updated_at ASC"
        )
        return [album_request_row(r) for r in cur.fetchall()]

    def get_acquisition(
        self,
        *,
        youtube_limit: int = 50,
    ) -> AcquisitionPayload:
        """Return active request acquisition plus YouTube ingest in one read.

        The request side is deliberately ``downloading|processing`` while the
        YouTube side remains its existing ``download_log`` feed. YouTube rows
        always carry a null processing owner: a rescue does not enter the
        automation ownership lifecycle even when it targets the same request.
        """
        cur = self._execute(
            """
            WITH active_requests AS (
                SELECT *
                FROM album_requests
                WHERE status = ANY(%s)
            ),
            youtube_rows AS (
                SELECT
                    dl.id AS download_log_id,
                    dl.request_id,
                    dl.source,
                    dl.outcome,
                    dl.youtube_metadata,
                    dl.created_at,
                    request.artist_name,
                    request.album_title,
                    request.mb_release_id,
                    request.status AS request_status,
                    NULL::jsonb AS processing_owner
                FROM download_log dl
                JOIN album_requests request ON request.id = dl.request_id
                WHERE dl.source = 'youtube'
                  AND dl.outcome = 'youtube_running'
                ORDER BY dl.created_at ASC, dl.id ASC
                LIMIT %s
            ),
            youtube AS (
                SELECT COALESCE(
                    jsonb_agg(to_jsonb(youtube_rows)),
                    '[]'::jsonb
                ) AS youtube_ingest
                FROM youtube_rows
            )
            SELECT
                request_row.*,
                processing_owner_job.id AS _processing_owner_job_id,
                processing_owner_job.status AS _processing_owner_status,
                processing_owner_job.preview_status
                    AS _processing_owner_preview_status,
                youtube.youtube_ingest
            FROM active_requests request_row
            RIGHT JOIN youtube ON TRUE
            LEFT JOIN import_jobs processing_owner_job
              ON processing_owner_job.id =
                 request_row.active_automation_import_job_id
            ORDER BY request_row.updated_at ASC NULLS LAST,
                     request_row.id ASC NULLS LAST
            """,
            (
                list(ACQUISITION_REQUEST_STATUSES),
                max(1, int(youtube_limit)),
            ),
        )
        raw_rows: list[Mapping[str, object]] = list(cur.fetchall())
        youtube_raw: object = (
            raw_rows[0].get("youtube_ingest") if raw_rows else []
        )
        youtube_ingest = (
            [dict(item) for item in youtube_raw if is_str_object_dict(item)]
            if is_object_list(youtube_raw)
            else []
        )
        acquisition = [
            self._request_presentation_row(row)
            for row in raw_rows
            if row.get("id") is not None
        ]
        return {
            "acquisition": acquisition,
            "youtube_ingest": youtube_ingest,
        }


    # --- Query methods ---

    def get_wanted(self, limit: int | None = None) -> list[AlbumRequestRow]:
        now = datetime.now(UTC)
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


    def get_by_status(
        self,
        status: str,
        *,
        limit: int | None = None,
        newest_first: bool = False,
    ) -> list[AlbumRequestRow]:
        """Rows in one status. ``newest_first`` orders by ``updated_at``
        DESC (recency window for the imported list, #426); ``limit``
        caps the result. Defaults preserve the original full-list shape.
        """
        order = (
            "request_row.updated_at DESC"
            if newest_first
            else "request_row.created_at ASC"
        )
        sql = f"""
            SELECT {REQUEST_PRESENTATION_SELECT}
            {REQUEST_PRESENTATION_FROM}
            WHERE request_row.status = %s
            ORDER BY {order}
        """
        params: list[object] = [status]
        if limit is not None:
            sql += " LIMIT %s"
            params.append(int(limit))
        cur = self._execute(sql, tuple(params))
        return [self._request_presentation_row(r) for r in cur.fetchall()]


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
            status_clause = " AND request_row.status = %s"
            params.append(status)
        params.append(max(1, min(int(limit), 500)))
        cur = self._execute(
            f"SELECT {REQUEST_PRESENTATION_SELECT}"
            f" {REQUEST_PRESENTATION_FROM}"
            " WHERE (request_row.artist_name ILIKE %s ESCAPE '\\'"
            "    OR request_row.album_title ILIKE %s ESCAPE '\\')"
            f"{status_clause}"
            " ORDER BY request_row.artist_name,"
            " request_row.year NULLS LAST, request_row.id"
            " LIMIT %s",
            tuple(params),
        )
        return [self._request_presentation_row(r) for r in cur.fetchall()]


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
    # The current-evidence join carries the codec facts behind the
    # worklist chip's audit-only flags (issue #829 Phase 5 PR4); the
    # service derives them per row via the shared adapter. The nine
    # aliases are spelled inline rather than generated so this SELECT
    # stays statically resolvable for the replaced-write audit;
    # ``tests/test_pipeline_db_column_contract.py`` pins them against
    # ``accusation_evidence_columns`` so the spellings cannot drift.
    _LONG_TAIL_SELECT = """
        SELECT
            current_evidence.format AS _current_evidence_format,
            current_evidence.spectral_grade
                AS _current_evidence_spectral_grade,
            current_evidence.spectral_bitrate_kbps
                AS _current_evidence_spectral_bitrate,
            current_evidence.spectral_subject
                AS _current_evidence_spectral_subject,
            current_evidence.was_converted_from
                AS _current_evidence_was_converted_from,
            current_evidence.cliff_hz AS _current_evidence_cliff_hz,
            current_evidence.codec_family AS _current_evidence_codec_family,
            current_evidence.storage_format
                AS _current_evidence_storage_format,
            current_evidence.filetype_band
                AS _current_evidence_filetype_band,
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
        LEFT JOIN album_quality_evidence current_evidence
          ON current_evidence.id = ar.current_evidence_id
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
    ) -> list[ArtistRequestRow]:
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
                f"""
                SELECT {REQUEST_PRESENTATION_SELECT},
                       {_CAPTURE_AND_EVIDENCE_SELECT}
                {REQUEST_PRESENTATION_FROM}
                LEFT JOIN album_quality_evidence current_evidence
                  ON current_evidence.id = request_row.current_evidence_id
                WHERE request_row.mb_artist_id = %s
                   OR (request_row.artist_name ILIKE %s ESCAPE '\\'
                       -- Hyphen-free ids (e.g. legacy numerics / Discogs ids)
                       -- deliberately fall back to the artist-name match.
                       AND (request_row.mb_artist_id IS NULL
                            OR request_row.mb_artist_id = ''
                            OR request_row.mb_artist_id NOT LIKE '%%-%%'))
                ORDER BY request_row.year, request_row.album_title
                """,
                (mb_artist_id, name_pattern),
            )
        else:
            cur = self._execute(
                f"""
                SELECT {REQUEST_PRESENTATION_SELECT},
                       {_CAPTURE_AND_EVIDENCE_SELECT}
                {REQUEST_PRESENTATION_FROM}
                LEFT JOIN album_quality_evidence current_evidence
                  ON current_evidence.id = request_row.current_evidence_id
                WHERE request_row.artist_name ILIKE %s ESCAPE '\\'
                ORDER BY request_row.year, request_row.album_title
                """,
                (name_pattern,),
            )
        return [self._artist_request_row(r) for r in cur.fetchall()]


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
        now = datetime.now(UTC)

        # Counter + backoff are one CAS. A Replace that wins before this
        # statement leaves the frozen ancestor byte-identical.
        cur = self._execute(f"""
            UPDATE album_requests
            SET {col} = COALESCE({col}, 0) + 1,
                last_attempt_at = %s,
                next_retry_after = %s + (
                    LEAST(
                        %s * POWER(2, LEAST(COALESCE({col}, 0), %s)),
                        %s
                    ) * INTERVAL '1 minute'
                ),
                updated_at = %s
            WHERE id = %s
              AND status = %s
              AND status != 'replaced'
              AND active_automation_import_job_id IS NULL
            RETURNING {col}
        """, (
            now,
            now,
            BACKOFF_BASE_MINUTES,
            SEARCH_BACKOFF_MAX_EXPONENT,
            BACKOFF_MAX_MINUTES,
            now,
            request_id,
            expected_status,
        ))
        row = cur.fetchone()
        self.conn.commit()
        return row is not None


    def merge_rekey_collision(
        self,
        request_id: int,
        *,
        old_release_id: str,
        new_release_id: str,
    ) -> MergeRekeyCollision:
        """Read what already occupies the survivor, before the library moves.

        The two worlds in which :meth:`update_request_release_for_merge`
        raises a ``UniqueViolation`` — a rival request already at the
        survivor, and an evidence row already at
        ``(survivor, snapshot_fingerprint)`` — are
        both plain reads, so the merge seam asks first and never retags the
        shared Beets library for a rekey that is already refused. Those two
        are the whole of this pre-check, and deliberately so: the write's
        other refusals (the identity compare-and-set, the frozen ``replaced``
        guard, and both claim arms) are ``rowcount = 0`` misses describing a
        world the next attempt re-derives, while these two persist until an
        operator resolves them.

        This is the ordering fix for the split state: retagging and THEN
        discovering the refusal leaves the installed album filed under the
        survivor while the request still names the merged-away id, and the
        collision that refused the write is still there on the next attempt —
        which this pre-check refuses before the library is read at all, so
        nothing repairs that.

        Deliberately NOT the authority — the write re-decides both conditions
        atomically under the row lock. A rival that appears between this read
        and that write is the residual, which the caller records and fails
        closed on.

        The rival query is ``mb_release_id = new AND id <> request_id`` with no
        status filter, because ``album_requests.mb_release_id`` is globally
        UNIQUE (migration 001): a frozen ``replaced`` audit ancestor collides
        exactly like a live request does.
        """
        cur = self._execute(
            "SELECT id FROM album_requests "
            "WHERE mb_release_id = %s AND id <> %s "
            "ORDER BY id LIMIT 1",
            (new_release_id, request_id),
        )
        rival_row = cur.fetchone()
        cur = self._execute(
            "SELECT moving.snapshot_fingerprint AS snapshot_fingerprint "
            "FROM album_quality_evidence AS moving "
            "JOIN album_quality_evidence AS held "
            "  ON held.mb_release_id = %s "
            " AND held.snapshot_fingerprint = moving.snapshot_fingerprint "
            "WHERE moving.mb_release_id = %s "
            "ORDER BY moving.snapshot_fingerprint",
            (new_release_id, old_release_id),
        )
        return MergeRekeyCollision(
            rival_request_id=(
                None if rival_row is None else int(rival_row["id"])
            ),
            colliding_fingerprints=tuple(
                str(row["snapshot_fingerprint"]) for row in cur.fetchall()
            ),
        )


    def update_request_release_for_merge(
        self,
        request_id: int,
        *,
        old_release_id: str,
        new_release_id: str,
        expected_import_job_id: int | None,
    ) -> bool:
        """Rekey one request AND its evidence onto the merge survivor (#1059).

        MusicBrainz editors merge release A into release B; the loser's MBID
        becomes a permanent 301 and a request stored at A can never import
        again — Beets offers B, the matcher demands A. This is the ONE write
        that moves ``mb_release_id``, and it is deliberately not
        ``update_request_fields``: that seam reserves immutable identity, and
        refuses any row with a processing owner attached, which is exactly the
        world this write happens in.

        Every predicate is load-bearing and the write fails closed on each:

        * ``mb_release_id = %s`` — a compare-and-set on the identity being
          moved. A row somebody else already rekeyed, superseded, or pointed
          elsewhere is left alone.
        * **the caller still holds the import claim it took, OR is the
          operator asking directly.** There are exactly three arms. The
          first two are each claim's own request predicate copied verbatim,
          so "still claimed" means the same thing here as it did at claim
          time; the third has no claim at all — it IS the operator, acting
          on an ``imported`` row nothing else currently owns. Since issue
          #1313 the two named claim methods are lane-taking wrappers, so the
          predicates below live in ``import_jobs.py``'s
          ``_claim_automation_job_in_lane`` and
          ``_claim_request_scoped_job_in_lane``:

          - ``claim_automation_import_job_under_lock`` — ``status =
            'processing'`` and ``active_automation_import_job_id = %s``. That
            pointer IS ownership (invariant 10), and it excludes the frozen
            ``replaced`` status by construction: a frozen audit ancestor is
            never ``processing``.
          - ``claim_force_import_job_under_lock`` / ``claim_local_import_
            job_under_lock`` — ``status NOT IN ('processing', 'replaced')``,
            no automation owner attached, and the named ``force_import`` OR
            ``local_import`` job ``running`` against this request (issue
            #1176 PR3 widened this arm's ``job_type`` term from a bare
            equality to ``IN ('force_import', 'local_import')`` — the
            local-import lane is modeled on force's copy chain end to end,
            and #1080's "don't diverge and recreate any pathways" authority
            below governs this arm exactly as it governs the rest of the
            lane). Neither runs on ``processing`` and CANNOT take the
            ``processing`` pointer: migration 066's owner-equivalence CHECK
            and its partial unique index reserve that for one active
            ``automation_import`` job. Before #1080 the force lane could
            therefore never follow a merge — it met the merged-away release
            at the apply-time comparison inside ``import_one.py`` instead,
            which has no redirect concept.
          - **the operator merge-rekey arm (#1089)** — admitted only when
            ``expected_import_job_id IS NULL`` (the two claim arms above are
            each keyed to a real job id and so can never satisfy this one by
            accident), ``status = 'imported'``, no automation owner attached,
            AND no ``import_jobs`` row for this request is currently
            ``queued`` or ``running`` (any job type — an in-flight force
            import or YouTube rescue could otherwise have its identity moved
            out from under it mid-launch). The web dashboard's drift panel
            (#1089 MINOR-3, review round 2) surfaces every ``imported``
            request Beets no longer resolves against, for ANY reason — a
            MusicBrainz merge is only one of them, and the panel cannot know
            ahead of a click whether Beets already holds the survivor — so
            it is not "exactly the rows this arm can act on": the button
            shows only for MB-sourced drift rows, and ``MergeRekeyService``
            discovers eligibility at click time (survivor state, evidence
            lineage, collisions) rather than the panel pre-filtering to it.
            When this arm DOES apply, it heals the request→Beets join by
            moving the LEDGER onto the identity Beets already has — Beets
            itself is never mutated by this arm.

            Authority: "really we need to re-key mbid and beets don't we so
            they go away. we could surface these here and have a button
            which re-keys with the current machinery we've built couldn't
            we?" —
            https://github.com/abl030/cratedigger/issues/1089#issuecomment-5274933957

          A YouTube rescue job matches neither import-claim arm and never
          rekeys through them; a ``queued``/``running`` rescue also blocks
          the operator arm via its ``NOT EXISTS`` term above.

        Admitting the force claim here is an operator decision, not an
        inference from the automation arm:

        Authority: "force import is supposed to be exactly the same as anything
        else just with beets distance over-ridden, so please don't diverge and
        recreate any pathways." —
        https://github.com/abl030/cratedigger/issues/1080

        **The evidence moves with the row, in the same transaction.**
        ``album_quality_evidence`` is content-addressed by
        ``(mb_release_id, snapshot_fingerprint)`` (migration 021) and
        ``album_requests.mb_release_id`` is UNIQUE, so every evidence row at
        the merged-away id belongs to this one request's pressing. Leaving
        them behind strands the request's whole evidence lineage at an id
        nothing names any more: ``backfill_current_evidence_from_album_info``
        nulls a current-evidence row whose identity no longer matches and the
        rebuilt HAVE row silently loses its ``verified_lossless_proof`` and
        ``cd_rip_verification`` (the proof lock is absolute for every import
        mode); ``_refresh_current_evidence_after_import`` returns
        ``identity_mismatch``; the quality gate then loads no state and
        reopens full-tier search on the very import this rekey exists to
        enable; the sidecar writer skips; and every ``_evidence_*`` field on
        the operator's Recents card blanks. The rekey is one identity change,
        so it is one transaction.

        Returns False rather than raising, writing NOTHING, when:

        * another request already holds the survivor
          (``UNIQUE(mb_release_id)``) — two requests are two curated
          pressings and merging or deleting either is the operator's call
          (invariant 5); or
        * a snapshot fingerprint being moved already exists at the survivor
          (``UNIQUE(mb_release_id, snapshot_fingerprint)``) — two independent
          measurements of the same bytes, and choosing between them would be
          an unowned quality decision.

        Either way the caller keeps its existing rejection and the request
        stays runnable — nothing is parked (invariant 11).

        The library must already be at ``new_release_id`` before this runs.
        Beets keys album duplicate detection on ``mb_albumid``, so rekeying a
        request whose installed album is still filed under the old id makes
        the next import land a SECOND album beside the first. See
        ``lib/beets_retag.py``.
        """
        if not old_release_id or not new_release_id:
            raise ValueError("merge rekey requires both release ids")
        if old_release_id == new_release_id:
            raise ValueError(
                "refusing to rekey a request onto itself: "
                f"{old_release_id}"
            )
        now = datetime.now(UTC)
        try:
            with self._atomic():
                with self.conn.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor,
                ) as cur:
                    cur.execute(
                        "UPDATE album_requests "
                        "SET mb_release_id = %s, updated_at = %s "
                        "WHERE id = %s AND mb_release_id = %s "
                        # The frozen-ancestor guard is its own top-level term,
                        # never a branch of the claim disjunction below: a
                        # ``replaced`` row is out of scope for EVERY arm,
                        # and stating it once keeps that unconditional.
                        "AND status <> 'replaced' "
                        "AND ("
                        "  (status = 'processing'"
                        "   AND active_automation_import_job_id = %s)"
                        "  OR ("
                        "    status <> 'processing'"
                        "    AND active_automation_import_job_id IS NULL"
                        "    AND EXISTS ("
                        "      SELECT 1 FROM import_jobs j"
                        "      WHERE j.id = %s"
                        "        AND j.request_id = album_requests.id"
                        "        AND j.job_type IN ('force_import', 'local_import')"
                        "        AND j.status = 'running'"
                        "    )"
                        "  )"
                        # The operator arm (#1089): ``%s IS NULL`` is the
                        # guard that keeps this from ever widening the two
                        # claim arms above — a real job id supplied by
                        # automation or force NEVER satisfies this term, no
                        # matter what the request row or the import_jobs
                        # table otherwise look like.
                        "  OR ("
                        "    %s IS NULL"
                        "    AND status = 'imported'"
                        "    AND active_automation_import_job_id IS NULL"
                        "    AND NOT EXISTS ("
                        "      SELECT 1 FROM import_jobs j"
                        "      WHERE j.request_id = album_requests.id"
                        "        AND j.status IN ('queued', 'running')"
                        "    )"
                        "  )"
                        ")",
                        (
                            new_release_id,
                            now,
                            request_id,
                            old_release_id,
                            expected_import_job_id,
                            expected_import_job_id,
                            expected_import_job_id,
                        ),
                    )
                    if cur.rowcount <= 0:
                        self.conn.rollback()
                        return False
                    # The survivor's own evidence rows are the collision. A
                    # matching fingerprint on both sides means two rows
                    # describing the same bytes; the UNIQUE constraint would
                    # raise anyway, and choosing a winner is not this write's
                    # call.
                    cur.execute(
                        "UPDATE album_quality_evidence "
                        "SET mb_release_id = %s, updated_at = %s "
                        "WHERE mb_release_id = %s",
                        (new_release_id, now, old_release_id),
                    )
                self.conn.commit()
                return True
        except psycopg2.errors.UniqueViolation:
            # ``_atomic`` already rolled the whole transaction back, so the
            # request row is still at the merged-away id and every evidence
            # row is exactly where it was.
            return False
