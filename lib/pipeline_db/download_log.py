"""download_log audit rows and wrong-match bookkeeping."""
import json
from datetime import datetime, timedelta, timezone
from typing import Any
import msgspec
import psycopg2
import psycopg2.extras

from lib.pipeline_db._shared import (
    BACKOFF_BASE_MINUTES,
    BACKOFF_MAX_MINUTES,
)

from lib.pipeline_db._core import _PipelineDBBase
from lib.validation_envelope import (
    FAILED_PATH_KEY,
    SCENARIO_KEY,
    WRONG_MATCH_TRIAGE_KEY,
    WrongMatchTriageAudit,
)


class _DownloadLogMixin(_PipelineDBBase):
    """download_log audit rows and wrong-match bookkeeping."""


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
                   ar.prev_min_bitrate, ar.search_filetype_override,
                   ar.source AS request_source
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


    # --- Download logging ---

    def log_download(self, request_id: int,
                     soulseek_username: str | None = None,
                     filetype: str | None = None,
                     download_path: str | None = None,
                     beets_distance: float | None = None,
                     beets_scenario: str | None = None,
                     beets_detail: str | None = None,
                     valid: bool | None = None,
                     outcome: str | None = None,
                     staged_path: str | None = None,
                     error_message: str | None = None,
                     bitrate: int | None = None,
                     sample_rate: int | None = None,
                     bit_depth: int | None = None,
                     is_vbr: bool | None = None,
                     was_converted: bool | None = None,
                     original_filetype: str | None = None,
                     # Spectral quality verification fields
                     slskd_filetype: str | None = None,
                     slskd_bitrate: int | None = None,
                     actual_filetype: str | None = None,
                     actual_min_bitrate: int | None = None,
                     spectral_grade: str | None = None,
                     spectral_bitrate: int | None = None,
                     existing_min_bitrate: int | None = None,
                     existing_spectral_bitrate: int | None = None,
                     # Full import_one.py result (JSON string)
                     import_result: Any = None,
                     # Full validation result (JSON string)
                     validation_result: Any = None,
                     # Final format on disk
                     final_format: str | None = None,
                     v0_probe_kind: str | None = None,
                     v0_probe_min_bitrate: int | None = None,
                     v0_probe_avg_bitrate: int | None = None,
                     v0_probe_median_bitrate: int | None = None,
                     existing_v0_probe_kind: str | None = None,
                     existing_v0_probe_min_bitrate: int | None = None,
                     existing_v0_probe_avg_bitrate: int | None = None,
                     existing_v0_probe_median_bitrate: int | None = None,
                     ) -> int:
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


    def abandon_auto_import_request(
        self,
        *,
        request_id: int,
        current_path: str,
        soulseek_username: str | None,
        filetype: str | None,
        beets_scenario: str,
        beets_detail: str,
        outcome: str,
        staged_path: str,
        error_message: str,
        validation_result: str | None,
    ) -> int | None:
        """Atomically audit and reset an owned interrupted auto-import row."""
        with self._atomic():
            now = datetime.now(timezone.utc)
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                cur.execute(
                    """
                    UPDATE album_requests
                    SET status = 'wanted',
                        active_download_state = NULL,
                        manual_reason = NULL,
                        updated_at = %s
                    WHERE id = %s
                      AND status = 'downloading'
                      AND active_download_state IS NOT NULL
                      AND active_download_state->>'current_path' = %s
                      AND active_download_state->>'import_subprocess_started_at'
                          IS NOT NULL
                    RETURNING id
                    """,
                    (now, request_id, current_path),
                )
                if cur.fetchone() is None:
                    self.conn.rollback()
                    return None

                cur.execute(
                    """
                    UPDATE album_requests
                    SET download_attempts = COALESCE(download_attempts, 0) + 1,
                        last_attempt_at = %s,
                        updated_at = %s
                    WHERE id = %s
                    RETURNING download_attempts
                    """,
                    (now, now, request_id),
                )
                attempt_row = cur.fetchone()
                assert attempt_row is not None, f"Request {request_id} not found"
                new_count = int(attempt_row["download_attempts"])
                backoff_minutes = min(
                    BACKOFF_BASE_MINUTES * (2 ** (new_count - 1)),
                    BACKOFF_MAX_MINUTES,
                )
                cur.execute(
                    """
                    UPDATE album_requests
                    SET next_retry_after = %s
                    WHERE id = %s
                    """,
                    (now + timedelta(minutes=backoff_minutes), request_id),
                )

                cur.execute(
                    """
                    INSERT INTO download_log (
                        request_id, soulseek_username, filetype,
                        beets_scenario, beets_detail, outcome,
                        staged_path, error_message, validation_result
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        request_id,
                        soulseek_username,
                        filetype,
                        beets_scenario,
                        beets_detail,
                        outcome,
                        staged_path,
                        error_message,
                        validation_result,
                    ),
                )
                log_row = cur.fetchone()
                assert log_row is not None, "INSERT RETURNING should return a row"
                log_id = int(log_row["id"])
            self.conn.commit()
            return log_id


    # Evidence-overlay extension applied to every download_log read seam
    # (single entry / per-request history / batch). The legacy denorm
    # spectral / V0 columns on download_log are NULL whenever the
    # candidate was rejected before the dispatch path could backfill
    # them — every wrong-match reject hits this. The canonical
    # measurement lives on album_quality_evidence, addressed via
    # download_log.candidate_evidence_id. We LEFT JOIN it here and let
    # the Python overlay step COALESCE evidence over the denorm columns
    # before handing the row dict to downstream consumers (LogEntry,
    # build_download_history_row, the wrong-match route, ...). Doing it
    # at the read seam means there's exactly one place to maintain the
    # mapping, and downstream code keeps using the existing field names.
    #
    # ``dl.*`` automatically projects ``source`` and ``youtube_metadata``
    # (migration 037) onto every consumer; no additional column list
    # change is needed here.
    # History queries compose from these two named parts so variants
    # (plain SELECT vs DISTINCT ON) build explicitly instead of
    # string-replacing tokens inside a finished statement (#433).
    _DOWNLOAD_LOG_HISTORY_COLUMNS = """
            dl.*,
            e.spectral_grade        AS _evidence_spectral_grade,
            e.spectral_bitrate_kbps AS _evidence_spectral_bitrate,
            e.v0_source_lineage     AS _evidence_v0_probe_kind,
            e.v0_avg_bitrate_kbps   AS _evidence_v0_probe_avg_bitrate
    """
    _DOWNLOAD_LOG_HISTORY_FROM = """
        FROM download_log dl
        LEFT JOIN album_quality_evidence e
            ON e.id = dl.candidate_evidence_id
    """
    _DOWNLOAD_LOG_HISTORY_SELECT = (
        "SELECT" + _DOWNLOAD_LOG_HISTORY_COLUMNS + _DOWNLOAD_LOG_HISTORY_FROM
    )

    # Evidence stores lineage as ``lossless_source`` / ``native_lossy_research``;
    # download_log.v0_probe_kind stores the wire-shaped kind
    # ``lossless_source_v0`` / ``native_lossy_research_v0`` (constrained by
    # migration 007). When we overlay evidence lineage into the kind slot, we
    # have to translate, or the renderer (history.js::formatV0Probe) won't
    # recognize the value and will fall through to the raw-kind branch.
    _EVIDENCE_LINEAGE_TO_PROBE_KIND = {
        "lossless_source":       "lossless_source_v0",
        "native_lossy_research": "native_lossy_research_v0",
        "on_disk_research":      "on_disk_research_v0",
    }

    @classmethod
    def _overlay_evidence_onto_download_log_row(
        cls, row: dict[str, Any]
    ) -> dict[str, Any]:
        for legacy, overlay in (
            ("spectral_grade",       "_evidence_spectral_grade"),
            ("spectral_bitrate",     "_evidence_spectral_bitrate"),
            ("v0_probe_kind",        "_evidence_v0_probe_kind"),
            ("v0_probe_avg_bitrate", "_evidence_v0_probe_avg_bitrate"),
        ):
            evidence_value = row.pop(overlay, None)
            if row.get(legacy) is None and evidence_value is not None:
                if legacy == "v0_probe_kind":
                    evidence_value = cls._EVIDENCE_LINEAGE_TO_PROBE_KIND.get(
                        evidence_value, evidence_value
                    )
                row[legacy] = evidence_value
        return row


    def get_download_log_entry(self, log_id: int) -> dict[str, Any] | None:
        """Get a single download_log entry by its ID."""
        cur = self._execute(
            self._DOWNLOAD_LOG_HISTORY_SELECT + " WHERE dl.id = %s",
            (log_id,),
        )
        row = cur.fetchone()
        return self._overlay_evidence_onto_download_log_row(dict(row)) \
            if row else None


    def get_download_history(self, request_id):
        cur = self._execute(
            self._DOWNLOAD_LOG_HISTORY_SELECT
            + " WHERE dl.request_id = %s ORDER BY dl.id DESC",
            (request_id,),
        )
        return [
            self._overlay_evidence_onto_download_log_row(dict(r))
            for r in cur.fetchall()
        ]


    def get_download_history_batch(self, request_ids: list[int]) -> dict[int, list[dict]]:
        """Batch fetch download history for multiple request IDs.

        Returns dict of request_id → list of history rows (most recent first).
        """
        if not request_ids:
            return {}
        ph = ",".join(["%s"] * len(request_ids))
        cur = self._execute(
            self._DOWNLOAD_LOG_HISTORY_SELECT
            + f" WHERE dl.request_id IN ({ph}) ORDER BY dl.id DESC",
            tuple(request_ids),
        )
        result: dict[int, list[dict]] = {}
        for row in cur.fetchall():
            r = self._overlay_evidence_onto_download_log_row(dict(row))
            rid = r["request_id"]
            if rid not in result:
                result[rid] = []
            result[rid].append(r)
        return result


    def get_latest_download_summaries(
        self, request_ids: list[int],
    ) -> dict[int, dict]:
        """Batch fetch only the NEWEST download_log row + history count
        per request: ``{request_id: {"latest": row, "count": n}}``.

        #426: the pipeline queue only renders the latest verdict and a
        count, but ``get_download_history_batch`` dragged every
        historical row (with fat JSONB) through Postgres and Python to
        get them. ``DISTINCT ON`` returns one detoasted row per request;
        the count aggregate never touches the JSONB columns.
        """
        if not request_ids:
            return {}
        ids = [int(r) for r in request_ids]
        latest_cur = self._execute(
            "SELECT * FROM ("
            "SELECT DISTINCT ON (dl.request_id)"
            + self._DOWNLOAD_LOG_HISTORY_COLUMNS
            + self._DOWNLOAD_LOG_HISTORY_FROM
            + " WHERE dl.request_id = ANY(%s)"
            " ORDER BY dl.request_id, dl.id DESC"
            ") latest",
            (ids,),
        )
        result: dict[int, dict] = {}
        for row in latest_cur.fetchall():
            r = self._overlay_evidence_onto_download_log_row(dict(row))
            result[int(r["request_id"])] = {"latest": r, "count": 0}

        count_cur = self._execute(
            "SELECT request_id, COUNT(*)::int AS n FROM download_log"
            " WHERE request_id = ANY(%s) GROUP BY request_id",
            (ids,),
        )
        for row in count_cur.fetchall():
            rid = int(row["request_id"])
            if rid in result:
                result[rid]["count"] = int(row["n"])
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
        # Pull the per-candidate quality measurement straight from the
        # canonical evidence row (FK on download_log.candidate_evidence_id).
        # The legacy denorm columns on download_log are NULL for every
        # wrong-match reject — they only get populated for the request's
        # current-state row. COALESCE keeps the older audit history working
        # if any pre-evidence rows are still around.
        cur = self._execute(f"""
            SELECT DISTINCT ON (dl.request_id, dl.validation_result->>'{FAILED_PATH_KEY}')
                dl.id AS download_log_id,
                dl.request_id,
                ar.artist_name,
                ar.album_title,
                ar.mb_release_id,
                ar.mb_release_group_id,
                dl.soulseek_username,
                dl.validation_result,
                COALESCE(e.spectral_grade, dl.spectral_grade) AS spectral_grade,
                COALESCE(e.spectral_bitrate_kbps, dl.spectral_bitrate) AS spectral_bitrate,
                COALESCE(e.v0_source_lineage, dl.v0_probe_kind) AS v0_probe_kind,
                COALESCE(e.v0_avg_bitrate_kbps, dl.v0_probe_avg_bitrate) AS v0_probe_avg_bitrate,
                e.storage_format AS evidence_storage_format,
                e.min_bitrate_kbps AS evidence_min_bitrate,
                e.verified_lossless AS evidence_verified_lossless,
                ar.status AS request_status,
                ar.min_bitrate AS request_min_bitrate,
                ar.verified_lossless AS request_verified_lossless,
                ar.current_spectral_grade AS request_current_spectral_grade,
                ar.current_spectral_bitrate AS request_current_spectral_bitrate,
                ar.imported_path AS request_imported_path
            FROM download_log dl
            JOIN album_requests ar ON dl.request_id = ar.id
            LEFT JOIN album_quality_evidence e
                ON e.id = dl.candidate_evidence_id
            WHERE dl.outcome = 'rejected'
              AND dl.validation_result->>'{FAILED_PATH_KEY}' IS NOT NULL
              AND (dl.validation_result->>'{SCENARIO_KEY}' IS NULL
                   OR dl.validation_result->>'{SCENARIO_KEY}' NOT IN ('audio_corrupt', 'spectral_reject'))
            ORDER BY dl.request_id, dl.validation_result->>'{FAILED_PATH_KEY}', dl.id DESC
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
        cur = self._execute(f"""
            UPDATE download_log
            SET validation_result = validation_result - '{FAILED_PATH_KEY}'
            WHERE id = %s AND validation_result->>'{FAILED_PATH_KEY}' IS NOT NULL
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
            SET validation_result = validation_result - '{FAILED_PATH_KEY}'
            WHERE request_id = %s
              AND outcome = 'rejected'
              AND validation_result->>'{FAILED_PATH_KEY}' IN ({placeholders})
        """, tuple([request_id, *paths]))
        self.conn.commit()
        return cur.rowcount


    def record_wrong_match_triage(
        self,
        log_id: int,
        triage_result: WrongMatchTriageAudit,
    ) -> bool:
        """Persist cleanup audit details on a download_log row."""
        cur = self._execute(f"""
            UPDATE download_log
            SET validation_result = jsonb_set(
                CASE
                    WHEN jsonb_typeof(validation_result) = 'object'
                    THEN validation_result
                    ELSE '{{}}'::jsonb
                END,
                '{{{WRONG_MATCH_TRIAGE_KEY}}}',
                %s::jsonb,
                true
            )
            WHERE id = %s
        """, (msgspec.json.encode(triage_result).decode(), log_id))
        self.conn.commit()
        return cur.rowcount > 0


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
