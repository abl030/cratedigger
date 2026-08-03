"""Derived convergence reads and the atomic operator stop transition."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import psycopg2.extras

from lib.convergence_service import (
    ConvergenceSignal,
    StopConvergedSearchResult,
)
from lib.pipeline_db._core import _PipelineDBBase

_CONVERGENCE_CTES = """
WITH eligible_downloads AS MATERIALIZED (
    SELECT
        dl.request_id,
        dl.id AS log_id,
        dl.soulseek_username AS peer,
        dl.candidate_evidence_id,
        dl.created_at AS observed_at
    FROM download_log dl
    WHERE dl.source = 'slskd'
      AND dl.outcome IN ('success', 'rejected')
      AND NULLIF(BTRIM(dl.soulseek_username), '') IS NOT NULL
      AND dl.beets_scenario = 'strong_match'
      AND dl.beets_distance <= 0.15
      {request_filter}
), eligible AS (
    SELECT
        dl.request_id, dl.log_id, dl.peer,
        e.snapshot_fingerprint,
        dl.observed_at,
        CASE
            WHEN e.cliff_hz IS NULL THEN NULL
            ELSE (ROUND(e.cliff_hz / 500.0) * 500)::INTEGER
        END AS cliff_bin_hz
    FROM eligible_downloads dl
    JOIN album_quality_evidence e ON e.id = dl.candidate_evidence_id
    WHERE e.verified_lossless IS FALSE
      AND e.codec_family = 'lossless'
      AND e.spectral_subject = 'source'
      AND e.spectral_measurement_version = 2
), ordered AS (
    SELECT
        eligible.*,
        ROW_NUMBER() OVER (
            PARTITION BY request_id ORDER BY observed_at DESC, log_id DESC
        ) AS reverse_ordinal,
        FIRST_VALUE(cliff_bin_hz) OVER (
            PARTITION BY request_id ORDER BY observed_at DESC, log_id DESC
        ) AS latest_cliff_bin_hz
    FROM eligible
), bounded AS (
    SELECT
        ordered.*,
        MIN(reverse_ordinal) FILTER (
            WHERE cliff_bin_hz IS DISTINCT FROM latest_cliff_bin_hz
        ) OVER (PARTITION BY request_id) AS first_break_ordinal
    FROM ordered
), current_run AS (
    SELECT *
    FROM bounded
    WHERE latest_cliff_bin_hz IS NOT NULL
      AND reverse_ordinal < COALESCE(first_break_ordinal, 2147483647)
), signals AS (
    SELECT
        request_id,
        COUNT(*)::INTEGER AS observation_count,
        COUNT(DISTINCT peer)::INTEGER AS distinct_peer_count,
        COUNT(DISTINCT snapshot_fingerprint)::INTEGER
            AS distinct_candidate_snapshot_count,
        MIN(latest_cliff_bin_hz)::INTEGER AS cliff_hz,
        (ARRAY_AGG(log_id ORDER BY observed_at DESC, log_id DESC))[1]
            AS latest_qualifying_log_id,
        MIN(observed_at) AS first_observed_at,
        MAX(observed_at) AS latest_observed_at
    FROM current_run
    GROUP BY request_id
    HAVING COUNT(DISTINCT peer) >= 5
)
"""


def _signal_from_row(row: dict[str, Any]) -> ConvergenceSignal:
    return ConvergenceSignal(
        request_id=int(row["request_id"]),
        observation_count=int(row["observation_count"]),
        distinct_peer_count=int(row["distinct_peer_count"]),
        distinct_candidate_snapshot_count=int(
            row["distinct_candidate_snapshot_count"]
        ),
        cliff_hz=int(row["cliff_hz"]),
        latest_qualifying_log_id=int(row["latest_qualifying_log_id"]),
        first_observed_at=row["first_observed_at"],
        latest_observed_at=row["latest_observed_at"],
    )


class _ConvergenceMixin(_PipelineDBBase):
    def _get_convergence_signals_with_cursor(
        self,
        cur: Any,
        request_ids: Sequence[int] | None,
    ) -> dict[int, ConvergenceSignal]:
        request_filter = ""
        params: tuple[object, ...] = ()
        if request_ids is not None:
            ids = [int(request_id) for request_id in request_ids]
            if not ids:
                return {}
            request_filter = "AND dl.request_id = ANY(%s)"
            params = (ids,)
        sql = _CONVERGENCE_CTES.format(request_filter=request_filter) + """
SELECT signals.*
FROM signals
JOIN album_requests ar ON ar.id = signals.request_id
JOIN album_quality_evidence current_evidence
  ON current_evidence.id = ar.current_evidence_id
WHERE current_evidence.v0_subject = 'source'
  AND current_evidence.verified_lossless IS FALSE
ORDER BY signals.request_id
"""
        cur.execute(sql, params)
        return {
            int(row["request_id"]): _signal_from_row(row)
            for row in cur.fetchall()
        }

    def get_convergence_signals(
        self, request_ids: Sequence[int] | None = None,
    ) -> dict[int, ConvergenceSignal]:
        """Derive current signals; no flag is persisted and no cadence changes."""
        self._ensure_conn()
        with self.conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor,
        ) as cur:
            return self._get_convergence_signals_with_cursor(cur, request_ids)

    def stop_search_for_convergence(
        self,
        request_id: int,
        *,
        latest_qualifying_log_id: int,
        cliff_hz: int,
    ) -> StopConvergedSearchResult:
        """Lock, rederive, reject stale identity, then wanted -> unsearchable."""
        rid = int(request_id)
        with self._atomic(), self.conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor,
        ) as cur:
            cur.execute(
                "SELECT status FROM album_requests WHERE id = %s FOR UPDATE",
                (rid,),
            )
            request = cur.fetchone()
            if request is None:
                self.conn.rollback()
                return StopConvergedSearchResult(
                    outcome="not_found", request_id=rid,
                )
            status = str(request["status"])
            if status != "wanted":
                self.conn.rollback()
                return StopConvergedSearchResult(
                    outcome="wrong_state", request_id=rid,
                    observed_status=status,
                )
            signal = self._get_convergence_signals_with_cursor(cur, [rid]).get(rid)
            if signal is None:
                self.conn.rollback()
                return StopConvergedSearchResult(
                    outcome="not_converged", request_id=rid,
                    observed_status=status,
                )
            if (
                signal.latest_qualifying_log_id
                != int(latest_qualifying_log_id)
                or signal.cliff_hz != int(cliff_hz)
            ):
                self.conn.rollback()
                return StopConvergedSearchResult(
                    outcome="stale", request_id=rid, signal=signal,
                    observed_status=status,
                )
            cur.execute(
                """
                    UPDATE album_requests
                    SET status = 'unsearchable',
                        active_download_state = NULL,
                        updated_at = NOW()
                    WHERE id = %s AND status = 'wanted'
                    """,
                (rid,),
            )
            self.conn.commit()
        return StopConvergedSearchResult(
            outcome="stopped", request_id=rid, signal=signal,
            observed_status="unsearchable",
        )
