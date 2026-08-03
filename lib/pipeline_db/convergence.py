"""Derived convergence reads and the atomic operator stop transition."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import msgspec
import psycopg2.extras

from lib.convergence_service import (
    ConvergenceSignal,
    StopConvergedSearchResult,
)
from lib.pipeline_db._core import _PipelineDBBase


def _signal_from_row(row: Mapping[str, object]) -> ConvergenceSignal:
    return msgspec.convert(row, type=ConvergenceSignal)


class _ConvergenceMixin(_PipelineDBBase):
    def get_convergence_signals(
        self, request_ids: Sequence[int],
    ) -> dict[int, ConvergenceSignal]:
        """Derive signals request-locally; no flag or policy is persisted."""
        ids = list(dict.fromkeys(int(value) for value in request_ids))
        if not ids:
            return {}
        self._ensure_conn()
        with self.conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor,
        ) as cur:
            cur.execute(
                """
                SELECT signal.*
                FROM UNNEST(%s::BIGINT[]) request(id)
                CROSS JOIN LATERAL
                    derive_request_convergence_signal(request.id) signal
                ORDER BY signal.request_id
                """,
                (ids,),
            )
            signals = (_signal_from_row(row) for row in cur.fetchall())
            return {signal.request_id: signal for signal in signals}

    def stop_search_for_convergence(
        self,
        request_id: int,
        *,
        signal_token: str,
    ) -> StopConvergedSearchResult:
        """Atomically rederive the complete signal token and stop searching.

        PostgreSQL gives the statement one MVCC snapshot.  A signal writer
        committed before that snapshot changes the token and loses the CAS;
        one committed afterwards linearizes after this operator decision.
        There is no cross-system atomicity claim beyond PostgreSQL.
        """
        rid = int(request_id)
        with self._atomic(), self.conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor,
        ) as cur:
            cur.execute(
                """
                UPDATE album_requests request
                SET status = 'unsearchable',
                    active_download_state = NULL,
                    updated_at = NOW()
                FROM derive_request_convergence_signal(%s) signal
                WHERE request.id = %s
                  AND request.status = 'wanted'
                  AND signal.request_id = request.id
                  AND signal.signal_token = %s
                RETURNING signal.*
                """,
                (rid, rid, str(signal_token)),
            )
            row = cur.fetchone()
            if row is not None:
                self.conn.commit()
                signal = _signal_from_row(row)
                return StopConvergedSearchResult(
                    outcome="stopped",
                    request_id=rid,
                    signal=signal,
                    observed_status="unsearchable",
                )
            self.conn.rollback()

        request = self._execute(
            "SELECT status FROM album_requests WHERE id = %s",
            (rid,),
        ).fetchone()
        if request is None:
            return StopConvergedSearchResult(
                outcome="not_found", request_id=rid,
            )
        status = str(request["status"])
        if status != "wanted":
            return StopConvergedSearchResult(
                outcome="wrong_state", request_id=rid,
                observed_status=status,
            )
        signal = self.get_convergence_signals([rid]).get(rid)
        if signal is None:
            return StopConvergedSearchResult(
                outcome="not_converged", request_id=rid,
                observed_status=status,
            )
        return StopConvergedSearchResult(
            outcome="stale", request_id=rid, signal=signal,
            observed_status=status,
        )
