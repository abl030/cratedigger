"""FakePipelineDB convergence cluster — mirrors ``lib/pipeline_db/convergence.py``.

Search-convergence signals and the stop decision.
"""
from __future__ import annotations

from typing import (
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from lib.convergence_service import (
        ConvergenceSignal,
        StopConvergedSearchResult,
    )

from tests.fakes.pipeline_db._base import _FakePipelineDBBase


class _FakeConvergenceMixin(_FakePipelineDBBase):
    """Search-convergence signals and the stop decision."""

    def get_convergence_signals(
        self, request_ids: list[int],
    ) -> dict[int, ConvergenceSignal]:
        self.query_counts["get_convergence_signals"] = (
            self.query_counts.get("get_convergence_signals", 0) + 1
        )
        wanted = {int(request_id) for request_id in request_ids}
        return {
            request_id: signal
            for request_id, signal in self.convergence_signals.items()
            if request_id in wanted
        }

    def stop_search_for_convergence(
        self,
        request_id: int,
        *,
        signal_token: str,
    ) -> StopConvergedSearchResult:
        from lib.convergence_service import StopConvergedSearchResult

        rid = int(request_id)
        row = self._requests.get(rid)
        if row is None:
            return StopConvergedSearchResult(outcome="not_found", request_id=rid)
        status = str(row["status"])
        if status != "wanted":
            return StopConvergedSearchResult(
                outcome="wrong_state", request_id=rid,
                observed_status=status,
            )
        signal = self.convergence_signals.get(rid)
        if signal is None:
            return StopConvergedSearchResult(
                outcome="not_converged", request_id=rid,
                observed_status=status,
            )
        if signal.signal_token != signal_token:
            return StopConvergedSearchResult(
                outcome="stale", request_id=rid, signal=signal,
                observed_status=status,
            )
        row["status"] = "unsearchable"
        row["active_download_state"] = None
        return StopConvergedSearchResult(
            outcome="stopped", request_id=rid, signal=signal,
            observed_status="unsearchable",
        )

