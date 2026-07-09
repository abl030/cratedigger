"""Worker-safe ownership writes for newly enqueued downloads."""

from __future__ import annotations

import logging
from typing import Callable, Protocol, TYPE_CHECKING, runtime_checkable

from lib import transitions

if TYPE_CHECKING:
    from lib.pipeline_db import TransferLedgerRow
    from lib.search import PlanExecutionContext

logger = logging.getLogger("cratedigger")


@runtime_checkable
class DownloadOwnershipDB(transitions.TransitionsDB, Protocol):
    """The PipelineDB surface the ownership writer uses (#409).

    Extends ``TransitionsDB`` because the writer forwards its handle into
    ``transitions.finalize_request``. Parity tests live in
    ``tests/test_download.py``.
    """

    def set_downloading_if_plan_current(
        self,
        request_id: int,
        state_json: str,
        *,
        plan_id: int,
        plan_ordinal: int,
        cycle_count_snapshot: int,
    ) -> bool: ...

    def update_download_state_if_downloading(
        self, request_id: int, state_json: str,
    ) -> bool: ...

    def record_transfer_enqueue(self, rows: "list[TransferLedgerRow]") -> None: ...

    def stamp_transfer_id(
        self, username: str, filename: str, transfer_id: str,
    ) -> int: ...

    def close(self) -> None: ...


class DownloadOwnershipWriter:
    """Persist download ownership using a fresh DB handle per operation.

    find_download workers intentionally cannot use the owner thread's cached
    DatabaseSource connection. This collaborator gives workers a narrow write
    surface for the status/state transition that makes an accepted slskd enqueue
    durable before the cycle can crash.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        db_factory: Callable[[], DownloadOwnershipDB] | None = None,
        close_after_use: bool | None = None,
    ) -> None:
        self.dsn = dsn
        self._db_factory = db_factory
        self._close_after_use = (
            db_factory is None if close_after_use is None else close_after_use
        )

    def _open_db(self) -> DownloadOwnershipDB:
        if self._db_factory is not None:
            return self._db_factory()
        from lib.pipeline_db import PipelineDB

        return PipelineDB(self.dsn)

    def _close_db(self, db: DownloadOwnershipDB) -> None:
        if not self._close_after_use:
            return
        db.close()

    def claim_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        plan_execution: "PlanExecutionContext | None" = None,
    ) -> bool:
        """Guarded wanted -> downloading claim with planned download state.

        When ``plan_execution`` is supplied (search-execution-driven
        claim), the wanted->downloading flip and the plan-currentness
        check happen in a single atomic UPDATE
        (``set_downloading_if_plan_current``). This eliminates the
        TOCTOU window where a regenerate could land between a separate
        currentness probe and the status flip.

        Stale completions (the request was regenerated mid-flight after
        this search was accepted) skip the claim with a
        STALE_DOWNLOAD_CLAIM log.

        Stale-completion contract: log against the executed old plan
        (handled by ``_log_search_result``); do NOT mutate active request
        status.
        """
        db = self._open_db()
        try:
            if plan_execution is not None:
                claimed = bool(db.set_downloading_if_plan_current(
                    request_id,
                    state_json,
                    plan_id=plan_execution.plan_id,
                    plan_ordinal=plan_execution.plan_ordinal,
                    cycle_count_snapshot=plan_execution.cycle_count_snapshot,
                ))
                if not claimed:
                    logger.warning(
                        "STALE_DOWNLOAD_CLAIM request_id=%s plan_id=%s "
                        "ordinal=%s cycle=%s; request was regenerated "
                        "mid-flight or already non-wanted, skipping "
                        "wanted->downloading claim",
                        request_id,
                        plan_execution.plan_id,
                        plan_execution.plan_ordinal,
                        plan_execution.cycle_count_snapshot,
                    )
                return claimed
            return transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_downloading(
                    from_status="wanted",
                    state_json=state_json,
                ),
            )
        finally:
            self._close_db(db)

    def reset_after_no_acceptance(self, request_id: int) -> bool:
        """Guarded downloading -> wanted reset for verified no-acceptance."""
        db = self._open_db()
        try:
            return transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_wanted(
                    from_status="downloading",
                    attempt_type="download",
                ),
            )
        finally:
            self._close_db(db)

    def update_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
    ) -> bool:
        """Guard active_download_state enrichment after slskd returns IDs."""
        db = self._open_db()
        try:
            return bool(
                db.update_download_state_if_downloading(request_id, state_json)
            )
        finally:
            self._close_db(db)

    def record_transfer_enqueue(self, rows: "list[TransferLedgerRow]") -> None:
        """Write-ahead ownership ledger insert (issue #571, T1) using a
        fresh DB handle -- same worker-safety rationale as every other
        method here: find_download workers cannot reach the owner
        thread's cached connection, so every call site (worker or the
        sequential poll loop alike) goes through this collaborator
        uniformly rather than threading the owner connection down.
        """
        if not rows:
            return
        db = self._open_db()
        try:
            db.record_transfer_enqueue(rows)
        finally:
            self._close_db(db)

    def stamp_transfer_ids(
        self, username: str, pairs: list[tuple[str, str]],
    ) -> int:
        """Enqueue-response ownership write (issue #571 PR 5, T1.5) --
        same worker-safety rationale as ``record_transfer_enqueue``, and
        the same one-handle-per-batch shape: an enqueue reconciles every
        file's transfer id at once, so a 20-track album is one connection,
        not twenty. Called right after ``slskd_enqueue_with_outcome``
        reconciles a POST's transfer ids, so the rows T1 just inserted
        carry them before the purge flip ever needs to match a completed
        transfer back to its ledger row. ``pairs`` is
        ``[(filename, transfer_id), ...]``; returns total rows stamped.
        """
        db = self._open_db()
        try:
            return sum(
                db.stamp_transfer_id(username, filename, transfer_id)
                for filename, transfer_id in pairs
            )
        finally:
            self._close_db(db)
