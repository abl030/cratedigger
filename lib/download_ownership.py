"""Worker-safe ownership writes for newly enqueued downloads."""

from __future__ import annotations

import logging
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Protocol, runtime_checkable

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
        self,
        request_id: int,
        state_json: str,
        *,
        expected_enqueued_at: str,
    ) -> bool: ...

    def record_transfer_enqueue(self, rows: list[TransferLedgerRow]) -> None: ...

    def confirm_transfer_enqueue(
        self, username: str, filename: str,
    ) -> int: ...

    def get_conflicting_transfer_request_ids(
        self,
        keys: Sequence[tuple[str, str]],
        exclude_request_id: int,
    ) -> set[int]: ...

    def get_owned_transfer_keys_for(
        self,
        keys: Sequence[tuple[str, str]],
    ) -> set[tuple[str, str]]: ...

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
        plan_execution: PlanExecutionContext | None = None,
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
            result = transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_downloading(
                    from_status="wanted",
                    state_json=state_json,
                ),
            )
            return isinstance(result, transitions.TransitionApplied)
        finally:
            self._close_db(db)

    def reset_after_no_acceptance(self, request_id: int) -> bool:
        """Guarded downloading -> wanted reset for verified no-acceptance."""
        db = self._open_db()
        try:
            result = transitions.finalize_request(
                db,
                request_id,
                transitions.RequestTransition.to_wanted(
                    from_status="downloading",
                    attempt_type="download",
                ),
            )
            return isinstance(result, transitions.TransitionApplied)
        finally:
            self._close_db(db)

    def update_state_if_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_enqueued_at: str,
    ) -> bool:
        """Guard state enrichment with an independently held attempt witness."""
        db = self._open_db()
        try:
            return bool(
                db.update_download_state_if_downloading(
                    request_id,
                    state_json,
                    expected_enqueued_at=expected_enqueued_at,
                )
            )
        finally:
            self._close_db(db)

    def record_transfer_enqueue(self, rows: list[TransferLedgerRow]) -> None:
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

    def confirm_transfer_enqueues(
        self, username: str, filenames: list[str],
    ) -> int:
        """Confirm one accepted POST's write-ahead rows using one DB handle."""
        if not filenames:
            return 0
        db = self._open_db()
        try:
            return sum(
                db.confirm_transfer_enqueue(username, filename)
                for filename in filenames
            )
        finally:
            self._close_db(db)

    def owned_transfer_keys(
        self, keys: Sequence[tuple[str, str]],
    ) -> set[tuple[str, str]]:
        """Which of these slskd queue keys the ledger proves are ours.

        The read half of the write-ahead ownership ledger, exposed on the
        same worker-safe collaborator that writes it: the destructive
        paths in ``lib/slskd_transfers.py`` run on find_download worker
        threads, which cannot reach the owner thread's cached
        ``pipeline_db_source`` connection
        (``lib.enqueue._WorkerPipelineDBSource`` raises on access).

        Only an accepted POST enters the returned set. A pending
        write-ahead intent is deliberately absent: it records that we
        ASKED, never that slskd agreed, so it is not authority to destroy
        anything.
        """
        if not keys:
            return set()
        db = self._open_db()
        try:
            return db.get_owned_transfer_keys_for(keys)
        finally:
            self._close_db(db)

    @contextmanager
    def open_conflict_check_session(
        self,
    ) -> Generator[Callable[[Sequence[tuple[str, str]], int], set[int]]]:
        """Open ONE fresh DB handle covering every cross-cycle guard
        check in one ``try_enqueue`` / ``try_multi_enqueue`` invocation
        (issue #1178 PR2 review F7).

        The guard runs before the peer-online probe, for every matched
        candidate, across a worker pool sized to the whole cycle --
        opening a fresh connection per CANDIDATE (as an earlier version
        of this method did) risks a transient connection storm at
        post-browse convergence. Sharing one handle across every guard
        check in the invocation is safe because ``try_enqueue`` /
        ``try_multi_enqueue`` each run on a single worker thread for
        their whole call -- same worker-safety rationale as every other
        method here (find_download workers cannot reach the owner
        thread's cached ``pipeline_db_source`` connection;
        ``lib.enqueue._WorkerPipelineDBSource`` raises on access), just
        opened once per call instead of once per operation.
        """
        db = self._open_db()
        try:
            def check(
                keys: Sequence[tuple[str, str]], exclude_request_id: int,
            ) -> set[int]:
                if not keys:
                    return set()
                return db.get_conflicting_transfer_request_ids(
                    keys, exclude_request_id)
            yield check
        finally:
            self._close_db(db)
