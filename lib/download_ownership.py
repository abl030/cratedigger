"""Worker-safe ownership writes for newly enqueued downloads."""

from __future__ import annotations

import logging
from typing import Any, Callable

from lib import transitions

logger = logging.getLogger("cratedigger")


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
        db_factory: Callable[[], Any] | None = None,
        close_after_use: bool | None = None,
    ) -> None:
        self.dsn = dsn
        self._db_factory = db_factory
        self._close_after_use = (
            db_factory is None if close_after_use is None else close_after_use
        )

    def _open_db(self) -> Any:
        if self._db_factory is not None:
            return self._db_factory()
        from lib.pipeline_db import PipelineDB

        return PipelineDB(self.dsn)

    def _close_db(self, db: Any) -> None:
        if not self._close_after_use:
            return
        close = getattr(db, "close", None)
        if close is not None:
            close()

    def claim_downloading(self, request_id: int, state_json: str) -> bool:
        """Guarded wanted -> downloading claim with planned download state."""
        db = self._open_db()
        try:
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
            update = getattr(db, "update_download_state_if_downloading", None)
            if update is None:
                row = db.get_request(request_id)
                if not row or row.get("status") != "downloading":
                    logger.warning(
                        "download ownership state update blocked for request %s: "
                        "request is no longer downloading",
                        request_id,
                    )
                    return False
                db.update_download_state(request_id, state_json)
                return True
            return bool(update(request_id, state_json))
        finally:
            self._close_db(db)
