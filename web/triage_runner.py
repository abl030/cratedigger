"""Background runner for the bulk Wrong Matches triage sweep.

The web server is a single-threaded ``http.server`` — a synchronous bulk
triage sweep (minutes when stale rows trigger re-measurement, see #271)
held the only request thread and made the whole UI unresponsive. The POST
handler now starts the sweep here and returns immediately; the UI polls
the status endpoint for the summary.

The sweep thread gets its OWN pipeline-DB connection from ``db_factory``
— psycopg2 connections must not be shared between the handler thread and
the sweep thread.

In-memory state only: a web-service restart aborts the sweep and resets
the status to idle, which matches the old synchronous behaviour (the
sweep died with the process there too) and is fine for the single
operator. Per-row deletions stay protected by the WMCL advisory lock
inside the cleanup service itself.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Callable

logger = logging.getLogger("cratedigger")

STATE_IDLE = "idle"
STATE_RUNNING = "running"
STATE_COMPLETED = "completed"
STATE_FAILED = "failed"


class TriageRunner:
    """Owns at most one background bulk-triage sweep at a time."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._state: str = STATE_IDLE
        self._summary: dict[str, Any] | None = None
        self._error: str | None = None
        self._started_at: str | None = None
        self._finished_at: str | None = None

    def start(
        self,
        *,
        db_factory: Callable[[], Any],
        cleanup_fn: Callable[..., Any],
    ) -> bool:
        """Start a sweep on a background thread.

        Returns False (and starts nothing) when a sweep is already
        running. ``db_factory`` is called ON the sweep thread so the
        connection is created and used by one thread only.
        """
        with self._lock:
            if self._state == STATE_RUNNING:
                return False
            self._state = STATE_RUNNING
            self._summary = None
            self._error = None
            self._started_at = _utcnow_iso()
            self._finished_at = None
            self._thread = threading.Thread(
                target=self._run,
                args=(db_factory, cleanup_fn),
                name="wrong-match-triage",
                daemon=True,
            )
            self._thread.start()
        return True

    def status(self) -> dict[str, Any]:
        """Snapshot of the current sweep state for the status endpoint."""
        with self._lock:
            return {
                "state": self._state,
                "started_at": self._started_at,
                "finished_at": self._finished_at,
                "summary": self._summary,
                "error": self._error,
            }

    def join(self, timeout: float | None = None) -> None:
        """Wait for the in-flight sweep thread (tests / shutdown)."""
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)

    def _run(
        self,
        db_factory: Callable[[], Any],
        cleanup_fn: Callable[..., Any],
    ) -> None:
        db: Any = None
        try:
            db = db_factory()
            summary = cleanup_fn(db, confirm_all_wrong_matches=True)
            with self._lock:
                self._state = STATE_COMPLETED
                self._summary = summary.to_dict()
                self._finished_at = _utcnow_iso()
            logger.info("wrong_match_triage_sweep.completed")
        except Exception as exc:  # noqa: BLE001
            logger.exception("wrong_match_triage_sweep.failed")
            with self._lock:
                self._state = STATE_FAILED
                self._error = f"{type(exc).__name__}: {exc}"
                self._finished_at = _utcnow_iso()
        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "wrong_match_triage_sweep.db_close_failed",
                    )


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
