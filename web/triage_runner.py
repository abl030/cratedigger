"""Background runner for the bulk Wrong Matches triage sweep.

A synchronous bulk triage sweep (minutes when stale rows trigger
re-measurement, see #271) held its request thread for the whole run and
made the UI unresponsive. The POST handler now starts the sweep here and
returns immediately; the UI — and, since issue #1063,
``pipeline-cli wrong-match-triage`` — polls the status endpoint for the
summary. Issue #1083 added ``cancel()``: it sets a per-sweep
``CancellationToken`` that ``cleanup_all_wrong_matches`` checks between
rows (never mid-delete), so a still-running sweep can be stopped instead
of merely detached from.

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
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, TypedDict

from lib.import_execution import CancellationToken

logger = logging.getLogger("cratedigger")

STATE_IDLE = "idle"
STATE_RUNNING = "running"
STATE_COMPLETED = "completed"
STATE_CANCELLED = "cancelled"
STATE_FAILED = "failed"


class TriageStatusSnapshot(TypedDict):
    """The JSON-serializable shape ``status()``/``cancel()`` return.

    ``summary`` is the sweep's own ``WrongMatchCleanupSummary.to_dict()``
    output (already ``dict[str, object]`` — no further attribute access
    happens on it here, so ``object`` covers it without reaching for
    ``Any``)."""

    state: str
    started_at: str | None
    finished_at: str | None
    summary: dict[str, object] | None
    error: str | None


class TriageRunner:
    """Owns at most one background bulk-triage sweep at a time."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._state: str = STATE_IDLE
        self._summary: dict[str, object] | None = None
        self._error: str | None = None
        self._started_at: str | None = None
        self._finished_at: str | None = None
        self._token: CancellationToken | None = None

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
            token = CancellationToken()
            self._token = token
            self._thread = threading.Thread(
                target=self._run,
                args=(db_factory, cleanup_fn, token),
                name="wrong-match-triage",
                daemon=True,
            )
            self._thread.start()
        return True

    def status(self) -> TriageStatusSnapshot:
        """Snapshot of the current sweep state for the status endpoint."""
        with self._lock:
            return self._status_locked()

    def cancel(self, reason: str = "operator_requested") -> TriageStatusSnapshot:
        """Request cancellation of the in-flight sweep, if any.

        Not an error when idle or already finished (#1083 invariant): a
        cancel with nothing running, and a cancel racing a sweep that is
        about to record its own terminal state, both just return the
        same snapshot ``status()`` would — never a 409. Cancellation
        itself is observed between rows inside the cleanup service, never
        mid-delete, so this can only ever stop the NEXT row, not the one
        in flight.
        """
        with self._lock:
            token = self._token
            if token is not None and self._state == STATE_RUNNING:
                token.cancel(reason)
            return self._status_locked()

    def _status_locked(self) -> TriageStatusSnapshot:
        """Build the status snapshot; caller already holds ``self._lock``."""
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
        token: CancellationToken,
    ) -> None:
        db: Any = None
        try:
            db = db_factory()
            summary = cleanup_fn(
                db, confirm_all_wrong_matches=True, cancellation_token=token,
            )
            with self._lock:
                self._state = (
                    STATE_CANCELLED if summary.cancelled else STATE_COMPLETED
                )
                self._summary = summary.to_dict()
                self._finished_at = _utcnow_iso()
            logger.info(
                "wrong_match_triage_sweep.%s",
                "cancelled" if summary.cancelled else "completed",
            )
        except Exception as exc:
            logger.exception("wrong_match_triage_sweep.failed")
            with self._lock:
                self._state = STATE_FAILED
                self._error = f"{type(exc).__name__}: {exc}"
                self._finished_at = _utcnow_iso()
        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    logger.exception(
                        "wrong_match_triage_sweep.db_close_failed",
                    )


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()
