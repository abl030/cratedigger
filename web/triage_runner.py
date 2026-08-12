"""Background runner for the bulk Wrong Matches triage sweep.

A synchronous bulk triage sweep (minutes when stale rows trigger
re-measurement, see #271) held its request thread for the whole run and
made the UI unresponsive. The POST handler now starts the sweep here and
returns immediately; the UI — and, since issue #1063,
``pipeline-cli wrong-match-triage`` — polls the status endpoint for the
summary. Issue #1083 added ``cancel()``: it sets a per-sweep
``CancellationToken`` that ``cleanup_all_wrong_matches`` checks between
rows (never mid-delete), so a still-running sweep can be stopped instead
of merely detached from. Issue #1106 made that cancel STICKY: a cancel
arriving while ``start()`` has not yet flipped the state to RUNNING (the
CLI's own ``Ctrl-C`` handler racing its still-in-flight start POST) is
recorded as a pending cancel and consumed by the next ``start()``
admitted within ``PENDING_CANCEL_WINDOW_SECONDS`` — that sweep is still
admitted (never a refusal), but its token is cancelled before any row
runs, so it terminates ``cancelled`` with zero rows processed.

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
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol, TypedDict

from lib.import_execution import CancellationToken
from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary

logger = logging.getLogger("cratedigger")

# Bounded consume window for a cancel recorded while no sweep is running
# (#1106). This exists ONLY to cover the CLI's own Ctrl-C racing its
# still-in-flight start POST -- a window of ordinary HTTP request-handling
# latency (milliseconds), not minutes. Ten seconds is generous slack above
# that (a contended lock or a GC pause on the request thread is still
# covered) while staying far short of the sweep's own multi-minute
# runtime, and short enough that an operator's cancel of an
# ALREADY-FINISHED sweep does not silently poison whatever they start
# next, unrelated, minutes later.
PENDING_CANCEL_WINDOW_SECONDS = 10.0


class _ClosableDB(Protocol):
    """The only fact ``TriageRunner`` itself needs about the sweep's own
    DB handle: it can be closed once the sweep finishes. Everything else
    about the handle is opaque here and passed straight through to
    ``cleanup_fn``, which owns the real narrow contract
    (``lib.wrong_match_cleanup_service.WrongMatchCleanupDB``) — this
    layer never calls another method on it."""

    def close(self) -> None: ...


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

    def __init__(self, *, now_fn: Callable[[], float] = time.monotonic) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._state: str = STATE_IDLE
        self._summary: dict[str, object] | None = None
        self._error: str | None = None
        self._started_at: str | None = None
        self._finished_at: str | None = None
        self._token: CancellationToken | None = None
        # #1106 sticky cancel: kwarg-DI seam over the wall clock so tests
        # control the pending-cancel window deterministically. Production
        # never passes this; the default is the real monotonic clock.
        self._now_fn = now_fn
        self._pending_cancel_at: float | None = None
        self._pending_cancel_reason: str | None = None

    def start(
        self,
        *,
        db_factory: Callable[[], _ClosableDB],
        cleanup_fn: Callable[..., WrongMatchCleanupSummary],
    ) -> bool:
        """Start a sweep on a background thread.

        Returns False (and starts nothing) when a sweep is already
        running. ``db_factory`` is called ON the sweep thread so the
        connection is created and used by one thread only. When a
        pending cancel (#1106) is still within its window, the sweep is
        still admitted (still True, still starts a thread) but its
        token is pre-cancelled before ``cleanup_fn`` ever runs.
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
            pending_reason = self._consume_pending_cancel_locked()
            if pending_reason is not None:
                token.cancel(pending_reason)
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
        same snapshot ``status()`` would — never a 409. Cancellation of
        a RUNNING sweep is observed between rows inside the cleanup
        service, never mid-delete, so this can only ever stop the NEXT
        row, not the one in flight.

        A cancel that lands while nothing is RUNNING (idle, or between
        two sweeps) is STICKY (#1106): it is recorded as a pending
        cancel, first-cancel-wins if one is already pending, and
        ``start()`` consumes it if admitted within
        ``PENDING_CANCEL_WINDOW_SECONDS``. This is what makes the CLI's
        Ctrl-C-races-its-own-start-POST window safe — without it, that
        cancel would silently no-op and the sweep would run to
        completion unstoppable by that invocation.
        """
        with self._lock:
            token = self._token
            if token is not None and self._state == STATE_RUNNING:
                token.cancel(reason)
            elif self._pending_cancel_at is None:
                self._pending_cancel_at = self._now_fn()
                self._pending_cancel_reason = reason
            return self._status_locked()

    def _consume_pending_cancel_locked(self) -> str | None:
        """Caller already holds ``self._lock``. A pending cancel does not
        survive past this call either way: it is cleared unconditionally,
        and its reason is returned only when it is still within
        ``PENDING_CANCEL_WINDOW_SECONDS`` -- an expired one is discarded
        and constrains nothing."""
        pending_at = self._pending_cancel_at
        reason = self._pending_cancel_reason
        self._pending_cancel_at = None
        self._pending_cancel_reason = None
        if pending_at is None:
            return None
        if self._now_fn() - pending_at > PENDING_CANCEL_WINDOW_SECONDS:
            return None
        return reason

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
        db_factory: Callable[[], _ClosableDB],
        cleanup_fn: Callable[..., WrongMatchCleanupSummary],
        token: CancellationToken,
    ) -> None:
        db: _ClosableDB | None = None
        try:
            db = db_factory()
            summary = cleanup_fn(
                db, confirm_all_wrong_matches=True, cancellation_token=token,
            )
            # Compute the whole terminal payload BEFORE taking the lock
            # or writing any state. If ``to_dict()`` raised after
            # ``self._state`` had already been written, the ``except``
            # below would overwrite a genuinely cancelled/completed
            # sweep with STATE_FAILED and leave ``_summary`` at None --
            # a cancelled sweep surfacing as failed with no partial
            # summary, exactly what this issue exists to prevent.
            state = STATE_CANCELLED if summary.cancelled else STATE_COMPLETED
            summary_dict = summary.to_dict()
            with self._lock:
                self._state = state
                self._summary = summary_dict
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
