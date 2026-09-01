"""Background runner for the bulk Wrong Matches triage sweep.

A synchronous bulk triage sweep (minutes when stale rows trigger
re-measurement, see #271) held its request thread for the whole run and
made the UI unresponsive. The POST handler now starts the sweep here and
returns immediately; the UI — and, since issue #1063,
``pipeline-cli wrong-match-triage`` — polls the status endpoint for the
summary. Issue #1083 added ``cancel()``: it sets a per-sweep
``CancellationToken`` that ``cleanup_all_wrong_matches`` checks between
rows (never mid-delete), so a still-running sweep can be stopped instead
of merely detached from. Issue #1106 made that cancel STICKY, but only
when explicitly ARMED: a cancel arriving while ``start()`` has not yet
flipped the state to RUNNING is recorded as a pending cancel ONLY when
the caller passes ``arm_pending=True`` — reserved for the CLI's own
``Ctrl-C`` handler, which is specifically racing its own still-in-flight
start POST. Every other caller (the web UI's Stop button, the standalone
``wrong-match-triage-cancel`` command) defaults unarmed: a cancel with
nothing running is a pure #1083 no-op and never affects a sweep it did
not itself observe running — an armed cancel that DID arm is consumed by
the next ``start()`` admitted within ``PENDING_CANCEL_WINDOW_SECONDS``;
that sweep is still admitted (never a refusal), but its token is
cancelled before any row runs, so it terminates ``cancelled`` with zero
rows processed. Every ARMED cancel refreshes the pending timestamp
(latest-wins) — review round 1 (F1) found that a first-wins slot lets a
later, genuinely-in-window armed cancel be silently dropped once an
earlier one had gone stale-but-uncleared.

The sweep thread gets its pipeline handle by entering ``db_session`` on
that thread — ``web/runtime.py::WebRuntime.open_background_db``, which
opens a connection of its own under a DSN so nothing is shared with the
handler thread. Handle ownership stays there: this module enters and
exits the session and never closes anything itself.

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
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from typing import TypedDict

from lib.import_execution import CancellationToken
from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary

logger = logging.getLogger("cratedigger")

# Bounded consume window for an ARMED cancel recorded while no sweep is
# running (#1106). Only the CLI's own Ctrl-C handler ever arms
# (#1106 F3) -- an unarmed cancel (the web UI's Stop button, the
# standalone wrong-match-triage-cancel command) never touches this
# window at all, so the ALREADY-FINISHED-sweep hazard the window used
# to be justified against no longer applies to those callers; the arm
# gate itself is what removed it, not the window's length.
#
# What the window still bounds, now that only one caller can ever arm:
# the CLI's Ctrl-C can land not just while its OWN start POST is still
# in flight (milliseconds), but also in the up-to-~2s gap between the
# sweep finishing server-side and the CLI's own poll loop next
# observing that (`_TRIAGE_POLL_INTERVAL_SECONDS`) -- Ctrl-C during
# that gap is caught by the exact same handler and sends the exact
# same armed cancel, even though nothing is running anymore. Ten
# seconds is generous slack above that (a contended lock or a GC pause
# on the request thread is still covered) while staying far short of
# the sweep's own multi-minute runtime. The residual: a fresh
# `wrong-match-triage --apply` re-run within the window lands
# pre-cancelled -- but VISIBLY (state `cancelled`, zero rows
# processed, exit 5), never silently, so the operator sees exactly
# what happened and can just run it again once the window has passed.
# Repeated armed cancels DO slide this window forward (latest-wins,
# #1106 F1) -- bounded in practice, since only the CLI ever arms and
# its own retry-on-failure logic sends at most two POSTs per Ctrl-C.
PENDING_CANCEL_WINDOW_SECONDS = 10.0


#: Opens the sweep's pipeline handle for the duration of one sweep.
#: The handle itself is opaque here and passed straight through to
#: ``cleanup_fn``, which owns the real narrow contract
#: (``lib.wrong_match_cleanup_service.WrongMatchCleanupDB``); this layer
#: never touches it, and closing it is the session's own business.
type SweepDbSession = Callable[[], AbstractContextManager[object]]

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
        db_session: SweepDbSession,
        cleanup_fn: Callable[..., WrongMatchCleanupSummary],
    ) -> bool:
        """Start a sweep on a background thread.

        Returns False (and starts nothing) when a sweep is already
        running. ``db_session`` is entered ON the sweep thread so the
        connection is opened, used, and released by one thread only, and
        it is exited on every terminal path, a raising ``cleanup_fn``
        included. The exit happens BEFORE the terminal state is
        recorded, so a session whose own exit raises marks the sweep
        failed rather than completed; production's session
        (``WebRuntime.open_background_db``) swallows its own close
        errors, so it cannot reach that, but an injected one can.

        When a pending cancel (#1106) is still within its window, the
        sweep is still admitted (still True, still starts a thread) but
        its token is pre-cancelled before ``cleanup_fn`` ever runs.
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
                args=(db_session, cleanup_fn, token),
                name="wrong-match-triage",
                daemon=True,
            )
            self._thread.start()
        return True

    def status(self) -> TriageStatusSnapshot:
        """Snapshot of the current sweep state for the status endpoint."""
        with self._lock:
            return self._status_locked()

    def cancel(
        self, reason: str = "operator_requested", *, arm_pending: bool = False,
    ) -> TriageStatusSnapshot:
        """Request cancellation of the in-flight sweep, if any.

        Not an error when idle or already finished (#1083 invariant): a
        cancel with nothing running, and a cancel racing a sweep that is
        about to record its own terminal state, both just return the
        same snapshot ``status()`` would — never a 409. Cancellation of
        a RUNNING sweep is observed between rows inside the cleanup
        service, never mid-delete, so this can only ever stop the NEXT
        row, not the one in flight. This branch is UNCONDITIONAL,
        unaffected by ``arm_pending``.

        A cancel that lands while nothing is RUNNING only arms a sticky
        pending cancel (#1106) when ``arm_pending=True`` (#1106 F3) —
        reserved for the CLI's own ``Ctrl-C`` handler, which is
        specifically racing its own still-in-flight start POST. Every
        other caller defaults unarmed and is a pure #1083 no-op here: it
        must never affect a sweep it did not itself observe running (a
        Stop click on a sweep that just finished, or the documented
        ``wrong-match-triage-cancel`` recovery sequence, must not
        silently pre-cancel the NEXT, unrelated sweep). An armed cancel
        ALWAYS overwrites the pending timestamp — latest-wins, never
        first-wins (#1106 F1): a stale, already-armed-but-unconsumed
        slot must not block a later, genuinely-in-window armed cancel
        from being recorded. ``start()`` consumes a non-expired pending
        cancel if admitted within ``PENDING_CANCEL_WINDOW_SECONDS``.
        """
        with self._lock:
            token = self._token
            if token is not None and self._state == STATE_RUNNING:
                token.cancel(reason)
            elif arm_pending:
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
        db_session: SweepDbSession,
        cleanup_fn: Callable[..., WrongMatchCleanupSummary],
        token: CancellationToken,
    ) -> None:
        try:
            with db_session() as db:
                summary = cleanup_fn(
                    db,
                    confirm_all_wrong_matches=True,
                    cancellation_token=token,
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
            self._record_failure(exc)
        except BaseException as exc:
            # A KeyboardInterrupt or SystemExit reaching this thread
            # used to leave the runner at RUNNING for the life of the
            # process: every later start() refused, the only way out a
            # web restart. That is the parked state invariant 11
            # forbids, so record the same terminal outcome before
            # letting it propagate.
            logger.exception("wrong_match_triage_sweep.failed")
            self._record_failure(exc)
            raise

    def _record_failure(self, exc: BaseException) -> None:
        with self._lock:
            self._state = STATE_FAILED
            self._error = f"{type(exc).__name__}: {exc}"
            self._finished_at = _utcnow_iso()


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()
