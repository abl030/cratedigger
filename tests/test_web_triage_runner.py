"""Tests for the background Wrong Matches bulk-triage runner."""

from __future__ import annotations

import threading
import time
import unittest
from contextlib import closing, contextmanager
from unittest.mock import patch

from lib.import_execution import CancellationToken
from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary
from web.triage_runner import (
    PENDING_CANCEL_WINDOW_SECONDS,
    STATE_CANCELLED,
    STATE_COMPLETED,
    STATE_FAILED,
    STATE_IDLE,
    STATE_RUNNING,
    TriageRunner,
)


class _ClosableDB:
    """Minimal sweep-DB stand-in recording close() calls.

    Entered through ``contextlib.closing`` below, which is exactly what
    ``WebRuntime.open_background_db`` does for a handle it opened: yield
    it, then close it when the block ends.
    """

    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


class _FakeClock:
    """Deterministic stand-in for ``TriageRunner``'s ``now_fn`` kwarg-DI
    seam (production default ``time.monotonic``) — issue #1106."""

    def __init__(self, start: float = 0.0) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value

    def advance(self, delta: float) -> None:
        self.value += delta


class TestPendingCancelWindowConstant(unittest.TestCase):
    """Every cancellation test below exercises this window only relative to
    itself (``/ 2``, ``+ 1.0``), never pins its own value — a changed window
    moves when a re-armed triage run lands pre-cancelled instead of
    executing and must be a deliberate edit.
    """

    def test_value(self) -> None:
        self.assertEqual(PENDING_CANCEL_WINDOW_SECONDS, 10.0)


class TriageRunnerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = TriageRunner()
        self.db = _ClosableDB()

    def _session(self):
        return closing(self.db)

    def test_initial_status_is_idle(self) -> None:
        status = self.runner.status()
        self.assertEqual(status["state"], STATE_IDLE)
        self.assertIsNone(status["started_at"])
        self.assertIsNone(status["finished_at"])
        self.assertIsNone(status["summary"])
        self.assertIsNone(status["error"])

    def test_start_runs_cleanup_to_completion(self) -> None:
        seen: dict[str, object] = {}

        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            seen["db"] = db
            seen["confirm"] = confirm_all_wrong_matches
            seen["token"] = cancellation_token
            return WrongMatchCleanupSummary(processed=3, deleted=2,
                                            kept_uncertain=1)

        started = self.runner.start(
            db_session=self._session, cleanup_fn=cleanup_fn,
        )
        self.assertTrue(started)
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_COMPLETED)
        self.assertIsNotNone(status["started_at"])
        self.assertIsNotNone(status["finished_at"])
        self.assertIsNone(status["error"])
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 3)
        self.assertEqual(summary["deleted"], 2)
        self.assertIs(seen["db"], self.db)
        self.assertTrue(seen["confirm"])
        self.assertIsInstance(seen["token"], CancellationToken)
        self.assertEqual(self.db.closed, 1)

    def test_summary_payload_is_computed_before_the_lock_is_taken(self) -> None:
        """#1083 review: ``to_dict()`` must run BEFORE ``self._lock`` is
        acquired for the terminal write. If the payload were instead
        computed INSIDE the lock-held block (state written first, then
        ``to_dict()``), a raise partway through would leave
        ``self._state`` already CANCELLED/COMPLETED while ``_summary``
        stayed ``None`` -- then the ``except`` handler overwrites state
        to FAILED, briefly exposing a cancelled/completed sweep with no
        summary to a concurrent ``status()`` poll. Structural regression
        guard: if the payload were computed inside the lock, this would
        observe the lock already held."""
        observed_locked: list[bool] = []
        real_to_dict = WrongMatchCleanupSummary.to_dict

        def spy_to_dict(self_summary):
            observed_locked.append(self.runner._lock.locked())
            return real_to_dict(self_summary)

        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            # A brief yield so ``start()``'s OWN initial lock hold
            # (acquired around ``self._thread.start()``, unrelated to
            # the terminal-write lock this test targets) has certainly
            # been released before ``_run`` reaches ``to_dict()`` --
            # otherwise a fast synchronous fake races the caller thread
            # and observes the wrong lock hold.
            time.sleep(0.05)
            return WrongMatchCleanupSummary(processed=1, deleted=1)

        with patch.object(WrongMatchCleanupSummary, "to_dict", spy_to_dict):
            self.assertTrue(self.runner.start(
                db_session=self._session, cleanup_fn=cleanup_fn,
            ))
            self.runner.join(timeout=5)

        self.assertEqual(observed_locked, [False])
        self.assertEqual(self.runner.status()["state"], STATE_COMPLETED)

    def test_second_start_rejected_while_running(self) -> None:
        release = threading.Event()
        entered = threading.Event()

        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            entered.set()
            release.wait(timeout=5)
            return WrongMatchCleanupSummary(processed=0)

        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=cleanup_fn,
        ))
        self.assertTrue(entered.wait(timeout=5))
        self.assertEqual(self.runner.status()["state"], STATE_RUNNING)
        self.assertFalse(self.runner.start(
            db_session=self._session, cleanup_fn=cleanup_fn,
        ))

        release.set()
        self.runner.join(timeout=5)
        self.assertEqual(self.runner.status()["state"], STATE_COMPLETED)

        # A finished runner accepts the next sweep.
        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=cleanup_fn,
        ))
        self.runner.join(timeout=5)

    def test_cleanup_failure_records_error_and_closes_db(self) -> None:
        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            raise RuntimeError("sweep blew up")

        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=cleanup_fn,
        ))
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_FAILED)
        self.assertIn("RuntimeError", str(status["error"]))
        self.assertIn("sweep blew up", str(status["error"]))
        self.assertIsNone(status["summary"])
        self.assertEqual(self.db.closed, 1)

    def test_db_session_failure_records_error(self) -> None:
        def bad_session():
            raise RuntimeError("no dsn")

        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            raise AssertionError("must not run without a db")

        self.assertTrue(self.runner.start(
            db_session=bad_session, cleanup_fn=cleanup_fn,
        ))
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_FAILED)
        self.assertIn("no dsn", str(status["error"]))
        # The status endpoint renders finished_at on every terminal
        # state, so the failure path owes one too.
        self.assertIsNotNone(status["finished_at"])

    def test_a_session_whose_exit_fails_marks_the_sweep_failed(self) -> None:
        """The exit runs before the terminal state is written.

        Production's session swallows its own close errors, so it cannot
        reach this; an injected session can, and the contract should be
        the one ``start`` documents rather than an accident of ordering.
        """
        @contextmanager
        def exit_fails():
            yield self.db
            raise RuntimeError("close blew up")

        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            return WrongMatchCleanupSummary(processed=7, deleted=0)

        self.assertTrue(self.runner.start(
            db_session=exit_fails, cleanup_fn=cleanup_fn,
        ))
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_FAILED)
        self.assertIn("close blew up", str(status["error"]))
        self.assertIsNotNone(status["finished_at"])
        # The cost of that ordering, pinned so it stays visible: seven
        # rows genuinely ran and the operator is shown nothing.
        self.assertIsNone(status["summary"])

    def test_a_base_exception_still_records_a_terminal_state(self) -> None:
        """Nothing is parked: a sweep killed by a KeyboardInterrupt on
        its own thread must not leave the runner RUNNING forever, which
        would refuse every later start until the web process restarted.
        """
        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            raise KeyboardInterrupt("operator interrupted the sweep thread")

        # The re-raise reaches threading's excepthook, which would print
        # a traceback into every green suite log. Swallow it here and
        # assert it fired, which also pins that the exception really is
        # re-raised rather than absorbed.
        escaped: list[type[BaseException] | None] = []
        default_hook = threading.excepthook
        threading.excepthook = lambda args: escaped.append(args.exc_type)
        self.addCleanup(setattr, threading, "excepthook", default_hook)

        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=cleanup_fn,
        ))
        self.runner.join(timeout=5)

        self.assertEqual(escaped, [KeyboardInterrupt])
        status = self.runner.status()
        self.assertEqual(status["state"], STATE_FAILED)
        self.assertIn("KeyboardInterrupt", str(status["error"]))
        self.assertIsNotNone(status["finished_at"])
        # And the runner is usable again rather than wedged.
        self.assertTrue(self.runner.start(
            db_session=self._session,
            cleanup_fn=lambda db, **kwargs: WrongMatchCleanupSummary(processed=0),
        ))
        self.runner.join(timeout=5)
        self.assertEqual(self.runner.status()["state"], STATE_COMPLETED)

    def test_a_refused_thread_spawn_does_not_leave_the_runner_running(
        self,
    ) -> None:
        """``start`` writes RUNNING before it spawns.

        A refused spawn (``RuntimeError: can't start new thread``, which
        a ThreadingHTTPServer under thread pressure can genuinely reach)
        otherwise leaves that RUNNING with nothing to clear it: every
        later sweep refused, and ``join`` raising "cannot join thread
        before it is started" for the life of the process.
        """
        with (
            patch.object(
                threading.Thread,
                "start",
                side_effect=RuntimeError("can't start new thread"),
            ),
            self.assertRaisesRegex(RuntimeError, "can't start new thread"),
        ):
            self.runner.start(
                db_session=self._session, cleanup_fn=self._never_runs,
            )

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_FAILED)
        self.assertIn("can't start new thread", str(status["error"]))
        self.assertIsNotNone(status["finished_at"])
        # Not wedged: join is safe and the next sweep is admitted.
        self.runner.join(timeout=1)
        self.assertTrue(self.runner.start(
            db_session=self._session,
            cleanup_fn=lambda db, **kwargs: WrongMatchCleanupSummary(processed=0),
        ))
        self.runner.join(timeout=5)
        self.assertEqual(self.runner.status()["state"], STATE_COMPLETED)

    @staticmethod
    def _never_runs(db, *, confirm_all_wrong_matches, cancellation_token=None):
        raise AssertionError("the sweep must not run when the spawn failed")


class TriageRunnerCancellationTest(unittest.TestCase):
    """Issue #1083: TriageRunner.cancel() and the STATE_CANCELLED terminal
    state, distinct from STATE_COMPLETED and STATE_FAILED."""

    def setUp(self) -> None:
        self.runner = TriageRunner()
        self.db = _ClosableDB()

    def _session(self):
        return closing(self.db)

    def test_cancel_with_no_sweep_running_is_not_an_error(self) -> None:
        """Cancel with nothing running just returns the idle snapshot."""
        result = self.runner.cancel("operator_stop")
        self.assertEqual(result["state"], STATE_IDLE)
        self.assertIsNone(result["error"])

    def test_cancel_sets_the_token_the_sweep_receives(self) -> None:
        entered = threading.Event()
        received: dict[str, object] = {}

        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            received["token"] = cancellation_token
            entered.set()
            assert cancellation_token is not None
            deadline = time.monotonic() + 5
            while (
                not cancellation_token.cancelled
                and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            return WrongMatchCleanupSummary(
                processed=1, deleted=1, cancelled=cancellation_token.cancelled,
            )

        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=cleanup_fn,
        ))
        self.assertTrue(entered.wait(timeout=5))

        result = self.runner.cancel("operator_stop")
        self.assertEqual(result["state"], STATE_RUNNING)

        self.runner.join(timeout=5)
        token = received["token"]
        assert isinstance(token, CancellationToken)
        self.assertTrue(token.cancelled)
        self.assertEqual(token.reason, "operator_stop")

    def test_cancelled_sweep_reports_cancelled_state_not_failed(self) -> None:
        """A sweep that honors the token reports STATE_CANCELLED, and the
        partial summary is preserved exactly — not discarded."""
        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            assert cancellation_token is not None
            cancellation_token.cancel("operator_stop")
            return WrongMatchCleanupSummary(
                processed=1, deleted=1, cancelled=True,
            )

        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=cleanup_fn,
        ))
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_CANCELLED)
        self.assertNotEqual(status["state"], STATE_FAILED)
        self.assertIsNone(status["error"])
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["deleted"], 1)
        self.assertTrue(summary["cancelled"])

    def test_cancel_racing_a_finishing_sweep_reports_completed_not_failed(
        self,
    ) -> None:
        """#1083 invariant: a cancel that races the tail of a sweep that
        already finished its own work is not an error. The sweep here
        never actually observes the cancellation (it returns before ever
        re-checking the token, exactly like the real per-row loop when
        cancel() lands after the last row) — the runner must still report
        the accurate COMPLETED summary, never failed, never a stale
        RUNNING state."""
        entered = threading.Event()
        proceed = threading.Event()

        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            entered.set()
            proceed.wait(timeout=5)
            return WrongMatchCleanupSummary(processed=2, deleted=2)

        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=cleanup_fn,
        ))
        self.assertTrue(entered.wait(timeout=5))

        result = self.runner.cancel("operator_stop")
        self.assertEqual(result["state"], STATE_RUNNING)
        proceed.set()
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_COMPLETED)
        self.assertIsNone(status["error"])
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 2)
        self.assertEqual(summary["deleted"], 2)
        self.assertFalse(summary["cancelled"])

    def test_cancel_after_sweep_already_finished_is_not_an_error(self) -> None:
        """Cancel arriving after the runner already recorded a terminal
        state (completed) is a no-op, not a conflict."""
        def cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
            return WrongMatchCleanupSummary(processed=1, deleted=1)

        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=cleanup_fn,
        ))
        self.runner.join(timeout=5)
        self.assertEqual(self.runner.status()["state"], STATE_COMPLETED)

        result = self.runner.cancel("operator_stop")
        self.assertEqual(result["state"], STATE_COMPLETED)
        self.assertIsNone(result["error"])


class TriageRunnerStickyCancelTest(unittest.TestCase):
    """Issue #1106: a cancel that arrives before ``start()`` has flipped
    the state to RUNNING — the CLI's ``Ctrl-C`` racing its own
    still-in-flight start POST — must not be silently lost, but ONLY
    when the caller explicitly arms it (``arm_pending=True``, #1106 F3;
    reserved for the CLI's own Ctrl-C handler).

    Invariant: an acknowledged ARMED cancel is never lost -- it cancels
    the running sweep, or pre-cancels the next start() admitted within
    ``PENDING_CANCEL_WINDOW_SECONDS``, or expires having cancelled
    nothing; a start() admitted while a non-expired pending cancel
    exists never processes a row. An UNARMED cancel never affects a
    sweep it did not itself observe running -- it must never arm the
    pending slot, whether idle or between two sweeps.
    """

    def setUp(self) -> None:
        self.clock = _FakeClock()
        self.runner = TriageRunner(now_fn=self.clock)
        self.db = _ClosableDB()

    def _session(self):
        return closing(self.db)

    @staticmethod
    def _cleanup_fn(db, *, confirm_all_wrong_matches, cancellation_token=None):
        """Mirrors ``cleanup_all_wrong_matches``'s own contract: check
        the token BEFORE touching the first row, never mid-row."""
        assert cancellation_token is not None
        if cancellation_token.cancelled:
            return WrongMatchCleanupSummary(processed=0, deleted=0, cancelled=True)
        return WrongMatchCleanupSummary(processed=3, deleted=2, cancelled=False)

    def test_armed_cancel_while_idle_is_consumed_by_the_immediately_following_start(
        self,
    ) -> None:
        """The exact CLI race: Ctrl-C posts an ARMED cancel while the
        start POST is still in flight, and the cancel is served first.
        The next start() must still be stopped -- admitted (still
        202/True), but pre-cancelled before any row runs."""
        result = self.runner.cancel("ctrl_c_race", arm_pending=True)
        self.assertEqual(result["state"], STATE_IDLE)

        started = self.runner.start(
            db_session=self._session, cleanup_fn=self._cleanup_fn,
        )
        self.assertTrue(started, "a start() admitted within the window is "
                                  "still admitted, never refused")
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_CANCELLED)
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 0,
                          "a pre-cancelled sweep must never process a row")
        self.assertTrue(summary["cancelled"])

    def test_unarmed_cancel_while_idle_never_arms_the_pending_slot(self) -> None:
        """#1106 F3: an UNARMED cancel (the web UI's Stop button, or the
        standalone wrong-match-triage-cancel command) with nothing
        running is a pure #1083 no-op -- it must never affect a LATER
        sweep it did not itself observe running."""
        result = self.runner.cancel("browser_stop_click")
        self.assertEqual(result["state"], STATE_IDLE)

        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=self._cleanup_fn,
        ))
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_COMPLETED)
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 3)
        self.assertFalse(summary["cancelled"])

    def test_pending_cancel_within_the_window_stops_a_start_seconds_later(
        self,
    ) -> None:
        """Not just an instantaneous race -- any start() admitted before
        the window elapses is stopped."""
        self.runner.cancel("ctrl_c_race", arm_pending=True)
        self.clock.advance(PENDING_CANCEL_WINDOW_SECONDS / 2)

        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=self._cleanup_fn,
        ))
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_CANCELLED)
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 0)

    def test_pending_cancel_expires_and_does_not_poison_a_later_start(
        self,
    ) -> None:
        """An operator's cancel of an ALREADY-finished sweep must not
        silently sabotage whatever they start minutes later."""
        self.runner.cancel("stale_operator_click", arm_pending=True)
        self.clock.advance(PENDING_CANCEL_WINDOW_SECONDS + 1.0)

        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=self._cleanup_fn,
        ))
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_COMPLETED)
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 3)
        self.assertFalse(summary["cancelled"])

    def test_f1_blocker_a_stale_unconsumed_armed_cancel_does_not_swallow_a_later_one(
        self,
    ) -> None:
        """F1 BLOCKER (review round 1, verified empirically against the
        real runner): first-cancel-wins let a stale, never-consumed
        pending slot (nothing started between the two cancels to clear
        it) silently DROP a second, genuinely-in-window armed cancel --
        start() then discarded the FIRST (expired) reason and ran
        normally, missing the SECOND cancel entirely. Every armed
        cancel now unconditionally REFRESHES the pending timestamp
        (latest-wins), so the second cancel is the one that governs."""
        self.runner.cancel("stale_first_cancel", arm_pending=True)
        self.clock.advance(PENDING_CANCEL_WINDOW_SECONDS + 1.0)
        self.runner.cancel("genuine_second_cancel", arm_pending=True)

        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=self._cleanup_fn,
        ))
        self.runner.join(timeout=5)

        status = self.runner.status()
        self.assertEqual(status["state"], STATE_CANCELLED,
                          "the second (latest) armed cancel must still stop this start")
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 0)

    def test_expired_pending_cancel_does_not_swallow_a_later_genuine_cancel(
        self,
    ) -> None:
        """A related shape (fault-injection find, #1106 review round 1
        of the earlier -- since superseded -- fix): an intervening
        start() consumes/expires the first cancel, and a later,
        independent armed cancel must still stop the NEXT start()."""
        self.runner.cancel("first_stale_cancel", arm_pending=True)
        self.clock.advance(PENDING_CANCEL_WINDOW_SECONDS + 1.0)
        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=self._cleanup_fn,
        ))
        self.runner.join(timeout=5)
        self.assertEqual(self.runner.status()["state"], STATE_COMPLETED,
                          "the first cancel expired -- this start runs")

        self.runner.cancel("second_genuine_cancel", arm_pending=True)
        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=self._cleanup_fn,
        ))
        self.runner.join(timeout=5)
        status = self.runner.status()
        self.assertEqual(status["state"], STATE_CANCELLED,
                          "the second cancel must still stop this start")
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 0)

    def test_pending_cancel_is_consumed_at_most_once(self) -> None:
        """A single armed cancel() only ever stops ONE start() -- it
        must not leak forward and poison a second, later sweep too."""
        self.runner.cancel("ctrl_c_race", arm_pending=True)
        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=self._cleanup_fn,
        ))
        self.runner.join(timeout=5)
        self.assertEqual(self.runner.status()["state"], STATE_CANCELLED)

        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=self._cleanup_fn,
        ))
        self.runner.join(timeout=5)
        status = self.runner.status()
        self.assertEqual(status["state"], STATE_COMPLETED)
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 3)

    def test_cancel_while_running_never_arms_the_pending_slot_for_a_later_start(
        self,
    ) -> None:
        """#1106 F2: a cancel that stops a genuinely RUNNING sweep must
        not leak into arming the pending slot for a LATER, unrelated
        start() -- that would mean an ordinary Stop-during-a-sweep
        silently pre-cancels the next sweep nobody asked to stop."""
        entered = threading.Event()
        release = threading.Event()

        def blocking_cleanup_fn(
            db, *, confirm_all_wrong_matches, cancellation_token=None,
        ):
            assert cancellation_token is not None
            entered.set()
            release.wait(timeout=5)
            return WrongMatchCleanupSummary(
                processed=1, deleted=1, cancelled=cancellation_token.cancelled,
            )

        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=blocking_cleanup_fn,
        ))
        self.assertTrue(entered.wait(timeout=5))
        result = self.runner.cancel("operator_stop")
        self.assertEqual(result["state"], STATE_RUNNING)
        release.set()
        self.runner.join(timeout=5)
        self.assertEqual(self.runner.status()["state"], STATE_CANCELLED)

        # No cancel() was armed since -- this start() must run normally.
        self.assertTrue(self.runner.start(
            db_session=self._session, cleanup_fn=self._cleanup_fn,
        ))
        self.runner.join(timeout=5)
        status = self.runner.status()
        self.assertEqual(status["state"], STATE_COMPLETED)
        summary = status["summary"]
        assert isinstance(summary, dict)
        self.assertEqual(summary["processed"], 3)


if __name__ == "__main__":
    unittest.main()
