"""Generated sticky-cancel boundary for the Wrong Matches triage runner
(issue #1106).

Invariant under patrol: an acknowledged ARMED cancel is never lost -- it
cancels the running sweep, or pre-cancels the next start() admitted
within ``PENDING_CANCEL_WINDOW_SECONDS``, or expires having cancelled
nothing. An UNARMED cancel never affects a sweep it did not itself
observe running -- it must never arm the pending slot, whether idle or
between two sweeps. A start() admitted while a non-expired pending
cancel exists never processes a row. A cancel that stops a genuinely
RUNNING sweep never leaks into arming the pending slot for a LATER,
unrelated start().

Drives the REAL ``TriageRunner`` over generated interleavings of
cancel (armed/unarmed) / advance-clock / start / cancel-while-running,
joining after every start-type step so the timeline is deterministic.

Review round 1 (F1/F2) found two gaps in the FIRST version of this
module: the model itself encoded the buggy first-wins implementation
(so it scored the F1 counterexample "expect COMPLETED" and CERTIFIED
the bug instead of catching it), and the step strategy excluded
cancel-while-RUNNING entirely. Both are fixed here: the model now
encodes the invariant (latest-wins, armed-only), and
``cancel_while_running`` widens the domain using real thread
synchronization so a sweep is genuinely mid-flight when cancelled.
"""

from __future__ import annotations

import threading
import unittest
from collections.abc import Callable, Generator
from contextlib import contextmanager

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.import_execution import CancellationToken
from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary
from web.triage_runner import (
    PENDING_CANCEL_WINDOW_SECONDS,
    STATE_CANCELLED,
    STATE_COMPLETED,
    STATE_FAILED,
    TriageRunner,
)

_ROWS_TOTAL = 3


class _FakeClock:
    """Deterministic stand-in for the ``now_fn`` kwarg-DI seam."""

    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, delta: float) -> None:
        self.value += delta


class _ClosableDB:
    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


class _SessionLedger:
    """Records every sweep-DB scope the runner opens.

    Stands in for ``WebRuntime.open_background_db``, including the part
    that matters most here: the session owns the handle. It yields one
    and does NOT close it, exactly as the DSN-less production branch
    hands over the injected handle it must leave open — so anything
    closing a handle was the runner, not the session.
    """

    def __init__(self) -> None:
        self.entered = 0
        self.exited = 0
        self.handles: list[_ClosableDB] = []

    @contextmanager
    def __call__(self) -> Generator[object]:
        handle = _ClosableDB()
        self.handles.append(handle)
        self.entered += 1
        try:
            yield handle
        finally:
            self.exited += 1


def session_ledger_violations(
    ledger: _SessionLedger, *, admitted: int,
) -> list[str]:
    """Every way the runner can mishandle the sweep's DB session.

    Accumulates rather than short-circuits, so a run breaking several
    clauses reports all of them.

    The "left open" clause is fail-closed legislation with no reaching
    mutant, and more stubbornly so than it looks: the obvious future
    writer's mistake, ``db = db_session().__enter__()`` instead of a
    ``with``, does NOT trip it, because the orphaned context manager is
    collected immediately and ``GeneratorExit`` runs the generator's
    ``finally`` anyway. It can only fire if something holds the session
    open past this check — opening it in ``start()``, say. Kept per
    "widen the strategy, never delete the clause"; recorded so nobody
    hunts for a mutant that cannot exist.
    """
    violations: list[str] = []
    if ledger.entered != admitted:
        violations.append(
            f"the runner entered {ledger.entered} sessions for "
            f"{admitted} admitted sweeps",
        )
    if ledger.exited != ledger.entered:
        violations.append(
            f"the runner left {ledger.entered - ledger.exited} session(s) "
            "open",
        )
    closed_by_the_runner = sum(handle.closed for handle in ledger.handles)
    if closed_by_the_runner:
        violations.append(
            "the runner closed a handle the session owns "
            f"({closed_by_the_runner} time(s))",
        )
    return violations


def _cleanup_fn(
    db: object,
    *,
    confirm_all_wrong_matches: bool,
    cancellation_token: CancellationToken | None = None,
) -> WrongMatchCleanupSummary:
    """Mirrors ``cleanup_all_wrong_matches``'s own contract: the
    cancellation token is checked BEFORE the first row, never mid-row."""
    del db, confirm_all_wrong_matches
    assert cancellation_token is not None
    if cancellation_token.cancelled:
        return WrongMatchCleanupSummary(processed=0, deleted=0, cancelled=True)
    return WrongMatchCleanupSummary(
        processed=_ROWS_TOTAL, deleted=_ROWS_TOTAL, cancelled=False,
    )


def _blocking_cleanup_fn_factory() -> tuple[
    Callable[..., WrongMatchCleanupSummary], threading.Event, threading.Event,
]:
    """A controllable sweep body for the generated
    ``cancel_while_running`` step: represents one row's processing that
    blocks after entering, until released, so the step can genuinely
    cancel a sweep that is mid-run rather than merely simulate one
    synchronously. The row always "completes" once released (production
    never checks the token mid-row, only before the NEXT one) -- so
    ``processed`` is always 1 here regardless of when cancel() lands."""
    entered = threading.Event()
    release = threading.Event()

    def cleanup_fn(
        db: object,
        *,
        confirm_all_wrong_matches: bool,
        cancellation_token: CancellationToken | None = None,
    ) -> WrongMatchCleanupSummary:
        del db, confirm_all_wrong_matches
        assert cancellation_token is not None
        entered.set()
        release.wait(timeout=5)
        return WrongMatchCleanupSummary(
            processed=1, deleted=1, cancelled=cancellation_token.cancelled,
        )

    return cleanup_fn, entered, release


def assert_pending_cancel_outcome(
    *, pending_valid: bool, state: str, processed: int,
) -> None:
    """The #1106 sticky-cancel invariant, split into its two exhaustive
    clauses: a start() admitted within a non-expired ARMED pending
    cancel's window must never process a row; a start() with no valid
    pending cancel must run to normal completion over every row."""
    if pending_valid:
        if state != STATE_CANCELLED or processed != 0:
            raise AssertionError(
                "a start() admitted within the pending-cancel window "
                f"must terminate cancelled with zero rows processed; "
                f"got state={state!r} processed={processed}"
            )
        return
    if state != STATE_COMPLETED or processed != _ROWS_TOTAL:
        raise AssertionError(
            "a start() admitted with no valid pending cancel must run "
            f"to normal completion over every row; got state={state!r} "
            f"processed={processed} (expected {_ROWS_TOTAL})"
        )


def assert_running_cancel_outcome(*, state: str, processed: int) -> None:
    """A cancel() issued while a sweep is genuinely RUNNING must stop
    it -- unconditionally, regardless of arm_pending -- with the
    in-flight row (never mid-delete) still counted (#1106 F2)."""
    if state != STATE_CANCELLED or processed != 1:
        raise AssertionError(
            "a cancel() issued while RUNNING must terminate the sweep "
            f"cancelled with its one in-flight row counted; got "
            f"state={state!r} processed={processed}"
        )


# One generated step is a uniform (kind, delta, armed) triple -- delta
# only means anything for "advance", armed only for "cancel" -- so the
# strategy and the runner below need no heterogeneous tuple typing.
# Advances are bounded to a few multiples of the window so both "well
# inside" and "well past" the boundary are common, not just values that
# happen to graze it.
_STEP_STRATEGY = st.one_of(
    st.tuples(st.just("cancel"), st.just(0.0), st.booleans()),
    st.tuples(
        st.just("advance"),
        st.floats(
            min_value=0.0,
            max_value=PENDING_CANCEL_WINDOW_SECONDS * 3,
            allow_nan=False,
            allow_infinity=False,
        ),
        st.just(False),
    ),
    st.tuples(st.just("start"), st.just(0.0), st.just(False)),
    st.tuples(st.just("cancel_while_running"), st.just(0.0), st.just(False)),
)


def _run_steps(steps: list[tuple[str, float, bool]]) -> None:
    clock = _FakeClock()
    runner = TriageRunner(now_fn=clock)
    session = _SessionLedger()
    admitted = 0
    model_time = 0.0
    # The model of the ARMED pending-cancel slot: None when nothing is
    # armed, else the model-time of the LATEST armed cancel (#1106 F1 --
    # latest-wins, not first-wins). An unarmed cancel never touches this.
    last_armed_cancel_at: float | None = None

    for kind, delta, armed in steps:
        if kind == "cancel":
            runner.cancel("generated_reason", arm_pending=armed)
            if armed:
                last_armed_cancel_at = model_time
        elif kind == "advance":
            model_time += delta
            clock.advance(delta)
        elif kind == "start":
            pending_valid = (
                last_armed_cancel_at is not None
                and (model_time - last_armed_cancel_at)
                <= PENDING_CANCEL_WINDOW_SECONDS
            )
            started = runner.start(
                db_session=session, cleanup_fn=_cleanup_fn,
            )
            admitted += 1
            if not started:
                raise AssertionError(
                    "start() refused while joined-idle -- every prior "
                    "start-type step in this generated sequence was "
                    "joined before the next step, so the runner can "
                    "never be RUNNING here"
                )
            runner.join(timeout=5)
            # Every start() consumes-or-expires the pending slot,
            # unconditionally (mirrors _consume_pending_cancel_locked).
            last_armed_cancel_at = None
            status = runner.status()
            summary = status["summary"]
            assert isinstance(summary, dict)
            processed = summary["processed"]
            assert isinstance(processed, int)
            assert_pending_cancel_outcome(
                pending_valid=pending_valid,
                state=status["state"],
                processed=processed,
            )
        elif kind == "cancel_while_running":
            cleanup_fn, entered, release = _blocking_cleanup_fn_factory()
            started = runner.start(
                db_session=session, cleanup_fn=cleanup_fn,
            )
            admitted += 1
            if not started:
                raise AssertionError(
                    "cancel_while_running: start() refused while "
                    "joined-idle"
                )
            if not entered.wait(timeout=5):
                raise AssertionError(
                    "cancel_while_running: sweep never entered cleanup_fn"
                )
            runner.cancel("generated_running_cancel")
            release.set()
            runner.join(timeout=5)
            # This start() ALSO consumes-or-expires any pending slot.
            last_armed_cancel_at = None
            status = runner.status()
            summary = status["summary"]
            assert isinstance(summary, dict)
            processed = summary["processed"]
            assert isinstance(processed, int)
            assert_running_cancel_outcome(
                state=status["state"], processed=processed,
            )
        else:
            raise AssertionError(f"unknown generated step kind: {kind}")

    violations = session_ledger_violations(session, admitted=admitted)
    if violations:
        raise AssertionError("; ".join(violations))


class TriageRunnerStickyCancelGeneratedTest(unittest.TestCase):
    def test_checker_rejects_a_pending_cancel_that_did_not_stop_the_sweep(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            AssertionError, "must terminate cancelled with zero rows",
        ):
            assert_pending_cancel_outcome(
                pending_valid=True, state=STATE_COMPLETED, processed=_ROWS_TOTAL,
            )

    def test_checker_rejects_an_expired_cancel_that_still_stopped_the_sweep(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            AssertionError, "must run to normal completion",
        ):
            assert_pending_cancel_outcome(
                pending_valid=False, state=STATE_CANCELLED, processed=0,
            )

    def test_ledger_checker_rejects_a_sweep_that_opened_no_session(
        self,
    ) -> None:
        self.assertIn(
            "the runner entered 0 sessions for 1 admitted sweeps",
            session_ledger_violations(_SessionLedger(), admitted=1),
        )

    def test_ledger_checker_rejects_a_session_left_open(self) -> None:
        ledger = _SessionLedger()
        scope = ledger()
        scope.__enter__()

        self.assertIn(
            "the runner left 1 session(s) open",
            session_ledger_violations(ledger, admitted=1),
        )

    def test_ledger_checker_rejects_a_handle_the_runner_closed(self) -> None:
        ledger = _SessionLedger()
        with ledger():
            # The session owns the handle; a close from inside the block
            # is the runner reaching past that ownership.
            ledger.handles[-1].close()

        self.assertIn(
            "the runner closed a handle the session owns (1 time(s))",
            session_ledger_violations(ledger, admitted=1),
        )

    def test_ledger_checker_stays_quiet_on_outcomes_it_does_not_read(
        self,
    ) -> None:
        """Q3 for all three ledger clauses.

        They read session scopes and handle closes, and ignore the
        dimension that decides everything else about a sweep: how it
        ended. Drive the real runner through the three endings the
        clauses never look at — a refusal, a raising cleanup, and a
        cancellation — and require silence.
        """
        session = _SessionLedger()
        runner = TriageRunner()
        admitted = 0

        cleanup_fn, entered, release = _blocking_cleanup_fn_factory()
        self.assertTrue(runner.start(db_session=session, cleanup_fn=cleanup_fn))
        admitted += 1
        self.assertTrue(entered.wait(timeout=5))
        # Refused while one is running: no session, and not admitted.
        self.assertFalse(runner.start(db_session=session, cleanup_fn=cleanup_fn))
        runner.cancel("q3_cancel")
        release.set()
        runner.join(timeout=5)
        self.assertEqual(runner.status()["state"], STATE_CANCELLED)

        def raises(
            db: object,
            *,
            confirm_all_wrong_matches: bool,
            cancellation_token: CancellationToken | None = None,
        ) -> WrongMatchCleanupSummary:
            raise RuntimeError("the sweep blew up")

        self.assertTrue(runner.start(db_session=session, cleanup_fn=raises))
        admitted += 1
        runner.join(timeout=5)
        self.assertEqual(runner.status()["state"], STATE_FAILED)

        self.assertEqual(
            session_ledger_violations(session, admitted=admitted), [],
        )

    def test_checker_rejects_a_running_cancel_that_did_not_stop_the_sweep(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            AssertionError, "must terminate the sweep cancelled",
        ):
            assert_running_cancel_outcome(state=STATE_COMPLETED, processed=1)

    @given(steps=st.lists(_STEP_STRATEGY, min_size=1, max_size=12))
    @example(steps=[("cancel", 0.0, True), ("start", 0.0, False)])
    @example(steps=[
        ("cancel", 0.0, True),
        ("advance", PENDING_CANCEL_WINDOW_SECONDS + 1.0, False),
        ("start", 0.0, False),
    ])
    @example(steps=[
        ("cancel", 0.0, True),
        ("advance", PENDING_CANCEL_WINDOW_SECONDS / 2, False),
        ("start", 0.0, False),
    ])
    @example(steps=[
        ("cancel", 0.0, True), ("start", 0.0, False),
        ("cancel", 0.0, True), ("start", 0.0, False),
    ])
    @example(steps=[
        # Unarmed cancel while idle must never arm the slot (#1106 F3):
        # a later start() runs to completion, unaffected.
        ("cancel", 0.0, False),
        ("start", 0.0, False),
    ])
    @example(steps=[
        # A cancel that stops a RUNNING sweep must not leak into arming
        # the pending slot for a later, unrelated start() (#1106 F2).
        ("cancel_while_running", 0.0, False),
        ("start", 0.0, False),
    ])
    @example(steps=[
        # F1 BLOCKER world (review round 1) -- the reviewer's own exact
        # shape: cancel A (armed), advance PAST the window (so A alone
        # would already be stale), cancel B (armed -- latest-wins must
        # overwrite A's stale timestamp), start immediately after B.
        # Under first-wins this world FAILS (B is silently dropped
        # because the still-occupied slot blocks recording it); under
        # latest-wins it passes. Verified via fault injection: reverting
        # to first-wins here reproduces the exact review finding.
        ("cancel", 0.0, True),
        ("advance", PENDING_CANCEL_WINDOW_SECONDS + 1.0, False),
        ("cancel", 0.0, True),
        ("start", 0.0, False),
    ])
    @example(steps=[
        # An earlier fault-injection find (superseded by the F1 world
        # above, kept for extra coverage of the same defect class): an
        # intervening start() consumes/expires the first cancel, then a
        # second, independent armed cancel must still stop the NEXT one.
        ("cancel", 0.0, True),
        ("advance", PENDING_CANCEL_WINDOW_SECONDS + 1.0, False),
        ("start", 0.0, False),
        ("cancel", 0.0, True),
        ("start", 0.0, False),
    ])
    def test_sticky_cancel_is_never_lost(
        self, steps: list[tuple[str, float, bool]],
    ) -> None:
        _run_steps(steps)


if __name__ == "__main__":
    unittest.main()
