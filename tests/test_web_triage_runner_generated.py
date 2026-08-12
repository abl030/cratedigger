"""Generated sticky-cancel boundary for the Wrong Matches triage runner
(issue #1106).

Invariant under patrol: an acknowledged cancel is never lost -- it
cancels the running sweep, or cancels the next start() admitted within
``PENDING_CANCEL_WINDOW_SECONDS``, or expires having cancelled nothing;
a start() admitted while a non-expired pending cancel exists never
processes a row.

Drives the REAL ``TriageRunner`` over generated interleavings of
cancel/advance-clock/start, joining after every start so the timeline is
deterministic (the concurrent-cancel-while-RUNNING half of the runner's
contract is already pinned by ``tests/test_web_triage_runner.py`` and is
out of scope here -- this module isolates the NEW sticky-pending half).
"""

from __future__ import annotations

import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.import_execution import CancellationToken
from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary
from web.triage_runner import (
    PENDING_CANCEL_WINDOW_SECONDS,
    STATE_CANCELLED,
    STATE_COMPLETED,
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
    def close(self) -> None:
        return None


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


def assert_pending_cancel_outcome(
    *, pending_valid: bool, state: str, processed: int,
) -> None:
    """The #1106 sticky-cancel invariant, split into its two exhaustive
    clauses: a start() admitted within a non-expired pending cancel's
    window must never process a row; a start() with no valid pending
    cancel must run to normal completion over every row."""
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


# One generated step is either a cancel() call, a clock advance, or a
# start()+join() cycle -- always a uniform (kind, delta) pair (delta is
# unused except for "advance") so the strategy and the runner below need
# no heterogeneous tuple typing. Advances are bounded to a few multiples
# of the window so both "well inside" and "well past" the boundary are
# common, not just values that happen to graze it.
_STEP_STRATEGY = st.one_of(
    st.tuples(st.just("cancel"), st.just(0.0)),
    st.tuples(
        st.just("advance"),
        st.floats(
            min_value=0.0,
            max_value=PENDING_CANCEL_WINDOW_SECONDS * 3,
            allow_nan=False,
            allow_infinity=False,
        ),
    ),
    st.tuples(st.just("start"), st.just(0.0)),
)


def _run_steps(steps: list[tuple[str, float]]) -> None:
    clock = _FakeClock()
    runner = TriageRunner(now_fn=clock)
    model_time = 0.0
    last_cancel_at: float | None = None
    for kind, delta in steps:
        if kind == "cancel":
            runner.cancel("generated_reason")
            if last_cancel_at is None:
                # #1106: first-cancel-wins while nothing is running --
                # a redundant cancel before the next start() does not
                # refresh (and so cannot extend) the pending window.
                last_cancel_at = model_time
        elif kind == "advance":
            model_time += delta
            clock.advance(delta)
        elif kind == "start":
            pending_valid = (
                last_cancel_at is not None
                and (model_time - last_cancel_at) <= PENDING_CANCEL_WINDOW_SECONDS
            )
            started = runner.start(
                db_factory=_ClosableDB, cleanup_fn=_cleanup_fn,
            )
            if not started:
                raise AssertionError(
                    "start() refused while joined-idle -- every prior "
                    "start in this generated sequence was joined before "
                    "the next step, so the runner can never be RUNNING "
                    "here"
                )
            runner.join(timeout=5)
            last_cancel_at = None
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
        else:
            raise AssertionError(f"unknown generated step kind: {kind}")


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

    @given(steps=st.lists(_STEP_STRATEGY, min_size=1, max_size=12))
    @example(steps=[("cancel", 0.0), ("start", 0.0)])
    @example(steps=[
        ("cancel", 0.0),
        ("advance", PENDING_CANCEL_WINDOW_SECONDS + 1.0),
        ("start", 0.0),
    ])
    @example(steps=[
        ("cancel", 0.0),
        ("advance", PENDING_CANCEL_WINDOW_SECONDS / 2),
        ("start", 0.0),
    ])
    @example(steps=[
        ("cancel", 0.0), ("start", 0.0), ("cancel", 0.0), ("start", 0.0),
    ])
    @example(steps=[
        # Fault-injection find (#1106 review): a mutant that skipped
        # clearing an EXPIRED pending cancel left its stale timestamp in
        # place, silently swallowing the SECOND cancel() (the
        # first-cancel-wins guard saw the stale slot as still occupied)
        # -- so the genuinely-in-window second cancel never got recorded
        # and the following start() ran unstopped. No deterministic pin
        # in tests/test_web_triage_runner.py caught this; only this
        # property did.
        ("cancel", 0.0),
        ("advance", PENDING_CANCEL_WINDOW_SECONDS + 1.0),
        ("start", 0.0),
        ("cancel", 0.0),
        ("start", 0.0),
    ])
    def test_sticky_cancel_is_never_lost(
        self, steps: list[tuple[str, float]],
    ) -> None:
        _run_steps(steps)


if __name__ == "__main__":
    unittest.main()
