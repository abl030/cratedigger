"""Convergence-signal derivation and stop-action contract (#978)."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/fuzz
from lib.convergence_service import (
    ConvergenceObservation,
    ConvergenceSignal,
    ConvergenceStopService,
    StopConvergedSearchResult,
    derive_convergence_signal,
)
from web.routes.pipeline import _attach_convergence_prompts

_NOW = datetime(2026, 8, 3, tzinfo=UTC)


def _observation(
    log_id: int,
    peer: str,
    cliff_hz: int | None = 15_000,
    *,
    eligible: bool = True,
) -> ConvergenceObservation:
    return ConvergenceObservation(
        log_id=log_id,
        peer=peer,
        snapshot_fingerprint=f"snapshot-{log_id}",
        cliff_hz=cliff_hz,
        observed_at=_NOW + timedelta(seconds=log_id),
        eligible=eligible,
    )


def _assert_threshold_result(
    case: unittest.TestCase,
    *,
    distinct_peers: int,
    signal: ConvergenceSignal | None,
) -> None:
    case.assertEqual(signal is not None, distinct_peers >= 5)


class TestConvergenceDerivation(unittest.TestCase):
    def test_five_distinct_peers_at_same_cliff_converge(self) -> None:
        signal = derive_convergence_signal(
            41,
            [_observation(i, f"peer-{i}") for i in range(1, 6)],
        )
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.cliff_hz, 15_000)
        self.assertEqual(signal.observation_count, 5)
        self.assertEqual(signal.distinct_peer_count, 5)
        self.assertEqual(signal.distinct_candidate_snapshot_count, 5)
        self.assertEqual(signal.latest_qualifying_log_id, 5)

    def test_repeat_peer_does_not_satisfy_threshold(self) -> None:
        self.assertIsNone(derive_convergence_signal(
            41,
            [_observation(i, "same-peer") for i in range(1, 20)],
        ))

    def test_newest_null_cliff_is_upward_break(self) -> None:
        rows = [_observation(i, f"peer-{i}") for i in range(1, 6)]
        rows.append(_observation(6, "new-peer", None))
        self.assertIsNone(derive_convergence_signal(41, rows))

    def test_different_cliff_resets_current_run(self) -> None:
        rows = [_observation(i, f"old-{i}") for i in range(1, 7)]
        rows.extend(_observation(i, f"new-{i}", 16_000) for i in range(7, 11))
        self.assertIsNone(derive_convergence_signal(41, rows))

    def test_ineligible_worlds_are_ignored_not_breaks(self) -> None:
        rows = [_observation(i, f"peer-{i}") for i in range(1, 6)]
        rows.append(_observation(6, "legacy", None, eligible=False))
        self.assertIsNotNone(derive_convergence_signal(41, rows))

    @given(
        distinct_peers=st.integers(min_value=0, max_value=12),
        repeats=st.integers(min_value=0, max_value=20),
    )
    def test_generated_threshold_depends_on_distinct_peers(
        self, distinct_peers: int, repeats: int,
    ) -> None:
        peers = [f"p-{i}" for i in range(distinct_peers)]
        rows = [_observation(i + 1, p) for i, p in enumerate(peers)]
        if peers:
            rows.extend(
                _observation(100 + i, peers[0]) for i in range(repeats)
            )
        signal = derive_convergence_signal(9, rows)
        _assert_threshold_result(
            self, distinct_peers=distinct_peers, signal=signal,
        )

    @given(
        ignored_count=st.integers(min_value=0, max_value=30),
        ignored_cliff=st.one_of(st.none(), st.integers(min_value=0, max_value=24_000)),
    )
    def test_generated_ineligible_worlds_never_break_current_run(
        self, ignored_count: int, ignored_cliff: int | None,
    ) -> None:
        rows = [_observation(i, f"peer-{i}") for i in range(1, 6)]
        rows.extend(
            _observation(100 + i, f"ignored-{i}", ignored_cliff, eligible=False)
            for i in range(ignored_count)
        )
        self.assertIsNotNone(derive_convergence_signal(41, rows))

    @given(repeats=st.integers(min_value=5, max_value=80))
    def test_generated_repeated_peer_never_meets_distinct_threshold(
        self, repeats: int,
    ) -> None:
        rows = [_observation(i, "one-peer") for i in range(repeats)]
        self.assertIsNone(derive_convergence_signal(41, rows))

    @given(new_cliff=st.integers(min_value=0, max_value=24_000).filter(
        lambda value: not 14_750 <= value < 15_250
    ))
    def test_generated_newer_different_bin_resets_the_run(
        self, new_cliff: int,
    ) -> None:
        rows = [_observation(i, f"old-{i}") for i in range(1, 10)]
        rows.extend(
            _observation(i, f"new-{i}", new_cliff) for i in range(10, 14)
        )
        self.assertIsNone(derive_convergence_signal(41, rows))

    @given(cliff=st.integers(min_value=14_750, max_value=15_249))
    def test_generated_same_500hz_bin_is_stable(self, cliff: int) -> None:
        rows = [_observation(i, f"peer-{i}", cliff) for i in range(1, 6)]
        signal = derive_convergence_signal(41, rows)
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal.cliff_hz, 15_000)

    def test_known_bad_threshold_checker_self_test(self) -> None:
        """The generated checker must reject the old observation-count gate."""
        bad_signal = ConvergenceSignal(
            request_id=9,
            observation_count=20,
            distinct_peer_count=1,
            distinct_candidate_snapshot_count=20,
            cliff_hz=15_000,
            latest_qualifying_log_id=20,
            first_observed_at=_NOW,
            latest_observed_at=_NOW,
        )
        with self.assertRaises(AssertionError):
            _assert_threshold_result(
                self, distinct_peers=1, signal=bad_signal,
            )


class _StopDB:
    def __init__(self, result: StopConvergedSearchResult) -> None:
        self.result = result
        self.calls: list[tuple[int, int, int]] = []

    def stop_search_for_convergence(
        self, request_id: int, *, latest_qualifying_log_id: int, cliff_hz: int,
    ) -> StopConvergedSearchResult:
        self.calls.append((request_id, latest_qualifying_log_id, cliff_hz))
        return self.result


class TestConvergenceStopService(unittest.TestCase):
    def test_service_is_the_single_action_seam(self) -> None:
        signal = ConvergenceSignal(
            request_id=41,
            observation_count=7,
            distinct_peer_count=6,
            distinct_candidate_snapshot_count=5,
            cliff_hz=15_000,
            latest_qualifying_log_id=99,
            first_observed_at=_NOW,
            latest_observed_at=_NOW,
        )
        expected = StopConvergedSearchResult(
            outcome="stopped", request_id=41, signal=signal,
            observed_status="unsearchable",
        )
        db = _StopDB(expected)
        actual = ConvergenceStopService(db).stop(
            41, latest_qualifying_log_id=99, cliff_hz=15_000,
        )
        self.assertEqual(actual, expected)
        self.assertEqual(db.calls, [(41, 99, 15_000)])

    def test_recents_prompt_attaches_only_to_newest_visible_row(self) -> None:
        signal = ConvergenceSignal(
            request_id=41,
            observation_count=7,
            distinct_peer_count=6,
            distinct_candidate_snapshot_count=5,
            cliff_hz=15_000,
            latest_qualifying_log_id=99,
            first_observed_at=_NOW,
            latest_observed_at=_NOW,
        )
        rows: list[dict[str, object]] = [
            {"id": 3, "request_id": 41},
            {"id": 2, "request_id": 42},
            {"id": 1, "request_id": 41},
        ]
        _attach_convergence_prompts(rows, {41: signal})
        self.assertIn("convergence", rows[0])
        self.assertNotIn("convergence", rows[1])
        self.assertNotIn("convergence", rows[2])


if __name__ == "__main__":
    unittest.main()
