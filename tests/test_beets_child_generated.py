"""Generated property for the shared pinned-Beets child spawner (#1278 item 4).

Invariant T — **token integrity**: whatever argv tail a lane supplies, the
injected runner receives exactly ``[<pinned python>, *tail]`` — every token
a separate argv element, none joined, split, reordered, or dropped — with
the lane's timeout, captured output, the resolved beets environment, and
the optional stdin payload forwarded unchanged. Patrols the seam the three
run-to-completion mutation lanes share; a regression here touches every
lane at once. The deterministic pins live in ``tests/test_beets_child.py``;
the ``@example`` pins here are the three lanes' real argv shapes.
"""

from __future__ import annotations

import subprocess as sp
import unittest
from typing import ClassVar

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.beets_child import run_pinned_beets_child
from tests.test_beets_child import FAKE_PYTHON, _RecordingRunner, runtime_config

#: Tokens a lane could plausibly (or implausibly) put in an argv tail —
#: no plausibility filter: empty strings, whitespace, ``=``/``:`` query
#: shapes, and non-ASCII are all in-domain.
_tokens = st.text(min_size=0, max_size=40)
_tails = st.lists(_tokens, min_size=0, max_size=8)
_payloads = st.none() | st.binary(max_size=64)
_timeouts = st.integers(min_value=1, max_value=100_000)


def token_integrity_violations(
    argv: list[str],
    kwargs: dict[str, object],
    *,
    tail: list[str],
    timeout: int,
    payload: bytes | None,
) -> list[str]:
    """Accumulating checker: every clause evaluates, ordering cannot mask
    one (`.claude/rules/code-quality.md` § known-bad self-tests)."""
    violations: list[str] = []
    if not argv or argv[0] != FAKE_PYTHON:
        violations.append("argv does not begin with the pinned interpreter")
    if argv[1:] != tail:
        violations.append("argv tail was not forwarded verbatim")
    if kwargs.get("timeout") != timeout:
        violations.append("the lane's timeout was not forwarded")
    if kwargs.get("capture_output") is not True:
        violations.append("output is not captured")
    env = kwargs.get("env")
    if not (
        isinstance(env, dict)
        and env.get("CRATEDIGGER_BEETS_PYTHON") == FAKE_PYTHON
        and env.get("BEETSDIR")
    ):
        violations.append("the resolved beets environment was not forwarded")
    if payload is None:
        if "input" in kwargs:
            violations.append("a stdin payload appeared from nowhere")
    elif kwargs.get("input") != payload:
        violations.append("the stdin payload was not forwarded unchanged")
    return violations


class TestTokenIntegrity(unittest.TestCase):
    @settings(deadline=None)
    @given(tail=_tails, payload=_payloads, timeout=_timeouts)
    @example(
        tail=[
            "-m", "beets", "modify", "-a", "-M", "-W", "-y",
            "id:=7", "mb_albumid:=8987a929-0000-4000-8000-000000000001",
            "mb_albumid=8987a929-0000-4000-8000-000000000002",
        ],
        payload=None,
        timeout=120,
    )
    @example(
        tail=[
            "-m", "beets", "write",
            "album_id:=7", "mb_albumid:=8987a929-0000-4000-8000-000000000001",
        ],
        payload=None,
        timeout=300,
    )
    @example(
        tail=["/repo/harness/delete_album.py"],
        payload=b'{"album_id": 7}',
        timeout=600,
    )
    def test_runner_receives_the_lane_argv_unchanged(
        self, tail: list[str], payload: bytes | None, timeout: int,
    ) -> None:
        runner = _RecordingRunner()
        with runtime_config():
            run_pinned_beets_child(
                tail, timeout=timeout, input_bytes=payload, runner=runner,
            )

        argv, kwargs = runner.calls[0]
        self.assertEqual(
            token_integrity_violations(
                argv, dict(kwargs), tail=tail, timeout=timeout, payload=payload,
            ),
            [],
        )


class TestTokenIntegrityCheckerTripsOnViolations(unittest.TestCase):
    """Q1 per clause: each clause trips on the minimal world violating it
    alone while every other clause passes."""

    _GOOD_TAIL: ClassVar[list[str]] = ["-m", "beets", "write", "album_id:=7"]

    def _good_call(self) -> tuple[list[str], dict[str, object]]:
        env: dict[str, str] = {
            "CRATEDIGGER_BEETS_PYTHON": FAKE_PYTHON,
            "BEETSDIR": "/var/lib/cratedigger/beets",
        }
        kwargs: dict[str, object] = {
            "capture_output": True, "timeout": 42, "env": env,
        }
        return [FAKE_PYTHON, *self._GOOD_TAIL], kwargs

    def _assert_single_violation(
        self,
        message: str,
        argv: list[str],
        kwargs: dict[str, object],
        *,
        payload: bytes | None = None,
    ) -> None:
        self.assertEqual(
            token_integrity_violations(
                argv, kwargs, tail=self._GOOD_TAIL, timeout=42, payload=payload,
            ),
            [message],
        )

    def test_a_wrong_interpreter_trips_only_the_prefix_clause(self) -> None:
        argv, kwargs = self._good_call()
        argv[0] = "/usr/bin/python3"
        self._assert_single_violation(
            "argv does not begin with the pinned interpreter", argv, kwargs,
        )

    def test_joined_tokens_trip_only_the_tail_clause(self) -> None:
        argv, kwargs = self._good_call()
        argv[1:] = [" ".join(self._GOOD_TAIL)]
        self._assert_single_violation(
            "argv tail was not forwarded verbatim", argv, kwargs,
        )

    def test_a_dropped_timeout_trips_only_the_timeout_clause(self) -> None:
        argv, kwargs = self._good_call()
        del kwargs["timeout"]
        self._assert_single_violation(
            "the lane's timeout was not forwarded", argv, kwargs,
        )

    def test_uncaptured_output_trips_only_the_capture_clause(self) -> None:
        argv, kwargs = self._good_call()
        kwargs["capture_output"] = False
        self._assert_single_violation("output is not captured", argv, kwargs)

    def test_a_missing_env_trips_only_the_environment_clause(self) -> None:
        argv, kwargs = self._good_call()
        del kwargs["env"]
        self._assert_single_violation(
            "the resolved beets environment was not forwarded", argv, kwargs,
        )

    def test_an_uninvited_stdin_payload_trips_only_the_absence_clause(
        self,
    ) -> None:
        argv, kwargs = self._good_call()
        kwargs["input"] = b"{}"
        self._assert_single_violation(
            "a stdin payload appeared from nowhere", argv, kwargs,
        )

    def test_a_mutated_stdin_payload_trips_only_the_payload_clause(self) -> None:
        argv, kwargs = self._good_call()
        kwargs["input"] = b"tampered"
        self._assert_single_violation(
            "the stdin payload was not forwarded unchanged",
            argv, kwargs, payload=b'{"album_id": 7}',
        )

    def test_the_checker_accepts_the_faithful_call(self) -> None:
        argv, kwargs = self._good_call()
        self.assertEqual(
            token_integrity_violations(
                argv, kwargs, tail=self._GOOD_TAIL, timeout=42, payload=None,
            ),
            [],
        )

    def test_sp_run_is_the_captured_default(self) -> None:
        """Every property example injects a runner; pin that production
        still gets the real one (definition-time default, never patched)."""
        import inspect

        default = inspect.signature(
            run_pinned_beets_child,
        ).parameters["runner"].default
        self.assertIs(default, sp.run)


if __name__ == "__main__":
    unittest.main()
