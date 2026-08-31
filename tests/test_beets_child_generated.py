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
from lib.beets_child import harness_session_argv, run_pinned_beets_child
from lib.util import beets_subprocess_env
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
    expected_env: dict[str, str],
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
    # Full equality with beets_subprocess_env()'s own result — a spawner
    # that filters or rebuilds the env (dropping PATH/HOME/BEETS_DB) would
    # satisfy any key-subset check while breaking every lane at once
    # (review round, reader finding 5).
    if kwargs.get("env") != expected_env:
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
        timeout=60,
    )
    def test_runner_receives_the_lane_argv_unchanged(
        self, tail: list[str], payload: bytes | None, timeout: int,
    ) -> None:
        runner = _RecordingRunner()
        with runtime_config():
            expected_env = beets_subprocess_env()
            run_pinned_beets_child(
                tail, timeout=timeout, input_bytes=payload, runner=runner,
            )

        argv, kwargs = runner.calls[0]
        self.assertEqual(
            token_integrity_violations(
                argv, dict(kwargs), tail=tail, timeout=timeout, payload=payload,
                expected_env=expected_env,
            ),
            [],
        )


class TestTokenIntegrityCheckerTripsOnViolations(unittest.TestCase):
    """Q1 per clause: each clause trips on the minimal world violating it
    alone while every other clause passes."""

    _GOOD_TAIL: ClassVar[list[str]] = ["-m", "beets", "write", "album_id:=7"]

    def _good_call(
        self,
    ) -> tuple[list[str], dict[str, object], dict[str, str]]:
        env: dict[str, str] = {
            "CRATEDIGGER_BEETS_PYTHON": FAKE_PYTHON,
            "BEETSDIR": "/var/lib/cratedigger/beets",
        }
        kwargs: dict[str, object] = {
            "capture_output": True, "timeout": 42, "env": env,
        }
        return [FAKE_PYTHON, *self._GOOD_TAIL], kwargs, env

    def _assert_single_violation(
        self,
        message: str,
        argv: list[str],
        kwargs: dict[str, object],
        *,
        expected_env: dict[str, str],
        payload: bytes | None = None,
    ) -> None:
        self.assertEqual(
            token_integrity_violations(
                argv, kwargs, tail=self._GOOD_TAIL, timeout=42, payload=payload,
                expected_env=expected_env,
            ),
            [message],
        )

    def test_a_wrong_interpreter_trips_only_the_prefix_clause(self) -> None:
        argv, kwargs, env = self._good_call()
        argv[0] = "/usr/bin/python3"
        self._assert_single_violation(
            "argv does not begin with the pinned interpreter", argv, kwargs,
            expected_env=env,
        )

    def test_joined_tokens_trip_only_the_tail_clause(self) -> None:
        argv, kwargs, env = self._good_call()
        argv[1:] = [" ".join(self._GOOD_TAIL)]
        self._assert_single_violation(
            "argv tail was not forwarded verbatim", argv, kwargs,
            expected_env=env,
        )

    def test_a_dropped_timeout_trips_only_the_timeout_clause(self) -> None:
        argv, kwargs, env = self._good_call()
        del kwargs["timeout"]
        self._assert_single_violation(
            "the lane's timeout was not forwarded", argv, kwargs,
            expected_env=env,
        )

    def test_uncaptured_output_trips_only_the_capture_clause(self) -> None:
        argv, kwargs, env = self._good_call()
        kwargs["capture_output"] = False
        self._assert_single_violation(
            "output is not captured", argv, kwargs, expected_env=env,
        )

    def test_a_missing_env_trips_only_the_environment_clause(self) -> None:
        argv, kwargs, env = self._good_call()
        del kwargs["env"]
        self._assert_single_violation(
            "the resolved beets environment was not forwarded", argv, kwargs,
            expected_env=env,
        )

    def test_a_filtered_env_trips_only_the_environment_clause(self) -> None:
        """A spawner that rebuilds the env from the two beets keys alone
        (dropping PATH/HOME/BEETS_DB) is a violation — the exact mutant a
        key-subset check tolerated (review round, reader finding 5)."""
        argv, kwargs, env = self._good_call()
        expected = {**env, "PATH": "/usr/bin", "HOME": "/home/op"}
        self._assert_single_violation(
            "the resolved beets environment was not forwarded", argv, kwargs,
            expected_env=expected,
        )

    def test_an_uninvited_stdin_payload_trips_only_the_absence_clause(
        self,
    ) -> None:
        argv, kwargs, env = self._good_call()
        kwargs["input"] = b"{}"
        self._assert_single_violation(
            "a stdin payload appeared from nowhere", argv, kwargs,
            expected_env=env,
        )

    def test_a_mutated_stdin_payload_trips_only_the_payload_clause(self) -> None:
        argv, kwargs, env = self._good_call()
        kwargs["input"] = b"tampered"
        self._assert_single_violation(
            "the stdin payload was not forwarded unchanged",
            argv, kwargs, payload=b'{"album_id": 7}', expected_env=env,
        )

    def test_the_checker_accepts_the_faithful_call(self) -> None:
        argv, kwargs, env = self._good_call()
        self.assertEqual(
            token_integrity_violations(
                argv, kwargs, tail=self._GOOD_TAIL, timeout=42, payload=None,
                expected_env=env,
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


#: The harness session argv grammar: [wrapper] (--pretend?) --noincremental
#: (--preserve-discogs-flat-subtracks?) --search-id <id> <path>. Length is
#: fully determined by the two flags.
def _session_argv_length(pretend: bool, preserve: bool) -> int:
    return 5 + int(pretend) + int(preserve)


def session_argv_violations(
    argv: list[str],
    *,
    harness_path: str,
    mb_release_id: str,
    album_path: str,
    pretend: bool,
    preserve_discogs_flat_subtracks: bool,
) -> list[str]:
    """Accumulating checker for invariant S — the session argv grammar.

    Positional clauses (never membership tests), so a generated
    ``mb_release_id`` that happens to equal a flag string can never fake a
    violation or mask one. Every clause evaluates; ordering cannot mask one.
    """
    def token(index: int) -> str | None:
        return argv[index] if 0 <= index < len(argv) else None

    violations: list[str] = []
    if token(0) != harness_path:
        violations.append("argv does not begin with the harness wrapper")
    if (token(1) == "--pretend") != pretend:
        violations.append("--pretend does not track the pretend flag")
    if token(1 + int(pretend)) != "--noincremental":
        violations.append("--noincremental is not at its place")
    if preserve_discogs_flat_subtracks and token(
        2 + int(pretend),
    ) != "--preserve-discogs-flat-subtracks":
        violations.append("the flat-subtracks token is not at its place")
    if argv[-3:] != ["--search-id", mb_release_id, album_path]:
        violations.append("the search-id/album-path tail is not verbatim")
    if len(argv) != _session_argv_length(
        pretend, preserve_discogs_flat_subtracks,
    ):
        violations.append("argv carries a missing or stray token")
    return violations


class TestSessionArgvGrammar(unittest.TestCase):
    """Invariant S: whatever strings a session supplies, the builder emits
    exactly the session grammar — arbitrary harness path, release id, and
    album path pass through verbatim, flags track their booleans, nothing
    is joined, dropped, or invented. The deterministic 2x2 table in
    ``tests/test_beets_child.py`` pins the exact shapes; this patrols the
    string space around them (review round 2, reader finding 3)."""

    @settings(deadline=None)
    @given(
        harness_path=st.text(min_size=1, max_size=40),
        mb_release_id=st.text(min_size=0, max_size=40),
        album_path=st.text(min_size=0, max_size=40),
        pretend=st.booleans(),
        preserve=st.booleans(),
    )
    @example(
        harness_path="/nix/store/x/harness/run_beets_harness.sh",
        mb_release_id="8987a929-0000-4000-8000-000000000001",
        album_path="/mnt/virtio/music/slskd/album",
        pretend=True,
        preserve=False,
    )
    @example(
        harness_path="/nix/store/x/harness/run_beets_harness.sh",
        mb_release_id="2085134",
        album_path="/processing/albums/x",
        pretend=False,
        preserve=True,
    )
    def test_builder_emits_the_session_grammar(
        self,
        harness_path: str,
        mb_release_id: str,
        album_path: str,
        pretend: bool,
        preserve: bool,
    ) -> None:
        argv = harness_session_argv(
            harness_path,
            mb_release_id=mb_release_id,
            album_path=album_path,
            pretend=pretend,
            preserve_discogs_flat_subtracks=preserve,
        )
        self.assertEqual(
            session_argv_violations(
                argv,
                harness_path=harness_path,
                mb_release_id=mb_release_id,
                album_path=album_path,
                pretend=pretend,
                preserve_discogs_flat_subtracks=preserve,
            ),
            [],
        )


class TestSessionArgvCheckerTripsOnViolations(unittest.TestCase):
    """Q1 per clause: each clause trips on a world violating it alone
    (in-place token substitutions keep every other clause satisfied)."""

    def _violations(
        self,
        argv: list[str],
        *,
        pretend: bool = True,
        preserve: bool = False,
    ) -> list[str]:
        return session_argv_violations(
            argv,
            harness_path="/h",
            mb_release_id="m",
            album_path="/a",
            pretend=pretend,
            preserve_discogs_flat_subtracks=preserve,
        )

    def test_a_wrong_wrapper_trips_only_the_wrapper_clause(self) -> None:
        self.assertEqual(
            self._violations([
                "/wrong", "--pretend", "--noincremental",
                "--search-id", "m", "/a",
            ]),
            ["argv does not begin with the harness wrapper"],
        )

    def test_a_replaced_pretend_token_trips_only_the_pretend_clause(
        self,
    ) -> None:
        self.assertEqual(
            self._violations([
                "/h", "--verbose", "--noincremental",
                "--search-id", "m", "/a",
            ]),
            ["--pretend does not track the pretend flag"],
        )

    def test_a_replaced_noincremental_trips_only_its_clause(self) -> None:
        self.assertEqual(
            self._violations([
                "/h", "--pretend", "--incremental",
                "--search-id", "m", "/a",
            ]),
            ["--noincremental is not at its place"],
        )

    def test_a_replaced_flat_subtracks_token_trips_only_its_clause(
        self,
    ) -> None:
        self.assertEqual(
            self._violations([
                "/h", "--pretend", "--noincremental", "--wrong-flag",
                "--search-id", "m", "/a",
            ], preserve=True),
            ["the flat-subtracks token is not at its place"],
        )

    def test_a_swapped_tail_trips_only_the_tail_clause(self) -> None:
        self.assertEqual(
            self._violations([
                "/h", "--pretend", "--noincremental",
                "--search-id", "/a", "m",
            ]),
            ["the search-id/album-path tail is not verbatim"],
        )

    def test_a_stray_token_trips_only_the_length_clause(self) -> None:
        self.assertEqual(
            self._violations([
                "/h", "--pretend", "--noincremental", "--stray",
                "--search-id", "m", "/a",
            ]),
            ["argv carries a missing or stray token"],
        )

    def test_the_checker_accepts_both_faithful_shapes(self) -> None:
        self.assertEqual(
            self._violations([
                "/h", "--pretend", "--noincremental",
                "--search-id", "m", "/a",
            ]),
            [],
        )
        self.assertEqual(
            self._violations([
                "/h", "--noincremental",
                "--preserve-discogs-flat-subtracks",
                "--search-id", "m", "/a",
            ], pretend=False, preserve=True),
            [],
        )


if __name__ == "__main__":
    unittest.main()
