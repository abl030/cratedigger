"""Per-clause proof for tests.parent_signal_guard (issue #1250).

Every clause is exercised with an INJECTED current-parent reader / getppid
function -- never a real process or a real signal. See
``tests/parent_signal_guard.py``'s module docstring and
``.claude/rules/code-quality.md`` § "Per-clause proof": each test builds the
minimal world that trips exactly ONE clause while every earlier clause
passes, and asserts that clause's own refusal message via
``assertRaisesRegex``-equivalent substring assertions (the checker here
returns a string rather than raising, so a plain ``assertIn`` on the
returned reason is the direct analogue).
"""

from __future__ import annotations

import unittest
from collections.abc import Callable
from typing import Self

from tests.parent_signal_guard import (
    DEFAULT_PARENT_SIGNATURE,
    capture_intended_parent_pid,
    guard_and_signal_parent,
    guard_kill_statement,
    guard_refusal_reason,
    guard_source_prelude,
)

_REAL_COMM = "/proc/4242/comm"
_REAL_CMDLINE = "/proc/4242/cmdline"


def _proc_reader(table: dict[str, str | None]) -> Callable[[str], str | None]:
    """A read_proc_fn stand-in: exact-path lookup, `None` for anything
    else (mirrors _read_proc_text's own None-on-OSError contract for an
    unmapped/unreadable path)."""

    def _read(path: str) -> str | None:
        return table.get(path)

    return _read


class TestGuardRefusalReasonClauses(unittest.TestCase):
    """Q1 from code-quality.md's per-clause rule: does each clause trip at
    all, with every earlier clause passing?"""

    def test_reparented_refuses(self) -> None:
        reason = guard_refusal_reason(
            4242,
            current_ppid_fn=lambda: 9999,
            read_proc_fn=_proc_reader({}),
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("reparented", reason)
        self.assertIn("4242", reason)
        self.assertIn("9999", reason)

    def test_pid_1_refuses(self) -> None:
        reason = guard_refusal_reason(
            1,
            current_ppid_fn=lambda: 1,
            read_proc_fn=_proc_reader({}),
        )
        self.assertEqual(reason, "refusing to signal pid 1")

    def test_unreadable_comm_refuses(self) -> None:
        reason = guard_refusal_reason(
            4242,
            current_ppid_fn=lambda: 4242,
            read_proc_fn=_proc_reader({}),  # comm path absent -> None
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("unreadable", reason)
        self.assertIn(_REAL_COMM, reason)

    def test_systemd_comm_refuses(self) -> None:
        reason = guard_refusal_reason(
            4242,
            current_ppid_fn=lambda: 4242,
            read_proc_fn=_proc_reader({_REAL_COMM: "systemd\n"}),
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("systemd", reason)

    def test_unreadable_cmdline_refuses(self) -> None:
        reason = guard_refusal_reason(
            4242,
            current_ppid_fn=lambda: 4242,
            read_proc_fn=_proc_reader({_REAL_COMM: "python3\n"}),
            # cmdline path absent -> None
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("unreadable", reason)
        self.assertIn(_REAL_CMDLINE, reason)

    def test_missing_signature_refuses(self) -> None:
        reason = guard_refusal_reason(
            4242,
            current_ppid_fn=lambda: 4242,
            read_proc_fn=_proc_reader(
                {
                    _REAL_COMM: "python3\n",
                    _REAL_CMDLINE: "python3\x00-c\x00print(1)\x00",
                }
            ),
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("expected signature", reason)
        self.assertIn(DEFAULT_PARENT_SIGNATURE, reason)

    def test_happy_path_returns_none(self) -> None:
        reason = guard_refusal_reason(
            4242,
            current_ppid_fn=lambda: 4242,
            read_proc_fn=_proc_reader(
                {
                    _REAL_COMM: "python3\n",
                    _REAL_CMDLINE: (
                        "/nix/store/xyz-python3/bin/python3\x00-s\x00-c"
                        "\x00from multiprocessing.spawn import spawn_main"
                        "\x00--multiprocessing-fork\x00"
                    ),
                }
            ),
        )
        self.assertIsNone(reason)

    def test_expected_signature_none_skips_cmdline_clause(self) -> None:
        """A caller with no meaningful parent-shape (deploy_pin.py,
        test_suite_coordinator.py) never even attempts the cmdline read --
        prove it by NOT registering that path in the reader table at all
        (an unmapped path would refuse with "unreadable ... cmdline" if the
        clause fired, so `None` here is only reachable by skipping it)."""
        reason = guard_refusal_reason(
            4242,
            expected_signature=None,
            current_ppid_fn=lambda: 4242,
            read_proc_fn=_proc_reader({_REAL_COMM: "bash\n"}),
        )
        self.assertIsNone(reason)


class TestGuardAndSignalParent(unittest.TestCase):
    """Q2-equivalent: does the wrapper actually gate the real signal call,
    using an injected kill_fn recorder -- never a real os.kill."""

    def test_refusal_never_calls_kill_fn(self) -> None:
        calls: list[tuple[int, int]] = []
        result = guard_and_signal_parent(
            4242,
            9,
            current_ppid_fn=lambda: 1,  # reparented to pid 1
            read_proc_fn=_proc_reader({}),
            kill_fn=lambda pid, sig: calls.append((pid, sig)),
        )
        self.assertIsNotNone(result)
        self.assertEqual(calls, [])

    def test_happy_path_calls_kill_fn_with_intended_pid_and_signal(self) -> None:
        calls: list[tuple[int, int]] = []
        result = guard_and_signal_parent(
            4242,
            9,
            current_ppid_fn=lambda: 4242,
            read_proc_fn=_proc_reader(
                {
                    _REAL_COMM: "python3\n",
                    _REAL_CMDLINE: f"python3\x00{DEFAULT_PARENT_SIGNATURE}\x00",
                }
            ),
            kill_fn=lambda pid, sig: calls.append((pid, sig)),
        )
        self.assertIsNone(result)
        self.assertEqual(calls, [(4242, 9)])

    def test_never_raises_for_a_refusal(self) -> None:
        """A skip must be a silent, successful no-op -- assert no
        exception propagates for every refusal clause's world, not just
        that a string comes back."""
        worlds: list[tuple[int, Callable[[], int], dict[str, str | None]]] = [
            (4242, lambda: 9999, {}),
            (1, lambda: 1, {}),
            (4242, lambda: 4242, {}),
            (4242, lambda: 4242, {_REAL_COMM: "systemd\n"}),
        ]
        for intended, current_fn, table in worlds:
            with self.subTest(intended=intended):
                try:
                    result = guard_and_signal_parent(
                        intended,
                        9,
                        current_ppid_fn=current_fn,
                        read_proc_fn=_proc_reader(table),
                        kill_fn=lambda pid, sig: self.fail(
                            "kill_fn must not be called on refusal"
                        ),
                    )
                except Exception as exc:  # noqa: BLE001 - proving no exception, any is a failure
                    self.fail(f"refusal raised {exc!r} instead of returning a reason")
                self.assertIsNotNone(result)


class TestCaptureIntendedParentPid(unittest.TestCase):
    def test_reads_os_getppid(self) -> None:
        import os
        from unittest import mock

        with mock.patch.object(os, "getppid", return_value=13131):
            self.assertEqual(capture_intended_parent_pid(), 13131)


# ---------------------------------------------------------------------------
# Source-emitting form: exec the ACTUAL generated text (not a re-implemented
# copy) against a fully fake `os` + `open`, so these tests prove the
# STRING guard_source_prelude()/guard_kill_statement() emit -- the exact
# text embedded into tests/fakes/deploy_pin.py's -S shim and
# tests/test_suite_coordinator.py's inline -c commands -- behaves like
# guard_refusal_reason(). No real process, no real /proc, no real signal.
# ---------------------------------------------------------------------------


class _FakeProcFile:
    def __init__(self, data: str) -> None:
        self._data = data.encode("utf-8")

    def read(self) -> bytes:
        return self._data

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False


def _fake_open_over(table: dict[str, str | None]) -> Callable[[str, str], _FakeProcFile]:
    def _open(path: str, mode: str) -> _FakeProcFile:
        assert mode == "rb", mode
        value = table.get(path)
        if value is None:
            raise OSError(f"no such fake proc file: {path}")
        return _FakeProcFile(value)

    return _open


class _FakeOsModule:
    def __init__(self, ppid: int, kill_calls: list[tuple[int, int]] | None = None) -> None:
        self._ppid = ppid
        self.kill_calls = kill_calls if kill_calls is not None else []

    def getppid(self) -> int:
        return self._ppid

    def kill(self, pid: int, sig: int) -> None:
        self.kill_calls.append((pid, sig))


def _exec_refusal_reason(
    *,
    expected_signature: str | None,
    live_ppid: int,
    intended_pid: int,
    proc_table: dict[str, str | None],
) -> object:
    namespace: dict[str, object] = {
        "os": _FakeOsModule(live_ppid),
        "open": _fake_open_over(proc_table),
    }
    source = guard_source_prelude(expected_signature)
    exec(  # noqa: S102 - exec's own generated text is under test, no untrusted input
        compile(source, "<guard_source_prelude>", "exec"), namespace
    )
    fn = namespace["__pg_refusal_reason"]
    assert callable(fn)
    return fn(intended_pid)


class TestGuardSourcePreludeExecutedText(unittest.TestCase):
    """Same clause set as TestGuardRefusalReasonClauses, but driving the
    literal emitted TEXT via exec() -- proves the generator, not a
    hand-written re-implementation of the same logic."""

    def test_reparented_refuses(self) -> None:
        result = _exec_refusal_reason(
            expected_signature=DEFAULT_PARENT_SIGNATURE,
            live_ppid=9999,
            intended_pid=4242,
            proc_table={},
        )
        self.assertEqual(result, "reparented")

    def test_pid_1_refuses(self) -> None:
        result = _exec_refusal_reason(
            expected_signature=DEFAULT_PARENT_SIGNATURE,
            live_ppid=1,
            intended_pid=1,
            proc_table={},
        )
        self.assertEqual(result, "pid1")

    def test_unreadable_comm_refuses(self) -> None:
        result = _exec_refusal_reason(
            expected_signature=DEFAULT_PARENT_SIGNATURE,
            live_ppid=4242,
            intended_pid=4242,
            proc_table={},
        )
        self.assertEqual(result, "no-comm")

    def test_systemd_comm_refuses(self) -> None:
        result = _exec_refusal_reason(
            expected_signature=DEFAULT_PARENT_SIGNATURE,
            live_ppid=4242,
            intended_pid=4242,
            proc_table={_REAL_COMM: "systemd\n"},
        )
        self.assertEqual(result, "systemd-comm")

    def test_unreadable_cmdline_refuses(self) -> None:
        result = _exec_refusal_reason(
            expected_signature=DEFAULT_PARENT_SIGNATURE,
            live_ppid=4242,
            intended_pid=4242,
            proc_table={_REAL_COMM: "python3\n"},
        )
        self.assertEqual(result, "no-cmdline")

    def test_missing_signature_refuses(self) -> None:
        result = _exec_refusal_reason(
            expected_signature=DEFAULT_PARENT_SIGNATURE,
            live_ppid=4242,
            intended_pid=4242,
            proc_table={
                _REAL_COMM: "python3\n",
                _REAL_CMDLINE: "python3\x00-c\x00print(1)\x00",
            },
        )
        self.assertEqual(result, "bad-signature")

    def test_happy_path_returns_none(self) -> None:
        result = _exec_refusal_reason(
            expected_signature=DEFAULT_PARENT_SIGNATURE,
            live_ppid=4242,
            intended_pid=4242,
            proc_table={
                _REAL_COMM: "python3\n",
                _REAL_CMDLINE: f"python3\x00{DEFAULT_PARENT_SIGNATURE}\x00",
            },
        )
        self.assertIsNone(result)

    def test_expected_signature_none_skips_cmdline_clause(self) -> None:
        result = _exec_refusal_reason(
            expected_signature=None,
            live_ppid=4242,
            intended_pid=4242,
            proc_table={_REAL_COMM: "bash\n"},  # cmdline deliberately absent
        )
        self.assertIsNone(result)


class TestGuardKillStatementExecutedText(unittest.TestCase):
    """Prove the emitted `guard_kill_statement` text actually gates
    `os.kill`, using the fake `os` module's own call recorder -- still no
    real signal."""

    def _run(
        self,
        *,
        expected_signature: str | None,
        live_ppid: int,
        intended_pid: int,
        proc_table: dict[str, str | None],
        sig: int = 9,
    ) -> list[tuple[int, int]]:
        fake_os = _FakeOsModule(live_ppid)
        namespace: dict[str, object] = {
            "os": fake_os,
            "open": _fake_open_over(proc_table),
        }
        source = (
            guard_source_prelude(expected_signature)
            + f"__pg_intended = {intended_pid}\n"
            + guard_kill_statement("__pg_intended", str(sig))
        )
        exec(  # noqa: S102 - exec's own generated text is under test, no untrusted input
            compile(source, "<guard_kill_statement>", "exec"), namespace
        )
        return fake_os.kill_calls

    def test_reparented_never_signals(self) -> None:
        calls = self._run(
            expected_signature=DEFAULT_PARENT_SIGNATURE,
            live_ppid=9999,
            intended_pid=4242,
            proc_table={},
        )
        self.assertEqual(calls, [])

    def test_happy_path_signals_the_intended_pid(self) -> None:
        calls = self._run(
            expected_signature=None,
            live_ppid=4242,
            intended_pid=4242,
            proc_table={_REAL_COMM: "bash\n"},
            sig=15,
        )
        self.assertEqual(calls, [(4242, 15)])


if __name__ == "__main__":
    unittest.main()
