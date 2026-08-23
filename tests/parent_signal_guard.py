"""Shared guard for tests that intentionally kill "the parent process" to
simulate a broken/dead worker (BrokenProcessPool fixtures, drained-signal
fixtures, deploy-pin fault injection).

THE HAZARD (issue #1250): ``os.kill(os.getppid(), SIG)`` performs exactly
ONE ``getppid()`` syscall -- not two -- and that single read is a
snapshot of "whoever is my parent RIGHT NOW," evaluated the instant the
kill fires, not whenever the test's author reasoned about "my parent."
A test that captures the intended parent's PID once, early, and signals
THAT value later is narrowing the window between "decided to kill" and
"actually killed"; a test that reads ``os.getppid()`` bare, inline, as
the kill's own argument, has no such window to narrow at all -- it
trusts whatever the OS reports at that exact moment, with no record of
who the parent was ever supposed to be. If the INTENDED parent (a
``ProcessPoolExecutor`` pool worker, or another process expected to
still be alive) has already exited, the OS has already reparented this
process to whatever subreaper claims orphans, and that single late read
reports the ADOPTER, not the intended target. On a bare/container init,
orphans reparent to PID 1, which
refuses arbitrary signals from a non-root sender (EPERM) -- harmless. Under
a ``systemd --user`` session (interactive desktop/dev-host use, e.g. doc1),
``PR_SET_CHILD_SUBREAPER`` makes the **user manager itself** the new
parent, and it accepts signals sent by its own user -- so a stale
``os.getppid()`` read can SIGKILL/SIGTERM the manager, taking the entire
session down with it (tmux, every running agent, dbus, ...). Measured live
on doc1 2026-08-22 and 2026-08-23; full account in
``docs/solutions/testing/parent-signal-guard-worker-death-fixture.md``.

CAPTURING THE PID AT SPAWN IS NOT SUFFICIENT ON ITS OWN. PIDs are reused by
the OS; a value captured early and blindly trusted later can, in
principle, now name a completely unrelated process by the time the signal
fires. The live identity re-check below (``guard_refusal_reason``'s
reparent/PID-1/comm/cmdline clauses) is the load-bearing half regardless
of when the PID was captured -- capturing early only narrows the window,
it does not close it. Both halves are required together.

This module is the ONE place that decides "is it still safe to signal my
intended parent". ``tests/test_parent_signal_guard_audit.py`` (a bounded
text-pattern audit, modelled on
``tests/test_generated_node_worker_audit.py``) rejects any OTHER literal
``os.kill(os.getppid(), ...)`` call anywhere in the tree -- including
inside a generated child body written as a Python string, which is how
every real call site in this repo actually spells the hazard (see that
audit's own docstring for why a plain AST walk would miss those). This
is a bounded LITERAL-shape match, not semantic tracking: it does not
catch, and is not meant to catch, the shape a captured-and-verified kill
actually takes -- ``__pg_intended = os.getppid()`` followed later by
``os.kill(__pg_intended, SIG)`` -- which is exactly what
``guard_kill_statement`` below emits, and exactly what
``tests/fakes/deploy_pin.py``'s three real, literal, on-disk kill sites
now read. That shape is the FIX, not the hazard; widening the grammar to
flag it would also flag every ordinary known-PID kill elsewhere in the
tree that has nothing to do with this module at all.

Two call shapes are exposed, because not every call site can import this
module:

- ``guard_refusal_reason`` / ``guard_and_signal_parent`` -- the real,
  directly-testable implementation. Use these from any process that can
  ``import tests.parent_signal_guard`` (a fixture-generated ``.py`` file
  discovered by ``scripts/run_python_tests.py``'s own test loader always
  can: that runner inserts the real repository root onto ``sys.path`` at
  its own module top, unconditionally, before it ever loads a discovered
  test module -- see ``tests/test_parallel_test_runner.py``'s fixture for
  the worked example).
- ``guard_source_prelude`` / ``guard_kill_statement`` -- a source-text
  generator for a child body that CANNOT import this module: a script
  executed via ``python -S`` skipping ``site``, or one whose own
  ``sys.path[0]`` is a throwaway fixture directory rather than the repo
  root (``tests/fakes/deploy_pin.py``'s ``-S`` shim, and the inline
  ``python -c ...`` commands ``tests/test_suite_coordinator.py`` feeds a
  synthetic phase). Both functions only ever concatenate text -- they
  reimplement the SAME clauses as ``guard_refusal_reason`` in a
  stdlib-only, zero-import source snippet, never evaluate anything
  themselves, and never call ``os.kill`` directly at define time.
"""

from __future__ import annotations

import os
from collections.abc import Callable

#: The pool-worker argv fragment a real ``ProcessPoolExecutor`` child
#: carries (measured on CPython 3.14.7,
#: ``multiprocessing.spawn.spawn_main`` invoked via
#: ``python3 -s -c 'from multiprocessing.spawn import spawn_main; ...'
#: --multiprocessing-fork``). A real pool worker's cmdline does NOT contain
#: this repository's runner path -- issue #1250's own suggested
#: "``/proc/<ppid>/cmdline`` contains the runner path" guard was measured
#: and silently disables the fixture (zero kills fire, no markers, exit 0)
#: because that substring is never present. This flag is specific to the
#: ``spawn`` multiprocessing start method: CPython 3.14's own DEFAULT
#: start method is ``forkserver``, whose worker cmdline does NOT carry
#: ``--multiprocessing-fork`` at all -- this constant is only ever true
#: because ``scripts/run_python_tests.py`` explicitly pins
#: ``multiprocessing.get_context("spawn")`` for its own pool. If that pin
#: is ever dropped, every real call site using this default fails LOUDLY
#: (every kill refuses, the fixture stops exercising anything, and the
#: existing "does the fixture still work" assertions catch it), not
#: silently. Only meaningful when the intended parent actually IS
#: expected to be a pool worker; pass ``expected_signature=None`` when it
#: is not (see module docstring).
DEFAULT_PARENT_SIGNATURE = "--multiprocessing-fork"

#: A function that reads one ``/proc/<pid>/...`` pseudo-file's text, or
#: ``None`` if it could not be read. Injectable so ``guard_refusal_reason``
#: is unit-testable against a simulated orphaned/systemd/malformed parent
#: without ever touching a real process.
ProcReader = Callable[[str], str | None]


def _read_proc_text(path: str) -> str | None:
    try:
        with open(path, "rb") as handle:
            return handle.read().decode("utf-8", errors="replace")
    except OSError:
        return None


def capture_intended_parent_pid() -> int:
    """Read ``os.getppid()`` NOW. Call this as early as possible in a
    short-lived child's life -- ideally the first statement executed --
    and pass the RETURNED value to ``guard_refusal_reason`` /
    ``guard_and_signal_parent`` later. See the module docstring for why
    this alone does not make a later signal safe: the live re-check is
    what actually decides that."""
    return os.getppid()


def guard_refusal_reason(
    intended_parent_pid: int,
    *,
    expected_signature: str | None = DEFAULT_PARENT_SIGNATURE,
    current_ppid_fn: Callable[[], int] = os.getppid,
    read_proc_fn: ProcReader = _read_proc_text,
) -> str | None:
    """Return a reason it is UNSAFE to signal ``intended_parent_pid`` right
    now, or ``None`` if every clause passes and signalling is safe.

    Clauses, each independently sufficient to refuse, checked in order:

    1. reparented -- the live parent (``current_ppid_fn()``) is no longer
       ``intended_parent_pid``. The single most common real-world trigger
       (issue #1250): the intended parent already exited and this process
       was reparented to a subreaper.
    2. pid 1 -- belt-and-braces: never signal init / a container reaper,
       even if (impossibly) it were still reported as the live parent.
    3. ``comm`` is ``systemd`` -- belt-and-braces: never signal a
       systemd-family manager by name, regardless of its PID. This is
       what actually protects a ``systemd --user`` session, where the
       reparented-to PID is the user manager, not PID 1.
    4. missing/wrong signature -- only checked when ``expected_signature``
       is not ``None``: the live parent's ``/proc/<pid>/cmdline`` must
       contain that substring, or refuse. Pass ``expected_signature=None``
       for a caller whose intended parent is never a pool worker (there is
       no meaningful "expected shape" to check) -- this also skips the
       one extra ``/proc`` read that clause costs.

    Never raises for a missing/unreadable ``/proc`` entry; a refusal is
    always a plain string, never an exception -- a guard that could itself
    crash the fixture would defeat the point.
    """
    current_parent = current_ppid_fn()
    if current_parent != intended_parent_pid:
        return (
            f"reparented: intended parent {intended_parent_pid} is no "
            f"longer this process's live parent (now {current_parent})"
        )
    if current_parent == 1:
        return "refusing to signal pid 1"
    comm = read_proc_fn(f"/proc/{current_parent}/comm")
    if comm is None:
        return f"unreadable /proc/{current_parent}/comm"
    if comm.strip() == "systemd":
        return f"refusing to signal a systemd-comm process (pid {current_parent})"
    if expected_signature is not None:
        cmdline = read_proc_fn(f"/proc/{current_parent}/cmdline")
        if cmdline is None:
            return f"unreadable /proc/{current_parent}/cmdline"
        if expected_signature not in cmdline:
            return (
                f"parent {current_parent} cmdline does not contain the "
                f"expected signature {expected_signature!r}"
            )
    return None


def guard_and_signal_parent(
    intended_parent_pid: int,
    sig: int,
    *,
    expected_signature: str | None = DEFAULT_PARENT_SIGNATURE,
    current_ppid_fn: Callable[[], int] = os.getppid,
    read_proc_fn: ProcReader = _read_proc_text,
    kill_fn: Callable[[int, int], None] = os.kill,
) -> str | None:
    """Signal ``intended_parent_pid`` with ``sig`` iff every
    ``guard_refusal_reason`` clause passes. Returns ``None`` on success
    (the signal was sent), or the refusal reason if the signal was
    skipped. NEVER raises for a refusal -- a skip must be a silent,
    successful no-op, not a fixture failure."""
    reason = guard_refusal_reason(
        intended_parent_pid,
        expected_signature=expected_signature,
        current_ppid_fn=current_ppid_fn,
        read_proc_fn=read_proc_fn,
    )
    if reason is not None:
        return reason
    kill_fn(intended_parent_pid, sig)
    return None


def guard_source_prelude(
    expected_signature: str | None = DEFAULT_PARENT_SIGNATURE,
) -> str:
    """Return a self-contained, stdlib-only Python source snippet defining
    ``__pg_refusal_reason(intended_pid)`` -- the SAME clauses as
    ``guard_refusal_reason`` above, reimplemented as plain source text for
    a generated child body that cannot import this module (see module
    docstring for which sites need this and why). Assumes ``os`` is
    already imported in the embedding scope; imports nothing itself, so it
    is safe to embed inside a stdlib-only ``-S`` shim
    (``tests/fakes/deploy_pin.py``) with zero added import cost. Embed
    this ONCE per file/process, near the top -- the emitted function
    closes over nothing and is cheap to define once and call from
    multiple guarded-kill sites in the same process.

    ``expected_signature=None`` omits the cmdline signature clause
    entirely (one fewer ``/proc`` read) for a caller with no meaningful
    "expected parent shape" to check -- e.g. ``deploy_pin.py``'s fake
    git/nix/hostname commands, whose real parent is a bash/pytest process,
    never a ``ProcessPoolExecutor`` worker, and
    ``tests/test_suite_coordinator.py``'s synthetic phase commands, whose
    real parent is the coordinator test process itself.
    """
    signature_repr = "None" if expected_signature is None else repr(expected_signature)
    lines = [
        "def __pg_refusal_reason(__pg_intended):",
        "    __pg_current = os.getppid()",
        "    if __pg_current != __pg_intended:",
        "        return 'reparented'",
        "    if __pg_current == 1:",
        "        return 'pid1'",
        "    try:",
        "        with open('/proc/%d/comm' % __pg_current, 'rb') as __pg_f:",
        "            __pg_comm = __pg_f.read().decode('utf-8', 'replace').strip()",
        "    except OSError:",
        "        return 'no-comm'",
        "    if __pg_comm == 'systemd':",
        "        return 'systemd-comm'",
        f"    __pg_sig = {signature_repr}",
        "    if __pg_sig is not None:",
        "        try:",
        "            with open('/proc/%d/cmdline' % __pg_current, 'rb') as __pg_f:",
        "                __pg_cmdline = __pg_f.read().decode('utf-8', 'replace')",
        "        except OSError:",
        "            return 'no-cmdline'",
        "        if __pg_sig not in __pg_cmdline:",
        "            return 'bad-signature'",
        "    return None",
        "",
    ]
    return "\n".join(lines)


def guard_kill_statement(intended_pid_expr: str, sig_expr: str) -> str:
    """One guarded-kill statement using the ``__pg_refusal_reason`` helper
    ``guard_source_prelude`` defines. ``intended_pid_expr`` / ``sig_expr``
    are Python source EXPRESSIONS -- already-available variable names or
    literals in the embedding scope -- not values: this function only
    concatenates text, it never evaluates anything, and never appears
    beside a bare ``os.getppid()`` argument (the exact shape
    ``tests/test_parent_signal_guard_audit.py`` rejects)."""
    return (
        f"if __pg_refusal_reason({intended_pid_expr}) is None:\n"
        f"    os.kill({intended_pid_expr}, {sig_expr})\n"
    )
