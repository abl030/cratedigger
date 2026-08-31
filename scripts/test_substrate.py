"""Shared test-runtime substrate: admission, headroom, liveness, reaping.

One home for everything every test-runtime coordinator needs from the shared
RAM root before it may run: the private runtime tmpfs, the exclusive
admission lock and its holder identity, ``/proc`` process-liveness reads,
the headroom precondition, the scratch/bundle/receipt reapers, the final
gate itself, and every on-disk name those formats are spelled with (issue
#1278 item 6). Extracted verbatim from ``scripts/run_test_suite.py``, which
now imports what it still uses from here: the suite coordinator owns phases,
markers, and the bundle report; this module owns the substrate they all sit
on.

**Standard library only, deliberately.** Every OTHER Python consumer runs
inside the Nix dev shell, where third-party dependencies are on the path.
This module cannot assume that: ``scripts/run_final_gate.sh`` is a thin
wrapper that execs ``final-gate`` here with whatever bare interpreter is on
PATH, OUTSIDE any dev shell — the gate is what launches
``nix develop --command`` in the first place. (``scripts/test_tmpfs.sh``
runs ``write-owner-marker`` as a subprocess of its shell hook, so THAT
caller's ``python3`` is the dev shell's own; it is the gate that fixes this
constraint, not the marker.) Both shell files used to carry their own copies
of the ``/proc`` start-ticks read (and the gate, of the whole receipt
format); folding them onto this one module is only possible while it imports
nothing but the standard library.
``tests/test_test_substrate.py`` pins that from both sides: an AST audit of
this file's imports, and a real subprocess import with third-party
``site-packages`` stripped from ``sys.path``. Do not import ``msgspec``,
``lib/``, or ``tests/`` from this file.

Subcommands (each has exactly one caller; do not add a speculative verb):

``final-gate [status RECEIPT]``
    Run the canonical suite on a committed clean tree and publish a
    receipt, or report an existing receipt's state. Reached through
    ``scripts/run_final_gate.sh``, whose operator interface is unchanged.
``write-owner-marker DIRECTORY PID``
    Write the ``.owner`` scratch-tree ownership marker
    ``_scratch_tree_owner_dead`` reads back. Run as a subprocess by
    ``scripts/test_tmpfs.sh``'s shell hook. Best-effort: every failure
    exits 0 silently, because a lost marker leaves a tree unreaped
    forever, never wrongly reaped.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from pathlib import Path
from types import FrameType
from typing import TextIO


def private_runtime_dir(candidate: Path | None = None) -> Path:
    """Resolve and validate the caller-owned runtime tmpfs.

    Issue #1208 review D7 (documented, not changed here to avoid a merge
    collision with concurrent headroom-logic work in this same file): this
    reads ``XDG_RUNTIME_DIR`` only, with no ``CRATEDIGGER_TEST_RAM_ROOT``
    override — unlike ``check_suite_headroom`` below and
    ``scripts/test_tmpfs.sh``'s own scratch-tree parent, both of which DO
    honor that override. With ``CRATEDIGGER_TEST_RAM_ROOT`` set, the
    reaper (this module) and the scratch trees it exists to reap
    (``scripts/test_tmpfs.sh``) would resolve to DIFFERENT directories,
    so item 1's fix would not apply at all. Latent today: nothing in this
    repository sets that override outside tests and one doc recipe.
    """
    runtime = candidate or Path(
        os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
    )
    try:
        info = runtime.lstat()
    except OSError as exc:
        raise RuntimeError(f"private runtime directory is unavailable: {runtime}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise RuntimeError(f"private runtime directory is not a real directory: {runtime}")
    if info.st_uid != os.getuid():
        raise RuntimeError(f"private runtime directory is not owned by this user: {runtime}")
    if stat.S_IMODE(info.st_mode) != 0o700:
        raise RuntimeError(f"private runtime directory is not mode 0700: {runtime}")
    resolved = runtime.resolve(strict=True)
    completed = subprocess.run(
        ["findmnt", "-no", "FSTYPE", "-T", str(resolved)],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0 or completed.stdout.strip() != "tmpfs":
        raise RuntimeError(f"private runtime directory is not tmpfs: {runtime}")
    return resolved


#: Bound for a second concurrently-launched canonical suite waiting on the
#: exclusive test-RAM-root admission lock (issue #1111). Generous enough to
#: outlast one full canonical suite's own runtime; still bounded, so a wait
#: never becomes a silent hang.
DEFAULT_ADMISSION_TIMEOUT_SECONDS = 1200.0
DEFAULT_ADMISSION_POLL_SECONDS = 1.0
DEFAULT_ADMISSION_PROGRESS_INTERVAL_SECONDS = 30.0

#: Same env var and default as scripts/test_tmpfs.sh's own shell-entry
#: headroom guard (CRATEDIGGER_TEST_RAM_MIN_BYTES) — one threshold, not two
#: that can silently drift apart.
DEFAULT_MIN_HEADROOM_BYTES = 1_073_741_824

#: A completed check bundle survives at least this long before admission-time
#: reaping may remove it. This protects the bundle's DETAILED EVIDENCE (the
#: per-phase logs and summary.md a final-gate receipt points at), not
#: receipt reuse itself: `run_final_gate.sh status` only confirms the
#: receipt's own `terminal` and `bundle` FILE exist (a path string) — a
#: `pass` receipt's verdict outlives its bundle regardless of this floor.
#: `_status_receipt` separately stats the bundle path and fails visibly when
#: it is gone (issue #1111 review). The floor's real job is keeping that
#: detailed evidence around for one ordinary session (CLAUDE.md "Running
#: tests"), not proving the receipt itself is still trustworthy. Reaping
#: only ever runs from `run_suite` while holding the admission lock
#: exclusively, so any bundle another `run_suite` call created predates this
#: run regardless of age — see the narrower claim in
#: `reap_stale_check_bundles` below; the age gate exists purely to protect
#: near-term evidence, not to prove liveness.
DEFAULT_STALE_BUNDLE_MAX_AGE_SECONDS = 4 * 60 * 60

#: The one named identity every RAM-root-exhaustion signal uses: at suite
#: start it is emitted by this module's own ``check_suite_headroom``, which
#: ``scripts/run_test_suite.py::run_suite`` calls before any phase runs (that
#: file no longer spells this string at all), and mid-run by
#: ``scripts/run_python_tests.py`` — issue #1111 item 2's single
#: failure-index entry instead of N disguised test failures.
TEST_RAM_ROOT_EXHAUSTED = "test RAM root exhausted"


class SuiteAdmissionTimeout(RuntimeError):
    """The bounded wait for exclusive canonical-suite admission expired."""


class RamRootExhaustedError(RuntimeError):
    """The shared test RAM root lacks the headroom a suite run requires."""


#: Name of the advisory admission lockfile inside the shared test RAM root.
#: Spelled once in PRODUCTION code, here (issue #1278 item 6): this module is
#: the one home for the on-disk names the test runtime uses, so renaming one
#: is a single production edit rather than a search across coordinators.
#: Tests (``tests/test_suite_coordinator.py``) and ``.claude/rules/
#: code-quality.md`` spell it again on purpose — a pin that re-derived the
#: name from this constant would agree with a rename by construction.
ADMISSION_LOCK_NAME = ".cratedigger-test-admission.lock"


def admission_lock_path(runtime: Path) -> Path:
    """Lockfile scoped to the shared test RAM root, not to any one suite run."""
    return runtime / ADMISSION_LOCK_NAME


def _read_proc_stat(pid: int) -> str:
    """Real ``/proc/<pid>/stat`` reader — the sole production default for
    ``_proc_stat_start_ticks`` below.

    Issue #1208 review D-F7: ``Path.read_text()`` with the default codec
    can raise ``UnicodeDecodeError`` — a ``ValueError``, not an
    ``OSError`` — which would escape ``_proc_stat_start_ticks``'s
    ``except OSError`` and abort the whole suite rather than degrading to
    "cannot verify" like every other read failure here.
    ``errors="replace"`` makes a decode failure produce mangled-but-valid
    text instead (which then correctly fails the caller's field-count /
    digit checks), never an exception this reader's caller cannot catch.
    """
    return Path(f"/proc/{pid}/stat").read_text(errors="replace")


def _proc_stat_start_ticks(
    pid: int, *, read_stat: Callable[[int], str] = _read_proc_stat
) -> tuple[bool, str | None]:
    """(confirmed_absent, start_ticks) for /proc/<pid>/stat — field 22.

    Three real outcomes, not two: the pid is CONFIRMED gone (``ENOENT`` —
    the kernel's own proof no such process exists); the pid exists and its
    start ticks were read and parsed; or the read failed for some OTHER
    reason (a permission boundary, e.g. a restrictive ``hidepid`` mount, or
    a malformed stat line) and liveness could not be determined AT ALL.
    Only the first case returns ``confirmed_absent=True`` — every other
    failure mode, including a failed read, returns ``False`` there, because
    "cannot verify" is never the same fact as "confirmed gone" (issue
    #1208 review D4).

    ``_proc_start_ticks`` below collapses the second and third outcomes
    into one ``None`` — the right posture for its own callers (the
    admission-lock holder identity, receipt liveness): "unverified"
    already degrades to "not live" there, and the worst consequence is a
    wrong display name or an unretired receipt. ``_scratch_tree_owner_dead``
    needs the finer distinction, because ITS decision is deletion: treating
    "cannot verify" as "provably dead" there is exactly the conflation
    issue #1208 review D4 named, so it calls this function directly
    instead of going through the collapsed wrapper.

    ``read_stat`` is a kwarg-DI seam (code-quality.md "Picking a
    strategy" #2): production always uses the real default
    (``_read_proc_stat``); tests inject a fake to exercise the
    non-``FileNotFoundError`` OSError branch and the malformed-content
    branch, neither of which is constructible against a real ``/proc``
    entry without root or a user-namespace remap.

    Acknowledged residual (issue #1208 review D6 follow-up): ``ENOENT``
    means "no such pid IN THIS PROCESS'S OWN PID NAMESPACE" — an owner
    process alive in a DIFFERENT pid namespace (one this reader cannot
    see into) would read as ``ENOENT`` here too, and be treated as
    ``confirmed_absent=True`` exactly like a genuinely dead process.
    Unreachable today: nothing in this repository ever enters a pid
    namespace (the ``unshare`` calls elsewhere in this test suite remap
    USER namespaces only, for uid manipulation — a completely different
    namespace kind — never pid namespaces), so every real caller and
    every real owning shell share the host's single pid namespace. Stated
    here rather than defended against, since there is nothing in this
    codebase that could construct the scenario to defend against.
    """
    try:
        raw = read_stat(pid)
    except FileNotFoundError:
        return True, None
    except OSError:
        return False, None
    close = raw.rfind(")")
    if close == -1:
        return False, None
    fields = raw[close + 2 :].split()
    if len(fields) < 20:
        return False, None
    return False, fields[19]


def _proc_start_ticks(pid: int) -> str | None:
    """Field 22 of /proc/<pid>/stat — the one process-liveness read here.

    A thin wrapper over ``_proc_stat_start_ticks`` that collapses "confirmed
    gone" and "cannot verify" into the same ``None`` — see that function's
    docstring for why that collapse is correct for THIS function's callers
    but not for ``_scratch_tree_owner_dead``.
    """
    _confirmed_absent, ticks = _proc_stat_start_ticks(pid)
    return ticks


def _write_lock_holder_identity(descriptor: int) -> None:
    """Best-effort holder identity a waiter's progress message can read.

    Same shape as the final gate's own pid+start-ticks receipt identities
    (helper_pid/helper_start_ticks, gate_pid/gate_start_ticks): a bare pid is
    ambiguous across process reuse, start ticks disambiguate it. This write
    can itself be lost to the very exhaustion this PR exists for (the
    ``except OSError: pass`` below) — safe only because the identity is
    always cleared on release first (``_clear_lock_holder_identity``), so a
    lost write degrades to an empty file, never a stale one, and the reader
    additionally verifies liveness before trusting any content it finds.
    """
    ticks = _proc_start_ticks(os.getpid()) or "0"
    payload = f"{os.getpid()} {ticks}\n".encode()
    try:
        os.ftruncate(descriptor, 0)
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.write(descriptor, payload)
    except OSError:
        pass


def _clear_lock_holder_identity(descriptor: int) -> None:
    """Erase the holder identity before releasing the lock.

    Must run while still holding the exclusive flock (issue #1111 review
    MAJOR-2): without this, a released holder's identity lingers in the
    lockfile until the next acquirer overwrites it, so a waiter reading in
    that window would confidently name a pid that no longer holds — or no
    longer exists at all. Clearing before unlock also means a delayed clear
    can never race a new holder's own write and wipe it.
    """
    try:
        os.ftruncate(descriptor, 0)
    except OSError:
        pass


def _read_lock_holder_identity(lock_path: Path) -> str | None:
    """The lock's live holder identity, or None if stale/malformed/unreadable.

    A stored pid+ticks pair is trusted only after an explicit liveness
    check (``_proc_start_ticks(pid) == ticks``, the same comparison
    ``_receipt_process_field_live`` makes) — never on readability alone.
    Without it, a lingering write from a process that has since released or
    died (a write the release path failed to clear, or a write the current
    holder's own ``except OSError: pass`` swallowed) would be reported as
    live. A mismatch or a dead pid falls back to ``None``, and the caller
    falls back to the plain lockfile-path message.
    """
    try:
        content = lock_path.read_text().strip()
    except OSError:
        return None
    parts = content.split()
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        return None
    pid_str, ticks = parts
    if _proc_start_ticks(int(pid_str)) != ticks:
        return None
    return f"pid {pid_str}, start ticks {ticks}"


@contextmanager
def acquire_suite_admission(
    runtime: Path,
    *,
    stream: TextIO,
    timeout_seconds: float = DEFAULT_ADMISSION_TIMEOUT_SECONDS,
    poll_seconds: float = DEFAULT_ADMISSION_POLL_SECONDS,
    progress_interval_seconds: float = DEFAULT_ADMISSION_PROGRESS_INTERVAL_SECONDS,
) -> Generator[None]:
    """Serialize every ``run_suite`` invocation sharing one fixed-size test RAM root.

    Scoped to the suite-runner level (``scripts/run_test_suite.py``'s
    ``run_suite``) — never to every ``nix-shell`` entry, which would also
    serialize interactive dev shells that never call ``run_suite`` at all.

    Targeted runs DO take this same lock: ``scripts/test.sh`` ->
    ``scripts/run_targeted_tests.py`` -> this same ``run_suite``, with no
    ``runtime_dir`` override, so it resolves the identical shared root and
    contends for the identical lockfile as the canonical suite. This is
    deliberate, not an oversight — issue #1111's own incident record
    includes a ``scripts/test.sh`` run hitting ``BrokenProcessPool`` at host
    load ~46, a full canonical suite is short-lived relative to the default
    wait budget below (an operator can raise it via
    ``admission_timeout_seconds`` on any installation where that stops being
    true), and the reap-safety argument in ``reap_stale_check_bundles``
    below depends on every caller of this function serializing through it.
    A second concurrently-launched ``run_suite`` call — canonical or
    targeted — waits here, bounded, reporting what it is waiting for
    (including the current holder's identity when verified live), instead
    of colliding with the first one's roughly a dozen ephemeral PostgreSQL
    clusters.
    """
    lock_path = admission_lock_path(runtime)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        deadline = time.monotonic() + timeout_seconds
        reported = False
        next_progress = 0.0
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                now = time.monotonic()
                if now >= deadline:
                    raise SuiteAdmissionTimeout(
                        f"timed out after {timeout_seconds:.0f}s waiting for "
                        f"exclusive test admission: {lock_path} is held by "
                        "another canonical suite run"
                    ) from None
                if not reported or now >= next_progress:
                    holder = _read_lock_holder_identity(lock_path)
                    holder_note = f" ({holder})" if holder else ""
                    stream.write(
                        f"waiting for admission: another canonical suite{holder_note} "
                        f"holds the test RAM root ({lock_path}); "
                        f"{deadline - now:.0f}s left before timeout\n"
                    )
                    stream.flush()
                    reported = True
                    next_progress = now + progress_interval_seconds
                time.sleep(min(poll_seconds, max(0.0, deadline - now)))
        _write_lock_holder_identity(descriptor)
        if reported:
            stream.write(f"admission acquired: exclusive lock on {lock_path}\n")
            stream.flush()
        yield
    finally:
        try:
            _clear_lock_holder_identity(descriptor)
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)


#: Name of the typed summary ``scripts/run_test_suite.py::_publish_summary``
#: writes at the top level of a check bundle, in its final publication step
#: (first of the two files that step writes; ``summary.md`` follows it).
#: Spelled once in PRODUCTION code (issue #1278 item 6) because three separate
#: readers depend on it meaning the same file: that writer, this module's own
#: ``_bundle_last_activity`` (which keys a bundle's staleness on its mtime),
#: and ``_record_suite_bundle`` below (which refuses to record a bundle
#: without one). Tests keep their own hand-typed copies on purpose — a
#: fixture re-deriving the name from this constant could never detect a
#: rename.
SUMMARY_JSON_NAME = "summary.json"


def _bundle_last_activity(bundle: Path) -> float:
    """The bundle's most recent evidence write, or its own mtime as a fallback."""
    summary_path = bundle / SUMMARY_JSON_NAME
    try:
        return summary_path.stat().st_mtime
    except OSError:
        pass
    try:
        return bundle.stat().st_mtime
    except OSError:
        return 0.0


def _scratch_tree_last_activity(tree: Path) -> float:
    """Most recent mtime anywhere WITHIN ``tree``, not just its own top-level dir.

    ``_bundle_last_activity`` (above) is right for a ``run_suite`` check
    bundle: ``run_suite`` is the bundle's only writer, and it writes
    ``summary.json`` at the top level in its final publication step (with
    ``summary.md`` written immediately after), so the bundle's own dirent
    set is exactly what changes as work happens.

    A dev-shell scratch tree (``SCRATCH_TREE_PREFIX``) has no such shape
    (issue #1208 review D3). Its own top-level directory mtime is bumped
    only by a DIRECT child dirent change (create/delete/rename of an
    immediate child) — a process writing INTO an existing nested
    subdirectory never touches it. The owning shell's own children, or an
    orphaned descendant that outlives the shell itself (e.g. issue #1214's
    fuzz burst, which takes neither the admission lock nor a headroom
    guard), can write deep inside the tree for the whole run without the
    top-level dir's own mtime ever moving — so an age gate keyed on that
    one timestamp can silently stop tracking real activity long before
    writing actually stops, exactly the "mtime cannot distinguish
    abandoned from just idle" argument the ownership-marker design itself
    makes, just one level deeper. Walks the whole tree, seeds ``latest``
    with the tree's own top-level mtime, and takes the maximum observed
    across every entry, so ongoing nested activity that WRITES within
    ``max_age_seconds`` is never mistaken for staleness.

    Stated limitation (issue #1208 review D-F4): this only helps when a
    descendant WRITES. A descendant that is alive but genuinely quiet — a
    shell blocked on ``read``/``sleep`` while holding the tree open, with
    nothing touching any mtime — falls back to exactly the mtime
    heuristic this rationale argues against; the owner-death marker is
    what actually distinguishes "abandoned" from "idle" for the SHELL
    itself, but says nothing about a descendant of a dead owner that is
    still alive and simply not writing. Reproduced directly: a real
    SIGKILLed owner whose ``bash``/``sleep`` descendants were still alive
    and holding ``$TMPDIR`` was reaped anyway, because they were quiet.
    Not a defect this function can fix on its own — recorded as a known
    residual of the composed design.

    Deliberately does NOT check ``summary.json`` — nothing writes one
    inside a scratch tree today, and checking it here would silently key
    the age gate on a file that happens to share a name with an unrelated
    concept.
    """
    latest = 0.0
    try:
        latest = tree.stat().st_mtime
    except OSError:
        pass
    try:
        for root, dirs, files in os.walk(tree, onerror=lambda _exc: None):
            for name in (*dirs, *files):
                try:
                    entry_mtime = os.lstat(os.path.join(root, name)).st_mtime
                except OSError:
                    continue
                latest = max(latest, entry_mtime)
    except OSError:
        pass
    return latest


#: Directory-name prefixes the reaper protects the shared runtime tmpfs from
#: leaking forever: ``run_suite``'s own check bundles, plus this test-infra
#: change's own per-test scratch directories (``tests/test_suite_coordinator.py``)
#: — a killed test process (SIGKILL, OOM) leaks its ``tempfile.mkdtemp``
#: fixture before ``tearDown`` can remove it, in the exact directory this PR
#: exists to keep clean. Age-gated the same way as check bundles; a fresh
#: fixture belonging to a currently-running test is never touched.
#:
#: ``SCRATCH_TREE_PREFIX`` (issue #1208 item 1) is the dev shell's OWN main
#: scratch ``TMPDIR``: ``scripts/test_tmpfs.sh``'s ``setup_cratedigger_test_
#: tmpfs`` creates it at ``nix-shell`` entry with
#: ``mktemp -d "$parent/cratedigger-tests.XXXXXX"``, OUTSIDE the admission
#: lock, and cleans it up only via a shell EXIT trap — SIGTERM/SIGINT/SIGHUP
#: all run that trap correctly; only SIGKILL (the OOM killer, ``kill -9``,
#: host loss) skips it and leaks the tree forever. Live incident
#: 2026-08-19: two leaked trees (910M, 339M) starved a 3.1G RAM root to
#: 1.9G free at idle and deterministically failed a headroom-measuring test
#: on three consecutive otherwise-green gate runs.
#:
#: This prefix is deliberately NOT reaped on age alone. A prior attempt
#: used directory mtime as the liveness signal and was reverted after
#: review found it reaps LIVE trees: a busy suite's scratch root can go
#: quiet (old mtime) for hours while very much in use, so mtime cannot
#: distinguish "abandoned" from "just idle". Instead
#: ``reap_stale_check_bundles`` requires ``_scratch_tree_owner_dead`` to
#: PROVE the owning shell process is gone (pid+start-ticks, same
#: pid-reuse-safe ``_proc_start_ticks`` precedent as the admission-lock
#: holder identity and the final gate's own helper/gate identity)
#: before the age gate ever applies to a ``cratedigger-tests.*`` entry —
#: a live owner is never touched regardless of age, and a missing or
#: malformed marker fails closed (treated as "unknown, never reap", not
#: "abandoned").
SCRATCH_TREE_PREFIX = "cratedigger-tests."

#: Directory-name prefix of one ``run_suite`` check bundle. Spelled once in
#: PRODUCTION code (issue #1278 item 6): ``scripts/run_test_suite.py::
#: _create_bundle`` CREATES these, this module REAPS them, and a drift
#: between those two spellings would silently stop every bundle from ever
#: being reaped. The tests keep their own hand-typed copies — this module's
#: own reaper docstrings below name the ``cratedigger-checks.*`` glob in
#: prose too — deliberately: a fixture that re-derived the prefix from this
#: constant could never detect a rename.
CHECK_BUNDLE_PREFIX = "cratedigger-checks."
_REAPABLE_PREFIXES = (
    CHECK_BUNDLE_PREFIX,
    "cratedigger-suite-test-",
    "cratedigger-admission-test-",
    "cratedigger-reap-test-",
    "cratedigger-headroom-test-",
    SCRATCH_TREE_PREFIX,
)

#: Name of the ownership-marker file ``scripts/test_tmpfs.sh`` writes inside
#: its own ``cratedigger-tests.*`` scratch tree, immediately after
#: ``mktemp`` — same "<pid> <ticks>\n" content shape as the admission-lock
#: holder identity file.
SCRATCH_TREE_OWNER_MARKER_NAME = ".owner"


def _scratch_tree_owner_dead(
    tree: Path,
    *,
    proc_stat: Callable[[int], tuple[bool, str | None]] = _proc_stat_start_ticks,
) -> bool:
    """True only when ``tree``'s recorded pid+start-ticks owner is PROVABLY gone.

    Returns ``False`` — never eligible for reaping by this signal — for
    every case that is not a proven death: a missing marker file (the
    shell died before ``mktemp`` returned, or in the brief window before
    the marker write in ``scripts/test_tmpfs.sh``), unreadable content, a
    malformed pid/ticks pair, or a ``/proc`` read that failed for a reason
    OTHER than the pid being confirmed gone (issue #1208 review D4 — a
    permission boundary such as a restrictive ``hidepid`` mount, or a
    malformed stat line, is "cannot verify", never "provably dead"; this
    calls ``_proc_stat_start_ticks`` directly, not the collapsed
    ``_proc_start_ticks`` wrapper, specifically to keep that distinction —
    see its docstring). ``False`` here means "unknown", not "alive"; the
    caller must treat unknown exactly like alive. Only a syntactically
    well-formed marker naming a pid the kernel CONFIRMS is gone, or one
    whose current start ticks were read successfully and differ from the
    recorded value (the process is gone, or — since start ticks make pid
    reuse detectable, the same precedent ``_read_lock_holder_identity``
    already relies on — a different, unrelated process now holds that pid)
    returns ``True``.

    This is the fail-closed half of issue #1208 item 1's ownership-marker
    design: the reverted mtime-based attempt reaped live trees because an
    idle mtime is not a liveness signal. Here, "cannot prove dead" always
    wins over "looks stale".
    """
    try:
        content = (tree / SCRATCH_TREE_OWNER_MARKER_NAME).read_text().strip()
    except OSError:
        return False
    parts = content.split()
    # issue #1208 review D-F7: isdigit() (not isdecimal()) would accept
    # e.g. the superscript "²" — True for isdigit(), but int("²") raises
    # ValueError, escaping uncaught and aborting the whole suite.
    # isdecimal() is the correct predicate for "int() can parse this".
    if len(parts) != 2 or not all(part.isdecimal() for part in parts):
        return False
    pid_str, ticks = parts
    confirmed_absent, current_ticks = proc_stat(int(pid_str))
    if confirmed_absent:
        return True
    if current_ticks is None:
        return False
    return current_ticks != ticks


#: On-disk shape of one final-gate receipt directory, spelled exactly once
#: (issue #1278 item 6). Both sides now live here: ``_run_gate`` below WRITES
#: these files, ``_status_receipt`` and the reapers above READ them back, and
#: ``scripts/run_final_gate.sh`` is a wrapper that spells none of them.
FINAL_GATE_RECEIPT_PREFIX = "cratedigger-final-gate."
RECEIPT_TERMINAL_FIELD = "terminal"
RECEIPT_BUNDLE_FIELD = "bundle"
RECEIPT_HELPER_PID_FIELD = "helper_pid"
RECEIPT_HELPER_START_TICKS_FIELD = "helper_start_ticks"
RECEIPT_GATE_PID_FIELD = "gate_pid"
RECEIPT_GATE_START_TICKS_FIELD = "gate_start_ticks"
RECEIPT_REPO_ROOT_FIELD = "repo_root"
RECEIPT_HEAD_FIELD = "head"
RECEIPT_CLEAN_FIELD = "clean"
RECEIPT_COMMAND_FIELD = "command"
RECEIPT_OUTPUT_LOG_FIELD = "output.log"
#: Staging name for the atomic terminal publication: written in full, then
#: renamed over ``RECEIPT_TERMINAL_FIELD``, so no reader ever observes a
#: half-written verdict.
RECEIPT_TERMINAL_STAGING_FIELD = ".terminal"


def _receipt_protected_bundles(runtime: Path) -> frozenset[Path]:
    """Bundle paths a final-gate receipt still references.

    ``cratedigger-final-gate.*`` receipts are not in ``_REAPABLE_PREFIXES``
    (a different prefix, not reaped by this function) — they have their own
    age-gated retirement pass, ``reap_stale_final_gate_receipts`` (issue
    #1208 item 4), which ``run_suite`` calls before this function on every
    admitted run. A receipt still on disk when THIS function runs — whether
    fresh, or old but not yet retired because its recorded helper/gate
    process is still live — protects the bundle path it names, preserving
    both the pass/fail verdict AND its evidence for a long-running review
    (issue #1111 review m13) — without this, a review that outlives the age
    floor would lose the bundle out from under a still-valid receipt.
    Retiring a receipt (deleting it) naturally lapses this protection on the
    very next call, with no second cleanup path: an unprotected bundle then
    ages out through this function's ordinary age gate like any other. If a
    protected bundle is somehow gone anyway, that is a genuinely dangling
    receipt; this function's exclusion does not paper over it —
    ``run_final_gate.sh status``'s own stat check (issue #1111 review m5) is
    what surfaces that honestly.
    """
    protected: set[Path] = set()
    for receipt in sorted(runtime.glob(f"{FINAL_GATE_RECEIPT_PREFIX}*")):
        try:
            content = (receipt / RECEIPT_BUNDLE_FIELD).read_text().strip()
        except OSError:
            continue
        if content:
            protected.add(Path(content))
    return frozenset(protected)


#: A final-gate receipt survives at least this long before admission-
#: time retirement may remove it (issue #1208 item 4) — well past any
#: realistic review horizon. Unlike ``DEFAULT_STALE_BUNDLE_MAX_AGE_SECONDS``
#: this is a fixed constant, not an env-overridable knob: shrinking receipt
#: retention is not something an ambient env var should be able to do by
#: accident. Before this, "Receipts are never retried or deleted
#: automatically" (the ``check`` skill) was true without qualification —
#: 61 of 68 receipts were pinning a bundle against the reaper in perpetuity
#: on the live 2026-08-20 root, with no exit path at all.
DEFAULT_RECEIPT_RETIREMENT_MAX_AGE_SECONDS = 7 * 24 * 60 * 60


def _receipt_last_activity(receipt: Path) -> float:
    """The receipt's most recent completion evidence, or its own mtime.

    Mirrors ``_bundle_last_activity``'s shape. ``_run_gate`` below writes
    ``terminal`` LAST, only after the suite itself has
    finished and the tree-match check has passed — so its mtime is the true
    "this receipt became final" timestamp for a completed run. A receipt
    that never reached that point (crashed, interrupted) falls back to the
    receipt directory's own mtime — issue #1208 review D8: that is NOT a
    timestamp that "only ever gets older". ``run_gate`` writes roughly ten
    files into the receipt over the run (``repo_root``, ``head``, ``clean``,
    ``command``, ``helper_pid``, ``helper_start_ticks``, ``output.log``,
    ``gate_pid``, ``gate_start_ticks``, ...), and each new dirent bumps the
    directory's own mtime — the true claim is only that it stops moving
    once nothing more is written into it, which for a crashed or
    interrupted run is exactly when the crash happened.
    """
    try:
        return (receipt / RECEIPT_TERMINAL_FIELD).stat().st_mtime
    except OSError:
        pass
    try:
        return receipt.stat().st_mtime
    except OSError:
        return 0.0


def _receipt_process_field_live(
    receipt: Path, pid_field: str, ticks_field: str
) -> bool:
    """True only when the named pid+ticks pair names a process still alive.

    The one receipt-identity liveness read (issue #1278 item 6): receipt
    retirement above and ``_status_receipt``'s own ``exact-active`` verdict
    below both call it, where the status side used to be a separate
    ``same_process`` function in bash. A missing, unreadable, or malformed
    identity is never treated as live — it is not evidence a process is
    running, so it cannot block retirement, and it cannot earn
    ``exact-active`` either; it is exactly the same "unverified is not
    live" posture ``_read_lock_holder_identity`` already takes for the
    admission lock.

    The missing-file case is real, not defensive: the gate announces its
    receipt BEFORE it can know the suite child's pid, so a helper killed in
    that window leaves a receipt with no gate identity at all
    (``tests/test_final_gate_receipt.py``'s hard-interruption scenario
    reproduces it). "Incomplete" is the honest verdict for such a receipt —
    it is unfinished, not malformed.
    """
    try:
        pid_text = (receipt / pid_field).read_text(errors="replace").strip()
        ticks_text = (receipt / ticks_field).read_text(errors="replace").strip()
    except OSError:
        return False
    # issue #1208 review D-F7: isdecimal(), not isdigit() — see
    # _scratch_tree_owner_dead's identical fix for why.
    if not pid_text.isdecimal() or not ticks_text.isdecimal():
        return False
    return _proc_start_ticks(int(pid_text)) == ticks_text


def _receipt_is_retirable(receipt: Path) -> bool:
    """A receipt's lifecycle is provably over — the age gate decides the rest.

    A ``terminal`` file is ``_run_gate``'s own completion marker, written
    last; its mere presence is definitive, exactly as ``_status_receipt``
    treats it (it never re-checks process liveness once ``terminal``
    exists). Without a ``terminal`` file the receipt is either still an
    in-progress run or one that crashed before finishing — retirement is
    safe only when BOTH its recorded helper (the gate process itself) and
    gate (the ``nix develop`` suite child)
    process identities are conclusively not live. Either one still live
    blocks retirement (issue #1208 item 4).
    """
    if (receipt / RECEIPT_TERMINAL_FIELD).exists():
        return True
    return not (
        _receipt_process_field_live(
            receipt, RECEIPT_HELPER_PID_FIELD, RECEIPT_HELPER_START_TICKS_FIELD
        )
        or _receipt_process_field_live(
            receipt, RECEIPT_GATE_PID_FIELD, RECEIPT_GATE_START_TICKS_FIELD
        )
    )


def reap_stale_final_gate_receipts(
    runtime: Path,
    *,
    max_age_seconds: float = DEFAULT_RECEIPT_RETIREMENT_MAX_AGE_SECONDS,
    reference_time: float | None = None,
) -> tuple[Path, ...]:
    """Age-gated retirement of final-gate receipts (issue #1208 item 4).

    Only ever called from ``run_suite``, while holding
    ``acquire_suite_admission`` exclusively, and BEFORE
    ``reap_stale_check_bundles`` — the same ownership precondition that
    function documents for itself. Retiring a receipt here deletes it from
    disk, so ``_receipt_protected_bundles``'s very next glob (inside
    ``reap_stale_check_bundles``) naturally no longer sees it: a retired
    receipt's bundle protection lapses on the same admitted pass, and the
    now-unprotected bundle ages out through that function's ordinary path.
    No second cleanup path exists for either receipts or bundles — this is
    the one lifecycle owner for both.

    A receipt is eligible only when ``_receipt_is_retirable`` proves its
    lifecycle is over AND it is older than ``max_age_seconds``. Both
    conditions are required: an old-but-still-live receipt is never touched,
    and a dead-but-fresh receipt waits out the floor like everything else.
    """
    reference = reference_time if reference_time is not None else time.time()
    retired: list[Path] = []
    for candidate in sorted(runtime.glob(f"{FINAL_GATE_RECEIPT_PREFIX}*")):
        try:
            info = candidate.lstat()
        except OSError:
            continue
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            continue
        if info.st_uid != os.getuid():
            continue
        if not _receipt_is_retirable(candidate):
            continue
        age = reference - _receipt_last_activity(candidate)
        if age < max_age_seconds:
            continue
        try:
            shutil.rmtree(candidate)
        except OSError:
            continue
        retired.append(candidate)
    return tuple(retired)


def reap_stale_check_bundles(
    runtime: Path,
    *,
    max_age_seconds: float = DEFAULT_STALE_BUNDLE_MAX_AGE_SECONDS,
    reference_time: float | None = None,
) -> tuple[Path, ...]:
    """Best-effort cleanup of scratch directories nothing can still be writing.

    Only ever called from ``run_suite``, before it creates its own bundle,
    while holding ``acquire_suite_admission`` exclusively (issue #1111 item
    1). That guarantees every ``cratedigger-checks.*`` directory belonging to
    ANOTHER ``run_suite`` invocation — including one a crashed prior holder
    left mid-run — predates this run: no other ``run_suite`` call can be
    concurrently writing a new one under the same lock. It does not cover
    every possible creator of a matched prefix: a test fixture can (and
    does — ``tests/test_final_gate_receipt.py``) construct a
    ``cratedigger-checks.*`` directory directly, entirely outside
    ``run_suite``. The age gate is what actually protects those, not lock
    exclusivity, which is also why it covers this PR's own test-scaffolding
    prefixes above — a killed test process can leak one with no
    ``run_suite`` involved at all. A bundle a live receipt still references
    (``_receipt_protected_bundles``) is never reaped regardless of age.

    ``SCRATCH_TREE_PREFIX`` (``cratedigger-tests.*``, issue #1208 item 1) is
    additionally gated on ``_scratch_tree_owner_dead`` — a live owner is
    never reaped regardless of age, and a missing/malformed marker is
    treated as unknown liveness, never as abandoned. Every other prefix
    keeps the pure age gate described above; only this one has a
    provable-death precondition (issue #1208 review D-F8: every prefix's
    creator is, in principle, a process capable of writing an ownership
    marker — this one is simply the only prefix whose creator actually
    DOES). Its age itself is also computed
    differently (``_scratch_tree_last_activity``, not
    ``_bundle_last_activity``): a scratch tree's own top-level directory
    mtime does not move when something writes into an existing nested
    subdirectory, so an owner-dead-but-still-being-written-by-a-descendant
    tree (issue #1208 review D3) is protected by walking the whole tree
    for its true most recent activity, not just its top-level dirent.
    """
    reference = reference_time if reference_time is not None else time.time()
    protected = _receipt_protected_bundles(runtime)
    reaped: list[Path] = []
    candidates = sorted(
        {
            candidate
            for prefix in _REAPABLE_PREFIXES
            for candidate in runtime.glob(f"{prefix}*")
        }
    )
    for candidate in candidates:
        if candidate in protected:
            continue
        try:
            info = candidate.lstat()
        except OSError:
            continue
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            continue
        if info.st_uid != os.getuid():
            continue
        is_scratch_tree = candidate.name.startswith(SCRATCH_TREE_PREFIX)
        if is_scratch_tree and not _scratch_tree_owner_dead(candidate):
            continue
        last_activity = (
            _scratch_tree_last_activity(candidate)
            if is_scratch_tree
            else _bundle_last_activity(candidate)
        )
        age = reference - last_activity
        if age < max_age_seconds:
            continue
        try:
            shutil.rmtree(candidate)
        except OSError:
            continue
        reaped.append(candidate)
    return tuple(reaped)


def recommended_worker_count(cpu_count: int) -> int:
    """Use three quarters of the host's threads, with no fixed ceiling.

    Issue #1131: the prior ``cpu_count // 2`` formula, capped at a flat 12,
    predates issue #1111's admission control and per-run headroom
    precondition (``check_suite_headroom`` below), which now fails the
    whole suite closed before any phase runs if the shared test RAM root
    lacks headroom, and predates this same issue's own ~61%
    ephemeral-PostgreSQL RAM/tmpfs diet and ``test_nix_module``
    concurrency cap (at most 2 heavy nix evals at once, regardless of the
    overall worker count). Those changes are what make raising the
    ceiling safe; the flat cap chosen before them was leaving real
    throughput on a quiet host on the table (8 workers on a 16-thread
    host).

    Measured on a quiet 16-thread/8-physical-core host (epimetheus, this
    issue's own review — see the PR body for the full table): Python-phase
    internal wall time was 164.5s/12 workers, 157.7s/16, then WORSE at
    157.8s/20 and 162.3s/24 — and 20 and 24 workers each additionally
    produced 2 spurious failures in unrelated, pre-existing timing-budget
    tests (``test_discogs_artist_concurrency``, ``test_node_jsonl_worker``)
    that never fail at 12 or 16, i.e. real degradation, not just a slower
    average. Peak tmpfs on that same host grew roughly linearly with
    worker count (776 MB/12 workers up to 1331 MB/24, of a 6.3 GB root)
    with peak RAM well within budget throughout (10.9-13.8 GB of 62 GB).

    Three quarters lands exactly on the operator's stated target of 12
    workers on a 16-thread host (comfortably below the knee above, ~4%
    slower than the 16-worker optimum with meaningfully more RAM/tmpfs
    margin and zero observed flakiness). On a 30-core host it lands at
    22, independently measured live (issue #1131 review, canonical
    ``run_tests.sh`` with the phase overlap below also active): wall
    107.4s, 6/6 phases PASSED, Python phase 102.1s, peak tmpfs 1178 MB of
    3245 MB (36%), peak RAM 19.3 GB of 30 GB, peak load1 21.08.

    That measured peak tmpfs (1178-1331 MB depending on host) EXCEEDS the
    OLD flat 1 GiB (1073.7 MB) headroom-precondition floor below — a run
    admitted with only slightly more than 1 GiB free could have exhausted
    the root mid-run under this worker count on the tighter host.
    ``default_min_headroom_bytes`` below is worker-aware for exactly
    that reason: its unset-default floor scales with THIS formula's own
    prediction (via ``_expected_worker_count``), so admission now refuses
    a run that does not have enough headroom for the workers it is about
    to start, rather than admitting it and finding out mid-run. No fixed
    ceiling beyond the three-quarters fraction itself: a far larger host
    is assumed to carry proportionally more RAM too, and that
    worker-aware floor, plus the ENOSPC-classified infrastructure-failure
    handling
    ``scripts/run_python_tests.py::_classify_target_infrastructure_failure``
    already provides, is the fail-closed backstop if that assumption is
    ever wrong on a given installation. ``CRATEDIGGER_TEST_JOBS`` remains
    the override in either direction, honored by both this function's own
    caller (``run_python_tests.py::_default_worker_count``) and by
    ``_expected_worker_count`` below.
    """
    if cpu_count < 1:
        raise ValueError("cpu_count must be at least 1")
    return max(1, cpu_count * 3 // 4)


def _expected_worker_count() -> int:
    """Best-effort prediction of the Python phase's own worker count.

    Used ONLY to size the tmpfs headroom floor before any phase has
    actually run (issue #1131 review N1) — not authoritative. Mirrors
    ``run_python_tests.py::_default_worker_count``'s precedence
    (``CRATEDIGGER_TEST_JOBS`` override, else ``recommended_worker_count``),
    but never raises on a malformed override: a bad value here just falls
    back to the formula's own estimate for sizing purposes, since the
    real, authoritative parse — and its hard failure on a genuinely
    malformed override — happens later, inside that phase's own argparse.
    """
    configured = os.environ.get("CRATEDIGGER_TEST_JOBS")
    if configured is not None:
        try:
            parsed = int(configured)
        except ValueError:
            parsed = 0
        if parsed >= 1:
            return parsed
    return recommended_worker_count(os.cpu_count() or 1)


#: Issue #1131 review N1: base term for the worker-aware headroom floor
#: below — the reviewer's own suggested model
#: (``max(1 GiB, 256 MB + 64 MB * jobs)``), sized to sit comfortably above
#: the measured peaks in ``recommended_worker_count``'s docstring (a
#: 1664 MB floor at doc1's 22 workers against a measured 1178 MB peak —
#: roughly 41% margin), not to exactly track them.
_HEADROOM_BASE_BYTES = 256 * 1024 * 1024
_HEADROOM_PER_WORKER_BYTES = 64 * 1024 * 1024


def headroom_floor_bytes(worker_count: int) -> int:
    """Worker-aware headroom floor, shared by every coordinator on this tmpfs.

    Reads the same ``CRATEDIGGER_TEST_RAM_MIN_BYTES`` env var
    ``scripts/test_tmpfs.sh``'s own shell-entry guard uses; an EXPLICIT
    override is honored exactly as given, with no worker-aware adjustment.
    The unset-default floor is
    ``max(DEFAULT_MIN_HEADROOM_BYTES, _HEADROOM_BASE_BYTES +
    _HEADROOM_PER_WORKER_BYTES * worker_count)`` — see
    ``recommended_worker_count``'s docstring for the measured evidence this
    per-worker term is sized against: the deterministic suite's own
    ephemeral-PostgreSQL worker footprint.

    Issue #1156 item 3: this function takes a plain, explicit
    ``worker_count`` rather than reading a suite-specific global, so every
    coordinator that admits its own pool of processes onto the shared
    tmpfs can call it. The deterministic suite passes its own predicted
    worker count (``_expected_worker_count``, via
    ``default_min_headroom_bytes`` below) because issue #1131 measured
    that specific per-worker footprint. The fuzz burst
    (``scripts/run_fuzz_tests.py``) and the world-model burst
    (``scripts/run_world_model_burst.py``) instead call this with
    ``worker_count=1`` — i.e. the flat ``DEFAULT_MIN_HEADROOM_BYTES``,
    still override-respecting — because there is no equivalent MEASURED
    per-worker footprint for either burst's own, differently-shaped worker
    pool (up to several dozen short-lived fuzz targets, vs. the suite's
    handful of ephemeral-PostgreSQL-backed workers), and issue #1214 is
    concurrently changing what that footprint even is. Extending the
    suite's own multiplier to them would be an unjustified guess dressed
    up as a measurement — confirmed live: sizing the fuzz burst's floor by
    its own default worker count (up to 60 on a 30-core host) demanded
    EXACTLY 4 GiB (256 MiB base + 64 MiB/worker * 60 workers = 4096 MiB,
    independent review B7 item 2 — "over 4 GiB" overstated it), more than
    this repository's own 3.1 GiB interactive dev tmpfs actually has free. Reusing the SAME flat floor
    ``scripts/test_tmpfs.sh``'s shell-entry guard already enforces for
    them today just moves the check from "one-shot at shell entry" to "a
    real coordinator precondition, with a mid-run recheck," without
    inventing a number nothing measures.
    """
    if worker_count < 1:
        raise ValueError("worker_count must be at least 1")
    raw = os.environ.get("CRATEDIGGER_TEST_RAM_MIN_BYTES")
    if raw is not None:
        try:
            value = int(raw)
        except ValueError:
            value = -1
        if value < 0:
            raise ValueError(
                "CRATEDIGGER_TEST_RAM_MIN_BYTES must be a non-negative integer"
            )
        return value
    return max(
        DEFAULT_MIN_HEADROOM_BYTES,
        _HEADROOM_BASE_BYTES + _HEADROOM_PER_WORKER_BYTES * worker_count,
    )


def default_min_headroom_bytes() -> int:
    """The deterministic suite's own floor: ``headroom_floor_bytes`` sized by
    the Python phase's own expected worker count (``_expected_worker_count``).

    Issue #1131 review N1: raising the default worker count means more
    concurrent ephemeral PostgreSQL clusters, so a flat 1 GiB floor no
    longer bounds a run's OWN peak tmpfs on a tight host — doc1 (30
    cores, 3.2 GB root) measured a 1178 MB peak at 22 workers, above the
    flat 1 GiB (1073.7 MB) floor a run could previously have been
    admitted under. ``scripts/test_tmpfs.sh``'s own shell-entry guard
    deliberately stays a flat 1 GiB for the DIFFERENT case it covers — a
    bare interactive ``nix-shell`` entry, skipped entirely once
    ``CRATEDIGGER_SUITE_OWNS_HEADROOM`` is set, where no worker count is
    even known yet.
    """
    return headroom_floor_bytes(_expected_worker_count())


def check_suite_headroom(runtime: Path, *, minimum_bytes: int) -> None:
    """One measured headroom check against the shared tmpfs root, raising
    ``RamRootExhaustedError`` on insufficient free bytes.

    Measures the same root scripts/test_tmpfs.sh's shell-entry guard does:
    ``CRATEDIGGER_TEST_RAM_ROOT`` when an operator has overridden it, else
    the validated ``runtime`` this suite was actually admitted under.

    The deterministic suite (``scripts/run_test_suite.py::run_suite``) calls
    this exactly once, immediately, before any phase runs (issue #1111) —
    for that caller ONLY, a trip fails the whole suite once and never
    mid-run. Issue #1156
    item 3: the fuzz burst (``scripts/run_fuzz_tests.py``) and the
    world-model burst (``scripts/run_world_model_burst.py``) also call
    this SAME function, but NOT exactly twice per coordinator run
    (independent review B7 item 1 — corrected): each makes exactly one
    preflight call, plus one call per outer admission-loop iteration for
    as long as ``pending`` remains non-empty — as few as one such mid-run
    call when the whole pool is admitted in a single cycle (two calls
    total), or several more when admission is staggered across multiple
    cycles because the worker pool or the separate PostgreSQL ceiling is
    already full. What stays true regardless of the exact count: "never
    mid-run" is a property of ``run_suite``'s own call site, not of this
    function itself.
    """
    target = Path(os.environ.get("CRATEDIGGER_TEST_RAM_ROOT") or runtime)
    available = shutil.disk_usage(target).free
    if available < minimum_bytes:
        raise RamRootExhaustedError(
            f"{TEST_RAM_ROOT_EXHAUSTED}: {target} has {available} bytes "
            f"free, needs {minimum_bytes}"
        )


#: The one command a final-gate receipt may record, and the only thing the
#: gate ever launches. One spelling for every reader that COMPARES it (issue
#: #1278 item 6): ``scripts/run_test_suite.py`` imports it as its own default
#: ``command``, ``_run_gate`` writes it into the receipt, and
#: ``_status_receipt`` checks a receipt's recorded command against it. It was
#: previously spelled in both that coordinator and
#: ``scripts/run_final_gate.sh``, where a drift would silently have made
#: every existing receipt read as non-canonical. Not an absolute claim about
#: the repository: ``scripts/daily_flake_update.sh`` and
#: ``scripts/daily_beets_tip_update.sh`` each spell the same command in their
#: own dev-shell invocation, where nothing compares it to a receipt and a
#: drift would fail loudly (a missing script) rather than silently.
CANONICAL_COMMAND = "bash scripts/run_tests.sh"

#: Operator interface of ``scripts/run_final_gate.sh``, which is now a thin
#: wrapper around ``final-gate`` here. Named for the wrapper, not for this
#: module: the wrapper is what an operator (and `.claude/skills/check`) types.
FINAL_GATE_USAGE = "usage: run_final_gate.sh [status RECEIPT]"

#: Marker the suite prints on stdout to publish its bundle path; the gate
#: harvests it out of the captured child output. Spelled once in PRODUCTION
#: code (issue #1278 item 6) because it is a contract between two processes:
#: ``scripts/run_test_suite.py::_terminal_summary`` WRITES it and
#: ``_record_suite_bundle`` below PARSES it, so a drift would silently make
#: every passing gate report "published no valid bundle" instead.
BUNDLE_ANNOUNCEMENT_PREFIX = "bundle: "

#: A signal is not command completion. Each of these leaves the receipt
#: inspectable-but-incomplete and exits with this status, which is also the
#: floor a child's own signal death is reported at (128 + signal number).
_SIGNAL_EXIT_STATUS = 128
_GATE_SIGNALS = (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)

_TERMINAL_PASS = "pass 0"
_TERMINAL_FAIL_PATTERN = re.compile(r"fail [1-9][0-9]*")


class FinalGateError(RuntimeError):
    """A gate refusal: its message goes to stderr and the CLI exits 2."""


def _receipt_field(receipt: Path, field: str) -> str:
    """One receipt field, or a refusal naming the field that is missing.

    Trailing newlines are stripped exactly as the shell's ``$(cat …)`` did,
    so every recorded value compares as it was written.
    """
    path = receipt / field
    if not path.is_file():
        raise FinalGateError(f"receipt is missing {field}: {receipt}")
    try:
        return path.read_text(errors="replace").rstrip("\n")
    except OSError as exc:
        raise FinalGateError(f"receipt is missing {field}: {receipt}") from exc


def _write_receipt_field(receipt: Path, field: str, content: str) -> None:
    (receipt / field).write_text(content, encoding="utf-8")


def _git(*arguments: str) -> tuple[bool, str]:
    """(succeeded, stdout without trailing newlines) for one git command.

    Runs in the caller's own working directory — the gate deliberately never
    changes it, so the receipt is bound to the worktree the operator ran it
    from, not to wherever this file happens to live.
    """
    completed = subprocess.run(
        ["git", *arguments], text=True, capture_output=True, check=False
    )
    return completed.returncode == 0, completed.stdout.rstrip("\n")


def _tree_mismatch_reason(receipt: Path) -> str | None:
    """Why ``receipt`` is not bound to the current committed clean tree.

    ``None`` means it is. One implementation for both callers: the status
    ladder raises the returned reason as a refusal, while ``_run_gate``
    only needs the yes/no before it publishes a terminal verdict.
    """
    repo_root = _receipt_field(receipt, RECEIPT_REPO_ROOT_FIELD)
    head = _receipt_field(receipt, RECEIPT_HEAD_FIELD)
    if _receipt_field(receipt, RECEIPT_CLEAN_FIELD) != "true":
        return f"receipt is not bound to a clean tree: {receipt}"
    in_worktree, current_root = _git("rev-parse", "--show-toplevel")
    if not in_worktree:
        return "status must run from a git worktree"
    head_resolved, current_head = _git("rev-parse", "HEAD")
    if not head_resolved:
        return "status cannot resolve HEAD"
    _succeeded, dirty = _git("status", "--porcelain", "--untracked-files=all")
    if current_root != repo_root or current_head != head or dirty:
        return f"receipt is not for this committed clean tree: {receipt}"
    return None


def _record_suite_bundle(receipt: Path, output_path: Path) -> bool:
    """Validate the bundle path the suite announced and record it.

    False means no bundle was recorded; the caller decides whether that is
    fatal (a PASSING suite owes one) or merely noted. Every VALIDATION
    rejection is reported on stderr as it happens, so a bundle rejected on
    its merits is never rejected silently. Three paths return False without
    a word, all of them cases where there is nothing to report ON: no
    announcement at all (the ordinary shape of a suite that died before
    publishing anything), an unreadable ``output.log``, and a bundle whose
    resolve/stat raises between the ``lstat`` above and the check below —
    a directory disappearing mid-validation, which the caller's own
    "published no valid bundle" message then covers.
    """
    try:
        lines = output_path.read_text(errors="replace").splitlines()
    except OSError:
        return False
    announced = [
        line.removeprefix(BUNDLE_ANNOUNCEMENT_PREFIX)
        for line in lines
        if line.startswith(BUNDLE_ANNOUNCEMENT_PREFIX)
    ]
    if not announced:
        return False
    if len(announced) != 1:
        print("test gate published multiple bundle paths", file=sys.stderr)
        return False
    runtime = private_runtime_dir()
    bundle = announced[0]
    try:
        info = Path(bundle).lstat()
    except OSError:
        print(f"test gate bundle is unavailable: {bundle}", file=sys.stderr)
        return False
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        print(f"test gate bundle is unavailable: {bundle}", file=sys.stderr)
        return False
    try:
        resolved = Path(bundle).resolve(strict=True)
        mode = stat.S_IMODE(resolved.stat().st_mode)
    except OSError:
        return False
    if resolved.parent != runtime or mode != 0o700:
        print(
            f"test gate bundle is not a private runtime directory: {bundle}",
            file=sys.stderr,
        )
        return False
    try:
        summary = (resolved / SUMMARY_JSON_NAME).lstat()
    except OSError:
        print(
            f"test gate bundle is missing {SUMMARY_JSON_NAME}: {bundle}",
            file=sys.stderr,
        )
        return False
    if stat.S_ISLNK(summary.st_mode) or not stat.S_ISREG(summary.st_mode):
        print(
            f"test gate bundle is missing {SUMMARY_JSON_NAME}: {bundle}",
            file=sys.stderr,
        )
        return False
    _write_receipt_field(receipt, RECEIPT_BUNDLE_FIELD, f"{resolved}\n")
    return True


def _leave_incomplete_receipt(_signum: int, _frame: FrameType | None) -> None:
    """Exit 128 without publishing a terminal verdict.

    ``os._exit`` rather than ``sys.exit``, because it is the exact analog of
    the shell's ``trap 'exit 128'``: the process is gone at the moment the
    signal is handled, with no interpreter teardown in between and nothing
    that can intercept it. A ``SystemExit`` instead only takes effect when
    the interrupted call returns to the interpreter, and unwinds through
    every ``finally`` and ``atexit`` handler on the way — any of which could
    alter this exit status or touch the receipt this handler exists to leave
    untouched and incomplete.

    NOT for the child's safety, and not to suppress output — both were
    measured, and neither is a reason: CPython's shutdown does not kill a
    ``Popen`` child (the suite survives either way, for the same reason it
    did under bash — nothing signals it), and a ``SystemExit`` raised from
    this handler exits 128 with an empty stderr, no ``ResourceWarning``
    among it. Skipping the buffer flush costs nothing here either: the
    receipt line was flushed explicitly and nothing else has been buffered.
    """
    os._exit(_SIGNAL_EXIT_STATUS)


def _committed_clean_tree() -> bool:
    """No unstaged, staged, or untracked change in the current worktree."""
    unstaged_clean, _ = _git("diff", "--quiet")
    if not unstaged_clean:
        return False
    staged_clean, _ = _git("diff", "--cached", "--quiet")
    if not staged_clean:
        return False
    _succeeded, untracked = _git("ls-files", "--others", "--exclude-standard")
    return not untracked


def _await_suite(receipt: Path, output_path: Path) -> int:
    """Launch the one canonical command, record its identity, and wait.

    Returns the child's status the way the shell's ``wait`` reported it:
    an ordinary exit code, or ``128 + signal`` for a signal death.
    ``Popen.wait`` reports the latter as a NEGATIVE return code, so the
    translation happens exactly here, once.

    ``CRATEDIGGER_SUITE_OWNS_HEADROOM`` goes into the child's environment
    (never its argv): ``run_suite``'s own post-lock headroom precondition
    is the single enforcement point for suite runs, so the dev-shell
    shellHook entry guard defers to it here (issue #1111 M2).
    ``nix develop`` rather than ``nix-shell`` for the eval-cache reason
    documented in full at ``scripts/test.sh`` (issue #1229). The gate is
    the best case for it: it already refuses to run on anything but a
    committed clean tree, which is exactly the state whose flake
    fingerprint Nix can cache — ~4.7s off every gate invocation.
    ``CANONICAL_COMMAND`` is unchanged, so the receipt still records the
    same canonical command and ``status`` still compares it the same way;
    only the launcher around it moved.
    """
    environment = dict(os.environ)
    environment["CRATEDIGGER_SUITE_OWNS_HEADROOM"] = "1"
    with output_path.open("wb") as log:
        child = subprocess.Popen(
            ["nix", "develop", "--command", "bash", "-c", CANONICAL_COMMAND],
            stdout=log,
            stderr=subprocess.STDOUT,
            env=environment,
        )
    gate_ticks = _proc_start_ticks(child.pid) or ""
    _write_receipt_field(receipt, RECEIPT_GATE_PID_FIELD, f"{child.pid}\n")
    _write_receipt_field(receipt, RECEIPT_GATE_START_TICKS_FIELD, f"{gate_ticks}\n")
    returncode = child.wait()
    if returncode < 0:
        return _SIGNAL_EXIT_STATUS - returncode
    return returncode


def _publish_terminal_verdict(receipt: Path, status: int) -> None:
    """Land the run's verdict under ``terminal``, atomically.

    Written in full under the staging name and then RENAMED into place, so
    no reader — a concurrent ``status``, or ``_receipt_is_retirable``'s own
    existence check — can ever observe a half-written verdict, or a
    ``terminal`` file that exists before its content does.

    Its own function so that contract is directly drivable: a test can hand
    it a receipt whose staging name is blocked and prove the verdict never
    lands, which is exactly what a direct write to ``terminal`` would break
    while leaving the whole gate suite green.
    """
    staging = receipt / RECEIPT_TERMINAL_STAGING_FIELD
    staging.write_text(
        f"{_TERMINAL_PASS}\n" if status == 0 else f"fail {status}\n", encoding="utf-8"
    )
    staging.replace(receipt / RECEIPT_TERMINAL_FIELD)


def _run_gate() -> int:
    """Run the canonical suite once on a committed clean tree, with a receipt."""
    runtime = private_runtime_dir()
    try:
        receipt = Path(tempfile.mkdtemp(prefix=FINAL_GATE_RECEIPT_PREFIX, dir=runtime))
    except OSError as exc:
        raise FinalGateError(
            f"cannot create final-gate receipt beneath {runtime}"
        ) from exc
    try:
        receipt.chmod(0o700)
    except OSError as exc:
        raise FinalGateError(f"cannot secure receipt directory: {receipt}") from exc

    in_worktree, repo_root = _git("rev-parse", "--show-toplevel")
    if not in_worktree:
        raise FinalGateError("final gate must run from a git worktree")
    _write_receipt_field(receipt, RECEIPT_REPO_ROOT_FIELD, f"{repo_root}\n")
    if not _committed_clean_tree():
        raise FinalGateError("final gate requires a committed clean tree")
    _head_resolved, head = _git("rev-parse", "HEAD")
    _write_receipt_field(receipt, RECEIPT_HEAD_FIELD, f"{head}\n")
    _write_receipt_field(receipt, RECEIPT_CLEAN_FIELD, "true\n")
    _write_receipt_field(receipt, RECEIPT_COMMAND_FIELD, f"{CANONICAL_COMMAND}\n")
    _write_receipt_field(receipt, RECEIPT_HELPER_PID_FIELD, f"{os.getpid()}\n")
    helper_ticks = _proc_start_ticks(os.getpid())
    if helper_ticks is None:
        raise FinalGateError("cannot record helper process identity")
    _write_receipt_field(
        receipt, RECEIPT_HELPER_START_TICKS_FIELD, f"{helper_ticks}\n"
    )
    output_path = receipt / RECEIPT_OUTPUT_LOG_FIELD
    output_path.write_text("", encoding="utf-8")

    print(f"receipt: {receipt}", flush=True)
    for gate_signal in _GATE_SIGNALS:
        signal.signal(gate_signal, _leave_incomplete_receipt)

    status = _await_suite(receipt, output_path)
    if status >= _SIGNAL_EXIT_STATUS:
        print(
            f"final gate: incomplete (signal-shaped exit {status})", file=sys.stderr
        )
        return status
    if not _record_suite_bundle(receipt, output_path):
        if status == 0:
            print(
                "final gate: incomplete (passing suite published no valid bundle)",
                file=sys.stderr,
            )
            return 2
        print("final gate: bundle unavailable for failed suite", file=sys.stderr)
    if _tree_mismatch_reason(receipt) is not None:
        print(
            "final gate: incomplete (tree changed before terminal receipt)",
            file=sys.stderr,
        )
        return 2
    _publish_terminal_verdict(receipt, status)
    for gate_signal in _GATE_SIGNALS:
        signal.signal(gate_signal, signal.SIG_DFL)
    if status == 0:
        print("final gate: pass (exit 0)")
    else:
        print(f"final gate: fail (exit {status})")
    return status


def _status_receipt(argument: str) -> int:
    """Report one receipt's state: pass, fail, exact-active, or incomplete.

    Every refusal on the way is a distinct message naming what failed, so a
    receipt that cannot be trusted is never silently reported as a verdict.
    One deliberate exception: a MISSING process-identity field is not a
    refusal but part of the ``incomplete`` verdict itself — see
    ``_receipt_process_field_live`` for the window that produces one.
    """
    runtime = private_runtime_dir()
    candidate = Path(argument)
    try:
        info = candidate.lstat()
    except OSError as exc:
        raise FinalGateError(f"receipt directory is unavailable: {argument}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise FinalGateError(f"receipt directory is unavailable: {argument}")
    try:
        receipt = candidate.resolve(strict=True)
    except OSError as exc:
        raise FinalGateError(f"receipt directory is unavailable: {argument}") from exc
    if receipt.parent != runtime:
        raise FinalGateError(
            "receipt is not directly beneath the private runtime directory: "
            f"{receipt}"
        )
    if stat.S_IMODE(receipt.stat().st_mode) != 0o700:
        raise FinalGateError(f"receipt is not mode 0700: {receipt}")
    mismatch = _tree_mismatch_reason(receipt)
    if mismatch is not None:
        raise FinalGateError(mismatch)
    if not (receipt / RECEIPT_OUTPUT_LOG_FIELD).is_file() or not (
        receipt / RECEIPT_COMMAND_FIELD
    ).is_file():
        raise FinalGateError(f"receipt metadata is incomplete: {receipt}")
    if _receipt_field(receipt, RECEIPT_COMMAND_FIELD) != CANONICAL_COMMAND:
        raise FinalGateError(f"receipt command is not canonical: {receipt}")

    terminal_path = receipt / RECEIPT_TERMINAL_FIELD
    bundle_path = receipt / RECEIPT_BUNDLE_FIELD
    terminal = (
        terminal_path.read_text(errors="replace").rstrip("\n")
        if terminal_path.is_file()
        else None
    )
    if terminal == _TERMINAL_PASS and not bundle_path.is_file():
        raise FinalGateError(
            f"passing receipt is missing its suite bundle path: {receipt}"
        )
    # A receipt's own terminal/bundle FILE surviving is not evidence the
    # bundle DIRECTORY it names still exists — admission-time reaping
    # (``reap_stale_check_bundles`` above) can remove an idle one out from
    # under an old receipt. One stat check, fail-visible, rather than
    # silently reporting "pass" over evidence that is gone (issue #1111
    # review m5).
    if bundle_path.is_file():
        recorded = bundle_path.read_text(errors="replace").rstrip("\n")
        if not Path(recorded).is_dir():
            raise FinalGateError(
                f"receipt's suite bundle no longer exists (likely reaped): {recorded}"
            )

    if terminal is not None:
        if terminal == _TERMINAL_PASS:
            print("pass")
        elif _TERMINAL_FAIL_PATTERN.fullmatch(terminal):
            print("fail")
        else:
            raise FinalGateError(f"receipt has an invalid terminal state: {receipt}")
        return 0

    helper_live = _receipt_process_field_live(
        receipt, RECEIPT_HELPER_PID_FIELD, RECEIPT_HELPER_START_TICKS_FIELD
    )
    gate_live = _receipt_process_field_live(
        receipt, RECEIPT_GATE_PID_FIELD, RECEIPT_GATE_START_TICKS_FIELD
    )
    print("exact-active" if helper_live and gate_live else "incomplete")
    return 0


def _final_gate(arguments: Sequence[str]) -> int:
    """``run_final_gate.sh``'s own argv contract, unchanged by the port."""
    if not arguments:
        return _run_gate()
    if arguments[0] == "status":
        if len(arguments) != 2:
            raise FinalGateError(FINAL_GATE_USAGE)
        return _status_receipt(arguments[1])
    raise FinalGateError(FINAL_GATE_USAGE)


def _write_owner_marker(directory: str, pid_text: str) -> int:
    """Write ``directory``'s ``.owner`` marker for ``pid_text``, best-effort.

    Exits 0 for every failure it can meet — a malformed pid, a ``/proc``
    entry it cannot read, an unwritable tree. The marker contract is
    fail-closed on the READER side
    (``_scratch_tree_owner_dead`` treats a missing or malformed marker as
    "unknown, never reap"), so a lost write leaves a scratch tree unreaped
    forever rather than wrongly reaped while its owner is alive — which is
    exactly why this must not turn a failed write into a failed shell
    entry. Skips the write entirely when the pid's start ticks cannot be
    read, rather than recording an identity that could never match.
    """
    try:
        pid = int(pid_text)
        ticks = _proc_start_ticks(pid)
        if ticks is None:
            return 0
        marker = Path(directory) / SCRATCH_TREE_OWNER_MARKER_NAME
        marker.write_text(f"{pid} {ticks}\n", encoding="utf-8")
    except (OSError, ValueError):
        return 0
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="test_substrate.py",
        description="Shared test-runtime substrate commands.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    gate = subcommands.add_parser(
        "final-gate",
        help="Run the canonical suite with a receipt, or inspect one.",
    )
    gate.add_argument(
        "arguments",
        nargs="*",
        help="nothing to run the gate, or `status RECEIPT` to inspect one",
    )
    marker = subcommands.add_parser(
        "write-owner-marker",
        help="Write a scratch tree's ownership marker (best-effort).",
    )
    marker.add_argument("directory", help="scratch tree to mark")
    marker.add_argument("pid", help="owning process id")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parsed = _parser().parse_args(argv)
    try:
        if parsed.command == "final-gate":
            arguments: list[str] = list(parsed.arguments)
            return _final_gate(arguments)
        directory: str = parsed.directory
        pid_text: str = parsed.pid
        return _write_owner_marker(directory, pid_text)
    except (RuntimeError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
