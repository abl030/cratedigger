"""Contracts for RAM-backed test scratch storage."""

from __future__ import annotations

import ast
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from collections.abc import Mapping
from pathlib import Path

from scripts.test_substrate import (
    SCRATCH_TREE_OWNER_MARKER_NAME,
    SCRATCH_TREE_PREFIX,
    _scratch_tree_owner_dead,
)
from tests._source_pins import pinned_source

REPO_ROOT = Path(__file__).resolve().parent.parent
TMPFS_SETUP = REPO_ROOT / "scripts" / "test_tmpfs.sh"
SUBSTRATE_CLI = REPO_ROOT / "scripts" / "test_substrate.py"
NIX_SHELL = REPO_ROOT / "nix" / "shell.nix"
TMPFS_SETUP_AND_PRINT_TMPDIR = (
    'source "$1" && setup_cratedigger_test_tmpfs && printf "%s" "$TMPDIR"'
)
TMPFS_SETUP_AND_HOLD = (
    'source "$1" && setup_cratedigger_test_tmpfs '
    '&& printf "%s\n" "$TMPDIR" && read -r _cratedigger_test_tmpfs_hold_line'
)
LOW_HEADROOM_MINIMUM_BYTES = 1 << 50


def run_tmpfs_setup_and_print_tmpdir(
    *,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Drive the real shell helper and report the TMPDIR it selected."""
    return subprocess.run(
        [
            "bash",
            "-c",
            TMPFS_SETUP_AND_PRINT_TMPDIR,
            "bash",
            str(TMPFS_SETUP),
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def run_tmpfs_setup_and_hold(
    *,
    env: Mapping[str, str] | None = None,
) -> subprocess.Popen[str]:
    """Drive the real shell helper and keep the owning shell alive, blocked
    on stdin, until the caller lets it proceed.

    Issue #1208 review D1: ``run_tmpfs_setup_and_print_tmpdir`` prints
    TMPDIR then lets the whole script exit immediately, firing the EXIT
    trap before a caller can observe anything about the still-live tree —
    useless for proving the ownership marker's PRODUCER (this file) agrees
    with its READER (``scripts.test_substrate._scratch_tree_owner_dead``).
    This variant blocks on a `read` after allocation so a test can inspect
    the real ``.owner`` marker the real ``setup_cratedigger_test_tmpfs``
    wrote, and the real process it names, while genuinely alive — then
    either release it (closing stdin lets `read` return and the script
    exit normally, EXIT trap fires) or SIGKILL it (trap skipped, matching
    the founding incident).
    """
    return subprocess.Popen(
        ["bash", "-c", TMPFS_SETUP_AND_HOLD, "bash", str(TMPFS_SETUP)],
        cwd=REPO_ROOT,
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def tmpfs_runtime_root() -> str:
    """Return the helper's default root when no explicit override is set."""
    return os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")


def allocation_environment() -> dict[str, str]:
    """Make allocation seam tests independent of shared tmpfs headroom."""
    return {**os.environ, "CRATEDIGGER_TEST_RAM_MIN_BYTES": "0"}


def low_headroom_environment(
    *,
    inherited_tmpdir: str,
    minimum_bytes: int,
) -> dict[str, str]:
    """Force the helper's real default tmpfs root below the headroom gate.

    Explicitly clears CRATEDIGGER_SUITE_OWNS_HEADROOM: when this test suite
    itself runs via scripts/test.sh or run_final_gate.sh, THAT wrapper sets
    the var in the ambient environment for its whole nix-shell invocation
    (issue #1111 review M2), so a naive dict(os.environ) copy here would
    silently inherit the skip and defeat the very refusal this fixture
    exists to force.
    """
    env = dict(os.environ)
    env.pop("CRATEDIGGER_TEST_RAM_ROOT", None)
    env.pop("CRATEDIGGER_SUITE_OWNS_HEADROOM", None)
    env["TMPDIR"] = inherited_tmpdir
    env["CRATEDIGGER_TEST_RAM_MIN_BYTES"] = str(minimum_bytes)
    return env


def assert_tmpfs_setup_failure_contract(
    completed: subprocess.CompletedProcess[str],
    *,
    inherited_tmpdir: str,
    runtime_dir: str,
    minimum_bytes: int,
) -> None:
    """Reject masked setup failure or any inherited TMPDIR success shape."""
    if completed.returncode == 0:
        raise AssertionError("tmpfs setup failure was reported as success")
    if completed.stdout != "":
        raise AssertionError(
            "tmpfs setup failure exposed a selected TMPDIR: "
            f"{completed.stdout!r} (inherited {inherited_tmpdir!r})"
        )
    expected_diagnostic = re.compile(
        rf"^Test RAM root lacks headroom: {re.escape(runtime_dir)} has \d+ "
        rf"bytes, needs {minimum_bytes}\n$",
    )
    if not expected_diagnostic.fullmatch(completed.stderr):
        raise AssertionError(
            f"tmpfs setup failure lost its headroom diagnostic: {completed.stderr!r}"
        )


class TestTmpfsSetup(unittest.TestCase):
    def test_allocates_isolated_tmpfs_directory_and_cleans_it_on_exit(self) -> None:
        completed = run_tmpfs_setup_and_print_tmpdir(env=allocation_environment())

        self.assertEqual(completed.returncode, 0, completed.stderr)
        selected = Path(completed.stdout)
        runtime_dir = Path(
            os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
        )
        self.assertEqual(selected.parent, runtime_dir)
        self.assertTrue(selected.name.startswith("cratedigger-tests."))
        self.assertFalse(selected.exists())

    def test_low_headroom_does_not_report_inherited_tmpdir_as_allocation(self) -> None:
        runtime_dir = tmpfs_runtime_root()
        inherited_tmpdir = "/tmp/cratedigger-inherited-tmpdir"

        completed = run_tmpfs_setup_and_print_tmpdir(
            env=low_headroom_environment(
                inherited_tmpdir=inherited_tmpdir,
                minimum_bytes=LOW_HEADROOM_MINIMUM_BYTES,
            ),
        )

        assert_tmpfs_setup_failure_contract(
            completed,
            inherited_tmpdir=inherited_tmpdir,
            runtime_dir=runtime_dir,
            minimum_bytes=LOW_HEADROOM_MINIMUM_BYTES,
        )

    def test_suite_owns_headroom_skips_only_the_free_bytes_refusal(self) -> None:
        """Issue #1111 review M2: scripts/test.sh and run_final_gate.sh set
        CRATEDIGGER_SUITE_OWNS_HEADROOM=1 before their own nix-shell
        invocation so run_suite()'s own post-lock headroom precondition is
        the single enforcement point for suite runs — this proves the skip
        applies to the free-bytes check specifically (setup still runs,
        allocating a real TMPDIR) rather than disabling the guard wholesale.
        """
        env = low_headroom_environment(
            inherited_tmpdir="/tmp/cratedigger-inherited-tmpdir",
            minimum_bytes=LOW_HEADROOM_MINIMUM_BYTES,
        )
        env["CRATEDIGGER_SUITE_OWNS_HEADROOM"] = "1"

        completed = run_tmpfs_setup_and_print_tmpdir(env=env)

        self.assertEqual(completed.returncode, 0, completed.stderr)
        selected = Path(completed.stdout)
        self.assertEqual(selected.parent, Path(tmpfs_runtime_root()))
        self.assertTrue(selected.name.startswith("cratedigger-tests."))
        self.assertFalse(selected.exists())

    def test_suite_owns_headroom_does_not_skip_the_other_safety_checks(self) -> None:
        """The skip is scoped to the free-bytes refusal only — a world-
        writable ancestor is still refused even with the env var set."""
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; setup_cratedigger_test_tmpfs',
                "bash",
                str(TMPFS_SETUP),
            ],
            cwd=REPO_ROOT,
            env={
                **os.environ,
                "CRATEDIGGER_TEST_RAM_ROOT": "/dev/shm",
                "CRATEDIGGER_TEST_RAM_MIN_BYTES": "0",
                "CRATEDIGGER_SUITE_OWNS_HEADROOM": "1",
            },
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("replaceable ancestor", completed.stderr)

    def test_active_tmpdir_has_private_ancestry(self) -> None:
        current = Path(tempfile.gettempdir()).resolve()
        while True:
            mode = stat.S_IMODE(current.stat().st_mode)
            self.assertEqual(
                mode & 0o022,
                0,
                f"test TMPDIR has replaceable ancestor: {current}",
            )
            if current.parent == current:
                break
            current = current.parent

    def test_test_fixtures_do_not_bypass_tmpdir_for_repository_scratch(self) -> None:
        forbidden_dir_expressions = {
            "os.getcwd()",
            "Path.cwd()",
            "REPO_ROOT",
            "self._repo_root",
        }
        offenders: list[str] = []

        for path in sorted((REPO_ROOT / "tests").rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                for keyword in node.keywords:
                    if keyword.arg != "dir":
                        continue
                    rendered = ast.unparse(keyword.value)
                    if rendered in forbidden_dir_expressions:
                        offenders.append(
                            f"{path.relative_to(REPO_ROOT)}:{node.lineno}: {rendered}"
                        )

        self.assertEqual(offenders, [])

    def test_rejects_world_writable_tmpfs_ancestry(self) -> None:
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; setup_cratedigger_test_tmpfs',
                "bash",
                str(TMPFS_SETUP),
            ],
            cwd=REPO_ROOT,
            env={
                **os.environ,
                "CRATEDIGGER_TEST_RAM_ROOT": "/dev/shm",
                "CRATEDIGGER_TEST_RAM_MIN_BYTES": "0",
            },
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("replaceable ancestor", completed.stderr)

    def test_rejects_disk_backed_override_instead_of_falling_back(self) -> None:
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; setup_cratedigger_test_tmpfs',
                "bash",
                str(TMPFS_SETUP),
            ],
            cwd=REPO_ROOT,
            env={
                **os.environ,
                "CRATEDIGGER_TEST_RAM_ROOT": str(REPO_ROOT),
            },
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("is not tmpfs", completed.stderr)

    def test_cleanup_preserves_the_command_exit_status(self) -> None:
        completed = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1" && setup_cratedigger_test_tmpfs && exit 7',
                "bash",
                str(TMPFS_SETUP),
            ],
            cwd=REPO_ROOT,
            env=allocation_environment(),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 7, completed.stderr)

    def test_nix_shell_activates_tmpfs_before_dev_commands(self) -> None:
        source = pinned_source(NIX_SHELL)

        self.assertIn("scripts/test_tmpfs.sh", source)
        self.assertIn("setup_cratedigger_test_tmpfs", source)


class ScratchTreeOwnershipMarkerTestCase(unittest.TestCase):
    """Issue #1208 review D1: nothing bound the ``.owner`` PRODUCER (this
    file, ``scripts/test_tmpfs.sh``) to its READER
    (``scripts.test_substrate._scratch_tree_owner_dead``). Every existing
    test of the reader hand-typed the marker content
    (``tests/test_suite_coordinator.py``'s ``_write_owner_marker``), so a
    one-character producer edit — the wrong ``/proc`` field, ``$$`` ->
    ``$PPID``, the marker filename, the "<pid> <ticks>" delimiter — was
    invisible to the whole suite. This class drives the REAL
    ``setup_cratedigger_test_tmpfs`` and feeds the marker IT wrote
    straight into the real reader (test-fidelity.md Rule C: the trigger
    must come from the producer, never a literal)."""

    def test_real_marker_reports_alive_then_dead_across_a_real_sigkill(
        self,
    ) -> None:
        """THE producer<->reader binding proof. While the real owning
        shell (blocked on `read`, genuinely alive) holds the tree,
        ``_scratch_tree_owner_dead`` must read its real ``.owner`` marker
        as NOT dead. SIGKILLing that same shell — the founding incident,
        and the only signal that skips the EXIT trap — must flip the same
        function's verdict to dead, proven against the real process the
        marker actually names, not a hand-typed pid+ticks pair."""
        proc = run_tmpfs_setup_and_hold(env=allocation_environment())
        tree: Path | None = None
        try:
            assert proc.stdout is not None
            tmpdir_line = proc.stdout.readline()
            if not tmpdir_line:
                # Only read stderr on the failure path: the child is still
                # alive and blocked on `read` at this point on the happy
                # path, so an unconditional proc.stderr.read() here would
                # block until EOF — i.e. until the child exits, which it
                # never will while blocked. An f-string argument to
                # assertTrue is evaluated eagerly regardless of the
                # assertion's outcome, so that shape deadlocks even when
                # tmpdir_line IS truthy; this explicit branch is required,
                # not stylistic.
                self.fail(
                    "no TMPDIR line from the real setup function; "
                    f"stderr={proc.stderr.read() if proc.stderr else ''!r}"
                )
            tree = Path(tmpdir_line.strip())
            self.assertTrue(tree.name.startswith(SCRATCH_TREE_PREFIX))

            marker = tree / SCRATCH_TREE_OWNER_MARKER_NAME
            deadline = time.monotonic() + 5.0
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(
                marker.exists(),
                "the real setup_cratedigger_test_tmpfs never wrote .owner",
            )

            self.assertFalse(
                _scratch_tree_owner_dead(tree),
                "the real owning shell is alive but the real marker it "
                "wrote was read as dead",
            )

            proc.kill()
            proc.wait(timeout=5)

            self.assertTrue(
                tree.exists(),
                "SIGKILL must skip the EXIT trap, exactly like the "
                "founding incident (only SIGKILL does)",
            )
            self.assertTrue(
                _scratch_tree_owner_dead(tree),
                "the real owning shell is confirmed dead (SIGKILLed and "
                "reaped by wait()) but the real marker it wrote was read "
                "as alive",
            )
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
            for pipe in (proc.stdin, proc.stdout, proc.stderr):
                if pipe is not None:
                    pipe.close()
            if tree is not None:
                shutil.rmtree(tree, ignore_errors=True)

    def test_owner_marker_write_failure_does_not_leak_a_stray_diagnostic(
        self,
    ) -> None:
        """Issue #1208 review D5: bash redirections apply LEFT TO RIGHT,
        so a naive ``>file 2>/dev/null`` order does NOT suppress a failed
        OPEN of ``file`` — the diagnostic prints on the still-live
        original stderr before ``2>/dev/null`` is even installed.
        Overrides ``mktemp`` to chmod the real scratch tree read-only the
        instant it is created, forcing the real ``.owner`` write inside
        ``setup_cratedigger_test_tmpfs`` to fail exactly the way a
        full/permission-denied tmpfs does — reproduced against the real
        function, not a snippet in isolation. Uses the same held-shell
        pattern as the round-trip test above (not a plain
        ``subprocess.run``): the EXIT trap removes the read-only-but-empty
        tree the instant the script would otherwise exit, so the marker's
        absence can only be observed while the shell is still genuinely
        alive and deliberately blocked before that point."""
        override_mktemp_readonly = (
            "mktemp() {\n"
            "    local real_dir\n"
            '    real_dir=$(command mktemp "$@") || return 1\n'
            '    chmod 500 "$real_dir"\n'
            '    printf "%s\\n" "$real_dir"\n'
            "}\n"
            "export -f mktemp\n"
        )
        proc = subprocess.Popen(
            [
                "bash",
                "-c",
                override_mktemp_readonly + TMPFS_SETUP_AND_HOLD,
                "bash",
                str(TMPFS_SETUP),
            ],
            cwd=REPO_ROOT,
            env=allocation_environment(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        tree: Path | None = None
        try:
            assert proc.stdout is not None
            tmpdir_line = proc.stdout.readline()
            if not tmpdir_line:
                self.fail(
                    "no TMPDIR line from the real setup function; "
                    f"stderr={proc.stderr.read() if proc.stderr else ''!r}"
                )
            tree = Path(tmpdir_line.strip())

            # The shell is still blocked at `read` here — genuinely alive
            # and holding the read-only tree open — so this observes the
            # write's real outcome, not the EXIT trap's aftermath.
            self.assertTrue(tree.exists())
            self.assertFalse(
                (tree / SCRATCH_TREE_OWNER_MARKER_NAME).exists(),
                "the marker write should have failed on the read-only tree",
            )

            # A blank line, not a bare close: bash's `read` returns
            # non-zero on EOF-without-a-line, which the EXIT trap would
            # then propagate as this script's own exit status — nothing
            # to do with the diagnostic-suppression fix under test, but a
            # real newline lets `read` succeed normally so the script's
            # actual exit code reflects the setup itself, not this
            # release mechanism's own plumbing.
            assert proc.stdin is not None
            proc.stdin.write("\n")
            proc.stdin.close()
            stderr_output = proc.stderr.read() if proc.stderr else ""
            proc.wait(timeout=5)

            self.assertEqual(proc.returncode, 0, stderr_output)
            self.assertEqual(
                stderr_output,
                "",
                "the failed .owner write leaked a diagnostic instead of "
                "being silently suppressed",
            )
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
            for pipe in (proc.stdin, proc.stdout, proc.stderr):
                if pipe is not None:
                    pipe.close()
            if tree is not None and tree.exists():
                tree.chmod(0o700)
                shutil.rmtree(tree, ignore_errors=True)


class OwnerMarkerCliTestCase(unittest.TestCase):
    """Issue #1278 item 6: ``scripts/test_tmpfs.sh`` no longer writes the
    ``.owner`` marker itself — it execs ``write-owner-marker`` on
    ``scripts/test_substrate.py``, so the marker's format and the ``/proc``
    start-ticks read behind it exist once, beside the reader that consumes
    them. That makes the CLI a writer of a namespace another process reads,
    so it is pinned by composing the REAL writer with the REAL reader over
    a real process, never by asserting the file's bytes on either side
    (code-quality.md, "Invariants live at the widest boundary").

    ``ScratchTreeOwnershipMarkerTestCase`` above still drives the shell
    end to end; this covers the CLI's own contract, including the
    best-effort failure posture that the shell's ``|| true`` would
    otherwise hide.
    """

    def _write_marker(self, tree: Path, pid: int) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                str(SUBSTRATE_CLI),
                "write-owner-marker",
                str(tree),
                str(pid),
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    def _scratch_tree(self) -> Path:
        tree = Path(tempfile.mkdtemp(prefix=SCRATCH_TREE_PREFIX))
        self.addCleanup(shutil.rmtree, tree, True)
        return tree

    def test_marker_reads_alive_then_dead_across_the_owner_exiting(self) -> None:
        """THE writer<->reader binding proof for the CLI. The marker names a
        real, live process, so the real reader must report NOT dead; once
        that process is gone and reaped, the same marker must flip the same
        reader's verdict to dead."""
        tree = self._scratch_tree()
        owner = subprocess.Popen(["sleep", "60"])
        self.addCleanup(owner.wait)
        self.addCleanup(owner.kill)

        completed = self._write_marker(tree, owner.pid)

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout, "")
        self.assertEqual(completed.stderr, "")
        self.assertTrue((tree / SCRATCH_TREE_OWNER_MARKER_NAME).is_file())
        self.assertFalse(
            _scratch_tree_owner_dead(tree),
            "the owning process is alive but the marker the CLI wrote for "
            "it was read as dead",
        )

        owner.kill()
        owner.wait(timeout=5)

        self.assertTrue(
            _scratch_tree_owner_dead(tree),
            "the owning process is confirmed dead but the marker the CLI "
            "wrote for it was read as alive",
        )

    def test_a_failed_write_stays_silent_and_leaves_the_tree_unreapable(
        self,
    ) -> None:
        """Best-effort, fail-closed: a marker that cannot be written must
        not fail the shell entry that asked for it (exit 0, nothing on
        either stream), and the unmarked tree must read as unknown — never
        reaped — rather than as abandoned."""
        tree = self._scratch_tree()
        self.addCleanup(tree.chmod, 0o700)
        tree.chmod(0o500)

        completed = self._write_marker(tree, os.getpid())

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout, "")
        self.assertEqual(completed.stderr, "")
        self.assertFalse((tree / SCRATCH_TREE_OWNER_MARKER_NAME).exists())
        self.assertFalse(_scratch_tree_owner_dead(tree))


if __name__ == "__main__":
    unittest.main()
