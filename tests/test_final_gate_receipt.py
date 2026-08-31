"""Process-level contracts for the final-gate receipt launcher."""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from scripts.test_substrate import (
    RECEIPT_TERMINAL_FIELD,
    RECEIPT_TERMINAL_STAGING_FIELD,
    _publish_terminal_verdict,
)
from tests._source_pins import pinned_source

REPO_ROOT = Path(__file__).resolve().parents[1]
HELPER = REPO_ROOT / "scripts" / "run_final_gate.sh"


_FAKE_NIX = """#!/usr/bin/env bash
set -u
printf '%s\\n' "$@" > "$FAKE_NIX_RECORD"
printf '%s' "${CRATEDIGGER_SUITE_OWNS_HEADROOM:-}" > "$FAKE_NIX_HEADROOM_ENV_RECORD"
printf 'gate output\\n'
printf 'gate error\\n' >&2
if [[ "${5:-}" == "bash scripts/run_tests.sh" ]]; then
    printf 'bundle: %s\\n' "$FAKE_NIX_BUNDLE"
fi
case "$FAKE_NIX_MODE" in
    exit) exit "$FAKE_NIX_EXIT" ;;
    sleep) sleep 30 ;;
    term) kill -TERM "$$" ;;
    kill) kill -KILL "$$" ;;
    dirty) touch "$FAKE_NIX_REPO/dirty" ;;
    double-bundle) printf 'bundle: %s\\n' "$FAKE_NIX_BUNDLE" ;;
    head-change) git -C "$FAKE_NIX_REPO" commit --allow-empty -qm changed ;;
    *) exit 126 ;;
esac
"""


class FinalGateReceiptTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = tempfile.TemporaryDirectory(prefix="cratedigger-final-gate-repo-")
        self.runtime = Path(os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}"))
        self.assertTrue(self.runtime.is_dir(), "private runtime tmpfs is required for this test")
        self.fake_bin = tempfile.TemporaryDirectory(prefix="cratedigger-final-gate-bin-")
        self.bundle = Path(
            tempfile.mkdtemp(
                prefix="cratedigger-checks.final-gate-test.",
                dir=self.runtime,
            )
        )
        self.bundle.chmod(0o700)
        (self.bundle / "summary.json").write_text("{}\n", encoding="utf-8")
        # Issue #1229: the gate launches `nix develop --command bash -c ...`,
        # so the binary to fake is `nix`, not `nix-shell`.
        fake_nix = Path(self.fake_bin.name) / "nix"
        fake_nix.write_text(_FAKE_NIX)
        fake_nix.chmod(0o755)
        self.record = Path(self.fake_bin.name) / "nix.argv"
        self.headroom_env_record = Path(self.fake_bin.name) / "nix.headroom-env"
        self.created_receipts: list[Path] = []
        self._git("init", "-q")
        self._git("config", "user.email", "tests@example.invalid")
        self._git("config", "user.name", "Final gate tests")
        (Path(self.repo.name) / "README").write_text("receipt fixture\n")
        self._git("add", "README")
        self._git("commit", "-qm", "fixture")

    def tearDown(self) -> None:
        for receipt in self.created_receipts:
            shutil.rmtree(receipt, ignore_errors=True)
        shutil.rmtree(self.bundle, ignore_errors=True)
        self.fake_bin.cleanup()
        self.repo.cleanup()

    def _git(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args], cwd=self.repo.name, check=True, text=True,
            capture_output=True,
        )

    def _env(self, mode: str = "exit", exit_code: int = 0) -> dict[str, str]:
        # Explicitly cleared, not just left to os.environ: if this test's
        # OWN suite run was itself launched via scripts/test.sh or the
        # final gate, the ambient environment already carries
        # CRATEDIGGER_SUITE_OWNS_HEADROOM=1 from THAT launcher — inheriting
        # it here would let the MAJOR-3 pin below pass even if the gate's
        # own environment assignment (scripts/test_substrate.py's
        # `_await_suite`) were deleted, exactly the leakage class issue
        # #1111 review M2 already caught once
        # (tests/test_test_tmpfs.py::low_headroom_environment).
        env = dict(os.environ)
        env.pop("CRATEDIGGER_SUITE_OWNS_HEADROOM", None)
        env |= {
            "XDG_RUNTIME_DIR": str(self.runtime),
            "PATH": f"{self.fake_bin.name}:{os.environ['PATH']}",
            "FAKE_NIX_RECORD": str(self.record),
            "FAKE_NIX_HEADROOM_ENV_RECORD": str(self.headroom_env_record),
            "FAKE_NIX_MODE": mode,
            "FAKE_NIX_EXIT": str(exit_code),
            "FAKE_NIX_REPO": self.repo.name,
            "FAKE_NIX_BUNDLE": str(self.bundle),
        }
        return env

    def _launch(self, mode: str = "exit", exit_code: int = 0) -> subprocess.Popen[str]:
        process = subprocess.Popen(
            [str(HELPER)], cwd=self.repo.name, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self._env(mode, exit_code),
            start_new_session=True,
        )
        self.addCleanup(self._stop, process)
        return process

    def _receipt_from(self, process: subprocess.Popen[str]) -> Path:
        assert process.stdout is not None
        line = process.stdout.readline().strip()
        self.assertTrue(line.startswith("receipt: "), line)
        receipt = Path(line.removeprefix("receipt: "))
        self.created_receipts.append(receipt)
        return receipt

    @staticmethod
    def _stop(process: subprocess.Popen[str]) -> None:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.communicate(timeout=10)

    def _status(self, receipt: Path) -> str:
        result = subprocess.run(
            [str(HELPER), "status", str(receipt)], cwd=self.repo.name,
            text=True, capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def _gate_pid(self, receipt: Path) -> int:
        path = receipt / "gate_pid"
        deadline = time.monotonic() + 5
        while not path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(path.exists(), "helper did not record a gate PID")
        return int(path.read_text())

    def test_success_runs_only_the_canonical_suite_and_preserves_output(self) -> None:
        process = self._launch()
        receipt = self._receipt_from(process)
        stdout, stderr = process.communicate(timeout=10)

        self.assertEqual(process.returncode, 0, stderr)
        self.assertIn("final gate: pass (exit 0)", stdout)
        self.assertEqual(
            self.record.read_text(),
            "develop\n--command\nbash\n-c\nbash scripts/run_tests.sh\n",
        )
        self.assertEqual(self._status(receipt), "pass")
        self.assertEqual((receipt / "terminal").read_text(), "pass 0\n")
        self.assertIn("gate output", (receipt / "output.log").read_text())
        self.assertIn("gate error", (receipt / "output.log").read_text())
        self.assertEqual((receipt / "repo_root").read_text().strip(), self.repo.name)
        self.assertEqual((receipt / "head").read_text().strip(), self._git("rev-parse", "HEAD").stdout.strip())
        self.assertEqual((receipt / "clean").read_text(), "true\n")
        self.assertFalse((receipt / "label").exists())
        self.assertEqual((receipt / "command").read_text(), "bash scripts/run_tests.sh\n")
        self.assertEqual((receipt / "bundle").read_text().strip(), str(self.bundle))
        self.assertEqual((receipt.stat().st_mode & 0o777), 0o700)

    def test_gate_sets_the_suite_owns_headroom_env_var(self) -> None:
        """Issue #1111 review MAJOR-3: the M2 producer side is otherwise
        unpinned — deleting the `CRATEDIGGER_SUITE_OWNS_HEADROOM = "1"`
        assignment from the child environment `_await_suite` builds
        (scripts/test_substrate.py) would leave every other test green
        while M2 silently reverts. The fake `nix` records the var's value
        from its OWN received environment, not the argv — the gate passes
        it as the child's environment, so it never appears in argv at
        all."""
        process = self._launch()
        self._receipt_from(process)
        process.communicate(timeout=10)

        self.assertEqual(self.headroom_env_record.read_text(), "1")

    def test_nonzero_exit_is_preserved_not_masked(self) -> None:
        process = self._launch(exit_code=23)
        receipt = self._receipt_from(process)
        _stdout, stderr = process.communicate(timeout=10)

        self.assertEqual(process.returncode, 23, stderr)
        self.assertEqual(self._status(receipt), "fail")
        self.assertEqual((receipt / "terminal").read_text(), "fail 23\n")

    def test_passing_receipt_without_bundle_path_is_rejected(self) -> None:
        process = self._launch()
        receipt = self._receipt_from(process)
        process.communicate(timeout=10)
        (receipt / "bundle").unlink()

        result = subprocess.run(
            [str(HELPER), "status", str(receipt)],
            cwd=self.repo.name,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing its suite bundle path", result.stderr)

    def test_passing_receipt_whose_bundle_directory_was_reaped_is_rejected(self) -> None:
        """Issue #1111 review m5: a receipt's own `terminal`/`bundle` FILE
        surviving is not evidence the bundle DIRECTORY it names still
        exists — admission-time reaping can remove an idle one. `status`
        must fail visibly rather than silently report `pass` over evidence
        that is gone."""
        process = self._launch()
        receipt = self._receipt_from(process)
        process.communicate(timeout=10)
        shutil.rmtree(self.bundle)

        result = subprocess.run(
            [str(HELPER), "status", str(receipt)],
            cwd=self.repo.name,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no longer exists", result.stderr)
        self.assertIn(str(self.bundle), result.stderr)

    def test_active_detached_recovery_requires_the_exact_live_process_identity(self) -> None:
        process = self._launch(mode="sleep")
        receipt = self._receipt_from(process)
        self._gate_pid(receipt)

        self.assertEqual(self._status(receipt), "exact-active")
        self.assertTrue((receipt / "gate_pid").read_text().strip().isdigit())
        self.assertTrue((receipt / "gate_start_ticks").read_text().strip().isdigit())
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate(timeout=10)

    def test_hard_helper_interruption_without_terminal_marker_is_incomplete(self) -> None:
        process = self._launch(mode="sleep")
        receipt = self._receipt_from(process)
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)
        time.sleep(0.05)

        self.assertFalse((receipt / "terminal").exists())
        self.assertEqual(self._status(receipt), "incomplete")

    def test_helper_sigterm_leaves_a_surviving_child_incomplete(self) -> None:
        process = self._launch(mode="sleep")
        receipt = self._receipt_from(process)
        gate_pid = self._gate_pid(receipt)
        os.kill(process.pid, signal.SIGTERM)
        process.communicate(timeout=10)

        self.assertFalse((receipt / "terminal").exists())
        self.assertEqual(self._status(receipt), "incomplete")
        os.kill(gate_pid, signal.SIGKILL)

    def test_child_sigterm_is_incomplete_without_terminal_marker(self) -> None:
        process = self._launch(mode="term")
        receipt = self._receipt_from(process)
        _stdout, _stderr = process.communicate(timeout=10)

        self.assertEqual(process.returncode, 143)
        self.assertFalse((receipt / "terminal").exists())
        self.assertEqual(self._status(receipt), "incomplete")

    def test_child_sigkill_is_incomplete_without_terminal_marker(self) -> None:
        process = self._launch(mode="kill")
        receipt = self._receipt_from(process)
        _stdout, _stderr = process.communicate(timeout=10)

        self.assertEqual(process.returncode, 137)
        self.assertFalse((receipt / "terminal").exists())
        self.assertEqual(self._status(receipt), "incomplete")

    def test_status_rejects_a_receipt_after_the_tree_becomes_dirty(self) -> None:
        process = self._launch()
        receipt = self._receipt_from(process)
        process.communicate(timeout=10)
        (Path(self.repo.name) / "dirty").write_text("no reuse\n")

        result = subprocess.run(
            [str(HELPER), "status", str(receipt)], cwd=self.repo.name,
            text=True, capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not for this committed clean tree", result.stderr)

    def test_dirty_tree_before_terminal_publication_remains_incomplete(self) -> None:
        process = self._launch(mode="dirty")
        receipt = self._receipt_from(process)
        _stdout, stderr = process.communicate(timeout=10)

        self.assertEqual(process.returncode, 2, stderr)
        self.assertFalse((receipt / "terminal").exists())

    def test_head_change_before_terminal_publication_remains_incomplete(self) -> None:
        process = self._launch(mode="head-change")
        receipt = self._receipt_from(process)
        _stdout, stderr = process.communicate(timeout=10)

        self.assertEqual(process.returncode, 2, stderr)
        self.assertFalse((receipt / "terminal").exists())

    def test_two_bundle_announcements_are_refused_as_ambiguous(self) -> None:
        """The bundle ladder's "exactly one announcement" clause: a suite
        that published two paths gives the gate no way to know which run's
        evidence the receipt would be pointing at, so it records neither
        and refuses to publish a terminal verdict at all — even though the
        suite itself exited 0."""
        process = self._launch(mode="double-bundle")
        receipt = self._receipt_from(process)
        _stdout, stderr = process.communicate(timeout=10)

        self.assertEqual(process.returncode, 2, stderr)
        self.assertIn("published multiple bundle paths", stderr)
        self.assertIn("published no valid bundle", stderr)
        self.assertFalse((receipt / "terminal").exists())
        self.assertFalse((receipt / "bundle").exists())

    def test_status_rejects_a_receipt_outside_the_private_runtime_dir(self) -> None:
        """Tamper guard: a receipt is only trustworthy where only this user
        can have created it. A real directory somewhere else is refused by
        location, before any of its contents are read."""
        result = subprocess.run(
            [str(HELPER), "status", self.repo.name],
            cwd=self.repo.name,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not directly beneath the private runtime directory", result.stderr)

    def test_status_rejects_a_receipt_whose_mode_was_widened(self) -> None:
        """Tamper guard: 0700 is what makes the receipt this user's alone.
        A widened mode means someone else could have written its verdict."""
        process = self._launch()
        receipt = self._receipt_from(process)
        process.communicate(timeout=10)
        receipt.chmod(0o755)

        result = subprocess.run(
            [str(HELPER), "status", str(receipt)],
            cwd=self.repo.name,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not mode 0700", result.stderr)

    def test_status_rejects_a_receipt_not_bound_to_a_clean_tree(self) -> None:
        """Tamper guard: the `clean` file is the receipt's own claim that
        it was taken on a committed clean tree. Anything but `true` makes
        every later HEAD comparison meaningless, so it is refused rather
        than compared."""
        process = self._launch()
        receipt = self._receipt_from(process)
        process.communicate(timeout=10)
        (receipt / "clean").write_text("false\n")

        result = subprocess.run(
            [str(HELPER), "status", str(receipt)],
            cwd=self.repo.name,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not bound to a clean tree", result.stderr)

    def test_status_rejects_substituted_command(self) -> None:
        process = self._launch()
        receipt = self._receipt_from(process)
        process.communicate(timeout=10)
        (receipt / "command").write_text("echo substituted\n")

        result = subprocess.run(
            [str(HELPER), "status", str(receipt)], cwd=self.repo.name,
            text=True, capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not canonical", result.stderr)

    def test_alternate_gate_labels_are_rejected(self) -> None:
        for label in ("pyright", "tests", "alternate"):
            with self.subTest(label=label):
                result = subprocess.run(
                    [str(HELPER), label], cwd=self.repo.name,
                    text=True, capture_output=True, check=False,
                )

                self.assertEqual(result.returncode, 2)
                self.assertIn("usage", result.stderr)


class FinalGateWrapperTestCase(unittest.TestCase):
    """The gate is Python; the ``.sh`` is a wrapper (issue #1278 item 6).

    Every other test in this file drives the wrapper as a black box and so
    passes whether the logic behind it is bash or Python. These two name
    the split itself: one canonical implementation, reached by an ``exec``,
    with no second copy of the gate left behind in shell. Same shape as
    ``scripts/run_tests.sh``'s own wrapper pin
    (``tests/test_parallel_test_runner.py::TestRunTestsWiring``).
    """

    def test_the_wrapper_execs_the_substrate_cli(self) -> None:
        self.assertIn(
            'exec python3 "$here/test_substrate.py" final-gate "$@"',
            pinned_source(HELPER),
        )

    def test_the_wrapper_keeps_no_copy_of_the_gate_it_delegates(self) -> None:
        """A BOUNDED spelling list, not a proof of absence.

        This rejects the specific vocabulary the ported bash carried — its
        function names, its receipt field names, the `/proc` path and
        receipt-directory prefix it spelled, and the commands it ran
        (`awk` included) — so re-growing a copy in the recognisable shape
        goes red here. It cannot decide that no second implementation
        exists under different names; a semantic scanner that tried would
        be the prohibited shape (`.claude/rules/code-quality.md`, "Semantic
        source scanners are prohibited"), so review owns the general case
        and this list owns the regression.

        Read through ``pinned_source``, so a spelling that survives only
        inside a comment is correctly ignored — a comment naming
        ``nix develop`` is documentation, while a command running it is a
        second implementation.
        """
        source = pinned_source(HELPER)

        for spelling in (
            "/proc/",
            "awk",
            "bash scripts/run_tests.sh",
            "cratedigger-final-gate.",
            "gate_start_ticks",
            "helper_start_ticks",
            "nix develop",
            "proc_start_ticks",
            "receipt_field",
            "same_process",
        ):
            with self.subTest(spelling=spelling):
                self.assertNotIn(spelling, source)


class TerminalVerdictPublicationTestCase(unittest.TestCase):
    """The verdict lands atomically, or it does not land.

    Nothing else in this file can see the difference: a direct write to
    ``terminal`` produces byte-identical receipts and leaves every
    process-level test above green, while breaking the guarantee
    ``_publish_terminal_verdict`` documents — that no reader (a concurrent
    ``status``, or ``_receipt_is_retirable``'s own existence check) can
    observe a ``terminal`` file that exists before its content does. These
    drive the real publication function over a real directory and make the
    staging step observable by blocking it.
    """

    def _receipt(self) -> Path:
        receipt = Path(tempfile.mkdtemp(prefix="cratedigger-terminal-publication-"))
        self.addCleanup(shutil.rmtree, receipt, True)
        return receipt

    def test_the_verdict_is_staged_before_it_is_named_terminal(self) -> None:
        """Known-bad world for the staging step: with the staging name
        occupied by a directory, the write that must come first cannot
        happen — so the verdict must not land at all. A publication that
        wrote ``terminal`` directly would sail past this untouched."""
        receipt = self._receipt()
        (receipt / RECEIPT_TERMINAL_STAGING_FIELD).mkdir()

        with self.assertRaises(IsADirectoryError):
            _publish_terminal_verdict(receipt, 0)

        self.assertFalse((receipt / RECEIPT_TERMINAL_FIELD).exists())

    def test_publication_moves_the_staged_file_rather_than_copying_it(self) -> None:
        """Must-still-work, plus the second half of atomicity: the verdict
        is correct AND the staging name is gone afterwards. A copy would
        leave it behind; a rename cannot."""
        receipt = self._receipt()

        _publish_terminal_verdict(receipt, 0)

        self.assertEqual(
            (receipt / RECEIPT_TERMINAL_FIELD).read_text(), "pass 0\n"
        )
        self.assertFalse((receipt / RECEIPT_TERMINAL_STAGING_FIELD).exists())

    def test_a_failing_run_publishes_its_own_exit_status(self) -> None:
        receipt = self._receipt()

        _publish_terminal_verdict(receipt, 23)

        self.assertEqual(
            (receipt / RECEIPT_TERMINAL_FIELD).read_text(), "fail 23\n"
        )
