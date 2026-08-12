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

REPO_ROOT = Path(__file__).resolve().parents[1]
HELPER = REPO_ROOT / "scripts" / "run_final_gate.sh"


_FAKE_NIX_SHELL = """#!/usr/bin/env bash
set -u
printf '%s\\n' "$@" > "$FAKE_NIX_SHELL_RECORD"
printf 'gate output\\n'
printf 'gate error\\n' >&2
if [[ "${2:-}" == "bash scripts/run_tests.sh" ]]; then
    printf 'bundle: %s\\n' "$FAKE_NIX_SHELL_BUNDLE"
fi
case "$FAKE_NIX_SHELL_MODE" in
    exit) exit "$FAKE_NIX_SHELL_EXIT" ;;
    sleep) sleep 30 ;;
    term) kill -TERM "$$" ;;
    kill) kill -KILL "$$" ;;
    dirty) touch "$FAKE_NIX_SHELL_REPO/dirty" ;;
    head-change) git -C "$FAKE_NIX_SHELL_REPO" commit --allow-empty -qm changed ;;
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
        fake_nix_shell = Path(self.fake_bin.name) / "nix-shell"
        fake_nix_shell.write_text(_FAKE_NIX_SHELL)
        fake_nix_shell.chmod(0o755)
        self.record = Path(self.fake_bin.name) / "nix-shell.argv"
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
        return os.environ | {
            "XDG_RUNTIME_DIR": str(self.runtime),
            "PATH": f"{self.fake_bin.name}:{os.environ['PATH']}",
            "FAKE_NIX_SHELL_RECORD": str(self.record),
            "FAKE_NIX_SHELL_MODE": mode,
            "FAKE_NIX_SHELL_EXIT": str(exit_code),
            "FAKE_NIX_SHELL_REPO": self.repo.name,
            "FAKE_NIX_SHELL_BUNDLE": str(self.bundle),
        }

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
        self.assertEqual(self.record.read_text(), "--run\nbash scripts/run_tests.sh\n")
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
