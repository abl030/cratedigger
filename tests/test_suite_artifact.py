"""Deterministic contracts for full-suite artifact provenance."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

import msgspec

from scripts.test_artifact import (
    ArtifactVerificationError,
    create_artifact,
    finalize_artifact,
    read_summary,
    verify_artifact,
)


RUNNER = Path(__file__).parents[1] / "scripts" / "run_tests.sh"
REPO_ROOT = RUNNER.parents[1]


class TestSuiteArtifact(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self._git(self.repo, "init", "-q")
        self._git(self.repo, "config", "user.name", "Artifact Test")
        self._git(
            self.repo, "config", "user.email", "artifact@example.invalid"
        )
        self._git(self.repo, "config", "commit.gpgSign", "false")
        (self.repo / "tracked.txt").write_text("base\n")
        self._git(self.repo, "add", "tracked.txt")
        self._git(self.repo, "commit", "-q", "-m", "base")
        self.head = self._git(self.repo, "rev-parse", "HEAD")
        self.artifact_root = self.root / "artifacts"

    @staticmethod
    def _git(cwd: Path, *args: str) -> str:
        return subprocess.run(
            ["git", *args],
            cwd=cwd,
            text=True,
            capture_output=True,
            check=True,
            env={**os.environ, "GIT_EDITOR": "true"},
        ).stdout.strip()

    def _complete_green(self, worktree: Path) -> Path:
        artifact = create_artifact(worktree, self.artifact_root)
        (artifact / "output.log").write_text("Ran 2 tests\nOK\n")
        finalize_artifact(
            artifact,
            worktree,
            gate_exit_code=0,
            capture_exit_code=0,
            discovered_tests=2,
            run_tests=2,
        )
        return artifact

    def test_concurrent_real_worktrees_get_distinct_attributed_artifacts(self) -> None:
        left = self.root / "left"
        right = self.root / "right"
        self._git(self.repo, "worktree", "add", "-q", "-b", "left", str(left))
        self._git(
            self.repo, "worktree", "add", "-q", "-b", "right", str(right)
        )

        with ThreadPoolExecutor(max_workers=2) as pool:
            artifacts = tuple(
                pool.map(self._complete_green, (left, right))
            )

        self.assertNotEqual(artifacts[0], artifacts[1])
        self.assertTrue(all(path.is_dir() for path in artifacts))
        for worktree, artifact in zip((left, right), artifacts, strict=True):
            self.assertTrue(artifact.name.startswith(f"{worktree.name}-"))
            self.assertIn(self.head[:12], artifact.name)
        summaries = tuple(read_summary(path) for path in artifacts)
        self.assertEqual(
            {summary.worktree_path for summary in summaries},
            {str(left.resolve()), str(right.resolve())},
        )
        self.assertEqual(
            {summary.artifact_path for summary in summaries},
            {str(path.resolve()) for path in artifacts},
        )
        for artifact in artifacts:
            verified = verify_artifact(artifact, self.head)
            self.assertEqual(verified.start_head, self.head)

    def test_exact_target_verifier_rejects_planted_head_mismatch(self) -> None:
        artifact = self._complete_green(self.repo)
        wrong_head = "f" * len(self.head)

        with self.assertRaisesRegex(
            ArtifactVerificationError, "expected HEAD"
        ):
            verify_artifact(artifact, wrong_head)

    def test_dirty_artifact_cannot_be_cited(self) -> None:
        (self.repo / "dirty.txt").write_text("uncommitted\n")
        artifact = create_artifact(self.repo, self.artifact_root)
        (artifact / "output.log").write_text("Ran 1 test\nOK\n")
        finalize_artifact(
            artifact,
            self.repo,
            gate_exit_code=0,
            capture_exit_code=0,
            discovered_tests=1,
            run_tests=1,
        )

        with self.assertRaisesRegex(ArtifactVerificationError, "dirty"):
            verify_artifact(artifact, self.head)

    def test_incomplete_and_incoherent_artifacts_fail_loudly(self) -> None:
        running = create_artifact(self.repo, self.artifact_root)
        (running / "output.log").write_text("started\n")
        with self.assertRaisesRegex(ArtifactVerificationError, "completed"):
            verify_artifact(running, self.head)

        incoherent = create_artifact(self.repo, self.artifact_root)
        (incoherent / "output.log").write_text("Ran 1 test\nOK\n")
        finalize_artifact(
            incoherent,
            self.repo,
            gate_exit_code=0,
            capture_exit_code=0,
            discovered_tests=2,
            run_tests=1,
        )
        with self.assertRaisesRegex(ArtifactVerificationError, "counts"):
            verify_artifact(incoherent, self.head)

    def test_output_mutation_after_finalization_is_rejected(self) -> None:
        artifact = self._complete_green(self.repo)
        with (artifact / "output.log").open("ab") as output:
            output.write(b"late writer bytes\n")

        with self.assertRaisesRegex(
            ArtifactVerificationError, "changed after finalization"
        ):
            verify_artifact(artifact, self.head)

    def test_v1_summary_is_rejected_without_a_compatibility_path(self) -> None:
        artifact = create_artifact(self.repo, self.artifact_root)
        summary_path = artifact / "summary.json"
        payload = msgspec.json.decode(summary_path.read_bytes())
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        payload["schema_version"] = 1
        for field in (
            "gate_exit_code",
            "capture_exit_code",
            "output_bytes",
            "output_sha256",
        ):
            payload.pop(field)
        summary_path.write_bytes(msgspec.json.encode(payload))

        with self.assertRaisesRegex(
            ArtifactVerificationError, "invalid or missing suite summary"
        ):
            read_summary(artifact)

    def test_runner_prints_unique_path_and_finalizes_early_gate_failure(self) -> None:
        stub_dir = self.root / "bin"
        stub_dir.mkdir()
        node = stub_dir / "node"
        node.write_text(
            "#!/usr/bin/env bash\n"
            "echo 'PLANTED EARLY JS FAILURE'\n"
            "exit 17\n"
        )
        node.chmod(0o755)
        artifact_root = self.root / "runner-artifacts"
        stdout_path = self.root / "runner.stdout"
        with stdout_path.open("w") as stdout:
            result = subprocess.run(
                ["bash", str(RUNNER)],
                cwd=REPO_ROOT,
                text=True,
                stdout=stdout,
                stderr=subprocess.STDOUT,
                env={
                    **os.environ,
                    "CRATEDIGGER_TEST_ARTIFACT_ROOT": str(artifact_root),
                    "PATH": f"{stub_dir}:{os.environ['PATH']}",
                },
            )
        self.assertNotEqual(result.returncode, 0)
        runner_stdout = stdout_path.read_text()
        paths = re.findall(r"^Artifact directory: (.+)$", runner_stdout, re.M)
        self.assertEqual(len(paths), 2, runner_stdout)
        self.assertEqual(paths[0], paths[1])
        artifact = Path(paths[0])
        self.addCleanup(shutil.rmtree, artifact, True)

        output = (artifact / "output.log").read_text()
        self.assertIn("=== TEST ARTIFACT ===", output)
        self.assertIn("=== JS syntax check ===", output)
        self.assertIn("PLANTED EARLY JS FAILURE", output)
        self.assertIn("=== TEST ARTIFACT COMPLETE ===", runner_stdout)
        summary = read_summary(artifact)
        self.assertEqual(summary.status, "failed")
        self.assertIsNotNone(summary.ended_at)
        self.assertIsNotNone(summary.end_head)
        self.assertIsNotNone(summary.end_dirty)
        self.assertEqual(summary.discovered_tests, 0)
        self.assertEqual(summary.run_tests, 0)

    def test_runner_waits_for_delayed_capture_before_finalizing(self) -> None:
        stub_dir = self.root / "delayed-bin"
        stub_dir.mkdir()
        real_tee = shutil.which("tee")
        self.assertIsNotNone(real_tee)
        assert real_tee is not None
        tee = stub_dir / "tee"
        tee.write_text(
            "#!/usr/bin/env bash\n"
            "sleep 0.5\n"
            f'exec "{real_tee}" "$@"\n'
        )
        tee.chmod(0o755)
        node = stub_dir / "node"
        node.write_text(
            "#!/usr/bin/env bash\n"
            "echo 'PLANTED DELAYED CAPTURE FAILURE'\n"
            "exit 17\n"
        )
        node.chmod(0o755)
        artifact_root = self.root / "delayed-artifacts"
        stdout_path = self.root / "delayed.stdout"
        started = time.monotonic()
        with stdout_path.open("w") as stdout:
            process = subprocess.Popen(
                ["bash", str(RUNNER)],
                cwd=REPO_ROOT,
                text=True,
                stdout=stdout,
                stderr=subprocess.STDOUT,
                env={
                    **os.environ,
                    "CRATEDIGGER_TEST_ARTIFACT_ROOT": str(artifact_root),
                    "PATH": f"{stub_dir}:{os.environ['PATH']}",
                },
            )
            deadline = time.monotonic() + 2
            artifacts: list[Path] = []
            while time.monotonic() < deadline:
                artifacts = list(artifact_root.glob("*"))
                if (
                    len(artifacts) == 1
                    and (artifacts[0] / "summary.json").exists()
                ):
                    break
                time.sleep(0.01)
            self.assertEqual(len(artifacts), 1)
            during_capture = read_summary(artifacts[0])
            returncode = process.wait(timeout=5)
        elapsed = time.monotonic() - started
        # Let the old asynchronous implementation's orphaned tee finish before
        # TemporaryDirectory cleanup; the assertions below remain RED there.
        if elapsed < 0.45:
            time.sleep(0.6)

        self.assertEqual(during_capture.status, "running")
        self.assertGreaterEqual(elapsed, 0.45)
        self.assertNotEqual(returncode, 0)
        summary = read_summary(artifacts[0])
        output = (artifacts[0] / "output.log").read_bytes()
        self.assertEqual(summary.capture_exit_code, 0)
        self.assertEqual(summary.output_bytes, len(output))
        self.assertEqual(
            summary.output_sha256,
            hashlib.sha256(output).hexdigest(),
        )
        self.assertIn(b"PLANTED DELAYED CAPTURE FAILURE", output)

    def test_runner_records_and_propagates_capture_failure(self) -> None:
        stub_dir = self.root / "failing-bin"
        stub_dir.mkdir()
        tee = stub_dir / "tee"
        tee.write_text(
            "#!/usr/bin/env bash\n"
            "cat >/dev/null\n"
            "exit 23\n"
        )
        tee.chmod(0o755)
        node = stub_dir / "node"
        node.write_text(
            "#!/usr/bin/env bash\n"
            "echo 'PLANTED CAPTURE FAILURE'\n"
            "exit 17\n"
        )
        node.chmod(0o755)
        artifact_root = self.root / "failing-artifacts"
        with (self.root / "failing.stdout").open("w") as stdout:
            result = subprocess.run(
                ["bash", str(RUNNER)],
                cwd=REPO_ROOT,
                text=True,
                stdout=stdout,
                stderr=subprocess.STDOUT,
                timeout=5,
                env={
                    **os.environ,
                    "CRATEDIGGER_TEST_ARTIFACT_ROOT": str(artifact_root),
                    "PATH": f"{stub_dir}:{os.environ['PATH']}",
                },
            )

        self.assertEqual(result.returncode, 23)
        artifacts = list(artifact_root.glob("*"))
        self.assertEqual(len(artifacts), 1)
        summary = read_summary(artifacts[0])
        self.assertEqual(summary.status, "failed")
        self.assertEqual(summary.gate_exit_code, 1)
        self.assertEqual(summary.capture_exit_code, 23)

    def test_authoritative_guidance_uses_emitted_artifact_path(self) -> None:
        authoritative = (
            REPO_ROOT / ".claude/memory/feedback_use_nix_shell.md",
            REPO_ROOT / ".claude/memory/feedback_full_suite_before_merge.md",
        )
        for path in authoritative:
            with self.subTest(path=path.name):
                text = path.read_text()
                self.assertNotIn("/tmp/cratedigger-test-output.txt", text)
                self.assertIn("output.log", text)

    def test_documented_artifact_export_reaches_inner_nix_shell(self) -> None:
        validation = self.root / "documented-verifier-worktree"
        self._git(
            REPO_ROOT,
            "worktree",
            "add",
            "--detach",
            "-q",
            str(validation),
            "HEAD",
        )
        self.addCleanup(
            self._git,
            REPO_ROOT,
            "worktree",
            "remove",
            "--force",
            str(validation),
        )
        artifact = create_artifact(validation, self.artifact_root)
        (artifact / "output.log").write_text("Ran 1 test\nOK\n")
        finalize_artifact(
            artifact,
            validation,
            gate_exit_code=0,
            capture_exit_code=0,
            discovered_tests=1,
            run_tests=1,
        )

        docs = (
            REPO_ROOT / "CLAUDE.md",
            REPO_ROOT / ".claude/skills/check/SKILL.md",
        )
        verifier_pattern = re.compile(
            r"nix-shell --run 'python3 scripts/test_artifact\.py verify "
            r"--artifact \\\n  \"\$ARTIFACT\" --expected-head "
            r"\"\$\(git rev-parse HEAD\)\"'"
        )
        for path in docs:
            with self.subTest(path=path.relative_to(REPO_ROOT)):
                blocks = re.findall(
                    r"```bash\n(.*?)\n```", path.read_text(), re.DOTALL
                )
                verifier_blocks = [
                    block for block in blocks
                    if "scripts/test_artifact.py verify" in block
                ]
                self.assertEqual(len(verifier_blocks), 1)
                block = verifier_blocks[0]
                exports = re.findall(
                    r"^export ARTIFACT=.*$", block, re.MULTILINE
                )
                self.assertEqual(
                    len(exports),
                    1,
                    f"{path} must export ARTIFACT in its verifier block",
                )
                verifier = verifier_pattern.search(block)
                self.assertIsNotNone(verifier)
                assert verifier is not None
                documented_export = exports[0].split("#", 1)[0].strip()
                export_lhs = documented_export.split("=", 1)[0]
                export = f"{export_lhs}={shlex.quote(str(artifact))}"
                result = subprocess.run(
                    ["bash", "-c", f"{export}\n{verifier.group(0)}"],
                    cwd=validation,
                    text=True,
                    capture_output=True,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("Verified suite artifact", result.stdout)

    def test_ephemeral_tmpdir_does_not_control_default_artifact_root(self) -> None:
        hostile_tmpdir = self.root / "nix-shell-ephemeral"
        hostile_tmpdir.mkdir()
        with patch.dict(os.environ, {"TMPDIR": str(hostile_tmpdir)}):
            os.environ.pop("CRATEDIGGER_TEST_ARTIFACT_ROOT", None)
            artifact = create_artifact(self.repo)
        self.addCleanup(shutil.rmtree, artifact, True)

        self.assertEqual(artifact.parent, Path("/tmp"))
        self.assertFalse(artifact.is_relative_to(hostile_tmpdir))


if __name__ == "__main__":
    unittest.main()
