"""Deterministic contracts for full-suite artifact provenance."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts.test_artifact import (
    ArtifactVerificationError,
    create_artifact,
    finalize_artifact,
    read_summary,
    verify_artifact,
)


RUNNER = Path(__file__).parents[1] / "scripts" / "run_tests.sh"


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
            exit_code=0,
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
            exit_code=0,
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
            exit_code=0,
            discovered_tests=2,
            run_tests=1,
        )
        with self.assertRaisesRegex(ArtifactVerificationError, "counts"):
            verify_artifact(incoherent, self.head)

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
        result = subprocess.run(
            ["bash", str(RUNNER)],
            cwd=RUNNER.parents[1],
            text=True,
            capture_output=True,
            env={
                **os.environ,
                "PATH": f"{stub_dir}:{os.environ['PATH']}",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        paths = re.findall(r"^Artifact directory: (.+)$", result.stdout, re.M)
        self.assertEqual(len(paths), 2, result.stdout)
        self.assertEqual(paths[0], paths[1])
        artifact = Path(paths[0])
        self.addCleanup(shutil.rmtree, artifact, True)

        output = (artifact / "output.log").read_text()
        self.assertIn("=== TEST ARTIFACT ===", output)
        self.assertIn("=== JS syntax check ===", output)
        self.assertIn("PLANTED EARLY JS FAILURE", output)
        self.assertIn("=== TEST ARTIFACT COMPLETE ===", output)
        summary = read_summary(artifact)
        self.assertEqual(summary.status, "failed")
        self.assertIsNotNone(summary.ended_at)
        self.assertIsNotNone(summary.end_head)
        self.assertIsNotNone(summary.end_dirty)
        self.assertEqual(summary.discovered_tests, 0)
        self.assertEqual(summary.run_tests, 0)

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
