"""Behavioral contracts for exact-ref pre-push validation."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from scripts.test_artifact import create_artifact, finalize_artifact


HOOK = Path(__file__).parents[1] / "scripts" / "pre-push"


class TestPrePushHook(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self._git("init", "-q")
        self._git("config", "user.name", "Pre-push Test")
        self._git("config", "user.email", "pre-push@example.invalid")
        self._git("config", "commit.gpgSign", "false")
        self._git("config", "tag.gpgSign", "false")

        self.stub_dir = self.root / "bin"
        self.stub_dir.mkdir()
        self.gate_log = self.root / "gates.tsv"
        for command in ("nix-shell", "nix"):
            stub = self.stub_dir / command
            stub.write_text(
                "#!/usr/bin/env bash\n"
                "kind='" + command + "'\n"
                "if [[ \"$kind\" == 'nix-shell' && \"${2:-}\" == "
                "*'CRATEDIGGER_ARTIFACT_TOOL'* ]]; then\n"
                "  kind='nix-shell-verify'\n"
                "fi\n"
                "printf '%s\\t%s\\t%s\\n' "
                "\"$kind\" \"$(git rev-parse HEAD)\" \"$PWD\" >> \"$GATE_LOG\"\n"
                "if [[ \"$kind\" == 'nix-shell-verify' ]]; then\n"
                "  bash -c \"$2\"\n"
                "  exit $?\n"
                "fi\n"
                f"[[ \"${{FAIL_GATE:-}}\" != '{command}' ]]\n"
            )
            stub.chmod(0o755)

        self.first = self._commit("first")
        self.second = self._commit("second")
        self.zero = "0" * len(self.second)

    def _git(self, *args: str, input_text: str | None = None) -> str:
        return self._git_at(self.repo, *args, input_text=input_text)

    def _git_at(
        self, cwd: Path, *args: str, input_text: str | None = None
    ) -> str:
        env = os.environ.copy()
        env["GIT_EDITOR"] = "true"
        env["GIT_SEQUENCE_EDITOR"] = "true"
        return subprocess.run(
            ["git", *args],
            cwd=cwd,
            input=input_text,
            text=True,
            capture_output=True,
            check=True,
            env=env,
        ).stdout.strip()

    def _commit(self, value: str) -> str:
        (self.repo / "value.txt").write_text(value)
        self._git("add", "value.txt")
        self._git("commit", "-q", "-m", value)
        return self._git("rev-parse", "HEAD")

    def _record(
        self,
        local_ref: str,
        local_sha: str,
        remote_ref: str | None = None,
        remote_sha: str | None = None,
    ) -> str:
        return (
            f"{local_ref} {local_sha} {remote_ref or local_ref} "
            f"{remote_sha or self.zero}\n"
        )

    def _run(
        self,
        records: str,
        *,
        fail_gate: str | None = None,
        artifact: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["GIT_EDITOR"] = "true"
        env["GIT_SEQUENCE_EDITOR"] = "true"
        env["PATH"] = f"{self.stub_dir}:{env['PATH']}"
        env["GATE_LOG"] = str(self.gate_log)
        if fail_gate is not None:
            env["FAIL_GATE"] = fail_gate
        if artifact is not None:
            env["CRATEDIGGER_TEST_ARTIFACT"] = str(artifact)
        return subprocess.run(
            [str(HOOK), "origin", "ssh://example.invalid/repo.git"],
            cwd=self.repo,
            input=records,
            text=True,
            capture_output=True,
            env=env,
        )

    def _green_artifact(self) -> Path:
        artifact = create_artifact(self.repo, self.root / "artifacts")
        (artifact / "output.log").write_text("Ran 1 test\nOK\n")
        finalize_artifact(
            artifact,
            self.repo,
            gate_exit_code=0,
            capture_exit_code=0,
            discovered_tests=1,
            run_tests=1,
        )
        return artifact

    def _gates(self) -> list[tuple[str, str, Path]]:
        if not self.gate_log.exists():
            return []
        return [
            (command, commit, Path(tree))
            for command, commit, tree in (
                line.split("\t") for line in self.gate_log.read_text().splitlines()
            )
        ]

    def _assert_one_validation(self, commit: str) -> list[tuple[str, str, Path]]:
        gates = self._gates()
        self.assertEqual([gate[0] for gate in gates], ["nix-shell", "nix"])
        self.assertEqual([gate[1] for gate in gates], [commit, commit])
        return gates

    def _assert_worktrees_removed(
        self, gates: list[tuple[str, str, Path]]
    ) -> None:
        registered = self._git("worktree", "list", "--porcelain")
        for _, _, tree in gates:
            self.assertNotEqual(tree, self.repo)
            self.assertFalse(tree.exists(), f"temporary worktree leaked: {tree}")
            self.assertNotIn(str(tree), registered, f"worktree metadata leaked: {tree}")

    def test_current_branch_commit_is_gated_and_named(self) -> None:
        result = self._run(
            self._record("refs/heads/main", self.second, remote_sha=self.first)
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self._assert_one_validation(self.second)
        self.assertIn(self.second, result.stderr)
        self.assertIn("refs/heads/main", result.stderr)

    def test_annotated_tag_gates_peeled_commit_not_current_checkout(self) -> None:
        self._git("tag", "-a", "v1", self.first, "-m", "release")
        tag_object = self._git("rev-parse", "refs/tags/v1")

        result = self._run(self._record("refs/tags/v1", tag_object))

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self._git("rev-parse", "HEAD"), self.second)
        self._assert_one_validation(self.first)
        self.assertIn(self.first, result.stderr)
        self.assertIn("refs/tags/v1", result.stderr)

    def test_lightweight_tag_gates_its_commit(self) -> None:
        self._git("tag", "lightweight", self.first)
        tag_target = self._git("rev-parse", "refs/tags/lightweight")

        result = self._run(
            self._record("refs/tags/lightweight", tag_target)
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self._assert_one_validation(self.first)

    def test_deletion_is_skipped_without_running_gates(self) -> None:
        result = self._run(
            self._record(
                "(delete)",
                self.zero,
                remote_ref="refs/heads/obsolete",
                remote_sha=self.first,
            )
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self._gates(), [])
        self.assertIn("refs/heads/obsolete", result.stderr)
        self.assertIn("deletion", result.stderr)

    def test_branch_and_tag_at_same_commit_are_deduplicated(self) -> None:
        records = self._record("refs/heads/main", self.second)
        records += self._record("refs/tags/v2", self.second)

        result = self._run(records)

        self.assertEqual(result.returncode, 0, result.stderr)
        self._assert_one_validation(self.second)
        self.assertIn("refs/heads/main", result.stderr)
        self.assertIn("refs/tags/v2", result.stderr)

    def test_distinct_commits_are_each_gated_once(self) -> None:
        records = self._record("refs/heads/main", self.second)
        records += self._record("refs/tags/v1", self.first)

        result = self._run(records)

        self.assertEqual(result.returncode, 0, result.stderr)
        gates = self._gates()
        self.assertEqual([gate[0] for gate in gates], ["nix-shell", "nix"] * 2)
        self.assertEqual([gate[1] for gate in gates], [self.second] * 2 + [self.first] * 2)

    def test_cited_artifact_matching_target_is_verified_before_gates(self) -> None:
        artifact = self._green_artifact()

        result = self._run(
            self._record("refs/heads/main", self.second), artifact=artifact
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        gates = self._gates()
        self.assertEqual(
            [gate[0] for gate in gates],
            ["nix-shell-verify", "nix-shell", "nix"],
        )
        self.assertIn("suite artifact verified", result.stderr)
        self.assertIn(str(artifact), result.stderr)

    def test_cited_artifact_mismatching_target_fails_without_gates(self) -> None:
        artifact = self._green_artifact()

        result = self._run(
            self._record("refs/heads/old", self.first), artifact=artifact
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(
            [gate[0] for gate in self._gates()], ["nix-shell-verify"]
        )
        self.assertIn("expected HEAD", result.stderr)
        self.assertIn(self.first, result.stderr)

    def test_one_cited_artifact_cannot_cover_distinct_target_commits(self) -> None:
        artifact = self._green_artifact()
        records = self._record("refs/heads/main", self.second)
        records += self._record("refs/tags/old", self.first)

        result = self._run(records, artifact=artifact)

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(
            [gate[0] for gate in self._gates()],
            ["nix-shell-verify", "nix-shell-verify"],
        )
        self.assertIn(self.first, result.stderr)
        self.assertIn("expected HEAD", result.stderr)

    def test_noncommit_ref_fails_loudly_without_running_gates(self) -> None:
        blob = self._git("hash-object", "-w", "--stdin", input_text="not a commit")

        result = self._run(self._record("refs/tags/blob", blob))

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self._gates(), [])
        self.assertIn("refs/tags/blob", result.stderr)
        self.assertIn(blob, result.stderr)
        self.assertIn("commit", result.stderr.lower())

    def test_temporary_validation_worktree_is_always_cleaned_up(self) -> None:
        result = self._run(self._record("refs/heads/main", self.second))

        self.assertEqual(result.returncode, 0, result.stderr)
        gates = self._assert_one_validation(self.second)
        self._assert_worktrees_removed(gates)

    def test_temporary_validation_worktree_is_cleaned_up_when_gate_fails(self) -> None:
        result = self._run(
            self._record("refs/heads/main", self.second), fail_gate="nix-shell"
        )

        self.assertNotEqual(result.returncode, 0)
        gates = self._gates()
        self.assertEqual([gate[0] for gate in gates], ["nix-shell"])
        self._assert_worktrees_removed(gates)

    def test_tracked_hooks_path_uses_linked_worktree_hook_on_real_push(self) -> None:
        # Make the primary checkout's tracked hook observably stale.
        primary_hook = self.repo / "scripts" / "pre-push"
        primary_hook.parent.mkdir()
        primary_hook.write_text(
            "#!/usr/bin/env bash\n"
            "echo 'STALE PRIMARY HOOK RAN' >&2\n"
            "exit 91\n"
        )
        primary_hook.chmod(0o755)
        self._git("add", "scripts/pre-push")
        self._git("commit", "-q", "-m", "stale primary hook")

        linked = self.root / "linked"
        self._git(
            "worktree", "add", "-q", "-b", "linked-hook", str(linked), self.second
        )
        linked_hook = linked / "scripts" / "pre-push"
        linked_hook.parent.mkdir()
        linked_hook.write_text(HOOK.read_text())
        linked_hook.chmod(0o755)
        self._git_at(linked, "add", "scripts/pre-push")
        self._git_at(linked, "commit", "-q", "-m", "current linked hook")
        pushed_commit = self._git_at(linked, "rev-parse", "HEAD")

        remote = self.root / "remote.git"
        self._git_at(self.root, "init", "-q", "--bare", str(remote))
        self._git_at(linked, "remote", "add", "target", str(remote))

        # Relative core.hooksPath values are resolved from the worktree root
        # for pre-push, so every linked worktree uses its own tracked script.
        self._git("config", "core.hooksPath", "scripts")
        env = os.environ.copy()
        env["GIT_EDITOR"] = "true"
        env["GIT_SEQUENCE_EDITOR"] = "true"
        env["PATH"] = f"{self.stub_dir}:{env['PATH']}"
        env["GATE_LOG"] = str(self.gate_log)
        result = subprocess.run(
            ["git", "push", "target", "HEAD:refs/heads/hook-test"],
            cwd=linked,
            text=True,
            capture_output=True,
            env=env,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self._assert_one_validation(pushed_commit)
        self.assertIn(pushed_commit, result.stderr)
        self.assertNotIn("STALE PRIMARY HOOK RAN", result.stderr)
        self.assertEqual(
            self._git_at(remote, "rev-parse", "refs/heads/hook-test"), pushed_commit
        )


if __name__ == "__main__":
    unittest.main()
