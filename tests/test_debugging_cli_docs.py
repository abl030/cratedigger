"""Executable runbook pins for docs/debugging-cli.md."""

from __future__ import annotations

import os
import subprocess
import unittest
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

REPO_ROOT = Path(__file__).resolve().parent.parent
DEBUGGING_CLI = REPO_ROOT / "docs" / "debugging-cli.md"


def _decision_export_remote_fragment() -> str:
    """Extract the actual doc2 command passed through SSH by the runbook."""
    for line in DEBUGGING_CLI.read_text(encoding="utf-8").splitlines():
        if line.startswith("ssh doc2 '") and "export-status" in line:
            start = len("ssh doc2 '")
            remote, separator, _local = line[start:].partition("' | tar ")
            if separator:
                return remote
    raise AssertionError("decision-corpus SSH export fragment missing from runbook")


def _decision_export_local_acceptance() -> str:
    """Extract the runbook's local 0/2 export-status admission check."""
    for line in DEBUGGING_CLI.read_text(encoding="utf-8").splitlines():
        if line.startswith('test "$(cat "$EXPORT_DIR/export-status")" = 0'):
            return line
    raise AssertionError("decision-corpus local export-status check missing from runbook")


@dataclass(frozen=True)
class _RunbookExecution:
    remote_returncode: int
    local_acceptance_returncode: int
    decision_returncode: str
    export_status: str | None
    tar_work: str | None
    work: Path


class TestDecisionCorpusExportRunbook(unittest.TestCase):
    def _run_remote_fragment(
        self,
        export_returncode: int,
        fragment: str | None = None,
    ) -> _RunbookExecution:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            bin_dir = root / "bin"
            marker_dir = root / "markers"
            transfer_dir = root / "transfer"
            work = root / "work"
            bin_dir.mkdir()
            marker_dir.mkdir()
            transfer_dir.mkdir()
            (bin_dir / "sudo").write_text(
                "#!/usr/bin/env zsh\nprintf 'PGPASSWORD=test\\n'\n",
                encoding="utf-8",
            )
            (bin_dir / "mktemp").write_text(
                "#!/usr/bin/env zsh\nmkdir -p \"$RUNBOOK_WORK\"\nprintf '%s\\n' \"$RUNBOOK_WORK\"\n",
                encoding="utf-8",
            )
            (bin_dir / "decision-differential").write_text(
                """#!/usr/bin/env zsh
while (( $# )); do
  case "$1" in
    --corpus|--coverage) touch "$2"; shift 2 ;;
    *) shift ;;
  esac
done
printf '%s\\n' "$FAKE_EXPORT_RC" > "$RUNBOOK_MARKERS/export-returncode"
exit "$FAKE_EXPORT_RC"
""",
                encoding="utf-8",
            )
            (bin_dir / "tar").write_text(
                """#!/usr/bin/env zsh
work="$2"
printf '%s\\n' "$work" > "$RUNBOOK_MARKERS/tar-work"
if [[ -f "$work/export-status" ]]; then
  cp "$work/export-status" "$RUNBOOK_TRANSFER_DIR/export-status"
else
  : > "$RUNBOOK_MARKERS/status-missing"
fi
""",
                encoding="utf-8",
            )
            for command in bin_dir.iterdir():
                command.chmod(0o755)
            env = {
                **os.environ,
                "FAKE_EXPORT_RC": str(export_returncode),
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "RUNBOOK_TEST_PATH": f"{bin_dir}:{os.environ['PATH']}",
                "RUNBOOK_MARKERS": str(marker_dir),
                "RUNBOOK_TRANSFER_DIR": str(transfer_dir),
                "RUNBOOK_WORK": str(work),
            }
            remote = subprocess.run(
                [
                    "zsh",
                    "-fc",
                    'export PATH="$RUNBOOK_TEST_PATH"; '
                    + (fragment or _decision_export_remote_fragment()),
                ],
                capture_output=True,
                check=False,
                cwd=root,
                env=env,
                text=True,
            )
            local = subprocess.run(
                [
                    "zsh",
                    "-fc",
                    'export PATH="$RUNBOOK_TEST_PATH"; '
                    + _decision_export_local_acceptance(),
                ],
                capture_output=True,
                check=False,
                cwd=root,
                env={**env, "EXPORT_DIR": str(transfer_dir)},
                text=True,
            )
            status_path = transfer_dir / "export-status"
            return _RunbookExecution(
                remote_returncode=remote.returncode,
                local_acceptance_returncode=local.returncode,
                decision_returncode=(marker_dir / "export-returncode").read_text(
                    encoding="utf-8"
                ).strip(),
                export_status=(
                    status_path.read_text(encoding="utf-8").strip()
                    if status_path.exists()
                    else None
                ),
                tar_work=(marker_dir / "tar-work").read_text(encoding="utf-8").strip()
                if (marker_dir / "tar-work").exists()
                else None,
                work=work,
            )

    def _assert_complete_transfer(
        self,
        result: _RunbookExecution,
        expected_export_returncode: int,
        expected_local_acceptance: int,
    ) -> None:
        self.assertEqual(result.remote_returncode, 0)
        self.assertEqual(result.decision_returncode, str(expected_export_returncode))
        self.assertEqual(result.export_status, str(expected_export_returncode))
        self.assertEqual(result.tar_work, str(result.work))
        self.assertEqual(result.local_acceptance_returncode, expected_local_acceptance)

    def test_remote_export_fragment_preserves_every_exit_before_tar_under_zsh(self) -> None:
        """The actual doc2 fragment records export exit before the transfer."""
        for export_returncode, local_acceptance in ((0, 0), (2, 0), (7, 1)):
            with self.subTest(export_returncode=export_returncode):
                self._assert_complete_transfer(
                    self._run_remote_fragment(export_returncode),
                    export_returncode,
                    local_acceptance,
                )

    def test_forced_export_status_mutant_is_rejected(self) -> None:
        fragment = _decision_export_remote_fragment().replace(
            "export_rc=$?", "export_rc=0"
        )
        with self.assertRaises(AssertionError):
            self._assert_complete_transfer(self._run_remote_fragment(2, fragment), 2, 0)

    def test_missing_export_status_mutant_is_rejected(self) -> None:
        fragment = _decision_export_remote_fragment().replace(
            'printf "%s\\n" "$export_rc" > "$work/export-status"; ', ":; "
        )
        with self.assertRaises(AssertionError):
            self._assert_complete_transfer(self._run_remote_fragment(2, fragment), 2, 0)

    def test_tar_not_reached_mutant_is_rejected(self) -> None:
        fragment = _decision_export_remote_fragment().replace(
            'tar -C "$work" -cf - corpus.jsonl coverage.json export-status;', "false;"
        )
        with self.assertRaises(AssertionError):
            self._assert_complete_transfer(self._run_remote_fragment(2, fragment), 2, 0)


if __name__ == "__main__":
    unittest.main()
