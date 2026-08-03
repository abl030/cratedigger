"""Executable runbook pins for docs/debugging-cli.md."""

from __future__ import annotations

import os
import subprocess
import unittest
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


class TestDecisionCorpusExportRunbook(unittest.TestCase):
    def test_remote_export_fragment_preserves_debt_exit_under_zsh(self) -> None:
        """The documented doc2 command reaches tar after an admitted debt exit."""
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            (bin_dir / "sudo").write_text(
                "#!/usr/bin/env zsh\nprintf 'PGPASSWORD=test\\n'\n",
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
exit 2
""",
                encoding="utf-8",
            )
            (bin_dir / "tar").write_text(
                """#!/usr/bin/env zsh
work="$2"
test "$(cat "$work/export-status")" = 2
test -f "$work/corpus.jsonl"
test -f "$work/coverage.json"
""",
                encoding="utf-8",
            )
            for command in bin_dir.iterdir():
                command.chmod(0o755)
            completed = subprocess.run(
                ["zsh", "-fc", _decision_export_remote_fragment()],
                capture_output=True,
                check=False,
                cwd=root,
                env={**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"},
                text=True,
            )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
