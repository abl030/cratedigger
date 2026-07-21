"""Process-level fakes for the unattended flake-update runner."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any


_FAKE_COMMAND = r'''#!/usr/bin/env python3
import fcntl
import json
import os
import sys
from pathlib import Path

state_path = Path(os.environ["DAILY_UPDATE_FAKE_STATE"])
with state_path.with_suffix(".lock").open("a+", encoding="utf-8") as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    command = Path(sys.argv[0]).name
    args = sys.argv[1:]

    def save():
        state_path.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")

    def fail(message):
        print(message, file=sys.stderr)
        save()
        raise SystemExit(1)

    state["events"].append([command, *args])

    if command == "git":
        if args[:1] == ["clone"]:
            clone_path = Path(args[-1])
            clone_path.mkdir(parents=True)
            clone_path.joinpath(".git").mkdir()
            clone_path.joinpath("flake.lock").write_text("old lock\n", encoding="utf-8")
            state["clone_path"] = str(clone_path)
        elif args == ["diff", "--quiet", "--", "flake.lock"]:
            save()
            raise SystemExit(1 if state["lock_changed"] else 0)
        elif args[:2] == ["commit", "--only"]:
            if state.get("fault") == "commit":
                fail("fake commit failed")
            state["commit_count"] += 1
            state["commit_args"] = args
        elif args[:2] == ["push", "origin"]:
            if state.get("fault") == "push":
                fail("fake push failed")
            state["push_count"] += 1
            state["push_ref"] = args[2]
        else:
            fail(f"unexpected git argv: {args!r}")
        save()
        raise SystemExit(0)

    if command == "nix":
        if args == ["flake", "update"]:
            if state.get("fault") == "update":
                fail("fake flake update failed")
            if state["lock_changed"]:
                Path.cwd().joinpath("flake.lock").write_text(
                    "new lock\n", encoding="utf-8"
                )
            save()
            raise SystemExit(0)
        if args == ["flake", "check", "--print-build-logs"]:
            stage = "flake-check"
        else:
            fail(f"unexpected nix argv: {args!r}")
    elif command == "nix-shell":
        if args[:1] != ["--run"] or len(args) != 2:
            fail(f"unexpected nix-shell argv: {args!r}")
        shell_command = args[1]
        if shell_command == "pyright --threads 4":
            stage = "pyright"
        elif shell_command == "bash scripts/run_tests.sh":
            stage = "suite"
        elif shell_command == "bash scripts/fuzz_burst.sh":
            stage = "fuzz"
        elif shell_command == "bash scripts/world_model_burst.sh":
            stage = (
                "mirror"
                if os.environ.get("CRATEDIGGER_WORLD_ENGINE") == "mirror-harness"
                else "world"
            )
        else:
            fail(f"unexpected nix-shell command: {shell_command!r}")
    else:
        fail(f"unexpected fake command: {command}")

    state["stages"].append(stage)
    state["stage_env"][stage] = {
        "TEST_DB_DSN": os.environ.get("TEST_DB_DSN"),
        "CRATEDIGGER_WORLD_DATABASE": os.environ.get(
            "CRATEDIGGER_WORLD_DATABASE"
        ),
        "CRATEDIGGER_WORLD_ENGINE": os.environ.get("CRATEDIGGER_WORLD_ENGINE"),
        "CRATEDIGGER_WORLD_MIRROR_URL": os.environ.get(
            "CRATEDIGGER_WORLD_MIRROR_URL"
        ),
        "CRATEDIGGER_WORLD_EXAMPLES": os.environ.get(
            "CRATEDIGGER_WORLD_EXAMPLES"
        ),
        "CRATEDIGGER_WORLD_STEPS": os.environ.get("CRATEDIGGER_WORLD_STEPS"),
        "HYPOTHESIS_STORAGE_DIRECTORY": os.environ.get(
            "HYPOTHESIS_STORAGE_DIRECTORY"
        ),
        "CRATEDIGGER_FUZZ_OUTPUT_DIR": os.environ.get(
            "CRATEDIGGER_FUZZ_OUTPUT_DIR"
        ),
    }
    if state.get("fault") == stage:
        fail(f"fake {stage} failed")
    save()
'''


class FakeDailyFlakeUpdateCommands:
    """Fake git/Nix commands while the real Bash runner owns orchestration."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.fake_bin = root / "fake-bin"
        self.fake_bin.mkdir()
        self.state_path = root / "state.json"
        self.automation_state = root / "automation-state"
        self.tmpdir = root / "tmp"
        self.tmpdir.mkdir()
        command = self.fake_bin / "command"
        command.write_text(_FAKE_COMMAND, encoding="utf-8")
        command.chmod(0o755)
        for name in ("git", "nix", "nix-shell"):
            (self.fake_bin / name).symlink_to(command)
        self._write_state(
            {
                "fault": None,
                "lock_changed": True,
                "events": [],
                "stages": [],
                "stage_env": {},
                "clone_path": None,
                "commit_count": 0,
                "commit_args": [],
                "push_count": 0,
                "push_ref": None,
            }
        )

    @property
    def state(self) -> dict[str, Any]:
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def _write_state(self, state: dict[str, Any]) -> None:
        self.state_path.write_text(
            json.dumps(state, sort_keys=True), encoding="utf-8"
        )

    def update_state(self, **changes: Any) -> None:
        state = self.state
        state.update(changes)
        self._write_state(state)

    def run(
        self,
        script: Path,
        *,
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.fake_bin}:{env['PATH']}",
                "DAILY_UPDATE_FAKE_STATE": str(self.state_path),
                "CRATEDIGGER_AUTOMATION_STATE_DIR": str(self.automation_state),
                "CRATEDIGGER_MIRROR_URL": "http://mirror.example.test/ws/2",
                "CRATEDIGGER_UPDATE_REPOSITORY": (
                    "https://github.com/abl030/cratedigger.git"
                ),
                "CRATEDIGGER_UPDATE_BRANCH": "main",
                "TMPDIR": str(self.tmpdir),
                "TEST_DB_DSN": "postgresql://production-must-not-leak",
            }
        )
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            ["bash", str(script)],
            cwd=self.root,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
