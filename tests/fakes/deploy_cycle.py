"""Process-level fake for the exact Cratedigger cycle verifier."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any


_FAKE_SSH = r'''#!/usr/bin/env python3
import json
import os
import re
import sys
from pathlib import Path

state_path = Path(os.environ["DEPLOY_CYCLE_FAKE_STATE"])
state = json.loads(state_path.read_text(encoding="utf-8"))
args = sys.argv[1:]
remote = " ".join(args[1:]) if len(args) > 1 else ""
state["events"].append(["ssh", *args])


def save():
    state_path.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")


if "systemctl show cratedigger.service" in remote:
    states = state["system_states"]
    index = min(state["system_state_index"], len(states) - 1)
    current = states[index]
    state["system_state_index"] += 1
    for key in ("InvocationID", "ActiveState", "SubState", "Result"):
        print(f"{key}={current.get(key, '')}")
    save()
    raise SystemExit(0)

if "journalctl" in remote and "--invocation=" in remote:
    match = re.search(r"--invocation=([0-9a-f]{32})", remote)
    if match is None:
        print(f"invalid invocation command: {remote}", file=sys.stderr)
        save()
        raise SystemExit(2)
    invocation = match.group(1)
    snapshots = state["journal_snapshots"].get(invocation, [[]])
    journal_indexes = state["journal_indexes"]
    index = min(journal_indexes.get(invocation, 0), len(snapshots) - 1)
    journal_indexes[invocation] = index + 1
    for record in snapshots[index]:
        print(json.dumps(record, sort_keys=True))
    save()
    raise SystemExit(0)

print(f"unexpected fake ssh command: {args!r}", file=sys.stderr)
save()
raise SystemExit(2)
'''


class FakeDeployCycleCommands:
    """Drive the real Bash verifier through deterministic systemd worlds."""

    OLD = "1" * 32
    OLD_SUCCESSOR = "2" * 32
    TARGET = "3" * 32
    NEXT = "4" * 32
    SOURCE = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-source"

    def __init__(self, root: Path) -> None:
        self.root = root
        self.fake_bin = root / "bin"
        self.state_path = root / "state.json"
        self.fake_bin.mkdir()
        ssh = self.fake_bin / "ssh"
        ssh.write_text(_FAKE_SSH, encoding="utf-8")
        ssh.chmod(0o755)
        self.write_state(
            system_states=[self.system_state(self.OLD)],
            journal_snapshots={},
        )

    @staticmethod
    def system_state(
        invocation: str,
        *,
        active: str = "activating",
        sub: str = "start",
        result: str = "success",
    ) -> dict[str, str]:
        return {
            "InvocationID": invocation,
            "ActiveState": active,
            "SubState": sub,
            "Result": result,
        }

    @classmethod
    def source_record(
        cls,
        invocation: str = TARGET,
        *,
        source: str = SOURCE,
    ) -> dict[str, str]:
        return {
            "_SYSTEMD_INVOCATION_ID": invocation,
            "_CMDLINE": f"/nix/store/python/bin/python {source}/cratedigger.py --redis-host 127.0.0.1",
            "MESSAGE": "Cratedigger starting",
        }

    @classmethod
    def success_records(
        cls,
        invocation: str = TARGET,
        *,
        source: str = SOURCE,
    ) -> list[dict[str, str]]:
        return [
            cls.source_record(invocation, source=source),
            {
                "_SYSTEMD_INVOCATION_ID": invocation,
                "MESSAGE": "[INFO] Cratedigger cycle complete in 301.2s",
            },
            {
                "INVOCATION_ID": invocation,
                "MESSAGE": "cratedigger.service: Deactivated successfully.",
            },
            {
                "INVOCATION_ID": invocation,
                "JOB_RESULT": "done",
                "JOB_TYPE": "start",
                "MESSAGE": "Finished Cratedigger — Soulseek download pipeline.",
            },
        ]

    @property
    def state(self) -> dict[str, Any]:
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def write_state(
        self,
        *,
        system_states: list[dict[str, str]],
        journal_snapshots: dict[str, list[list[dict[str, str]]]],
    ) -> None:
        self.state_path.write_text(
            json.dumps(
                {
                    "events": [],
                    "system_states": system_states,
                    "system_state_index": 0,
                    "journal_snapshots": journal_snapshots,
                    "journal_indexes": {},
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    def run(
        self,
        script: Path,
        *args: str,
        max_polls: int = 4,
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.fake_bin}:{env['PATH']}",
                "DEPLOY_CYCLE_FAKE_STATE": str(self.state_path),
                "CRATEDIGGER_CYCLE_VERIFY_POLL_SECONDS": "0",
                "CRATEDIGGER_CYCLE_VERIFY_MAX_POLLS": str(max_polls),
                "CRATEDIGGER_CYCLE_VERIFY_TIMEOUT_SECONDS": "60",
            }
        )
        return subprocess.run(
            [str(script), *args],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )


__all__ = ["FakeDeployCycleCommands"]
