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
remote = " ".join(args)
state["events"].append(["ssh", *args])


def save():
    state_path.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")


agent_disabled = any(
    args[index] == "-o"
    and index + 1 < len(args)
    and args[index + 1] == "IdentityAgent=none"
    for index in range(len(args))
) or "-oIdentityAgent=none" in args
if state["forced_agent_present"] and not agent_disabled:
    state["forced_command_hits"] += 1
    save()
    raise SystemExit(0)


# A real read can fail outright: an unreachable host, a systemctl that exits
# non-zero. The verifier's `die "could not read ..."` branches exist for that
# world, so the fake has to be able to produce it -- otherwise those branches
# are unreachable from every test and a regression there is invisible (#1172
# item 6). Exit 255 is ssh's own transport-failure code.
for fragment in state["ssh_failures"]:
    if fragment in remote:
        print(f"ssh: connect to host: {fragment}", file=sys.stderr)
        save()
        raise SystemExit(255)


def show_properties(current):
    """Emit only the properties the command actually asked for, in the order
    real `systemctl show` uses. Printing all four unconditionally would let a
    dropped `--property=` survive every test while breaking every real deploy
    (the caller's sed would yield an empty value)."""
    for key in ("ActiveState", "SubState", "InvocationID", "Result"):
        if f"--property={key}" in remote:
            print(f"{key}={current.get(key, '')}")


if "systemctl show cratedigger-db-migrate.service" in remote:
    states = state["migrate_states"]
    index = min(state["migrate_state_index"], len(states) - 1)
    current = states[index]
    state["migrate_state_index"] += 1
    show_properties(current)
    save()
    raise SystemExit(0)

if "systemctl show cratedigger.service" in remote:
    states = state["system_states"]
    index = min(state["system_state_index"], len(states) - 1)
    current = states[index]
    state["system_state_index"] += 1
    show_properties(current)
    save()
    raise SystemExit(0)

if "journalctl" in remote and "--show-cursor" in remote:
    print("-- No entries --")
    print(f"-- cursor: {state['cursor']}")
    save()
    raise SystemExit(0)

if "journalctl" in remote and "--after-cursor=" in remote:
    snapshots = state["start_journal_snapshots"]
    index = min(state["start_journal_index"], len(snapshots) - 1)
    state["start_journal_index"] += 1
    for record in snapshots[index]:
        print(json.dumps(record, sort_keys=True))
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
    # Distinct from every cratedigger.service value on purpose: a migrate-unit
    # read that actually queried cratedigger.service must not be able to pass.
    # They also share a 24-character prefix, so a comparison that truncated the
    # InvocationID instead of comparing it whole would see them as equal and
    # fail the tests rather than silently passing.
    MIGRATE_OLD = "5" * 24 + "1" * 8
    MIGRATE_NEXT = "5" * 24 + "2" * 8
    SOURCE = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-source"
    CURSOR = "s=abc;i=1;b=def;m=2;t=3;x=4"

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

    @staticmethod
    def migrate_state(
        invocation: str,
        *,
        active: str = "active",
        sub: str = "exited",
        result: str = "success",
    ) -> dict[str, str]:
        """The migrate oneshot's resting shape (#1161): RemainAfterExit keeps
        it at active/exited/success indefinitely, which is precisely why those
        three fields cannot distinguish a fresh run from a stale one."""
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

    @staticmethod
    def start_record(invocation: str) -> dict[str, str]:
        return {
            "INVOCATION_ID": invocation,
            "JOB_TYPE": "start",
            "MESSAGE": "Starting Cratedigger — Soulseek download pipeline...",
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
        start_journal_snapshots: list[list[dict[str, str]]] | None = None,
        migrate_states: list[dict[str, str]] | None = None,
        forced_agent_present: bool = False,
        ssh_failures: list[str] | None = None,
    ) -> None:
        """``ssh_failures`` holds command fragments the fake ssh must fail on,
        reproducing an unreachable host or a non-zero ``systemctl``."""
        if migrate_states is None:
            migrate_states = [self.migrate_state(self.MIGRATE_OLD)]
        self.state_path.write_text(
            json.dumps(
                {
                    "events": [],
                    "forced_agent_present": forced_agent_present,
                    "forced_command_hits": 0,
                    "system_states": system_states,
                    "system_state_index": 0,
                    "migrate_states": migrate_states,
                    "migrate_state_index": 0,
                    "journal_snapshots": journal_snapshots,
                    "journal_indexes": {},
                    "cursor": self.CURSOR,
                    "start_journal_snapshots": start_journal_snapshots or [[]],
                    "start_journal_index": 0,
                    "ssh_failures": ssh_failures or [],
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
