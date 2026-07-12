"""Process-level fakes for the nixosconfig deploy-pin Bash entrypoint."""

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
import shutil
import signal
import sys
import time
from pathlib import Path

state_path = Path(os.environ["DEPLOY_PIN_FAKE_STATE"])
state_lock = state_path.with_suffix(".lock").open("a+", encoding="utf-8")
fcntl.flock(state_lock, fcntl.LOCK_EX)
state = json.loads(state_path.read_text(encoding="utf-8"))
command = Path(sys.argv[0]).name
raw_args = sys.argv[1:]


def save():
    state_path.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")


def fail(message):
    print(message, file=sys.stderr)
    save()
    raise SystemExit(1)


def lock_payload(revision):
    return json.dumps({
        "nodes": {
            "cratedigger-src": {
                "locked": {"rev": revision},
            },
        },
    }) + "\n"


state["argv_calls"].append([command, *raw_args])

if (
    command == "git"
    and os.environ.get("GIT_TRACE2")
    and "GIT_CONFIG_VALUE_0" in os.environ.get("GIT_TRACE2_ENV_VARS", "")
    and os.environ.get("GIT_CONFIG_VALUE_0")
):
    print(
        "trace2: GIT_CONFIG_VALUE_0=" + os.environ["GIT_CONFIG_VALUE_0"],
        file=sys.stderr,
    )

if command == "hostname":
    print(state.get("hostname", "proxmox-vm"))
    save()
    raise SystemExit(0)

if command == "nix":
    state["events"].append(["nix", *raw_args])
    save()
    time.sleep(state.get("nix_delay_seconds", 0))
    state = json.loads(state_path.read_text(encoding="utf-8"))
    if state.get("fault") == "nix":
        fail("fake nix update failed")
    if raw_args != ["flake", "update", "cratedigger-src"]:
        fail(f"unexpected nix argv: {raw_args!r}")
    Path.cwd().joinpath("flake.lock").write_text(
        lock_payload(os.environ["DEPLOY_PIN_FAKE_TARGET"]),
        encoding="utf-8",
    )
    save()
    raise SystemExit(0)

if command != "git":
    fail(f"unexpected fake command: {command}")

args = list(raw_args)
cwd = Path.cwd()
if args[:1] == ["-C"]:
    cwd = Path(args[1])
    args = args[2:]

if args[:1] == ["fetch"]:
    state["events"].append(["fetch"])
elif args == ["remote", "get-url", "origin"]:
    print(state["origin_url"])
elif args == ["remote", "get-url", "--all", "origin"]:
    print("\n".join(state["fetch_urls"]))
elif args == ["remote", "get-url", "--push", "--all", "origin"]:
    print("\n".join(state["push_urls"]))
elif args == ["rev-parse", "--path-format=absolute", "--git-common-dir"]:
    print(state["git_common_dir"])
elif args == ["rev-parse", "refs/remotes/origin/master"]:
    print(state["remote_rev"])
elif args[:3] == ["rev-parse", "--verify", "--quiet"]:
    ref = args[3]
    value = (
        state.get("pending_rev")
        if ref == "refs/cratedigger-deploy/cratedigger-src-pending"
        else state.get("receipt_rev")
    )
    if value:
        if (
            state.get("fault") == "post_commit_rev_parse"
            and state.get("pending_rev") in state["commits"]
        ):
            fail("fake post-commit rev-parse failed")
        if state.get("fault") in {
            "signal_after_commit",
            "invalid_signature_signal_after_commit",
        } and value in state["commits"]:
            save()
            os.kill(os.getppid(), signal.SIGTERM)
            time.sleep(0.1)
            raise SystemExit(143)
        print(value)
    else:
        save()
        raise SystemExit(1)
elif args[:2] == ["rev-parse", "--verify"]:
    ref = args[2]
    value = state.get("pending_rev") if ref.endswith("-pending") else None
    if not value:
        fail(f"unknown fake ref: {ref}")
    if state.get("fault") == "post_commit_rev_parse" and value in state["commits"]:
        fail("fake post-commit rev-parse failed")
    if state.get("fault") in {
        "signal_after_commit",
        "invalid_signature_signal_after_commit",
    } and value in state["commits"]:
        save()
        os.kill(os.getppid(), signal.SIGTERM)
        time.sleep(0.1)
        raise SystemExit(143)
    print(value)
elif args[:3] == ["worktree", "add", "--detach"]:
    worktree = Path(args[3])
    worktree.mkdir(parents=True)
    worktree.joinpath("flake.lock").write_text(
        lock_payload(state["remote_target"]), encoding="utf-8"
    )
    state["worktree"] = str(worktree)
    state["worktree_base"] = state["remote_rev"]
    state["events"].append(["worktree-add", str(worktree)])
elif args == ["status", "--porcelain"]:
    print(" M flake.lock")
elif args == ["add", "flake.lock"]:
    state["events"].append(["add", "flake.lock"])
elif args[:2] == ["symbolic-ref", "HEAD"]:
    state["worktree_attached_ref"] = args[2]
    state["events"].append(["symbolic-ref", args[2]])
elif args[:2] == ["commit", "-m"]:
    state["commit_count"] += 1
    revision = f'{0xC000 + state["commit_count"]:040x}'
    target = json.loads(cwd.joinpath("flake.lock").read_text(
        encoding="utf-8"
    ))["nodes"]["cratedigger-src"]["locked"]["rev"]
    state["commits"][revision] = {
        "parent": state["worktree_base"],
        "target": target,
        "message": args[2],
        "signature_good": state.get("fault") not in {
            "signature",
            "invalid_signature_signal_after_commit",
        },
    }
    state["worktree_head"] = revision
    if state.get("worktree_attached_ref") == (
        "refs/cratedigger-deploy/cratedigger-src-pending"
    ):
        state["pending_rev"] = revision
    state["events"].append(["commit", revision])
elif args == ["rev-parse", "HEAD"]:
    if state.get("fault") == "post_commit_rev_parse":
        fail("fake post-commit rev-parse failed")
    if state.get("fault") in {
        "signal_after_commit",
        "invalid_signature_signal_after_commit",
    }:
        save()
        os.kill(os.getppid(), signal.SIGTERM)
        time.sleep(0.1)
        raise SystemExit(143)
    print(state["worktree_head"])
elif args[:3] == ["log", "-1", "--format=%G?"]:
    revision = args[3]
    if state.get("fault") == "post_commit_verify":
        fail("fake post-commit verification failed")
    commit = state["commits"].get(revision)
    if commit is not None and not commit["signature_good"]:
        print("B")
    else:
        print("G")
elif args[:2] == ["cat-file", "commit"]:
    print("tree deadbeef")
    print("parent " + state["commits"].get(args[2], {}).get(
        "parent", state["remote_rev"]
    ))
    print("gpgsig -----BEGIN SSH SIGNATURE-----")
    print(" fake")
    print(" -----END SSH SIGNATURE-----")
elif args[:3] == ["rev-list", "--parents", "-n1"]:
    revision = args[3]
    commit = state["commits"].get(revision)
    if commit is None:
        fail(f"unknown fake commit: {revision}")
    print(revision, commit["parent"])
elif args[:2] == ["merge-base", "--is-ancestor"]:
    ancestor = args[2]
    descendant = args[3]
    if descendant != state["remote_rev"] or ancestor not in state["remote_ancestors"]:
        save()
        raise SystemExit(1)
elif args[:3] == ["show-ref", "--verify", "--hash"]:
    value = state.get("pending_rev") if args[3].endswith("-pending") else None
    if value:
        print(value)
    else:
        save()
        raise SystemExit(1)
elif args[:4] == ["diff-tree", "--no-commit-id", "--name-only", "-r"]:
    print("flake.lock")
elif args[:1] == ["show"] and args[1].endswith(":flake.lock"):
    revision = args[1].split(":", 1)[0]
    if revision in state["commits"]:
        target = state["commits"][revision]["target"]
    elif revision == state["remote_rev"]:
        target = state["remote_target"]
    else:
        fail(f"unknown fake revision: {revision}")
    print(lock_payload(target), end="")
elif args[:1] == ["update-ref"]:
    if args[1] == "-d":
        ref = args[2]
        if ref == "refs/cratedigger-deploy/cratedigger-src-pending":
            state["pending_rev"] = None
        state["events"].append(["delete-ref", ref])
        save()
        raise SystemExit(0)
    ref = args[1]
    expected_old = args[3] if len(args) == 4 else None
    current = (
        state.get("pending_rev")
        if ref == "refs/cratedigger-deploy/cratedigger-src-pending"
        else state.get("receipt_rev")
    )
    if expected_old is not None and (current or "") != expected_old:
        fail("fake update-ref compare-and-swap failed")
    if (
        ref == "refs/cratedigger-deploy/cratedigger-src"
        and state.get("fault") == "post_commit_update_ref"
        and args[2] in state["commits"]
    ):
        fail("fake receipt update-ref failed")
    if ref == "refs/cratedigger-deploy/cratedigger-src-pending":
        state["pending_rev"] = args[2]
    else:
        state["receipt_rev"] = args[2]
    state["events"].append(["update-ref", ref, args[2]])
elif args[:1] == ["push"]:
    revision = args[2].split(":", 1)[0]
    state["events"].append([
        "push", revision,
        "header-present" if os.environ.get("GIT_CONFIG_VALUE_0") else "no-header",
    ])
    if state.get("fault") == "push":
        fail("fake push rejected")
    commit = state["commits"][revision]
    if state["remote_rev"] != commit["parent"]:
        fail("fake non-fast-forward push rejected")
    state["remote_rev"] = revision
    state["remote_target"] = commit["target"]
    state["remote_ancestors"] = [*state["remote_ancestors"], commit["parent"]]
elif args[:1] == ["ls-remote"] and args[-1] == "refs/heads/master":
    state["events"].append(["ls-remote"])
    print(f'{state["remote_rev"]}\trefs/heads/master')
elif args[:2] == ["worktree", "remove"]:
    worktree = Path(args[-1])
    state["events"].append(["worktree-remove", str(worktree)])
    if state.get("fault") == "cleanup":
        fail("fake worktree cleanup failed")
    shutil.rmtree(worktree, ignore_errors=True)
    state["worktree"] = None
else:
    fail(f"unexpected git argv in {cwd}: {args!r}")

save()
'''


class FakeDeployPinCommands:
    """State-respecting fake git/nix/token environment for the Bash helper."""

    BASE_REV = "1" * 40
    OLD_TARGET = "2" * 40
    TARGET_REV = "3" * 40
    OTHER_REV = "4" * 40

    def __init__(self, root: Path) -> None:
        self.root = root
        self.home = root / "home"
        self.repo = self.home / "nixosconfig"
        self.fake_bin = root / "bin"
        self.tmp = root / "tmp"
        self.state_path = root / "state.json"
        self.token_file = root / "forgejo-token"
        self.repo.mkdir(parents=True)
        (self.repo / ".git").mkdir()
        self.fake_bin.mkdir()
        self.tmp.mkdir()
        self.token_file.write_text("test-secret-token\n", encoding="utf-8")
        for name in ("git", "nix", "hostname"):
            path = self.fake_bin / name
            path.write_text(_FAKE_COMMAND, encoding="utf-8")
            path.chmod(0o755)
        self.write_state({
            "argv_calls": [],
            "events": [],
            "hostname": "proxmox-vm",
            "origin_url": "https://git.ablz.au/abl030/nixosconfig.git",
            "fetch_urls": ["https://git.ablz.au/abl030/nixosconfig.git"],
            "push_urls": ["https://git.ablz.au/abl030/nixosconfig.git"],
            "git_common_dir": str(self.repo / ".git"),
            "fault": None,
            "nix_delay_seconds": 0,
            "remote_rev": self.BASE_REV,
            "remote_target": self.OLD_TARGET,
            "remote_ancestors": [],
            "receipt_rev": None,
            "pending_rev": None,
            "worktree": None,
            "worktree_base": None,
            "worktree_head": None,
            "worktree_attached_ref": None,
            "commit_count": 0,
            "commits": {},
        })

    @property
    def state(self) -> dict[str, Any]:
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def write_state(self, state: dict[str, Any]) -> None:
        self.state_path.write_text(
            json.dumps(state, sort_keys=True), encoding="utf-8"
        )

    def update_state(self, **changes: Any) -> None:
        state = self.state
        state.update(changes)
        self.write_state(state)

    def clear_fault(self) -> None:
        self.update_state(fault=None)

    def environment(
        self, target: str, *, extra_env: dict[str, str] | None = None
    ) -> dict[str, str]:
        env = {
            **os.environ,
            "PATH": f"{self.fake_bin}:{os.environ['PATH']}",
            "HOME": str(self.home),
            "TMPDIR": str(self.tmp),
            "NIXOSCONFIG_TOKEN_FILE": str(self.token_file),
            "DEPLOY_PIN_FAKE_STATE": str(self.state_path),
            "DEPLOY_PIN_FAKE_TARGET": target,
        }
        env.update(extra_env or {})
        return env

    def popen(
        self,
        script: Path,
        *,
        target: str | None = None,
        message: str = "cratedigger: test pin",
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.Popen[str]:
        target = target or self.TARGET_REV
        return subprocess.Popen(
            [str(script), target, message],
            env=self.environment(target, extra_env=extra_env),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def run(
        self,
        script: Path,
        *,
        target: str | None = None,
        message: str = "cratedigger: test pin",
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        target = target or self.TARGET_REV
        return subprocess.run(
            [str(script), target, message],
            env=self.environment(target, extra_env=extra_env),
            capture_output=True,
            text=True,
            timeout=20,
        )
