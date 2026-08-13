"""Process-level fakes for the nixosconfig deploy-pin Bash entrypoint."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

_FAKE_COMMAND = r'''#!/usr/bin/env -S python3 -S
import fcntl
import json
import os
import sys
import time

# SIGTERM's POSIX signal number, hardcoded to avoid importing `signal` on
# every one of the ~28 subprocess spawns per script run (tests/fakes/deploy_pin.py
# profiling, issue #1131): the `signal` module costs real per-process import
# time and this fake never needs anything from it beyond this one constant.
SIGTERM = 15

state_path = os.environ["DEPLOY_PIN_FAKE_STATE"]
lock_path = os.path.splitext(state_path)[0] + ".lock"
state_lock = open(lock_path, "a+", encoding="utf-8")
fcntl.flock(state_lock, fcntl.LOCK_EX)
with open(state_path, encoding="utf-8") as _f:
    state = json.loads(_f.read())
command = os.path.basename(sys.argv[0])
raw_args = sys.argv[1:]


def save():
    with open(state_path, "w", encoding="utf-8") as _f:
        _f.write(json.dumps(state, sort_keys=True))


def fail(message):
    print(message, file=sys.stderr)
    save()
    raise SystemExit(1)


def rmtree(path):
    """Best-effort recursive delete, matching shutil.rmtree(ignore_errors=True)
    for the plain worktree directories this fake ever creates (no symlinks,
    no cross-device edge cases) -- avoids importing `shutil` per call."""
    if not os.path.isdir(path) or os.path.islink(path):
        try:
            os.remove(path)
        except OSError:
            pass
        return
    try:
        entries = os.listdir(path)
    except OSError:
        return
    for name in entries:
        rmtree(os.path.join(path, name))
    try:
        os.rmdir(path)
    except OSError:
        pass


def lock_payload(revision):
    return json.dumps({
        "nodes": {
            "cratedigger-src": {
                "locked": {"rev": revision},
            },
        },
    }) + "\n"


def live_remote_object():
    return {
        "parent": state["remote_parent"],
        "target": state["remote_target"],
        "signature_status": state["remote_signature_status"],
        "lock_readable": state["remote_lock_readable"],
    }


def capture_live_remote():
    revision = state["remote_rev"]
    state["captured_objects"][revision] = live_remote_object()
    state["fetched_rev"] = revision
    return revision


def move_live_remote():
    state["remote_rev"] = state["moved_remote_rev"]
    state["remote_parent"] = state["moved_remote_parent"]
    state["remote_target"] = state["moved_remote_target"]
    state["remote_signature_status"] = state["moved_remote_signature_status"]
    state["remote_lock_readable"] = state["moved_remote_lock_readable"]


def captured_object(revision):
    return state["commits"].get(revision) or state["captured_objects"].get(revision)


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
    with open(state_path, encoding="utf-8") as _f:
        state = json.loads(_f.read())
    if state.get("fault") == "nix":
        fail("fake nix update failed")
    if raw_args != ["flake", "update", "cratedigger-src"]:
        fail(f"unexpected nix argv: {raw_args!r}")
    if state["remote_move_on_nix"]:
        move_live_remote()
    with open(os.path.join(os.getcwd(), "flake.lock"), "w", encoding="utf-8") as _f:
        _f.write(lock_payload(os.environ["DEPLOY_PIN_FAKE_TARGET"]))
    save()
    raise SystemExit(0)

if command != "git":
    fail(f"unexpected fake command: {command}")

args = list(raw_args)
cwd = os.getcwd()
if args[:1] == ["-C"]:
    cwd = args[1]
    args = args[2:]

if args[:1] == ["fetch"]:
    state["events"].append(["fetch", capture_live_remote()])
elif args == ["remote", "get-url", "origin"]:
    print(state["origin_url"])
elif args == ["remote", "get-url", "--all", "origin"]:
    print("\n".join(state["fetch_urls"]))
elif args == ["remote", "get-url", "--push", "--all", "origin"]:
    print("\n".join(state["push_urls"]))
elif args == ["rev-parse", "--path-format=absolute", "--git-common-dir"]:
    print(state["git_common_dir"])
elif args == ["rev-parse", "refs/remotes/origin/master"]:
    print(state["fetched_rev"])
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
        if (
            state.get("fault") in {
                "signal_after_commit",
                "invalid_signature_signal_after_commit",
            } and value in state["commits"]
        ) or (
            state.get("fault") == "signal_after_pending_commit"
            and value == state.get("pending_rev")
            and value in state["commits"]
        ):
            save()
            os.kill(os.getppid(), SIGTERM)
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
    if (
        state.get("fault") in {
            "signal_after_commit",
            "invalid_signature_signal_after_commit",
        } and value in state["commits"]
    ) or (
        state.get("fault") == "signal_after_pending_commit"
        and value == state.get("pending_rev")
        and value in state["commits"]
    ):
        save()
        os.kill(os.getppid(), SIGTERM)
        time.sleep(0.1)
        raise SystemExit(143)
    print(value)
elif args[:3] == ["worktree", "add", "--detach"]:
    worktree = args[3]
    revision = args[4]
    commit = captured_object(revision)
    if commit is None:
        fail(f"worktree revision was not captured: {revision}")
    if state["remote_move_on_worktree_add"]:
        move_live_remote()
    os.makedirs(worktree)
    with open(os.path.join(worktree, "flake.lock"), "w", encoding="utf-8") as _f:
        _f.write(lock_payload(commit["target"]))
    state["worktree"] = worktree
    state["worktree_base"] = revision
    state["events"].append(["worktree-add", worktree, revision])
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
    with open(os.path.join(cwd, "flake.lock"), encoding="utf-8") as _f:
        target = json.loads(_f.read())["nodes"]["cratedigger-src"]["locked"]["rev"]
    state["commits"][revision] = {
        "parent": state["worktree_base"],
        "target": target,
        "message": args[2],
        "signature_material": (
            "bad"
            if state.get("fault") in {
                "signature",
                "invalid_signature_signal_after_commit",
            }
            else "good"
        ),
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
        os.kill(os.getppid(), SIGTERM)
        time.sleep(0.1)
        raise SystemExit(143)
    print(state["worktree_head"])
elif args[:3] == ["log", "-1", "--format=%G?"]:
    revision = args[3]
    if state.get("fault") == "post_commit_verify":
        fail("fake post-commit verification failed")
    commit = captured_object(revision)
    if commit is None:
        fail(f"uncaptured fake commit: {revision}")
    if revision in state["commits"] and state.get("fault") == "signature_unknown":
        signature_status = "U"
    elif revision in state["captured_objects"]:
        signature_status = commit.get("signature_status", "G")
    elif commit is not None and commit["signature_material"] == "bad":
        signature_status = "B"
    else:
        signature_status = "G"
    state["events"].append(["signature-status", revision, signature_status])
    print(signature_status)
elif args[:2] == ["cat-file", "commit"]:
    commit = captured_object(args[2])
    if commit is None:
        fail(f"uncaptured fake commit: {args[2]}")
    print("tree deadbeef")
    print("parent " + commit["parent"])
    print("gpgsig -----BEGIN SSH SIGNATURE-----")
    print(" fake")
    print(" -----END SSH SIGNATURE-----")
elif args[:3] == ["rev-list", "--parents", "-n1"]:
    revision = args[3]
    commit = captured_object(revision)
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
    commit = captured_object(revision)
    if commit is None:
        fail(f"uncaptured fake revision: {revision}")
    if not commit.get("lock_readable", True):
        fail("fake remote flake.lock is unreadable")
    print(lock_payload(commit["target"]), end="")
elif args[:1] == ["update-ref"]:
    if args[1] == "-d":
        ref = args[2]
        expected_old = args[3] if len(args) == 4 else None
        current = (
            state.get("pending_rev")
            if ref == "refs/cratedigger-deploy/cratedigger-src-pending"
            else state.get("receipt_rev")
        )
        if expected_old is None or (current or "") != expected_old:
            fail("fake delete-ref compare-and-swap failed")
        if ref == "refs/cratedigger-deploy/cratedigger-src-pending":
            state["pending_rev"] = None
        state["events"].append(["delete-ref", ref, expected_old])
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
    state["events"].append(["update-ref", ref, args[2], expected_old])
elif args[:1] == ["push"]:
    revision = args[2].split(":", 1)[0]
    state["events"].append([
        "push", revision,
        "header-present" if os.environ.get("GIT_CONFIG_VALUE_0") else "no-header",
    ])
    if state.get("fault") == "push":
        fail("fake push rejected")
    if state["remote_move_on_push"]:
        move_live_remote()
    commit = state["commits"][revision]
    if state["remote_rev"] != commit["parent"]:
        fail("fake non-fast-forward push rejected")
    state["remote_rev"] = revision
    state["remote_parent"] = commit["parent"]
    state["remote_target"] = commit["target"]
    state["remote_signature_status"] = "G"
    state["remote_lock_readable"] = True
    state["remote_ancestors"] = [*state["remote_ancestors"], commit["parent"]]
elif args[:1] == ["ls-remote"] and args[-1] == "refs/heads/master":
    state["events"].append(["ls-remote"])
    state["ls_remote_count"] += 1
    if state["ls_remote_count"] == state["remote_change_on_ls_remote_call"]:
        move_live_remote()
    print(f'{state["remote_rev"]}\trefs/heads/master')
elif args[:2] == ["worktree", "remove"]:
    worktree = args[-1]
    state["events"].append(["worktree-remove", worktree])
    if state.get("fault") == "cleanup":
        fail("fake worktree cleanup failed")
    rmtree(worktree)
    state["worktree"] = None
else:
    fail(f"unexpected git argv in {cwd}: {args!r}")

save()
'''


class FakeDeployPinCommands:
    """State-respecting fake git/nix/token environment for the Bash helper."""

    TOKEN_BYTES = b"test-secret-token" + (b"x" * 23)
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
        self.token_file.write_bytes(self.TOKEN_BYTES)
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
            "remote_parent": "0" * 40,
            "remote_target": self.OLD_TARGET,
            "remote_ancestors": [],
            "remote_signature_status": "G",
            "remote_lock_readable": True,
            "captured_objects": {
                self.BASE_REV: {
                    "parent": "0" * 40,
                    "target": self.OLD_TARGET,
                    "signature_status": "G",
                    "lock_readable": True,
                },
            },
            "fetched_rev": self.BASE_REV,
            "ls_remote_count": 0,
            "remote_change_on_ls_remote_call": None,
            "remote_move_on_nix": False,
            "remote_move_on_worktree_add": False,
            "remote_move_on_push": False,
            "moved_remote_rev": "6" * 40,
            "moved_remote_parent": self.BASE_REV,
            "moved_remote_target": self.OLD_TARGET,
            "moved_remote_signature_status": "G",
            "moved_remote_lock_readable": True,
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

    def seed_divergent_receipt(
        self, *, receipt_target: str | None = None,
        remote_target: str | None = None,
    ) -> str:
        """Seed a signed sibling receipt and current remote master."""
        receipt = "5" * 40
        state = self.state
        state["commits"][receipt] = {
            "parent": self.BASE_REV,
            "target": receipt_target or self.OLD_TARGET,
            "message": "cratedigger: prior pin",
            "signature_material": "good",
        }
        state.update(
            receipt_rev=receipt,
            remote_rev=self.OTHER_REV,
            remote_parent=self.BASE_REV,
            remote_target=remote_target or self.OLD_TARGET,
            remote_ancestors=[],
        )
        self.write_state(state)
        return receipt

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
            check=False,
        )
