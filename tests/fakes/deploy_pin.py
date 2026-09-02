"""Process-level fakes for the nixosconfig deploy-pin Bash entrypoint."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

from tests.fakes.subprocess_env import inherited_environment
from tests.parent_signal_guard import guard_source_prelude

# _SHIM_MODULE_HEADER/_SHIM_MODULE_BODY are split around the guard prelude
# (issue #1250) so that generated text can be spliced in at define time --
# deploy_pin.py is a normal importable module (unlike the -S shim itself)
# and can call tests.parent_signal_guard directly. expected_signature=None:
# this shim's real parent is never a ProcessPoolExecutor pool worker (it is
# whatever invoked scripts/pin_nixosconfig.sh under test -- bash or pytest),
# so there is no --multiprocessing-fork shape to check, and skipping that
# clause also means one fewer /proc read per guarded call (see
# tests/parent_signal_guard.py's own docstring for the budget this respects:
# the shim runs with `-S` and must add no new imports).
_SHIM_MODULE_HEADER = r'''# Shared body for the nixosconfig deploy-pin fake git/nix/hostname commands.
# Imported by a tiny per-command stub (never executed directly as __main__)
# so CPython compiles it once and caches the bytecode in __pycache__: the
# ~28 subprocess spawns per script run (tests/fakes/deploy_pin.py profiling,
# issue #1131 -- 26 git + 1 nix + 1 hostname) then load the cached .pyc
# instead of recompiling this whole body from source on every single one
# (issue #1156 item 4). Must stay stdlib-only -- the stub invoking main()
# runs with `-S` (skips `site`).
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
'''

_SHIM_MODULE_BODY = r'''

def main():
    # Issue #1250: captured ONCE, as the very first statement -- never a
    # bare getppid() re-read right before each signal below. See
    # tests/parent_signal_guard.py's module docstring for why capturing
    # early is necessary but not, on its own, sufficient; __pg_refusal_reason
    # (defined above, spliced in from guard_source_prelude()) is the live
    # re-check that actually makes each guarded kill below safe.
    __pg_intended_parent = os.getppid()
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
        """Best-effort recursive delete -- avoids importing `shutil` per call.
        Matches shutil.rmtree(ignore_errors=True) for a real directory or a
        missing path, the only two shapes this fake ever produces (it only
        ever deletes a plain directory it itself created via os.makedirs()).
        Diverges for a non-directory root: shutil's ignore_errors=True leaves
        a file/symlink root untouched, this unlinks that one entry instead --
        unreachable here, but not identical, so said plainly rather than
        claimed away. It never follows a symlink out of the tree: a symlinked
        child is unlinked, never descended into."""
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
                    # Mirrors the real flake input's unpinned `original` node
                    # (github:abl030/cratedigger, no ref) -- production derives
                    # its `--override-input` ref from exactly this shape, never
                    # a hardcoded string, so the fake must carry it too.
                    "original": dict(state["cratedigger_input_original"]),
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
        override_marker = ["--override-input", "cratedigger-src"]
        if raw_args[:3] == ["flake", "update", "cratedigger-src"] and (
            raw_args[3:5] == override_marker
        ):
            # Production derives this ref from flake.lock's own cratedigger-src
            # `original` node and never hardcodes it -- parse it back out here
            # so a test can prove the SCRIPT actually derived it (rather than
            # this fake just trusting DEPLOY_PIN_FAKE_TARGET), and so a mutant
            # that reverts to a plain `nix flake update cratedigger-src` (no
            # override) is caught: that shape falls through to the branch_tip
            # case below instead of pinning the requested revision.
            if len(raw_args) != 6:
                fail(f"unexpected nix argv: {raw_args!r}")
            ref = raw_args[5]
            scheme, _, path = ref.partition(":")
            segments = path.split("/")
            if scheme != "github" or len(segments) != 3 or not segments[2]:
                fail(f"unparseable override-input ref: {ref!r}")
            revision = segments[2]
            if state.get("fault") == "nix_missing_revision":
                fail(f"fake nix: revision does not exist on remote: {revision}")
            target_pin = revision
        elif raw_args == ["flake", "update", "cratedigger-src"]:
            # The plain (no-override) form now models what production used to
            # do: follow whatever the branch currently resolves to, which is
            # NOT necessarily the requested target. Reachable only by a mutant
            # that drops the --override-input argv.
            target_pin = state["branch_tip"]
        else:
            fail(f"unexpected nix argv: {raw_args!r}")
        if state["remote_move_on_nix"]:
            move_live_remote()
        with open(os.path.join(os.getcwd(), "flake.lock"), "w", encoding="utf-8") as _f:
            _f.write(lock_payload(target_pin))
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
                if __pg_refusal_reason(__pg_intended_parent) is None:
                    os.kill(__pg_intended_parent, SIGTERM)
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
            if __pg_refusal_reason(__pg_intended_parent) is None:
                os.kill(__pg_intended_parent, SIGTERM)
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
            # A correctly signed pin commit that also carries a module change --
            # the world verify_pin_commit's changed-paths guard exists to reject
            # as definitively invalid (#1172 item 2).
            "changed_paths": (
                ["flake.lock", "modules/nixos/services/cratedigger.nix"]
                if state.get("fault") == "extra_changed_paths"
                else ["flake.lock"]
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
            if __pg_refusal_reason(__pg_intended_parent) is None:
                os.kill(__pg_intended_parent, SIGTERM)
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
        # Answer for the revision actually asked about. Printing "flake.lock"
        # unconditionally made verify_pin_revision's "changes paths other than
        # flake.lock" rejection unreachable from every test -- a pin commit that
        # smuggled in a module change alongside the lock bump could not be
        # modelled at all (#1172 item 2).
        revision = args[4]
        commit = captured_object(revision)
        if commit is None:
            fail(f"uncaptured fake revision: {revision}")
        print("\n".join(commit.get("changed_paths") or ["flake.lock"]))
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

_SHIM_MODULE = (
    _SHIM_MODULE_HEADER
    + guard_source_prelude(expected_signature=None)
    + _SHIM_MODULE_BODY
)

# Each fake command is this tiny stub, not the shim body itself: the body
# lives in one shared `_shim.py` module written once per fixture instance
# (see FakeDeployPinCommands.__init__). CPython never caches bytecode for a
# script run directly as __main__, so leaving the shim body inline here
# would recompile it from source on every one of the ~28 subprocess spawns
# per script run. Importing it instead lets CPython write
# `__pycache__/_shim.cpython-*.pyc` on the first call and reuse it on every
# later one within the same fixture directory (issue #1156 item 4).
#
# No explicit sys.path manipulation: the interpreter inserts the running
# script's own directory as sys.path[0] before user code executes, `-S`
# (skip `site`) does not change that, and `_shim.py` always sits beside this
# stub in the same fixture directory -- so a bare `import _shim` already
# resolves. Nothing here changes the process's current working directory;
# the shim's own `git -C ...` handling tracks `cwd` as a plain local to
# build paths with, it never calls `os.chdir`.
_STUB_COMMAND = r'''#!/usr/bin/env -S python3 -S
import _shim

_shim.main()
'''


class FakeDeployPinCommands:
    """State-respecting fake git/nix/token environment for the Bash helper."""

    TOKEN_BYTES = b"test-secret-token" + (b"x" * 23)
    BASE_REV = "1" * 40
    OLD_TARGET = "2" * 40
    TARGET_REV = "3" * 40
    OTHER_REV = "4" * 40
    DEFAULT_INPUT_OWNER = "abl030"
    DEFAULT_INPUT_REPO = "cratedigger"

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
        # The heavy shim body is written once as an importable module so
        # CPython caches its compiled bytecode in __pycache__ across the
        # ~28 subprocess spawns per script run; each fake command below is
        # a tiny stub that imports it (issue #1156 item 4).
        (self.fake_bin / "_shim.py").write_text(_SHIM_MODULE, encoding="utf-8")
        for name in ("git", "nix", "hostname"):
            path = self.fake_bin / name
            path.write_text(_STUB_COMMAND, encoding="utf-8")
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
            # Default keeps every test that never exercises the plain
            # (no-override) `nix flake update` path passing unchanged: real
            # production now always passes --override-input, so branch_tip
            # only matters to the tests written specifically to probe it (and
            # to a reverted-to-plain-update mutant, which this is what makes
            # detectable).
            "branch_tip": self.TARGET_REV,
            "cratedigger_input_original": {
                "type": "github",
                "owner": self.DEFAULT_INPUT_OWNER,
                "repo": self.DEFAULT_INPUT_REPO,
            },
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
        # `target` is unused here: the fake `nix` binary now derives the
        # pinned revision from the `--override-input` argv the script itself
        # passes it (mirroring production, which reads it from
        # $target_revision), never from an env-var side channel. Kept as a
        # parameter for call-site symmetry with the script's own argv.
        del target
        env = {
            **inherited_environment(),
            "PATH": f"{self.fake_bin}:{os.environ['PATH']}",
            "HOME": str(self.home),
            "TMPDIR": str(self.tmp),
            "NIXOSCONFIG_TOKEN_FILE": str(self.token_file),
            "DEPLOY_PIN_FAKE_STATE": str(self.state_path),
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
