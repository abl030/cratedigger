"""A real-git world for ``scripts/deploy.sh`` with fakes at its external edges.

The deploy script is mostly git: fetch two repositories, add a detached
worktree, commit a one-file change, verify its signature, push, read the
remote back. All of that runs for real here against two local bare
repositories standing in for GitHub and Forgejo, with a throwaway SSH signing
key so ``git log --format=%G?`` genuinely reports ``G``. Only the edges the
test host cannot supply are faked, each as a tiny stub on ``PATH`` importing
one shared shim module (so CPython caches its bytecode once, issue #1156):

* ``hostname`` -- the doc1 refusal;
* ``nix`` -- rewrites ``flake.lock``'s ``cratedigger-src`` revision from the
  ``--override-input`` argument the script passes, exactly as the real
  command would, or fails on demand;
* ``fleet-deploy`` -- records the trigger;
* ``ssh`` -- replays a scripted sequence of ``nixos-upgrade.service`` states,
  answers the journal, and reports the fleet anchor as whatever Forgejo
  master currently is (or an override);
* ``scripts/forgejo-auth.sh`` inside the nixosconfig fixture -- the credential
  boundary the real script calls by that path, here pushing and reading back
  with plain git against the local bare "Forgejo".

Every fake records what it was asked, with whether ``SSH_AUTH_SOCK`` reached
it, into one JSON state file the tests read back.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

from tests.fakes.subprocess_env import inherited_environment

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEPLOY_SCRIPT = REPO_ROOT / "scripts" / "deploy.sh"

FORGEJO_URL = "https://git.ablz.au/abl030/nixosconfig.git"
PREVIOUS_INVOCATION = "a" * 32
NEW_INVOCATION = "b" * 32

_SHIM_MODULE = r'''# Shared body for the deploy fake commands (hostname, nix, ssh,
# fleet-deploy, and the forgejo-auth helper committed into the nixosconfig
# fixture). Imported by tiny stubs so CPython compiles it once per fixture.
import json
import os
import subprocess
import sys
from pathlib import Path

FORGEJO_URL = "https://git.ablz.au/abl030/nixosconfig.git"


def load():
    path = Path(os.environ["DEPLOY_FAKE_STATE"])
    return path, json.loads(path.read_text(encoding="utf-8"))


def save(path, state):
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def record(state, *event):
    state["events"].append([*event, "SSH_AUTH_SOCK" in os.environ])


def fail(path, state, message, code=1):
    print(message, file=sys.stderr)
    save(path, state)
    raise SystemExit(code)


def main(command):
    path, state = load()
    args = sys.argv[1:]
    if command == "hostname":
        record(state, "hostname")
        print(state["hostname"])
    elif command == "fleet-deploy":
        record(state, "fleet-deploy", *args)
        if state["fault"] == "fleet_deploy":
            fail(path, state, "fleet-deploy: fake trigger failure")
    elif command == "nix":
        record(state, "nix", *args)
        if state["fault"] == "nix":
            fail(path, state, "nix: fake update failure")
        expected = ["flake", "update", "cratedigger-src",
                    "--override-input", "cratedigger-src"]
        if args[:5] != expected or len(args) != 6:
            fail(path, state, f"unexpected fake nix command: {args!r}", 2)
        ref = args[5]
        prefix = "github:abl030/cratedigger/"
        if not ref.startswith(prefix):
            fail(path, state, f"unexpected flake ref: {ref!r}", 2)
        lock_path = Path.cwd() / "flake.lock"
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
        pinned = ref[len(prefix):]
        if state["fault"] == "nix_wrong_rev":
            pinned = "e" * 40
        lock["nodes"]["cratedigger-src"]["locked"]["rev"] = pinned
        lock_path.write_text(json.dumps(lock, indent=2, sort_keys=True) + "\n",
                             encoding="utf-8")
        if state["fault"] == "nix_extra_file":
            (Path.cwd() / "extra.txt").write_text("debris\n", encoding="utf-8")
    elif command == "ssh":
        host, remote = args[0], " ".join(args[1:])
        record(state, "ssh", host, remote)
        if state["fault"] == "ssh":
            fail(path, state, "ssh: connect to host doc2: fake failure", 255)
        if ("systemctl show nixos-upgrade.service" in remote
                and "--property=ActiveState --value" in remote):
            states = state["previous_active_states"]
            index = min(state["previous_active_index"], len(states) - 1)
            state["previous_active_index"] += 1
            print(states[index])
        elif ("systemctl show nixos-upgrade.service" in remote
                and "--property=InvocationID --value" in remote):
            print(state["previous_invocation"])
        elif "systemctl show nixos-upgrade.service" in remote:
            states = state["upgrade_states"]
            index = min(state["upgrade_state_index"], len(states) - 1)
            state["upgrade_state_index"] += 1
            current = states[index]
            # Only the properties asked for, in real systemctl order: a
            # dropped --property= must break the test, as it breaks doc2.
            for key in ("ActiveState", "SubState", "InvocationID", "Result"):
                if f"--property={key}" in remote:
                    print(f"{key}={current.get(key, '')}")
        elif "journalctl" in remote:
            print(state["journal"])
        elif "last-verified-rev" in remote:
            anchor = state["anchor"]
            if anchor is None:
                anchor = subprocess.run(
                    ["git", "-C", state["forgejo_bare"], "rev-parse",
                     "refs/heads/master"],
                    check=True, capture_output=True, text=True,
                ).stdout.strip()
            print(anchor)
        else:
            fail(path, state, f"unexpected fake ssh command: {remote!r}", 2)
    elif command == "forgejo-auth":
        sub = args[0]
        options = {}
        rest = args[1:]
        while rest:
            key, value = rest[0], rest[1]
            options[key] = value
            rest = rest[2:]
        record(state, "forgejo-auth", sub, options)
        if (options.get("--expected-fetch-url") != FORGEJO_URL
                or options.get("--expected-push-url") != FORGEJO_URL):
            fail(path, state, "forgejo-auth: expected URLs are not Forgejo", 2)
        if not Path(options.get("--token-file", "")).is_file():
            fail(path, state, "forgejo-auth: token file is missing", 2)
        repo, remote = options["--repo"], options["--remote"]
        if sub == "git-push":
            if state["fault"] == "push":
                fail(path, state, "forgejo-auth: fake push rejection")
            save(path, state)
            os.execvp("git", ["git", "-C", repo, "push", "--quiet", remote,
                              "--", options["--refspec"]])
        elif sub == "git-ls-remote":
            if state["fault"] == "readback_stale":
                print("d" * 40 + "\t" + options["--ref"])
                save(path, state)
                raise SystemExit(0)
            save(path, state)
            os.execvp("git", ["git", "-C", repo, "ls-remote", "--exit-code",
                              "--refs", remote, options["--ref"]])
        else:
            fail(path, state, f"unexpected forgejo-auth command: {sub!r}", 2)
    else:
        fail(path, state, f"unknown fake command: {command!r}", 2)
    save(path, state)
'''

_STUB = "#!/usr/bin/env python3\nimport _shim\n_shim.main({command!r})\n"
_HELPER_STUB = (
    "#!/usr/bin/env python3\nimport sys\nsys.path.insert(0, {bin_dir!r})\n"
    "import _shim\n_shim.main('forgejo-auth')\n"
)


def _write_executable(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)


def upgrade_state(
    invocation: str,
    active: str,
    sub: str,
    result: str = "success",
) -> dict[str, str]:
    """One ``systemctl show nixos-upgrade.service`` snapshot."""
    return {
        "InvocationID": invocation,
        "ActiveState": active,
        "SubState": sub,
        "Result": result,
    }


def successful_upgrade_states() -> list[dict[str, str]]:
    """The ordinary trigger: the old run still shown, then the new one runs."""
    return [
        upgrade_state(PREVIOUS_INVOCATION, "inactive", "dead"),
        upgrade_state(NEW_INVOCATION, "activating", "start"),
        upgrade_state(NEW_INVOCATION, "active", "running"),
        upgrade_state(NEW_INVOCATION, "inactive", "dead"),
    ]


class FakeDeployWorld:
    """Two bare repositories, two clones, a signing key, and the fakes."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.home = root / "home"
        self.tmp = root / "tmp"
        self.fake_bin = root / "bin"
        self.github_bare = root / "github.git"
        self.forgejo_bare = root / "forgejo.git"
        self.cratedigger = root / "cratedigger"
        self.nixosconfig = self.home / "nixosconfig"
        self.token_file = root / "token"
        self.state_path = root / "state.json"
        self.signing_dir = root / "signing"
        for directory in (self.home, self.tmp, self.fake_bin, self.signing_dir):
            directory.mkdir(parents=True)
        self.token_file.write_text("f" * 40 + "\n", encoding="utf-8")

        (self.fake_bin / "_shim.py").write_text(_SHIM_MODULE, encoding="utf-8")
        for command in ("hostname", "nix", "ssh", "fleet-deploy"):
            _write_executable(self.fake_bin / command, _STUB.format(command=command))

        self._make_signing_key()
        self.cratedigger_revisions = self._make_cratedigger()
        self.old_target = self.cratedigger_revisions[0]
        self.main_tip = self.cratedigger_revisions[-1]
        self.nixosconfig_base = self._make_nixosconfig()

        self.write_state({
            "hostname": "proxmox-vm",
            "events": [],
            "fault": None,
            "previous_invocation": PREVIOUS_INVOCATION,
            "previous_active_states": ["inactive"],
            "previous_active_index": 0,
            "upgrade_states": successful_upgrade_states(),
            "upgrade_state_index": 0,
            "anchor": None,
            "forgejo_bare": str(self.forgejo_bare),
            "journal": "fake nixos-upgrade journal",
        })

    # -- construction --------------------------------------------------------

    def _git_env(self) -> dict[str, str]:
        env = inherited_environment()
        env.update({
            "HOME": str(self.home),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "Deploy Test",
            "GIT_AUTHOR_EMAIL": "deploy-test@example.invalid",
            "GIT_COMMITTER_NAME": "Deploy Test",
            "GIT_COMMITTER_EMAIL": "deploy-test@example.invalid",
        })
        env.pop("XDG_CONFIG_HOME", None)
        env.pop("SSH_AUTH_SOCK", None)
        return env

    def git(self, repo: Path, *args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            env=self._git_env(),
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def _make_signing_key(self) -> None:
        key = self.signing_dir / "key"
        subprocess.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C",
             "deploy-test", "-f", str(key)],
            check=True,
            capture_output=True,
        )
        key_type, key_data = key.with_suffix(".pub").read_text(
            encoding="utf-8"
        ).split()[:2]
        (self.signing_dir / "allowed_signers").write_text(
            f"deploy-test@example.invalid {key_type} {key_data}\n",
            encoding="utf-8",
        )

    def _make_cratedigger(self) -> list[str]:
        """A bare "GitHub" with three commits on main, cloned with the script."""
        self.git(self.root, "init", "--quiet", "--bare", str(self.github_bare))
        self.git(self.root, "clone", "--quiet", str(self.github_bare),
                 str(self.cratedigger))
        self.git(self.cratedigger, "checkout", "--quiet", "-b", "main")
        revisions: list[str] = []
        for name in ("first", "second", "third"):
            (self.cratedigger / f"{name}.txt").write_text(f"{name}\n", encoding="utf-8")
            self.git(self.cratedigger, "add", f"{name}.txt")
            self.git(self.cratedigger, "commit", "--quiet", "-m", f"feat: {name}")
            revisions.append(self.git(self.cratedigger, "rev-parse", "HEAD"))
        self.git(self.cratedigger, "push", "--quiet", "-u", "origin", "main")
        scripts = self.cratedigger / "scripts"
        scripts.mkdir()
        shutil.copy(DEPLOY_SCRIPT, scripts / "deploy.sh")
        (scripts / "deploy.sh").chmod(0o755)
        return revisions

    def _make_nixosconfig(self) -> str:
        """A bare "Forgejo" whose master pins ``old_target``, cloned signed."""
        self.git(self.root, "init", "--quiet", "--bare", str(self.forgejo_bare))
        self.git(self.root, "clone", "--quiet", str(self.forgejo_bare),
                 str(self.nixosconfig))
        self.git(self.nixosconfig, "checkout", "--quiet", "-b", "master")
        self.git(self.nixosconfig, "config", "user.name", "Deploy Test")
        self.git(self.nixosconfig, "config", "user.email",
                 "deploy-test@example.invalid")
        self.git(self.nixosconfig, "config", "gpg.format", "ssh")
        self.git(self.nixosconfig, "config", "user.signingkey",
                 str(self.signing_dir / "key"))
        self.git(self.nixosconfig, "config", "gpg.ssh.allowedSignersFile",
                 str(self.signing_dir / "allowed_signers"))
        self.git(self.nixosconfig, "config", "commit.gpgsign", "true")
        self.write_lock(self.old_target)
        scripts = self.nixosconfig / "scripts"
        scripts.mkdir()
        _write_executable(
            scripts / "forgejo-auth.sh",
            _HELPER_STUB.format(bin_dir=str(self.fake_bin)),
        )
        (self.nixosconfig / "hosts.nix").write_text("{ }\n", encoding="utf-8")
        self.git(self.nixosconfig, "add", "-A")
        self.git(self.nixosconfig, "commit", "--quiet", "-m", "fixture base")
        self.git(self.nixosconfig, "push", "--quiet", "-u", "origin", "master")
        return self.git(self.nixosconfig, "rev-parse", "HEAD")

    def write_lock(self, revision: str, *, input_type: str = "github") -> None:
        lock = {
            "nodes": {
                "cratedigger-src": {
                    "locked": {
                        "lastModified": 1700000000,
                        "narHash": "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                        "owner": "abl030",
                        "repo": "cratedigger",
                        "rev": revision,
                        "type": input_type,
                    },
                    "original": {
                        "owner": "abl030",
                        "repo": "cratedigger",
                        "type": input_type,
                    },
                },
                "root": {"inputs": {"cratedigger-src": "cratedigger-src"}},
            },
            "root": "root",
            "version": 7,
        }
        (self.nixosconfig / "flake.lock").write_text(
            json.dumps(lock, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    def commit_forgejo_master(self, message: str) -> str:
        """Commit the clone's current ``flake.lock`` signed and push it."""
        self.git(self.nixosconfig, "add", "flake.lock")
        self.git(self.nixosconfig, "commit", "--quiet", "-m", message)
        self.git(self.nixosconfig, "push", "--quiet", "origin", "master")
        return self.git(self.nixosconfig, "rev-parse", "HEAD")

    def pin_forgejo_master(self, revision: str) -> str:
        """Advance the fixture's Forgejo master to a signed pin of ``revision``."""
        self.write_lock(revision)
        return self.commit_forgejo_master(f"cratedigger: fixture pin {revision[:12]}")

    # -- state ---------------------------------------------------------------

    @property
    def state(self) -> dict[str, object]:
        state: dict[str, object] = json.loads(
            self.state_path.read_text(encoding="utf-8")
        )
        return state

    def write_state(self, state: dict[str, object]) -> None:
        self.state_path.write_text(
            json.dumps(state, sort_keys=True), encoding="utf-8"
        )

    def update_state(self, **changes: object) -> None:
        state = self.state
        state.update(changes)
        self.write_state(state)

    def events(self) -> list[list[object]]:
        events: list[list[object]] = []
        raw = self.state["events"]
        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, list):
                    events.append(list(item))
        return events

    # -- observations --------------------------------------------------------

    def forgejo_master(self) -> str:
        return self.git(self.forgejo_bare, "rev-parse", "refs/heads/master")

    def forgejo_master_pins(self) -> str:
        lock = json.loads(
            self.git(self.forgejo_bare, "show", "refs/heads/master:flake.lock")
        )
        return str(lock["nodes"]["cratedigger-src"]["locked"]["rev"])

    def forgejo_master_signature(self) -> str:
        """``%G?`` of Forgejo master, verified against the fixture signers."""
        self.git(self.nixosconfig, "fetch", "--quiet", "origin")
        return self.git(self.nixosconfig, "log", "-1", "--format=%G?",
                        "refs/remotes/origin/master")

    def forgejo_master_changed_paths(self) -> list[str]:
        output = self.git(self.forgejo_bare, "diff-tree", "--no-commit-id",
                          "--name-only", "-r", "refs/heads/master")
        return output.split() if output else []

    def nixosconfig_worktrees(self) -> list[str]:
        return self.git(self.nixosconfig, "worktree", "list", "--porcelain").split(
            "\n\n"
        )

    # -- running -------------------------------------------------------------

    def environment(self, extra_env: dict[str, str] | None = None) -> dict[str, str]:
        env = inherited_environment()
        env.update({
            "PATH": f"{self.fake_bin}:{env['PATH']}",
            "HOME": str(self.home),
            "TMPDIR": str(self.tmp),
            "GIT_CONFIG_NOSYSTEM": "1",
            "NIXOSCONFIG_TOKEN_FILE": str(self.token_file),
            "CRATEDIGGER_DEPLOY_POLL_SECONDS": "0",
            # A deadline no loaded host can beat: the script's prelude alone
            # took 11 s once at load average 45 and turned a green run into a
            # timeout. Tests that need a timeout set 1 explicitly.
            "CRATEDIGGER_DEPLOY_TIMEOUT_SECONDS": "60",
            "DEPLOY_FAKE_STATE": str(self.state_path),
            # Set on purpose: every fake records whether it still saw this,
            # proving the script drops the shared agent before any edge.
            "SSH_AUTH_SOCK": str(self.root / "agent.sock"),
        })
        env.pop("XDG_CONFIG_HOME", None)
        env.update(extra_env or {})
        return env

    def run(
        self,
        *args: str,
        extra_env: dict[str, str] | None = None,
        timeout: float = 60,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(self.cratedigger / "scripts" / "deploy.sh"), *args],
            env=self.environment(extra_env),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
