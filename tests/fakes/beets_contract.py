"""Shared real-filesystem fixture for Beets configuration authority tests."""

from __future__ import annotations

import configparser
import os
import stat
import subprocess
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Self

import yaml

from lib.beets_config_contract import (
    FETCHART_IDENTITY_FIRST_SOURCES,
    BeetsConfigError,
    BeetsConfigReport,
    ContractFinding,
)
from lib.config import read_runtime_config_strict

SAFE_DEFAULT_PATH = (
    "$albumartist/$year - $album%aunique{albumartist album,path_disambig}/"
    "$track $title"
)
SAFE_COMP_PATH = (
    "Compilations/$album%aunique{albumartist album,path_disambig}/$track $title"
)
SAFE_SINGLETON_PATH = "Non-Album/$artist/$title"
SAFE_PATH_DISAMBIG = (
    "albumdisambig or releasegroupdisambig or catalognum or label or str(year)"
)

#: The valid baseline plugin list every fixture world starts from: every
#: plugin the contract requires, plus the optional ones the tests exercise.
#: Mutant worlds are written as this list minus or plus exactly one entry.
BASELINE_PLUGINS: tuple[str, ...] = (
    "musicbrainz",
    "mbsync",
    "discogs",
    "inline",
    "permissions",
    "fetchart",
)

RUNTIME_AUTHORITIES = (
    "beets_config_dir",
    "beets_library_db",
    "beets_directory",
    "beets_state_file",
    "beets_python",
    "beets_secret_include",
)
_ROOT_ANCHOR_PROBE = r"""
import os
import sys
from pathlib import Path

from lib.beets_config_contract import _declared_path, _immutable_declared_file

root = Path(sys.argv[1])
depth = int(sys.argv[2])
current = root
relative_parts = []
for index in range(depth):
    relative_parts.append(f"authority-{index}")
    current /= relative_parts[-1]
    current.mkdir()
target = current / "runtime.ini"
target.write_text("[Beets]\n", encoding="utf-8")

for path in (*tuple(root.joinpath(*relative_parts[:index]) for index in range(1, depth + 1)), target):
    os.chown(path, 1, 1)
    path.chmod(0o555 if path.is_dir() else 0o444)
os.chown(root, 2, 2)
root.chmod(0o555)

pid = os.fork()
if pid == 0:
    os.chroot(root)
    os.chdir("/")
    os.setgroups([])
    os.setgid(2)
    os.setuid(2)
    declared = Path("/").joinpath(*relative_parts, "runtime.ini")
    if Path("/").stat().st_uid != os.geteuid():
        raise AssertionError("application does not own the chroot anchor")
    for component in (
        Path("/").joinpath(*relative_parts[:index])
        for index in range(1, depth + 1)
    ):
        if component.stat().st_uid == os.geteuid():
            raise AssertionError(f"application unexpectedly owns {component}")
        if component.stat().st_mode & 0o222:
            raise AssertionError(f"authority component is writable: {component}")
    if declared.stat().st_uid == os.geteuid() or declared.stat().st_mode & 0o222:
        raise AssertionError("declared file is not externally owned and read-only")
    if _immutable_declared_file(_declared_path(str(declared))):
        raise AssertionError(
            "app-owned filesystem root was omitted from immutability proof"
        )
    os._exit(0)

_, status = os.waitpid(pid, 0)
exit_code = os.waitstatus_to_exitcode(status)

# Restore host-user ownership so TemporaryDirectory can clean up outside the
# user namespace even when the child found a regression.
root.chmod(0o700)
for current_dir, directories, files in os.walk(root):
    current_path = Path(current_dir)
    current_path.chmod(0o700)
    os.chown(current_path, 0, 0)
    for name in directories:
        child = current_path / name
        child.chmod(0o700)
        os.chown(child, 0, 0)
    for name in files:
        child = current_path / name
        child.chmod(0o600)
        os.chown(child, 0, 0)
os.chown(root, 0, 0)
sys.exit(exit_code)
"""


def assert_app_owned_root_anchor_is_rejected(*, depth: int) -> None:
    """Exercise the real path guard with only the filesystem root app-owned."""
    with tempfile.TemporaryDirectory(
        prefix="beets-contract-root-anchor-",
        dir="/dev/shm",
    ) as root:
        proc = subprocess.run(
            [
                "unshare",
                "--map-root-user",
                "--map-auto",
                sys.executable,
                "-c",
                _ROOT_ANCHOR_PROBE,
                root,
                str(depth),
            ],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            check=False,
        )
    if proc.returncode != 0:
        raise AssertionError(
            "app-owned chroot anchor was admitted\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )





def assert_token_absent_from_owned_report(encoded_report: str, token: str) -> None:
    if token in encoded_report:
        raise AssertionError("valid Discogs token leaked into an owned report field")


def assert_redacted_load_failure(output: str, token: str) -> None:
    """Prove process output keeps only the stable load-failure category."""
    if "config_load_error" not in output:
        raise AssertionError("redacted config load category is missing")
    if token in output or any(
        marker in output
        for marker in (
            "while parsing",
            'in "<unicode string>"',
            "column ",
            "\n    ^",
        )
    ):
        raise AssertionError("raw parser/load diagnostic leaked into process output")


def assert_hard_code(
    hard_failures: tuple[ContractFinding, ...], expected_code: str
) -> None:
    codes = [finding.code for finding in hard_failures]
    if expected_code not in codes:
        raise AssertionError(f"contract admitted known-bad mutant: {expected_code}")


def assert_discogs_token_missing(report: BeetsConfigReport) -> None:
    assert_hard_code(report.hard_failures, "discogs_token_missing")


def assert_loader_is_strict(loader: Callable[[str, str], object]) -> None:
    try:
        loader("/definitely/missing/runtime.ini", "/tmp")
    except OSError:
        return
    raise AssertionError("runtime loader admitted a missing contract")


def assert_native_config_rejection(check: Callable[[], object]) -> None:
    try:
        check()
    except BeetsConfigError:
        return
    except Exception as exc:
        raise AssertionError("config load escaped as a non-contract error") from exc
    raise AssertionError("config loader admitted malformed authority")


def assert_raw_authority_rejected(load: Callable[[], object]) -> None:
    try:
        load()
    except ValueError:
        return
    raise AssertionError("strict loader admitted blank runtime authority")


def _snapshot_tree(root: Path) -> tuple[tuple[str, str, bytes | None], ...]:
    """Capture names, kinds, and bytes without mutating the observed tree."""
    if not root.exists():
        return ()
    snapshot: list[tuple[str, str, bytes | None]] = []
    for path in sorted(root.rglob("*")):
        relative = str(path.relative_to(root))
        if path.is_file():
            snapshot.append((relative, "file", path.read_bytes()))
        elif path.is_dir():
            snapshot.append((relative, "dir", None))
        else:
            snapshot.append((relative, "other", None))
    return tuple(snapshot)


def snapshot_contract_world(world: BeetsContractWorld) -> dict[str, object]:
    """Snapshot every file/tree whose purity the standalone checker claims."""
    return {
        "runtime": world.runtime_config.read_bytes(),
        "main": world.main_config.read_bytes(),
        "secret": world.secret_include.read_bytes(),
        "state": world.state_file.read_bytes(),
        "database": (
            world.library_db.read_bytes() if world.library_db.exists() else None
        ),
        "library": _snapshot_tree(world.library_root),
    }


class BeetsContractWorld:
    """One real file/config world; filesystem permissions are the capability.

    **Issue #1214 review finding F6 -- the sealed-authority-tree residual,
    analyzed and accepted, not papered over.** While sealed (from the end of
    ``__init__`` until ``close()``/``unseal()`` runs), ``authority_root`` and
    its interior directories are chown'd via
    ``unshare --map-root-user --map-auto chown`` to a subordinate uid, NOT
    this process's own real uid. That is not cosmetic:
    ``lib.beets_config_contract._has_app_owned_component`` walks every
    ancestor of a declared authority path (``authority_root`` is always one,
    since ``beets_dir``/``secret_dir``/``state_dir`` all live directly under
    it) and treats ANY ancestor owned by the running process's own euid as
    "app-owned" -- i.e. mutable, unsafe -- and real production call sites
    depend on exactly this (``_immutable_declared_file`` on ``runtime.ini``
    and the secret/main-config sources; ``_nonreplaceable_declared_path`` on
    ``state_file``). Leaving ``authority_root`` (or any directory between it
    and a declared leaf) real-user-owned, even with every write bit stripped
    by ``chmod``, would make every "this authority tree is safely immutable"
    test scenario fail against real production logic, because
    chmod-without-chown is always self-reversible by its owner (``chmod()``
    only requires ownership, not the CURRENT mode bits) -- exactly the
    mutability the contract exists to detect. So genuine foreign ownership,
    not merely restrictive permissions, is load-bearing for every test that
    exercises the sealed/admitted path, not just the handful of explicit
    tamper-detection tests. No directory layout change moves this
    requirement: any real-user-owned buffer directory anywhere between
    ``/dev/shm`` and a declared leaf trips the very ancestor-ownership check
    the sealed/admitted baseline needs to pass.

    **What actually happened in #1214 (review finding A2 -- correcting an
    earlier draft of this docstring, which called it an OOM kill): an
    ENOSPC exception, not a process kill.** Issue #1214 states explicitly
    that there were no host OOM kills; the symptom was
    ``OSError: [Errno 28] No space left on device`` from
    ``cratedigger-daily-checks.service`` filling the tmpfs the fuzz phase
    runs on. That distinction matters here specifically: an ``OSError``
    raised while EXECUTING INSIDE an already-entered
    ``with BeetsContractWorld() as world:`` block is an ordinary Python
    exception -- the ``with`` statement's normal unwinding semantics
    guarantee ``__exit__`` (and therefore ``close()``, therefore
    ``unseal()``) runs regardless of what exception type propagates through
    the block. So the failure mode that actually occurred in production is
    fully handled by this issue's core fix; this docstring must not argue
    the fix is weaker than it is by resting its case on an event that did
    not happen.

    What remains a genuinely open, narrower, UNCONFIRMED residual: a
    process-level kill the interpreter cannot catch at all (a real SIGKILL
    or OOM-killer action -- distinct from, and not what occurred in, the
    #1214 incident), or disk exhaustion striking mid-``_seal()``'s own
    ``unshare``/``chown`` subprocess call -- i.e. DURING ``__init__``,
    BEFORE the ``with`` statement's ``__enter__`` even completes, so no
    context-manager protocol is active yet to catch it. No in-process
    mechanism -- not ``close()``'s own ``finally``, not an ``atexit`` hook,
    not a ``weakref.finalize`` callback, not a caught-signal handler -- runs
    on an uncatchable kill; the kernel terminates the process before any of
    that code can execute. If either of these narrower cases strands a
    world, its authority tree is left owned by that subordinate uid, mode
    ``dr-xr-x---`` -- unremovable by this process's own real uid via any
    ordinary tool (``rm -rf``, plain ``chown``), because ``chmod``/``chown``
    require OWNERSHIP, which the real uid no longer has. Reclaiming such a
    directory requires deliberately re-running the same
    ``unshare --map-root-user --map-auto chown`` trick this fixture's own
    ``_chown_path`` uses. **This residual is accepted and currently
    UNOWNED** -- no existing or planned workstream reclaims a stranded
    sealed authority tree (an earlier draft of this docstring named the
    test-suite runner's own scratch-reaper work as the owner; that claim
    was checked and is false -- that workstream contains no reference to
    ``/dev/shm`` or this fixture, and structurally could not reclaim one
    with an ordinary ``rmtree`` regardless, since only the exact
    ``unshare``-based chown this class itself uses can undo the seal).
    Each stranded tree costs about 16 KB of tmpfs (measured: one sealed
    ``authority_root``, ``du -sh`` block accounting on ``/dev/shm``) --
    small enough, and the window narrow enough (see below), that this is a
    reasoned, bounded, accepted gap, not an oversight.

    What IS fixed, and is the real lever available without weakening the
    tested contract: the SIZE of the exposure window for that narrower
    residual. Before issue #1214's core fix, every ``@given`` example's
    world leaked past its own example (``addCleanup`` fires once per test
    METHOD, but Hypothesis re-executes the body once per EXAMPLE) and
    stayed alive -- sealed -- for the rest of the method, so up to
    ``max_examples`` worlds (a fresh re-measurement found 2491 at the daily
    gate's real budget, ``CRATEDIGGER_FUZZ_MAX_EXAMPLES=2500``; issue #1214
    itself first measured 2469 from a slightly different run) were
    simultaneously sealed at any instant an uncatchable kill could land.
    Binding each world's lifetime to the example that created it
    (``with BeetsContractWorld() as world:``) means ``close()`` -- and
    therefore ``unseal()`` -- runs before the NEXT example's world is even
    constructed, so at most ONE world is ever sealed and resident at a time.
    That is roughly a 2500x reduction in the instantaneous exposure surface
    for the narrower, unconfirmed hard-kill residual described above -- it
    is not a claim about the actual #1214 incident, which this fix handles
    completely by ordinary exception unwinding.
    """

    _scratch_validated = False

    @classmethod
    def _validate_authority_scratch(cls) -> None:
        if cls._scratch_validated:
            return
        scratch = Path("/dev/shm")
        filesystem = subprocess.run(
            ["stat", "-f", "-c", "%T", str(scratch)],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        metadata = scratch.stat()
        if (
            filesystem != "tmpfs"
            or metadata.st_uid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o1777
        ):
            raise AssertionError(
                "Beets contract tests require root-owned 01777 tmpfs /dev/shm"
            )
        cls._scratch_validated = True

    def __init__(self, *, role: str = "importer", token: str = "safe-token"):
        self._validate_authority_scratch()
        self._tmp = tempfile.TemporaryDirectory(prefix="beets-contract-")
        # Declared configuration lives under a subordinate-UID-owned entry in
        # root-owned sticky /dev/shm. The test identity can read it but cannot
        # rename that entry, so the fixture models a real deployment authority
        # instead of pretending chmod beneath a user-owned temp root is enough.
        self._authority_tmp = tempfile.TemporaryDirectory(
            prefix="beets-contract-authority-", dir="/dev/shm"
        )
        self._closed = False
        self.root = Path(self._tmp.name)
        self.authority_root = Path(self._authority_tmp.name)
        self.contract_dir = self.authority_root / "contract"
        self.runtime_dir = self.root / "runtime"
        self.beets_dir = self.authority_root / "beets"
        self.secret_dir = self.authority_root / "secret"
        self.state_dir = self.authority_root / "state"
        self.library_root = self.root / "library"
        self.library_db = self.root / "library.db"
        self.state_file = self.state_dir / "state.pickle"
        # Startup write-probe required paths (issue #1085). Ordinary
        # Cratedigger-owned/externally-provisioned directories, NOT part of
        # the externally owned Beets authority the role-scoped seal/unseal
        # machinery below governs -- every entrypoint's `main()` now probes
        # these before its first application effect, so every world this
        # fixture builds needs them real and writable by default. Dedicated
        # startup-write-probe tests break exactly one at a time.
        # Nested under runtime_dir (not a bare root sibling): matches
        # production's own default relationship (processing_dir defaults to
        # ``<var_dir>/processing``) and keeps every one of these inside the
        # tree ~180 rejection tests already snapshot via
        # ``tests.beets_config_startup_support._snapshot_runtime_tree``, so
        # an accidental write during a REJECTED admission would be caught
        # there rather than silently escaping the comparison (issue #1085
        # review round 2).
        self.slskd_download_dir = self.runtime_dir / "slskd-download"
        self.processing_dir = self.runtime_dir / "processing"
        self.beets_staging_dir = self.runtime_dir / "staging"
        for path in (
            self.contract_dir,
            self.runtime_dir,
            self.beets_dir,
            self.secret_dir,
            self.state_dir,
            self.library_root,
            self.slskd_download_dir,
            self.processing_dir,
            self.beets_staging_dir,
        ):
            path.mkdir()
        # The private processing tree's own strict contract
        # (lib.fs_authority._assert_private_parent / open_private_child_directory):
        # every ancestor down to processing_dir, INCLUDING runtime_dir
        # itself now that processing_dir is nested under it, must carry no
        # group/other-write bit, and processing_dir plus its albums/preview
        # children must be exactly 0700 and owned by this process. Plain
        # mkdir() leaves runtime_dir at the ambient umask's default (0775
        # under a collaborative umask 002), which _assert_private_parent
        # correctly refuses as "group/other writable" -- strip that
        # explicitly rather than relying on the ambient umask.
        os.chmod(self.runtime_dir, 0o755)
        os.chmod(self.processing_dir, 0o700)
        albums_dir = self.processing_dir / "albums"
        preview_dir = self.processing_dir / "preview"
        albums_dir.mkdir()
        preview_dir.mkdir()
        os.chmod(albums_dir, 0o700)
        os.chmod(preview_dir, 0o700)
        from beets.library import Library

        library = Library(str(self.library_db), str(self.library_root))
        library._close()
        self.state_file.write_bytes(b"state-before")
        self.secret_include = self.secret_dir / "discogs.yaml"
        self.secret_include.write_text(
            yaml.safe_dump({"discogs": {"user_token": token}}),
            encoding="utf-8",
        )
        self.main_config = self.beets_dir / "config.yaml"
        self._write_main_config()
        self.runtime_config = self.contract_dir / "runtime.ini"
        self._write_runtime_config()
        self._seal(role)

    def _write_main_config(self, **overrides: object) -> None:
        config: dict[str, object] = {
            "library": str(self.library_db),
            "directory": str(self.library_root),
            "statefile": str(self.state_file),
            "include": [str(self.secret_include)],
            "plugins": list(BASELINE_PLUGINS),
            "import": {
                "autotag": True,
                "move": True,
                "write": True,
                "duplicate_keys": {
                    "album": ["mb_albumid", "discogs_albumid"],
                },
            },
            "paths": {"default": SAFE_DEFAULT_PATH, "comp": SAFE_COMP_PATH},
            "album_fields": {"path_disambig": SAFE_PATH_DISAMBIG},
            "permissions": {"file": "0664", "dir": "02775"},
            "musicbrainz": {"host": "musicbrainz.org", "https": True},
            "convert": {"auto": False, "auto_keep": False},
            # Baseline world stays warning-free: rank cover_art_url ahead of
            # itunes (#1200) so tests that assert a clean default report
            # aren't tripped by the new fetchart_cover_art_url_ranked_after_
            # itunes warning. Tests exercising that warning override this key
            # explicitly (absent, or reordered).
            "fetchart": {
                "auto": True,
                "sources": list(FETCHART_IDENTITY_FIRST_SOURCES),
            },
        }
        config.update(overrides)
        self.main_config.write_text(yaml.safe_dump(config), encoding="utf-8")

    def _write_runtime_config(
        self,
        *,
        musicbrainz_api_base: str = "https://musicbrainz.org",
        **overrides: str,
    ) -> None:
        values = {
            "config_dir": str(self.beets_dir),
            "library": str(self.library_db),
            "directory": str(self.library_root),
            "state_file": str(self.state_file),
            "python": sys.executable,
            "secret_include": str(self.secret_include),
        }
        values.update(overrides)
        parser = configparser.RawConfigParser()
        parser["Beets"] = values
        parser["MusicBrainz"] = {"api_base": musicbrainz_api_base}
        # Startup write-probe required paths (issue #1085) -- fixed, real,
        # writable-by-default directories every entrypoint's `main()` now
        # probes before its first application effect. Not part of the
        # `**overrides` kwarg mechanism: no existing caller overrides these,
        # and dedicated startup-write-probe tests break the real directories
        # directly rather than repointing the config at missing ones.
        parser["Slskd"] = {"download_dir": str(self.slskd_download_dir)}
        parser["Paths"] = {"processing_dir": str(self.processing_dir)}
        parser["Beets Validation"] = {
            "staging_dir": str(self.beets_staging_dir),
        }
        with self.runtime_config.open("w", encoding="utf-8") as handle:
            parser.write(handle)

    def _seal(self, role: str) -> None:
        for path in (self.runtime_config, self.main_config):
            if path.exists():
                path.chmod(stat.S_IRUSR | stat.S_IRGRP)
        for path in (self.contract_dir, self.beets_dir, self.state_dir):
            path.chmod(
                stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
            )
        if self.secret_include.exists():
            self.secret_include.chmod(stat.S_IRUSR | stat.S_IRGRP)
        self.secret_dir.chmod(
            stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
        )
        self.authority_root.chmod(
            stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
        )
        self.state_file.chmod(
            stat.S_IRUSR
            | stat.S_IWUSR
            | stat.S_IRGRP
            | (stat.S_IWGRP if role == "importer" else 0)
        )
        self._chown_authority("1:0")

    def unseal(self) -> None:
        self._chown_authority("0:0")
        self.authority_root.chmod(stat.S_IRWXU)
        for path in (
            self.contract_dir,
            self.beets_dir,
            self.secret_dir,
            self.state_dir,
        ):
            path.chmod(stat.S_IRWXU)
        for path in (self.runtime_config, self.main_config, self.secret_include):
            if path.exists():
                path.chmod(stat.S_IRUSR | stat.S_IWUSR)

    def _chown_authority(self, owner: str) -> None:
        self._chown_path(self.authority_root, owner, recursive=True)

    def _chown_path(self, path: Path, owner: str, *, recursive: bool = False) -> None:
        recursive_flag = ["-R"] if recursive else []
        subprocess.run(
            [
                "unshare", "--map-root-user", "--map-auto", "chown",
                *recursive_flag, owner, str(path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

    def set_state_mode(self, mode: int) -> None:
        """Change the external state mode through its owning user namespace."""
        self.set_authority_mode(self.state_file, mode)

    def set_authority_mode(self, path: Path, mode: int) -> None:
        """Change a sealed authority entry through its owning user namespace."""
        subprocess.run(
            [
                "unshare", "--map-root-user", "--map-auto", "chmod",
                f"{mode:o}", str(path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

    def make_state_leaf_app_owned(self, mode: int) -> None:
        """Give only the state leaf to this process under external ancestors."""
        self._chown_path(self.state_file, "0:0")
        self.state_file.chmod(mode)
        if self.state_file.stat().st_uid != os.geteuid():
            raise AssertionError("state fixture leaf is not application-owned")
        if self.state_dir.stat().st_uid == os.geteuid():
            raise AssertionError("state fixture ancestor must remain externally owned")

    def alias_state_to_library(self, kind: str) -> None:
        """Make the declared state and catalog paths name one inode."""
        self.unseal()
        if kind == "same_path":
            self.state_file = self.library_db
        elif kind == "hardlink":
            external_library = self.state_dir / "library.db"
            external_library.write_bytes(self.library_db.read_bytes())
            self.state_file.unlink()
            os.link(external_library, self.state_file)
            self.library_db = external_library
        else:
            raise AssertionError(f"unknown state/library alias kind: {kind}")
        self._write_runtime_config(
            library=str(self.library_db),
            state_file=str(self.state_file),
        )
        self._write_main_config(
            library=str(self.library_db),
            statefile=str(self.state_file),
        )
        self._seal("importer")

    def cfg(self):
        return read_runtime_config_strict(str(self.runtime_config), str(self.runtime_dir))

    def put_authority_behind_writable_ancestor(self, kind: str) -> str:
        """Plant a readonly leaf below a replaceable ordinary ancestor."""
        self.unseal()
        attacker = self.root / f"writable-ancestor-{kind}"
        sealed = attacker / "sealed"
        sealed.mkdir(parents=True)
        attacker.chmod(stat.S_IRWXU)

        if kind == "runtime":
            candidate = sealed / "runtime.ini"
            candidate.write_text(
                self.runtime_config.read_text(encoding="utf-8"), encoding="utf-8"
            )
            self.runtime_config = candidate
            expected = "mutable_runtime_config"
        elif kind == "main":
            candidate = sealed / "config.yaml"
            candidate.write_text(
                self.main_config.read_text(encoding="utf-8"), encoding="utf-8"
            )
            self._write_runtime_config(config_dir=str(sealed))
            expected = "mutable_main_config"
        elif kind == "include":
            candidate = sealed / "extra.yaml"
            candidate.write_text("fetchart:\n  auto: true\n", encoding="utf-8")
            self._write_main_config(
                include=[str(candidate), str(self.secret_include)]
            )
            expected = "mutable_include"
        elif kind == "secret":
            candidate = sealed / "discogs.yaml"
            candidate.write_text(
                self.secret_include.read_text(encoding="utf-8"), encoding="utf-8"
            )
            self._write_main_config(include=[str(candidate)])
            self._write_runtime_config(secret_include=str(candidate))
            expected = "mutable_secret_include"
        else:
            raise AssertionError(f"unknown declared authority kind: {kind}")

        candidate.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        sealed.chmod(
            stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
            | stat.S_IROTH | stat.S_IXOTH
        )
        self._seal("importer")
        return expected

    def put_app_owned_readonly_authority(
        self,
        kind: str,
        *,
        component: str,
    ) -> str:
        """Transfer one readonly declared leaf or ancestor to the app UID."""
        self.unseal()
        if kind == "runtime":
            candidate = self.runtime_config
            expected = "mutable_runtime_config"
        elif kind == "main":
            candidate = self.main_config
            expected = "mutable_main_config"
        elif kind == "include":
            include_dir = self.authority_root / "nonsecret"
            include_dir.mkdir()
            candidate = include_dir / "extra.yaml"
            candidate.write_text("fetchart:\n  auto: true\n", encoding="utf-8")
            candidate.chmod(stat.S_IRUSR | stat.S_IRGRP)
            include_dir.chmod(
                stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
            )
            self._write_main_config(
                include=[str(candidate), str(self.secret_include)]
            )
            expected = "mutable_include"
        elif kind == "secret":
            candidate = self.secret_include
            expected = "mutable_secret_include"
        else:
            raise AssertionError(f"unknown declared authority kind: {kind}")

        self._seal("importer")
        owned = candidate if component == "leaf" else candidate.parent
        self._chown_path(owned, "0:0")
        self.assert_readonly(owned)
        return expected

    @staticmethod
    def assert_readonly(path: Path) -> None:
        if path.stat().st_mode & stat.S_IWUSR:
            raise AssertionError(f"expected readonly fixture component: {path}")

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.unseal()
            self.state_file.chmod(stat.S_IRUSR | stat.S_IWUSR)
            for current, directories, files in os.walk(self.root):
                Path(current).chmod(stat.S_IRWXU)
                for name in files:
                    child = Path(current, name)
                    if not child.is_symlink():
                        child.chmod(stat.S_IRUSR | stat.S_IWUSR)
                for name in directories:
                    child = Path(current, name)
                    if not child.is_symlink():
                        child.chmod(stat.S_IRWXU)
        finally:
            self._authority_tmp.cleanup()
            self._tmp.cleanup()
