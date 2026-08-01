"""Deterministic contract tests for the external Beets authority."""

from __future__ import annotations

import configparser
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import msgspec
import yaml

from lib.beets_config_contract import BeetsConfigError, check_beets_config
from lib.config import read_runtime_config_strict
from scripts.check_beets_config import CheckerResult

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
            cwd=Path(__file__).resolve().parent.parent,
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
    """One real file/config world; filesystem permissions are the capability."""

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
        for path in (
            self.contract_dir,
            self.runtime_dir,
            self.beets_dir,
            self.secret_dir,
            self.state_dir,
            self.library_root,
        ):
            path.mkdir()
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
            "plugins": [
                "musicbrainz", "discogs", "inline", "permissions", "fetchart",
            ],
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
            "fetchart": {"auto": True},
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


class TestBeetsConfigContract(unittest.TestCase):
    def setUp(self) -> None:
        self.world = BeetsContractWorld()
        self.addCleanup(self.world.close)

    def test_safe_effective_config_passes_without_touching_state(self):
        before = self.world.state_file.read_bytes()

        report = check_beets_config(self.world.cfg(), role="importer")

        self.assertTrue(report.ok, report.hard_failures)
        self.assertEqual(report.hard_failures, ())
        self.assertEqual(report.warnings, ())
        self.assertEqual(self.world.state_file.read_bytes(), before)
        self.assertEqual(report.authority.config_dir, str(self.world.beets_dir))
        self.assertEqual(report.authority.state_file, str(self.world.state_file))
        self.assertNotEqual(self.world.state_dir.stat().st_uid, os.geteuid())
        self.assertFalse(self.world.state_dir.stat().st_mode & stat.S_IWGRP)
        self.assertTrue(self.world.state_file.stat().st_mode & stat.S_IWGRP)

    def test_each_runtime_authority_is_required_independently(self):
        cfg = self.world.cfg()
        for field in RUNTIME_AUTHORITIES:
            with self.subTest(field=field):
                report = check_beets_config(
                    replace(cfg, **{field: ""}),
                    role="importer",
                )
                self.assertEqual(
                    [finding.code for finding in report.hard_failures],
                    ["runtime_authority_missing"],
                )

    def test_missing_main_config_is_a_native_contract_load_failure(self):
        self.world.unseal()
        self.world.main_config.unlink()
        self.world._seal("importer")

        with self.assertRaisesRegex(BeetsConfigError, "config.yaml"):
            check_beets_config(self.world.cfg(), role="importer")

    def test_unreadable_runtime_and_main_config_fail_before_effective_loading(self):
        for authority in ("runtime", "main"):
            with self.subTest(authority=authority):
                world = BeetsContractWorld()
                try:
                    target = (
                        world.runtime_config
                        if authority == "runtime"
                        else world.main_config
                    )
                    world.set_authority_mode(target, 0)
                    if authority == "runtime":
                        with self.assertRaises(PermissionError):
                            world.cfg()
                    else:
                        with self.assertRaisesRegex(BeetsConfigError, "config.yaml"):
                            check_beets_config(world.cfg(), role="importer")
                finally:
                    world.close()

    def test_malformed_and_nonmapping_main_config_are_native_load_failures(self):
        for raw in ("broken: [\n", "- not-a-mapping\n"):
            with self.subTest(raw=raw):
                world = BeetsContractWorld()
                try:
                    world.unseal()
                    world.main_config.write_text(raw, encoding="utf-8")
                    world._seal("importer")
                    with self.assertRaises(BeetsConfigError):
                        check_beets_config(world.cfg(), role="importer")
                finally:
                    world.close()

    def test_unreadable_designated_secret_is_a_native_load_failure(self):
        self.world.set_authority_mode(self.world.secret_include, 0)
        with self.assertRaisesRegex(BeetsConfigError, "discogs.yaml"):
            check_beets_config(self.world.cfg(), role="importer")

    def test_sticky_scratch_trust_depends_on_authority_entry_owner(self):
        self.assertNotEqual(os.geteuid(), 0)
        self.assertNotEqual(self.world.authority_root.lstat().st_uid, os.geteuid())
        admitted = check_beets_config(self.world.cfg(), role="importer")
        self.assertTrue(admitted.ok, admitted.hard_failures)

        # Keep the exact readonly modes but transfer the /dev/shm child from
        # the subordinate owner to the application identity. Sticky /dev/shm
        # then permits that identity to replace the whole authority entry.
        self.world._chown_authority("0:0")
        self.assertEqual(self.world.authority_root.lstat().st_uid, os.geteuid())
        rejected = check_beets_config(self.world.cfg(), role="importer")
        self.assertIn(
            "mutable_runtime_config",
            [finding.code for finding in rejected.hard_failures],
        )

    def test_app_owned_filesystem_root_invalidates_readonly_authority(self):
        assert_app_owned_root_anchor_is_rejected(depth=1)

    def test_app_owned_readonly_declared_files_and_ancestors_are_rejected(self):
        for kind in ("runtime", "main", "include", "secret"):
            for component in ("leaf", "ancestor"):
                with self.subTest(kind=kind, component=component):
                    world = BeetsContractWorld()
                    try:
                        expected = world.put_app_owned_readonly_authority(
                            kind,
                            component=component,
                        )
                        report = check_beets_config(world.cfg(), role="importer")
                        self.assertIn(
                            expected,
                            [finding.code for finding in report.hard_failures],
                        )
                    finally:
                        world.close()

    def test_report_and_fingerprint_never_contain_token(self):
        token = "never-print-this-token"
        self.world.close()
        self.world = BeetsContractWorld(token=token)
        self.addCleanup(self.world.close)

        report = check_beets_config(self.world.cfg(), role="importer")
        encoded = msgspec.json.encode(report).decode()

        self.assertTrue(report.ok, report.hard_failures)
        self.assertNotIn(token, encoded)
        self.assertNotIn(token, report.fingerprint)

    def test_secret_overlay_is_rejected_before_effective_authority_validation(self):
        self.world.unseal()
        self.world.secret_include.write_text(
            "discogs:\n  user_token: safe\nlibrary: /attacker/library.db\n",
            encoding="utf-8",
        )
        self.world._seal("importer")

        report = check_beets_config(self.world.cfg(), role="importer")

        self.assertFalse(report.ok)
        self.assertEqual([f.code for f in report.hard_failures], ["secret_schema"])

    def test_designated_secret_include_must_appear_exactly_once(self):
        for includes in ([], [str(self.world.secret_include)] * 2):
            with self.subTest(includes=includes):
                self.world.unseal()
                self.world._write_main_config(include=includes)
                self.world._seal("importer")
                report = check_beets_config(self.world.cfg(), role="importer")
                self.assertIn("secret_include_count", [f.code for f in report.hard_failures])

    def test_non_importer_must_not_have_state_write_access(self):
        report = check_beets_config(self.world.cfg(), role="web")
        self.assertIn("state_writable_by_reader", [f.code for f in report.hard_failures])

    def test_musicbrainz_endpoint_drift_warns_without_failing(self):
        self.world.unseal()
        self.world._write_main_config(
            musicbrainz={"host": "mirror.invalid", "https": True}
        )
        self.world._seal("importer")

        report = check_beets_config(self.world.cfg(), role="importer")

        self.assertTrue(report.ok, report.hard_failures)
        self.assertEqual([warning.code for warning in report.warnings], ["musicbrainz_endpoint_drift"])

    def test_malformed_musicbrainz_runtime_url_is_endpoint_drift_warning(self):
        self.world.unseal()
        self.world._write_runtime_config(musicbrainz_api_base="https://[")
        self.world._seal("importer")

        report = check_beets_config(self.world.cfg(), role="importer")

        self.assertTrue(report.ok, report.hard_failures)
        self.assertEqual(
            [warning.code for warning in report.warnings],
            ["musicbrainz_endpoint_drift"],
        )

    def test_declared_secret_duplicate_key_is_rejected(self):
        self.world.unseal()
        self.world.secret_include.write_text(
            "discogs:\n  user_token: first\n  user_token: second\n",
            encoding="utf-8",
        )
        self.world._seal("importer")

        report = check_beets_config(self.world.cfg(), role="importer")

        self.assertFalse(report.ok)
        self.assertEqual([finding.code for finding in report.hard_failures], ["secret_duplicate_key"])

    def test_unhashable_secret_yaml_key_is_a_stable_load_failure(self):
        self.world.unseal()
        self.world.secret_include.write_text(
            "? [unhashable, key]\n: value\n",
            encoding="utf-8",
        )
        self.world._seal("importer")

        with self.assertRaisesRegex(BeetsConfigError, "unhashable YAML mapping key"):
            check_beets_config(self.world.cfg(), role="importer")

    def test_discogs_token_may_only_be_declared_by_designated_secret(self):
        for source in ("main", "nonsecret_include"):
            with self.subTest(source=source):
                world = BeetsContractWorld()
                try:
                    world.unseal()
                    if source == "main":
                        world._write_main_config(
                            discogs={"user_token": "outside-designated-secret"}
                        )
                    else:
                        extra = world.beets_dir / "nonsecret-token.yaml"
                        extra.write_text(
                            "discogs:\n  user_token: outside-designated-secret\n",
                            encoding="utf-8",
                        )
                        world._write_main_config(
                            include=[str(extra), str(world.secret_include)]
                        )
                    world._seal("importer")

                    report = check_beets_config(world.cfg(), role="importer")

                    self.assertIn(
                        "discogs_token_outside_secret_include",
                        [finding.code for finding in report.hard_failures],
                    )
                finally:
                    world.close()

    def test_every_named_hard_setting_is_rejected(self):
        unsafe_cases: tuple[tuple[str, dict[str, object], str], ...] = (
            (
                "missing musicbrainz",
                {"plugins": ["discogs", "inline", "permissions"]},
                "musicbrainz_plugin_missing",
            ),
            (
                "unavailable configured plugin",
                {"plugins": ["musicbrainz", "discogs", "inline", "permissions", "not_a_plugin"]},
                "plugin_unavailable",
            ),
            (
                "missing permissions",
                {"plugins": ["musicbrainz", "discogs", "inline"]},
                "permissions_plugin_missing",
            ),
            (
                "missing inline",
                {"plugins": ["musicbrainz", "discogs", "permissions"]},
                "inline_plugin_missing",
            ),
            (
                "wrong duplicate keys",
                {"import": {"autotag": True, "move": True, "write": True,
                            "duplicate_keys": {"album": ["album", "mb_albumid"]}}},
                "duplicate_keys_unsafe",
            ),
            (
                "autotag disabled",
                {"import": {"autotag": False, "move": True, "write": True,
                            "duplicate_keys": {"album": ["mb_albumid", "discogs_albumid"]}}},
                "import_autotag_disabled",
            ),
            (
                "move disabled",
                {"import": {"autotag": True, "move": False, "write": True,
                            "duplicate_keys": {"album": ["mb_albumid", "discogs_albumid"]}}},
                "import_move_disabled",
            ),
            (
                "write disabled",
                {"import": {"autotag": True, "move": True, "write": False,
                            "duplicate_keys": {"album": ["mb_albumid", "discogs_albumid"]}}},
                "import_write_disabled",
            ),
            ("unsafe default path", {"paths": {"default": "$album", "comp": SAFE_COMP_PATH}}, "default_path_unsafe"),
            (
                "unsafe singleton path",
                {"paths": {
                    "default": SAFE_DEFAULT_PATH,
                    "singleton": "$title",
                    "comp": SAFE_COMP_PATH,
                }},
                "singleton_path_unsafe",
            ),
            ("unsafe comp path", {"paths": {"default": SAFE_DEFAULT_PATH, "comp": "$album"}}, "comp_path_unsafe"),
            (
                "query-specific path override",
                {"paths": {
                    "default": SAFE_DEFAULT_PATH,
                    "singleton": SAFE_SINGLETON_PATH,
                    "comp": SAFE_COMP_PATH,
                    "albumtype:soundtrack": "Soundtracks/$album",
                }},
                "paths_keys_unsupported",
            ),
            ("unsafe path field", {"album_fields": {"path_disambig": "label"}}, "path_disambig_unsafe"),
            ("unsafe file mode", {"permissions": {"file": "0644", "dir": "02775"}}, "permissions_file_unsafe"),
            ("unsafe dir mode", {"permissions": {"file": "0664", "dir": "0755"}}, "permissions_dir_unsafe"),
            (
                "convert auto",
                {"plugins": ["musicbrainz", "discogs", "inline", "permissions", "convert"],
                 "convert": {"auto": True, "auto_keep": False}},
                "convert_auto_conflict",
            ),
            (
                "convert auto keep",
                {"plugins": ["musicbrainz", "discogs", "inline", "permissions", "convert"],
                 "convert": {"auto": False, "auto_keep": True}},
                "convert_auto_keep_conflict",
            ),
            ("wrong library", {"library": str(self.world.root / "other.db")}, "library_mismatch"),
            ("wrong directory", {"directory": str(self.world.root / "other-library")}, "directory_mismatch"),
            ("wrong state", {"statefile": str(self.world.root / "other-state")}, "state_mismatch"),
        )
        for description, overrides, expected_code in unsafe_cases:
            with self.subTest(description=description):
                world = BeetsContractWorld()
                try:
                    world.unseal()
                    world._write_main_config(**overrides)
                    world._seal("importer")
                    report = check_beets_config(world.cfg(), role="importer")
                    self.assertIn(expected_code, [finding.code for finding in report.hard_failures])
                finally:
                    world.close()

    def test_package_level_musicbrainz_absence_is_rejected(self):
        available_without_musicbrainz = frozenset((
            "discogs",
            "inline",
            "permissions",
            "fetchart",
        ))
        report = check_beets_config(
            self.world.cfg(),
            role="importer",
            available_plugins=lambda: available_without_musicbrainz,
        )

        self.assertIn(
            "plugin_unavailable",
            [finding.code for finding in report.hard_failures],
        )

    def test_library_database_and_root_must_exist_with_exact_types(self):
        cases = (
            ("missing database", "library", "missing", "library_not_regular"),
            ("directory database", "library", "wrong", "library_not_regular"),
            ("empty database", "library", "empty", "library_schema_missing"),
            ("corrupt database", "library", "corrupt", "library_unreadable"),
            (
                "missing library root",
                "directory",
                "missing",
                "directory_not_directory",
            ),
            (
                "file library root",
                "directory",
                "wrong",
                "directory_not_directory",
            ),
        )
        for description, authority, mutation, expected in cases:
            with self.subTest(description=description):
                world = BeetsContractWorld()
                try:
                    target = (
                        world.library_db
                        if authority == "library"
                        else world.library_root
                    )
                    if mutation in ("missing", "wrong"):
                        if target.is_dir():
                            target.rmdir()
                        else:
                            target.unlink()
                    if mutation == "wrong":
                        if authority == "library":
                            target.mkdir()
                        else:
                            target.write_bytes(b"not a directory")
                    elif mutation == "empty":
                        target.write_bytes(b"")
                    elif mutation == "corrupt":
                        target.write_bytes(b"not sqlite")

                    report = check_beets_config(world.cfg(), role="importer")

                    self.assertIn(
                        expected,
                        [finding.code for finding in report.hard_failures],
                    )
                finally:
                    world.close()

    def test_reader_only_state_access_passes_for_non_importer(self):
        world = BeetsContractWorld(role="web")
        self.addCleanup(world.close)
        report = check_beets_config(world.cfg(), role="web")
        self.assertTrue(report.ok, report.hard_failures)
        self.assertFalse(world.state_file.stat().st_mode & stat.S_IWGRP)

    def test_importer_requires_write_access_without_changing_state(self):
        world = BeetsContractWorld(role="web")
        self.addCleanup(world.close)
        before = world.state_file.read_bytes()
        report = check_beets_config(world.cfg(), role="importer")
        self.assertIn("state_not_writable_by_importer", [f.code for f in report.hard_failures])
        self.assertEqual(world.state_file.read_bytes(), before)

    def test_scalar_include_is_rejected_as_beets_rejects_it(self):
        self.world.unseal()
        self.world._write_main_config(include=str(self.world.secret_include))
        self.world._seal("importer")
        report = check_beets_config(self.world.cfg(), role="importer")
        self.assertEqual([finding.code for finding in report.hard_failures], ["include_shape"])

    def test_relative_designated_include_resolves_against_beetsdir(self):
        self.world.unseal()
        relative = os.path.relpath(self.world.secret_include, self.world.beets_dir)
        self.world._write_main_config(include=[relative])
        self.world._seal("importer")
        report = check_beets_config(self.world.cfg(), role="importer")
        self.assertTrue(report.ok, report.hard_failures)

    def test_real_confuse_include_precedence_is_used(self):
        self.world.unseal()
        extra_dir = self.world.beets_dir / "extra"
        extra_dir.mkdir()
        extra = extra_dir / "override.yaml"
        extra.write_text("import:\n  write: false\n", encoding="utf-8")
        extra.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        extra_dir.chmod(stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        self.world._write_main_config(include=[str(extra), str(self.world.secret_include)])
        self.world._seal("importer")

        report = check_beets_config(self.world.cfg(), role="importer")

        self.assertIn("import_write_disabled", [finding.code for finding in report.hard_failures])

    def test_later_include_replaces_scalar_and_list_without_custom_merge(self):
        self.world.unseal()
        extra_dir = self.world.beets_dir / "extra-precedence"
        extra_dir.mkdir()
        first = extra_dir / "first.yaml"
        second = extra_dir / "second.yaml"
        first.write_text(
            "plugins: [not_a_plugin]\nimport:\n  write: false\n",
            encoding="utf-8",
        )
        second.write_text(
            "plugins: [musicbrainz, discogs, inline, permissions]\n"
            "import:\n  write: true\n",
            encoding="utf-8",
        )
        for path in (first, second):
            path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        extra_dir.chmod(
            stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
            | stat.S_IROTH | stat.S_IXOTH
        )
        self.world._write_main_config(
            include=[str(first), str(second), str(self.world.secret_include)]
        )
        self.world._seal("importer")

        report = check_beets_config(self.world.cfg(), role="importer")

        self.assertTrue(report.ok, report.hard_failures)
        self.assertTrue(report.plugin_contract.musicbrainz)

    def test_checker_never_instantiates_plugins(self):
        from beets import plugins as beets_plugins

        before = tuple(beets_plugins.find_plugins())
        report = check_beets_config(self.world.cfg(), role="importer")
        after = tuple(beets_plugins.find_plugins())
        self.assertTrue(report.ok, report.hard_failures)
        self.assertEqual(after, before)

    def test_nonempty_pluginpath_is_rejected_without_mutating_search_paths(self):
        import beetsplug

        self.world.unseal()
        path_token = "SECRET-pluginpath-TOKEN"
        plugin_token = "SECRET-plugin-name-TOKEN"
        plugin_dir = self.world.root / path_token
        plugin_dir.mkdir()
        plugin_file = plugin_dir / f"{plugin_token}.py"
        plugin_file.write_text("", encoding="utf-8")
        plugin_file.chmod(stat.S_IRUSR)
        plugin_dir.chmod(stat.S_IRUSR | stat.S_IXUSR)
        self.world._write_main_config(
            pluginpath=[str(plugin_dir)],
            plugins=[
                "musicbrainz", "discogs", "inline", "permissions",
                plugin_token,
            ],
        )
        self.world._seal("importer")
        path_before = tuple(beetsplug.__path__)
        sys_path_before = tuple(sys.path)

        encoded = msgspec.json.encode(
            check_beets_config(self.world.cfg(), role="importer")
        ).decode()

        self.assertIn("pluginpath_unsupported", encoded)
        self.assertNotIn(path_token, encoded)
        self.assertNotIn(plugin_token, encoded)
        self.assertEqual(tuple(beetsplug.__path__), path_before)
        self.assertEqual(tuple(sys.path), sys_path_before)

    def test_mutable_nonsecret_include_is_rejected(self):
        self.world.unseal()
        extra = self.world.root / "extra.yaml"
        extra.write_text("fetchart:\n  auto: true\n", encoding="utf-8")
        self.world._write_main_config(
            include=[str(extra), str(self.world.secret_include)]
        )
        self.world._seal("importer")
        # The include is writable even though the main file has been sealed.
        extra.chmod(stat.S_IRUSR | stat.S_IWUSR)
        report = check_beets_config(self.world.cfg(), role="importer")
        self.assertIn("mutable_include", [finding.code for finding in report.hard_failures])

    def test_python_authority_must_name_the_active_interpreter(self):
        invocation = Path(sys.executable)
        resolved = invocation.resolve()
        self.assertNotEqual(invocation, resolved)

        admitted = check_beets_config(self.world.cfg(), role="importer")
        self.assertTrue(admitted.ok, admitted.hard_failures)
        self.assertEqual(admitted.authority.python, sys.executable)

        normalized_spellings = (
            f"{invocation.parent}/./{invocation.name}",
            os.path.relpath(invocation, Path.cwd()),
        )
        for spelling in normalized_spellings:
            with self.subTest(admitted_spelling=spelling):
                report = check_beets_config(
                    replace(self.world.cfg(), beets_python=spelling),
                    role="importer",
                )
                self.assertTrue(report.ok, report.hard_failures)
                self.assertEqual(report.authority.python, sys.executable)

        alias = self.world.root / "python-alias"
        alias.symlink_to(invocation)
        for spelling in (str(resolved), str(alias)):
            with self.subTest(rejected_spelling=spelling):
                report = check_beets_config(
                    replace(self.world.cfg(), beets_python=spelling),
                    role="importer",
                )
                self.assertIn(
                    "python_mismatch",
                    [finding.code for finding in report.hard_failures],
                )

    def test_app_owned_python_alias_is_rejected_as_mutable(self):
        alias = self.world.root / "app-owned-python"
        alias.symlink_to(sys.executable)

        report = check_beets_config(
            replace(self.world.cfg(), beets_python=str(alias)),
            role="importer",
        )

        self.assertIn(
            "mutable_python",
            [finding.code for finding in report.hard_failures],
        )

    def test_reader_owned_readonly_state_is_rejected_but_importer_owned_passes(
        self,
    ) -> None:
        for role in ("main", "preview", "web"):
            with self.subTest(role=role):
                world = BeetsContractWorld(role=role)
                try:
                    world.make_state_leaf_app_owned(0o440)
                    before = world.state_file.read_bytes()

                    report = check_beets_config(world.cfg(), role=role)

                    codes = [finding.code for finding in report.hard_failures]
                    self.assertIn("state_owned_by_reader", codes)
                    self.assertNotIn("state_writable_by_reader", codes)
                    self.assertNotIn("state_replaceable", codes)
                    world.state_file.chmod(0o640)
                    fd = os.open(world.state_file, os.O_WRONLY)
                    os.close(fd)
                    self.assertEqual(world.state_file.read_bytes(), before)
                finally:
                    world.close()

        importer = BeetsContractWorld(role="importer")
        self.addCleanup(importer.close)
        importer.make_state_leaf_app_owned(0o640)

        report = check_beets_config(importer.cfg(), role="importer")

        self.assertTrue(report.ok, report.hard_failures)
        self.assertEqual(importer.state_file.stat().st_uid, os.geteuid())
        self.assertNotEqual(importer.state_dir.stat().st_uid, os.geteuid())

    def test_state_path_must_be_existing_regular_file_outside_beetsdir(self):
        cases = ("absent", "directory", "inside")
        for case in cases:
            with self.subTest(case=case):
                world = BeetsContractWorld()
                try:
                    world.unseal()
                    if case == "absent":
                        state = world.root / "absent-state"
                    elif case == "directory":
                        state = world.root / "state-directory"
                        state.mkdir()
                    else:
                        state = world.beets_dir / "state.pickle"
                        state.write_bytes(b"state")
                    world._write_runtime_config(state_file=str(state))
                    world._write_main_config(statefile=str(state))
                    world._seal("importer")
                    report = check_beets_config(world.cfg(), role="importer")
                    expected = "state_inside_config_dir" if case == "inside" else "state_not_regular"
                    self.assertIn(expected, [finding.code for finding in report.hard_failures])
                finally:
                    world.close()

    def test_state_file_must_not_alias_the_library_database(self):
        for kind in ("same_path", "hardlink"):
            with self.subTest(kind=kind):
                world = BeetsContractWorld()
                try:
                    world.alias_state_to_library(kind)
                    report = check_beets_config(world.cfg(), role="importer")
                    self.assertIn(
                        "state_library_alias",
                        [finding.code for finding in report.hard_failures],
                    )
                finally:
                    world.close()

    def test_malformed_authority_paths_are_stable_contract_load_failures(self):
        cfg = self.world.cfg()
        for value in ("~cratedigger-no-such-user-759/config", "invalid\x00path"):
            with self.subTest(value=value), self.assertRaises(BeetsConfigError):
                check_beets_config(
                    replace(cfg, beets_config_dir=value),
                    role="importer",
                )

    def test_state_path_identity_must_not_be_replaceable(self):
        for case in (
            "replaceable_parent",
            "app_owned_readonly_parent",
            "replaceable_symlink",
        ):
            with self.subTest(case=case):
                world = BeetsContractWorld()
                try:
                    world.unseal()
                    if case in ("replaceable_parent", "app_owned_readonly_parent"):
                        state_dir = world.root / f"{case}-state"
                        state_dir.mkdir()
                        state = state_dir / "state.pickle"
                        state.write_bytes(world.state_file.read_bytes())
                        if case == "app_owned_readonly_parent":
                            state_dir.chmod(
                                stat.S_IRUSR
                                | stat.S_IXUSR
                                | stat.S_IRGRP
                                | stat.S_IXGRP
                            )
                    else:
                        state = world.runtime_dir / "state-link.pickle"
                        state.symlink_to(world.state_file)
                    world._write_runtime_config(state_file=str(state))
                    world._write_main_config(statefile=str(state))
                    world._seal("importer")

                    report = check_beets_config(world.cfg(), role="importer")

                    self.assertIn(
                        "state_replaceable",
                        [finding.code for finding in report.hard_failures],
                    )
                finally:
                    world.close()

    def test_malformed_yaml_preserves_native_parser_diagnostic(self):
        self.world.unseal()
        self.world.secret_include.write_text("discogs: [\n", encoding="utf-8")
        self.world._seal("importer")
        with self.assertRaisesRegex(BeetsConfigError, "while parsing"):
            check_beets_config(self.world.cfg(), role="importer")

    def test_mutable_runtime_contract_is_rejected(self):
        self.world.unseal()
        report = check_beets_config(self.world.cfg(), role="importer")
        self.assertIn("mutable_runtime_config", [finding.code for finding in report.hard_failures])

    def test_mutable_main_config_is_rejected(self):
        cfg = self.world.cfg()
        self.world.unseal()
        self.world.beets_dir.chmod(stat.S_IRWXU)
        self.world.main_config.chmod(stat.S_IRUSR | stat.S_IWUSR)
        report = check_beets_config(cfg, role="importer")
        self.assertIn("mutable_main_config", [finding.code for finding in report.hard_failures])

    def test_secret_rotation_does_not_change_fingerprint(self):
        first = check_beets_config(self.world.cfg(), role="importer")
        self.world.unseal()
        self.world.secret_include.write_text(
            yaml.safe_dump({"discogs": {"user_token": "rotated-token"}}),
            encoding="utf-8",
        )
        self.world._seal("importer")
        second = check_beets_config(self.world.cfg(), role="importer")
        self.assertEqual(first.fingerprint, second.fingerprint)


class TestBeetsConfigIntegrationFindings(unittest.TestCase):
    def setUp(self) -> None:
        self.world = BeetsContractWorld()
        self.addCleanup(self.world.close)

    def test_included_file_cannot_redirect_beets_to_an_unchecked_source(self):
        self.world.unseal()
        immutable_dir = self.world.beets_dir / "immutable-includes"
        immutable_dir.mkdir()
        redirect = self.world.runtime_dir / "unchecked.yaml"
        redirect.write_text("import:\n  write: false\n", encoding="utf-8")
        first = immutable_dir / "first.yaml"
        first.write_text(f"include:\n  - {redirect}\n", encoding="utf-8")
        first.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        immutable_dir.chmod(
            stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
            | stat.S_IROTH | stat.S_IXOTH
        )
        self.world._write_main_config(include=[str(first), str(self.world.secret_include)])
        self.world._seal("importer")
        report = check_beets_config(self.world.cfg(), role="importer")
        self.assertEqual(
            [finding.code for finding in report.hard_failures],
            ["included_include_forbidden"],
        )

    def test_every_declared_file_rejects_a_writable_ordinary_ancestor(self):
        for kind in ("runtime", "main", "include", "secret"):
            with self.subTest(kind=kind):
                world = BeetsContractWorld()
                try:
                    expected = world.put_authority_behind_writable_ancestor(kind)
                    report = check_beets_config(world.cfg(), role="importer")
                    self.assertIn(
                        expected, [finding.code for finding in report.hard_failures]
                    )
                finally:
                    world.close()

    def test_active_plugins_follow_pinned_disabled_and_musicbrainz_semantics(self):
        cases: tuple[tuple[str, dict[str, object], bool, str | None], ...] = (
            ("disabled required", {"disabled_plugins": ["permissions"]}, False,
             "permissions_plugin_missing"),
            ("disabled convert", {
                "plugins": ["musicbrainz", "discogs", "inline", "permissions", "convert"],
                "disabled_plugins": ["convert"],
                "convert": {"auto": True, "auto_keep": True},
            }, True, None),
            ("mb enabled", {
                "plugins": ["discogs", "inline", "permissions"],
                "musicbrainz": {"enabled": True, "host": "musicbrainz.org", "https": True},
            }, True, None),
            ("mb false", {
                "musicbrainz": {"enabled": False, "host": "musicbrainz.org", "https": True},
            }, False, "musicbrainz_plugin_missing"),
        )
        for description, overrides, expected_ok, expected_code in cases:
            with self.subTest(description=description):
                world = BeetsContractWorld()
                try:
                    world.unseal()
                    world._write_main_config(**overrides)
                    world._seal("importer")
                    report = check_beets_config(world.cfg(), role="importer")
                    self.assertEqual(report.ok, expected_ok, report.hard_failures)
                    if expected_code is not None:
                        self.assertIn(expected_code, [f.code for f in report.hard_failures])
                finally:
                    world.close()

    def test_active_convert_with_omitted_options_uses_pinned_false_defaults(self):
        self.world.unseal()
        self.world._write_main_config(
            plugins=["musicbrainz", "discogs", "inline", "permissions", "convert"],
            convert={},
        )
        self.world._seal("importer")
        report = check_beets_config(self.world.cfg(), role="importer")
        self.assertTrue(report.ok, report.hard_failures)

    def test_effective_nonabsolute_statefile_is_rejected_before_expansion(self):
        values = (
            os.path.relpath(self.world.state_file, self.world.beets_dir),
            "~/state.pickle",
            "~root/state.pickle",
        )
        for value in values:
            with self.subTest(value=value):
                self.world.unseal()
                self.world._write_main_config(statefile=value)
                self.world._seal("importer")
                report = check_beets_config(self.world.cfg(), role="importer")
                self.assertIn(
                    "effective_state_relative",
                    [finding.code for finding in report.hard_failures],
                )

    def test_runtime_state_authority_must_also_be_absolute(self):
        self.world.unseal()
        self.world._write_runtime_config(state_file="state.pickle")
        self.world._seal("importer")
        report = check_beets_config(self.world.cfg(), role="importer")
        self.assertIn("state_relative", [f.code for f in report.hard_failures])

    def test_unreadable_statefile_is_rejected(self):
        self.world.set_state_mode(0)
        report = check_beets_config(self.world.cfg(), role="importer")
        self.assertIn("state_unreadable", [f.code for f in report.hard_failures])

    def test_mutable_designated_secret_is_rejected(self):
        self.world.unseal()
        self.world.secret_dir.chmod(stat.S_IRWXU)
        self.world.secret_include.chmod(stat.S_IRUSR | stat.S_IWUSR)
        report = check_beets_config(self.world.cfg(), role="importer")
        self.assertIn("mutable_secret_include", [f.code for f in report.hard_failures])

    def test_replaceable_runtime_config_symlink_is_rejected(self):
        self.world.unseal()
        target = self.world.contract_dir / "target.ini"
        self.world.runtime_config.replace(target)
        target.chmod(stat.S_IRUSR)
        self.world.contract_dir.chmod(stat.S_IRUSR | stat.S_IXUSR)
        link = self.world.runtime_dir / "runtime-link.ini"
        link.symlink_to(target)
        self.world.runtime_config = link
        report = check_beets_config(self.world.cfg(), role="importer")
        self.assertIn("mutable_runtime_config", [f.code for f in report.hard_failures])

    def test_replaceable_declared_secret_symlink_is_rejected(self):
        self.world.unseal()
        link = self.world.runtime_dir / "secret-link.yaml"
        link.symlink_to(self.world.secret_include)
        self.world._write_main_config(include=[str(link)])
        self.world._seal("importer")
        report = check_beets_config(self.world.cfg(), role="importer")
        self.assertIn("mutable_secret_include", [f.code for f in report.hard_failures])

    def test_owned_output_never_echoes_arbitrary_plugin_text(self):
        token = "SECRET::plugin-shaped-token::TOKEN"
        self.world.unseal()
        self.world._write_main_config(
            plugins=["musicbrainz", "discogs", "inline", "permissions", token]
        )
        self.world._seal("importer")
        encoded = msgspec.json.encode(
            check_beets_config(self.world.cfg(), role="importer")
        ).decode()
        self.assertNotIn(token, encoded)

    def test_missing_unreadable_and_malformed_nonsecret_includes_are_hard(self):
        for case in ("missing", "unreadable", "malformed"):
            with self.subTest(case=case):
                world = BeetsContractWorld()
                try:
                    world.unseal()
                    extra_dir = world.root / f"extra-{case}"
                    extra_dir.mkdir()
                    extra = extra_dir / "extra.yaml"
                    if case == "malformed":
                        extra.write_text("fetchart: [\n", encoding="utf-8")
                        extra.chmod(stat.S_IRUSR)
                    elif case == "unreadable":
                        extra.write_text("fetchart:\n  auto: true\n", encoding="utf-8")
                        extra.chmod(0)
                    extra_dir.chmod(stat.S_IRUSR | stat.S_IXUSR)
                    world._write_main_config(include=[str(extra), str(world.secret_include)])
                    world._seal("importer")
                    with self.assertRaises(BeetsConfigError):
                        check_beets_config(world.cfg(), role="importer")
                finally:
                    world.close()

    def test_designated_secret_rejects_nonmapping_empty_and_nested_extra(self):
        invalid_values = (
            "- token\n",
            "",
            "discogs:\n  user_token: safe\n  consumer_key: extra\n",
        )
        for value in invalid_values:
            with self.subTest(value=value):
                self.world.unseal()
                self.world.secret_include.write_text(value, encoding="utf-8")
                self.world._seal("importer")
                report = check_beets_config(self.world.cfg(), role="importer")
                self.assertIn("secret_schema", [f.code for f in report.hard_failures])

    def test_missing_designated_secret_is_a_load_failure(self):
        self.world.unseal()
        self.world.secret_include.unlink()
        self.world._seal("importer")
        with self.assertRaises(BeetsConfigError):
            check_beets_config(self.world.cfg(), role="importer")

    def test_both_exact_duplicate_key_orders_are_accepted(self):
        for keys in (
            ["mb_albumid", "discogs_albumid"],
            ["discogs_albumid", "mb_albumid"],
        ):
            with self.subTest(keys=keys):
                self.world.unseal()
                config = yaml.safe_load(self.world.main_config.read_text(encoding="utf-8"))
                config["import"]["duplicate_keys"]["album"] = keys
                self.world.main_config.write_text(yaml.safe_dump(config), encoding="utf-8")
                self.world._seal("importer")
                report = check_beets_config(self.world.cfg(), role="importer")
                self.assertTrue(report.ok, report.hard_failures)

    def test_duplicate_key_contract_rejects_extra_and_repeated_entries(self):
        unsafe_lists = (
            ["mb_albumid", "discogs_albumid", "mb_albumid"],
            ["mb_albumid", "mb_albumid"],
            ["discogs_albumid", "discogs_albumid"],
        )
        for keys in unsafe_lists:
            with self.subTest(keys=keys):
                world = BeetsContractWorld()
                try:
                    world.unseal()
                    config = yaml.safe_load(
                        world.main_config.read_text(encoding="utf-8")
                    )
                    config["import"]["duplicate_keys"]["album"] = keys
                    world.main_config.write_text(
                        yaml.safe_dump(config), encoding="utf-8"
                    )
                    world._seal("importer")
                    report = check_beets_config(world.cfg(), role="importer")
                    self.assertIn(
                        "duplicate_keys_unsafe",
                        [finding.code for finding in report.hard_failures],
                    )
                finally:
                    world.close()

    def test_later_include_cannot_blank_effective_discogs_token(self):
        self.world.unseal()
        extra_dir = self.world.beets_dir / "token-override"
        extra_dir.mkdir()
        override = extra_dir / "override.yaml"
        override.write_text("discogs:\n  user_token: '   '\n", encoding="utf-8")
        override.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        extra_dir.chmod(
            stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
            | stat.S_IROTH | stat.S_IXOTH
        )
        self.world._write_main_config(include=[str(self.world.secret_include), str(override)])
        self.world._seal("importer")
        report = check_beets_config(self.world.cfg(), role="importer")
        self.assertIn(
            "discogs_token_outside_secret_include",
            [f.code for f in report.hard_failures],
        )


class TestStandaloneBeetsConfigChecker(unittest.TestCase):
    def setUp(self) -> None:
        self.world = BeetsContractWorld()
        self.addCleanup(self.world.close)

    def _run(self, role: str = "importer") -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                sys.executable,
                "scripts/check_beets_config.py",
                "--config", str(self.world.runtime_config),
                "--runtime-dir", str(self.world.runtime_dir),
                "--role", role,
            ],
            cwd=Path(__file__).resolve().parent.parent,
            text=True,
            capture_output=True,
            check=False,
        )

    def _decode(self, proc: subprocess.CompletedProcess[str]) -> CheckerResult:
        return msgspec.json.decode(proc.stdout, type=CheckerResult)

    def test_machine_json_is_stable_and_success_has_no_side_effects(self):
        before = snapshot_contract_world(self.world)
        proc = self._run()
        payload = self._decode(proc)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(payload.ok)
        self.assertIsNotNone(payload.report)
        assert payload.report is not None
        self.assertTrue(payload.report.ok)
        self.assertIsNone(payload.error)
        self.assertEqual(proc.stderr, "")
        self.assertEqual(snapshot_contract_world(self.world), before)

    def test_hard_failure_is_json_on_stdout_and_human_on_stderr(self):
        proc = self._run(role="web")
        payload = self._decode(proc)
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse(payload.ok)
        self.assertIn("state_writable_by_reader", proc.stderr)

    def test_native_runtime_load_failure_keeps_json_machine_channel(self):
        self.world.unseal()
        self.world.runtime_config.write_text("[Beets\nbroken = true\n", encoding="utf-8")
        self.world._seal("importer")
        proc = self._run()
        payload = self._decode(proc)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(payload.error, "config_load_error")
        self.assertIn("Beets configuration load failed", proc.stderr)

    def test_runtime_value_error_keeps_json_machine_channel(self):
        self.world.unseal()
        with self.world.runtime_config.open("a", encoding="utf-8") as handle:
            handle.write(
                "\n[Search Settings]\n"
                "number_of_albums_to_grab = many\n"
            )
        self.world._seal("importer")

        proc = self._run()
        payload = self._decode(proc)

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(payload.error, "config_load_error")
        self.assertIn("invalid literal", proc.stderr)

    def test_malformed_authority_path_keeps_json_machine_channel(self):
        self.world.unseal()
        self.world._write_runtime_config(
            config_dir="~cratedigger-no-such-user-759/config",
        )
        self.world._seal("importer")

        proc = self._run()
        payload = self._decode(proc)

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(payload.error, "config_load_error")
        self.assertIn("Beets configuration load failed", proc.stderr)

    def test_unhashable_secret_key_has_stable_machine_output(self):
        self.world.unseal()
        self.world.secret_include.write_text(
            "? [unhashable, key]\n: value\n",
            encoding="utf-8",
        )
        self.world._seal("importer")

        proc = self._run()
        payload = self._decode(proc)

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(payload.error, "config_load_error")
        self.assertIn("unhashable YAML mapping key", proc.stderr)

    def test_checker_result_rejects_wrong_wire_types(self):
        with self.assertRaises(msgspec.ValidationError):
            msgspec.json.decode(
                b'{"ok":"false","report":null,"error":null}',
                type=CheckerResult,
            )


if __name__ == "__main__":
    unittest.main()
