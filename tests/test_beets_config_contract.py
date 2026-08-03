"""Deterministic contract tests for the external Beets authority."""

from __future__ import annotations

import errno
import os
import stat
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

import msgspec
import yaml

from lib.beets_config_contract import BeetsConfigError, check_beets_config
from tests.fakes.beets_contract import (
    RUNTIME_AUTHORITIES,
    SAFE_COMP_PATH,
    SAFE_DEFAULT_PATH,
    SAFE_SINGLETON_PATH,
    BeetsContractWorld,
    assert_app_owned_root_anchor_is_rejected,
)


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

    def test_inconclusive_write_probe_os_errors_fail_startup_closed(self):
        for error_number in (errno.EIO, errno.ESTALE):
            with self.subTest(error_number=error_number), mock.patch(
                "os.open",
                side_effect=OSError(error_number, os.strerror(error_number)),
            ), self.assertRaisesRegex(
                BeetsConfigError,
                "cannot determine whether Beets authority is writable",
            ):
                check_beets_config(self.world.cfg(), role="importer")

    def test_readonly_filesystem_write_probe_proves_state_nonwritability(self):
        with mock.patch(
            "os.open",
            side_effect=OSError(errno.EROFS, os.strerror(errno.EROFS)),
        ):
            report = check_beets_config(self.world.cfg(), role="importer")

        self.assertIn(
            "state_not_writable_by_importer",
            [finding.code for finding in report.hard_failures],
        )

    def test_invalid_designated_discogs_token_values_are_missing(self):
        invalid_tokens = ("", " \t\n", None, False, True, 0, 42, [], {})
        for token in invalid_tokens:
            with self.subTest(token=repr(token)):
                self.world.unseal()
                self.world.secret_include.write_text(
                    yaml.safe_dump({"discogs": {"user_token": token}}),
                    encoding="utf-8",
                )
                self.world._seal("importer")

                report = check_beets_config(self.world.cfg(), role="importer")

                self.assertIn(
                    "discogs_token_missing",
                    [finding.code for finding in report.hard_failures],
                )

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

    def test_authority_symlinks_resolving_to_root_fail_before_config_load(self):
        root_alias = self.world.root / "root-alias"
        root_alias.symlink_to("/")
        cfg = self.world.cfg()
        for field, value, code in (
            ("beets_config_dir", str(root_alias), "config_dir_root"),
            (
                "beets_library_db",
                str(root_alias / "beets-library.db"),
                "library_parent_root",
            ),
            ("beets_directory", str(root_alias), "directory_root"),
            ("beets_state_file", str(root_alias), "state_root"),
        ):
            with self.subTest(field=field):
                report = check_beets_config(
                    replace(cfg, **{field: value}),
                    role="importer",
                )
                self.assertIn(
                    code,
                    [finding.code for finding in report.hard_failures],
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

    def test_malformed_yaml_remains_a_contract_load_failure(self):
        token = "PLANTED_CONTRACT_TOKEN_759"
        self.world.unseal()
        self.world.secret_include.write_text(
            f"discogs:\n  user_token: [{token}\n",
            encoding="utf-8",
        )
        self.world._seal("importer")
        with self.assertRaises(BeetsConfigError) as caught:
            check_beets_config(self.world.cfg(), role="importer")
        self.assertIn("ParserError", str(caught.exception))
        self.assertNotIn(token, str(caught.exception))
        self.assertNotIn("while parsing", str(caught.exception))

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
