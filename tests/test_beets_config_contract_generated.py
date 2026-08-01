"""Generated patrol for token-only Beets configuration authority."""

from __future__ import annotations

import configparser
import os
import stat
import sys
import unittest
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import msgspec
import yaml
from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_config_contract import (
    BeetsConfigError,
    BeetsRole,
    ContractFinding,
    check_beets_config,
)
from lib.config import CratediggerConfig, read_runtime_config_strict
from tests.test_beets_config_contract import (
    RUNTIME_AUTHORITIES,
    SAFE_COMP_PATH,
    SAFE_DEFAULT_PATH,
    SAFE_SINGLETON_PATH,
    BeetsContractWorld,
    assert_app_owned_root_anchor_is_rejected,
)


def assert_token_absent_from_owned_report(encoded_report: str, token: str) -> None:
    if token in encoded_report:
        raise AssertionError("valid Discogs token leaked into an owned report field")


def assert_hard_code(
    hard_failures: tuple[ContractFinding, ...], expected_code: str
) -> None:
    codes = [finding.code for finding in hard_failures]
    if expected_code not in codes:
        raise AssertionError(f"contract admitted known-bad mutant: {expected_code}")


def assert_loader_is_strict(loader) -> None:
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


_SETTING_MUTANTS: tuple[tuple[str, dict[str, object], str], ...] = (
    ("musicbrainz", {"plugins": ["discogs", "inline", "permissions"]},
     "musicbrainz_plugin_missing"),
    ("permissions-plugin", {"plugins": ["musicbrainz", "discogs", "inline"]},
     "permissions_plugin_missing"),
    ("inline-plugin", {"plugins": ["musicbrainz", "discogs", "permissions"]},
     "inline_plugin_missing"),
    ("autotag", {"import": {"autotag": False, "move": True, "write": True,
                              "duplicate_keys": {"album": ["mb_albumid", "discogs_albumid"]}}},
     "import_autotag_disabled"),
    ("move", {"import": {"autotag": True, "move": False, "write": True,
                           "duplicate_keys": {"album": ["mb_albumid", "discogs_albumid"]}}},
     "import_move_disabled"),
    ("write", {"import": {"autotag": True, "move": True, "write": False,
                            "duplicate_keys": {"album": ["mb_albumid", "discogs_albumid"]}}},
     "import_write_disabled"),
    ("duplicate", {"import": {"autotag": True, "move": True, "write": True,
                                "duplicate_keys": {"album": ["mb_albumid"]}}},
     "duplicate_keys_unsafe"),
    ("path", {"paths": {"default": "$album", "comp": "$album"}},
     "default_path_unsafe"),
    ("singleton-path", {"paths": {
        "default": SAFE_DEFAULT_PATH,
        "singleton": "$title",
        "comp": SAFE_COMP_PATH,
    }}, "singleton_path_unsafe"),
    ("comp-path", {"paths": {
        "default": "$albumartist/$year - $album%aunique{albumartist album,path_disambig}/$track $title",
        "comp": "$album",
    }}, "comp_path_unsafe"),
    ("field", {"album_fields": {"path_disambig": "label"}},
     "path_disambig_unsafe"),
    ("file-mode", {"permissions": {"file": "0644", "dir": "02775"}},
     "permissions_file_unsafe"),
    ("dir-mode", {"permissions": {"file": "0664", "dir": "0755"}},
     "permissions_dir_unsafe"),
    ("plugin", {"plugins": ["musicbrainz", "discogs", "inline", "permissions", "absent_plugin"]},
     "plugin_unavailable"),
    ("convert-auto", {
        "plugins": ["musicbrainz", "discogs", "inline", "permissions", "convert"],
        "convert": {"auto": True, "auto_keep": False},
    }, "convert_auto_conflict"),
    ("convert-auto-keep", {
        "plugins": ["musicbrainz", "discogs", "inline", "permissions", "convert"],
        "convert": {"auto": False, "auto_keep": True},
    }, "convert_auto_keep_conflict"),
)

_DECLARED_CORRUPTION_CASES: tuple[tuple[str, str], ...] = tuple(
    (authority, corruption)
    for authority in ("runtime", "main", "include", "secret")
    for corruption in ("missing", "unreadable", "malformed", "nonmapping")
)


def _corrupt_declared_authority(
    world: BeetsContractWorld,
    *,
    authority: str,
    corruption: str,
) -> None:
    """Plant one declared-file failure while retaining external ownership."""
    world.unseal()
    if authority == "runtime":
        target = world.runtime_config
    elif authority == "main":
        target = world.main_config
    elif authority == "include":
        target = world.beets_dir / "nonsecret.yaml"
        target.write_text("fetchart:\n  auto: true\n", encoding="utf-8")
        world._write_main_config(
            include=[str(target), str(world.secret_include)]
        )
    elif authority == "secret":
        target = world.secret_include
    else:
        raise AssertionError(f"unknown declared authority: {authority}")

    if corruption == "missing":
        target.unlink()
    elif corruption == "malformed":
        target.write_text(
            "[Beets\nbroken = true\n"
            if authority == "runtime"
            else "broken: [\n",
            encoding="utf-8",
        )
    elif corruption == "nonmapping":
        target.write_text(
            "[Other]\nvalue = present\n"
            if authority == "runtime"
            else "- not-a-mapping\n",
            encoding="utf-8",
        )
    elif corruption != "unreadable":
        raise AssertionError(f"unknown corruption: {corruption}")

    world._seal("importer")
    if corruption == "unreadable":
        world.set_authority_mode(target, 0)


class TestGeneratedDeclaredFileFailures(unittest.TestCase):
    @settings(max_examples=len(_DECLARED_CORRUPTION_CASES))
    @given(case=st.sampled_from(_DECLARED_CORRUPTION_CASES))
    def test_every_declared_file_corruption_fails_closed(
        self,
        case: tuple[str, str],
    ) -> None:
        authority, corruption = case
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        _corrupt_declared_authority(
            world,
            authority=authority,
            corruption=corruption,
        )

        if authority == "runtime":
            with self.assertRaises(
                (OSError, UnicodeError, configparser.Error, ValueError)
            ):
                world.cfg()
            return

        if authority == "secret" and corruption == "nonmapping":
            report = check_beets_config(world.cfg(), role="importer")
            assert_hard_code(report.hard_failures, "secret_schema")
            return

        with self.assertRaises(BeetsConfigError):
            check_beets_config(world.cfg(), role="importer")


class TestGeneratedTokenOnlySecret(unittest.TestCase):
    @example("SECRET::library: /attacker/library.db\nplugins: []::TOKEN")
    @example("SECRET::quoted: ' value\nwith:newline'::TOKEN")
    @settings(max_examples=40)
    @given(st.text(min_size=1, max_size=60).map(lambda value: f"SECRET::{value}::TOKEN"))
    def test_any_scalar_token_remains_data_and_never_report_content(self, token: str) -> None:
        world = BeetsContractWorld(token=token)
        self.addCleanup(world.close)

        report = check_beets_config(world.cfg(), role="importer")
        encoded = msgspec.json.encode(report).decode()

        self.assertTrue(report.ok, report.hard_failures)
        assert_token_absent_from_owned_report(encoded, token)

    @given(st.sampled_from(["library", "directory", "statefile", "import", "paths", "plugins"]))
    def test_any_extra_top_level_secret_authority_is_rejected(self, key: str) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world.secret_include.write_text(
            yaml.safe_dump({"discogs": {"user_token": "safe"}, key: {}}),
            encoding="utf-8",
        )
        world._seal("importer")

        report = check_beets_config(world.cfg(), role="importer")

        self.assertEqual([finding.code for finding in report.hard_failures], ["secret_schema"])

    @given(st.integers(min_value=0, max_value=3))
    def test_designated_secret_requires_exactly_one_occurrence(self, count: int) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(include=[str(world.secret_include)] * count)
        world._seal("importer")

        report = check_beets_config(world.cfg(), role="importer")

        if count == 1:
            self.assertTrue(report.ok, report.hard_failures)
        else:
            assert_hard_code(report.hard_failures, "secret_include_count")

    @given(st.sampled_from(("discogs.yaml", "", 0, False, True)))
    def test_every_scalar_include_shape_is_rejected(self, include: object) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(include=include)
        world._seal("importer")

        report = check_beets_config(world.cfg(), role="importer")

        assert_hard_code(report.hard_failures, "include_shape")

    @given(st.sampled_from(("top_level", "nested_token")))
    def test_duplicate_designated_secret_keys_are_always_rejected(
        self,
        duplicate_at: str,
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        if duplicate_at == "top_level":
            raw = (
                "discogs:\n  user_token: first\n"
                "discogs:\n  user_token: second\n"
            )
        else:
            raw = (
                "discogs:\n"
                "  user_token: first\n"
                "  user_token: second\n"
            )
        world.secret_include.write_text(raw, encoding="utf-8")
        world._seal("importer")

        report = check_beets_config(world.cfg(), role="importer")

        assert_hard_code(report.hard_failures, "secret_duplicate_key")

    @given(st.sampled_from((
        "? [discogs]\n: token\n",
        "? {discogs: token}\n: value\n",
    )))
    def test_unhashable_designated_secret_keys_are_native_load_errors(
        self,
        raw: str,
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world.secret_include.write_text(raw, encoding="utf-8")
        world._seal("importer")

        assert_native_config_rejection(
            lambda: check_beets_config(world.cfg(), role="importer")
        )


class TestGeneratedEffectiveSettings(unittest.TestCase):
    @settings(max_examples=5)
    @given(depth=st.integers(min_value=1, max_value=5))
    def test_app_owned_filesystem_root_rejects_every_authority_depth(
        self,
        depth: int,
    ) -> None:
        assert_app_owned_root_anchor_is_rejected(depth=depth)

    @given(
        field=st.sampled_from(RUNTIME_AUTHORITIES),
        role=st.sampled_from(("main", "importer", "preview", "web")),
    )
    def test_no_runtime_authority_can_be_omitted(
        self,
        field: str,
        role: BeetsRole,
    ) -> None:
        world = BeetsContractWorld(role=role)
        self.addCleanup(world.close)

        report = check_beets_config(
            replace(world.cfg(), **{field: ""}),
            role=role,
        )

        assert_hard_code(report.hard_failures, "runtime_authority_missing")

    @given(
        field=st.sampled_from(RUNTIME_AUTHORITIES),
        raw_case=st.sampled_from(("missing", "empty", "whitespace")),
    )
    def test_raw_runtime_authority_omission_never_reaches_normalization(
        self,
        field: str,
        raw_case: str,
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        ini_key = {
            "beets_config_dir": "config_dir",
            "beets_library_db": "library",
            "beets_directory": "directory",
            "beets_state_file": "state_file",
            "beets_python": "python",
            "beets_secret_include": "secret_include",
        }[field]
        world.unseal()
        if raw_case == "missing":
            parser = configparser.RawConfigParser()
            parser.read(world.runtime_config, encoding="utf-8")
            parser.remove_option("Beets", ini_key)
            with world.runtime_config.open("w", encoding="utf-8") as handle:
                parser.write(handle)
        else:
            blank = "" if raw_case == "empty" else " \t "
            world._write_runtime_config(**{ini_key: blank})
        world._seal("importer")

        assert_raw_authority_rejected(world.cfg)

    @given(st.sampled_from((
        ("python", "python_mismatch"),
        ("library", "library_mismatch"),
        ("directory", "directory_mismatch"),
        ("state", "state_mismatch"),
    )))
    def test_runtime_authority_must_equal_effective_beets_authority(
        self,
        case: tuple[str, str],
    ) -> None:
        authority, expected_code = case
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        cfg = world.cfg()
        if authority == "python":
            cfg = replace(cfg, beets_python=str(world.root / "other-python"))
        elif authority == "library":
            other_library = world.root / "other-library.db"
            other_library.write_bytes(world.library_db.read_bytes())
            cfg = replace(cfg, beets_library_db=str(other_library))
        elif authority == "directory":
            other_directory = world.root / "other-library-root"
            other_directory.mkdir()
            cfg = replace(cfg, beets_directory=str(other_directory))
        else:
            other_state = world.root / "other-state.pickle"
            other_state.write_bytes(b"other-state")
            cfg = replace(cfg, beets_state_file=str(other_state))

        report = check_beets_config(cfg, role="importer")

        assert_hard_code(report.hard_failures, expected_code)

    @given(
        role=st.sampled_from(("main", "preview", "web")),
        mode=st.sampled_from((0o400, 0o440, 0o444)),
    )
    def test_reader_cannot_own_even_a_readonly_state_leaf(
        self,
        role: BeetsRole,
        mode: int,
    ) -> None:
        world = BeetsContractWorld(role=role)
        self.addCleanup(world.close)
        world.make_state_leaf_app_owned(mode)
        before = world.state_file.read_bytes()

        report = check_beets_config(world.cfg(), role=role)

        assert_hard_code(report.hard_failures, "state_owned_by_reader")
        self.assertNotIn(
            "state_writable_by_reader",
            [finding.code for finding in report.hard_failures],
        )
        world.state_file.chmod(mode | stat.S_IWUSR)
        fd = os.open(world.state_file, os.O_WRONLY)
        os.close(fd)
        self.assertEqual(world.state_file.read_bytes(), before)

    @given(mode=st.sampled_from((0o600, 0o620, 0o640, 0o660)))
    def test_importer_may_own_its_writable_state_leaf(self, mode: int) -> None:
        world = BeetsContractWorld(role="importer")
        self.addCleanup(world.close)
        world.make_state_leaf_app_owned(mode)

        report = check_beets_config(world.cfg(), role="importer")

        self.assertTrue(report.ok, report.hard_failures)
        self.assertEqual(world.state_file.stat().st_uid, os.geteuid())
        self.assertNotEqual(world.state_dir.stat().st_uid, os.geteuid())

    @given(
        variant=st.sampled_from((
            "exact",
            "dot_segment",
            "relative",
            "resolved_target",
            "app_symlink",
            "nested_app_symlink",
        )),
    )
    def test_python_authority_is_the_normalized_invocation_entry(
        self,
        variant: str,
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        invocation = Path(sys.executable)
        resolved = invocation.resolve()
        self.assertNotEqual(invocation, resolved)

        if variant == "exact":
            spelling = sys.executable
            admitted = True
        elif variant == "dot_segment":
            spelling = f"{invocation.parent}/./{invocation.name}"
            admitted = True
        elif variant == "relative":
            spelling = os.path.relpath(invocation, Path.cwd())
            admitted = True
        elif variant == "resolved_target":
            spelling = str(resolved)
            admitted = False
        else:
            alias_parent = world.root
            if variant == "nested_app_symlink":
                alias_parent = world.root / "python-aliases"
                alias_parent.mkdir()
            alias = alias_parent / "python"
            alias.symlink_to(invocation)
            spelling = str(alias)
            admitted = False

        report = check_beets_config(
            replace(world.cfg(), beets_python=spelling),
            role="importer",
        )

        if admitted:
            self.assertTrue(report.ok, report.hard_failures)
            self.assertEqual(report.authority.python, sys.executable)
        else:
            assert_hard_code(report.hard_failures, "python_mismatch")
            if variant in {"app_symlink", "nested_app_symlink"}:
                assert_hard_code(report.hard_failures, "mutable_python")

    @given(kind=st.sampled_from(("same_path", "hardlink")))
    def test_state_and_library_must_never_share_an_inode(self, kind: str) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.alias_state_to_library(kind)

        report = check_beets_config(world.cfg(), role="importer")

        assert_hard_code(report.hard_failures, "state_library_alias")

    @given(
        field=st.sampled_from(RUNTIME_AUTHORITIES),
        value=st.sampled_from((
            "~cratedigger-no-such-user-759/authority",
            "invalid\x00authority",
        )),
    )
    def test_authority_path_resolution_failures_stay_inside_contract_boundary(
        self,
        field: str,
        value: str,
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)

        with self.assertRaises(BeetsConfigError):
            check_beets_config(
                replace(world.cfg(), **{field: value}),
                role="importer",
            )

    @given(
        kind=st.sampled_from(("runtime", "main", "include", "secret")),
        component=st.sampled_from(("leaf", "ancestor")),
    )
    def test_app_owned_declared_authority_is_replaceable_even_when_readonly(
        self,
        kind: str,
        component: str,
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        expected_code = world.put_app_owned_readonly_authority(
            kind,
            component=component,
        )

        report = check_beets_config(world.cfg(), role="importer")

        assert_hard_code(report.hard_failures, expected_code)

    @given(st.sampled_from((
        "replaceable_parent",
        "app_owned_readonly_parent",
        "replaceable_symlink",
    )))
    def test_state_identity_must_not_be_replaceable(self, mutation: str) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        if mutation in {"replaceable_parent", "app_owned_readonly_parent"}:
            state_dir = world.root / f"{mutation}-state"
            state_dir.mkdir()
            state = state_dir / "state.pickle"
            state.write_bytes(world.state_file.read_bytes())
            if mutation == "app_owned_readonly_parent":
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

        assert_hard_code(report.hard_failures, "state_replaceable")

    @given(st.sampled_from(("main", "nonsecret_include")))
    def test_only_designated_secret_include_may_supply_discogs_token(
        self,
        source: str,
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
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

        assert_hard_code(
            report.hard_failures,
            "discogs_token_outside_secret_include",
        )

    @given(st.sampled_from((
        ("absent", "state_not_regular"),
        ("nonregular", "state_not_regular"),
        ("inside_config", "state_inside_config_dir"),
        ("unreadable", "state_unreadable"),
        ("importer_readonly", "state_not_writable_by_importer"),
    )))
    def test_state_authority_must_be_safe_for_the_importer(
        self,
        case: tuple[str, str],
    ) -> None:
        mutation, expected_code = case
        world = BeetsContractWorld(
            role="web" if mutation == "importer_readonly" else "importer"
        )
        self.addCleanup(world.close)
        if mutation in {"absent", "nonregular", "inside_config"}:
            world.unseal()
            if mutation == "absent":
                state = world.state_dir / "absent-state.pickle"
            elif mutation == "nonregular":
                state = world.state_dir / "state-directory"
                state.mkdir()
            else:
                state = world.beets_dir / "state.pickle"
                state.write_bytes(b"state")
            world._write_runtime_config(state_file=str(state))
            world._write_main_config(statefile=str(state))
            world._seal("importer")
        elif mutation == "unreadable":
            world.set_state_mode(0)

        report = check_beets_config(world.cfg(), role="importer")

        assert_hard_code(report.hard_failures, expected_code)

    @given(st.sampled_from((
        "",
        "musicbrainz.org",
        "ftp://musicbrainz.org",
        "https://[",
        "://missing-scheme",
    )))
    def test_malformed_musicbrainz_authority_is_warning_only(
        self,
        expected_endpoint: str,
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        report = check_beets_config(
            replace(world.cfg(), musicbrainz_api_base=expected_endpoint),
            role="importer",
        )

        self.assertTrue(report.ok, report.hard_failures)
        assert_hard_code(report.warnings, "musicbrainz_endpoint_drift")

    @given(st.sampled_from(("main", "importer", "preview", "web")))
    def test_missing_main_config_never_reaches_effective_loading(
        self,
        role: BeetsRole,
    ) -> None:
        world = BeetsContractWorld(role=role)
        self.addCleanup(world.close)
        world.unseal()
        world.main_config.unlink()
        world._seal(role)

        with self.assertRaisesRegex(BeetsConfigError, "config.yaml"):
            check_beets_config(world.cfg(), role=role)

    @given(
        additionally_absent=st.sets(
            st.sampled_from(("discogs", "inline", "permissions", "fetchart")),
        )
    )
    def test_configured_musicbrainz_requires_package_level_availability(
        self,
        additionally_absent: set[str],
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        available_without_musicbrainz = frozenset((
            "discogs",
            "inline",
            "permissions",
            "fetchart",
        )) - additionally_absent
        report = check_beets_config(
            world.cfg(),
            role="importer",
            available_plugins=lambda: available_without_musicbrainz,
        )

        assert_hard_code(report.hard_failures, "plugin_unavailable")

    @given(
        keys=st.lists(
            st.sampled_from(("mb_albumid", "discogs_albumid")),
            min_size=0,
            max_size=5,
        ).filter(
            lambda values: not (
                len(values) == 2
                and set(values) == {"mb_albumid", "discogs_albumid"}
            )
        )
    )
    def test_only_two_distinct_exact_duplicate_keys_are_admitted(
        self,
        keys: list[str],
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        config = yaml.safe_load(world.main_config.read_text(encoding="utf-8"))
        config["import"]["duplicate_keys"]["album"] = keys
        world.main_config.write_text(yaml.safe_dump(config), encoding="utf-8")
        world._seal("importer")

        report = check_beets_config(world.cfg(), role="importer")

        assert_hard_code(report.hard_failures, "duplicate_keys_unsafe")

    @given(case=st.sampled_from((
        ("library", "missing", "library_not_regular"),
        ("library", "wrong_type", "library_not_regular"),
        ("library", "empty", "library_schema_missing"),
        ("library", "corrupt", "library_unreadable"),
        ("directory", "missing", "directory_not_directory"),
        ("directory", "wrong_type", "directory_not_directory"),
    )))
    def test_library_authority_requires_existing_exact_object_types(
        self,
        case: tuple[str, str, str],
    ) -> None:
        authority, object_kind, expected = case
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        target = (
            world.library_db if authority == "library" else world.library_root
        )
        if object_kind in ("missing", "wrong_type"):
            if target.is_dir():
                target.rmdir()
            else:
                target.unlink()
        if object_kind == "wrong_type":
            if authority == "library":
                target.mkdir()
            else:
                target.write_bytes(b"not a directory")
        elif object_kind == "empty":
            target.write_bytes(b"")
        elif object_kind == "corrupt":
            target.write_bytes(b"not sqlite")

        report = check_beets_config(world.cfg(), role="importer")

        assert_hard_code(
            report.hard_failures,
            expected,
        )

    @settings(max_examples=40)
    @given(
        query_key=st.text(
            alphabet="abcdefghijklmnopqrstuvwxyz_:",
            min_size=1,
            max_size=30,
        ).filter(
            lambda value: value not in {"default", "singleton", "comp"}
        ),
        template=st.text(min_size=0, max_size=40),
    )
    def test_query_specific_path_keys_are_never_admitted(
        self,
        query_key: str,
        template: str,
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(paths={
            "default": SAFE_DEFAULT_PATH,
            "singleton": SAFE_SINGLETON_PATH,
            "comp": SAFE_COMP_PATH,
            query_key: template,
        })
        world._seal("importer")

        report = check_beets_config(world.cfg(), role="importer")

        assert_hard_code(report.hard_failures, "paths_keys_unsupported")

    @given(st.sampled_from(_SETTING_MUTANTS))
    def test_every_generated_effective_mutant_is_rejected(
        self, mutant: tuple[str, dict[str, object], str]
    ) -> None:
        _name, overrides, expected_code = mutant
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(**overrides)
        world._seal("importer")

        report = check_beets_config(world.cfg(), role="importer")

        assert_hard_code(report.hard_failures, expected_code)

    _ROLES: tuple[BeetsRole, ...] = ("main", "preview", "web", "importer")

    @given(st.sampled_from(_ROLES))
    def test_each_role_gets_only_its_statefile_capability(self, role: BeetsRole) -> None:
        world = BeetsContractWorld(role=role)
        self.addCleanup(world.close)
        report = check_beets_config(world.cfg(), role=role)
        self.assertTrue(report.ok, report.hard_failures)

    @given(st.booleans())
    def test_endpoint_drift_is_warning_only(self, drifted: bool) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(
            musicbrainz={
                "host": "mirror.invalid" if drifted else "musicbrainz.org",
                "https": True,
            }
        )
        world._seal("importer")
        report = check_beets_config(world.cfg(), role="importer")
        self.assertTrue(report.ok, report.hard_failures)
        warning_codes = [warning.code for warning in report.warnings]
        self.assertEqual(warning_codes, ["musicbrainz_endpoint_drift"] if drifted else [])


class TestKnownBadContractCheckers(unittest.TestCase):
    def test_token_free_checker_rejects_planted_leak(self) -> None:
        with self.assertRaisesRegex(AssertionError, "token leaked"):
            assert_token_absent_from_owned_report(
                '{"fingerprint":"valid-token"}', "valid-token"
            )

    def test_permissive_runtime_loader_mutant_is_detected(self) -> None:
        assert_loader_is_strict(read_runtime_config_strict)
        with self.assertRaisesRegex(AssertionError, "missing contract"):
            assert_loader_is_strict(lambda _path, _runtime: CratediggerConfig())

    def test_native_config_error_checker_rejects_escape_and_admission(self) -> None:
        def escaped_error() -> None:
            raise TypeError("unhashable key escaped its contract boundary")

        with self.assertRaisesRegex(AssertionError, "non-contract error"):
            assert_native_config_rejection(escaped_error)
        with self.assertRaisesRegex(AssertionError, "admitted malformed"):
            assert_native_config_rejection(lambda: None)

    def test_raw_authority_checker_rejects_a_permissive_loader(self) -> None:
        with self.assertRaisesRegex(AssertionError, "admitted blank"):
            assert_raw_authority_rejected(lambda: CratediggerConfig())

    def test_broad_secret_overlay_mutant_is_detected(self) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world.secret_include.write_text(
            "discogs:\n  user_token: safe\nlibrary: /mutant.db\n",
            encoding="utf-8",
        )
        world._seal("importer")
        assert_hard_code(
            check_beets_config(world.cfg(), role="importer").hard_failures,
            "secret_schema",
        )

    def test_manual_merge_mutant_is_detected_by_real_confuse_view(self) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        extra_dir = world.beets_dir / "extra"
        extra_dir.mkdir()
        extra = extra_dir / "override.yaml"
        extra.write_text("import:\n  write: false\n", encoding="utf-8")
        extra.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        extra_dir.chmod(
            stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
            | stat.S_IROTH | stat.S_IXOTH
        )
        world._write_main_config(include=[str(extra), str(world.secret_include)])
        world._seal("importer")
        assert_hard_code(
            check_beets_config(world.cfg(), role="importer").hard_failures,
            "import_write_disabled",
        )

    def test_weak_path_mutant_is_detected(self) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(paths={"default": "$album", "comp": "$album"})
        world._seal("importer")
        assert_hard_code(
            check_beets_config(world.cfg(), role="importer").hard_failures,
            "default_path_unsafe",
        )

    def test_broad_state_access_mutant_is_detected(self) -> None:
        world = BeetsContractWorld(role="importer")
        self.addCleanup(world.close)
        assert_hard_code(
            check_beets_config(world.cfg(), role="web").hard_failures,
            "state_writable_by_reader",
        )

    def test_implicit_statefile_mutant_is_detected(self) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(statefile="state.pickle")
        world._seal("importer")
        assert_hard_code(
            check_beets_config(world.cfg(), role="importer").hard_failures,
            "state_mismatch",
        )

    def test_pluginpath_admission_mutant_is_detected(self) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        plugin_dir = world.root / "external-plugins"
        plugin_dir.mkdir()
        world._write_main_config(pluginpath=[str(plugin_dir)])
        world._seal("importer")
        assert_hard_code(
            check_beets_config(world.cfg(), role="importer").hard_failures,
            "pluginpath_unsupported",
        )
        with self.assertRaisesRegex(AssertionError, "known-bad mutant"):
            assert_hard_code((), "pluginpath_unsupported")

    def test_writable_ancestor_admission_mutant_is_detected(self) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        expected = world.put_authority_behind_writable_ancestor("runtime")
        assert_hard_code(
            check_beets_config(world.cfg(), role="importer").hard_failures,
            expected,
        )
        with self.assertRaisesRegex(AssertionError, "known-bad mutant"):
            assert_hard_code((), expected)

    def test_hard_code_checker_rejects_a_planted_admission(self) -> None:
        with self.assertRaisesRegex(AssertionError, "known-bad mutant"):
            assert_hard_code((), "secret_schema")


class TestGeneratedIntegrationRegressions(unittest.TestCase):
    @settings(max_examples=40)
    @given(st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=24))
    def test_any_nonempty_pluginpath_is_rejected_without_owned_output_leak(
        self, suffix: str
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        token = f"SECRET-pluginpath-{suffix}-TOKEN"
        plugin_dir = world.root / token
        plugin_dir.mkdir()
        world._write_main_config(pluginpath=[str(plugin_dir)])
        world._seal("importer")
        report = check_beets_config(world.cfg(), role="importer")
        assert_hard_code(report.hard_failures, "pluginpath_unsupported")
        assert_token_absent_from_owned_report(
            msgspec.json.encode(report).decode(), token
        )

    @given(st.sampled_from(("runtime", "main", "include", "secret")))
    def test_writable_ordinary_ancestors_are_always_rejected(
        self, kind: str
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        expected = world.put_authority_behind_writable_ancestor(kind)
        report = check_beets_config(world.cfg(), role="importer")
        assert_hard_code(report.hard_failures, expected)

    @given(st.sampled_from(("musicbrainz", "permissions", "inline", "convert")))
    def test_disabled_plugins_are_absent_from_the_active_contract(
        self, plugin: str
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(
            plugins=[
                "musicbrainz", "discogs", "inline", "permissions", "convert",
            ],
            disabled_plugins=[plugin],
            convert={"auto": True, "auto_keep": True},
        )
        world._seal("importer")
        report = check_beets_config(world.cfg(), role="importer")
        if plugin == "convert":
            self.assertNotIn(
                "convert_auto_conflict",
                [finding.code for finding in report.hard_failures],
            )
        else:
            assert_hard_code(report.hard_failures, f"{plugin}_plugin_missing")

    @settings(max_examples=40)
    @given(st.text(min_size=1, max_size=40).map(lambda text: f"SECRET::{text}::TOKEN"))
    def test_arbitrary_plugin_names_never_reach_owned_output(self, token: str) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(
            plugins=["musicbrainz", "discogs", "inline", "permissions", token]
        )
        world._seal("importer")
        encoded = msgspec.json.encode(
            check_beets_config(world.cfg(), role="importer")
        ).decode()
        assert_token_absent_from_owned_report(encoded, token)

    @given(st.sampled_from(("redirect.yaml", "nested.yaml", "another.yaml")))
    def test_included_include_redirects_are_always_rejected(self, name: str) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        immutable_dir = world.beets_dir / "redirect-source"
        immutable_dir.mkdir()
        included = immutable_dir / "included.yaml"
        included.write_text(f"include:\n  - {name}\n", encoding="utf-8")
        included.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        immutable_dir.chmod(
            stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP
            | stat.S_IROTH | stat.S_IXOTH
        )
        world._write_main_config(include=[str(included), str(world.secret_include)])
        world._seal("importer")
        report = check_beets_config(world.cfg(), role="importer")
        assert_hard_code(report.hard_failures, "included_include_forbidden")

    @given(st.sampled_from(("runtime", "secret")))
    def test_writable_lexical_symlink_parents_are_rejected(self, authority: str) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        if authority == "runtime":
            target = world.contract_dir / "target.ini"
            world.runtime_config.replace(target)
            target.chmod(stat.S_IRUSR)
            world.contract_dir.chmod(stat.S_IRUSR | stat.S_IXUSR)
            link = world.runtime_dir / "runtime-link.ini"
            link.symlink_to(target)
            world.runtime_config = link
            expected = "mutable_runtime_config"
        else:
            link = world.runtime_dir / "secret-link.yaml"
            link.symlink_to(world.secret_include)
            world._write_main_config(include=[str(link)])
            world._seal("importer")
            expected = "mutable_secret_include"
        report = check_beets_config(world.cfg(), role="importer")
        assert_hard_code(report.hard_failures, expected)

    @given(st.sampled_from((
        "../runtime/state.pickle", "state.pickle", "~/state.pickle",
        "~root/state.pickle",
    )))
    def test_all_effective_relative_statefiles_are_rejected(self, value: str) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(statefile=value)
        world._seal("importer")
        report = check_beets_config(world.cfg(), role="importer")
        assert_hard_code(report.hard_failures, "effective_state_relative")

    @given(st.one_of(st.none(), st.booleans()))
    def test_convert_defaults_false_but_explicit_true_conflicts(
        self, configured: bool | None
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        convert = {} if configured is None else {"auto": configured, "auto_keep": False}
        world._write_main_config(
            plugins=["musicbrainz", "discogs", "inline", "permissions", "convert"],
            convert=convert,
        )
        world._seal("importer")
        report = check_beets_config(world.cfg(), role="importer")
        if configured:
            assert_hard_code(report.hard_failures, "convert_auto_conflict")
        else:
            self.assertTrue(report.ok, report.hard_failures)


if __name__ == "__main__":
    unittest.main()
