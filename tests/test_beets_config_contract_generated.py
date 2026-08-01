"""Generated patrol for token-only Beets configuration authority."""

from __future__ import annotations

import stat
import unittest

import msgspec
import yaml
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_config_contract import BeetsRole, ContractFinding, check_beets_config
from lib.config import CratediggerConfig, read_runtime_config_strict
from tests.test_beets_config_contract import BeetsContractWorld


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


class TestGeneratedTokenOnlySecret(unittest.TestCase):
    @example("SECRET::library: /attacker/library.db\nplugins: []::TOKEN")
    @example("SECRET::quoted: ' value\nwith:newline'::TOKEN")
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


class TestGeneratedEffectiveSettings(unittest.TestCase):
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
