"""Integration and standalone-checker tests for the Beets authority contract."""

from __future__ import annotations

import os
import stat
import subprocess
import sys
import unittest
from pathlib import Path

import msgspec
import yaml

from lib.beets_config_contract import BeetsConfigError, check_beets_config
from scripts.check_beets_config import CheckerResult
from tests.fakes.beets_contract import (
    BASELINE_PLUGINS,
    BeetsContractWorld,
    assert_redacted_load_failure,
    snapshot_contract_world,
)


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
                "plugins": [*BASELINE_PLUGINS, "convert"],
                "disabled_plugins": ["convert"],
                "convert": {"auto": True, "auto_keep": True},
            }, True, None),
            ("mb enabled", {
                "plugins": ["mbsync", "discogs", "inline", "permissions"],
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
            plugins=[*BASELINE_PLUGINS, "convert"],
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
            plugins=[*BASELINE_PLUGINS, token]
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

    def test_redacted_runtime_load_failure_keeps_json_machine_channel(self):
        token = "PLANTED_RUNTIME_TOKEN_759"
        self.world.unseal()
        self.world.runtime_config.write_text(
            f"[Beets\nuser_token = [{token}\n",
            encoding="utf-8",
        )
        self.world._seal("importer")
        proc = self._run()
        payload = self._decode(proc)
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(payload.error, "config_load_error")
        self.assertEqual(
            proc.stderr,
            "ERROR [runtime_config_load_error] Beets configuration load failed\n",
        )
        assert_redacted_load_failure(proc.stderr + proc.stdout, token)

    def test_malformed_secret_never_echoes_token_or_parser_source(self):
        token = "PLANTED_SECRET_TOKEN_759"
        self.world.unseal()
        self.world.secret_include.write_text(
            f"discogs:\n  user_token: [{token}\n",
            encoding="utf-8",
        )
        self.world._seal("importer")

        proc = self._run()
        payload = self._decode(proc)

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(payload.error, "config_load_error")
        self.assertEqual(
            proc.stderr,
            "ERROR [beets_config_load_error] Beets configuration load failed\n",
        )
        assert_redacted_load_failure(proc.stderr + proc.stdout, token)

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
        self.assertEqual(
            proc.stderr,
            "ERROR [runtime_config_load_error] Beets configuration load failed\n",
        )

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
        self.assertEqual(
            proc.stderr,
            "ERROR [beets_config_load_error] Beets configuration load failed\n",
        )

    def test_checker_result_rejects_wrong_wire_types(self):
        with self.assertRaises(msgspec.ValidationError):
            msgspec.json.decode(
                b'{"ok":"false","report":null,"error":null}',
                type=CheckerResult,
            )


if __name__ == "__main__":
    unittest.main()
