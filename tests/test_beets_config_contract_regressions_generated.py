"""Generated integration regressions and known-bad checks for Beets authority."""

from __future__ import annotations

import errno
import os
import stat
import unittest
from collections.abc import Callable
from pathlib import Path
from unittest.mock import patch

import msgspec
from hypothesis import given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.beets_config_contract import (
    REQUIRED_PLUGINS,
    BeetsConfigError,
    _can_open_for_write,
    check_beets_config,
)
from lib.config import CratediggerConfig, read_runtime_config_strict
from tests.fakes.beets_contract import (
    BASELINE_PLUGINS,
    BeetsContractWorld,
    assert_discogs_token_missing,
    assert_hard_code,
    assert_loader_is_strict,
    assert_native_config_rejection,
    assert_raw_authority_rejected,
    assert_token_absent_from_owned_report,
)

_WRITE_DENIAL_ERRNOS = frozenset((errno.EACCES, errno.EPERM, errno.EROFS))
_WRITE_INCONCLUSIVE_ERRNOS = (
    errno.EIO,
    errno.ESTALE,
    errno.EMFILE,
    errno.ENOSPC,
    errno.ENOENT,
)
_WRITE_PROBE_ERRNOS = tuple(sorted(_WRITE_DENIAL_ERRNOS)) + tuple(
    sorted(_WRITE_INCONCLUSIVE_ERRNOS)
)


def assert_write_probe_errno_contract(
    probe: Callable[[Path], bool],
    error_number: int,
) -> None:
    """Require only permission or read-only-filesystem errors to prove denial."""
    error = OSError(error_number, os.strerror(error_number))
    with patch("os.open", side_effect=error):
        if error_number in _WRITE_DENIAL_ERRNOS:
            if probe(Path("/declared/beets/authority")) is not False:
                raise AssertionError("write-denial OSError did not prove non-writability")
            return

        try:
            probe(Path("/declared/beets/authority"))
        except BeetsConfigError:
            return
        raise AssertionError("inconclusive OSError was treated as proven non-writability")


def _broad_oserror_as_denial_mutant(path: Path) -> bool:
    """Known-bad probe that launders every write-open failure into denial."""
    try:
        fd = os.open(path, os.O_WRONLY | getattr(os, "O_CLOEXEC", 0))
    except OSError:
        return False
    os.close(fd)
    return True


class TestKnownBadContractCheckers(unittest.TestCase):
    def test_write_probe_checker_rejects_broad_oserror_denial_mutant(self) -> None:
        with self.assertRaisesRegex(AssertionError, "inconclusive OSError"):
            assert_write_probe_errno_contract(
                _broad_oserror_as_denial_mutant,
                errno.EIO,
            )

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

    def test_discogs_token_checker_rejects_a_planted_admission(self) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        with self.assertRaisesRegex(AssertionError, "known-bad mutant"):
            assert_discogs_token_missing(check_beets_config(world.cfg(), role="importer"))


class TestGeneratedIntegrationRegressions(unittest.TestCase):
    @settings(max_examples=len(_WRITE_PROBE_ERRNOS))
    @given(error_number=st.sampled_from(_WRITE_PROBE_ERRNOS))
    def test_write_probe_errors_are_classified_fail_closed(
        self,
        error_number: int,
    ) -> None:
        assert_write_probe_errno_contract(_can_open_for_write, error_number)

        world = BeetsContractWorld()
        self.addCleanup(world.close)
        error = OSError(error_number, os.strerror(error_number))
        with patch("os.open", side_effect=error):
            if error_number in _WRITE_DENIAL_ERRNOS:
                report = check_beets_config(world.cfg(), role="importer")
                assert_hard_code(
                    report.hard_failures,
                    "state_not_writable_by_importer",
                )
            else:
                with self.assertRaisesRegex(
                    BeetsConfigError,
                    "cannot determine whether Beets authority is writable",
                ):
                    check_beets_config(world.cfg(), role="importer")

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

    @given(st.sampled_from((*REQUIRED_PLUGINS, "convert")))
    def test_disabled_plugins_are_absent_from_the_active_contract(
        self, plugin: str
    ) -> None:
        world = BeetsContractWorld()
        self.addCleanup(world.close)
        world.unseal()
        world._write_main_config(
            plugins=[*BASELINE_PLUGINS, "convert"],
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
            plugins=[*BASELINE_PLUGINS, token]
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
            plugins=[*BASELINE_PLUGINS, "convert"],
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
