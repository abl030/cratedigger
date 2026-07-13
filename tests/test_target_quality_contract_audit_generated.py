"""Production audit and generated qualification for explicit MP3 mode."""

from __future__ import annotations

from pathlib import Path
import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz tiers
from tests._target_quality_contract_audit import target_contract_call_violations


_REPO_ROOT = Path(__file__).resolve().parents[1]
_PRODUCTION_MANIFEST = _REPO_ROOT / "tools/production_python_sources.txt"


def _production_paths() -> list[Path]:
    paths: list[Path] = []
    for raw_line in _PRODUCTION_MANIFEST.read_text(encoding="utf-8").splitlines():
        entry = raw_line.strip()
        if not entry or entry.startswith("#"):
            continue
        root = _REPO_ROOT / entry
        paths.extend(root.rglob("*.py") if root.is_dir() else [root])
    return sorted(paths)


def _production_violations() -> list[str]:
    violations: list[str] = []
    for path in _production_paths():
        relative_path = str(path.relative_to(_REPO_ROOT))
        for violation in target_contract_call_violations(
            relative_path,
            path.read_text(encoding="utf-8"),
        ):
            violations.append(f"{violation.relative_path}:{violation.line}")
    return sorted(violations)


class TestTargetQualityContractProductionAudit(unittest.TestCase):
    def test_scope_uses_every_canonical_production_surface(self):
        paths = {str(path.relative_to(_REPO_ROOT)) for path in _production_paths()}

        self.assertIn("cratedigger.py", paths)
        self.assertIn("album_source.py", paths)
        for directory in ("lib", "web", "harness", "scripts"):
            self.assertTrue(
                any(path.startswith(f"{directory}/") for path in paths),
                directory,
            )

    def test_every_potential_bare_mp3_production_call_names_the_mode(self):
        self.assertEqual(_production_violations(), [])

    def test_checker_trips_on_planted_bare_mp3_omission(self):
        source = (
            "from lib.quality import TargetQualityContract\n"
            "contract = TargetQualityContract.from_format('  Mp3  ')\n"
        )

        violations = target_contract_call_violations("lib/planted.py", source)

        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0].relative_path, "lib/planted.py")

    def test_checker_trips_on_function_local_direct_import_omission(self):
        source = (
            "def build():\n"
            "    from lib.quality import TargetQualityContract\n"
            "    return TargetQualityContract.from_format('MP3')\n"
        )

        violations = target_contract_call_violations("web/local_direct.py", source)

        self.assertEqual(len(violations), 1)

    def test_checker_trips_on_function_local_alias_import_omission(self):
        source = (
            "def build():\n"
            "    from lib.quality import TargetQualityContract as Contract\n"
            "    return Contract.from_format('MP3')\n"
        )

        violations = target_contract_call_violations("scripts/local_alias.py", source)

        self.assertEqual(len(violations), 1)

    def test_unrelated_same_named_class_is_not_a_target_contract(self):
        source = (
            "class TargetQualityContract:\n"
            "    from_format = staticmethod(lambda label: label)\n"
            "contract = TargetQualityContract.from_format('MP3')\n"
        )

        self.assertEqual(
            target_contract_call_violations("web/unrelated.py", source),
            (),
        )

    def test_unrelated_same_named_import_is_not_a_target_contract(self):
        source = (
            "from other.quality import TargetQualityContract\n"
            "contract = TargetQualityContract.from_format('MP3')\n"
        )

        self.assertEqual(
            target_contract_call_violations("scripts/unrelated.py", source),
            (),
        )

    def test_real_implementation_import_is_recognized(self):
        source = (
            "from .evidence_types import TargetQualityContract as Contract\n"
            "contract = Contract.from_format('MP3')\n"
        )

        violations = target_contract_call_violations(
            "lib/quality/generated.py",
            source,
        )

        self.assertEqual(len(violations), 1)

    def test_local_target_alias_does_not_leak_into_a_sibling_scope(self):
        source = (
            "def target_scope():\n"
            "    from lib.quality import TargetQualityContract as Contract\n"
            "    return Contract.from_format('mp3 v0')\n"
            "def unrelated_scope():\n"
            "    class Contract:\n"
            "        from_format = staticmethod(lambda label: label)\n"
            "    return Contract.from_format('MP3')\n"
        )

        self.assertEqual(
            target_contract_call_violations("web/sibling_scopes.py", source),
            (),
        )

    def test_local_unrelated_import_shadows_real_module_binding(self):
        source = (
            "from lib.quality import TargetQualityContract\n"
            "def unrelated_scope():\n"
            "    from other.quality import TargetQualityContract\n"
            "    return TargetQualityContract.from_format('MP3')\n"
        )

        self.assertEqual(
            target_contract_call_violations("web/shadowed_scope.py", source),
            (),
        )

    def test_from_lib_module_binding_is_audited(self):
        source = (
            "from lib import quality\n"
            "contract = quality.TargetQualityContract.from_format('MP3')\n"
        )

        self.assertEqual(
            len(target_contract_call_violations("web/from_lib.py", source)),
            1,
        )

    def test_peer_lib_import_does_not_hide_qualified_binding(self):
        source = (
            "import lib.quality\n"
            "import lib.config\n"
            "contract = lib.quality.TargetQualityContract.from_format('MP3')\n"
        )

        self.assertEqual(
            len(target_contract_call_violations("scripts/peer_import.py", source)),
            1,
        )

    def test_factory_alias_escape_fails_closed(self):
        source = (
            "from lib.quality import TargetQualityContract\n"
            "factory = TargetQualityContract.from_format\n"
            "contract = factory('MP3')\n"
        )

        self.assertEqual(
            len(target_contract_call_violations("lib/factory_escape.py", source)),
            1,
        )

    def test_later_rebind_of_proven_target_fails_closed(self):
        source = (
            "from lib.quality import TargetQualityContract\n"
            "contract = TargetQualityContract.from_format(\n"
            "    'MP3', projected_is_cbr=False\n"
            ")\n"
            "TargetQualityContract = object()\n"
        )

        self.assertEqual(
            len(target_contract_call_violations("lib/later_rebind.py", source)),
            1,
        )


class TestTargetQualityContractAuditGenerated(unittest.TestCase):
    @given(
        label=st.sampled_from(("MP3", " mp3 ", "Mp3", "flac", "mp3 v0", "opus 128")),
        mode=st.one_of(st.none(), st.booleans()),
        supplies_mode=st.booleans(),
        scope=st.sampled_from(
            ("module", "function", "nested_function", "class")
        ),
        binding=st.sampled_from(
            (
                "direct",
                "direct_impl",
                "relative_impl",
                "alias",
                "module",
                "module_full",
                "module_impl",
                "from_lib",
                "module_with_peer",
            )
        ),
    )
    @example(
        label="MP3",
        mode=None,
        supplies_mode=False,
        scope="function",
        binding="direct",
    )
    @example(
        label=" mp3 ",
        mode=True,
        supplies_mode=True,
        scope="function",
        binding="alias",
    )
    def test_audit_rejects_exactly_possible_bare_mp3_omissions(
        self,
        label: str,
        mode: bool | None,
        supplies_mode: bool,
        scope: str,
        binding: str,
    ) -> None:
        if binding == "direct":
            imports = "from lib.quality import TargetQualityContract\n"
            owner = "TargetQualityContract"
            relative_path = "lib/generated.py"
        elif binding == "direct_impl":
            imports = (
                "from lib.quality.evidence_types import TargetQualityContract\n"
            )
            owner = "TargetQualityContract"
            relative_path = "lib/generated.py"
        elif binding == "relative_impl":
            imports = (
                "from .evidence_types import TargetQualityContract as Contract\n"
            )
            owner = "Contract"
            relative_path = "lib/quality/generated.py"
        elif binding == "alias":
            imports = "from lib.quality import TargetQualityContract as Contract\n"
            owner = "Contract"
            relative_path = "lib/generated.py"
        elif binding == "module":
            imports = "import lib.quality as quality\n"
            owner = "quality.TargetQualityContract"
            relative_path = "lib/generated.py"
        elif binding == "module_full":
            imports = "import lib.quality\n"
            owner = "lib.quality.TargetQualityContract"
            relative_path = "lib/generated.py"
        elif binding == "from_lib":
            imports = "from lib import quality\n"
            owner = "quality.TargetQualityContract"
            relative_path = "lib/generated.py"
        elif binding == "module_with_peer":
            imports = "import lib.quality\nimport lib.config\n"
            owner = "lib.quality.TargetQualityContract"
            relative_path = "lib/generated.py"
        else:
            imports = "import lib.quality.evidence_types as evidence_types\n"
            owner = "evidence_types.TargetQualityContract"
            relative_path = "lib/generated.py"
        mode_argument = (
            f", projected_is_cbr={mode!r}" if supplies_mode else ""
        )
        call = f"contract = {owner}.from_format({label!r}{mode_argument})\n"
        if scope == "function":
            source = "def build():\n" + "".join(
                f"    {line}\n" for line in (imports + call).splitlines()
            )
        elif scope == "nested_function":
            source = "def outer():\n    def build():\n" + "".join(
                f"        {line}\n" for line in (imports + call).splitlines()
            )
        elif scope == "class":
            source = "class Builder:\n" + "".join(
                f"    {line}\n" for line in (imports + call).splitlines()
            )
        else:
            source = imports + call

        violations = target_contract_call_violations(relative_path, source)

        expected_violation = label.strip().lower() == "mp3" and not supplies_mode
        self.assertEqual(bool(violations), expected_violation)

    @given(
        escape=st.sampled_from(("factory", "class")),
        scope=st.sampled_from(("module", "function")),
    )
    def test_dynamic_target_factory_escapes_fail_closed(
        self,
        escape: str,
        scope: str,
    ) -> None:
        value = (
            "TargetQualityContract.from_format"
            if escape == "factory"
            else "TargetQualityContract"
        )
        lines = (
            "from lib.quality import TargetQualityContract\n"
            f"factory = {value}\n"
        )
        source = (
            "def build():\n"
            + "".join(f"    {line}\n" for line in lines.splitlines())
            if scope == "function"
            else lines
        )

        self.assertTrue(
            target_contract_call_violations("lib/generated_escape.py", source)
        )

    @given(supplies_mode=st.booleans())
    def test_dynamic_labels_always_name_the_mode(
        self,
        supplies_mode: bool,
    ) -> None:
        mode_argument = ", projected_is_cbr=None" if supplies_mode else ""
        source = (
            "from lib.quality import TargetQualityContract\n"
            f"contract = TargetQualityContract.from_format(label{mode_argument})\n"
        )

        violations = target_contract_call_violations("scripts/generated.py", source)

        self.assertEqual(bool(violations), not supplies_mode)


if __name__ == "__main__":
    unittest.main()
