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


class TestTargetQualityContractAuditGenerated(unittest.TestCase):
    @given(
        label=st.sampled_from(("MP3", " mp3 ", "Mp3", "flac", "mp3 v0", "opus 128")),
        mode=st.one_of(st.none(), st.booleans()),
        supplies_mode=st.booleans(),
        binding=st.sampled_from(
            (
                "direct",
                "direct_impl",
                "relative_impl",
                "alias",
                "module",
                "module_full",
                "module_impl",
            )
        ),
    )
    @example(label="MP3", mode=None, supplies_mode=False, binding="direct")
    @example(label=" mp3 ", mode=True, supplies_mode=True, binding="alias")
    def test_audit_rejects_exactly_possible_bare_mp3_omissions(
        self,
        label: str,
        mode: bool | None,
        supplies_mode: bool,
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
        else:
            imports = "import lib.quality.evidence_types as evidence_types\n"
            owner = "evidence_types.TargetQualityContract"
            relative_path = "lib/generated.py"
        mode_argument = (
            f", projected_is_cbr={mode!r}" if supplies_mode else ""
        )
        source = (
            imports
            + f"contract = {owner}.from_format({label!r}{mode_argument})\n"
        )

        violations = target_contract_call_violations(relative_path, source)

        expected_violation = label.strip().lower() == "mp3" and not supplies_mode
        self.assertEqual(bool(violations), expected_violation)

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
