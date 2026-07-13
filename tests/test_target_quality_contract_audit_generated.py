"""Production audit and generated qualification for explicit MP3 mode."""

from __future__ import annotations

from pathlib import Path
import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz tiers
from tests._target_quality_contract_audit import target_contract_call_violations


_REPO_ROOT = Path(__file__).resolve().parents[1]
_PRODUCTION_ROOTS = ("harness", "lib", "scripts")


def _production_violations() -> list[str]:
    violations: list[str] = []
    for root_name in _PRODUCTION_ROOTS:
        for path in (_REPO_ROOT / root_name).rglob("*.py"):
            relative_path = str(path.relative_to(_REPO_ROOT))
            for violation in target_contract_call_violations(
                relative_path,
                path.read_text(encoding="utf-8"),
            ):
                violations.append(
                    f"{violation.relative_path}:{violation.line}"
                )
    return sorted(violations)


class TestTargetQualityContractProductionAudit(unittest.TestCase):
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


class TestTargetQualityContractAuditGenerated(unittest.TestCase):
    @given(
        label=st.sampled_from(("MP3", " mp3 ", "Mp3", "flac", "mp3 v0", "opus 128")),
        mode=st.one_of(st.none(), st.booleans()),
        supplies_mode=st.booleans(),
        binding=st.sampled_from(("direct", "alias", "module", "module_full")),
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
        elif binding == "alias":
            imports = "from lib.quality import TargetQualityContract as Contract\n"
            owner = "Contract"
        elif binding == "module":
            imports = "import lib.quality as quality\n"
            owner = "quality.TargetQualityContract"
        else:
            imports = "import lib.quality\n"
            owner = "lib.quality.TargetQualityContract"
        mode_argument = (
            f", projected_is_cbr={mode!r}" if supplies_mode else ""
        )
        source = (
            imports
            + f"contract = {owner}.from_format({label!r}{mode_argument})\n"
        )

        violations = target_contract_call_violations("lib/generated.py", source)

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
