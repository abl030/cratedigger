"""Contract tests for the canonical definition-default patch grammar."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal
import unittest

from tests._definition_default_patch_audit import (
    DefaultPatchFinding,
    find_ineffective_default_patches,
    repository_default_patch_findings,
)


@dataclass(frozen=True)
class AuditWorld:
    """One authored world shared by deterministic and generated tiers."""

    captured_patch: bool
    injected: bool
    default_style: Literal["local_name", "module_attribute"] = "local_name"
    call_style: Literal["from_import", "module_alias"] = "from_import"
    patch_style: Literal["string", "object"] = "string"
    patch_api: Literal["direct", "qualified"] = "direct"
    activation: Literal["with", "decorator"] = "with"

    @property
    def expected_valid(self) -> bool:
        return not self.captured_patch or self.injected


@dataclass(frozen=True)
class RenderedWorld:
    production: dict[str, str]
    tests: dict[str, str]
    expected_valid: bool


def render_world(world: AuditWorld) -> RenderedWorld:
    """Render only repository-native canonical shapes."""
    if world.default_style == "local_name":
        production_import = "from lib.matching import check_for_match"
        default = "check_for_match"
        captured_target = "lib.enqueue.check_for_match"
    else:
        production_import = "import lib.matching as matching"
        default = "matching.check_for_match"
        captured_target = "lib.matching.check_for_match"
    production = {
        "lib/enqueue.py": (
            f"{production_import}\n\n"
            f"def try_enqueue(tracks, *, match_fn={default}):\n"
            "    return match_fn(tracks)\n"
        ),
    }

    if world.call_style == "from_import":
        call_import = "from lib.enqueue import try_enqueue"
        callable_expression = "try_enqueue"
    else:
        call_import = "import lib.enqueue as enqueue"
        callable_expression = "enqueue.try_enqueue"

    patch_target = (
        captured_target
        if world.captured_patch
        else "lib.unrelated.observe"
    )
    if world.patch_api == "direct":
        patch_import = "from unittest.mock import patch"
        patch_expression = "patch"
    else:
        patch_import = "import unittest.mock as mock"
        patch_expression = "mock.patch"
    if world.patch_style == "string":
        patch_call = f'{patch_expression}("{patch_target}")'
        owner_import = ""
    else:
        owner_path, attribute = patch_target.rsplit(".", 1)
        patch_call = f'{patch_expression}.object(owner, "{attribute}")'
        owner_import = f"import {owner_path} as owner\n"

    injection = ", match_fn=object()" if world.injected else ""
    call = f"{callable_expression}([]{injection})"
    if world.activation == "with":
        test = (
            f"{patch_import}\n{owner_import}\n"
            "def test_subject():\n"
            f"    {call_import}\n"
            f"    with {patch_call}:\n"
            f"        {call}\n"
        )
    else:
        test = (
            f"{patch_import}\n{owner_import}{call_import}\n\n"
            f"@{patch_call}\n"
            "def test_subject(mock_dependency):\n"
            f"    {call}\n"
        )
    return RenderedWorld(
        production=production,
        tests={"tests/test_subject.py": test},
        expected_valid=world.expected_valid,
    )


def assert_world_contract(
    case: unittest.TestCase,
    world: AuditWorld,
    findings: tuple[DefaultPatchFinding, ...],
) -> None:
    """Compare the checker with the independent world oracle."""
    case.assertEqual(not findings, world.expected_valid, msg=(world, findings))


class TestDefinitionDefaultPatchAudit(unittest.TestCase):
    def test_prefixed_654_cooldown_source_has_exact_finding(self) -> None:
        production = {
            "lib/enqueue.py": (
                "from lib.matching import check_for_match\n\n"
                "def try_enqueue(tracks, *, match_fn=check_for_match):\n"
                "    return match_fn(tracks)\n"
            ),
        }
        tests = {
            "tests/test_cooldown.py": (
                "from unittest.mock import patch\n\n"
                "def test_non_cooled_user_proceeds():\n"
                "    from lib.enqueue import try_enqueue\n"
                "    with patch(\"lib.enqueue.check_for_match\"):\n"
                "        try_enqueue([])\n"
            ),
        }

        self.assertEqual(
            find_ineffective_default_patches(production, tests),
            (
                DefaultPatchFinding(
                    test_path="tests/test_cooldown.py",
                    line=6,
                    callable_path="lib.enqueue.try_enqueue",
                    patched_target="lib.enqueue.check_for_match",
                    injectable_keyword="match_fn",
                ),
            ),
        )

    def test_paired_good_pins_accept_injection_and_unrelated_patch(self) -> None:
        for world in (
            AuditWorld(captured_patch=True, injected=True),
            AuditWorld(captured_patch=False, injected=False),
        ):
            with self.subTest(world=world):
                rendered = render_world(world)
                self.assertEqual(
                    find_ineffective_default_patches(
                        rendered.production,
                        rendered.tests,
                    ),
                    (),
                )

    def test_omission_mutation_is_rejected(self) -> None:
        good = render_world(AuditWorld(captured_patch=True, injected=True))
        bad_tests = {
            path: source.replace(", match_fn=object()", "")
            for path, source in good.tests.items()
        }
        self.assertEqual(
            len(find_ineffective_default_patches(good.production, bad_tests)),
            1,
        )

    def test_world_oracle_rejects_both_planted_checker_faults(self) -> None:
        bad_world = AuditWorld(captured_patch=True, injected=False)
        with self.assertRaises(AssertionError):
            assert_world_contract(self, bad_world, ())

        good_world = AuditWorld(captured_patch=False, injected=False)
        planted_false_finding = DefaultPatchFinding(
            test_path="tests/test_subject.py",
            line=6,
            callable_path="lib.enqueue.try_enqueue",
            patched_target="lib.unrelated.observe",
            injectable_keyword="match_fn",
        )
        with self.assertRaises(AssertionError):
            assert_world_contract(self, good_world, (planted_false_finding,))

    def test_dynamic_patch_target_fails_closed_at_source(self) -> None:
        production = render_world(
            AuditWorld(captured_patch=True, injected=False),
        ).production
        tests = {
            "tests/test_subject.py": (
                "from unittest.mock import patch\n"
                "target = choose_target()\n"
                "with patch(target):\n"
                "    pass\n"
            ),
        }

        with self.assertRaisesRegex(
            ValueError,
            r"tests/test_subject\.py:3: unsupported definition-default patch "
            r"syntax: patch target must be a string literal",
        ):
            find_ineffective_default_patches(production, tests)

    def test_captured_callable_alias_fails_closed_at_source(self) -> None:
        production = render_world(
            AuditWorld(captured_patch=True, injected=False),
        ).production
        tests = {
            "tests/test_subject.py": (
                "from unittest.mock import patch\n"
                "from lib.enqueue import try_enqueue\n\n"
                "with patch(\"lib.enqueue.check_for_match\"):\n"
                "    invoke = try_enqueue\n"
                "    invoke([])\n"
            ),
        }

        with self.assertRaisesRegex(
            ValueError,
            r"tests/test_subject\.py:5: unsupported definition-default patch "
            r"syntax: lib\.enqueue\.try_enqueue must be called directly",
        ):
            find_ineffective_default_patches(production, tests)

    def test_relevant_manual_patcher_fails_closed_at_source(self) -> None:
        production = render_world(
            AuditWorld(captured_patch=True, injected=False),
        ).production
        tests = {
            "tests/test_subject.py": (
                "from unittest.mock import patch\n"
                "from lib.enqueue import try_enqueue\n\n"
                "def test_subject():\n"
                "    patcher = patch(\"lib.enqueue.check_for_match\")\n"
                "    patcher.start()\n"
                "    try_enqueue([])\n"
            ),
        }

        with self.assertRaisesRegex(
            ValueError,
            r"tests/test_subject\.py:5: unsupported definition-default patch "
            r"syntax: captured-default patches must be direct with-items or "
            r"test decorators",
        ):
            find_ineffective_default_patches(production, tests)

    def test_repository_has_no_ineffective_definition_default_patches(self) -> None:
        repo_root = Path(__file__).resolve().parent.parent
        self.assertEqual(repository_default_patch_findings(repo_root), ())


if __name__ == "__main__":
    unittest.main()
