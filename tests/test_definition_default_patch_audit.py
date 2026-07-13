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


_ENQUEUE_CAPTURE_TARGET = "lib.enqueue.check_for_match"
_FIRST_CAPTURE_TARGET = "lib.first.check_for_match"


@dataclass(frozen=True)
class AuditWorld:
    """One authored world shared by deterministic and generated tiers."""

    captured_patch: bool
    injected: bool
    default_style: Literal[
        "local_name",
        "module_attribute",
        "wrapper",
        "tuple",
    ] = "local_name"
    callable_shape: Literal["function", "constructor", "instance"] = "function"
    call_style: Literal[
        "from_import",
        "module_alias",
        "conflicting_import",
        "shadowed_import",
    ] = "from_import"
    patch_style: Literal[
        "string",
        "object",
        "alias_object",
        "dict",
        "dict_values",
        "multiple",
        "dynamic_object",
    ] = "string"
    patch_api: Literal["direct", "qualified", "unittest"] = "direct"
    activation: Literal["with", "decorator", "later_context"] = "with"
    production_conflict: bool = False
    call_capture: bool = True

    @property
    def expected_error(self) -> bool:
        if not self.captured_patch or not self.call_capture:
            return False
        return (
            self.callable_shape == "instance"
            or self.call_style in {"conflicting_import", "shadowed_import"}
            or (
                not self.injected
                and (
                    self.default_style in {"wrapper", "tuple"}
                    or self.patch_style
                    in {
                        "alias_object",
                        "dict",
                        "dict_values",
                        "multiple",
                        "dynamic_object",
                    }
                    or self.production_conflict
                )
            )
        )

    @property
    def expected_valid(self) -> bool:
        return not self.expected_error and (
            not self.captured_patch or not self.call_capture or self.injected
        )


@dataclass(frozen=True)
class RenderedWorld:
    production: dict[str, str]
    tests: dict[str, str]
    expected_valid: bool
    expected_error: bool


def render_world(world: AuditWorld) -> RenderedWorld:
    """Render only repository-native canonical shapes."""
    if world.production_conflict:
        production_import = (
            "from lib.first import check_for_match\n"
            "from lib.second import check_for_match"
        )
        default = "check_for_match"
        captured_target = "lib.enqueue.check_for_match"
    elif world.default_style == "local_name":
        production_import = "from lib.matching import check_for_match"
        default = "check_for_match"
        captured_target = "lib.enqueue.check_for_match"
    elif world.default_style == "module_attribute":
        production_import = "import lib.matching as matching"
        default = "matching.check_for_match"
        captured_target = "lib.matching.check_for_match"
    else:
        production_import = "from lib.matching import check_for_match"
        default = (
            "bind(check_for_match)"
            if world.default_style == "wrapper"
            else "(check_for_match,)"
        )
        captured_target = "lib.enqueue.check_for_match"
    helper = (
        "\ndef bind(value):\n    return value\n"
        if world.default_style == "wrapper"
        else ""
    )
    if world.callable_shape == "function":
        definition = (
            f"def try_enqueue(tracks, *, match_fn={default}):\n"
            "    return match_fn(tracks)\n"
        )
        exported_name = "try_enqueue"
        member = ""
    elif world.callable_shape == "constructor":
        definition = (
            "class Worker:\n"
            f"    def __init__(self, tracks, *, match_fn={default}):\n"
            "        self.result = match_fn(tracks)\n"
        )
        exported_name = "Worker"
        member = ""
    else:
        definition = (
            "class Worker:\n"
            f"    def run(self, tracks, *, match_fn={default}):\n"
            "        return match_fn(tracks)\n"
        )
        exported_name = "Worker"
        member = ".run"
    production = {
        "lib/enqueue.py": (f"{production_import}\n{helper}\n{definition}"),
    }

    if world.call_style == "from_import":
        call_prelude = f"from lib.enqueue import {exported_name}"
        callable_expression = exported_name
    elif world.call_style == "module_alias":
        call_prelude = "import lib.enqueue as enqueue"
        callable_expression = f"enqueue.{exported_name}"
    elif world.call_style == "conflicting_import":
        call_prelude = (
            f"from lib.other import {exported_name}\n"
            f"from lib.enqueue import {exported_name}"
        )
        callable_expression = exported_name
    else:
        call_prelude = f"from lib.enqueue import {exported_name}\n"
        if exported_name == "Worker":
            call_prelude += "class Worker:\n    pass"
        else:
            call_prelude += "def try_enqueue(*args, **kwargs):\n    return None"
        callable_expression = exported_name
    scoped_prelude = "".join(f"    {line}\n" for line in call_prelude.splitlines())

    patch_target = captured_target if world.captured_patch else "lib.unrelated.observe"
    if world.patch_api == "direct":
        patch_import = "from unittest.mock import patch"
        patch_expression = "patch"
    elif world.patch_api == "qualified":
        patch_import = "import unittest.mock as mock"
        patch_expression = "mock.patch"
    else:
        patch_import = "import unittest"
        patch_expression = "unittest.mock.patch"
    if world.patch_style == "string":
        patch_call = f'{patch_expression}("{patch_target}")'
        owner_import = ""
    elif world.patch_style == "object":
        owner_path, attribute = patch_target.rsplit(".", 1)
        patch_call = f'{patch_expression}.object(owner, "{attribute}")'
        owner_import = f"import {owner_path} as owner\n"
    elif world.patch_style == "alias_object":
        owner_path, attribute = patch_target.rsplit(".", 1)
        patch_call = f'{patch_expression}.object(owner, "{attribute}")'
        owner_import = (
            f"import {owner_path} as dependency_owner\nowner = dependency_owner\n"
        )
    elif world.patch_style == "dict":
        owner_path, attribute = patch_target.rsplit(".", 1)
        patch_call = f"{patch_expression}.dict(owner.__dict__, {attribute}=object())"
        owner_import = f"import {owner_path} as owner\n"
    elif world.patch_style == "dict_values":
        owner_path, attribute = patch_target.rsplit(".", 1)
        patch_call = (
            f"{patch_expression}.dict(owner.__dict__, "
            f'values={{"{attribute}": object()}})'
        )
        owner_import = f"import {owner_path} as owner\n"
    elif world.patch_style == "multiple":
        owner_path, attribute = patch_target.rsplit(".", 1)
        patch_call = f"{patch_expression}.multiple(owner, {attribute}=object())"
        owner_import = f"import {owner_path} as owner\n"
    else:
        owner_path, _ = patch_target.rsplit(".", 1)
        patch_call = f"{patch_expression}.object(owner, dynamic_attribute)"
        owner_import = f"import {owner_path} as owner\n"

    injection = ", match_fn=object()" if world.injected else ""
    setup = ""
    if not world.call_capture:
        callable_expression = "unrelated"
    elif world.callable_shape == "instance":
        setup = f"    worker = {callable_expression}()\n"
        callable_expression = f"worker{member}"
    call = f"{callable_expression}([]{injection})"
    if world.activation == "with":
        test = (
            f"{patch_import}\n{owner_import}\n"
            "def test_subject():\n"
            f"{scoped_prelude}"
            f"{setup}"
            f"    with {patch_call}:\n"
            f"        {call}\n"
        )
    elif world.activation == "decorator":
        test = (
            f"{patch_import}\n{owner_import}{call_prelude}\n\n"
            f"@{patch_call}\n"
            "def test_subject(mock_dependency):\n"
            f"{setup}"
            f"    {call}\n"
        )
    else:
        test = (
            f"{patch_import}\nfrom contextlib import nullcontext\n{owner_import}\n"
            "def test_subject():\n"
            f"{scoped_prelude}"
            f"{setup}"
            f"    with {patch_call}, nullcontext({call}):\n"
            "        pass\n"
        )
    return RenderedWorld(
        production=production,
        tests={"tests/test_subject.py": test},
        expected_valid=world.expected_valid,
        expected_error=world.expected_error,
    )


def assert_world_contract(
    case: unittest.TestCase,
    world: AuditWorld,
    findings: tuple[DefaultPatchFinding, ...],
    error: ValueError | None = None,
) -> None:
    """Compare the checker with the independent world oracle."""
    if world.expected_error:
        case.assertIsNotNone(error, msg=world)
        return
    case.assertIsNone(error, msg=(world, error))
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
                f'    with patch("{_ENQUEUE_CAPTURE_TARGET}"):\n'
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
                "from lib.enqueue import try_enqueue\n"
                "target = choose_target()\n"
                "with patch(target):\n"
                "    try_enqueue([])\n"
            ),
        }

        with self.assertRaisesRegex(
            ValueError,
            r"tests/test_subject\.py:4: unsupported definition-default patch "
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
                f'with patch("{_ENQUEUE_CAPTURE_TARGET}"):\n'
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
                f'    patcher = patch("{_ENQUEUE_CAPTURE_TARGET}")\n'
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

    def test_wrapper_and_tuple_defaults_only_fail_when_omitted(self) -> None:
        for default_style in ("wrapper", "tuple"):
            injected = render_world(
                AuditWorld(
                    captured_patch=True,
                    injected=True,
                    default_style=default_style,
                ),
            )
            unrelated = render_world(
                AuditWorld(
                    captured_patch=True,
                    injected=False,
                    default_style=default_style,
                    call_capture=False,
                ),
            )
            omitted = render_world(
                AuditWorld(
                    captured_patch=True,
                    injected=False,
                    default_style=default_style,
                ),
            )
            with self.subTest(default_style=default_style):
                self.assertEqual(
                    find_ineffective_default_patches(
                        injected.production,
                        injected.tests,
                    ),
                    (),
                )
                self.assertEqual(
                    find_ineffective_default_patches(
                        unrelated.production,
                        unrelated.tests,
                    ),
                    (),
                )
                with self.assertRaisesRegex(
                    ValueError,
                    r"lib/enqueue\.py:\d+: unsupported definition-default "
                    r"expression for lib\.enqueue\.try_enqueue\.match_fn",
                ):
                    find_ineffective_default_patches(
                        omitted.production,
                        omitted.tests,
                    )

    def test_standard_unsupported_patch_forms_fail_closed_when_relevant(self) -> None:
        worlds = (
            AuditWorld(True, False, patch_style="dict_values"),
            AuditWorld(True, False, patch_style="multiple"),
            AuditWorld(True, False, patch_style="dynamic_object"),
        )
        for world in worlds:
            rendered = render_world(world)
            with self.subTest(world=world), self.assertRaises(ValueError):
                find_ineffective_default_patches(
                    rendered.production,
                    rendered.tests,
                )
        qualified = render_world(AuditWorld(True, False, patch_api="unittest"))
        self.assertEqual(
            len(
                find_ineffective_default_patches(
                    qualified.production,
                    qualified.tests,
                ),
            ),
            1,
        )

    def test_direct_constructor_call_is_checked_as_init(self) -> None:
        world = AuditWorld(
            captured_patch=True,
            injected=False,
            callable_shape="constructor",
        )
        rendered = render_world(world)

        self.assertEqual(
            find_ineffective_default_patches(rendered.production, rendered.tests),
            (
                DefaultPatchFinding(
                    test_path="tests/test_subject.py",
                    line=6,
                    callable_path="lib.enqueue.Worker.__init__",
                    patched_target="lib.enqueue.check_for_match",
                    injectable_keyword="match_fn",
                ),
            ),
        )

    def test_constructor_positional_override_fails_closed(self) -> None:
        production = {
            "lib/enqueue.py": (
                "from lib.matching import check_for_match\n\n"
                "class Worker:\n"
                "    def __init__(self, tracks, match_fn=check_for_match):\n"
                "        self.result = match_fn(tracks)\n"
            ),
        }
        tests = {
            "tests/test_subject.py": (
                "from unittest.mock import patch\n"
                "from lib.enqueue import Worker\n\n"
                f'with patch("{_ENQUEUE_CAPTURE_TARGET}"):\n'
                "    Worker([], object())\n"
            ),
        }

        with self.assertRaisesRegex(
            ValueError,
            r"tests/test_subject\.py:5: unsupported definition-default patch "
            r"syntax: inject match_fn as an explicit keyword",
        ):
            find_ineffective_default_patches(production, tests)

    def test_assigned_instance_member_call_fails_closed(self) -> None:
        world = AuditWorld(
            captured_patch=True,
            injected=True,
            callable_shape="instance",
        )
        rendered = render_world(world)

        with self.assertRaisesRegex(
            ValueError,
            r"tests/test_subject\.py:\d+: unsupported definition-default patch "
            r"syntax: assigned instance call worker\.run cannot be proven",
        ):
            find_ineffective_default_patches(rendered.production, rendered.tests)

    def test_alias_owned_patch_object_fails_closed_when_attribute_overlaps(
        self,
    ) -> None:
        production = render_world(
            AuditWorld(captured_patch=True, injected=False),
        ).production
        tests = {
            "tests/test_subject.py": (
                "from unittest.mock import patch\n"
                "import lib.enqueue as enqueue\n"
                "owner = enqueue\n\n"
                "def test_subject():\n"
                '    with patch.object(owner, "check_for_match"):\n'
                "        enqueue.try_enqueue([])\n"
            ),
        }

        with self.assertRaisesRegex(
            ValueError,
            r"tests/test_subject\.py:6: unsupported definition-default patch "
            r"syntax: patch\.object owner must be a direct import",
        ):
            find_ineffective_default_patches(production, tests)

    def test_patch_dict_key_overlap_fails_closed(self) -> None:
        production = render_world(
            AuditWorld(captured_patch=True, injected=False),
        ).production
        tests = {
            "tests/test_subject.py": (
                "from unittest.mock import patch\n"
                "import lib.enqueue as enqueue\n\n"
                "def test_subject():\n"
                "    with patch.dict(enqueue.__dict__, check_for_match=object()):\n"
                "        enqueue.try_enqueue([])\n"
            ),
        }

        with self.assertRaisesRegex(
            ValueError,
            r"tests/test_subject\.py:5: unsupported definition-default patch "
            r"syntax: patch\.dict overlaps captured target check_for_match",
        ):
            find_ineffective_default_patches(production, tests)

    def test_patch_dict_control_keywords_do_not_count_as_dict_keys(self) -> None:
        production = {
            "lib/enqueue.py": (
                "from lib.matching import clear\n\n"
                "def try_enqueue(tracks, *, match_fn=clear):\n"
                "    return match_fn(tracks)\n"
            ),
        }
        tests = {
            "tests/test_subject.py": (
                "from unittest.mock import patch\n"
                "from lib.enqueue import try_enqueue\n"
                "import lib.unrelated as unrelated\n\n"
                "with patch.dict(unrelated.__dict__, clear=True):\n"
                "    try_enqueue([], match_fn=object())\n"
            ),
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_later_with_context_executes_inside_earlier_patch(self) -> None:
        world = AuditWorld(
            captured_patch=True,
            injected=False,
            activation="later_context",
        )
        rendered = render_world(world)

        findings = find_ineffective_default_patches(
            rendered.production,
            rendered.tests,
        )
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].callable_path, "lib.enqueue.try_enqueue")

    def test_conflicting_production_imports_fail_at_default_source(self) -> None:
        production = {
            "lib/enqueue.py": (
                "from lib.first import check_for_match\n"
                "from lib.second import check_for_match\n\n"
                "def try_enqueue(tracks, *, match_fn=check_for_match):\n"
                "    return match_fn(tracks)\n"
            ),
        }
        tests = {
            "tests/test_subject.py": (
                "from unittest.mock import patch\n"
                "from lib.enqueue import try_enqueue\n\n"
                f'with patch("{_FIRST_CAPTURE_TARGET}"):\n'
                "    try_enqueue([])\n"
            ),
        }

        with self.assertRaisesRegex(
            ValueError,
            r"lib/enqueue\.py:4: unsupported definition-default expression "
            r"for lib\.enqueue\.try_enqueue\.match_fn: binding "
            r"check_for_match has conflicting imports",
        ):
            find_ineffective_default_patches(production, tests)

    def test_local_callable_declaration_shadows_import_fail_closed(self) -> None:
        production = render_world(
            AuditWorld(captured_patch=True, injected=False),
        ).production
        tests = {
            "tests/test_subject.py": (
                "from unittest.mock import patch\n"
                "from lib.enqueue import try_enqueue\n\n"
                "def try_enqueue(tracks):\n"
                "    return tracks\n\n"
                f'with patch("{_ENQUEUE_CAPTURE_TARGET}"):\n'
                "    try_enqueue([])\n"
            ),
        }

        with self.assertRaisesRegex(
            ValueError,
            r"tests/test_subject\.py:4: unsupported definition-default patch "
            r"syntax: imported callable binding try_enqueue is rebound",
        ):
            find_ineffective_default_patches(production, tests)

    def test_repository_has_no_ineffective_definition_default_patches(self) -> None:
        repo_root = Path(__file__).resolve().parent.parent
        self.assertEqual(repository_default_patch_findings(repo_root), ())


if __name__ == "__main__":
    unittest.main()
