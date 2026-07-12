"""Generated qualification for the definition-default patch audit."""

from __future__ import annotations

import keyword
import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz tiers
from tests._definition_default_patch_audit import (
    DefaultPatchFinding,
    assert_default_patch_invariant,
    find_ineffective_default_patches,
)


_SCAFFOLDING_NAMES = frozenset({
    "dependencies",
    "object",
    "patch",
    "subject",
    "test_subject",
    "unrelated",
})
_PATCH_NAME = "pa" + "tch"


_IDENTIFIERS = st.from_regex(
    r"[A-Za-z_][A-Za-z0-9_]{0,12}",
    fullmatch=True,
).filter(
    lambda value: not keyword.iskeyword(value) and value not in _SCAFFOLDING_NAMES
)


@st.composite
def _default_patch_worlds(draw):
    dependency = draw(_IDENTIFIERS)
    callable_name = draw(_IDENTIFIERS.filter(lambda value: value != dependency))
    keyword_name = draw(
        _IDENTIFIERS.filter(lambda value: value not in {dependency, callable_name})
    )
    default_shape = draw(st.sampled_from(("from_import", "module_attribute")))
    call_shape = draw(st.sampled_from(("from_import", "module_alias")))
    relation = draw(st.sampled_from((
        "omitted",
        "explicit",
        "unrelated_patch",
        "unrelated_call",
    )))

    if default_shape == "from_import":
        imports = f"from lib.dependencies import {dependency}\n"
        default = dependency
        captured_target = f"lib.subject.{dependency}"
    else:
        imports = "import lib.dependencies as dependencies\n"
        default = f"dependencies.{dependency}"
        captured_target = f"lib.dependencies.{dependency}"

    production = {
        "lib/subject.py": (
            imports
            + f"\ndef {callable_name}(*, {keyword_name}={default}):\n"
            + f"    return {keyword_name}()\n"
            + "\ndef unrelated():\n    return None\n"
        ),
    }
    if call_shape == "from_import":
        callable_import = f"from lib.subject import {callable_name}\n"
        call_target = callable_name
        unrelated_target = "unrelated"
        callable_import += "from lib.subject import unrelated\n"
    else:
        callable_import = "import lib.subject as subject\n"
        call_target = f"subject.{callable_name}"
        unrelated_target = "subject.unrelated"

    patch_target = (
        "lib.subject.unrelated_dependency"
        if relation == "unrelated_patch"
        else captured_target
    )
    called = unrelated_target if relation == "unrelated_call" else call_target
    injection = f", {keyword_name}=object()" if relation == "explicit" else ""
    tests = {
        "tests/test_subject.py": (
            f"from unittest.mock import {_PATCH_NAME}\n"
            + callable_import
            + "\ndef test_subject():\n"
            + f"    with {_PATCH_NAME}(\"{patch_target}\"):\n"
            + f"        {called}({injection.lstrip(', ')})\n"
        ),
    }
    expected_valid = relation != "omitted"
    return production, tests, expected_valid


class TestGeneratedDefinitionDefaultPatchAudit(unittest.TestCase):
    @given(world=_default_patch_worlds())
    @example(
        world=(
            {
                "lib/subject.py": (
                    "from lib.dependencies import match\n\n"
                    "def enqueue(*, match_fn=match):\n"
                    "    return match_fn()\n\n"
                    "def unrelated():\n"
                    "    return None\n"
                ),
            },
            {
                "tests/test_subject.py": (
                    f"from unittest.mock import {_PATCH_NAME}\n"
                    "from lib.subject import enqueue\n"
                    "from lib.subject import unrelated\n\n"
                    "def test_subject():\n"
                    f"    with {_PATCH_NAME}(\"lib.subject.match\"):\n"
                    "        enqueue()\n"
                ),
            },
            False,
        ),
    )
    def test_omission_is_the_only_invalid_relationship(self, world) -> None:
        production, tests, expected_valid = world
        findings = find_ineffective_default_patches(production, tests)
        assert_default_patch_invariant(findings, expected_valid=expected_valid)

    def test_known_bad_checker_rejects_a_planted_omission(self) -> None:
        finding = DefaultPatchFinding(
            test_path="tests/test_bad.py",
            line=4,
            callable_path="lib.subject.enqueue",
            patched_target="lib.subject.match",
            injectable_keyword="match_fn",
        )

        with self.assertRaises(AssertionError):
            assert_default_patch_invariant((finding,), expected_valid=True)


if __name__ == "__main__":
    unittest.main()
