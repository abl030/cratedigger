"""Generated property for the canonical definition-default patch grammar."""

from __future__ import annotations

import unittest

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz tiers
from tests._definition_default_patch_audit import (
    DefaultPatchFinding,
    find_ineffective_default_patches,
)
from tests.test_definition_default_patch_audit import (
    AuditWorld,
    assert_world_contract,
    render_world,
)


_WORLDS = st.builds(
    AuditWorld,
    captured_patch=st.booleans(),
    injected=st.booleans(),
    default_style=st.sampled_from(
        ("local_name", "module_attribute", "wrapper", "tuple"),
    ),
    callable_shape=st.sampled_from(("function", "constructor", "instance")),
    call_style=st.sampled_from(
        ("from_import", "module_alias", "conflicting_import", "shadowed_import"),
    ),
    patch_style=st.sampled_from(
        (
            "string",
            "object",
            "alias_object",
            "dict",
            "dict_values",
            "multiple",
            "dynamic_object",
        ),
    ),
    patch_api=st.sampled_from(("direct", "qualified", "unittest")),
    activation=st.sampled_from(("with", "decorator", "later_context")),
    production_conflict=st.booleans(),
    call_capture=st.booleans(),
)


class TestDefinitionDefaultPatchAuditGenerated(unittest.TestCase):
    @given(world=_WORLDS)
    def test_pin_pair_holds_across_canonical_worlds(self, world: AuditWorld) -> None:
        rendered = render_world(world)
        try:
            findings = find_ineffective_default_patches(
                rendered.production,
                rendered.tests,
            )
        except ValueError as error:
            assert_world_contract(self, world, (), error)
        else:
            assert_world_contract(self, world, findings)

    @given(world=_WORLDS)
    def test_known_bad_verdicts_are_rejected(self, world: AuditWorld) -> None:
        rendered = render_world(world)
        if world.expected_error:
            with self.assertRaises(AssertionError):
                assert_world_contract(self, world, (), None)
            return
        findings = find_ineffective_default_patches(
            rendered.production,
            rendered.tests,
        )
        wrong_findings = (
            ()
            if findings
            else (
                DefaultPatchFinding(
                    test_path="tests/test_subject.py",
                    line=1,
                    callable_path="lib.enqueue.try_enqueue",
                    patched_target="lib.unrelated.observe",
                    injectable_keyword="match_fn",
                ),
            )
        )
        with self.assertRaises(AssertionError):
            assert_world_contract(self, world, wrong_findings)


if __name__ == "__main__":
    unittest.main()
