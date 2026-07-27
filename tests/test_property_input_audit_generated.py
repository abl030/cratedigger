"""Generated worlds for the drawn-input audit (issue #882 item 5).

The deterministic pins in ``tests/test_property_input_audit.py`` prove the
exact #868 shape — a property that ``del``'d its generated input under a
"real worlds, real filesystem" banner. These properties patrol the space
around it: any container, any decorator form, any mix of used / discarded /
never-mentioned / rebound-only drawn inputs, plus the malformed shapes the
audit must fail closed on.

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""

from __future__ import annotations

import unittest
from itertools import product

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - loads the active profile
from tests.test_property_input_audit import (
    PropertyFunction,
    assert_every_drawn_input_used,
    find_property_functions,
)

_RELPATH = "tests/test_synthetic_world.py"

_NAME_VALUES = (
    "alpha", "beta", "gamma", "delta", "world", "seed", "album", "download",
)
_NAMES = st.sampled_from(_NAME_VALUES)

#: Modes that leave a ``Load`` of the drawn name in the body, and modes that
#: do not. The second group is the defect class.
_USING_MODES = ("call", "attribute", "fstring", "closure", "comprehension")
_IGNORING_MODES = ("deleted", "absent", "store_only")
_MODES = st.sampled_from(_USING_MODES + _IGNORING_MODES)

_INPUTS = st.lists(
    st.tuples(_NAMES, _MODES),
    min_size=1,
    max_size=4,
    unique_by=lambda pair: pair[0],
)
_CONTAINERS = st.sampled_from(["module", "method", "nested"])
_DECORATORS = st.sampled_from(["given_kw", "given_pos", "rule_kw"])

_MALFORMED_VALUES = (
    "bare",
    "star_args",
    "star_star_kwargs",
    "mixed",
    "surplus_positional",
    "unknown_keyword",
    "variadic",
)


def _body_lines(name: str, mode: str) -> list[str]:
    if mode == "call":
        return [f"_sink({name})"]
    if mode == "attribute":
        return [f"_sink({name}.field)"]
    if mode == "fstring":
        return [f'_sink(f"{{{name}}}")']
    if mode == "closure":
        return ["def _inner():", f"    return {name}", "_sink(_inner)"]
    if mode == "comprehension":
        return [f"_sink([item for item in {name}])"]
    if mode == "deleted":
        return [f"del {name}"]
    if mode == "store_only":
        return [f"{name} = FIXED"]
    if mode == "absent":
        return []
    raise AssertionError(f"unknown usage mode {mode!r}")


def render_property(
    inputs: list[tuple[str, str]],
    container: str,
    decorator: str,
) -> str:
    """Render one synthetic Hypothesis property module."""
    names = [name for name, _mode in inputs]
    if decorator == "given_pos":
        decorator_line = "@given(" + ", ".join("st.none()" for _ in names) + ")"
    elif decorator == "given_kw":
        decorator_line = "@given(" + ", ".join(f"{n}=st.none()" for n in names) + ")"
    else:
        decorator_line = (
            "@rule(target=rows, " + ", ".join(f"{n}=st.none()" for n in names) + ")"
        )

    body: list[str] = []
    for name, mode in inputs:
        body.extend(_body_lines(name, mode))
    body.append("assert CHECKED")

    if decorator == "rule_kw":
        header = ["class Machine(RuleBasedStateMachine):"]
        signature = "def step(self, " + ", ".join(names) + "):"
        indent = 4
    elif container == "module":
        header = []
        signature = "def prop(" + ", ".join(names) + "):"
        indent = 0
    elif container == "method":
        header = ["class TestSynthetic:"]
        signature = "def test_prop(self, " + ", ".join(names) + "):"
        indent = 4
    else:
        header = ["class TestSynthetic:", "    def test_outer(self):"]
        signature = "def prop(" + ", ".join(names) + "):"
        indent = 8

    pad = " " * indent
    lines = [*header, pad + decorator_line, pad + signature]
    lines.extend(pad + "    " + line for line in body)
    if container == "nested" and decorator != "rule_kw":
        lines.append(pad + "prop()")
    return "\n".join(lines) + "\n"


def render_malformed_property(name: str, shape: str) -> str:
    """Render one shape the bounded grammar must refuse to classify."""
    if shape == "bare":
        decorator_line = "@given"
    elif shape == "star_args":
        decorator_line = "@given(*strategies)"
    elif shape == "star_star_kwargs":
        decorator_line = "@given(**strategies)"
    elif shape == "mixed":
        decorator_line = f"@given(st.none(), {name}=st.none())"
    elif shape == "surplus_positional":
        decorator_line = "@given(st.none(), st.none())"
    elif shape == "unknown_keyword":
        decorator_line = "@given(not_a_parameter=st.none())"
    elif shape == "variadic":
        decorator_line = "@given(st.none())"
    else:
        raise AssertionError(f"unknown malformed shape {shape!r}")

    if shape == "mixed":
        signature = f"def prop(other, {name}):"
        body = f"    _sink(other, {name})"
    elif shape == "variadic":
        signature = f"def prop(*{name}):"
        body = f"    _sink({name})"
    else:
        signature = f"def prop({name}):"
        body = f"    _sink({name})"
    return f"{decorator_line}\n{signature}\n{body}\n"


def assert_no_deleted_drawn_inputs(prop: PropertyFunction) -> None:
    """Known-bad checker: sees only ``del``, misses never-mentioned inputs."""
    assert not prop.deleted_inputs, f"{prop.key} deletes {prop.deleted_inputs!r}"


class TestGeneratedPropertyInputAudit(unittest.TestCase):
    @example(
        inputs=[("album", "deleted"), ("download", "deleted")],
        container="nested",
        decorator="given_kw",
    )
    @given(inputs=_INPUTS, container=_CONTAINERS, decorator=_DECORATORS)
    def test_unused_verdict_matches_the_independent_oracle(
        self,
        inputs: list[tuple[str, str]],
        container: str,
        decorator: str,
    ) -> None:
        source = render_property(inputs, container, decorator)
        expected = tuple(
            name for name, mode in inputs if mode in _IGNORING_MODES
        )

        (prop,) = find_property_functions(source, _RELPATH)

        self.assertIsNone(prop.unclassified_reason, source)
        self.assertEqual(prop.drawn_inputs, tuple(name for name, _ in inputs))
        self.assertEqual(prop.unused_inputs, expected, source)
        self.assertEqual(
            prop.deleted_inputs,
            tuple(name for name, mode in inputs if mode == "deleted"),
            source,
        )

    @given(inputs=_INPUTS, container=_CONTAINERS, decorator=_DECORATORS)
    def test_checker_trips_exactly_when_an_input_is_ignored(
        self,
        inputs: list[tuple[str, str]],
        container: str,
        decorator: str,
    ) -> None:
        source = render_property(inputs, container, decorator)
        ignored = [name for name, mode in inputs if mode in _IGNORING_MODES]

        (prop,) = find_property_functions(source, _RELPATH)

        if ignored:
            with self.assertRaises(AssertionError):
                assert_every_drawn_input_used((prop,), {})
            assert_every_drawn_input_used((prop,), {prop.key: "allowlisted"})
        else:
            assert_every_drawn_input_used((prop,), {})
            with self.assertRaisesRegex(AssertionError, "stale allowlist"):
                assert_every_drawn_input_used((prop,), {prop.key: "stale"})

    def test_malformed_shapes_fail_closed_even_when_allowlisted(self) -> None:
        for name, shape in product(_NAME_VALUES, _MALFORMED_VALUES):
            with self.subTest(name=name, shape=shape):
                source = render_malformed_property(name, shape)

                (prop,) = find_property_functions(source, _RELPATH)

                self.assertIsNotNone(prop.unclassified_reason, source)
                with self.assertRaisesRegex(AssertionError, "unclassifiable"):
                    assert_every_drawn_input_used((prop,), {prop.key: "excused"})

    @given(inputs=_INPUTS, container=_CONTAINERS)
    def test_known_bad_del_only_checker_accepts_a_never_mentioned_input(
        self,
        inputs: list[tuple[str, str]],
        container: str,
    ) -> None:
        absent = [(name, "absent") for name, _mode in inputs]
        source = render_property(absent, container, "given_kw")

        (prop,) = find_property_functions(source, _RELPATH)

        assert_no_deleted_drawn_inputs(prop)
        with self.assertRaisesRegex(AssertionError, "never uses"):
            assert_every_drawn_input_used((prop,), {})


if __name__ == "__main__":
    unittest.main()
