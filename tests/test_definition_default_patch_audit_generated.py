"""Generated qualification for the definition-default patch audit."""

from __future__ import annotations

from dataclasses import dataclass
import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz tiers
from tests._definition_default_patch_audit import (
    assert_default_patch_invariant,
    find_ineffective_default_patches,
)


_PATCH_NAME = "pa" + "tch"


@dataclass(frozen=True)
class _AuditWorld:
    production: dict[str, str]
    tests: dict[str, str]
    expected_valid: bool
    label: str


def _indent(source: str, spaces: int) -> str:
    prefix = " " * spaces
    return "".join(
        prefix + line if line.strip() else line
        for line in source.splitlines(keepends=True)
    )


def _production_source(
    callable_shape: str,
    parameter_kind: str,
    default_shape: str,
) -> tuple[str, str]:
    if default_shape == "from_import":
        imports = "from lib.dependencies import deliver\n"
        default = "deliver"
        patched_target = "lib.subject.deliver"
    else:
        imports = "import lib.dependencies as dependencies\n"
        default = "dependencies.deliver"
        patched_target = "lib.dependencies.deliver"
    if parameter_kind == "positional":
        parameter = f"value, dependency_fn={default}"
    elif parameter_kind == "positional_only":
        parameter = f"value, dependency_fn={default}, /"
    else:
        parameter = f"value, *, dependency_fn={default}"
    if callable_shape == "module":
        definition = (
            f"def execute({parameter}):\n"
            "    return dependency_fn(value)\n"
        )
    elif callable_shape == "constructor":
        definition = (
            "class Worker:\n"
            f"    def __init__(self, {parameter}):\n"
            "        self.result = dependency_fn(value)\n"
        )
    else:
        definition = (
            "class Worker:\n"
            f"    def run(self, {parameter}):\n"
            "        return dependency_fn(value)\n"
        )
    return (
        imports
        + "\n"
        + definition
        + "\ndef unrelated(value):\n"
        + "    return value\n",
        patched_target,
    )


def _patch_surface(
    provenance: str,
    patch_shape: str,
    default_shape: str,
    target: str,
) -> tuple[str, str, str]:
    """Return module imports, patch expression, and optional test parameters."""
    if provenance == "canonical":
        imports = f"from unittest.mock import {_PATCH_NAME}\n"
        patch_callable = _PATCH_NAME
        parameters = ""
    elif provenance == "alias":
        imports = f"from unittest.mock import {_PATCH_NAME} as replace\n"
        patch_callable = "replace"
        parameters = ""
    elif provenance == "qualified":
        imports = "import unittest.mock as mock\n"
        patch_callable = f"mock.{_PATCH_NAME}"
        parameters = ""
    elif provenance == "helper":
        imports = f"from helper import {_PATCH_NAME}\n"
        patch_callable = _PATCH_NAME
        parameters = ""
    elif provenance == "shadowed":
        imports = f"from unittest.mock import {_PATCH_NAME}\n"
        patch_callable = _PATCH_NAME
        parameters = _PATCH_NAME
    else:
        imports = ""
        patch_callable = f"fake.{_PATCH_NAME}"
        parameters = "fake"

    if patch_shape == "object":
        if default_shape == "from_import":
            imports += "import lib.subject as patch_owner\n"
            owner = "patch_owner"
        else:
            imports += "import lib.dependencies as patch_owner\n"
            owner = "patch_owner"
        attribute = target.rsplit(".", 1)[-1]
        expression = f'{patch_callable}.object({owner}, "{attribute}")'
    else:
        expression = f'{patch_callable}("{target}")'
    return imports, expression, parameters


def _call_surface(
    callable_shape: str,
    call_shape: str,
    relation: str,
    parameter_kind: str,
) -> tuple[str, str, str]:
    if callable_shape == "module":
        if call_shape == "alias":
            imports = "from lib.subject import execute as invoke, unrelated\n"
            target = "invoke"
            unrelated_target = "unrelated"
        else:
            imports = "import lib.subject as subject\n"
            target = "subject.execute"
            unrelated_target = "subject.unrelated"
        setup = ""
    elif callable_shape == "constructor":
        if call_shape == "alias":
            imports = "from lib.subject import Worker as Target, unrelated\n"
            target = "Target"
            unrelated_target = "unrelated"
        else:
            imports = "import lib.subject as subject\n"
            target = "subject.Worker"
            unrelated_target = "subject.unrelated"
        setup = ""
    else:
        if call_shape == "direct_instance":
            imports = "from lib.subject import Worker, unrelated\n"
            target = "Worker().run"
            unrelated_target = "unrelated"
            setup = ""
        elif call_shape == "module":
            imports = "import lib.subject as subject\n"
            target = "worker.run"
            unrelated_target = "subject.unrelated"
            setup = "worker = subject.Worker()\n"
        else:
            imports = "from lib.subject import Worker, unrelated\n"
            target = "worker.run"
            unrelated_target = "unrelated"
            setup = "worker = Worker()\n"

    if relation == "unrelated_call":
        call = f'{unrelated_target}("payload")'
    elif relation == "keyword":
        call = f'{target}("payload", dependency_fn=object())'
    elif relation == "positional":
        if parameter_kind in {"positional", "positional_only"}:
            call = f'{target}("payload", object())'
        else:
            call = f'{target}("payload", dependency_fn=object())'
    elif relation == "literal_kwargs":
        call = f'{target}("payload", **{{"dependency_fn": object()}})'
    elif relation == "dynamic_kwargs":
        setup += "options = {}\n"
        call = f'{target}("payload", **options)'
    elif relation == "dynamic_args":
        setup += 'arguments = ["payload"]\n'
        call = f"{target}(*arguments)"
    else:
        call = f'{target}("payload")'
    return imports, setup, call


@st.composite
def _default_patch_worlds(draw):
    callable_shape = draw(st.sampled_from(("module", "constructor", "instance")))
    parameter_kind = draw(
        st.sampled_from(("positional", "positional_only", "keyword_only"))
    )
    default_shape = draw(st.sampled_from(("from_import", "module_attribute")))
    call_shape = draw(
        st.sampled_from(
            ("alias", "module", "direct_instance")
            if callable_shape == "instance"
            else ("alias", "module")
        )
    )
    relation = draw(st.sampled_from((
        "omitted",
        "keyword",
        "positional",
        "literal_kwargs",
        "dynamic_kwargs",
        "dynamic_args",
        "unrelated_patch",
        "unrelated_call",
    )))
    provenance = draw(st.sampled_from((
        "canonical",
        "alias",
        "qualified",
        "helper",
        "shadowed",
        "fake_attribute",
    )))
    patch_shape = draw(st.sampled_from(("with", "decorator", "object")))
    scope = draw(st.sampled_from(("module", "function", "nested")))

    if provenance == "shadowed" and patch_shape == "decorator":
        patch_shape = "with"
    production, captured_target = _production_source(
        callable_shape,
        parameter_kind,
        default_shape,
    )
    patch_target = (
        "lib.subject.unrelated_dependency"
        if relation == "unrelated_patch"
        else captured_target
    )
    patch_imports, patch_expression, test_parameters = _patch_surface(
        provenance,
        patch_shape,
        default_shape,
        patch_target,
    )
    call_imports, setup, call = _call_surface(
        callable_shape,
        call_shape,
        relation,
        parameter_kind,
    )

    if scope == "module":
        module_call_imports = call_imports
        local_call_imports = ""
    else:
        module_call_imports = ""
        local_call_imports = call_imports

    parameter_suffix = test_parameters
    if patch_shape == "decorator":
        parameter_suffix = ", ".join(
            parameter for parameter in (test_parameters, "mock_dependency") if parameter
        )
    test_header = f"def test_subject({parameter_suffix}):\n"
    local_lines = local_call_imports + setup
    if patch_shape == "decorator":
        if scope == "nested":
            body = (
                test_header
                + _indent(local_call_imports, 4)
                + f"    @{patch_expression}\n"
                + "    def exercise(mock_dependency):\n"
                + _indent(setup, 8)
                + f"        {call}\n"
                + "    exercise()\n"
            )
        else:
            body = (
                f"@{patch_expression}\n"
                + test_header
                + _indent(local_lines, 4)
                + f"    {call}\n"
            )
    elif scope == "nested":
        body = (
            test_header
            + "    def exercise():\n"
            + _indent(local_lines, 8)
            + f"        with {patch_expression}:\n"
            + f"            {call}\n"
            + "    exercise()\n"
        )
    else:
        body = (
            test_header
            + _indent(local_lines, 4)
            + f"    with {patch_expression}:\n"
            + f"        {call}\n"
        )

    recognized_patch = provenance in {"canonical", "alias", "qualified"}
    omitted = relation in {"omitted", "dynamic_kwargs", "dynamic_args"}
    if parameter_kind == "positional_only" and relation in {
        "keyword",
        "literal_kwargs",
    }:
        omitted = True
    expected_valid = not (recognized_patch and omitted)
    label = "/".join((
        callable_shape,
        parameter_kind,
        default_shape,
        call_shape,
        relation,
        provenance,
        patch_shape,
        scope,
    ))
    return _AuditWorld(
        production={"lib/subject.py": production},
        tests={
            "tests/test_subject.py": patch_imports + module_call_imports + "\n" + body,
        },
        expected_valid=expected_valid,
        label=label,
    )


_OMITTED_CONSTRUCTOR = _AuditWorld(
    production={
        "lib/subject.py": (
            "from lib.dependencies import deliver\n\n"
            "class Worker:\n"
            "    def __init__(self, *, dependency_fn=deliver):\n"
            "        self.dependency_fn = dependency_fn\n"
        ),
    },
    tests={
        "tests/test_subject.py": (
            f"from unittest.mock import {_PATCH_NAME}\n"
            "from lib.subject import Worker\n\n"
            "def test_subject():\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "        Worker()\n"
        ),
    },
    expected_valid=False,
    label="pinned omitted constructor",
)


@st.composite
def _nested_call_worlds(draw):
    patch_shape = draw(st.sampled_from(("with", "decorator")))
    call_site = draw(
        st.sampled_from(("inside", "outside"))
        if patch_shape == "with"
        else st.just("inside")
    )
    production = {
        "lib/subject.py": (
            "from lib.dependencies import deliver\n\n"
            "def execute(*, dependency_fn=deliver):\n"
            "    return dependency_fn()\n"
        ),
    }
    if patch_shape == "decorator":
        body = (
            f"@{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "def test_subject(mock_dependency):\n"
            "    def exercise():\n"
            "        execute()\n"
            "    exercise()\n"
        )
        expected_valid = False
    elif call_site == "inside":
        body = (
            "def test_subject():\n"
            "    def exercise():\n"
            "        execute()\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "        exercise()\n"
        )
        expected_valid = False
    else:
        body = (
            "def test_subject():\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "        def exercise():\n"
            "            execute()\n"
            "        callback = exercise\n"
            "    callback()\n"
        )
        expected_valid = True
    return _AuditWorld(
        production=production,
        tests={
            "tests/test_subject.py": (
                f"from unittest.mock import {_PATCH_NAME}\n"
                "from lib.subject import execute\n\n"
                + body
            ),
        },
        expected_valid=expected_valid,
        label=f"nested/{patch_shape}/{call_site}",
    )


@st.composite
def _descriptor_worlds(draw):
    descriptor = draw(st.sampled_from(("instance", "class", "static")))
    access = draw(st.sampled_from(("class", "instance")))
    relation = draw(st.sampled_from(("omitted", "positional")))
    decorator = {
        "instance": "",
        "class": "    @classmethod\n",
        "static": "    @staticmethod\n",
    }[descriptor]
    receiver = {"instance": "self", "class": "cls", "static": None}[descriptor]
    parameters = "value, dependency_fn=deliver"
    if receiver is not None:
        parameters = f"{receiver}, {parameters}"
    production = {
        "lib/subject.py": (
            "from lib.dependencies import deliver\n\n"
            "class Worker:\n"
            + decorator
            + f"    def run({parameters}):\n"
            + "        return dependency_fn(value)\n"
        ),
    }
    arguments = ['"payload"']
    if descriptor == "instance" and access == "class":
        arguments.insert(0, "worker")
    if relation == "positional":
        arguments.append("object()")
    target = "Worker.run" if access == "class" else "worker.run"
    tests = {
        "tests/test_subject.py": (
            f"from unittest.mock import {_PATCH_NAME}\n"
            "from lib.subject import Worker\n\n"
            "def test_subject():\n"
            "    worker = Worker()\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            + f"        {target}({', '.join(arguments)})\n"
        ),
    }
    return _AuditWorld(
        production=production,
        tests=tests,
        expected_valid=relation == "positional",
        label=f"descriptor/{descriptor}/{access}/{relation}",
    )


@st.composite
def _binder_worlds(draw):
    binder = draw(st.sampled_from((
        "for",
        "async_for",
        "with",
        "except",
        "walrus",
        "comprehension",
        "comprehension_instance",
        "comprehension_walrus",
    )))
    if binder == "comprehension_instance":
        return _AuditWorld(
            production={
                "lib/subject.py": (
                    "from lib.dependencies import deliver\n\n"
                    "class Worker:\n"
                    "    def run(self, *, dependency_fn=deliver):\n"
                    "        return dependency_fn()\n"
                ),
            },
            tests={
                "tests/test_subject.py": (
                    f"from unittest.mock import {_PATCH_NAME}\n"
                    "from lib.subject import Worker\n\n"
                    "def test_subject(foreign_workers):\n"
                    "    worker = Worker()\n"
                    f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
                    "        values = [worker.run() for worker in foreign_workers]\n"
                ),
            },
            expected_valid=True,
            label="binder/comprehension_instance",
        )
    if binder == "comprehension_walrus":
        return _AuditWorld(
            production={
                "lib/subject.py": (
                    "from lib.dependencies import deliver\n\n"
                    "def execute(*, dependency_fn=deliver):\n"
                    "    return dependency_fn()\n"
                ),
            },
            tests={
                "tests/test_subject.py": (
                    f"from unittest.mock import {_PATCH_NAME}\n"
                    "from helper import helper_patch\n"
                    "from lib.subject import execute\n\n"
                    "def test_subject(values):\n"
                    f"    bound = [({_PATCH_NAME} := helper_patch) for value in values]\n"
                    f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
                    "        execute()\n"
                ),
            },
            expected_valid=True,
            label="binder/comprehension_walrus",
        )
    if binder == "for":
        body = (
            "def test_subject():\n"
            f"    for {_PATCH_NAME} in [helper_patch]:\n"
            f"        with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "            execute()\n"
        )
    elif binder == "async_for":
        body = (
            "async def test_subject(values):\n"
            f"    async for {_PATCH_NAME} in values:\n"
            f"        with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "            execute()\n"
        )
    elif binder == "with":
        body = (
            "def test_subject():\n"
            f"    with provider() as {_PATCH_NAME}:\n"
            f"        with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "            execute()\n"
        )
    elif binder == "except":
        body = (
            "def test_subject():\n"
            "    try:\n"
            "        raise RuntimeError\n"
            f"    except RuntimeError as {_PATCH_NAME}:\n"
            f"        with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "            execute()\n"
        )
    elif binder == "walrus":
        body = (
            "def test_subject():\n"
            f"    if ({_PATCH_NAME} := helper_patch):\n"
            f"        with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "            execute()\n"
        )
    else:
        body = (
            "def test_subject(helpers):\n"
            f"    values = [{_PATCH_NAME} for {_PATCH_NAME} in helpers]\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "        execute()\n"
        )
    return _AuditWorld(
        production={
            "lib/subject.py": (
                "from lib.dependencies import deliver\n\n"
                "def execute(*, dependency_fn=deliver):\n"
                "    return dependency_fn()\n"
            ),
        },
        tests={
            "tests/test_subject.py": (
                f"from unittest.mock import {_PATCH_NAME}\n"
                "from helper import helper_patch, provider\n"
                "from lib.subject import execute\n\n"
                + body
            ),
        },
        expected_valid=binder != "comprehension",
        label=f"binder/{binder}",
    )


class TestGeneratedDefinitionDefaultPatchAudit(unittest.TestCase):
    @given(world=_default_patch_worlds())
    @example(world=_OMITTED_CONSTRUCTOR)
    def test_audit_matches_independent_world_oracle(self, world: _AuditWorld) -> None:
        findings = find_ineffective_default_patches(world.production, world.tests)
        try:
            assert_default_patch_invariant(
                findings,
                expected_valid=world.expected_valid,
            )
        except AssertionError as error:
            raise AssertionError(f"world={world.label}: {error}") from error

    def test_known_bad_source_fixture_trips_the_checker(self) -> None:
        findings = find_ineffective_default_patches(
            _OMITTED_CONSTRUCTOR.production,
            _OMITTED_CONSTRUCTOR.tests,
        )

        with self.assertRaises(AssertionError):
            assert_default_patch_invariant(findings, expected_valid=True)

    @given(world=_nested_call_worlds())
    def test_nested_helpers_use_direct_call_site_patch_state(
        self,
        world: _AuditWorld,
    ) -> None:
        findings = find_ineffective_default_patches(world.production, world.tests)
        assert_default_patch_invariant(findings, expected_valid=world.expected_valid)

    @given(world=_descriptor_worlds())
    def test_descriptor_binding_matches_python(self, world: _AuditWorld) -> None:
        findings = find_ineffective_default_patches(world.production, world.tests)
        assert_default_patch_invariant(findings, expected_valid=world.expected_valid)

    @given(world=_binder_worlds())
    def test_lexical_binders_control_patch_provenance(
        self,
        world: _AuditWorld,
    ) -> None:
        findings = find_ineffective_default_patches(world.production, world.tests)
        assert_default_patch_invariant(findings, expected_valid=world.expected_valid)


if __name__ == "__main__":
    unittest.main()
