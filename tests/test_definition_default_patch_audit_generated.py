"""Generated qualification for the definition-default patch audit."""

from __future__ import annotations

import ast
from dataclasses import dataclass
import unittest

from hypothesis import example, given, strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/push/fuzz tiers
from tests._definition_default_patch_audit import (
    _TestPatchVisitor,
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


@st.composite
def _nested_registry_worlds(draw):
    shape = draw(st.sampled_from((
        "sibling_chain",
        "canonical_then_helper",
        "helper_then_canonical",
    )))
    if shape == "sibling_chain":
        body = (
            "def test_subject():\n"
            "    def inner():\n"
            "        execute()\n"
            "    def outer():\n"
            "        inner()\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "        outer()\n"
        )
        imports = f"from unittest.mock import {_PATCH_NAME}\n"
        expected_valid = False
    elif shape == "canonical_then_helper":
        body = (
            "def test_subject():\n"
            f"    from unittest.mock import {_PATCH_NAME}\n"
            f"    @{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "    def exercise(mock_dependency):\n"
            "        execute()\n"
            f"    from helper import {_PATCH_NAME}\n"
            "    exercise()\n"
        )
        imports = ""
        expected_valid = False
    else:
        body = (
            "def test_subject():\n"
            f"    from helper import {_PATCH_NAME}\n"
            f"    @{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "    def exercise(mock_dependency):\n"
            "        execute()\n"
            f"    from unittest.mock import {_PATCH_NAME}\n"
            "    exercise()\n"
        )
        imports = ""
        expected_valid = True
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
                imports
                + "from lib.subject import execute\n\n"
                + body
            ),
        },
        expected_valid=expected_valid,
        label=f"nested_registry/{shape}",
    )


@st.composite
def _with_timing_worlds(draw):
    shape = draw(st.sampled_from((
        "multi_item",
        "outer_patch",
        "context_walrus",
        "async_multi_item",
    )))
    if shape == "multi_item":
        body = (
            "def test_subject():\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"), cm(execute()):\n"
            "        pass\n"
        )
        expected_valid = False
    elif shape == "outer_patch":
        body = (
            "def test_subject():\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "        with cm(execute()):\n"
            "            pass\n"
        )
        expected_valid = False
    elif shape == "async_multi_item":
        body = (
            "async def test_subject():\n"
            f"    async with {_PATCH_NAME}(\"lib.subject.deliver\"), async_cm(execute()):\n"
            "        pass\n"
        )
        expected_valid = False
    else:
        body = (
            "def test_subject():\n"
            f"    with cm(({_PATCH_NAME} := helper_patch)):\n"
            f"        with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "            execute()\n"
        )
        expected_valid = True
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
                "from helper import async_cm, cm, helper_patch\n"
                "from lib.subject import execute\n\n"
                + body
            ),
        },
        expected_valid=expected_valid,
        label=f"with_timing/{shape}",
    )


@st.composite
def _definition_expression_worlds(draw):
    shape = draw(st.sampled_from((
        "default",
        "decorator",
        "annotation",
        "future_annotation",
        "lambda_default",
        "lambda_escape",
    )))
    imports = (
        "from __future__ import annotations\n\n"
        if shape == "future_annotation"
        else ""
    )
    if shape == "default":
        expression = "        def exercise(value=execute()):\n            pass\n"
        expected_valid = False
    elif shape == "decorator":
        expression = (
            "        @decorate(execute())\n"
            "        def exercise():\n"
            "            pass\n"
        )
        expected_valid = False
    elif shape in {"annotation", "future_annotation"}:
        expression = (
            "        def exercise(value: execute()) -> execute():\n"
            "            pass\n"
        )
        expected_valid = shape == "future_annotation"
    elif shape == "lambda_default":
        expression = (
            "        callback = lambda value=execute(): (\n"
            "            execute()\n"
            "        )\n"
            "    callback()\n"
        )
        expected_valid = False
    else:
        expression = (
            "        callback = lambda: (\n"
            "            execute()\n"
            "        )\n"
            "    callback()\n"
        )
        expected_valid = True
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
                imports
                + f"from unittest.mock import {_PATCH_NAME}\n"
                + "from helper import decorate\n"
                + "from lib.subject import execute\n\n"
                + "def test_subject():\n"
                + f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
                + expression
            ),
        },
        expected_valid=expected_valid,
        label=f"definition_expression/{shape}",
    )


@st.composite
def _local_call_order_worlds(draw):
    shape = draw(st.sampled_from((
        "canonical_to_helper",
        "helper_to_canonical",
        "argument_call",
        "single_traversal",
    )))
    if shape == "canonical_to_helper":
        imports = (
            f"from unittest.mock import {_PATCH_NAME}\n"
            "from helper import helper_patch\n"
        )
        body = (
            "def test_subject():\n"
            "    def exercise(value):\n"
            f"        with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "            execute()\n"
            f"    exercise(({_PATCH_NAME} := helper_patch))\n"
        )
        expected_count = 0
    elif shape == "helper_to_canonical":
        imports = (
            f"from helper import {_PATCH_NAME}\n"
            f"from unittest.mock import {_PATCH_NAME} as canonical_patch\n"
        )
        body = (
            "def test_subject():\n"
            "    def exercise(value):\n"
            f"        with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "            execute()\n"
            f"    exercise(({_PATCH_NAME} := canonical_patch))\n"
        )
        expected_count = 1
    elif shape == "argument_call":
        imports = f"from unittest.mock import {_PATCH_NAME}\n"
        body = (
            "def test_subject():\n"
            "    def exercise(value):\n"
            "        pass\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "        exercise(execute())\n"
        )
        expected_count = 1
    else:
        imports = (
            f"from helper import {_PATCH_NAME}\n"
            f"from unittest.mock import {_PATCH_NAME} as canonical_patch\n"
        )
        body = (
            "def test_subject():\n"
            "    def exercise(first, second, third):\n"
            f"        with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "            execute()\n"
            "    exercise(\n"
            f"        (temporary := {_PATCH_NAME}),\n"
            f"        ({_PATCH_NAME} := canonical_patch),\n"
            "        (canonical_patch := temporary),\n"
            "    )\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "        execute()\n"
        )
        expected_count = 2
    return (
        _AuditWorld(
            production={
                "lib/subject.py": (
                    "from lib.dependencies import deliver\n\n"
                    "def execute(*, dependency_fn=deliver):\n"
                    "    return dependency_fn()\n"
                ),
            },
            tests={
                "tests/test_subject.py": (
                    imports
                    + "from lib.subject import execute\n\n"
                    + body
                ),
            },
            expected_valid=expected_count == 0,
            label=f"local_call_order/{shape}",
        ),
        expected_count,
    )


@st.composite
def _class_definition_worlds(draw):
    shape = draw(st.sampled_from((
        "class_patch",
        "outer_body",
        "outer_decorator_expression",
        "class_patch_body_only",
        "canonical_then_helper",
        "helper_then_canonical",
        "non_patch_decorator",
    )))
    imports = (
        f"from unittest.mock import {_PATCH_NAME}\n"
        "from helper import decorate, helper_patch\n"
    )
    if shape == "class_patch":
        body = (
            f"@{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "class TestSubject:\n"
            "    def test_subject(self, mock_dependency):\n"
            "        execute()\n"
        )
        expected_valid = False
    elif shape == "outer_body":
        body = (
            "def test_subject():\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "        class Helper:\n"
            "            value = execute()\n"
        )
        expected_valid = False
    elif shape == "outer_decorator_expression":
        body = (
            "def test_subject():\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "        @decorate(execute())\n"
            "        class Helper:\n"
            "            pass\n"
        )
        expected_valid = False
    elif shape == "class_patch_body_only":
        body = (
            f"@{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "class TestSubject:\n"
            "    value = execute()\n"
            "    def test_subject(self, mock_dependency):\n"
            "        execute(dependency_fn=object())\n"
        )
        expected_valid = True
    elif shape == "canonical_then_helper":
        body = (
            f"@{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "class TestSubject:\n"
            "    def test_subject(self, mock_dependency):\n"
            "        execute()\n"
            f"{_PATCH_NAME} = helper_patch\n"
        )
        expected_valid = False
    elif shape == "helper_then_canonical":
        imports = (
            f"from helper import {_PATCH_NAME}, decorate, helper_patch\n"
            f"from unittest.mock import {_PATCH_NAME} as canonical_patch\n"
        )
        body = (
            f"@{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "class TestSubject:\n"
            "    def test_subject(self, mock_dependency):\n"
            "        execute()\n"
            f"{_PATCH_NAME} = canonical_patch\n"
        )
        expected_valid = True
    else:
        body = (
            "@decorate(object())\n"
            "class TestSubject:\n"
            "    def test_subject(self):\n"
            "        execute()\n"
        )
        expected_valid = True
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
                imports
                + "from lib.subject import execute\n\n"
                + body
            ),
        },
        expected_valid=expected_valid,
        label=f"class_definition/{shape}",
    )


@st.composite
def _assignment_patch_alias_worlds(draw):
    shape = draw(st.sampled_from((
        "assign",
        "annassign",
        "walrus",
        "helper_overwrite",
        "assigned_callback",
    )))
    if shape == "assign":
        setup = f"    replace = {_PATCH_NAME}\n"
        patch_name = "replace"
        expected_valid = False
        call_name = "execute"
    elif shape == "annassign":
        setup = f"    replace: object = {_PATCH_NAME}\n"
        patch_name = "replace"
        expected_valid = False
        call_name = "execute"
    elif shape == "walrus":
        setup = ""
        patch_name = f"(replace := {_PATCH_NAME})"
        expected_valid = False
        call_name = "execute"
    elif shape == "helper_overwrite":
        setup = f"    replace = {_PATCH_NAME}\n    replace = helper_patch\n"
        patch_name = "replace"
        expected_valid = True
        call_name = "execute"
    else:
        setup = "    callback = execute\n"
        patch_name = _PATCH_NAME
        expected_valid = True
        call_name = "callback"
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
                "def test_subject():\n"
                + setup
                + f"    with {patch_name}(\"lib.subject.deliver\"):\n"
                + f"        {call_name}()\n"
            ),
        },
        expected_valid=expected_valid,
        label=f"assignment_patch_alias/{shape}",
    )


@st.composite
def _declaration_shadow_worlds(draw):
    shape = draw(st.sampled_from((
        "module_function",
        "module_class",
        "nested_function",
        "nested_class",
        "unshadowed",
    )))
    production = {
        "lib/subject.py": (
            "from lib.dependencies import deliver\n\n"
            "def execute(*, dependency_fn=deliver):\n"
            "    return dependency_fn()\n\n"
            "class Worker:\n"
            "    def __init__(self, *, dependency_fn=deliver):\n"
            "        self.dependency_fn = dependency_fn\n"
        ),
    }
    if shape == "module_function":
        imported = "from lib.subject import execute\n"
        declarations = "def execute():\n    pass\n\n"
        body = "        execute()\n"
        expected_valid = True
    elif shape == "module_class":
        imported = "from lib.subject import Worker\n"
        declarations = "class Worker:\n    pass\n\n"
        body = "        Worker()\n"
        expected_valid = True
    elif shape == "nested_function":
        imported = "from lib.subject import execute\n"
        declarations = ""
        body = "    def execute():\n        pass\n    with PATCH:\n        execute()\n"
        expected_valid = True
    elif shape == "nested_class":
        imported = "from lib.subject import Worker\n"
        declarations = ""
        body = "    class Worker:\n        pass\n    with PATCH:\n        Worker()\n"
        expected_valid = True
    else:
        imported = "from lib.subject import execute\n"
        declarations = ""
        body = "        execute()\n"
        expected_valid = False
    if shape in {"nested_function", "nested_class"}:
        function = (
            "def test_subject():\n"
            + body.replace(
                "with PATCH:",
                f"with {_PATCH_NAME}(\"lib.subject.deliver\"):",
            )
        )
    else:
        function = (
            "def test_subject():\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            + body
        )
    return _AuditWorld(
        production=production,
        tests={
            "tests/test_subject.py": (
                f"from unittest.mock import {_PATCH_NAME}\n"
                + imported
                + "\n"
                + declarations
                + function
            ),
        },
        expected_valid=expected_valid,
        label=f"declaration_shadow/{shape}",
    )


@st.composite
def _import_binding_worlds(draw):
    shape = draw(st.sampled_from((
        "absolute_overwrites_helper",
        "relative_shadows_canonical",
        "import_overwrites_instance",
        "absolute_canonical_control",
    )))
    if shape == "absolute_overwrites_helper":
        production = {
            "lib/subject.py": (
                "from lib.dependencies import deliver\n\n"
                "def execute(*, dependency_fn=deliver):\n"
                "    return dependency_fn()\n"
            ),
        }
        imports = (
            f"from unittest.mock import {_PATCH_NAME} as outer_patch\n"
            "from lib.subject import execute\n"
        )
        body = (
            "def test_subject():\n"
            f"    def {_PATCH_NAME}(*args):\n"
            "        execute()\n"
            "    with outer_patch(\"lib.subject.deliver\"):\n"
            f"        from unittest.mock import {_PATCH_NAME}\n"
            f"        with {_PATCH_NAME}(\"lib.subject.unrelated\"):\n"
            "            pass\n"
        )
        expected_valid = True
    elif shape == "relative_shadows_canonical":
        production = {
            "lib/subject.py": (
                "from lib.dependencies import deliver\n\n"
                "def execute(*, dependency_fn=deliver):\n"
                "    return dependency_fn()\n"
            ),
        }
        imports = (
            f"from unittest.mock import {_PATCH_NAME}\n"
            "from lib.subject import execute\n"
        )
        body = (
            "def test_subject():\n"
            f"    from .helper import {_PATCH_NAME}\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "        execute()\n"
        )
        expected_valid = True
    elif shape == "import_overwrites_instance":
        production = {
            "lib/subject.py": (
                "from lib.dependencies import deliver\n\n"
                "class Worker:\n"
                "    def run(self, *, dependency_fn=deliver):\n"
                "        return dependency_fn()\n"
            ),
        }
        imports = (
            f"from unittest.mock import {_PATCH_NAME}\n"
            "from lib.subject import Worker\n"
        )
        body = (
            "def test_subject():\n"
            "    worker = Worker()\n"
            "    import helper as worker\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "        worker.run()\n"
        )
        expected_valid = True
    else:
        production = {
            "lib/subject.py": (
                "from lib.dependencies import deliver\n\n"
                "def execute(*, dependency_fn=deliver):\n"
                "    return dependency_fn()\n"
            ),
        }
        imports = "from lib.subject import execute\n"
        body = (
            "def test_subject():\n"
            f"    from unittest.mock import {_PATCH_NAME}\n"
            f"    with {_PATCH_NAME}(\"lib.subject.deliver\"):\n"
            "        execute()\n"
        )
        expected_valid = False
    return _AuditWorld(
        production=production,
        tests={
            "tests/test_subject.py": imports + "\n" + body,
        },
        expected_valid=expected_valid,
        label=f"import_binding/{shape}",
    )


@st.composite
def _class_test_prefix_worlds(draw):
    shape = draw(st.sampled_from((
        "default",
        "changed",
        "restored",
        "alias",
        "qualified",
        "dynamic",
        "unrelated",
    )))
    imports = (
        f"from unittest.mock import {_PATCH_NAME}\n"
        "from lib.subject import execute\n"
    )
    if shape == "default":
        body = (
            f"@{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "class SubjectTests:\n"
            "    def test_run(self, mock_dependency):\n"
            "        execute()  # selected\n"
            "    def check_run(self):\n"
            "        execute()  # unselected\n"
        )
    elif shape == "changed":
        body = (
            f"{_PATCH_NAME}.TEST_PREFIX = \"check\"\n"
            f"@{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "class SubjectTests:\n"
            "    def test_run(self):\n"
            "        execute()  # unselected\n"
            "    def check_run(self, mock_dependency):\n"
            "        execute()  # selected\n"
        )
    elif shape == "restored":
        body = (
            f"{_PATCH_NAME}.TEST_PREFIX = \"check\"\n"
            f"{_PATCH_NAME}.TEST_PREFIX = \"test\"\n"
            f"@{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "class SubjectTests:\n"
            "    def test_run(self, mock_dependency):\n"
            "        execute()  # selected\n"
            "    def check_run(self):\n"
            "        execute()  # unselected\n"
        )
    elif shape == "alias":
        imports = (
            f"from unittest.mock import {_PATCH_NAME} as replace\n"
            "from lib.subject import execute\n"
        )
        body = (
            "replace.TEST_PREFIX = \"verify\"\n"
            "@replace(\"lib.subject.deliver\")\n"
            "class SubjectTests:\n"
            "    def verify_run(self, mock_dependency):\n"
            "        execute()  # selected\n"
            "    def test_run(self):\n"
            "        execute()  # unselected\n"
        )
    elif shape == "qualified":
        imports = (
            "import unittest.mock as mock\n"
            "from lib.subject import execute\n"
        )
        body = (
            "mock.patch.TEST_PREFIX = \"verify\"\n"
            "@mock.patch(\"lib.subject.deliver\")\n"
            "class SubjectTests:\n"
            "    def verify_run(self, mock_dependency):\n"
            "        execute()  # selected\n"
            "    def test_run(self):\n"
            "        execute()  # unselected\n"
        )
    elif shape == "dynamic":
        body = (
            "def configure(prefix):\n"
            f"    {_PATCH_NAME}.TEST_PREFIX = prefix\n"
        )
    else:
        imports = "import helper\n"
        body = "helper.TEST_PREFIX = dynamic_prefix()\n"
    source = imports + "\n" + body
    selected_lines = tuple(
        line_number
        for line_number, line in enumerate(source.splitlines(), start=1)
        if "# selected" in line
    )
    return (
        _AuditWorld(
            production={
                "lib/subject.py": (
                    "from lib.dependencies import deliver\n\n"
                    "def execute(*, dependency_fn=deliver):\n"
                    "    return dependency_fn()\n"
                ),
            },
            tests={"tests/test_subject.py": source},
            expected_valid=not selected_lines,
            label=f"class_test_prefix/{shape}",
        ),
        None if shape == "dynamic" else selected_lines,
    )


@st.composite
def _test_prefix_scope_worlds(draw):
    shape = draw(st.sampled_from((
        "test_changes_to_check",
        "test_restores_to_test",
        "method_changes_to_check",
    )))
    imports = (
        f"from unittest.mock import {_PATCH_NAME}\n"
        "from lib.subject import execute\n\n"
    )
    if shape == "test_changes_to_check":
        body = (
            "def test_changes_prefix():\n"
            f"    {_PATCH_NAME}.TEST_PREFIX = \"check\"\n\n"
            f"@{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "class PrefixIsolation:\n"
            "    def test_run(self, mock_dependency):\n"
            "        execute()  # selected\n"
            "    def check_run(self):\n"
            "        execute()  # unselected\n"
        )
    elif shape == "test_restores_to_test":
        body = (
            f"{_PATCH_NAME}.TEST_PREFIX = \"check\"\n"
            "def test_changes_prefix():\n"
            f"    {_PATCH_NAME}.TEST_PREFIX = \"test\"\n\n"
            f"@{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "class PrefixIsolation:\n"
            "    def test_run(self):\n"
            "        execute()  # unselected\n"
            "    def check_run(self, mock_dependency):\n"
            "        execute()  # selected\n"
        )
    else:
        body = (
            f"@{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "class First:\n"
            "    def test_changes_prefix(self, mock_dependency):\n"
            f"        {_PATCH_NAME}.TEST_PREFIX = \"check\"\n\n"
            f"@{_PATCH_NAME}(\"lib.subject.deliver\")\n"
            "class Second:\n"
            "    def test_run(self, mock_dependency):\n"
            "        execute()  # selected\n"
            "    def check_run(self):\n"
            "        execute()  # unselected\n"
        )
    source = imports + body
    selected_lines = tuple(
        line_number
        for line_number, line in enumerate(source.splitlines(), start=1)
        if "# selected" in line
    )
    return (
        _AuditWorld(
            production={
                "lib/subject.py": (
                    "from lib.dependencies import deliver\n\n"
                    "def execute(*, dependency_fn=deliver):\n"
                    "    return dependency_fn()\n"
                ),
            },
            tests={"tests/test_subject.py": source},
            expected_valid=False,
            label=f"test_prefix_scope/{shape}",
        ),
        selected_lines,
    )


@st.composite
def _star_import_worlds(draw):
    shape = draw(st.sampled_from((
        "canonical",
        "absolute_shadow",
        "relative_shadow",
    )))
    if shape == "canonical":
        source = "from unittest.mock import *\n"
        line = 1
    elif shape == "absolute_shadow":
        source = (
            f"from unittest.mock import {_PATCH_NAME}\n"
            "from helper import *\n"
        )
        line = 2
    else:
        source = (
            f"from unittest.mock import {_PATCH_NAME}\n"
            "from .helper import *\n"
        )
        line = 2
    return source, line, shape


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

    @given(world=_nested_registry_worlds())
    def test_nested_registry_and_decorator_timing(
        self,
        world: _AuditWorld,
    ) -> None:
        findings = find_ineffective_default_patches(world.production, world.tests)
        assert_default_patch_invariant(findings, expected_valid=world.expected_valid)

    @given(world=_with_timing_worlds())
    def test_with_items_follow_sequential_evaluation(
        self,
        world: _AuditWorld,
    ) -> None:
        findings = find_ineffective_default_patches(world.production, world.tests)
        assert_default_patch_invariant(findings, expected_valid=world.expected_valid)

    @given(world=_definition_expression_worlds())
    def test_definition_expressions_follow_runtime_timing(
        self,
        world: _AuditWorld,
    ) -> None:
        findings = find_ineffective_default_patches(world.production, world.tests)
        assert_default_patch_invariant(findings, expected_valid=world.expected_valid)

    @given(world_and_count=_local_call_order_worlds())
    def test_local_call_arguments_precede_one_body_traversal(
        self,
        world_and_count: tuple[_AuditWorld, int],
    ) -> None:
        world, expected_count = world_and_count
        findings = find_ineffective_default_patches(world.production, world.tests)
        self.assertEqual(
            len(findings),
            expected_count,
            msg=f"world={world.label}: findings={findings!r}",
        )

    @given(world=_class_definition_worlds())
    def test_class_definition_and_decorator_timing(
        self,
        world: _AuditWorld,
    ) -> None:
        findings = find_ineffective_default_patches(world.production, world.tests)
        assert_default_patch_invariant(findings, expected_valid=world.expected_valid)

    @given(world=_assignment_patch_alias_worlds())
    def test_assignment_forms_preserve_patch_provenance(
        self,
        world: _AuditWorld,
    ) -> None:
        findings = find_ineffective_default_patches(world.production, world.tests)
        assert_default_patch_invariant(findings, expected_valid=world.expected_valid)

    @given(world=_declaration_shadow_worlds())
    def test_declarations_shadow_imported_callables(
        self,
        world: _AuditWorld,
    ) -> None:
        findings = find_ineffective_default_patches(world.production, world.tests)
        assert_default_patch_invariant(findings, expected_valid=world.expected_valid)

    @given(world=_import_binding_worlds())
    def test_imports_replace_every_prior_binding(
        self,
        world: _AuditWorld,
    ) -> None:
        findings = find_ineffective_default_patches(world.production, world.tests)
        assert_default_patch_invariant(findings, expected_valid=world.expected_valid)

    @given(world_and_lines=_class_test_prefix_worlds())
    def test_class_patch_uses_current_test_prefix(
        self,
        world_and_lines: tuple[_AuditWorld, tuple[int, ...] | None],
    ) -> None:
        world, expected_lines = world_and_lines
        if expected_lines is None:
            with self.assertRaisesRegex(
                ValueError,
                r"unsupported dynamic unittest\.mock\.patch\.TEST_PREFIX",
            ):
                find_ineffective_default_patches(world.production, world.tests)
            return
        findings = find_ineffective_default_patches(world.production, world.tests)
        self.assertEqual(
            tuple(finding.line for finding in findings),
            expected_lines,
            msg=f"world={world.label}: findings={findings!r}",
        )

    @given(world_and_lines=_test_prefix_scope_worlds())
    def test_independent_function_analysis_restores_test_prefix(
        self,
        world_and_lines: tuple[_AuditWorld, tuple[int, ...]],
    ) -> None:
        world, expected_lines = world_and_lines
        findings = find_ineffective_default_patches(world.production, world.tests)
        self.assertEqual(
            tuple(finding.line for finding in findings),
            expected_lines,
            msg=f"world={world.label}: findings={findings!r}",
        )

    @given(prefix=st.sampled_from(("check", "verify", "case")))
    def test_direct_local_helper_propagates_test_prefix(self, prefix: str) -> None:
        visitor = _TestPatchVisitor(
            test_path="tests/test_subject.py",
            captures={},
            class_paths=frozenset(),
            annotations_deferred=False,
        )
        visitor.aliases[_PATCH_NAME] = "unittest.mock.patch"
        visitor.patch_aliases[_PATCH_NAME] = "unittest.mock.patch"
        tree = ast.parse(
            "def change_prefix():\n"
            f"    {_PATCH_NAME}.TEST_PREFIX = {prefix!r}\n"
            "change_prefix()\n"
        )
        visitor.function_depth = 1
        for statement in tree.body:
            visitor.visit(statement)
        self.assertEqual(visitor.patch_test_prefix, prefix)

    @given(star_world=_star_import_worlds())
    def test_star_imports_fail_closed(
        self,
        star_world: tuple[str, int, str],
    ) -> None:
        source, line, shape = star_world
        with self.assertRaisesRegex(
            ValueError,
            rf"tests/test_subject\.py:{line}: unsupported star import in "
            rf"definition-default patch audit",
            msg=f"world=star_import/{shape}",
        ):
            find_ineffective_default_patches(
                {},
                {"tests/test_subject.py": source},
            )


if __name__ == "__main__":
    unittest.main()
