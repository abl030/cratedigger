"""Reject tests that patch dependencies captured in definition defaults."""

from __future__ import annotations

import ast
from pathlib import Path
import unittest

from tests._definition_default_patch_audit import (
    DefaultPatchFinding,
    _TestPatchVisitor,
    assert_default_patch_invariant,
    find_ineffective_default_patches,
    repository_default_patch_findings,
)


_PATCH_NAME = "pa" + "tch"


class TestDefinitionDefaultPatchAudit(unittest.TestCase):
    def test_planted_in_function_import_and_with_patch_is_rejected(self) -> None:
        production = {
            "lib/enqueue.py": """
from lib.matching import check_for_match

def try_enqueue(tracks, *, match_fn=check_for_match):
    return match_fn(tracks)
""",
        }
        tests = {
            "tests/test_cooldown.py": f"""
from unittest.mock import {_PATCH_NAME}

def test_non_cooled_user_proceeds():
    from lib.enqueue import try_enqueue
    with {_PATCH_NAME}("lib.enqueue.check_for_match"):
        try_enqueue([])
""",
        }

        self.assertEqual(
            find_ineffective_default_patches(production, tests),
            (
                DefaultPatchFinding(
                    test_path="tests/test_cooldown.py",
                    line=7,
                    callable_path="lib.enqueue.try_enqueue",
                    patched_target="lib.enqueue.check_for_match",
                    injectable_keyword="match_fn",
                ),
            ),
        )

    def test_explicit_injection_and_unrelated_shapes_are_accepted(self) -> None:
        production = {
            "lib/enqueue.py": """
from lib.matching import check_for_match
from lib.search import search

def try_enqueue(tracks, *, match_fn=check_for_match):
    return match_fn(tracks)

def find_album(query, *, search_fn=search):
    return search_fn(query)
""",
        }
        tests = {
            "tests/test_enqueue.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.enqueue import find_album, try_enqueue

def test_explicit_injection():
    with {_PATCH_NAME}("lib.enqueue.check_for_match"):
        try_enqueue([], match_fn=object())

def test_unrelated_patch():
    with {_PATCH_NAME}("lib.enqueue.logger"):
        try_enqueue([])

def test_unrelated_default():
    with {_PATCH_NAME}("lib.enqueue.check_for_match"):
        find_album("needle")
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_attribute_default_and_module_alias_call_are_correlated(self) -> None:
        production = {
            "lib/chooser.py": """
import lib.matching as matching

def choose(*, matcher=matching.check_for_match):
    return matcher()
""",
        }
        tests = {
            "tests/test_chooser.py": f"""
from unittest.mock import {_PATCH_NAME}
import lib.chooser as chooser

def test_choose():
    with {_PATCH_NAME}("lib.matching.check_for_match"):
        chooser.choose()
""",
        }

        findings = find_ineffective_default_patches(production, tests)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].injectable_keyword, "matcher")

    def test_patch_decorator_is_correlated_with_aliased_from_import(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run as execute

@{_PATCH_NAME}("lib.worker.send")
def test_run(mock_send):
    execute()
""",
        }

        findings = find_ineffective_default_patches(production, tests)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].callable_path, "lib.worker.run")

    def test_constructor_call_correlates_with_init_default(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

class Worker:
    def __init__(self, *, sender=send):
        self.sender = sender
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import Worker

def test_worker():
    with {_PATCH_NAME}("lib.worker.send"):
        Worker()
""",
        }

        findings = find_ineffective_default_patches(production, tests)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].callable_path, "lib.worker.Worker.__init__")
        self.assertEqual(findings[0].injectable_keyword, "sender")

    def test_bound_and_direct_instance_method_calls_are_correlated(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

class Worker:
    def run(self, *, sender=send):
        return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import Worker

def test_bound_worker():
    worker = Worker()
    with {_PATCH_NAME}("lib.worker.send"):
        worker.run()

def test_direct_worker():
    with {_PATCH_NAME}("lib.worker.send"):
        Worker().run()
""",
        }

        findings = find_ineffective_default_patches(production, tests)
        self.assertEqual(len(findings), 2)
        self.assertEqual(
            {finding.callable_path for finding in findings},
            {"lib.worker.Worker.run"},
        )

    def test_positional_injection_of_positional_default_is_accepted(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(value, sender=send):
    return sender(value)
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_run():
    with {_PATCH_NAME}("lib.worker.send"):
        run("payload", object())
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_positional_only_default_rejects_keyword_injection(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(value, sender=send, /):
    return sender(value)
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_run():
    with {_PATCH_NAME}("lib.worker.send"):
        run("payload", sender=object())
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_ambiguous_star_args_remain_fail_closed(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(value, sender=send):
    return sender(value)
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_run(arguments):
    with {_PATCH_NAME}("lib.worker.send"):
        run(*arguments)
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_unrelated_and_shadowed_patch_names_are_ignored(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from helper import {_PATCH_NAME}
from lib.worker import run

def test_helper_patch():
    with {_PATCH_NAME}("lib.worker.send"):
        run()

def test_shadowed_patch({_PATCH_NAME}):
    with {_PATCH_NAME}("lib.worker.send"):
        run()

def test_attribute_patch(fake):
    with fake.{_PATCH_NAME}("lib.worker.send"):
        run()
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_canonical_patch_object_is_correlated(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
import lib.worker as worker

def test_worker():
    with {_PATCH_NAME}.object(worker, "send"):
        worker.run()
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_nested_helper_inherits_enclosing_with_patch_at_direct_call(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_worker():
    def exercise():
        run()
    with {_PATCH_NAME}("lib.worker.send"):
        exercise()
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_nested_helper_inherits_outer_patch_decorator_at_call(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

@{_PATCH_NAME}("lib.worker.send")
def test_worker(mock_send):
    def exercise():
        run()
    exercise()
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_nested_helper_called_after_patch_exit_is_accepted(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_worker():
    with {_PATCH_NAME}("lib.worker.send"):
        def exercise():
            run()
        callback = exercise
    callback()
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_descriptor_positional_binding_matches_python(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

class Worker:
    def instance_run(self, value, sender=send):
        return sender(value)

    @classmethod
    def class_run(cls, value, sender=send):
        return sender(value)

    @staticmethod
    def static_run(value, sender=send):
        return sender(value)
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import Worker

def test_descriptors():
    worker = Worker()
    with {_PATCH_NAME}("lib.worker.send"):
        worker.instance_run("payload", object())
        Worker.instance_run(worker, "payload", object())
        Worker.class_run("payload", object())
        worker.class_run("payload", object())
        Worker.static_run("payload", object())
        worker.static_run("payload", object())
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_non_assignment_binders_shadow_patch_provenance(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from helper import helper_patch, provider
from lib.worker import run

def test_for_shadow():
    for {_PATCH_NAME} in [helper_patch]:
        with {_PATCH_NAME}("lib.worker.send"):
            run()

async def test_async_for_shadow(values):
    async for {_PATCH_NAME} in values:
        with {_PATCH_NAME}("lib.worker.send"):
            run()

def test_with_shadow():
    with provider() as {_PATCH_NAME}:
        with {_PATCH_NAME}("lib.worker.send"):
            run()

def test_except_shadow():
    try:
        raise RuntimeError
    except RuntimeError as {_PATCH_NAME}:
        with {_PATCH_NAME}("lib.worker.send"):
            run()

def test_walrus_shadow():
    if ({_PATCH_NAME} := helper_patch):
        with {_PATCH_NAME}("lib.worker.send"):
            run()
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_comprehension_target_does_not_shadow_later_canonical_patch(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_comprehension(helpers):
    values = [{_PATCH_NAME} for {_PATCH_NAME} in helpers]
    with {_PATCH_NAME}("lib.worker.send"):
        run()
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_comprehension_target_shadows_outer_instance_binding_inside(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

class Worker:
    def run(self, *, sender=send):
        return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import Worker

def test_comprehension(foreign_workers):
    worker = Worker()
    with {_PATCH_NAME}("lib.worker.send"):
        values = [worker.run() for worker in foreign_workers]
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_canonical_patch_remains_active_when_as_target_shadows_name(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_worker():
    with {_PATCH_NAME}("lib.worker.send") as {_PATCH_NAME}:
        run()
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_comprehension_walrus_invalidates_outer_patch_binding(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from helper import helper_patch
from lib.worker import run

def test_worker(values):
    bound = [({_PATCH_NAME} := helper_patch) for value in values]
    with {_PATCH_NAME}("lib.worker.send"):
        run()
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_sibling_nested_helper_chain_uses_call_site_patch(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_worker():
    def inner():
        run()
    def outer():
        inner()
    with {_PATCH_NAME}("lib.worker.send"):
        outer()
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_nested_decorator_uses_definition_time_canonical_binding(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from lib.worker import run

def test_worker():
    from unittest.mock import {_PATCH_NAME}
    @{_PATCH_NAME}("lib.worker.send")
    def exercise(mock_send):
        run()
    from helper import {_PATCH_NAME}
    exercise()
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_nested_decorator_keeps_definition_time_helper_binding(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from lib.worker import run

def test_worker():
    from helper import {_PATCH_NAME}
    @{_PATCH_NAME}("lib.worker.send")
    def exercise(mock_send):
        run()
    from unittest.mock import {_PATCH_NAME}
    exercise()
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_with_context_expressions_follow_python_item_timing(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from helper import cm
from lib.worker import run

def test_multi_item():
    with {_PATCH_NAME}("lib.worker.send"), cm(run()):
        pass

def test_outer_patch():
    with {_PATCH_NAME}("lib.worker.send"):
        with cm(run()):
            pass
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 2)

    def test_async_with_context_items_activate_sequentially(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from helper import async_cm
from lib.worker import run

async def test_worker():
    async with {_PATCH_NAME}("lib.worker.send"), async_cm(run()):
        pass
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_with_context_walrus_rebinds_patch_before_body(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from helper import cm, helper_patch
from lib.worker import run

def test_worker():
    with cm(({_PATCH_NAME} := helper_patch)):
        with {_PATCH_NAME}("lib.worker.send"):
            run()
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_definition_expressions_execute_under_the_enclosing_patch(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from helper import decorate
from lib.worker import run

def test_worker():
    with {_PATCH_NAME}("lib.worker.send"):
        @decorate(run())
        def exercise(
            value: (
                run()
            ) = run(),
            *,
            named=run(),
        ) -> run():
            pass
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 5)

    def test_future_annotations_are_not_evaluated_at_definition_time(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from __future__ import annotations

from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_worker():
    with {_PATCH_NAME}("lib.worker.send"):
        def exercise(value: run()) -> run():
            pass
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_lambda_default_is_immediate_but_escaping_body_is_deferred(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_worker():
    with {_PATCH_NAME}("lib.worker.send"):
        callback = lambda value=run(): (
            run()
        )
    callback()
""",
        }

        findings = find_ineffective_default_patches(production, tests)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].line, 7)

    def test_escaping_lambda_body_does_not_inherit_definition_patch(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_worker():
    with {_PATCH_NAME}("lib.worker.send"):
        callback = lambda: (
            run()
        )
    callback()
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_local_call_arguments_rebind_patch_before_helper_body(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from helper import helper_patch
from lib.worker import run

def test_worker():
    def exercise(value):
        with {_PATCH_NAME}("lib.worker.send"):
            run()
    exercise(({_PATCH_NAME} := helper_patch))
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_local_call_arguments_can_restore_patch_before_helper_body(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from helper import {_PATCH_NAME}
from unittest.mock import {_PATCH_NAME} as canonical_patch
from lib.worker import run

def test_worker():
    def exercise(value):
        with {_PATCH_NAME}("lib.worker.send"):
            run()
    exercise(({_PATCH_NAME} := canonical_patch))
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_local_call_argument_expression_is_visited_once_before_body(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_worker():
    def exercise(value):
        pass
    with {_PATCH_NAME}("lib.worker.send"):
        exercise(run())
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_local_call_argument_alias_swap_is_applied_exactly_once(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from helper import {_PATCH_NAME}
from unittest.mock import {_PATCH_NAME} as canonical_patch
from lib.worker import run

def test_worker():
    def exercise(first, second, third):
        with {_PATCH_NAME}("lib.worker.send"):
            run()
    exercise(
        (temporary := {_PATCH_NAME}),
        ({_PATCH_NAME} := canonical_patch),
        (canonical_patch := temporary),
    )
    with {_PATCH_NAME}("lib.worker.send"):
        run()
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 2)

    def test_class_patch_decorator_applies_to_test_methods(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

@{_PATCH_NAME}("lib.worker.send")
class TestWorker:
    def test_worker(self, mock_send):
        run()
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_class_construction_expressions_and_body_use_outer_patch(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from helper import decorate, make_base, make_metaclass
from lib.worker import run

def test_worker():
    with {_PATCH_NAME}("lib.worker.send"):
        @decorate(run())
        class Helper(
            make_base(run()),
            metaclass=make_metaclass(run()),
        ):
            value = run()
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 4)

    def test_class_patch_decorator_is_inactive_during_class_body(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

@{_PATCH_NAME}("lib.worker.send")
class TestWorker:
    value = run()

    def test_worker(self, mock_send):
        run(sender=object())
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_class_decorator_uses_definition_time_patch_binding(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from lib.worker import run

from unittest.mock import {_PATCH_NAME}
@{_PATCH_NAME}("lib.worker.send")
class TestCanonical:
    def test_worker(self, mock_send):
        run()
from helper import {_PATCH_NAME}

class TestHelper:
    @{_PATCH_NAME}("lib.worker.send")
    def test_worker(self, mock_send):
        run()
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 1)

    def test_assigned_patch_aliases_match_walrus_provenance(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_worker():
    assigned = {_PATCH_NAME}
    annotated: object = {_PATCH_NAME}
    with assigned("lib.worker.send"):
        run()
    with annotated("lib.worker.send"):
        run()
    with (walrus := {_PATCH_NAME})("lib.worker.send"):
        run()
""",
        }

        self.assertEqual(len(find_ineffective_default_patches(production, tests)), 3)

    def test_helper_overwrite_and_assigned_callbacks_stay_outside_boundary(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from helper import helper_patch
from lib.worker import run

def test_worker():
    replace = {_PATCH_NAME}
    replace = helper_patch
    callback = run
    with replace("lib.worker.send"):
        run()
    with {_PATCH_NAME}("lib.worker.send"):
        callback()
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_function_and_class_declarations_shadow_imported_callables(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()

class Worker:
    def __init__(self, *, sender=send):
        self.sender = sender
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run, Worker as ImportedWorker

def run():
    pass

class ImportedWorker:
    pass

def test_worker():
    with {_PATCH_NAME}("lib.worker.send"):
        run()
        ImportedWorker()
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_nested_class_declaration_shadows_imported_constructor(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

class Worker:
    def __init__(self, *, sender=send):
        self.sender = sender
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import Worker

def test_worker():
    class Worker:
        pass
    with {_PATCH_NAME}("lib.worker.send"):
        Worker()
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_absolute_import_overwrites_same_name_local_helper(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME} as outer_patch
from lib.worker import run

def test_worker():
    def {_PATCH_NAME}(*args):
        run()
    with outer_patch("lib.worker.send"):
        from unittest.mock import {_PATCH_NAME}
        with {_PATCH_NAME}("lib.worker.unrelated"):
            pass
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_relative_import_invalidates_canonical_patch_provenance(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_worker():
    from .helper import {_PATCH_NAME}
    with {_PATCH_NAME}("lib.worker.send"):
        run()
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_import_overwrites_constructed_instance_binding(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

class Worker:
    def run(self, *, sender=send):
        return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import Worker

def test_worker():
    worker = Worker()
    import helper as worker
    with {_PATCH_NAME}("lib.worker.send"):
        worker.run()
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_class_patch_uses_changed_and_restored_test_prefix(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

{_PATCH_NAME}.TEST_PREFIX = "check"
@{_PATCH_NAME}("lib.worker.send")
class ChangedPrefix:
    def check_run(self, mock_send):
        run()
    def test_run(self):
        run()

{_PATCH_NAME}.TEST_PREFIX = "test"
@{_PATCH_NAME}("lib.worker.send")
class RestoredPrefix:
    def check_run(self):
        run()
    def test_run(self, mock_send):
        run()
""",
        }

        findings = find_ineffective_default_patches(production, tests)
        self.assertEqual(tuple(finding.line for finding in findings), (9, 19))

    def test_alias_and_qualified_patch_prefix_assignments_are_tracked(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME} as replace
import unittest.mock as mock
from lib.worker import run

replace.TEST_PREFIX = "verify"
@replace("lib.worker.send")
class AliasedPrefix:
    def verify_run(self, mock_send):
        run()
    def test_run(self):
        run()

mock.{_PATCH_NAME}.TEST_PREFIX = "check"
@mock.{_PATCH_NAME}("lib.worker.send")
class QualifiedPrefix:
    def check_run(self, mock_send):
        run()
    def verify_run(self):
        run()
""",
        }

        findings = find_ineffective_default_patches(production, tests)
        self.assertEqual(tuple(finding.line for finding in findings), (10, 18))

    def test_dynamic_patch_test_prefix_assignment_fails_closed(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def configure(prefix):
    {_PATCH_NAME}.TEST_PREFIX = prefix
""",
        }

        with self.assertRaisesRegex(
            ValueError,
            r"tests/test_worker\.py:6: unsupported dynamic "
            r"unittest\.mock\.patch\.TEST_PREFIX assignment",
        ):
            find_ineffective_default_patches(production, tests)

    def test_unrelated_test_prefix_attribute_is_accepted(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": """
import helper

helper.TEST_PREFIX = configure_prefix()
""",
        }

        self.assertEqual(find_ineffective_default_patches(production, tests), ())

    def test_test_body_prefix_mutation_does_not_precede_module_class(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

def test_changes_prefix():
    {_PATCH_NAME}.TEST_PREFIX = "check"

@{_PATCH_NAME}("lib.worker.send")
class PrefixIsolation:
    def test_run(self, mock_send):
        run()
    def check_run(self):
        run()
""",
        }

        findings = find_ineffective_default_patches(production, tests)
        self.assertEqual(tuple(finding.line for finding in findings), (11,))

    def test_test_body_prefix_restore_does_not_override_module_prefix(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

{_PATCH_NAME}.TEST_PREFIX = "check"
def test_changes_prefix():
    {_PATCH_NAME}.TEST_PREFIX = "test"

@{_PATCH_NAME}("lib.worker.send")
class PrefixIsolation:
    def test_run(self):
        run()
    def check_run(self, mock_send):
        run()
""",
        }

        findings = find_ineffective_default_patches(production, tests)
        self.assertEqual(tuple(finding.line for finding in findings), (14,))

    def test_independently_analyzed_test_methods_restore_prefix(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        tests = {
            "tests/test_worker.py": f"""
from unittest.mock import {_PATCH_NAME}
from lib.worker import run

@{_PATCH_NAME}("lib.worker.send")
class First:
    def test_changes_prefix(self, mock_send):
        {_PATCH_NAME}.TEST_PREFIX = "check"

@{_PATCH_NAME}("lib.worker.send")
class Second:
    def test_run(self, mock_send):
        run()
    def check_run(self):
        run()
""",
        }

        findings = find_ineffective_default_patches(production, tests)
        self.assertEqual(tuple(finding.line for finding in findings), (13,))

    def test_direct_local_helper_propagates_prefix_within_runtime_chain(self) -> None:
        visitor = _TestPatchVisitor(
            test_path="tests/test_worker.py",
            captures={},
            class_paths=frozenset(),
            annotations_deferred=False,
        )
        visitor.aliases[_PATCH_NAME] = "unittest.mock.patch"
        visitor.patch_aliases[_PATCH_NAME] = "unittest.mock.patch"
        tree = ast.parse(
            f"""
def change_prefix():
    {_PATCH_NAME}.TEST_PREFIX = "check"
change_prefix()
""",
        )
        visitor.function_depth = 1
        for statement in tree.body:
            visitor.visit(statement)

        self.assertEqual(visitor.patch_test_prefix, "check")

    def test_star_imports_fail_closed_with_source_location(self) -> None:
        production = {
            "lib/worker.py": """
from lib.gateway import send

def run(*, sender=send):
    return sender()
""",
        }
        canonical = {
            "tests/test_worker.py": "from unittest.mock import *\n",
        }
        shadowing = {
            "tests/test_worker.py": f"""from unittest.mock import {_PATCH_NAME}
from helper import *
""",
        }

        with self.assertRaisesRegex(
            ValueError,
            r"tests/test_worker\.py:1: unsupported star import in "
            r"definition-default patch audit",
        ):
            find_ineffective_default_patches(production, canonical)
        with self.assertRaisesRegex(
            ValueError,
            r"tests/test_worker\.py:2: unsupported star import in "
            r"definition-default patch audit",
        ):
            find_ineffective_default_patches(production, shadowing)

    def test_known_bad_checker_rejects_a_planted_omission(self) -> None:
        finding = DefaultPatchFinding(
            test_path="tests/test_bad.py",
            line=4,
            callable_path="lib.worker.run",
            patched_target="lib.worker.send",
            injectable_keyword="sender",
        )

        with self.assertRaises(AssertionError):
            assert_default_patch_invariant((finding,), expected_valid=True)

    def test_repository_has_no_ineffective_definition_default_patches(self) -> None:
        repo_root = Path(__file__).resolve().parent.parent
        self.assertEqual(repository_default_patch_findings(repo_root), ())


if __name__ == "__main__":
    unittest.main()
