"""Reject tests that patch dependencies captured in definition defaults."""

from __future__ import annotations

from pathlib import Path
import unittest

from tests._definition_default_patch_audit import (
    DefaultPatchFinding,
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
