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
