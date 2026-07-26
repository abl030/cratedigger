"""Generated patrol for the tests-tree Hypothesis profile-import audit.

The deterministic pins in ``test_hypothesis_profile_audit.py`` name the exact
module shapes that were live on ``main``. This property ranges over the whole
cross product of hypothesis-import forms, profile-import forms, surrounding
noise and tree depth, driving the REAL audit entry point (``audit_tests_tree``)
over a planted tree: only a canonical, module-level
``import tests._hypothesis_profiles`` may satisfy it, and every other shape —
including one that merely mentions the phrase in a docstring, comment or string
constant — must fail closed.
"""

from __future__ import annotations

import os
import tempfile
import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from tests.test_hypothesis_profile_audit import (
    CANONICAL_PROFILE_MODULE,
    PROFILE_MODULE_RELPATH,
    audit_tests_tree,
)


_HYPOTHESIS_FORMS: dict[str, str] = {
    "none": "",
    "import_root": "import hypothesis\n",
    "import_submodule_alias": "import hypothesis.strategies as st\n",
    "from_root": "from hypothesis import given\n",
    "from_submodule": "from hypothesis.strategies import integers\n",
    "from_stateful": "from hypothesis.stateful import rule\n",
    "deferred_in_function": (
        "def build():\n    import hypothesis\n    return hypothesis\n"
    ),
}

_PROFILE_FORMS: dict[str, str] = {
    "none": "",
    "canonical": f"import {CANONICAL_PROFILE_MODULE}\n",
    "canonical_noqa": f"import {CANONICAL_PROFILE_MODULE}  # noqa: F401\n",
    "aliased": f"import {CANONICAL_PROFILE_MODULE} as profiles\n",
    "from_package": "from tests import _hypothesis_profiles\n",
    "from_module": f"from {CANONICAL_PROFILE_MODULE} import settings\n",
    "nested_function": f"def wire():\n    import {CANONICAL_PROFILE_MODULE}\n",
    "nested_if": f"if True:\n    import {CANONICAL_PROFILE_MODULE}\n",
    "canonical_after_class": (
        "class _Planted:\n    pass\n"
        f"import {CANONICAL_PROFILE_MODULE}\n"
    ),
    "canonical_after_function": (
        "def _planted():\n    return None\n"
        f"import {CANONICAL_PROFILE_MODULE}\n"
    ),
}

#: Only these spellings run the profile side effect before any decorator
#: below them snapshots ``settings.default``.
CANONICAL_FORMS = frozenset({"canonical", "canonical_noqa"})

#: Canonical spelling, module level, but below the first class/function — the
#: shape the #882 PR1 review drove through real discovery and reproduced the
#: burst blocker with.
LATE_CANONICAL_FORMS = frozenset(
    {"canonical_after_class", "canonical_after_function"},
)

_NOISE: dict[str, str] = {
    "none": "",
    "docstring": (
        f'"""Mentions `import {CANONICAL_PROFILE_MODULE}` and '
        '`from hypothesis import given` in prose."""\n'
    ),
    "string_constant": (
        f'SNIPPET = "import {CANONICAL_PROFILE_MODULE}"\n'
        'OTHER = "from hypothesis import given"\n'
    ),
    "comment": (
        f"# import {CANONICAL_PROFILE_MODULE}\n"
        "# from hypothesis import given\n"
    ),
    "unrelated_imports": "import os\nimport unittest\nfrom pathlib import Path\n",
}

_SUBDIRECTORIES = ("", "web", os.path.join("world_model", "deep"))

#: A module shape that must never be reported, whatever it contains.
_EXCLUDED_SOURCE = "from hypothesis import given\n"


def assert_audit_verdict(
    *,
    offenders: tuple[str, ...],
    relpath: str,
    hypothesis_form: str,
    profile_form: str,
) -> None:
    """Independent oracle for one planted module's audit outcome.

    Derived from the chosen construction, deliberately not by re-parsing the
    source the audit already read.
    """
    expects_violation = (
        hypothesis_form != "none" and profile_form not in CANONICAL_FORMS
    )
    reported = [line for line in offenders if line.startswith(f"{relpath}:")]
    if len(reported) > 1:
        raise AssertionError(f"module reported more than once: {reported!r}")
    if expects_violation and not reported:
        raise AssertionError(
            f"audit accepted {hypothesis_form}/{profile_form} without a "
            "canonical module-level profile import",
        )
    if not expects_violation and reported:
        raise AssertionError(
            f"audit rejected compliant {hypothesis_form}/{profile_form}: "
            f"{reported!r}",
        )
    if not reported:
        return
    if profile_form in LATE_CANONICAL_FORMS:
        expected_phrase = "below the first class/function"
    elif profile_form != "none":
        expected_phrase = "non-canonical"
    else:
        expected_phrase = "without a module-level"
    if expected_phrase not in reported[0]:
        raise AssertionError(
            f"violation for {profile_form} was not named {expected_phrase!r}: "
            f"{reported[0]!r}",
        )


def write_planted_module(root: str, relpath: str, source: str) -> None:
    """Write one synthetic module into a planted tests tree."""
    path = os.path.join(root, relpath)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(source)


def substring_offenders(sources: dict[str, str]) -> tuple[str, ...]:
    """Known-bad mutant: classify by substring instead of import syntax."""
    return tuple(
        f"{relpath}: imports hypothesis without a module-level import"
        for relpath, source in sorted(sources.items())
        if "hypothesis" in source
        and f"import {CANONICAL_PROFILE_MODULE}" not in source
    )


class TestGeneratedProfileImportAudit(unittest.TestCase):
    @given(
        hypothesis_form=st.sampled_from(sorted(_HYPOTHESIS_FORMS)),
        profile_form=st.sampled_from(sorted(_PROFILE_FORMS)),
        noise=st.sampled_from(sorted(_NOISE)),
        subdirectory=st.sampled_from(_SUBDIRECTORIES),
    )
    @example(
        # The two burst blockers live on main: a bare ``@given`` module with
        # no profile import at all.
        hypothesis_form="from_root",
        profile_form="none",
        noise="none",
        subdirectory="",
    )
    @example(
        # A module that only talks about the import in its docstring.
        hypothesis_form="from_root",
        profile_form="none",
        noise="docstring",
        subdirectory="",
    )
    @example(
        # Compliant: the canonical spelling every repository site uses.
        hypothesis_form="from_root",
        profile_form="canonical_noqa",
        noise="unrelated_imports",
        subdirectory="web",
    )
    @example(
        # The review's planted probe: canonical spelling, module level, but
        # below the decorated class it was supposed to wire.
        hypothesis_form="from_root",
        profile_form="canonical_after_class",
        noise="none",
        subdirectory="",
    )
    def test_only_a_canonical_module_level_import_satisfies_the_audit(
        self,
        hypothesis_form: str,
        profile_form: str,
        noise: str,
        subdirectory: str,
    ) -> None:
        # The profile form precedes the hypothesis form so each form owns its
        # own position: the late-canonical forms carry the definition they must
        # sit below, and no other form is accidentally pushed past one.
        source = (
            _NOISE[noise]
            + _PROFILE_FORMS[profile_form]
            + _HYPOTHESIS_FORMS[hypothesis_form]
            + "VALUE = 1\n"
        )
        relpath = os.path.join(subdirectory, "test_planted.py")
        with tempfile.TemporaryDirectory() as root:
            write_planted_module(root, relpath, source)
            # Never reported: the profile module itself is the one structural
            # exclusion, and __pycache__ is pruned from the walk.
            write_planted_module(root, PROFILE_MODULE_RELPATH, _EXCLUDED_SOURCE)
            write_planted_module(
                root,
                os.path.join("__pycache__", "test_cached.py"),
                _EXCLUDED_SOURCE,
            )

            offenders = tuple(audit_tests_tree(root))

        assert_audit_verdict(
            offenders=offenders,
            relpath=relpath,
            hypothesis_form=hypothesis_form,
            profile_form=profile_form,
        )
        self.assertEqual(
            [line for line in offenders if not line.startswith(f"{relpath}:")],
            [],
            "an excluded or pruned path was audited",
        )


class TestProfileAuditCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests: the oracle and the syntax basis must both trip."""

    def test_oracle_trips_when_a_violating_module_is_accepted(self) -> None:
        with self.assertRaises(AssertionError):
            assert_audit_verdict(
                offenders=(),
                relpath="test_planted.py",
                hypothesis_form="from_root",
                profile_form="none",
            )

    def test_oracle_trips_when_a_compliant_module_is_rejected(self) -> None:
        with self.assertRaises(AssertionError):
            assert_audit_verdict(
                offenders=("test_planted.py: imports hypothesis",),
                relpath="test_planted.py",
                hypothesis_form="from_root",
                profile_form="canonical",
            )

    def test_oracle_trips_when_a_non_canonical_import_is_misnamed(self) -> None:
        with self.assertRaises(AssertionError):
            assert_audit_verdict(
                offenders=(
                    "test_planted.py: imports hypothesis without a "
                    "module-level import",
                ),
                relpath="test_planted.py",
                hypothesis_form="from_root",
                profile_form="aliased",
            )

    def test_oracle_trips_when_a_late_import_is_reported_as_missing(self) -> None:
        """A late canonical import must be named as a POSITION problem — the
        author has the statement, and 'missing' would send them looking for
        one they already wrote."""
        with self.assertRaises(AssertionError):
            assert_audit_verdict(
                offenders=(
                    "test_planted.py: imports hypothesis without a "
                    "module-level import",
                ),
                relpath="test_planted.py",
                hypothesis_form="from_root",
                profile_form="canonical_after_class",
            )

    def test_substring_mutant_is_fooled_by_a_string_constant(self) -> None:
        """The real audit is AST-based; prove that basis is load-bearing."""
        relpath = "test_planted.py"
        source = (
            _NOISE["string_constant"]
            + _HYPOTHESIS_FORMS["from_root"]
            + "VALUE = 1\n"
        )
        with tempfile.TemporaryDirectory() as root:
            write_planted_module(root, relpath, source)
            offenders = tuple(audit_tests_tree(root))

        self.assertEqual(substring_offenders({relpath: source}), ())
        assert_audit_verdict(
            offenders=offenders,
            relpath=relpath,
            hypothesis_form="from_root",
            profile_form="none",
        )
        with self.assertRaises(AssertionError):
            assert_audit_verdict(
                offenders=substring_offenders({relpath: source}),
                relpath=relpath,
                hypothesis_form="from_root",
                profile_form="none",
            )


if __name__ == "__main__":
    unittest.main()
