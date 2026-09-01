"""Audit: every JS suite on disk must be reached by the canonical full suite.

See issue #537. PR #531 fixed a hardcoded ``node tests/test_js_X.mjs`` list
in ``scripts/run_tests.sh`` that had silently stopped covering three suites
(issue #520) — a glob (``for f in tests/test_js_*.mjs``) replaced it. This
audit is the JS analogue of ``tests/test_skip_audit.py``: it fails the
suite the moment that gap could reopen, whichever shape it takes:

    1. Someone reverts the glob to a hardcoded list that misses a file.
    2. Someone removes or narrows the glob so JS suites stop running
       altogether.

The parser tolerates both an explicit ``node tests/test_js_X.mjs`` line and
a glob-driven ``for`` loop whose body actually invokes the interpreter on
the loop variable — so a future reshuffle of ``run_tests.sh`` that still
exercises every suite does not false-fail this audit.
"""

from __future__ import annotations

import fnmatch
import glob
import os
import re
import unittest
from pathlib import Path

from tests._source_pins import pinned_source

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN_TESTS_SH = os.path.join(REPO_ROOT, "scripts", "run_tests.sh")
RUN_TEST_SUITE = os.path.join(REPO_ROOT, "scripts", "run_test_suite.py")
RUN_JS_CHECKS = os.path.join(REPO_ROOT, "scripts", "run_js_checks.sh")
TESTS_DIR = os.path.dirname(os.path.abspath(__file__))

# An explicit, hardcoded invocation: node tests/test_js_foo.mjs
_EXPLICIT_NODE_RE = re.compile(
    r'node\s+"?(tests/test_js_[A-Za-z0-9_]+\.mjs)"?'
)

# A shell "for VAR in PATTERN; do" loop whose pattern names JS test suites.
_FOR_GLOB_RE = re.compile(
    r'for\s+(\w+)\s+in\s+([^\s;]*test_js_[^\s;]*\.mjs)\s*;\s*do'
)

# How many lines after the "for" line to scan for a node invocation of the
# loop variable — generous enough for a multi-line loop body, small enough
# to stay tied to the loop that declared the variable.
_LOOP_BODY_WINDOW = 8


def _js_suite_names_on_disk() -> set[str]:
    """Every tests/test_js_*.mjs file that exists right now."""
    return {
        os.path.basename(p)
        for p in glob.glob(os.path.join(TESTS_DIR, "test_js_*.mjs"))
    }


def covered_js_suite_names(script_text: str, suite_names: set[str]) -> set[str]:
    """Return the subset of ``suite_names`` that ``script_text`` runs.

    Recognises two independent coverage shapes and unions their results:
    explicit hardcoded invocations, and glob-driven for loops that
    demonstrably invoke the interpreter on their loop variable.
    """
    covered: set[str] = set()

    for m in _EXPLICIT_NODE_RE.finditer(script_text):
        covered.add(os.path.basename(m.group(1)))

    lines = script_text.splitlines()
    for lineno, line in enumerate(lines):
        glob_match = _FOR_GLOB_RE.search(line)
        if glob_match is None:
            continue
        loop_var, pattern = glob_match.group(1), glob_match.group(2)
        body = "\n".join(lines[lineno:lineno + _LOOP_BODY_WINDOW])
        invokes_loop_var = re.search(
            r'node\s+"?\$\{?' + re.escape(loop_var) + r'\}?"?', body
        )
        if invokes_loop_var is None:
            continue  # the loop exists but never hands $VAR to node
        pattern_basename = os.path.basename(pattern)
        for name in suite_names:
            if fnmatch.fnmatch(name, pattern_basename):
                covered.add(name)

    return covered


# The three shapes a suite must have to be on the shared harness at all:
# it imports the module, builds a checker from its own module URL, and
# reaches the single exit path. Deliberately bounded syntax -- this is a
# local syntactic fact, not an attempt to understand the file.
_HARNESS_IMPORT_RE = re.compile(
    r"^import \{[^}]*\bsuite\b[^}]*\} from '\./js_harness\.mjs';$", re.MULTILINE
)
_HARNESS_SUITE_RE = re.compile(r"^const \w+ = suite\(import\.meta\.url\);$", re.MULTILINE)
_HARNESS_DONE_RE = re.compile(r"^\w+\.done\(\);$", re.MULTILINE)

# The two abandoned idioms. A suite carrying either is running its own
# harness again, whatever else it also does.
_OWN_COUNTER_RE = re.compile(r"^let (?:passed|failed) = 0;$", re.MULTILINE)
_NODE_ASSERT_RE = re.compile(r"^import .*from 'node:assert(?:/strict)?';$", re.MULTILINE)


def harness_violations(name: str, source: str) -> list[str]:
    """Every way ``source`` fails to be a suite on the shared harness.

    Accumulating rather than short-circuiting, so the known-bad self-tests
    below can prove each clause trips on its own world instead of only
    whichever happens to be checked first.
    """
    violations: list[str] = []
    if _HARNESS_IMPORT_RE.search(source) is None:
        violations.append(f"{name} does not import suite from ./js_harness.mjs")
    if _HARNESS_SUITE_RE.search(source) is None:
        violations.append(f"{name} never calls suite(import.meta.url)")
    if _HARNESS_DONE_RE.search(source) is None:
        violations.append(f"{name} never reaches the harness exit path")
    if _OWN_COUNTER_RE.search(source) is not None:
        violations.append(f"{name} declares its own passed/failed counter")
    if _NODE_ASSERT_RE.search(source) is not None:
        violations.append(f"{name} imports node:assert instead of the harness")
    return violations


class TestEveryJsSuiteUsesTheSharedHarness(unittest.TestCase):
    """One harness, one idiom (issue #1313 candidate 6).

    Before the shared module every suite hand-rolled its own assertion
    helpers -- two incompatible idioms across 23 files, ~500 duplicated
    lines, and failure reporting the coordinator could only read at FILE
    granularity. Nothing but this audit stops the next suite from starting
    a third idiom, because a bespoke harness passes perfectly well on its
    own.
    """

    def test_every_suite_on_disk_is_on_the_shared_harness(self) -> None:
        names = sorted(_js_suite_names_on_disk())
        self.assertTrue(names, "no tests/test_js_*.mjs files found")
        violations: list[str] = []
        for name in names:
            source = pinned_source(Path(TESTS_DIR) / name)
            violations.extend(harness_violations(name, source))
        self.assertEqual(
            violations,
            [],
            "JavaScript suites must use tests/js_harness.mjs: "
            + "; ".join(violations),
        )

    def test_the_harness_module_itself_is_present(self) -> None:
        harness = Path(TESTS_DIR) / "js_harness.mjs"
        self.assertTrue(harness.is_file(), "tests/js_harness.mjs is missing")
        source = pinned_source(harness)
        self.assertIn("export function suite(", source)
        self.assertIn("CRATEDIGGER_JS_FAILURE", source)
        self.assertIn("CRATEDIGGER_JS_DONE", source)

    def test_the_harness_module_is_not_itself_run_as_a_suite(self) -> None:
        """The harness must not match the glob `run_js_checks.sh` really uses.

        The pattern is READ OUT of the script rather than hand-typed: an
        earlier version of this test compared two literals
        (``fnmatch("js_harness.mjs", "test_js_*.mjs")``) and so proved
        nothing about the script at all — widening its glob to
        ``tests/*js*.mjs``, which DOES match the harness, left this test
        green (independent review, mutant B9). That is the constant-versus-
        hand-typed-literal shape `.claude/rules/code-quality.md` names.
        """
        script = pinned_source(Path(RUN_JS_CHECKS))
        patterns = [
            os.path.basename(m.group(2)) for m in _FOR_GLOB_RE.finditer(script)
        ]
        self.assertEqual(
            len(patterns),
            1,
            f"expected exactly one JS-suite glob in the script, got {patterns}",
        )
        self.assertFalse(
            fnmatch.fnmatch("js_harness.mjs", patterns[0]),
            f"the script's glob {patterns[0]!r} would run the harness module "
            "as if it were a suite",
        )
        self.assertNotIn("js_harness.mjs", _js_suite_names_on_disk())

    KNOWN_BAD = (
        (
            "no harness import",
            "const t = suite(import.meta.url);\nt.done();\n",
            "does not import suite",
        ),
        (
            "imported but never constructed",
            "import { suite } from './js_harness.mjs';\nt.done();\n",
            "never calls suite(import.meta.url)",
        ),
        (
            "constructed but never finished",
            (
                "import { suite } from './js_harness.mjs';\n"
                "const t = suite(import.meta.url);\n"
            ),
            "never reaches the harness exit path",
        ),
        (
            "a third idiom smuggled in beside the harness",
            (
                "import { suite } from './js_harness.mjs';\n"
                "const t = suite(import.meta.url);\n"
                "let passed = 0;\n"
                "t.done();\n"
            ),
            "declares its own passed/failed counter",
        ),
        (
            "node:assert smuggled in beside the harness",
            (
                "import assert from 'node:assert/strict';\n"
                "import { suite } from './js_harness.mjs';\n"
                "const t = suite(import.meta.url);\n"
                "t.done();\n"
            ),
            "imports node:assert instead of the harness",
        ),
    )

    def test_each_clause_trips_on_its_own_known_bad_world(self) -> None:
        for label, source, expected in self.KNOWN_BAD:
            with self.subTest(world=label):
                violations = harness_violations("test_js_x.mjs", source)
                self.assertEqual(
                    len(violations),
                    1,
                    f"{label} should trip exactly one clause, got {violations}",
                )
                self.assertIn(expected, violations[0])

    def test_a_conforming_world_trips_nothing(self) -> None:
        source = (
            "import { renderThing } from '../web/js/thing.js';\n"
            "\n"
            "import { suite } from './js_harness.mjs';\n"
            "\n"
            "const t = suite(import.meta.url);\n"
            "\n"
            "t.section('renderThing()');\n"
            "t.equal(renderThing(), 'x', 'it renders');\n"
            "\n"
            "t.done();\n"
        )
        self.assertEqual(harness_violations("test_js_x.mjs", source), [])


class TestJsSuiteAudit(unittest.TestCase):
    """Every tests/test_js_*.mjs file must run every scripts/run_tests.sh pass."""

    def setUp(self) -> None:
        self.wrapper_text = pinned_source(Path(RUN_TESTS_SH))
        self.coordinator_text = pinned_source(Path(RUN_TEST_SUITE))
        self.script_text = pinned_source(Path(RUN_JS_CHECKS))

    def test_every_js_suite_on_disk_is_covered(self) -> None:
        suite_names = _js_suite_names_on_disk()
        self.assertTrue(
            suite_names,
            "no tests/test_js_*.mjs files found — the fixture set that "
            "backs this audit is gone",
        )
        self.assertIn("scripts/run_test_suite.py", self.wrapper_text)
        self.assertIn('"scripts/run_js_checks.sh", "unit"', self.coordinator_text)
        covered = covered_js_suite_names(self.script_text, suite_names)
        missing = sorted(suite_names - covered)
        self.assertEqual(
            missing, [],
            "run_tests.sh does not reach these JS suites — issue #520/#537 "
            "gap has reopened: " + ", ".join(missing),
        )

    def test_parser_flags_a_suite_missing_from_a_hardcoded_list(self) -> None:
        """RED-case proof: a reverted hardcoded list that drops a suite must
        be caught without touching the real run_tests.sh."""
        suite_names = {"test_js_util.mjs", "test_js_pipeline.mjs", "test_js_new_thing.mjs"}
        fake_script = (
            "#!/usr/bin/env bash\n"
            "echo === JS unit tests ===\n"
            "node tests/test_js_util.mjs || exit 1\n"
            "node tests/test_js_pipeline.mjs || exit 1\n"
        )
        covered = covered_js_suite_names(fake_script, suite_names)
        self.assertEqual(covered, {"test_js_util.mjs", "test_js_pipeline.mjs"})
        self.assertNotIn("test_js_new_thing.mjs", covered)

    def test_parser_flags_a_glob_that_never_invokes_its_loop_variable(self) -> None:
        """RED-case proof: a glob loop that iterates but forgets to actually
        run the interpreter must not be credited as coverage."""
        suite_names = {"test_js_util.mjs"}
        fake_script = (
            "#!/usr/bin/env bash\n"
            "for f in tests/test_js_*.mjs; do\n"
            "  echo \"found $f\"\n"
            "done\n"
        )
        covered = covered_js_suite_names(fake_script, suite_names)
        self.assertEqual(covered, set())

    def test_parser_credits_a_genuine_glob_loop(self) -> None:
        """A real glob loop that hands $f to node covers every matching file."""
        suite_names = {"test_js_util.mjs", "test_js_pipeline.mjs"}
        fake_script = (
            "#!/usr/bin/env bash\n"
            "for f in tests/test_js_*.mjs; do\n"
            "  node \"$f\" || exit 1\n"
            "done\n"
        )
        covered = covered_js_suite_names(fake_script, suite_names)
        self.assertEqual(covered, suite_names)


if __name__ == "__main__":
    unittest.main()
