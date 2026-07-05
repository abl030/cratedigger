"""Audit: every JS suite on disk must be reached by scripts/run_tests.sh.

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

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN_TESTS_SH = os.path.join(REPO_ROOT, "scripts", "run_tests.sh")
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


class TestJsSuiteAudit(unittest.TestCase):
    """Every tests/test_js_*.mjs file must run every scripts/run_tests.sh pass."""

    def setUp(self) -> None:
        with open(RUN_TESTS_SH, encoding="utf-8") as f:
            self.script_text = f.read()

    def test_every_js_suite_on_disk_is_covered(self) -> None:
        suite_names = _js_suite_names_on_disk()
        self.assertTrue(
            suite_names,
            "no tests/test_js_*.mjs files found — the fixture set that "
            "backs this audit is gone",
        )
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
