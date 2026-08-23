"""Audit: ban the literal `os.kill(os.getppid(), ...)` shape anywhere in the
repo except tests/parent_signal_guard.py's own docstring.

Issue #1250: that exact shape SIGKILLed the developer's entire systemd user
session twice (2026-08-22, 2026-08-23) -- a fixture re-read `os.getppid()`
at signal time, raced its own sibling's teardown, and hit the reparented-to
user manager instead of the intended ProcessPoolExecutor worker. Every real
occurrence of this shape in the repo lives inside a Python STRING LITERAL,
not as a live call expression in the scanning file's own AST -- each of the
six sites this audit's own history covers builds a source string for a
CHILD process (a generated unittest fixture file, a `python -c` argv
element, or a stdlib-only `-S` shim body) rather than calling `os.kill`
directly. An AST walk for a real `Call` node (the pattern
`tests/test_generated_node_worker_audit.py` uses for a literal
`subprocess.run(["node", ...])`) would never see inside those strings --
Python's own `ast.parse` reports a big string constant, not a nested call
expression, for source text embedded in a literal. This audit instead
scans raw file TEXT for the hazard's exact substring shape, which is what
actually decides whether the eventual child process carries the bug,
regardless of whether that text is "real code" or a string in this file's
own `.py` source.

The fix is `tests.parent_signal_guard`: capture the intended parent PID
once, then re-verify its live identity (reparent / pid-1 / systemd-comm /
signature) before ever signalling it. Every real call site in the repo now
goes through it -- either the importable `guard_and_signal_parent` (for a
generated child that can import the repo) or the source-emitting
`guard_source_prelude` / `guard_kill_statement` (for one that cannot,
e.g. a stdlib-only `-S` shim or an inline `python -c` command).
"""

from __future__ import annotations

import os
import re
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Same class of stale-worktree/venv/build-noise exclusion as
# tests/test_lint_no_is_on_enum.py -- .claude holds nested agent worktree
# checkouts of this very repo, which would otherwise be scanned too.
SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", "node_modules", ".claude"}

# The two files allowed to spell the hazardous shape literally, both
# EXPLICITLY listed here (never inferred): tests/parent_signal_guard.py's
# own module docstring explains the hazard this audit exists to reject
# everywhere else, and this file's own known-bad self-tests below
# construct the literal shape as a string to prove the checker catches
# it -- both would otherwise trip the repo-wide scan against themselves.
ALLOWLISTED_FILES = {
    "tests/parent_signal_guard.py",
    "tests/test_parent_signal_guard_audit.py",
}

# Bounded grammar: `os.kill(` followed by whitespace then a bare
# `os.getppid()` call as the first argument. Flexible whitespace (`\s`
# includes newlines) so a call split across lines is still caught; this is
# a literal-text pattern match, not a parser -- it does not, and is not
# meant to, understand aliasing, indirection through a differently-named
# import, or any other semantic disguise. See module docstring for why a
# text scan (not an AST walk) is the correct bounded tool here: the
# dangerous text lives inside string literals, which AST parsing of the
# CONTAINING file cannot see into.
_HAZARD_PATTERN = re.compile(r"os\.kill\(\s*os\.getppid\(\s*\)")


def find_unguarded_getppid_kill(source: str) -> list[int]:
    """Return the 1-based line number of each hazard match's START."""
    return [
        source.count("\n", 0, match.start()) + 1
        for match in _HAZARD_PATTERN.finditer(source)
    ]


def _iter_python_files(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if name.endswith(".py"):
                yield Path(dirpath) / name


def audit_unguarded_getppid_kills(root: Path = REPO_ROOT) -> list[str]:
    offenders: list[str] = []
    for path in sorted(_iter_python_files(root)):
        relative = path.relative_to(root).as_posix()
        if relative in ALLOWLISTED_FILES:
            continue
        try:
            source = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for lineno in find_unguarded_getppid_kill(source):
            offenders.append(f"{relative}:{lineno}")
    return offenders


class TestParentSignalGuardAudit(unittest.TestCase):
    def test_no_unguarded_getppid_kill_anywhere_in_the_repo(self) -> None:
        offenders = audit_unguarded_getppid_kills()
        self.assertEqual(
            offenders,
            [],
            "Found a bare os.kill(os.getppid(), ...) -- issue #1250 (this "
            "shape SIGKILLed the developer's whole systemd session twice). "
            "Route through tests.parent_signal_guard instead: "
            "guard_and_signal_parent() for a call site that can import the "
            "repo, or guard_source_prelude()/guard_kill_statement() for a "
            "generated child body that cannot.\n\n" + "\n".join(offenders),
        )

    def test_checker_rejects_the_historical_literal_shape(self) -> None:
        source = (
            "import os\n"
            "import signal\n\n"
            "def test_worker_dies(self):\n"
            "    os.kill(os.getppid(), signal.SIGKILL)\n"
        )
        self.assertEqual(find_unguarded_getppid_kill(source), [5])

    def test_checker_rejects_the_shape_split_across_lines(self) -> None:
        source = (
            "import os\n\n"
            "os.kill(\n"
            "    os.getppid(),\n"
            "    9,\n"
            ")\n"
        )
        self.assertEqual(find_unguarded_getppid_kill(source), [3])

    def test_checker_rejects_the_shape_embedded_in_a_string_literal(self) -> None:
        """The real historical sites all spell the hazard inside a STRING
        -- generated child source, not live code in the scanning file's
        own AST. Prove the text scan catches this shape too (an AST-Call
        audit, by contrast, would see only one big string constant here)."""
        source = (
            'BODY = (\n'
            '    "import os\\n"\n'
            '    "os.kill(os.getppid(), signal.SIGKILL)\\n"\n'
            ')\n'
        )
        self.assertEqual(find_unguarded_getppid_kill(source), [3])

    def test_checker_accepts_the_guarded_call_shape(self) -> None:
        source = (
            "import tests.parent_signal_guard as parent_signal_guard\n\n"
            "_INTENDED_PARENT_PID = "
            "parent_signal_guard.capture_intended_parent_pid()\n\n"
            "def test_worker_dies(self):\n"
            "    parent_signal_guard.guard_and_signal_parent(\n"
            "        _INTENDED_PARENT_PID, signal.SIGKILL,\n"
            "    )\n"
        )
        self.assertEqual(find_unguarded_getppid_kill(source), [])

    def test_checker_accepts_a_captured_variable_kill(self) -> None:
        """The guard's own kill_fn(intended_parent_pid, sig) call, and the
        source-emitting form's os.kill(__pg_intended, sig) -- neither ever
        spells os.getppid() as the argument to os.kill."""
        source = (
            "def guard_and_signal_parent(intended_parent_pid, sig):\n"
            "    kill_fn(intended_parent_pid, sig)\n"
        )
        self.assertEqual(find_unguarded_getppid_kill(source), [])

    def test_allowlist_entries_are_exactly_the_guard_module_and_this_audit(
        self,
    ) -> None:
        """A stale or padded allowlist would silently widen the exemption
        -- pin its exact membership so a reviewer sees any change."""
        self.assertEqual(
            ALLOWLISTED_FILES,
            {"tests/parent_signal_guard.py", "tests/test_parent_signal_guard_audit.py"},
        )

    def test_audit_finds_a_planted_offender_on_a_real_filesystem_walk(
        self,
    ) -> None:
        """F10 (#1250 review): every other test here calls
        `find_unguarded_getppid_kill` directly on an in-memory string --
        none of them drives `audit_unguarded_getppid_kills` itself, the
        function `test_no_unguarded_getppid_kill_anywhere_in_the_repo`
        actually calls. Concretely, this closes the gap a mutant that
        makes `audit_unguarded_getppid_kills` return an empty list
        unconditionally (e.g. before its file loop ever runs) would
        otherwise leave open: `test_scan_reaches_tests_fakes_subpackage`
        exercises `_iter_python_files` directly and does not call
        `audit_unguarded_getppid_kills` at all, so it cannot catch a
        defect scoped to that function. (Review finding R4: an
        os.walk-pruning mutant is a DIFFERENT gap this specific test does
        NOT close -- `test_scan_reaches_tests_fakes_subpackage` already
        catches that one, since it walks the real repo tree where
        pruning has something to prune; this test's planted offender
        sits at its temp root, which `os.walk` always visits regardless
        of subdirectory pruning.) Drive the real function against a
        throwaway root with one planted offender and one clean neighbour
        using the guard's OWN accepted shape, and assert the offender --
        and ONLY the offender -- is found."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "planted_offender.py").write_text(
                "import os\n"
                "import signal\n\n"
                "def test_worker_dies(self):\n"
                "    os.kill(os.getppid(), signal.SIGKILL)\n",
                encoding="utf-8",
            )
            (root / "clean_neighbour.py").write_text(
                "import os\n\n"
                "def guard_and_signal_parent(intended_parent_pid, sig):\n"
                "    os.kill(intended_parent_pid, sig)\n",
                encoding="utf-8",
            )
            offenders = audit_unguarded_getppid_kills(root=root)
        self.assertEqual(offenders, ["planted_offender.py:5"])

    def test_scan_reaches_tests_fakes_subpackage(self) -> None:
        """Pin the recursive walk -- tests/fakes/deploy_pin.py is exactly
        the file whose -S shim originally carried three of the six real
        occurrences; a walk that stopped at the top of tests/ would miss
        it silently."""
        files = {p.relative_to(REPO_ROOT).as_posix() for p in _iter_python_files(REPO_ROOT)}
        self.assertIn("tests/fakes/deploy_pin.py", files)


if __name__ == "__main__":
    unittest.main()
