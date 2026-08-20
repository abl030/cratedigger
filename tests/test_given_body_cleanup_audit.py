"""Audit: no ``self.addCleanup``/``self.enterContext`` lexically inside a
function carrying a ``@given`` decorator.

Issue #1214's root cause: ``BeetsContractWorld`` was constructed inside
``@given`` bodies with cleanup registered via ``self.addCleanup(world.close)``.
``addCleanup`` fires once per test METHOD; Hypothesis re-executes the method
body once per EXAMPLE. So every example before the last leaked a live world
(two real tmpfs trees) until the method finally returned -- measured at the
daily gate's real budget, 2491 resident worlds / 1.59 GB from a single test
class, reproducing a production ``cratedigger-daily-checks.service`` ENOSPC
failure. The fix bound each resource's lifetime to the example that created
it (``with BeetsContractWorld() as world:`` and equivalents) instead of the
method. This audit is the missing other half of that fix per
``code-quality.md``'s own authoring question -- "what one-line change to
production would make this test fail?" -- applied to the FIX itself: without
this audit, reverting any one of the converted call sites back to the
addCleanup shape passes every other test in the suite untouched (issue #1214
review finding F1).

**Scope, precisely, and why it stops here.** This audit flags
``self.addCleanup(...)``/``self.enterContext(...)`` (and their async twins,
``self.addAsyncCleanup``/``self.enterAsyncContext`` -- unused in this repo
today, per ``tests/test_beets_contract_world_lifetime.py``'s own docstring,
but the grammar covers them so a future one is caught too) only when the call
is a LEXICAL descendant of a function whose own decorator list carries
``@given`` -- walking into ``with``/``for``/``if``/``try`` bodies, but
stopping at any NESTED ``def``/``async def``/``lambda`` boundary, since that
opens its own call frame with its own lifetime. It does **not** trace calls:
a resource built and cleaned up inside a plain helper METHOD that a
``@given`` body merely calls by name (``self._helper()``) is invisible to
this audit, because resolving "does this call end up back inside a
Hypothesis-driven loop" is call-graph/data-flow reasoning, which
``.claude/rules/code-quality.md`` § "Semantic source scanners are prohibited"
forbids. Three of the five extra leaks issue #1214 found and fixed while
widening the check were exactly this helper-mediated shape
(``tests/test_import_queue.py``'s ``_world``,
``tests/test_mbid_replace_service.py``'s ``_patch_externals``,
``tests/test_automation_recovery_debris_generated.py``'s cross-file
``_album_dir`` mixin call) -- this audit does not, and structurally cannot,
catch that shape; it was found by a one-shot manual sweep, not committed
machinery (``.claude/rules/scope.md``). What this audit DOES catch, and
permanently, is the direct-construction shape that was 42 of those 45 sites
and is the one a future author is most likely to reintroduce by copying an
existing ``@given`` test.

This audit reads decorators and calls with the stdlib ``ast`` and nothing
else: no data flow, no alias tracking, no inference about runtime behaviour.
"""

from __future__ import annotations

import ast
import os
import unittest
from dataclasses import dataclass

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))

#: The unittest cleanup-registration methods that bind a resource's release
#: to the enclosing test METHOD rather than to whatever loop is re-running
#: the function body.
CLEANUP_METHODS = frozenset({
    "addCleanup", "addAsyncCleanup", "enterContext", "enterAsyncContext",
})


class GivenBodyCleanupAuditError(Exception):
    """A tests-tree module could not be parsed, so it cannot be audited."""


@dataclass(frozen=True)
class CleanupViolation:
    """One ``self.<cleanup_method>(...)`` call found lexically inside a
    ``@given``-decorated function."""

    relpath: str
    lineno: int
    function_name: str
    cleanup_method: str

    def describe(self) -> str:
        return (
            f"{self.relpath}:{self.lineno}: `self.{self.cleanup_method}(...)` "
            f"lexically inside @given-decorated `{self.function_name}` -- "
            "addCleanup/enterContext (and their async twins) fire once per "
            "test METHOD, but Hypothesis re-executes a @given body once per "
            "EXAMPLE (issue #1214): bind the resource's lifetime to the "
            "example instead (`with <resource>() as x:` / an equivalent "
            "context manager), not the method"
        )


def _decorator_leaf_name(decorator: ast.expr) -> str | None:
    """Return the leaf name/attribute a decorator resolves to, or None.

    Handles both ``@given(...)`` (an ``ast.Call``) and a bare ``@given``
    name/attribute, and both ``@given`` and ``@hypothesis.given``.
    """
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    if isinstance(target, ast.Name):
        return target.id
    if isinstance(target, ast.Attribute):
        return target.attr
    return None


def _is_given_decorated(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
) -> bool:
    return any(
        _decorator_leaf_name(decorator) == "given"
        for decorator in node.decorator_list
    )


def _is_self_cleanup_call(node: ast.Call) -> str | None:
    """Return the cleanup method name for a ``self.<method>(...)`` call."""
    func = node.func
    if (
        isinstance(func, ast.Attribute)
        and isinstance(func.value, ast.Name)
        and func.value.id == "self"
        and func.attr in CLEANUP_METHODS
    ):
        return func.attr
    return None


class _BoundedCallCollector(ast.NodeVisitor):
    """Collect ``ast.Call`` nodes without crossing a nested function/lambda
    boundary -- those open their own call frame with their own lifetime, so
    a cleanup call inside one is not "lexically inside" the outer function
    in the sense this audit polices."""

    def __init__(self) -> None:
        self.calls: list[ast.Call] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        return

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        return

    def visit_Lambda(self, node: ast.Lambda) -> None:
        return

    def visit_Call(self, node: ast.Call) -> None:
        self.calls.append(node)
        self.generic_visit(node)


def _bounded_calls(body: list[ast.stmt]) -> list[ast.Call]:
    collector = _BoundedCallCollector()
    for stmt in body:
        collector.visit(stmt)
    return collector.calls


def _function_violations(
    node: ast.FunctionDef | ast.AsyncFunctionDef, *, relpath: str,
) -> list[CleanupViolation]:
    if not _is_given_decorated(node):
        return []
    violations: list[CleanupViolation] = []
    for call in _bounded_calls(node.body):
        method = _is_self_cleanup_call(call)
        if method is not None:
            violations.append(CleanupViolation(
                relpath=relpath,
                lineno=call.lineno,
                function_name=node.name,
                cleanup_method=method,
            ))
    return violations


def module_violations(source: str, *, label: str) -> list[CleanupViolation]:
    """Return every violation in this module source, or raise on a parse
    failure -- this audit never silently passes a module it could not read.
    """
    try:
        tree = ast.parse(source, filename=label)
    except SyntaxError as exc:
        raise GivenBodyCleanupAuditError(
            f"{label}: could not be parsed, so it cannot be audited for "
            f"example-scoped cleanup: {exc}"
        ) from exc

    violations: list[CleanupViolation] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            violations.extend(_function_violations(node, relpath=label))
    return violations


def iter_tests_tree_modules(root: str = TESTS_DIR) -> list[tuple[str, str]]:
    """Yield ``(relpath, abspath)`` for every ``.py`` under ``root``."""
    found: list[tuple[str, str]] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d != "__pycache__")
        for filename in sorted(filenames):
            if not filename.endswith(".py"):
                continue
            path = os.path.join(dirpath, filename)
            found.append((os.path.relpath(path, root), path))
    return found


def audit_tests_tree(root: str = TESTS_DIR) -> list[str]:
    """Return one offender line per violation, sorted by discovery order."""
    offenders: list[str] = []
    for relpath, path in iter_tests_tree_modules(root):
        with open(path, encoding="utf-8") as handle:
            source = handle.read()
        for violation in module_violations(source, label=relpath):
            offenders.append(violation.describe())
    return offenders


class TestGivenBodyCleanupAudit(unittest.TestCase):
    """No @given-decorated function registers cleanup that outlives it."""

    def test_no_given_body_registers_cleanup_via_addcleanup_or_entercontext(
        self,
    ) -> None:
        offenders = audit_tests_tree()
        self.assertEqual(
            offenders, [],
            "A resource constructed directly inside a @given body must bind "
            "its lifetime to the example (a `with` block), not the test "
            "method -- addCleanup/enterContext fire once per method while "
            "Hypothesis re-executes the body once per example (issue "
            "#1214). Offenders (paths relative to tests/):\n  "
            + "\n  ".join(offenders),
        )

    def test_scan_reaches_every_tests_subpackage(self) -> None:
        """Pin the recursive walk: a revert to a glob or ``os.listdir``
        would silently drop the subpackages this audit must cover."""
        relpaths = {relpath for relpath, _ in iter_tests_tree_modules()}
        self.assertIn("test_beets_contract_world_lifetime.py", relpaths)
        self.assertIn(os.path.join("web", "_harness.py"), relpaths)
        self.assertIn(os.path.join("world_model", "state_machine.py"), relpaths)
        self.assertIn(os.path.join("fakes", "beets_contract.py"), relpaths)


class TestGivenBodyCleanupCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests: the checker must reject planted module shapes,
    and must not flag shapes it deliberately does not cover."""

    def test_addcleanup_directly_in_a_given_body_is_rejected(self) -> None:
        source = (
            "import unittest\n"
            "from hypothesis import given, strategies as st\n"
            "\n"
            "class T(unittest.TestCase):\n"
            "    @given(n=st.integers())\n"
            "    def test_x(self, n):\n"
            "        self.addCleanup(lambda: None)\n"
        )
        violations = module_violations(source, label="planted.py")
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0].cleanup_method, "addCleanup")
        self.assertEqual(violations[0].function_name, "test_x")
        self.assertIn("issue #1214", violations[0].describe())

    def test_entercontext_directly_in_a_given_body_is_rejected(self) -> None:
        source = (
            "import unittest\n"
            "from hypothesis import given, strategies as st\n"
            "\n"
            "class T(unittest.TestCase):\n"
            "    @given(n=st.integers())\n"
            "    def test_x(self, n):\n"
            "        world = self.enterContext(open('/dev/null'))\n"
        )
        violations = module_violations(source, label="planted.py")
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0].cleanup_method, "enterContext")

    def test_addcleanup_inside_settings_plus_given_is_still_rejected(
        self,
    ) -> None:
        """A ``@settings(...)`` decorator stacked above ``@given`` must not
        blind the check to the ``given``-carrying function underneath."""
        source = (
            "import unittest\n"
            "from hypothesis import given, settings, strategies as st\n"
            "\n"
            "class T(unittest.TestCase):\n"
            "    @settings(max_examples=5)\n"
            "    @given(n=st.integers())\n"
            "    def test_x(self, n):\n"
            "        self.addCleanup(lambda: None)\n"
        )
        violations = module_violations(source, label="planted.py")
        self.assertEqual(len(violations), 1)

    def test_addcleanup_in_a_plain_non_given_method_is_accepted(self) -> None:
        source = (
            "import unittest\n"
            "class T(unittest.TestCase):\n"
            "    def test_x(self):\n"
            "        self.addCleanup(lambda: None)\n"
        )
        self.assertEqual(module_violations(source, label="planted.py"), [])

    def test_addcleanup_in_a_helper_called_from_a_given_body_is_not_caught(
        self,
    ) -> None:
        """Documents the audit's deliberate bound (see module docstring):
        this is exactly the shape of 3 of the 5 helper-mediated leaks issue
        #1214 found and fixed by manual sweep, not by static analysis --
        the audit does not trace call graphs, so it cannot see through
        ``self._helper()`` to the addCleanup inside ``_helper`` itself."""
        source = (
            "import unittest\n"
            "from hypothesis import given, strategies as st\n"
            "\n"
            "class T(unittest.TestCase):\n"
            "    def _helper(self):\n"
            "        self.addCleanup(lambda: None)\n"
            "\n"
            "    @given(n=st.integers())\n"
            "    def test_x(self, n):\n"
            "        self._helper()\n"
        )
        self.assertEqual(module_violations(source, label="planted.py"), [])

    def test_addcleanup_inside_a_nested_def_within_a_given_body_is_not_caught(
        self,
    ) -> None:
        """A nested ``def``/``lambda`` opens its own frame; a cleanup call
        inside one is out of scope for this audit's bounded walk."""
        source = (
            "import unittest\n"
            "from hypothesis import given, strategies as st\n"
            "\n"
            "class T(unittest.TestCase):\n"
            "    @given(n=st.integers())\n"
            "    def test_x(self, n):\n"
            "        def _inner():\n"
            "            self.addCleanup(lambda: None)\n"
            "        _inner()\n"
        )
        self.assertEqual(module_violations(source, label="planted.py"), [])

    def test_addcleanup_on_a_plain_object_is_not_flagged(self) -> None:
        """Only ``self.<method>(...)`` is in scope -- an unrelated object
        that happens to expose an ``addCleanup``-named method is not."""
        source = (
            "import unittest\n"
            "from hypothesis import given, strategies as st\n"
            "\n"
            "class T(unittest.TestCase):\n"
            "    @given(n=st.integers())\n"
            "    def test_x(self, n):\n"
            "        other.addCleanup(lambda: None)\n"
        )
        self.assertEqual(module_violations(source, label="planted.py"), [])

    def test_unparseable_module_fails_closed(self) -> None:
        with self.assertRaises(GivenBodyCleanupAuditError):
            module_violations("def broken(:\n", label="planted.py")


if __name__ == "__main__":
    unittest.main()
