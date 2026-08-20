"""Audit: no resource lexically inside a function carrying a ``@given``
decorator binds its lifetime to the enclosing test METHOD instead of the
Hypothesis EXAMPLE.

Issue #1214's root cause: ``BeetsContractWorld`` was constructed inside
``@given`` bodies with cleanup registered via ``self.addCleanup(world.close)``.
``addCleanup`` fires once per test METHOD; Hypothesis re-executes the method
body once per EXAMPLE. So every example before the last leaked a live world
(two real tmpfs trees) until the method finally returned -- a fresh
re-measurement at the daily gate's real budget
(``CRATEDIGGER_FUZZ_MAX_EXAMPLES=2500``) found a peak of 2491 concurrently
resident worlds / 1.59 GB within one test method's own run (issue #1214
itself first measured 2469 worlds / 1576.99 MB from a slightly different
run; both reproduce the same production ``cratedigger-daily-checks.service``
failure: ``OSError: [Errno 28] No space left on device`` -- an ENOSPC
exception, NOT a host OOM kill; issue #1214 states explicitly that no host
OOM kill occurred). The fix bound each resource's lifetime to the example
that created it (``with BeetsContractWorld() as world:`` and equivalents)
instead of the method. This audit is the missing other half of that fix per
``code-quality.md``'s own authoring question -- "what one-line change to
production would make this test fail?" -- applied to the FIX itself: without
this audit, reverting any one of the converted call sites back to the
addCleanup shape passes every other test in the suite untouched (issue #1214
review finding F1).

**Two clauses, each deliberately narrow.**

1. **Registered-cleanup clause.** Flags ``self.addCleanup(...)``/
   ``self.enterContext(...)`` (and their async twins,
   ``self.addAsyncCleanup``/``self.enterAsyncContext`` -- a repo-wide grep
   for either outside this audit module finds zero hits today, but the
   grammar covers them so a future one is caught too) when the call is a
   LEXICAL descendant of a function whose own decorator list carries
   ``@given``.
2. **Bare-construction clause (issue #1214 review finding A3).** The
   registered-cleanup clause alone is blind to a STRICTLY WORSE shape: a
   resource constructed inside a ``@given`` body with NO cleanup
   registered at all -- no ``with``, no ``addCleanup``, nothing. That
   shape is worse, not merely uncaught, because it strands the resource
   for the rest of the PROCESS's life, not merely the rest of the test
   METHOD (a bare ``BeetsContractWorld()`` construction whose seal is
   never undone leaves an operator-unremovable ``/dev/shm`` directory
   requiring the exact ``unshare --map-root-user --map-auto chown`` this
   fixture itself uses to reclaim -- verified: ``cleanup()`` while sealed
   raises ``PermissionError: [Errno 1] Operation not permitted``; the
   directory survives that failed cleanup and is gone only after a real
   ``close()``). This clause flags any call naming one of
   ``BARE_CONSTRUCTION_RESOURCE_NAMES`` lexically inside a ``@given`` body
   whose call node is not the ``context_expr`` of an enclosing ``with``/
   ``async with`` statement. Deliberately a small, explicit, hand-maintained
   name set -- not a general "infer which calls construct a resource"
   scanner -- mirroring ``tests/_lambda_audit.py``'s
   ``STRICT_RAISE_ADAPTER_KWARGS`` precedent for the same reason: the set
   is reviewed by a human each time it grows, not inferred from source.

**Both clauses stop at the same boundary, and it does NOT trace calls.**
Each walks into ``with``/``for``/``if``/``try`` bodies, but stops at any
NESTED ``def``/``async def``/``lambda`` boundary, since that opens its own
call frame with its own lifetime. A resource built and cleaned up inside a
plain helper METHOD that a ``@given`` body merely calls by name
(``self._helper()``) is invisible to both clauses, because resolving "does
this call end up back inside a Hypothesis-driven loop" is call-graph/
data-flow reasoning, which ``.claude/rules/code-quality.md`` § "Semantic
source scanners are prohibited" forbids. Three of the 45 leaking call sites
issue #1214 found and fixed (42 direct-construction, 3 helper-mediated) were
exactly this helper-mediated shape (``tests/test_import_queue.py``'s
``_world``, ``tests/test_mbid_replace_service.py``'s ``_patch_externals``,
``tests/test_automation_recovery_debris_generated.py``'s cross-file
``_album_dir`` mixin call) -- this audit does not, and structurally cannot,
catch that shape; it was found by a one-shot manual sweep, not committed
machinery (``.claude/rules/scope.md``). What this audit DOES catch, and
permanently, is the direct-construction shape that was 42 of those 45 sites
and is the one a future author is most likely to reintroduce by copying an
existing ``@given`` test.

This audit reads decorators and calls with the stdlib ``ast`` and nothing
else: no data flow, no alias tracking, no inference about runtime behaviour.
The driver (``audit_tests_tree``/``iter_tests_tree_modules``) is proven
end-to-end against real files on real disk, not only against synthetic
source strings handed straight to the checker (issue #1214 review finding
A1) -- see ``TestGivenBodyCleanupAuditDriverEndToEnd``.
"""

from __future__ import annotations

import ast
import os
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))

#: The unittest cleanup-registration methods that bind a resource's release
#: to the enclosing test METHOD rather than to whatever loop is re-running
#: the function body.
CLEANUP_METHODS = frozenset({
    "addCleanup", "addAsyncCleanup", "enterContext", "enterAsyncContext",
})

#: Deliberately small and explicit (issue #1214 review finding A3) -- the
#: bare-construction clause only ever looks for calls naming one of these.
#: Grow this set by hand, one reviewed entry at a time, when another
#: resource earns the same "must always be `with`-bound" treatment; never
#: infer membership from a call's arguments or return type.
BARE_CONSTRUCTION_RESOURCE_NAMES = frozenset({"BeetsContractWorld"})


class GivenBodyCleanupAuditError(Exception):
    """A tests-tree module could not be parsed, so it cannot be audited."""


@dataclass(frozen=True)
class CleanupViolation:
    """One offending call found lexically inside a ``@given``-decorated
    function -- either a ``self.<cleanup_method>(...)`` registration
    (``kind="registered_cleanup"``, ``detail`` is the method name) or a bare
    resource construction with no cleanup at all
    (``kind="bare_construction"``, ``detail`` is the resource's name)."""

    relpath: str
    lineno: int
    function_name: str
    kind: str
    detail: str

    def describe(self) -> str:
        if self.kind == "registered_cleanup":
            return (
                f"{self.relpath}:{self.lineno}: `self.{self.detail}(...)` "
                f"lexically inside @given-decorated `{self.function_name}` "
                "-- addCleanup/enterContext (and their async twins) fire "
                "once per test METHOD, but Hypothesis re-executes a @given "
                "body once per EXAMPLE (issue #1214): bind the resource's "
                "lifetime to the example instead (`with <resource>() as x:` "
                "/ an equivalent context manager), not the method"
            )
        return (
            f"{self.relpath}:{self.lineno}: `{self.detail}(...)` "
            f"constructed lexically inside @given-decorated "
            f"`{self.function_name}` with NO `with` statement and no "
            "cleanup registration at all -- worse than the "
            "addCleanup/enterContext shape above, this strands the "
            "resource for the rest of the process's life, not merely the "
            "rest of the test METHOD (issue #1214 review finding A3): wrap "
            f"the construction in `with {self.detail}(...) as x:`"
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


def _call_leaf_name(node: ast.Call) -> str | None:
    """Return the leaf name/attribute this call's callee resolves to."""
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


class _BoundedCallCollector(ast.NodeVisitor):
    """Collect ``ast.Call`` nodes without crossing a nested function/lambda
    boundary -- those open their own call frame with their own lifetime, so
    a call inside one is not "lexically inside" the outer function in the
    sense this audit polices. Separately records which ``ast.Call`` node
    ids appear as the ``context_expr`` of a ``with``/``async with`` item,
    so the bare-construction clause can tell a `with`-bound construction
    from a bare one without leaving this same bounded walk."""

    def __init__(self) -> None:
        self.calls: list[ast.Call] = []
        self.with_context_call_ids: set[int] = set()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        return

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        return

    def visit_Lambda(self, node: ast.Lambda) -> None:
        return

    def _mark_with_items(self, node: ast.With | ast.AsyncWith) -> None:
        for item in node.items:
            if isinstance(item.context_expr, ast.Call):
                self.with_context_call_ids.add(id(item.context_expr))

    def visit_With(self, node: ast.With) -> None:
        self._mark_with_items(node)
        self.generic_visit(node)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        self._mark_with_items(node)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        self.calls.append(node)
        self.generic_visit(node)


def _function_violations(
    node: ast.FunctionDef | ast.AsyncFunctionDef, *, relpath: str,
) -> list[CleanupViolation]:
    if not _is_given_decorated(node):
        return []
    collector = _BoundedCallCollector()
    for stmt in node.body:
        collector.visit(stmt)

    violations: list[CleanupViolation] = []
    for call in collector.calls:
        method = _is_self_cleanup_call(call)
        if method is not None:
            violations.append(CleanupViolation(
                relpath=relpath,
                lineno=call.lineno,
                function_name=node.name,
                kind="registered_cleanup",
                detail=method,
            ))
            continue
        name = _call_leaf_name(call)
        if (
            name in BARE_CONSTRUCTION_RESOURCE_NAMES
            and id(call) not in collector.with_context_call_ids
        ):
            violations.append(CleanupViolation(
                relpath=relpath,
                lineno=call.lineno,
                function_name=node.name,
                kind="bare_construction",
                detail=name,
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
    """No @given-decorated function registers cleanup that outlives it, and
    none constructs a policed resource with no cleanup at all."""

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
        would silently drop the subpackages this audit must cover. Also
        pins a real ``*_generated.py`` module by name (issue #1214 review
        finding A1): every one of the 42 real direct-construction
        violations this audit exists to catch lives in a ``*_generated.py``
        file, so a walk that silently excluded that name shape would defeat
        the audit's entire practical purpose while every other pin here
        (none of which name a ``*_generated.py`` module) stayed green."""
        relpaths = {relpath for relpath, _ in iter_tests_tree_modules()}
        self.assertIn("test_beets_contract_world_lifetime.py", relpaths)
        self.assertIn("test_beets_config_contract_generated.py", relpaths)
        self.assertIn(os.path.join("web", "_harness.py"), relpaths)
        self.assertIn(os.path.join("world_model", "state_machine.py"), relpaths)
        self.assertIn(os.path.join("fakes", "beets_contract.py"), relpaths)


class TestGivenBodyCleanupAuditDriverEndToEnd(unittest.TestCase):
    """Issue #1214 review finding A1: prove ``audit_tests_tree``/
    ``iter_tests_tree_modules`` actually read real file bytes from real
    disk and walk every real filename shape -- not merely that
    ``module_violations`` works correctly on synthetic source, which
    ``TestGivenBodyCleanupCheckerTripsOnViolations`` already proves. Two
    one-line mutants at the driver layer (replace the real file read with
    an empty string; additionally skip any ``*_generated.py`` name) left
    every other test in this module green -- see the issue #1214 kill
    matrix. The planted files below are deliberately named
    ``..._generated.py``: 42 of the 42 real violations this audit exists to
    catch live in ``*_generated.py`` files, so a driver proof using any
    other name would miss exactly the shape that matters most."""

    def test_audit_tests_tree_finds_a_planted_violation_on_real_disk(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="given-body-audit-e2e-") as tmp:
            planted = Path(tmp) / "test_planted_violation_generated.py"
            planted.write_text(
                "import unittest\n"
                "from hypothesis import given, strategies as st\n"
                "\n"
                "class T(unittest.TestCase):\n"
                "    @given(n=st.integers())\n"
                "    def test_x(self, n):\n"
                "        self.addCleanup(lambda: None)\n",
                encoding="utf-8",
            )
            offenders = audit_tests_tree(tmp)

        self.assertEqual(len(offenders), 1, offenders)
        self.assertIn("test_planted_violation_generated.py", offenders[0])
        self.assertIn("addCleanup", offenders[0])

    def test_audit_tests_tree_is_clean_on_a_planted_compliant_generated_file(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="given-body-audit-e2e-") as tmp:
            compliant = Path(tmp) / "test_planted_compliant_generated.py"
            compliant.write_text(
                "import unittest\n"
                "from hypothesis import given, strategies as st\n"
                "\n"
                "class T(unittest.TestCase):\n"
                "    @given(n=st.integers())\n"
                "    def test_x(self, n):\n"
                "        with open('/dev/null'):\n"
                "            pass\n",
                encoding="utf-8",
            )
            offenders = audit_tests_tree(tmp)

        self.assertEqual(offenders, [])


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
        self.assertEqual(violations[0].kind, "registered_cleanup")
        self.assertEqual(violations[0].detail, "addCleanup")
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
        self.assertEqual(violations[0].kind, "registered_cleanup")
        self.assertEqual(violations[0].detail, "enterContext")

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
        this is exactly the shape of 3 of the 45 leaks issue #1214 found
        and fixed by manual sweep, not by static analysis -- the audit does
        not trace call graphs, so it cannot see through ``self._helper()``
        to the addCleanup inside ``_helper`` itself."""
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

    def test_bare_construction_with_no_cleanup_at_all_is_rejected(
        self,
    ) -> None:
        """Issue #1214 review finding A3: the strictly worse, previously
        uncaught shape -- no `with`, no addCleanup, nothing."""
        source = (
            "import unittest\n"
            "from hypothesis import given, strategies as st\n"
            "from tests.fakes.beets_contract import BeetsContractWorld\n"
            "\n"
            "class T(unittest.TestCase):\n"
            "    @given(n=st.integers())\n"
            "    def test_x(self, n):\n"
            "        world = BeetsContractWorld()\n"
            "        world.cfg()\n"
        )
        violations = module_violations(source, label="planted.py")
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0].kind, "bare_construction")
        self.assertEqual(violations[0].detail, "BeetsContractWorld")
        self.assertIn("`with` statement", violations[0].describe())

    def test_with_bound_construction_is_not_a_bare_construction_violation(
        self,
    ) -> None:
        source = (
            "import unittest\n"
            "from hypothesis import given, strategies as st\n"
            "from tests.fakes.beets_contract import BeetsContractWorld\n"
            "\n"
            "class T(unittest.TestCase):\n"
            "    @given(n=st.integers())\n"
            "    def test_x(self, n):\n"
            "        with BeetsContractWorld() as world:\n"
            "            world.cfg()\n"
        )
        self.assertEqual(module_violations(source, label="planted.py"), [])

    def test_bare_construction_plus_addcleanup_reports_both_violations(
        self,
    ) -> None:
        """The exact original #1214 defect shape trips BOTH clauses -- the
        registered-cleanup clause (addCleanup fires per method) and the
        bare-construction clause (the call itself is not `with`-bound).
        Reporting both is intentional: each names a real, independently
        true fact about the same site, and either fact alone is enough to
        justify the fix."""
        source = (
            "import unittest\n"
            "from hypothesis import given, strategies as st\n"
            "from tests.fakes.beets_contract import BeetsContractWorld\n"
            "\n"
            "class T(unittest.TestCase):\n"
            "    @given(n=st.integers())\n"
            "    def test_x(self, n):\n"
            "        world = BeetsContractWorld()\n"
            "        self.addCleanup(world.close)\n"
        )
        violations = module_violations(source, label="planted.py")
        self.assertEqual(len(violations), 2)
        self.assertEqual(
            {v.kind for v in violations},
            {"registered_cleanup", "bare_construction"},
        )

    def test_bare_construction_of_an_unlisted_resource_is_not_flagged(
        self,
    ) -> None:
        """The bare-construction clause is a small, explicit, hand-maintained
        name set (see ``BARE_CONSTRUCTION_RESOURCE_NAMES``), not a general
        "any resource" inference -- a call naming something else is not in
        scope, however resource-shaped it looks."""
        source = (
            "import unittest\n"
            "from hypothesis import given, strategies as st\n"
            "\n"
            "class T(unittest.TestCase):\n"
            "    @given(n=st.integers())\n"
            "    def test_x(self, n):\n"
            "        handle = SomeOtherResource()\n"
        )
        self.assertEqual(module_violations(source, label="planted.py"), [])


if __name__ == "__main__":
    unittest.main()
