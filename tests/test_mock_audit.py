"""Audit: ban stateful-collaborator MagicMock and patches against our own code.

See ``.claude/rules/code-quality.md`` § "Mocks: leaf-seam only".

Zero-tolerance: any new flagged usage anywhere under ``tests/`` fails the
audit. Pre-existing multiline patch debt, previously invisible to the
physical-line scanner, is pinned by an exact target-count ratchet. If you
genuinely need to mock something, either drive the real function with
constructed inputs, use a typed fake from ``tests/fakes.py``, refactor the
caller to take a kwarg-DI seam, or add the target to the leaf-seam allowlist
in ``_mock_audit_scanner.py`` with a one-line rationale.
"""

from __future__ import annotations

import os
import sys
import unittest

# Import through the ``tests`` package, NOT via a tests-dir sys.path
# insert — putting tests/ at sys.path[0] makes a later ``import
# web.server`` resolve ``web`` to tests/web in module-order-dependent
# ways (the package shadows the real one whenever no earlier import
# already registered repo-root ``web`` in sys.modules).
sys.path.insert(0, os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..")))
from tests._mock_audit_scanner import (
    MULTILINE_PATCH_BASELINE,
    WEB_BEETS_MOCK_BASELINE,
    WEB_HARNESS_MOCK_BASELINE,
    count_beets_mock_overrides,
    count_harness_overrides,
    iter_scan_paths,
    find_multiline_patch_targets,
    scan_multiline_patch_targets,
    scan_tree,
    scan_web_beets_overrides,
    scan_web_harness_overrides,
)


class TestStatefulMockAudit(unittest.TestCase):
    def test_scan_reaches_tests_web_subpackage(self) -> None:
        """Pin the recursive walk (#408) — a revert to os.listdir would
        silently drop tests/web/ (including the shared harness) from
        the audit."""
        rels = {rel for rel, _ in iter_scan_paths()}
        self.assertIn(os.path.join("web", "test_routes_pipeline.py"), rels)
        self.assertIn(os.path.join("web", "_harness.py"), rels)

    def test_no_anti_pattern_call_sites(self) -> None:
        current = scan_tree()
        if not current:
            return
        lines = []
        for fname in sorted(current):
            for kind, count in sorted(current[fname].items()):
                lines.append(f"  - {fname}: {kind} ({count}×)")
        self.fail(
            "Stateful-MagicMock audit — see "
            "`.claude/rules/code-quality.md` § 'Mocks: leaf-seam only'.\n"
            "Replace each flagged usage with a typed fake, kwarg-DI seam, "
            "or real-input call.\n\n"
            + "\n".join(lines)
        )

    def test_multiline_patch_scanner_catches_known_bad_shape(self) -> None:
        source = '''
from unittest.mock import patch

with patch(
    "lib.owned.orchestration",
):
    pass
'''
        self.assertEqual(
            find_multiline_patch_targets(source),
            ["lib.owned.orchestration"],
        )

    def test_multiline_patch_target_counts_match_baseline(self) -> None:
        current = scan_multiline_patch_targets()
        self.assertEqual(
            current,
            MULTILINE_PATCH_BASELINE,
            "Multiline patch-target audit changed. New owned-function patches "
            "are forbidden; when removing legacy patches, shrink "
            "MULTILINE_PATCH_BASELINE in tests/_mock_audit_scanner.py.",
        )


class TestSysPathAudit(unittest.TestCase):
    """Ban every sys.path mutation that can shadow a real package.

    The audit resolves each ``<anything>.path.insert/append(...)`` call
    in tests/ via the AST — folding ``os.path.join/dirname/abspath/
    normpath`` chains and simple module-level variable assignments, so
    naming the directory through a variable is not an evasion. The
    policy (no exceptions — the tests/web/_harness.py inserts that
    deliberately reproduced the dual-load ambiguity were removed with
    #445 item 3; production strips its script-dir entry too, see
    tests/test_no_dual_load.py):

    - the repo root may be inserted or appended (how ``from lib.X``
      resolves in standalone module runs);
    - the tests/ dir (or a subdir) may only be APPENDED — a front
      insert makes ``tests/web`` shadow the real ``web`` package in
      module-order-dependent ways that full discovery masks;
    - any other directory (lib/, web/, scripts/, harness/, anything
      out-of-repo) is banned outright: it makes repo modules importable
      under bare second names — the issue #95 / PR #94 dual-load class;
    - a target the folder can't resolve is banned too: use a literal
      ``os.path`` shape the audit can prove safe.
    """

    @staticmethod
    def _fold(node, file_path: str, env: dict[str, str]) -> str | None:
        """Constant-fold a string expression: literals, ``__file__``,
        previously-folded module-level names, and os.path calls."""
        import ast
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            if node.id == "__file__":
                return os.path.abspath(file_path)
            return env.get(node.id)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            func = node.func
            if (isinstance(func.value, ast.Attribute)
                    and isinstance(func.value.value, ast.Name)
                    and func.value.value.id == "os"
                    and func.value.attr == "path"
                    and func.attr in ("join", "dirname", "abspath",
                                      "normpath")):
                args = [TestSysPathAudit._fold(a, file_path, env)
                        for a in node.args]
                if any(a is None for a in args):
                    return None
                fn = getattr(os.path, func.attr)
                return fn(*args)
        return None

    def test_no_shadowing_sys_path_mutations(self) -> None:
        import ast
        offenders: list[str] = []
        from tests._mock_audit_scanner import TESTS_DIR
        repo_root = os.path.dirname(TESTS_DIR)
        for dirpath, dirnames, filenames in os.walk(TESTS_DIR):
            dirnames[:] = [d for d in dirnames if d != "__pycache__"]
            for fname in sorted(filenames):
                if not fname.endswith(".py"):
                    continue
                path = os.path.join(dirpath, fname)
                rel = os.path.relpath(path, TESTS_DIR)
                with open(path, encoding="utf-8") as f:
                    tree = ast.parse(f.read(), filename=path)
                # Module-level NAME = <foldable str expr> assignments.
                env: dict[str, str] = {}
                for stmt in tree.body:
                    if (isinstance(stmt, ast.Assign)
                            and len(stmt.targets) == 1
                            and isinstance(stmt.targets[0], ast.Name)):
                        folded = self._fold(stmt.value, path, env)
                        if folded is not None:
                            env[stmt.targets[0].id] = folded
                for node in ast.walk(tree):
                    if not (isinstance(node, ast.Call)
                            and isinstance(node.func, ast.Attribute)
                            and node.func.attr in ("insert", "append")
                            and isinstance(node.func.value, ast.Attribute)
                            and node.func.value.attr == "path"):
                        continue
                    is_insert = node.func.attr == "insert"
                    dir_arg = node.args[1] if is_insert else node.args[0]
                    target = self._fold(dir_arg, path, env)
                    if target is None:
                        offenders.append(
                            f"  - {rel}:{node.lineno} (unresolvable "
                            "sys.path target — use a literal os.path "
                            "shape the audit can fold)")
                        continue
                    target = os.path.normpath(os.path.abspath(target))
                    if target == repo_root:
                        continue
                    in_tests = (target == TESTS_DIR
                                or target.startswith(TESTS_DIR + os.sep))
                    if in_tests and not is_insert:
                        continue
                    kind = ("front-insert of a tests/ dir" if in_tests
                            else f"non-root dir {target!r}")
                    offenders.append(f"  - {rel}:{node.lineno} ({kind})")
        self.assertFalse(
            offenders,
            "sys.path mutation that can shadow a real package (issue "
            "#95 dual-load class). Only the repo root may be inserted; "
            "tests/ dirs may only be appended:\n" + "\n".join(offenders),
        )


class TestWebHarnessMockRatchet(unittest.TestCase):
    """Ratchet for the #430 MagicMock → FakePipelineDB harness migration.

    Per-file counts of remaining MagicMock-harness usage in ``tests/web``
    must match ``WEB_HARNESS_MOCK_BASELINE`` exactly: growth means a new
    test leaned on mock shapes instead of FakePipelineDB state; shrink
    means a migration landed and the baseline entry must drop with it.
    """

    def test_counter_pins_evasion_shapes(self) -> None:
        """Document exactly what the ratchet counts — occurrences, not
        lines, including alias/bare-reference shapes that a dotted-only
        regex would miss (the r1 adversarial-review evasion vectors)."""
        cases = [
            ("dotted config", "self.mock_db.get_log.return_value = []", 1),
            ("alias assignment", "m = self.mock_db", 1),
            ("getattr reflection", "getattr(self.mock_db, name)", 1),
            ("bare positional arg", "helper(self.mock_db, x)", 1),
            ("two occurrences one line",
             "mock_db.a.return_value = 1; mock_db.b.side_effect = e", 2),
            ("substring does not count", "my_mock_database = 1", 0),
            ("harness ctor", "db = _pipeline_db_test_harness()", 1),
        ]
        for desc, line, expected in cases:
            with self.subTest(desc=desc):
                self.assertEqual(
                    count_harness_overrides(line, web_file=True), expected)
        # mock_db is only meaningful inside tests/web; the transitional
        # wrapped-MagicMock constructor is counted EVERYWHERE so it
        # cannot leak outside tests/web unseen.
        self.assertEqual(count_harness_overrides(
            "self.mock_db.get_log()", web_file=False), 0)
        self.assertEqual(count_harness_overrides(
            "db = _pipeline_db_test_harness()", web_file=False), 1)

    def test_counts_match_baseline_exactly(self) -> None:
        current = scan_web_harness_overrides()
        problems: list[str] = []
        for rel in sorted(set(current) | set(WEB_HARNESS_MOCK_BASELINE)):
            cur = current.get(rel, 0)
            base = WEB_HARNESS_MOCK_BASELINE.get(rel, 0)
            if cur > base:
                problems.append(
                    f"  - {rel}: {base} → {cur} MagicMock-harness occurrences. "
                    "New tests must seed FakePipelineDB state (see "
                    "tests/fakes.py), not configure mock_db returns."
                )
            elif cur < base:
                problems.append(
                    f"  - {rel}: {base} → {cur} MagicMock-harness occurrences. "
                    "Migration progress — shrink WEB_HARNESS_MOCK_BASELINE "
                    "in tests/_mock_audit_scanner.py to match"
                    + (" (delete the entry)." if cur == 0 else ".")
                )
        if problems:
            self.fail(
                "Web-harness MagicMock ratchet (#430) out of sync with "
                "WEB_HARNESS_MOCK_BASELINE:\n" + "\n".join(problems)
            )


class TestWebBeetsMockRatchet(unittest.TestCase):
    """Ratchet for the #445 beets-MagicMock → FakeBeetsDB migration.

    Per-file counts of remaining beets-mock variable-name occurrences in
    ``tests/web`` must match ``WEB_BEETS_MOCK_BASELINE`` exactly: growth
    means a new test configured beets mock shapes instead of seeding
    ``FakeBeetsDB`` state; shrink means a migration landed and the
    baseline entry must drop with it.
    """

    def test_counter_pins_evasion_shapes(self) -> None:
        """Document exactly what the ratchet counts — occurrences of the
        beets-mock variable names, including alias / suffixed / dotted
        shapes, while leaving the production ``web.server._beets``
        attribute and real-BeetsDB class fixtures uncounted."""
        cases = [
            ("dotted config",
             "self._beets.album_exists.return_value = True", 1),
            ("bare assignment", "mock_beets = MagicMock()", 1),
            ("suffixed patch arg", "mock_beets_cls: MagicMock,", 1),
            ("alias assignment", "b = self._beets", 1),
            ("library dotted form", "self.beets.get_recent.return_value", 1),
            ("injection counts the mock side only",
             "srv._beets = self.beets", 1),
            ("two occurrences one line",
             "srv._beets = self._beets; m = mock_beets", 2),
            ("production attr alone does not count",
             "self._orig_beets = srv._beets", 0),
            ("real-BeetsDB class fixture does not count",
             "cls._beets = BeetsDB(cls._db_path)", 0),
            ("beets_db var name does not count (general audit owns it)",
             "self.beets_db = FakeBeetsDB()", 0),
        ]
        for desc, line, expected in cases:
            with self.subTest(desc=desc):
                self.assertEqual(count_beets_mock_overrides(line), expected)

    def test_counts_match_baseline_exactly(self) -> None:
        current = scan_web_beets_overrides()
        problems: list[str] = []
        for rel in sorted(set(current) | set(WEB_BEETS_MOCK_BASELINE)):
            cur = current.get(rel, 0)
            base = WEB_BEETS_MOCK_BASELINE.get(rel, 0)
            if cur > base:
                problems.append(
                    f"  - {rel}: {base} → {cur} beets-mock occurrences. "
                    "New tests must seed FakeBeetsDB state (see "
                    "tests/fakes.py), not configure beets mock returns."
                )
            elif cur < base:
                problems.append(
                    f"  - {rel}: {base} → {cur} beets-mock occurrences. "
                    "Migration progress — shrink WEB_BEETS_MOCK_BASELINE "
                    "in tests/_mock_audit_scanner.py to match"
                    + (" (delete the entry)." if cur == 0 else ".")
                    + " Only shrink if the mocks were replaced with "
                    "FakeBeetsDB seeding — aliasing the mock away is "
                    "an evasion, not progress."
                )
        if problems:
            self.fail(
                "Web beets-MagicMock ratchet (#445) out of sync with "
                "WEB_BEETS_MOCK_BASELINE:\n" + "\n".join(problems)
            )


if __name__ == "__main__":
    unittest.main()
